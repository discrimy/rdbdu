// rdbdu — ncdu-style TUI for Redis RDB snapshots.
//
// Streams the RDB (10s of GB are fine) and aggregates estimated memory into a
// prefix tree split by `:`. Only the aggregated tree lives in RAM.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gdamore/tcell/v2"
	"github.com/hdt3213/rdb/model"
	"github.com/hdt3213/rdb/parser"
	"github.com/rivo/tview"
)

// ----------------------------------------------------------------------------
// Tree
// ----------------------------------------------------------------------------

type Node struct {
	Name     string
	Size     int64
	Count    int64
	Types    map[string]struct{}
	Children map[string]*Node
	Parent   *Node
	sorted   []*Node // cached, built lazily on first access
}

func (n *Node) Path() string {
	if n.Parent == nil {
		return "/"
	}
	var parts []string
	for x := n; x != nil && x.Parent != nil; x = x.Parent {
		parts = append([]string{x.Name}, parts...)
	}
	return "/" + strings.Join(parts, ":")
}

// Sorted returns children largest-first. The result is cached on first call.
// We also drop the Children map afterwards — once we've sorted, the map's
// only job (lookup during insert) is done, and freeing it on huge trees
// reclaims a meaningful amount of RAM.
func (n *Node) Sorted() []*Node {
	if n.sorted != nil {
		return n.sorted
	}
	out := make([]*Node, 0, len(n.Children))
	for _, c := range n.Children {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Size > out[j].Size })
	n.sorted = out
	n.Children = nil
	return out
}

func (n *Node) ChildCount() int {
	if n.sorted != nil {
		return len(n.sorted)
	}
	return len(n.Children)
}

func (n *Node) HasChildren() bool {
	return n.ChildCount() > 0
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$`)

func normalize(s string) string {
	if s == "" {
		return s
	}
	allDigit := true
	for _, r := range s {
		if r < '0' || r > '9' {
			allDigit = false
			break
		}
	}
	if allDigit {
		return "<n>"
	}
	if uuidRe.MatchString(s) {
		return "<uuid>"
	}
	if len(s) >= 16 {
		allHex := true
		for _, r := range s {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
				allHex = false
				break
			}
		}
		if allHex {
			return "<hex>"
		}
	}
	return s
}

func insert(root *Node, key string, size int64, typeStr string, sep string, maxDepth int, doNormalize bool, collapseThreshold int) {
	parts := strings.Split(key, sep)
	if len(parts) > maxDepth {
		parts = parts[:maxDepth]
	}
	root.Size += size
	root.Count++
	if typeStr != "" {
		if root.Types == nil {
			root.Types = make(map[string]struct{})
		}
		root.Types[typeStr] = struct{}{}
	}
	n := root
	for _, p := range parts {
		if doNormalize {
			p = normalize(p)
		}
		if n.Children == nil {
			n.Children = make(map[string]*Node)
		}
		c, ok := n.Children[p]
		if !ok {
			// Inline collapse: when this node already has threshold distinct
			// children and a new segment arrives, redirect it into the <*>
			// bucket instead of creating yet another child.  This bounds the
			// number of children per node to threshold+1 at all times, so RAM
			// never explodes even when a prefix has millions of distinct keys
			// that don't match normalization patterns.
			if collapseThreshold > 0 && len(n.Children) >= collapseThreshold && p != "<*>" {
				star, starOk := n.Children["<*>"]
				if !starOk {
					star = &Node{Name: "<*>", Parent: n}
					n.Children["<*>"] = star
				}
				c = star
			} else {
				c = &Node{Name: p, Parent: n}
				n.Children[p] = c
			}
		}
		c.Size += size
		c.Count++
		if typeStr != "" {
			if c.Types == nil {
				c.Types = make(map[string]struct{})
			}
			c.Types[typeStr] = struct{}{}
		}
		n = c
	}
}

// TypeString returns a comma-separated sorted list of Redis types under this node.
func (n *Node) TypeString() string {
	if len(n.Types) == 0 {
		return ""
	}
	types := make([]string, 0, len(n.Types))
	for t := range n.Types {
		types = append(types, t)
	}
	sort.Strings(types)
	return strings.Join(types, ",")
}

// ----------------------------------------------------------------------------
// Streaming RDB parse
// ----------------------------------------------------------------------------

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	atomic.AddInt64(&c.n, int64(n))
	return n, err
}

func progressLoop(cr *countReader, total int64, keys *int64, stop <-chan struct{}) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			read := atomic.LoadInt64(&cr.n)
			pct := 0.0
			if total > 0 {
				pct = 100 * float64(read) / float64(total)
			}
			fmt.Fprintf(os.Stderr, "\rparsing %s / %s (%.1f%%) — %s keys      ",
				humanize.Bytes(uint64(read)),
				humanize.Bytes(uint64(total)),
				pct,
				humanize.Comma(atomic.LoadInt64(keys)))
		}
	}
}

// ----------------------------------------------------------------------------
// TUI
// ----------------------------------------------------------------------------

type ui struct {
	app      *tview.Application
	table    *tview.Table
	header   *tview.TextView
	footer   *tview.TextView
	root     *Node
	current  *Node
	topN     int
	cursorAt map[*Node]int // remembered cursor row per visited node
}

func runTUI(root *Node, topN int) {
	u := &ui{
		app:      tview.NewApplication(),
		root:     root,
		current:  root,
		topN:     topN,
		cursorAt: make(map[*Node]int, 64),
	}

	u.table = tview.NewTable().
		SetSelectable(true, false).
		SetFixed(1, 0)
	u.table.SetBorderPadding(0, 0, 0, 0)

	u.header = tview.NewTextView().SetDynamicColors(true)
	u.footer = tview.NewTextView().SetDynamicColors(true).SetText(
		" [yellow]↑/↓[-] move   [yellow]Enter/→[-] in   [yellow]←/Bksp[-] up   " +
			"[yellow]g[-] root   [yellow]Home/End[-] jump   [yellow]q[-] quit")

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(u.header, 1, 0, false).
		AddItem(u.table, 0, 1, true).
		AddItem(u.footer, 1, 0, false)

	u.table.SetSelectedFunc(func(row, _ int) { u.drillIn(row) })

	u.table.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		switch ev.Key() {
		case tcell.KeyLeft, tcell.KeyBackspace, tcell.KeyBackspace2, tcell.KeyEsc:
			u.drillOut()
			return nil
		case tcell.KeyRight:
			row, _ := u.table.GetSelection()
			u.drillIn(row)
			return nil
		case tcell.KeyHome:
			u.table.Select(1, 0)
			return nil
		case tcell.KeyEnd:
			n := len(u.visibleChildren())
			if n > 0 {
				u.table.Select(n, 0)
			}
			return nil
		case tcell.KeyRune:
			switch ev.Rune() {
			case 'q':
				u.app.Stop()
				return nil
			case 'g':
				u.cursorAt[u.current] = u.tableSelected()
				u.current = u.root
				u.render()
				return nil
			}
		}
		return ev
	})

	u.render()
	if err := u.app.SetRoot(flex, true).Run(); err != nil {
		fatal(err)
	}
}

func (u *ui) tableSelected() int {
	r, _ := u.table.GetSelection()
	return r
}

func (u *ui) drillIn(row int) {
	children := u.visibleChildren()
	idx := row - 1
	if idx < 0 || idx >= len(children) {
		return
	}
	next := children[idx]
	if !next.HasChildren() {
		return
	}
	u.cursorAt[u.current] = row
	u.current = next
	u.render()
}

func (u *ui) drillOut() {
	if u.current.Parent == nil {
		return
	}
	u.cursorAt[u.current] = u.tableSelected()
	u.current = u.current.Parent
	u.render()
}

func (u *ui) visibleChildren() []*Node {
	all := u.current.Sorted()
	if len(all) > u.topN {
		return all[:u.topN]
	}
	return all
}

func (u *ui) render() {
	all := u.current.Sorted()
	shown := all
	truncated := 0
	var truncSize int64
	if len(all) > u.topN {
		shown = all[:u.topN]
		truncated = len(all) - u.topN
		for _, c := range all[u.topN:] {
			truncSize += c.Size
		}
	}

	headExtra := ""
	if truncated > 0 {
		headExtra = fmt.Sprintf("   [yellow](showing top %d)[-]", u.topN)
	}
	u.header.SetText(fmt.Sprintf(" [::b]%s[-]   %s   %s keys   %d children%s",
		u.current.Path(),
		humanize.Bytes(uint64(u.current.Size)),
		humanize.Comma(u.current.Count),
		len(all),
		headExtra))

	u.table.Clear()

	u.table.SetCell(0, 0, headerCell("SIZE"))
	u.table.SetCell(0, 1, headerCell("%"))
	u.table.SetCell(0, 2, headerCell(""))
	u.table.SetCell(0, 3, headerCell("NAME"))
	u.table.SetCell(0, 4, headerCell("TYPE"))
	u.table.SetCell(0, 5, headerCell("KEYS"))
	u.table.SetCell(0, 6, headerCell("SUB"))

	parentSize := u.current.Size
	for i, c := range shown {
		row := i + 1
		pct := 0.0
		if parentSize > 0 {
			pct = 100 * float64(c.Size) / float64(parentSize)
		}
		nameColor := tcell.ColorWhite
		if c.Name == "<*>" {
			nameColor = tcell.ColorOrange
		} else if c.HasChildren() {
			nameColor = tcell.ColorAqua
		}

		u.table.SetCell(row, 0, tview.NewTableCell(humanize.Bytes(uint64(c.Size))).
			SetAlign(tview.AlignRight).SetMaxWidth(10))
		u.table.SetCell(row, 1, tview.NewTableCell(fmt.Sprintf("%5.1f%%", pct)).
			SetAlign(tview.AlignRight))
		u.table.SetCell(row, 2, tview.NewTableCell(bar(pct, 16)).
			SetTextColor(tcell.ColorGreen))
		u.table.SetCell(row, 3, tview.NewTableCell(c.Name).
			SetTextColor(nameColor).SetExpansion(1))
		u.table.SetCell(row, 4, tview.NewTableCell(c.TypeString()).
			SetTextColor(tcell.ColorDarkCyan))
		u.table.SetCell(row, 5, tview.NewTableCell(humanize.Comma(c.Count)).
			SetAlign(tview.AlignRight).SetTextColor(tcell.ColorGray))
		sub := c.ChildCount()
		subTxt := ""
		if sub > 0 {
			subTxt = fmt.Sprintf("%d", sub)
		}
		u.table.SetCell(row, 6, tview.NewTableCell(subTxt).
			SetAlign(tview.AlignRight).SetTextColor(tcell.ColorGray))
	}

	if truncated > 0 {
		row := len(shown) + 1
		msg := fmt.Sprintf("… %s more children, %s total (raise -top to see them)",
			humanize.Comma(int64(truncated)), humanize.Bytes(uint64(truncSize)))
		for col := 0; col < 7; col++ {
			cell := tview.NewTableCell("").SetSelectable(false)
			if col == 3 {
				cell = tview.NewTableCell(msg).
					SetTextColor(tcell.ColorYellow).SetSelectable(false).SetExpansion(1)
			}
			u.table.SetCell(row, col, cell)
		}
	}

	want := u.cursorAt[u.current]
	if want < 1 {
		want = 1
	}
	if want > len(shown) {
		want = len(shown)
	}
	if want >= 1 {
		u.table.Select(want, 0)
	}
	u.table.ScrollToBeginning()
}

func headerCell(s string) *tview.TableCell {
	return tview.NewTableCell(s).
		SetSelectable(false).
		SetTextColor(tcell.ColorYellow).
		SetAttributes(tcell.AttrBold)
}

func bar(pct float64, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct/100.0*float64(width) + 0.5)
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("·", width-filled)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func main() {
	var (
		sep      string
		maxDepth int
		raw      bool
		topN     int
		collapse int
	)
	flag.StringVar(&sep, "sep", ":", "key separator")
	flag.IntVar(&maxDepth, "depth", 8, "max key depth to track")
	flag.BoolVar(&raw, "raw", false, "disable normalization of numeric/uuid/hex/ip segments")
	flag.IntVar(&topN, "top", 1000, "max children shown per level (sorted by size desc)")
	flag.IntVar(&collapse, "collapse", 1000, "collapse nodes with more than N children into <*> bucket (0=disabled)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] dump.rdb\n\nFlags:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		fatal(err)
	}

	cr := &countReader{r: bufio.NewReaderSize(f, 1<<20)}
	root := &Node{Name: ""}

	var keys int64
	stop := make(chan struct{})
	go progressLoop(cr, fi.Size(), &keys, stop)

	dec := parser.NewDecoder(cr)
	err = dec.Parse(func(o model.RedisObject) bool {
		atomic.AddInt64(&keys, 1)
		insert(root, o.GetKey(), int64(o.GetSize()), o.GetType(), sep, maxDepth, !raw, collapse)
		return true
	})
	close(stop)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		fatal(err)
	}

	runTUI(root, topN)
}
