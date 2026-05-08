# rdbdu

`ncdu` for Redis RDB snapshots. Streams the file (no full load), aggregates
estimated memory into a prefix tree split by `:`, and lets you walk it in
a TUI.

## Build

```bash
go mod tidy
go build -o rdbdu
```

## Run

```bash
./rdbdu /path/to/dump.rdb
```

Flags:

- `-sep ":"`   — separator for splitting keys into tree segments.
- `-depth 8`   — max segments tracked. Deeper parts are truncated so the tree
                 doesn't blow up on pathologically nested keys. Bump it if you
                 want to see more depth and have RAM to spare.
- `-raw`       — disable normalization. By default segments that look like
                 integers, UUIDs, or long hex blobs are folded into `<n>`,
                 `<uuid>`, `<hex>` so that, e.g., `user:<uuid>:profile`
                 collapses into a single subtree instead of one per user.

## Keys

| key                | action          |
| ------------------ | --------------- |
| `↑` / `↓`          | move            |
| `Enter` / `→`      | drill into node |
| `←` / `Backspace`  | up one level    |
| `g`                | back to root    |
| `q`                | quit            |

## Memory notes

Only the aggregated tree lives in RAM, not the keys. Tree size is roughly
*number of distinct prefix paths × ~150 bytes*. With normalization on, this
stays small even for hundred-million-key datasets. With `-raw` and many
unique IDs, expect several GB of RSS on big dumps — use `-depth` to bound it.

The "size" shown is the in-memory estimate from the RDB parser
(`hdt3213/rdb`'s `GetSize()`), not the on-disk RDB size — which is what you
actually want when hunting for what's eating Redis RAM.
