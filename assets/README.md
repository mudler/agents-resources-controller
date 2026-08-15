# Logo

## The concept

A slot with one thing in it. Two jaws hold a single token between them, and the
token is drawn wider than the mouth they leave — it cannot slip out and a second
one cannot get in. That is the whole product: a device is held by exactly one
holder, who eventually hands it back.

The mark is built from the same vocabulary as the dashboard: flat cuts, a
4-unit grid, a muted grey that stays quiet, and one warm accent for the thing
that is currently in use.

## The files

| File | What it is | Use it for |
|---|---|---|
| `logo-mark.svg` | The mark alone, square, `viewBox="0 0 64 64"` | Favicon, avatar, app icon, anywhere square. This is the primary file. |
| `logo.svg` | Mark plus the `rc` wordmark, horizontal, `viewBox="0 0 148 64"` | README header, docs, slides, anywhere with room for a line. |
| `logo-mono.svg` | The mark in one colour via `fill="currentColor"` | Inlining into `internal/server/dashboard/index.html`. It inherits the surrounding text colour, so it needs no light/dark handling of its own. |

`logo-mono.svg` only works **inlined**. Loaded through `<img src>` or
`background-image`, `currentColor` has nothing to inherit and falls back to
black — use `logo-mark.svg` there instead.

### Why the wordmark says `rc` and not the project name

`rc` is what the operator types, a hundred times a day. The full name is long
enough that setting it beside the mark would either shrink it to caption size or
stretch the lockup to an aspect ratio nothing can use. Two glyphs also let the
letterforms be drawn to the mark's own construction rather than approximated
from a font: the `c` is one ring cut open on the same axis as the slot's mouth,
and every curved terminal in the wordmark is a radial cut exactly one stem
wide. Where the full name is wanted, set it in the host page's own type next to
`logo-mark.svg`.

## Palette

| Role | Value | On `#fbfbfd` | On `#16181d` |
|---|---|---|---|
| Structure — the jaws, the letterforms | `#6b7280` | 4.68:1 | 3.67:1 |
| Holder — the token | `#c86c08` | 3.63:1 | 4.74:1 |

`#6b7280` is the dashboard's `--muted` verbatim. `#c86c08` sits between the
dashboard's light `--busy` (`#b45309`) and dark `--busy` (`#fbbf24`), pulled
just deep enough to clear 3:1 on the light ground while staying above 4.5:1 on
the dark one — the mark has to work on both without swapping files.

Both are above the 3:1 WCAG floor for non-text contrast on both grounds. There
is no third colour and no gradient.

## Minimum size

**16 px** for the mark. That is the favicon size and it was checked there, on
both grounds: the jaws stay two pixels wide and the token stays a distinct
round mass. Below 16 px the jaws' arms close up and it turns into a blob.

For the lockup, **24 px tall** — at that height `rc` is still unambiguous.
Below that, drop the wordmark and use the mark alone.

## Constraints these files honour

- No external references of any kind: no fonts, no images, no filters, no
  `xlink:href`. The dashboard's no-external-requests rule is a security
  boundary, and these files can be inlined into it as-is.
- The wordmark is filled paths, not `<text>`. It renders identically with no
  font installed.
- Hand-authored. Integers on a 4-unit grid wherever the geometry allows;
  two decimal places only where an arc terminal lands off-grid.
- Nothing overlaps. Every shape is disjoint, which is why the one-colour
  version still reads.

## Editing

Keep the 4-unit grid and the 8-unit stem. If you scale the token, keep it wider
than the gap between the jaws' arms (currently `x` 24 to 40) — that overlap is
the idea, not a coincidence.
