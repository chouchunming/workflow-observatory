# Codex and LLM Wiki operating contract

This file is the repository guidance for Codex. Apply it together with any
more-specific `AGENTS.md` found below the current directory; the more-specific
file governs files in its subtree.

## Codex working rules

- Work from the repository root unless a task explicitly targets a subdirectory.
- Inspect and search before editing. Use `rg` for repository text and file
  discovery when available, and preserve unrelated user changes in a dirty
  worktree.
- Make focused edits with `apply_patch`; do not use destructive Git commands
  such as `git reset --hard` or overwrite unrelated changes.
- Use a relevant Codex skill when its trigger matches the task. Do not assume a
  marketplace plugin, MCP server, external credential, or particular model is
  installed or available.
- Verify changes in proportion to risk. For this wiki, run the relevant
  `wiki_cli.py` checks listed below; report unrelated pre-existing warnings
  rather than changing them opportunistically.
- Do not run `publish`, create commits, push, or make other external changes
  unless the user explicitly requests that action.

This repository is a personal knowledge base compiled by an LLM. Treat raw
source bytes as immutable evidence and `wiki/` as the compiled, human-browsable
knowledge product. A newly captured generically named Markdown source must pass
the path-normalization gate below before it enters triage or compiled coverage.

## Four-phase loop

1. **Ingest** — put new material in `raw/`; never edit or delete its contents.
   Before first triage or citation, rename exact generic Markdown basenames
   `未命名`, `無標題`, or `Untitled` with an optional numeric suffix to a
   concise, content-derived, collision-free name.
2. **Compile** — run `python3 wiki_cli.py pending`, read one bounded source set, search existing concepts, then create or update the smallest relevant wiki pages.
3. **Query and enhance** — search the index and relevant pages first. File a reusable answer, comparison, chart, or slide outline back into `wiki/summary/` when it adds durable value.
4. **Lint and maintain** — run `python3 wiki_cli.py sources` after changing source citations and `python3 wiki_cli.py lint` before hand-off. Repair broken links and stale indices; do not claim an uncompiled source is covered.

## Compiled-page contract

- Store concepts in `wiki/concept/` and durable answers/derived outputs in `wiki/summary/`.
- Store canonical open loops as one Markdown record per file in `wiki/tasks/`; `wiki/_todo_list.md` is generated with `python3 wiki_cli.py tasks` and must not be hand-edited.
- Store workflow observations in `wiki/observations/` as operational records, not compiled concept, summary, raw evidence, or canonical task pages. Their `sources` describe a run but never count as compiled source coverage.
- Apply dedicated observation and tombstone lint to these records, but exclude them from generic graph, orphan, outbound-link, overview-drift, and source coverage scans. Exclude `invalidations/` and `.locks/` from observation record discovery; lint invalidation tombstones with their own schema and ignore lock files.
- Include `type`, `title`, `tags`, `timestamp`, and `sources` YAML frontmatter on every non-system page.
- Cite raw inputs as exact relative paths, such as `"raw/article.md"`.
- Use `[[Wikilinks]]` for relationships and update the smallest relevant index or overview section.
- Treat `wiki/_index.md`, `wiki/_overview.md`, `wiki/_sources.md`, `wiki/_queries.md`, and `wiki/_maintenance.md` as entry points, not ordinary notes.

## Raw triage policy

Before adding a generically named Markdown source to `_source_triage.md`, read
enough to assign it a descriptive filename without changing its bytes. Search
the exact source path first and update an existing triage record rather than
appending a duplicate decision. Once a
raw path is triaged or cited, it is frozen. A later rename requires an explicit
machine-readable migration manifest containing old path, new path, SHA-256, and
reason; preflight collisions and hashes, update every exact reference, and
verify source catalogs and lint afterward. Do not create compatibility copies.

Session evidence exports use
`raw/sessions/YYYY/MM/DD/session-<semantic-topic-slug>.md`. Multiple sessions on
the same day use different content-derived slugs; only an exact destination
collision receives an eight-character lowercase hexadecimal suffix. Preflight
the destination and never overwrite. The complete file is immutable after
creation, including frontmatter; later tags, domains, and synthesis state
belong in a sidecar or compiled index.

A session ingest that creates raw evidence and updates wiki pages or generated
catalogs is a substantial multi-step workflow. Invoke `observing-workflows`
and start one parent observation before the first mutation. If an unchanged
`Observation managed by parent run <run-id>` marker exists, reuse it and do not
start a child observation.

Record each intentional raw-data decision in `wiki/_source_triage.md`; raw files omitted from that manifest are `untriaged` by default. Do not edit the raw file itself. Use one of the following triage states:
- `compile`: 有價值之知識，需進行編譯 (Valuable knowledge that needs compilation)
- `untriaged`: 新來源，尚未決定是否處理 (New source awaiting a decision)
- `noise-ignore`: 無價值之廣告、登入成功通知，忽略 (Low-value ads or login notifications to ignore)
- `binary-extract-later`: dwg, zip, 大體積 pdf，待後續提取 (Binaries, archives, or large documents to extract later)
- `pii-sensitive`: 包含密碼、身分證等敏感性帳務/發票，需嚴密保護 (Credentials, ID numbers, or financial data requiring strict protection)
- `duplicate`: 與既有來源或已編譯資料重複，待需要時再比對 (Duplicate source)
- `reference-only`: 僅在診斷或查證時參考，非一般編譯待辦 (Reference material)

`python3 wiki_cli.py pending` 只列出 `untriaged` 或 `compile` 且尚未被編譯頁引用的來源。使用 `python3 wiki_cli.py sources --check` 確認 `_sources.md` 與當前 frontmatter、triage manifest 同步。

## Useful commands

```bash
python3 wiki_cli.py pending
python3 wiki_cli.py search "query"
python3 wiki_cli.py sources
python3 wiki_cli.py tasks
python3 wiki_cli.py lint
python3 wiki_cli.py fileback --title "..." --tags "..." --from-file /path/to/answer.md --source "raw/source.md"
```

Do not run `publish` or commit on the user's behalf unless explicitly requested. Its git side effect is not part of ordinary ingest, query, or maintenance work.
