# Observe 記錄機制設計

**日期：** 2026-07-12  
**修訂：** 2026-07-14，閉合 Scope、並行寫入、CLI automation、report 與隔離 skill-eval 契約  
**範圍：** LLM Wiki 工作，以及由 Codex 執行、達到記錄門檻的跨 repository 實作、修復、重構、文件與維護工作。

## 目標

把已完成工作的執行脈絡與結果集中保存成可閱讀、可稽核、可比較的 observation records，為日後改善 workflow 提供證據。全域個人 skill 負責在達到門檻的工作開始與結束時驅動記錄；第一版的重點是產生一致的資料，而不是自動選出「最佳」流程。

## 非目標

- 不取代 `wiki/tasks/`：task record 仍是未完成工作與下一步行動的唯一記錄。
- 不修改 `raw/`，也不將 observation 當作原始證據。
- 不自動判斷成功、缺陷、返工原因或 token 成本。
- 不根據小樣本自動推薦 workflow。
- 不記錄純聊天、唯讀搜尋或閱讀、只回答問題、只寫規劃而未實作，或簡單單檔小修。
- 不在工作的 repository 建立另一套 Wiki，也不修改其 `AGENTS.md`。

## 儲存模型

新增 `wiki/observations/`。一個已開始的工作 run 對應一份 Markdown record，從 `draft` 一次性轉為 final status；文件兼具人類可讀性與機器可彙整性。

每筆 record 的 frontmatter：

```yaml
type: observation
title: "Compile one bounded source set"
tags: [observation, workflow]
run_id: "obs-YYYYMMDD-HHMMSS-a1b2c3"
timestamp: "2026-07-12T21:00:00+08:00"
project: "llmwiki"
workspace: "llmwiki"
workspace_id: "7f4a1c29e083"
revision: "7316e5b"
working_tree: "dirty"
agent_surface: "codex"
task_type: compile
workflow_variant: compile-with-review
status: success
start_mode: planned
task_ref: "[[open-loop-record]]" # optional
sources: []
```

- `run_id` 是 record 的唯一識別，格式為 `obs-YYYYMMDD-HHMMSS-<6 lowercase hex>`，亦為檔名 stem，且是 `observe finish` 接受的引數。隨機 suffix 解決同秒並行碰撞，不得調整或虛構開始 timestamp。
- `project` 是人類可辨識的專案名稱；`workspace` 是經清理的 repository 或工作區 basename，不得保存完整絕對路徑。這些 provenance fields 由 CLI 根據不持久化的 subject workspace root 產生，不由 skill 重作 normalization。
- `workspace_id` 是 stable identity：subject root 位於 Git worktree 內時先 canonicalize 至 Git top-level；優先對 normalized Git remote 做 SHA-256，無 remote 時對 canonical workspace root 做 SHA-256，只保存前 12 個 lowercase hex，不保存 hash input。Remote normalization 將 scp-style SSH 與 URL-style remote 統一為 `lowercase-host/path-without-leading-slash-or-.git`，移除 scheme、userinfo、port 的預設值與重複 slash，但保留 repository path 的大小寫。
- `project` 預設為 normalized remote path 的最後一段；無 remote 時使用 sanitized workspace basename。只有使用者明確提供專案名稱時才覆寫預設值。
- `revision` 是開始時的 Git SHA，無 Git 時為 `unknown`；`working_tree` 只能是 `clean`、`dirty` 或 `unknown`；`agent_surface` 第一版固定為 `codex`。
- `task_type` 與 `workflow_variant` 使用下表的合法組合，讓樣本可以比較。無法可靠細分的變更使用 `maintenance`，不得臨時創造新值。
- `task_ref` 可省略，只在 run 對應中央 Wiki open loop 時使用。`sources` frontmatter 必須存在；未處理中央 raw source 時使用空 list，處理來源時才列出存在的 `raw/...`。
- `timestamp` 是開始時間；完成時間與 elapsed time 在正文 Metrics 區記錄，避免把派生欄位與起始記錄混淆。
- `start_mode` 只能是 `planned` 或 `late`；任務開始前已符合門檻使用 `planned`，執行途中才越過門檻使用 `late`。
- `observe start` 建立的未完成 record 使用內部狀態 `draft`；`draft` 不納入完成率。`observe finish` 只允許把它一次性轉為 `success`、`partial`、`failed`、`rolled-back` 或 `superseded`。

合法 taxonomy：

| task_type | 合法 workflow_variant |
|---|---|
| `feature`、`bugfix`、`refactor`、`documentation` | `implementation-basic`、`implementation-with-review` |
| `maintenance` | `maintenance-basic`、`implementation-with-review` |
| `compile`、`inbox-processing` | `compile-basic`、`compile-with-review` |
| `query` | `research-basic` |

`query` 僅用於會產生 durable fileback 的查詢；純唯讀回答不記錄。

Canonical skill 必須在第一次真實 `observe start` 呼叫前直接列出上表，不得只提供 `<type>` 與 `<variant>` 佔位符。Agent 必須先選定合法配對；`maintenance-basic` 只能與 `maintenance` 搭配。若 CLI 因 taxonomy 拒絕 start，該次沒有建立 run，必須保留此 recording failure 事實，並可在不改變原任務範圍下使用合法配對重試。Skill 也必須在 completion 呼叫點直接列出 `success`、`partial`、`failed`、`rolled-back`、`superseded`，並明確排除非法的 `completed`。

## Operational-record 邊界

`wiki/observations/` 是 operational records，不是 concept、summary、raw evidence 或 canonical task。實作時必須同步更新 `AGENTS.md` 的 compiled-page contract，明確加入此類型。Observation：

- 接受專用 observation schema lint。
- 不參與 concept orphan、零 outbound-link 與 overview drift 檢查。
- 不被 `_sources.md` 視為 raw source 已編譯的證據；其 `sources` 只記錄該 run 處理過的中央 `raw/...`，不得填入外部 repository 路徑。
- `invalidations/` 與 `.locks/` 不納入 observation record discovery；invalidation tombstone 接受獨立 schema lint。
- 第一版不新增 dashboard；人類入口為 `wiki/observations/` 與唯讀的 `observe report`。

## 文件內容

正文使用固定段落：

1. **Scope**：目標、處理範圍與排除範圍。
2. **Execution evidence**：關鍵指令、檢查點，以及去敏感化的相對 artifact labels；只有中央 Wiki 頁面使用 Wikilinks。
3. **Outcome and observation**：結果、失敗模式與下一次應保留或驗證的假設；觀察與推論要明確區隔。
4. **Follow-up**：尚未完成時記錄一個去敏感化的 next action；若對應中央 Wiki open loop，另外以 `task_ref` 連結 canonical `wiki/tasks/` record。
5. **Metrics**：一個固定格式的 YAML code block，包含 `finished_at`、`elapsed_seconds`、`verification`、`review_rounds`、`defects_found`、`rework_count` 與 `rework_reason`。無可靠值必須寫為 `unknown`，不可代以零。

Metrics block 的欄位與格式如下；數值未知時用字串 `unknown`：

```yaml
finished_at: "2026-07-12T21:18:00+08:00"
elapsed_seconds: 1080
verification: pass | fail | not-run | unknown
review_rounds: 1 | unknown
defects_found: 0 | unknown
rework_count: 1 | unknown
rework_reason: none | "revised source citations" | unknown
```

- `review_rounds`：初次實作後完成的正式 review cycle 數；clean review 也計一次，自我檢查不計。
- `defects_found`：初次實作完成後，由測試或 reviewer 確認、且需要修正的獨立缺陷數；建議或偏好不計。
- `rework_count`：因 failed validation 或 review finding 而重新進入實作的次數。
- `rework_reason`：以精簡、去敏感化文字說明主要返工原因；無返工使用 `none`。

已完成 record 不得被 `observe finish` 或人工編輯覆寫。第一版不更正原 record；若 record 本身錯誤，使用 `observe invalidate <run-id> --reason <sanitized-reason>` 建立獨立 tombstone `wiki/observations/invalidations/<run-id>.md`。原 record 保持不變，report 將其排除於 aggregates 並另列 invalidated count。重複 invalidation 回傳 state error。

Invalidation tombstone 使用固定 frontmatter，且沒有正文：

```yaml
type: observation-invalidation
title: "Invalidate obs-20260713-100000-a1b2c3"
tags: [observation, invalidation]
timestamp: "2026-07-13T11:00:00+08:00"
target_run_id: "obs-20260713-100000-a1b2c3"
reason: "invalid fixture"
sources: []
```

`target_run_id` 必須存在且已是 final status；draft 不得 invalidate。Reason 遵守單行、200-code-point 與隱私限制。

Final status 語義與一致性規則：

- `success`：承諾的 Scope 全部完成，且 `verification` 不得為 `fail`。
- `partial`：存在可用產物，但 Outcome 必須明列哪些 Included items 未完成；Follow-up 必須包含實際 next action 或中央 task reference，不得使用 `None — no further action`。
- `failed`：沒有符合 Scope 的可交付結果；Follow-up 必須包含下一步，或明列 `None — no further action`。
- `rolled-back`：本 run 的變更已撤銷；Follow-up 必須包含下一步，或明列 `None — no further action`。
- `superseded`：使用者以 material scope change 取代目前 Scope；必須記錄 `superseded_by: <new-run-id>`，不進入成功率分子或分母。

## CLI 工作流

`wiki_cli.py` 新增以下命令：

- `observe start`：要求 title、`--subject-root`、agent surface、start mode、task type、workflow variant 與 `--scope-from-file`；CLI 從 existing subject directory 內部推導 project、workspace、workspace ID、revision 與 working-tree state，再從 UTF-8 Scope payload 建立帶唯一 run ID 與開始時間的 draft record。可選 `--project` 明確覆寫 derived project，並可選 `--task` 與重複的 `--source`。`--subject-root` 只作 provenance input，不寫入 record。
- `observe finish <run-id>`：要求 status 與 `--from-file`；從 UTF-8 Markdown completion payload 寫入 Execution evidence、Outcome and observation、Follow-up，以及依時間戳計算的 Metrics。`superseded` 另要求 `--superseded-by <new-run-id>`。Payload 必須包含上述固定 headings；CLI 拒絕缺段、重複段落或不可解析的 payload。CLI 不自行猜測缺陷、返工、結果或成本。
- `observe invalidate <run-id>`：要求單行、去敏感化的 `--reason`；以 atomic exclusive create 建立 immutable tombstone，不修改原 record。
- `observe report`：只讀取 record frontmatter 與固定格式的 Metrics block，支援 `--project`、`--workspace`、`--workspace-id`、`--task-type`、`--status`、`--since YYYY-MM-DD` 與 `--until YYYY-MM-DD`。日期篩選以 record 的 aware start timestamp 換算至 LLM Wiki 本地時區後取日期，since/until 皆含端點。預設按 project、workspace ID、task type、workflow variant 分組，在組內顯示各 status count、樣本數、成功率、平均 elapsed time、總返工次數、平均返工次數、缺漏率，以及仍為 `draft` 的數量與年齡。

此為半自動流程：CLI 寫入客觀可得的識別、時間與驗證資料；人或 Agent 在 start 時提供 Scope payload，在 finish 時提供 completion payload。不得直接手改 draft 來繞過 CLI validation。

Scope payload 使用以下固定格式，不得包含其他二級 headings：

```markdown
## Scope

- Goal: Implement validated observation reporting.
- Included: Observation report aggregation and filters.
- Excluded: Workflow recommendation and automatic correction.
```

Goal 與 Included 不得空白；Excluded 無排除項時使用 `None.`。CLI 必須在建立 draft 前完整驗證 payload。

Completion payload 使用以下固定格式；CLI 由 start/finish timestamps 計算並補入 `finished_at` 與 `elapsed_seconds`：

````markdown
## Execution evidence

- Verification: `python3 -m unittest tests.test_observation_records -v` — pass
- Artifacts: `wiki_cli.py`, `tests/test_observation_records.py`

## Outcome and observation

- Outcome: Added observation lifecycle validation.
- Observation: Review found two input-serialization defects before final approval.

## Follow-up

- None — no further action

## Metrics

```yaml
verification: pass
review_rounds: 2
defects_found: 2
rework_count: 2
rework_reason: input serialization and parser delimiters
```
````

`verification` 只能是 `pass`、`fail`、`not-run` 或 `unknown`。數值 metrics 只能是非負整數或 `unknown`。Completion payload 不得包含其他二級 headings。

Completion payload 的 `Verification` 與 `Artifacts` 必須各出現一次；沒有驗證或產物時使用 `None.`。`Outcome` 與 `Observation` 必須各出現一次且不得空白。`Follow-up` 必須包含至少一個 next action、中央 task reference，或精確值 `None — no further action`；但 `partial` status 僅接受前兩者。Payload 總大小上限為 64 KiB。

Report aggregation 規則：

- 成功率為 `success / (success + partial + failed + rolled-back)`；`draft`、`superseded` 與 invalidated records 不進入分子或分母。
- 平均 elapsed time、defects 與 rework 只使用數值樣本；`unknown` 另計 missing count，不得轉為零。
- 每個 project／workspace／task type／workflow variant 分組少於 5 筆 final records 時顯示 `small sample (n=<count>)`。
- Report 永遠只描述觀察資料，不跨 project 合併推論，也不輸出最佳 workflow。

`observe start` 以 `secrets.token_hex(3)` 產生 suffix，並以 atomic exclusive create 防止並行 run ID 衝突。`observe finish` 使用 `wiki/observations/.locks/<run-id>.lock` 取得 exclusive lock；`.locks/` 不納入 record discovery。取得 lock 後重新讀取 record 並確認仍為 `draft`；完整結果先寫入同目錄 temporary file，flush 並驗證後以 atomic `os.replace` 取代原檔。競爭的 finish 只能有一個成功，失敗或 crash 不得留下截斷 record。

若 session 意外中斷，record 保持 `draft`；`observe report` 將超過 24 小時的 draft 標示為 stale，但不自動改成失敗。使用者或 Agent 取得正確結果後，仍以正常 `observe finish` 明確結案。

CLI automation contract：

| 情境 | stdout | stderr | exit code |
|---|---|---|---|
| start 成功 | 僅 `<run-id>\n` | 空 | `0` |
| finish 成功 | `finished <run-id>\n` | 空 | `0` |
| invalidate 成功 | `invalidated <run-id>\n` | 空 | `0` |
| report 成功 | report text | 空 | `0` |
| usage、payload 或 schema 錯誤 | 空 | 固定前綴 `observation validation error:` | `2` |
| run 不存在或已完成 | 空 | 固定前綴 `observation state error:` | `3` |
| I/O、lock 或權限錯誤 | 空 | 固定前綴 `observation io error:` | `4` |

Skill 只從 start 的 stdout 解析 run ID，不從人類可讀訊息或 stderr 猜測。

## 全域 Observe Skill

建立全域個人 skill `observing-workflows`，安裝於 `~/.codex/skills/observing-workflows/`。Skill 依下列順序判斷，排除條件優先：

1. 若是純聊天、唯讀查詢、只回答問題、只產生計畫而不實作，或不需測試／lint 的簡單單檔小修，停止且不建立 observation。
2. 其餘工作符合下列任一條件時建立 observation：

- 涉及多個檔案。
- 需要執行測試或 lint。
- 包含兩個以上實作步驟。

若無法判斷是否符合，預設不觸發並在不修改任何狀態下繼續原任務；不得以主觀的「可能返工」作為單獨觸發理由。

所有符合門檻的 observation 都集中寫入此 LLM Wiki。Skill 必須以明確的 Python 執行器、script path 與 wiki root 呼叫：

```text
python3 "${LLMWIKI_ROOT}/wiki_cli.py" observe --wiki-root "${LLMWIKI_ROOT}" start --title "Implement validated observation reporting" --subject-root "/path/to/subject-workspace" --agent-surface codex --start-mode planned --task-type feature --workflow-variant implementation-with-review --scope-from-file /tmp/observation-scope-a1b2c3.md
```

`--wiki-root` 只影響 `observe` 子命令；既有 CLI 命令仍保留其 current-working-directory 行為。Observation helper 接受明確 root，所有中央路徑都由該 root 解析，不依賴呼叫者的 current working directory。

Skill 的第一次實際 start attempt 必須使用上例的 required flag set；`--project` 只在使用者明確提供穩定 label 時加入，`--task` 與 `--source` 只在對應 canonical central Wiki task/raw source 確實存在時加入。Help 必須以不含 payload flags 的 standalone command 執行，不得用帶 `--scope-from-file`／`--from-file` 的命令探測語法。

為了讓 forward evaluation 不污染中央 store，skill 僅在三個環境變數**同時**存在且 `OBSERVATION_EVAL=1` 時，允許以 `OBSERVATION_CLI_PATH` 與 `OBSERVATION_WIKI_ROOT` 取代上述 command 的 script/root。這是受控 eval harness 專用行為；正常任務、缺少任一變數，或未設 `OBSERVATION_EVAL=1` 時，必須使用固定中央 command。Eval harness 必須指向 temporary wiki root，且不得把此 override 視為一般部署設定。

跨 workspace 寫入中央 Wiki 是部署前置條件：使用者需批准上述精確 CLI prefix 對中央路徑的寫入權限。若 sandbox 或權限拒絕，第一版不建立 local fallback queue；原任務繼續，但最終回覆必須揭露 start 或 finish 未保存成功。成功標準中的跨 repository 記錄，只適用於已授予此權限的環境。

Scope 與 completion input payload 可位於系統 temporary directory；必須使用安全 temporary-file API 建立，檔名每次唯一、mode `0600`，不得使用共享的固定檔名。CLI 呼叫完成後，無論成功或失敗都在 cleanup/finally 路徑刪除。Atomic record output temporary files 則必須位於 `wiki/observations/` 內。任何 temporary file 都不得包含本 spec 禁止保存的敏感資訊。

Observation unit 第一版固定為 top-level user-authorized task。Parent agent 已建立 observation 時，所有 subagent handoff 必須包含：`Observation managed by parent run <run-id>; do not start a child observation.` Subagent 不建立自己的 run；其 commands、defects、review 與 rework 由 parent 彙入 completion payload，避免重複計數。

Skill 的執行順序：

1. 在第一次修改前判斷 `task_type` 與 `workflow_variant`，建立唯一的去敏感化 Scope payload，把目前 subject workspace root 傳給 CLI，執行 `observe start` 並保存 run ID。Skill 不自行計算 project、workspace ID、revision 或 working-tree state。
2. 執行原任務，不讓 observation 改變原任務的授權範圍。
3. 以去敏感化的 completion payload 收集 Execution evidence、Outcome and observation、Follow-up 與 Metrics。
4. 在正常完成、部分完成、失敗或回滾時，建立唯一 completion payload，執行 `observe finish <run-id> --status <final-status> --from-file /tmp/<run-id>-completion-<nonce>.md`；final status 分別使用 `success`、`partial`、`failed` 或 `rolled-back`。
5. 寫入實際 verification、review rounds、defects、rework count 與 reason；不可取得的值保持 `unknown`。
6. Observation start 或 finish 失敗時，不阻斷原任務，但必須在最終回覆中揭露記錄失敗；不得聲稱已成功保存。

若原先未觸發的任務在執行中首次越過門檻，立即以 `start_mode: late` 建立 observation；timestamp 與 elapsed time 從 late start 起算，不回填先前時間。若使用者以 material scope change 取代目前 Scope，先成功建立新 run，再以 `superseded --superseded-by <new-run-id>` 結束舊 run；若新 run 建立失敗，舊 run 保持 active，不得留下無目標的 superseded record。

Skill 不包含另一份 schema 或 CLI 實作；它只負責觸發判斷與呼叫中央 `wiki_cli.py`，避免規則分叉。

Skill description 提供的是 best-effort 自動發現，不構成硬性 runtime hook。第一版以 forward tests 衡量觸發品質；若未達成功標準，再另案評估全域 instruction 或 hook，不在本版暗示 skill 能保證 100% runtime invocation。

Canonical skill source 位於 `skills/observing-workflows/`；全域安裝目標為 `~/.codex/skills/observing-workflows/`，安裝後以 recursive diff 驗證兩者一致。版本化 eval manifest 位於 `tests/skill_evals/observing_workflows_cases.json`，eval runner 位於 `tests/run_observing_workflows_eval.py`。

Skill decision validation 使用固定的 20-case manifest：10 個應觸發案例與 10 個排除案例。每筆固定記錄有順序的 synthetic turns、預期 trigger checkpoints、task type 與 workflow variant；baseline 必須在 canonical skill 建立或安裝前完成，forward test 使用同一 manifest。不得在看到 agent 結果後修改 expected values。Decision manifest 必須包含 parent/subagent、late trigger 與 material scope change 案例。

Lifecycle validation 使用另一份在 skill 建立前凍結的 integration manifest，不改動上述 20-case decision manifest。它至少覆蓋：完整 planned start/finish、late start、material scope supersession、parent/subagent 去重、任務失敗仍以 `failed` finish、中央 CLI 無法執行時不阻斷原任務且揭露失敗，以及只有 `OBSERVATION_EVAL=1` 加上兩個完整 override 變數時才使用 temporary store。Forward evaluator 必須檢查 temporary wiki root 中的 record count、draft count、final statuses 與 failure disclosure，而不是只記錄 agent 的自述 decision。Incomplete-override negative case 只能驗證 command selection，不得實際執行會回退到 production central command 的寫入。

所有會執行 synthetic task 的 forward/lifecycle cases 必須在每案獨立、由 deterministic fixture builder 建立的 temporary project workspace 中運行；fixture 提供完成該 prompt 所需的最小檔案、測試與 Git metadata。Evaluator 的 CWD、task artifacts 與 observation wiki root 都不得指向 production repository。Baseline decision-only cases 不執行 task mutation。Eval harness 只負責 fixture/env preparation、checkpoint inspection、store inspection 與 cleanup，不直接啟動 Codex agents；當前 agent/subagent orchestration 依 manifest dispatch initial turn 與 follow-up turns。Harness 必須在 cleanup 中刪除 fixture workspace，並驗證 production repository 在 eval 前後沒有新增 task artifacts 或 observation records。每案結果先留在 system temporary directory；整個 suite 的 production snapshot 驗證通過後，才把宣告過的 result JSON atomic 寫回 repository。

Forward/lifecycle executable cases 另使用每案獨立、由 `OBSERVATION_PAYLOAD_TMPDIR` 指定的 payload directory 與 temporary CLI audit wrapper。該目錄只供 Scope/completion 的 secure temporary-file API 明確使用，不得取代 evaluator process-wide `TMPDIR`，避免 Codex runtime、sandbox 或工具鏈的非 payload 暫存污染稽核。Wrapper 只在非 help 呼叫真正可能消費 `--scope-from-file` 或 `--from-file` 時，側錄 temporary path、device/inode、regular-file 狀態與 mode；包含 `-h` 或 `--help` 的呼叫直接交給真實 CLI，不計 payload call。Wrapper 不讀取或保存 payload 內容，並以原 stdout/stderr/exit-code 語義交給真實 v2 CLI。Harness 在每案結束後必須驗證：Scope 與 completion paths 沒有重用，每個被呼叫的 payload 都是 regular file 且 mode 精確為 `0600`，所有被側錄的 paths 已不存在。Central-CLI-unavailable case 不執行 wrapper，但必須在失敗後證明其獨立 payload directory 為空。Audit log 只存在 evaluation temporary root，在 `finally` 中刪除，不寫入 result JSON 或 production repository，也不修改 frozen manifests/result schemas。

Decision 與 lifecycle manifests 使用有順序的 `turns`，而不是單一 prompt。每個 turn 指定 prompt 與 `dispatch_when`；單 turn 案例使用 `immediate`。Multi-turn fixture 提供最長 15 秒的 synchronization gate：late-trigger 在 initial single-file mutation 且仍無 observation 後進 gate，scope-supersession 在舊 run 為 draft 且尚未做第一個 code mutation 時進 gate。Harness 偵測 predicate 後，orchestrator 先把 follow-up 發給相同且仍 running 的 evaluator，再 release gate。Timeout、evaluator 提前結束或 agent identity 改變都使該 case 明確失敗，不得以 retry 覆蓋。

Expected checkpoints 不保存隨機 run IDs。Harness 在每案第一次看見 record 時依觀察順序配置 `run-1`、`run-2` 等 role，並在後續 checkpoints 保持同一 mapping；同一 inspection 同時首次出現多筆時以 `(timestamp, run_id)` 作 deterministic tie-break。Manifest 比較 normalized records 的 role、status、start mode 與 `superseded_by_role`。Scope supersession 因舊 draft 已在前一 checkpoint 出現，必須正規化為 `run-1` superseded by `run-2`。

Eval runner 必須使用明確的 `baseline`、`forward` 與 `lifecycle` modes。只有 baseline mode 可省略 recording fields；forward mode 必須要求每筆 result 都有 run count、draft count 與 final statuses。每個 mode 都要求 manifest/result ID sets 完全相同，並拒絕 missing、extra 或 duplicate IDs。

## 隱私與輸入安全

- 不保存完整 user prompt、conversation transcript、secret、token、credential、個資或未遮蔽的敏感絕對路徑。
- Title、project、workspace、evidence、outcome、observation、rework reason 與 artifact labels 必須去敏感化；只保存理解 workflow 成效所需的最小資訊。
- 外部 repository 的檔案可在 Execution evidence 中以去敏感化相對路徑或 artifact label 描述，不可放進 `sources`。
- CLI 對所有 frontmatter 與 payload 值使用安全 serialization，拒絕控制字元、frontmatter delimiter、路徑 traversal 與會造成 parser ambiguity 的輸入；驗證必須在建立或覆寫 record 前完成。
- `workspace_id` 必須是 12 個 lowercase hex；`revision` 必須是 7–40 個 hex 或 `unknown`；人類文字 frontmatter scalar 必須是單行且不超過 200 個 Unicode code points。
- `--wiki-root` 必須解析為既有 directory，且 `wiki/observations/`、`.locks/`、`invalidations/` 與 atomic record output 經 canonicalization 後都必須仍是該 root 的 descendants；拒絕 symlink 或 traversal escape。`--subject-root` 必須是 existing directory，可位於中央 Wiki 外部，只用來讀取 provenance 且不得持久化其絕對路徑。系統 temporary directory 中的 input payload 不受 descendant 規則限制，但仍須符合 mode、大小與 cleanup 規則。

## 驗證與錯誤處理

`wiki_cli.py lint` 增加 observation 檢查：

- frontmatter 必填欄位、受控 enum 值與時間格式。
- 完成時間不可早於開始時間，elapsed time 必須可重算。
- `task_ref` 的 wikilink 若存在，必須解析至有效 task record。
- 已完成 record 不能再透過 finish 轉態。
- Metrics 的 `unknown` 視為缺值；report 不得把它歸零。
- `project`、`workspace`、`workspace_id`、`revision`、`working_tree`、`agent_surface`、taxonomy 組合、status／verification 一致性、Scope payload 與 completion payload。
- `start_mode`、`superseded_by` 與 invalidation tombstone schema。
- Observation records 與 tombstones 不觸發 concept orphan、零 outbound-link 或 overview drift 診斷。

自動化測試覆蓋：Scope validation、required/empty sources、scalar/payload size limits、subject-root provenance derivation/non-persistence、planned/late start、finish completion payload、partial follow-up requirement、無效 taxonomy/status、status／verification 衝突、superseded transition、draft-invalidation rejection、invalidation tombstone、時間順序錯誤、重複與並行 finish、失效 task link、input injection、wiki-root symlink/traversal escape、同秒並行 start、start temporary-write/fsync/link failure 與 collision retry、finish atomic-write failure、lock release、temporary payload permissions/cleanup、stale draft，以及 report filters/date boundaries、unknown、小樣本與 operational-record exclusions。

Skill 以 documentation TDD 驗證：先用未載入 skill 的 baseline agents 執行版本化 20-case decision manifest，確認其會漏記或誤判；再用載入 skill 的 agents 重跑同一 decision manifest。獨立 lifecycle integration manifest 驗證實際 records 與 failure disclosure，涵蓋中央 CLI 無法執行、任務失敗後仍需 finish、parent/subagent 去重、late trigger、material scope change 與 eval-only override。

CLI 測試另需涵蓋 stdout／stderr／exit-code contract（含 argparse invalid argument 不得輸出 usage）、新增 task types、合法與非法 taxonomy 組合、subject-root 不持久化，以及從 LLM Wiki 以外的 current working directory 透過絕對路徑、`--wiki-root` 與外部 `--subject-root` 呼叫時，仍將 record 寫入中央 `wiki/observations/`。測試也要確認既有非-observe 子命令的 current-working-directory 行為未改變。

## 成功標準

第一版完成後，使用者能各以一個 CLI 指令開始與結束含 durable completion content 的工作 run，能以 tombstone 讓錯誤 record 退出 aggregates，lint 能拒絕不一致的 record，report 能在不虛構缺失值的前提下列出可比較的基本趨勢、invalidated count 與 stale drafts。已授權中央路徑的環境中，全域 skill 的 decision forward test 必須讓 10/10 個應觸發案例實際建立並結束 observation，且 10/10 個排除案例不建立 observation；獨立 lifecycle integration tests 必須證明 parent/subagent 不重複計數、late trigger、material scope change、failed finish、CLI failure disclosure 與 eval-only override 都符合契約。這是測試目標，不宣稱 skill matching 是 runtime hard guarantee。累積足夠且一致的樣本後，才另行設計 decision-pattern 或 workflow recommendation 機制。
