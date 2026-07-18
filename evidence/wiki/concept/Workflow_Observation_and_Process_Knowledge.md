---
type: concept
title: "Workflow Observation and Process Knowledge"
tags: [ai-team, decision-patterns, llmwiki, multi-agent, observation, opentelemetry, privacy, process-knowledge, telemetry, workflow]
timestamp: "2026-07-16"
sources:
  - "raw/LLM_Compiler_and_Workflow_Knowledge_Base.md"
  - "raw/Codex_Subagent啟動成本與模型路由實測.md"
  - "raw/Workflow_Improvement_Market_Landscape_and_Palantir_Fit_2026-07-15.md"
  - "raw/Workflow_Observability_Products_Purpose_and_Direct_Installation_Risks_2026-07-15.md"
  - "raw/Workflow_Telemetry_Best_Practices_Evidence_Ledger_2026-07-15.md"
  - "raw/Workflow_Telemetry_Best_Practices_Research_Report_2026-07-15.md"
---

# Workflow Observation and Process Knowledge

LLM 知識系統不只可以累積「知道什麼」，也可以累積「用什麼流程解題比較有效」。前者是內容知識，後者是 process knowledge（流程知識）與 decision patterns（決策模式）。

這個概念延伸 [[AI_Architecture_Cognition_and_Knowledge_Workflow]] 的 LLM compiler 模型：LLM 先把 raw evidence 編譯成穩定的知識中介表示，再由 workflow layer 選擇適合特定 Agent 角色的知識與步驟，而不是每次把整個 Wiki 交給單一 Agent 重新理解。

## 三層分工

```text
Knowledge layer
    ↓ 提供可驗證的領域知識與規則
Workflow layer
    ↓ 決定步驟、檢查點與上下文路由
Agent layer
    ↓ Planner / Coder / Tester / Reviewer 等角色執行
```

- **Knowledge**：API、架構、ADR、coding style、研究與最佳實務。
- **Workflow**：feature、bug、release、research 等任務的步驟與關卡。
- **Agent role**：負責規劃、規格、實作、測試、審查或決策的執行角色。

這個分層的設計假設是：workflow 能限制每個角色取得的 context，使知識路由比「整庫放入 context」更精準且成本更低。這仍是待量測的假設，不是目前已有的效能結論。

## 在編譯循環加入 Observe

既有循環是 Ingest → Compile → Query → Lint。若要累積流程知識，可在任務完成後加入 **Observe**：保存執行軌跡與結果，使工作流本身成為可比較、可改善的知識對象。

最小 observation record 可包含：

| 欄位 | 用途 |
|---|---|
| task_type | 區分 feature、bug、refactor、research 等任務 |
| workflow_variant | 記錄實際採用的角色順序與檢查點 |
| outcome | 成功、失敗、部分完成或回滾 |
| elapsed_time | 端到端耗時 |
| token_cost | 可取得時記錄模型成本 |
| review_rounds | 審查與修改輪數 |
| defects_found | 測試或審查發現的問題數與嚴重度 |
| rework | 返工次數或返工時間 |

## 從紀錄到決策模式

單次軌跡只能說明發生過什麼，不能直接證明某個 workflow 更好。要形成可重用的 decision pattern，至少需要：

1. 按 task type 與複雜度分組，避免把不同難度的任務直接比較。
2. 明確定義 outcome 與 defect，避免用主觀「感覺成功」當標籤。
3. 對 workflow variant 保留樣本數與失敗案例。
4. 比較品質、成本與時間，而不是只看單一成功率。
5. 將觀察、推論與推薦分開保存。

例如「加入 Spec Builder 後成功率由 65% 變成 93%」只能作為待驗證示例；沒有樣本數、任務難度與評分規則時，不應寫成已證實的組織經驗。

## Observation 的目的決定資料模型

市場與 telemetry 研究顯示，workflow-learning summary、runtime diagnostics、recorder health 與 evaluation score 是不同資料目的，不應強迫共用同一筆可變 record：

- 終態 workflow summary 是可比較的 canonical outcome，完成後不可變；更正使用 invalidation 或 supersession。
- Recorder health 描述觀察系統自身，不得把記錄失敗誤寫成授權工作的失敗。
- Evaluation 是事後、具評分者與方法來源的 judgment，不等同 capture-time tags。
- Child operations、sessions 與外部 OTel export 只有在具體 consumer 出現時才擴充；外部表示是 derived view，不是本地 source of truth。

內容擷取預設關閉，聚合欄位維持低 cardinality；prompt、tool payload、絕對路徑、錯誤全文、credentials 與個資不應進入 retained telemetry。當前低流量資料不抽樣，因為只保留成功案例或熱門路徑會直接扭曲流程學習。完整市場比較、安裝風險與 schema 決策見 [[Workflow_Observability_Market_and_Telemetry_Research]]。

## Orchestration 成本也是 workflow outcome

[[Codex_Subagent_Cost_Probe]] 顯示，單次最小 subagent probe 仍可能承擔約 20k tokens 的固定 context，而多輪研究成本主要由反覆帶入 context 構成。這不證明 subagent 不值得使用；它要求 workflow variant 同時衡量獨立審查價值、wall time、主線 context 節省與完整 token 結構，不能只看最終輸出字數。

## 優先驗證的窄假設

可以先檢查一個邊界清楚的假設：

> 對跨三個以上模組的程式修改，在實作前加入獨立 specification review，是否能降低首次完整測試後的返工輪數？

主要失敗模式是 observation 本身增加管理成本，或不同任務的標記方式不一致，最後得到看似精確但不可比較的統計。初期應先使用少量欄位與固定 task taxonomy，再決定是否擴充。

## 與個人化模型的關係

Wiki 內容可以用於檢索、合成資料或未來微調，但更值得保留的是「在何種條件下採用哪種流程」的決策證據。個人化不只代表讓模型背熟內容，也可以代表讓系統逐步學會：

- 哪類任務需要先規格化。
- 哪種 reviewer checklist 對特定缺陷最有效。
- 哪種 agent 組合適合某種風險與規模。
- 何時增加一道關卡的收益高於額外成本。

這也提供一個長期的人因指標：隨著 [[Knowledge_Base_Rebuild_Plan|知識庫、workflow 與 Agent playbook]] 成熟，管理 AI 團隊所需的認知負荷是否下降。這項效果必須透過持續 observation 才能判斷。
