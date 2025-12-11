# Cision Data Governance Demo: The "Omni-Channel Crisis" Scenario

This demo showcases how **Spark Expectations** provides stability, lineage, and "Shift-Left" quality gates during a high-velocity data crisis.

**Scenario**: A viral rumor spreads about "BrandX Phones" exploding.
*   **Social Data**: Flooded with negative sentiment and keyword "explode".
*   **Inventory**: Showing discrepancies (potential theft/shrinkage).
*   **Transactions**: Some sales data is malformed (`TXN_LOST`).

Your Goal: Show how the Governance Framework catches these issues *before* they break the Executive Dashboard, and how you can trace missing data instantly.

---

## 🎭 Demo Script / Walkthrough

### 1. The "Bird's Eye" View (Tab 1: Lineage)
*   **Narrative**: "Let's look at the complexity of our modern pipeline. We aren't just moving tables; we are ingesting Social Streams (Kafka), Web Clickstream (S3), and ERP Data (Oracle)."
*   **Visual**: Show the graph. Point to the **Red "Rule" Nodes**.
*   **Key Point**: "Notice these Quality Gates. Unlike traditional pipelines that crash when bad data hits, these Gates actively filter and quarantine toxic data."

### 2. The "Crisis" Dashboard (Tab 2: Business Rules)
*   **Narrative**: "The system is flashing Red. Why? Let's look at the Business Rules."
*   **Action**: Scroll to the failures.
*   **Highlight 1**: `exp_prohibited_content` (CRITICAL).
    *   *Say*: "We detected safety keywords like 'explode' in the raw social stream. This triggered an immediate alert."
*   **Highlight 2**: `exp_inventory_balance` (CRITICAL).
    *   *Say*: "Our Cross-System Integrity check failed. Sales + Remaining Inventory does not equal Starting Inventory. We have leakage."
*   **Highlight 3**: `exp_pos_integrity` (HIGH).
    *   *Say*: "We also have negative sales amounts trying to enter the ledger."

### 3. The "Investigator" (Tab 5: Data Investigator)
*   **Narrative**: "A Finance Analyst calls you. 'I can't find transaction `TXN_LOST` for $500. Did we lose it?'"
*   **Action**: Go to Tab 5. Enter `TXN_LOST` and click **Trace Record**.
*   **Visual**: Expand the headers.
    *   ✅ **Source**: "Success - Record Ingested"
    *   ❌ **Quality Gate**: "FAILED - Rule Violation: Amount (-500) <= 0"
    *   ⚠️ **Target**: "Quarantined - Saved to Error Table"
*   **Closing**: "We didn't lose the money. The system protected the ledger from a negative transaction. It's safely in the quarantine table for review."

### 4. The "Shift Left" (Tab 4: Process)
*   **Narrative**: "How do we prevent this next time? Business users can define rules themselves."
*   **Action**: Show the Rule Editor. Type "Sentiment must be > -0.5".
*   **Key Point**: "This generates the code automatically. No 2-week engineering sprint required."

---

## 🛠️ How to Run
1.  **Install**: `pip install streamlit pandas graphviz`
2.  **Run**: `streamlit run app.py`
