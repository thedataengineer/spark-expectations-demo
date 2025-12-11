# Running the Cision Spark Expectations Demo

This demo application showcases the power of **Spark Expectations** for Data Governance, Lineage, and Business Rule Transparency.

## 📋 Prerequisites
*   Python 3.10+
*   Libraries: `streamlit`, `pandas`, `graphviz`
*   (Optional) Graphviz system library installed (if visualization fails)

## 🛠️ Installation

1.  **Install Python Dependencies**:
    ```bash
    pip install streamlit pandas graphviz
    ```

    *Note: If you are on Mac and `graphviz` gives errors, install the binary:* `brew install graphviz`

## 🚀 Launching the Demo

1.  Navigate to the demo directory:
    ```bash
    cd spark-expectations-demo
    ```

2.  Run the Streamlit App:
    ```bash
    streamlit run app.py
    ```

3.  The application will open in your default browser (usually at `http://localhost:8501`).

## 🎯 Demo Walkthrough Script

**Tab 1: Data Lineage**
*   **Narrative**: "Start by showing the 'Bird's Eye View'. We don't just run jobs; we have explicit Quality Gates checking data as it flows from Bronze (Customers/Orders) to Gold (Analytics)."
*   **Visual**: Point out the yellow "Rules" nodes.

**Tab 2: Business Rules & Status**
*   **Narrative**: "This is what the Business sees. Not error logs, but 'Translated Rules'. We can see exactly why the LTV calculation failed—because of critical failures in the upstream Orders table."
*   **Action**: Select "Orders must contain a valid Customer ID" in the dropdown to see the orphan records.

**Tab 3: Inspect Data**
*   **Narrative**: "Proof of transparency. Here is the raw data showing the issues (e.g., Row 5 in Customers has age 120)."

**Tab 4: Rule Editor**
*   **Narrative**: "The 'Shift Left' moment. A business user can propose a rule like 'Age < 100', and the system auto-generates the technical JSON config. No engineering bottleneck."
