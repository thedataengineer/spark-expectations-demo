import streamlit as st
import pandas as pd
import graphviz
import json
from mock_data import get_mock_data
from engine import GovernanceEngine

st.set_page_config(page_title="Cision Data Governance Demo", layout="wide")

# -- Header --
st.title("🛡️ Cision Data Governance: Spark Expectations Demo")
st.markdown("### From Hidden Code to Visible Business Rules")
st.info("This demo illustrates how 'Spark Expectations' provides lineage, transparency, and business-readable quality gates for Cision's data pipelines.")

# -- Load Data & Run Engine --
@st.cache_data
def load_and_run():
    data = get_mock_data()
    
    with open("rules_config.json", "r") as f:
        rules = json.load(f)
        
    engine = GovernanceEngine(data, rules)
    results_df = engine.run_validation()
    lineage_graph = engine.get_lineage_data()
    
    return data, results_df, lineage_graph, rules

data_dict, results_df, lineage_graph, rules_config = load_and_run()

# -- Tabs --
tab1, tab2, tab3, tab4, tab5 = st.tabs(["🧩 Data Lineage", "📋 Business Rules & Status", "🔎 Inspect Data", "✏️ Rule Editor (Mock)", "🕵️ Data Investigator"])

# === TAB 1: LINEAGE ===
with tab1:
    st.header("Pipeline Lineage & Quality Gates")
    st.caption("Visualizing how data flows and where rules are applied. Note the explicit Quality Gate nodes.")
    
    col_graph, col_details = st.columns([2, 1])
    
    with col_graph:
        graph = graphviz.Digraph()
        graph.attr(rankdir='LR')
        
        for node in lineage_graph["nodes"]:
            graph.node(node["id"], node["label"], shape=node["shape"], style="filled", fillcolor=node["color"])
            
        for edge in lineage_graph["edges"]:
            graph.edge(edge[0], edge[1])
            
        st.graphviz_chart(graph)
        
    with col_details:
        st.subheader("Traceability")
        st.markdown("""
        **Data Source**: `Customers` + `Orders`
        **Transformation**: `LTV Aggregation`
        **Target**: `Gold_Customer_Analytics`
        
        **Impact Analysis**:
        If `Orders` fail critical checks, the `Gold` table is **not updated**, preventing bad LTV numbers from reaching the C-Suite dashboard.
        """)
        st.warning("Detected: 5 Critical Failures in Upstream Orders")

# === TAB 2: RULES ===
with tab2:
    st.header("Business Rule Validation")
    st.caption("No more reading Python logs. Rules are translated to plain English and tracked.")
    
    # Styled Dataframe
    def highlight_status(val):
        color = 'red' if val == 'FAIL' else 'green'
        return f'color: {color}; font-weight: bold'

    st.dataframe(
        results_df[["rule_id", "severity", "table", "column", "description", "status", "failed_count"]].style.applymap(highlight_status, subset=['status']),
        use_container_width=True
    )
    
    # Drill Down
    st.divider()
    st.subheader("Failure Analysis")
    failed_rules = results_df[results_df["status"] == "FAIL"]
    
    if not failed_rules.empty:
        rule_select = st.selectbox("Select a Failed Rule to Inspect:", failed_rules["description"].unique())
        
        # Get details
        record = failed_rules[failed_rules["description"] == rule_select].iloc[0]
        table_name = record["table"]
        failed_indices = record["failed_rows"]
        
        st.write(f"**Table**: `{table_name}` | **Rule ID**: `{record['rule_id']}`")
        if isinstance(failed_indices, list) and len(failed_indices) > 0 and isinstance(failed_indices[0], int):
            # Display the actual bad rows
            st.error(f"Showing {len(failed_indices)} failed rows:")
            bad_data = data_dict[table_name].iloc[failed_indices]
            st.dataframe(bad_data)
        else:
            st.write("Aggregated Failure. See details above.")
            
    else:
        st.success("All Systems Operational!")

# === TAB 3: DATA INSPECT ===
with tab3:
    st.header("Raw Data Inspection")
    dataset = st.selectbox("Choose Dataset:", list(data_dict.keys()))
    st.dataframe(data_dict[dataset], use_container_width=True)

# === TAB 4: EDITOR ===
with tab4:
    st.header("Governance Control Plane")
    st.markdown("Business Stewards can add rules here without needing Engineering tickets.")
    
    c1, c2 = st.columns(2)
    with c1:
        new_desc = st.text_input("Business Rule (Plain English)", "Customer Age must be under 100")
        target_table = st.selectbox("Target Table", ["customers", "orders"])
        target_col = st.selectbox("Column", ["age", "amount", "email"])
        
    with c2:
        st.markdown("**Generated Spark Expectation (Preview)**")
        st.code(f"""
{{
  "id": "auto_gen_{target_col}_limit",
  "table": "{target_table}",
  "column": "{target_col}",
  "expectation_type": "expect_column_values_to_be_less_than",
  "kwargs": {{ "value": 100 }},
  "description": "{new_desc}"
}}
        """, language="json")
        
    if st.button("Deploy Rule to Production"):
        st.toast("Rule deployed to Rules Engine! (Simulation)", icon="🚀")

# === TAB 5: INVESTIGATOR ===
with tab5:
    st.header("Traceability Investigator")
    st.markdown("Use this tool to find 'Missing Data' caused by quality failures (GDPR / Audit Compliance).")
    
    st.info("Scenario: Finance team cannot find Transaction `TXN_LOST` in the dashboard.")
    
    search_id = st.text_input("Enter Transaction ID", "TXN_LOST")
    
    if st.button("Trace Record"):
        # Re-instantiate engine to use the trace method
        # (In prod, this would be an API call)
        eng = GovernanceEngine(data_dict, rules_config)
        trace = eng.get_record_trace(search_id)
        
        st.subheader(f"Lineage Path for: `{search_id}`")
        st.write(f"Final Status: **{trace['final_status']}**")
        
        for step in trace["steps"]:
            status_icon = "✅" if step["status"] == "Success" else "❌" if step["status"] == "FAILED" else "⚠️"
            with st.expander(f"{status_icon} {step['stage']}", expanded=True):
                st.write(f"**Status**: {step['status']}")
                st.write(f"**Details**: {step['msg']}")

# -- Footer --
st.divider()
st.markdown("*Demo for Cision | Built with simulated Spark Expectations & Streamlit*")
