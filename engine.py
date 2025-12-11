import pandas as pd
import re
import json

class GovernanceEngine:
    def __init__(self, data_dict, rules_config):
        self.data_dict = data_dict
        self.rules_config = rules_config
        self.results = []

    # --- Standard Checks ---
    def _check_not_null(self, df, column):
        if column not in df.columns: return []
        return df[df[column].isnull()].index.tolist()

    def _check_regex_match(self, df, column, pattern):
        series = df[column].astype(str)
        return series[~series.str.match(pattern)].index.tolist()
        
    def _check_regex_not_match(self, df, column, pattern):
        """Fails if the pattern IS found (e.g. prohibited words)"""
        series = df[column].astype(str)
        return series[series.str.contains(pattern, regex=True)].index.tolist()

    def _check_range(self, df, column, min_val, max_val):
        return df[(df[column] < min_val) | (df[column] > max_val)].index.tolist()

    def _check_greater_than(self, df, column, value):
        return df[df[column] <= value].index.tolist()

    def _check_set(self, df, column, value_set):
        return df[~df[column].isin(value_set)].index.tolist()

    # --- COMPLEX SCENARIO CHECKS ---
    
    def _check_inventory_calculation(self, df):
        """
        Scenario: Cross-System Integrity Check.
        Logic: Load POS data, Aggregate Sales by SKU, Compare with Inventory Table.
        Equation: End_Count == Start_Count - Sold_Qty
        """
        # 1. Get POS Sales Count per SKU
        pos_df = self.data_dict.get("pos_transactions")
        if pos_df is None: return []
        
        sales_agg = pos_df.groupby("sku").size().reset_index(name="sold_qty")
        
        # 2. Merge with Inventory DF (df passed in is erp_inventory)
        merged = pd.merge(df, sales_agg, on="sku", how="left").fillna(0)
        
        # 3. Calculate Expected End Count
        merged["expected_end"] = merged["start_count_daily"] - merged["sold_qty"]
        
        # 4. Find Mismatch
        failures = merged[merged["end_count_realtime"] != merged["expected_end"]]
        return failures.index.tolist()

    def _check_sentiment_correlation(self, df):
        """
        Scenario: Crisis Detection.
        Logic: If Sentiment < -0.3 AND AbandonRate > 0.3, it's a 'Crisis', but legally valid data.
               However, for this 'Expectation', we might define it as a Quality Warning 
               that the correlation is 'Strong Negative'.
               
        Let's invert it for the demo: We expect that if Sentiment is GOOD (>0), Abandonment is LOW (<0.2).
        If Sentiment is BAD but Abandonment is LOW, maybe our tracking pixel is broken? (Data Quality Issue)
        
        Rule: "If Sentiment is Negative (< -0.2), Abandonment Rate SHOULD be High (> 0.2)". 
        If it's NOT, we have a data anomaly (e.g. bots are clicking happily despite bad news?).
        """
        # For simplicity in this mock, we just flag the rows where correlation seems 'broken' or 'alarming'.
        # Let's say we expect: avg_sentiment < -0.2 ==> shutdown ad spend.
        # So we fail if bad sentiment exists.
        return df[df["avg_sentiment"] < -0.2].index.tolist()

    def run_validation(self):
        self.results = []
        for rule in self.rules_config:
            table_name = rule["table"]
            df = self.data_dict.get(table_name)
            if df is None: continue
                
            rule_id = rule["id"]
            exp_type = rule["expectation_type"]
            col = rule.get("column")
            kwargs = rule.get("kwargs", {})
            failed_rows = []

            # Routing
            if exp_type == "expect_column_values_to_be_between":
                failed_rows = self._check_range(df, col, kwargs.get("min_value"), kwargs.get("max_value"))
            elif exp_type == "expect_column_values_to_not_match_regex":
                failed_rows = self._check_regex_not_match(df, col, kwargs.get("regex"))
            elif exp_type == "expect_column_values_to_be_in_set":
                failed_rows = self._check_set(df, col, kwargs.get("value_set"))
            elif exp_type == "expect_column_values_to_be_greater_than":
                failed_rows = self._check_greater_than(df, col, kwargs.get("value"))
            
            # Complex Handlers
            elif exp_type == "expect_inventory_calculation_match":
                failed_rows = self._check_inventory_calculation(df)
            elif exp_type == "expect_sentiment_abandonment_correlation":
                failed_rows = self._check_sentiment_correlation(df)

            status = "PASS" if len(failed_rows) == 0 else "FAIL"
            self.results.append({
                "rule_id": rule_id,
                "table": table_name,
                "column": col if col else "Multi-Column",
                "description": rule["business_description"],
                "status": status,
                "failed_count": len(failed_rows),
                "severity": rule["severity"],
                "failed_rows": failed_rows 
            })
            
        return pd.DataFrame(self.results)

    def get_lineage_data(self):
        """
        Complex Omni-Channel Lineage
        """
        return {
            "nodes": [
                # Source Systems
                {"id": "social", "label": "Source: Social Stream (Kafka)", "shape": "cylinder", "color": "lightblue"},
                {"id": "web", "label": "Source: Clickstream (S3)", "shape": "cylinder", "color": "lightblue"},
                {"id": "pos", "label": "Source: POS Transactions (SQL)", "shape": "cylinder", "color": "lightblue"},
                {"id": "erp", "label": "Source: ERP Inventory (Oracle)", "shape": "cylinder", "color": "lightblue"},
                
                # Silver Transformation Layers
                {"id": "nlp", "label": "Transform: Sentiment Engine (ML)", "shape": "component", "color": "lightgrey"},
                {"id": "session", "label": "Transform: Sessionization (Spark)", "shape": "component", "color": "lightgrey"},
                
                # Joins
                {"id": "join_brand", "label": "Join: Brand Health Aggregation", "shape": "rect", "color": "gold"},
                {"id": "join_inv", "label": "Join: Inventory Integrity Check", "shape": "rect", "color": "gold"},
                
                # Gold Targets
                {"id": "dash", "label": "Target: Exec Dashboard", "shape": "folder", "color": "lightgreen"},
                
                # Rules
                {"id": "r_safety", "label": "Rule: Safety Checks (Explosions?)", "shape": "note", "color": "red"},
                {"id": "r_leak", "label": "Rule: Inventory Leakage", "shape": "note", "color": "red"}
            ],
            "edges": [
                # Social Flow
                ("social", "nlp"),
                ("nlp", "join_brand"),
                ("social", "r_safety"), # Safety check on raw stream
                
                # Web Flow
                ("web", "session"),
                ("session", "join_brand"),
                
                # POS & Inventory Flow
                ("pos", "join_inv"),
                ("erp", "join_inv"),
                ("join_inv", "r_leak"), # Leakage check happens at the join
                
                # Dashboard Population
                ("join_brand", "dash"),
                ("join_inv", "dash")
            ]
        }
