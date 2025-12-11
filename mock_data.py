import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def get_mock_data():
    """
    Generates an 'Extremely Complex' mock dataset for the Cision Scenario:
    'The Omni-Channel Crisis: Correlation of Media Sentiment vs. Supply Chain Integrity'
    """
    
    # SYSTEM A: SOCIAL LISTENING (Unstructured / Noisy)
    # Scenario: A viral rumor is affecting product perception.
    social_data = {
        "tweet_id": [f"tw_{i}" for i in range(1001, 1011)],
        "timestamp": [datetime(2023, 11, 24, 10, i) for i in range(10)], # 10 minute window
        "content": [
            "Love the new phone!", "It's okay.", "Mine exploded!", "Mine exploded too!!", "Fake news?",
            "Returning it.", "Best gift ever.", "Scary stuff.", "Refund please.", "Cision detected this."
        ],
        "sentiment_score": [0.9, 0.1, -0.9, -0.95, -0.2, -0.8, 0.95, -0.7, -0.9, 0.0],
        "brand_mention": ["BrandX", "BrandX", "BrandX", "BrandX", "BrandX", "BrandX", "BrandX", "BrandX", "BrandX", "BrandX"]
    }
    social_df = pd.DataFrame(social_data)

    # SYSTEM B: WEB CLICKSTREAM (High Volume / Sessionized)
    # Scenario: High traffic but Cart Abandonment is spiking correlated to negative tweets.
    web_data = {
        "session_id": [f"sess_{i}" for i in range(5001, 5011)],
        "user_id": [f"usr_{i}" for i in range(1, 11)],
        "timestamp": [datetime(2023, 11, 24, 10, i+2) for i in range(10)], # Slight lag from social
        "event_type": [
            "checkout", "view_item", "cart_abandon", "cart_abandon", "view_item",
            "return_page", "checkout", "cart_abandon", "return_page", "view_item"
        ],
        "sku_interaction": ["SKU_999", "SKU_999", "SKU_999", "SKU_999", "SKU_999", "SKU_999", "SKU_999", "SKU_999", "SKU_999", "SKU_999"]
    }
    web_df = pd.DataFrame(web_data)

    # SYSTEM C: POINT OF SALE (Structured / Financial)
    # Scenario: Actual verified sales.
    pos_data = {
        "txn_id": ["TXN_100", "TXN_101", "TXN_LOST"], # TXN_LOST has a negative amount!
        "session_id": ["sess_5001", "sess_5007", "sess_5009"],
        "sku": ["SKU_999", "SKU_999", "SKU_999"],
        "amount": [999.99, 999.99, -500.00],
        "store_id": ["online_01", "online_01", "online_01"]
    }
    pos_df = pd.DataFrame(pos_data)

    # SYSTEM D: SUPPLY CHAIN / INVENTORY (ERP System)
    # Scenario: Inventory is leaking. Warehouse shows 5 items left, but system says we sold 2. 
    # Start: 100. End should be 98. 
    # ERROR INJECTED: End count is 95. (3 items 'Disappeared' or Stolen)
    inventory_data = {
        "sku": ["SKU_999", "SKU_888"],
        "warehouse_loc": ["WH_NJ", "WH_NJ"],
        "start_count_daily": [100, 50],
        "end_count_realtime": [95, 50], # SKU_999 should be 98 (100 - 2 sold). It is 95.
        "last_audit": ["2023-11-23", "2023-11-23"]
    }
    inventory_df = pd.DataFrame(inventory_data)

    # COMPLEX TRANSFORMATION ARTIFACT: "The Brand Health Dashboard"
    # Logic: 
    # 1. Join Social (avg sentiment) to Web (abandonment rate) per 10min window.
    # 2. Join Sales to Inventory to calc 'Leakage'.
    
    # Simulating the dashboard DataFrame directly
    brand_health_data = {
        "window_time": ["10:00-10:10"],
        "avg_sentiment": [-0.34], # (Sum of social scores / 10)
        "cart_abandon_rate": [0.4], # 4 abandons / 10 sessions
        "verified_revenue": [1999.98],
        "inventory_shrinkage_qty": [3] # (Start - Sales) - End. (100-2) - 95 = 3
    }
    dashboard_df = pd.DataFrame(brand_health_data)

    return {
        "social_stream": social_df,
        "web_clickstream": web_df,
        "pos_transactions": pos_df,
        "erp_inventory": inventory_df,
        "gold_brand_health": dashboard_df
    }
