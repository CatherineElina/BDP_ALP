import streamlit as st
import json
import os
import time
import pandas as pd

SNAPSHOT_PATH = "/app/dashboard_data/latest_snapshot.json"
HISTORY_PATH = "/app/dashboard_data/history.jsonl"

st.set_page_config(page_title="Stock Market Live Dashboard", layout="wide")
st.title("📈 US Stock Market — Live Streaming Dashboard")

placeholder = st.empty()

while True:
    with placeholder.container():
        if not os.path.exists(SNAPSHOT_PATH):
            st.warning("Waiting for Spark streaming job to produce data...")
        else:
            with open(SNAPSHOT_PATH) as f:
                data = json.load(f)

            df = pd.DataFrame(data)

            st.subheader("🔴 Live: Avg Closing Price by Sector (last 30s window)")
            if "sector" in df.columns and "avg_close" in df.columns:
                chart_df = df[["sector", "avg_close"]].sort_values("avg_close", ascending=False)
                st.bar_chart(chart_df.set_index("sector"))
                st.dataframe(df[["sector", "avg_close", "event_count"]], use_container_width=True)

            # History line chart
            if os.path.exists(HISTORY_PATH):
                history_rows = []
                with open(HISTORY_PATH) as f:
                    for line in f:
                        history_rows.append(json.loads(line))
                hist_df = pd.DataFrame(history_rows)
                if "sector" in hist_df.columns:
                    st.subheader("📊 Historical Trend: Avg Close per Sector")
                    pivot = hist_df.pivot_table(index=hist_df.index, columns="sector", values="avg_close")
                    st.line_chart(pivot)

        st.caption(f"Last refreshed: {pd.Timestamp.now().strftime('%H:%M:%S')}")
    time.sleep(5)