import streamlit as st
import json
import os
import time
import pandas as pd
import datetime
import plotly.graph_objects as go
import plotly.express as px

HISTORY_PATH = "/app/dashboard_data/history.jsonl"

st.set_page_config(page_title="Institutional Market Intelligence Console", layout="wide")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 26px; font-weight: bold; }
    div.stDataFrame { border: 1px solid #2D3139; border-radius: 8px; }
    h1, h2, h3 { color: #FFFFFF; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }
    </style>
""", unsafe_allow_html=True)

# =========================
# DASHBOARD SETTINGS
# =========================

with st.sidebar:
    st.header("Dashboard Settings")

    auto_refresh = st.toggle(
        "Auto Refresh",
        value=False
    )

    refresh_interval = st.slider(
        "Refresh Interval (seconds)",
        5,
        60,
        10
    )

st.title("US Stock Market Dashboard")
st.markdown("Real-Time Ingestion Pipeline: **Kafka Broker** ➡️ **PySpark Stream Processor** ➡️ **Analyst Dashboard**")
st.markdown("---")

if not os.path.exists(HISTORY_PATH):
    st.warning("⏳ Waiting for PySpark streaming job to write data... (history.jsonl not found)")
else:
    history_rows = []
    with open(HISTORY_PATH) as f:
        for line in f:
            if line.strip():
                history_rows.append(json.loads(line))
                
    full_df = pd.DataFrame(history_rows)
    
    if not full_df.empty:
        full_df['date'] = pd.to_datetime(full_df['date'], utc=True).dt.tz_localize(None)

        st.info(
            f"""
            Records Loaded: {len(full_df):,}
            |
            Companies: {full_df['ticker'].nunique()}
            |
            Sectors: {full_df['sector'].nunique()}
            |
            Date Range:
            {full_df['date'].min().date()}
            → {full_df['date'].max().date()}
            """
        )
        
        st.header("Global Time Horizon Filter")
        date_range = st.date_input(
            "Define the analysis date range for all dashboard components:",
            value=(datetime.date(2020, 1, 1), datetime.date(2023, 12, 31)),
            min_value=datetime.date(2020, 1, 1),
            max_value=datetime.date(2026, 12, 31)
        )

        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = date_range[0]
            end_date = datetime.date(2026, 12, 31)

        filtered_df = full_df[
            (full_df['date'] >= pd.to_datetime(start_date)) & 
            (full_df['date'] <= pd.to_datetime(end_date))
        ]

        all_sectors = sorted(
            filtered_df["sector"].dropna().unique()
        )

        selected_sectors = st.multiselect(
            "Sector Filter",
            all_sectors,
            default=all_sectors
        )

        filtered_df = filtered_df[
            filtered_df["sector"].isin(selected_sectors)
        ]

        if filtered_df.empty:
            st.info(f"ℹ️ No market transaction data available for the date range {start_date} to {end_date}.")
        else:
            latest_date = filtered_df['date'].max()
            latest_df = filtered_df[filtered_df['date'] == latest_date]

            st.markdown("---")
            st.header(
                f"Latest Trading Session Available ({latest_date.strftime('%Y-%m-%d')})"
            )
            
            total_market_volume = latest_df['volume'].sum()
            avg_market_return = latest_df['daily_return'].mean() * 100
            market_volatility = latest_df['daily_return'].std() * 100
            
            green_stocks = (latest_df['daily_return'] > 0).sum()
            total_stocks = len(latest_df)
            red_stocks = total_stocks - green_stocks
            pct_green = (green_stocks / total_stocks * 100) if total_stocks > 0 else 0

            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("Total Volume Transaksi", f"{total_market_volume:,} Shares")
            kpi2.metric("Rata-Rata Return Pasar", f"{avg_market_return:+.2f}%")
            kpi3.metric("Market Breadth (% Saham Naik)", f"{pct_green:.1f}% Hijau", f"{green_stocks} Naik / {red_stocks} Turun")
            kpi4.metric("Market Risk (Volatilitas)", f"{market_volatility:.2f}%")
            
            st.markdown("---")
            st.header("Sector Performance & Capital Flow")
            
            sector_analysis = latest_df.groupby("sector").agg({
                "daily_return": "mean",
                "volume": "sum"
            }).reset_index()
            sector_analysis['daily_return_pct'] = sector_analysis['daily_return'] * 100
            sector_analysis = sector_analysis.sort_values("daily_return_pct", ascending=False)
            st.subheader("Sector Heatmap")
            fig_heat = px.treemap(
                sector_analysis,
                path=["sector"],
                values="volume",
                color="daily_return_pct",
                color_continuous_scale="RdYlGn",
                hover_data=["volume"]
            )

            st.plotly_chart(
                fig_heat,
                use_container_width=True,
                key="sector_heatmap"
            )
            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.write("**Average Return per Sector (%)**")
                st.bar_chart(sector_analysis.set_index("sector")['daily_return_pct'])
            with col_chart2:
                st.write("**Total Transaction Volume per Sector (Shares)**")
                st.bar_chart(sector_analysis.set_index("sector")['volume'])
                
            st.markdown("---")
            st.header("Market Leaders")

            leaders = latest_df[
                [
                    "company",
                    "sector",
                    "close",
                    "daily_return",
                    "volume"
                ]
            ].copy()

            leaders["daily_return"] *= 100

            leaders = leaders.sort_values(
                "volume",
                ascending=False
            ).head(10)

            st.dataframe(
                leaders.rename(
                    columns={
                        "company": "Company",
                        "sector": "Sector",
                        "close": "Price",
                        "daily_return": "Return %",
                        "volume": "Volume"
                    }
                ),
                use_container_width=True,
                hide_index=True
            )

            st.header("Real-Time Top Movers & Anomalies")
            
            movers_df = latest_df.copy()
            movers_df['Return %'] = movers_df['daily_return'] * 100
            
            avg_hist_vol = filtered_df.groupby('ticker')['volume'].mean().reset_index().rename(columns={'volume': 'avg_hist_volume'})
            movers_df = movers_df.merge(avg_hist_vol, on='ticker', how='left')
            movers_df['Volume Spike (x)'] = movers_df['volume'] / movers_df['avg_hist_volume']

            tab_gainer, tab_loser, tab_anomaly = st.tabs(["🚀 Top Gainers", "📉 Top Losers", "🚨 Volume Spikes (Anomali)"])
            
            with tab_gainer:
                top_gainers = movers_df.sort_values("Return %", ascending=False).head(5)
                st.dataframe(top_gainers[['ticker', 'company', 'close', 'Return %']].rename(columns={'ticker': 'Stock ID', 'company': 'Company', 'close': 'Price'}), use_container_width=True, hide_index=True)
            with tab_loser:
                top_losers = movers_df.sort_values("Return %", ascending=True).head(5)
                st.dataframe(top_losers[['ticker', 'company', 'close', 'Return %']].rename(columns={'ticker': 'Stock ID', 'company': 'Company', 'close': 'Price'}), use_container_width=True, hide_index=True)
            with tab_anomaly:
                volume_spikes = movers_df.sort_values("Volume Spike (x)", ascending=False).head(5)
                st.dataframe(volume_spikes[['ticker', 'company', 'volume', 'Volume Spike (x)']].rename(columns={'ticker': 'Stock ID', 'company': 'Company', 'volume': 'Today Volume'}), use_container_width=True, hide_index=True)

            st.markdown("---")
            st.header("Interactive Ticker Explorer")
            ticker_options = (
                filtered_df[
                    ["ticker", "company"]
                ]
                .drop_duplicates()
                .sort_values("company")
            )

            ticker_options["display"] = (
                ticker_options["company"]
                + " ("
                + ticker_options["ticker"]
                + ")"
            )

            default_idx = 0

            if "AAPL" in ticker_options["ticker"].values:
                default_idx = ticker_options[
                    ticker_options["ticker"] == "AAPL"
                ].index[0]

            selected_display = st.selectbox(
                "Select Company",
                ticker_options["display"],
                index=0
            )

            selected_ticker = ticker_options[
                ticker_options["display"] == selected_display
            ]["ticker"].iloc[0]
            
            ticker_history = filtered_df[filtered_df['ticker'] == selected_ticker].sort_values("date").reset_index(drop=True)

            if ticker_history.empty:
                st.info(f"ℹ️ No historical data available for stock {selected_ticker} for the specified date range.")
            else:
                ticker_history['MA5'] = ticker_history['close'].rolling(window=5, min_periods=1).mean()
                ticker_history['MA20'] = ticker_history['close'].rolling(window=20, min_periods=1).mean()

                fig = go.Figure()
                fig.add_trace(go.Candlestick(
                    x=ticker_history['date'],
                    open=ticker_history['open'], high=ticker_history['high'],
                    low=ticker_history['low'], close=ticker_history['close'],
                    name="Harga OHLC"
                ))
                fig.add_trace(go.Scatter(x=ticker_history['date'], y=ticker_history['MA5'], line=dict(color='orange', width=1.5), name="MA 5 Hari"))
                fig.add_trace(go.Scatter(x=ticker_history['date'], y=ticker_history['MA20'], line=dict(color='#00FFFF', width=1.5), name="MA 20 Hari"))

                fig.update_layout(
                    title=f"Grafik Candlestick Saham {selected_ticker} ({start_date} s/d {end_date})",
                    xaxis_title="Tanggal", yaxis_title="Harga ($ USD)",
                    xaxis_rangeslider_visible=False, height=450, template="plotly_dark",
                    margin=dict(l=20, r=20, t=40, b=20),
                    dragmode=False 
                )
                
                fig.update_xaxes(fixedrange=True)
                fig.update_yaxes(fixedrange=True)

                st.plotly_chart(fig, use_container_width=True, key="quant_chart")

                st.markdown("---")
                st.header("Latest Stream Records")

                st.dataframe(
                    latest_df[
                        [
                            "ticker",
                            "company",
                            "sector",
                            "close",
                            "daily_return",
                            "volume"
                        ]
                    ].tail(20),
                    use_container_width=True
                )

st.caption(f"Terminal Polling Cycle Heartbeat: {pd.Timestamp.now().strftime('%H:%M:%S')} (4s Refresh Rate)")
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()