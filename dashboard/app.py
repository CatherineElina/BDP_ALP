import streamlit as st
import json
import os
import time
import pandas as pd
import datetime
import plotly.graph_objects as go

HISTORY_PATH = "/app/dashboard_data/history.jsonl"

st.set_page_config(page_title="Institutional Market Intelligence Console", layout="wide")

# Styling tema gelap finansial
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 26px; font-weight: bold; }
    div.stDataFrame { border: 1px solid #2D3139; border-radius: 8px; }
    h1, h2, h3 { color: #FFFFFF; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }
    </style>
""", unsafe_allow_html=True)

st.title("US Stock Market Dashboard")
st.markdown("Real-Time Ingestion Pipeline: **Kafka Broker** ➡️ **PySpark Stream Processor** ➡️ **Analyst Dashboard**")
st.markdown("---")

if not os.path.exists(HISTORY_PATH):
    st.warning("⏳ Waiting for PySpark streaming job to write data... (history.jsonl not found)")
else:
    # 1. Membaca data log dari PySpark
    history_rows = []
    with open(HISTORY_PATH) as f:
        for line in f:
            if line.strip():
                history_rows.append(json.loads(line))
                
    full_df = pd.DataFrame(history_rows)
    
    if not full_df.empty:
        # Standarisasi format tanggal
        full_df['date'] = pd.to_datetime(full_df['date'], utc=True).dt.tz_localize(None)
        
        # =============================================================
        # 📆 CONTROLLER UTAMA: FILTER TANGGAL GLOBAL
        # =============================================================
        st.header("Global Time Horizon Filter")
        date_range = st.date_input(
            "Define the analysis date range for all dashboard components:",
            value=(datetime.date(2020, 1, 1), datetime.date(2023, 12, 31)),
            min_value=datetime.date(2020, 1, 1),
            max_value=datetime.date(2026, 12, 31)
        )

        # Unpack tanggal filter
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = date_range[0]
            end_date = datetime.date(2026, 12, 31)

        # Potong data berdasarkan rentang tanggal
        filtered_df = full_df[
            (full_df['date'] >= pd.to_datetime(start_date)) & 
            (full_df['date'] <= pd.to_datetime(end_date))
        ]

        if filtered_df.empty:
            st.info(f"ℹ️ No market transaction data available for the date range {start_date} to {end_date}.")
        else:
            latest_date = filtered_df['date'].max()
            latest_df = filtered_df[filtered_df['date'] == latest_date]

            # =============================================================
            # 🟦 BAGIAN 2: MARKET PULSE & BREADTH
            # =============================================================
            st.markdown("---")
            st.header(f"Market Pulse Session: {latest_date.strftime('%Y-%m-%d')}")
            
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
            
            # =============================================================
            # 🟩 BAGIAN 3: SECTOR PERFORMANCE & CAPITAL FLOW
            # =============================================================
            st.markdown("---")
            st.header("Sector Performance & Capital Flow")
            
            sector_analysis = latest_df.groupby("sector").agg({
                "daily_return": "mean",
                "volume": "sum"
            }).reset_index()
            sector_analysis['daily_return_pct'] = sector_analysis['daily_return'] * 100
            sector_analysis = sector_analysis.sort_values("daily_return_pct", ascending=False)

            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.write("**Average Return per Sector (%)**")
                st.bar_chart(sector_analysis.set_index("sector")['daily_return_pct'])
            with col_chart2:
                st.write("**Total Transaction Volume per Sector (Shares)**")
                st.bar_chart(sector_analysis.set_index("sector")['volume'])
                
            # =============================================================
            # 🟨 BAGIAN 4: ANOMALI & TOP MOVERS
            # =============================================================
            st.markdown("---")
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

            # =============================================================
            # 🟪 BAGIAN 5: TICKER DEEP-DIVE EXPLORER (DIKUNCI / ANTI-ZOOM)
            # =============================================================
            st.markdown("---")
            st.header("Interactive Ticker Explorer")
            
            all_tickers = sorted(list(filtered_df['ticker'].unique()))
            selected_ticker = st.selectbox("Select Stock Code for Candlestick Analysis:", all_tickers, index=all_tickers.index("AAPL") if "AAPL" in all_tickers else 0)

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
                    dragmode=False # 🔥 Mematikan fitur drag-to-zoom default Plotly
                )
                
                # 🔥 CRITICAL FIX: Mengunci sumbu X dan Y agar tidak bisa di-zoom/pan pake gesture iPad atau scroll mouse
                fig.update_xaxes(fixedrange=True)
                fig.update_yaxes(fixedrange=True)

                st.plotly_chart(fig, use_container_width=True, key="quant_chart")

# -------------------------------------------------------------
# AUTOMATED REFRESH RUNTIME TRIGGER
# -------------------------------------------------------------
st.caption(f"Terminal Polling Cycle Heartbeat: {pd.Timestamp.now().strftime('%H:%M:%S')} (4s Refresh Rate)")
time.sleep(4)
st.rerun()