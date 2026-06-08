import streamlit as st
import json
import os
import time
import pandas as pd
import numpy as np
import datetime
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime as dt, timedelta

# ════════════════════════════════════════════════════════════════════════════════
# KONFIGURASI GLOBAL
# ════════════════════════════════════════════════════════════════════════════════

HISTORY_PATH = "/app/dashboard_data/history.jsonl"

# TradingView Color Palette
COLOR_GREEN = "#26a69a"
COLOR_RED = "#ef5350"
COLOR_NEUTRAL = "#8A94A6"
COLOR_TEXT = "#E6EDF3"

# Streamlit Config
st.set_page_config(
    page_title="Dashboard Analisis Pasar Saham",
    page_icon="▲",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ════════════════════════════════════════════════════════════════════════════════
# STYLING CSS MINIMAL & PROFESIONAL
# ════════════════════════════════════════════════════════════════════════════════

st.markdown("""
    <style>
    .main {
        padding: 1rem;
        background-color: #0E1117;
    }
    
    [data-testid="stMetricValue"] {
        font-size: 26px;
        font-weight: 700;
        font-family: 'Courier New', monospace;
        color: #00D9FF;
        letter-spacing: 0.5px;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 11px;
        font-weight: 600;
        color: #8A94A6;
        text-transform: uppercase;
        letter-spacing: 0.3px;
    }
    
    [data-testid="stMetricDelta"] {
        font-size: 10px;
        font-weight: 500;
        color: #B8C5D6;
    }
    
    div.stDataFrame {
        border: 1px solid #2D3139;
        border-radius: 4px;
        background-color: #0E1117;
    }
    
    h1, h2, h3, h4, h5, h6 {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-weight: 700;
        letter-spacing: 0.3px;
        color: #E6EDF3;
    }
    
    button[data-baseweb="tab"] {
        font-size: 13px;
        font-weight: 600;
    }
    
    [data-testid="stSidebar"] {
        background-color: #161B22;
        border-right: 1px solid #2D3139;
    }
    </style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60)
def load_streaming_data(file_path):
    """Memuat data dari file JSONL streaming."""
    df = pd.DataFrame()
    
    if not os.path.exists(file_path):
        return df, False
    
    try:
        history_rows = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        history_rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        
        if history_rows:
            df = pd.DataFrame(history_rows)
            
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(
                    df['date'], 
                    errors='coerce', 
                    utc=True
                ).dt.tz_localize(None)
            
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'daily_return']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.dropna(subset=['date', 'close', 'volume'])
            
            return df, True
        else:
            return df, False
            
    except Exception as e:
        st.sidebar.error(f"Kesalahan membaca log transaksi: {str(e)}")
        return df, False


def calculate_bollinger_bands(series, window=20, num_std=2):
    """Hitung Bollinger Bands."""
    sma = series.rolling(window=window).mean()
    std = series.rolling(window=window).std()
    upper = sma + (std * num_std)
    lower = sma - (std * num_std)
    return sma, upper, lower


def format_currency(value):
    """Format ke mata uang USD."""
    try:
        return f"${value:,.2f}"
    except (ValueError, TypeError):
        return "N/A"


def format_percentage(value):
    """Format ke persentase dengan tanda arah."""
    try:
        return f"{value:+.2f}%"
    except (ValueError, TypeError):
        return "N/A"


def format_volume(value):
    """Format volume dengan separator ribuan."""
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return "N/A"


def detect_volume_anomalies(df, ticker, window=20, threshold=2.0):
    """Deteksi anomali volume spike menggunakan Z-score."""
    ticker_data = df[df['ticker'] == ticker].copy().sort_values('date')
    
    if len(ticker_data) < window:
        ticker_data['volume_anomaly'] = False
        return ticker_data
    
    ticker_data['volume_ma'] = ticker_data['volume'].rolling(window=window).mean()
    ticker_data['volume_std'] = ticker_data['volume'].rolling(window=window).std()
    
    ticker_data['volume_ma'] = ticker_data['volume_ma'].fillna(1).replace(0, 1)
    ticker_data['volume_std'] = ticker_data['volume_std'].fillna(1).replace(0, 1)
    
    ticker_data['volume_zscore'] = (
        (ticker_data['volume'] - ticker_data['volume_ma']) / ticker_data['volume_std']
    )
    
    ticker_data['volume_anomaly'] = abs(ticker_data['volume_zscore']) > threshold
    
    return ticker_data


# ════════════════════════════════════════════════════════════════════════════════
# LOAD DATA & SESSION STATE
# ════════════════════════════════════════════════════════════════════════════════

if 'last_reload' not in st.session_state:
    st.session_state.last_reload = dt.now()

full_df, data_loaded = load_streaming_data(HISTORY_PATH)

# ════════════════════════════════════════════════════════════════════════════════
# SIDEBAR NAVIGATION & FILTERING
# ════════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("**NAVIGASI APLIKASI**")
    st.divider()
    
    current_page = st.radio(
        "Pilih Halaman",
        options=["Ringkasan Eksekutif", "Analisis Sektor", "Analisis Teknikal"],
        label_visibility="collapsed"
    )
    
    st.divider()
    st.markdown("**KONTROL STREAMING**")
    
    auto_refresh = st.toggle(
        "Aktifkan Auto-Refresh",
        value=True,
        help="Menyegarkan halaman secara berkala"
    )
    
    refresh_interval = st.slider(
        "Interval Detik",
        min_value=5,
        max_value=60,
        value=10,
        step=5
    )
    
    st.divider()
    st.markdown("**FILTER GLOBAL**")
    
    if data_loaded and not full_df.empty:
        min_log_date = full_df['date'].min().date()
        max_log_date = full_df['date'].max().date()
        
        selected_range = st.date_input(
            "Rentang Tanggal",
            value=(min_log_date, max_log_date),
            min_value=datetime.date(2018, 1, 1),
            max_value=datetime.date(2028, 12, 31)
        )
        
        # Crash Guard
        if isinstance(selected_range, (tuple, list)):
            if len(selected_range) == 2:
                start_date, end_date = selected_range
            else:
                start_date = selected_range[0]
                end_date = max_log_date
        else:
            start_date = selected_range
            end_date = max_log_date
        
        unique_sectors = sorted(full_df["sector"].dropna().unique())
        selected_sectors = st.multiselect(
            "Sektor Industri",
            unique_sectors,
            default=unique_sectors
        )
        
        if current_page == "Analisis Teknikal":
            ticker_options = full_df[["ticker", "company"]].drop_duplicates().sort_values("company")
            ticker_options["display"] = ticker_options["company"] + " (" + ticker_options["ticker"] + ")"
            
            selected_display = st.selectbox(
                "Emiten Terpilih",
                ticker_options["display"],
                index=0
            )
            
            selected_ticker = ticker_options[
                ticker_options["display"] == selected_display
            ]["ticker"].iloc[0]
        
        st.divider()
        st.caption(
            f"Data: {len(full_df):,} baris | "
            f"{full_df['ticker'].nunique()} emiten | "
            f"{full_df['sector'].nunique()} sektor"
        )
    
    else:
        st.warning("Menunggu data dari Spark Streaming Job...")

# ════════════════════════════════════════════════════════════════════════════════
# PAGE 1: RINGKASAN EKSEKUTIF & SENTIMEN MAKRO
# ════════════════════════════════════════════════════════════════════════════════

if current_page == "Ringkasan Eksekutif":
    
    if not data_loaded or full_df.empty:
        st.warning("Dashboard sedang menunggu inisialisasi data dari pipeline streaming.")
        if auto_refresh:
            time.sleep(refresh_interval)
            st.rerun()
    
    else:
        filtered_df = full_df[
            (full_df['date'] >= pd.to_datetime(start_date)) & 
            (full_df['date'] <= pd.to_datetime(end_date)) &
            (full_df['sector'].isin(selected_sectors))
        ].copy()
        
        if filtered_df.empty:
            st.info("Tidak ada data yang sesuai dengan filter terpilih.")
        
        else:
            latest_date = filtered_df['date'].max()
            latest_df = filtered_df[filtered_df['date'] == latest_date]
            
            # Header
            st.markdown("## RINGKASAN EKSEKUTIF PASAR")
            st.markdown(f"Status Sesi: {latest_date.strftime('%d-%m-%Y')}")
            st.divider()
            
            # Telemetri Pipeline
            st.markdown("**TELEMETRI INFRASTRUKTUR PIPELINE**")
            
            telem_col1, telem_col2, telem_col3, telem_col4 = st.columns(4)
            
            with telem_col1:
                st.metric(
                    "Total Ingestion",
                    f"{len(full_df):,}"
                )
            
            with telem_col2:
                st.metric(
                    "Emiten Unik",
                    f"{full_df['ticker'].nunique()}"
                )
            
            with telem_col3:
                st.metric(
                    "Sektor Aktif",
                    f"{full_df['sector'].nunique()}"
                )
            
            with telem_col4:
                st.metric(
                    "Sesi Terakhir",
                    latest_date.strftime('%d-%m-%Y')
                )
            
            st.divider()
            
            # KPI Grid
            st.markdown("**MARKET PULSE MATRIX**")
            
            total_market_volume = latest_df['volume'].sum()
            avg_market_return = latest_df['daily_return'].mean() * 100
            market_volatility = latest_df['daily_return'].std() * 100
            
            green_stocks = (latest_df['daily_return'] > 0).sum()
            total_stocks = len(latest_df)
            red_stocks = total_stocks - green_stocks
            pct_green = (green_stocks / total_stocks * 100) if total_stocks > 0 else 0
            
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            
            with kpi1:
                st.metric(
                    "Volume Transaksi Pasar",
                    format_volume(total_market_volume / 1e6) + "M",
                    delta=f"Emiten aktif: {total_stocks}"
                )
            
            with kpi2:
                return_sentiment = "Bullish" if avg_market_return > 0 else "Bearish"
                st.metric(
                    "Rata-Rata Return Sesi",
                    format_percentage(avg_market_return),
                    delta=return_sentiment
                )
            
            with kpi3:
                st.metric(
                    "Market Breadth",
                    f"{pct_green:.1f}%",
                    delta=f"{green_stocks} naik, {red_stocks} turun"
                )
            
            with kpi4:
                risk_level = "Tinggi" if market_volatility > 1.5 else "Rendah"
                st.metric(
                    "Volatilitas Pasar",
                    f"{market_volatility:.2f}%",
                    delta=f"Risiko {risk_level}"
                )
            
            st.divider()
            
            # Log Pemantauan
            st.markdown("**LOG PEMANTAUAN ALIRAN DATA**")
            
            log_df = filtered_df[[
                'date', 'ticker', 'company', 'sector', 'close', 'volume', 'daily_return'
            ]].tail(10).copy()
            
            log_df['date'] = log_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            log_df['close'] = log_df['close'].apply(format_currency)
            log_df['volume'] = log_df['volume'].apply(format_volume)
            log_df['daily_return'] = log_df['daily_return'].apply(format_percentage)
            
            log_df = log_df.rename(columns={
                'date': 'Tanggal',
                'ticker': 'Ticker',
                'company': 'Perusahaan',
                'sector': 'Sektor',
                'close': 'Harga Penutup',
                'volume': 'Volume',
                'daily_return': 'Return'
            })
            
            st.dataframe(
                log_df,
                use_container_width=True,
                hide_index=True
            )
            
            st.divider()
            
            current_time = dt.now().strftime('%H:%M:%S')
            st.caption(f"Pembaruan terakhir: {current_time} | Data point: {len(filtered_df):,}")
            
            if auto_refresh:
                time.sleep(refresh_interval)
                st.rerun()

# ════════════════════════════════════════════════════════════════════════════════
# PAGE 2: ANALISIS ROTASI SEKTOR & ALIRAN KAPITAL
# ════════════════════════════════════════════════════════════════════════════════

elif current_page == "Analisis Sektor":
    
    if not data_loaded or full_df.empty:
        st.warning("Dashboard sedang menunggu inisialisasi data dari pipeline streaming.")
        if auto_refresh:
            time.sleep(refresh_interval)
            st.rerun()
    
    else:
        filtered_df = full_df[
            (full_df['date'] >= pd.to_datetime(start_date)) & 
            (full_df['date'] <= pd.to_datetime(end_date)) &
            (full_df['sector'].isin(selected_sectors))
        ].copy()
        
        if filtered_df.empty:
            st.info("Tidak ada data yang sesuai dengan filter terpilih.")
        
        else:
            latest_date = filtered_df['date'].max()
            latest_df = filtered_df[filtered_df['date'] == latest_date]
            
            st.markdown("## ANALISIS ROTASI SEKTOR")
            st.markdown(f"Sesi: {latest_date.strftime('%d-%m-%Y')}")
            st.divider()
            
            # Agregasi sektor
            sector_analysis = latest_df.groupby("sector").agg({
                "daily_return": "mean",
                "volume": "sum"
            }).reset_index()
            sector_analysis['daily_return_pct'] = sector_analysis['daily_return'] * 100
            sector_analysis = sector_analysis.sort_values("daily_return_pct", ascending=False)
            
            # Treemap dan Bar Charts
            col_left, col_right = st.columns([1.1, 0.9])
            
            with col_left:
                fig_treemap = px.treemap(
                    sector_analysis,
                    path=["sector"],
                    values="volume",
                    color="daily_return_pct",
                    color_continuous_scale=["#ef5350", "#f5f5f5", "#26a69a"],
                    color_continuous_midpoint=0.0,
                    title="Peta Panas Sektor",
                    template="plotly_dark"
                )
                
                fig_treemap.update_traces(
                    textposition="middle center",
                    textfont=dict(size=11, color="white")
                )
                
                fig_treemap.update_layout(
                    margin=dict(l=5, r=5, t=40, b=5),
                    height=380,
                    coloraxis_colorbar=dict(title="Return %", thickness=12, len=0.6)
                )
                
                st.plotly_chart(fig_treemap, use_container_width=True, key="treemap")
            
            with col_right:
                tab_return, tab_volume = st.tabs(["Return Sektor", "Volume Sektor"])
                
                with tab_return:
                    fig_bar_ret = px.bar(
                        sector_analysis,
                        x="daily_return_pct",
                        y="sector",
                        orientation="h",
                        title="Return per Sektor (%)",
                        color="daily_return_pct",
                        color_continuous_scale=["#ef5350", "#f5f5f5", "#26a69a"],
                        color_continuous_midpoint=0.0,
                        template="plotly_dark"
                    )
                    
                    fig_bar_ret.update_layout(
                        margin=dict(l=5, r=5, t=40, b=5),
                        height=300,
                        showlegend=False,
                        coloraxis_showscale=False
                    )
                    fig_bar_ret.update_yaxes(categoryorder="total ascending")
                    fig_bar_ret.update_xaxes(fixedrange=True)
                    fig_bar_ret.update_yaxes(fixedrange=True)
                    
                    st.plotly_chart(fig_bar_ret, use_container_width=True, key="bar_ret")
                
                with tab_volume:
                    fig_bar_vol = px.bar(
                        sector_analysis,
                        x="volume",
                        y="sector",
                        orientation="h",
                        title="Volume per Sektor",
                        color="volume",
                        color_continuous_scale="Blues",
                        template="plotly_dark"
                    )
                    
                    fig_bar_vol.update_layout(
                        margin=dict(l=5, r=5, t=40, b=5),
                        height=300,
                        showlegend=False,
                        coloraxis_showscale=False
                    )
                    fig_bar_vol.update_yaxes(categoryorder="total ascending")
                    fig_bar_vol.update_xaxes(fixedrange=True)
                    fig_bar_vol.update_yaxes(fixedrange=True)
                    
                    st.plotly_chart(fig_bar_vol, use_container_width=True, key="bar_vol")
            
            st.divider()
            
            # Papan Peringkat Saham
            st.markdown("**PAPAN PERINGKAT SAHAM**")
            
            col_gainers, col_losers = st.columns(2)
            
            with col_gainers:
                st.subheader("Top 5 Gainers")
                
                top_gainers = latest_df.nlargest(5, 'daily_return')[[
                    'ticker', 'company', 'daily_return', 'close', 'volume'
                ]].copy()
                top_gainers['daily_return'] = top_gainers['daily_return'].apply(format_percentage)
                top_gainers['close'] = top_gainers['close'].apply(format_currency)
                top_gainers['volume'] = top_gainers['volume'].apply(format_volume)
                
                top_gainers = top_gainers.rename(columns={
                    'ticker': 'Ticker',
                    'company': 'Perusahaan',
                    'daily_return': 'Return',
                    'close': 'Harga',
                    'volume': 'Volume'
                })
                
                st.dataframe(
                    top_gainers,
                    use_container_width=True,
                    hide_index=True,
                    height=250
                )
            
            with col_losers:
                st.subheader("Top 5 Losers")
                
                top_losers = latest_df.nsmallest(5, 'daily_return')[[
                    'ticker', 'company', 'daily_return', 'close', 'volume'
                ]].copy()
                top_losers['daily_return'] = top_losers['daily_return'].apply(format_percentage)
                top_losers['close'] = top_losers['close'].apply(format_currency)
                top_losers['volume'] = top_losers['volume'].apply(format_volume)
                
                top_losers = top_losers.rename(columns={
                    'ticker': 'Ticker',
                    'company': 'Perusahaan',
                    'daily_return': 'Return',
                    'close': 'Harga',
                    'volume': 'Volume'
                })
                
                st.dataframe(
                    top_losers,
                    use_container_width=True,
                    hide_index=True,
                    height=250
                )
            
            st.divider()
            
            current_time = dt.now().strftime('%H:%M:%S')
            st.caption(f"Pembaruan terakhir: {current_time} | Data point: {len(filtered_df):,}")
            
            if auto_refresh:
                time.sleep(refresh_interval)
                st.rerun()

# ════════════════════════════════════════════════════════════════════════════════
# PAGE 3: WORKBENCH ANALISIS TEKNIKAL MIKRO
# ════════════════════════════════════════════════════════════════════════════════

elif current_page == "Analisis Teknikal":
    
    if not data_loaded or full_df.empty:
        st.warning("Dashboard sedang menunggu inisialisasi data dari pipeline streaming.")
        if auto_refresh:
            time.sleep(refresh_interval)
            st.rerun()
    
    else:
        filtered_df = full_df[
            (full_df['date'] >= pd.to_datetime(start_date)) & 
            (full_df['date'] <= pd.to_datetime(end_date)) &
            (full_df['sector'].isin(selected_sectors))
        ].copy()
        
        if filtered_df.empty:
            st.info("Tidak ada data yang sesuai dengan filter terpilih.")
        
        else:
            st.markdown(f"## ANALISIS TEKNIKAL MIKRO: {selected_ticker}")
            st.divider()
            
            ticker_data = filtered_df[filtered_df['ticker'] == selected_ticker].sort_values('date').copy()
            
            if ticker_data.empty:
                st.warning(f"Tidak ada data tersedia untuk {selected_ticker}.")
            
            else:
                # Hitung indikator teknikal
                ticker_data['MA5'] = ticker_data['close'].rolling(window=5).mean()
                ticker_data['MA20'] = ticker_data['close'].rolling(window=20).mean()
                ticker_data['BB_MA'], ticker_data['BB_Upper'], ticker_data['BB_Lower'] = (
                    calculate_bollinger_bands(ticker_data['close'], window=20, num_std=2)
                )
                
                ticker_data['candle_color'] = ticker_data.apply(
                    lambda row: COLOR_GREEN if row['close'] >= row['open'] else COLOR_RED,
                    axis=1
                )
                
                # Subplot: Candlestick + Volume
                fig = make_subplots(
                    rows=2,
                    cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.03,
                    row_heights=[0.75, 0.25]
                )
                
                # Candlestick
                fig.add_trace(
                    go.Candlestick(
                        x=ticker_data['date'],
                        open=ticker_data['open'],
                        high=ticker_data['high'],
                        low=ticker_data['low'],
                        close=ticker_data['close'],
                        name='OHLC',
                        increasing_line_color=COLOR_GREEN,
                        decreasing_line_color=COLOR_RED
                    ),
                    row=1,
                    col=1
                )
                
                # MA5
                fig.add_trace(
                    go.Scatter(
                        x=ticker_data['date'],
                        y=ticker_data['MA5'],
                        name='MA 5',
                        line=dict(color='#FFA500', width=1.5)
                    ),
                    row=1,
                    col=1
                )
                
                # MA20
                fig.add_trace(
                    go.Scatter(
                        x=ticker_data['date'],
                        y=ticker_data['MA20'],
                        name='MA 20',
                        line=dict(color='#FFD700', width=1.5)
                    ),
                    row=1,
                    col=1
                )
                
                # Bollinger Upper
                fig.add_trace(
                    go.Scatter(
                        x=ticker_data['date'],
                        y=ticker_data['BB_Upper'],
                        name='BB Upper',
                        line=dict(width=0),
                        hoverinfo='skip'
                    ),
                    row=1,
                    col=1
                )
                
                # Bollinger Lower
                fig.add_trace(
                    go.Scatter(
                        x=ticker_data['date'],
                        y=ticker_data['BB_Lower'],
                        name='BB Lower',
                        line=dict(width=0),
                        fillcolor='rgba(173, 216, 230, 0.04)',
                        fill='tonexty',
                        hoverinfo='skip'
                    ),
                    row=1,
                    col=1
                )
                
                # Volume
                fig.add_trace(
                    go.Bar(
                        x=ticker_data['date'],
                        y=ticker_data['volume'],
                        name='Volume',
                        marker_color=ticker_data['candle_color'],
                        marker_line_width=0
                    ),
                    row=2,
                    col=1
                )
                
                fig.update_layout(
                    title={
                        'text': f"Candlestick: {selected_ticker}",
                        'x': 0.5,
                        'xanchor': 'center',
                        'font': {'size': 14}
                    },
                    template="plotly_dark",
                    height=600,
                    hovermode='x unified',
                    margin=dict(l=5, r=5, t=40, b=5),
                    xaxis_rangeslider_visible=False
                )
                
                fig.update_xaxes(fixedrange=True)
                fig.update_yaxes(fixedrange=True)
                
                fig.update_yaxes(title_text="Harga (USD)", row=1, col=1)
                fig.update_yaxes(title_text="Volume", row=2, col=1)
                
                st.plotly_chart(fig, use_container_width=True, key="candlestick")
                
                # Statistik Teknikal
                st.markdown("**STATISTIK TEKNIKAL**")
                
                col_stats1, col_stats2, col_stats3 = st.columns(3)
                
                with col_stats1:
                    st.metric(
                        "Harga Tertinggi",
                        format_currency(ticker_data['high'].max())
                    )
                
                with col_stats2:
                    st.metric(
                        "Harga Terendah",
                        format_currency(ticker_data['low'].min())
                    )
                
                with col_stats3:
                    latest_price = ticker_data['close'].iloc[-1]
                    intraday_return = ((latest_price - ticker_data['open'].iloc[-1]) / 
                                      ticker_data['open'].iloc[-1] * 100)
                    st.metric(
                        "Return Intraday",
                        format_percentage(intraday_return),
                        delta=format_currency(latest_price)
                    )
                
                st.divider()
                
                # Deteksi Anomali Volume
                st.markdown("**DETEKSI ANOMALI: VOLUME SPIKE**")
                
                ticker_history_full = filtered_df[filtered_df['ticker'] == selected_ticker].sort_values('date')
                
                if len(ticker_history_full) > 20:
                    anomaly_df = detect_volume_anomalies(ticker_history_full, selected_ticker, window=20, threshold=2.0)
                    anomalies = anomaly_df[anomaly_df['volume_anomaly']]
                    
                    if not anomalies.empty:
                        anomaly_display = anomalies[[
                            'date', 'volume', 'volume_ma', 'volume_zscore'
                        ]].copy()
                        
                        anomaly_display['date'] = anomaly_display['date'].dt.strftime('%Y-%m-%d')
                        anomaly_display['volume'] = anomaly_display['volume'].apply(format_volume)
                        anomaly_display['volume_ma'] = anomaly_display['volume_ma'].apply(format_volume)
                        anomaly_display['volume_zscore'] = anomaly_display['volume_zscore'].apply(
                            lambda x: f"{x:+.2f}"
                        )
                        
                        anomaly_display = anomaly_display.rename(columns={
                            'date': 'Tanggal',
                            'volume': 'Volume Aktual',
                            'volume_ma': 'MA 20 Historis',
                            'volume_zscore': 'Z-Score'
                        })
                        
                        st.dataframe(
                            anomaly_display,
                            use_container_width=True,
                            hide_index=True,
                            height=200
                        )
                    else:
                        st.info("Tidak ada anomali volume terdeteksi pada periode ini.")
                else:
                    st.info("Data historis kurang dari 20 periode untuk analisis anomali.")
                
                st.divider()
                
                current_time = dt.now().strftime('%H:%M:%S')
                st.caption(f"Pembaruan terakhir: {current_time} | Data historis: {len(ticker_data)} periode")
                
                if auto_refresh:
                    time.sleep(refresh_interval)
                    st.rerun()
