# ===-Oil_Well_Dashboard.py----------------------------------------------===//
# Oil Well Monitoring Dashboard with Forecasting
# Real-time monitoring and analysis of oil well production data with next-day predictions
# ===----------------------------------------------------------------------===//

import streamlit as st
import json
import os
import pandas as pd
import threading
import socket
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timezone, timedelta
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Oil Well Dashboard with Forecasting", layout="wide", page_icon="🛢️")
# Auto-refresh every 10 seconds
st_autorefresh(interval=10 * 1000, key="datarefresh")
st.title("🛢️ Oil Well Production Dashboard with AI Forecasting")

history_file = "oil_well_history.json"

if "server_active" not in st.session_state:
    st.session_state.server_active = False

if not os.path.exists(history_file):
    with open(history_file, "w") as f:
        json.dump([], f)


# Socket listener for oil well data
def dashboard_listener(host='0.0.0.0', port=9090):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen()
    print(f"🛢️ [OIL WELL DASHBOARD LISTENING] on {host}:{port}")

    while True:
        conn, addr = server.accept()
        with conn:
            try:
                buffer = ""
                while True:
                    chunk = conn.recv(8192).decode()  # Larger buffer for oil well data
                    if not chunk:
                        break
                    buffer += chunk

                if buffer:
                    try:
                        data = json.loads(buffer)

                        # Handle both single records and batch data
                        if isinstance(data, dict) and "well_data" in data:
                            new_data = data["well_data"]  # Extract well data from summary
                        elif isinstance(data, list):
                            new_data = data
                        else:
                            new_data = [data]

                        print(f"🛢️ [RECEIVED] from {addr}: {len(new_data)} oil well records")

                        # Count records with forecasts
                        forecast_count = sum(1 for record in new_data if record.get("forecast"))
                        if forecast_count > 0:
                            print(f"🔮 [FORECASTS] Received {forecast_count} records with forecasts")

                        # Load existing history
                        if os.path.exists(history_file):
                            with open(history_file, "r") as f:
                                history = json.load(f)
                        else:
                            history = []

                        # Add new data to the beginning (newest first)
                        history = [*new_data, *history]

                        # Keep only last 1000 records to prevent file from growing too large
                        history = history[:1000]

                        # Save updated history
                        with open(history_file, "w") as f:
                            json.dump(history, f, indent=2)

                    except json.JSONDecodeError as e:
                        print(f"❌ [ERROR] JSON decode failed: {e}")
            except Exception as e:
                print(f"❌ [ERROR] Receiving oil well data: {e}")


# Start dashboard listener thread
threading.Thread(target=dashboard_listener, daemon=True).start()

# Server control
st.markdown("### 🎛️ Dashboard Control")
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("🟢 Start Dashboard"):
        st.session_state.server_active = True
        st.success("🛢️ Oil Well Dashboard server started on port 9090")
with col2:
    if st.button("🔴 Stop Dashboard"):
        st.session_state.server_active = False
        st.warning("Dashboard server stopped.")
with col3:
    if st.button("🧹 Reset Data"):
        with open(history_file, "w") as f:
            json.dump([], f)
        st.session_state["cleared"] = True
        st.success("Dashboard data has been reset.")

# Oil Well Status Legend
st.markdown("---")
st.markdown("""
### 🚩 Oil Well Status Indicators
- 🔴 **Critical**: Immediate intervention required
- 🟠 **Warning**: Parameter needs attention  
- 🟡 **Caution**: Monitor closely
- 🟢 **Normal/Optimal**: Operating within acceptable range
- 🔮 **Forecast Alert**: Predicted issue for next day
- ⚪ **Unknown**: Status not available
""")

# Oil Well Parameter Ranges
with st.expander("ℹ️ Oil Well Parameter Ranges & Thresholds"):
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        ### 🛢️ Oil Volume (barrels/day)
        - **Critical Low**: < 20
        - **Low**: 20-34
        - **Optimal**: 40-80
        - **High**: 80-100
        - **Exceptional**: > 100

        ### 💧 Water Cut (%)
        - **Excellent**: < 20%
        - **Good**: 20-40%
        - **Concerning**: 40-60%
        - **High**: 60-80%
        - **Critical**: > 80%

        ### ⛽ Gas Volume (MCF)
        - **Low**: < 5,000
        - **Normal**: 8,000-15,000
        - **High**: 15,000-20,000
        - **Very High**: > 20,000
        """)

    with col2:
        st.markdown("""
        ### 🏭 Reservoir Pressure (psi)
        - **Critical Low**: < 150
        - **Low**: 150-199
        - **Normal**: 200-250
        - **High**: 251-280
        - **Very High**: > 280

        ### 📏 Dynamic Level (feet)
        - **Very Low**: < 1500 (Excellent)
        - **Normal**: 1600-1750
        - **High**: 1751-1800
        - **Critical High**: > 1900

        ### ⏰ Working Hours (hours/day)
        - **Low**: < 16
        - **Below Normal**: 16-19
        - **Normal**: 20-23
        - **Optimal**: 24
        """)

# Main dashboard display
if st.session_state.get("server_active", False) and not st.session_state.get("cleared", False):
    try:
        with open(history_file, "r") as f:
            data = json.load(f)

        # Flatten nested data structures
        flattened_data = []
        for entry in data:
            if isinstance(entry, list):
                flattened_data.extend(entry)
            else:
                flattened_data.append(entry)

        df = pd.DataFrame(flattened_data)

        if not df.empty:
            # Convert timestamp to local timezone
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Karachi")

            # Check for recent data
            latest_time = df["timestamp"].max()
            now_local = datetime.now(timezone.utc).astimezone()

            if (now_local - latest_time.to_pydatetime()) > timedelta(seconds=30):
                st.warning(
                    "⚠️ No new data received in the last 30 seconds. Oil well sensors may have stopped transmitting.")

            # Create status flags for display
            def get_status_flag(status):
                if isinstance(status, str):
                    if "Critical" in status:
                        return "🔴"
                    elif "Warning" in status or "High" in status or "Low" in status:
                        return "🟠"
                    elif "Concerning" in status or "Caution" in status:
                        return "🟡"
                    elif "Normal" in status or "Optimal" in status or "Excellent" in status or "Healthy" in status:
                        return "🟢"
                return "⚪"

            def format_parameter(value, status_dict, key, unit=""):
                if isinstance(status_dict, dict):
                    status = status_dict.get(key, "Unknown")
                    flag = get_status_flag(status)
                    if pd.isna(value) or value is None:
                        return f"N/A {flag}"
                    return f"{value}{unit} {flag}"
                return f"{value}{unit} ⚪"

            # Extract forecast data for analysis
            forecast_available = df['forecast'].notna().sum()
            
            # Forecast Summary Section
            if forecast_available > 0:
                st.markdown("### 🔮 AI Forecast Summary")
                
                latest_forecast = None
                for _, record in df.iterrows():
                    if record.get('forecast'):
                        latest_forecast = record['forecast']
                        break
                
                if latest_forecast and latest_forecast.get('predictions'):
                    col1, col2, col3, col4 = st.columns(4)
                    
                    predictions = latest_forecast['predictions']
                    forecast_date = latest_forecast.get('forecast_date', 'Unknown')
                    
                    with col1:
                        predicted_oil = predictions.get('Oil volume', 'N/A')
                        st.metric("🔮 Tomorrow's Oil Production", 
                                f"{predicted_oil} bbl/day",
                                help=f"Forecast for {forecast_date}")
                    
                    with col2:
                        predicted_water_cut = predictions.get('Water cut', 'N/A')
                        st.metric("🔮 Tomorrow's Water Cut", 
                                f"{predicted_water_cut}%",
                                help=f"Forecast for {forecast_date}")
                    
                    with col3:
                        predicted_pressure = predictions.get('Reservoir pressure', 'N/A')
                        st.metric("🔮 Tomorrow's Pressure", 
                                f"{predicted_pressure} psi",
                                help=f"Forecast for {forecast_date}")
                    
                    with col4:
                        forecast_time = latest_forecast.get('generated_at', 'Unknown')
                        if forecast_time != 'Unknown':
                            forecast_dt = pd.to_datetime(forecast_time).strftime('%H:%M:%S')
                        else:
                            forecast_dt = 'Unknown'
                        st.metric("🕐 Last Forecast", forecast_dt)

            # Summary Statistics
            st.markdown("### 📊 Current Well Performance Summary")

            if len(df) > 0:
                latest_record = df.iloc[0]  # Most recent record

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    oil_vol = latest_record.get("Oil volume", "N/A")
                    oil_status = latest_record.get("status", {}).get("oil_volume", "Unknown")
                    st.metric("🛢️ Oil Production", f"{oil_vol} bbl/day",
                              help=f"Status: {oil_status}")

                with col2:
                    water_cut = latest_record.get("Water cut", "N/A")
                    water_status = latest_record.get("status", {}).get("water_cut", "Unknown")
                    st.metric("💧 Water Cut", f"{water_cut}%",
                              help=f"Status: {water_status}")

                with col3:
                    pressure = latest_record.get("Reservoir pressure", "N/A")
                    pressure_status = latest_record.get("status", {}).get("reservoir_pressure", "Unknown")
                    st.metric("🏭 Reservoir Pressure", f"{pressure} psi",
                              help=f"Status: {pressure_status}")

                with col4:
                    overall_health = latest_record.get("status", {}).get("overall_well_health", "Unknown")
                    health_flag = get_status_flag(overall_health)
                    st.metric("🏥 Overall Health", f"{overall_health} {health_flag}")

            # Active Alerts (including forecast alerts)
            st.markdown("### 🚨 Active Alerts & Forecast Warnings")
            recent_alerts = []
            forecast_alerts = []
            
            for _, record in df.head(10).iterrows():  # Check last 10 records
                alerts = record.get("alerts", [])
                if alerts:
                    client = record.get("client", "Unknown")
                    timestamp = record["timestamp"].strftime("%H:%M:%S")
                    for alert in alerts:
                        if "FORECAST" in alert:
                            forecast_alerts.append(f"**{timestamp}** - {client}: {alert}")
                        else:
                            recent_alerts.append(f"**{timestamp}** - {client}: {alert}")

            # Show forecast alerts first
            if forecast_alerts:
                st.markdown("#### 🔮 Forecast Alerts")
                for alert in forecast_alerts[:5]:  # Show last 5 forecast alerts
                    if "CRITICAL" in alert or "ALERT" in alert:
                        st.error(f"🔮 {alert}")
                    elif "WARNING" in alert:
                        st.warning(f"🔮 {alert}")
                    else:
                        st.info(f"🔮 {alert}")
            
            # Show current alerts
            if recent_alerts:
                st.markdown("#### ⚡ Current Alerts")
                for alert in recent_alerts[:5]:  # Show last 5 current alerts
                    if "CRITICAL" in alert:
                        st.error(alert)
                    elif "WARNING" in alert:
                        st.warning(alert)
                    else:
                        st.info(alert)
            
            if not recent_alerts and not forecast_alerts:
                st.success("✅ No active alerts - All wells operating normally with positive forecasts")

            # Next-Day Forecast Predictions Chart
            if forecast_available > 0:
                st.markdown("### 🔮 Next-Day Forecast Predictions")
                
                # Prepare forecast data for visualization
                forecast_data = []
                
                for _, record in df.head(20).iterrows():
                    if record.get('forecast') and record['forecast'].get('predictions'):
                        predictions = record['forecast']['predictions']
                        forecast_date = record['forecast']['forecast_date']
                        generated_time = record['timestamp']
                        
                        forecast_data.append({
                            'Generated_At': generated_time,
                            'Forecast_Date': forecast_date,
                            'Oil Volume': predictions.get('Oil volume', 0),
                            'Water Cut': predictions.get('Water cut', 0),
                            'Gas Volume': predictions.get('Gas volume', 0),
                            'Reservoir Pressure': predictions.get('Reservoir pressure', 0),
                            'Dynamic Level': predictions.get('Dynamic level', 0),
                            'Working Hours': predictions.get('Working hours', 0),
                            'Water Volume': predictions.get('Water volume', 0),
                            'Volume of Liquid': predictions.get('Volume of liquid', 0)
                        })
                
                if forecast_data:
                    forecast_df = pd.DataFrame(forecast_data)
                    
                    # Create forecast prediction charts
                    tab1, tab2, tab3 = st.tabs(["🔮 All Forecast Parameters", "🔮 Production Forecasts", "🔮 Operational Forecasts"])
                    
                    with tab1:
                        # All forecast parameters
                        forecast_cols = ['Oil Volume', 'Water Cut', 'Gas Volume', 'Reservoir Pressure', 'Dynamic Level', 'Working Hours']
                        available_forecast_cols = [col for col in forecast_cols if col in forecast_df.columns and forecast_df[col].notna().any()]
                        
                        if available_forecast_cols:
                            forecast_chart_data = forecast_df.set_index('Generated_At')[available_forecast_cols]
                            st.line_chart(forecast_chart_data, height=400)
                            st.caption("📊 All forecasted parameters over time")
                    
                    with tab2:
                        # Production-focused forecasts
                        prod_forecast_cols = ['Oil Volume', 'Water Cut', 'Gas Volume', 'Water Volume', 'Volume of Liquid']
                        available_prod_cols = [col for col in prod_forecast_cols if col in forecast_df.columns and forecast_df[col].notna().any()]
                        
                        if available_prod_cols:
                            prod_forecast_data = forecast_df.set_index('Generated_At')[available_prod_cols]
                            st.line_chart(prod_forecast_data, height=400)
                            st.caption("🛢️ Production-related forecast parameters")
                    
                    with tab3:
                        # Operational-focused forecasts
                        ops_forecast_cols = ['Reservoir Pressure', 'Dynamic Level', 'Working Hours']
                        available_ops_cols = [col for col in ops_forecast_cols if col in forecast_df.columns and forecast_df[col].notna().any()]
                        
                        if available_ops_cols:
                            ops_forecast_data = forecast_df.set_index('Generated_At')[available_ops_cols]
                            st.line_chart(ops_forecast_data, height=400)
                            st.caption("🔧 Operational forecast parameters")
                    
                    # Show forecast data table
                    st.markdown("#### 📋 Forecast Predictions Table")
                    display_forecast_df = forecast_df.copy()
                    display_forecast_df['Generated_At'] = pd.to_datetime(display_forecast_df['Generated_At']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Reorder columns for better display
                    column_order = ['Generated_At', 'Forecast_Date', 'Oil Volume', 'Water Cut', 'Gas Volume', 
                                  'Water Volume', 'Volume of Liquid', 'Reservoir Pressure', 'Dynamic Level', 'Working Hours']
                    available_columns = [col for col in column_order if col in display_forecast_df.columns]
                    
                    st.dataframe(
                        display_forecast_df[available_columns].head(10),
                        use_container_width=True,
                        height=300
                    )

            # Historical Data Table with Forecast Information
            st.markdown("### 📋 Historical Oil Well Data with Forecasts")

            # Prepare display dataframe
            display_df = df.copy()

            # Format columns with status indicators
            display_df["Oil Production"] = df.apply(lambda row: format_parameter(
                row.get("Oil volume"), row.get("status"), "oil_volume", " bbl/day"), axis=1)
            display_df["Water Cut"] = df.apply(lambda row: format_parameter(
                row.get("Water cut"), row.get("status"), "water_cut", "%"), axis=1)
            display_df["Gas Volume"] = df.apply(lambda row: format_parameter(
                row.get("Gas volume"), row.get("status"), "gas_volume", " MCF"), axis=1)
            display_df["Pressure"] = df.apply(lambda row: format_parameter(
                row.get("Reservoir pressure"), row.get("status"), "reservoir_pressure", " psi"), axis=1)
            display_df["Dynamic Level"] = df.apply(lambda row: format_parameter(
                row.get("Dynamic level"), row.get("status"), "dynamic_level", " ft"), axis=1)
            display_df["Working Hours"] = df.apply(lambda row: format_parameter(
                row.get("Working hours"), row.get("status"), "working_hours", " hrs"), axis=1)
            
            # Add forecast indicator
            display_df["Forecast"] = df.apply(lambda row: "🔮 Yes" if row.get("forecast") else "❌ No", axis=1)

            # Select columns for display
            table_df = display_df[["timestamp", "client", "Oil Production", "Water Cut",
                                   "Gas Volume", "Pressure", "Dynamic Level", "Working Hours", "Forecast"]].copy()
            table_df.columns = ["Timestamp", "Well ID", "Oil Production", "Water Cut",
                                "Gas Volume", "Pressure", "Dynamic Level", "Working Hours", "Forecast"]

            # Add serial numbers (newest = 1)
            table_df.insert(0, "Record #", range(1, len(table_df) + 1))

            # Display table with HTML formatting
            st.markdown(
                table_df.head(20).to_html(escape=False, index=False),  # Show last 20 records
                unsafe_allow_html=True
            )

            # Real-time Charts
            st.markdown("### 📈 Real-Time Production Charts")

            # Prepare chart data
            chart_df = df.copy()
            chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp"])
            chart_df.set_index("timestamp", inplace=True)

            # Select numeric columns for charting
            numeric_cols = ["Oil volume", "Water cut", "Gas volume", "Reservoir pressure",
                            "Dynamic level", "Working hours"]

            available_cols = [col for col in numeric_cols if col in chart_df.columns]

            if available_cols:
                # Create tabs for different chart types
                tab1, tab2, tab3 = st.tabs(["📊 All Parameters", "🛢️ Production Focus", "🔧 Operational Focus"])

                with tab1:
                    # All parameters chart
                    chart_data = chart_df[available_cols].head(50)  # Last 50 records
                    st.line_chart(chart_data)

                with tab2:
                    # Production-focused charts
                    prod_cols = [col for col in ["Oil volume", "Water cut", "Gas volume"] if col in available_cols]
                    if prod_cols:
                        prod_data = chart_df[prod_cols].head(50)
                        st.line_chart(prod_data)

                with tab3:
                    # Operational-focused charts
                    ops_cols = [col for col in ["Reservoir pressure", "Dynamic level", "Working hours"] if
                                col in available_cols]
                    if ops_cols:
                        ops_data = chart_df[ops_cols].head(50)
                        st.line_chart(ops_data)

            # Forecast Statistics
            if forecast_available > 0:
                st.markdown("### 🔮 Forecast Statistics")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Records with Forecasts", forecast_available)
                
                with col2:
                    forecast_percentage = (forecast_available / len(df)) * 100
                    st.metric("Forecast Coverage", f"{forecast_percentage:.1f}%")
                
                with col3:
                    # Count forecast alerts
                    total_forecast_alerts = sum(
                        len([alert for alert in record.get('alerts', []) if 'FORECAST' in alert])
                        for record in flattened_data
                    )
                    st.metric("Total Forecast Alerts", total_forecast_alerts)

        else:
            st.info("📊 No oil well data available yet. Start the sensor simulator to begin receiving data.")

    except Exception as e:
        st.error(f"❌ Error loading oil well data: {e}")

elif st.session_state.get("cleared", False):
    st.info("🧹 Dashboard data has been cleared. No data to display.")
else:
    st.info("🛢️ Oil Well Dashboard is not active. Click 'Start Dashboard' to begin monitoring.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <small>🛢️ Oil Well Production Monitoring Dashboard with AI Forecasting | Real-time data processing and predictive analysis</small>
</div>
""", unsafe_allow_html=True)