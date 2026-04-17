"""
Streamlit Dashboard — Parallel Geospatial Analytics Engine
Run with: streamlit run app.py
"""
import subprocess, time, os, json, glob, sys
from datetime import datetime
import pandas as pd
import folium
import altair as alt
import streamlit as st
from streamlit_folium import st_folium

st.set_page_config(page_title="Soil Analytics", layout="wide", page_icon="🌾")

HISTORY_FILE = "run_history.json"

REGIONS = {
    "🏜️ Arid Region (Rajasthan)":    {"file": "Arid_Region_Data.csv",       "center": [26.0, 73.0]},
    "🌴 Tropical Region (Kerala)":   {"file": "Tropical_Region_Data.csv",    "center": [10.0, 76.3]},
    "🌉 California Region (USA)":    {"file": "California_Region_Data.csv",  "center": [36.5, -120.2]},
    "🏔️ Kashmir Region (India)":     {"file": "Kashmir_Region_Data.csv",     "center": [33.8, 74.5]},
    "🌾 Tamil Nadu Region (India)":  {"file": "TamilNadu_Region_Data.csv",   "center": [10.5, 79.0]},
    "🚀 Arid Region 1M (Large Scale)": {"file": "Arid_Region_1M_Data.csv", "center": [26.0, 73.0]},
}
# Auto-detect any future *_Region_Data.csv files
for f in sorted(glob.glob("*_Region_Data.csv")):
    if f not in [v["file"] for v in REGIONS.values()]:
        label = "📍 " + f.replace("_Data.csv", "").replace("_", " ")
        REGIONS[label] = {"file": f, "center": [20.0, 78.0]}

# Filter REGIONS to only include those where the file actually exists
REGIONS = {k: v for k, v in REGIONS.items() if os.path.exists(v["file"])}
if not REGIONS:
    REGIONS["⚠️ No Data Files Found"] = {"file": "missing.csv", "center": [0.0, 0.0]}

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    body, .main { background-color: transparent; }
    .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
    h1, h2, h3, h4 { color: #f8fafc !important; font-weight: 800 !important; }
    .section-box {
        background: white; border-radius: 14px; padding: 24px;
        margin-bottom: 22px; box-shadow: 0 4px 15px rgba(0,0,0,0.06); border: 1px solid #eee;
        color: #333;
    }
    .section-heading { 
        font-size: 20px; 
        font-weight: 800; 
        color: #1e3a5f !important; 
        margin-bottom: 8px; 
        display: flex; 
        align-items: center; 
        gap: 10px;
        text-shadow: 0 1px 2px rgba(0,0,0,0.1);
    }
    .section-desc { font-size: 13px; color: #666; margin-bottom: 18px; line-height: 1.6; }
    .metric-card {
        background: white; border-radius: 14px; padding: 22px 18px;
        text-align: center; box-shadow: 0 3px 12px rgba(0,0,0,0.07); height: 100%;
        border: 1px solid #eee; transition: transform 0.2s ease;
    }
    .metric-card:hover { transform: translateY(-3px); }
    .metric-label { font-size: 12px; font-weight: 700; color: #aaa; letter-spacing: 0.8px; text-transform: uppercase; }
    .metric-value { font-size: 34px; font-weight: 800; margin: 6px 0 4px; }
    .metric-sub { font-size: 12px; color: #888; }
    .history-row {
        background: rgba(255, 255, 255, 0.05); border-left: 4px solid #3b82f6; border-radius: 8px;
        padding: 10px 16px; margin-bottom: 8px; font-size: 13px; color: #f8fafc;
    }
    .history-row b { color: #60a5fa; }
    .legend-dot { display: inline-block; width: 11px; height: 11px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
</style>
""", unsafe_allow_html=True)

# ── History helpers ───────────────────────────────────────────────────────────
def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE) as f:
            return json.load(f)
    return []

def save_history(entry):
    history = load_history()
    history.append(entry)
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)

# ── Page Header ───────────────────────────────────────────────────────────────
st.markdown("# 🌾 Agricultural Risk Monitor")

st.divider()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🌍 Select Region")
    selected_region = st.selectbox("Select Region", list(REGIONS.keys()), label_visibility="collapsed")
    region_cfg  = REGIONS[selected_region]
    DATA_FILE   = region_cfg["file"]
    RESULT_FILE = DATA_FILE.replace("_Data.csv", "_results.csv")
    
    if not os.path.exists(DATA_FILE):
        st.error(f"❌ Data file `{DATA_FILE}` is missing from the project folder. Please add it to view this region.")
        st.stop()
        
    total_rows  = sum(1 for _ in open(DATA_FILE)) - 1
    st.caption(f"📄 `{DATA_FILE}` — {total_rows:,} rows")

    st.divider()
    st.markdown("## ⚙️ Run Controls")
    
    # Parallel mode selection
    parallel_mode = st.radio(
        "Parallel Processing Mode",
        ["Data Parallel (Scatter/Gather)", "Pipeline Parallel (Sequential Stages)"],
        help="Data Parallel: All workers do same task on different data chunks.\n\nPipeline Parallel: Different workers do different tasks in sequence."
    )
    
    n_workers = st.slider("MPI Workers (CPU Cores)", 1, 8, 4)
    if parallel_mode == "Pipeline Parallel (Sequential Stages)" and n_workers < 4:
        st.warning("⚠️ Pipeline mode requires at least 4 workers. Adjusting to 4.")
        n_workers = 4
    
    st.caption(f"Using **{n_workers} core(s)** — {parallel_mode.split(' ')[0].lower()} parallelism")
    run_clicked = st.button("▶ Run Parallel Analysis", use_container_width=True, type="primary")

    st.divider()
    st.markdown("## 🗺️ Map Legend")
    for color, label in [("#e74c3c","Drought Risk"),("#f39c12","Water Stress"),
                          ("#27ae60","Healthy Vegetation"),("#3498db","Moderate")]:
        st.markdown(f'<span class="legend-dot" style="background:{color}"></span> {label}',
                    unsafe_allow_html=True)

    st.divider()
    st.markdown("## 📋 Run History")
    history = load_history()
    if not history:
        st.caption("No runs yet. Click Run to start.")
    else:
        for h in reversed(history[-8:]):
            fastest = min(r["time_sec"] for r in history if r["workers"] == h["workers"] and r.get("region") == h.get("region") and r.get("mode") == h.get("mode"))
            badge = " 🏆" if h["time_sec"] == fastest else ""
            mode_badge = f" 🔄" if h.get("mode") == "Pipeline" else " 📦"
            st.markdown(
                f'<div class="history-row">'
                f'🕐 <b>{h["timestamp"]}</b><br>'
                f'📍 {h.get("region","—")}<br>'
                f'Workers: <b>{h["workers"]}</b> &nbsp;|&nbsp; '
                f'Time: <b>{h["time_sec"]}s</b>{badge}<br>'
                f'Mode: <b>{h.get("mode", "Data Parallel")}</b>{mode_badge} &nbsp;|&nbsp; '
                f'Rows: {h["rows"]:,}'
                f'</div>',
                unsafe_allow_html=True
            )

# ── Run MPI ───────────────────────────────────────────────────────────────────
if run_clicked:
    # Determine which process file to use
    process_file = "process_pipeline.py" if "Pipeline" in parallel_mode else "process.py"
    
    # ── Live Worker Heartbeat ──────────────────────────────────────────────────
    heartbeat_placeholder = st.empty()
    colors_hb = ["#c0392b","#1a5276","#1e8449","#b7770d","#6c3483","#117a65","#784212","#2c3e50"]
    if "Pipeline" in parallel_mode:
        # Pipeline mode - show different tasks per worker
        pipeline_tasks = ["Coordinator", "Vegetation (NDVI+SAVI)", "Water (LSWI+MSI)", "Soil+Risk (BSI+Alerts)"]
        worker_cards = "".join([
            f"""<div style='display:inline-block;margin:6px;text-align:center;width:140px'>
                <div style='width:52px;height:52px;border-radius:50%;background:{colors_hb[i % 8]};
                    margin:0 auto;display:flex;align-items:center;justify-content:center;
                    font-size:22px;animation:pulse 1.2s ease-in-out {i*0.3:.2f}s infinite;'>⚙️</div>
                <div style='font-size:10px;font-weight:700;color:#ffffff;margin-top:6px'>Worker {i}<br>
                <span style='font-size:9px;color:#ccc'>{pipeline_tasks[i] if i < len(pipeline_tasks) else 'Extra'}</span><br>
                <span style='color:#27ae60;font-size:10px'>● ACTIVE</span></div></div>"""
            for i in range(n_workers)
        ])
        mode_desc = f"🚀 Pipeline Processing — {n_workers} Workers in Sequential Stages"
        flow_desc = "Data flows: Worker 0 → Worker 1 → Worker 2 → Worker 3 → Worker 0"
    else:
        # Data parallel mode - all workers do same task
        worker_cards = "".join([
            f"""<div style='display:inline-block;margin:6px;text-align:center;width:80px'>
                <div style='width:52px;height:52px;border-radius:50%;background:{colors_hb[i % 8]};
                    margin:0 auto;display:flex;align-items:center;justify-content:center;
                    font-size:22px;animation:pulse 1.2s ease-in-out {i*0.15:.2f}s infinite;'>⚙️</div>
                <div style='font-size:11px;font-weight:700;color:#ffffff;margin-top:6px'>Worker {i}<br>
                <span style='color:#27ae60;font-size:10px'>● ACTIVE</span></div></div>"""
            for i in range(n_workers)
        ])
        mode_desc = f"🚀 Data Parallel Processing — {n_workers} Workers on Different Data Chunks"
        flow_desc = f"Processing {total_rows:,} rows split across {n_workers} workers simultaneously"

    heartbeat_placeholder.markdown(f"""
    <style>
    @keyframes pulse {{
        0%,100% {{ transform: scale(1); opacity:1; box-shadow: 0 0 0 0 rgba(255,255,255,0.4); }}
        50% {{ transform: scale(1.18); opacity:0.85; box-shadow: 0 0 0 10px rgba(255,255,255,0); }}
    }}
    </style>
    <div style='background:#0d1b2a;border-radius:14px;padding:18px 24px;margin-bottom:12px'>
        <div style='color:white;font-size:15px;font-weight:800;margin-bottom:12px'>
            {mode_desc} on {selected_region}
        </div>
        <div>{worker_cards}</div>
        <div style='color:#aaa;font-size:12px;margin-top:12px'>⏳ {flow_desc}</div>
    </div>
    """, unsafe_allow_html=True)

    t0 = time.time()
    result = subprocess.run(
        ["mpiexec", "-n", str(n_workers),
         sys.executable, process_file, DATA_FILE, RESULT_FILE],

        capture_output=True, text=True
    )
    elapsed = round(time.time() - t0, 2)
    heartbeat_placeholder.empty()

    if result.returncode == 0:
        save_history({
            "timestamp": datetime.now().strftime("%d %b %Y, %I:%M %p"),
            "region": selected_region,
            "workers": n_workers,
            "time_sec": elapsed,
            "rows": total_rows,
            "mode": "Pipeline" if "Pipeline" in parallel_mode else "Data Parallel"
        })
        mode_name = "pipeline" if "Pipeline" in parallel_mode else "data parallel"
        st.success(f"✅ Done! Processed {total_rows:,} rows in **{elapsed}s** using **{n_workers} core(s)** ({mode_name})")
        st.rerun()
    else:
        st.error("MPI failed")
        st.code(result.stderr)

# ── Load results ──────────────────────────────────────────────────────────────
if not os.path.exists(RESULT_FILE):
    st.info(f"👈 Select **{selected_region}** and click **Run Parallel Analysis** to generate results.")
    st.stop()

df = pd.read_csv(RESULT_FILE)


st.divider()


# ── Tabs ──────────────────────────────────────────────────────────────────────
st.markdown(f"### 📍 Showing results for: {selected_region} — {total_rows:,} farm points")
tab1, tab2, tab3 = st.tabs(["🌾 Agricultural Dashboard", "🧪 Soil Analysis Report", "🔍 Region Comparison"])

with tab2:
    SOIL_REPORT_FILE = RESULT_FILE.replace("_results.csv", "_soil_report.json")
    col_run1, col_run2 = st.columns([2, 3])
    with col_run1:
        soil_workers = st.slider("Workers for Soil Analysis", 1, 8, 4, key="soil_workers")
    with col_run2:
        st.markdown("<br>", unsafe_allow_html=True)
        run_soil = st.button("🧪 Generate Soil Report (Parallel)", type="primary", use_container_width=True)

    if run_soil:
        with st.spinner(f"⚡ Running parallel soil analysis with {soil_workers} MPI workers..."):
            soil_result = subprocess.run(
                ["mpiexec", "-n", str(soil_workers),
                 sys.executable, "soil_report.py", RESULT_FILE, SOIL_REPORT_FILE],
                capture_output=True, text=True
            )
        if soil_result.returncode == 0:
            st.success(f"✅ Soil report generated using {soil_workers} parallel workers!")
            st.rerun()
        else:
            st.error("Soil analysis failed")
            st.code(soil_result.stderr)

    if not os.path.exists(SOIL_REPORT_FILE):
        st.info("👆 Click **Generate Soil Report** above to run parallel soil analysis for this region.")
    else:
        with open(SOIL_REPORT_FILE) as f:
            report = json.load(f)

        # Report Header
        st.markdown(f"""
        <div style='background:linear-gradient(135deg,#0d1b2a,#1a3a5c);border-radius:16px;padding:28px 32px;margin-bottom:24px;color:white'>
            <div style='display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px'>
                <div>
                    <div style='font-size:22px;font-weight:900;letter-spacing:1px'>🌍 Soil Analysis Report</div>
                    <div style='font-size:13px;opacity:0.7;margin-top:4px'>Satellite-derived soil intelligence · MPI parallel computing</div>
                </div>
                <div style='text-align:right'>
                    <div style='font-size:12px;opacity:0.6'>Generated</div>
                    <div style='font-size:14px;font-weight:700'>{datetime.now().strftime('%d %b %Y')}</div>
                </div>
            </div>
            <hr style='border-color:rgba(255,255,255,0.15);margin:16px 0'>
            <div style='display:flex;gap:32px;flex-wrap:wrap'>
                <div><span style='opacity:0.6;font-size:11px'>REGION</span><br><b style='font-size:14px'>{selected_region}</b></div>
                <div><span style='opacity:0.6;font-size:11px'>PIXELS ANALYZED</span><br><b style='font-size:14px'>{report['total_pixels']:,}</b></div>
                <div><span style='opacity:0.6;font-size:11px'>MPI WORKERS</span><br><b style='font-size:14px'>{report['workers']}</b></div>
                <div><span style='opacity:0.6;font-size:11px'>PROCESSING TIME</span><br><b style='font-size:14px'>{report['processing_time']}s</b></div>
                <div><span style='opacity:0.6;font-size:11px'>DATA SOURCE</span><br><b style='font-size:14px'>Sentinel-2 (10m)</b></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Soil Parameter Cards
        params = [
            ("nitrogen",   "🌿", "Nitrogen",            "N",   "kg/ha"),
            ("phosphorus", "🔶", "Phosphorus",          "P",   "kg/ha"),
            ("potassium",  "🟡", "Potassium",           "K",   "kg/ha"),
            ("soc",        "🪨", "Soil Organic Carbon", "SOC", "%"),
            ("ph",         "🧪", "Soil pH",             "pH",  ""),
        ]

        for key, icon, label, short, unit in params:
            p = report[key]
            val_display = f"{p['value']} {unit}".strip()
            st.markdown(f"""
            <div style='background:white;border-radius:14px;padding:22px 28px;margin-bottom:16px;
                        box-shadow:0 3px 16px rgba(0,0,0,0.08);border-left:5px solid {p["color"]}'>
                <div style='display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:16px'>
                    <div style='flex:1;min-width:180px'>
                        <div style='font-size:12px;color:#888;font-weight:700;text-transform:uppercase;letter-spacing:0.8px'>{icon} {label} ({short})</div>
                        <div style='font-size:38px;font-weight:900;color:{p["color"]};margin:6px 0;line-height:1'>{val_display}</div>
                        <div style='display:inline-block;background:{p["color"]}22;color:{p["color"]};border-radius:20px;
                                    padding:4px 14px;font-size:12px;font-weight:700'>{p["status"]}</div>
                    </div>
                    <div style='flex:2;min-width:240px;border-left:1px solid #eee;padding-left:20px'>
                        <div style='font-size:12px;color:#aaa;margin-bottom:8px'>Ideal Range: <b style="color:#555">{p["ideal"]} {unit}</b></div>
                        <div style='font-size:13px;color:#444;line-height:1.7'>💡 {p["remark"]}</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # Overview summary cards
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-heading">📊 Soil Health Overview</div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        overview_cols = st.columns(5)
        for col, (key, icon, label, short, unit) in zip(overview_cols, params):
            p = report[key]
            with col:
                st.markdown(f"""
                <div style='background:white;border-radius:12px;padding:18px 12px;text-align:center;
                            box-shadow:0 2px 10px rgba(0,0,0,0.07);border-top:4px solid {p["color"]}'>
                    <div style='font-size:26px'>{icon}</div>
                    <div style='font-size:11px;color:#888;font-weight:700;margin:6px 0;text-transform:uppercase'>{short}</div>
                    <div style='font-size:22px;font-weight:900;color:{p["color"]}'>{p['value']}</div>
                    <div style='font-size:10px;color:#bbb'>{unit if unit else 'index'}</div>
                    <div style='font-size:11px;font-weight:700;color:{p["color"]};margin-top:8px;
                                background:{p["color"]}15;border-radius:10px;padding:3px 0'>{p["status"]}</div>
                </div>
                """, unsafe_allow_html=True)

        # ── Crop Recommendation ─────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="section-heading">🌾 Crop Recommendation</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-desc">Based on satellite indices and soil health analysis for this region.</div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        # Read NDVI, LSWI, BSI averages from results
        ndvi_avg = float(df["NDVI"].mean()) if "NDVI" in df.columns else 0.3
        lswi_avg = float(df["LSWI"].mean()) if "LSWI" in df.columns else 0.2
        bsi_avg  = float(df["BSI"].mean())  if "BSI"  in df.columns else 0.0

        n_status  = report["nitrogen"]["status"]
        p_status  = report["phosphorus"]["status"]
        k_status  = report["potassium"]["status"]
        ph_val    = report["ph"]["value"]
        ph_status = report["ph"]["status"]

        # Build recommendations based on conditions
        recommendations = []

        if ndvi_avg > 0.4 and n_status == "Ideal":
            recommendations.append({
                "crop": "🌾 Rice & Wheat",
                "status": "✅ Highly Suitable",
                "color": "#27ae60",
                "reason": f"NDVI {ndvi_avg:.2f} shows healthy vegetation. Nitrogen is ideal for cereal crops.",
                "action": "Maintain current irrigation and fertilization schedule."
            })

        if ndvi_avg > 0.35 and k_status == "Ideal" and lswi_avg > 0.3:
            recommendations.append({
                "crop": "🍌 Sugarcane & Banana",
                "status": "✅ Suitable",
                "color": "#2ecc71",
                "reason": f"Good moisture (LSWI {lswi_avg:.2f}) and ideal potassium support water-intensive crops.",
                "action": "Ensure consistent water supply. Monitor for pest infestation."
            })

        if ndvi_avg > 0.3 and p_status == "Ideal":
            recommendations.append({
                "crop": "🌱 Vegetables & Pulses",
                "status": "✅ Suitable",
                "color": "#16a085",
                "reason": f"Phosphorus is ideal supporting root development. Moderate vegetation cover.",
                "action": "Good for short-duration crops. Rotate with legumes to maintain soil health."
            })

        if bsi_avg > 0.1 and ph_val > 7.5:
            recommendations.append({
                "crop": "🌵 Drought-Resistant Crops (Millets, Sorghum)",
                "status": "⚠️ Conditionally Suitable",
                "color": "#f39c12",
                "reason": f"High bare soil (BSI {bsi_avg:.2f}) and alkaline pH {ph_val} indicate dry conditions.",
                "action": "Apply lime to reduce alkalinity. Choose drought-tolerant varieties."
            })

        if ndvi_avg < 0.2 and n_status == "Low":
            recommendations.append({
                "crop": "🌿 Any Crop — After Treatment",
                "status": "🔴 Not Suitable Currently",
                "color": "#e74c3c",
                "reason": f"NDVI {ndvi_avg:.2f} is critically low. Nitrogen deficiency detected.",
                "action": "Apply Urea fertilizer at split intervals. Start drip irrigation immediately before sowing."
            })

        if lswi_avg < 0.1 and bsi_avg > 0.2:
            recommendations.append({
                "crop": "🌵 Cactus / Xerophytes Only",
                "status": "🔴 Severe Drought Conditions",
                "color": "#c0392b",
                "reason": f"Extremely low water content (LSWI {lswi_avg:.2f}) and high bare soil.",
                "action": "Land needs water harvesting and soil restoration before any crop cultivation."
            })

        if ndvi_avg >= 0.2 and ndvi_avg <= 0.4 and n_status in ["Low", "Ideal"]:
            recommendations.append({
                "crop": "🌼 Cotton & Groundnut",
                "status": "⚠️ Moderately Suitable",
                "color": "#e67e22",
                "reason": f"Moderate vegetation (NDVI {ndvi_avg:.2f}). Semi-arid crops can adapt to these conditions.",
                "action": "Apply balanced NPK fertilizer. Use mulching to retain soil moisture."
            })

        # If no specific recommendation matched
        if not recommendations:
            recommendations.append({
                "crop": "🌻 General Farming",
                "status": "⚠️ Moderate Conditions",
                "color": "#3498db",
                "reason": "Soil and vegetation conditions are moderate.",
                "action": "Conduct detailed soil testing before selecting specific crops."
            })

        # Display recommendation cards
        for rec in recommendations:
            st.markdown(f"""
            <div style='background:white;border-radius:14px;padding:20px 26px;margin-bottom:14px;
                        box-shadow:0 3px 14px rgba(0,0,0,0.08);border-left:5px solid {rec["color"]}'>
                <div style='display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px'>
                    <div style='flex:1;min-width:200px'>
                        <div style='font-size:18px;font-weight:900;color:{rec["color"]}'>{rec["crop"]}</div>
                        <div style='display:inline-block;background:{rec["color"]}22;color:{rec["color"]};
                                    border-radius:20px;padding:4px 14px;font-size:12px;font-weight:700;margin-top:6px'>
                            {rec["status"]}</div>
                    </div>
                    <div style='flex:2;min-width:240px;border-left:1px solid #eee;padding-left:20px'>
                        <div style='font-size:13px;color:#555;margin-bottom:6px'>📊 <b>Why:</b> {rec["reason"]}</div>
                        <div style='font-size:13px;color:#27ae60'>💡 <b>Action:</b> {rec["action"]}</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)


with tab3:
    st.markdown("<br>", unsafe_allow_html=True)

    # Region selectors
    available_regions = {k: v for k, v in REGIONS.items() if os.path.exists(v["file"])}
    region_keys = list(available_regions.keys())

    col_r1, col_r2, col_r3 = st.columns([2, 2, 1])
    with col_r1:
        region1 = st.selectbox("🌍 Region 1", region_keys, index=0, key="comp_r1")
    with col_r2:
        region2 = st.selectbox("🌍 Region 2", region_keys, index=1, key="comp_r2")
    with col_r3:
        comp_workers = st.slider("Workers", 1, 8, 4, key="comp_workers")

    st.markdown("<br>", unsafe_allow_html=True)
    compare_clicked = st.button("▶ Compare Regions", type="primary", use_container_width=True, key="compare_btn")

    if compare_clicked:
        if region1 == region2:
            st.warning("⚠️ Please select two different regions.")
        else:
            r1_file   = available_regions[region1]["file"]
            r2_file   = available_regions[region2]["file"]
            r1_result = r1_file.replace("_Data.csv", "_results.csv")
            r2_result = r2_file.replace("_Data.csv", "_results.csv")
            r1_soil   = r1_file.replace("_Data.csv", "_soil_report.json")
            r2_soil   = r2_file.replace("_Data.csv", "_soil_report.json")

            # Run MPI for both regions
            with st.spinner(f"⚡ Running parallel analysis for both regions using {comp_workers} workers..."):
                import concurrent.futures
                def run_mpi(data_file, result_file):
                    return subprocess.run(
                        ["/opt/homebrew/bin/mpiexec", "-n", str(comp_workers),
                         "/opt/anaconda3/bin/python3", "process.py", data_file, result_file],
                        capture_output=True, text=True
                    )
                def run_soil(result_file, soil_file):
                    return subprocess.run(
                        ["/opt/homebrew/bin/mpiexec", "-n", str(comp_workers),
                         "/opt/anaconda3/bin/python3", "soil_report.py", result_file, soil_file],
                        capture_output=True, text=True
                    )
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    f1 = executor.submit(run_mpi, r1_file, r1_result)
                    f2 = executor.submit(run_mpi, r2_file, r2_result)
                    f1.result(); f2.result()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    s1 = executor.submit(run_soil, r1_result, r1_soil)
                    s2 = executor.submit(run_soil, r2_result, r2_soil)
                    s1.result(); s2.result()
            st.success("✅ Comparison complete!")
            st.rerun()

    # Show comparison if both results exist
    r1_file   = available_regions[region1]["file"]
    r2_file   = available_regions[region2]["file"]
    r1_result = r1_file.replace("_Data.csv", "_results.csv")
    r2_result = r2_file.replace("_Data.csv", "_results.csv")
    r1_soil   = r1_file.replace("_Data.csv", "_soil_report.json")
    r2_soil   = r2_file.replace("_Data.csv", "_soil_report.json")

    if os.path.exists(r1_result) and os.path.exists(r2_result) and os.path.exists(r1_soil) and os.path.exists(r2_soil):
        df1 = pd.read_csv(r1_result)
        df2 = pd.read_csv(r2_result)
        with open(r1_soil) as f: soil1 = json.load(f)
        with open(r2_soil) as f: soil2 = json.load(f)

        # ── Comparison Header ─────────────────────────────────────────────────
        st.markdown(f"""
        <div style='background:linear-gradient(135deg,#0d1b2a,#1a3a5c);border-radius:16px;
                    padding:24px 32px;margin-bottom:24px;color:white;text-align:center'>
            <div style='font-size:20px;font-weight:900'>🔍 Region Comparison Report</div>
            <div style='font-size:15px;margin-top:10px;opacity:0.9'>
                <b>{region1}</b> &nbsp;⚔️&nbsp; <b>{region2}</b>
            </div>
            <div style='font-size:12px;opacity:0.6;margin-top:6px'>Processed using {comp_workers} MPI workers</div>
        </div>
        """, unsafe_allow_html=True)

        # ── Side by Side Metrics ──────────────────────────────────────────────
        def pct(df, col): return round(df[col].sum() / len(df) * 100, 1)

        metrics = [
            ("NDVI Average",       round(df1["NDVI"].mean(),3),      round(df2["NDVI"].mean(),3),      "🌱",  True),
            ("Healthy %",          pct(df1,"Vegetation_Healthy"),     pct(df2,"Vegetation_Healthy"),     "🟢",  True),
            ("Drought Risk %",     pct(df1,"Drought_Risk"),           pct(df2,"Drought_Risk"),           "🔴",  False),
            ("Water Stress %",     pct(df1,"Water_Stress"),           pct(df2,"Water_Stress"),           "🟠",  False),
            ("Nitrogen (kg/ha)",   soil1["nitrogen"]["value"],        soil2["nitrogen"]["value"],        "🌿",  True),
            ("Phosphorus (kg/ha)", soil1["phosphorus"]["value"],      soil2["phosphorus"]["value"],      "🔶",  True),
            ("Potassium (kg/ha)",  soil1["potassium"]["value"],       soil2["potassium"]["value"],       "🟡",  True),
            ("Soil pH",            soil1["ph"]["value"],              soil2["ph"]["value"],              "🧪",  None),
        ]

        # Header row
        h1, h2, h3 = st.columns([2, 1, 1])
        h1.markdown(f"**Metric**")
        h2.markdown(f"**{region1.split('(')[0].strip()}**")
        h3.markdown(f"**{region2.split('(')[0].strip()}**")
        st.markdown("---")

        r1_score = 0
        r2_score = 0

        for label, v1, v2, icon, higher_better in metrics:
            c1, c2, c3 = st.columns([2, 1, 1])
            if higher_better is True:
                win1 = v1 > v2
                win2 = v2 > v1
            elif higher_better is False:
                win1 = v1 < v2
                win2 = v2 < v1
            else:
                # pH — closer to 7 is better
                win1 = abs(v1 - 7) < abs(v2 - 7)
                win2 = abs(v2 - 7) < abs(v1 - 7)

            if win1: r1_score += 1
            if win2: r2_score += 1

            badge1 = "🏆" if win1 else ""
            badge2 = "🏆" if win2 else ""
            color1 = "#27ae60" if win1 else "#e74c3c" if win2 else "#888"
            color2 = "#27ae60" if win2 else "#e74c3c" if win1 else "#888"

            c1.markdown(f"{icon} **{label}**")
            c2.markdown(f"<span style='color:{color1};font-weight:700;font-size:16px'>{v1} {badge1}</span>", unsafe_allow_html=True)
            c3.markdown(f"<span style='color:{color2};font-weight:700;font-size:16px'>{v2} {badge2}</span>", unsafe_allow_html=True)

        st.markdown("---")

        # ── Winner Banner ─────────────────────────────────────────────────────
        if r1_score > r2_score:
            winner = region1.split('(')[0].strip()
            loser  = region2.split('(')[0].strip()
            win_color = "#27ae60"
        elif r2_score > r1_score:
            winner = region2.split('(')[0].strip()
            loser  = region1.split('(')[0].strip()
            win_color = "#27ae60"
        else:
            winner = None
            win_color = "#3498db"

        if winner:
            st.markdown(f"""
            <div style='background:{win_color}15;border:2px solid {win_color};border-radius:14px;
                        padding:20px;text-align:center;margin-top:10px'>
                <div style='font-size:32px'>🏆</div>
                <div style='font-size:20px;font-weight:900;color:{win_color};margin-top:6px'>
                    {winner} is Better for Farming
                </div>
                <div style='font-size:13px;color:#555;margin-top:8px'>
                    Won {max(r1_score,r2_score)} out of {len(metrics)} categories
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style='background:#3498db15;border:2px solid #3498db;border-radius:14px;
                        padding:20px;text-align:center;margin-top:10px'>
                <div style='font-size:32px'>🤝</div>
                <div style='font-size:20px;font-weight:900;color:#3498db;margin-top:6px'>Both Regions are Equal</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("👆 Select two regions and click **Compare Regions** to start analysis.")

with tab1:
    # ── Dashboard Header ────────────────────────────────────────────────────────
    st.markdown(f"""
    <div style='background:linear-gradient(135deg,#1e3a5f,#0d1b2a);border-radius:16px;padding:28px 32px;margin-bottom:24px;color:white'>
        <div style='display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px'>
            <div>
                <div style='font-size:24px;font-weight:900;letter-spacing:1px'>🌾 Agricultural Risk Dashboard</div>
                <div style='font-size:13px;opacity:0.8;margin-top:4px'></div>
            </div>
            <div style='text-align:right'>
                <div style='font-size:11px;opacity:0.7;text-transform:uppercase;letter-spacing:1px'>Active Mode</div>
                <div style='font-size:14px;font-weight:700;background:rgba(255,255,255,0.15);padding:4px 12px;border-radius:20px;display:inline-block;margin-top:4px'>
                    {parallel_mode.split(' ')[0]} Parallel
                </div>
            </div>
        </div>
        <hr style='border-color:rgba(255,255,255,0.15);margin:16px 0'>
        <div style='display:flex;gap:32px;flex-wrap:wrap'>
            <div><span style='opacity:0.7;font-size:11px;text-transform:uppercase'>Selected Region</span><br><b style='font-size:15px'>{selected_region}</b></div>
            <div><span style='opacity:0.7;font-size:11px;text-transform:uppercase'>MPI Workers</span><br><b style='font-size:15px'>{n_workers} Cores</b></div>
            <div><span style='opacity:0.7;font-size:11px;text-transform:uppercase'>Data Points</span><br><b style='font-size:15px'>{total_rows:,} pixels</b></div>
            <div><span style='opacity:0.7;font-size:11px;text-transform:uppercase'>Satellite Source</span><br><b style='font-size:15px'>Sentinel-2 (High Res)</b></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── HOW IT WORKS visualizer ───────────────────────────────────────────────────
    with st.expander("🔍 How does Parallel Processing work here? — Click to see Step-by-Step", expanded=False):
        if "viz_step" not in st.session_state:
            st.session_state.viz_step = 0

        # Data initialization for the visualizer
        raw = pd.read_csv(DATA_FILE).head(500 * n_workers)
        viz_workers = n_workers
        chunk_size  = len(raw) // viz_workers
        chunks = [raw.iloc[i*chunk_size:(i+1)*chunk_size].reset_index(drop=True) for i in range(viz_workers)]
        colors = ["#c0392b","#1a5276","#1e8449","#b7770d","#6c3483","#117a65","#784212","#2c3e50"]

        # Walkthrough Header
        st.markdown(f"""
            <div style='background: linear-gradient(135deg, #1e3a5f 0%, #0d1b2a 100%); padding: 25px; border-radius: 12px; margin-bottom: 25px;'>
                <div style='display: flex; align-items: center; gap: 15px;'>
                    <div style='font-size: 40px;'>🧠</div>
                    <div>
                        <div style='color: white; font-size: 22px; font-weight: 800;'>MPI Pipeline Walkthrough</div>
                        <div style='color: rgba(255,255,255,0.7); font-size: 14px;'>Step-by-step visual of the parallel satellite processing engine</div>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        # Progress Stepper
        step = st.session_state.viz_step
        cols = st.columns(5)
        step_labels = ["LOAD", "SCATTER", "COMPUTE", "GATHER", "SAVE"]
        step_icons = ["⏳", "✂️", "⚙️", "📦", "✅"]
        
        for i, (col, label, icon) in enumerate(zip(cols, step_labels, step_icons)):
            is_active = (i == step)
            is_done = (i < step)
            bg = "#27ae60" if is_done else "#2980b9" if is_active else "#f8fafc"
            txt = "white" if (is_active or is_done) else "#94a3b8"
            border = "2px solid #2980b9" if is_active else "none"
            with col:
                st.markdown(f"""
                    <div style='text-align:center; padding: 10px 5px; background: {bg}; border-radius: 10px; border: {border}; color: {txt}; transition: all 0.3s ease;'>
                        <div style='font-size: 16px; margin-bottom: 4px;'>{icon}</div>
                        <div style='font-size: 10px; font-weight: 800; text-transform: uppercase; letter-spacing: 1px;'>{label}</div>
                    </div>
                """, unsafe_allow_html=True)

        st.markdown("<div style='margin-bottom: 30px;'></div>", unsafe_allow_html=True)
        
        # Navigation
        b1, b2, b3 = st.columns([1, 1, 4])
        if b1.button("◀ Back", key="prev_step") and st.session_state.viz_step > 0:
            st.session_state.viz_step -= 1
        if b2.button("Continue ▶", key="next_step", type="primary") and st.session_state.viz_step < 4:
            st.session_state.viz_step += 1
        if b3.button("↺ Reset Walkthrough", key="restart_step"):
            st.session_state.viz_step = 0

        st.markdown("<hr style='border: 1px solid rgba(0,0,0,0.05); margin-bottom: 25px;'>", unsafe_allow_html=True)

        if step == 0:
            st.markdown(f"""
                <div style='background:linear-gradient(135deg, #1e3a5f, #0d1b2a);color:white !important;border-radius:15px;padding:30px;margin-bottom:25px;border-left:8px solid #2980b9;box-shadow: 0 10px 30px rgba(0,0,0,0.2)'>
                    <div style='font-size:22px;font-weight:900;color:white !important;margin-bottom:15px;display:flex;align-items:center;gap:12px'>
                        <span style='background:#2980b9;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px'>⏳</span>
                        Parallel Engine: Data Injection (Master)
                    </div>
                    <div style='color:rgba(255,255,255,0.85) !important;font-size:15px;line-height:1.7'>
                        The Master process (Rank 0) acts as the logic controller. It initializes the environment and anchors the dataset.<br><br>
                        📁 File: <b style='color:#3498db'>{DATA_FILE}</b><br>
                        📊 Magnitude: <b style='color:#3498db'>{total_rows:,}</b> farm coordinates detected.
                    </div>
                </div>
            """, unsafe_allow_html=True)
            st.caption("👇 Data Stream Preview (Master Buffer)")
            st.dataframe(raw[["B4","B8","B11"]].head(6), use_container_width=True)

        elif step == 1:
            st.markdown(f"""<div style='background:linear-gradient(135deg, #2c3e50, #0d1b2a);color:white !important;border-radius:15px;padding:30px;margin-bottom:25px;border-left:8px solid #3498db;box-shadow: 0 10px 30px rgba(0,0,0,0.2)'>
                <div style='font-size:22px;font-weight:900;color:white !important;margin-bottom:15px;display:flex;align-items:center;gap:12px'>
                    <span style='background:#3498db;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px'>✂️</span>
                    MPI_Scatter: Data Distribution
                </div>
                <div style='color:rgba(255,255,255,0.85) !important;font-size:15px;line-height:1.7'>
                    The Master process divides the <b style='color:#3498db'>{total_rows:,}</b> rows into <b style='color:#3498db'>{viz_workers}</b> equal packages.<br>
                    Each worker receives exactly <b style='color:#3498db'>{total_rows // viz_workers:,}</b> rows to handle.
                </div>
            </div>""", unsafe_allow_html=True)
            
            st.markdown("<div style='font-size:14px;font-weight:700;margin-bottom:15px;color:#2c3e50'>🖥️ Active Worker Nodes (MPI Ranks)</div>", unsafe_allow_html=True)
            cols = st.columns(viz_workers)
            for i, (col, chunk) in enumerate(zip(cols, chunks)):
                with col:
                    st.markdown(f"""
                        <div style='background:white; border-radius:12px; padding:15px; border:1px solid #eee; box-shadow:0 4px 6px rgba(0,0,0,0.05); text-align:center'>
                            <div style='font-size:24px;margin-bottom:10px'>📦</div>
                            <div style='color:#7f8c8d;font-size:11px;text-transform:uppercase;font-weight:800'>Rank {i}</div>
                            <div style='color:#2c3e50;font-size:16px;font-weight:800'>{len(chunk):,}</div>
                            <div style='color:#95a5a6;font-size:10px'>Rows assigned</div>
                        </div>
                    """, unsafe_allow_html=True)
                    st.dataframe(chunk[["B4","B8","B11"]].head(50), use_container_width=True, height=180)

        elif step == 2:
            st.markdown(f"""<div style='background:linear-gradient(135deg, #1b5e20, #0a2e12);color:white !important;border-radius:15px;padding:30px;margin-bottom:25px;border-left:8px solid #27ae60;box-shadow: 0 10px 30px rgba(0,0,0,0.2)'>
                <div style='font-size:22px;font-weight:900;color:white !important;margin-bottom:15px;display:flex;align-items:center;gap:12px'>
                    <span style='background:#27ae60;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px'>⚙️</span>
                    Parallel Execution: Computation Phase
                </div>
                <div style='color:rgba(255,255,255,0.85) !important;font-size:15px;line-height:1.7'>
                    This is where the MPI magic happens. All <b style='color:#27ae60'>{viz_workers}</b> workers compute the NDVI and LSWI indices <b style='color:white;padding:2px 6px;background:#27ae60;border-radius:4px'>at the same time</b>.<br><br>
                    No worker waits for another. This eliminates the "bottleneck" of sequential processing.
                </div>
            </div>""", unsafe_allow_html=True)
            
            st.markdown("<div style='font-size:14px;font-weight:700;margin-bottom:15px;color:#2c3e50'>⚙️ Computation Progress — Parallel Processing Nodes</div>", unsafe_allow_html=True)
            cols = st.columns(viz_workers)
            for i, (col, chunk) in enumerate(zip(cols, chunks)):
                with col:
                    st.markdown(f"""<div style='background:{colors[i]};color:white;border-radius:8px;
                        padding:10px;text-align:center;margin-bottom:8px'>
                        <b>Worker {i} ⚙️ Done</b><br>
                        <span style='font-size:11px;opacity:0.8'>(scrollable preview)</span>
                    </div>""", unsafe_allow_html=True)
                    # Simulated computation result for preview
                    ndvi_sim = (chunk["B8"] - chunk["B4"]) / (chunk["B8"] + chunk["B4"])
                    st.dataframe(pd.DataFrame({"NDVI": ndvi_sim.round(3)}).head(50), use_container_width=True, height=180)

        elif step == 3:
            st.markdown(f"""<div style='background:linear-gradient(135deg, #4b0082, #240041);color:white !important;border-radius:15px;padding:30px;margin-bottom:25px;border-left:8px solid #9b59b6;box-shadow: 0 10px 30px rgba(0,0,0,0.2)'>
                <div style='font-size:22px;font-weight:900;color:white !important;margin-bottom:15px;display:flex;align-items:center;gap:12px'>
                    <span style='background:#9b59b6;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px'>📦</span>
                    MPI_Gather: Result Consolidation
                </div>
                <div style='color:rgba(255,255,255,0.85) !important;font-size:15px;line-height:1.7'>
                    The Master process collects the results from all <b style='color:#9b59b6'>{viz_workers}</b> worker nodes.<br>
                    Data is stitched back together in the original sequence to create the final analysis table.
                </div>
            </div>""", unsafe_allow_html=True)
            
            st.markdown(f"**✅ Master Buffer: Successfully consolidated records from {viz_workers} nodes**")
            st.dataframe(raw.head(200), use_container_width=True, height=250)

        elif step == 4:
            st.markdown(f"""<div style='background:linear-gradient(135deg, #1b5e20, #0a2e12);color:white !important;border-radius:15px;padding:30px;margin-bottom:25px;border-left:8px solid #2ecc71;box-shadow: 0 10px 30px rgba(0,0,0,0.2)'>
                <div style='font-size:22px;font-weight:900;color:white !important;margin-bottom:15px;display:flex;align-items:center;gap:12px'>
                    <span style='background:#2ecc71;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px'>🎉</span>
                    Pipeline Complete: System Synchronization
                </div>
                <div style='color:rgba(255,255,255,0.85) !important;font-size:15px;line-height:1.7'>
                    Analysis complete. Results are committed to <b style='color:#2ecc71'>{RESULT_FILE}</b>.<br>
                    The dashboard widgets below are now reading the live engine output.
                </div>
            </div>""", unsafe_allow_html=True)

            st.markdown(f"""
                <div style='background:#d4edda; border-left:4px solid #28a745; padding:20px; border-radius:10px;'>
                    <div style='font-weight:800; color:#155724; margin-bottom:10px;'>🚀 SUCCESS RECAP</div>
                    <div style='color:#155724; font-size:14px; line-height:1.8'>
                        • 💻 <b>Master Node</b> synchronized with {viz_workers} workers.<br>
                        • 🛡️ <b>Integrity Check</b>: {total_rows:,} records processed without loss.<br>
                        • 📊 <b>Update Status</b>: Map, Charts, and Metrics are now **LIVE**.
                    </div>
                </div>
            """, unsafe_allow_html=True)

    # ── Metric Cards ──────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    cards = [
        (c1, "📍", "TOTAL POINTS", f"{len(df):,}", "#2c3e50", "Satellite pixels analyzed"),
        (c2, "🔴", "DROUGHT RISK", f"{df['Drought_Risk'].sum():,}", "#e74c3c", 
         f"{round(df['Drought_Risk'].sum()/len(df)*100,1)}% — NDVI < 0.2"),
        (c3, "🟠", "WATER STRESS", f"{df['Water_Stress'].sum():,}", "#f39c12", 
         f"{round(df['Water_Stress'].sum()/len(df)*100,1)}% — LSWI < 0.2"),
        (c4, "🟢", "HEALTHY", f"{df['Vegetation_Healthy'].sum():,}", "#27ae60", 
         f"{round(df['Vegetation_Healthy'].sum()/len(df)*100,1)}% — NDVI ≥ 0.4"),
    ]
    for col, icon, label, value, color, sub in cards:
        with col:
            st.markdown(f"""<div class="metric-card" style="border-top: 5px solid {color}">
                <div style='font-size:24px;margin-bottom:8px'>{icon}</div>
                <div class="metric-label">{label}</div>
                <div class="metric-value" style="color:{color}">{value}</div>
                <div class="metric-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    if "Bare_Soil" in df.columns:
        r1, r2, r3 = st.columns(3)
        extra_cards = [
            (r1, "🟤", "BARE SOIL", f"{df['Bare_Soil'].sum():,}", "#a04000", 
             f"{round(df['Bare_Soil'].sum()/len(df)*100,1)}% — BSI > 0"),
            (r2, "💧", "PLANT WATER STRESS", f"{df['Plant_Water_Stress'].sum():,}", "#1a5276", 
             f"{round(df['Plant_Water_Stress'].sum()/len(df)*100,1)}% — MSI > 1.0"),
            (r3, "🌱", "AVG SAVI", f"{df['SAVI'].mean():.3f}", "#1e8449", 
             "Soil-adjusted vegetation index"),
        ]
        for col, icon, label, value, color, sub in extra_cards:
            with col:
                st.markdown(f"""<div class="metric-card" style="border-top: 5px solid {color}">
                    <div style='font-size:24px;margin-bottom:8px'>{icon}</div>
                    <div class="metric-label">{label}</div>
                    <div class="metric-value" style="color:{color}">{value}</div>
                    <div class="metric-sub">{sub}</div>
                </div>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # ── Map ───────────────────────────────────────────────────────────────────────
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-heading">🗺️ Farm Risk Map — {selected_region}</div>', unsafe_allow_html=True)

    ALERT_COLOR = {"Drought":"red","Water Stress":"orange","Healthy":"green","Moderate":"blue"}
    sample = df.sample(min(2000, len(df)), random_state=42)
    m = folium.Map(location=[sample["lat"].mean(), sample["lon"].mean()],
                   zoom_start=9, tiles="CartoDB positron")
    for _, row in sample.iterrows():
        folium.CircleMarker(
            location=[row["lat"], row["lon"]], radius=4,
            color=ALERT_COLOR.get(row["Alert"], "gray"),
            fill=True, fill_opacity=0.85,
            tooltip=(f"<b style='color:#333'>{row['Alert']}</b><br>"
                     f"NDVI: <b>{row['NDVI']:.3f}</b> (vegetation health)<br>"
                     f"LSWI: <b>{row['LSWI']:.3f}</b> (water content)<br>"
                     f"📍 {row['lat']:.4f}, {row['lon']:.4f}")
        ).add_to(m)
    st_folium(m, width="100%", height=480)
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Speedup Graph + Amdahl's Law ─────────────────────────────────────────────
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-heading">⚡ Amdahl\'s Law — Why More Workers Don\'t Always Mean Faster</div>', unsafe_allow_html=True)

    # Load fresh history for the graph
    history_data = load_history()
    region_history = [h for h in history_data if h.get("region") == selected_region]
    history_df = pd.DataFrame(region_history) if region_history else pd.DataFrame()

    baseline = None
    if not history_df.empty and "workers" in history_df.columns and 1 in history_df["workers"].values:
        baseline = history_df[history_df["workers"] == 1]["time_sec"].min()

    worker_range = list(range(1, 9))
    
    # Estimate parallel fraction p for the theoretical curve
    if baseline:
        best_times = history_df.groupby("workers")["time_sec"].min()
        multi = best_times[best_times.index > 1]
        if not multi.empty:
            n_est, t_est = multi.index[-1], multi.iloc[-1]
            p = min(0.99, max(0.5, (1 - t_est / baseline) / (1 - 1 / n_est))) if n_est > 1 else 0.85
        else:
            p = 0.85
    else:
        p = 0.85 # Default 85% parallelizable

    amdahl_speedup = [1 / ((1 - p) + p / n) for n in worker_range]
    amdahl_df = pd.DataFrame({"🔵 Theoretical Ceiling (Amdahl's Law)": amdahl_speedup}, index=worker_range)
    amdahl_df.index.name = "Workers"

    if baseline:
        best_times = history_df.groupby("workers")["time_sec"].min().sort_index()
        actual_speedup = (baseline / best_times).round(2)
        # Ensure the actual results line connects existing data points in the chart
        amdahl_df["🔴 Your Actual Results"] = actual_speedup.reindex(worker_range)
        
        # Efficiency = Speedup / Workers
        actual_indices = actual_speedup.index
        efficiency = (actual_speedup / pd.Series(actual_indices, index=actual_indices) * 100).round(1)

        st.line_chart(amdahl_df)
        
        st.markdown("**⚡ Efficiency Breakdown**")
        eff_cols = st.columns(len(best_times))
        for i, (workers, eff_val) in enumerate(zip(best_times.index, efficiency.values)):
            with eff_cols[i]:
                eff_color = "#27ae60" if eff_val >= 80 else "#f39c12" if eff_val >= 50 else "#e74c3c"
                st.markdown(f"""
                <div style='text-align:center;margin-bottom:10px'>
                    <div style='font-size:24px;font-weight:800;color:{eff_color}'>{eff_val:.0f}%</div>
                    <div style='width:60px;height:60px;border-radius:50%;background:conic-gradient({eff_color} {eff_val*3.6}deg, #eee 0deg);margin:0 auto;display:flex;align-items:center;justify-content:center'>
                        <div style='width:40px;height:40px;border-radius:50%;background:white;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700'>{workers}</div>
                    </div>
                    <div style='font-size:11px;margin-top:4px'>Workers</div>
                </div>
                """, unsafe_allow_html=True)
                
        # Sweet spot analysis
        sweet_spot = int(best_times.idxmin())
        sweet_time = best_times.min()
        
        if sweet_spot == 1 and len(best_times) > 1:
            st.warning(f"⚠️ **Parallelism Overhead**: Using 1 worker was actually faster ({sweet_time}s) than multiple workers for this region. This happens when data coordination takes more time than the actual computation.")
        elif sweet_spot > 1:
            st.success(f"🎯 **Sweet Spot Found**: Your most efficient configuration is **{sweet_spot} workers** ({sweet_time}s).")

        st.markdown("---")
        st.markdown('<div class="section-heading" style="font-size:15px; color:#1e3a5f !important">📊 Performance Metrics Table</div>', unsafe_allow_html=True)
        table_df = pd.DataFrame({
            "Workers": best_times.index,
            "Total Time (s)": best_times.values,
            "Speedup (×)": actual_speedup.values,
            "Efficiency (%)": efficiency.values
        })
        st.dataframe(table_df, use_container_width=True, hide_index=True)
    else:
        st.line_chart(amdahl_df)
        st.info("👈 **Run once with 1 worker** in Data Parallel mode to establish a baseline and unlock actual speedup analysis.")

    st.markdown('</div>', unsafe_allow_html=True)

    # ── Charts removed - now only clickable index icons below ──

    # ── Satellite Index Icons - Interactive Dashboard ────────────────────────────
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">🛰️ Interactive Satellite Index Dashboard</div>', unsafe_allow_html=True)

    # Add custom CSS for attractive cards
    st.markdown("""
    <style>
    .index-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        padding: 20px;
        margin: 10px 5px;
        text-align: center;
        color: white;
        cursor: pointer;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        border: none;
        width: 100%;
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .index-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.2);
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    .index-icon {
        font-size: 2.5em;
        margin-bottom: 8px;
    }
    .index-title {
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 4px;
    }
    .index-subtitle {
        font-size: 11px;
        opacity: 0.9;
        line-height: 1.3;
    }
    .chart-container {
        background: #f8fafc;
        border-radius: 12px;
        padding: 20px;
        margin: 15px 0;
        border-left: 4px solid #3498db;
    }
    </style>
    """, unsafe_allow_html=True)

    # Create attractive index cards with categories
    vegetation_indices = {
        "🌱 NDVI": {"name": "Vegetation Health", "data": "NDVI", "desc": "Primary crop vitality indicator", "color": "#27ae60"},
        "🌿 EVI": {"name": "Enhanced Vegetation", "data": "EVI", "desc": "Advanced vegetation monitoring", "color": "#2ecc71"},
        "🌱 SAVI": {"name": "Soil-Adjusted Vegetation", "data": "SAVI", "desc": "Vegetation corrected for soil", "color": "#1e8449"},
        "💚 GNDVI": {"name": "Green Vegetation", "data": "GNDVI", "desc": "Green leaf assessment", "color": "#16a085"},
    }

    water_indices = {
        "💧 LSWI": {"name": "Surface Water", "data": "LSWI", "desc": "Soil & leaf water content", "color": "#3498db"},
        "💦 MSI": {"name": "Moisture Stress", "data": "MSI", "desc": "Plant dehydration detector", "color": "#2980b9"},
    }

    soil_indices = {
        "🏔️ BSI": {"name": "Bare Soil", "data": "BSI", "desc": "Exposed soil detection", "color": "#a04000"},
        "🏜️ DSI": {"name": "Drought Stress", "data": "DSI", "desc": "Drought condition analysis", "color": "#e74c3c"},
    }

    advanced_indices = {
        "🌪️ ARVI": {"name": "Atmospheric Resistant", "data": "ARVI", "desc": "Weather-corrected analysis", "color": "#9b59b6"},
        "🔬 SIPI": {"name": "Pigment Structure", "data": "SIPI", "desc": "Chlorophyll ratio analysis", "color": "#e67e22"},
        "🌍 GEMI": {"name": "Global Monitoring", "data": "GEMI", "desc": "Non-linear vegetation index", "color": "#34495e"},
    }

    # Display categories with attractive cards
    categories = [
        ("🌿 Vegetation Health", vegetation_indices, "#27ae60"),
        ("💧 Water & Moisture", water_indices, "#3498db"),
        ("🏔️ Soil Analysis", soil_indices, "#e67e22"),
        ("🔬 Advanced Indices", advanced_indices, "#9b59b6")
    ]

    for category_name, indices, category_color in categories:
        if any(info["data"] in df.columns for info in indices.values()):
            st.markdown(f"**{category_name}**")
            cols = st.columns(len([info for info in indices.values() if info["data"] in df.columns]))
        
            col_idx = 0
            for icon_name, info in indices.items():
                if info["data"] in df.columns:
                    with cols[col_idx]:
                        # Create attractive button with custom HTML
                        button_html = f"""
                        <div class="index-card" style="background: linear-gradient(135deg, {info['color']}22 0%, {info['color']}44 100%); border-left: 4px solid {info['color']};">
                            <div class="index-icon">{icon_name.split()[0]}</div>
                            <div class="index-title">{info['name']}</div>
                            <div class="index-subtitle">{info['desc']}</div>
                        </div>
                        """
                    
                        if st.button(f"{icon_name}\n{info['name']}", key=f"btn_{info['data']}", help=info['desc']):
                            # Show chart in attractive container
                            st.markdown(f'<div class="chart-container">', unsafe_allow_html=True)
                        
                            # Header with icon and description
                            st.markdown(f"### {icon_name} {info['name']}")
                            st.markdown(f"*{info['desc']}*")
                        
                            # Create enhanced chart
                            chart_data = df[info['data']].dropna().round(2).value_counts().sort_index().rename("Farm Points")
                        
                            # Add interpretation guide
                            if info['data'] == 'NDVI':
                                st.info("📊 **Interpretation:** < 0.2 = Bare soil/Drought | 0.2-0.4 = Stressed crops | > 0.4 = Healthy vegetation")
                            elif info['data'] == 'LSWI':
                                st.info("📊 **Interpretation:** < 0.2 = Water stress | 0.2-0.4 = Moderate moisture | > 0.4 = Good water content")
                            elif info['data'] == 'BSI':
                                st.info("📊 **Interpretation:** > 0 = Bare soil detected | < 0 = Vegetation cover present")
                            elif info['data'] == 'MSI':
                                st.info("📊 **Interpretation:** > 1.0 = Plant water stressed | < 1.0 = Plant well hydrated")
                        
                            # Enhanced chart with better styling
                            st.bar_chart(chart_data, color=info['color'])
                        
                            st.markdown('</div>', unsafe_allow_html=True)
                        
                    col_idx += 1
        
            st.markdown("---")

    st.markdown('</div>', unsafe_allow_html=True)

    # ── Alert Breakdown ───────────────────────────────────────────────────────────
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-heading">🚨 Alert Breakdown — {selected_region}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="section-desc">Risk category distribution across all {total_rows:,} farm points.</div>', unsafe_allow_html=True)
    
    # Prepare data for Altair
    alert_counts = df["Alert"].value_counts().reset_index()
    alert_counts.columns = ["Alert", "Farm Points"]
    
    # Define mapping and sorting
    alert_order = ["Healthy", "Moderate", "Water Stress", "Drought"]
    alert_colors = ["#27ae60", "#3498db", "#f39c12", "#e74c3c"]
    
    # Ensure all categories exist even if count is 0 for visualization consistency
    for alert in alert_order:
        if alert not in alert_counts["Alert"].values:
            alert_counts = pd.concat([alert_counts, pd.DataFrame({"Alert": [alert], "Farm Points": [0]})], ignore_index=True)
    
    c_chart1, c_chart2 = st.columns([1, 1])
    
    with c_chart1:
        # Donut Chart - Distribution
        donut = alt.Chart(alert_counts).mark_arc(innerRadius=60, stroke="#fff").encode(
            theta=alt.Theta(field="Farm Points", type="quantitative"),
            color=alt.Color(field="Alert", type="nominal", 
                           scale=alt.Scale(domain=alert_order, range=alert_colors),
                           legend=alt.Legend(title="Risk Level", orient="bottom")),
            tooltip=["Alert", "Farm Points"]
        ).properties(height=300, title="Regional Distribution")
        st.altair_chart(donut, use_container_width=True)

    with c_chart2:
        # Bar Chart - Comparison
        bars = alt.Chart(alert_counts).mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8).encode(
            x=alt.X("Alert:N", sort=alert_order, title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("Farm Points:Q", title="Number of Points"),
            color=alt.Color("Alert:N", scale=alt.Scale(domain=alert_order, range=alert_colors), legend=None),
            tooltip=["Alert", "Farm Points"]
        ).properties(height=300, title="Category Counts")
        st.altair_chart(bars, use_container_width=True)

    # Metrics Row
    st.markdown("<br>", unsafe_allow_html=True)
    metric_cols = st.columns(len(alert_order))
    counts_dict = alert_counts.set_index("Alert")["Farm Points"].to_dict()
    for col, label in zip(metric_cols, alert_order):
        count = counts_dict.get(label, 0)
        col.metric(label, f"{count:,}", f"{round(count/len(df)*100,1)}% of region")
    st.markdown('</div>', unsafe_allow_html=True)

    # End of tab1 logic (Amdahl's Law moved higher up)
    pass
