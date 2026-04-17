"""
Streamlit Dashboard — Parallel Geospatial Analytics Engine
Run with: streamlit run app.py
"""
import subprocess, time, os, json, glob
from datetime import datetime
import pandas as pd
import folium
import streamlit as st
from streamlit_folium import st_folium

st.set_page_config(page_title="Sat2Farm", layout="wide", page_icon="🌾")

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

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    body, .main { background-color: #eef2f7; }
    .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
    h1, h2, h3, h4 { color: #0d1b2a !important; font-weight: 800 !important; }
    .section-box {
        background: #ffffff; border-radius: 14px; padding: 20px 24px;
        margin-bottom: 18px; box-shadow: 0 3px 12px rgba(0,0,0,0.09);
    }
    .section-heading { font-size: 17px; font-weight: 800; color: #0d1b2a; margin-bottom: 4px; }
    .section-desc { font-size: 13px; color: #444; margin-bottom: 14px; line-height: 1.6; }
    .metric-card {
        background: white; border-radius: 14px; padding: 20px 16px;
        text-align: center; box-shadow: 0 3px 10px rgba(0,0,0,0.08); height: 100%;
    }
    .metric-label { font-size: 12px; font-weight: 700; color: #555; letter-spacing: 0.8px; text-transform: uppercase; }
    .metric-value { font-size: 34px; font-weight: 800; margin: 6px 0 4px; }
    .metric-sub { font-size: 12px; color: #777; }
    .history-row {
        background: #f8fafc; border-left: 4px solid #2563eb; border-radius: 8px;
        padding: 10px 16px; margin-bottom: 8px; font-size: 13px; color: #0d1b2a;
    }
    .history-row b { color: #2563eb; }
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
st.markdown("# 🌾 Sat2Farm — Agricultural Risk Monitor")
st.markdown("**Satellite-powered parallel analysis** using MPI distributed computing.")
st.divider()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🌍 Select Region")
    selected_region = st.selectbox("", list(REGIONS.keys()), label_visibility="collapsed")
    region_cfg  = REGIONS[selected_region]
    DATA_FILE   = region_cfg["file"]
    RESULT_FILE = DATA_FILE.replace("_Data.csv", "_results.csv")
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
                <div style='font-size:10px;font-weight:700;color:#0d1b2a;margin-top:6px'>Worker {i}<br>
                <span style='font-size:9px;color:#555'>{pipeline_tasks[i] if i < len(pipeline_tasks) else 'Extra'}</span><br>
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
                <div style='font-size:11px;font-weight:700;color:#0d1b2a;margin-top:6px'>Worker {i}<br>
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
        ["/opt/homebrew/bin/mpiexec", "-n", str(n_workers),
         "/opt/anaconda3/bin/python3", process_file, DATA_FILE, RESULT_FILE],
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

st.markdown(f"### 📍 Showing results for: {selected_region} — {total_rows:,} farm points")
st.divider()


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["🌾 Agricultural Dashboard", "🧪 Soil Analysis Report"])

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
                ["/opt/homebrew/bin/mpiexec", "-n", str(soil_workers),
                 "/opt/anaconda3/bin/python3", "soil_report.py", RESULT_FILE, SOIL_REPORT_FILE],
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
                    <div style='font-size:22px;font-weight:900;letter-spacing:1px'>🌍 Sat2Farm — Soil Analysis Report</div>
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

        # Disclaimer
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("""
        <div style='background:#f8f9fa;border-radius:10px;padding:16px 20px;border:1px solid #dee2e6'>
            <div style='font-size:12px;font-weight:700;color:#666;margin-bottom:6px'>⚠️ DISCLAIMER</div>
            <div style='font-size:11px;color:#888;line-height:1.7'>
                This report is generated from satellite imagery (Sentinel-2, 10m resolution) using MPI parallel computing.
                Soil parameters are estimated using spectral index correlations and may differ from laboratory tests.
                Values are region-level averages across all analyzed pixels. Not to be used as sole basis for agricultural decisions.
            </div>
        </div>
        """, unsafe_allow_html=True)

with tab1:

    # ── HOW IT WORKS visualizer ───────────────────────────────────────────────────
    if "viz_step" not in st.session_state:
        st.session_state.viz_step = 0

    with st.expander("🔍 How does Parallel Processing work here? — Click to see Step-by-Step", expanded=False):
        st.markdown('<div class="section-heading">🧠 MPI Pipeline Walkthrough — What happens when you click Run</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="section-desc">Step through the full MPI pipeline with real data from <b>{DATA_FILE}</b>. Using <b>{n_workers} worker(s)</b> — same as selected in the sidebar.</div>', unsafe_allow_html=True)

        raw = pd.read_csv(DATA_FILE).head(500 * n_workers)
        viz_workers = n_workers
        chunk_size  = len(raw) // viz_workers
        chunks = [raw.iloc[i*chunk_size:(i+1)*chunk_size].reset_index(drop=True) for i in range(viz_workers)]
        colors = ["#c0392b","#1a5276","#1e8449","#b7770d","#6c3483","#117a65","#784212","#2c3e50"]

        b1, b2, b3 = st.columns([1, 1, 4])
        if b1.button("◀ Previous", key="prev_step") and st.session_state.viz_step > 0:
            st.session_state.viz_step -= 1
        if b2.button("Next ▶", key="next_step", type="primary") and st.session_state.viz_step < 4:
            st.session_state.viz_step += 1
        if b3.button("↺ Restart", key="restart_step"):
            st.session_state.viz_step = 0

        step = st.session_state.viz_step
        st.markdown(f"**Step {step + 1} of 5** — {'⏳ Load' if step==0 else '✂️ Scatter' if step==1 else '⚙️ Compute' if step==2 else '📦 Gather' if step==3 else '✅ Done'}")
        st.progress((step + 1) / 5)
        st.markdown("---")

        if step == 0:
            st.markdown(f"""<div style='background:#1a1a2e;color:white;border-radius:10px;padding:18px 22px;margin-bottom:14px'>
                <span style='font-size:16px;font-weight:800'>⏳ STEP 1 — Master (Rank 0) loads the CSV</span><br><br>
                Master reads <b>{DATA_FILE}</b> into memory.<br><br>
                📄 Total rows: <b>{total_rows:,}</b><br>
                📊 Columns: <b>B4</b> (Red), <b>B8</b> (Near-Infrared), <b>B11</b> (Short-wave Infrared), <b>.geo</b> (coordinates)<br><br>
                Just raw numbers at this point — no NDVI, no risk labels yet.
            </div>""", unsafe_allow_html=True)
            st.caption("👇 First 6 rows of raw satellite data:")
            st.dataframe(raw[["B4","B8","B11"]].head(6), use_container_width=True)

        elif step == 1:
            st.markdown(f"""<div style='background:#1a3a5c;color:white;border-radius:10px;padding:18px 22px;margin-bottom:14px'>
                <span style='font-size:16px;font-weight:800'>✂️ STEP 2 — MPI_Scatter: Master splits data and sends to workers</span><br><br>
                {total_rows:,} rows ÷ {viz_workers} workers = <b>~{total_rows // viz_workers:,} rows per worker</b><br><br>
                Each worker gets a <b>unique slice</b>. No overlap. All sent <b>simultaneously</b>.
            </div>""", unsafe_allow_html=True)
            cols = st.columns(viz_workers)
            for i, (col, chunk) in enumerate(zip(cols, chunks)):
                with col:
                    st.markdown(f"""<div style='background:{colors[i]};color:white;border-radius:8px;
                        padding:10px;text-align:center;margin-bottom:8px'>
                        <b>Worker {i} (Rank {i})</b><br>
                        <span style='font-size:12px'>{total_rows // viz_workers:,} rows received</span><br>
                        <span style='font-size:11px;opacity:0.8'>(scrollable preview — 500 rows)</span>
                    </div>""", unsafe_allow_html=True)
                    st.dataframe(chunk[["B4","B8","B11"]].head(500), use_container_width=True, height=250)

        elif step == 2:
            st.markdown(f"""<div style='background:#145a32;color:white;border-radius:10px;padding:18px 22px;margin-bottom:14px'>
                <span style='font-size:16px;font-weight:800'>⚙️ STEP 3 — All {viz_workers} workers compute SIMULTANEOUSLY</span><br><br>
                &nbsp;&nbsp;🌿 <b>NDVI = (B8 − B4) / (B8 + B4)</b> → How green/alive are the crops?<br>
                &nbsp;&nbsp;💧 <b>LSWI = (B8 − B11) / (B8 + B11)</b> → How much water is in the soil?<br><br>
                Risk rules: NDVI &lt; 0.2 → 🔴 Drought &nbsp;|&nbsp; LSWI &lt; 0.2 → 🟠 Water Stress &nbsp;|&nbsp; NDVI ≥ 0.4 → 🟢 Healthy
            </div>""", unsafe_allow_html=True)
            cols = st.columns(viz_workers)
            for i, (col, chunk) in enumerate(zip(cols, chunks)):
                B4  = chunk["B4"].values.astype(float)
                B8  = chunk["B8"].values.astype(float)
                B11 = chunk["B11"].values.astype(float)
                ndvi = (B8 - B4) / (B8 + B4)
                lswi = (B8 - B11) / (B8 + B11)
                alerts = ["Drought" if n < 0.2 else "Water Stress" if l < 0.2
                          else "Healthy" if n >= 0.4 else "Moderate" for n, l in zip(ndvi, lswi)]
                computed = pd.DataFrame({"NDVI": ndvi.round(3), "LSWI": lswi.round(3), "Alert": alerts})
                with col:
                    st.markdown(f"""<div style='background:{colors[i]};color:white;border-radius:8px;
                        padding:10px;text-align:center;margin-bottom:8px'>
                        <b>Worker {i} ⚙️ Done</b><br>
                        <span style='font-size:11px;opacity:0.8'>(scrollable preview — 500 rows)</span>
                    </div>""", unsafe_allow_html=True)
                    st.dataframe(computed.head(500), use_container_width=True, height=250)

        elif step == 3:
            st.markdown(f"""<div style='background:#4a235a;color:white;border-radius:10px;padding:18px 22px;margin-bottom:14px'>
                <span style='font-size:16px;font-weight:800'>📦 STEP 4 — MPI_Gather: Workers send results back to Master</span><br><br>
                All {viz_workers} workers send chunks back to <b>Master (Rank 0)</b>.<br>
                Master stitches them in order → full {total_rows:,}-row result table.
            </div>""", unsafe_allow_html=True)
            all_parts = []
            for chunk in chunks:
                B4  = chunk["B4"].values.astype(float)
                B8  = chunk["B8"].values.astype(float)
                B11 = chunk["B11"].values.astype(float)
                ndvi = (B8 - B4) / (B8 + B4)
                lswi = (B8 - B11) / (B8 + B11)
                alerts = ["Drought" if n < 0.2 else "Water Stress" if l < 0.2
                          else "Healthy" if n >= 0.4 else "Moderate" for n, l in zip(ndvi, lswi)]
                all_parts.append(pd.DataFrame({"NDVI": ndvi.round(3), "LSWI": lswi.round(3), "Alert": alerts}))
            gathered = pd.concat(all_parts, ignore_index=True)
            st.markdown(f"**✅ Gathered: {len(gathered)} rows combined from all {viz_workers} workers**")
            st.dataframe(gathered.head(500), use_container_width=True, height=300)

        elif step == 4:
            all_parts = []
            for chunk in chunks:
                B4  = chunk["B4"].values.astype(float)
                B8  = chunk["B8"].values.astype(float)
                B11 = chunk["B11"].values.astype(float)
                ndvi = (B8 - B4) / (B8 + B4)
                lswi = (B8 - B11) / (B8 + B11)
                alerts = ["Drought" if n < 0.2 else "Water Stress" if l < 0.2
                          else "Healthy" if n >= 0.4 else "Moderate" for n, l in zip(ndvi, lswi)]
                all_parts.append(pd.DataFrame({"NDVI": ndvi.round(3), "LSWI": lswi.round(3), "Alert": alerts}))
            gathered = pd.concat(all_parts, ignore_index=True)
            alert_summary = gathered["Alert"].value_counts()
            st.markdown(f"""<div style='background:#1a4a1a;color:white;border-radius:10px;padding:18px 22px;margin-bottom:14px'>
                <span style='font-size:16px;font-weight:800'>✅ STEP 5 — Master saves {RESULT_FILE} → Dashboard updates</span><br><br>
                Full {total_rows:,}-row result written to <b>{RESULT_FILE}</b>.<br>
                The map, charts, and risk alerts below are all read from this file.
            </div>""", unsafe_allow_html=True)
            s_cols = st.columns(len(alert_summary))
            for scol, (label, count) in zip(s_cols, alert_summary.items()):
                scol.metric(label, f"{count:,}", f"{round(count/len(gathered)*100,1)}%")
            st.success("🎉 Pipeline complete!")
            st.markdown(f"""
    **Full pipeline recap for {selected_region}:**
    - ✅ Step 1: Master loaded `{DATA_FILE}` ({total_rows:,} rows)
    - ✅ Step 2: MPI_Scatter — split into {viz_workers} chunks of ~{total_rows // viz_workers:,} rows each
    - ✅ Step 3: All workers computed NDVI, LSWI, Risk labels **at the same time**
    - ✅ Step 4: MPI_Gather — all results collected back to Master in order
    - ✅ Step 5: Saved to `{RESULT_FILE}` → map and charts updated""")

    # ── Metric Cards ──────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    cards = [
        (c1, "📍 TOTAL POINTS", f"{len(df):,}", "#0d1b2a", "Satellite pixels analyzed"),
        (c2, "🔴 DROUGHT RISK",  f"{df['Drought_Risk'].sum():,}", "#e74c3c",
         f"{round(df['Drought_Risk'].sum()/len(df)*100,1)}% — NDVI < 0.2 (dry/bare land)"),
        (c3, "🟠 WATER STRESS",  f"{df['Water_Stress'].sum():,}", "#f39c12",
         f"{round(df['Water_Stress'].sum()/len(df)*100,1)}% — LSWI < 0.2 (low moisture)"),
        (c4, "🟢 HEALTHY",       f"{df['Vegetation_Healthy'].sum():,}", "#27ae60",
         f"{round(df['Vegetation_Healthy'].sum()/len(df)*100,1)}% — NDVI ≥ 0.4 (good crops)"),
    ]
    for col, label, value, color, sub in cards:
        with col:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">{label}</div>
                <div class="metric-value" style="color:{color}">{value}</div>
                <div class="metric-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    if "Bare_Soil" in df.columns:
        r1, r2, r3 = st.columns(3)
        extra_cards = [
            (r1, "🟤 BARE SOIL",         f"{df['Bare_Soil'].sum():,}",          "#a04000",
             f"{round(df['Bare_Soil'].sum()/len(df)*100,1)}% — BSI > 0 (no crop cover)"),
            (r2, "💧 PLANT WATER STRESS", f"{df['Plant_Water_Stress'].sum():,}", "#1a5276",
             f"{round(df['Plant_Water_Stress'].sum()/len(df)*100,1)}% — MSI > 1.0 (plant dehydrated)"),
            (r3, "🌱 AVG SAVI",           f"{df['SAVI'].mean():.3f}",            "#1e8449",
             "Soil-adjusted vegetation (arid-region accurate)"),
        ]
        for col, label, value, color, sub in extra_cards:
            with col:
                st.markdown(f"""<div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value" style="color:{color}">{value}</div>
                    <div class="metric-sub">{sub}</div>
                </div>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # ── Map ───────────────────────────────────────────────────────────────────────
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-heading">🗺️ Farm Risk Map — {selected_region}</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-desc">Each dot = one satellite pixel. Color shows risk level. <b>Hover</b> to see NDVI, LSWI and coordinates. Showing 2,000 sampled points.</div>', unsafe_allow_html=True)

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

    # ── Charts removed - now only clickable index icons below ──

    # ── Satellite Index Icons - Interactive Dashboard ────────────────────────────
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">🛰️ Interactive Satellite Index Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-desc">Hover over cards to see descriptions, click to analyze. Each index reveals different agricultural insights.</div>', unsafe_allow_html=True)

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
    st.markdown(f'<div class="section-desc">Risk category counts across all {total_rows:,} farm points.</div>', unsafe_allow_html=True)
    alert_counts = df["Alert"].value_counts().rename("Farm Points")
    st.bar_chart(alert_counts, color="#e74c3c")
    cols = st.columns(len(alert_counts))
    for col, (label, count) in zip(cols, alert_counts.items()):
        col.metric(label, f"{count:,}", f"{round(count/len(df)*100,1)}% of region")
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Speedup Graph + Amdahl's Law ─────────────────────────────────────────────
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">⚡ Amdahl\'s Law — Why More Workers Don\'t Always Mean Faster</div>', unsafe_allow_html=True)

    history = load_history()
    region_history = [h for h in history if h.get("region") == selected_region]
    history_df = pd.DataFrame(region_history) if region_history else pd.DataFrame()

    baseline = None
    if not history_df.empty and 1 in history_df["workers"].values:
        baseline = history_df[history_df["workers"] == 1]["time_sec"].min()

    worker_range = list(range(1, 9))

    # Amdahl's Law curve — estimate parallel fraction from actual data if possible
    if baseline:
        best_times = history_df.groupby("workers")["time_sec"].min()
        # Estimate parallel fraction p: T(n) = T1 * ((1-p) + p/n)
        # Use best multi-worker run to estimate p
        multi = best_times[best_times.index > 1]
        if not multi.empty:
            n_est, t_est = multi.index[-1], multi.iloc[-1]
            p = min(0.99, max(0.5, (1 - t_est / baseline) / (1 - 1 / n_est))) if n_est > 1 else 0.8
        else:
            p = 0.8
    else:
        p = 0.8  # default assumption: 80% parallelizable

    amdahl_speedup = [1 / ((1 - p) + p / n) for n in worker_range]
    amdahl_df = pd.DataFrame({"🔵 Theoretical Ceiling (Amdahl's Law)": amdahl_speedup}, index=worker_range)
    amdahl_df.index.name = "Workers"

    if baseline:
        best_times = history_df.groupby("workers")["time_sec"].min().sort_index()
        actual_speedup = (baseline / best_times).round(2)
        amdahl_df["🔴 Your Actual Results"] = actual_speedup
        # Efficiency = Speedup / Workers
        efficiency = (actual_speedup / pd.Series(best_times.index, index=best_times.index) * 100).round(1)

    st.line_chart(amdahl_df)

    # Visual efficiency breakdown
    if baseline:
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
    
        # Sweet spot visual indicator
        best_times = history_df.groupby("workers")["time_sec"].min()
        sweet_spot = int(best_times.idxmin())
        sweet_time = best_times.min()
    
        if sweet_spot == 1:
            st.markdown(f"""
            <div style='background:#fff3cd;border-left:4px solid #ffc107;padding:15px;border-radius:8px;margin-top:15px'>
                <div style='font-size:16px;font-weight:800;color:#856404;margin-bottom:8px'>⚠️ Dataset Too Small for Parallelism</div>
                <div style='display:flex;align-items:center;gap:20px'>
                    <div style='font-size:48px'>📊</div>
                    <div>
                        <div style='font-size:14px;color:#856404'><b>Sweet Spot:</b> 1 Worker ({sweet_time}s)</div>
                        <div style='font-size:12px;color:#6c757d'>Coordination overhead > Computation savings</div>
                        <div style='font-size:12px;color:#6c757d'>Need ~10M+ rows for parallel benefits</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style='background:#d4edda;border-left:4px solid #28a745;padding:15px;border-radius:8px;margin-top:15px'>
                <div style='font-size:16px;font-weight:800;color:#155724;margin-bottom:8px'>✅ Optimal Parallel Configuration</div>
                <div style='display:flex;align-items:center;gap:20px'>
                    <div style='font-size:48px'>🎯</div>
                    <div>
                        <div style='font-size:14px;color:#155724'><b>Sweet Spot:</b> {sweet_spot} Workers ({sweet_time}s)</div>
                        <div style='font-size:12px;color:#6c757d'>Best balance of parallel gains vs overhead</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Run with **1 worker** to unlock visual efficiency analysis and sweet spot detection.")

    if baseline:
        st.markdown("---")
        st.markdown('<div class="section-heading" style="font-size:15px">📊 Speedup & Efficiency Table</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-desc">Efficiency = how well each worker is being used. 100% = perfect. Lower = workers are sitting idle due to overhead.</div>', unsafe_allow_html=True)
        table_df = pd.DataFrame({
            "Workers": best_times.index,
            "Time (s)": best_times.values,
            "Speedup (×)": actual_speedup.values,
            "Efficiency (%)": efficiency.values
        })
        st.dataframe(table_df, use_container_width=True, hide_index=True)
    else:
        st.info("Run once with **1 worker** to unlock actual speedup and efficiency comparison.")

    st.markdown('</div>', unsafe_allow_html=True)
