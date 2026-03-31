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
    n_workers = st.slider("MPI Workers (CPU Cores)", 1, 8, 4)
    st.caption(f"Using **{n_workers} core(s)** — more cores = faster processing")
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
            fastest = min(r["time_sec"] for r in history if r["workers"] == h["workers"] and r.get("region") == h.get("region"))
            badge = " 🏆" if h["time_sec"] == fastest else ""
            st.markdown(
                f'<div class="history-row">'
                f'🕐 <b>{h["timestamp"]}</b><br>'
                f'📍 {h.get("region","—")}<br>'
                f'Workers: <b>{h["workers"]}</b> &nbsp;|&nbsp; '
                f'Time: <b>{h["time_sec"]}s</b>{badge}<br>'
                f'Rows: {h["rows"]:,}'
                f'</div>',
                unsafe_allow_html=True
            )

# ── Run MPI ───────────────────────────────────────────────────────────────────
if run_clicked:
    # ── Live Worker Heartbeat ──────────────────────────────────────────────────
    heartbeat_placeholder = st.empty()
    colors_hb = ["#c0392b","#1a5276","#1e8449","#b7770d","#6c3483","#117a65","#784212","#2c3e50"]
    worker_cards = "".join([
        f"""<div style='display:inline-block;margin:6px;text-align:center;width:80px'>
            <div style='width:52px;height:52px;border-radius:50%;background:{colors_hb[i % 8]};
                margin:0 auto;display:flex;align-items:center;justify-content:center;
                font-size:22px;animation:pulse 1.2s ease-in-out {i*0.15:.2f}s infinite;'>⚙️</div>
            <div style='font-size:11px;font-weight:700;color:#0d1b2a;margin-top:6px'>Worker {i}<br>
            <span style='color:#27ae60;font-size:10px'>● ACTIVE</span></div></div>"""
        for i in range(n_workers)
    ])
    heartbeat_placeholder.markdown(f"""
    <style>
    @keyframes pulse {{
        0%,100% {{ transform: scale(1); opacity:1; box-shadow: 0 0 0 0 rgba(255,255,255,0.4); }}
        50% {{ transform: scale(1.18); opacity:0.85; box-shadow: 0 0 0 10px rgba(255,255,255,0); }}
    }}
    </style>
    <div style='background:#0d1b2a;border-radius:14px;padding:18px 24px;margin-bottom:12px'>
        <div style='color:white;font-size:15px;font-weight:800;margin-bottom:12px'>
            🚀 MPI Workers Running — {n_workers} Core(s) Active on {selected_region}
        </div>
        <div>{worker_cards}</div>
        <div style='color:#aaa;font-size:12px;margin-top:12px'>⏳ Processing {total_rows:,} rows in parallel...</div>
    </div>
    """, unsafe_allow_html=True)

    t0 = time.time()
    result = subprocess.run(
        ["/opt/homebrew/bin/mpiexec", "-n", str(n_workers),
         "/opt/anaconda3/bin/python3", "process.py", DATA_FILE, RESULT_FILE],
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
            "rows": total_rows
        })
        st.success(f"✅ Done! Processed {total_rows:,} rows in **{elapsed}s** using **{n_workers} core(s)**")
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

# ── NDVI / LSWI / SAVI / BSI / MSI ──────────────────────────────────────────
col_a, col_b = st.columns(2)
with col_a:
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">🌿 NDVI — Vegetation Health</div>', unsafe_allow_html=True)
    st.markdown("""<div class="section-desc">Measures how green and alive the crops are.<br>
        🔴 <b>Below 0.2</b> → Bare soil / Drought &nbsp;|&nbsp;
        🟡 <b>0.2–0.4</b> → Stressed crops &nbsp;|&nbsp;
        🟢 <b>Above 0.4</b> → Healthy crops</div>""", unsafe_allow_html=True)
    st.bar_chart(df["NDVI"].dropna().round(1).value_counts().sort_index().rename("Farm Points"), color="#27ae60")
    st.markdown('</div>', unsafe_allow_html=True)

with col_b:
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">💧 LSWI — Soil Water Content</div>', unsafe_allow_html=True)
    st.markdown("""<div class="section-desc">Measures how much water is in the soil and leaves.<br>
        🔴 <b>Below 0.2</b> → Critical water stress &nbsp;|&nbsp;
        🟡 <b>0.2–0.4</b> → Low moisture &nbsp;|&nbsp;
        🟢 <b>Above 0.4</b> → Adequate water</div>""", unsafe_allow_html=True)
    st.bar_chart(df["LSWI"].dropna().round(1).value_counts().sort_index().rename("Farm Points"), color="#3498db")
    st.markdown('</div>', unsafe_allow_html=True)

if "SAVI" in df.columns:
    col_c, col_d, col_e = st.columns(3)
    with col_c:
        st.markdown('<div class="section-box">', unsafe_allow_html=True)
        st.markdown('<div class="section-heading">🌱 SAVI — Soil-Adjusted Vegetation</div>', unsafe_allow_html=True)
        st.markdown("""<div class="section-desc">Like NDVI but corrected for bare soil — more accurate in dry/arid regions.<br>
            🔴 <b>Below 0.2</b> → Sparse/no vegetation &nbsp;|&nbsp; 🟢 <b>Above 0.4</b> → Good crop cover</div>""", unsafe_allow_html=True)
        st.bar_chart(df["SAVI"].dropna().round(1).value_counts().sort_index().rename("Farm Points"), color="#1e8449")
        st.markdown('</div>', unsafe_allow_html=True)
    with col_d:
        st.markdown('<div class="section-box">', unsafe_allow_html=True)
        st.markdown('<div class="section-heading">🟤 BSI — Bare Soil Index</div>', unsafe_allow_html=True)
        st.markdown("""<div class="section-desc">Detects how much land has zero crop cover — exposed bare soil.<br>
            🟤 <b>Above 0</b> → Bare soil detected &nbsp;|&nbsp; 🟢 <b>Below 0</b> → Vegetation present</div>""", unsafe_allow_html=True)
        st.bar_chart(df["BSI"].dropna().round(1).value_counts().sort_index().rename("Farm Points"), color="#a04000")
        st.markdown('</div>', unsafe_allow_html=True)
    with col_e:
        st.markdown('<div class="section-box">', unsafe_allow_html=True)
        st.markdown('<div class="section-heading">💦 MSI — Plant Moisture Stress</div>', unsafe_allow_html=True)
        st.markdown("""<div class="section-desc">Measures if the plant itself is dehydrated (different from soil water).<br>
            🔴 <b>Above 1.0</b> → Plant is water stressed &nbsp;|&nbsp; 🟢 <b>Below 1.0</b> → Plant hydrated</div>""", unsafe_allow_html=True)
        st.bar_chart(df["MSI"].dropna().round(1).value_counts().sort_index().rename("Farm Points"), color="#1a5276")
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
st.markdown('<div class="section-heading">⚡ Amdahl\'s Law — Theoretical Limit vs Your Actual Speedup</div>', unsafe_allow_html=True)
st.markdown('<div class="section-desc">'
    '<b>Speedup</b> = how many times faster than 1 worker. '
    'Amdahl\'s Law says there is a <b>ceiling</b> — no matter how many workers you add, '
    'the non-parallelizable parts (file load, scatter, gather) always take the same time. '
    'The curve flattens out and you can never cross the ceiling.</div>', unsafe_allow_html=True)

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
amdahl_df = pd.DataFrame({"Amdahl's Law (Theoretical Limit)": amdahl_speedup}, index=worker_range)
amdahl_df.index.name = "Workers"

if baseline:
    best_times = history_df.groupby("workers")["time_sec"].min().sort_index()
    actual_speedup = (baseline / best_times).round(2)
    amdahl_df["Your Actual Speedup"] = actual_speedup
    # Efficiency = Speedup / Workers
    efficiency = (actual_speedup / pd.Series(best_times.index, index=best_times.index) * 100).round(1)

st.line_chart(amdahl_df)
st.caption("📌 X-axis = number of workers | Y-axis = speedup (1.0 = same as 1 worker, 2.0 = twice as fast). "
           "The curve flattens = Amdahl's ceiling.")

# Sweet spot note
if baseline:
    best_times = history_df.groupby("workers")["time_sec"].min()
    sweet_spot = int(best_times.idxmin())
    sweet_time = best_times.min()
    if sweet_spot == 1:
        st.warning(f"⚠️ **Sweet Spot: 1 Worker ({sweet_time}s)** — For this dataset size (~{total_rows:,} rows), "
                   "MPI communication overhead exceeds the computation gain. "
                   "Parallelism becomes beneficial at ~10M+ rows where compute time dominates overhead.")
    else:
        st.success(f"✅ **Sweet Spot: {sweet_spot} Workers ({sweet_time}s)** — This is the optimal core count "
                   "for this dataset. Beyond this point, coordination overhead starts to outweigh the gains.")

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
