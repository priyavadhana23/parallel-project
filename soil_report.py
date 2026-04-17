"""
MPI Parallel Soil Report Generator
Estimates N, P, K, SOC, pH from satellite indices using parallel computing
Run with: mpiexec -n 4 python3 soil_report.py <results_csv> <output_json>
"""
import sys, json, time
import numpy as np
import pandas as pd
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

RESULT_FILE = sys.argv[1] if len(sys.argv) > 1 else "Arid_Region_results.csv"
OUTPUT_FILE = sys.argv[2] if len(sys.argv) > 2 else RESULT_FILE.replace("_results.csv", "_soil_report.json")


def estimate_soil_params(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Estimate soil parameters from satellite indices.
    Each parameter is derived from combinations of spectral indices.
    """
    ndvi = chunk["NDVI"].values
    savi = chunk["SAVI"].values
    bsi  = chunk["BSI"].values
    msi  = chunk["MSI"].values
    lswi = chunk["LSWI"].values
    evi  = chunk["EVI"].values if "EVI" in chunk.columns else ndvi

    # Nitrogen (kg/ha): high NDVI + high SAVI = high N
    # Range mapped to 0–800 kg/ha
    nitrogen = np.clip(((ndvi + savi) / 2) * 600 + 100, 0, 800)

    # Phosphorus (kg/ha): correlated with EVI and GNDVI
    # Range mapped to 0–100 kg/ha
    phosphorus = np.clip((evi * 0.5 + ndvi * 0.5) * 80 + 10, 0, 100)

    # Potassium (kg/ha): correlated with LSWI and SAVI
    # Range mapped to 0–600 kg/ha
    potassium = np.clip(((lswi + savi) / 2) * 400 + 80, 0, 600)

    # Soil Organic Carbon (%): BSI inversely related (more bare soil = less SOC)
    # Range mapped to 0–3%
    soc = np.clip(0.75 - bsi * 0.5 + ndvi * 0.3, 0, 3.0)

    # pH: MSI and BSI influence pH (dry/bare soils tend to be more alkaline)
    # Range mapped to 5.5–8.5
    ph = np.clip(6.5 + bsi * 0.8 - lswi * 0.5, 5.5, 8.5)

    chunk = chunk.copy()
    chunk["N_kgha"]  = nitrogen
    chunk["P_kgha"]  = phosphorus
    chunk["K_kgha"]  = potassium
    chunk["SOC_pct"] = soc
    chunk["pH"]      = ph
    return chunk


# ── Master loads & scatters ──────────────────────────────────────────────────
if rank == 0:
    df = pd.read_csv(RESULT_FILE)
    remainder = len(df) % size
    if remainder:
        df = pd.concat([df, df.iloc[:size - remainder]], ignore_index=True)
    chunks = np.array_split(df, size)
    t_start = time.time()
else:
    chunks = None
    t_start = None

# ── Scatter ──────────────────────────────────────────────────────────────────
local_chunk = comm.scatter(chunks, root=0)

# ── Each worker estimates soil params ────────────────────────────────────────
local_result = estimate_soil_params(local_chunk)

# ── Gather ───────────────────────────────────────────────────────────────────
all_results = comm.gather(local_result, root=0)

# ── Master aggregates and saves JSON report ──────────────────────────────────
if rank == 0:
    t_end = time.time()
    result_df = pd.concat(all_results, ignore_index=True)
    result_df = result_df.iloc[:len(df)]

    def classify_nitrogen(val):
        if val < 280:   return {"status": "Low",    "color": "#e74c3c", "remark": "Nitrogen content is lower than normal. Apply nitrogen at split intervals to improve crop yield."}
        if val < 560:   return {"status": "Ideal",  "color": "#27ae60", "remark": "Nitrogen content is within the ideal range. Maintain current fertilization practices."}
        return              {"status": "High",   "color": "#f39c12", "remark": "Nitrogen content is above normal. Reduce nitrogen application to avoid toxicity."}

    def classify_phosphorus(val):
        if val < 20:    return {"status": "Low",    "color": "#e74c3c", "remark": "Phosphorus is deficient. Apply phosphatic fertilizers before sowing."}
        if val < 60:    return {"status": "Ideal",  "color": "#27ae60", "remark": "Phosphorus is within optimal range. No immediate action needed."}
        return              {"status": "High",   "color": "#f39c12", "remark": "Phosphorus is high. Avoid additional phosphatic fertilizers."}

    def classify_potassium(val):
        if val < 150:   return {"status": "Low",    "color": "#e74c3c", "remark": "Potassium is deficient. Apply potassic fertilizers to improve crop quality."}
        if val < 400:   return {"status": "Ideal",  "color": "#27ae60", "remark": "Potassium is within optimal range."}
        return              {"status": "High",   "color": "#f39c12", "remark": "Potassium is high. Monitor and reduce potassic inputs."}

    def classify_soc(val):
        if val < 0.5:   return {"status": "Low",    "color": "#e74c3c", "remark": "Soil organic carbon is low. Add organic matter or compost to improve soil health."}
        if val < 0.75:  return {"status": "Ideal",  "color": "#27ae60", "remark": "Soil organic carbon is within standard range."}
        return              {"status": "High",   "color": "#2980b9", "remark": "Soil organic carbon is high. Excellent soil health."}

    def classify_ph(val):
        if val < 6.0:   return {"status": "Acidic",          "color": "#e74c3c", "remark": "Soil is acidic. Apply lime to raise pH for better nutrient availability."}
        if val < 6.5:   return {"status": "Slightly Acidic", "color": "#f39c12", "remark": "Slightly acidic soil. Monitor and adjust based on crop requirements."}
        if val < 7.5:   return {"status": "Neutral",         "color": "#27ae60", "remark": "Soil pH is ideal for most crops."}
        if val < 8.0:   return {"status": "Slightly Alkaline","color": "#f39c12","remark": "Slightly alkaline. Consider sulfur application if crops show deficiency."}
        return              {"status": "Alkaline",        "color": "#e74c3c", "remark": "Soil is alkaline. Apply acidifying agents to improve nutrient uptake."}

    n_mean  = round(float(result_df["N_kgha"].mean()), 3)
    p_mean  = round(float(result_df["P_kgha"].mean()), 3)
    k_mean  = round(float(result_df["K_kgha"].mean()), 3)
    soc_mean = round(float(result_df["SOC_pct"].mean()), 3)
    ph_mean = round(float(result_df["pH"].mean()), 2)

    report = {
        "processing_time": round(t_end - t_start, 2),
        "workers": size,
        "total_pixels": len(result_df),
        "nitrogen":   {"value": n_mean,   "unit": "kg/ha", "ideal": "280–560",  **classify_nitrogen(n_mean)},
        "phosphorus": {"value": p_mean,   "unit": "kg/ha", "ideal": "20–60",    **classify_phosphorus(p_mean)},
        "potassium":  {"value": k_mean,   "unit": "kg/ha", "ideal": "150–400",  **classify_potassium(k_mean)},
        "soc":        {"value": soc_mean, "unit": "%",     "ideal": "0.5–0.75", **classify_soc(soc_mean)},
        "ph":         {"value": ph_mean,  "unit": "",      "ideal": "6.5–7.5",  **classify_ph(ph_mean)},
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(report, f, indent=2)

    print(f"[Soil Report] {len(result_df)} pixels processed in {t_end - t_start:.2f}s using {size} workers → {OUTPUT_FILE}")
