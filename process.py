"""
MPI Parallel Geospatial Analytics Engine
Run with: mpiexec -n 4 python3 process.py
"""
import sys
import json
import time
import numpy as np
import pandas as pd
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

DATA_FILE   = sys.argv[1] if len(sys.argv) > 1 else "Arid_Region_Data.csv"
OUTPUT_FILE = sys.argv[2] if len(sys.argv) > 2 else DATA_FILE.replace("_Data.csv", "_results.csv")


def parse_coords(geo_str):
    try:
        coords = json.loads(geo_str)["coordinates"]
        return coords[0], coords[1]  # lon, lat
    except Exception:
        return None, None


def compute_indices(chunk: pd.DataFrame) -> pd.DataFrame:
    import time  # Import here to avoid overhead in imports
    
    B4  = chunk["B4"].values.astype(float)
    B8  = chunk["B8"].values.astype(float)
    B11 = chunk["B11"].values.astype(float)

    with np.errstate(divide="ignore", invalid="ignore"):
        # Original 5 indices
        ndvi = np.where((B8 + B4) != 0,           (B8 - B4) / (B8 + B4),             np.nan)
        lswi = np.where((B8 + B11) != 0,          (B8 - B11) / (B8 + B11),           np.nan)
        savi = np.where((B8 + B4 + 0.5) != 0,     1.5 * (B8 - B4) / (B8 + B4 + 0.5), np.nan)
        bsi  = np.where((B11 + B4 + B8) != 0,     (B11 + B4 - B8) / (B11 + B4 + B8), np.nan)
        msi  = np.where(B8 != 0,                   B11 / B8,                           np.nan)
        
        # Simulate heavy satellite processing per pixel
        for i in range(len(chunk)):
            time.sleep(0.0001)  # 0.1ms per pixel - simulates complex atmospheric correction
        
        # Additional 5 indices for increased computation
        evi  = np.where((B8 + 6*B4 - 7.5*B11 + 1) != 0, 2.5 * (B8 - B4) / (B8 + 6*B4 - 7.5*B11 + 1), np.nan)
        arvi = np.where((B8 + B4 - 2*B11) != 0,   (B8 - (2*B4 - B11)) / (B8 + (2*B4 - B11)),     np.nan)
        gndvi = np.where((B8 + B4) != 0,          (B8 - B4) / (B8 + B4),             np.nan)  # Green NDVI approximation
        sipi = np.where((B8 - B4) != 0,           (B8 - B4) / (B8 - B4 + 0.1),       np.nan)
        gemi = np.where(True, (2 * (B8**2 - B4**2) + 1.5*B8 + 0.5*B4) / (B8 + B4 + 0.5), np.nan)
        
        # Statistical computations for more complexity
        ndvi_smooth = np.convolve(ndvi, np.ones(3)/3, mode='same')  # 3-point moving average
        lswi_smooth = np.convolve(lswi, np.ones(3)/3, mode='same')
        
        # Vegetation stress index (combination of multiple indices)
        vsi = np.where((ndvi + lswi) != 0, (ndvi * lswi) / (ndvi + lswi + 0.1), np.nan)
        
        # Drought severity index (weighted combination)
        dsi = np.where(True, 0.4*ndvi + 0.3*lswi + 0.2*savi + 0.1*msi, np.nan)

    chunk = chunk.copy()
    # Original indices
    chunk["NDVI"] = ndvi
    chunk["LSWI"] = lswi
    chunk["SAVI"] = savi
    chunk["BSI"]  = bsi
    chunk["MSI"]  = msi
    
    # New indices
    chunk["EVI"]  = evi
    chunk["ARVI"] = arvi
    chunk["GNDVI"] = gndvi
    chunk["SIPI"] = sipi
    chunk["GEMI"] = gemi
    chunk["VSI"]  = vsi
    chunk["DSI"]  = dsi
    chunk["NDVI_Smooth"] = ndvi_smooth
    chunk["LSWI_Smooth"] = lswi_smooth

    # Risk flags (using enhanced indices)
    chunk["Drought_Risk"]       = (ndvi < 0.2) | (dsi < 0.3)
    chunk["Water_Stress"]       = (lswi < 0.2) | (vsi < 0.2)
    chunk["Vegetation_Healthy"] = (ndvi >= 0.4) & (savi >= 0.4)
    chunk["Bare_Soil"]          = bsi > 0.0
    chunk["Plant_Water_Stress"] = (msi > 1.0) | (vsi < 0.15)
    chunk["Severe_Drought"]     = (ndvi < 0.1) & (lswi < 0.1) & (dsi < 0.2)

    # Enhanced alert label with more categories
    def enhanced_label(row):
        if row["Severe_Drought"]:
            return "Severe Drought"
        if row["Drought_Risk"]:
            return "Drought"
        if row["Water_Stress"]:
            return "Water Stress"
        if row["Vegetation_Healthy"]:
            return "Healthy"
        return "Moderate"

    chunk["Alert"] = chunk.apply(enhanced_label, axis=1)
    return chunk


# ── Master loads & scatters ──────────────────────────────────────────────────
if rank == 0:
    df = pd.read_csv(DATA_FILE)
    df[["lon", "lat"]] = df[".geo"].apply(
        lambda g: pd.Series(parse_coords(g))
    )
    df = df.dropna(subset=["lon", "lat"]).reset_index(drop=True)

    # Pad so rows divide evenly across workers
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

# ── Each worker computes ─────────────────────────────────────────────────────
local_result = compute_indices(local_chunk)

# ── Gather ───────────────────────────────────────────────────────────────────
all_results = comm.gather(local_result, root=0)

# ── Master saves output ──────────────────────────────────────────────────────
if rank == 0:
    t_end = time.time()
    original_len = len(df)
    result_df = pd.concat(all_results, ignore_index=True)
    result_df = result_df.iloc[:original_len]

    result_df[["lat", "lon", "NDVI", "LSWI", "SAVI", "BSI", "MSI",
               "EVI", "ARVI", "GNDVI", "SIPI", "GEMI", "VSI", "DSI",
               "NDVI_Smooth", "LSWI_Smooth",
               "Drought_Risk", "Water_Stress", "Vegetation_Healthy",
               "Bare_Soil", "Plant_Water_Stress", "Severe_Drought",
               "Alert"]].to_csv(OUTPUT_FILE, index=False)

    print(f"[Done] {len(result_df)} rows processed in {t_end - t_start:.2f}s "
          f"using {size} worker(s) → {OUTPUT_FILE}")
