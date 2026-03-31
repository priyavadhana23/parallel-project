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
    B4  = chunk["B4"].values.astype(float)
    B8  = chunk["B8"].values.astype(float)
    B11 = chunk["B11"].values.astype(float)

    with np.errstate(divide="ignore", invalid="ignore"):
        ndvi = np.where((B8 + B4) != 0,           (B8 - B4) / (B8 + B4),             np.nan)
        lswi = np.where((B8 + B11) != 0,          (B8 - B11) / (B8 + B11),           np.nan)
        savi = np.where((B8 + B4 + 0.5) != 0,     1.5 * (B8 - B4) / (B8 + B4 + 0.5), np.nan)
        bsi  = np.where((B11 + B4 + B8) != 0,     (B11 + B4 - B8) / (B11 + B4 + B8), np.nan)
        msi  = np.where(B8 != 0,                   B11 / B8,                           np.nan)

    chunk = chunk.copy()
    chunk["NDVI"] = ndvi
    chunk["LSWI"] = lswi
    chunk["SAVI"] = savi
    chunk["BSI"]  = bsi
    chunk["MSI"]  = msi

    # Risk flags
    chunk["Drought_Risk"]       = ndvi < 0.2
    chunk["Water_Stress"]       = lswi < 0.2
    chunk["Vegetation_Healthy"] = ndvi >= 0.4
    chunk["Bare_Soil"]          = bsi > 0.0
    chunk["Plant_Water_Stress"] = msi > 1.0

    # Overall alert label
    def label(row):
        if row["Drought_Risk"]:
            return "Drought"
        if row["Water_Stress"]:
            return "Water Stress"
        if row["Vegetation_Healthy"]:
            return "Healthy"
        return "Moderate"

    chunk["Alert"] = chunk.apply(label, axis=1)
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
               "Drought_Risk", "Water_Stress", "Vegetation_Healthy",
               "Bare_Soil", "Plant_Water_Stress",
               "Alert"]].to_csv(OUTPUT_FILE, index=False)

    print(f"[Done] {len(result_df)} rows processed in {t_end - t_start:.2f}s "
          f"using {size} worker(s) → {OUTPUT_FILE}")
