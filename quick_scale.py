#!/usr/bin/env python3
"""
Quick Dataset Scaling - Creates 1M row datasets for parallel demo
"""
import pandas as pd
import numpy as np
import json

def scale_one_dataset(input_file):
    """Scale one dataset to ~1M rows quickly"""
    print(f"Scaling {input_file}...")
    
    # Load original
    df = pd.read_csv(input_file)
    original_rows = len(df)
    target_rows = 1000000  # 1M rows
    
    # Calculate replications needed
    reps = int(target_rows / original_rows) + 1
    
    # Create variations with small noise
    chunks = []
    for i in range(reps):
        df_copy = df.copy()
        
        # Add 1-3% noise to satellite bands
        noise_level = 0.01 + i * 0.005
        for band in ['B4', 'B8', 'B11']:
            if band in df_copy.columns:
                noise = np.random.normal(0, df_copy[band] * noise_level)
                df_copy[band] = np.maximum(0, df_copy[band] + noise)
        
        chunks.append(df_copy)
        
        if len(pd.concat(chunks)) >= target_rows:
            break
    
    # Combine and trim
    df_large = pd.concat(chunks, ignore_index=True).iloc[:target_rows]
    
    # Save
    output_file = input_file.replace("_Data.csv", "_1M_Data.csv")
    df_large.to_csv(output_file, index=False)
    
    print(f"✅ {input_file} → {output_file} ({len(df_large):,} rows)")

# Scale just one dataset for testing
scale_one_dataset("Arid_Region_Data.csv")