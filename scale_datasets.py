#!/usr/bin/env python3
"""
Dataset Scaling Script for Parallel Computing Demonstration
Replicates existing satellite data with statistical variations to create larger datasets
for demonstrating parallel computing scalability.
"""
import pandas as pd
import numpy as np
import json
import glob
import os

def add_statistical_noise(df, noise_factor=0.05):
    """Add small statistical variations to satellite band data"""
    df_noisy = df.copy()
    
    # Add noise to satellite bands (B4, B8, B11)
    for band in ['B4', 'B8', 'B11']:
        if band in df.columns:
            original_values = df[band].values
            # Add gaussian noise (5% of original value)
            noise = np.random.normal(0, original_values * noise_factor)
            df_noisy[band] = np.maximum(0, original_values + noise)  # Keep values positive
    
    return df_noisy

def vary_coordinates(geo_str, coord_variation=0.001):
    """Add small variations to GPS coordinates"""
    try:
        geo_data = json.loads(geo_str)
        lon, lat = geo_data["coordinates"]
        
        # Add small random variation (about 100m at equator)
        new_lon = lon + np.random.normal(0, coord_variation)
        new_lat = lat + np.random.normal(0, coord_variation)
        
        geo_data["coordinates"] = [new_lon, new_lat]
        return json.dumps(geo_data)
    except:
        return geo_str

def scale_dataset(input_file, target_rows, output_file):
    """Scale dataset to target number of rows with variations"""
    print(f"Scaling {input_file} to {target_rows:,} rows...")
    
    # Load original data
    df_original = pd.read_csv(input_file)
    original_rows = len(df_original)
    
    if target_rows <= original_rows:
        print(f"Target rows ({target_rows:,}) <= original rows ({original_rows:,}). No scaling needed.")
        return
    
    # Calculate how many replications we need
    replications_needed = int(np.ceil(target_rows / original_rows))
    
    print(f"Creating {replications_needed} replications with variations...")
    
    scaled_chunks = []
    
    # First chunk: original data (no noise)
    scaled_chunks.append(df_original.copy())
    
    # Additional chunks: with statistical variations
    for i in range(1, replications_needed):
        print(f"  Creating variation {i}/{replications_needed-1}...")
        
        # Add noise to satellite bands
        df_variant = add_statistical_noise(df_original, noise_factor=0.03 + i*0.01)
        
        # Vary GPS coordinates slightly
        if '.geo' in df_variant.columns:
            df_variant['.geo'] = df_variant['.geo'].apply(
                lambda x: vary_coordinates(x, coord_variation=0.0005 + i*0.0002)
            )
        
        scaled_chunks.append(df_variant)
    
    # Combine all chunks
    df_scaled = pd.concat(scaled_chunks, ignore_index=True)
    
    # Trim to exact target size
    df_scaled = df_scaled.iloc[:target_rows].reset_index(drop=True)
    
    # Save scaled dataset
    df_scaled.to_csv(output_file, index=False)
    
    print(f"✅ Saved {len(df_scaled):,} rows to {output_file}")
    print(f"   Original: {original_rows:,} → Scaled: {len(df_scaled):,} ({len(df_scaled)/original_rows:.1f}x)")

def main():
    # Target sizes for demonstration
    # Small: ~300k (current), Medium: ~3M, Large: ~10M
    scale_configs = [
        {"multiplier": 10, "suffix": "_3M"},    # ~3 million rows
        {"multiplier": 32, "suffix": "_10M"},   # ~10 million rows
    ]
    
    # Find all region data files
    data_files = glob.glob("*_Region_Data.csv")
    
    if not data_files:
        print("No *_Region_Data.csv files found!")
        return
    
    print(f"Found {len(data_files)} datasets to scale:")
    for f in data_files:
        rows = sum(1 for _ in open(f)) - 1
        print(f"  {f}: {rows:,} rows")
    
    print("\n" + "="*60)
    
    # Scale each dataset
    for data_file in data_files:
        base_name = data_file.replace("_Data.csv", "")
        original_rows = sum(1 for _ in open(data_file)) - 1
        
        for config in scale_configs:
            target_rows = original_rows * config["multiplier"]
            output_file = f"{base_name}{config['suffix']}_Data.csv"
            
            scale_dataset(data_file, target_rows, output_file)
        
        print()
    
    print("="*60)
    print("🎉 Dataset scaling complete!")
    print("\nYou now have:")
    print("  Original datasets: ~300K rows each")
    print("  Medium datasets (*_3M_Data.csv): ~3M rows each") 
    print("  Large datasets (*_10M_Data.csv): ~10M rows each")
    print("\nUse the larger datasets to demonstrate true parallel speedup!")

if __name__ == "__main__":
    main()