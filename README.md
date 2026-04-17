# 🌾 Parallel Geospatial Analytics — Sat2Farm

Satellite-powered agricultural risk analysis using **MPI parallel computing**.

## Setup Instructions

### 1. Clone the repo
```bash
git clone https://github.com/priyavadhana23/parallel-project.git
cd parallel-project
```

### 2. Install dependencies
```bash
pip install streamlit mpi4py pandas numpy folium streamlit-folium fpdf2
```

### 3. Download CSV data files
The CSV data files are too large for GitHub. Download them from the shared Google Drive link and place them in the project folder:

> 📁 **[Download CSV Files — Google Drive Link Here]**

Required files:
- `Arid_Region_Data.csv`
- `Tropical_Region_Data.csv`
- `California_Region_Data.csv`
- `Kashmir_Region_Data.csv`
- `TamilNadu_Region_Data.csv`

### 4. Run the dashboard
```bash
streamlit run app.py
```

### 5. How to use
1. Select a region from the sidebar
2. Choose parallel mode (Data Parallel or Pipeline Parallel)
3. Set number of MPI workers (1–8)
4. Click **▶ Run Parallel Analysis**
5. After analysis, click **🧪 Soil Analysis Report** tab to generate soil report

## Project Structure
```
parallel-project/
├── app.py                  # Streamlit dashboard
├── process.py              # MPI data parallel processing
├── process_pipeline.py     # MPI pipeline parallel processing
├── soil_report.py          # MPI parallel soil analysis
├── quick_scale.py          # Dataset scaling utility
└── README.md
```

## Parallel Computing Concepts Demonstrated
- **Data Parallelism** — MPI Scatter/Gather pattern
- **Pipeline Parallelism** — Sequential stage workers
- **Amdahl's Law** — Visual efficiency analysis
