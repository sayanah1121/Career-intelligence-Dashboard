import os
import sys
import glob
import json
import pandas as pd
from datetime import datetime

# Add root to path for utility imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils.helpers import setup_logging

logger = setup_logging()

def safe_json_dumps(val):
    """Safely converts Python lists/dicts to JSON strings for CSV export."""
    if isinstance(val, (list, dict)):
        return json.dumps(val)
    return "[]"

def clean_string(val):
    """
    Ensures that lists or dicts accidentally returned by AI are flattened to strings.
    This prevents 'unhashable type' errors in Pandas.
    """
    if isinstance(val, list):
        return ", ".join([str(v) for v in val])
    if isinstance(val, dict):
        return json.dumps(val)
    if pd.isna(val) or val is None:
        return "Unknown"
    return str(val).strip()

def process_silver_to_gold():
    """
    Reads enriched JSON data from Silver, flattens the schema using Pandas, 
    removes duplicates, and outputs a pristine CSV for Power BI.
    """
    silver_files = glob.glob("data/2_silver_enriched/*.json")
    if not silver_files:
        logger.warning("No files found in Silver layer. Run ai_enricher.py first.")
        return

    logger.info(f"Reading {len(silver_files)} files from Silver Layer...")
    
    all_jobs = []
    
    for file_path in silver_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                jobs = data.get("jobs", [])
                all_jobs.extend(jobs)
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")

    if not all_jobs:
        logger.warning("No jobs found inside the Silver files.")
        return

    raw_df = pd.DataFrame(all_jobs)
    initial_count = len(raw_df)
    logger.info(f"Loaded {initial_count} total job records.")

    # Safely extract columns using Pandas Series
    titles = raw_df.get("title", pd.Series(["Unknown"] * initial_count))
    ai_comps = raw_df.get("ai_company", pd.Series([None] * initial_count))
    raw_comps = raw_df.get("company", pd.Series(["Unknown"] * initial_count))
    ai_locs = raw_df.get("ai_location", pd.Series([None] * initial_count))
    raw_locs = raw_df.get("location", pd.Series(["Unknown"] * initial_count))

    # 3. Build the Gold Standard DataFrame safely
    gold_data = {
        "Job_Title": titles.apply(clean_string),
        # Use AI company if valid, otherwise fallback to Serper's company
        "Company": ai_comps.where(ai_comps.notna() & (ai_comps != "Unknown"), raw_comps).apply(clean_string),
        "Location": ai_locs.where(ai_locs.notna() & (ai_locs != "Unknown"), raw_locs).apply(clean_string),
        "Min_Experience_Years": pd.to_numeric(raw_df.get("ai_min_exp"), errors='coerce').fillna(0).astype(int),
        "Complexity_Score": pd.to_numeric(raw_df.get("ai_complexity"), errors='coerce').fillna(0).astype(int),
        "Is_Remote": raw_df.get("ai_is_remote", False).astype(bool),
        "AI_Engine_Used": raw_df.get("ai_engine", pd.Series(["Unknown"] * initial_count)).apply(clean_string),
        "Skills_Market_Data": raw_df.get("ai_skills", pd.Series([[]]*initial_count)).apply(safe_json_dumps),
        "Processed_Date": datetime.now().strftime('%Y-%m-%d')
    }
    
    gold_df = pd.DataFrame(gold_data)

    # 4. Deduplication
    gold_df.drop_duplicates(subset=["Job_Title", "Company"], inplace=True)
    final_count = len(gold_df)
    
    logger.info(f"Removed {initial_count - final_count} duplicate job postings.")

    # 5. Save to Gold Layer
    output_dir = "data/3_gold_standard/powerbi_export"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, "gold_standard_jobs.csv")
    
    logger.info("Writing Gold standard CSV...")
    gold_df.to_csv(output_file, index=False, encoding='utf-8')
    
    logger.info(f"Success! Analytics-ready data saved to: {output_file}")

if __name__ == "__main__":
    logger.info("--- Starting Pandas Transformation (Silver -> Gold) ---")
    process_silver_to_gold()
    logger.info("--- Transformation Complete ---")