import requests
import json
import os
from datetime import datetime
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils.helpers import get_config, setup_logging

logger = setup_logging()

def fetch_job_data(query, location="India"):
    url = "https://google.serper.dev/search"
    api_key = get_config("SERPER_API_KEY")

    if not api_key:
        logger.error("SERPER_API_KEY not found in .env file!")
        return None

    payload = json.dumps({
        "q": f"{query} jobs {location}",
        "gl": "in",
        "hl": "en",
        "num": 10,
        "type": "search"
    })

    headers = {
        'X-API-KEY': api_key,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=15)
        response.raise_for_status()
        data = response.json()

        jobs = data.get("jobs", [])
        if not jobs:
            logger.warning(f"No 'jobs' block returned for '{query}' — falling back to organic results")
            jobs = data.get("organic", [])

        logger.info(f"Found {len(jobs)} results for '{query}'")
        return jobs

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for '{query}': {e}")
        return None

def save_raw_data(jobs, query):
    os.makedirs("data/1_bronze_raw", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    clean_query = query.replace(" ", "_").lower()
    filename = f"data/1_bronze_raw/raw_{clean_query}_{timestamp}.json"

    payload = {
        "query": query,
        "fetched_at": datetime.now().isoformat(),
        "total_results": len(jobs),
        "jobs": jobs
    }

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)
        logger.info(f"Saved {len(jobs)} results → {filename}")
    except Exception as e:
        logger.error(f"Failed to save file: {e}")

if __name__ == "__main__":
    target_roles = [
        "Junior Data Analyst",
        "Associate Software Engineer",
        "Trainee Data Engineer",
        "Graduate SDE",
        "Junior Business Analyst",
        "Graduate Engineer Trainee",
        "Graduate Software Engineer",
        "Graduate Data Analyst",
        "Graduate Data Engineer",
        "Entry Level Data Engineer",
    ]

    location = get_config("SEARCH_LOCATION") or "India"
    logger.info("--- Starting Daily Job Ingestion ---")

    for role in target_roles:
        logger.info(f"Searching: '{role}'")
        jobs = fetch_job_data(role, location)
        if jobs:
            save_raw_data(jobs, role)
        time.sleep(1) 

    logger.info("--- Ingestion Complete ---")