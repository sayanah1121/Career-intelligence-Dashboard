import os
import json
import glob
import sys
import time
import requests
from datetime import datetime
from google import genai
from google.genai import types
from groq import Groq

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils.helpers import get_config, setup_logging

logger = setup_logging()

github_skill_cache = {}

def get_github_demand_score(skill):
    if skill in github_skill_cache:
        return github_skill_cache[skill]

    token = get_config("GITHUB_TOKEN")
    url = f"https://api.github.com/search/repositories?q={skill}&per_page=1"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            total_count = response.json().get("total_count", 0)
            github_skill_cache[skill] = total_count
            time.sleep(2)
            return total_count
        return 0
    except Exception as e:
        return 0

def call_gemini(prompt, api_key):
    """Instantiates a fresh client with the currently active key."""
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt,
        config=types.GenerateContentConfig(response_mime_type="application/json")
    )
    return json.loads(response.text)

def call_groq(prompt, client):
    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)

def enrich_job_data(job_description, gemini_keys, groq_client):
    if not job_description or len(job_description) < 20:
        return None

    prompt = f"""
    Analyze the following job description or search snippet and extract the data into JSON format.
    Output ONLY a single JSON object (dictionary), NOT a list.
    If a field is not mentioned, use null for numbers or [] for lists. For missing text, use "Unknown".
    
    Fields to extract:
    1. "company_name": String. Name of hiring company.
    2. "location": String. Job location.
    3. "primary_skills": List of technical skills (max 5).
    4. "min_experience_years": Integer.
    5. "is_remote": Boolean (true/false).
    6. "complexity_score": Integer (1-10).
    
    Job Description:
    {job_description}
    """

    result = None
    engine_used = "Unknown"
    
    # ROTATION LOGIC: Try every Gemini key we have before giving up
    for attempt in range(len(gemini_keys)):
        current_key = gemini_keys[attempt]
        try:
            result = call_gemini(prompt, current_key)
            engine_used = f"Gemini (Key {attempt+1})"
            break  # Success! Exit the retry loop
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "503" in error_str:
                logger.warning(f"Gemini Key {attempt+1} exhausted. Rotating to next key...")
                continue # Instantly try the next loop iteration (next key)
            else:
                logger.error(f"Gemini API Error: {e}")
                break

    # FALLBACK: Groq
    if not result:
        logger.warning("All Gemini keys exhausted! Failing over to Groq...")
        try:
            result = call_groq(prompt, groq_client)
            engine_used = "Groq"
        except Exception as groq_e:
            logger.error(f"Groq Fallback failed: {groq_e}")
            return None

    if isinstance(result, list):
        result = result[0] if len(result) > 0 else {}
    if not isinstance(result, dict):
        result = {}

    result["ai_engine_used"] = engine_used
    return result

def process_bronze_to_silver():
    gemini_env = get_config("GEMINI_API_KEY")
    groq_key = get_config("GROQ_API_KEY")

    if not gemini_env or not groq_key:
        logger.error("Missing API Keys in .env file!")
        return

    # Split the comma-separated string into a python list of keys
    gemini_keys = [k.strip() for k in gemini_env.split(",") if k.strip()]
    groq_client = Groq(api_key=groq_key)
    
    os.makedirs("data/2_silver_enriched", exist_ok=True)
    bronze_files = glob.glob("data/1_bronze_raw/*.json")

    for file_path in bronze_files:
        filename = os.path.basename(file_path).replace("raw_", "enriched_")
        save_path = f"data/2_silver_enriched/{filename}"
        
        if os.path.exists(save_path):
            logger.info(f"Skipping {filename} — already processed.")
            continue

        logger.info(f"Processing file: {os.path.basename(file_path)}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            
        jobs = raw_data.get("jobs", [])
        enriched_jobs = []

        for job in jobs:
            description = job.get("description") or job.get("snippet") or ""
            
            # Pass the list of keys instead of a single client
            ai_insights = enrich_job_data(description, gemini_keys, groq_client)
            
            if ai_insights:
                raw_skills = ai_insights.get("primary_skills")
                skills = raw_skills if isinstance(raw_skills, list) else []
                
                skill_market_data = []
                for skill in skills:
                    repo_count = get_github_demand_score(skill)
                    skill_market_data.append({"skill": skill, "github_repos": repo_count})

                job.update({
                    "ai_company": ai_insights.get("company_name", "Unknown"),
                    "ai_location": ai_insights.get("location", "Unknown"),
                    "ai_skills": skill_market_data,
                    "ai_min_exp": ai_insights.get("min_experience_years", 0),
                    "ai_is_remote": ai_insights.get("is_remote", False),
                    "ai_complexity": ai_insights.get("complexity_score", 5),
                    "ai_engine": ai_insights.get("ai_engine_used", "Unknown")
                })
            else:
                job.update({
                    "ai_company": "Unknown", "ai_location": "Unknown", "ai_skills": [], 
                    "ai_min_exp": None, "ai_is_remote": False, "ai_complexity": None, "ai_engine": "Failed"
                })
            enriched_jobs.append(job)
            
            # Base throttle 
            time.sleep(3) 

        payload = {
            "query": raw_data.get("query"),
            "processed_at": datetime.now().isoformat(),
            "total_jobs": len(enriched_jobs),
            "jobs": enriched_jobs
        }
        
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Saved enriched file to: {save_path}")

if __name__ == "__main__":
    logger.info("--- Starting Multi-Engine AI Enrichment with Key Rotation ---")
    process_bronze_to_silver()