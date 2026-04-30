-- ==========================================
-- 1. EXECUTIVE DASHBOARD KPIs
-- ==========================================
-- Calculates top-level metrics, filtering out hallucinated outliers (experience > 10)
SELECT 
    COUNT(*) AS total_jobs,
    ROUND(SUM(CASE WHEN is_remote = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS remote_percentage,
    ROUND(AVG(complexity_score), 1) AS avg_difficulty,
    ROUND(AVG(CASE WHEN min_experience_years < 10 THEN min_experience_years END), 1) AS avg_experience
FROM fact_job_posting;


-- ==========================================
-- 2. THE "SWEET SPOT" FINDER
-- ==========================================
-- Identifies high-complexity (great for learning) roles that require little to no experience
SELECT 
    f.job_title, 
    c.company_name, 
    l.location_name, 
    f.complexity_score
FROM fact_job_posting f
JOIN dim_company c ON f.company_id = c.company_id
JOIN dim_location l ON f.location_id = l.location_id
WHERE f.min_experience_years <= 1 
  AND f.complexity_score >= 6
ORDER BY f.complexity_score DESC, f.job_title ASC;


-- ==========================================
-- 3. THE "ENTRY-LEVEL TRAP" IDENTIFIER
-- ==========================================
-- Uses Window Functions to find companies posting "entry-level" roles with unreasonably high technical complexity
WITH EntryLevelJobs AS (
    SELECT 
        c.company_name,
        f.job_title,
        f.complexity_score,
        f.min_experience_years
    FROM fact_job_posting f
    JOIN dim_company c ON f.company_id = c.company_id
    WHERE f.min_experience_years <= 1
)
SELECT 
    company_name,
    job_title,
    complexity_score,
    RANK() OVER (PARTITION BY company_name ORDER BY complexity_score DESC) as complexity_rank
FROM EntryLevelJobs
WHERE complexity_score >= 7 
ORDER BY complexity_score DESC;


-- ==========================================
-- 4. REMOTE MARKET VALUE OF SKILLS
-- ==========================================
-- Uses CTEs to find which specific skills give the highest probability of landing a remote job
WITH RemoteJobs AS (
    SELECT job_id 
    FROM fact_job_posting 
    WHERE is_remote = TRUE
),
SkillDemand AS (
    SELECT 
        s.skill_name,
        s.github_repos,
        COUNT(b.job_id) as demand_count
    FROM bridge_job_skill b
    JOIN dim_skill s ON b.skill_id = s.skill_id
    WHERE b.job_id IN (SELECT job_id FROM RemoteJobs)
    GROUP BY s.skill_name, s.github_repos
)
SELECT 
    skill_name,
    demand_count AS remote_jobs_requiring_skill,
    github_repos AS total_open_source_projects
FROM SkillDemand
ORDER BY demand_count DESC, github_repos DESC
LIMIT 10;