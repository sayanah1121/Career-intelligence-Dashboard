-- ==========================================
-- DIMENSION TABLES
-- ==========================================

-- 1. Company Dimension
CREATE TABLE dim_company (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) UNIQUE NOT NULL
);

-- 2. Location Dimension
CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(255) UNIQUE NOT NULL
);

-- 3. Skill Dimension
CREATE TABLE dim_skill (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) UNIQUE NOT NULL,
    github_repos BIGINT
);

-- ==========================================
-- FACT TABLE
-- ==========================================

-- 4. Job Posting Fact Table
CREATE TABLE fact_job_posting (
    job_id SERIAL PRIMARY KEY,
    job_title VARCHAR(255) NOT NULL,
    company_id INT REFERENCES dim_company(company_id),
    location_id INT REFERENCES dim_location(location_id),
    min_experience_years INT,
    complexity_score INT,
    is_remote BOOLEAN,
    processed_date DATE
);

-- ==========================================
-- BRIDGE TABLE (Many-to-Many Resolution)
-- ==========================================

-- 5. Job-Skill Bridge Table
CREATE TABLE bridge_job_skill (
    job_id INT REFERENCES fact_job_posting(job_id),
    skill_id INT REFERENCES dim_skill(skill_id),
    PRIMARY KEY (job_id, skill_id)
);