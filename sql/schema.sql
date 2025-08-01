CREATE TABLE raw_common_crawl (
    id SERIAL PRIMARY KEY,
    url VARCHAR(2048) NOT NULL,
    company_name VARCHAR(255),
    industry VARCHAR(1000),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw_abr (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(11) NOT NULL,
    entity_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(50),
    entity_status VARCHAR(50),
    address VARCHAR(255),
    postcode VARCHAR(10),
    state VARCHAR(10),
    start_date DATE,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE unified_companies (
    unified_id SERIAL PRIMARY KEY,
    abn VARCHAR(11),
    company_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(50),
    entity_status VARCHAR(50),
    address VARCHAR(255),
    postcode VARCHAR(10),
    state VARCHAR(10),
    start_date DATE,
    industry VARCHAR(100),
    website_url VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE match_log (
    id SERIAL PRIMARY KEY,
    common_crawl_id INT REFERENCES raw_common_crawl(id),
    abr_id INT REFERENCES raw_abr(id),
    unified_id INT REFERENCES unified_companies(unified_id),
    match_confidence FLOAT,
    match_reason TEXT,
    matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);