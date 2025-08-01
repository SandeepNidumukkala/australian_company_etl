CREATE INDEX idx_raw_common_crawl_url ON raw_common_crawl(url);
CREATE INDEX idx_raw_abr_abn ON raw_abr(abn);
CREATE INDEX idx_unified_companies_abn ON unified_companies(abn);
CREATE INDEX idx_unified_companies_company_name ON unified_companies(company_name);