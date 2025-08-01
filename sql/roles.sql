CREATE ROLE etl_user WITH LOGIN PASSWORD 'secure_password';
CREATE ROLE readonly_user WITH LOGIN PASSWORD 'readonly_password';

GRANT CONNECT ON DATABASE company_db TO etl_user, readonly_user;
GRANT USAGE ON SCHEMA public TO etl_user, readonly_user;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

GRANT USAGE, SELECT, UPDATE ON SEQUENCE raw_common_crawl_id_seq TO etl_user;

GRANT INSERT, SELECT, UPDATE ON raw_abr TO etl_user;

GRANT USAGE ON SCHEMA public TO etl_user;
GRANT CREATE ON SCHEMA public TO etl_user;

GRANT SELECT,INSERT,UPDATE ON TABLE unified_companies TO etl_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON unified_companies TO readonly_user;


ALTER TABLE unified_companies OWNER TO etl_user;

ALTER TABLE unified_companies DISABLE ROW LEVEL SECURITY;



