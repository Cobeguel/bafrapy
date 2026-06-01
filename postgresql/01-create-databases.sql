SELECT 'CREATE DATABASE intranet'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'intranet'
)\gexec

SELECT 'CREATE DATABASE dagster'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'dagster'
)\gexec

SELECT 'CREATE DATABASE ducklake'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'ducklake'
)\gexec