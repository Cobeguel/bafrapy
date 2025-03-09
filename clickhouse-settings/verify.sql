SELECT
    name,
    value
FROM system.settings
WHERE name IN ('max_block_size', 'max_threads', 'max_download_threads', 'input_format_parallel_parsing', 'output_format_parallel_formatting')
UNION ALL
SELECT
    name,
    value
FROM system.server_settings
WHERE name = 'mark_cache_size'