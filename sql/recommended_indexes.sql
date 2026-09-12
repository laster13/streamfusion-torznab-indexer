CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_items_raw_title_trgm_idx
ON torrent_items USING gin (raw_title gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_items_normalized_title_trgm_idx
ON torrent_items USING gin ((parsed_data::jsonb->>'normalized_title') gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_items_parsed_title_trgm_idx
ON torrent_items USING gin ((parsed_data::jsonb->>'parsed_title') gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_items_imdb_id_idx
ON torrent_items (imdb_id) WHERE imdb_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_items_tmdb_id_idx
ON torrent_items (tmdb_id) WHERE tmdb_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_items_info_hash_idx
ON torrent_items (info_hash) WHERE info_hash IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_items_type_idx
ON torrent_items (type);
