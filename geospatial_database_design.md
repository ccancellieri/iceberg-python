# Part 1: AlloyDB / PostgreSQL + PostGIS Configuration (Revised for Temporal Versioning)

This document outlines the **revised** database design for storing and querying massive-scale geospatial data, incorporating **temporal versioning** and other enhancements based on user feedback. It remains compatible with PostgreSQL+PostGIS and Google Cloud AlloyDB.

## 1. Core Requirements Addressed in Revision

*   **Temporal Versioning:** Geometries with the same `external_id` will have multiple versions, tracked over time.
*   **GeoID:** A stable, unique UUID for each geometry version record.
*   **Ingestion Job ID:** Link records to a specific ingestion batch/file. This ID is for the job/batch itself.
*   **Content Hash:** Store a hash of geometry content for duplication analysis.
*   **Geometry Fixing Flag:** Indicate if a geometry was modified by a validation process.
*   **Python-Side H3/S2:** H3/S2 cell IDs will be computed by the application (Python script) and inserted, not by database triggers.

## 2. Revised Schema: `geometries` Table

The `geometries` table will now store historical versions of features.

```sql
CREATE TABLE geometries (
    -- Core Identifiers
    id BIGSERIAL PRIMARY KEY,                      -- Unique surrogate key for each version record
    external_id VARCHAR(255) NOT NULL,           -- Identifier for the conceptual feature across versions
    geoid UUID DEFAULT gen_random_uuid() NOT NULL UNIQUE, -- Stable unique ID for this specific geometry version record

    -- Temporal Versioning Columns
    transaction_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When this version was recorded in the DB
    valid_from TIMESTAMPTZ NOT NULL,             -- Business time: when this version became effective
    valid_to TIMESTAMPTZ DEFAULT 'infinity',     -- Business time: when this version ceased to be effective
                                                 -- Active records have valid_to = 'infinity'

    -- Geometry and Attributes
    geom GEOMETRY NOT NULL,                      -- The actual geometry. SRID must be enforced (e.g., 4326).
                                                 -- For mixed types: GEOMETRY(Geometry, 4326)
    geom_type VARCHAR(50) NOT NULL,              -- 'Point', 'LineString', 'Polygon', etc.
    attributes JSONB,                            -- Flexible feature properties

    -- H3/S2 Index Columns (Populated by Python Ingestion Script)
    h3_lvl4 BIGINT,
    h3_lvl7 BIGINT,
    h3_lvl10 BIGINT,
    h3_lvl13 BIGINT,
    s2_lvl6 BIGINT,
    s2_lvl10 BIGINT,
    s2_lvl15 BIGINT,
    s2_lvl20 BIGINT,

    -- Ingestion & Data Quality Metadata
    ingestion_job_id VARCHAR(255),               -- ID for the ingestion job/file (e.g., UUID or custom string from config)
    was_geom_fixed BOOLEAN DEFAULT FALSE,        -- True if geometry was altered by a fixing process (e.g., make_valid)
    content_hash VARCHAR(64),                    -- SHA256 hash of the geometry content (WKB) for duplication analysis

    CONSTRAINT check_temporal_order CHECK (valid_from < valid_to)
);

COMMENT ON COLUMN geometries.transaction_time IS 'Timestamp of when this version record was created/committed to the database.';
COMMENT ON COLUMN geometries.valid_from IS 'Timestamp indicating the start of the period this geometry version is considered valid in business terms.';
COMMENT ON COLUMN geometries.valid_to IS 'Timestamp indicating the end of the period this geometry version is considered valid. ''infinity'' for current versions.';
COMMENT ON COLUMN geometries.ingestion_job_id IS 'Identifier for the ingestion batch or process that created this version.';
COMMENT ON COLUMN geometries.was_geom_fixed IS 'Flag indicating if the original geometry data was modified by a cleaning/validation process before insertion.';
COMMENT ON COLUMN geometries.content_hash IS 'SHA256 hash of the canonical geometry WKB, used for content duplication analysis.';
```

**Notes on Schema:**
*   `valid_to = 'infinity'` is a common convention for active temporal records.
*   `ingestion_job_id` is a single ID for the entire ingestion run/file. Per-row incremental/composite identifiers (if needed for reporting) are handled by the ingestion script logic and stored in the report, not necessarily directly in this column unless explicitly redesigned for that.

## 3. Indexing Strategy (Revised for Temporal Data)

```sql
-- Primary Key Index (automatically created on id)

-- Index for efficiently finding the current version(s) of a feature
CREATE INDEX idx_geometries_external_id_valid_to_current ON geometries (external_id) WHERE valid_to = 'infinity';
-- If you also need to order by valid_from for current records of the same external_id:
-- CREATE INDEX idx_geometries_external_id_valid_from_current ON geometries (external_id, valid_from DESC) WHERE valid_to = 'infinity';


-- Index for "time-slice" queries (finding version active at a specific point in time)
-- Requires btree_gist extension: CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE INDEX idx_geometries_external_id_valid_period ON geometries USING GIST (external_id, TSRANGE(valid_from, valid_to, '[)'));
-- Query: SELECT * FROM geometries WHERE external_id = 'some_id' AND TSRANGE(valid_from, valid_to, '[)') @> 'YYYY-MM-DD HH:MM:SS'::timestamptz;

-- Spatial Index (essential for geometric queries)
CREATE INDEX idx_geometries_geom_gist ON geometries USING GIST (geom);

-- Consider a partial spatial index for current geometries if frequently queried
CREATE INDEX idx_geometries_geom_current_gist ON geometries USING GIST (geom) WHERE valid_to = 'infinity';

-- BTREE Indexes on H3/S2 columns (populated by Python)
CREATE INDEX idx_geometries_h3_lvl7 ON geometries (h3_lvl7) WHERE h3_lvl7 IS NOT NULL; -- Add others as needed
CREATE INDEX idx_geometries_s2_lvl10 ON geometries (s2_lvl10) WHERE s2_lvl10 IS NOT NULL; -- Add others as needed
-- Adding WHERE IS NOT NULL can be useful if these are sparsely populated.

-- Index on geoid (UNIQUE constraint already creates an index)
-- CREATE UNIQUE INDEX idx_geometries_geoid ON geometries (geoid); -- This is implicitly created by UNIQUE on geoid.

-- Index on ingestion_job_id for tracking data lineage
CREATE INDEX idx_geometries_ingestion_job_id ON geometries (ingestion_job_id);

-- Index on content_hash for finding duplicate geometry content
CREATE INDEX idx_geometries_content_hash ON geometries (content_hash);

-- Index on geometry type if frequently used in filters
CREATE INDEX idx_geometries_geom_type ON geometries (geom_type);

-- BRIN index on transaction_time if physical ordering is correlated and range queries are common
CREATE INDEX idx_geometries_transaction_time_brin ON geometries USING BRIN (transaction_time);
```

## 4. Partitioning

*   **Primary Recommendation: RANGE partitioning by `transaction_time`** (e.g., monthly/yearly partitions). This is excellent for managing historical data growth and archiving.
    ```sql
    -- Example: Main table definition needs to include PARTITION BY RANGE(transaction_time)
    -- CREATE TABLE geometries ( ... ) PARTITION BY RANGE (transaction_time);
    -- Then create partitions, e.g.:
    -- CREATE TABLE geometries_y2023m01 PARTITION OF geometries
    --    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
    ```
*   Other strategies (or sub-partitioning) by H3/S2 cell ID or `external_id` HASH can be considered for specific query optimization if needed later.

## 5. H3/S2 Integration (Python-Side)
*   H3/S2 cell IDs are calculated in the Python ingestion script (Part 2) using `h3` and `s2geometry` libraries.
*   For polygons/lines, the Python script will (initially) calculate the H3/S2 cell of the geometry's centroid.

## 6. Other Considerations
*   Parallel Query Execution, Read Optimization (Materialized Views), AlloyDB vs. PostgreSQL, Docker Compose setup remain relevant as previously discussed.

This revised design provides a robust foundation for temporal geospatial data management.
```
