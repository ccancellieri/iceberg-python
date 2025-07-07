# Part 2: Cloud Run-Based Parquet Ingestion Pipeline (Revised for Temporal Versioning & Enhanced Features)

This document outlines the **revised design** for a scalable Parquet ingestion pipeline using Google Cloud Run. It incorporates **temporal versioning** into the PostGIS/AlloyDB database (as per revised Part 1), Python-side H3/S2 calculation, content hashing, configurable column mapping, geometry validation, and detailed reporting.

## 1. Core Requirements Addressed in Revision

*   **Temporal Data Model:** Ingest data into the versioned `geometries` table (managing `valid_from`, `valid_to`, `transaction_time`).
*   **Configurable Versioning Behavior:** Allow new data to either create a new version (default) or update the current active version of a feature.
*   **Python-Side H3/S2 Calculation:** Compute H3/S2 cell IDs in the Python script using `h3` and `s2geometry` libraries, with parallelization for batches.
*   **Content Hash:** Compute and store a SHA256 hash of geometry content.
*   **Geometry Validation & Fixing:** Use `shapely` to validate geometries, attempt `make_valid`, and flag if fixed. Optional 2D stripping.
*   **Configurable `ingestion_job_id`:** A single ID (e.g., UUID or custom string from config) for the entire ingestion batch/file.
*   **Flexible Column Mapping:** Map Parquet columns to database columns via configuration.
*   **Detailed Reporting:** Generate a per-row status report for each ingested file.
*   **Direct Script Execution:** Focus `main.py` on being callable with parameters (file path, config), deferring Pub/Sub wrapper.

## 2. Overall Architecture (Conceptual for Direct Script Execution)

1.  **Invocation:** The `main.py` script is invoked with parameters:
    *   `parquet_file_path`: GCS path to the input Parquet file.
    *   `config_file_path` (or direct JSON string/dict): Path to the ingestion configuration JSON.
2.  **Processing Steps in `main.py`:**
    *   **Load Configuration:** Parse the JSON configuration.
    *   **Determine `ingestion_job_id`:** Based on config (`constant` value or `generate_uuid`).
    *   **Initialize:** Setup logging, database connection (pool), GCS filesystem access.
    *   **Reporting:** Prepare a list to store per-source-row status details.
    *   **Parquet Streaming:** Open Parquet file from GCS (`pyarrow.parquet.ParquetFile`). Iterate through row groups or batches (`iter_batches`).
    *   **For each batch of records from Parquet:**
        *   **Column Mapping & Initial Transformation:** Apply mappings defined in config to extract `external_id`, geometry string (WKT/WKB), `geom_type`, and raw attributes. Construct a preliminary list of dictionaries for the batch.
        *   **Geometry Processing (Shapely):** For each record:
            *   Load geometry string to `shapely` object.
            *   If configured, validate (`is_valid`) and fix (`shapely.validation.make_valid`). Set `was_geom_fixed` flag.
            *   If configured, force to 2D (`shapely.ops.transform` or similar).
            *   Store the processed `shapely` object and `was_geom_fixed` status.
        *   **Content Hash Calculation:** For each processed `shapely` geometry, get its WKB_hex, compute SHA256 hash, and store it.
        *   **H3/S2 Calculation (Parallelized):**
            *   Collect all processed `shapely` geometries (or their centroids if that's the strategy for lines/polygons) from the batch.
            *   Use `concurrent.futures.ProcessPoolExecutor` to distribute the calculation of H3/S2 cell IDs (for all configured resolutions) across multiple CPU cores.
            *   Merge the calculated H3/S2 values back into the records.
        *   **Prepare for DB:** Assemble final list of records for database operation, including all new fields (`ingestion_job_id`, `was_geom_fixed`, `content_hash`, H3/S2 values, attributes as JSONB string). Determine `transaction_time` and `valid_from` for the batch based on config (`processing_time` or from a Parquet column).
        *   **Database Temporal Logic & Batch Upsert (within a transaction):**
            *   **If `versioning_behavior: "always_create_new_version"` (default):**
                1.  Collect all unique `external_id`s from the current batch.
                2.  `UPDATE geometries SET valid_to = %(batch_valid_from_time)s WHERE external_id = ANY(%(list_of_external_ids)s) AND valid_to = 'infinity';`
                3.  Batch `INSERT` all new records as new versions (with their `valid_from = batch_valid_from_time`, `valid_to = 'infinity'`, `transaction_time = CURRENT_TIMESTAMP` (DB default)).
            *   **If `versioning_behavior: "update_current_version_if_exists"`:**
                *   For each record, attempt an `UPDATE` on `geometries` for the current active version (`WHERE external_id = ? AND valid_to = 'infinity'`).
                *   If no row is updated, `INSERT` as a new feature (first version for this `external_id`). This is more complex to batch efficiently and might involve row-by-row logic or advanced SQL.
        *   **Track Status:** For each source row, record its `external_id`, final `geoid` (if available from DB insert), status ("inserted_new_version", "updated_current_version", "failed"), `was_geom_fixed`, and any error messages in the report list.
    *   **Finalize & Upload Report:** After all batches are processed, convert the report list to CSV/JSON and upload to GCS using path from config.
    *   **Logging:** Log overall progress, errors, and summary.

## 3. Detailed Design Aspects

### 3.1. JSON Ingestion Configuration (Example)

```json
{
  "ingestion_job_id_source": "generate_uuid", // "constant" or "generate_uuid"
  "ingestion_job_id_value": null, // Value if source is "constant" (e.g., "my_daily_batch_20231026")
  "versioning_behavior": "always_create_new_version", // or "update_current_version_if_exists"
  "effective_timestamp_source": "processing_time", // or "parquet_column" (for valid_from)
  "effective_timestamp_column": "event_time_col", // Parquet column if source is "parquet_column"
  "geometry_processing": {
    "srid": 4326,
    "force_2d": true,
    "fix_invalid_geometries": true
  },
  "column_mapping": {
    "external_id": {"parquet_column": "feature_ID"},
    "geometry_wkt": {"parquet_column": "geometryWKT"}, // Or geometry_wkb_hex
    "geom_type": {"parquet_column": "type_of_geometry"},
    // Attributes: either prefix-based or explicit list
    "attributes_source_type": "prefix", // "prefix" or "explicit_list"
    "attributes_prefix": "prop_", // if "prefix"
    "attributes_explicit_list": [ // if "explicit_list"
        {"parquet_column": "prop_A", "db_key": "propertyA"},
        {"parquet_column": "prop_B", "db_key": "propertyB"}
    ],
    "attributes_rename_map": { // Applied after prefix/list selection
      "prop_originalName": "finalAttributeName"
    }
  },
  "h3_config": {
    "resolutions": [4, 7, 10, 13], // Corresponds to h3_lvl4, h3_lvl7 etc.
    "polygon_strategy": "centroid" // "centroid" or potentially "covering_cell_if_single"
  },
  "s2_config": {
    "resolutions": [6, 10, 15, 20],
    "polygon_strategy": "centroid"
  },
  "output_report": {
    "gcs_path_template": "gs://your-bucket/ingestion_reports/{ingestion_job_id}/{filename_base}_report.csv",
    "include_source_columns": ["feature_ID", "original_event_time"] // Optional: include some original data in report
  },
  "database_batch_size": 500
}
```

### 3.2. Python-Side Processing Modules (Conceptual)

*   **`config_loader.py`**: Loads and validates the JSON configuration.
*   **`parquet_reader.py`**: Handles streaming Parquet data with column mapping.
*   **`geometry_processor.py`**:
    *   Uses `shapely` for WKT/WKB parsing, validation, fixing (`make_valid`), 2D stripping.
    *   Calculates content hash (SHA256 of processed WKB).
*   **`spatial_indexer.py`**:
    *   Uses `h3` and `s2geometry` (or `pys2cells`).
    *   Calculates H3/S2 cell IDs for batches of `shapely` geometries (or centroids).
    *   Uses `concurrent.futures.ProcessPoolExecutor` for parallelization.
*   **`db_writer.py`**:
    *   Handles database connections (`psycopg2`).
    *   Implements temporal logic for `always_create_new_version` (batch UPDATE then batch INSERT).
    *   Implements logic for `update_current_version_if_exists` (more complex, may be row-by-row or small batches with careful SQL).
*   **`reporter.py`**: Accumulates per-row status and writes final report to GCS.
*   **`main.py`**: Orchestrates these components.

### 3.3. H3/S2 Calculation for Lines/Polygons

*   **Initial Strategy: Centroid-based.** For a line or polygon, calculate its centroid (`shapely_geom.centroid`). Use the H3/S2 cell ID of this centroid point for the corresponding `h3_lvlX` / `s2_lvlX` columns.
*   **Pros:** Simple, guarantees one cell ID per resolution.
*   **Cons:** Less accurate for spatial queries on large/complex lines/polygons compared to full coverage.
*   **Future Enhancement:** Could support a strategy to get a full covering set and store it in `BIGINT[]` array columns if schema changes, or select a "primary" cell from the covering set based on area or other heuristics. This is deferred for now.

### 3.4. Temporal Database Interaction Example (`always_create_new_version`)

```python
# In db_writer.py (simplified, using psycopg2 style)
# def handle_batch_temporal_insert(db_cursor, records_batch, batch_valid_from_time, ingestion_job_id, srid):
#     external_ids_in_batch = list(set(r['external_id'] for r in records_batch))

#     # Step 1: End active versions for all relevant external_ids
#     if external_ids_in_batch:
#         update_sql = """
#             UPDATE geometries SET valid_to = %s
#             WHERE external_id = ANY(%s) AND valid_to = 'infinity';
#         """
#         db_cursor.execute(update_sql, (batch_valid_from_time, external_ids_in_batch))

#     # Step 2: Prepare data for new version insertion
#     insert_data_tuples = []
#     for r in records_batch:
#         insert_data_tuples.append((
#             r['external_id'], # external_id
#             # geoid has DB default (use DEFAULT in SQL or omit from column list)
#             batch_valid_from_time, # transaction_time (or use CURRENT_TIMESTAMP in SQL)
#             batch_valid_from_time, # valid_from
#             'infinity', # valid_to
#             psycopg2.Binary(bytes.fromhex(r['processed_wkb_hex'])), # geom (from WKB hex)
#             r['geom_type'],
#             json.dumps(r['attributes']), # attributes
#             r.get('h3_lvl4'), r.get('h3_lvl7'), r.get('h3_lvl10'), r.get('h3_lvl13'),
#             r.get('s2_lvl6'), r.get('s2_lvl10'), r.get('s2_lvl15'), r.get('s2_lvl20'),
#             ingestion_job_id,
#             r['was_geom_fixed'],
#             r['content_hash']
#         ))

#     # Step 3: Batch insert new versions
#     insert_sql_template = f"""
#         INSERT INTO geometries (
#             external_id, transaction_time, valid_from, valid_to,
#             geom, geom_type, attributes,
#             h3_lvl4, h3_lvl7, h3_lvl10, h3_lvl13,
#             s2_lvl6, s2_lvl10, s2_lvl15, s2_lvl20,
#             ingestion_job_id, was_geom_fixed, content_hash
#             -- geoid has DB default
#         ) VALUES (
#             %s, %s, %s, %s,
#             ST_SetSRID(%s, {srid}), %s, %s,  -- Use ST_SetSRID with WKB Binary
#             %s, %s, %s, %s, %s, %s, %s, %s,
#             %s, %s, %s
#         ) RETURNING geoid;
#     """
#     # For psycopg2.extras.execute_values, the template is just the VALUES part:
#     # template = "(%s, %s, ...)"
#     # And the main SQL is "INSERT INTO ... VALUES %s RETURNING geoid"
#     # psycopg2.extras.execute_values(db_cursor, insert_sql_main, insert_data_tuples, template=template_values_part, page_size=len(insert_data_tuples))
#     # geoids_inserted = [row[0] for row in db_cursor.fetchall()]
#     # return geoids_inserted
```

### 3.5. `main.py` Orchestration (Conceptual)
The `main.py` script will be designed to be callable, taking `parquet_gcs_path` and `config_path` (or config object) as arguments. It will orchestrate the steps: load config, determine `ingestion_job_id`, stream Parquet batches, call processing modules (column mapping, geometry processing, content hashing, H3/S2 indexing), call DB writing module (handling temporal logic), and finally call reporting module.

### 3.6. Resource Considerations for Cloud Run
*   **CPU:** Python-side H3/S2 calculation, geometry processing, and parallelization will require more CPU. Configure 1 or more vCPUs.
*   **Memory:** Loading Parquet batches, `shapely` objects, and data for parallel processing will consume memory. Size appropriately (e.g., 1-4 GiB or more).
*   **Timeout:** Max 60 minutes for Cloud Run (if used as a service). For Cloud Run Jobs (better for this), timeouts can be longer (up to 24 hours). This design assumes a Cloud Run Job context for potentially long-running file ingestions.

This revised Part 2 design is significantly more robust and feature-rich. The Python script becomes a sophisticated ETL tool.
```
