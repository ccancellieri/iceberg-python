# Phase 5: Testing and Compatibility Strategy

This document outlines the testing strategies for the components of the Scalable Geospatial Platform and provides an example Docker Compose configuration for local development and testing.

## 1. General Testing Principles

*   **Automation:** Strive for automated tests where possible (unit, integration).
*   **Isolation:** Unit tests should test components in isolation. Integration tests verify interactions.
*   **Reproducibility:** Tests should be reproducible with consistent outcomes.
*   **Coverage:** Aim for good test coverage of critical logic.
*   **CI/CD Integration:** Automated tests should be part of a CI/CD pipeline.

## 2. Testing Strategies per Component

### 2.1. Part 1: Database (PostgreSQL/AlloyDB)

*   **Local Testing Environment:** PostgreSQL with PostGIS extension (via Docker).
*   **Tests:**
    *   **Schema Validation:**
        *   SQL scripts to verify table creation, column names, data types, constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY (if any), CHECK), and default values as defined in `geospatial_database_design.md`.
        *   Verify index creation (GIST, BTREE, BRIN) on correct columns and types.
    *   **Temporal Logic Tests (SQL-based):**
        *   Scripts to insert sample data representing different versions of features.
        *   Queries to test selection of "current" versions (e.g., `WHERE valid_to = 'infinity'`).
        *   Queries to test "as-of" selections (e.g., `WHERE valid_from <= T AND valid_to > T`).
        *   Queries to test period overlap selections.
    *   **Data Integrity Tests:**
        *   Test `CHECK (valid_from < valid_to)`.
        *   Test `geoid` uniqueness.
    *   **Function/Trigger Tests (if any were DB-side):**
        *   The current design moved H3/S2 to Python. If any utility functions or complex default value logic remained in SQL, they'd be tested here. (e.g., `gen_random_uuid()` for `geoid` is a built-in DB function).
    *   **Performance Benchmarks (Optional, more for pre-production):**
        *   Use `EXPLAIN ANALYZE` on key query patterns with sample large datasets to ensure indexes are used effectively. This is more relevant in a staging environment mimicking production.
*   **Tools:** `psql`, Python with `psycopg2` for test automation, `pgTAP` (for advanced DB unit testing).

### 2.2. Part 2: Ingestion Pipeline (Python on Cloud Run)

*   **Local Testing Environment:** Python environment, access to a local PostGIS DB (from Docker Compose), local file system for Parquet, GCS emulator (e.g., `fsouza/fake-gcs-server`) or mocked GCS client.
*   **Unit Tests (e.g., using `pytest`):**
    *   **Config Parsing:** Test loading and validation of `ingestion_config.json`.
    *   **Column Mapping:** Test the logic that maps Parquet columns to DB schema fields, including renaming and attribute collection.
    *   **Geometry Processing (`geometry_processor.py`):**
        *   Test WKT/WKB parsing using `shapely`.
        *   Test `make_valid` logic with sample invalid and valid geometries.
        *   Test 2D stripping.
        *   Mock `shapely` functions if testing higher-level logic that uses them.
    *   **Content Hashing (`geometry_processor.py`):** Test SHA256 hash generation for known WKB inputs.
    *   **Spatial Indexing (`spatial_indexer.py`):**
        *   Unit test H3/S2 calculation logic for points, lines, polygons (centroid-based for now).
        *   Mock `h3` and `s2geometry` libraries to test the calling logic and data transformation, rather than the core algorithms of these libraries.
        *   Test the parallel execution wrapper (`ProcessPoolExecutor`) with mock functions.
    *   **Temporal DB Logic (`db_writer.py`):**
        *   Test the SQL construction for ending old versions and inserting new ones.
        *   Test logic for `update_current_version_if_exists` mode.
        *   Mock `psycopg2` connection/cursor to verify SQL queries and parameters without hitting a real DB.
    *   **Reporting (`reporter.py`):** Test accumulation of status records and formatting for CSV/JSON output.
*   **Integration Tests:**
    *   **File to DB (Simplified):**
        *   Provide a small sample Parquet file.
        *   Run the core ingestion script logic against a live local PostGIS DB.
        *   Verify that data is inserted correctly, temporal versions are managed, H3/S2/hash/flags are populated, and `ingestion_job_id` is set.
    *   **GCS Interaction:**
        *   Test reading Parquet from a GCS emulator or mocked GCS client.
        *   Test writing the final report to the GCS emulator/mock.
*   **End-to-End Test (Conceptual for Cloud Run Job):**
    *   Deploy the ingestion Cloud Run Job.
    *   Upload a test Parquet file to a test GCS bucket.
    *   Trigger the job (e.g., manually or via a test Pub/Sub message if that wrapper is built).
    *   Verify data in a test AlloyDB/PostgreSQL instance in the cloud.
    *   Verify the report file in GCS.
*   **Tools:** `pytest`, `pytest-mock`, `psycopg2`, `gcsfs` with emulator, `shapely`, `h3`, `s2geometry`.

### 2.3. Part 3: FastAPI WFS-Like Service (Python on Cloud Run)

*   **Local Testing Environment:** FastAPI test client (`httpx`), local PostGIS DB, mocked BigQuery client.
*   **Unit Tests (`pytest`):**
    *   **Request Validation:** Test Pydantic models for `GetFeatureQueryParams` with valid and invalid inputs.
    *   **Query Building (`query_builder.py` - mock `asyncpg`):**
        *   Test translation of BBOX parameter to SQL.
        *   Test temporal filter (`TIME` parameter) translation to SQL.
        *   Test H3/S2 filter translation.
        *   Test `FEATUREID` (geoid list) filter.
        *   **CQL Parsing (Critical):** Unit test the CQL parser and its translation to SQL WHERE clauses for the supported subset of operators (attributes, `ST_DWithin`, `INTERSECTS`, etc.). Focus on correctness and SQL injection prevention (parameterization).
    *   **BigQuery Parameter Construction:** Test how BQ join parameters are used to build BQ queries (mock BQ client).
    *   **Output Formatting (`output_formatter.py` - mock data):**
        *   Unit test GeoJSON, CSV, Parquet streaming logic with sample data.
        *   Unit test Shapefile/GeoPackage in-memory generation logic.
*   **Integration Tests:**
    *   **API to DB (FastAPI TestClient, live local PostGIS):**
        *   Send requests to `/GetFeature` with various filter combinations.
        *   Verify that the correct SQL is generated (by capturing/logging it or inspecting a mock DB layer).
        *   Verify that the response structure and data (from local DB) are correct for different `OUTPUTFORMAT` values.
    *   **BigQuery Join Logic (FastAPI TestClient, local PostGIS, mocked BQ client):**
        *   Test the flow: Get PostGIS results -> extract join keys -> mock BQ client returns data -> verify merged output.
*   **End-to-End API Tests (Conceptual for Cloud Run Service):**
    *   Deploy the FastAPI service to Cloud Run.
    *   Use `curl` or an API testing tool (e.g., Postman, `httpx` script) to hit the deployed endpoints with various valid and invalid parameters.
    *   Verify responses, status codes, and performance.
*   **Tools:** `pytest`, `httpx`, `FastAPI.TestClient`, `asyncpg` (for type hints, mock target), `google-cloud-bigquery` (mock target), `pyogrio`, `pyarrow`.

### 2.4. Part 4: PMTiles Generation (Cloud Run Jobs & Workflow)

*   **Local Testing Environment:** Docker with `tippecanoe`, `pmtiles` CLI, Python environment, GCS emulator or mocked GCS.
*   **Unit Tests (`pytest`):**
    *   **Task Generation Logic (if any Python helper is used for complex splitting):** Test the logic that defines spatial splits and zoom segments. (The current design aims for this in Workflow YAML, which is harder to unit test directly).
    *   **Tippecanoe/PMTiles CLI Parameter Generation:** Test Python code that constructs command-line arguments for these tools based on config.
    *   **Parquet to GeoJSONSeq Conversion Script:** Unit test this conversion logic.
*   **Component Integration Tests (Individual Cloud Run Jobs - test locally via Docker if possible):**
    *   **MVT Generation Job:**
        *   Input: Sample Parquet, task definition (BBOX, zooms).
        *   Process: Run the job's script locally.
        *   Output: Verify correctness of generated GeoJSONSeq (to local file/GCS emulator) and MVT tile directory structure (local output, then check `gsutil rsync` separately if that's part of the job).
    *   **MVT Consolidation Job:**
        *   Input: Multiple sample MVT directories (in GCS emulator or local fs).
        *   Process: Run the job's script (which calls `gsutil rsync`).
        *   Output: Verify consolidated MVT directory.
    *   **PMTiles Assembly Job:**
        *   Input: Consolidated MVT directory (or sample MBTiles).
        *   Process: Run job's script (calls `tile-join`, `pmtiles convert`).
        *   Output: Verify final PMTiles file (e.g., using `pmtiles show` or loading in a viewer).
*   **Cloud Workflows Orchestration Tests:**
    *   **Syntax Validation:** Use `gcloud workflows validate`.
    *   **Unit Testing Workflow Logic (Limited):** Workflow YAML itself is declarative. Testing involves:
        *   Testing expressions and variable assignments with known inputs.
        *   Testing conditional logic by forcing branches.
    *   **Integration Testing with Mocked Jobs:**
        *   Deploy the workflow.
        *   Replace actual Cloud Run Job calls with calls to mock HTTP services (e.g., Cloud Functions or simple Cloud Run services) that simulate job success/failure and expected outputs.
        *   Verify workflow branching, error handling, retry logic, parallel execution, and final reporting.
    *   **End-to-End Test (Small Scale):**
        *   Deploy the full workflow and actual Cloud Run Job containers.
        *   Use a very small Parquet input file and a simple tiling configuration (e.g., few grid cells, few zoom levels).
        *   Trigger the workflow and verify it completes successfully, producing a valid PMTiles file and report in GCS.
*   **Tools:** `pytest`, Docker, `tippecanoe` CLI, `pmtiles` CLI, `gsutil`, `gcloud workflows deploy/execute`, GCS emulator.

## 3. Example `docker-compose.yml` for Local Development

This provides a local environment for testing the database, ingestion, and API components. PMTiles workflow testing is more complex to fully replicate locally.

```yaml
version: '3.8'

services:
  # 1. PostgreSQL with PostGIS
  postgis_db:
    image: postgis/postgis:15-3.4 # Use a recent version
    container_name: local_postgis
    environment:
      POSTGRES_DB: gis_geospatial_dev
      POSTGRES_USER: testuser
      POSTGRES_PASSWORD: testpassword
    ports:
      - "54320:5432" # Map to a non-default host port to avoid conflicts
    volumes:
      - postgis_data_vol:/var/lib/postgresql/data
      - ./scripts/db_init:/docker-entrypoint-initdb.d # For schema.sql
    restart: unless-stopped

  # 2. Ingestion Service (Conceptual - run Python script manually against local DB)
  # No dedicated service, but you'd run your Part 2 Python script from your dev environment,
  # configured to connect to 'localhost:54320' (postgis_db service).
  # You might mount your project code into a generic Python container for this.

  # 3. FastAPI WFS-like Service
  wfs_api_service:
    build:
      context: ./path_to_your_fastapi_app # Directory containing FastAPI app and its Dockerfile
      dockerfile: Dockerfile # Assuming a Dockerfile for the Part 3 service
    container_name: local_wfs_api
    depends_on:
      - postgis_db
    environment:
      DATABASE_URL: "postgresql://testuser:testpassword@postgis_db:5432/gis_geospatial_dev" # Internal Docker network
      BIGQUERY_PROJECT: "mock-bq-project" # For local testing, BQ calls would be mocked
      # Other necessary environment variables for the API
    ports:
      - "8001:8000" # Map FastAPI app port (e.g., 8000) to host port 8001
    volumes:
      - ./path_to_your_fastapi_app:/app # Mount code for live reload if dev server supports it
    restart: unless-stopped

  # 4. GCS Emulator (Optional, for testing GCS interactions locally)
  fake_gcs_server:
    image: fsouza/fake-gcs-server:latest
    container_name: local_fake_gcs
    ports:
      - "4443:4443" # Default port for fake-gcs-server
    volumes:
      - ./local_gcs_data:/data # Persist data for fake GCS server
    entrypoint: /bin/fake-gcs-server -scheme http -host 0.0.0.0 -port 4443 -data /data -public-host localhost # Run with http for easier local dev

  # 5. PMTiles Tools (Optional - for local execution of tippecanoe/pmtiles CLI)
  # You would typically install these tools on your host machine or use a Docker container
  # that has them pre-installed for ad-hoc local PMTiles generation tests.
  # Example:
  # pmtiles_tools:
  #   image: your_custom_image_with_tippecanoe_and_pmtiles # Or use a public one if suitable
  #   container_name: local_pmtiles_tools
  #   volumes:
  #     - ./sample_data:/data # Mount sample data for tiling
  #   entrypoint: /bin/bash # To run commands manually

volumes:
  postgis_data_vol:

# To use this:
# 1. Create ./scripts/db_init/schema.sql with the DDL from geospatial_database_design.md
# 2. Create ./path_to_your_fastapi_app with your FastAPI code and a Dockerfile.
# 3. Create ./local_gcs_data directory.
# 4. Run `docker-compose up -d`
# Your services will be accessible:
# - PostGIS: localhost:54320
# - WFS API: localhost:8001
# - Fake GCS: http://localhost:4443 (configure Python GCS clients to use this endpoint for local tests)
```

**`./scripts/db_init/01_schema.sql` (Example Content):**
```sql
-- Paste the full DDL from geospatial_database_design.md here
-- Including CREATE TABLE geometries, CREATE INDEX statements, etc.
-- Ensure any necessary extensions like btree_gist are created if not default:
-- CREATE EXTENSION IF NOT EXISTS btree_gist;
```

## 4. AlloyDB vs. PostgreSQL Compatibility

*   **High Compatibility:** AlloyDB maintains high compatibility with PostgreSQL. The SQL DDL (tables, columns, types, most indexes) and DML (queries) designed for PostgreSQL should work on AlloyDB with minimal to no changes.
*   **Extensions:** The primary difference encountered was H3/S2 extensions not being available on AlloyDB, which led to the Python-side calculation strategy. This design is now robust to that. Ensure any other non-standard extensions are checked for AlloyDB compatibility if introduced later.
*   **Performance Characteristics:** While SQL is compatible, performance characteristics can differ. AlloyDB's architecture (storage, caching, columnar engine) may optimize certain queries differently. Final performance tuning and testing should ideally occur on an AlloyDB instance.
*   **Testing Locally (PostgreSQL) vs. Cloud (AlloyDB):**
    *   Local `docker-compose` setup uses PostgreSQL. This is excellent for functional testing, CI, and development.
    *   Staging/Pre-production environments should use AlloyDB to catch any subtle behavioral differences or to perform realistic performance testing.

This testing strategy aims to provide confidence in each component's correctness and the overall system's integration.
```
