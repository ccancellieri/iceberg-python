# Part 3: FastAPI WFS-Like Geospatial Service (Revised)

This document outlines the **revised design** for a Python FastAPI-based microservice that exposes a WFS-like REST interface. It will serve geospatial features from the **temporal PostGIS/AlloyDB database** (designed in revised Part 1, populated by revised Part 2), support enhanced CQL filtering, joins with BigQuery tables, and multiple output formats.

## 1. Core Functional Requirements (Revised)

*   **WFS-like Endpoints:** `/GetCapabilities`, `/GetFeature`, `/DescribeFeatureType`.
*   **Input Filtering:**
    *   Standard: BBOX.
    *   Temporal: `TIME` parameter for querying versions (latest, as-of, range).
    *   Spatial: H3/S2 cell ID filters (on Python-populated columns), `ST_DWithin` via CQL.
    *   Attribute: CQL for general attributes, `geoid`, `content_hash`, `ingestion_job_id`.
*   **BigQuery Joins:** Augment feature data with attributes from BigQuery tables (backend join strategy).
*   **Output Formats:**
    *   Streaming: GeoJSON, CSV, Parquet.
    *   In-memory (size-limited): Shapefile, GeoPackage.
*   **CRS Transformation:** Support output in different coordinate reference systems.
*   **Asynchronous Operations:** FastAPI with `asyncpg`.

## 2. API Endpoints

### 2.1. `/GetCapabilities` (GET)
*   **Response:** XML or JSON.
*   **Content:** Will now also advertise:
    *   Support for temporal queries via the `TIME` parameter.
    *   Queryable fields including `geoid`, `content_hash`, `ingestion_job_id`, and H3/S2 level columns.
    *   Supported CQL predicates (basic attributes, key spatial functions like `INTERSECTS`, `DWITHIN`).

### 2.2. `/DescribeFeatureType` (GET)
*   **Response:** JSON Schema or GML Application Schema.
*   **Content:** Reflect the full temporal schema of the `geometries` table, including `id`, `external_id`, `geoid`, `transaction_time`, `valid_from`, `valid_to`, H3/S2 columns, `ingestion_job_id`, `was_geom_fixed`, `content_hash`.

### 2.3. `/GetFeature` (GET) - Primary Data Endpoint

*   **Revised/Confirmed Request Parameters:**
    *   `TYPENAME`: e.g., `geometries`.
    *   `OUTPUTFORMAT`: `application/geo+json` (default), `application/vnd.shp+zip`, `application/vnd.sqlite3+geopackage`, `text/csv`, `application/vnd.apache.parquet`.
    *   `SRSNAME`: Target output CRS.
    *   `BBOX`: Standard bounding box filter.
    *   `CQL_FILTER`: ECQL text. Key supported predicates:
        *   Attribute: `=`, `<`, `>`, `LIKE`, `IN`, `IS NULL` on fields like `external_id`, `geom_type`, `attributes->'key'`, `geoid`, `content_hash`, `ingestion_job_id`.
        *   Spatial: `INTERSECTS`, `DWITHIN`, `CONTAINS`, `BBOX` (can also be via BBOX param).
    *   `H3_CELL_IDS`, `H3_RESOLUTION`, `S2_CELL_IDS`, `S2_RESOLUTION`: For filtering on pre-computed H3/S2 columns.
    *   `TIME`: Temporal filter (e.g., `latest`, ISO timestamp for "as-of", ISO interval for "overlaps").
    *   `BIGQUERY_JOIN_TABLE`, `BIGQUERY_JOIN_KEY_POSTGIS`, `BIGQUERY_JOIN_KEY_BQ`, `BIGQUERY_SELECT_COLUMNS`: For BigQuery join.
    *   `LIMIT`, `OFFSET`: For pagination.
    *   `FEATUREID`: Optional, comma-separated list of `geoid` values to fetch specific features.

## 3. Query Logic and Backend Interaction (Revised)

### 3.1. Dynamic SQL Query Building

The query builder is critical and must now handle:

1.  **Base Table:** `geometries`.
2.  **Temporal Filtering (Priority):** Based on `TIME` parameter.
    *   `latest`: `AND valid_to = 'infinity'`
    *   `as-of T`: `AND valid_from <= T AND valid_to > T`
    *   `overlaps T1/T2`: `AND TSRANGE(valid_from, valid_to, '[)') && TSRANGE(T1, T2, '[)')`
3.  **Feature ID Filter:** If `FEATUREID` (list of `geoid`s) is provided: `AND geoid = ANY(%(geoid_array)s)`.
4.  **Spatial Filtering:**
    *   `BBOX`: `AND geom && ST_Transform(ST_MakeEnvelope(minx, miny, maxx, maxy, bbox_srid), storage_srid)`
    *   H3/S2: `AND h3_lvlX = ANY(%(h3_ids_array)s)` (using H3/S2 columns populated by Python).
5.  **CQL Filter Translation:**
    *   Parse CQL. Translate recognized predicates to SQL WHERE clauses.
    *   `geoid = '...'` -> `AND geoid = %(uuid_val)s`
    *   `content_hash = '...'` -> `AND content_hash = %(hash_val)s`
    *   `ingestion_job_id = '...'` -> `AND ingestion_job_id = %(job_id_val)s`
    *   `DWITHIN(geom, POINT(x y), distance, units)` -> `AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint(x,y), query_srid), distance_in_geom_units)` (units conversion might be needed).
    *   Safely parameterize all values.
6.  **Column Selection:**
    *   Select all necessary fields from `geometries` table.
    *   For geometry output: `ST_AsGeoJSON(ST_Transform(geom, target_srid))` or `ST_AsBinary(ST_Transform(geom, target_srid))`.
7.  **Ordering & Pagination:** `ORDER BY id` (or `geoid`) then `LIMIT`/`OFFSET`.

### 3.2. BigQuery Joins

*   Strategy remains: FastAPI queries PostGIS, gets join keys (e.g., `external_id` or `geoid`), queries BQ, merges results.
*   The PostGIS query will already be filtered (temporally, spatially, by attributes).

### 3.3. Output Format Generation
*   **GeoJSON, CSV, Parquet:** Streaming as previously designed.
*   **Shapefile, GeoPackage:** In-memory using `pyogrio`/`geopandas` with size limits enforced by the service (e.g., max N features). If limit is exceeded, API returns an error or link to an async export mechanism (future enhancement).

## 4. Technology Stack & Key Libraries (Additions/Changes)
*   **CQL Parser:** Focus on a robust solution for the chosen subset of ECQL. `pycql-ast` is a good candidate to evaluate. If its translation to SQL is complex, a more direct mapping of specific supported filter types might be initially implemented.
*   **`pyogrio`:** Preferred for GeoPackage/Shapefile I/O due to its directness and performance.

## 5. Conceptual Python Snippets (Updates)

The structure of `main:app` with endpoints remains. Key changes are in the query builder logic.

```python
# query_builder.py (conceptual)
# class QueryBuilder:
#     def __init__(self, params: GetFeatureRequestParamsModel):
#         self.params = params
#         self.sql_parts = ["SELECT ... FROM geometries"] # Select list includes ST_AsGeoJSON etc.
#         self.where_clauses = []
#         self.query_params = {} # For asyncpg parameter substitution
#         self.param_counter = 1 # For naming parameters like $1, $2

#     def _add_param(self, value):
#         key = f"p{self.param_counter}"
#         self.query_params[key] = value
#         self.param_counter += 1
#         return f"${{{key}}}" # asyncpg uses $1, $2 - adjust generation

#     def _build_temporal_filter(self):
#         # if self.params.TIME == "latest":
#         #    self.where_clauses.append("valid_to = 'infinity'")
#         # elif "as of": ... add to self.where_clauses and self.query_params
#         pass

#     def _build_featureid_filter(self):
#         # if self.params.FEATUREID:
#         #    geoid_list = self.params.FEATUREID.split(',')
#         #    param_name = self._add_param(geoid_list)
#         #    self.where_clauses.append(f"geoid = ANY({param_name}::uuid[])")
#         pass

#     def _build_cql_filter(self):
#         # if self.params.CQL_FILTER:
#         #    # Parse CQL_FILTER
#         #    # Translate to SQL, adding to self.where_clauses and self.query_params
#         #    # Example for DWITHIN:
#         #    # self.where_clauses.append(f"ST_DWithin(geom, ST_SetSRID(ST_MakePoint({self._add_param(x)}, {self._add_param(y)}), {self._add_param(srid)}), {self._add_param(dist)})")
#         pass

#     # ... other filter methods (BBOX, H3/S2) ...

#     def build_sql(self):
#         # self._build_temporal_filter()
#         # self._build_featureid_filter()
#         # self._build_cql_filter()
#         # ... call all filter builders ...
#         # if self.where_clauses:
#         #    self.sql_parts.append("WHERE " + " AND ".join(self.where_clauses))
#         # Add ORDER BY, LIMIT, OFFSET
#         # return " ".join(self.sql_parts), self.query_params
#         pass
```

## 6. Key Challenges & Considerations (Reiteration)
*   **Secure CQL to SQL translation** is paramount for the chosen subset.
*   Performance of BigQuery joins for many keys.
*   Memory limits for Shapefile/GeoPackage.
*   Complexity of temporal queries must be well-encapsulated.

This revised design for Part 3 incorporates the ability to query new fields from the temporal database and enhances filtering capabilities.
```
