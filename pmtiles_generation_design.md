# Part 4: PMTiles Generation from Parquet (Revised for High Parallelism & Concrete Workflow Logic)

This document outlines the **third revised design** for a scalable pipeline to convert Parquet files into PMTiles v3 archives. This version specifies that **task generation logic is explicitly defined within the Cloud Workflow YAML** and details the MVT consolidation step before final PMTiles assembly.

## 1. Core Requirements Addressed in This Revision

*   **Workflow-Defined Task Generation:** The Cloud Workflow YAML will contain the logic to calculate spatial grid cells and combine them with zoom segments to create a granular list of MVT generation tasks. No external helper service for this core splitting logic.
*   **MVT Consolidation:** A dedicated step to merge MVT tile directories from parallel jobs into a single GCS root MVT directory before final assembly.
*   **Assembly Process:** Confirmed as `tile-join` (from consolidated MVT to MBTiles) followed by `pmtiles convert` (MBTiles to PMTiles).
*   Continued emphasis on Cloud Run Jobs, Cloud Workflows, GCS for intermediate storage, error handling, and reporting.

## 2. Overall Architecture: Cloud Workflows with Explicit Task Splitting

The process remains orchestrated by Google Cloud Workflows:

1.  **Workflow Trigger & Initialization:** (As previously defined)
    *   Inputs: `input_parquet_gcs_path`, `tiling_config_gcs_path`, `output_pmtiles_gcs_path`, `temp_gcs_bucket`, `workflow_execution_id`.

2.  **Stage 1: Configuration Loading & Explicit Task Generation (Workflow Steps)**
    *   Workflow reads `tiling_config.json` from GCS. This config contains:
        *   `spatial_split_config.data_bbox_override`: e.g., `[-180, -90, 180, 90]` (lon_min, lat_min, lon_max, lat_max).
        *   `spatial_split_config.grid_divisions`: e.g., `[8, 8]` (cols, rows for the grid).
        *   `zoom_level_segmentation`: e.g., `[[0,5], [6,8], ..., [20,20]]`.
    *   **Workflow Logic for Task Generation (within the YAML):**
        1.  Calculate grid cell width and height:
            *   `cell_width = (data_bbox_override[2] - data_bbox_override[0]) / grid_divisions[0]`
            *   `cell_height = (data_bbox_override[3] - data_bbox_override[1]) / grid_divisions[1]`
        2.  Use nested loops (e.g., `forEach` with `range` in Workflow syntax if available, or generate a list of indices):
            *   Outer loop for grid rows (`j` from `0` to `grid_divisions[1] - 1`).
            *   Inner loop for grid columns (`i` from `0` to `grid_divisions[0] - 1`).
        3.  Inside the loops, calculate BBOX for current cell `(i, j)`:
            *   `min_lon = data_bbox_override[0] + i * cell_width`
            *   `min_lat = data_bbox_override[1] + j * cell_height`
            *   `max_lon = min_lon + cell_width`
            *   `max_lat = min_lat + cell_height`
            *   `filter_bbox_str = "${min_lon},${min_lat},${max_lon},${max_lat}"`
        4.  Innermost loop iterates through `zoom_level_segmentation`:
            *   For each `zoom_segment = [min_z, max_z]`:
                *   Create a task definition:
                    `{ "task_id": "grid_R${j}_C${i}_Z${min_z}-${max_z}", "filter_bbox_str": filter_bbox_str, "min_zoom_for_task": min_z, "max_zoom_for_task": max_z }`
                *   Add this task to a list `mvt_generation_tasks`.
    *   This list `mvt_generation_tasks` is then used for parallel job dispatch.

3.  **Stage 2: Parallel MVT Tile Generation (Cloud Run Jobs orchestrated by Workflow)**
    *   (Largely as previously defined) Workflow iterates `mvt_generation_tasks`, launches "MVT Generation" Cloud Run Jobs in parallel.
    *   Each job processes its assigned Parquet portion (filtered by `task_filter_bbox`) for its assigned zoom range (`task_min_zoom` to `task_max_zoom`).
    *   Streams GeoJSONSeq to GCS: `gs://{temp_gcs_bucket}/geojsonseq/{workflow_id}/{task_id}.geojsonseq`.
    *   Runs `tippecanoe` (input GeoJSONSeq from GCS, output MVT dir to GCS): `gs://{temp_gcs_bucket}/mvt_tiles/{workflow_id}/{task_id}/z/x/y.pbf`. (Requires `gcsfuse` or equivalent for Tippecanoe to treat GCS paths as directories).

4.  **Stage 3: Error Handling & Retries (Workflow Feature)**
    *   (As previously defined) Cloud Workflows `try/retry` for job invocations with robust polling subworkflow. Collect all results.

5.  **Stage 4: Consolidate MVT Tile Directories (New Dedicated Cloud Run Job by Workflow)**
    *   Launched if a sufficient number of MVT jobs succeed.
    *   **"MVT Consolidation" Cloud Run Job:**
        *   **Input Parameters:** List of successful GCS MVT directory source paths (from Stage 2 results), target consolidated MVT root GCS path (e.g., `gs://{temp_gcs_bucket}/mvt_merged/{workflow_id}/`).
        *   **Processing:**
            *   Uses `gsutil rsync -r -m ...` (multi-threaded recursive copy) to copy all content from source MVT directories into the single target consolidated MVT root directory.
            *   Example: `gsutil -m rsync -r gs://{temp_gcs_bucket}/mvt_tiles/{workflow_id}/task_1/ gs://{temp_gcs_bucket}/mvt_merged/{workflow_id}/` (repeated or scripted for all sources).
            *   This job essentially creates a unified `z/x/y.pbf` structure from all parallel outputs.
        *   **Return Status:** Success/failure, and the GCS path to the consolidated MVT directory.

6.  **Stage 5: Assemble Final PMTiles from Consolidated MVT (Cloud Run Job by Workflow)**
    *   Launched if MVT consolidation succeeds.
    *   **"PMTiles Assembly" Cloud Run Job:**
        *   **Input Parameters:** GCS path to the consolidated MVT root directory, `output_pmtiles_gcs_path`, `tiling_config_json_string`.
        *   **Processing (Confirmed Strategy):**
            1.  **Run `tile-join`:**
                *   Input: The consolidated MVT GCS directory path (e.g., accessible via `gcsfuse`).
                *   Output: A single MBTiles file, written to a temporary GCS location (e.g., `gs://{temp_gcs_bucket}/mbtiles_temp/{workflow_id}/merged.mbtiles`).
                *   Command: `tile-join -o {output_mbtiles_gcs_or_local_fuse_path} --no-tile-compression --force {consolidated_mvt_gcs_fuse_path}`
            2.  **Run `pmtiles convert`:**
                *   Input: The GCS path to the `merged.mbtiles` file.
                *   Output: The final PMTiles file, written directly to `output_pmtiles_gcs_path`.
                *   Command: `pmtiles convert {input_mbtiles_gcs_path} {output_pmtiles_gcs_path}`
        *   **Return Status:** Success/failure.

7.  **Stage 6: Reporting (Workflow Step)**
    *   (As previously defined) Compile and write a final summary report to GCS.

## 3. Tiling Configuration (`tiling_config.json` - Key Fields for Task Generation)

```json
{
  // ... (layer_name, min_overall_zoom, max_overall_zoom, tippecanoe_options, parquet_input_options)
  "spatial_split_config": {
    "strategy": "fixed_grid",
    "data_bbox_override": [-180.0, -90.0, 180.0, 90.0], // Must be [minLon, minLat, maxLon, maxLat]
    "grid_divisions": [4, 4] // [number_of_columns, number_of_rows] for the grid
  },
  "zoom_level_segmentation": [
    // Defines how zoom levels are grouped for individual Tippecanoe jobs
    // Each sub-array is [min_zoom_for_job, max_zoom_for_job]
    [0, 5], [6, 8], [9, 10],
    [11, 11], [12, 12], [13, 13], [14, 14], [15, 15],
    [16, 16], [17, 17], [18, 18], [19, 19], [20, 20]
  ]
  // ... (output_report config)
}
```

## 4. Dockerfile Considerations

*   **MVT Generation Job:** Needs `tippecanoe`, Python tools, `gcsfuse`.
*   **MVT Consolidation Job:** Needs `google-cloud-sdk` (for `gsutil`). Could be a very lightweight container.
*   **PMTiles Assembly Job:** Needs `tile-join` (from Tippecanoe suite, so might be same base as MVT gen job or just install the specific tool) and `pmtiles` CLI, `gcsfuse` (if `tile-join` needs local fs access to GCS MVT dir).

## 5. Workflow YAML (`pmtiles_workflow.yaml`)

The corresponding YAML (`pmtiles_workflow.yaml`) has been updated (in its Fourth Revision) to include the explicit loops and calculations for generating `mvt_generation_tasks`, the MVT consolidation step, and the robust subworkflow for Cloud Run Job execution with polling.

This design makes the workflow more self-contained and the MVT merge process more structured, addressing the need for high parallelism and GCS-based intermediate storage.
```
