"""
create_flat_parquet.py

Converts a folder of geospatial JSON files into a single flat Parquet file.

Expected files in the input folder (one of each pattern):
    aquifer*.json           -> prefix: aq
    deltaG_fortnight*.json  -> prefix: dw
    deltaG_well_depth*.json -> prefix: df
    *cluster*.json          -> prefix: tc
    *intensity*.json        -> prefix: ci
    soge*.json              -> prefix: sg

Column rules applied before merging:
    - GLOBAL columns (never prefixed, taken from the first file only):
          mws_id, geom, area_in_ha, state
    - id column is dropped from every file.
    - uid column is renamed to mws_id; no layer prefix is added.
    - Columns whose name already starts with the layer prefix are kept as-is
      (prevents double-prefixing like df_df_2024_2025_DeltaG).
    - All other unique columns receive the layer prefix.
    - Aquifer split columns receive the aq_ prefix along with _min/_max suffix.
    - The aquifer-only column "newcode43" is dropped.

Final Parquet column order:
    mws_id, geom, area_in_ha, state, <all layer-prefixed columns ...>

Usage:
    python create_flat_parquet.py
    python create_flat_parquet.py /path/to/folder
"""

import os
import sys
import glob
import re
import json
import duckdb
from loguru import logger


# ---------------------------------------------------------------------------
# FILE PATTERNS
# Each entry: (glob_pattern, human_label, prefix)
# ---------------------------------------------------------------------------
FILE_PATTERNS = [
    ("aquifer*.json", "aquifer", "aq"),
    ("deltaG_fortnight*.json", "deltaG_fortnight", "dw"),
    ("deltaG_well_depth*.json", "deltaG_well_depth", "df"),
    ("*cluster*.json", "cluster", "tc"),
    ("*intensity*.json", "intensity", "ci"),
    ("soge*.json", "soge", "sg"),
]

# These columns are GLOBAL - kept exactly once in the output, never prefixed.
# uid is the raw join key and is also treated as global (becomes mws_id).
GLOBAL_COLS = {"uid", "geom", "area_in_ha", "state"}

# Columns that must be dropped from every file before any processing.
COLS_TO_DROP = {"id", "newcode43"}


# ===========================================================================
# SECTION 1 - SETUP
# ===========================================================================


def get_folder_path(passed_path=None):
    # 1. Use the path if we passed it from the worker
    if passed_path:
        return passed_path

    # 2. Use CLI argument if running as a script
    if len(sys.argv) > 1:
        return sys.argv[1]

    # 3. Fallback to input only if interactive
    return input("Enter the path to the folder: ").strip()


def setup_output_dir(folder_path):
    """
    Create and return <folder_path>/parquet/ as the output directory.
    Also return the final parquet output file path.
    """
    folder_name = os.path.basename(folder_path.rstrip("/\\"))
    output_dir = os.path.join(folder_path, "parquet")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{folder_name}_clean.parquet")
    return output_dir, output_file


def init_duckdb():
    """
    Create a DuckDB in-memory connection with the spatial extension loaded.
    """
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    return con


# ===========================================================================
# SECTION 2 - FILE DISCOVERY AND VALIDATION
# ===========================================================================


def find_file(folder_path, pattern):
    """
    Return the first file path matching <folder_path>/<pattern>.
    Raises FileNotFoundError if no match is found.
    Warns if multiple matches exist (uses the first one).
    """
    matches = glob.glob(os.path.join(folder_path, pattern))
    if not matches:
        raise FileNotFoundError(
            f"Required file matching '{pattern}' not found in: {folder_path}"
        )
    if len(matches) > 1:
        print(
            f"  WARNING: Multiple files match '{pattern}'. "
            f"Using: {os.path.basename(matches[0])}"
        )
    return matches[0].replace("\\", "/")


def validate_required_files(folder_path):
    """
    Verify that every required file pattern has at least one match.
    Returns a dict: {label: {"path": ..., "prefix": ...}}.
    Aborts with a clear error message if any pattern is missing.
    """
    print("\n--- Step 1: Validating required files ---")
    found = {}
    missing = []

    for pattern, label, prefix in FILE_PATTERNS:
        try:
            path = find_file(folder_path, pattern)
            found[label] = {"path": path, "prefix": prefix}
            print(f"  [OK]      {label:25s} -> {os.path.basename(path)}")
        except FileNotFoundError as exc:
            missing.append(str(exc))
            print(f"  [MISSING] {label}")

    if missing:
        print("\nAborting. Missing files:")
        for m in missing:
            print(f"  {m}")
        sys.exit(1)

    print("  All required files found.\n")
    return found


# ===========================================================================
# SECTION 3 - GENERIC HELPERS
# ===========================================================================


def load_geojson_to_table(con, file_path, table_name):
    """
    Load a GeoJSON / JSON file via ST_READ into a named DuckDB table.
    """
    con.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM ST_READ('{file_path}');"
    )


def get_table_columns(con, table_name):
    """
    Return a list of column names present in the given DuckDB table.
    """
    return con.execute(f"PRAGMA table_info({table_name})").df()["name"].tolist()


def detect_columns_by_pattern(con, table_name, regex):
    """
    Return column names from <table_name> whose names match the compiled regex.
    """
    return [c for c in get_table_columns(con, table_name) if regex.match(c)]


def column_type_contains(con, table_name, column_name, keyword):
    """
    Return True if the DuckDB type of the column contains keyword (case-insensitive).
    """
    for row in con.execute(f"DESCRIBE {table_name}").fetchall():
        if row[0] == column_name:
            return keyword.upper() in row[1].upper()
    return False


def detect_json_metrics(con, table_name, sample_column):
    """
    Inspect one cell of <sample_column> in <table_name> and return the list of
    metric keys stored inside that JSON / struct cell.
    Returns an empty list if detection fails.
    """
    sample = con.execute(
        f'SELECT "{sample_column}" FROM {table_name} LIMIT 1'
    ).fetchone()[0]

    if sample is None:
        return []
    if isinstance(sample, str):
        try:
            return list(json.loads(sample).keys())
        except Exception:
            pass
        result = con.execute(f"SELECT json_keys('{sample}') AS k").fetchone()
        if result and result[0]:
            return result[0]
    if hasattr(sample, "keys"):
        return list(sample.keys())
    return []


def safe_prefix(col_name, prefix):
    """
    Return <prefix>_<col_name> only if col_name does not already start with
    <prefix>_. This prevents double-prefixing like df_df_2024_2025_DeltaG.
    """
    if col_name.startswith(f"{prefix}_"):
        return col_name
    return f"{prefix}_{col_name}"


def drop_unwanted_cols(con, src_table, out_table):
    """
    Copy <src_table> to <out_table> while silently dropping every column
    listed in COLS_TO_DROP that actually exists in the source table.
    Returns the list of columns kept in out_table.
    """
    existing = get_table_columns(con, src_table)
    keep = [c for c in existing if c not in COLS_TO_DROP]
    select_sql = ", ".join([f'"{c}"' for c in keep])
    con.execute(
        f"CREATE OR REPLACE TABLE {out_table} AS SELECT {select_sql} FROM {src_table};"
    )
    dropped = [c for c in existing if c in COLS_TO_DROP]
    if dropped:
        print(f"    Dropped columns: {dropped}")
    return get_table_columns(con, out_table)


def apply_prefix_to_unique_cols(con, src_table, prefix, out_table):
    """
    Rename columns that are NOT in GLOBAL_COLS by conditionally prepending
    <prefix>_ (skipped if the column already starts with <prefix>_).

    Global columns (uid, geom, area_in_ha, state) are passed through as-is.
    uid is NOT renamed here; it is renamed to mws_id at merge time.

    Returns out_table name.
    """
    cols = get_table_columns(con, src_table)
    select_parts = []

    for col in cols:
        if col in GLOBAL_COLS:
            select_parts.append(f'"{col}"')
        else:
            new_name = safe_prefix(col, prefix)
            if new_name != col:
                select_parts.append(f'"{col}" AS "{new_name}"')
            else:
                select_parts.append(f'"{col}"')

    con.execute(
        f"CREATE OR REPLACE TABLE {out_table} AS "
        f"SELECT {', '.join(select_parts)} FROM {src_table};"
    )
    return out_table


def flatten_nested_columns(con, src_table, nested_cols, metrics, prefix, out_table):
    """
    Expand nested JSON / STRUCT columns into individual numeric columns.

    For each column in nested_cols and each metric in metrics a new column is
    created. The alias is built with safe_prefix to prevent double-prefixing:
        safe_prefix("<nested_col>_<metric>", prefix)
    e.g. for prefix="df" and col="df_2024_2025":
        safe_prefix("df_2024_2025_DeltaG", "df") -> "df_2024_2025_DeltaG"  (no double df_)

    The original nested columns are excluded from the output.
    All other columns (including global ones) are kept as-is.

    Returns out_table name.
    """
    if not nested_cols:
        con.execute(
            f"CREATE OR REPLACE TABLE {out_table} AS SELECT * FROM {src_table};"
        )
        return out_table

    sample_col = nested_cols[0]
    is_struct = column_type_contains(con, src_table, sample_col, "STRUCT")

    flat_clauses = []
    for col in nested_cols:
        for metric in metrics:
            raw_alias = f"{col}_{metric}"
            final_name = safe_prefix(raw_alias, prefix)
            alias = f'"{final_name}"'
            if is_struct:
                flat_clauses.append(f'("{col}").{metric} AS {alias}')
            else:
                flat_clauses.append(
                    f"CAST(json_extract(\"{col}\", '$.{metric}') AS DOUBLE) AS {alias}"
                )

    exclude_sql = ", ".join([f'"{c}"' for c in nested_cols])
    flat_sql = ",\n            ".join(flat_clauses)

    con.execute(f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            * EXCLUDE ({exclude_sql}),
            {flat_sql}
        FROM {src_table};
    """)

    print(
        f"    Expanded {len(nested_cols)} nested columns x "
        f"{len(metrics)} metrics -> {len(flat_clauses)} flat columns."
    )
    return out_table


# ===========================================================================
# SECTION 4 - LAYER-SPECIFIC TRANSFORMATIONS
# ===========================================================================

# ---------------------------------------------------------------------------
# 4a. aquifer -> prefix: aq
# ---------------------------------------------------------------------------


def transform_aquifer(con, file_path, prefix="aq"):
    """
    Load and transform the aquifer layer.

    Special operations (in addition to general rules):
        - Drop: id, newcode43.
        - uid kept as uid (renamed to mws_id at merge).
        - geom, area_in_ha, state treated as global (no prefix).
        - Split columns (range strings -> float min/max), ALL receive aq_ prefix:
              avg_mbgl   "X-Y"      -> aq_avg_mbgl_min,   aq_avg_mbgl_max
              m2_perday  "upto X"   -> aq_m2_per_day_max
              m3_per_day "X to Y"   -> aq_m3_per_day_min, aq_m3_per_day_max
              mbgl       "X - Y"    -> aq_mbgl_min,        aq_mbgl_max
              per_cm     "X-Y"      -> aq_per_cm_min,      aq_per_cm_max
              yeild__    "X%-Y%"    -> aq_yeild__min,      aq_yeild__max
              zone_m     "X-Y"      -> aq_zone_m_min,      aq_zone_m_max
        - All remaining unique columns get the aq_ prefix (with duplicate-check).

    Returns the name of the final DuckDB table.
    """
    print(f"  Transforming: {os.path.basename(file_path)}")

    p = prefix  # short alias used in the SQL strings below

    # Columns that are consumed by the split logic - excluded from the generic pass
    split_source_cols = {
        "avg_mbgl",
        "m2_perday",
        "m3_per_day",
        "mbgl",
        "per_cm",
        "yeild__",
        "zone_m",
    }

    # Load raw file to inspect columns
    raw_table = "raw_aquifer"
    load_geojson_to_table(con, file_path, raw_table)

    all_cols = get_table_columns(con, raw_table)

    # Columns that are skipped in the generic unique-column pass
    skip_in_generic = COLS_TO_DROP | GLOBAL_COLS | split_source_cols | {"uid"}

    # Build SELECT parts for remaining unique columns (with safe_prefix check)
    unique_parts = []
    for col in all_cols:
        if col in skip_in_generic:
            continue
        new_name = safe_prefix(col, prefix)
        if new_name != col:
            unique_parts.append(f'"{col}" AS "{new_name}"')
        else:
            unique_parts.append(f'"{col}"')

    unique_sql = (
        (",\n        ".join(unique_parts) + ",\n        ") if unique_parts else ""
    )

    # Only include global columns that actually exist in this file
    present_globals = [c for c in ["geom", "area_in_ha", "state"] if c in all_cols]
    globals_sql = (
        (", ".join([f'"{c}"' for c in present_globals]) + ",")
        if present_globals
        else ""
    )

    out_table = "transformed_aquifer"
    con.execute(f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            uid,
            {globals_sql}
            {unique_sql}
            -- avg_mbgl "X-Y" -> aq_avg_mbgl_min, aq_avg_mbgl_max
            CAST(SPLIT_PART("avg_mbgl", '-', 1) AS FLOAT)                    AS "{p}_avg_mbgl_min",
            CAST(SPLIT_PART("avg_mbgl", '-', 2) AS FLOAT)                    AS "{p}_avg_mbgl_max",

            -- m2_perday "upto X" -> aq_m2_per_day_max
            CAST(
                REPLACE(SPLIT_PART("m2_perday", 'upto ', 2), 'upto ', '')
            AS FLOAT)                                                          AS "{p}_m2_per_day_max",

            -- m3_per_day "X to Y" -> aq_m3_per_day_min, aq_m3_per_day_max
            CAST(SPLIT_PART("m3_per_day", ' to', 1) AS FLOAT)                AS "{p}_m3_per_day_min",
            CAST(SPLIT_PART("m3_per_day", ' to', 2) AS FLOAT)                AS "{p}_m3_per_day_max",

            -- mbgl "X - Y" -> aq_mbgl_min, aq_mbgl_max
            CAST(SPLIT_PART("mbgl", ' -', 1) AS FLOAT)                       AS "{p}_mbgl_min",
            CAST(SPLIT_PART("mbgl", ' -', 2) AS FLOAT)                       AS "{p}_mbgl_max",

            -- per_cm "X-Y" -> aq_per_cm_min, aq_per_cm_max
            CAST(SPLIT_PART("per_cm", '-', 1) AS FLOAT)                      AS "{p}_per_cm_min",
            CAST(SPLIT_PART("per_cm", '-', 2) AS FLOAT)                      AS "{p}_per_cm_max",

            -- yeild__ "X%-Y%" -> aq_yeild__min, aq_yeild__max
            CAST(REPLACE(SPLIT_PART("yeild__", '-', 1), '%', '') AS FLOAT)   AS "{p}_yeild__min",
            CAST(REPLACE(SPLIT_PART("yeild__", '-', 2), '%', '') AS FLOAT)   AS "{p}_yeild__max",

            -- zone_m "X-Y" -> aq_zone_m_min, aq_zone_m_max
            CAST(SPLIT_PART("zone_m", '-', 1) AS FLOAT)                      AS "{p}_zone_m_min",
            CAST(SPLIT_PART("zone_m", '-', 2) AS FLOAT)                      AS "{p}_zone_m_max"

        FROM ST_READ('{file_path}')
        -- id and newcode43 are simply never selected above, so they are dropped.
    """)

    n_cols = len(get_table_columns(con, out_table))
    print(f"    -> {n_cols} columns in transformed_aquifer")
    return out_table


# ---------------------------------------------------------------------------
# 4b. deltaG_fortnight -> prefix: dw
# ---------------------------------------------------------------------------


def transform_deltaG_fortnight(con, file_path, prefix="dw"):
    """
    Load and transform the deltaG_fortnight layer.

    Columns with the pattern YYYY-MM-DD contain nested JSON / STRUCT data
    (keys: DeltaG, ET, Precipitation, RunOff, G). Each nested cell is
    expanded into flat columns using safe_prefix to avoid double dw_:
        dw_<YYYY-MM-DD>_DeltaG  (if column was already dw_..., kept as-is)

    id is dropped. uid kept as uid. Global cols kept without prefix.
    All other unique columns get the dw_ prefix (with duplicate-check).

    Returns the name of the final DuckDB table.
    """
    print(f"  Transforming: {os.path.basename(file_path)}")

    raw_table = "raw_dg_fortnight"
    load_geojson_to_table(con, file_path, raw_table)
    drop_unwanted_cols(con, raw_table, "base_dg_fortnight")

    # Detect date-named columns (YYYY-MM-DD)
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    date_cols = detect_columns_by_pattern(con, "base_dg_fortnight", date_pattern)
    print(f"    Found {len(date_cols)} date columns.")

    # Detect metrics from the first date column
    metrics = ["DeltaG", "ET", "Precipitation", "RunOff", "G"]
    if date_cols:
        detected = detect_json_metrics(con, "base_dg_fortnight", date_cols[0])
        if detected:
            metrics = detected

    # Expand nested date columns into flat columns
    flatten_nested_columns(
        con, "base_dg_fortnight", date_cols, metrics, prefix, "flat_dg_fortnight"
    )

    # Apply prefix to remaining non-global columns (safe_prefix prevents double dw_)
    out_table = "transformed_dg_fortnight"
    apply_prefix_to_unique_cols(con, "flat_dg_fortnight", prefix, out_table)

    n_cols = len(get_table_columns(con, out_table))
    print(f"    -> {n_cols} columns in transformed_dg_fortnight")
    return out_table


# ---------------------------------------------------------------------------
# 4c. deltaG_well_depth -> prefix: df
# ---------------------------------------------------------------------------


def transform_deltaG_well_depth(con, file_path, prefix="df"):
    """
    Load and transform the deltaG_well_depth layer.

    Columns with the pattern YYYY_YYYY (year ranges) contain nested JSON /
    STRUCT data. Each nested cell is expanded into flat columns using
    safe_prefix to avoid double df_:
        df_<YYYY_YYYY>_<Metric>  (if column was already df_..., kept as-is)

    id is dropped. uid kept as uid. Global cols kept without prefix.
    All other unique columns get the df_ prefix (with duplicate-check).

    Returns the name of the final DuckDB table.
    """
    print(f"  Transforming: {os.path.basename(file_path)}")

    raw_table = "raw_dg_well_depth"
    load_geojson_to_table(con, file_path, raw_table)
    drop_unwanted_cols(con, raw_table, "base_dg_well_depth")

    # Detect year-range columns YYYY_YYYY
    year_pattern = re.compile(r"^\d{4}_\d{4}$")
    year_cols = detect_columns_by_pattern(con, "base_dg_well_depth", year_pattern)
    print(f"    Found {len(year_cols)} year-range columns.")

    # Detect metrics
    metrics = ["DeltaG"]
    if year_cols:
        detected = detect_json_metrics(con, "base_dg_well_depth", year_cols[0])
        if detected:
            metrics = detected

    # Expand nested year-range columns
    flatten_nested_columns(
        con, "base_dg_well_depth", year_cols, metrics, prefix, "flat_dg_well_depth"
    )

    out_table = "transformed_dg_well_depth"
    apply_prefix_to_unique_cols(con, "flat_dg_well_depth", prefix, out_table)

    n_cols = len(get_table_columns(con, out_table))
    print(f"    -> {n_cols} columns in transformed_dg_well_depth")
    return out_table


# ---------------------------------------------------------------------------
# 4d. cluster -> prefix: tc
# ---------------------------------------------------------------------------


def transform_cluster(con, file_path, prefix="tc"):
    """
    Load and transform the cluster layer.

    No column splitting needed. id is dropped. uid kept as uid.
    Global cols kept without prefix. All other unique columns get tc_ prefix
    (with duplicate-check via safe_prefix).

    Returns the name of the final DuckDB table.
    """
    print(f"  Transforming: {os.path.basename(file_path)}")

    raw_table = "raw_cluster"
    load_geojson_to_table(con, file_path, raw_table)
    drop_unwanted_cols(con, raw_table, "base_cluster")

    out_table = "transformed_cluster"
    apply_prefix_to_unique_cols(con, "base_cluster", prefix, out_table)

    n_cols = len(get_table_columns(con, out_table))
    print(f"    -> {n_cols} columns in transformed_cluster")
    return out_table


# ---------------------------------------------------------------------------
# 4e. intensity -> prefix: ci
# ---------------------------------------------------------------------------


def transform_intensity(con, file_path, prefix="ci"):
    """
    Load and transform the intensity layer.

    No column splitting needed. id is dropped. uid kept as uid.
    Global cols kept without prefix. All other unique columns get ci_ prefix
    (with duplicate-check via safe_prefix).

    Returns the name of the final DuckDB table.
    """
    print(f"  Transforming: {os.path.basename(file_path)}")

    raw_table = "raw_intensity"
    load_geojson_to_table(con, file_path, raw_table)
    drop_unwanted_cols(con, raw_table, "base_intensity")

    out_table = "transformed_intensity"
    apply_prefix_to_unique_cols(con, "base_intensity", prefix, out_table)

    n_cols = len(get_table_columns(con, out_table))
    print(f"    -> {n_cols} columns in transformed_intensity")
    return out_table


# ---------------------------------------------------------------------------
# 4f. soge -> prefix: sg
# ---------------------------------------------------------------------------


def transform_soge(con, file_path, prefix="sg"):
    """
    Load and transform the soge layer.

    No column splitting needed. id is dropped. uid kept as uid.
    Global cols kept without prefix. All other unique columns get sg_ prefix
    (with duplicate-check via safe_prefix).

    Returns the name of the final DuckDB table.
    """
    print(f"  Transforming: {os.path.basename(file_path)}")

    raw_table = "raw_soge"
    load_geojson_to_table(con, file_path, raw_table)
    drop_unwanted_cols(con, raw_table, "base_soge")

    out_table = "transformed_soge"
    apply_prefix_to_unique_cols(con, "base_soge", prefix, out_table)

    n_cols = len(get_table_columns(con, out_table))
    print(f"    -> {n_cols} columns in transformed_soge")
    return out_table


# ---------------------------------------------------------------------------
# Dispatcher - map each label to its transform function
# ---------------------------------------------------------------------------

TRANSFORM_DISPATCH = {
    "aquifer": transform_aquifer,
    "deltaG_fortnight": transform_deltaG_fortnight,
    "deltaG_well_depth": transform_deltaG_well_depth,
    "cluster": transform_cluster,
    "intensity": transform_intensity,
    "soge": transform_soge,
}


def transform_all_layers(con, file_info):
    """
    Run the layer-specific transform for each discovered file.

    file_info: dict returned by validate_required_files()
               {label: {"path": ..., "prefix": ...}}

    Returns a dict {label: duckdb_table_name}.
    """
    print("\n--- Step 2: Transforming each layer ---")
    tables = {}
    for label, info in file_info.items():
        fn = TRANSFORM_DISPATCH[label]
        tables[label] = fn(con, info["path"], info["prefix"])
    return tables


# ===========================================================================
# SECTION 5 - MERGE
# ===========================================================================


def merge_on_mws_id(con, tables, output_table="merged_data"):
    """
    INNER JOIN all transformed tables on uid, producing a final table where:

        Column 1 : mws_id     (uid renamed, sourced from base table only)
        Column 2 : geom       (global, sourced from base table only)
        Column 3 : area_in_ha (global, sourced from base table only)
        Column 4 : state      (global, sourced from base table only)
        Remaining : every non-global, non-uid column from each layer table
                    in order: aquifer -> deltaG_fortnight -> deltaG_well_depth
                              -> cluster -> intensity -> soge

    Global columns (geom, area_in_ha, state) from non-base tables are skipped
    so each appears exactly once in the output.

    Returns output_table name.
    """
    print("\n--- Step 3: Merging all layers on uid -> mws_id ---")

    labels = list(tables.keys())
    base_lbl = labels[0]
    base_tbl = tables[base_lbl]

    # Assign t0 .. tN aliases to each layer table
    aliases = {lbl: f"t{i}" for i, lbl in enumerate(labels)}
    base_alias = aliases[base_lbl]
    base_cols = get_table_columns(con, base_tbl)

    # ---- SELECT clause ----
    # First four columns are always the anchor / global ones from the base table
    select_parts = [f'{base_alias}."uid" AS mws_id']
    for gcol in ["geom", "area_in_ha", "state"]:
        if gcol in base_cols:
            select_parts.append(f'{base_alias}."{gcol}"')

    # Then every unique (non-global, non-uid) column from each table in order
    for lbl in labels:
        alias = aliases[lbl]
        cols = get_table_columns(con, tables[lbl])
        for col in cols:
            if col in GLOBAL_COLS:  # skip uid, geom, area_in_ha, state duplicates
                continue
            select_parts.append(f'{alias}."{col}"')

    # ---- FROM / JOIN clause ----
    from_clause = f"{base_tbl} {base_alias}"
    for lbl in labels[1:]:
        tbl = tables[lbl]
        alias = aliases[lbl]
        from_clause += (
            f"\n    INNER JOIN {tbl} {alias} ON {base_alias}.uid = {alias}.uid"
        )

    select_sql = ",\n    ".join(select_parts)

    con.execute(f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT
            {select_sql}
        FROM {from_clause};
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
    col_count = len(get_table_columns(con, output_table))
    print(f"    Merged table : {row_count:,} rows x {col_count:,} columns")
    print("    Lead columns : mws_id, geom, area_in_ha, state")
    return output_table


# ===========================================================================
# SECTION 6 - EXPORT
# ===========================================================================


def export_to_parquet(con, table_name, output_file):
    """
    Write the DuckDB table to a single Parquet file.
    """
    print("\n--- Step 4: Exporting to Parquet ---")
    con.execute(f"COPY {table_name} TO '{output_file}' (FORMAT PARQUET);")
    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"    Written   : {output_file}")
    print(f"    File size : {size_mb:.2f} MB")


# ===========================================================================
# SECTION 7 - MAIN
# ===========================================================================


def main():
    folder_path = get_folder_path()

    if not os.path.isdir(folder_path):
        print(f"ERROR: '{folder_path}' is not a valid directory.")
        sys.exit(1)

    output_dir, output_file = setup_output_dir(folder_path)
    con = init_duckdb()

    # 1. Validate all required files exist before any work is done
    file_info = validate_required_files(folder_path)

    # 2. Transform each layer individually
    tables = transform_all_layers(con, file_info)

    # 3. Merge all layers on uid -> mws_id
    merged = merge_on_mws_id(con, tables)

    # 4. Export to a single Parquet file
    export_to_parquet(con, merged, output_file)

    print("\nDone.")


# ===========================================================================
# SECTION 8 - clean_parquet
# ===========================================================================


def clean_parquet(manual_path=None):
    folder_path = get_folder_path(manual_path)

    if not os.path.isdir(folder_path):
        # Use logger instead of sys.exit in a worker
        logger.error(f"'{folder_path}' is not a valid directory.")
        return

    output_dir, output_file = setup_output_dir(folder_path)
    con = init_duckdb()

    # 1. Validate all required files exist before any work is done
    file_info = validate_required_files(folder_path)

    # 2. Transform each layer individually
    tables = transform_all_layers(con, file_info)

    # 3. Merge all layers on uid -> mws_id
    merged = merge_on_mws_id(con, tables)

    # 4. Export to a single Parquet file
    export_to_parquet(con, merged, output_file)

    print("\nDone.")


if __name__ == "__main__":
    main()
