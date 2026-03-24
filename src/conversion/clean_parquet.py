"""

======================
Config-driven converter: reads one JSON config file and produces a single
flat GeoParquet (or other supported format) from a set of geospatial JSON /
GeoJSON layer files.

Usage
-----
    python clean_parquet.py config.json
    python clean_parquet.py          # prompts for config path

Can also be called programmatically:
    from clean_parquet import run
    run("path/to/config.json")
"""

import os
import sys
import json
import duckdb
from tqdm import tqdm
from typing import Dict, List, Optional
from loguru import logger

# Configure logger
logger.remove()  # Remove default handler
logger.add(
    sys.stderr,
    format="{time:HH:mm:ss} | {level:8} | {name}:{line} | {message}",
    level="INFO",
)
logger.add("pipeline.log", rotation="10 MB", level="DEBUG")

# ===========================================================================
# SECTION 1 — CONFIG
# ===========================================================================


def derive_prefix(layer_name: str) -> str:
    """
    Auto-derive a short prefix from the layer label.
    Rule:
        No underscore in name  →  first two characters lowercased  (aquifer → aq)
        Has underscores        →  first character of each part     (deltaG_fortnight → df)
    """
    parts = layer_name.split("_")
    if len(parts) == 1:
        return layer_name[:2].lower()
    return "".join(p[0].lower() for p in parts if p)


def load_config(path: str) -> dict:
    """Load and validate the JSON config, backfilling defaults where needed."""
    logger.info(f"Loading config from: {path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    _validate_and_fill(cfg)
    logger.debug(f"Config loaded with {len(cfg['layers'])} layers")
    return cfg


def _validate_and_fill(cfg: dict):
    """Raise ValueError for missing mandatory keys; backfill optional ones."""
    mandatory = ["layers", "column_map", "common_cols", "key", "output_path"]
    missing = [k for k in mandatory if k not in cfg]
    if missing:
        logger.error(f"Missing required keys: {missing}")
        raise ValueError(f"Config is missing required keys: {missing}")

    # Backfill optional top-level keys
    cfg.setdefault("unit", "unknown")
    cfg.setdefault("input_format", "geojson")
    cfg.setdefault("output_format", "parquet")
    cfg.setdefault("rename_key_to", cfg["key"])
    cfg.setdefault("base_layer", list(cfg["layers"].keys())[0])
    cfg.setdefault("parquet_version", 1)
    cfg.setdefault("use_prev_mapping", False)

    logger.debug(f"Base layer: {cfg['base_layer']}, Key column: {cfg['key']}")

    # Backfill missing column_map entries
    for label in cfg["layers"]:
        cfg["column_map"].setdefault(label, {})
        lmap = cfg["column_map"][label]
        lmap.setdefault("drop_columns", [])
        lmap.setdefault("rename_columns", {})
        lmap.setdefault("column_changes", {})


# ===========================================================================
# SECTION 2 — DUCKDB HELPERS
# ===========================================================================


def init_duckdb() -> duckdb.DuckDBPyConnection:
    logger.debug("Initializing DuckDB with spatial extension")
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    return con


def get_columns(con, table: str) -> list:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]


def get_col_type(con, table: str, col: str) -> str:
    for row in con.execute(f"DESCRIBE {table}").fetchall():
        if row[0] == col:
            return row[1].upper()
    return ""


# ===========================================================================
# SECTION 3 — COLUMN-NAME UTILITIES
# ===========================================================================


def safe_prefix(col: str, prefix: str) -> str:
    """
    Prepend <prefix>_ to col unless col already starts with <prefix>_.
    Prevents double-prefixing: df_df_2024_DeltaG → df_2024_DeltaG.
    """
    return col if col.startswith(f"{prefix}_") else f"{prefix}_{col}"


def detect_json_metrics(con, table: str, sample_col: str) -> list:
    """
    Read one cell of sample_col and return the metric key list it contains.
    Works for both raw JSON strings and DuckDB STRUCT values.
    Returns [] if detection fails.
    """
    row = con.execute(f'SELECT "{sample_col}" FROM {table} LIMIT 1').fetchone()
    if not row or row[0] is None:
        return []
    val = row[0]
    if isinstance(val, str):
        try:
            return list(json.loads(val).keys())
        except Exception as e:
            logger.debug(f"Failed to parse JSON from {sample_col}: {e}")
            pass
    if hasattr(val, "keys"):  # DuckDB struct dict-like
        return list(val.keys())
    return []


def normalise_range_sql(expr: str) -> str:
    """
    Return a SQL expression that normalises all range-separator variants to a
    single bare '-' so SPLIT_PART can reliably split on it.
    Handles: ' - ', ' to ', 'to ', ' to', 'to'  →  '-'
    """
    no_to = f"REGEXP_REPLACE({expr}, '[ ]*to[ ]*', '-')"
    no_spc = f"REGEXP_REPLACE({no_to}, ' *- *', '-')"
    return no_spc


# ===========================================================================
# SECTION 4 — LOAD
# ===========================================================================


class DataLakeLoader:
    """
    Production-ready loader for geospatial data from local or S3.
    Handles lazy S3 configuration, schema validation, and progress tracking.
    """

    def __init__(self, extensions: List[str] = None):
        self.extensions = extensions or ["spatial"]
        self.conn = None
        self._s3_configured = False

    def __enter__(self):
        logger.info("Establishing DuckDB connection")
        self.conn = duckdb.connect()
        for ext in self.extensions:
            logger.debug(f"Loading extension: {ext}")
            self.conn.execute(f"INSTALL {ext}; LOAD {ext};")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            logger.info("Closing DuckDB connection")
            self.conn.close()
        if exc_type:
            logger.exception(f"Error during pipeline execution: {exc_val}")

    def _configure_s3(self):
        """Configure S3 on the connection (once per connection)."""
        if self._s3_configured:
            return

        logger.info("Configuring S3 support")
        # Check if httpfs is loaded
        loaded = self.conn.execute("""
            SELECT COUNT(*) FROM duckdb_extensions()
            WHERE extension_name = 'httpfs' AND loaded = true
        """).fetchone()[0]

        if loaded == 0:
            logger.debug("Loading httpfs extension")
            self.conn.execute("INSTALL httpfs; LOAD httpfs;")

        # Set credentials from environment
        if os.getenv("AWS_ACCESS_KEY_ID"):
            logger.debug("Setting AWS credentials from environment")
            self.conn.execute(
                f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID')}'"
            )
            self.conn.execute(
                f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}'"
            )
        if os.getenv("AWS_REGION"):
            logger.debug(f"Setting AWS region: {os.getenv('AWS_REGION')}")
            self.conn.execute(f"SET s3_region='{os.getenv('AWS_REGION')}'")
        if os.getenv("AWS_ENDPOINT"):
            logger.debug(f"Setting S3 endpoint: {os.getenv('AWS_ENDPOINT')}")
            self.conn.execute(f"SET s3_endpoint='{os.getenv('AWS_ENDPOINT')}'")

        self._s3_configured = True
        logger.info("S3 configuration complete")

    def validate_schema(self, file_path: str) -> Dict:
        """
        Dry run to get schema without loading data.
        Minimal data transfer - only fetches metadata.
        """
        fp = file_path.replace("\\", "/")
        is_s3 = fp.startswith("s3://")

        logger.debug(f"Validating schema for: {fp}")

        if is_s3:
            self._configure_s3()

        # Get schema without loading data
        result = self.conn.execute(
            f"DESCRIBE SELECT * FROM ST_READ('{fp}') LIMIT 0"
        ).fetchall()

        schema = {
            "columns": [row[0] for row in result],
            "types": [row[1] for row in result],
            "has_geometry": "geometry" in [row[0] for row in result],
        }
        logger.debug(
            f"Schema: {len(schema['columns'])} columns, geometry: {schema['has_geometry']}"
        )
        return schema

    def load_layer(
        self,
        file_path: str,
        table_name: str,
        input_format: str,
        expected_columns: Optional[List[str]] = None,
    ) -> int:
        """
        Load a single layer with optional schema validation and progress tracking.
        Returns row count.
        """
        fp = file_path.replace("\\", "/")
        fmt = input_format.lower()
        is_s3 = fp.startswith("s3://")

        # Validate format
        supported = ("geojson", "json", "shapefile", "geopackage")
        if fmt not in supported:
            logger.error(f"Unsupported format: {input_format}")
            raise ValueError(
                f"Unsupported format: '{input_format}'. Supported: {supported}"
            )

        logger.info(f"Loading {table_name} from: {fp[:100]}...")

        # Configure S3 if needed
        if is_s3:
            self._configure_s3()

        # Optional: Validate schema before loading
        if expected_columns:
            logger.debug(f"Validating expected columns: {expected_columns}")
            schema = self.validate_schema(fp)
            missing = set(expected_columns) - set(schema["columns"])
            if missing:
                logger.error(f"Missing expected columns: {missing}")
                raise ValueError(f"Expected columns missing: {missing}")

        # Load with progress tracking
        with tqdm(
            desc=f"Loading {table_name}", unit="rows", position=0, leave=True
        ) as pbar:
            try:
                # Try DuckDB's native progress bar (0.10+)
                try:
                    logger.debug("Attempting load with native progress bar")
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE {table_name} AS
                        SELECT * FROM ST_READ('{fp}')
                        WITH (PROGRESS_BAR = true)
                    """)
                except Exception:
                    # Fallback: load and get count
                    logger.debug("Falling back to manual progress tracking")
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE {table_name} AS
                        SELECT * FROM ST_READ('{fp}')
                    """)

                # Update progress bar
                row_count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                pbar.update(row_count)
                pbar.set_postfix({"rows": f"{row_count:,}"})

                logger.info(f"Loaded {table_name}: {row_count:,} rows")
                return row_count

            except Exception as e:
                logger.exception(f"Failed to load {table_name}: {e}")
                pbar.set_description(f" Failed loading {table_name}")
                raise e

    def load_layers_parallel(
        self, layers: Dict[str, str], input_format: str, max_workers: int = 4
    ) -> Dict[str, int]:
        """
        Load multiple layers in parallel (for in-memory database).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger.info(
            f"Loading {len(layers)} layers in parallel with {max_workers} workers"
        )
        results = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self.load_layer, path, f"raw_{label}", input_format
                ): label
                for label, path in layers.items()
            }

            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Loading layers"
            ):
                label = futures[future]
                try:
                    row_count = future.result()
                    results[label] = row_count
                    logger.info(f"   {label}: {row_count:,} rows")
                except Exception as e:
                    logger.error(f"   {label}: {e}")
                    raise

        return results


# ===========================================================================
# SECTION 5 — PER-LAYER TRANSFORM PIPELINE
# ===========================================================================


def _step_drop(con, src: str, dst: str, drop_cols: list):
    """Drop listed columns that exist in src; copy rest to dst."""
    all_cols = get_columns(con, src)
    dropped = [c for c in drop_cols if c in all_cols]
    keep = [c for c in all_cols if c not in drop_cols]
    if dropped:
        logger.debug(f"    Dropped columns: {dropped}")
    sel = ", ".join([f'"{c}"' for c in keep])
    con.execute(f"CREATE OR REPLACE TABLE {dst} AS SELECT {sel} FROM {src}")


def _step_rename(con, src: str, dst: str, renames: dict):
    """Rename columns per renames dict; copy to dst."""
    all_cols = get_columns(con, src)
    applied = {k: v for k, v in renames.items() if k in all_cols}
    parts = [f'"{c}" AS "{renames[c]}"' if c in renames else f'"{c}"' for c in all_cols]
    if applied:
        logger.debug(f"    Renamed columns: {applied}")
    con.execute(
        f"CREATE OR REPLACE TABLE {dst} AS SELECT {', '.join(parts)} FROM {src}"
    )


def _step_column_changes(
    con, src: str, dst: str, changes: dict, prefix: str, layer_label: str
):
    all_cols = get_columns(con, src)
    logger.debug(
        f"Processing column changes for {layer_label}, {len(all_cols)} columns"
    )

    resolved = {}
    for c in all_cols:
        # STRATEGY: If it looks like a date or a year range, mark it for flattening
        if "20" in c and ("-" in c or "_" in c):
            resolved[c] = "flatten-nested"
            logger.debug(f"    Auto-detected JSON column: {c}")

        # Check against your config 'changes'
        for key, action in changes.items():
            if key in c:
                resolved[c] = action
                logger.debug(f"    Config matched: {c} -> {action}")

    exclude = []
    derived = []
    metrics = ["DeltaG", "ET", "Precipitation", "RunOff", "G"]

    for col, action in resolved.items():
        if action == "flatten-nested":
            logger.info(f"    Flattening JSON column: {col}")
            exclude.append(col)
            for m in metrics:
                alias = f"{col}_{m}"
                expr = f"TRY_CAST(json_extract(\"{col}\", '$.{m}') AS DOUBLE)"
                derived.append(f'{expr} AS "{alias}"')
            logger.debug(f"    Created {len(metrics)} metrics for {col}")

        elif action in ["split-min-max", "percentage-split"]:
            logger.debug(f"    Splitting column: {col} with action {action}")
            exclude.append(col)
            base = f"{prefix}_{col}" if not col.startswith(prefix) else col
            norm = normalise_range_sql(f'"{col}"')
            derived += [
                f"TRY_CAST(NULLIF(TRIM(SPLIT_PART({norm}, '-', 1)), '') AS DOUBLE) AS \"{base}_min\"",
                f"TRY_CAST(NULLIF(TRIM(SPLIT_PART({norm}, '-', 2)), '') AS DOUBLE) AS \"{base}_max\"",
            ]
        elif action == "only-max":
            logger.debug(f"    Extracting max from: {col}")
            exclude.append(col)
            base = f"{prefix}_{col}" if not col.startswith(prefix) else col
            derived.append(
                f"TRY_CAST(REGEXP_REPLACE(\"{col}\", '(?i)^up[ ]*to[ ]*', '') AS DOUBLE) "
                f'AS "{base}_max"'
            )

    # Rebuild the table
    unique_excl = [c for c in exclude if c in all_cols]
    excl_clause = (
        f"* EXCLUDE ({', '.join(f'"{c}"' for c in unique_excl)})"
        if unique_excl
        else "*"
    )
    select_sql = f"{excl_clause}, {', '.join(derived)}" if derived else excl_clause

    con.execute(f"CREATE OR REPLACE TABLE {dst} AS SELECT {select_sql} FROM {src}")
    logger.info(f"    Table {dst}: {len(derived)} derived columns")


def _step_apply_prefix(con, src: str, dst: str, prefix: str, skip: set):
    """
    Prepend <prefix>_ to every column not in the skip set, using safe_prefix
    to prevent double-prefixing.
    """
    parts = []
    for col in get_columns(con, src):
        if col in skip:
            parts.append(f'"{col}"')
        else:
            new = safe_prefix(col, prefix)
            parts.append(f'"{col}" AS "{new}"' if new != col else f'"{col}"')
    con.execute(
        f"CREATE OR REPLACE TABLE {dst} AS SELECT {', '.join(parts)} FROM {src}"
    )


def transform_layer(
    con,
    label: str,
    file_path: str,
    prefix: str,
    layer_cfg: dict,
    common_cols: list,
    key_col: str,
    input_format: str,
    skip_load: bool = True,
) -> str:
    """
    Full single-layer pipeline.
    If skip_load=True, assumes table 'raw_{label}' already exists.
    """
    logger.info(f"  Transforming layer: {label} (prefix={prefix})")

    t_raw = f"raw_{label}" if skip_load else f"raw_{label}_tmp"
    t_drop = f"drop_{label}"
    t_ren = f"ren_{label}"
    t_chg = f"chg_{label}"
    t_out = f"out_{label}"

    # Only load if skip_load is False
    if not skip_load:
        logger.debug(f"    Loading {label} from {file_path}")
        fp = file_path.replace("\\", "/")
        con.execute(f"CREATE OR REPLACE TABLE {t_raw} AS SELECT * FROM ST_READ('{fp}')")

    logger.debug("    Dropping unwanted columns")
    _step_drop(con, t_raw, t_drop, layer_cfg["drop_columns"])

    logger.debug("    Renaming columns")
    _step_rename(con, t_drop, t_ren, layer_cfg["rename_columns"])

    logger.debug("    Applying column transformations")
    _step_column_changes(con, t_ren, t_chg, layer_cfg["column_changes"], prefix, label)

    logger.debug("    Applying prefixes")
    _step_apply_prefix(con, t_chg, t_out, prefix, set(common_cols) | {key_col})

    n = len(get_columns(con, t_out))
    logger.info(f"    → {n} columns after transform")
    return t_out


# ===========================================================================
# SECTION 6 — MERGE
# ===========================================================================


def merge_layers(
    con, layer_tables: dict, key_col: str, rename_key_to: str, common_cols: list
) -> str:
    """
    INNER JOIN all transformed layer tables on key_col.
    """
    logger.info("Merging layers")

    labels = list(layer_tables.keys())
    logger.debug(f"Layers to merge: {labels}")

    aliases = {lbl: f"t{i}" for i, lbl in enumerate(labels)}
    base_lbl = labels[0]
    base_alias = aliases[base_lbl]
    base_cols = get_columns(con, layer_tables[base_lbl])

    # 1. key → renamed
    select = [f'{base_alias}."{key_col}" AS "{rename_key_to}"']

    # 2. common cols (except key) from base table
    for gcol in common_cols:
        if gcol != key_col and gcol in base_cols:
            select.append(f'{base_alias}."{gcol}"')

    # 3. layer-unique cols in layer order
    skip = set(common_cols) | {key_col}
    for lbl in labels:
        alias = aliases[lbl]
        for col in get_columns(con, layer_tables[lbl]):
            if col not in skip:
                select.append(f'{alias}."{col}"')

    # 4. FROM + JOINs
    base_tbl = layer_tables[base_lbl]
    joins = [f"{base_tbl} {base_alias}"]
    for lbl in labels[1:]:
        tbl = layer_tables[lbl]
        alias = aliases[lbl]
        joins.append(
            f"INNER JOIN {tbl} {alias} "
            f'ON {base_alias}."{key_col}" = {alias}."{key_col}"'
        )

    con.execute(f"""
        CREATE OR REPLACE TABLE merged_data AS
        SELECT
            {(",\n    ").join(select)}
        FROM {chr(10).join(f"    {j}" for j in joins)}
    """)

    rows = con.execute("SELECT COUNT(*) FROM merged_data").fetchone()[0]
    cols = len(get_columns(con, "merged_data"))
    logger.success(f"Merged: {rows:,} rows × {cols:,} columns")
    return "merged_data"


# ===========================================================================
# SECTION 7 — EXPORT
# ===========================================================================


def export_table(con, table: str, output_path: str, output_format: str):
    """
    Write the DuckDB table to the requested output format.
    Supported: parquet, geojson.
    """
    logger.info(f"Exporting to {output_format}: {output_path}")
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    out = output_path.replace("\\", "/")
    fmt = output_format.lower()

    if fmt == "parquet":
        con.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET)")
    elif fmt == "geojson":
        con.execute(f"COPY {table} TO '{out}' (FORMAT GDAL, DRIVER 'GeoJSON')")
    else:
        logger.error(f"Unsupported output format: {output_format}")
        raise ValueError(f"Unsupported output_format: '{output_format}'")

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.success(f"Written: {output_path} ({size_mb:.2f} MB)")


# ===========================================================================
# SECTION 8 — MAIN ORCHESTRATOR
# ===========================================================================


def run(config_path: str) -> str:
    logger.info(f"Starting pipeline with config: {config_path}")
    cfg = load_config(config_path)

    output_path = cfg["output_path"]
    output_format = cfg["output_format"]
    input_format = cfg["input_format"]
    layers = cfg["layers"]
    column_map = cfg["column_map"]
    common_cols = cfg["common_cols"]
    key_col = cfg["key"]
    rename_key = cfg["rename_key_to"]
    base_layer = cfg["base_layer"]

    prefixes = {label: derive_prefix(label) for label in layers}
    ordered = [base_layer] + [layer for layer in layers if layer != base_layer]
    logger.debug(f"Layer order: {ordered}")

    # Use DataLakeLoader for connection management AND loading
    with DataLakeLoader(["spatial"]) as loader:
        # Step 1: Validate
        logger.info("Step 1: Validating layer files")
        for label in ordered:
            path = layers[label]
            if not path.startswith("s3://") and not os.path.exists(path):
                logger.error(f"[{label}] file not found: {path}")
                raise FileNotFoundError(f"[{label}] file not found: {path}")

            if path.startswith("s3://"):
                try:
                    schema = loader.validate_schema(path)
                    logger.info(f"  [OK] {label:30s}  {len(schema['columns'])} columns")
                except Exception as e:
                    logger.error(f"  [FAIL] {label}: {e}")
                    raise
            else:
                logger.info(f"  [OK] {label:30s}  file={os.path.basename(path)}")

        # Step 2: Load layers using DataLakeLoader
        logger.info("Step 2: Loading raw layers")
        for label in ordered:
            try:
                loader.load_layer(
                    file_path=layers[label],
                    table_name=f"raw_{label}",
                    input_format=input_format,
                )
            except Exception:
                logger.exception(f"Failed to load {label}")
                raise

        # Step 3: Transform
        logger.info("Step 3: Transforming layers")
        layer_tables = {}
        for label in ordered:
            try:
                layer_tables[label] = transform_layer(
                    loader.conn,
                    label=label,
                    file_path=layers[label],
                    prefix=prefixes[label],
                    layer_cfg=column_map[label],
                    common_cols=common_cols,
                    key_col=key_col,
                    input_format=input_format,
                    skip_load=True,
                )
            except Exception:
                logger.exception(f"Failed to transform {label}")
                raise

        # Step 4: Merge
        logger.info("Step 4: Merging layers")
        try:
            merged = merge_layers(
                loader.conn, layer_tables, key_col, rename_key, common_cols
            )
        except Exception:
            logger.exception("Failed to merge layers")
            raise

        # Step 5: Export
        logger.info("Step 5: Exporting")
        try:
            export_table(loader.conn, merged, output_path, output_format)
        except Exception:
            logger.exception("Failed to export")
            raise

    logger.success("Pipeline completed successfully!")
    return output_path


def main():
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = input("Enter path to config JSON: ").strip()

    if not os.path.exists(config_path):
        logger.error(f"Config not found: {config_path}")
        sys.exit(1)

    try:
        run(config_path)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
