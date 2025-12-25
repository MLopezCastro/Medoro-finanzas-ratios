"""
run_pipeline.py (PUBLIC / SHOWCASE)

Pipeline local:
Excel -> CSV -> SQL bronze (TRUNCATE + BULK INSERT) -> SQL silver refresh (scripts .sql)

Modes:
- PROCESS_ALL=True  -> procesa TODOS los archivos definidos en config/tables.yml
- PROCESS_ALL=False -> procesa solo TARGET_FILE

Features:
- Evita locks (OneDrive) copiando el Excel a staging_csv/__tmp__
- Valida columnas EXACTAS (expected_columns)
- Si sheet=null y hay múltiples sheets, falla y lista nombres
- Valida comas solo donde rules.no_commas_in lo indique
- Trim de texto solo donde rules.trim_text lo indique
- Rename de columnas opcional (rename_columns) ANTES del CSV (ej: AÑO->ANIO)
- Exporta CSV con QUOTE_MINIMAL
- Ejecuta SQL .sql separando lotes por GO
- Logs a consola + archivo
"""

import os
import sys
import csv
import yaml
import shutil
import logging
from datetime import datetime
import time
import re

import pandas as pd
import pyodbc


# ======================
# Base paths (repo)
# ======================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Estructura estándar del repo (NO contiene datos reales)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
INPUT_DIR = os.path.join(DATA_DIR, "input_excels")     # no se versiona
CSV_DIR = os.path.join(DATA_DIR, "staging_csv")        # no se versiona
REJECT_DIR = os.path.join(DATA_DIR, "rejected")        # no se versiona
LOG_DIR = os.path.join(DATA_DIR, "logs")               # no se versiona

CONFIG_DIR = os.path.join(BASE_DIR, "config")
SQL_DIR = os.path.join(BASE_DIR, "..", "sql")

for d in [INPUT_DIR, CSV_DIR, REJECT_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

TMP_PREFIX = "__tmp__"


# ======================
# Logging
# ======================
log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)


# ======================
# Config loading
# ======================
def load_yaml(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe config: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_app_config():
    """
    Lee config/app.yml (no versionado) o config/app.example.yml (template).
    """
    path = os.path.join(CONFIG_DIR, "app.yml")
    if not os.path.exists(path):
        path = os.path.join(CONFIG_DIR, "app.example.yml")
        logging.warning("No se encontró config/app.yml. Usando config/app.example.yml (template).")
    return load_yaml(path)


def load_tables_config():
    path = os.path.join(CONFIG_DIR, "tables.yml")
    if not os.path.exists(path):
        path = os.path.join(CONFIG_DIR, "tables.example.yml")
        logging.warning("No se encontró config/tables.yml. Usando config/tables.example.yml (template).")
    cfg = load_yaml(path)
    tables = cfg.get("tables") or []
    if not isinstance(tables, list):
        raise ValueError("tables.yml inválido: 'tables' debe ser una lista.")
    return tables


# ======================
# SQL Connection
# ======================
def get_conn(sql_cfg: dict):
    """
    Soporta:
    - Trusted_Connection (Windows auth)
    - o usuario/password
    """
    driver = sql_cfg.get("driver", "ODBC Driver 17 for SQL Server")
    server = sql_cfg.get("server")
    database = sql_cfg.get("database")
    trusted = bool(sql_cfg.get("trusted_connection", True))
    user = sql_cfg.get("user")
    password = sql_cfg.get("password")

    if not server or not database:
        raise ValueError("Config SQL incompleta: faltan 'server' y/o 'database' en app.yml")

    if trusted:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
    else:
        if not user or not password:
            raise ValueError("trusted_connection=false requiere user y password en app.yml")
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};PWD={password};"
        )

    return pyodbc.connect(conn_str, autocommit=False)


# ======================
# Validations / Transforms
# ======================
def validate_expected_columns(df, expected):
    expected = expected or []
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected]
    return missing, extra


def trim_text_columns(df, cols):
    cols = cols or []
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


def rename_columns(df, mapping: dict):
    """
    mapping ejemplo:
      {"AÑO":"ANIO"}
    """
    if not mapping:
        return df
    existing = {k: v for k, v in mapping.items() if k in df.columns}
    if existing:
        df = df.rename(columns=existing)
    return df


def validate_no_commas(df, col, file_name):
    errors = []
    if col not in df.columns:
        return errors

    s = df[col].astype(str)
    mask = s.str.contains(",", na=False)
    for idx in df.index[mask]:
        errors.append(
            {
                "file": file_name,
                "column": col,
                "row_excel": int(idx) + 2,  # header row = 1
                "value": s.loc[idx],
                "error": f"Comma ',' found in {col}. Remove/replace comma.",
            }
        )
    return errors


# ======================
# IO
# ======================
def safe_copy_to_tmp(src_path, tmp_path, retries=8, wait_sec=1):
    for _ in range(retries):
        try:
            shutil.copy2(src_path, tmp_path)
            return True
        except PermissionError:
            time.sleep(wait_sec)
    return False


def export_csv_safe(df, out_path):
    df.to_csv(
        out_path,
        index=False,
        encoding="utf-8",
        sep=",",
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )


def read_excel_one_sheet(tmp_excel_path, spec, file_name):
    sheet_spec = spec.get("sheet", None)

    if sheet_spec is None:
        xls = pd.ExcelFile(tmp_excel_path, engine="openpyxl")
        if len(xls.sheet_names) == 1:
            return pd.read_excel(tmp_excel_path, engine="openpyxl", sheet_name=xls.sheet_names[0])
        raise ValueError(
            f"{file_name}: Excel tiene múltiples sheets {xls.sheet_names}. "
            f"Seteá 'sheet' en tables.yml (nombre o índice)."
        )

    return pd.read_excel(tmp_excel_path, engine="openpyxl", sheet_name=sheet_spec)


# ======================
# SQL Exec
# ======================
def run_sql_file(conn, rel_path):
    """
    Ejecuta un .sql y separa batches por GO (case-insensitive, line-only).
    """
    if not rel_path:
        raise ValueError("silver_refresh_sql vacío en tables.yml")

    # rel_path puede venir como: sql/silver/01_cheques_recibidos.sql
    sql_path = os.path.join(BASE_DIR, "..", rel_path.replace("/", os.path.sep))
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"No existe SQL file: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    cur = conn.cursor()
    batches = re.split(r"(?im)^\s*GO\s*$", sql)
    for batch in batches:
        if batch.strip():
            cur.execute(batch)
    conn.commit()


def bulk_insert_csv(conn, bronze_table, csv_path):
    """
    Nota: En SQL Server, BULK INSERT NO acepta parámetros.
    Por eso se arma un string SQL con la ruta literal.
    """
    if not bronze_table:
        raise ValueError("bronze_table vacío en tables.yml")

    # Normaliza backslashes para SQL
    csv_path_sql = csv_path.replace("\\", "\\\\")

    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {bronze_table};")

    bulk_sql = f"""
BULK INSERT {bronze_table}
FROM '{csv_path_sql}'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    CODEPAGE = '65001'
);
"""
    cur.execute(bulk_sql)
    conn.commit()


# ======================
# Core per-table
# ======================
def process_one_table(spec, sql_cfg):
    file_name = str(spec.get("file", "")).strip()
    if not file_name:
        raise ValueError("Spec inválido: falta 'file'.")

    excel_path = os.path.join(INPUT_DIR, file_name)
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"No existe el Excel: {excel_path}")

    logging.info(f"Procesando {file_name}")

    tmp_excel = os.path.join(CSV_DIR, f"{TMP_PREFIX}{file_name}")
    if not safe_copy_to_tmp(excel_path, tmp_excel):
        raise PermissionError(f"No se puede leer/copiar (bloqueado): {excel_path}")

    df = read_excel_one_sheet(tmp_excel, spec, file_name)
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"{file_name}: lectura inválida (no devolvió DataFrame).")

    df.columns = [str(c).strip() for c in df.columns]

    # Validación columnas exactas ANTES de renombrar
    expected = spec.get("expected_columns") or []
    missing, extra = validate_expected_columns(df, expected)
    if missing or extra:
        raise ValueError(f"Columnas inválidas. Missing={missing} Extra={extra}")

    rules = spec.get("rules") or {}
    df = trim_text_columns(df, rules.get("trim_text") or [])

    # Comas
    errors = []
    for col in (rules.get("no_commas_in") or []):
        errors.extend(validate_no_commas(df, col, file_name))

    if errors:
        err_csv = os.path.join(
            LOG_DIR,
            f"errors_{os.path.splitext(file_name)[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        )
        pd.DataFrame(errors).to_csv(err_csv, index=False, encoding="utf-8")
        raise ValueError(f"Errores de validación (comas). Ver: {err_csv}")

    # Rename antes del CSV
    df = rename_columns(df, spec.get("rename_columns") or {})

    csv_name = str(spec.get("csv_name", "")).strip()
    if not csv_name:
        raise ValueError(f"{file_name}: falta csv_name en tables.yml")

    csv_path = os.path.join(CSV_DIR, csv_name)
    export_csv_safe(df, csv_path)
    logging.info(f"CSV generado: {csv_path}")

    conn = get_conn(sql_cfg)
    try:
        bulk_insert_csv(conn, spec.get("bronze_table"), csv_path)
        logging.info("Bulk insert bronze OK")

        run_sql_file(conn, spec.get("silver_refresh_sql"))
        logging.info("Refresh silver OK")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logging.info(f"OK: {file_name}")


# ======================
# Main
# ======================
def main():
    app_cfg = load_app_config()
    sql_cfg = app_cfg.get("sql") or {}

    tables = load_tables_config()

    # ===== CONFIG =====
    PROCESS_ALL = bool(app_cfg.get("process_all", True))
    TARGET_FILE = str(app_cfg.get("target_file", "")).strip()  # usado solo si PROCESS_ALL=False

    if PROCESS_ALL:
        ok = 0
        fail = 0
        for spec in tables:
            try:
                process_one_table(spec, sql_cfg)
                ok += 1
            except Exception as e:
                fail += 1
                logging.exception(f"FALLÓ {spec.get('file')}: {e}")
        logging.info(f"FIN. OK={ok} FAIL={fail}")
        if fail > 0:
            sys.exit(1)
        return

    if not TARGET_FILE:
        logging.error("PROCESS_ALL=False pero target_file está vacío en config/app.yml")
        sys.exit(1)

    spec = next(
        (t for t in tables if str(t.get("file", "")).strip().upper() == TARGET_FILE.upper()),
        None,
    )
    if not spec:
        logging.error(f"{TARGET_FILE} no encontrado en config/tables.yml")
        sys.exit(1)

    try:
        process_one_table(spec, sql_cfg)
    except Exception as e:
        logging.exception(f"FALLÓ {TARGET_FILE}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
