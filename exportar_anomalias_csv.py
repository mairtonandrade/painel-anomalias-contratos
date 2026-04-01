"""
Exporta a tabela public.anomalias_contratos (PostgreSQL) para CSV.

Uso (PowerShell):
  $env:DB_HOST="localhost"
  $env:DB_PORT="5432"
  $env:DB_NAME="aula"
  $env:DB_USER="postgres"
  $env:DB_PASSWORD="1234"
  python exportar_anomalias_csv.py

Saída:
  c:\\airflow\\anomalias_contratos_suite\\data\\anomalias_contratos.csv
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "aula"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "1234"),
    )


def main() -> int:
    sql = """
        SELECT
            numero_contrato,
            objeto,
            fornecedor_nome,
            orgao_nome,
            valor_global,
            prazo_vigencia_dias,
            score_anomalia,
            percentil_risco,
            nivel_risco,
            data_assinatura,
            detectado_em
        FROM public.anomalias_contratos
        ORDER BY score_anomalia ASC, detectado_em DESC
    """

    with get_db_conn() as conn:
        df = pd.read_sql(sql, conn)

    out_dir = Path(__file__).resolve().parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "anomalias_contratos.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"CSV exportado: {out_path} | linhas={len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

