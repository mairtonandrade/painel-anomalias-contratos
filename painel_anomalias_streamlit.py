"""
Painel Streamlit para visualização das anomalias (tabela anomalias_contratos).

Como executar:
  streamlit run painel_anomalias_streamlit.py

Fonte de dados (prioridade):
  1) CSV local (recomendado para Streamlit Cloud):
     - coloque `data/anomalias_contratos.csv` no repositório
  2) PostgreSQL (opcional):
     - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD (env vars ou st.secrets)
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import psycopg2
import streamlit as st


def _get_setting(key: str, default: str) -> str:
    v = os.getenv(key)
    if v is not None and str(v).strip() != "":
        return str(v)
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return default


def get_db_conn():
    return psycopg2.connect(
        host=_get_setting("DB_HOST", "localhost"),
        port=int(_get_setting("DB_PORT", "5432")),
        dbname=_get_setting("DB_NAME", "aula"),
        user=_get_setting("DB_USER", "postgres"),
        password=_get_setting("DB_PASSWORD", "1234"),
    )


@st.cache_data(ttl=60)
def carregar_anomalias() -> pd.DataFrame:
    # Se existir CSV local no repo, usa ele (melhor para publicar no Streamlit Cloud).
    csv_path = os.path.join(os.path.dirname(__file__), "data", "anomalias_contratos.csv")
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        if "data_assinatura" in df.columns:
            df["data_assinatura"] = pd.to_datetime(df["data_assinatura"], errors="coerce").dt.date
        if "detectado_em" in df.columns:
            df["detectado_em"] = pd.to_datetime(df["detectado_em"], errors="coerce")
        return df

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
    if "data_assinatura" in df.columns:
        df["data_assinatura"] = pd.to_datetime(df["data_assinatura"], errors="coerce").dt.date
    if "detectado_em" in df.columns:
        df["detectado_em"] = pd.to_datetime(df["detectado_em"], errors="coerce")
    return df


def _fmt_int(v: float | int | None) -> str:
    try:
        return f"{int(v or 0):,}".replace(",", ".")
    except Exception:
        return "0"


def _fmt_currency(v: float | int | None) -> str:
    try:
        s = f"{float(v or 0.0):,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return "R$ 0,00"


def _kpi_card(title: str, value: str, subtitle: str | None = None, tone: str = "neutral") -> None:
    tone_map = {
        "neutral": ("#a78bfa", "#0b1220"),
        "ok": ("#22c55e", "#071a10"),
        "warn": ("#fbbf24", "#1a1206"),
        "danger": ("#fb7185", "#1a0b10"),
        "info": ("#38bdf8", "#06131b"),
    }
    accent, bg = tone_map.get(tone, tone_map["neutral"])
    subtitle_html = f"""<div class="kpi-sub">{subtitle}</div>""" if subtitle else ""
    st.markdown(
        f"""
<div class="kpi" style="background:{bg}; border:1px solid rgba(255,255,255,0.10);">
  <div class="kpi-accent" style="background:{accent};"></div>
  <div class="kpi-title">{title}</div>
  <div class="kpi-value">{value}</div>
  {subtitle_html}
</div>
""".strip(),
        unsafe_allow_html=True,
    )

def _chart_container(inner_html: str) -> None:
    st.markdown(
        f"""
<div class="chart-card">
  {inner_html}
</div>
""".strip(),
        unsafe_allow_html=True,
    )


def _bar_html(title: str, series: pd.Series, color: str = "#60a5fa") -> None:
    if series is None or series.empty:
        st.info("Sem dados para exibir.")
        return
    s = series.copy()
    s.index = [str(x) for x in s.index]
    max_v = float(s.max()) if float(s.max()) > 0 else 1.0

    rows = []
    for label, val in s.items():
        w = (float(val) / max_v) * 100.0
        rows.append(
            f"""
<div class="bar-row">
  <div class="bar-label" title="{label}">{label}</div>
  <div class="bar-track">
    <div class="bar-fill" style="width:{w:.2f}%; background:{color};"></div>
  </div>
  <div class="bar-val">{int(val)}</div>
</div>
""".strip()
        )
    _chart_container(
        f"""
<div class="chart-title">{title}</div>
<div class="bar-list">
  {''.join(rows)}
</div>
""".strip()
    )


def _line_svg(title: str, series: pd.Series, color: str = "#34d399") -> None:
    if series is None or series.empty:
        st.info("Sem dados para exibir.")
        return

    s = series.copy().sort_index()
    vals = [float(v) for v in s.values.tolist()]
    if len(vals) < 2:
        st.info("Sem dados suficientes para exibir a série temporal.")
        return

    w, h = 640, 220
    pad_l, pad_r, pad_t, pad_b = 44, 16, 18, 34
    inner_w = w - pad_l - pad_r
    inner_h = h - pad_t - pad_b

    min_v = min(vals)
    max_v = max(vals)
    span = (max_v - min_v) if (max_v - min_v) != 0 else 1.0

    pts = []
    for i, v in enumerate(vals):
        x = pad_l + (i / (len(vals) - 1)) * inner_w
        y = pad_t + (1 - ((v - min_v) / span)) * inner_h
        pts.append((x, y))

    path_d = "M " + " L ".join([f"{x:.1f},{y:.1f}" for x, y in pts])
    area_d = path_d + f" L {pts[-1][0]:.1f},{pad_t + inner_h:.1f} L {pts[0][0]:.1f},{pad_t + inner_h:.1f} Z"

    idx = list(s.index)
    lbl_first = pd.to_datetime(idx[0]).strftime("%Y-%m") if idx else ""
    lbl_last = pd.to_datetime(idx[-1]).strftime("%Y-%m") if idx else ""
    y_top = int(max_v)
    y_bot = int(min_v)

    _chart_container(
        f"""
<div class="chart-title">{title}</div>
<svg viewBox="0 0 {w} {h}" width="100%" height="240" role="img" aria-label="{title}">
  <rect x="0" y="0" width="{w}" height="{h}" rx="12" ry="12" fill="rgba(255,255,255,0.02)" />
  <line x1="{pad_l}" y1="{pad_t + inner_h}" x2="{w - pad_r}" y2="{pad_t + inner_h}" stroke="rgba(255,255,255,0.12)" />
  <line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + inner_h}" stroke="rgba(255,255,255,0.12)" />
  <text x="{pad_l - 8}" y="{pad_t + 10}" text-anchor="end" fill="rgba(255,255,255,0.70)" font-size="11">{y_top}</text>
  <text x="{pad_l - 8}" y="{pad_t + inner_h}" text-anchor="end" fill="rgba(255,255,255,0.70)" font-size="11">{y_bot}</text>
  <text x="{pad_l}" y="{h - 10}" text-anchor="start" fill="rgba(255,255,255,0.70)" font-size="11">{lbl_first}</text>
  <text x="{w - pad_r}" y="{h - 10}" text-anchor="end" fill="rgba(255,255,255,0.70)" font-size="11">{lbl_last}</text>
  <path d="{area_d}" fill="{color}" opacity="0.10"></path>
  <path d="{path_d}" fill="none" stroke="{color}" stroke-width="2.5"></path>
  {''.join([f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.6" fill="{color}" />' for x,y in pts])}
</svg>
""".strip()
    )


def main():
    st.set_page_config(page_title="Painel de Anomalias (Contratos)", layout="wide")
    st.markdown(
        """
<style>
/* Layout base */
.block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
/* Tipografia */
h1, h2, h3 { letter-spacing: -0.02em; }
/* Cards KPI */
.kpi { border-radius: 16px; padding: 14px 16px 14px; position: relative; overflow: hidden; min-height: 118px; display: flex; flex-direction: column; justify-content: space-between; }
.kpi-accent { height: 4px; width: 100%; position: absolute; top: 0; left: 0; opacity: 0.9; }
.kpi-title { color: rgba(255,255,255,0.72); font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em; }
.kpi-value { color: rgba(255,255,255,0.96); font-size: 26px; font-weight: 900; margin-top: 10px; margin-bottom: 4px; line-height: 1.10; text-shadow: 0 1px 10px rgba(0,0,0,0.35); }
.kpi-sub { color: rgba(255,255,255,0.72); font-size: 12px; }
/* Seções */
.section-title { margin-top: 14px; margin-bottom: 10px; font-size: 14px; font-weight: 800; color: rgba(255,255,255,0.85); }
/* Charts */
.chart-card { border-radius: 16px; padding: 14px 14px 12px; border: 1px solid rgba(255,255,255,0.10); background: #0b1220; min-height: 300px; display: flex; flex-direction: column; }
.chart-title { color: rgba(255,255,255,0.85); font-size: 12px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 10px; }
.bar-list { display: flex; flex-direction: column; gap: 10px; }
.bar-row { display: grid; grid-template-columns: 150px 1fr 52px; gap: 10px; align-items: center; }
.bar-label { color: rgba(255,255,255,0.78); font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.bar-track { height: 10px; background: rgba(255,255,255,0.08); border-radius: 999px; overflow: hidden; }
.bar-fill { height: 100%; border-radius: 999px; }
.bar-val { color: rgba(255,255,255,0.78); font-size: 12px; text-align: right; font-variant-numeric: tabular-nums; }
/* Headings dentro do grid de gráficos */
h3 { margin-top: 0.4rem; margin-bottom: 0.6rem; font-weight: 850; }
/* Sidebar */
section[data-testid="stSidebar"] { border-right: 1px solid rgba(255,255,255,0.06); }
</style>
""".strip(),
        unsafe_allow_html=True,
    )

    st.markdown(
        """
<div style="display:flex; align-items:flex-end; justify-content:space-between; gap:16px; margin-bottom: 10px;">
  <div>
    <div style="font-size:12px; color: rgba(255,255,255,0.62); font-weight:600; letter-spacing:0.08em; text-transform:uppercase;">
      Monitoramento diário
    </div>
    <div style="font-size:34px; font-weight:900; letter-spacing:-0.03em; margin-top:6px;">
      Painel de Anomalias (Contratos)
    </div>
    <div style="color: rgba(255,255,255,0.65); font-size:13px; margin-top:6px;">
      Indicadores consolidados a partir de <code>public.anomalias_contratos</code>.
    </div>
  </div>
</div>
""".strip(),
        unsafe_allow_html=True,
    )

    df = carregar_anomalias()

    with st.sidebar:
        st.markdown("### Filtros")

        riscos = ["ALTO", "MÉDIO", "BAIXO"]
        riscos_sel = st.multiselect("Nível de risco", riscos, default=riscos)

        orgaos = sorted([x for x in df.get("orgao_nome", pd.Series(dtype=str)).dropna().unique().tolist()])
        orgao_sel = st.selectbox("Órgão", options=["Todos"] + orgaos, index=0)

        min_d = df["data_assinatura"].min() if "data_assinatura" in df.columns else None
        max_d = df["data_assinatura"].max() if "data_assinatura" in df.columns else None
        if isinstance(min_d, date) and isinstance(max_d, date):
            intervalo = st.date_input("Data de assinatura (intervalo)", value=(min_d, max_d))
        else:
            intervalo = None

        st.divider()
        if st.button("Atualizar agora"):
            st.cache_data.clear()
            st.rerun()

    df_f = df.copy()
    if "nivel_risco" in df_f.columns:
        df_f = df_f[df_f["nivel_risco"].isin(riscos_sel)]
    if orgao_sel != "Todos" and "orgao_nome" in df_f.columns:
        df_f = df_f[df_f["orgao_nome"] == orgao_sel]
    if intervalo and "data_assinatura" in df_f.columns:
        ini, fim = intervalo
        df_f = df_f[(df_f["data_assinatura"] >= ini) & (df_f["data_assinatura"] <= fim)]

    alto = int((df_f.get("nivel_risco") == "ALTO").sum()) if "nivel_risco" in df_f.columns else 0
    medio = int((df_f.get("nivel_risco") == "MÉDIO").sum()) if "nivel_risco" in df_f.columns else 0
    baixo = int((df_f.get("nivel_risco") == "BAIXO").sum()) if "nivel_risco" in df_f.columns else 0
    total = int(len(df_f))

    total_valor = float(df_f["valor_global"].fillna(0).sum()) if "valor_global" in df_f.columns else 0.0
    media_valor = float(df_f["valor_global"].dropna().mean()) if "valor_global" in df_f.columns and not df_f["valor_global"].dropna().empty else 0.0
    max_valor = float(df_f["valor_global"].dropna().max()) if "valor_global" in df_f.columns and not df_f["valor_global"].dropna().empty else 0.0
    prazo_medio = float(df_f["prazo_vigencia_dias"].dropna().mean()) if "prazo_vigencia_dias" in df_f.columns and not df_f["prazo_vigencia_dias"].dropna().empty else 0.0

    orgao_top = ""
    if "orgao_nome" in df_f.columns and not df_f["orgao_nome"].dropna().empty:
        orgao_top = str(df_f["orgao_nome"].value_counts().idxmax())
    forn_top = ""
    if "fornecedor_nome" in df_f.columns and not df_f["fornecedor_nome"].dropna().empty:
        forn_top = str(df_f["fornecedor_nome"].value_counts().idxmax())
    perc_medio = float(df_f["percentil_risco"].dropna().mean()) if "percentil_risco" in df_f.columns and not df_f["percentil_risco"].dropna().empty else 0.0
    score_min = float(df_f["score_anomalia"].dropna().min()) if "score_anomalia" in df_f.columns and not df_f["score_anomalia"].dropna().empty else 0.0

    st.markdown('<div class="section-title">Resumo executivo</div>', unsafe_allow_html=True)
    r1, r2, r3, r4 = st.columns(4)
    with r1:
        _kpi_card("Risco alto", _fmt_int(alto), subtitle="Prioridade máxima", tone="danger")
    with r2:
        _kpi_card("Risco médio", _fmt_int(medio), subtitle="Monitoramento", tone="warn")
    with r3:
        _kpi_card("Risco baixo", _fmt_int(baixo), subtitle="Baixa prioridade", tone="ok")
    with r4:
        _kpi_card("Total", _fmt_int(total), subtitle="Anomalias filtradas", tone="info")

    st.markdown('<div class="section-title">Impacto financeiro</div>', unsafe_allow_html=True)
    i1, i2, i3, i4 = st.columns(4)
    with i1:
        _kpi_card("Soma dos valores", _fmt_currency(total_valor), subtitle="Somatório de valor_global", tone="info")
    with i2:
        _kpi_card("Ticket médio", _fmt_currency(media_valor), subtitle="Média de valor_global", tone="neutral")
    with i3:
        _kpi_card("Maior valor", _fmt_currency(max_valor), subtitle="Máximo de valor_global", tone="neutral")
    with i4:
        _kpi_card("Prazo médio", f"{_fmt_int(prazo_medio)} dias", subtitle="Média de prazo_vigencia_dias", tone="neutral")

    st.markdown('<div class="section-title">Quem concentra o risco</div>', unsafe_allow_html=True)
    j1, j2, j3, j4 = st.columns(4)
    with j1:
        _kpi_card("Órgão com mais anomalias", orgao_top or "-", subtitle="Mais recorrente no filtro atual", tone="neutral")
    with j2:
        _kpi_card("Fornecedor mais recorrente", forn_top or "-", subtitle="Mais recorrente no filtro atual", tone="neutral")
    with j3:
        _kpi_card("Percentil médio", _fmt_int(perc_medio), subtitle="Média de percentil_risco", tone="neutral")
    with j4:
        _kpi_card("Score mais anômalo", f"{score_min:,.6f}", subtitle="Mínimo de score_anomalia", tone="neutral")

    st.markdown('<div class="section-title">Gráficos</div>', unsafe_allow_html=True)
    g1, g2 = st.columns([1, 1])
    with g1:
        st.subheader("Distribuição por risco")
        if "nivel_risco" in df_f.columns and not df_f.empty:
            serie = df_f["nivel_risco"].value_counts().reindex(["ALTO", "MÉDIO", "BAIXO"]).fillna(0)
            _bar_html("Distribuição por risco", serie, color="#60a5fa")
        else:
            st.info("Sem dados para exibir.")

    with g2:
        st.subheader("Top 10 órgãos (quantidade)")
        if "orgao_nome" in df_f.columns and not df_f.empty:
            top = df_f["orgao_nome"].fillna("N/I").value_counts().head(10)
            _bar_html("Top 10 órgãos (quantidade)", top, color="#a78bfa")
        else:
            st.info("Sem dados para exibir.")

    g3, g4 = st.columns([1, 1])
    with g3:
        st.subheader("Top 10 fornecedores (quantidade)")
        if "fornecedor_nome" in df_f.columns and not df_f.empty:
            top = df_f["fornecedor_nome"].fillna("N/I").value_counts().head(10)
            _bar_html("Top 10 fornecedores (quantidade)", top, color="#fbbf24")
        else:
            st.info("Sem dados para exibir.")

    with g4:
        st.subheader("Evolução (anomalias por mês)")
        if "data_assinatura" in df_f.columns and not df_f.empty:
            tmp = df_f.dropna(subset=["data_assinatura"]).copy()
            if not tmp.empty:
                tmp["mes"] = pd.to_datetime(tmp["data_assinatura"]).dt.to_period("M").dt.to_timestamp()
                serie = tmp.groupby("mes").size().sort_index()
                _line_svg("Evolução (anomalias por mês)", serie, color="#34d399")
            else:
                st.info("Sem datas válidas para exibir.")
        else:
            st.info("Sem dados para exibir.")


if __name__ == "__main__":
    main()

