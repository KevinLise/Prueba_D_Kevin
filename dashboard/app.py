"""Dashboard Streamlit de SismoWatch sobre la API, MinIO Gold y PostgreSQL operativo."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from config.settings import get_settings
from dashboard.api_client import ApiError, SismoWatchClient

st.set_page_config(
    page_title="SismoWatch",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .stApp { background: #080f1e; }
    header[data-testid="stHeader"] { display: none; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    .stDeployButton { display: none; }
    .block-container { padding-top: 0.7rem; padding-bottom: 2rem; max-width: 1620px; }
    .sismo-topbar {
        display: flex; justify-content: space-between; align-items: center;
        padding: 0.2rem 0.15rem 0.75rem 0.15rem;
        border-bottom: 1px solid #1e293b; margin-bottom: 0.2rem;
    }
    .brand {
        display: flex; align-items: center; gap: 0.65rem;
        color: #f8fafc; font-size: 1.5rem; font-weight: 750; letter-spacing: -0.03em;
    }
    .brand .accent { color: #38bdf8; }
    .user-chip {
        display: flex; align-items: center; gap: 0.5rem;
        color: #cbd5e1; font-size: 0.9rem;
    }
    .avatar {
        width: 30px; height: 30px; border-radius: 50%;
        background: #1e293b; border: 1px solid #475569;
        display: flex; align-items: center; justify-content: center;
        color: #f8fafc; font-weight: 600;
    }
    .section-title { margin: 0.9rem 0 0.1rem 0; color: #f8fafc; font-size: 1.5rem; font-weight: 700; }
    .section-subtitle { margin: 0 0 0.85rem 0; color: #94a3b8; font-size: 0.9rem; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 1.65rem; border-bottom: 1px solid #1e293b;
        padding: 0.15rem 0.15rem 0 0.15rem;
    }
    .stTabs [data-baseweb="tab"] {
        color: #94a3b8; font-size: 0.86rem; font-weight: 600; padding: 0.55rem 0;
    }
    .stTabs [aria-selected="true"] { color: #f8fafc; border-bottom: 2px solid #ef4444; }
    .stTabs [data-baseweb="tab-highlight"] { background-color: #ef4444; }
    .stTabs [data-baseweb="tab-border"] { display: none; }
    section[data-testid="stSidebar"] { background: #0b1220; border-right: 1px solid #1e293b; }
    section[data-testid="stSidebar"] .block-container { padding-top: 1.1rem; }
    div[data-testid="stMetric"] {
        background: #0f172a; border: 1px solid #1e293b;
        border-radius: 0.6rem; padding: 0.8rem 1rem;
    }
    div[data-testid="stMetricLabel"] { color: #94a3b8; }
    div[data-testid="stMetricValue"] { color: #f8fafc; }
    .stButton > button {
        background: #111c30; color: #e2e8f0;
        border: 1px solid #334155; border-radius: 0.45rem;
    }
    .stButton > button:hover { border-color: #ef4444; color: #ffffff; }
    .stDataFrame { border: 1px solid #1e293b; border-radius: 0.5rem; }
    hr { border-color: #1e293b; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="sismo-topbar">
      <div class="brand">
        <svg width="44" height="34" viewBox="0 0 44 34" fill="none" aria-hidden="true">
          <polyline points="2,17 9,17 12,8 16,27 20,11 24,23 28,13 32,21 36,17 42,17"
            stroke="#38bdf8" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <span>Sismo<span class="accent">Watch</span></span>
      </div>
      <div class="user-chip"><span class="avatar">K</span>Kevin</div>
    </div>
    """,
    unsafe_allow_html=True,
)

settings = get_settings()
client = SismoWatchClient(settings.api_url, settings.api_key)


@st.cache_data(ttl=30, show_spinner=False)
def load_summary(_client):
    return _client.summary()


@st.cache_data(ttl=30, show_spinner=False)
def load_daily(_client, days: int):
    return _client.daily(days)


@st.cache_data(ttl=30, show_spinner=False)
def load_magnitudes(_client):
    return _client.magnitudes()


@st.cache_data(ttl=30, show_spinner=False)
def load_places(_client, limit: int):
    return _client.places(limit)


@st.cache_data(ttl=30, show_spinner=False)
def load_regions(_client, limit: int):
    return _client.regions(limit)


@st.cache_data(ttl=30, show_spinner=False)
def load_events(_client, limit: int, min_magnitude: float, region: str, place: str):
    return _client.events(
        limit=limit,
        min_magnitude=min_magnitude,
        region=region or None,
        place=place or None,
    )


@st.cache_data(ttl=30, show_spinner=False)
def load_quality(_client, limit: int):
    return _client.quality(limit)


@st.cache_data(ttl=30, show_spinner=False)
def load_runs(_client, limit: int):
    return _client.runs(limit)


@st.cache_data(ttl=30, show_spinner=False)
def load_metadata_summary(_client):
    return _client.metadata_summary()


@st.cache_data(ttl=30, show_spinner=False)
def load_metadata_runs(_client, limit: int):
    return _client.metadata_runs(limit)


@st.cache_data(ttl=30, show_spinner=False)
def load_metadata_quality(_client, limit: int):
    return _client.metadata_quality(limit)


@st.cache_data(ttl=30, show_spinner=False)
def load_reports(_client, limit: int = 10):
    return _client.reports(limit)


@st.cache_data(ttl=30, show_spinner=False)
def load_anomalies(_client, limit: int = 30):
    return _client.anomalies(limit)


@st.cache_data(ttl=15, show_spinner=False)
def load_ai_audit(_client, limit: int = 25):
    return _client.ai_audit(limit)


STATUS_LABELS = {
    "pending": "Pendiente",
    "running": "Ejecutando",
    "ingested": "Ejecutando",
    "processed": "Ejecutando",
    "completed": "Exitoso",
    "successful": "Exitoso",
    "failed": "Fallido",
}


def status_label(value) -> str:
    return STATUS_LABELS.get(str(value or "").lower(), str(value or "Sin datos"))


def format_duration(value) -> str:
    if value is None:
        return "Sin datos"
    seconds = float(value)
    if seconds >= 60:
        return f"{seconds / 60:.1f} min"
    return f"{seconds:.0f} s"


try:
    client.health()
except (ApiError, Exception) as exc:
    st.error(
        "No se pudo conectar con la API o con PostgreSQL. "
        "Revisa que los servicios api y sismowatch-db esten activos."
    )
    st.exception(exc)
    st.stop()

region_rows = load_regions(client, 200)
place_rows = load_places(client, 300)
region_options = ["Todas las regiones"] + sorted(
    {row["region"] for row in region_rows if row.get("region")},
    key=str.casefold,
)
place_options = ["Todos los lugares"] + sorted(
    {row["place"] for row in place_rows if row.get("place")},
    key=str.casefold,
)

with st.sidebar:
    st.header("Filtros")
    auto_refresh = st.checkbox("Actualizar cada 60 segundos", value=True)
    if auto_refresh:
        st_autorefresh(interval=60000, key="sismowatch_auto_refresh")
    if st.button("↻ Actualizar ahora"):
        st.cache_data.clear()
        st.rerun()
    min_magnitude = st.slider("Magnitud mínima", -1.5, 10.0, 2.5, 0.1)
    days = st.slider("Días de actividad", 1, 30, 30)
    map_limit = st.slider("Eventos en el mapa", 100, 5000, 1000, 100)
    region_choice = st.selectbox("Filtrar región", region_options)
    place_choice = st.selectbox("Filtrar lugar", place_options)
    user_role = st.selectbox("Rol de sesión", ["admin", "analyst", "viewer"], index=0)
    region_filter = "" if region_choice == "Todas las regiones" else region_choice
    place_filter = "" if place_choice == "Todos los lugares" else place_choice
    st.divider()
    st.caption(
        "Los datos se actualizan con el DAG de Airflow cada 5 minutos. "
        "El dashboard solo lee la API; no modifica Bronze, Silver, Gold ni PostgreSQL."
    )

summary_rows = load_summary(client)
summary = summary_rows[0] if summary_rows else {}
total_events = int(summary.get("total_events") or 0)
avg_magnitude = summary.get("avg_magnitude")
max_magnitude = summary.get("max_magnitude")
last_event_time = summary.get("max_event_time")

tabs = st.tabs(
    [
        "Resumen",
        "Evolución",
        "Mapa",
        "Zonas",
        "Regiones",
        "Calidad",
        "Agente IA",
        "Monitoreo",
    ],
    default="Mapa",
)

with tabs[0]:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Sismos", f"{total_events:,}")
    col2.metric(
        "Magnitud promedio",
        "Sin datos" if avg_magnitude is None else f"{float(avg_magnitude):.2f}",
    )
    col3.metric(
        "Magnitud maxima",
        "Sin datos" if max_magnitude is None else f"{float(max_magnitude):.2f}",
    )
    col4.metric(
        "Ultimo evento",
        "Sin datos" if last_event_time is None else str(last_event_time),
    )

    try:
        all_events_kpi = load_events(client, 5000, -1.5, "", "")
        tsunami_count = sum(1 for ev in all_events_kpi if int(ev.get("tsunami") or 0) > 0)
        ev_df_kpi = pd.DataFrame(all_events_kpi)
        if not ev_df_kpi.empty and "event_time" in ev_df_kpi.columns:
            ev_df_kpi["dt"] = pd.to_datetime(ev_df_kpi["event_time"], errors="coerce")
            ref_max_dt = ev_df_kpi["dt"].max()
            sismos_24h = (
                int((ev_df_kpi["dt"] >= (ref_max_dt - pd.Timedelta(hours=24))).sum())
                if pd.notna(ref_max_dt)
                else 0
            )
        else:
            sismos_24h = 0
        meta_kpi = load_metadata_summary(client)
        qual_kpi = (meta_kpi.get("quality") or {}).get("quality_percentage")
        anom_kpi = len(load_anomalies(client, 30))
    except Exception:
        sismos_24h = 0
        tsunami_count = 0
        qual_kpi = None
        anom_kpi = 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sismos (últimas 24h)", f"{sismos_24h:,}")
    k2.metric("Alertas de tsunami", f"{tsunami_count:,}")
    k3.metric("Calidad de datos", "Sin datos" if qual_kpi is None else f"{float(qual_kpi):.2f}%")
    k4.metric("Anomalías detectadas", f"{anom_kpi:,}")

    st.subheader("Resumen Gold")
    if summary:
        st.dataframe(pd.DataFrame([summary]), use_container_width=True, hide_index=True)
    else:
        st.info("Todavia no hay resumen Gold. Ejecuta el DAG de Airflow.")

    st.subheader("Distribucion por magnitud")
    magnitudes = pd.DataFrame(load_magnitudes(client))
    if magnitudes.empty:
        st.info("No hay distribucion de magnitudes disponible.")
    else:
        figure = px.bar(
            magnitudes,
            x="magnitude_class",
            y="event_count",
            color="magnitude_class",
            labels={
                "magnitude_class": "Clase de magnitud",
                "event_count": "Cantidad de sismos",
            },
            title="Sismos por clase de magnitud",
        )
        figure.update_layout(showlegend=False)
        st.plotly_chart(figure, use_container_width=True)

with tabs[1]:
    st.subheader("Evolucion temporal")
    daily = pd.DataFrame(load_daily(client, days))
    if daily.empty:
        st.info("No hay actividad diaria en el periodo consultado.")
    else:
        figure = px.bar(
            daily,
            x="event_date",
            y="event_count",
            labels={"event_date": "Fecha", "event_count": "Sismos"},
            title="Sismos por dia",
        )
        st.plotly_chart(figure, use_container_width=True)
        if {"avg_magnitude", "max_magnitude"}.issubset(daily.columns):
            mag_fig = px.line(
                daily,
                x="event_date",
                y=["max_magnitude", "avg_magnitude"],
                markers=True,
                labels={
                    "event_date": "Fecha",
                    "value": "Magnitud",
                    "variable": "Métrica",
                },
                title="Tendencia diaria de magnitud máxima y promedio",
            )
            st.plotly_chart(mag_fig, use_container_width=True)
        st.dataframe(daily, use_container_width=True, hide_index=True)

with tabs[2]:
    st.markdown(
        '<div class="section-title">Mapa de epicentros</div>'
        f'<div class="section-subtitle">Epicentros registrados en los últimos {days} días</div>',
        unsafe_allow_html=True,
    )
    events = pd.DataFrame(
        load_events(client, map_limit, min_magnitude, region_filter, place_filter)
    )
    if events.empty:
        st.info("No hay eventos con los filtros seleccionados.")
    else:
        events["event_time"] = pd.to_datetime(events["event_time"], errors="coerce")
        map_data = events.dropna(subset=["latitude", "longitude", "magnitude"]).copy()
        map_data["plot_size"] = map_data["magnitude"].clip(lower=0.5)
        map_data["place_label"] = map_data["place"].fillna("Sin ubicación")
        map_data["region_label"] = map_data["region"].fillna("Sin región")

        if region_filter or place_filter:
            center = {
                "lat": float(map_data["latitude"].mean()),
                "lon": float(map_data["longitude"].mean()),
            }
            lat_span = float(map_data["latitude"].max() - map_data["latitude"].min())
            lon_span = float(map_data["longitude"].max() - map_data["longitude"].min())
            span = max(lat_span, lon_span)
            if span <= 3:
                zoom = 5.0
            elif span <= 10:
                zoom = 4.0
            elif span <= 30:
                zoom = 3.0
            elif span <= 80:
                zoom = 2.3
            else:
                zoom = 1.8
        else:
            # Vista inicial: América Latina y el Caribe.
            center = {"lat": -3.0, "lon": -70.0}
            zoom = 2.4

        figure = px.scatter_map(
            map_data,
            lat="latitude",
            lon="longitude",
            color="magnitude",
            size="plot_size",
            size_max=18,
            opacity=0.78,
            hover_name="place_label",
            custom_data=["magnitude", "depth_km", "event_time", "region_label"],
            zoom=zoom,
            center=center,
            map_style="carto-darkmatter",
            color_continuous_scale="Turbo",
            range_color=[2.5, 7.0],
            height=560,
        )
        figure.update_traces(
            marker=dict(opacity=0.7),
            hovertemplate=(
                "<b>Magnitud: %{customdata[0]:.2f}</b><br>"
                "Ubicación: %{hovertext}<br>"
                "Profundidad: %{customdata[1]:.1f} km<br>"
                "Fecha: %{customdata[2]|%Y-%m-%d %H:%M:%S UTC}<br>"
                "Región: %{customdata[3]}"
                "<extra></extra>"
            ),
            selector=dict(type="scattermap"),
        )
        figure.update_layout(
            height=560,
            paper_bgcolor="#0b1220",
            plot_bgcolor="#0b1220",
            font=dict(color="#e5e7eb"),
            margin=dict(l=0, r=0, t=0, b=0),
            hoverlabel=dict(
                bgcolor="#111827",
                bordercolor="#374151",
                font=dict(color="#f9fafb"),
            ),
            coloraxis_colorbar=dict(
                title="Magnitud",
                title_font=dict(color="#e5e7eb"),
                tickfont=dict(color="#cbd5e1"),
                bgcolor="rgba(0,0,0,0)",
                bordercolor="#374151",
            ),
        )
        st.plotly_chart(
            figure,
            use_container_width=True,
            config={"displaylogo": False, "scrollZoom": True},
        )

        table = map_data[
            [
                "event_time",
                "place",
                "region",
                "magnitude",
                "depth_km",
                "latitude",
                "longitude",
            ]
        ].copy()
        table = table.rename(
            columns={
                "event_time": "Fecha y hora (UTC)",
                "place": "Lugar",
                "region": "Región",
                "magnitude": "Magnitud",
                "depth_km": "Profundidad (km)",
                "latitude": "Latitud",
                "longitude": "Longitud",
            }
        )
        table["Fecha y hora (UTC)"] = pd.to_datetime(
            table["Fecha y hora (UTC)"], errors="coerce"
        )
        table = table.sort_values("Fecha y hora (UTC)", ascending=False)
        st.dataframe(
            table,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Fecha y hora (UTC)": st.column_config.DatetimeColumn(
                    format="YYYY-MM-DD HH:mm:ss"
                ),
                "Magnitud": st.column_config.NumberColumn(format="%.2f"),
                "Profundidad (km)": st.column_config.NumberColumn(format="%.1f"),
                "Latitud": st.column_config.NumberColumn(format="%.4f"),
                "Longitud": st.column_config.NumberColumn(format="%.4f"),
            },
        )

with tabs[3]:
    st.subheader("Zonas con mayor actividad")
    places = pd.DataFrame(load_places(client, 20))
    if places.empty:
        st.info("No hay datos de lugares disponibles.")
    else:
        figure = px.bar(
            places.sort_values("event_count"),
            x="event_count",
            y="place",
            orientation="h",
            labels={"event_count": "Sismos", "place": "Lugar"},
            title="Top de lugares por cantidad de sismos",
        )
        figure.update_layout(height=600)
        st.plotly_chart(figure, use_container_width=True)
        st.dataframe(places, use_container_width=True, hide_index=True)

with tabs[4]:
    st.subheader("Regiones con mayor actividad")
    regions = pd.DataFrame(load_regions(client, 20))
    if regions.empty:
        st.info("No hay datos de regiones disponibles.")
    else:
        figure = px.bar(
            regions.sort_values("event_count"),
            x="event_count",
            y="region",
            orientation="h",
            labels={"event_count": "Sismos", "region": "Region"},
            title="Top de regiones por cantidad de sismos",
        )
        figure.update_layout(height=600)
        st.plotly_chart(figure, use_container_width=True)
        st.dataframe(regions, use_container_width=True, hide_index=True)

with tabs[5]:
    st.subheader("Calidad y frescura")
    quality = pd.DataFrame(load_quality(client, 20))
    if quality.empty:
        st.info("No hay metricas de calidad disponibles.")
    else:
        if {"recorded_at", "quality_percentage"}.issubset(quality.columns):
            q_plot = quality.sort_values("recorded_at").copy()
            q_fig = px.line(
                q_plot,
                x="recorded_at",
                y="quality_percentage",
                markers=True,
                labels={
                    "recorded_at": "Fecha de registro (UTC)",
                    "quality_percentage": "Calidad (%)",
                },
                title="Evolución del porcentaje de calidad de datos",
            )
            q_fig.update_yaxes(range=[0, 105])
            st.plotly_chart(q_fig, use_container_width=True)
        st.dataframe(quality, use_container_width=True, hide_index=True)
    st.subheader("Ejecuciones recientes")
    runs = pd.DataFrame(load_runs(client, 20))
    if runs.empty:
        st.info("No hay ejecuciones registradas.")
    else:
        if {"started_at", "duration_seconds", "status"}.issubset(runs.columns):
            r_plot = runs.dropna(subset=["duration_seconds"]).sort_values("started_at").copy()
            if not r_plot.empty:
                r_fig = px.bar(
                    r_plot,
                    x="started_at",
                    y="duration_seconds",
                    color="status",
                    labels={
                        "started_at": "Inicio de ejecución (UTC)",
                        "duration_seconds": "Duración (segundos)",
                        "status": "Estado",
                    },
                    title="Duración y estado de las ejecuciones del pipeline",
                )
                st.plotly_chart(r_fig, use_container_width=True)
        st.dataframe(runs, use_container_width=True, hide_index=True)

with tabs[6]:
    st.subheader("Agente de IA sobre Gold y Operaciones (Ollama / Read-Only)")
    st.caption(
        "El agente ejecuta planes validados de solo lectura sobre MinIO Gold y PostgreSQL ops. "
        "Las cifras las calcula Python/SQL (regla anti-alucinación) y Ollama redacta la explicación."
    )

    if "agent_history" not in st.session_state:
        st.session_state.agent_history = []

    default_suggestions = [
        "¿Cuántos sismos hubo en las últimas 24 horas?",
        "¿Cuál fue el sismo de mayor magnitud en los últimos 30 días?",
        "¿Qué regiones registraron mayor actividad?",
        "¿Cómo cambió la actividad sísmica durante la última semana?",
        "¿Cuándo se ejecutó por última vez el pipeline?",
        "¿Cuántos registros fueron rechazados por los controles de calidad?",
    ]
    st.markdown("**Preguntas frecuentes sugeridas:**")
    sug_cols = st.columns(3)
    selected_suggestion = None
    for idx, sug_text in enumerate(default_suggestions):
        if sug_cols[idx % 3].button(sug_text, key=f"sug_btn_{idx}", use_container_width=True):
            selected_suggestion = sug_text

    q_col1, q_col2, q_col3 = st.columns([4, 1, 1])
    with q_col1:
        question = st.text_input(
            "Pregunta en lenguaje natural",
            placeholder="Ejemplo: ¿Cuáles son los sismos de mayor magnitud en los últimos 30 días?",
            key="agent_question",
            label_visibility="collapsed",
        )
    with q_col2:
        ask_clicked = st.button("Consultar agente", use_container_width=True)
    with q_col3:
        if st.button("Limpiar chat", use_container_width=True):
            st.session_state.agent_history = []
            st.rerun()

    active_question = selected_suggestion or (question.strip() if ask_clicked and question.strip() else None)
    if active_question:
        with st.spinner("Ejecutando consulta autorizada y redactando respuesta con Ollama..."):
            try:
                answer = client.ask(active_question)
                st.session_state.agent_history.append(
                    {"question": active_question, **answer}
                )
            except Exception as exc:
                st.error(f"No se pudo ejecutar la consulta: {exc}")

    for item in reversed(st.session_state.agent_history[-10:]):
        st.markdown(f"**Pregunta:** {item['question']}")
        st.markdown(f"**Respuesta:** {item['answer']}")
        meta_parts = [f"Modo: `{item.get('mode', 'reglas')}`"]
        if item.get("model"):
            meta_parts.append(f"Modelo: `{item['model']}`")
        if item.get("prompt_version"):
            meta_parts.append(f"Prompt: `{item['prompt_version']}`")
        if item.get("latency_ms") is not None:
            meta_parts.append(f"Latencia: `{item['latency_ms']} ms`")
        tok = item.get("tokens") or {}
        if tok.get("total_tokens") is not None:
            meta_parts.append(f"Tokens: `{tok.get('total_tokens')}`")
        st.caption(" · ".join(meta_parts))
        if item.get("warning"):
            st.warning(item["warning"])
        exp_col1, exp_col2 = st.columns(2)
        with exp_col1:
            with st.expander("Plan de consulta validado"):
                st.json(item["plan"])
        with exp_col2:
            with st.expander("Evidencia de consulta (Solo lectura)"):
                st.code(item.get("sql_evidence") or "-- Plan validado sobre Gold", language="sql")
        rows = item.get("rows") or []
        if not rows:
            st.info("La consulta no devolvio filas.")
        else:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.divider()

    st.subheader("Automatización 1: Informe Narrativo Diario")
    rep_col1, rep_col2 = st.columns([3, 1])
    with rep_col2:
        if user_role in {"admin", "analyst"}:
            if st.button("Generar / Actualizar informe", use_container_width=True):
                with st.spinner("Generando informe narrativo con estadísticas reales..."):
                    try:
                        client.generate_report(role=user_role)
                        load_reports.clear()
                        load_ai_audit.clear()
                        st.rerun()
                    except Exception as exc:
                        st.error(f"No se pudo generar el informe: {exc}")
        else:
            st.caption("Rol `viewer`: solo lectura de informes.")

    try:
        reports_list = load_reports(client, 10)
    except Exception as exc:
        reports_list = []
        st.warning(f"No se pudieron cargar los informes diarios: {exc}")

    if reports_list:
        latest_rep = reports_list[0]
        st.markdown(f"#### {latest_rep.get('title', 'Informe Sísmico Diario')}")
        st.caption(
            f"Fecha: `{latest_rep.get('report_date')}` · Modo: `{latest_rep.get('generation_mode')}` · "
            f"Modelo: `{latest_rep.get('model_used') or 'determinista'}` · "
            f"Prompt: `{latest_rep.get('prompt_version')}` · Estado: `{latest_rep.get('status')}`"
        )
        st.write(latest_rep.get("executive_summary", ""))
        for finding in latest_rep.get("key_findings") or []:
            st.markdown(f"- {finding}")

        dl1, dl2, dl3 = st.columns(3)
        try:
            md_content = client.export_report("md")
            json_content = client.export_report("json")
            csv_content = client.export_report("csv")
            rep_date = latest_rep.get("report_date", "hoy")
            dl1.download_button(
                "Exportar Markdown (.md)",
                data=md_content,
                file_name=f"sismowatch_informe_{rep_date}.md",
                mime="text/markdown",
                use_container_width=True,
            )
            dl2.download_button(
                "Exportar JSON (.json)",
                data=json_content,
                file_name=f"sismowatch_informe_{rep_date}.json",
                mime="application/json",
                use_container_width=True,
            )
            dl3.download_button(
                "Exportar CSV (.csv)",
                data=csv_content,
                file_name=f"sismowatch_informe_{rep_date}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        except Exception:
            pass

        if len(reports_list) > 1:
            with st.expander("Historial de informes diarios"):
                st.dataframe(
                    pd.DataFrame(reports_list)[
                        ["report_date", "title", "generation_mode", "model_used", "status", "validation_passed"]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

    st.divider()
    st.subheader("Automatización 2: Detección Estadística de Anomalías")
    st.caption(
        "Detección analítica por z-score de frecuencia horaria/diaria y picos de magnitud sobre Gold. "
        "Una anomalía estadística describe actividad histórica observada y no predice terremotos."
    )
    if user_role in {"admin", "analyst"}:
        if st.button("Recalcular anomalías estadísticas"):
            with st.spinner("Analizando series sísmicas en Gold..."):
                try:
                    client.detect_anomalies(role=user_role)
                    load_anomalies.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Error al detectar anomalías: {exc}")

    try:
        anomaly_rows = load_anomalies(client, 25)
    except Exception:
        anomaly_rows = []

    if not anomaly_rows:
        st.info("No se detectaron anomalías estadísticas con el umbral actual.")
    else:
        anom_df = pd.DataFrame(anomaly_rows)
        anom_cols = [
            c
            for c in [
                "severity",
                "anomaly_type",
                "scope",
                "z_score",
                "observed_count",
                "baseline_mean",
                "max_magnitude",
                "window_start",
                "explanation",
            ]
            if c in anom_df.columns
        ]
        st.dataframe(anom_df[anom_cols], use_container_width=True, hide_index=True)

    with st.expander("Auditoría de operaciones de IA (ops.ai_audit_logs)"):
        try:
            audit_rows = load_ai_audit(client, 25)
        except Exception:
            audit_rows = []
        if not audit_rows:
            st.info("Aún no hay registros de auditoría de IA.")
        else:
            st.dataframe(pd.DataFrame(audit_rows), use_container_width=True, hide_index=True)

with tabs[7]:
    st.subheader("Monitoreo del pipeline")
    st.caption(
        "Estado real de Airflow, Spark y las capas Medallion. "
        "Los valores provienen de ops.runs, ops.quality_metrics y ops.layer_metadata."
    )
    metadata = load_metadata_summary(client)
    run = metadata.get("run") or {}
    quality = metadata.get("quality") or {}
    layers = metadata.get("layers") or []

    if not run:
        st.info("Todavia no hay ejecuciones registradas.")
    else:
        if run.get("error_message"):
            st.error(f"Ultimo error: {run['error_message']}")

        row1 = st.columns(4)
        row1[0].metric("Estado", status_label(run.get("status")))
        row1[1].metric("Etapa", str(run.get("current_stage") or "Sin datos"))
        row1[2].metric(
            "Ultima actualizacion",
            str(run.get("last_data_update") or quality.get("recorded_at") or "Sin datos"),
        )
        row1[3].metric("Duracion", format_duration(run.get("duration_seconds")))

        received = int(run.get("records_received") or quality.get("bronze_records") or 0)
        processed = int(run.get("records_processed") or quality.get("silver_records") or 0)
        duplicates = int(quality.get("duplicate_records") or 0)
        invalid = int(quality.get("invalid_records") or 0)
        quality_pct = quality.get("quality_percentage")

        row2 = st.columns(4)
        row2[0].metric("Registros recibidos", f"{received:,}")
        row2[1].metric("Registros procesados", f"{processed:,}")
        row2[2].metric("Duplicados", f"{duplicates:,}")
        row2[3].metric("Invalidos", f"{invalid:,}")

        if quality_pct is None:
            st.info("La ultima ejecucion no tiene metricas de calidad.")
        else:
            quality_value = float(quality_pct)
            st.metric("Calidad de los datos", f"{quality_value:.2f}%")
            st.progress(max(0.0, min(1.0, quality_value / 100.0)))

        st.subheader("Estado de las capas Medallion")
        if layers:
            layers_df = pd.DataFrame(layers).copy()
            layers_df["status"] = layers_df["status"].map(status_label)
            show_columns = [
                column
                for column in [
                    "layer",
                    "status",
                    "records_in",
                    "records_out",
                    "records_duplicates",
                    "records_invalid",
                    "location",
                    "updated_at",
                ]
                if column in layers_df.columns
            ]
            st.dataframe(
                layers_df[show_columns],
                use_container_width=True,
                hide_index=True,
            )
            with st.expander("Detalle tecnico de capas"):
                st.json(layers)
        else:
            st.info("No hay metadatos de capas para la ultima ejecucion.")

    st.subheader("Historial reciente de ejecuciones")
    recent_runs = pd.DataFrame(load_metadata_runs(client, 15))
    if recent_runs.empty:
        st.info("No hay ejecuciones registradas.")
    else:
        recent_runs["status"] = recent_runs["status"].map(status_label)
        run_columns = [
            column
            for column in [
                "started_at",
                "status",
                "current_stage",
                "records_received",
                "records_processed",
                "duration_seconds",
                "last_data_update",
                "error_message",
            ]
            if column in recent_runs.columns
        ]
        st.dataframe(
            recent_runs[run_columns],
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Historial de calidad")
    quality_history = pd.DataFrame(load_metadata_quality(client, 10))
    if quality_history.empty:
        st.info("No hay metricas de calidad registradas.")
    else:
        st.dataframe(quality_history, use_container_width=True, hide_index=True)


st.divider()
st.caption(
    "MinIO conserva Bronze, Silver y Gold. PostgreSQL almacena datos operativos "
    "(ejecuciones, calidad, usuarios y configuracion). El procesamiento es Batch "
    "y Airflow controla las dependencias."
)
