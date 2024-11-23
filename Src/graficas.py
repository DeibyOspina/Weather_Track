from dash import Dash, html, dcc
import plotly.express as px
import psycopg2
import pandas as pd
import webbrowser

# Conexión a la base de datos PostgreSQL
def conectar_postgresql():
    return psycopg2.connect(
        host='localhost',
        database='whater_track',
        user='postgres',
        password='1234'
    )

# Consultar datos
def consultar_datos(query):
    conexion = conectar_postgresql()
    try:
        df = pd.read_sql_query(query, conexion)
    finally:
        conexion.close()
    return df

# Aplicación Dash
app = Dash(__name__)

# Consultas SQL para cada gráfico
queries = {
    "alertas_ciudad_barras": """
        SELECT c.nombre_ciudad AS ciudad, COUNT(a.alerta_id) AS total_alertas
        FROM alertas a
        JOIN sensores s ON a.sensor_id = s.sensor_id
        JOIN ciudades c ON s.ciudad_id = c.ciudad_id
        GROUP BY c.nombre_ciudad
        ORDER BY total_alertas DESC;
    """,
    "alertas_ciudad_circular": """
        SELECT c.nombre_ciudad AS ciudad, COUNT(a.alerta_id) AS total_alertas
        FROM alertas a
        JOIN sensores s ON a.sensor_id = s.sensor_id
        JOIN ciudades c ON s.ciudad_id = c.ciudad_id
        GROUP BY c.nombre_ciudad
        ORDER BY total_alertas DESC;
    """,
    "evolucion_direccion_viento": """
        SELECT c.nombre_ciudad AS ciudad, EXTRACT(MONTH FROM i.tiempo_lectura)::INT AS mes,
               AVG(i.direccion_viento)::NUMERIC AS promedio_direccion
        FROM informacion_sensores i
        JOIN sensores s ON i.sensor_id = s.sensor_id
        JOIN ciudades c ON s.ciudad_id = c.ciudad_id
        WHERE i.direccion_viento IS NOT NULL
        GROUP BY c.nombre_ciudad, mes
        ORDER BY c.nombre_ciudad, mes;
    """,
    "dias_viento_fuerte": """
        SELECT c.nombre_ciudad AS ciudad, i.tiempo_lectura AS dia, MAX(i.velocidad_viento) AS max_velocidad
        FROM informacion_sensores i
        JOIN sensores s ON i.sensor_id = s.sensor_id
        JOIN ciudades c ON s.ciudad_id = c.ciudad_id
        WHERE i.velocidad_viento IS NOT NULL
        GROUP BY c.nombre_ciudad, i.tiempo_lectura
        ORDER BY max_velocidad DESC LIMIT 10;
    """,
    "promedio_mensual_precipitaciones": """
        SELECT c.nombre_ciudad AS ciudad, EXTRACT(MONTH FROM i.tiempo_lectura)::INT AS mes,
               AVG(i.precipitacion)::NUMERIC AS promedio_precipitacion
        FROM informacion_sensores i
        JOIN sensores s ON i.sensor_id = s.sensor_id
        JOIN ciudades c ON s.ciudad_id = c.ciudad_id
        WHERE i.precipitacion IS NOT NULL
        GROUP BY c.nombre_ciudad, mes
        ORDER BY c.nombre_ciudad, mes;
    """,
    "sensores_por_tipo": """
        SELECT ts.tipo_sensor AS tipo_sensor, COUNT(sts.sensor_id) AS total_sensores
        FROM tipo_sensores ts
        LEFT JOIN sensor_tipo_sensor sts ON ts.tipo_sensor_id = sts.tipo_sensor_id
        GROUP BY ts.tipo_sensor
        ORDER BY total_sensores DESC;
    """,
    "alertas_por_tipo_sensor": """
        SELECT ts.tipo_sensor AS tipo_sensor, COUNT(a.alerta_id) AS total_alertas
        FROM alertas a
        JOIN sensores s ON a.sensor_id = s.sensor_id
        JOIN sensor_tipo_sensor sts ON s.sensor_id = sts.sensor_id
        JOIN tipo_sensores ts ON sts.tipo_sensor_id = ts.tipo_sensor_id
        GROUP BY ts.tipo_sensor;
    """,
    "comparacion_velocidad_viento": """
        SELECT c.nombre_ciudad AS ciudad, MAX(i.velocidad_viento) AS max_velocidad
        FROM informacion_sensores i
        JOIN sensores s ON i.sensor_id = s.sensor_id
        JOIN ciudades c ON s.ciudad_id = c.ciudad_id
        WHERE i.velocidad_viento IS NOT NULL
        GROUP BY c.nombre_ciudad
        ORDER BY max_velocidad DESC;
    """
}

# Generación de gráficos
figs = {
    "alertas_ciudad_barras": px.bar(
        consultar_datos(queries["alertas_ciudad_barras"]),
        x="ciudad", y="total_alertas", title="Alertas por Ciudad"
    ),
    "alertas_ciudad_circular": px.pie(
        consultar_datos(queries["alertas_ciudad_circular"]),
        names="ciudad", values="total_alertas", title="Distribución de Alertas por Ciudad"
    ),
    "evolucion_direccion_viento": px.line(
        consultar_datos(queries["evolucion_direccion_viento"]),
        x="mes", y="promedio_direccion", color="ciudad", title="Evolución Dirección del Viento"
    ),
    "dias_viento_fuerte": px.scatter(
        consultar_datos(queries["dias_viento_fuerte"]),
        x="dia", y="max_velocidad", color="ciudad", title="Días con Viento Más Fuerte"
    ),
    "promedio_mensual_precipitaciones": px.line(
    consultar_datos(queries["promedio_mensual_precipitaciones"]),
    x="mes", y="promedio_precipitacion", color="ciudad", 
    title="Promedio Mensual de Precipitaciones"
    ),
    "sensores_por_tipo": px.bar(
        consultar_datos(queries["sensores_por_tipo"]),
        x="tipo_sensor", y="total_sensores", title="Sensores por Tipo"
    ),
    "alertas_por_tipo_sensor": px.pie(
        consultar_datos(queries["alertas_por_tipo_sensor"]),
        names="tipo_sensor", values="total_alertas", title="Alertas por Tipo de Sensor"
    ),
    "comparacion_velocidad_viento": px.bar(
        consultar_datos(queries["comparacion_velocidad_viento"]),
        x="ciudad", y="max_velocidad", title="Comparación Velocidad Máxima del Viento"
    )
}

# Layout de la aplicación Dash
app.layout = html.Div(children=[
    html.H1("Visualización de Datos - Water Track"),
    dcc.Graph(figure=figs["alertas_ciudad_barras"]),
    dcc.Graph(figure=figs["alertas_ciudad_circular"]),
    dcc.Graph(figure=figs["evolucion_direccion_viento"]),
    dcc.Graph(figure=figs["dias_viento_fuerte"]),
    dcc.Graph(figure=figs["promedio_mensual_precipitaciones"]),
    dcc.Graph(figure=figs["sensores_por_tipo"]),
    dcc.Graph(figure=figs["alertas_por_tipo_sensor"]),
    dcc.Graph(figure=figs["comparacion_velocidad_viento"]),
])

# Abrir navegador automáticamente
def open_browser():
    webbrowser.open_new("http://127.0.0.1:8050/")

if __name__ == "__main__":
    open_browser()
    app.run_server(debug=True)
