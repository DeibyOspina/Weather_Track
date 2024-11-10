import psycopg2

def conectar_postgresql():
    """Conecta a la base de datos PostgreSQL y retorna la conexión."""
    conexion = psycopg2.connect(
        host='localhost',
        database='whater_track',
        user='postgres',
        password='1234'
    )
    print("Conectado a la base de datos PostgreSQL.")
    return conexion

# Funciones para consultar cada tabla
def consultar_ciudades(conexion):
    """Consulta todas las filas de la tabla ciudades."""
    with conexion.cursor() as cursor:
        cursor.execute("SELECT * FROM ciudades;")
        return cursor.fetchall()

def consultar_informacion_sensores(conexion):
    """Consulta todas las filas de la tabla informacion_sensores."""
    with conexion.cursor() as cursor:
        cursor.execute("SELECT * FROM informacion_sensores;")
        return cursor.fetchall()

def consultar_sensor_tipo_sensor(conexion):
    """Consulta todas las filas de la tabla sensor_tipo_sensor."""
    with conexion.cursor() as cursor:
        cursor.execute("SELECT * FROM sensor_tipo_sensor;")
        return cursor.fetchall()

def consultar_sensores(conexion):
    """Consulta todas las filas de la tabla sensores."""
    with conexion.cursor() as cursor:
        cursor.execute("SELECT * FROM sensores;")
        return cursor.fetchall()

def consultar_tipos_de_sensores(conexion):
    """Consulta todas las filas de la tabla tipo_sensores."""
    with conexion.cursor() as cursor:
        cursor.execute("SELECT * FROM tipo_sensores;")  # Cambiado a 'tipo_sensores'
        return cursor.fetchall()

# Bloque principal para probar la conexión y consultas
if __name__ == "__main__":
    conexion = conectar_postgresql()
    
    # Ejecuta y muestra los resultados de cada consulta
    print("Ciudades:", consultar_ciudades(conexion))
    print("Información de Sensores:", consultar_informacion_sensores(conexion))
    print("Sensor Tipo Sensor:", consultar_sensor_tipo_sensor(conexion))
    print("Sensores:", consultar_sensores(conexion))
    print("Tipos de Sensores:", consultar_tipos_de_sensores(conexion))

    # Cierra la conexión
    conexion.close()