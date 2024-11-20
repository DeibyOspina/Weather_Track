/* Whater Track - Proyecto Ingenieria de datos 
Deiby Ospina - Cristian Ramos*/

--- Declaracion Tabalas 

-- Tabla de rol del usuario
CREATE TABLE rol_usuario (
    rol_id SERIAL PRIMARY KEY,
    nombre_rol VARCHAR(50) NOT NULL
);

-- Tabla de usuarios
CREATE TABLE usuarios (
    usuario_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    segundo_nombre VARCHAR(100),
    email VARCHAR(100) UNIQUE NOT NULL,
    contraseña VARCHAR(255) NOT NULL,
    rol_id INT REFERENCES rol_usuario(rol_id)
);

-- Tabla de ciudades 
CREATE TABLE ciudades (
    ciudad_id SERIAL PRIMARY KEY,
    nombre_ciudad VARCHAR(100) NOT NULL,
    pais VARCHAR(100),
    latitud DECIMAL(9, 6),
    longitud DECIMAL(9, 6),
    elevacion DECIMAL(9, 6)
);

-- Tabla de tipos de sensores
CREATE TABLE tipo_sensores (
    tipo_sensor_id SERIAL PRIMARY KEY,
    tipo_sensor VARCHAR(50) NOT NULL
);

-- Tabla de sensores
CREATE TABLE sensores (
    sensor_id SERIAL PRIMARY KEY,
    nombre_sensor VARCHAR(100) NOT NULL,
    tipo_sensor_id INT REFERENCES tipo_sensores(tipo_sensor_id),
    ciudad_id INT REFERENCES ciudades(ciudad_id)
);

-- Tabla de informacion de sensores
CREATE TABLE informacion_sensores (
    info_sensor_id SERIAL PRIMARY KEY,
    sensor_id INT REFERENCES sensores(sensor_id),
    temperatura DECIMAL(5, 2),
    velocidad_viento DECIMAL(5, 2),
    precipitacion DECIMAL(5, 2),  
    direccion_viento DECIMAL(5, 1), 
    tiempo_lectura DATE DEFAULT CURRENT_DATE
);


-- Tabla de alertas
CREATE TABLE alertas (
    alerta_id SERIAL PRIMARY KEY,
    sensor_id INT REFERENCES sensores(sensor_id),
    mensaje_alerta VARCHAR(255),
    tiempo_alerta DATE DEFAULT CURRENT_DATE
);


-- Tabla de relaciones entre información y alertas
CREATE TABLE genera (
    genera_id SERIAL PRIMARY KEY,
    info_sensor_id INT REFERENCES informacion_sensores(info_sensor_id),
    alerta_id INT REFERENCES alertas(alerta_id),
    fecha DATE DEFAULT CURRENT_DATE
);

--- Tabla de relaciones entre tipos de sensor y sensores

CREATE TABLE sensor_tipo_sensor (
    sensor_id INT REFERENCES sensores(sensor_id),
    tipo_sensor_id INT REFERENCES tipo_sensores(tipo_sensor_id),
    PRIMARY KEY (sensor_id, tipo_sensor_id)
);


--- Carga Masiva De Archivos 

--- Carga Datos Tabla Ciudades
COPY ciudades(ciudad_id, nombre_ciudad, pais, latitud, longitud, elevacion) FROM 'Resources/ciudades.csv' DELIMITER ',' CSV HEADER;

--- Carga Datos Tabla Informacion_sensores
COPY informacion_sensores(sensor_id, temperatura, velocidad_viento, precipitacion, direccion_viento, tiempo_lectura) 
FROM 'Resources/informacion_sensores.csv' DELIMITER ',' CSV HEADER;

--- Carga Datos Tabla Tipo_de_sensores
COPY tipo_sensores(tipo_sensor_id, tipo_sensor) FROM 'Resources/tipos_de_sensores.csv' DELIMITER ',' CSV HEADER;


--- Carga Datos Tabla Sensores
COPY sensores FROM(Sensor_id, nombre_sensor, ciudad_id) 'Resources/sensores.csv' DELIMITER ',' CSV HEADER;


--- Carga Datos Tabla Sensor_tipo_sensor
COPY sensor_tipo_sensor(sensor_id, tipo_sensor_id) FROM 'Resources/sensor_tipo_sensor.csv' DELIMITER ',' CSV HEADER;


--- Autorizaciones 


--- Escenarios de análisis / (Funciones - Triggers - Consultas - Subconsultas)


---



---



---



--- Evolución de la dirección del viento en una ciudad específica

CREATE OR REPLACE FUNCTION evolucion_direccion_viento(ciudad_nombre TEXT)
RETURNS TABLE(fecha DATE, direccion NUMERIC) AS $$
BEGIN
    RETURN QUERY
    SELECT i.tiempo_lectura, i.direccion_viento
    FROM informacion_sensores i
    JOIN sensores s ON i.sensor_id = s.sensor_id
    JOIN ciudades c ON s.ciudad_id = c.ciudad_id
    WHERE c.nombre_ciudad = ciudad_nombre
    ORDER BY i.tiempo_lectura;
END;
$$ LANGUAGE plpgsql;

    --- Visualizar
SELECT * FROM evolucion_direccion_viento(''); /* Escribir el nombre de la ciudad */


--- Alertas generadas por cada tipo de sensor 

CREATE OR REPLACE FUNCTION alertas_por_tipo_sensor()
RETURNS TABLE(tipo_sensor TEXT, total_alertas INT) AS $$
BEGIN
    RETURN QUERY
    SELECT ts.nombre_tipo_sensor, COUNT(*)
    FROM informacion_sensores i
    JOIN sensores s ON i.sensor_id = s.sensor_id
    JOIN sensor_tipo_sensor st ON s.sensor_id = st.sensor_id
    JOIN tipos_de_sensores ts ON st.tipo_sensor_id = ts.tipo_sensor_id
    WHERE i.temperatura > 35 -- Por ejemplo, alerta de alta temperatura
    GROUP BY ts.nombre_tipo_sensor
    ORDER BY total_alertas DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION trigger_nueva_alerta()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.temperatura > 35 THEN
        RAISE NOTICE 'Nueva alerta generada por alta temperatura: Sensor %', NEW.sensor_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER alerta_alta_temperatura
AFTER INSERT ON informacion_sensores
FOR EACH ROW
EXECUTE FUNCTION trigger_nueva_alerta();

--- Días con viento más fuerte en cada mes

CREATE OR REPLACE FUNCTION dias_viento_fuerte()
RETURNS TABLE(ciudad TEXT, mes INT, dia DATE, max_velocidad NUMERIC) AS $$
BEGIN
    RETURN QUERY
    SELECT c.nombre_ciudad, EXTRACT(MONTH FROM i.tiempo_lectura) AS mes, i.tiempo_lectura AS dia, MAX(i.velocidad_viento)
    FROM informacion_sensores i
    JOIN sensores s ON i.sensor_id = s.sensor_id
    JOIN ciudades c ON s.ciudad_id = c.ciudad_id
    GROUP BY c.nombre_ciudad, mes, dia
    ORDER BY c.nombre_ciudad, mes, max_velocidad DESC;
END;
$$ LANGUAGE plpgsql;

    --- Visualizar
SELECT * FROM dias_viento_fuerte();

--- Máximos y mínimos históricos de temperatura por ciudad



--- Promedio mensual de precipitaciones por ciudad


 