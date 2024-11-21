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
    sensor_id VARCHAR(50) PRIMARY KEY,
    nombre_sensor VARCHAR(100) NOT NULL,
    tipo_sensor_id INT REFERENCES tipo_sensores(tipo_sensor_id),
    ciudad_id INT REFERENCES ciudades(ciudad_id)
);

-- Tabla de información de sensores
CREATE TABLE informacion_sensores (
    info_sensor_id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) REFERENCES sensores(sensor_id),
    temperatura DECIMAL(5, 2),
    velocidad_viento DECIMAL(5, 2),
    precipitacion DECIMAL(5, 2),  
    direccion_viento DECIMAL(5, 1), 
    tiempo_lectura DATE DEFAULT CURRENT_DATE
);

-- Tabla de alertas
CREATE TABLE alertas (
    alerta_id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) REFERENCES sensores(sensor_id),
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

-- Tabla de relaciones entre tipos de sensor y sensores
CREATE TABLE sensor_tipo_sensor (
    sensor_id VARCHAR(50) REFERENCES sensores(sensor_id),
    tipo_sensor_id INT REFERENCES tipo_sensores(tipo_sensor_id),
    PRIMARY KEY (sensor_id, tipo_sensor_id)
);


--- Carga Masiva De Archivos 

--- Carga Datos Tabla Ciudades
COPY ciudades(ciudad_id, nombre_ciudad, pais, latitud, longitud, elevacion) FROM 'C:/temp/ciudades.csv' DELIMITER ';' CSV HEADER;

--- Carga Datos Tabla Sensores
COPY sensores (Sensor_id, nombre_sensor, ciudad_id) FROM 'C:/temp/sensores.csv' DELIMITER ';' CSV HEADER;

--- Carga Datos Tabla Informacion_sensores
COPY informacion_sensores(sensor_id, temperatura, velocidad_viento, precipitacion, direccion_viento, tiempo_lectura) 
FROM 'C:/temp/informacion_sensores.csv' DELIMITER ';' CSV HEADER;

--- Carga Datos Tabla Tipo_de_sensores
COPY tipo_sensores(tipo_sensor_id, tipo_sensor) FROM 'C:/temp/tipos_de_sensores.csv' DELIMITER ';' CSV HEADER;

--- Carga Datos Tabla Sensor_tipo_sensor
COPY sensor_tipo_sensor(sensor_id, tipo_sensor_id) FROM 'C:/temp/sensor_tipo_sensor.csv' DELIMITER ';' CSV HEADER;

--- Escenarios de análisis / (Funciones - Triggers - Consultas - Subconsultas)

--- Alertas 
	---  Funcion para generar alertas 

WITH alertas_temperatura AS (
    INSERT INTO alertas (sensor_id, mensaje_alerta, tiempo_alerta)
    SELECT 
        sensor_id, 
        'Alta temperatura detectada: ' || temperatura || '°C',
        CURRENT_DATE
    FROM informacion_sensores
    WHERE temperatura > 35
    AND NOT EXISTS (
        SELECT 1 
        FROM alertas 
        WHERE alertas.sensor_id = informacion_sensores.sensor_id 
        AND alertas.mensaje_alerta = 'Alta temperatura detectada: ' || informacion_sensores.temperatura || '°C'
        AND alertas.tiempo_alerta = CURRENT_DATE
    )
),
alertas_viento AS (
    INSERT INTO alertas (sensor_id, mensaje_alerta, tiempo_alerta)
    SELECT 
        sensor_id, 
        'Viento fuerte detectado: ' || velocidad_viento || ' km/h',
        CURRENT_DATE
    FROM informacion_sensores
    WHERE velocidad_viento > 50
    AND NOT EXISTS (
        SELECT 1 
        FROM alertas 
        WHERE alertas.sensor_id = informacion_sensores.sensor_id 
        AND alertas.mensaje_alerta = 'Viento fuerte detectado: ' || informacion_sensores.velocidad_viento || ' km/h'
        AND alertas.tiempo_alerta = CURRENT_DATE
    )
),
alertas_precipitacion AS (
    INSERT INTO alertas (sensor_id, mensaje_alerta, tiempo_alerta)
    SELECT 
        sensor_id, 
        'Alta precipitación detectada: ' || precipitacion || ' mm',
        CURRENT_DATE
    FROM informacion_sensores
    WHERE precipitacion > 100
    AND NOT EXISTS (
        SELECT 1 
        FROM alertas 
        WHERE alertas.sensor_id = informacion_sensores.sensor_id 
        AND alertas.mensaje_alerta = 'Alta precipitación detectada: ' || informacion_sensores.precipitacion || ' mm'
        AND alertas.tiempo_alerta = CURRENT_DATE
    )
)
SELECT 1;

	--- Trigger para automatizar alertas al introducir datos
CREATE OR REPLACE FUNCTION trigger_generar_alertas_completo()
RETURNS TRIGGER AS $$
BEGIN
    -- Generar alerta por alta temperatura
    IF NEW.temperatura > 35 THEN
        INSERT INTO alertas (sensor_id, mensaje_alerta, tiempo_alerta)
        VALUES (
            NEW.sensor_id,
            'Alta temperatura detectada: ' || NEW.temperatura || '°C',
            CURRENT_DATE
        );
    END IF;
    
    -- Generar alerta por viento fuerte
    IF NEW.velocidad_viento > 50 THEN
        INSERT INTO alertas (sensor_id, mensaje_alerta, tiempo_alerta)
        VALUES (
            NEW.sensor_id,
            'Viento fuerte detectado: ' || NEW.velocidad_viento || ' km/h',
            CURRENT_DATE
        );
    END IF;
    
    -- Generar alerta por alta precipitación
    IF NEW.precipitacion > 100 THEN
        INSERT INTO alertas (sensor_id, mensaje_alerta, tiempo_alerta)
        VALUES (
            NEW.sensor_id,
            'Alta precipitación detectada: ' || NEW.precipitacion || ' mm',
            CURRENT_DATE
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_alertas_automatico
AFTER INSERT ON informacion_sensores
FOR EACH ROW
EXECUTE FUNCTION trigger_generar_alertas_completo();

	--- Visualizar
SELECT * FROM alertas;


--- Evolución de la dirección del viento en cada ciudad

CREATE OR REPLACE FUNCTION evolucion_direccion_viento_promedio()
RETURNS TABLE(
    ciudad TEXT, 
    mes INT, 
    anio INT, 
    promedio_direccion NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.nombre_ciudad::TEXT AS ciudad, 
        EXTRACT(MONTH FROM i.tiempo_lectura)::INT AS mes,
        EXTRACT(YEAR FROM i.tiempo_lectura)::INT AS anio,
        AVG(i.direccion_viento)::NUMERIC AS promedio_direccion
    FROM informacion_sensores i
    JOIN sensores s ON i.sensor_id = s.sensor_id
    JOIN ciudades c ON s.ciudad_id = c.ciudad_id
    WHERE i.direccion_viento IS NOT NULL -- Excluir registros con valores nulos
    GROUP BY c.nombre_ciudad, anio, mes
    ORDER BY c.nombre_ciudad, anio, mes;
END;
$$ LANGUAGE plpgsql;

	--- Visualizar
SELECT * FROM evolucion_direccion_viento_promedio();

--- Días con viento más fuerte en cada mes

CREATE OR REPLACE FUNCTION dias_viento_fuerte()
RETURNS TABLE(ciudad VARCHAR(100), mes INT, dia DATE, max_velocidad NUMERIC) AS $$
BEGIN
    RETURN QUERY
    SELECT sub.ciudad, 
           sub.mes, 
           sub.dia, 
           sub.max_velocidad
    FROM (
        SELECT c.nombre_ciudad AS ciudad, 
               CAST(EXTRACT(MONTH FROM i.tiempo_lectura) AS INT) AS mes, 
               i.tiempo_lectura AS dia, 
               i.velocidad_viento AS max_velocidad,
               ROW_NUMBER() OVER (
                   PARTITION BY c.nombre_ciudad, CAST(EXTRACT(MONTH FROM i.tiempo_lectura) AS INT) 
                   ORDER BY i.velocidad_viento DESC
               ) AS rn
        FROM informacion_sensores i
        JOIN sensores s ON i.sensor_id = s.sensor_id
        JOIN ciudades c ON s.ciudad_id = c.ciudad_id
        WHERE i.velocidad_viento IS NOT NULL 
    ) sub
    WHERE sub.rn = 1; 
END;
$$ LANGUAGE plpgsql;

    --- Visualizar
SELECT * FROM dias_viento_fuerte();


--- Máximos y mínimos históricos de temperatura por ciudad

CREATE OR REPLACE FUNCTION maximos_minimos_temperatura()
RETURNS TABLE(
    ciudad VARCHAR(100),
    temp_max NUMERIC,
    fecha_max DATE,
    temp_min NUMERIC,
    fecha_min DATE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.nombre_ciudad AS ciudad,
        max_data.temp_max,
        max_data.fecha_max,
        min_data.temp_min,
        min_data.fecha_min
    FROM ciudades c
    LEFT JOIN (
        SELECT sub_max.ciudad_id, sub_max.temp_max, sub_max.fecha_max
        FROM (
            SELECT 
                s.ciudad_id,
                i.temperatura AS temp_max,
                i.tiempo_lectura AS fecha_max,
                ROW_NUMBER() OVER (PARTITION BY s.ciudad_id ORDER BY i.tiempo_lectura DESC) AS rn
            FROM informacion_sensores i
            JOIN sensores s ON i.sensor_id = s.sensor_id
            WHERE i.temperatura = (
                SELECT MAX(i2.temperatura)
                FROM informacion_sensores i2
                JOIN sensores s2 ON i2.sensor_id = s2.sensor_id
                WHERE s2.ciudad_id = s.ciudad_id
            )
        ) sub_max
        WHERE sub_max.rn = 1 
    ) max_data ON c.ciudad_id = max_data.ciudad_id
    LEFT JOIN (
        SELECT sub_min.ciudad_id, sub_min.temp_min, sub_min.fecha_min
        FROM (
            SELECT 
                s.ciudad_id,
                i.temperatura AS temp_min,
                i.tiempo_lectura AS fecha_min,
                ROW_NUMBER() OVER (PARTITION BY s.ciudad_id ORDER BY i.tiempo_lectura ASC) AS rn
            FROM informacion_sensores i
            JOIN sensores s ON i.sensor_id = s.sensor_id
            WHERE i.temperatura = (
                SELECT MIN(i2.temperatura)
                FROM informacion_sensores i2
                JOIN sensores s2 ON i2.sensor_id = s2.sensor_id
                WHERE s2.ciudad_id = s.ciudad_id
            )
        ) sub_min
        WHERE sub_min.rn = 1 
    ) min_data ON c.ciudad_id = min_data.ciudad_id
    ORDER BY c.nombre_ciudad;
END;
$$ LANGUAGE plpgsql;

	--- Visualizar
SELECT * FROM maximos_minimos_temperatura();


--- Promedio mensual de precipitaciones por ciudad

CREATE OR REPLACE FUNCTION promedio_mensual_precipitaciones()
RETURNS TABLE(
    ciudad VARCHAR(100),
    mes INT,
    anio INT,
    promedio_precipitacion NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.nombre_ciudad AS ciudad,
        EXTRACT(MONTH FROM i.tiempo_lectura)::INT AS mes,
        EXTRACT(YEAR FROM i.tiempo_lectura)::INT AS anio,
        AVG(i.precipitacion)::NUMERIC AS promedio_precipitacion
    FROM informacion_sensores i
    JOIN sensores s ON i.sensor_id = s.sensor_id
    JOIN ciudades c ON s.ciudad_id = c.ciudad_id
    WHERE i.precipitacion IS NOT NULL 
    GROUP BY c.nombre_ciudad, anio, mes
    ORDER BY c.nombre_ciudad, anio, mes;
END;
$$ LANGUAGE plpgsql;

	--- Visualizar
SELECT * FROM promedio_mensual_precipitaciones();


--- Total alertas generadas por sensor

SELECT tipo_sensor, COUNT(*) as total_alertas
FROM alertas
JOIN sensores ON alertas.sensor_id = sensores.sensor_id
JOIN tipo_sensores ON sensores.tipo_sensor_id = tipo_sensores.tipo_sensor_id
GROUP BY tipo_sensor

--- Alertas por ciudad 

SELECT c.nombre_ciudad, COUNT(*) as total_alertas
FROM alertas a
JOIN sensores s ON a.sensor_id = s.sensor_id
JOIN ciudades c ON s.ciudad_id = c.ciudad_id
GROUP BY c.nombre_ciudad;

--- Alertas por tipo de sensor

SELECT 
    CASE
        WHEN mensaje_alerta LIKE '%temperatura%' THEN 'Temperatura'
        WHEN mensaje_alerta LIKE '%viento%' THEN 'Viento'
        WHEN mensaje_alerta LIKE '%precipitación%' THEN 'Precipitación'
    END AS tipo_alerta,
    COUNT(*) AS cantidad_alertas
FROM alertas
GROUP BY tipo_alerta;

--- Distribucion total de alertas por ciudad 

SELECT c.nombre_ciudad, COUNT(*) AS cantidad_alertas
FROM alertas a
JOIN sensores s ON a.sensor_id = s.sensor_id
JOIN ciudades c ON s.ciudad_id = c.ciudad_id
GROUP BY c.nombre_ciudad;





-- Opciones para los escenarios del proyecto realizados de otra manera:

-- 1. Tendencia de temperatura promedio anual:

SELECT EXTRACT(YEAR FROM tiempo_lectura) AS año, AVG(temperatura) AS temp_promedio,
       STDDEV(temperatura) AS desviacion_estandar
FROM informacion_sensores
GROUP BY año
ORDER BY año;

-- 2. Alertas de alta temperatura en una ciudad específica:


SELECT c.nombre_ciudad, COUNT(*) AS total_alertas_altas
FROM alertas a
JOIN sensores s ON a.sensor_id = s.sensor_id
JOIN ciudades c ON s.ciudad_id = c.ciudad_id
WHERE a.mensaje_alerta LIKE '%alta temperatura%' AND c.nombre_ciudad = 'Ciudad Ejemplo'
GROUP BY c.nombre_ciudad;

-- 3. Ciudades con la mayor precipitación promedio anual:

SELECT c.nombre_ciudad, AVG(i.precipitacion) AS precipitacion_promedio
FROM ciudades c
JOIN sensores s ON c.ciudad_id = s.ciudad_id
JOIN informacion_sensores i ON s.sensor_id = i.sensor_id
GROUP BY c.nombre_ciudad
ORDER BY precipitacion_promedio DESC
LIMIT 10;


-- 4. Evolución de la dirección del viento en una ciudad específica:


SELECT tiempo_lectura, direccion_viento
FROM informacion_sensores i_s
INNER JOIN ciudades c ON i_s.info_sensor_id = c.ciudad_id
WHERE ciudad_id = (SELECT ciudad_id FROM ciudades WHERE nombre_ciudad = 'Ciudad Ejemplo')
ORDER BY tiempo_lectura;


-- 5. Alertas generadas por cada tipo de sensor:

CREATE OR REPLACE FUNCTION obtener_alertas_por_tipo_sensor()
RETURNS TRIGGER AS $$
BEGIN
  
  UPDATE tipo_sensores ts
  SET total_alertas = (SELECT COUNT(*) 
                       FROM alertas a
                       JOIN sensores s ON a.sensor_id = s.sensor_id
                       WHERE s.tipo_sensor_id = ts.tipo_sensor_id);

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER actualizar_estadisticas_alertas
AFTER INSERT ON alertas
FOR EACH ROW
EXECUTE PROCEDURE obtener_alertas_por_tipo_sensor();


-- 6. Días con viento más fuerte en cada mes:

WITH maximos_vientos AS (
    SELECT ciudad_id, EXTRACT(MONTH FROM tiempo_lectura) AS mes, MAX(velocidad_viento) AS max_viento
    FROM informacion_sensores i_s
	INNER JOIN ciudades c ON i_s.info_sensor_id = c.ciudad_id
    GROUP BY ciudad_id, mes
)
SELECT c.nombre_ciudad, m.mes, m.max_viento
FROM maximos_vientos m
INNER JOIN ciudades c ON m.ciudad_id = c.ciudad_id;




