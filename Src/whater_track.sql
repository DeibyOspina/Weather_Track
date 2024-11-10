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

--- Escenarios de análisis / (Funciones - Triggers - Consultas - Subconsultas)
