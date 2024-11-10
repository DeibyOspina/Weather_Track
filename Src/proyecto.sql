/* Whater Track - Proyecto Ingenieria de datos 
Deiby Ospina - Cristian Ramos*/

--- Declaracion Tabalas 

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

--- Carga Masiva De Archivos 



--- Escenarios de análisis / (Funciones - Triggers - Consultas - Subconsultas)

