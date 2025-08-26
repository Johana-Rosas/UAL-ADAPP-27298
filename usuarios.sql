CREATE DATABASE IF NOT EXISTS dbo;
USE dbo;

CREATE TABLE Usuarios (
    userId INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    rol ENUM('admin','user') DEFAULT 'user',
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

