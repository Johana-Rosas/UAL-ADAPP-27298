DELIMITER $$
CREATE PROCEDURE sp_create_database_if_not_exists_27298(IN db_name VARCHAR(64))
BEGIN
    SET @sql = CONCAT('CREATE DATABASE IF NOT EXISTS `', db_name, '`');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;