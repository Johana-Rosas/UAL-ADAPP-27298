-- scripts/sp_create_table_if_not_exists_27298.sql
DELIMITER $$
CREATE PROCEDURE sp_create_table_if_not_exists_27298(
    IN tbl_name VARCHAR(64),
    IN table_cols TEXT
)
BEGIN
    SET @sql = CONCAT('CREATE TABLE IF NOT EXISTS ', tbl_name, ' (', table_cols, ')');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;