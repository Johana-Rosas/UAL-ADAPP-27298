DELIMITER $$
CREATE PROCEDURE sp_create_table_matched_record_27298(IN table_cols TEXT)
BEGIN
    SET @sql = CONCAT('CREATE TABLE IF NOT EXISTS matched_record (', table_cols, ')');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;