DELIMITER $$
CREATE PROCEDURE sp_drop_table_matched_record_27298()
BEGIN
    SET @sql = 'DROP TABLE IF EXISTS matched_record';
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;