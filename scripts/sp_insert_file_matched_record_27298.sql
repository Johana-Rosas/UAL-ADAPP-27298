DELIMITER $$
CREATE PROCEDURE sp_insert_file_matched_record_27298(
    IN p_col_names TEXT,
    IN p_values TEXT
)
BEGIN
    SET @sql = CONCAT('INSERT INTO matched_record (', p_col_names, ') VALUES (', p_values, ')');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;