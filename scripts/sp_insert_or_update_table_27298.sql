
DELIMITER $$
CREATE PROCEDURE sp_insert_or_update_table_27298(
    IN tbl_name VARCHAR(64),
    IN col_names TEXT,
    IN val_list TEXT,
    IN update_clause TEXT
)
BEGIN
    SET @sql = CONCAT('INSERT INTO ', tbl_name, ' (', col_names, ') VALUES (', val_list, ')');
    IF update_clause IS NOT NULL AND update_clause != '' THEN
        SET @sql = CONCAT(@sql, ' ON DUPLICATE KEY UPDATE ', update_clause);
    END IF;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$
DELIMITER ;