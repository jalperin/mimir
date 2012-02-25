DELIMITER $$

DROP PROCEDURE IF EXISTS explode_table $$
CREATE PROCEDURE explode_table(bound VARCHAR(255))

  BEGIN

    DECLARE id INT DEFAULT 0;
    DECLARE value TEXT;
    DECLARE occurance INT DEFAULT 0;
    DECLARE i INT DEFAULT 0;
    DECLARE splitted_value INT;
    DECLARE done INT DEFAULT 0;
    DECLARE cur1 CURSOR FOR SELECT isi_articles.id_when_cited, isi_articles.cited_article_ids
                                         FROM isi_articles
                                         WHERE isi_articles.cited_article_ids != '';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    -- DROP TEMPORARY TABLE IF EXISTS table2;
    -- CREATE TEMPORARY TABLE table2(
    -- `id` CHAR(16) NOT NULL,
    -- `value` VARCHAR(255) NOT NULL
    -- ) ENGINE=Memory;

    OPEN cur1;
      read_loop: LOOP
        FETCH cur1 INTO id, value;
        IF done THEN
          LEAVE read_loop;
        END IF;

        SET occurance = (SELECT LENGTH(value)
                                 - LENGTH(REPLACE(value, bound, ''))
                                 +1);
        SET i=1;
        WHILE i <= occurance DO
          SET splitted_value =
          (SELECT REPLACE(SUBSTRING(SUBSTRING_INDEX(value, bound, i),
          LENGTH(SUBSTRING_INDEX(value, bound, i - 1)) + 1), ',', ''));

          IF splitted_value != 'null' THEN
          	INSERT INTO isi_article_citations VALUES (id, splitted_value);
          END IF;

          SET i = i + 1;

        END WHILE;
      END LOOP;

      -- SELECT * FROM table2;
    CLOSE cur1;
  END; $$


  CALL explode_table(' ');