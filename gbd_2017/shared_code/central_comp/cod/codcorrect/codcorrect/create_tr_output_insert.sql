DELIMITER |

CREATE
DEFINER = 'db_dev'@'%'
TRIGGER cod.tr_output_insert
BEFORE INSERT ON cod.output

FOR EACH ROW

BEGIN

DECLARE v_user VARCHAR(50);

SELECT LEFT(USER() ,  POSITION('@' IN USER() ) -1  )  INTO v_user;

    IF v_user != 'db_dev' THEN
        SET NEW.date_inserted = NOW();
    END IF;

    IF v_user != 'db_dev' THEN
        SET NEW.inserted_by =  v_user;
    END IF;

    IF v_user != 'db_dev' THEN
        SET NEW.last_updated = NOW();
    END IF;

    IF v_user != 'db_dev' THEN
        SET NEW.last_updated_by =  v_user;
    END IF;

    IF v_user != 'db_dev' THEN
        SET NEW.last_updated_action =  'INSERT';
    END IF;


END;
|
DELIMITER ;
