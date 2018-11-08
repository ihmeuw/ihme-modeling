DELIMITER |

CREATE
DEFINER = 'db_dev'@'%'
TRIGGER cod.tr_output_update
BEFORE UPDATE ON cod.output

FOR EACH ROW

BEGIN


DECLARE v_user VARCHAR(50);

SELECT LEFT(USER() ,  POSITION('@' IN USER() ) -1  )  INTO v_user;

    IF v_user != 'db_dev' THEN
        SET NEW.date_inserted = OLD.date_inserted;
    END IF;

    IF v_user != 'db_dev' THEN
        SET NEW.inserted_by =  OLD.inserted_by;
    END IF;

    IF v_user != 'db_dev' THEN
        SET NEW.last_updated = NOW();
    END IF;

    IF v_user != 'db_dev' THEN
        SET NEW.last_updated_by =  v_user;
    END IF;

    IF v_user != 'db_dev'  THEN
        IF NEW.last_updated_action = 'DELETE' THEN
           SET NEW.last_updated_action = 'DELETE';
        ELSE
            SET NEW.last_updated_action =  'UPDATE';
        END IF;
    END IF;


END;
|
DELIMITER ;
