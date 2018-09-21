SET SQL_SAFE_UPDATES = 0;

DELETE FROM cod.submodel
	WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
    WHERE description="2015 initial test run of 50"
    );
    

DELETE FROM cod.submodel_version_covariate
	WHERE submodel_version_id IN (
		SELECT submodel_version_id FROM cod.submodel_version 
			WHERE model_version_id IN (
				SELECT model_version_id FROM cod.model_version
					WHERE description="2015 initial test run of 50"));

DELETE FROM cod.submodel_version 
	WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
							   WHERE description="2015 initial test run of 50");

DELETE FROM cod.model_version_log
	WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
							   WHERE description="2015 initial test run of 50");

DELETE FROM cod.model_covariate
	WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
							   WHERE description="2015 initial test run of 50");

DELETE FROM cod.model_covariate
	WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
							   WHERE description="2015 initial test run of 50");

SELECT model_version_id FROM cod.model_version
							   WHERE description="2015 initial test run of 50";