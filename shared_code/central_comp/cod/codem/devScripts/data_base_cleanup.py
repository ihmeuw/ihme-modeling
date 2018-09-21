import sqlalchemy as sql
import sys, os
folder = "INSERT_PATH_HERE"
sys.path.append(folder)
from codem.db_connect import query

db_connection = sys.argv[1]

description = "initial test run of 50"
calls = ["" for x in range(8)]

calls[0] = '''SET SQL_SAFE_UPDATES = 0;'''

calls[1] = '''DELETE FROM cod.submodel
WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
WHERE description="{desc}");'''.format(desc=description)


calls[2] = '''DELETE FROM cod.submodel_version_covariate
WHERE submodel_version_id IN (
SELECT submodel_version_id FROM cod.submodel_version
WHERE model_version_id IN (
SELECT model_version_id FROM cod.model_version
WHERE description="{desc}"));'''.format(desc=description)

calls[3] = '''DELETE FROM cod.submodel_version
WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
WHERE description="{desc}");'''.format(desc=description)

calls[4] = '''DELETE FROM cod.model_version_log
WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
WHERE description="{desc}");'''.format(desc=description)

calls[5] = '''DELETE FROM cod.model_covariate
WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
WHERE description="{desc}");'''.format(desc=description)

calls[6] = '''DELETE FROM cod.model
WHERE model_version_id IN (SELECT model_version_id FROM cod.model_version
WHERE description="{desc}");'''.format(desc=description)

calls[7] = '''DELETE FROM cod.model_version WHERE description="{desc}";'''.format(desc=description)


for i in range(len(calls)):
    print calls[i]
    query(calls[i], db_connection)
