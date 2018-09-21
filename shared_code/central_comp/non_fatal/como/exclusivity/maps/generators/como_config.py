import os
import json

from db_tools import ezfuncs

here = os.path.dirname(os.path.realpath(__file__))


q = """
SELECT modelable_entity_id FROM
epi.sequela_old_dd341
WHERE active_end IS NULL
"""
# get data
me_ids = ezfuncs.query(q, conn_def="epi")["modelable_entity_id"].tolist()

# build map
process_map = {}
process_map["como"] = {}
process_map["como"]["class"] = "__main__.COMO"
process_map["como"]["in"] = {}
process_map["como"]["out"] = {}

# add inputs
for me_id in me_ids:
    process_map["como"]["in"][str(int(me_id))] = (
        "__main__.ModelableEntity")
process_map["como"]["out"]["como_version"] = "__main__.ComoVersion"

# write to disk
with open(os.path.join(here, "..", "json", "como.json"), "w") as outfile:
    json.dump(process_map, outfile, sort_keys=True, indent=2)
