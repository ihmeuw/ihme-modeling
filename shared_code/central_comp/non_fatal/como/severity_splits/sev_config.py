import json
from db_tools import ezfuncs


def gen_config(env='dev'):
    q = """
    select
        distinct parent_meid, child_meid
    from
        severity_splits.split_version
    join
        severity_splits.split_proportion using (split_version_id)
    where
        is_best = 1
        and gbd_round_id = 4
    """
    df = ezfuncs.query(q, conn_def="epi")

    dct = {}
    for key in df["parent_meid"].unique():

        dct["split_" + str(key)] = {}
        dct["split_" + str(key)]["class"] = "__main__.SevSplits"
        dct["split_" + str(key)]["in"] = {}
        dct["split_" + str(key)]["in"][str(key)] = "__main__.ModelableEntity"
        dct["split_" + str(key)]["out"] = {}
        for out in df.ix[df["parent_meid"] == key, "child_meid"].unique():
            dct["split_" + str(key)]["out"][str(out)
                                            ] = "__main__.ModelableEntity"
        dct["split_" + str(key)]["args"] = []
        dct["split_" + str(key)]["kwargs"] = {"parent_meid": str(key)}

    with open("FILEPATH.json", "w"
              ) as outfile:
        json.dump(dct, outfile, indent=2)


gen_config()
