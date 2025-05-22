import json


# custom parsers
def int_parser(s):
    try:
        s = int(s)
        return s
    except:
        return s


# convert json objects to dicts
def json_parser(s):
    def json_kv_2_int(x):
        if isinstance(x, dict):
            tempd = {}
            for k, v in list(x.items()):
                # convert key
                try:
                    key = int(k)   # try int conversion
                except ValueError:
                    key = k  # otherwise leave the same

                # convert val
                if isinstance(v, str):
                    try:
                        val = int(v)  # try int conversion
                    except ValueError:
                        val = v  # otherwise leave the same
                elif isinstance(v, dict):
                    val = json_kv_2_int(v)  # recursive call if dict
                else:
                    val = v  # leave the same if not unicode
                tempd[key] = val
            return tempd
        return x

    d = json.loads(s)
    return json_kv_2_int(d)
