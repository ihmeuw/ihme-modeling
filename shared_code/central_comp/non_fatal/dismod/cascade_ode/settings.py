import os
import json

this_path = os.path.dirname(__file__)
this_path = os.path.normpath(this_path)


def load():
    """ Get configuration """
    if os.path.isfile(os.path.join(this_path, "../config.local")):
        settings = json.load(
            open(os.path.join(this_path, "../config.local")))
    else:
        settings = json.load(
            open(os.path.join(this_path, "../config.dUSERt")))
    return settings
