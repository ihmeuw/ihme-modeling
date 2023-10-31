import getpass
import sys
USER = getpass.getuser()
SHOCK_REPO = FILEPATH.format(USER)
sys.path.append(SHOCK_REPO)

from shock_world import ShockWorld


def ask_table_name():
    table = input("test or production? ").strip().lower()
    if table not in ["test", "production"]:
        print("Your answer was not one of the options.")
        return ask_table_name()
    else:
        return table


def ask_upload():
    load = input("do you want to upload? Yes or No ").strip().lower()
    if load not in ["yes", "no"]:
        print("Your answer was not one of the options.")
        return ask_upload()
    else:
        if load == "yes":
            load = True
        elif load == "no":
            load = False
        return load


table = ask_table_name()
load = ask_upload()


sw = ShockWorld(step="location_mapping", source="EMDAT",
                source_update=True, upload=load, conn_def=table)

sw.launch_location_matching()

sw.launch_step_uploader()
