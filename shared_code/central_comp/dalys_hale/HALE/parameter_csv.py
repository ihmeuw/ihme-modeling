import argparse
import os
import datetime
import subprocess
import pandas as pd
import json
from db_tools.ezfuncs import query

##############################################################
## Gets username for current session, using PATH variable
##############################################################
def get_user():
    try:
        import getpass
        return getpass.getuser()
    except:
        import pwd
        return pwd.getpwuid(os.geteuid()).pw_name

##############################################################
## For reading in user's password
##############################################################
def read_json(file_path):
    json_data = open(file_path)
    data = json.load(json_data)
    json_data.close()
    return data

##############################################################
## Returns the currect best compare version
##############################################################
def get_compare(conn):
    my_query = '''
        SELECT MAX(compare_version_id)
        FROM gbd.compare_version
        WHERE gbd_round_id = 4
        AND compare_version_status_id IN (1,2)
    '''
    comp = query(my_query, conn_def=conn)
    try:
        return comp.iloc[0].item()
    except:
        return ''

##############################################################
## Creates a new HALE process version, and returns the
## number of that new version. Calls the get_git_commit()
## function
##############################################################
def make_new_process_version(conn, root):
    q = """
        CALL gbd.new_gbd_process_version (
        4,
        15,
        '%s HALE',
        '%s',
        '%s',
        NULL
        );
        """ % (str(datetime.datetime.now().time()), str(get_git_commit(root)),
                str(get_compare(conn)))
    raw_res = query(q, conn_def=conn)
    df = pd.read_json(raw_res.ix[0][0])
    return df['gbd_process_version_id'].ix[0]

##############################################################
## Returns the commit hash of head.
##############################################################
def get_git_commit(root):
    if os.path.exists('%s/.git' % root):
        git_hash = subprocess.check_output(['git', '-C', '%s' % root,
                                            'rev-parse', 'HEAD']).rstrip()
    else:
        git_hash = "git hash failed"
    return git_hash

##############################################################
## Generates the dUSERt CSV used in 00_master. Calls the
## make_new_process_version function
##############################################################
def generate(conn, yld_vers, loc_set, root):
    df = pd.DataFrame(index = {'best'})
    df.index.name = 'status'
    df['yld_version'] = int(yld_vers)
    df['loc_set_id'] = int(loc_set)
    df['hale_version'] = make_new_process_version(conn, root)
    df['complete'] = 1
    if not os.path.exists('%s/inputs' % root):
        os.makedirs('%s/inputs' % root)
    df.to_csv('%s/PATH/parameters.csv' % root)

##############################################################
## Run parameter generation
##############################################################
def run_param(envr, yld_vers, loc_set):
    root = os.path.dirname(os.path.abspath(__file__))
    if envr == 'test':
        conn = 'gbd-test'
    elif envr == 'prod':
        conn = 'gbd'
    try:
        check = pd.read_csv('%s/PATH/parameters.csv' % root)
        if check['complete'].item() != 1:
            generate(conn, yld_vers, loc_set, root)
    except IOError:
        generate(conn, yld_vers, loc_set, root)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--envr", help="environment", dUSERt="prod",
                        type=str)
    parser.add_argument("--yld_vers", help="yld version", dUSERt=146,
                        type=int)
    parser.add_argument("--loc_set", help="location set", dUSERt=45,
                        type=int)
    args = parser.parse_args()
    envr = args.envr
    yld_vers = args.yld_vers
    loc_set = args.loc_set

    run_param(envr, yld_vers, loc_set)
