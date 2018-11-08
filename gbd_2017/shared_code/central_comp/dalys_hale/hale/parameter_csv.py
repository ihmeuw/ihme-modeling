import os
import datetime
import subprocess
import pandas as pd
import json
from db_tools.ezfuncs import query
from db_queries import get_envelope
from gbd.gbd_round import gbd_round_from_gbd_round_id as gbdr

def get_user():
    ##############################################################
    ## Gets username for current session, using PATH variable
    ##############################################################
    try:
        import getpass
        return getpass.getuser()
    except:
        import pwd
        return pwd.getpwuid(os.geteuid()).pw_name

def read_json(file_path):
    ##############################################################
    ## For reading in user's password
    ##############################################################
    json_data = open(file_path)
    data = json.load(json_data)
    json_data.close()
    return data

def get_compare(conn, gbd_round_id):
    ##############################################################
    ## Returns the current best compare version
    ##############################################################
    my_query = '''
        SELECT MAX(compare_version_id)
        FROM gbd.compare_version
        WHERE gbd_round_id = {}
        AND compare_version_status_id IN (1,2)
    '''.format(gbd_round_id)
    comp = query(my_query, conn_def=conn)
    try:
        return comp.iloc[0].item()
    except:
        return ''

def pull_mort_vers(gbd_round_id):
    ##############################################################
    ## Returns the current best with shock life table version
    ##############################################################
    env = get_envelope(gbd_round_id=gbd_round_id, with_shock=1, with_hiv=1,
                       location_id=1, sex_id=1, age_group_id=10,
                       year_id=gbdr(gbd_round_id))
    lt_vers = env['run_id'].item()
    return lt_vers

def make_new_process_version(conn, gbd_round_id, root):
    ##############################################################
    ## Creates a new HALE process version, and returns the
    ## number of that new version. Calls the get_git_commit()
    ## function
    ##############################################################
    q = """
        CALL gbd.new_gbd_process_version (
        {},
        15,
        '{} HALE',
        '{}',
        '{}',
        NULL
        );
        """.format(str(gbd_round_id), str(datetime.datetime.now().time()),
                   str(get_git_commit(root).decode('utf-8')),
                   str(get_compare(conn, gbd_round_id)))
    raw_res = query(q, conn_def=conn)
    df = pd.read_json(raw_res.ix[0][0])
    return df['gbd_process_version_id'].ix[0]

def get_git_commit(root):
    ##############################################################
    ## Returns the commit hash of head.
    ##############################################################
    if os.path.exists('%s/.git' % root):
        git_hash = subprocess.check_output(['git', '-C', '%s' % root,
                                            'rev-parse', 'HEAD']).rstrip()
    else:
        git_hash = "git hash failed"
    return git_hash

def generate(conn, yld_vers, loc_set, year_id, gbd_round_id, root):
    ##############################################################
    ## Generates the default CSV used in 00_master. Calls the
    ## make_new_process_version function
    ##############################################################
    df = pd.DataFrame(index = {'best'})
    df.index.name = 'status'
    df['yld_version'] = int(yld_vers)
    df['mort_run'] = pull_mort_vers(gbd_round_id)
    df['loc_set_id'] = str(loc_set)
    df['year_id'] = str(year_id)
    df['hale_version'] = make_new_process_version(conn, gbd_round_id, root)
    df['in_progress'] = 1
    if not os.path.exists('{}/inputs'.format(root)):
        os.makedirs('{}/inputs'.format(root))
    df.to_csv('{}/inputs/parameters.csv'.format(root))

def run_param(envr, yld_vers, loc_set, year_id, gbd_round_id):
    ##############################################################
    ## Run parameter generation
    ##############################################################
    root = os.path.dirname(os.path.abspath(__file__))
    if envr == 'prod':
        conn = 'gbd'
    elif envr == 'test':
        conn = 'gbd-test'
    else:
        raise RuntimeError("unrecognized env: {}".format(envr))
    try:
        check = pd.read_csv('{}/inputs/parameters.csv'.format(root))
        if check['in_progress'].item() != 1:
            generate(conn, yld_vers, loc_set, year_id, gbd_round_id, root)
    except FileNotFoundError:
        generate(conn, yld_vers, loc_set, year_id, gbd_round_id, root)
    except IOError:
        generate(conn, yld_vers, loc_set, year_id, gbd_round_id, root)
    except KeyError:
        generate(conn, yld_vers, loc_set, year_id, gbd_round_id, root)
