import subprocess
import os
import sys
import json
import codem.db_connect as db_connect
import numpy as np
from codem.query import acause_from_id, model_version_type_id

# grab the arguments passed into the python script
user, model_version_id, db_name, SITE = [
    sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]]
# right now CodViz can't pass in gbd-round-id, so default it here
gbd_round_id = 5


def get_codem_version(model_version_id):
    '''
    (str) -> str, str

    Given a model version id gets the branch and commit of the code
    '''
    call = "SELECT code_version FROM cod.model_version WHERE model_version_id = {0}"
    result = json.loads(db_connect.query(call.format(
        model_version_id), connection=db_name)["code_version"][0])
    return result["branch"], result["commit"]


def clone_codem():
    """
    Clones the codem repo, using the local host keys on odessa, into the
    current directory.
    :return: None
    """
    repo = "URL"
    site = 'p' if SITE == 'live' else 's'
    key = '/www/URL-{site}/URL'.format(site=site)
    if not os.path.isfile(key):
        raise IOError('COULD NOT FIND codem_rsa')
    call = "ssh-agent bash -c 'ssh-add {key}; git clone {repo}'"
    subprocess.call(call.format(key=key, repo=repo), shell=True)


def git(*args):
    """
    Run a list of git commands on the command line
    """
    return subprocess.call("git " + " ".join(list(args)), shell=True)


# get the branch and the commit of codem to be used in this model run
branch, commit = get_codem_version(model_version_id)

branch_dir = "FILEPATH" % model_version_id
commit_dir = "FILEPATH" % branch_dir

# create these directories if they do not exist for this models version of codem to live in
if not os.path.exists(branch_dir):
    os.makedirs(branch_dir)

if not os.path.exists(commit_dir):
    os.chdir(branch_dir)
    clone_codem()
    os.chdir(commit_dir)
    git("checkout", branch)
    git("checkout", commit)

# make sure the permissions are open so that any one can use or edit them
os.chdir(commit_dir + "/prod")
subprocess.call(
    "sudo chmod 777 {branch_dir} -R".format(branch_dir=branch_dir), shell=True)

sh_script = commit_dir + "FILENAME.sh"
py_script = commit_dir + "FILENAME.py"

acause = acause_from_id(model_version_id, db_name)
type_id = model_version_type_id(model_version_id, db_name)
if type_id == 1:
    model_type = "global"
else:
    model_type = "datarich"

# create a directory for this models results to be saved in and make it open to public use
base_dir = "FILEPATH".format(acause, model_version_id)

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

subprocess.call(
    "sudo chmod 777 -R {}".format('/'.join(base_dir.split("/")[:-1])), shell=True)

# submit a job for codem run
sudo = 'sudo -u {user} sh -c '.format(user=user)
qsub = '". PATH/TO/SGE/ENGINE '
name = '-N cod_{model_version_id}_{model_type}_{acause} -P proj_codem '.format(model_version_id=model_version_id,
                                                                               model_type=model_type, acause=acause)
outputs = '-e {base_dir}/ -o {base_dir}/ '.format(base_dir=base_dir)
slots = '-pe multi_slot 47 '
scripts = '{sh_script} {py_script} {model_version_id} {db_connection} {gbd_round_id} \
                    "' .format(sh_script=sh_script, py_script=py_script,
                               model_version_id=model_version_id, db_connection=db_name, gbd_round_id=gbd_round_id)

codem_call = sudo + qsub + name + outputs + slots + scripts

process = subprocess.Popen(codem_call, shell=True, stdout=subprocess.PIPE)
out, err = process.communicate()

# use that models job_id to create a watcher job that sends an email if codem fails
job_id = out.split(" ")[2]
hold = "-hold_jid {job_id} -P proj_codem ".format(job_id=job_id)
email_script = "FILEPATH"
cleanup_script = '{email_script} {user} {model_version_id} {db_name}"'.\
    format(email_script=email_script, user=user,
           model_version_id=model_version_id, db_name=db_name)

cleanup_call = sudo + qsub + hold + cleanup_script

subprocess.Popen(cleanup_call, shell=True)
sys.exit()
