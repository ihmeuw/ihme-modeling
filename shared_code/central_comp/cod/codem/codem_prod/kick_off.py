import subprocess, os, sys, json
import codem.db_connect as db_connect
import numpy as np
from codem.query import acause_from_id

# grab the arguments passed into the python script
user, model_version_id, db_name, SITE = [sys.argv[1], sys.argv[2], sys.argv[3],
    sys.argv[4]]


def get_codem_version(model_version_id, db_connection):
    '''
    (str) -> str, str

    Given a model version id gets the branch and commit of the code
    '''
    call = "SELECT code_version FROM cod.model_version WHERE model_version_id = {0}"
    result = json.loads(db_connect.query(call.format(model_version_id), connection=db_connection)["code_version"][0])
    return result["branch"], result["commit"]


def clone_codem():
    """
    Clones the codem repo, using the local host keys, into the
    current directory.
    :return: None
    """
    repo = "REPO_PATH"
    site = 'p' if SITE == 'live' else 's'
    key = 'SITE_PATH'
    if not os.path.isfile(key):
        raise IOError('COULD NOT FIND codem_rsa')
    call = "ssh-agent bash -c 'ssh-add {key}; git clone {repo}'"
    subprocess.call(call.format(key=key, repo=repo), shell=True)


def git(*args):
    """
    Run a list of git commands on the command line
    """
    return subprocess.call("git " + " ".join(list(args)), shell=True)


def make_code_dir(model_version_id, db_name):
    """
    Clones the codem code into the appropriate directory, creating the
    necessary folders if needed

    Returns the directory where the code is cloned to
    """
    # get the branch and the commit of codem to be used in this model run
    branch, commit = get_codem_version(model_version_id, db_name)

    branch_dir = "FILEPATH"
    commit_dir = "FILEPATH"

    if not os.path.exists(branch_dir):
        os.makedirs(branch_dir)

    if not os.path.exists(commit_dir):
        os.chdir(branch_dir)
        clone_codem()
        os.chdir(commit_dir)
        git("checkout", branch)
        git("checkout", commit)

    os.chdir("filepath")
    subprocess.call("sudo chmod 777 {branch_dir} -R".format(branch_dir=branch_dir), shell=True)

    return commit_dir


if user == 'USERNAME':
    py_script = "CODEV2.PY_PATH"
    sh_script = 'CODEV2.SH_PATH'

else:
    commit_dir = make_code_dir(model_version_id, db_name)
    sh_script = "CODEV2.SH_PATH" 
    py_script = "CODEV2.PY_PATH"


acause = acause_from_id(model_version_id, db_name)

# create a directory for this models results to be saved in and make it open to public use
base_dir = "FILEPATH"

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

# submit a job for codem run
sudo = 'sudo -u {user} sh -c ".'
qsub = ' QSUB_PATH '
name = '-N cod_{model_version_id}_global -P proj_codem '.format(model_version_id=model_version_id)
outputs = '-e {base_dir}/ -o {base_dir}/ '.format(base_dir=base_dir)
slots = '-pe multi_slot 50 '
scripts = '{sh_script} {py_script} {model_version_id} {db_connection} \
                    ' .format(sh_script=sh_script, py_script=py_script,
                               model_version_id=model_version_id, db_connection=db_name)

codem_call = qsub + name + outputs + slots + scripts
if user != 'USERNAME':
   codem_call = sudo + codem_call + '"'.format(user=user)
# call as sudo user if launching from viz
else:
    # add in optional arguments
    if len(sys.argv) > 5:
        for arg in sys.argv[5:]:
            codem_call += ' ' + str(arg)
    else:
        pass
    codem_call = 'source' + codem_call

process = subprocess.Popen(codem_call, shell=True, stdout=subprocess.PIPE)
out, err = process.communicate()

# use that models job_id to create a watcher job that sends an email if codem fails
job_id = out.split(" ")[2] # the output string takes the format 'Your job {job_id} has been submitted'
hold = "-hold_jid {job_id} -P proj_codem ".format(job_id=job_id) # job will launch once previous job has exited
email_script = "FILEPATH"
cleanup_script = '{email_script} {user} {model_version_id} {db_name}'.\
                            format(email_script=email_script, user=user,
                                   model_version_id=model_version_id, db_name=db_name)

cleanup_call = qsub + hold + outputs + cleanup_script
if user != 'USERNAME': # if launching from CodViz
    cleanup_call = sudo + cleanup_call + '"'.format(user=user)
else: # if launching centrally
    cleanup_call = 'source' + cleanup_call

subprocess.Popen(cleanup_call, shell=True)
sys.exit()
