import os
import subprocess
import getpass

# set up stdout and stderr
username = getpass.getuser()
root = os.path.join('FILEPATH', username)
error_path = os.path.join(root, 'errors')
output_path = os.path.join(root, 'output')

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)

epic_workflow = "epic_workflow"
call = ('qsub -l fthread=3 -l m_mem_free=5.0G'
        ' -l h_rt=120:00:00'
        ' -q long.q'
        ' -cwd -P proj_epic'
        ' -o {o}'
        ' -e {e}'
        ' -N {jn}'
        ' python_shell.sh'
        ' workflow.py'.format(
            o=output_path,
            e=error_path,
            jn=epic_workflow))
subprocess.call(call, shell=True)
