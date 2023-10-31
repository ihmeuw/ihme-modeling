import getpass
import os
import sys
import pandas as pd

from jobmon.client.tool import Tool

"""
Instructions:

  The steps in this example are:
  1. Create a tool
  2. Create  workflow using the tool from step 1
  3. Create task templates using the tool from step 1
  4. Create tasks using the template from step 3
  5. Add created tasks to the workflow
  6. Run the workflow

To actually run the provided example:
  Make sure Jobmon is installed in your activated conda environment, and that you're on
  the Slurm cluster in a srun session. From the root of the repo, run:
     $ python training_scripts/workflow_template_example.py
"""

user = getpass.getuser()

# Create a tool
tool = Tool(name="era2melt_fix_tool")

# Create a workflow, and set the executor
workflow = tool.create_workflow(
    name="era2melt_fix_workflow",
)

# Create task templates
era2melt_tt = tool.get_task_template(
    default_compute_resources={
        "queue": "all.q",
        "cores": 8,
        "memory": "30G",
        "runtime": "30m",
        "stdout": f"/FILEPATH/{user}",
        "stderr": f"/FILEPATH/{user}",
        "project": "PROJECT",
        "constraints": "archive"  # To request a J-drive access node
    },
    template_name="era2melt_templ",
    default_cluster_name="slurm",
    command_template="OMP_NUM_THREADS=1 {rshell} -s {script} --loc_id {loc_id} --year {year} --gbd_round_year {gbd_round_year} --outDir {outDir} --job_name {job_name}",
    task_args=['outDir', 'gbd_round_year'],
    node_args=['loc_id', 'year', 'job_name'],
    op_args=['rshell', 'script']
)

arg_table = pd.read_csv("/FILEPATH/era2melt_fix_config.csv")
r_shell = "/FILEPATH/execRscript.sh"
exp_outdir = "/FILEPATH/"
gbd_round_year = "2021"

# Create tasks
exp_subtasks = []
exp_numtasks = len(arg_table)
print("Building tasks")
for i in range(exp_numtasks):
  task = era2melt_tt.create_task(
      name = arg_table.iloc[i, arg_table.columns.get_loc('job_name')],
      rshell = r_shell,
      script = "/FILEPATH/era2melt.R",
      outDir = exp_outdir,
      gbd_round_year = gbd_round_year,
      loc_id = arg_table.iloc[i, arg_table.columns.get_loc('arg.loc_id')],
      year = arg_table.iloc[i, arg_table.columns.get_loc('arg.year')],
      job_name = arg_table.iloc[i, arg_table.columns.get_loc('job_name')]
  )
  exp_subtasks.append(task)

# add task to workflow
print("Building workflow")
workflow.add_tasks(exp_subtasks)

# run workflow
print("Launching workflow")
workflow.run()
