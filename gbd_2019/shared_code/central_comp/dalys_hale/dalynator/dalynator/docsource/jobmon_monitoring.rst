Running Queries in Jobmon
*************************

The 'nators write to log files and also send their job status to jobmon. The jobs are visible using qstat,
but jobmon and (especially) the log files have more detailed inforomation.

You can query the jobmon database. Open a SQL browser and connect to the following
db server: `SERVER`` with port 3312. The database name is ``docker``.

Make a note of the task_name shown in qstat. Look in the docker.job table for that task name along with its  stdout/stderr paths, which you can follow to find more debugging information. Alternatively, you can map job_id from the docker.job table to job_instance_id in the job_instance table, and use that job_instance_id to query the job_instance_error_log to see the errors produced by that job instance

Tables:

job
    The (potential) call of a job. Like a function definition in python
job_instance
    An actual run of a job. Like calling a function in python. One job can have multiple job_instances if they are retried
job_instance_error_log
    Any errors produced by a job_instance.
job_instance_status
    Has the status of the running job_instance (as defined in the job_status table).
job_status
    Meta-data table that defines the four states of a job_instance.
task_dag
    Has every entry of task dags created, as identified by a dag_id and dag_hash
workflow
    Has every workflow created, along iwth it's associated dag_id, and workflow_args
workflow_run
    Has every run of a workflow, paired with it's workflow, as identified by workflow_id
workflow_run_status
    Meta-data table that defines the four states of a Workflow Run
workflow_status
    Meta-data table that defines the five states of a Workflow

