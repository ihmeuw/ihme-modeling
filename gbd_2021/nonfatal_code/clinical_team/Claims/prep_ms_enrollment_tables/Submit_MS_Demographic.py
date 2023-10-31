"""
submit the MS demographic jobs by year
then submit a job to create the 350 processing
groups that are used to merge on enrolids
"""
import subprocess
import getpass
import time
from clinical_info.Functions import hosp_prep

user = getpass.getuser()

repo = FILEPATH

for year in [2000, 2010, 2015, 2011, 2014, 2012, 2013, 2016, 2017]:

    qsub = f"""
        QSUB
        """

    qsub = " ".join(qsub.split())

    subprocess.call(qsub, shell=True)
    print("sent out job for year {}".format(year))
    time.sleep(130)

# hold until all those jobs are finished
hosp_prep.job_holder(job_name="ms_samp", sleep_time=65, init_sleep=600)
# send out the db_helpers step to create the enrolid files that the parallel
# jobs use
qsub = f"""
    QSUB
    """

qsub = " ".join(qsub.split())

subprocess.call(qsub, shell=True)
