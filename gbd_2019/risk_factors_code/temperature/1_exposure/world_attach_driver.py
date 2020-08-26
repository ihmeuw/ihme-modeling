import subprocess

project = "proj_custom_models"
python_path = "FILEPATH"
script_path = "FILEPATH"
environment = "ADDRESS"

for year in range(1980,2019):
    jobname = f'FILEPATH{year}'
    error = f'FILEPATH{year}.txt'
    output = '-FILEPATH'
	
    call = f'qsub -pe multi_slot 8 -P {project} -N {jobname} {error} {output} -v {environment} -b y {python_path} {script_path} {year}'
    subprocess.call(call, shell= True)