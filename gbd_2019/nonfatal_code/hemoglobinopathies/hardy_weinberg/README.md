# Hardy-Weinberg Transformations

The purpose of this code is the calculate the prevalence of genetic traits based off of prevalence of blood disorders. 

Here is the main idea for the calculations. Let p^2 represent the prevelence of a genetic disease. We can use the Hardy-Weinberg equations 
to calculate the prevelence of carriers. Using the basic equation (p+q)^2 = p^2 + 2pq + q^2 = 1, along with q=1-p, we can solve for the
2pq term (the carriers). There are some slight variations depending on the hardy_type. 

The code uses jobmon and task_master which relies on the Python tool luigi. Here are some instructions from Logan Sandar:

#first switch to the epic envirnonment
source /ihme/code/central_comp/miniconda/bin/activate epic

#fire up the luigi server and make sure you logdir exists:
luigid --background --logdir ~/temp/

#get the hostname that the luigi server is runnon on using some bash
env | grep HOSTNAME

#type it into your browser url with :8082 at the end to access the visualizer (this is a small job and might not be neccesary. Still fun though!)
for example http://cn402.ihme.washington.edu:8082/

#run the pipeline
python -m tasks Hook --identity process_hook --workers 4 --dry-run (omit the dry-run argument if you want to actually run the process)

#to visualize the whole pipeline click on the "Hook" task under TASK FAMILIES on the far left.
#then click on the far right under Actions click "View Graph"

As a final note, the code uses Logan's job_utils package. When cloning the repo it would also be a good idea to clone a copy of job_utils. 






