# PAF calculation pipeline
This folder contains the code for the pipeline that builds temperature PAFs. It runs through exposure processing, TMREL processing, RRmax calculation, and then PAF calculation. It is launched as a single Jobmon pipeline by unified_launch.R, with each section of the pipeline having its task code stored in a separate subfolder.

The folder has been turned into an RStudio project, but this currently has no effect on the pipeline's processing.

## To run:
Open unified_launch.R in an RStudio session on the cluster or in your text editor of choice. Update the argument defaults to your desired values. If you are starting a new Jobmon workflow in the same day as a previous one, update the workflow name in workflow_args on line 127 (usually by adding "\_1" or the like at the end).

Once the updates are complete, the pipeline should be run from the command line via an R shell, inputting whatever arguments you don't want left as the defaults. Note that you cannot manually update the workflow name by this method, so you're limited to starting or resuming the workflow with the default (automatically date-based) workflow name. The standard launch format, to be run from within the PAF_calculation_pipeline folder that contains the code, is below:

/FILEPATH/execRscript.sh -s unified_launch.R --resume=T

## Warning:
There may be several hardcoded filepaths and version numbers still scattered throughout the code. If any major updates are made to temperature processing inputs (or if we just have spare time), I recommend checking if any of those need updating. 