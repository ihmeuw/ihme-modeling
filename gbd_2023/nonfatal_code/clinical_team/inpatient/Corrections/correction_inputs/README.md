This process takes in individual level hospital records and outputs tabulated
data by age/sex_id/location_id/year_start/year_end/bundle and then counts of
claims and individual cases for bundles from the primary diagnosis position only
and from any diagnosis position. So this currently results in 4 columns of
case counts (estimate id 14, 15, 16, 17 equivalent).

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
More technical detail and examples of how this is run:

An ezjobmon workflow taking a single run_id and 1 or more sources in a list can be processed
with `launch_cf_inputs.launch_workflow.py`. This workflow sends out a series of tasks that
call worker_distributor.py. This worker script serves to unify a few slightly
different ways of processing CF inputs that vary by source. It uses a task's source to call
the appropriate processing function in /sources/{source-name}_worker.py.
