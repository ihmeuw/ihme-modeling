Cholera attributable fraction modeling has an order that should be followed.

The code files are numbered. The 00_ prefix indicates files that should be run
to pull the input draws needed including an age curve, diarrhea cause specific
mortality, diarrhea incidence and prevalence, and cholera case fatality. These
outputs are saved as flat files and are very large.

*00_age_pattern_draws: Pulls global age curve draws for cholera proportion DisMod model.

*00_cholera_get_death_draws: Pulls location, year, age, sex draws for diarrhea from either 
CoDCorrect or from FauxCorrect.

*00_cholera_get_incprev_draws: Pulls location, year, age, sex draws for diarrhea incidence
and prevalence from the diarrhea best DisMod model

*00_get_cfr_draws: Pulls location, year, age, sex draws for cholera case fatality from
a DisMod model. 

Next, the cholera input proportion and WHO notification data are prepped in this file:

*01_update_data: Updates cholera proportion and notification data.

*prep_cholera_data_gbd2019.R: This file is currently not final (doesn't do everything in
01_update_data.do) but is an attempt to move some of the cholera process away from Stata
and into R. It also preps the case fatality and proportion data to be uploaded into
bundles to run DisMod models.

The last couple of files launch parallel jobs on the cluster where each job is for a single
draw. 

*02_launch_draws: Simply launches the jobs here:

*03_dobydraw_2019: This is the file that gets launched in parallel. This particular piece of
code does all of the cholera estimation. Run this without changing anything. 

Final file takes the Stata DTA draw files and saves the results as CSVs in a format for the
save_results_epi function.

*04_create_cholera_paf_csv: creates CSVs for the cholera PAFs!