The code scripts in this folder are the files that calculate and save the 
attributable fractions, by draw, for 11/13 diarrhea etiologies (all of the
etiologies that have odds ratios and DisMod models). This code should be
run after running each DisMod proportion model.

*launch_paf_calculation_ncluster.R: this file launches the script, parallel
by location, to calculate the attributable fractions. The "ncluster" suffix
simply indicates the new requirements to run on the Fair cluster (new to GBD 2019).

*paf_calculation_2019.R: This is the file that calculates the attributable 
fractions for the diarrhea etiologies. This file is run in parallel by 
location_id and performs a loop for each etiology. 

*paf_calculation_2019_rotavirus.R: This file is the same as the one above
except it accounts for rotavirus vaccine impact by adjusting the draws
depending on the vaccine coverage and predicted vaccine efficacy in a given
location and for eligible age groups (ids 4 & 5).

