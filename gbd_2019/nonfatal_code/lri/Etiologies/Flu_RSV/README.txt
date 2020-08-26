Description of files in this folder!

*create_cfr_draws.do: This script takes results from "winrun_lri_cfr.do" and makes a set of 
	draws for use in the PAF calculation.

*interpolate_odds_ratios.do: This file linearly interpolates odds ratios for age groups between
	5 and 65 years (these age groups have odds ratios from two published studies). Must be run
	before "create_odds_draws_2019.R"

*create_odds_draws_2019.R: This script takes estimates of the odds of LRI given influenza/RSV
	from a published review and makes draws for use in the PAF calculation. Must be run after
	"interpolate_odds_ratios.do"

*crosswalk_lri_etiologies_mr-brt.R: This file does a lot. It performs age-splitting, sex-splitting,
	and crosswalks for cv_inpatient and cv_explicit for RSV and influenza. It then
	saves these data into the appropriate bundle and crosswalk_versions.

*launch_lri_paf_calculation_ncluster.R: This file launches the PAF calculation code in parallel
	by location_id.

*lri_paf_calculation_2019.R: This file is the PAF calculation! In sequence, it makes draws
	for the PAFs for influenza and RSV and is run in parallel by location.

*winrun_lri_cfr.do: I have no idea why it is named "winrun", I just inherited something and kept the
	name the same. Basically, it sets up the needed information for BradMod (a wrapper for DisMod
	global fit) to estimate an age-integrated curve for the case fatality scalar, comparing
	CFR in bacterial to viral etiologies of LRI. It launches the file "winrun_troeger.do"

*winrun_file: This is a file that runs BradMod (a wrapper for the global fit in DisMod). The 
	output is a set of draws for the model result.