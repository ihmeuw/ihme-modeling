/*
AUTHOR: USERNAME

DATE: DATE

PURPOSE: Load parameters for injuries process.
*/

capture program drop load_params
program define load_params
	version 13
	syntax
	
	
// E-codes

	global nonshock_e_codes inj_trans_road_pedest inj_trans_road_pedal inj_trans_road_2wheel inj_trans_road_4wheel inj_trans_road_other inj_trans_other inj_falls inj_drowning inj_fires inj_poisoning inj_mech_gun inj_mech_suffocate inj_mech_other inj_medical inj_animal_venom inj_animal_nonven inj_foreign_aspiration inj_foreign_eye inj_foreign_other inj_othunintent inj_suicide_firearm inj_suicide_other inj_homicide_gun inj_homicide_knife inj_homicide_other inj_non_disaster
	
	global shock_e_codes inj_disaster inj_war_warterror inj_war_execution
	
	global modeled_e_codes ${nonshock_e_codes} ${shock_e_codes}
	
	global dismod_modeled_sequela_ids "2339 2585 2586 2587 2588 2589 2340 2341 2342 2343 2344 2345 2590 2591 2592 2346 2347 2593 2594 2348 2595 2596 2349 2350 10726 10727 2351 2597 2598 2599 2352 2353"

	global final_sequelae ${modeled_e_codes} inj_trans_road inj_mech inj_animal inj_homicide inj_suicide
	
// N-code names
	global n_codes
	forvalues i = 1/28 {
		global n_codes ${n_codes} N`i'
	}
	forvalues i = 30/48 {
		global n_codes ${n_codes} N`i'
	}

	global otp_ncodes N11 N12 N13 N14 N15 N16 N17 N18 N19 N20 N21 N22 N23 N24 N25 N26 N27 N3 N30 N31 N32 N35 N36 N37 N38 N39 N40 N41 N42 N43 N44 N45 N46 N47 N48 N6 N7 N8
	global inp_only_ncodes  N1 N10 N2 N28 N33 N34 N37 N4 N5 N9

	global inp_ncodes $otp_ncodes $inp_only_ncodes
	
// Demographic groups
	global ages 0 .01 .1 1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95
	global reporting_ages 0 .01 .1 1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95

	global sexes 1 2
	global income_levels 0 1
	
// Number of draws
	global drawnum 1000
	** maximum draw index
	global drawmax = $drawnum - 1
		
end
