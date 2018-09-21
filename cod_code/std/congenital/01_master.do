
	local root_dir = "`c(pwd)'"
	local date = "2017_05_29"
	local out_dir = "OUT_DIR"
	local in_dir = "IN_DIR"

	capture mkdir "TMP"
	capture mkdir "`out_dir'/checks"

	// format all inputs
	local step1 = 0

	// model death rate by treatment quality
	local step2 = 0

	// model antenatal syphilis testing
	local step3 = 0

	// model antenatal treatment
	local step4 = 0

	// model antenatal visit proportions
	local step5 = 0

	// get age sex pattern of syphilis
	local step6 = 0

	// calculate congenital syphilis deaths
	local step7 = 0
	
	// run diagnostic panels
	local step8 = 1

** **************************************************************************
** PREP STATA
** **************************************************************************

	// prep stata
	clear all
	set more off
	set maxvar 32000

	// Set OS flexibility
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		local h "~"
	}
	else if c(os) == "Windows" {
		local j "J:"
		local h "H:"
	}
	sysdir set PLUS "`h'/ado/plus"

** **************************************************************************
** run steps
** **************************************************************************

	if "`step1'" != "0" do "`root_dir'/format_input_data.do" root_dir(`root_dir') date(`date') out_dir(`out_dir')
	if "`step2'" != "0" do "`root_dir'/model_nn_deaths_by_treatment.do" root_dir(`root_dir') date(`date') out_dir(`out_dir')
	if "`step3'" != "0" do "`root_dir'/model_anc_syph_test.do" root_dir(`root_dir') date(`date') out_dir(`out_dir')
	if "`step4'" != "0" do "`root_dir'/model_anc_syph_treatment.do" root_dir(`root_dir') date(`date') out_dir(`out_dir')
	if "`step5'" != "0" do "`root_dir'/calc_anc_visit_props.do" root_dir(`root_dir') date(`date') out_dir(`out_dir')
	if "`step6'" != "0" do "`root_dir'/calc_cod_data_prop.do" root_dir(`root_dir') date(`date') out_dir(`out_dir')
	if "`step7'" != "0" do "`root_dir'/calc_nn_syph_deaths.do" root_dir(`root_dir') date(`date') out_dir(`out_dir')
	if "`step8'" != "0" do "`root_dir'/diagnostic.do" root_dir(`root_dir') date(`date') out_dir(`out_dir') in_dir(`in_dir')
