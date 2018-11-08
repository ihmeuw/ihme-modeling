** Make Interpolated life-table


// PREP STATA
	clear
	set more off
	if c(os) == "Unix" {
		global prefix ""
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix ""
	}
	local exclude = 1
	if `exclude' ==1 {
		local ex _excl
	}
	
global WORKING_DIR "`1'"

** Run through modules
		foreach mod in "FINAL_min" {
			insheet using "$WORKING_DIR/usable/`mod'_lt_final`ex'.csv", clear
			** make age groups at .01 level
			forvalues x = 0(.01)105 {
				local amax = `x' + .005
				local amin = `x' - .005
				count if age>`amin' & age<`amax'
				local c = `r(N)'
				if `c' == 0 {
					count 
					local max = `r(N)' +1
					set obs `max'
					replace age = `x' in `max'
				}
				if `c' >1 {
					BREAK
				}
				
			}
			sort age
			ipolate ex age, gen(Pred_ex)
			keep age Pred_ex
			outsheet using "$WORKING_DIR/usable/`mod'_pred_ex.csv", comma replace

		}
	
** Save a file that only have 1 year age-groups
	keep if mod(age,1) == 0
