	
*** BOILERPLATE ***
    clear
	set more off
	
	if c(os) == "Unix" {
		local j FILEPATH
		}
	else {
		local j FILEPATH
		}

	run FILEPATH/get_demographics.ado
	run FILEPATH/create_connection_string.ado
	
	tempfile ages appendTemp
	
	local dataDir FILEPATH

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
	noisily di "{hline}" _n _n "Connecting to the database"
		
	create_connection_string, database(ADDRESS)
	local shared = r(conn_string)
	
	
*** PULL AGE GROUP DATA ***
	get_demographics, gbd_team(epi) clear
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_ids)'", " ", ",", .)')") `shared' clear
	expand 2, generate(sex_id)
	replace sex_id = sex_id + 1
	expand 2, generate(chagas)
	save `ages'




foreach sex in Male Female {
foreach status in negative positive {
  import excel "`dataDir'/LVdysfunction_by_SerologicalStatus_gender_age_REDS2.xlsx", sheet("Sero`status'-`sex'") cellrange(A3:E44) firstrow clear
  drop A
  rename B age
  rename C noHf
  rename D hf
  rename Total total

  gen sex_id = ("`sex'"=="Female") + 1
  gen chagas = "`status'"=="positive"
  
  if "`sex'"=="Female" | "`status'"=="positive" append using `appendTemp'
  save `appendTemp', replace
  }
  }

  
gen age_start = round(age-2, 5)
collapse (sum) noHf hf total, by(sex_id chagas age_start)
drop if missing(age_start)


merge 1:1 age_start sex chagas using `ages', assert(2 3) nogenerate
egen age_mid = rowmean(age_start age_end)

replace total = 1 if missing(total)
replace hf = 0 if age_start<20
gen prHf = hf / total

gen sexB = (sex_id - 1.5) * 2
gen chagasB = (chagas - 0.5) * 2

capture drop pred 
capture drop ageS*
mkspline ageS = age_mid, cubic knots(0 25 40) displayknots

glm hf i.sex_id i.chagas ageS*, family(binomial total) 
predict pred, xb

meglm hf sexB chagasB || age_mid:, family(binomial total)
predict se, stdp


keep pred se total age_group_id age_mid sex_id chagas prHf
reshape wide pred se total prHf, i(age_group_id age_mid sex_id) j(chagas)

gen prHf = prHf1 - prHf0
drop prHf?

forvalues i = 0/999 {
	local random = rnormal(0, 1)
	generate draw_`i' = invlogit(pred1 + (`random' * se1)) - invlogit(pred0 + (`random' * se0)) 
	}
	
	
egen mean =rowmean(draw_*)
egen lower = rowpctile(draw_*), p(2.5)
egen upper = rowpctile(draw_*), p(97.5)

*twoway (rcap upper lower age_mid if sex==1, mcolor(navy)) (rcap upper lower age_mid if sex==2, mcolor(maroon))(scatter mean age_mid if sex==1, mcolor(navy)) (scatter mean age_mid if sex==2, mcolor(maroon)) (scatter prHf age_mid)

keep age_group_id sex_id draw_*
generate modelable_entity_id = 2413
generate outcome = "hf"

save FILEPATH/hfPrDraws.dta, replace

