* Purpose: Forward previously calculated Brazil subnational proportions (braSubPr)

use "FILEPATH", clear

expand 2 if year_id==2019, generate(new)
replace year_id=2020 if new==1
drop new


expand 2 if year_id==2019, generate(new)
replace year_id=2021 if new==1
drop new


expand 2 if year_id==2019, generate(new)
replace year_id=2022 if new==1
drop new

save "FILEPATH", replace

quietly append using "FILEPATH"

*this output file below contains all the subnational split proportions 

save "FILEPATH",replace
