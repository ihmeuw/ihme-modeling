* Purpose: Add new age groups for GBD 2020
*
use "FILEPATH", clear
*recodes former age pattern to accommodate new age groups for GBD 2020

table age_group_id
*expand new age groups

expand 2 if ( age_group_id==5 | age_group_id==4 ),generate(new)



replace age_group_id=388 if age_group_id==4 & new==1
replace age_group_id=389 if age_group_id==4  & new==0



replace age_group_id=238 if age_group_id==5 & new==1
replace age_group_id=34 if age_group_id==5 & new==0


drop new

*impute incidence in age_group==2
expand 2 if ( age_group_id==3),generate(new)
replace age_group_id=2 if new==1
drop if new==0 & age_group_id==2
drop new

save "FILEPATH", replace 

