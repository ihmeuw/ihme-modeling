

*update age recoding for new age categories 

*recode for new age groups

use "FILEPATH/ageDistribution.dta",clear

expand 2 if age_group_id==4, generate(new)
replace age_group_id=388 if new==1
replace age_group_id=389 if new==0 & age_group_id==4

drop new

expand 2 if age_group_id==5, generate(new)
replace age_group_id=238 if new==1
replace age_group_id=34 if new==0 & age_group_id==5

drop new

table age_group_id

save "FILEPATH/ageDistribution20.dta",replace
