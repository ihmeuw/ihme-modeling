import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
}

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
}

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATHs", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

*only has year=2017, all ages and sexes
import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace


*****************
clear all

import delimited using "FILEPATH", clear
tempfile master
save `master'

import delimited using "FILEPATH", clear
rename sex sex_id
rename year year_id
keep location_id year_id age_group_id sex_id deaths
tempfile cod_data
save `cod_data'

use `master'

merge 1:1 location_id year_id age_group_id sex_id using `cod_data', keep (1 3)

forvalues i = 0 / 999 {
     replace draw_`i' = draw_`i'+ deaths if year_id==2017 & deaths!=.
    }

drop _merge
drop deaths

export delimited using "FILEPATH", replace
