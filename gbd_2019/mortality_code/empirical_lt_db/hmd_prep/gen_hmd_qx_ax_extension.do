** From HMD results, generate paramaeters to extend qx from age 60 through 95 up, based on relative relationships
** with previous age and other relationships.

clear all
set more off

if (c(os)=="Unix") global root "FILEPATH/root"
if (c(os)=="Windows") global root "FILEPATH/root"

tempfile malelts femalelts lts
*** males ***
global working "$FILEPATH/root/HMD/mltper_5x1"
local sfiles: dir "$working" files "*.txt", respectcase
foreach file of local sfiles {
  cd "$working"
  infix year 1-10 str age 11-21 mx 23-31 qx 32-40 ax 41-46 lx 47-55 dx 56-63 nLx 63-71 Tx 72-80 ex 81-87 using `file', clear
  drop if year==.
  gen iso3=reverse(substr(reverse("`file'"),16,.))
  cap append using `malelts'
  save `malelts', replace
}

gen sex="male"
save `malelts', replace
** females ***
global working "$FILEPATH/root/HMD/fltper_5x1"
local sfiles: dir "$working" files "*.txt", respectcase
foreach file of local sfiles {
  cd "$working"
  infix year 1-10 str age 11-21 mx 23-31 qx 32-40 ax 41-46 lx 47-55 dx 56-63 nLx 63-71 Tx 72-80 ex 81-87 using `file', clear
  drop if year==.
  gen iso3=reverse(substr(reverse("`file'"),16,.))
  cap append using `femalelts'
  save `femalelts', replace
}

gen sex="female"
save `femalelts', replace
append using `malelts'

gen newage=.
replace newage=0 if age=="0"
replace newage=1 if age=="1-4"
replace newage=110 if age=="110+"

forvalues x=3/23 {
  local n1=(`x'-2)*5
  local n2=(`x'-2)*5+4
  replace newage=`n1' if age=="`n1'-`n2'"
}

drop age
rename newage age

tempfile hmdlts mxdata

** Drop lifetables deemed by HMD to be unworthy
drop if iso3=="TWN" & year<1980
drop if iso3=="UKR" & year<1970
drop if iso3=="BLR" & year<1970
drop if iso3=="BGR" & year<1970
drop if iso3=="EST" & year>2000
drop if iso3=="IRL" & year>1949 & year<1986
drop if iso3=="ITA" & year>1871 & year<1906
drop if iso3=="LVA" & year>1958 & year<1970
drop if iso3=="LTV" & year>1958 & year<1970
drop if iso3=="PRT" & year>1939 & year<1971
drop if iso3=="RUS" & year>1958 & year<1970
drop if iso3=="SVK" & year>1949 & year<1962
drop if iso3=="ESP" & year>1907 & year<1961
drop if iso3=="FIN" & year==1918
** civil war*
drop if iso3=="FIN" & year>1938 & year<1950
** war with Soviet Union *

**drop years during spanish flu and world war 2**
drop if year>1913 & year<1921
drop if year>1939 & year<1946
drop if year<1900

keep if year >= 1950
save `hmdlts'

drop if age>105
keep iso3 sex year age qx
keep if age>55
sort iso3 sex year age
tempfile hmds
save `hmds'


***** iso3 from KT database *****
global working "$FILEPATH/root/kt-database"
tempfile kts
local sfiles: dir "$working" files "*.txt", respectcase
cd "$working"

qui foreach file of local sfiles {
  insheet using `file', comma clear
  gen iso3=substr("`file'",2,3)
  gen sex=substr("`file'",1,1)
  cap append using `kts'
  save `kts',replace
  noisily dis "`file'"
}

keep iso3
duplicates drop iso3,force
sort iso3
merge 1:m iso3 using `hmds'
keep if _merge==3
drop _merge
gen logitqx=ln(qx/(1-qx))
reshape wide qx logitqx, i(iso3 sex year) j(age)

forvalues w=80(5)105 {
  drop if qx`w'==.
}

keep qx* iso3 sex year logitqx*
reshape long qx, i(iso3 sex year) j(age)
renpfix logitqx i_logitqx
gen logitqx=ln(qx/(1-qx))
sort iso3 sex year age
bysort iso3 sex year: gen logitdiff=logitqx[_n+1]-logitqx
drop if age==105
tempfile ktmx pars
save `ktmx'
*** predictions for different age groups
tempfile ratiopar

forvalue w=60(5)95 {
**no lndiff values for 105 age group
  local sexs="male female"
  foreach ss of local sexs {
    use `ktmx',clear
    keep if age>=`w'
    char age [omit] `w'
    keep if sex=="`ss'"
    xi:xtmixed logitdiff i.age i_logitqx`w' || iso3:
    local t=(100-`w')/5+2
    mat par=e(b)
    mat par=par[1,1..`t']
    clear
    svmat par,names(matcol)
    gen sex="`ss'"
    gen start=`w'
    cap append using `ratiopar'
    save `ratiopar',replace
    matrix drop par
  }
}

export delimited "$FILEPATH/empirical_lt/inputs/hmd_qx_extension.csv", replace

** Generate regression results in order to extend ax for ages 75-105, based on mx and squared mx
** Regression results based on HMD results
use `hmdlts', clear

keep iso3 sex  year age mx ax
keep if age>=75
gen smx=mx^2
tempfile mxax
save `mxax'

tempfile axpar
local sexs="male female"

foreach s of local sexs {
  forvalues j=75(5)105 {
        use `mxax',clear
      keep if sex=="`s'"
      reg ax mx smx if sex=="`s'" & age==`j'
    mat ax_`s'_`j'=e(b)
    mat ax_`s'_`j'=ax_`s'_`j'[1,1..3]
    matname ax_`s'_`j' par_mx par_smx par_con, col(1..3) explicit
    clear
    svmat ax_`s'_`j', names(col)
    gen sex="`s'"
    gen age=`j'
    cap append using `axpar'
    save `axpar',replace
  }
}

export delimited "$$FILEPATH/empirical_lt/inputs/hmd_ax_extension.csv", replace

