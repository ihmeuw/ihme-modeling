    local jdata_in_dir "FILEPATH"
    local cod_in_dir "FILEPATH"
    
** *********************************************************************************************
**
** State ID's
    import excel "FILEPATH", sheet("Code_State") firstrow clear
    
    rename CodeState state_id
    rename StateSigl abbrev
    keep state_id abbrev
    
    tempfile states
    save `states', replace
    

    use "FILEPATH", clear

** INITIAL CLEANING **
    gen list = "ICD10"
    gen year = 2013
    
    rename sexo sex
    replace sex = "9" if sex == "0"
    destring sex, replace
    
    ** root cause of death
    rename causabas cause    
    
    generate age = substr(idade,2,2)
    destring age, replace
    generate age_unit = substr(idade,1,1)
    destring age_unit, replace

    ** Remove unnecessary rows and columns from data
    keep sex age age_unit cause year list codmunres
    
    ** first two digits of codmunres (deceased's municipality of residence) is state id
    gen state_id = substr(codmunres, 1, 2)     
    destring state_id, replace
    drop codmunres
    
    merge m:1 state_id using `states', assert(2 3) keep(3) nogen
    
    drop state_id

** CAUSE AND CAUSE_NAME (cause, cause_name) **

    gen cause_name = ""

    forvalues i = 1/26 {
        generate deaths`i' = 0
    }
    replace deaths1 = 1
    replace deaths2 = 1 if (age_unit == 1) | (age_unit == 2) | (age_unit == 3) | (age_unit == 4 & age == 0)
    replace deaths3 = 1 if age == 1 & age_unit == 4
    replace deaths4 = 1 if age == 2 & age_unit == 4
    replace deaths5 = 1 if age == 3 & age_unit == 4
    replace deaths6 = 1 if age == 4 & age_unit == 4
    replace deaths7 = 1 if age >= 5 & age <= 9 & age_unit == 4
    replace deaths8 = 1 if age >= 10 & age <= 14 & age_unit == 4
    replace deaths9 = 1 if age >= 15 & age <= 19 & age_unit == 4
    replace deaths10 = 1 if age >= 20 & age <= 24 & age_unit == 4
    replace deaths11 = 1 if age >= 25 & age <= 29 & age_unit == 4
    replace deaths12 = 1 if age >= 30 & age <=34 & age_unit == 4
    replace deaths13 = 1 if age >= 35 & age <=39 & age_unit == 4
    replace deaths14 = 1 if age >= 40 & age <= 44 & age_unit == 4 
    replace deaths15 = 1 if age >= 45 & age <= 49 & age_unit == 4
    replace deaths16 = 1 if age >= 50 & age <= 54 & age_unit == 4 
    replace deaths17 = 1 if age >= 55 & age <= 59 & age_unit == 4
    replace deaths18 = 1 if age >= 60 & age <= 64 & age_unit == 4 
    replace deaths19 = 1 if age >= 65 & age <= 69 & age_unit == 4
    replace deaths20 = 1 if age >= 70 & age <= 74 & age_unit == 4
    replace deaths21 = 1 if age >= 75 & age <= 79 & age_unit == 4
    replace deaths22 = 1 if age >= 80 & age <= 84 & age_unit == 4
    replace deaths23 = 1 if age >= 85 & age <= 89 & age_unit == 4
    replace deaths24 = 1 if age >= 90 & age <= 94 & age_unit == 4
    replace deaths25 = 1 if age >= 95 & age != .
    replace deaths26 = 1 if age == . | age_unit == 5
    
    ** now do the same for infant deaths
    forvalues i = 91/94 {
        gen deaths`i' = .
    }
    replace deaths91 = 1 if age_unit == 1 | (age_unit == 2 & age == 0)
    replace deaths92 = 1 if age_unit == 2 & age >= 1 & age <= 6
    replace deaths93 = 1 if age_unit == 2 & age >= 7
    replace deaths94 = 1 if age_unit == 3 | (age_unit == 4 & age == 0)
    
    collapse (sum) deaths*, by(year cause cause_name sex abbrev list) fast

    gen frmat = 0

    gen im_frmat = 2
    
