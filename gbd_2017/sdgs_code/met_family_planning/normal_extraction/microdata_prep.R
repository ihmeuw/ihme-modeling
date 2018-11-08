####################################################################################################
## Purpose: Topic-specific code for contraception ubcov extraction
##          Main goals are to:
##          1. Fix survey-specific issues to make all surveys comparable
##          2. Determine which women were asked about contraceptive usage, and how many of them are using a modern method
##          3. Determine which women who are not using a method fit our definition of having a need for contraception
##          4. Classify women as having a met demand for contraception (or an unmet demand)
##          5. Allow re-extractions of surveys under certain counterfactual scenarios (ex. if missing all info on fecundity)
####################################################################################################

## for reference: %ni% is opposite of %in%

## by default, study-level covariates are false unless otherwise specified
if ("cv_subgeo" %ni% names(df)) df[,cv_subgeo := 0]


####################################################################################################
# SURVEY-SPECIFIC FIXES
####################################################################################################

## MEX Family Life surveys have multiple gateways before current use
if("current_use" %in% names(df)) df[nid %in% c(58419,160781) & is.na(current_use),current_use := 0]

## RUS Longitudinal fule_path issue
df[nid == 11238,file_path := "FILEPATH"]

## 1 NGA Household survey and 2 RUS Longitudinal Surveys do not have marital status,
## have to assume they're all sexually active (thus under-estimates met demand)
df[nid %in% c(11238,115896,6793), c("curr_cohabit","former_cohabit") := list(1,0)]

## SDN survey only asked about ever use of women who had heard of any method
if ("never_used_contra" %in% names(df)) df[nid == 24143 & is.na(never_used_contra),never_used_contra := 1]

## IRL Sexual Health Survey
## youngest age is 18, decided to exclude that lowest age from analysis
## many gateways before asking about current use, all of which can be assumed to
## correspond to non-use
if (unique(df$nid) == 23295){
  df <- df[age_year >= 20]
  df[is.na(current_use),current_use := 0]
}

## DHS PNG 2006, RUS Longitudinal single coded as missing
if (all(c("curr_cohabit","former_cohabit") %in% names(df))){
  df[nid %in% c(44870,11160) & is.na(curr_cohabit),curr_cohabit := 0]
  df[nid %in% c(44870,11160) & is.na(former_cohabit),former_cohabit := 0]
}

## CDC YA RHS MOZ 2001 had double gateway before contraception so used never_used_contra for that
if ("never_used_contra" %in% names(df)) df[nid == 27519 & never_used_contra == 1, current_use := 1]
if (unique(df$nid) == 27519) df[,never_used_contra := NULL]

## CDC RHS BLZ 1991 included breastfeeding as a reason not to use contraception, but didn't include it as a method itself
if ("last_sex" %in% names(df)) df[nid == 27646 & last_sex == 5, c("current_use","current_contra") := list(1,"lactational amenorrhea")]
if (unique(df$nid) == 27646) df[,last_sex := NULL]

## IRL Sexual Health Survey didn't ask about pregnancy
df[nid == 23295, pregnant := 0]

## CDC RHS PRI 1995 had many rows with only basic info filled out
if (unique(df$nid) == 14486) df <- df[!is.na(pregnant)]

## CDC RHS CRI 1991 only listed pregnancy as an answer to why not using, so must assume all others are not pregnant
df[is.na(pregnant), pregnant := 0]

## CDC RHS CRI 1986 used current_use variable to identify sterilization and lactational amenorrhea was part of reason for not using
if ("current_use" %in% names(df)) df[nid == 27301 & current_use == 1 & current_contra == "", current_contra := "sterilization"]
if ("last_menses" %in% names(df)) df[nid == 27301 & last_menses == 7, c("current_use","current_contra") := list(1,"lactational amenorrhea")]
if (unique(df$nid) == 27301) df[,last_menses := NULL]

## KEN AIDS Indicator survey has gateway before marital status question and also didn't
## count infertility from sterilization as contraceptive use in some cases
if (all(c("curr_cohabit","no_menses") %in% names(df))){
  df[nid == 133304 & is.na(curr_cohabit),former_cohabit := 0]
  df[nid == 133304 & is.na(curr_cohabit),curr_cohabit := 0]
  df[nid == 133304 & no_menses == 1,current_contra := "female sterilization"]
  df[nid == 133304 & no_menses == 1,current_use := 1]
  if (unique(df$nid) == 133304) df[,no_menses := NULL]
}

## ARG MICS 2011 and LBR MIS 2016 have neither marital status nor sexual activity, so must assume everyone has a need
## (by artificially setting everyone as curr_cohabit = 1 even though that is not how it was sampled)
df[nid %in% c(137208,286768),curr_cohabit := 1]
df[nid %in% c(137208,286768),former_cohabit := 0]

## MWI GF Household survey seems to have asked everyone about using contraception (since huge
## amount of use among those whose answer to "are they using anything" is missing), so for now
## set current_use to 0 for all NAs (and then will be reset to 1 where appropriate below)
if ("current_use" %in% names(df)) df[nid == 26683 & is.na(current_use),current_use := 0]

## MNG MICS survey used lack of sexual activity as gateway for pregnancy and contraception questions
if ("never_used_contra" %in% names(df)) df[nid == 8777 & never_used_contra == 1, pregnant := 0]

## KEN WELFARE SSURVEY 1994 did not have pregnancy info, so must assume not pregnant
df[nid == 7439, pregnant := 0]

## KEN URBAN Survey
if (all(c("curr_cohabit","former_cohabit") %in% names(df))) df[nid == 165740 & is.na(curr_cohabit) & is.na(former_cohabit), c("curr_cohabit","former_cohabit") := 0]

## SRB MICS 2010 only identified those not using contraception, convert to current_use
if ("current_use" %in% names(df)) df[nid == 56153 & pregnant == 0 & is.na(current_use), current_use := 1]

## NGA LIVING STANDARDS SURVEY 2008-10 coded contraceptives using strings of numbers
df[nid == 151719 & current_contra == "..",  current_contra := "not using"]
df[nid == 151719 & current_contra == "01",  current_contra := "pill"]
df[nid == 151719 & current_contra == "02",  current_contra := "condom"]
df[nid == 151719 & current_contra == "03",  current_contra := "injection"]
df[nid == 151719 & current_contra == "04",  current_contra := "iud"]
df[nid == 151719 & current_contra == "05",  current_contra := "female sterilization"]
df[nid == 151719 & current_contra == "06",  current_contra := "male sterilization"]
df[nid == 151719 & current_contra == "07",  current_contra := "douche"]
df[nid == 151719 & current_contra == "08",  current_contra := "implant"]
df[nid == 151719 & current_contra == "09",  current_contra := "foam"]
df[nid == 151719 & current_contra == "10",  current_contra := "diaphragm"]
df[nid == 151719 & current_contra == "16",  current_contra := "other"]

## DHS AFG SP 2010 lists all contraceptive methods rather than just the major one. Rather than lose that information due to wacky coding, can parse it out
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("A","female sterilization ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("B","male sterilization ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("C","iud ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("D","injections ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("E","implants ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("F","pill ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("G","condom ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("H","lactational amenorrhea ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("I","rhythm ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("J","withdrawal ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("X","other modern method ### ",current_contra)]
df[grepl("afg_sp_dhs6",tolower(file_path)),  current_contra := gsub("Y","other traditional method ### ",current_contra)]


## IND_YOUTH_SITUATIONS_AND_NEEDS lists all contraceptive methods rather than just the major one. Rather than lose that information due to wacky coding, can parse it out
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  former_cohabit := 0]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("A","pill ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("B","iud/loop ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("C","injectables ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("D","implants ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("E","condom/nirodh ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("F","diaphragm ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("G","foam/jelly ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("H","rhythm ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("I","withdrawal ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("J","female condom ### ",current_contra)]
df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)),  current_contra := gsub("K","other method ### ",current_contra)]

if ("reason_no_contra" %in% names(df)){
  df[grepl("ind_youth_situation_and_needs_2006",tolower(file_path)) & grepl("J|K",reason_no_contra), current_contra := paste0("sterilization ### ",current_contra)]
  if (grepl("ind_youth_situation_and_needs_2006",df[,unique(tolower(file_path))])) df[,reason_no_contra := NULL]
}


## DHS UZB SP 2002 and DHS GHA SP 2007 also lists all contraceptive methods in a similar way
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("A","female sterilization ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("C","pill ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("D","iud ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("E","injections ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("F","implants ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("G","male condom ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("H","female condom ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("J","foam ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("K","lactational amenorrhea ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("L","rhythm ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("M","withdrawal ### ",current_contra)]
df[grepl("uzb_sp_dhs4|gha_sp_dhs5",tolower(file_path)),  current_contra := gsub("X","other ### ",current_contra)]

## TZA KAP removed the 19__ part from years
if ("first_cohabit_year" %in% names(df)) df[grepl("tza_kap|mar_sp_dhs3",tolower(file_path)),  first_cohabit_year := first_cohabit_year + 1900]
if ("last_birth_year" %in% names(df)) df[grepl("tza_kap",tolower(file_path)),  last_birth_year := last_birth_year + 1900]
if ("interview_year" %in% names(df)) df[grepl("tza_kap",tolower(file_path)),  interview_year := interview_year + 1900]

## PSE HEALTH SURVEY 2004
if ("desire_timing" %in% names(df)) df[grepl("pse_health_survey_2004",tolower(file_path)) & desire_timing == 993, desire_soon := 1]

## RHS surveys do not ask contraceptive questions of women who have never had sex, and leave current_use as NA for those women
## If we want to calculate unmet need (as a proportion of all women), would need these women in the denominator.
## To properly include them, have the codebook set up with both sex_in_last_month (binary) and last_sex (numeric),
## to differentiate missingness from lack of sexual activity altogether.
## Set current use of contraception to "no" if it is missing and sex_in_last_month is also missing (almost entirely due to gateway from never having had sex)
if (all(c("current_use","last_sex") %in% names(df))) df[grepl("_rhs_",tolower(file_path)) & is.na(current_use) & is.na(last_sex), current_use := 0]

## JAMAICA RHS (pre-2000)
## Had separate questions for maried and in union, and the first is the gateway into the second. To work with codebook, chose the
## second question and used missingness to determine who said yes to the first question
if ("curr_cohabit" %in% names(df)) df[grepl("jam_rhs_19",tolower(file_path)) & is.na(curr_cohabit), curr_cohabit := 1]
if (all(c("curr_cohabit","former_cohabit","current_use") %in% names(df))) {
  df[grepl("jam_rhs_1997",tolower(file_path)) & former_cohabit == 1 & curr_cohabit == 1, former_cohabit := 0]
  df[grepl("jam_rhs_1997",tolower(file_path)) & !is.na(curr_cohabit) & is.na(former_cohabit), former_cohabit := 0]
  df[grepl("jam_rhs_1997",tolower(file_path)) & is.na(current_use), current_use := 0]
}
if (all(c("curr_cohabit","former_cohabit","current_use","last_sex") %in% names(df))) {
  df[grepl("jam_rhs_2002",tolower(file_path)) & is.na(former_cohabit), curr_cohabit := 1]
  df[grepl("jam_rhs_2002",tolower(file_path)) & curr_cohabit == 1, former_cohabit := 0]
  df[grepl("jam_rhs_2002",tolower(file_path)) & is.na(last_sex), pregnant := 0]
}

## INDIA DLHS 1998-2004
## Only sampled married women, so did not even have a variable for marital status
df[nid %in% c(23183,23219),curr_cohabit := 1]
df[nid %in% c(23183,23219),former_cohabit := 0]

## INDIA DLHS 2007-2013
## substantial number of women not asked about pregnancy but were using contraception (including all sterilized women),
## so should follow normal assumptions and exclude when pregnancy status is missing
df[nid %in% c(23258,165390) & is.na(pregnant),pregnant := 0]

## PAPFAM DJI 2012 and MAR 2010
## Only sampled married women, so did not even have a variable for marital status
df[nid %in% c(126909,218035),curr_cohabit := 1]
df[nid %in% c(126909,218035),former_cohabit := 0]

## NPL DHS IN DEPTH
## Only sampled married women
df[grepl("npl_in_dhs1",tolower(file_path)),curr_cohabit := 1]
df[grepl("npl_in_dhs1",tolower(file_path)),former_cohabit := 0]

## Turkey DHS 2003
## Only sampled psu's and households whose ids were both even or both odd
if (grepl("tur_2003_2004",tolower(survey))) df <- df[(psu %% 2) == (hh_id %% 2)]

## BOLIVIA DHS4
## coded non-pregnant as missing instead of 0, so must assume all missings are not pregnant
df[grepl("bol_dhs4_2003_2004_wn",tolower(file_path)) & is.na(pregnant),  pregnant := 0]

## HONDURAS RHS 1996
## only asked about pregnancy for women who weren't using contraceptives, so must assume those who were using were not pregnant
df[grepl("hnd_rhs_1996_wn",tolower(file_path)) & is.na(pregnant), pregnant := 0]

## AUSTRALIA HEALTH AND RELATIONSHIPS
## only asked about pregnancy if they weren't using contraception, so must assume that anyone using contraception was not pregnant
df[grepl("studies_of_health_and_relationships_2001_2002",tolower(file_path)) & current_contra != "", pregnant := 0]

## AUSTRALIA USE AND EASE
## age was coded categorically in this survey
df[grepl("contraception_use_and_ease_1995/aus_",tolower(file_path)), age_year := (age_year*5) + 10]
df[grepl("contraception_use_and_ease_1995/aus_",tolower(file_path)) & age_year == 10, age_year := 15]

## USA NATIONAL SURVEY OF FAMILY GROWTH 1988 had an issue with missingness in pregnancy status
df[grepl("usa_nsfg_1988",tolower(file_path)) & is.na(pregnant), pregnant := 0]

## CAPE VERDE REPRODUCTIVE HEALTH SURVEY 1998
## gateway to asking about current pregnancy was first asking if woman had ever been pregnant
df[grepl("cpv_rhs_1998",tolower(file_path)) & is.na(pregnant), pregnant := 0]

## COSTA RICA REPRODUCTIVE HEALTH SURVEY 1993
## unique coding of desire for children
if ("reason_no_contra" %in% names(df)) df[grepl("cri_rhs_1993",tolower(file_path)) & grepl("quiere embarazo",tolower(reason_no_contra)), desire_soon := 1]

## current use sometimes created (but all NA) in this process
if ("current_use" %in% names(df)){
  if (length(unique(df$current_use)) == 1){
    if (is.na(unique(df$current_use))) df[,current_use := NULL]
  }
}

## same with curr_cohabit
if ("curr_cohabit" %in% names(df)){
  if (length(unique(df$curr_cohabit)) == 1){
    if (is.na(unique(df$curr_cohabit))) df[,curr_cohabit := NULL]
  }
}

####################################################################################################
# CALCULATE SEXUAL ACTIVITY AND TIMING OF DESIRE FOR CHILDREN
####################################################################################################

## For some surveys, time since last ___ was coded with two variables, a number and its units (ex. days, weeks, months), so have
## to calculate who falls into which category manually

## LAST SEXUAL ACTIVITY
if (all(c("last_sex","last_sex_unit") %in% names(df))) {

  ## Global Fund survey and KEN URBAM survey
  df[grepl("gf_household_survey|urban_reproductive_health_initiative",tolower(file_path)) & ((last_sex >= 100 & last_sex <= 131) | (last_sex >= 200 & last_sex <= 204) | last_sex == 300), sex_in_last_month := 1]

  ## SPECIAL DHS SURVEYS, PMA2020, MICS, KEN AIDS 2007, EDSA BOL 2016
  df[(nid %in% c(133219,323944) | grepl("uzb_sp_dhs4|gha_sp_dhs5|mwi_kap|tza_kap|sen_dhs4_1999|uga_in_dhs3|_pma_2020_|_mics",tolower(file_path))) &
       ((last_sex_unit == 1 & last_sex <= 30) | (last_sex_unit == 2 & last_sex <= 4) | (last_sex_unit == 3 & last_sex < 1)), sex_in_last_month := 1]
}

## TIMING FOR DESIRE OF FUTURE CHILDREN
if (all(c("desire_timing","desire_unit","desire_gate") %in% names(df))) {

  ## BOL EDSA 2016
  df[nid == 323944 & (desire_gate %in% c(2,4) | desire_timing %in% c(2,995,998)),desire_later := 1]

  ## IND DLHS
  df[nid %in% c(23183) & (desire_gate %in% c(2,3) | (desire_timing >= 24 & desire_timing < 96) | desire_timing == 98), desire_later := 1]
  df[nid %in% c(23219) & (desire_gate %in% c(2,3) | (desire_timing >= 24 & desire_timing < 99)), desire_later := 1]
  df[nid %in% c(23258) & (desire_gate %in% c(2,5) | (desire_timing >= 202 & desire_timing < 993) | desire_timing == 998), desire_later := 1]
  df[nid %in% c(165390) & (desire_gate %in% c(2,4) | (desire_timing >= 202 & desire_timing < 993) | desire_timing == 998), desire_later := 1]

  ## CDC RHS ALB 2002
  df[nid == 27321 & (desire_gate %in% c(2,4,5) | desire_timing %in% c(4,6,8)),desire_later := 1]

  ## CDC RHS CPV 1998, HND 2001
  df[nid %in% c(27511,27551) & (desire_gate %in% c(2,9) | (desire_timing >= 202 & desire_timing < 333) | desire_timing %in% c(444,999)),desire_later := 1]

  ## CDC RHS CRI 1993, PRY 1995
  df[nid %in% c(27638) & (desire_gate %in% c(2,8) | (desire_timing >= 202 & desire_timing < 994) | desire_timing %in% c(995,998)),desire_later := 1]
  df[nid %in% c(10364) & (desire_gate %in% c(2,9) | (desire_timing >= 202 & desire_timing < 994) | desire_timing %in% c(995,998)),desire_later := 1]

  ## CDC RHS PRY 1998
  df[nid %in% c(10326) & (desire_gate %in% c(2,9) | (desire_timing >= 202 & desire_timing < 990) | desire_timing %in% c(994,999)),desire_later := 1]

  ## CDC RHS PRY 2004-2008
  df[nid %in% c(10370,27525) & (desire_gate %in% c(3,9) | (desire_timing >= 202 & desire_timing < 888) | desire_timing %in% c(995,998)),desire_later := 1]

  ## CDC RHS ECU 1994-2004
  df[nid %in% c(27615,27621) & (desire_gate %in% c(2,9) | (desire_timing >= 202 & desire_timing < 994) | desire_timing %in% c(995,998)),desire_later := 1]
  df[nid %in% c(27630) & (desire_gate %in% c(2,9) | (desire_timing >= 202 & desire_timing < 994) | desire_timing %in% c(995,999)),desire_later := 1]

  ## CDC RHS GEO 1999-2005, MDA 1997
  df[nid %in% c(27486,27494,8750) & (desire_gate %in% c(2,4,8) | desire_timing %in% c(4,5,6,8)),desire_later := 1]

  ## CDC RHS GTM 2002
  df[nid == 27563 & (desire_gate %in% c(2,8) | (desire_timing >= 2 & desire_unit == 2) | desire_timing %in% c(94,95,98)),desire_later := 1]

  ## CDC RHS GTM 2008
  df[nid == 4779 & (desire_gate %in% c(2,8) | (desire_timing >= 202 & desire_timing < 993) | desire_timing %in% c(994,995,998)),desire_later := 1]

  ## CDC RHS UKR 1999
  df[nid == 13218 & (desire_gate %in% c(2,7) | desire_timing %in% c(4,5,6,7)),desire_later := 1]

  ## CDC RHS NIC 2006
  df[nid == 9270 & (desire_gate %in% c(2,9) | (desire_timing >= 202 & desire_timing < 555) | desire_timing %in% c(666,999)),desire_later := 1]

  ## CDC RHS PRI 1995
  df[nid == 14486 & (desire_gate %in% c(3,9) | desire_timing == 130 | desire_timing >= 202),desire_later := 1]

  ## CDC RHS CRI 1986
  df[nid == 27301 & (desire_gate == 1 | desire_timing %in% c(4,5,6,9)),desire_later := 1]

  ## KEN AIDS Indicator survey 2012
  df[nid == 133304 & (desire_gate %in% c(2,8) | desire_unit == 88 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_later := 1]

  ## KEN AIDS Indicator survey 2007
  df[nid == 133219 & (desire_gate %in% c(2,8) | desire_timing %in% c(95,98) | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_later := 1]

  ## BRA custom DHS
  df[nid == 141948 & (desire_gate %in% c(2,98) | desire_timing %in% c(3,5,98)), desire_later := 1]

  ## KEN URBAN
  df[grepl("urban_reproductive_health_initiative",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8 | (desire_timing >= 202 & desire_timing != 933)), desire_later := 1]

  ## PSE HEALTH SURVEYS
  df[grepl("pse_health_survey_2000",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8), desire_later := 1]
  df[grepl("pse_health_survey_2000",tolower(file_path)) & (desire_timing %% 2 == 0) & desire_timing >= 22 & desire_timing < 190, desire_later := 1]
  df[grepl("pse_health_survey_2004",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8 | desire_timing == 998 | desire_unit == 998 | (desire_unit >= 2 & desire_unit < 900)), desire_later := 1]

  ## SPECIAL DHS SURVEYS
  df[grepl("tza_kap|mar_sp_dhs3",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8 | desire_timing >= 96 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_later := 1]
  df[grepl("uga_in_dhs3",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8 | desire_timing >= 95 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_later := 1]
  df[grepl("npl_in_dhs1",tolower(file_path)) & (desire_gate == 2 | desire_gate == 3 | desire_timing ==2 | desire_timing ==3), desire_later := 1]

  ## PAPFAM
  df[grepl("papfam_2001",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8 | desire_timing >= 96 | (desire_unit == 2 & desire_timing >= 2)), desire_later := 1]
  df[grepl("dji_papfam_2012|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8 | desire_timing >= 96 | (desire_unit == 2 & desire_timing >= 2)), desire_later := 1]
  df[grepl("mar_papfam_2010",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8 | desire_timing >= 2), desire_later := 1]


  ## ALL PMA2020 SURVEYS
  df[grepl("_pma2020_",tolower(file_path)) & (desire_gate == 2 | desire_gate == -88 | desire_unit == 5 | desire_unit == -88 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_later := 1]

  ## ALL MICS SURVEYS (THAT HAVE DESIRE OF FUTURE CHILDREN INFORMATION)
  df[grepl("_mics",tolower(file_path)) & desire_timing >= 99, desire_timing := NA]
  df[grepl("_mics",tolower(file_path)) & (desire_gate == 2 | desire_gate == 8 | desire_timing >= 95 | (desire_unit == 1 & desire_timing >= 24) | (desire_unit == 2 & desire_timing >= 2)), desire_later := 1]
}

####################################################################################################
# LAST MENSTRUATION (CONTINUOUS IN MONTHS)
####################################################################################################

## generate continuous estimate (in months) of time since last menstruation
if (all(c("last_menses","last_menses_unit") %in% names(df))) {

  ## CDC RHS PRI 1995 & SLV 1988
  df[nid %in% c(14486,27572) & last_menses < 94, months_since_last_menses := last_menses]
  df[nid %in% c(14486,27572) & last_menses == 96, months_since_last_menses := 999]

  ## BRA custom DHS, BOL EDSA 2016
  df[nid %in% c(141948,323944) & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[nid %in% c(141948,323944) & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[nid %in% c(141948,323944) & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[nid %in% c(141948,323944) & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[nid == 141948 & last_menses == 95, months_since_last_menses := 999]
  df[nid == 323944 & last_menses == 995, months_since_last_menses := 999]

  ## BOL EDSA 2016 also put date of last menses. Using variable for that year as last_birth_year and month
  ## as crude way to approximate this
  if (all(c("last_birth_year","last_birth_month","interview_year","interview_month") %in% names(df))){
    df[nid == 323944, months_since_last_menses := interview_month - last_birth_month + ((interview_year - last_birth_year)*12)]
    if (unique(df$nid) == 323944) df[,c("last_birth_year","last_birth_month") := NULL]
  }

  ## ALL PMA2020 SURVEYS
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 1 & last_menses > 99, months_since_last_menses := 1/30]
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 2 & last_menses > 99, months_since_last_menses := 1/4.3]
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 3 & last_menses > 99, months_since_last_menses := 1]
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 4 & last_menses > 99, months_since_last_menses := 1*12]
  df[grepl("_pma2020_",tolower(file_path)) & last_menses_unit == 6, months_since_last_menses := 999]

  ## MICS SURVEYS that have info on last menstruation
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 1 & last_menses == 99, months_since_last_menses := 1/30]
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 2 & last_menses == 99, months_since_last_menses := 1/4.3]
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 3 & last_menses == 99, months_since_last_menses := 1]
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 4 & last_menses == 99, months_since_last_menses := 1*12]
  df[grepl("_mics",tolower(file_path)) & last_menses_unit == 9 & last_menses == 95, months_since_last_menses := 999]

  ## Some special DHS / KAP surveys
  df[grepl("khm_sp_dhs3|gha_sp_dhs5|tza_kap|mar_sp_dhs3",tolower(file_path)) & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[grepl("khm_sp_dhs3|gha_sp_dhs5|tza_kap|mar_sp_dhs3",tolower(file_path)) & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[grepl("khm_sp_dhs3|gha_sp_dhs5|tza_kap|mar_sp_dhs3",tolower(file_path)) & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[grepl("khm_sp_dhs3|gha_sp_dhs5|tza_kap|mar_sp_dhs3",tolower(file_path)) & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[grepl("khm_sp_dhs3|gha_sp_dhs5|tza_kap",tolower(file_path)) & last_menses_unit == 9 & last_menses == 95, months_since_last_menses := 999]
  df[grepl("mar_sp_dhs3",tolower(file_path)) & last_menses_unit == 9 & last_menses == 94, months_since_last_menses := 999]

  ## ARAB LEAGUE PAPCHILD/PAPFAM
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses_unit == 1, months_since_last_menses := last_menses/30]
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses_unit == 1 & last_menses == 98, months_since_last_menses := 1/30]
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses_unit == 2, months_since_last_menses := last_menses/4.3]
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses_unit == 2 & last_menses == 98, months_since_last_menses := 1/4.3]
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses_unit == 3, months_since_last_menses := last_menses]
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses_unit == 3 & last_menses == 98, months_since_last_menses := 1]
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses_unit == 4, months_since_last_menses := last_menses*12]
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses_unit == 4 & last_menses == 98, months_since_last_menses := 1*12]
  df[grepl("papchild_1990_1997|pse_papfam_2006|syr_papfam_2009",tolower(file_path)) & last_menses == 95, months_since_last_menses := 999]

  ## Fixes for no_menses in some PAPFAMs
  if ("interview_year" %in% names(df)) df[grepl("mar_papfam_2010",tolower(file_path)) & last_menses_unit == 2 & (interview_year - last_menses) > 2, no_menses := 1]
  df[grepl("all_countries_papfam_2001",tolower(file_path)) & last_menses %in% c(4,5), no_menses := 1]

} else if ("last_menses" %in% names(df)) {
  ## At the moment only refers to DHS surveys
  df[last_menses >= 100 & last_menses <= 190,months_since_last_menses := (last_menses-100)/30]
  df[last_menses == 199,months_since_last_menses := 1/30]
  df[last_menses >= 200 & last_menses <= 290,months_since_last_menses := (last_menses-200)/4.3]
  df[last_menses == 299,months_since_last_menses := 1/4.3]
  df[last_menses >= 300 & last_menses <= 390,months_since_last_menses := last_menses - 300]
  df[last_menses == 399,months_since_last_menses := 1]
  df[last_menses >= 400 & last_menses <= 490,months_since_last_menses := (last_menses-400)*12]
  df[last_menses == 499,months_since_last_menses := 1*12]

  ## amenorrheic women
  df[grepl("dhs_prog|macro_dhs|urban_reproductive_health_initiative",tolower(file_path)) & last_menses == 995,months_since_last_menses := 999]
  df[grepl("district_level_household_survey",tolower(file_path)) & last_menses == 994,months_since_last_menses := 999]
  df[nid == 4779 & last_menses == 995,months_since_last_menses := 999]
}

####################################################################################################
# TIMING OF LAST BIRTH
####################################################################################################

## calculate time since most recent birth (done very differently depending on info available in each survey)
if (all(c("last_birth_year","last_birth_month","interview_year","interview_month") %in% names(df))) {
  ## MICS: determine months since most recent birth
  df[last_birth_month > 12,last_birth_month := 6]
  df[interview_month > 12,interview_month := 6]
  df[last_birth_year > 9990, last_birth_year := NA]
  df[interview_year > 9990, interview_year := NA]
  df[last_birth_month > interview_month, months_since_last_birth := ((interview_year-last_birth_year-1)*12) + (interview_month + 12 - last_birth_month)]
  df[interview_month >= last_birth_month, months_since_last_birth := ((interview_year-last_birth_year)*12) + (interview_month - last_birth_month)]
}

if (all(c("last_birth_date","interview_date") %in% names(df)) & grepl("_pma2020_",tolower(df[1,file_path]))) {
  ## PMA2020: determine months since most recent birth (mostly just parsing dates)
  ## Lots of variation in way it is coded across PMA surveys
  df[last_birth_date == ".", last_birth_date := NA]
  df[interview_date == ".", interview_date := NA]
  if (any(grepl("-",df$interview_date))) {
    df[,year_birth := str_sub(last_birth_date,1,4) %>% as.numeric]
    df[,year_interview := str_sub(interview_date,1,4) %>% as.numeric]
    df[,month_birth := str_sub(interview_date,6,7) %>% as.numeric]
    df[,month_interview := str_sub(interview_date,6,7) %>% as.numeric]
  } else if (any(grepl(",",df$interview_date))) {
    df[,year_birth := str_sub(last_birth_date,-4) %>% as.numeric]
    df[,year_interview := sapply(1:nrow(df),function(x) str_sub(strsplit(df$interview_date[x],", ")[[1]][2],1,4)) %>% as.numeric]
    df[tolower(str_sub(last_birth_date,1,3)) == "jan", month_birth := 1]
    df[tolower(str_sub(last_birth_date,1,3)) == "feb", month_birth := 2]
    df[tolower(str_sub(last_birth_date,1,3)) == "mar", month_birth := 3]
    df[tolower(str_sub(last_birth_date,1,3)) == "apr", month_birth := 4]
    df[tolower(str_sub(last_birth_date,1,3)) == "may", month_birth := 5]
    df[tolower(str_sub(last_birth_date,1,3)) == "jun", month_birth := 6]
    df[tolower(str_sub(last_birth_date,1,3)) == "jul", month_birth := 7]
    df[tolower(str_sub(last_birth_date,1,3)) == "aug", month_birth := 8]
    df[tolower(str_sub(last_birth_date,1,3)) == "sep", month_birth := 9]
    df[tolower(str_sub(last_birth_date,1,3)) == "oct", month_birth := 10]
    df[tolower(str_sub(last_birth_date,1,3)) == "nov", month_birth := 11]
    df[tolower(str_sub(last_birth_date,1,3)) == "dec", month_birth := 12]
    df[tolower(str_sub(interview_date,1,3)) == "jan", month_interview := 1]
    df[tolower(str_sub(interview_date,1,3)) == "feb", month_interview := 2]
    df[tolower(str_sub(interview_date,1,3)) == "mar", month_interview := 3]
    df[tolower(str_sub(interview_date,1,3)) == "apr", month_interview := 4]
    df[tolower(str_sub(interview_date,1,3)) == "may", month_interview := 5]
    df[tolower(str_sub(interview_date,1,3)) == "jun", month_interview := 6]
    df[tolower(str_sub(interview_date,1,3)) == "jul", month_interview := 7]
    df[tolower(str_sub(interview_date,1,3)) == "aug", month_interview := 8]
    df[tolower(str_sub(interview_date,1,3)) == "sep", month_interview := 9]
    df[tolower(str_sub(interview_date,1,3)) == "oct", month_interview := 10]
    df[tolower(str_sub(interview_date,1,3)) == "nov", month_interview := 11]
    df[tolower(str_sub(interview_date,1,3)) == "dec", month_interview := 12]
  } else {
    df[,year_birth := str_sub(last_birth_date,-4) %>% as.numeric]
    df[,year_interview := str_sub(interview_date,-4) %>% as.numeric]
    df[tolower(str_sub(last_birth_date,3,5)) == "jan", month_birth := 1]
    df[tolower(str_sub(last_birth_date,3,5)) == "feb", month_birth := 2]
    df[tolower(str_sub(last_birth_date,3,5)) == "mar", month_birth := 3]
    df[tolower(str_sub(last_birth_date,3,5)) == "apr", month_birth := 4]
    df[tolower(str_sub(last_birth_date,3,5)) == "may", month_birth := 5]
    df[tolower(str_sub(last_birth_date,3,5)) == "jun", month_birth := 6]
    df[tolower(str_sub(last_birth_date,3,5)) == "jul", month_birth := 7]
    df[tolower(str_sub(last_birth_date,3,5)) == "aug", month_birth := 8]
    df[tolower(str_sub(last_birth_date,3,5)) == "sep", month_birth := 9]
    df[tolower(str_sub(last_birth_date,3,5)) == "oct", month_birth := 10]
    df[tolower(str_sub(last_birth_date,3,5)) == "nov", month_birth := 11]
    df[tolower(str_sub(last_birth_date,3,5)) == "dec", month_birth := 12]
    df[tolower(str_sub(interview_date,3,5)) == "jan", month_interview := 1]
    df[tolower(str_sub(interview_date,3,5)) == "feb", month_interview := 2]
    df[tolower(str_sub(interview_date,3,5)) == "mar", month_interview := 3]
    df[tolower(str_sub(interview_date,3,5)) == "apr", month_interview := 4]
    df[tolower(str_sub(interview_date,3,5)) == "may", month_interview := 5]
    df[tolower(str_sub(interview_date,3,5)) == "jun", month_interview := 6]
    df[tolower(str_sub(interview_date,3,5)) == "jul", month_interview := 7]
    df[tolower(str_sub(interview_date,3,5)) == "aug", month_interview := 8]
    df[tolower(str_sub(interview_date,3,5)) == "sep", month_interview := 9]
    df[tolower(str_sub(interview_date,3,5)) == "oct", month_interview := 10]
    df[tolower(str_sub(interview_date,3,5)) == "nov", month_interview := 11]
    df[tolower(str_sub(interview_date,3,5)) == "dec", month_interview := 12]
  }

  df[month_birth > month_interview, months_since_last_birth := ((year_interview-year_birth-1)*12) + (month_interview + 12 - month_birth)]
  df[month_interview >= month_birth, months_since_last_birth := ((year_interview-year_birth)*12) + (month_interview - month_birth)]
}

## determine who is postpartum amenorrheic (regardless of how long they have been amenorrheic)
if (all(c("months_since_last_menses","months_since_last_birth") %in% names(df))) df[pregnant == 0 & months_since_last_menses >= months_since_last_birth,ppa := 1]
if ("reason_no_contra" %in% names(df)) df[pregnant == 0 & grepl("amenorr|no menses since last birth",reason_no_contra),ppa := 1]

## Women without their period for 6 or more months are considered infecund as long as they are not postpartum amenorrheic
## If they are postpartum amenorrheic, must have been 5 years or more since last child was born to be considered infecund
## When missing_fecund or no_pregppa, not possible to calculate this
if (counterfac_no_pregppa == 0 & counterfac_missing_fecund == 0 & all(c("ppa","months_since_last_menses") %in% names(df))) {
  df[is.na(ppa) & months_since_last_menses >= 6, no_menses := 1]
  if ("months_since_last_birth" %in% names(df)) df[ppa == 1 & months_since_last_birth >= 60, no_menses := 1]
} else if ("months_since_last_menses" %in% names(df)) {
  df[months_since_last_menses >= 6, no_menses := 1]
} else if ("ppa" %in% names(df)){
  df[ppa == 1, no_menses := 1]
}


####################################################################################################
# MODERN CONTRACEPTIVE USAGE
####################################################################################################

## Remove all men and those women that are outside of ages 15-49
## NOTE that in many of the surveys with missingness in sex or age within the women's modules, those individuals did not answer
## any of the relevant questions for contraception, and are thus rightfully excluded from the sample. Still, it is helpful while extracting
## to see if any surveys are unexpectedly having large numbers of observations dropped
if (nrow(df[is.na(sex_id)]) > 0) print(paste0(survey," HAD ",nrow(df[is.na(sex_id)]), " MISSING SEX!!!!!!!!!!!!!!!!!!!"))
if (nrow(df[is.na(age_year)]) > 0) print(paste0(survey," HAD ",nrow(df[is.na(age_year)]), " MISSING AGE!!!!!!!!!!!!!!!!!!!"))
df <- df[sex_id == 2 & age_year >= 15 & age_year < 50]


## Most surveys use the answer to whether or not a woman is pregnant as a gateway for asking about contraceptive usage. The codebook is set up such that
## any answer that passes through the gateway is coded as 0 (since it is always "no" or "not sure"), and those that specifically say they are pregnant are 1.
## Therefore, any missingness in pregnant variable correspond to individuals who were not asked about contraception, and should be dropped
## However, some surveys only bothered to fill out the variable when pregnant == 1, leaving everything else NA. In these scenarios, must assume that everyone with NA was not pregnant
if (nrow(df[pregnant == 0]) == 0) df[is.na(pregnant), pregnant := 0]
df <- df[!is.na(pregnant)]

## clarify missingness that arises when question about former cohabitation is separate and asked to a subset of those who said "no" to
## current cohabitation
if (all(c("curr_cohabit","former_cohabit") %in% names(df))) {
  df[is.na(former_cohabit) & !is.na(curr_cohabit), former_cohabit := 0]
  df[is.na(curr_cohabit) & !is.na(former_cohabit), curr_cohabit := 0]
}

## If survey is specific to married or ever-married women, drop observations that do not fall under that category. This controls for denominator issues
## for indicators that are proportions of all women, since some currmar_only/evermar_only surveys still sample unmarried women
## (without asking about contraception) and others don't sample them at all. This allows one crosswalk to be used for both
if (counterfac_currmar == 1) {
  ## Counterfactual gateway if re-extracting as if survey only sampled married women
  if("curr_cohabit" %in% names(df)) df <- df[curr_cohabit == 1]

} else if (counterfac_evermar == 1) {
  ## Counterfactual gateway if re-extracting as if survey only sampled ever-married women
  if (all(c("curr_cohabit","former_cohabit") %in% names(df))) df <- df[curr_cohabit == 1 | former_cohabit == 1]

} else if (all(c(counterfac_currmar,counterfac_evermar,counterfac_missing_fecund,counterfac_missing_desire,counterfac_missing_desire_later,counterfac_no_pregppa) == 0)) {
  ## In regular extraction, restrict sample according to sampling restrictions of the survey itself
  if ("curr_cohabit" %in% names(df) & unique(df$currmar_only) == 1) df <- df[curr_cohabit == 1]
  if (all(c("curr_cohabit","former_cohabit") %in% names(df)) & unique(df$evermar_only) == 1) df <- df[curr_cohabit == 1 | former_cohabit == 1]

}

## Mark modern contraceptive usage as 1 if current method contains strings pertaining to modern contraceptives
## Strings should cover all modern answers in any of the survey languages, and should
## not be ambiguous enough to apply to any methods that are not modern
methods <- paste(c("modern", #anything specified as modern (usually "other modern")
                   "pill", "pilul", "pildora", "pastilla", "orales", #the pill
                   "condom", "condon", "preservati", "camasinha", #condoms
                   "diaphragm", "diafragma", "diaphram", "cervical cap", "cones", "fem sci", "sci fem", "scien fem", "other female", "oth fsci", #diaphragms
                   "tablet", "foam", "jelly", "jalea", "mousse", "espuma", "creme", "cream", "gelee", "spermicid", "eponge", "esponja", "esonja", "sponge", "vaginale", "comprimidos vaginais", "vaginal method", #spermicides and sponges
                   "insert", "mplant", "nplant", "rplant", "transplant", #implants
                   "injec", "inyec", "sayana", "piqure", #injections
                   "iud", "diu", "spiral", "sterilet", "coil", "iucd", "i.u.d.", "loop", "ota ring", "copper t", "copper 7", #iuds
                   "sterilization", "sterilisation", "sterilized", "ester", "esterizacao", "vasectom", "ligation", "ligadura", "ligature", 'ester\\.', "est\\.", "m ster", "male ster", "male-ster", "other meths inc ster", #male or female sterilization
                   "patch", "parche anticonceptivo", #contraceptive patch
                   "contraceptive ring", #contraceptive ring
                   "emergency", "morning-after"), #emergency contraceptives
                 collapse = "|")
df[grepl(methods,tolower(current_contra)),mod_contra := 1]


####################################################################################################
# TRADITIONAL CONTRACEPTIVES AND SEPARATING MISSINGNESS FROM LACK OF USE
####################################################################################################

## Have to set 0s and NAs correctly for the different indicators so that, when collapsed, the right denominator is used (NAs are excluded in collapse code)

## While DHS surveys seem to have asked all women about contraceptive usage (or at least fill in assumed answers to the variable),
## other surveys only ask this to certain women, making missingess in current_contra
## ambiguous with respect to whether a woman is not using a contraceptive or was
## just never asked/didn't answer. This distinction is important for determining the appropriate
## denominator for the proportion we are calculating

## Some surveys have a variable corresponding to the gateway question "Are you currently
## using any method?" which helps clarify the issue, but others do not.

## If there is no variable corresponding to the question of whether a woman is
## using a method at all, then we assume that all non-pregnant women were asked
## about their contraceptive usage. This is necessary because of surveys like the early MICS
## surveys, which code non-use as missing but do not have a gateway variable.
## Therefore, missing answers for the variable in such surveys
## are taken to mean that she is not using a method

## NOTE: DHS surveys often have a current_use variable, but it is redundant
## because DHS codes non-use as an explicit answer in every survey encountered thus far,
## so it is usually not filled out in the codebook (which is fine)

## create string patterns found in current_contra that correspond to non-use of contraception, and also string patterns for
## answers that don't actually tell us if or what contraceptive is being used
nonuse <- paste(c("none",
                  "not using",
                  "no method",
                  "not current user",
                  "not currently using",
                  "no contraceptive",
                  "no usa",
                  "no esta usa",
                  "no estan usa",
                  "nonuser",
                  "non-user",
                  "not expose",
                  "non expose",
                  "n expose",
                  "not user",
                  "no current use",
                  "n'utilise"),
                collapse = "|")

unknown <- paste(c("refused",
                   "don't know",
                   "dont know",
                   "not stated",
                   "no respon",
                   "non repons",
                   "-99"),
                 collapse = "|")

## When current_use variable is missing, must assume everyone was asked about contraception
if ("current_use" %ni% names(df)) {

  ## unless specifically marked as missing/unknown, assume women were asked and should therefore be in denominator
  df[!grepl(unknown,tolower(current_contra)) & is.na(mod_contra), mod_contra := 0]
  df[!grepl(unknown,tolower(current_contra)), trad_contra := 0]

  ## If using a method but it is not modern, mark it as traditional
  df[mod_contra == 0 & current_contra != "" & !grepl(nonuse,tolower(current_contra)),trad_contra := 1]

} else {
  ## Survey does have a gateway variable corresponding to current use

  ## some surveys still leave current usage as missing if women already answered that she had never used a method
  if ("never_used_contra" %in% names(df)) df[never_used_contra == 1, current_use := 0]

  ## Some surveys do not ask about current usage if the woman was sterilized, even if it was for family planning purposes,
  ## so current_contra may say "sterilized" when current_use = 0.
  ## This fixes that issue by saying any actual methods provided in current_contra mean that they are currently using
  df[current_contra != "" & !grepl(nonuse,tolower(current_contra)) & !grepl(unknown,tolower(current_contra)),current_use := 1]

  ## any women who gave an answer regarding current usage should be in the denominator
  df[!is.na(current_use) & is.na(mod_contra),mod_contra := 0]
  df[!is.na(current_use),trad_contra := 0]

  ## if a woman is using a method and it is not modern, mark it as traditional
  df[current_use == 1 & mod_contra == 0,trad_contra := 1]
}

## Want to keep pregnant women in data set to be included in all-women denominator, but most surveys don't actually ask them
## about contraception, so must assume the answer
df[pregnant == 1 & is.na(mod_contra), mod_contra := 0]
df[pregnant == 1 & is.na(trad_contra), trad_contra := 0]

## If women are using any method, mark that
df[!is.na(mod_contra),any_contra := 0]
df[trad_contra == 1 | mod_contra == 1, any_contra := 1]


####################################################################################################
# NEED FOR CONTRACEPTION
####################################################################################################

## Women who have have a need for contraceptives are women who:
## 1. have had sex in the last 30 days or are married/in union
## 2. said that they do not want a child in the next 2 years
## 3. have not hit menopause, had a hysterectomy, or are otherwise known to be
##    infecund (including having never menstruated or not having menstruated
##    in at least 6 months if not postpartum amenorrheic, or having been postpartum
##    amenorrheic for 5 or more years)
## 4. have not been continuously married/living with a man for 5 years without
##    having a child despite answering that they have never used any method of
##    contraception.
## 5. For pregnant womena and women who are postpartum amenorrheic from a birth in
##    the last 2 years, need is determined separately based on whether they wanted
##    to space or limit their current/most recent pregnancy

## Since many surveys are missing one or more of these variables, have to check
## that the codebook is filled out for a variable before using it, otherwise you
## will get an error. Some variables are used as strings below based on their labels
## even though they are technically coded as numbers. When such variables exist in the dataset
## but are entirely missing, an error will be thrown when they are treated as strings.
## Therefore you first have to check if there are any nonmissing values for those vars

## first, assume no need for contraception
df[,need_contra := 0]

## The only women who are eligible for having a need for contraception are those that are married/in-union or
## have been sexually active in the last month, and have expressed that they do not want children in the next 2 years
## For some surveys, the period for when they wanted children differed by a small amount, but in others women were only asked
## about the desire for a child right now (the desire_soon variable. Is crosswalked later). In others, no question about
## desire for children is asked at all, and all married/in-union/sexually active women are assumed to have a need (also crosswalked later)

if (counterfac_missing_desire == 1) {
  ## Counterfactual gateway if re-extracting as if survey had no information regarding desire for children
  df[curr_cohabit == 1,need_contra := 1]
  if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1, need_contra := 1]

} else if (counterfac_missing_desire_later == 1) {
  ## Counterfactual gateway if re-extracting as if survey only had information regarding desire for children right now
  df[curr_cohabit == 1 & (is.na(desire_soon) | desire_soon == 0), need_contra := 1]
  if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1 & (is.na(desire_soon) | desire_soon == 0), need_contra := 1]

} else { ## normal extraction
  if ("desire_later" %in% names(df)) {
    ## Survey has information on women's desire for a child in the next 2 years
    df[curr_cohabit == 1 & desire_later == 1, need_contra := 1]
    if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1 & desire_later == 1, need_contra := 1]

  } else if ("desire_soon" %in% names(df)) {
    ## Survey only has info regarding desire for a child right now. Assume that anyone who is married/sexually active
    ## and did not answer that they want a child right now has a need
    df[curr_cohabit == 1 & (is.na(desire_soon) | desire_soon == 0), need_contra := 1]
    if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1 & (is.na(desire_soon) | desire_soon == 0), need_contra := 1]

  } else {
    ## No info regarding desire for children, assume that everyone who's married/sexually active has a need
    if ("curr_cohabit" %in% names(df)) df[curr_cohabit == 1,need_contra := 1]
    if ("sex_in_last_month" %in% names(df)) df[sex_in_last_month == 1, need_contra := 1]
    ## A Cuba survey has neither sexual activity nor marital status, and we thus assume that everyone has a need
    if (all(c("curr_cohabit","sex_in_last_month") %ni% names(df))) df[,need_contra := 1]
  }
}

## Now we exclude women from having a need for contraception if they are pregnant, postpartum amenorrheic, have expressed that
## they are infertile, or can be assumed to be infertile based on their recent menstruation or lack of children
## after 5 years of marriage with no contraceptive usage
df[pregnant == 1, need_contra := 0]

## If re-extracting as if survey had no information regarding fecundity, skip these
if (counterfac_missing_fecund == 0) {
  ## Indicated lack of menstruation (in any way)
  if ("no_menses" %in% names(df)) df[no_menses == 1, need_contra := 0]

  ## Indicated inability to have a child
  if ("desire_children_infertile" %in% names(df)) df[desire_children_infertile == 1, need_contra := 0]

  ## Indicated lack of menstruation or inability to have a child as reason why she was not using a contraceptive method
  infecund <- paste(c("infecund",
                      "menopaus",
                      "hyst",
                      "histerect",
                      "never mens",
                      "menstrua",
                      "cannot have children",
                      "cant become pregnant",
                      "no puede quedar emb",
                      "infertil",
                      "nao pode",
                      "impossible to have"),
                    collapse="|")
  if ("reason_no_contra" %in% names(df)) df[grepl(infecund,tolower(reason_no_contra)), need_contra := 0]

  ## Exclude others from need based on assumed infertility after 5 years without a child. Relevant variables
  ## coded differently depending on the survey series)
  if (all(c("curr_cohabit","never_used_contra") %in% names(df))) {
    ## DHS surveys
    ## can only deduce criteria in DHS among women who are still in their first marriage/union
    if (all(c("in_first_cohabit","years_since_first_cohabit","months_since_last_birth") %in% names(df))) {
      df[curr_cohabit == 1 & ##currently married/in union
           never_used_contra == 1 & ##never used contraception
           in_first_cohabit == 1 & ##still in first marriage/union
           !is.na(years_since_first_cohabit) & ##make sure years fo marriage is known
           years_since_first_cohabit > 4 & ##married for at least 5 years
           (months_since_last_birth >= 60 | ##5 years since last birth or never given birth
              is.na(months_since_last_birth)),
         need_contra := 0]
    }

    ## MICS surveys and some special DHS
    ## similar to normal DHS, but uses different variables
    if (all(c("in_first_cohabit","first_cohabit_year","last_birth_year","interview_year") %in% names(df))) {
      df[curr_cohabit == 1 & ##currently married/in union
           never_used_contra == 1 & ##never used contraception
           in_first_cohabit == 1 & ##still in first marriage/union
           interview_year < 9990 & ##make sure interview year is known
           interview_year - first_cohabit_year > 5 & ##been together for 5 years
           (interview_year - last_birth_year > 5 | ##5 years since last birth or never given birth
              is.na(last_birth_year)),
         need_contra := 0]
    }

    ## PMA 2020 surveys and a few other random surveys
    ## actually asks about most recent marriage/union, so can deduce for all women
    if (all(c("recent_cohabit_start_date","interview_date","last_birth_date,year_interview") %in% names(df))) {
      if (class(df$recent_cohabit_start_date) == "character") {
        ##parse dates for the years
        df[recent_cohabit_start_date == ".",recent_cohabit_start_date := NA]
        df[,year_married := str_sub(recent_cohabit_start_date,-4) %>% as.numeric]
        df[curr_cohabit == 1 & ##currently married/in union
             never_used_contra == 1 & ##never used contraception
             !is.na(year_interview) & ##make sure interview year is known
             year_interview - year_married > 5 & ##been together for 5 years
             (year_interview - year_birth > 5 | ##5 years since last birth or never given birth
                is.na(year_birth)),
           need_contra := 0]
      } else { ##assume CMC format
        df[curr_cohabit == 1 & ##currently married/in union
             never_used_contra == 1 & ##never used contraception
             !is.na(interview_date) & ##make sure interview year is known
             interview_date - recent_cohabit_start_date > 60 & ##been together for 5 years
             (interview_date - last_birth_date > 60 | ##5 years since last birth or never given birth
                is.na(last_birth_date)),
           need_contra := 0]
      }
    }
  }
}


####################################################################################################
# PREGNANT AND POST-PARTUM AMENORRHEIC WOMEN
####################################################################################################

## Pregnant and postpartum amenorrheic women  from a birth in the last 2 years can still contribute
## to unmet demand for contraception if they indicate that they wanted to space or limit their
## current/most recent pregnancy

## If re-extracting as if survey had no information regarding fecundity or postpartum amenorrhea, skip this
if (counterfac_no_pregppa == 0 & counterfac_missing_fecund == 0) {
  ## Pregnant women who had a need
  if ("preg_not_wanted" %in% names(df)) df[pregnant == 1 & preg_not_wanted == 1,need_contra := 1]

  ## Determine which postpartum amenorrheic women gave birth within the last 2 years
  if (all(c("ppa_not_wanted","ppa","months_since_last_birth") %in% names(df))) df[ppa == 1 & months_since_last_birth <= 24,ppa24 := 1]
  if (all(c("ppa_not_wanted","ppa","birth_in_last_two_years") %in% names(df))) df[ppa == 1 & birth_in_last_two_years == 1,ppa24 := 1]

  ## Designate which postpartum amenorrheic women had a need, and adjust the all-woman contraception denominator to match (in case
  ## postpartum women were not asked about contraception, similar to pregnant women)
  if ("ppa24" %in% names(df)) {
    df[ppa24 == 1 & is.na(mod_contra),mod_contra := 0]
    df[ppa24 == 1 & is.na(trad_contra),trad_contra := 0]
    df[ppa24 == 1 & is.na(any_contra),any_contra := 0]
    df[ppa24 == 1 & ppa_not_wanted == 1,need_contra := 1]
    df[ppa24 == 1 & (is.na(ppa_not_wanted) | ppa_not_wanted == 0),need_contra := 0]
  }
}


####################################################################################################
# NEED FOR CONTRACEPTION MET WITH MODERN METHODS
####################################################################################################

## Restrict need_contra to those observations where we know women were actually asked about contraception,
## making it consistent with the denominator of modern contraceptive usage (since otherwise it's impossible
## to know whether that need was met or not)
df[is.na(mod_contra), need_contra := NA]

## regardless of answers to any other questions, if a woman is currently using any contraceptive method then
## she is considered to have a need for contraception
df[any_contra == 1, need_contra := 1]

## Of those with a need for contraception, those using a modern method have a met demand, while those that
## aren't have an unmet demand
df[need_contra == 1 & mod_contra == 0, met_need_demanded := 0]
df[mod_contra == 1, met_need_demanded := 1]
