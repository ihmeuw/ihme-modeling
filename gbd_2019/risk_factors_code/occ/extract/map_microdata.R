############################################################################################################
## Purpose: Map occupation, industry, and HRH categories onto microdata (obtained from ubcov extractions)
###########################################################################################################

## clear memory
rm(list=ls())

## libraries
pacman::p_load(data.table,magrittr,ggplot2,parallel,haven,stringr)

## settings
prompt <- F ## Turn to False if you want it to run continuously without prompting user for input

## in/out
coding.system.root <- "FILEPATH"
dir.root <- "FILEPATH"
in.dir <- file.path(dir.root,"new")
out.dir <- "FILEPATH"

in.files <- list.files(in.dir)

## Load in ISCO coding systems and versions
isco_58 <- fread(file.path(coding.system.root,"ISCO_MAJOR_SUB_MAJOR_58.csv"),colClasses = "string")
  isco_58[occ_major != "Y",occ_sub_major := str_pad(occ_sub_major,2,side="left",pad=0)]
isco_68 <- fread(file.path(coding.system.root,"ISCO_MAJOR_SUB_MAJOR_68.csv"),colClasses = "string")
  isco_68[occ_major != "Y",occ_sub_major := str_pad(occ_sub_major,2,side="left",pad=0)]
isco_88 <- fread(file.path(coding.system.root,"ISCO_MAJOR_SUB_MAJOR_88.csv"),colClasses = "string")
  isco_88[,occ_sub_major := str_pad(occ_sub_major,2,side="left",pad=0)]
isco_08 <- fread(file.path(coding.system.root,"ISCO_MAJOR_SUB_MAJOR_08.csv"),colClasses = "string")
  isco_08[,occ_sub_major := str_pad(occ_sub_major,2,side="left",pad=0)]
## NOTE: ISCO sub-mdfajors include ambiguous sub-majors ending in 9 because these are common, even
## though they are not strictly part of the system. Excluding them would result in a bias in the
## version-matching function that would skew towards matching to ISCO-68

## Load in Industry coding systems abd versons
ipums <- fread(file.path(coding.system.root,"IPUMS_MAJOR_MINOR.csv"),colClasses = "string")
  ipums[,minor := str_pad(minor,3,side="left",pad=0)]
isic_2 <- fread(file.path(coding.system.root,"ISIC_MAJOR_MINOR_2.csv"),colClasses = "string")
isic_3 <- fread(file.path(coding.system.root,"ISIC_MAJOR_MINOR_3.csv"),colClasses = "string")
  isic_3[,minor := str_pad(minor,2,side="left",pad=0)]
isic_4 <- fread(file.path(coding.system.root,"ISIC_MAJOR_MINOR_4.csv"),colClasses = "string")
  isic_4[,minor := str_pad(minor,2,side="left",pad=0)]

## load in hrh coding systems and 4-digit valid occupation codes by-system
hrh_allsystems <- list.files(coding.system.root,full.names=T)[grepl("HRH_MERGE",list.files(coding.system.root))]
hrh <- rbindlist(lapply(hrh_allsystems,fread,colClasses = "string"))
  hrh[,occ_code := str_pad(occ_code,occ_length,side="left",pad=0)]
  hrh[,occ_length := as.numeric(occ_length)]
all_isco_4d <- fread(file.path(coding.system.root,"ALL_ISCO.csv"),colClasses = "string")[`ISCO-08` != 1]
  all_isco_4d[,`ISCO-08` := str_pad(`ISCO-08`,4,side="left",pad=0)]
  all_isco_4d[,`ISCO-88` := str_pad(`ISCO-88`,4,side="left",pad=0)]
all_isco_4d <- melt(all_isco_4d,id.vars = "title") %>% .[,list(occ_coding_system = variable,occ_code = value)]
## add TASCO's valid extra 4-digit codes (or at least those that might otherwise be interpreted as HRH-ambiguous)
all_isco_4d <- rbindlist(list(all_isco_4d,all_isco_4d[occ_coding_system == "ISCO-88",list(occ_coding_system = "TASCO",occ_code)],
                              data.table(occ_coding_system = "TASCO",occ_code = c(2225,2226,3214,3249,5191,5199,5310,5320,5330))))
## supplement list of 4-digit codes to reflect all codes present in ISCO documentation
all_isco_4d <- rbind(all_isco_4d,data.table(occ_coding_system = "ISCO-88",occ_code = c("0100","1239","2229","2421","2470","3139","3310","3330","3433","4212","6142","7139","8287","9320","9330")))
hrh_3d_split <- fread(file.path(coding.system.root,"HRH_3DIG_SPLIT.csv"),colClasses = "string")
setnames(hrh_3d_split,c("envelope_code","me_name"),c("cadre","underlying_cadre"))

# SURVEY-SPECIFIC FILE PREP ---------------------------------------------------
## make aggregate CHN_CENSUS_1990 file for national estimate (seems that sampling was representative
## such that simply binding datasets together is still nationally representative)
agg_filename <- "FILEPATH"
chn_files <- in.files[grepl("chn_census",tolower(in.files)) & grepl("_1990_1990",in.files) & !grepl(agg_filename,in.files)]
if (length(chn_files) == 30) {
  source("FUNCTION")
  locations <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
  locs <- copy(locations[, c("ihme_loc_id","location_name"), with=F])

  df <- rbindlist(lapply(file.path(in.dir,chn_files),read_dta))
  df <- merge(df,locs,by="ihme_loc_id")
  setnames(df,"location_name","admin_1_mapped")
  df[,admin_1_id := ihme_loc_id]
  df[,ihme_loc_id := "CHN"]
  df[,file_path := df$file_path[1]] ## choose one file_path for the entire survey
  write_dta(df,file.path(in.dir,agg_filename))

  ## move CHN underlying files out of new folder, overwriting older versions where applicable
  for (file in chn_files) {
    file.copy(file.path(in.dir,file),file.path(gsub("/new","",in.dir),file),overwrite = T)
    file.remove(file.path(in.dir,file))
  }
  in.files <- list.files(in.dir)
} else if (length(chn_files) > 0) {
  stop("Error: China 1990 Census is missing some of the underlying locations (30 expected)")
}


# FLAG EXCEPTIONS ---------------------------------------------------------------------
## identify all files that have occ_code but need to bypass occ mapping (used for employment and certain HRH cadres,
## but cannot/should not map to ISCO generally). Also identify the specific subset for which occ codes do not exist for the
## general sample, but do exist for the subset containing HRH (so employment variable shoyuld be used to impute denominator)
trust_employment <- c("tza_hh_budget_survey","wb_lsms_isa_hhm_tza_2010_2011","wb_lsms_isa_hhm_tza_2012_2013.dta")
bypass_strings <- paste0(c("chn_census","idn_2005","ipums_census_hhm_bra_1991","ipums_census_hhm_bra_2000",
                           "mex_census_hhm_mex_2000","mex_survey_income","bwa_aids_impact","bwa_household_income",
                           "ipums_census_hhm_usa","pnad_hhm_bra","bol_hh_survey","bol_health_nutrition",
                           "ipums_census_hhm_tha_1990",trust_employment),collapse="|")
trust_employment <- paste0(trust_employment,collapse="|")

## identify all files that have an occ_code and coding system/occ_length that is set up to map to HRH, but need
## to bypass hrh mapping (need a reason that doesn't apply to occ mapping as well!)
hrh_bypass <- paste0(c("ISSP_2014_HHM_IND","UGA_HOUSEHOLD_SURVEY_HHM_UGA_2002_2003","CHN_CENSUS_HHM_CHN_1990_1990"),collapse="|")

## identify all files that have enough garbage coding in more granular digits that they should be truncated (code will assume
## survey's occ_length is correct, even if it requires removing non-9 digits). occ_length should reflect desired length (rather
## than actual observed maximum length)
## NOTE: this should only be used if there's reason to believe that garbage coding is due to ambiguity at higher granularity,
## not actual data quality issues. Also note that such garbage coding in non-HRH occ_majors and sub_majors are irrelevant and can be kept
occ_truncate <- paste0(c("issp","eurostat_hhm_mlt","eurostat_hhm_rou"),collapse="|")

## identify all coding systems that are identical to ISCO at  occ_major and sub_major level, but are not identical at the 3- or
## 4-digit level BUT ONLY if the differences are HRH-relevant, such as (1) if an extra code exists that should be grouped within
## an HRH cadre, or (2) if non-HRH codes are added that would be perceived as hrh-ambiguous invalid codes (and would thus be
## incorrectly excluded from denominator). Also note the occ_length at which codes begin to differ in HRH-relevant ways
## NOTE: this is necessary for the code to get mapped to ISCO in occ mapping, so even if HRH mapping for the system has not
## been set up, it is still distinct from hrh_truncate above
isco_approx <- data.table(occ_code_type = c("TASCO"),
                          occ_length = c("2"),
                          replacement_type = c("ISCO-88"))


# MAP SURVEY ---------------------------------------------------------------------
## Loop through each newly-extracted survey
for (filename in in.files) {
  warnings <- c("")
  criticals <- NULL
  cat(paste0("\n\n",match(filename,in.files),"/",length(in.files),": ",filename))
  df <- as.data.table(read_dta(file.path(in.dir,filename)))
  df_og <- copy(df)

  # CHECK FOR MISCELLANEOUS ERRORS IN DATASET -------------------------------
  ## check survey id vars
  if (!all(c("ihme_loc_id","file_path","nid","survey_module","survey_name","year_start","year_end") %in% names(df))) stop("Error: survey missing basic ubcov identifying vars")
  if (nrow(unique(df[,list(nid,survey_name,year_start,year_end)])) > 1 | all(is.na(df$nid)) |
      all(is.na(df$survey_name)) | all(is.na(df$year_start)) | all(is.na(df$year_end))) stop("Error: missing or multiple distinct values for nid, survey_name, year_start, or year_end")
  if (!all(c("age_year","sex_id") %in% names(df))) stop("Error: survey missing age and/or sex vars")
  ## subset to non-missing, modeled ages and sexes
  df <- df[age_year >= 15 & age_year < 70 & sex_id %in% c(1,2)]
  if (!all(c("employed") %in% names(df))) stop("Error: survey has no employment variable")
  if (!all(c(0,1) %in% unique(df$employed))) stop("Error: survey has 0%, 100%, or entirely missing employment")
  if (any(c("occ_code","industry_code") %in% names(df))) {
    if (!all(c(1,2) %in% df[,unique(sex_id)])) warnings <- paste(warnings,paste0("Warning: ",filename," has only one sex, should not be representative for both-sex HRH"),sep="\n")
    if (!all(seq(4,12) %in% unique(df[,floor(age_year/5)]))) warnings <- paste(warnings,paste0("Warning: ",filename," does not seem to be representative across all ages for occ/ind"),sep="\n") ## tolerates restriction to ages 20-64
  }
  if ("occ_code" %in% names(df)) {
    ## check for valid presence of necessary occ variables
    if (!all(c("occ_length","occ_code_type") %in% names(df))) stop("Error: survey has occ_code but is missing occ_length or occ_code_type")
    df[,occ_code_type := iconv(occ_code_type,"WINDOWS-1252","UTF-8")]
    if ("occ_code_2" %in% names(df)) df[is.na(occ_code) & !is.na(occ_code_2),occ_code := occ_code_2]
    if (all(is.na(df[,unique(occ_code)]))) stop("Error: survey has entirely missing occ_code variable")
    if (all(is.na(df[,unique(occ_length)]))) stop("Error: survey has entirely missing occ_length variable")
    if (all(is.na(df[,unique(occ_code_type)]))) stop("Error: survey has entirely missing occ_code_type variable")
    if (nrow(unique(df[,list(occ_length,occ_code_type)])) > 1) stop("Error: multiple distinct values for occ_length or occ_code_type")
    ## check that occ_length matches actual occ_codes (when occ_codes contain all trailing zeros, should reduce occ_length to functional length)
    df[,occ_length := as.numeric(occ_length)]
    if (df[,max(nchar(occ_code),na.rm=T)] < df[1,occ_length]) stop("Error: occ_length is longer than any of the actual occ_codes")
    if (df[,max(nchar(occ_code),na.rm=T)] > df[1,occ_length]) {
      dif <- df[,max(nchar(occ_code),na.rm=T)] - df[1,as.numeric(occ_length)]
      if (grepl(occ_truncate,tolower(filename))) {
        ## if it's a code that is marked for truncation, trust occ_length
        df[,occ_code :=  str_sub(str_pad(occ_code,max(nchar(occ_code),na.rm=T),side="left",pad=0),1,occ_length)]
      } else if (all(str_sub(df[!is.na(occ_code),unique(occ_code)],-dif,-1) == paste(rep("0",dif),collapse=""))) {
        ## if occ_length is shorter than the longest code, check if all of the last digits are 0's and  if so, remove them
        warnings <- paste(warnings,paste0("Warning: removing trailing 0's from occ_code in ",filename," to match occ_length"),sep="\n")
        df[,occ_code :=  str_sub(str_pad(occ_code,max(nchar(occ_code),na.rm=T),side="left",pad=0),1,occ_length)]
        if (all(str_sub(df[!is.na(occ_code),unique(occ_code)],-1,-1) == "0")) stop("Error: at this occ_length, all occ_codes have trailing 0's. Adjust occ_length to reflect true granularity of occ_code")
      } else {
        ## otherwise, occ_length issues should be fixed in the ubcov extraction, not here in the code
        stop("Error: some occ_codes exceed occ_length. Check for values that should be marked as occ_missing, or find appropriate occ_length value")
      }
    }
    if (all(str_sub(df[!is.na(occ_code),unique(occ_code)],-1,-1) == "0")) stop("Error: all final digits are 0, occ_length needs to be adjusted")
  }
  if ("industry_code" %in% names(df)) {
    ## check for valid presence of necessary industry variables
    if (!all(c("industry_length","industry_code_type") %in% names(df))) stop("Error: survey has industry_code but is missing industry_length or industry_code_type")
    df[,industry_code_type := iconv(industry_code_type,"WINDOWS-1252","UTF-8")]
    if ("industry_code_2" %in% names(df)) df[is.na(industry_code) & !is.na(industry_code_2),industry_code := industry_code_2]
    if (all(is.na(df[,unique(industry_code)]))) stop("Error: survey has entirely missing industry_code variable")
    if (all(is.na(df[,unique(industry_length)]))) stop("Error: survey has entirely missing industry_length variable")
    if (all(is.na(df[,unique(industry_code_type)]))) stop("Error: survey has entirely missing industry_code_type variable")
    if (nrow(unique(df[,list(industry_length,industry_code_type)])) > 1) stop("Error: multiple distinct values for industry_length or industry_code_type")
    ## check that industry_length matches actual industry_codes (when industry_codes contain all trailing zeros, should reduce industry_length to functional length)
    df[,industry_length := as.numeric(industry_length)]
    if (df[,max(nchar(industry_code),na.rm=T)] < df[1,industry_length]) stop("Error: industry_length is longer than any of the actual industry_codes")
    if (df[,max(nchar(industry_code),na.rm=T)] > df[1,industry_length]) {
      dif <- df[,max(nchar(industry_code),na.rm=T)] - df[1,industry_length]
      if (all(str_sub(df[!is.na(industry_code),unique(industry_code)],-dif,-1) == paste(rep("0",dif),collapse=""))) {
        warnings <- paste(warnings,paste0("Warning: removing trailing 0's from industry_code in ",filename," to match industry_length"),sep="\n")
        df[,industry_code :=  str_sub(str_pad(industry_code,max(nchar(industry_code),na.rm=T),side="left",pad=0),1,industry_length)]
        if (all(str_sub(df[!is.na(industry_code),unique(industry_code)],-1,-1) == "0")) stop("Error: at this industry_length, all industry_codes have trailing 0's. Adjust industry_length to reflect true granularity of industry_code")
      } else {
        stop("Error: some industry_codes exceed industry_length. Check for values that should be marked as industry_missing, or find appropriate industry_length value")
      }
    }
    if (all(str_sub(df[!is.na(industry_code),unique(industry_code)],-1,-1) == "0")) stop("Error: all final digits are 0, industry_length needs to be adjusted")
  }
  
  ## for cases where multiple file_paths go into one survey, choose one to fill in for all of them (or else collapse code will
  ## treat them as separate surveys)
  df[,file_path := df[1,file_path]]
  df[,year_id := floor((year_start + year_end)/2)]

  ## save cleaned version of original data for reference in debugging
  df_clean <- copy(df)


  # SURVEY-SPECIFIC FIXES ---------------------------------------------------
  ## EXTRAVAR used as a gateway before asking about employment in last 7 days (so EXTRAVAR == F means they could not have been employed)
  relevant_nids <- c(243012)
  if ("extravar" %in% names(df)) df[is.na(employed) & !is.na(extravar) & df[1,nid] %in% relevant_nids,employed := 0]

  ## EXTRAVAR used as an additional variable for temp_absent
  relevant_nids <- c(32388,148344,80316,133786,265002,218773,165101)
  if ("temp_absent" %in% names(df) & df[1,nid] %in% relevant_nids) {
    df[extravar == 1,temp_absent := 1]
    df[is.na(temp_absent) & extravar == 0, temp_absent := 0]
  }

  ## TZA_INTEGRATED_LABOR_FORCE_SURVEY used EXTRAVAR to differentiate whether main or secondary activity was performed in
  ## last week, and also imputes ISIC code for agriculture/fishery work
  relevant_nids <- c(12653,12652)
  if (df[1,nid] %in% relevant_nids) {
    df[(employed == 1 | temp_absent == 1) & is.na(industry_code) & industry_code_type == "ISIC-3",industry_code := as.numeric(substr("02002",1,industry_length))]
    df[(employed == 1 | temp_absent == 1) & is.na(industry_code_2) & industry_code_type == "ISIC-3",industry_code_2 := as.numeric(substr("02002",1,industry_length))]
    df[(employed == 1 | temp_absent == 1) & is.na(industry_code) & industry_code_type == "ISIC-2",industry_code := as.numeric(substr("12110",1,industry_length))]
    df[(employed == 1 | temp_absent == 1) & is.na(industry_code_2) & industry_code_type == "ISIC-2",industry_code_2 := as.numeric(substr("12110",1,industry_length))]
    df[extravar == 1 & !is.na(occ_code_2),occ_code := occ_code_2]
    df[extravar == 1 & !is.na(industry_code_2),industry_code := industry_code_2]
  }

  ## GHA survey doesn't consider agriculture as part of employment, need to backfill occ_code (assigning everyone to subsistence
  ## to prevent errors from ambiguous coding but DO NOT trust agriculture 2-digit for this survey)
  if (df[1,nid] %in% c(236205)) df[temp_absent == 1,c("occ_code","industry_code") := .(621,0130)]


  # VET EMPLOYMENT --------------------------------------------------------------
  if ("temp_absent" %in% names(df)) {
    ## non-standard surveys that do not consider temporary absence from work to be employed need to be corrected
    ## NOTE: issues will arise if such absent workers were not asked about their occupation/industry
    df[temp_absent == 1, employed := 1]
    df[is.na(employed) & temp_absent == 0, employed := 0]
    df[!is.na(employed) & is.na(temp_absent),temp_absent := 0] ## make denominator for temp_absent and employed consistent
  }
  if ("military" %in% names(df)) df[!is.na(employed) & is.na(military),military := 0] ## make denominator for military and employed consistent
  if ((df[is.na(employed),.N]/df[,.N]) > 0.1) warnings <- paste(warnings,paste0("Warning: over 10% of 15-69 yos missing employment status in ",filename),sep="\n")
  if ("occ_code" %in% names(df)) {
    if (df[is.na(employed) & !is.na(occ_code),.N] > 0) warnings <- paste(warnings,paste0("Warning: ",round(df[is.na(employed) & !is.na(occ_code),.N]*100/df[is.na(employed),.N],1),"% of the ",df[is.na(employed),.N]," people missing employment variable have non-missing occ_code"),sep="\n")
  }
  if ("industry_code" %in% names(df)) {
    if (df[is.na(employed) & !is.na(industry_code),.N] > 0) warnings <- paste(warnings,paste0("Warning: ",round(df[is.na(employed) & !is.na(industry_code),.N]*100/df[is.na(employed),.N],1),"% of the ",df[is.na(employed),.N]," people missing employment variable have non-missing industry_code"),sep="\n")
  }
  df <- df[!is.na(employed)]
  if ((df[employed == 1,.N]/df[,.N]) > 0.9) warnings <- paste(warnings,paste0("Warning: over 90% of 15-69 yos seem to be employed in ",filename),sep="\n")
  if ((df[employed == 1,.N]/df[,.N]) < 0.1) warnings <- paste(warnings,paste0("Warning: under 10% of 15-69 yos seem to be employed in ",filename),sep="\n")

  ## start logfile for reviewing survey issues post-extraction
  log <- data.table(nid = df[1,nid],survey_name = df[1,survey_name],ihme_loc_id = df[1,ihme_loc_id],year_id = df[1,year_id])

  ## provide informative warning for surveys that are just employment info
  if (!"occ_code" %in% names(df) & !"industry_code" %in% names(df)) warnings <- paste(warnings,"No occ or ind info",sep="\n")


  # MAP OCC CODES -----------------------------------------------------------
  if ("occ_code" %in% names(df)) {

    ## if survey uses non-ISCO coding system that matches ISCO at the occ_major and sub-major level, treat is the same
    ## as ISCO for this section
    if (isco_approx[occ_code_type == df[1,occ_code_type] & occ_length < df[1,occ_length],.N] > 0) {
      df[,og_occ_type := df[1,occ_code_type]]
      replacement <- isco_approx[occ_code_type == df[1,occ_code_type],replacement_type]
      df[,occ_code_type := replacement]
    } else if (isco_approx[occ_code_type == df[1,occ_code_type],.N] > 0) {
      warnings <- paste(warnings,paste0("Warning: at this occ_length ",df[1,occ_code_type]," does not differ from ISCO at all, should just be considered ISCO"),sep="\n")
      df[,occ_code_type := "ISCO"]
    }

    ## convert codes to strings and pad in order to retain leading zeros where needed
    df[,occ_code := str_pad(occ_code,occ_length,side="left",pad=0)]

    ## remove occ_codes for individuals who are not currently employed
    if (df[!is.na(occ_code) & employed == 0,.N] > 0) warnings <- paste(warnings,paste0("Warning: ",df[!is.na(occ_code) & employed == 0,.N]," observations in ",filename," have occ_codes despite being unemployed"),sep="\n")
    df[employed == 0, occ_code := NA]
    if ("occ_code_label" %in% names(df)) df[employed == 0, occ_code_label := ""]

    ## alert to the number of missing occ_codes among those who are employed (and not in military)
    if ("military" %in% names(df)) {
      if (df[is.na(occ_code) & employed == 1 & (is.na(military) | military == 0),.N]/df[employed == 1,.N] > 0.05) warnings <- paste(warnings,paste0("Warning: occ_codes missing for ",round(df[is.na(occ_code) & employed == 1 & (is.na(military) | military == 0),.N]*100/df[employed == 1,.N],1),"% of employed persons in ",filename),sep="\n")
    } else {
      if (df[is.na(occ_code) & employed == 1,.N]/df[employed == 1,.N] > 0.05) warnings <- paste(warnings,paste0("Warning: occ_codes missing for ",round(df[is.na(occ_code) & employed == 1,.N]*100/df[employed == 1,.N],1),"% of employed persons in ",filename),sep="\n")
    }

    ## bypass mapping to ISCO if marked for exclusive use in HRH
    if (!grepl(bypass_strings,tolower(filename))) {

      ## identify occ_major and occ_sub_major from occ_code
      df[,occ_major := substr(occ_code,1,1)]
      if (df[1,occ_length] > 1) df[,occ_sub_major := substr(occ_code,1,2)]

      ## identify coding system for survey
      if (grepl("ISCO",toupper(df[1,occ_code_type]))) {
        if (df[1,occ_length] > 1) {
          match <- data.table(version = c("ISCO-08","ISCO-88","ISCO-68","ISCO-58"),
                              timing = c(df[1,year_end] - c(2008,1988,1968,1958)),
                              impossible_codes = c(as.numeric(!df[!is.na(occ_sub_major),unique(occ_sub_major)] %in% unique(isco_08$occ_sub_major)) %>% sum(),
                                                   as.numeric(!df[!is.na(occ_sub_major),unique(occ_sub_major)] %in% unique(isco_88$occ_sub_major)) %>% sum(),
                                                   as.numeric(!df[!is.na(occ_sub_major),unique(occ_sub_major)] %in% unique(isco_68$occ_sub_major)) %>% sum(),
                                                   as.numeric(!df[!is.na(occ_sub_major),unique(occ_sub_major)] %in% unique(isco_58$occ_sub_major)) %>% sum()),
                              representation = c((as.numeric(unique(isco_08$occ_sub_major) %in% df[!is.na(occ_sub_major),unique(occ_sub_major)]) %>% sum)/length(unique(isco_08$occ_sub_major)),
                                                 (as.numeric(unique(isco_88$occ_sub_major) %in% df[!is.na(occ_sub_major),unique(occ_sub_major)]) %>% sum)/length(unique(isco_88$occ_sub_major)),
                                                 (as.numeric(unique(isco_68$occ_sub_major) %in% df[!is.na(occ_sub_major),unique(occ_sub_major)]) %>% sum)/length(unique(isco_68$occ_sub_major)),
                                                 (as.numeric(unique(isco_58$occ_sub_major) %in% df[!is.na(occ_sub_major),unique(occ_sub_major)]) %>% sum)/length(unique(isco_58$occ_sub_major))))

        } else {
          match <- data.table(version = c("ISCO-08","ISCO-88","ISCO-68","ISCO-58"),
                              timing = c(df[1,year_end] - c(2008,1988,1968,1958)),
                              impossible_codes = c(rep(as.numeric(!df[!is.na(occ_major),unique(occ_major)] %in% unique(isco_08$occ_major)) %>% sum(),2),
                                                   rep(as.numeric(!df[!is.na(occ_major),unique(occ_major)] %in% unique(isco_68$occ_major)) %>% sum(),2)),
                              representation = c(rep((as.numeric(unique(isco_08$occ_major) %in% df[!is.na(occ_major),unique(occ_major)]) %>% sum)/length(unique(isco_08$occ_major)),2),
                                                 rep((as.numeric(unique(isco_08$occ_major) %in% df[!is.na(occ_major),unique(occ_major)]) %>% sum)/length(unique(isco_08$occ_major)),2)))
        }

        ## first select version(s) that minimize the number of invalid codes in the dataset, then
        ## select version(s) for which dataset represents a maximum proportion of the version's possible codes
        match_occ <- copy(match)
        match <- match[impossible_codes == min(match$impossible_codes)]
        match <- match[representation == max(match$representation)]

        ## if a specific ISCO version was provided, see whether it matches what the automated one has narrowed it down to,
        ## check that timing matches survey end year, and set version to the appropriate isco major and sub-major
        if (grepl("08",df[1,occ_code_type])) {
          if (df[1,year_end] < 2008) warnings <- paste(warnings,paste0("Warning: ",filename," precedes specified ISCO version"),sep="\n")
          if (!"ISCO-08" %in% unique(match$version)) warnings <- paste(warnings,paste0("Warning: closest ISCO version auto-match for ",filename," is ",unique(match$version),", not the specified version"),sep="\n")
          df[,occ_coding_system := "ISCO-08"]
        } else if (grepl("88",df[1,occ_code_type])) {
          if (df[1,year_end] < 1988) warnings <- paste(warnings,paste0("Warning: ",filename," precedes specified ISCO version"),sep="\n")
          if (!"ISCO-88" %in% unique(match$version)) warnings <- paste(warnings,paste0("Warning: closest ISCO version auto-match for ",filename," is ",unique(match$version),", not the specified version"),sep="\n")
          df[,occ_coding_system := "ISCO-88"]
        } else if (grepl("68",df[1,occ_code_type])) {
          if (df[1,year_end] < 1968) warnings <- paste(warnings,paste0("Warning: ",filename," precedes specified ISCO version"),sep="\n")
          if (!"ISCO-68" %in% unique(match$version)) warnings <- paste(warnings,paste0("Warning: closest ISCO version auto-match for ",filename," is ",unique(match$version),", not the specified version"),sep="\n")
          df[,occ_coding_system := "ISCO-68"]
        } else if (grepl("58",df[1,occ_code_type])) {
          if (df[1,year_end] < 1958) warnings <- paste(warnings,paste0("Warning: ",filename," precedes specified ISCO version"),sep="\n")
          if (!"ISCO-58" %in% unique(match$version)) warnings <- paste(warnings,paste0("Warning: closest ISCO version auto-match for ",filename," is ",unique(match$version),", not the specified version"),sep="\n")
          df[,occ_coding_system := "ISCO-58"]

          ## if no specified version, use the best match
        } else {
          ## further narrow down auto-match options (if necessary) using version that nearest precedes the survey
          ## temporally, or most closely follows it if none of the remaining options precede it (in which case print warning)
          if (match[,.N] > 1 & match[timing > 0,.N] > 0) {
            match <- match[timing == match[timing > 0,min(timing)]]
          } else if (match[,.N] > 1) {
            match <- match[timing == max(match$timing)]
            warnings <- paste(warnings,paste0("Warning: ",filename," precedes closest matched ISCO version by ",abs(match[1,timing])," years"),sep="\n")
          }

          ## assign best matched version
          df[,occ_coding_system := match[1,version]]
        }
        warnings <- paste(warnings,paste0("Matched to ",df[1,occ_coding_system]),sep="\n")

        ## depending on length of occ code, merge on major and sub major groups based on provided/matched coding system, and
        ## also output any remaining mismatched codes at most detailed level possible
        if (df[1,occ_length] > 1) {
          ## warn of any mismatches
          all <- df[!is.na(occ_sub_major),sort(unique(occ_sub_major))]
          if (df[1,occ_coding_system] == "ISCO-08") merge_version <- copy(isco_08)
          if (df[1,occ_coding_system] == "ISCO-88") merge_version <- copy(isco_88)
          if (df[1,occ_coding_system] == "ISCO-68") merge_version <- copy(isco_68)
          if (df[1,occ_coding_system] == "ISCO-58") merge_version <- copy(isco_58)
          mismatch <- all[!all %in% unique(merge_version$occ_sub_major)]
          if (length(mismatch) > 0) warnings <- paste(warnings,paste0("Warning: the following occ_sub_majors in ",filename," are not in its matched coding system: ",paste0(mismatch,collapse=",")),sep="\n")

          ## merge on both major and sub-major groups
          df <- merge(df,unique(merge_version[,list(occ_major,occ_major_label)]),by="occ_major",all.x=T)
          df <- merge(df,unique(merge_version[,list(occ_sub_major,occ_sub_major_label)]),by="occ_sub_major",all.x=T)
        } else {
          ## warn of any mismatches
          all <- df[!is.na(occ_major),sort(unique(occ_major))]
          if (df[1,occ_coding_system] == "ISCO-08") merge_version <- copy(isco_08)
          if (df[1,occ_coding_system] == "ISCO-88") merge_version <- copy(isco_88)
          if (df[1,occ_coding_system] == "ISCO-68") merge_version <- copy(isco_68)
          if (df[1,occ_coding_system] == "ISCO-58") merge_version <- copy(isco_58)
          mismatch <- all[!all %in% unique(merge_version$occ_major)]
          if (length(mismatch) > 0) warnings <- paste(warnings,paste0("Warning: the following occ_majors in ",filename," are not in its matched coding system: ",paste0(mismatch,collapse=",")),sep="\n")

          ## merge on major
          df <- merge(df,unique(merge_version[,list(occ_major,occ_major_label)]),by="occ_major",all.x=T)
        }

        ## Final ISCO Formatting for older versions (some have aggregate occ_majors)
        if (df[1,occ_coding_system] == "ISCO-68") {
          df[occ_major %in% c(0,1), occ_major := "0-1"]
          df[occ_major %in% seq(7,9), occ_major := "7-9"]
        }
        if (df[1,occ_coding_system] == "ISCO-58") df[occ_major %in% c(7,8), occ_major := "7-8"]

        df[!is.na(occ_major),occ_major := paste0('="',occ_major,'"')]

      }

      ## if mapped to ISCO
      if ("occ_coding_system" %in% names(df)) {
        ## For compulsory military personnel, impute occ codes (based on its coding system)
        if ("military" %in% names(df)) {
          if (all(is.na(df[,unique(military)]))) stop("Error: survey has entirely missing military variable")
          if (df[1,occ_length] > 1) {
            if (df[1,occ_coding_system] == "ISCO-08") df[military == 1 & is.na(occ_code),c("occ_major","occ_major_label","occ_sub_major","occ_sub_major_label") := isco_08[occ_sub_major == "03",list(occ_major,occ_major_label,occ_sub_major,occ_sub_major_label)]]
            if (df[1,occ_coding_system] == "ISCO-88") df[military == 1 & is.na(occ_code),c("occ_major","occ_major_label","occ_sub_major","occ_sub_major_label") := isco_88[occ_major == "0",list(occ_major,occ_major_label,occ_sub_major,occ_sub_major_label)]]
            if (df[1,occ_coding_system] == "ISCO-68") df[military == 1 & is.na(occ_code),c("occ_major","occ_major_label","occ_sub_major","occ_sub_major_label") := isco_68[occ_major == "Y",list(occ_major,occ_major_label,occ_sub_major,occ_sub_major_label)]]
            if (df[1,occ_coding_system] == "ISCO-58") df[military == 1 & is.na(occ_code),c("occ_major","occ_major_label","occ_sub_major","occ_sub_major_label") := isco_58[occ_major == "Y",list(occ_major,occ_major_label,occ_sub_major,occ_sub_major_label)]]
          } else {
            if (df[1,occ_coding_system] == "ISCO-08") df[military == 1 & is.na(occ_code),c("occ_major","occ_major_label") := isco_08[occ_sub_major == "03",list(occ_major,occ_major_label)]]
            if (df[1,occ_coding_system] == "ISCO-88") df[military == 1 & is.na(occ_code),c("occ_major","occ_major_label") := isco_88[occ_major == "0",list(occ_major,occ_major_label)]]
            if (df[1,occ_coding_system] == "ISCO-68") df[military == 1 & is.na(occ_code),c("occ_major","occ_major_label") := isco_68[occ_major == "Y",list(occ_major,occ_major_label)]]
            if (df[1,occ_coding_system] == "ISCO-58") df[military == 1 & is.na(occ_code),c("occ_major","occ_major_label") := isco_58[occ_major == "Y",list(occ_major,occ_major_label)]]
          }
        }

        ## Vet the map results (is this broadly usable, or ISCO specific?)
        if ((df[employed == 1 & is.na(occ_major_label),.N]/df[employed == 1,.N]) > 0.2) stop("Error: more than 20% of occ codes were missing or had invalid occ_majors (according to matched version)")
        if (df[1,occ_length] > 1) {
          if ((df[employed == 1 & is.na(occ_sub_major_label),.N]/df[employed == 1,.N]) > 0.2) warnings <- paste(warnings,paste0("Warning: more than 20% of occ codes had invalid sub-majors in ",filename),sep="\n")
        }
        occ_props <- df[employed == 1,.N/df[employed == 1,.N],by=occ_major_label]
        if (max(occ_props$V1) > .9) stop("Error: more than 90% of employed pop involved in ",occ_props[V1 == occ_props[,max(V1)],occ_major_label])

        ## create logfile of survey issues for reference later
        log[,c("occ_missing","occ_max") := list(round(df[employed == 1 & is.na(occ_major_label),.N]/df[employed == 1,.N],5),round(max(occ_props$V1),5))]
      } else {
        warnings <- paste(warnings,paste0("No occupation coding system match found for ",df[1,toupper(occ_code_type)]),sep="\n")
      }
    } else {
      warnings <- paste(warnings,paste0("Bypassing occ coding..."),sep="\n")
    }

    ## if survey was not ISCO and just happened to match ISCO at major and sub-major level, return it's original occ_code_type
    ## and have the HRH mapping treat it like it would an occ bypassed survey
    if ("og_occ_type" %in% names(df)) {
      if (isco_approx[occ_code_type == df[1,og_occ_type],occ_length] < 4) {
        df[,occ_code_type := og_occ_type]
        df[,isco_approx := 1]
        df[,og_occ_type := NULL]
        if ("occ_coding_system" %in% names(df)) df[,occ_coding_system := NULL]
      }
    }
  }


  # MAP INDUSTRY CODES -----------------------------------------------------------
  if ("industry_code" %in% names(df)) {
    ## convert codes to strings and pad in order to retain leading zeros where needed
    df[,industry_code := str_pad(industry_code,industry_length,side="left",pad=0)]

    ## remove codes for individuals who are not employed
    if (df[!is.na(industry_code) & employed == 0,.N] > 0) warnings <- paste(warnings,paste0("Warning: ",df[!is.na(industry_code) & employed == 0,.N]," observations have industry_codes despite being unemployed or missing employment status"),sep="\n")
    df[employed == 0, industry_code := NA]
    if ("industry_code_label" %in% names(df)) df[employed == 0, industry_code_label := ""]

    ## alert to the number of missing industry_codes among those who are employed (and not in military)
    if ("military" %in% names(df)) {
      if (df[is.na(industry_code) & employed == 1 & (is.na(military) | military == 0),.N]/df[employed == 1,.N] > 0.05) warnings <- paste(warnings,paste0("Warning: industry_codes missing for ",round(df[is.na(industry_code) & employed == 1 & (is.na(military) | military == 0),.N]*100/df[employed == 1,.N],1),"% of employed persons in ",filename),sep="\n")
    } else {
      if (df[is.na(industry_code) & employed == 1,.N]/df[employed == 1,.N] > 0.05) warnings <- paste(warnings,paste0("Warning: industry_codes missing for ",round(df[is.na(industry_code) & employed == 1,.N]*100/df[employed == 1,.N],1),"% of employed persons in ",filename),sep="\n")
    }

    ## determine coding system
    if (grepl("IPUMS",df[1,toupper(industry_code_type)])) {
      if (df[1,industry_length] != "3") stop("Error: industry length must be 3 if coding system is IPUMS")
      df[,minor := industry_code]
      df[,coding_system := "IPUMS"]
    } else if (grepl("ISIC",df[1,toupper(industry_code_type)])) {
      if (df[1,industry_length] > 1) {
        df[,minor := substr(industry_code,1,2)]
        match <- data.table(version = c("ISIC-4","ISIC-3","ISIC-2"),
                            timing = c(df[1,year_end] - c(2008,1989,1968)),
                            impossible_codes = c(as.numeric(!df[!is.na(industry_code),unique(minor)] %in% unique(isic_4$minor)) %>% sum,
                                                 as.numeric(!df[!is.na(industry_code),unique(minor)] %in% unique(isic_3$minor)) %>% sum,
                                                 as.numeric(!df[!is.na(industry_code),unique(minor)] %in% unique(isic_2$minor)) %>% sum),
                            representation = c((as.numeric(unique(isic_4$minor) %in% df[!is.na(industry_code),unique(minor)]) %>% sum)/length(unique(isic_4$minor)),
                                               (as.numeric(unique(isic_3$minor) %in% df[!is.na(industry_code),unique(minor)]) %>% sum)/length(unique(isic_3$minor)),
                                               (as.numeric(unique(isic_2$minor) %in% df[!is.na(industry_code),unique(minor)]) %>% sum)/length(unique(isic_2$minor))))
      } else {
        df[,major := substr(industry_code,1,1)]
        match <- data.table(version = c("ISIC-4","ISIC-3","ISIC-2"),
                            timing = c(df[1,year_end] - c(2008,1989,1968)),
                            impossible_codes = c(as.numeric(!df[!is.na(industry_code),unique(major)] %in% unique(isic_4$major)) %>% sum,
                                                 as.numeric(!df[!is.na(industry_code),unique(major)] %in% unique(isic_3$major)) %>% sum,
                                                 as.numeric(!df[!is.na(industry_code),unique(major)] %in% unique(isic_2$major)) %>% sum),
                            representation = c((as.numeric(unique(isic_4$major) %in% df[!is.na(industry_code),unique(major)]) %>% sum)/length(unique(isic_4$major)),
                                               (as.numeric(unique(isic_3$major) %in% df[!is.na(industry_code),unique(major)]) %>% sum)/length(unique(isic_3$major)),
                                               (as.numeric(unique(isic_2$major) %in% df[!is.na(industry_code),unique(major)]) %>% sum)/length(unique(isic_2$major))))
      }

      ## first select version(s) that minimize the number of invalid codes in the dataset, then
      ## select version(s) for which dataset represents a maximum proportion of the version's possible codes
      match_ind <- copy(match)
      match <- match[impossible_codes == min(match$impossible_codes)]
      match <- match[representation == max(match$representation)]

      ## if a specific ISIC version was provided, see whether it matches what the automated one has narrowed it down to,
      ## check that timing matches survey end year, and set version to the appropriate ISIC major and minor
      if (grepl("4",df[1,industry_code_type])) {
        if (df[1,year_end] < 2008) warnings <- paste(warnings,paste0("Warning: ",filename," precedes specified ISIC version"),sep="\n")
        if (!"ISIC-4" %in% unique(match$version)) warnings <- paste(warnings,paste0("Warning: closest ISIC version auto-match for ",filename," is ",unique(match$version),", not the specified version"),sep="\n")
        df[,coding_system := "ISIC-4"]
      } else if (grepl("3",df[1,industry_code_type])) {
        if (df[1,year_end] < 1989) warnings <- paste(warnings,paste0("Warning: ",filename," precedes specified ISIC version"),sep="\n")
        if (!"ISIC-3" %in% unique(match$version)) warnings <- paste(warnings,paste0("Warning: closest ISIC version auto-match for ",filename," is ",unique(match$version),", not the specified version"),sep="\n")
        df[,coding_system := "ISIC-3"]
      } else if (grepl("2",df[1,industry_code_type])) {
        if (df[1,year_end] < 1968) warnings <- paste(warnings,paste0("Warning: ",filename," precedes specified ISIC version"),sep="\n")
        if (!"ISIC-2" %in% unique(match$version)) warnings <- paste(warnings,paste0("Warning: closest ISIC version auto-match for ",filename," is ",unique(match$version),", not the specified version"),sep="\n")
        df[,coding_system := "ISIC-2"]

        ## if no specified version, use the best match
      } else {
        ## further narrow down auto-match options (if necessary) using version that nearest precedes the survey
        ## temporally, or most closely follows it if none of the remaining options precede it (in which case print warning)
        if (match[,.N] > 1 & match[timing > 0,.N] > 0) {
          match <- match[timing == match[timing > 0,min(timing)]]
        } else if (match[,.N] > 1) {
          match <- match[timing == max(match$timing)]
          warnings <- paste(warnings,paste0("Warning: ",filename," precedes closest matched ISIC version by ",abs(match[1,timing])," years"),sep="\n")
        }

        ## assign best matched version
        df[,coding_system := match[1,version]]
      }
    }

    if ("coding_system" %in% names(df)) {
      warnings <- paste(warnings,paste0("Matched to ",df[1,coding_system]),sep="\n")

      ## depending on length of industry code, merge on major and minor groups based on provided/matched coding system, and
      ## also output any remaining mismatched codes at most detailed level possible
      if (df[1,industry_length] > 1) {
        ## warn of any mismatches
        all <- df[!is.na(minor),sort(unique(minor))]
        if (df[1,coding_system] == "IPUMS") merge_version <- copy(ipums)
        if (df[1,coding_system] == "ISIC-4") merge_version <- copy(isic_4)
        if (df[1,coding_system] == "ISIC-3") merge_version <- copy(isic_3)
        if (df[1,coding_system] == "ISIC-2") merge_version <- copy(isic_2)
        mismatch <- all[!all %in% unique(merge_version$minor)]
        if (length(mismatch) > 0) {
          warnings <- paste(warnings,paste0("Warning: the following minors in ",filename," are not in its matched coding system: ",paste0(mismatch,collapse=",")),sep="\n")
          warnings <- paste(warnings,paste0("These invalid minors make up ",round(df[minor %in% mismatch,.N]*100/df[!is.na(minor),.N],1),"% of all industry minors"),sep="\n")
        }

        ## merge on both major and minor groups
        df <- merge(df,unique(merge_version[,list(major,major_label,major_label_short,minor,minor_label)]),by="minor",all.x=T)
      } else {
        ## warn of any mismatches
        all <- df[!is.na(major),sort(unique(major))]
        if (df[1,coding_system] == "ISIC-4") merge_version <- copy(isic_4)
        if (df[1,coding_system] == "ISIC-3") merge_version <- copy(isic_3)
        if (df[1,coding_system] == "ISIC-2") merge_version <- copy(isic_2)
        mismatch <- all[!all %in% unique(merge_version$major)]
        if (length(mismatch) > 0){
          warnings <- paste(warnings,paste0("Warning: the following majors in ",filename," are not in its matched coding system: ",paste0(mismatch,collapse=",")),sep="\n")
          warnings <- paste(warnings,paste0("These invalid majors make up ",round(df[major %in% mismatch,.N]*100/df[!is.na(major),.N],1),"% of all industry majors"),sep="\n")
        }

        ## merge on major groups
        df <- merge(df,unique(merge_version[,list(major,major_label,major_label_short)]),by="major",all.x=T)
      }


      ## For compulsory military personnel, impute industry codes (based on its coding system)
      if ("military" %in% names(df)) {
        if (df[1,industry_length] > 1) {
          if (df[1,coding_system] == "IPUMS") df[military == 1 & is.na(industry_code),c("major","major_label","major_label_short","minor","minor_label") := ipums[minor == "100",list(major,major_label,major_label_short,minor,minor_label)]]
          if (df[1,coding_system] == "ISIC-4") df[military == 1 & is.na(industry_code),c("major","major_label","major_label_short","minor","minor_label") := isic_4[minor == "84",list(major,major_label,major_label_short,minor,minor_label)]]
          if (df[1,coding_system] == "ISIC-3") df[military == 1 & is.na(industry_code),c("major","major_label","major_label_short","minor","minor_label") := isic_3[minor == "75",list(major,major_label,major_label_short,minor,minor_label)]]
          if (df[1,coding_system] == "ISIC-2") df[military == 1 & is.na(industry_code),c("major","major_label","major_label_short","minor","minor_label") := isic_2[minor == "91",list(major,major_label,major_label_short,minor,minor_label)]]
        } else {
          if (df[1,coding_system] == "ISIC-4") df[military == 1 & is.na(industry_code),c("major","major_label","major_label_short") := isic_4[minor == "84",list(major,major_label,major_label_short)]]
          if (df[1,coding_system] == "ISIC-3") df[military == 1 & is.na(industry_code),c("major","major_label","major_label_short") := isic_3[minor == "75",list(major,major_label,major_label_short)]]
          if (df[1,coding_system] == "ISIC-2") df[military == 1 & is.na(industry_code),c("major","major_label","major_label_short") := isic_2[minor == "91",list(major,major_label,major_label_short)]]
        }
      }

      ## Vet the map results 
      if ((df[employed == 1 & is.na(major_label),.N]/df[employed == 1,.N]) > 0.2) stop("Error: more than 20% of industry codes were missing or had invalid majors (according to matched version)")
      if (df[1,industry_length] > 1) {
        if ((df[employed == 1 & is.na(minor_label),.N]/df[employed == 1,.N]) > 0.2) warnings <- paste(warnings,paste0("Warning: more than 20% of industry codes had invalid minors in ",filename),sep="\n")
      }
      industry_props <- df[employed == 1,.N/df[employed == 1,.N],by=major_label]
      if (max(industry_props$V1) > .9) stop("Error: more than 90% of employed pop involved in ",industry_props[V1 == industry_props[,max(V1)],major_label])

      ## create logfile of survey issues for reference later
      log[,c("ind_missing","ind_max") := list(round(df[employed == 1 & is.na(major_label),.N]/df[employed == 1,.N],5),round(max(industry_props$V1),5))]
    } else {
      warnings <- paste(warnings,paste0("No industry coding system match found for ",df[1,toupper(industry_code_type)]),sep="\n")
    }
  }


  # HRH MAPPING -------------------------------------------------------------
  if ("occ_code" %in% names(df) & !grepl(hrh_bypass,filename)) {

    ## impute occ_code for denominator purposes where it is known to be incomplete for non-HRH occupations (this code chosen randomly as a non-HRH related code)
    if (grepl(trust_employment,tolower(filename))) df[is.na(occ_code) & employed == 1, occ_code := substr("6111",1,df[1,occ_length])]

    ## if survey bypassed occ mapping, assume its ubcov-identified coding system is correct
    if (!"occ_coding_system" %in% names(df)) df[,occ_coding_system := occ_code_type]

    ## subset to matching hrh coding system to dataset
    hrh_system <- hrh[occ_coding_system == df[1,occ_coding_system] & occ_length == df[1,occ_length]]
    hrh_system_cp <- copy(hrh_system)

    if (nrow(hrh_system) > 0) {
      cadres <- hrh_system[,unique(me_name)]
      hrh_system[,value := 1]
      hrh_system <- dcast(hrh_system,...~me_name,value.var = "value")
      hrh_system[,(cadres) := lapply(.SD,function(x) {as.numeric(!is.na(x))}),.SDcols = cadres]
      df <- merge(df,hrh_system,by=c("occ_code","occ_length","occ_coding_system"),all.x=T)
      if (all(is.na(as.vector(unlist(df[,cadres,with=F]))))) stop("Error: survey has no HRH cadres at all. Check coding system for issues")

      ## Determine appropriate denominator
      ## Theoretically would be all occ_codes with a valid occ_major, but it's tricky because some codes end up being
      ## invalid due to lack of specificity (not coded all the way to occ_length, remaining digits coded to 9 or 0 depending
      ## on the survey). For invalid codes that appear to be less granular, can include in denominator if they do not seem to
      ## refer to an hrh category anyways (based on valid major and/or sub-major), but if it could be an hrh category then
      ## should leave NA instead. This results in a slight downward bias in the estimates, since invalid codes are only removed
      ## when they potentially referred to hrh workers. But since the majority of invalid codes appear in non-hrh occ_majors
      ## (ex. agriculture), the alternative of dropping all invalid codes would result in significant upward bias.
      ## Also results in difference in data quality checks between ISCO vs. non-ISCO (as non-ISCO codes must all be assumed valid)
      ## NOTE: any survey that deviates from normal coding system besides using 9's for ambiguity (ex. ISSP
      ## using 0's or countries using their own 3- or 4-digit offshoots from ISCO) must be adjusted separately
      ## or else these numerators and/or denominators will not be accurate

      ## if survey's occ_codes were mapped, use presence of a valid occ_major to determine baseline validity
      if ("occ_major" %in% names(df)) {
        ## if survey was ISCO-88 or 08, can actually check for valid codes at 3- or 4-digit level
        if (df[1,occ_coding_system] %in% c("ISCO-88","ISCO-08")) {
          all_codes <- all_isco_4d[occ_coding_system == df[1,occ_coding_system],unique(substr(occ_code,1,df[1,occ_length]))]
          if (any(!df[,unique(occ_code)] %in% c(all_codes,NA))) {
            invalid_codes <- df[,unique(occ_code)][!df[,unique(occ_code)] %in% c(all_codes,NA)]
            warnings <- paste(warnings,paste0("Warning: ",filename," contains the invalid ",df[1,occ_coding_system]," codes: ",paste0(invalid_codes,collapse = ",")),sep="\n")
            warnings <- paste(warnings,paste0("These invalid codes make up ",round(df[occ_code %in% invalid_codes,.N]*100/df[!is.na(occ_code),.N],1),"% of all occ_codes"),sep="\n")

            ## create logfile of survey issues for reference later
            log[,occ_invalid := round(df[occ_code %in% invalid_codes,.N]/df[!is.na(occ_code),.N],5)]

            ## identify all invalid codes that were consistent with an HRH-relevant code up until
            ## the digit that caused the code to become invalid (going from left to right)
            criticals <- copy(invalid_codes)
            i <- df[1,occ_length] - 1
            while (i > 0) {
              ## only keep codes that, when moving back one digit, are either consistent with an HRH-relevant code or are
              ## still not yet a valid sub-code
              criticals <- criticals[substr(criticals,1,i) %in% hrh_system[,unique(substr(occ_code,1,i))] | !substr(criticals,1,i) %in% unique(substr(all_codes,1,i))]
              i <- i - 1
            }
            if (length(criticals) > 0) {
              warnings <- paste(warnings,paste0("And of these invalid codes the following are concerning for HRH: ",paste0(criticals,collapse=",")),sep="\n")
              warnings <- paste(warnings,paste0("These concerning codes make up ",round(df[occ_code %in% criticals,.N]*100/df[!is.na(occ_major),.N],1),"% of all occ_codes. May want to check if this survey requires truncation"),sep="\n")
              if (df[occ_code %in% criticals,.N]/df[!is.na(occ_major),.N]> 0.1) stop("Error: the percentage of concerning codes is too high, investigate!")
            } else {
              warnings <- paste(warnings,"None of these invalid codes is HRH-relevant",sep="\n")
            }
          }
        }
        if (!exists("criticals")) criticals <- character(0)

        ## If hrh-relevant invalid codes were found, exclude them from the denominator for those hrh indicators
        ## for which they could have been relevant (but for the other hrh indicators can still include them in denominator)
        ## Otherwise, only include valid occ_majors in denominator
        if (length(criticals) > 0) {

          ## cut down concerning codes to the length at which they are valid
          valid <- data.table(code = criticals,valid_code = criticals)
          critical_cp <- valid$valid_code
          i <- df[1,occ_length] - 1
          while (length(critical_cp) > 0) {
            valid[valid_code %in% critical_cp,valid_code := substr(valid_code,1,i)]
            critical_cp <- valid[!substr(valid_code,1,i) %in% hrh_system[,unique(substr(occ_code,1,i))],valid_code]
            i <- i - 1
          }

          ## assemble list of cadres affected by each ambiguous code
          cadre_merge <- data.table(valid_code = "000",cadre = "000")
          for (code in valid[,unique(valid_code)]) {
            cadre_merge <- rbind(cadre_merge,data.table(valid_code = code,cadre = hrh_system_cp[grepl(paste0("^",code),occ_code),unique(me_name)]))
          }
          cadre_merge <- cadre_merge[valid_code != "000"]
          valid <- merge(valid,cadre_merge,by="valid_code",allow.cartesian = T)

          ## for every cadre, include in the denominator everyone with a valid occ_major and whose occ_code
          ## does not make categorization into or out of that cadre ambiguous
          for (col in cadres) {
            df[!is.na(occ_major) & is.na(get(col)) & !occ_code %in% valid[cadre == col,code],(col) := 0]
          }

          ## create logfile of survey issues for reference later. For clarity, if affected cadres
          ## were 3-digit aggregates, also load column of underlying cadres affected
          setnames(valid,c("code","valid_code"),c("hrh_invalid","hrh_truncated"))
          valid <- merge(valid,hrh_3d_split,by="cadre",all.x=T,allow.cartesian = T)
          if (valid[!is.na(underlying_cadre),.N] > 0) valid[!is.na(underlying_cadre),c("interm_cadre","cadre") := list(cadre,underlying_cadre)]
          valid[,underlying_cadre := NULL]
          valid[,nid := df[1,nid]]

          ## calculate maximum possible effect of ambiguity on a single cadre
          hypoth <- merge(df[occ_code %in% criticals,list(occ_code)],valid[,list(occ_code = hrh_invalid,cadre)],by="occ_code",allow.cartesian = T)
          hypoth <- hypoth[,.N,by=cadre]
          hypoth[,prop_amb := round(N/df[!is.na(occ_major),.N],5)]
          warnings <- paste(warnings,paste0("Maximum ambiguity within any particular cadre is ",max(hypoth$prop_amb)*100,"% of all occ_codes"),sep="\n")
          valid <- merge(valid,hypoth[,list(cadre,prop_amb)],by="cadre")
          setcolorder(valid,c("hrh_invalid","hrh_truncated","cadre","prop_amb"))
          log[,amb_max := max(hypoth$prop_amb)]
          log <- merge(log,valid,by="nid")

        } else {
          df[!is.na(occ_major) & is.na(get(cadres[1])),(cadres) := 0]
        }
      } else {
        ## if survey bypassed occ_code mapping, must assume all occ_codes were valid and just use presence
        ## of occ_code to determine denominator
        df[!is.na(occ_code) & is.na(get(cadres[1])),(cadres) := 0]
      }
    } else {
      warnings <- paste(warnings,paste0("Warning: no HRH mapping available for ",df[1,occ_coding_system]," ",df[1,occ_length],"-digit "),sep="\n")
    }
  } else if ("occ_code" %in% names(df)) {
    warnings <- paste(warnings,"Bypassing HRH mapping",sep="\n")
  }

  ## Fill in hrh_any for those surveys that are already mappable to all included cadres
  all_cadres <- c("hrh_phys","hrh_clinic","hrh_opt","hrh_dent","hrh_pharm","hrh_diet","hrh_radio","hrh_pharmtech",
              "hrh_nurseass","hrh_midass","hrh_dentass","hrh_pcare","hrh_psych","hrh_therap","hrh_audio","hrh_trad",
              "hrh_amb","hrh_nurseprof","hrh_envir","hrh_medtech")
  # ^Make this no longer hardcoded (but be careful ex. isco codes and hrh_nurse_mid_ass)
  if (all(all_cadres %in% names(df))) {
    ## test if any rows are not NA, and if so sum to produce hrh_any column
    df[,test := rowSums(is.na(df[,all_cadres,with=F])) != length(all_cadres)]
    df[test == T,hrh_any := rowSums(df[test == T,all_cadres,with=F],na.rm=T)]
    df[,test := NULL]
  }


  # SAVE OUTPUTS ------------------------------------------------------------
  ## reorder columns
  survey_cols <- c("ihme_loc_id","nid","survey_module","survey_name","file_path","year_start","year_end","year_id")
  survey_cols <- survey_cols[survey_cols %in% names(df)]
  data_cols <- c("sex_id","age_year","pweight","strata","admin_1","admin_1_mapped","line_id","employed","military","temp_absent","extravar","missing_absent","missing_military",
         "occ_code","occ_length","occ_code_type","occ_coding_system","isco_approx","occ_code_label","occ_major","occ_major_label","occ_sub_major","occ_sub_major_label",
         "industry_code","industry_length","industry_code_type","coding_system","industry_code_label","major","major_label","major_label_short","minor","minor_label")
  data_cols <- data_cols[data_cols %in% names(df)]
  hrh_cols <- hrh[occ_coding_system %in% c("ISCO-08","ISCO-88"),unique(me_name)]
  hrh_cols <- hrh_cols[hrh_cols %in% names(df)]
  other_cols <- names(df)[!names(df) %in% c(survey_cols,data_cols,hrh_cols)]

  ## assume other cols are also survey_cols and reorder
  setcolorder(df,c(survey_cols,other_cols,data_cols,hrh_cols))

  ## order column values by employment, then occ, then industry
  if (all(c("occ_code","industry_code") %in% data_cols)) {
    df <- df[order(employed,occ_code,industry_code)]
  } else if ("occ_code" %in% data_cols) {
    df <- df[order(employed,occ_code)]
  } else if ("industry_code" %in% data_cols) {
    df <- df[order(employed,industry_code)]
  } else {
    df <- df[order(employed)]
  }

  ## output warnings
  cat(warnings)

  ## if worried about reviewing each output, prompt user's approval to write to file
  if (prompt == T) {
    acceptable <- readline(prompt="Are the above errors acceptable (y or n)? Or type q to quit or b to browse: ")

    while (acceptable == "b") {
      cat("\ntype 'c' to exit browser\n")
      browser()
      acceptable <<- readline(prompt="Are the above errors acceptable (y or n)? Or type q to quit or b to browse: ")
    }
  } else {
    acceptable <- "y"
  }

  ## write to file
  if (acceptable == "y") {
    if (grepl("census",tolower(filename))) {
      write.csv(df,file.path(out.dir,"census/new",gsub(".dta",".csv",filename)),row.names=F)
    } else {
      write.csv(df,file.path(out.dir,"regular/new",gsub(".dta",".csv",filename)),row.names=F)
    }

    ## write to logfile, overwriting any other logs from the same nid
    log[,warnings := gsub("\n",".   ",warnings)]
    existing_logs <- fread(file.path(out.dir,"mapping_logs.csv"))
    colorder <- names(existing_logs)
    existing_logs <- rbind(log,existing_logs[!(nid %in% log$nid & ihme_loc_id %in% log$ihme_loc_id & year_id %in% log$year_id)],fill=T)
    setcolorder(existing_logs,colorder)
    write.csv(existing_logs,file.path(out.dir,"mapping_logs.csv"),na = "",row.names = F)

    ## move ubcov extractions out of new folder, overwriting older versions where applicable
    file.copy(file.path(in.dir,filename),file.path(gsub("/new","",in.dir),filename),overwrite = T)
    file.remove(file.path(in.dir,filename))
  } else if (acceptable == "q") {
    stop("exiting...")
  } else {
    print("OK, NOT WRITING TO FILE")
  }
}
