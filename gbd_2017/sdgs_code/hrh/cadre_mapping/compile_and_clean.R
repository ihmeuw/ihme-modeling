############################################################################################################
## Purpose: compile collapsed hrh cadre estimates and clean data before modeling
###########################################################################################################

## clear memory
rm(list=ls())

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

pacman::p_load(data.table,magrittr,parallel)

collapse.version <- 1

# T for compiling all data right after collapse, F for running direct prep for intermediate ST-GPR models
first_prep <- T

if (first_prep){

  ## load locations to merge on
  source(file.path(j,"FILEPATH"))
  locations <- get_location_metadata(gbd_round_id = 5, location_set_id = 22)
  locs <- locations[, c("location_id","ihme_loc_id"), with=F]

  ## in/out
  in.dir <- file.path(j, "FILEPATH")
  out.dir <- file.path(j, "FILEPATH")
  in.files <- list.files(in.dir,full.names = T)
  in.files.sex <- in.files[grepl("hrh_",in.files) & grepl("_sex",in.files)]
  in.files <- in.files[grepl("hrh_",in.files) & !grepl("_sex",in.files)]

  ## Sex-Aggregated Data ##
  df <- rbindlist(lapply(in.files,fread),fill = T)
  df <- df[ihme_loc_id != ""]
  df <- merge(df,locs,by="ihme_loc_id")

  ## create standard ST-GPR variables
  df[,data := mean]
  df[,variance := standard_error**2]
  df[,year_id := floor((year_start + year_end)/2)]
  df[,age_group_id := 22]
  df[,sex_id := 3]

  ## where variance is non-existent or unexplainably low (ex. Survey of Adult Skills), impute it
  low_var <- df[!grepl("survey_adult_skills",tolower(survey_name)),min(variance,na.rm = T)]
  df[variance < low_var & data != 0, variance := NA]
  df[is.na(variance), variance := data*(1-data)/sample_size] # check on this

  ## write output for whole file for use in sourcing
  write.csv(df,file.path(j,"FILEPATH"),row.names=F)

  ## write cadre-specific files for modeling
  for (categ in unique(df$var)){
    dt <- df[var == categ]
    dt[,me_name := categ]
    write.csv(dt,file.path(out.dir,paste0(categ,"_",collapse.version,".csv")),row.names=F)
  }

  ## Sex-Disaggregated Data ##
  df <- rbindlist(lapply(in.files.sex,fread),fill = T)
  df <- df[ihme_loc_id != ""]
  df <- merge(df,locs,by="ihme_loc_id")

  ## create standard ST-GPR variables
  df[,data := mean]
  df[,variance := standard_error**2]
  df[is.na(variance), variance := data*(1-data)/sample_size] # check on this
  df[,year_id := floor((year_start + year_end)/2)]
  df[,age_group_id := 22]

  for (categ in unique(df$var)){
    dt <- df[var == categ]
    dt[,me_name := paste0(categ,"_sex")]
    write.csv(dt,file.path(out.dir,paste0(categ,"_sex_",collapse.version,".csv")),row.names=F)
  }

} else { ## CLEAN DATA BEFORE MODELING ##
  in.dir <- "FILEPATH"

  topics <- list.files(file.path(in.dir,"hrh"))
  topics <- topics[!grepl("prop_emp|prop_ofemp|clean_split_template",topics)]

  for (topic in topics){
    df <- fread(file.path(in.dir,"hrh",topic))

    ## multiply all cadre-specific data by 10000
    if (!grepl("hrh_any",topic)) {
      df[,data := data*10000]
      df[,variance := variance*(10000**2)]
    }

    ## Output Full Datasets ##
    ## nurseprof is only category directly mappable in all data (no need for splits)
    if (grepl("nurseprof",topic)) write.csv(df,file.path(in.dir,"hrh_final",topic),row.names=F)
    ## write out rescaled intermediate datasets before dropping any data
    write.csv(df,file.path(in.dir,"hrh_interm",topic),row.names=F)

    ## Output Cleaned Datasets ##
    ## sample sizes below 100 were extremely variable, and typically subnationals that
    ## differed substantially from national, even after raking
    df <- df[sample_size > 100]

    df <- df[!grepl("ISSP",survey_name) | !grepl("_",ihme_loc_id)] ## ISSP subnational estimates were wildly unstable

    ## write out skeleton of all surveys with sample_size above the cutoff that need a 3-digit split
    ## (for now, happens to coincide with all surveys with nurse_mid_assoc estimates)
    if (grepl("nurse_mid_ass_sex",topic)) {
      write.csv(df,file.path(in.dir,"hrh/clean_split_template_sex.csv"),row.names=F)
    } else if (grepl("nurse_mid_ass",topic)) {
      write.csv(df,file.path(in.dir,"hrh/clean_split_template.csv"),row.names=F)
    }

    df <- df[mean != 0] # remove estimates of zero due to instability it introduces into models

    ## nurseprof is only category directly mappable in all data (no need for splits)
    if (grepl("nurseprof",topic)) write.csv(df,file.path(in.dir,"hrh_final", gsub(".csv","_clean.csv",topic)),row.names=F)

    ## write out cleaned intermediate datasets
    write.csv(df,file.path(in.dir,"hrh_interm",gsub(".csv","_clean.csv",topic)),row.names=F)
  }
}
