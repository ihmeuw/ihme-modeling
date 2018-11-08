############################################################################################################
## Purpose: Launches topic-specific code for contraception ubcov (non-counterfactual) extraction
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

pacman::p_load(data.table,magrittr,ggplot2,haven,stringr,parallel)

cores.provided <- 5

## in/out
input_dir <- file.path(j,"FILEPATH")
output_dir <- file.path(j,"FILEPATH")
dir.create(output_dir,recursive = T,showWarnings = F)
age_map <- fread(file.path(j,"FILEPATH"))

## create the opposite of %in%
'%ni%' <- Negate('%in%')

## set values for gateways corresponding to counterfactual extractions (turn them all off for normal extraction)
counterfac_currmar <- 0
counterfac_evermar <- 0
counterfac_missing_fecund <- 0
counterfac_missing_desire <- 0
counterfac_missing_desire_later <- 0
counterfac_no_pregppa <- 0

## list all surveys
ubcov_all <- list.files(input_dir)

extract_survey <- function(survey) {
  print(survey)

  ## Read in survey
  df <- as.data.table(read_dta(file.path(input_dir, survey)))

  ## by default, study-level covariates are false unless otherwise specified
  if ("currmar_only" %ni% names(df)) df[,currmar_only := 0]
  if ("evermar_only" %ni% names(df)) df[,evermar_only := 0]
  if ("missing_fecund" %ni% names(df)) df[,missing_fecund := 0]
  if ("missing_desire" %ni% names(df)) df[,missing_desire := 0]
  if ("missing_desire_later" %ni% names(df)) df[,missing_desire_later := 0]
  if ("no_pregppa" %ni% names(df)) df[,no_pregppa := 0]

  ## convert string variables with accents into non-accented characters
  df[,current_contra := gsub("\x82|\xe9","e",current_contra)]
  df[,current_contra := gsub("\U3e33663c","o",current_contra)]
  df[,current_contra := gsub("\xf3|\xf4","o",current_contra)]
  df[,current_contra := gsub("\U3e64653c","i",current_contra)]
  df[,current_contra := gsub("\xed","i",current_contra)]
  df[,current_contra := str_replace_all(iconv(current_contra, to="ASCII//TRANSLIT"),"'","")]
  if ("reason_no_contra" %in% names(df)) {
    df[,reason_no_contra := gsub("\x82|\xe9","e",reason_no_contra)]
    df[,reason_no_contra := gsub("\U3e33663c","o",reason_no_contra)]
    df[,reason_no_contra := gsub("\xf3|\xf4","o",reason_no_contra)]
    df[,reason_no_contra := gsub("\U3e64653c","i",reason_no_contra)]
    df[,reason_no_contra := gsub("\xed","i",reason_no_contra)]
    df[,reason_no_contra := str_replace_all(iconv(reason_no_contra, to="ASCII//TRANSLIT"),"'","")]
  }

  if ("NA" %in% unique(df$current_contra)) print(paste0("MAY BE A CHARACTER MAPPING ISSUE IN ", survey))

  ## run topic-specific extraction
  source(file.path(j,"FILEPATH"),local = T)

  ## merge on all potential age groups for the collapse code
  df[,age_year := floor(age_year)]
  df <- merge(df,age_map,by="age_year",allow.cartesian = T)

  ## write output file
  write.csv(df,file.path(output_dir,str_replace(survey,".dta",".csv")),row.names = F)
}
