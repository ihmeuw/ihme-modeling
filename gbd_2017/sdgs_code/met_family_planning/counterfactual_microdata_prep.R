############################################################################################################
## Purpose: Compile gold standard contraception surveys in one folder. Then, for every combination
##          of possible missing variables perform a counterfactual re-extraction of the gold-standard
##          surveys to inform our crosswalk of non-gold-standard surveys
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

## in/out
input_dir <- file.path(j,"FILEPATH")
output_dir <- file.path(j,"FILEPATH")
dir.create(output_dir,recursive = T,showWarnings = F)
final_dir_root <- file.path(j,"FILEPATH")
age_map <- fread(file.path(j,"FILEPATH"))

## settings
file_prep <- T ## do files need to be identified as gold standard and saved to appropriate folder first?
cores.provided <- 5

## create the opposite of %in%
'%ni%' <- Negate('%in%')

if (file_prep){
  ## list all microdata surveys
  ubcov_all <- list.files(input_dir)

  ## compile folder of all gold-standard surveys to prepare for counterfactual re-extractions
  extract_survey <- function(survey) {
    ## read in microdata survey
    df <- read_dta(file.path(input_dir, survey)) %>% data.table

    ## check whether survey is gold standard (not marked as missing any necessary variables)
    if (all(c("currmar_only","evermar_only","missing_fecund","missing_desire","missing_desire_later","no_pregppa") %ni% names(df))) {
      print(survey)

      ## convert string variables with accents into non-accented characters
      df[,current_contra := gsub("\x82|\xe9","e",current_contra)]
      df[,current_contra := gsub("\U3e33663c","o",current_contra)]
      df[,current_contra := gsub("\xf3|\xf4","o",current_contra)]
      df[,current_contra := gsub("\U3e64653c","i",current_contra)]
      df[,current_contra := gsub("\xed","i",current_contra)]
      df[,current_contra := str_replace_all(iconv(current_contra, from="latin1", to="ASCII//TRANSLIT"),"'","")]
      if ("reason_no_contra" %in% names(df)) {
        df[,reason_no_contra := gsub("\x82|\xe9","e",reason_no_contra)]
        df[,reason_no_contra := gsub("\U3e33663c","o",reason_no_contra)]
        df[,reason_no_contra := gsub("\xf3|\xf4","o",reason_no_contra)]
        df[,reason_no_contra := gsub("\U3e64653c","i",reason_no_contra)]
        df[,reason_no_contra := gsub("\xed","i",reason_no_contra)]
        df[,reason_no_contra := str_replace_all(iconv(reason_no_contra, from="latin1", to="ASCII//TRANSLIT"),"'","")]
      }

      ## save gold standard files to their own folder
      write.csv(df,file.path(output_dir,str_replace(survey,".dta",".csv")),row.names=F)
    }
  }

  mclapply(ubcov_all,extract_survey,mc.cores = cores.provided)
}


####################################################################################################
# Perform counterfactual extractions
####################################################################################################

## list all gold sdtandard surveys
ubcov_all <- list.files(output_dir)

## list all possible combinations of missing variables. Will do a separate counterfactual extraction for each
counterfactuals <- c("currmar",
                     "evermar",
                     "fecund",
                     "desire",
                     "desire_later",
                     "no_pregppa",
                     "currmar_desire",
                     "currmar_desire_later",
                     "evermar_desire",
                     "evermar_desire_later",
                     "currmar_fecund",
                     "currmar_no_pregppa",
                     "evermar_fecund",
                     "evermar_no_pregppa",
                     "fecund_desire",
                     "fecund_desire_later",
                     "desire_no_pregppa",
                     "desire_later_no_pregppa",
                     "currmar_fecund_desire",
                     "currmar_fecund_desire_later",
                     "currmar_desire_no_pregppa",
                     "currmar_desire_later_no_pregppa",
                     "evermar_fecund_desire",
                     "evermar_fecund_desire_later",
                     "evermar_desire_no_pregppa",
                     "evermar_desire_later_no_pregppa")

for (counterfactual in counterfactuals) {
  print(paste0("COUNTERFACTUAL EXTRACTION FOR ",toupper(counterfactual)))

  ## set up variables corresponding to which counterfactual extraction this is (correspond to gateways in contraception_ubcov_calc.R)
  counterfac_currmar <- ifelse(grepl("currmar",counterfactual),1,0)
  counterfac_evermar <- ifelse(grepl("evermar",counterfactual),1,0)
  counterfac_missing_fecund <- ifelse(grepl("fecund",counterfactual),1,0)
  counterfac_missing_desire <- ifelse(grepl("desire",counterfactual) & !grepl("desire_later",counterfactual),1,0)
  counterfac_missing_desire_later <- ifelse(grepl("desire_later",counterfactual),1,0)
  counterfac_no_pregppa <- ifelse(grepl("no_pregppa",counterfactual),1,0)

  ## create the folder for saving the output from this counterfactual extraction
  final_output_dir <- file.path(final_dir_root,counterfactual,"new")
  dir.create(final_output_dir,recursive = T,showWarnings = F)

  ## re-extract every gold-standard survey according to this counterfactual scenario
  reextract_survey <- function(survey) {
    print(survey)
    df <- fread(file.path(output_dir,survey))

    ## If a gold standard survey doesn't have the information necessary for
    ## a counterfactual extraction that requires women to express desire for a child soon/now,
    ## exclude it from those corresponding counterfactual extractions
    if (counterfac_missing_desire_later == 1 & "desire_soon" %ni% names(df)) return()

    ## run topic-specific code
    source(file.path(j,"FILEPATH"),local = T)

    ## merge on all potential age groups for the collapse code
    df[,age_year := floor(age_year)]
    df <- merge(df,age_map,by="age_year",allow.cartesian = T)

    ## write output file
    write.csv(df,file.path(final_output_dir,str_replace(survey,".dta",".csv")),row.names = F)
  }

  mclapply(ubcov_all,reextract_survey,mc.cores = cores.provided) %>% invisible
}
