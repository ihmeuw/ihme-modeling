####################################################################################################
##
## Purpose: Compile gold standard contraception surveys in one folder for use in crosswalks
##
####################################################################################################

# clear memory
rm(list=ls())
library(data.table)
library(magrittr)
library(haven)
library(stringr)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# create the opposite of %in%
'%ni%' <- Negate('%in%')

# in/out
# CONTAINS ALL PUBLIC MICRODATA
input_dir <- file.path(j_root,"FILEPATH")
output_dir <- file.path(j_root,"FILEPATH")
dir.create(output_dir,recursive = T,showWarnings = F)

ubcov_all <- list.files(input_dir)

# compile folder of all gold-standard surveys to prepare for counterfactual re-extractions
for (survey in ubcov_all) {
  df <- read_dta(file.path(input_dir, survey)) %>% data.table

  if (all(c("currmar_only","evermar_only","missing_fecund","missing_desire","missing_desire_later","no_pregppa") %ni% names(df))){
    print(survey)
    
    df[,current_contra := str_replace_all(iconv(current_contra, to="ASCII//TRANSLIT"),"'","")]
    if ("reason_no_contra" %in% names(df)) df[,reason_no_contra := str_replace_all(iconv(reason_no_contra, to="ASCII//TRANSLIT"),"'","")]
    
    write.csv(df,file.path(output_dir,str_replace(survey,".dta",".csv")),row.names=F)
  }
}

####################################################################################################
# Perform counterfactual extractions
####################################################################################################

ubcov_all <- list.files(output_dir)

# list all counterfactual scenarios
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
  
  # set up values needed for different gateways within contraception_ubcov_calc
  counterfac_currmar <- ifelse(grepl("currmar",counterfactual),1,0) 
  counterfac_evermar <- ifelse(grepl("evermar",counterfactual),1,0)
  counterfac_missing_fecund <- ifelse(grepl("fecund",counterfactual),1,0) 
  counterfac_missing_desire <- ifelse(grepl("desire",counterfactual) & !grepl("desire_later",counterfactual),1,0)
  counterfac_missing_desire_later <- ifelse(grepl("desire_later",counterfactual),1,0)
  counterfac_no_pregppa <- ifelse(grepl("no_pregppa",counterfactual),1,0)
  
  # create output folder
  final_output_dir <- file.path(j_root,"FILEPATH",counterfactual)
  dir.create(final_output_dir,recursive = T,showWarnings = F)
  
  for (survey in ubcov_all) {
    print(survey)
    df <- fread(file.path(output_dir,survey))
    
    # run topic-specific code
    source(file.path(j_root,"FILEPATH"))
    
    # write output file
    write.csv(df,file.path(final_output_dir,str_replace(survey,".dta",".csv")),row.names = F)
  }
}

