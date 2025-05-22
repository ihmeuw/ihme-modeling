##' ***************************************************************************
##' Title: 1_full_registry_process.R
##' Purpose: Prepare registry datasets for crosswalks, prepare cv_livestill and 
##'          cv_excludes_chromos datasets for crosswalks, crosswalk registry datasets,
##'          and wipe bundle data to prepare for upload
##' To use: - Steps 1-4a can be run simply by sourcing. Ensure raw registries exist before
##'         moving on to prep for crosswalks.
##'         - If you wish to run funnel plots (step 4b) you must check to make sure 
##'         crosswalks have completed, then type 'repl_python()' in your console, followed
##'         by 'exit'. You can then run the remainder of step 3b - 5 by sourcing as normal.
##'         - If you wish to create diagnostics (step 6), check to ensure that your
##'         crosswalks have completed. Then source step 6 as normal.
##'         - Ensure that your diagnostics look as expected before wiping bundles
##'         and remember to archive your bundle data by specifying the archive argument.
##' Duration ~ 35 minutes if running full pipeline
##' ***************************************************************************



os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library(data.table)
library(ggplot2)
library(openxlsx)
library(dplyr)
library(stringr)
library(readxl)

# Source functions and function launch script
source(paste0(h, "FILEPATH", "1b_registry_cw_functions.R"))
source(paste0(h, "FILEPATH", "1a_registry_cw_function_launches.R"))
source(paste0(h, "FILEPATH", "process_eurocat.R"))


# Set constants
reg_cw_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
reg_cw_map <- reg_cw_map[target_registry != 'newzealand' & source_registry != 'newzealand' & !(target_registry == 'singapore' & bundle_id_output %in% c(437, 438)),
                         dummy_file_name := paste0(target_registry, '_to_', source_registry, '_', bundle_id_output)]
raw_registries_to_prep <- c("singapore", "congmalfworldwide", "icbdms", "nbdpn", "worldatlas", "china", "china_mortality")
target_registries <- unique(reg_cw_map[target_registry != 'newzealand']$target_registry)
source_registries <- unique(reg_cw_map[source_registry != 'newzealand']$source_registry)
congenital_registries <- c( "eurocat", "china", "china_mortality","congmalfworldwide", "icbdms", "nbdpn", "worldatlas")
congenital_registries_for_cw <- c("eurocat", "china", "congmalfworldwide", "icbdms", "nbdpn", "worldatlas", "singapore") 
map <- fread("FILEPATH")
bundles <- map[type == "congenital", bundle_id] #### whatever bundles we are uploading bundle data for
registry_bundles <-  'default' ### the bundles you are making new bundle data for from each registry (e.g. congenital heart bundles). if == 'default', will do all bundles for which each registry has data.
decomp_step <- 'iterative'
gbd_round_id <- 7
archive <- TRUE ##TRUE/FALSE depending on whether you want to save a copy of the bundle data you wipe


#Set arguments
#1
prep_raw_bundles <- FALSE ## TRUE/FALSE 
#2a
prep_cv_livestill <- FALSE ##TRUE/FALSE This must happen after prep_raw_bundles but can happen independently of all other functions
#2b
prep_cv_excludes_chromos <- FALSE ##TRUE/FALSE This must happen after prep_raw_bundles but can happen independently of all other functions
#3a
prep_reg_for_cw <- TRUE ##TRUE/FALSE
#3b
prep_sing_for_cw <- TRUE ##TRUE/FALSE
#4a
crosswalk_reg <- TRUE ##TRUE/FALSE
#4b
funnel_plots <- TRUE ##TRUE/FALSE
#5
aggregate_raw_data <- TRUE ##TRUE/FALSE, this will usually be TRUE if you just reprepped the data for crosswalking and are going to do the next step of applying the betas
#6
apply_crosswalks <- TRUE #TRUE/FALSE
#7
create_diagnostics <- 'pdf' #plotly/pdf/both
#8
wipe_buns <- FALSE #TRUE/FALSE



##' ******************************************************************************************
##' 0. shared functions, calculations for SE/mean/SS/cases/upper/lower, append pdf function
##' ******************************************************************************************
invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))

'%ni%' <- Negate('%in%')

measure <- 'prevalence'

## MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}
## STD ERROR
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[(is.na(standard_error) | standard_error == 0) & is.numeric(lower) & is.numeric(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[(is.na(standard_error) | standard_error == 0) & (measure == "prevalence" | measure == "proportion"),
     standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

## UPPER/LOWER
get_upper_lower <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[!is.na(mean) & !is.na(standard_error) & is.na(lower), lower := mean - (1.96 * standard_error)]
  dt[!is.na(mean) & !is.na(standard_error) & is.na(upper), upper := mean + (1.96 * standard_error)]
  return(dt)
}

append_pdf <- function(dir, starts_with) {
  files <- list.files(dir, pattern = paste0("^", starts_with), full.names = T)
  files <- paste(files, collapse = " ")
  cmd <- paste0("FILEPATH -dBATCH -dSAFER -dNOGC -DNOPAUSE -dNumRenderingThreads=4 -q -sDEVICE=pdfwrite -sOutputFile=",
                dir, "/", starts_with, ".pdf ", files)
  system(cmd)
}

##' *******************************************************
##' 1. prep_raw_bundles
##' Duration ~ 5 minutes
##' @param raw_registries_to_prep 
##' *******************************************************
if (prep_raw_bundles == TRUE){
  launch_prep_raw_bundles(raw_registries_to_prep = raw_registries_to_prep) #runs in a qsub
  launch_prep_raw_nzl() #runs in a qsub, **requires access to '/ihme/limited_use/IDENT/PROJECT_FOLDERS/NZL/BIRTH_DEFECTS_REGISTRY/'
  prep_raw_eurocat() #runs interactively, takes ~5 min
}


##' *******************************************************
##' 2a. prep_cv_livestill
##' 2b. prep_cv_excludes_chromos
##' Duration ~ 3 minutes
##' *******************************************************
if (prep_cv_livestill == TRUE){
  prep_covariates(registry = 'eurocat', covariate = 'cv_livestill') #runs interactively
  prep_covariates(registry = 'china', covariate = 'cv_livestill') #runs interactively
}
if (prep_cv_excludes_chromos == TRUE){
  prep_covariates(registry = 'eurocat', covariate = 'cv_excludes_chromos') #runs interactively
}

##' *******************************************************
##' 3a. prep_reg_for_cw
##' 3b. prep_sing_for_cw
##' Duration ~ 5.5 minutes
##' @param source_registries
##' *******************************************************
if (prep_reg_for_cw == TRUE){
  launch_prep_for_cw(source_registries = source_registries) # launches qsubs
}
if (prep_sing_for_cw == TRUE){
  launch_prep_singapore_for_cw() # launches qsubs
}

expected_dummy_files <- unique(reg_cw_map$dummy_file_name)
expected_dummy_files <- expected_dummy_files[!is.na(expected_dummy_files)]
dummy_dir <- "FILEPATH"

Sys.sleep(180)
if(length(list.files(dummy_dir, pattern = paste0('\\.csv$'))) == length(expected_dummy_files)){
  if (prep_reg_for_cw == TRUE ){launch_agg_dummy(source_registries = source_registries)} # launches qsubs
  if (prep_sing_for_cw == TRUE){launch_aggregate_singapore()} # launches qsubs
  agg_dummy_dir <- "FILEPATH"
}

Sys.sleep(90) 
if (prep_reg_for_cw == TRUE | prep_sing_for_cw == TRUE){
  if(length(list.files(agg_dummy_dir, pattern = paste0('\\.xlsx$'))) == length(expected_dummy_files)){
    combine_prepped(target_registries = target_registries) # runs interactively and takes about a minute to complete for all target registries
  }
}

##' *******************************************************
##' 4a. crosswalk_reg
##' Duration ~ 2 minutes
##' @param target_registries
##' *******************************************************
if (crosswalk_reg == TRUE){
  launch_crosswalks(target_registries = target_registries)
}

##' Check to make sure the crosswalks have completed before moving to step 4b.
##' *******************************************************
##' 4b. funnel_plots
##' Duration ~ 11 minutes
##' @param target_registries
##' *******************************************************
if (funnel_plots == TRUE){
  repl_python()
  # You must type 'exit' in your console -- this is a known inconvenience with the Crosswalk package and is under development
  exit
  # Run the funnel plots interactively
  make_funnel_plots(target_registries = target_registries) # this runs interactively and takes about 10 minutes to complete for all target registries
  combine_cw_detail_csv(target_registries = target_registries) # this runs interactively and takes about 1 min
}


##' *******************************************************
##' 5. aggregate_raw_data
##' Duration ~ 4 minutes
##' @param congenital_registries
##' *******************************************************
if (aggregate_raw_data == TRUE){
  launch_aggregate_raw(congenital_registries = congenital_registries) # this runs in a qsub, takes ~3 min
  launch_aggregate_sing_lvl2() # this runs in a qsub, takes 1 min
  aggregate_nzl_lvl2() # this runs interactively, takes ~ 1 min
}

##' *******************************************************
##' 6. apply_crosswalks
##' Duration ~ 6 minutes
##' @param cogenital_registries
##' @param registry_bundles. The bundles for which a registry tracks data that we are uploading. Usually "default". 
##' @param bundles. Bundles which we are uploading data for
##' *******************************************************
if (apply_crosswalks == TRUE){
  launch_apply_betas(congenital_registries = congenital_registries_for_cw, registry_bundles = registry_bundles) # runs in a qsub, takes ~3 minutes for all bundles
  Sys.sleep(180)
  if(length(list.files("FILEPATH", pattern = paste0('\\_CURRENT.xlsx$'))) == length(congenital_registries_for_cw)){
    launch_assemble_bundles(congenital_registries = congenital_registries_for_cw, bundles = bundles) #runs in a qsub, takes ~1 min
  }
}

##' *************************************************************************************
##' 7. create_diagnostics
##' Duration (just for pdfs) ~1.5 min
##' @param bundles Bundles for which you wish to make a scatter and violin plot diagnostic
##' *************************************************************************************
if (create_diagnostics == 'plotly'){
  source(paste0(h, "FILEPATH", "diagnostics_reg_crosswalks.Rmd"))
  render(paste0(h, "FILEPATH", "diagnostics_reg_crosswalks.Rmd"), output_format = 'html_document')
}

if (create_diagnostics == 'pdf'){
  launch_pdf_diagnostics(bundles = bundles)
}

if (create_diagnostics == 'both'){
  # create plotlys
  source(paste0(h, "FILEPATH", "diagnostics_reg_crosswalks.Rmd"))
  render(paste0(h, "FILEPATH", "diagnostics_reg_crosswalks.Rmd"), output_format = 'html_document')
  
  # create pdf
  launch_pdf_diagnostics(bundles = bundles)
}

##' *******************************************************
##' 8. wipe_buns
##' Duration ~ Varies
##' @param bundles Bundles for which you wish to clear all bundle data.
##' @param archive If you want to archive the current bundle data as a csv file
##' @param decomp_step 
##' @param gbd_round_id
##' *******************************************************
if (wipe_buns == TRUE){
  launch_wipe_bundle_data(bundles = bundles, archive = archive, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
}




