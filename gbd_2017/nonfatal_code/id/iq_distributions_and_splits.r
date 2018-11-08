#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: PART ONE - Run DisMod model for the prevalence of intellectual disability (as a proportion of the population with IQ < 70)
#          PART TWO - Run DisMod model for the mean IQ by country, year, age, and sex
#          PART THREE - Generate IQ distributions based on the mean score and the proportion below 70
#          PART FOUR - Split the IQ distributions into the 5 severity splits for GBD
#          PART FIVE - Format and upload to the epi database
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "FILEPATH" 
  h_root <- file.path("FILEPATH", username)
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
### load packages
source("FILEPATH/load_packages.R")
load_packages(c("magrittr", "data.table", "stats", "scales", "proto", "findpython", "getopt", "argparse"))

### setup arguments
parser <- ArgumentParser()
parser$add_argument("--action", help="what to do (init or launch)?", type="character")
parser$add_argument("--pop_locs", help="location_id to loop over", type="integer")

### save job args as objects
args <- parser$parse_args()
list2env(args, environment())
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set method for calculation
method <- "meta_analysis" # use a meta-analysis final, not beta distribution!
mark_model_best <- FALSE

### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")
save_results_description <- paste0("DM cascade updates, meta-analysis for all splits, using GBD 2016 ID dataset", " - custom version ", custom_version)
mark_model_best <- TRUE
gbd_round <- 5
year_end  <- gbd_round + 2012

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_gbd    <- paste0("draw_", draw_nums_gbd)
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd)

### directories
home <- file.path(j_root, "FILEPATH")
j.version.dir <- file.path(home, "_models", custom_version)
if (!dir.exists(j.version.dir)) dir.create(j.version.dir)
cl.version.dir <- file.path("FILEPATH", custom_version)
if (!dir.exists(cl.version.dir)) dir.create(cl.version.dir)
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### custom functions
file.path(j_root, "FILEPATH/cluster_tools.r") %>% source

### load shared functions
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_draws.R") %>% source
#***********************************************************************************************************************


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22)[, 
                   .(location_id, ihme_loc_id, location_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]

### prep mes for upload
mes_map <- data.table(me_id=9423:9427, severity=c("borderline", "mild", "moderate", "severe", "profound"))

### launch jobs to plot
if (action=="init") {
  
  pop_locs <- unique(locations[level >= 3, location_id])

  ### save description
  cat(save_results_description, file=file.path(j.version.dir, "model_description.txt"))
  
  ### launch jobs
  JOBS <- NULL
  pop_locs <- pop_locs[!pop_locs %in% gsub(".csv", "", list.files(file.path(cl.version.dir, "profound/draws")))]
  for (loc in pop_locs) {
    
    slots          <- 5
    shell          <- "FILEPATH/r_shell.sh"
    script         <- "FILEPATH/iq_distributions_and_splits.r"
    sge_output_dir <- paste0("-o /FILEPATH/", username, " -e /FILEPATH/", username)
    job_name       <- paste0("-N ", "id_", method, "_", loc)
    
    job <- paste("qsub -P PROJECT", sge_output_dir, job_name, "-pe multi_slot", slots, shell, script, 
                 "--args", "--action launch", "--pop_locs", loc)
    system(job); print(job)
      
    JOBS <- c(JOBS, file.path(cl.version.dir, "profound", "draws", paste0(loc, ".csv")))
  }
  
  ### upload function
  upload_id_splits <- function(severity_split) {
    
    ### set save directory
    cl.version.dir.split <- file.path(cl.version.dir, severity_split, "draws")
    me_id <- mes_map[severity==severity_split, me_id]
    
    ### upload results
    years <- fread(list.files(cl.version.dir.split, full.names=TRUE)[1])$year_id %>% unique
    save_results_description_split <- paste0(severity_split, " - ", save_results_description)
    job <- paste0("qsub -N save_id_", severity_split, " -pe multi_slot 30 -P PROJECT -o /FILEPATH", username, " -e /FILEPATH", username, " ",
                  "/FILEPATH/r_shell.sh /FILEPATH/save_results_wrapper.r",
                  " --args",
                  " --type epi",
                  " --me_id ", me_id,
                  " --input_directory ", cl.version.dir.split,
                  " --descript ", "\"", gsub(" ", "_", save_results_description_split), "\"",
                  " --measure_epi 5",
                  " --year_ids ", paste(years, collapse=","), 
                  " --best ", mark_model_best)
    system(job); print(job)
    
  }
  
  ### upload
  job_hold(paste0("id_", method, "_"), file_list=JOBS); print("DONE!!")
  upload_id_splits("borderline")
  upload_id_splits("mild")
  upload_id_splits("moderate")
  upload_id_splits("severe")
  upload_id_splits("profound")

} else if (action=="launch") {
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART ONE: DisMod MODEL FOR ID PREV ###############################################################################
  ########################################################################################################################
  
  
  #----PULL---------------------------------------------------------------------------------------------------------------
  ### get ID prevalence draws from DisMod model
  id_prev <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2420, source="epi", location_id=pop_locs, sex_id=1:2, 
                       age_group_id=c(2:20, 30:32, 235), measure_id=5, gbd_round_id=gbd_round, status="best")
  # save model version
  cat(paste0("Intellectual disability prevalence DisMod model (me_id 2420) - model run ", unique(id_prev$model_version_id)),
      file=file.path(j.version.dir, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  # remove excess columns
  id_prev <- id_prev[, draw_cols_upload, with=FALSE]
  colnames(id_prev) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("id_draw_", draw_nums_gbd))
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART TWO: DisMod MODEL FOR IQ MEAN ###############################################################################
  ########################################################################################################################
  
  
  #----PULL---------------------------------------------------------------------------------------------------------------
  if (method != "meta_analysis") {
    
    ### get IQ mean draws from DisMod model -- (continuous model measure_id=19)
    iq_means <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=10571, source="epi", location_ids=pop_locs, sex_ids=1:2,
                          age_group_ids=c(2:20, 30:32, 235), measure_ids=19, gbd_round_id=gbd_round, status="best")
    # save model version
    cat(paste0("IQ mean DisMod model (me_id 10571) - model run ", unique(iq_means$model_version_id)),
        file=file.path(j.version.dir, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    iq_means <- iq_means[, draw_cols_upload, with=FALSE]
    colnames(iq_means) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("iq_draw_", draw_nums_gbd))
    
    ### bring together both components
    id <- merge(iq_means, id_prev, by=c("location_id", "year_id", "age_group_id", "sex_id"))
    
  }
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART THREE: INFORM IQ DISTRIBUTION ###############################################################################
  ########################################################################################################################
  
  
  #----PREP---------------------------------------------------------------------------------------------------------------
  if (method=="normal") {
    
    ### calculate standard deviations of the location-, year-, age-, and sex-specific IQ distributions as:
    ### z <- qnorm(id_prev)
    ### sd <- (70 - iq_mean) / z
    sd_draw_cols <- paste0("sd_draw_", draw_nums_gbd)
    id[, (sd_draw_cols) := lapply(draw_nums_gbd, function(x) ( 70 - get(paste0("iq_draw_", x)) ) / qnorm( get(paste0("id_draw_", x)) ))]
    
  } else if (method=="beta") {
    
    ### rescaling function
    resc_iq <- function(x) rescale(x, c(0, 1), c(0, 200))
    
    ### function to solve for alpha and beta shape params
    find_betadist <- function(location, prob, alpha) {
      beta <- (alpha / location) - alpha
      abs(pbeta(resc_iq(70), shape1=alpha, shape2=beta, lower.tail=TRUE) - prob)
    }
    
    ### function to loop over each row in the data.table
    optimize <- Vectorize( function(mean, prop) {
      return(optim(40, find_betadist, location=mean, prob=prop)$par)
    } )
    
    ### calculate alpha
    alpha_draw_cols <- paste0("alpha_draw_", draw_nums_gbd)
    id[, (alpha_draw_cols) := lapply(draw_nums_gbd, function(x) optimize( mean=resc_iq(get(paste0("iq_draw_", x))), prop=get(paste0("id_draw_", x)) ) )]
    
    ### calculate beta
    beta_draw_cols <- paste0("beta_draw_", draw_nums_gbd)
    id[, (beta_draw_cols) := lapply(draw_nums_gbd, function(x) ( get(paste0("alpha_draw_", x)) / resc_iq(get(paste0("iq_draw_", x))) ) - get(paste0("alpha_draw_", x)) )]
    
  }
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART FOUR: SEVERITY SPLITS #######################################################################################
  ########################################################################################################################
  
  
  #----CALCULATE----------------------------------------------------------------------------------------------------------
  ### then, calculate the proportion of the population (i.e. prevalence) of each severity as:
  
  if (method=="normal") {
    
    ### pnorm(LEVEL, mean=iq_mean, sd=sd, lower.tail=TRUE)
    
    ### borderline ID: 70 <= IQ < 85
    borderline <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    borderline[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pnorm(85, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE) - 
                                             pnorm(69.9999999999, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE))]
    borderline <- borderline[, draw_cols_upload, with=FALSE]
    
    ### mild ID: 50 <= IQ < 70
    mild <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    mild[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pnorm(70, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE) - 
                                       pnorm(49.9999999999, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE))]
    mild <- mild[, draw_cols_upload, with=FALSE]
    
    ### moderate ID: 35 <= IQ < 50
    moderate <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    moderate[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pnorm(50, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE) - 
                                           pnorm(34.9999999999, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE))]
    moderate <- moderate[, draw_cols_upload, with=FALSE]
    
    ### severe ID: 20 <= IQ < 35
    severe <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    severe[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pnorm(35, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE) - 
                                         pnorm(19.9999999999, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE))]
    severe <- severe[, draw_cols_upload, with=FALSE]
    
    ### profound ID: IQ < 20
    profound <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    profound[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pnorm(20, mean=get(paste0("iq_draw_", x)), sd=get(paste0("sd_draw_", x)), lower.tail=TRUE))]
    profound <- profound[, draw_cols_upload, with=FALSE]
    
  } else if (method=="beta") {
  
    ### pbeta(LEVEL, shape1=alpha, shape2=beta, lower.tail=TRUE) per https://stat.ethz.ch/R-manual/R-devel/library/stats/html/Beta.html
  
    ### borderline ID: 70 <= IQ < 85
    borderline <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    borderline[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pbeta(resc_iq(85), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE) - 
                                             pbeta(resc_iq(69.9999999999), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE))]
    borderline <- borderline[, draw_cols_upload, with=FALSE]
    
    ### mild ID: 50 <= IQ < 70
    mild <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    mild[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pbeta(resc_iq(70), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE) - 
                                       pbeta(resc_iq(49.9999999999), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE))]
    mild <- mild[, draw_cols_upload, with=FALSE]
    
    ### moderate ID: 35 <= IQ < 50
    moderate <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    moderate[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pbeta(resc_iq(50), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE) - 
                                           pbeta(resc_iq(34.9999999999), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE))]
    moderate <- moderate[, draw_cols_upload, with=FALSE]
    
    ### severe ID: 20 <= IQ < 35
    severe <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    severe[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pbeta(resc_iq(35), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE) - 
                                         pbeta(resc_iq(19.9999999999), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE))]
    severe <- severe[, draw_cols_upload, with=FALSE]
    
    ### profound ID: IQ < 20
    profound <- copy(id)[, (paste0("id_draw_", draw_nums_gbd)) := NULL]
    profound[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) pbeta(resc_iq(20), shape1=get(paste0("alpha_draw_", x)), shape2=get(paste0("beta_draw_", x)), lower.tail=TRUE))]
    profound <- profound[, draw_cols_upload, with=FALSE]
    
  } else if (method=="meta_analysis") {
    
    ### get severity splits from meta-analysis
    # differs by high-income status (binary)
    if (locations[location_id==pop_locs, super_region_id]==64) {hic <- 1; income_level <- "HIC"} else {hic <- 0; income_level <- "LMIC"}
    # splits <- haven::read_dta("FILEPATH/sev_props.dta") %>% data.table %>%
    #   .[HIC==hic, ]
    splits <- haven::read_dta("FILEPATH/sev_props.dta") %>% data.table %>%
      .[income==income_level, ]
    
    ### borderline ID: 70 <= IQ < 85
    borderline <- copy(id_prev)
    borderline[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("id_draw_", x)) * splits[healthstate=="id_bord", get(paste0("v", x))] )]
    borderline <- borderline[, draw_cols_upload, with=FALSE]
    
    ### mild ID: 50 <= IQ < 70
    mild <- copy(id_prev)
    mild[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("id_draw_", x)) * splits[healthstate=="id_mild", get(paste0("v", x))] )]
    mild <- mild[, draw_cols_upload, with=FALSE]
    
    ### moderate ID: 35 <= IQ < 50
    moderate <- copy(id_prev)
    moderate[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("id_draw_", x)) * splits[healthstate=="id_mod", get(paste0("v", x))] )]
    moderate <- moderate[, draw_cols_upload, with=FALSE]
    
    ### severe ID: 20 <= IQ < 35
    severe <- copy(id_prev)
    severe[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("id_draw_", x)) * splits[healthstate=="id_sev", get(paste0("v", x))] )]
    severe <- severe[, draw_cols_upload, with=FALSE]
    
    ### profound ID: IQ < 20
    profound <- copy(id_prev)
    profound[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("id_draw_", x)) * splits[healthstate=="id_prof", get(paste0("v", x))] )]
    profound <- profound[, draw_cols_upload, with=FALSE]
    
  }
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART FIVE: UPLOAD ################################################################################################
  ########################################################################################################################
  
  
  #----UPLOAD-------------------------------------------------------------------------------------------------------------
  
  
  ### upload function
  save_id_splits <- function(severity_split) {
    
    ### set save directory
    cl.version.dir.split <- file.path(cl.version.dir, severity_split, "draws")
    if (!dir.exists(cl.version.dir.split)) dir.create(cl.version.dir.split, recursive=TRUE)
    me_id <- mes_map[severity==severity_split, me_id]
    
    ### format nonfatal for como
    get(severity_split)[, measure_id := 5]
    
    ### save to /share directory 
    lapply(unique(get(severity_split)$location_id), function(x) write.csv(get(severity_split)[location_id==x],
                                file.path(cl.version.dir.split, paste0(x, ".csv")), row.names=FALSE))
    print(paste0("nonfatal estimates saved in ", cl.version.dir.split))
    
  }
  
  ### upload
  save_id_splits("borderline")
  save_id_splits("mild")
  save_id_splits("moderate")
  save_id_splits("severe")
  save_id_splits("profound")
  #***********************************************************************************************************************
  
}
