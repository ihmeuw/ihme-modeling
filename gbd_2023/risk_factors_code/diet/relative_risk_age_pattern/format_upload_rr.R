##################################################################################
## DESCRIPTION: Read in a csv of draws that was created from draws 
##              apply age pattern for CVD outcomes,
##############################################################################
rm(list = ls())
# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FIELPATH", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, "FILEPATH")
share_dir <- "FILEPATH" 
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

## LOAD DEPENDENCIES -----------------------------------------------------
library(data.table)
library(argparse)
# load in central functions
source('FILEPATH/format_mrbrt_rr_results.R')
source("FILEPATH/save_results_risk.R")
source("FILEPATH/get_rei_metadata.R")

# load in my utility functions

source(paste0(code_dir, "FILEPATH/helper_functions.R"))
source(paste0(code_dir, "FILEPATH/age_trends_functions.R")) 

## PARSE ARGS ------------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--risk_rei", help = "name of risk to evaluate for",
    default = "diet_risk", type = "character")
parser$add_argument("--diet_ro_pair_map", help = "path to risk-outcome pair map dataset",
    default = 'FILEPATH/diet_ro_map.csv', type = "character")
 parser$add_argument("--results_dir", help = "directory to read draw files from",
     default = "FILEPATH/mrbrt_outer_draws", type = "character") 
parser$add_argument("--save_draws_dir", help = "directory to save draws to",
    default = "FILEPATH", type = "character")
parser$add_argument("--mediator_pct_draw_dir", help = "the folder of the current 'best' mediator age trends",
    default = "FILEPATH/attenuation_factor_draws/", 
    type = "character")
parser$add_argument("--year_start", help = "first year to estimate for", 
    default = 2015, type = "integer")
parser$add_argument("--year_end", help = "last year to estimate for",
    default = 2015, type = "integer")
parser$add_argument("--release_id", help = "GBD round ID for run",
    default = 16, type = "integer")
parser$add_argument("--save_description", help = "longer description to use in save_results_risk call",
    default = "GBD 2023 risk curve updated with extended draw exposure range", type = "character")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

years <- year_start:year_end
ages <- c(10:20, 30:32, 235)

implement_age_trend <- T


## BODY ------------------------------------------------------------------
# get reids 
reis <- get_rei_metadata(rei_set_id = 2, release_id = release_id)
reid <- reis[rei==risk_rei, rei_id]
diet_ro_pair_map <- fread(diet_ro_pair_map)[include == 1 & rei_id==reid]
draw_dir <- paste0(results_dir)

meids <- fread("FILEPATH/diet_re_meids.csv")
me_id <- meids[rei==risk_rei, modelable_entity_id]
version_dir <- paste0(save_draws_dir)

save_dir <- paste0(version_dir, risk_rei)
dir.create(version_dir)
dir.create(save_dir)

  
  pull_draws <- function(cid){
    ro <- diet_ro_pair_map[cause_id == cid, risk_cause]
    harmful <- ifelse(diet_ro_pair_map[cause_id == cid, harmful]=="no", F, T)
    if(cid %in% c(497, 496)){
      ro <- paste0(ro, "_", cid)
    }
    
    draws <- fread(paste0(draw_dir, "/", ro,  "_outer_draws.csv")) # the draws with gamma
    
    setnames(draws, "risk", "exposure")
    
    if(implement_age_trend){
      # if ihd or stroke, make age specific 
      if(cid %in% c(497, 496, 493, 495)){
        
        if(cid %in% c(493, 495)){
          saverage_age_pct <- simple_average(causeid = cid, med_ids = c(107,367), 
                                             mediator_pct_draw_dir = mediator_pct_draw_dir, plot = F)
          
          med_ids <- paste0(rei_table[rei_id %in% c(107, 367), rei], collapse = " & ")
        }else{
          # cholesterol not mediator for hemstroke
          saverage_age_pct <- simple_average(causeid = cid, med_ids = c(107),
                                             mediator_pct_draw_dir = mediator_pct_draw_dir, plot = F)
          
          med_ids <- paste0(rei_table[rei_id %in% c(107), rei], collapse = " & ")
        }
        
        af_summ <- summarize_draws(saverage_age_pct)
        
        pdf(paste0(version_dir, risk_rei, "_agetrendresults.pdf"), width = 11, height = 8)
  
        plot <- ggplot(af_summ, aes(x = age_start, y = mean)) + 
          geom_line() +
          geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.3) + 
          theme_bw() + 
          labs(x = "Age", y = "Attenuation factor", 
               title = paste0(ro, " AF averaged across mediators ", med_ids)) +
          geom_hline(yintercept = 0, linetype = 4, color = "grey")
        print(plot)
        
                
        agespecific_draws <- apply_age_pattern(causeid = cid,
                                               reid = reid,
                                               release_id = release_id,
                                               risk_curve_draws_df = draws,
                                               age_pattern_draws_df = saverage_age_pct,
                                               draws_in_log = T,
                                               return_draws_log = F)
        
      }else{
        
        # exponentiate curves since saved in log space
        draws <- exponentiate_draws(draws)
        
        # duplicate curve out to all ages
        draws[, merge_id := 1]
        agespecific_draws <- merge(draws, data.table(merge_id = 1, age_group_id = ages), 
                                   by = "merge_id", allow.cartesian = T)
        agespecific_draws[, merge_id := NULL]
      }
        
    }else{
      # exponentiate curves since saved in log space
      agespecific_draws <- exponentiate_draws(draws)
    }
    
    agespecific_draws[, cause_id := cid ]
    agespecific_draws[, morbidity:=1]
    agespecific_draws[, mortality:=1]
    agespecific_draws[, year_id := 2]
    
    if(implement_age_trend){
      setcolorder(agespecific_draws, c("exposure", "cause_id", "year_id", "age_group_id", "morbidity", "mortality"))
    }else{
      setcolorder(agespecific_draws, c("exposure", "cause_id", "year_id", "morbidity", "mortality"))
    }
    
    return(agespecific_draws)
    
    
  }
  
  # make a pdf to capture age trend if relevant
  cvd_outcome <- intersect(c(497, 496, 493, 495), diet_ro_pair_map$cause_id)
  if(length(cvd_outcome) >= 1){
    pdf(paste0(version_dir, risk_rei, "_agetrendresults.pdf"), width = 11, height = 8)
  }
  
  # make one dataframe for all outcomes draws! 
  all_outcome_draws <- rbindlist(lapply(diet_ro_pair_map$cause_id, pull_draws))
  
  if(length(cvd_outcome) >= 1){
   dev.off() 
  }
  
  expanded_draws <- format_mrbrt_rr_results(all_outcome_draws, release_id = release_id)  
  
  expanded_draws <- expanded_draws[age_group_id %in% ages]
  
  expanded_draws$year_id = years
  
  
  setcolorder(expanded_draws,  
              c("exposure", "cause_id", "year_id", "age_group_id", "location_id", "morbidity", "mortality"))
 
      # save years
  for(y in years){
      expanded_draws[, year_id := y]
    
    expanded_draws$rei_id    <- reid
    expanded_draws$modelable_entity_id  <-   me_id
    expanded_draws$parameter  <-   NA
    expanded_draws$morbidity <- 1
    expanded_draws$mortality <- 1
    expanded_draws$metric_id <- 3
    
    expanded_draws$exposure <- ifelse(expanded_draws$rei_id == 123, expanded_draws$exposure/100, expanded_draws$exposure)
    
    fwrite(expanded_draws, paste0(save_dir, "/", y, ".csv"))

  }


# # # Now call save_results_risk
meids <- fread("FILEPATH/diet_re_meids.csv")
me_id <- meids[rei==risk_rei, modelable_entity_id]
model = "description"


save_results_risk(input_dir = save_dir,
                  input_file_pattern = "{year_id}.csv",
                  modelable_entity_id = me_id,
                  description = paste(save_description, "_", model), 
                  risk_type = "rr",
                  year_id = years,
                  release_id = 16,
                  mark_best = T)








