##################################################################################
## DESCRIPTION: Read in a csv of draws that was saved during 04_mixed_effects_model, apply age pattern for CVD outcomes,
##              expand draws to age/sex/year specific results, upload
##              also need to combine causes
##
## AUTHOR: 
## DATE: 12/3/2020
#  
##############################################################################
rm(list = ls())
library(data.table)

# load in central functions
source('FILEPATH/format_mrbrt_rr_results.R')
source("FILEPATH/save_results_risk.R")
source("FILEPATH/get_rei_metadata.R")
# load in utility functions
source("FILEPATH/helper_functions.R")
source("FILEPATH/age_trends_functions.R")
# relevant filepaths and parameters
diet_ro_pair_map <- fread("FILEPATH/diet_ro_map.csv")
results_dir <- "FILEPATH"
save_draws_dir <- "FILEPATH"
years <- 2015  # PAF calculator will duplicate out for years. 
ages <- c(10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)


if(interactive()){
  
  risk_rei <- "diet_fruit"
  version <- "VERSION"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  risk_rei <- args[1]
  version <- args[2]
}


implement_age_trend <- T


# get reid and limit to ro pairs for this risk that should be included
reis <- get_rei_metadata(rei_set_id = 2, gbd_round_id = 7, decomp_step = "iterative")
reid <- reis[rei==risk_rei, rei_id]
diet_ro_pair_map <- diet_ro_pair_map[include == 1 & rei_id==reid]
draw_dir <- paste0(results_dir, version, "/05_draws_csvs/")

# make directory that corresponds to the MR-BRT outputs
version_dir <- paste0(save_draws_dir,"/", version,"_07_06.01_wo_FPG/")
save_dir <- paste0(version_dir, risk_rei)
dir.create(version_dir)
dir.create(save_dir)

completed_years <- gsub(".csv","",list.files(save_dir))

if(length(setdiff(years, completed_years)) > 0 ){
  
  # goal, combine all outcome draws, expand, save by year
  pull_draws <- function(cid){
    
    ro <- diet_ro_pair_map[cause_id == cid, risk_cause]
    harmful <- ifelse(diet_ro_pair_map[cause_id == cid, harmful]=="no", F, T)
    # keep in if ro pairs were saved with _cid
    if(cid %in% c(497, 496)){
      ro <- paste0(ro, "_", cid)
    }
    
    draws <- fread(paste0(draw_dir, "/", ro, "_y_draws.csv")) # the draws with gamma
    
    
    if(implement_age_trend){
      # if ihd or stroke, make age specific 
      if(cid %in% c(497, 496, 493, 495)){
        
        if(cid %in% c(493, 495)){
          # sbp = 107; fpg = 141; ldl = 367
          saverage_age_pct <- simple_average(causeid = cid,
                                            med_ids = c(107,367), plot = F)
          
          med_ids <- paste0(rei_table[rei_id %in% c(107, 367), rei], collapse = " & ")
        }else{
          # cholesterol not mediator for hemstroke
          saverage_age_pct <- simple_average(causeid = cid,
                                             med_ids = c(107), plot = F)
          
          med_ids <- paste0(rei_table[rei_id %in% c(107), rei], collapse = " & ")
        }
        
        af_summ <- summarize_draws(saverage_age_pct)
        
        plot <- ggplot(af_summ, aes(x = age_start, y = mean))+geom_line()+
          geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.3) + 
          theme_bw() + labs(x = "Age", y = "Attenuation factor", title = paste0(ro, " AF averaged across mediators ", med_ids))+
          geom_hline(yintercept = 0, linetype = 4, color = "grey")
        print(plot)
        
        
        
        agespecific_draws <- apply_age_pattern(causeid = cid,
                                                    reid = reid, 
                                                    risk_curve_draws_df = draws,
                                                    age_pattern_draws_df = saverage_age_pct,
                                                    draws_in_log = T,
                                                    return_draws_log = F)
        
      }else{
        
        # exponentiate curves since saved in log space
        draws <- exponentiate_draws(draws)
        
        # duplicate curve out to all ages
        draws[, merge_id := 1]
        agespecific_draws <- merge(draws, data.table(merge_id = 1, age_group_id = ages), by = "merge_id", allow.cartesian = T)
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
  cvd_outcome <- intersect(c(497, 496, 493, 495), diet_ro_pair_map$cause_id )
  if(length(cvd_outcome) >= 1){
    pdf(paste0(version_dir, risk_rei, "_agetrendresults.pdf"), width = 11, height = 8)
  }
  
  # make one dataframe for all outcomes draws!
  all_outcome_draws <- rbindlist(lapply(diet_ro_pair_map$cause_id, pull_draws))
  
  if(length(cvd_outcome) >= 1){
   dev.off() 
  }
  
  expanded_draws <- format_mrbrt_rr_results(all_outcome_draws, gbd_round_id = 7)  
  
  expanded_draws <- expanded_draws[age_group_id %in% ages]
  
  
  
  setcolorder(expanded_draws,  c("exposure", "cause_id", "year_id", "age_group_id", "location_id", "morbidity", "mortality"))
 
  # save years
  for(y in years){
  
    expanded_draws[, year_id := y]
    write.csv(expanded_draws, paste0(save_dir, "/", y, ".csv"), row.names = F)
    
  }

}


# # # Now call save_results_risk
meids <- fread("FILEPATH/diet_re_meids.csv")
me_id <- meids[rei==risk_rei, modelable_entity_id]

save_results_risk(input_dir = save_dir,
                  input_file_pattern = "{year_id}.csv",
                  modelable_entity_id = me_id,
                  description = "GBD 2020 risk curve updated with excess risk age pattern",
                  risk_type = "rr",
                  year_id = years,
                  gbd_round_id = 7,
                  decomp_step = "iterative",
                  mark_best = T)




if(F){
  
  all_pairs <- list.dirs(path = "FILEPATH", recursive = F, full.names = F)
  all_pairs <- all_pairs[grep("diet", all_pairs)]

  for(risk_rei in all_pairs){
  version <- "VERSION"
    
    
    log_dir <- paste0("FILEPATH",Sys.info()['user'])
    system(paste0("qsub -N upload_rr_",risk_rei,
                  " -P proj_diet -l m_mem_free=40G -l fthread=15 -q all.q",
                  " -o ", log_dir, "FILEPATH -e ", log_dir, "FILEPATH ",
                  "FILEPATH/execRscript.sh ",
                  "-s FILEPATH/format_upload_rr.R ", 
                  risk_rei, " ", version))
    
  }
    
  
  
}  







