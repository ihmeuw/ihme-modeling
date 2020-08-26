source("filepath/save_results_risk.R")

keep <- "all"
eti <- read.csv("filepath")

if(keep == "all"){
  info <- eti
} else {
  info <- subset(eti, model_source==keep)
}

##--------------------------------------------------------------------------------------
## Upload RSV and Influenza PAFs ##
info <- subset(info, rei_id %in% c(187, 190))
for(i in 1:2){
  print(paste0("Saving ", info$rei_name[i]))
  save_results_risk(input_dir=paste0("/filepath/", info$rei[i],"/"),
                  input_file_pattern = "{measure}_{location_id}.csv",
                  modelable_entity_id=info$paf_me[i],
                  risk_type = "paf",
                  n_draws = 1000,
                  description=(paste0("Attributable fractions for decomp 4")),
                  mark_best=TRUE,
                  decomp_step="step4")
}

##--------------------------------------------------------------------------------------
## Upload Hib and S pneumo PAFS
save_results_risk(input_dir=paste0("/filepath/"),
                  input_file_pattern = "paf_{measure}_{location_id}.csv",
                  modelable_entity_id=9339,
                  risk_type = "paf",
                  n_draws = 1000,
                  description=(paste0("Uploaded Hib PAFs for decomp 4")),
                  mark_best=TRUE,
                  decomp_step="step4")
save_results_risk(input_dir=paste0("/filepath/"),
                  input_file_pattern = "paf_{measure}_{location_id}.csv",
                  modelable_entity_id=9338,
                  risk_type = "paf",
                  n_draws = 1000,
                  description=(paste0("Uploaded S pneumo PAFs for decomp 4")),
                  mark_best=TRUE,
                  decomp_step="step4")
##-------------------------------------------------------------------------------------
## Upload diarrhea proportion PAFs
diarrhea_eti <- subset(eti, cause_id==302)
diarrhea_eti <- subset(diarrhea_eti, !(rei_id %in% c(173, 183)))
for(i in 5:11){
  print(paste0("Saving for ", diarrhea_eti$name_colloquial[i]))
  save_results_risk(input_dir=paste0("/filepath/",diarrhea_eti$rei[i],"/"),
                  input_file_pattern = "{measure}_{location_id}.csv",
                  modelable_entity_id=diarrhea_eti$paf_me[i],
                  year_id = c(1990,1995,2000,2005,2010,2015,2017,2019),
                  risk_type = "paf",
                  n_draws = 1000,
                  description=(paste0("Attributable fractions for decomp 4,")),
                  mark_best=TRUE,
                  decomp_step="step4")
  print(paste0("Saved for ", diarrhea_eti$name_colloquial[i],"!"))
}
##--------------------------------------------------------------------------------------
## Upload Cholera PAFs
save_results_risk(input_dir=paste0("/filepath/"),
                  input_file_pattern = "paf_{measure}_{location_id}.csv",
                  modelable_entity_id=9324,
                  risk_type = "paf",
                  n_draws = 100,
                  year_id = c(1990, 2000, 2017),
                  description=(paste0("Attributable fractions for decomp 3")),
                  mark_best=TRUE,
                  decomp_step="step3")
##--------------------------------------------------------------------------------------
## Upload C difficile PAFs
save_results_risk(input_dir=paste0("/filepath/"),
                  input_file_pattern = "{measure}_{location_id}.csv",
                  modelable_entity_id=9334,
                  risk_type = "paf",
                  n_draws = 100,
                  year_id = c(1990, 2000, 2017),
                  description=(paste0("Attributable fractions for decomp 3")),
                  mark_best=TRUE,
                  decomp_step="step3")
