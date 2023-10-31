## save cholera pafs for estimation years ##


source("/FILEPATH/save_results_risk.R")

keep <- "all"
eti <- read.csv("/FILEPATH/eti_rr_me_ids.csv")

if(keep == "all"){
  info <- eti
} else {
  info <- subset(eti, model_source==keep)
}

gbd_round_id <- 7



## upload PAFs esitimation year
save_results_risk(input_dir=paste0("/FILEPATH/"),
                  input_file_pattern = "paf_{measure}_{location_id}.csv",
                  modelable_entity_id=9324, ## Cholera PAF
                  risk_type = "paf",
                  n_draws = 1000,
                  ## year only up to 2020
                  year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022),
                  description=(paste0("Attributable fractions for gbd 2020, initial")),
                  mark_best=TRUE,
                  gbd_round_id = gbd_round_id,
                  decomp_step="iterative"
)

###########################
##### DO INTERPOLATION ##################
## launch pafs annual diarrheas.R
############################################



## Upload Cholera annual PAFs
save_results_risk(input_dir=paste0("/FILEPATH/"),
                  input_file_pattern = "paf_annual_{measure}_{location_id}.csv",
                  modelable_entity_id=9324, ## 9334 = c.diff; 9324 == cholera,
                  risk_type = "paf",
                  n_draws = 1000,
                  ## year only up to 2020
                  year_id = c(1990:2022),
                  description=(paste0("Annual PAFs for GBD 2020, Annual")),
                  mark_best=TRUE,
                  gbd_round_id = gbd_round_id,
                  decomp_step="iterative"
				  )

