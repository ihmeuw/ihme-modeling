### save c.diff pafs ###


source("/PATH/save_results_risk.R")

# keep <- "all"
eti <- read.csv("/PATH/eti_rr_me_ids.csv")
#
# if(keep == "all"){
#   info <- eti
# } else {
#   info <- subset(eti, model_source==keep)
# }

gbd_round_id = 7
cdiff_paf_me <- subset(eti, rei_name %like% "Clostridium")$paf_me # check me_id in functions below

## Upload annual C difficile PAFs
save_results_risk(input_dir=paste0("/PATH"),
                  input_file_pattern = "paf_annual_{measure}_{location_id}.csv",
                  modelable_entity_id=9334,
                  risk_type = "paf",
                  n_draws = 1000,
                  year_id = 1990:2022,
                  description=(paste0("Attributable fractions for gbd 2020, annual years")),
                  mark_best=TRUE,
                  gbd_round_id = gbd_round_id,
                  decomp_step="iterative")

# Upload estimation year pafs:
# save_results_risk(input_dir=paste0("/PATH"),
#                   input_file_pattern = "{measure}_{location_id}.csv",
#                   modelable_entity_id=9334,
#                   risk_type = "paf",
#                   n_draws = 1000,
#                   year_id = c(1990,1995,2000,2005,2010,2015,2019,2020, 2021, 2022),
#                   description=(paste0("Attributable fractions for gbd 2020, estimation years")),
#                   mark_best=TRUE,
#                   gbd_round_id = gbd_round_id,
#                   decomp_step="iterative")



