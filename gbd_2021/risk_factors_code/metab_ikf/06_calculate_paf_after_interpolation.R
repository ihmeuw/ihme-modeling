### Kidney dysfunction
# save PAFs
# GBD 2020


rm(list = ls())
require(ini)
source("/ihme/code/risk/paf/launch_paf.R")

# Before running, triple check that the save results went smoothly
source("/ihme/cc_resources/libraries/current/r/get_draws.R")
df <- get_draws("rei_id", 341, source="rr", decomp_step = "iterative", version_id =  632021)
table(df$cause_id, df$year_id)
df [cause_id == 502 & sex_id == 1 & year_id == 2020 & age_group_id == 13,
   names(df)[!names(df) %in% c("exposure", paste0("draw_",1:999))], with = F][order(parameter, draw_0)] # Expect this to return only 5 rows, one for each category of exposure.

# 
# gout_df <- df %>%  filter(cause_id == 632)
# launch_paf(rei_id = rei, decomp_step = "iterative",  gbd_round_id = 7,
#            cluster_proj = 'proj_yld', resume = FALSE, n_draws = 1000)


# launch pafs for annual estimation years
launch_paf(rei_id = 341, 
           decomp_step = "iterative", 
           cluster_proj = "proj_yld",
           save_results = T,
           year_id = c(1990:2022),
           resume = F,
           n_draws = 1000)

# ### For alton's testing purposes
# require(ini)
# source("/ihme/epi/ckd/ckd_code/ikf/launch_paf-copy.R")
# launch_paf(341, decomp_step="step4", cluster_proj = 'proj_custom_models', resume=TRUE)



## For checking

source("/ihme/code/risk/diagnostics/paf_scatter.R")
vid <-  632021
# 632021, no fisher
#model_version_id:  626711 evidence score and fisher 
# version 575483 was good, had some odd left tails
# version 578852 imposed linear left tails

paf_scatter(rei_id = 341, year_id = 1990, loc_level = 4, version_id = 626714, gbd_round_id = 7, measure_id = 3,
            file_path = paste0("/ihme/code/qwr/ckd_qwr/kd/",Sys.Date()," kd_scatter_1990.pdf"))
paf_scatter(rei_id = 341, year_id = 1995, loc_level = 4, version_id = 626714, gbd_round_id = 7, measure_id = 3,
            file_path = paste0("/ihme/code/qwr/ckd_qwr/kd/pafs/",Sys.Date()," kd_scatter_1995.pdf"))
paf_scatter(rei_id = 341, year_id = 2000, loc_level = 4, version_id = 626714, gbd_round_id = 7, measure_id = 3,
            file_path = paste0("/ihme/code/qwr/ckd_qwr/kd/pafs/",Sys.Date()," kd_scatter_2000.pdf"))
paf_scatter(rei_id = 341, year_id = 2005, loc_level = 4, version_id = 626714, gbd_round_id = 7, measure_id = 3,
            file_path = paste0("/ihme/code/qwr/ckd_qwr/kd/pafs/",Sys.Date()," kd_scatter_2005.pdf"))
paf_scatter(rei_id = 341, year_id = 2010, loc_level = 4, version_id = 626714, gbd_round_id = 7, measure_id = 3,
            file_path = paste0("/ihme/code/qwr/ckd_qwr/kd/pafs/",Sys.Date()," kd_scatter_2010.pdf"))
paf_scatter(rei_id = 341, year_id = 2015, loc_level = 4, version_id = 626714, gbd_round_id = 7, measure_id = 3,
            file_path = paste0("/ihme/code/qwr/ckd_qwr/kd/pafs/",Sys.Date()," kd_scatter_2015.pdf"))
paf_scatter(rei_id = 341, year_id = 2019, loc_level = 4, version_id = 626714, gbd_round_id = 7, measure_id = 3,
            file_path = paste0("/ihme/code/qwr/ckd_qwr/kd/pafs/",Sys.Date()," kd_scatter_2019.pdf"))
paf_scatter(rei_id = 341, year_id = 2020, loc_level = 4, version_id = 626714, gbd_round_id = 7, measure_id = 3,
            file_path = paste0("/ihme/code/qwr/ckd_qwr/kd/pafs/",Sys.Date()," kd_scatter_2020.pdf"))
