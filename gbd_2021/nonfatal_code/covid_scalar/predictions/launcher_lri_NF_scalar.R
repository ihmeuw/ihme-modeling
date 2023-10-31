#Save_results_epi for LRI NF
invisible(sapply(list.files("FILEPATH"), source))
source("FILEPATH")

ids <- get_elmo_ids(gbd_round_id = 7, decomp_step = "iterative", bundle_id = 19)
ids <- ids[is_best==1]
out_dir <- paste0("FILEPATH")
descript <- "COVID impacts on LRI for year 2020-2022, source MEID 1258, source MVID 600542, incorporate RSV/Flu model fixes"
save_results_epi("FILEPATH")

# submit_job("FILEPATH")