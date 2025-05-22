
# source libraries --------------------------------------------------------

library(data.table)

# source helper functions -------------------------------------------------

invisible(sapply(
  list.files(
    path = file.path(getwd(), "vmnis/src/"),
    full.names = TRUE,
    pattern = ".R"
  ),
  source
))

# load in vmnis data and codebook -----------------------------------------

anemia_cb <- fread( "FILEPATH/anemia_codebook.csv")
vmnis <- fread("FILEPATH/vmnis_data.csv")
release_id <- 9

# remove any nids from vmnis that are in our microdata --------------------

vmnis <- vmnis[!(underlying_nid %in% anemia_cb$nid)]

# assign sex ids ----------------------------------------------------------

vmnis <- assign_sex_id(input_df = vmnis)

# ensure all metadata fits within gbd structures --------------------------

vmnis_list <- qc_vmnis_data(input_df = vmnis, gbd_rel_id = release_id)

vmnis <- copy(vmnis_list$valid_df)
to_check_vmnis <- copy(vmnis_list$to_check_df)

# get trimester data ------------------------------------------------------

vmnis <- assign_trimester(input_df = vmnis)

# assign case definitions for the different MEs present -------------------

vmnis <- assign_case_definition(input_df = vmnis)

# append on elevation data ------------------------------------------------

vmnis <- append_elevation(input_df = vmnis)

# get adjustment types and determine if non-specified are adjusted --------

vmnis <- main_get_gbd_hb_results(input_df = vmnis, gbd_rel_id = release_id)

# assign anemia measure ---------------------------------------------------

vmnis <- assign_anemia_measure(input_df = vmnis)

# format final vmnis data set ---------------------------------------------

vmnis <- format_vmnis_data(input_df = vmnis)

# determine group_review column  ------------------------------------------

vmnis <- assign_group_review(vmnis)

# write out excel of vmnis data -------------------------------------------

file_name <- "FILEPATH/vmnis_for_bundle_gbd2022_new_group_review.xlsx"
writexl::write_xlsx(
  x = list(extraction = vmnis),
  path = file_name
)

# upload bundle and save bundle version -----------------------------------

clear_bundle_data(1505)
ihme::upload_bundle_data(
  bundle_id = 1505,
  filepath = file_name
)

# check bundle data -------------------------------------------------------

bun_df <- ihme::get_bundle_data(bundle_id = 1505)

# save a bundle version ---------------------------------------------------

ihme::save_bundle_version(
  bundle_id = 1505, 
  description = "..."
)

