os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}
source(sprintf("FILEPATH",prefix))

save_results_epi(input_dir=paste0(prefix, "FILEPATH"),
                 input_file_pattern="{location_id}_{year_id}.csv",
                 year_id = c(1990:2017),
                 modelable_entity_id = ADDRESS,
                 description = "cl ver2 test;",
                 mark_best=FALSE)
