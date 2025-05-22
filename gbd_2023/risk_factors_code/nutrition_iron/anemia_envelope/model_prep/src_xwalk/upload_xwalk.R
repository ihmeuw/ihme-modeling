
# source libraries --------------------------------------------------------

library(data.table)

source(file.path(getwd(), "model_prep/src_xwalk/xwalk_prep_helpers.R"))

# source maps -------------------------------------------------------------

bundle_map <- fread(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))
output_dir <- 'FILEPATH'

if(interactive()){
  r <- 15
}else{
  r <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
}

# load in data ------------------------------------------------------------

message(paste("Bundle ID =", bundle_map$id[r]))

xwalked_file_path <- file.path(
  output_dir,
  bundle_map$id[r],
  'final_data.fst'
)

xwalked_df <- fst::read.fst(xwalked_file_path, as.data.table = TRUE)

xwalked_df <- main_xwalk_prep_function(input_df = xwalked_df)

xwalk_excel_file_path <- file.path(
  output_dir,
  bundle_map$id[r],
  'final_xwalk_data.xlsx'
)

openxlsx::write.xlsx(
  xwalked_df, 
  xwalk_excel_file_path, 
  sheetName = "extraction"
)

ihme::save_crosswalk_version(
  bundle_version_id = bundle_map$bv_id[r],
  data_filepath = xwalk_excel_file_path,
  description = "Final xwalk steps."
)
