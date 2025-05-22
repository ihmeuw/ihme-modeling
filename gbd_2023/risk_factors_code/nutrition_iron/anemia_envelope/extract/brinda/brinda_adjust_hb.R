
# setup environment -------------------------------------------------------

src_dir <- file.path(getwd(), "extract/brinda/src/")
invisible(sapply(
  list.files(src_dir, full.names = T), 
  source
))

# global variables --------------------------------------------------------

winnower_dir <- "FILEPATH"
lu_winnower_dir <- "FILEPATH"
anemia_codebook_filepath <- file.path(getwd(), "extract/brinda/anemia_codebook.csv")

# call main function ------------------------------------------------------

brinda_cb <- main_brinda_function(
  winnower_dir = winnower_dir,
  lu_winnower_dir = NULL,
  cb_file_path = anemia_codebook_filepath,
  submit_jobs = TRUE
)

summary_df <- brinda_cb[,.(num_surveys = .N), .(brinda_adjustment_type)]
