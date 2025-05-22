####################
##
## Winnower Collapse Script
##
####################

winnower_file_dir <- "FILEPATH"
collapse_config_path <- file.path(getwd(), "extract/winnower_collapse/config.csv")
collapse_topic <- "anemia_winnower"
user_name <- Sys.getenv("USER")

source(file.path(getwd(), "extract/winnower_collapse/src/collapse_source_code.R"))

sift_winnower_files(extract_dir = winnower_file_dir,
                    config_file_path = collapse_config_path,
                    topic_name = collapse_topic,
                    usr_name = Sys.getenv("USER"),
                    split_file_flag = T,
                    run_collapse_flag = T)
