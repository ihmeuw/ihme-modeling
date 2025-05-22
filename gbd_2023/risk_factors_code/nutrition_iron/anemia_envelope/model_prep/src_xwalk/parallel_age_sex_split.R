
# source libraries --------------------------------------------------------

library(data.table)

box::use(
  as_split = ./age_sex_split
)

# age/sex split -----------------------------------------------------------

bundle_map <- fread(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))
output_dir <- 'FILEPATH'
age_sex_cascade_dir <- 'FILEPATH'

if(interactive()){
  r <- 1
}else{
  r <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
}

message(paste(r, bundle_map$id[r]))

age_sex_file_path <- file.path(
  output_dir,
  bundle_map$id[r],
  'age_sex_split_mrbrt.csv'
)

file_name <- file.path(
  output_dir,
  bundle_map$id[r],
  paste0(
    bundle_map$id[r], "bundle_data.csv"
  )
)

bun_df <- fread(file_name)

i_vec <- which(bun_df$imputed_elevation_adj_row == 1 | 
                 bun_df$imputed_measure_row == 1)
inverse_i_vec <- setdiff(seq_len(nrow(bun_df)), i_vec)
impute_df <- bun_df[i_vec, ]
bun_df <- bun_df[inverse_i_vec, ]

xwalked_df <- as_split$age_sex_split_anemia(
  input_df = bun_df,
  cascade_dir = age_sex_cascade_dir,
  release_id = 16
) 

xwalked_df <- rbindlist(
  list(xwalked_df, impute_df),
  use.names = TRUE,
  fill = TRUE
)

fwrite(
  x = xwalked_df,
  file = age_sex_file_path
)

message('DONE.')
