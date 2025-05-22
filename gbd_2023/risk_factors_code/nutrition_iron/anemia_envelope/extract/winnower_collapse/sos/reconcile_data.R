collapsed_india_files <- list.files(
  path = 'FILEPATH',
  pattern = 'india',
  full.names = TRUE
)

collapsed_other_files <- list.files(
  path = 'FILEPATH',
  pattern = '^collapse.*',
  full.names = TRUE
)

india_data <- lapply(collapsed_india_files, \(x) {
  data.table::fread(x) |>
    dplyr::mutate(
      src_collapse = fs::path_file(x) |>
        tools::file_path_sans_ext()
    )
}) |> data.table::rbindlist(use.names = T, fill = T) |>
  unique()

all_data <- lapply(collapsed_other_files, \(x) {
  data.table::fread(x)
}) |> data.table::rbindlist(use.names = T, fill = T) |>
  unique()

non_dup_data <- all_data[!(file_path %in% india_data$file_path)]

final_data <- data.table::rbindlist(
  list(india_data, non_dup_data),
  use.names = TRUE,
  fill = TRUE
) |>
  unique()

data.table::fwrite(
  x = final_data,
  file = 'FILEPATH/final_collapsed_data.csv'
)
