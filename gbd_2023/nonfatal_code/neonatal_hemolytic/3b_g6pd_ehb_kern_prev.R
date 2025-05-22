source("est_prevs.R")
source("get_prev_draws.R")
path_params_global <- "params_global.rds"
dir_mnt <- purrr::chuck(readr::read_rds(path_params_global), "dir_mnt")

name <- "g6pd"
# Using CI estimates given in Table 2, pg 91 of
# https://pubmed.ncbi.nlm.nih.gov/24366465/
mean <- 0.132 / 100
lower <- 0.085 / 100
upper <- 0.20 / 100
dir_old <- "FILEPATH"
params <- list(
  name = name,
  age_group_id = nch::id_for("age_group", "Early Neonatal"),
  path = fs::path(dir_mnt, "3b_G6PD", paste0(name, "_draws.csv")),
  me_id = 2112L # G6PD deficiency
)

if (!fs::file_exists(purrr::chuck(params, "path"))) {
  get_prev_draws(
    name = name,
    params = params,
    path_params_global = path_params_global
  )
}

if (fs::file_exists(purrr::chuck(params, "path"))) {
  est_prevs(
    name = name,
    directory = fs::path_dir(purrr::chuck(params, "path")),
    dir_old = dir_old,
    mean = mean,
    lower = lower,
    upper = upper
  )
}
