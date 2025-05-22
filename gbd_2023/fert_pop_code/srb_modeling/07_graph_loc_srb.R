################################################################################
## DESCRIPTION Creates diagnostic graphs
## INPUTS prepped data, final estimates from upload, previous best, and WPP estimates
## OUTPUTS a diagnostic graph for every location
## STEPS
## - Read in configuration files
## - Load inputs
##   - distinguish between final estimates and stage1 or 2 predictions
##   - convert data to SRB space
##   - remove duplicates
##   - get hyperparameters from previous version
## - Set up graph function
## - ggplot function
## - set pdf location (variable-height y-axis)

################################################################################

rm(list = ls())

library(data.table)
library(ggplot2)
library(haven)
library(mortcore)
library(mortdb)
library(RColorBrewer)
library(stringr)

## A. Read in configuration files ----------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  help = "base directory for this version run"
)
parser$add_argument(
  "--location_id",
  type = "integer",
  required = !interactive(),
  help = "numerical code for location_id"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# initial inputs
if(interactive()){
  version_id <- "Run id"
  main_dir <- "FILEPATH"
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
  loc <- "input location"
} else{
  loc = location_id
}

config <- config::get(
  file = fs::path(main_dir, "/srb_detailed.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# get old loc map
loc_map <- fread(fs::path(input_dir, "loc_map.csv")) |> 
  _[location_id == loc]
unique_loc <- loc_map[, ihme_loc_id]

# B. Load inputs -----------------------------------------------------------------------

prep_dir <- "FILEPATH"
list(data = "graphing_data", outliers = "outliers") |> purrr::map(\(x){
  fread(fs::path(output_dir, x, ext = "csv")) |> _[ihme_loc_id %in% unique_loc]
}) |> list2env(.GlobalEnv)
params <- fread(fs::path(input_dir, "hyper_params.csv")) |>
  _[ihme_loc_id %in% unique_loc]

# format
data <- merge(
  data,
  loc_map[, .(ihme_loc_id, region_name, location_name)], 
  by = "ihme_loc_id"
)

# add variable to distinguish between line types
data[, ver_type := "final est"]
data[grep("(stage pred)", version), ver_type := "not final est"] 

if(add_compare == TRUE) {
  data[grepl("ST run_id", version), ver_type := "not final est"]
}
# format outliers for current loc
outliers[, data := data/(1-data)]

# get comparison name if additional comparator added
if(add_compare == TRUE){ 

  lapply(
    c(compare_name = "Comp", comp_stg2 = "ST"), 
    \(x)(unique(data[grepl(paste(x, "run_id")), version]))
  ) |> as.list()
    
  # Get hyper parameters from compare to add to title
  comp_version <- substr(compare_name, 13, 15)
  prev_params <- fread(
    fs::path(
      "FILEPATH",
      "hyper_params.csv"
    )
  ) |> _[ihme_loc_id == unique_loc]
}

# C. Set up graph function -----------------------------------------------------

## set colors for points by data type
shs <- c(
  "vr" = 16,
  "cbh" = 17,
  "censusU1" = 18,
  "censusU5" = 15,
  "sample_registration" = 14,
  "census5to9" = 6
)
labels <- c(
  paste("GBD", c(gbd_year, prev_gbd_year)),
  "WPP",
  paste(c("1st", "2nd"), "stage pred"),
  1.05,
  1.2,
  compare_name,
  comp_stg2
)

shapescale <- scale_shape_manual(
  name = "",
  values = shs
)

## set lines based on version
scale_values <- c(
  "Current" = "black",
  "Previous" = "green",
  "WPP" = "blue",
  "1st stage pred" = "purple",
  "2nd stage pred" = "grey45",
  "1.05" = "turquoise",
  "1.2" = "turquoise"
)

scale_labels <- labels[1:7]
names(scale_labels) <- names(scale_values)

if(add_compare){
  scale_values <- c(scale_values, compare_name = "red") 
  scale_labels <- c(scale_labels, compare_name = compare_name)
}

# D. ggplot function -------------------------------------------------------------------

# Define Graphing Function
graph_loc <- function(y_lims) {
  # plot estimates
  p <- ggplot(data = data, aes(x = year_id, y = mean, color = version)) +
    geom_line(data = data, aes(linetype = ver_type)) +
    geom_ribbon(
      data = data[version == "Previous"],
      aes(ymin = lower, ymax = upper),
      fill = "green",
      alpha = 0.15,
      color = NA
    ) +
    geom_ribbon(
      data = data[(version == "Current")],
      aes(ymin = lower, ymax = upper),
      fill = "grey",
      alpha = 0.25,
      color = NA
    ) +
    scale_color_manual(values = scale_values, labels = scale_labels) +
    theme_bw() +
    theme(
      plot.caption = element_text(size = 8, hjust = 1),
      axis.text.x = element_text(angle = 90, hjust = 1)
    ) +
    labs(
      title = title_string,
      caption = "NOTE: faded data points are outliers."
    ) +
    ylab("SRB") +
    scale_x_continuous(name = "Year", breaks = seq(1950, end_year, by = 5))+
    lims(y = y_lims) +
    geom_hline(
      yintercept = 1.05,
      color = "grey45",
      linetype = "dashed",
      size = 1.2
    ) +
    geom_line(
      data = pre_rake,
      aes(x = year_id, y = mean),
      color = "black",
      linetype = "twodash"
    )
  
  # plot prepped data
  if(nrow(data[!is.na(data)])>0){
    p <- p +
      geom_point(
        data = data[!is.na(data) & outlier == 0],
        aes(x = year_id, y = data, shape = source),
        color = "black",
        size = 4,
        stroke = 1
      ) +
      geom_point(
        data = data[outlier > 0 & prev_outliered == "yes"],
        aes(x = year_id, y = data, shape = source),
        size = 4,
        alpha = 0.2,
        stroke = 1,
        color = "black"
      ) +
      geom_point(
        data = data[outlier == 1 & prev_outliered == "no"],
        aes(x = year_id, y = data, shape = source),
        size = 4,
        alpha = 0.2,
        stroke = 1,
        color = "red"
      ) +
      geom_point(
        data = data[outlier == 2 & prev_outliered == "no"],
        aes(x = year_id, y = data, shape = source),
        size = 4,
        alpha = 0.2,
        stroke = 1,
        color = "darkred"
      ) +
      shapescale +
      theme_bw()
  }
  return(p)
}

# Pre-scaled GPR outputs to add to graph
pre_rake <- fread(fs::path(
  gpr_dir, paste0("gpr_", unique_loc, "_sim.txt"))
)
pre_rake <- pre_rake[
  ,
  .(mean = mean(val)/(1 - mean(val))), by = c("ihme_loc_id", "year")
] |>
  _[, year_id := year - .5] |> 
  _[, year := NULL]

# Label data which was present in the previous best run, but is
# outliered currently
prev_best_id <- ifelse(
  get_best_versions("birth sex ratio data", gbd_year = gbd_year) |> is.na(),
  get_best_versions("birth sex ratio data", gbd_year = prev_gbd_year),
  get_best_versions("birth sex ratio data", gbd_year = gbd_year))

prev_data <- fread(
  fs::path(prep_dir, prev_best_id, "/FILEPATH/processed_data.csv")
) |> _[ihme_loc_id == unique_loc]

outliers[prev_data, prev_outliered := "no", on = c("nid", "year_id")]
outliers[is.na(prev_outlier), prev_outliered := "yes"]

# append outliers
data <- rbind(data, outliers, fill = TRUE)

# remove outliers with absurd values
data <- data[data > 0 | is.na(data)] 

# create graphing location name for title
loc_name <- paste(
  loc_map[, .(ihme_loc_id, location_name, location_id, region_name)],
  collapse = ", "
)

# specify hyper-parameters for loc
hyper_params <- sapply(c("zeta", "scale"), \(x) params[, get(x)])
  
# Define Titlestring
if(run_beta == FALSE){
  # print lambdas
  lambda <- params$lambda
  title_string <-  paste0(
    "SRB ", loc_name,
    "\n lambda: ", lambda,
    ", zeta: ", hyper_params[["zeta"]],
    ", scale: ", hyper_params[["scale"]]
  )
}else{
  # print betas
  beta <- params$beta
  title_string <-  paste0(
    "SRB ", loc_name,
    "\n Current: beta: ", beta,
    ", zeta: ", hyper_params[["zeta"]],
    ", scale: ", hyper_params[["scale"]],
    "\n Previous: beta: ", prev_params$beta,
    ", zeta: ", prev_params$zeta,
    ", scale: ", prev_params$scale
  )
}
min(data$lower-1, na.rm = TRUE)

# E. set pdf location (variable-height y-axis) ----------------------------

pdf_path <- sapply(c("", "_fixed"), \(x) fs::path(
  graphs_dir, paste0(unique_loc, "_", version_id, x), ext = "pdf"
))

pdf(pdf_path[[1]], width = 15, height = 10)
p <- graph_loc(y_lims = c(NA_integer_, NA_integer_))
print(p)
cat(paste0(loc_name, "\n")) ; flush.console()
dev.off()

# set pdf location (fixed-height y-axis)
pdf(pdf_path[[2]], width = 15, height = 10)
p2 <- graph_loc(y_lims = c(.9, 1.3))
print(p2)
cat(paste0(loc_name, "\n")); flush.console()
dev.off()
