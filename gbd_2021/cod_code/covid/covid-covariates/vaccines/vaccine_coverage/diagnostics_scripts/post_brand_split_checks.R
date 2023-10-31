
# Compares the cumulative vaccination count before splitting by brand with the cumulative vaccination count after splitting by brand, aggregating all brands and risk groups.

# Outputs plots and CSV of both sets merged. If write_full_space is TRUE, will plot brand-risk-groups with cumulatives and include them in the CSV. If FALSE, only plots and writes cumulatives.

# To find the locations/records in which the pre-split cumulatives do not match the post-split cumulatives, select records where wrong_count is not NA. Wrong counts are plotted as larger points that the rest.

# Note:
# Must be run with longer time than .submit_plot_job() allows if running with write_full_space as TRUE. In that case, use .submit_job():
# .submit_job(
#   script_path = script_path,
#   job_name = NULL, # Will be name of script if NULL.
#   mem = "10G",
#   archiveTF = TRUE,
#   threads = "2",
#   runtime = "90", # May need to be longer.
#   Partition = "d.q",
#   Account = "proj_covid",
#   args_list = list(
#     "--current_version" = current_version,
#     #"--compare_version" = compare_version,
#     #"--post_split_version" = "2022_03_11.06",
#     "--write_full_space" = T
#   )
# )

message("Running post-split checks.")

# # Set up. ####################################################################
library(data.table)
library(ggplot2)

source(paste0("FILEPATH", Sys.info()["user"], "FILEPATH/paths.R"))

current_version = "2022_04_07.02"
post_split_filename = "last_shots_in_arm_by_brand_w_booster_reference.csv"
pre_split_filename ="slow_scenario_vaccine_coverage.csv"
keys = c('location_id', 'date')
write_full_space = F


message(paste("interactive():", interactive()))
if (!interactive()) {
  parser <- argparse::ArgumentParser()
  parser$add_argument(
    "--current_version",
    type = "character",
    help = "Where to read data and write diagnostics."
  )
  parser$add_argument(
    "--post_split_filename",
    type = "character",
    default = "uncorrected_last_shots_in_arm_by_brand_w_booster_reference.csv",
    help = "The file to check against pre_split_filename."
  )
  parser$add_argument(
    "--pre_split_filename",
    type = "character",
    default = "slow_scenario_vaccine_coverage.csv",
    help = "The file to check pre_split_filename against."
  )
  parser$add_argument(
    "--keys",
    type = "character",
    default = "location_id,date",
    help = "Top-level keys common to pre- and post-split data sets."
  )
  parser$add_argument(
    "--write_full_space",
    # action = "store_false",
    default = "FALSE",
    type = "character",
    help = "Create and write CSV file and PDF plots of cumulatives for all brand-risk groups and total cumulatives. Default of FALSE will only write the pre- and post-split cumulatives and the post-split total noncumulative."
  )
  
  
  # message(paste("parser$parse_args('--write_full_space'):", parser$parse_args("--write_full_space")))
  args <- parser$parse_args()
  # message(paste("args['--write_full_space']:", args['--write_full_space']))
  # for (key in names(args)) {
  #   assign(key, args[[key]])
  # }

  current_version <- args$current_version
  post_split_filename <- args$post_split_filename
  pre_split_filename <- args$pre_split_filename
  keys <- args$keys
  write_full_space <- args$write_full_space

  for (arg in names(args)) {
    message(paste0(arg, ": ", get(arg)))
  }

  keys <- unlist(strsplit(keys, ","))
  
  if (
    is.character(write_full_space) &
    toupper(write_full_space) %in% c("F", "FALSE")
  ) {
    write_full_space <- F
  } else if (
    is.character(write_full_space) &
    toupper(write_full_space) %in% c("T", "TRUE")
  ) {
    write_full_space <- T
  } else {
    warning(paste("Invalid write_full_space:", write_full_space, ". Setting to FALSE."))
    write_full_space <- F
  }
}

message(paste("write_full_space:", write_full_space))


# # Set constants. ###########################################################
# source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))

# # Load. ####################################################################
split_dt <- fread(
  file.path(
    DATA_ROOTS$VACCINE_OUTPUT_ROOT,
    current_version,
    post_split_filename
  )
)


# # Set data. ##################################################################


setkeyv(x = split_dt, cols = keys)
split_dt[, V1 := NULL]
split_dt <- split_dt[vaccine_course == 1]
split_dt[, vaccine_course := NULL]


# # Construct data. ############################################################
# Get post-split totals.
vax_cols <- names(split_dt)[
  !(names(split_dt) %in% keys) &
    names(split_dt) != "risk_group"
]

split_dt <- dcast(
  data = split_dt,
  formula = location_id + date ~ risk_group,
  value.var = vax_cols
)

vax_cols <- names(split_dt)[!(names(split_dt) %in% keys)]

split_dt[, noncum_total_ct := rowSums(split_dt[, vax_cols, with = F])]

# Get post-split cumulatives.
# # Get them all, or ...
if (write_full_space) {
  message("Getting full set of cumulatives.")

  setnames(x = split_dt, old = c("noncum_total_ct"), new = c("total_ct"))

  split_dt <- melt(
    data = split_dt,
    id.vars = keys,
    measure.vars = c(vax_cols, "total_ct"),
    variable.name = "count_type",
    value.name = "noncum"
  )

  setorderv(
    x = split_dt,
    cols = c("location_id", "count_type", "date")
  )

  split_dt[, cumsum := cumsum(noncum), by = c("location_id", "count_type")]

  split_dt <- dcast(
    data = split_dt,
    formula = location_id + date ~ count_type,
    value.var = list("noncum", "cumsum")
  )

  # # ... get just the total.
} else if (!write_full_space) {
  message("Getting summary cumulatives.")

  vax_cols <- c("noncum_total_ct")

  split_dt <- split_dt[, c(keys, vax_cols), with = F]

  setorderv(x = split_dt, cols = c("location_id", "date"))

  split_dt[, cumsum_total_ct := cumsum(noncum_total_ct), by = c("location_id")]
} else {
  stop("Invalid write_full_space.")
}

# Merge pre-split cumulatives.
split_dt <- merge(
  x = fread(
    file.path(
      DATA_ROOTS$VACCINE_OUTPUT_ROOT,
      current_version,
      pre_split_filename
    ) 
  )[, c(keys, "location_name", "cumulative_all_vaccinated"), with = F],
  y = split_dt,
  by = keys,
  all = T
)

setnames(
  x = split_dt,
  old = c("cumulative_all_vaccinated", "cumsum_total_ct", "noncum_total_ct"),
  new = c("cumsum_presplit", "cumsum_postsplit", "noncum_postsplit")
)

# Get pre-split noncumulatives.
split_dt[, noncum_presplit := c(NA, diff(cumsum_presplit)), by = c("location_id")]


# # Interrogate data. ##########################################################
# Find wrong counts.
split_dt[cumsum_presplit != cumsum_postsplit, wrong_count := cumsum_postsplit]

locs_with_wrong_counts <- unique(split_dt[!is.na(wrong_count)][["location_id"]])
locs_without_wrong_counts <- setdiff(
  x = unique(split_dt[is.na(wrong_count)][["location_id"]]),
  y = locs_with_wrong_counts
)
num_wrong <- length(locs_with_wrong_counts)
if (num_wrong > 0) {
  warning(glue::glue("Found {num_wrong} locations with mismatch in pre- and post-split cumulative counts. Note: some records may be an artifact of rounding and/or precision limits of data type size; consult plots in 'split_cumulatives_check_{current_version}.pdf'."))
}


# # Write final data. ##########################################################
full_or_summary <- ""
if (write_full_space) {
  full_or_summary <- "full"
} else if (!write_full_space) {
  full_or_summary <- "summary"
}

fp <- file.path(
  DATA_ROOTS$VACCINE_OUTPUT_ROOT,
  current_version,
  glue::glue("split_cumulatives_check_{full_or_summary}_{current_version}.csv")
)

message(paste0("Writing split check data table to ", fp))

write.csv(x = split_dt, file = fp)


# # Plot. ######################################################################
# Prep.
vax_cols <-
  names(split_dt)[!(names(split_dt) %in% c("location_id", "date", "location_name"))]

noncum_cols <- vax_cols[grepl("noncum*", vax_cols)]
cumsum_cols <- vax_cols[grepl("cumsum*", vax_cols)] # Excludes "wrong_count"

split_dt <- melt(
  data = split_dt,
  id.vars = c(keys, "location_name"),
  measure.vars = vax_cols,
  variable.name = "count_type",
  value.name = "count"
)

setorderv(x = split_dt, cols = c("location_id", "count_type", "date"))

# Plot.
fp <- file.path(
  DATA_ROOTS$VACCINE_OUTPUT_ROOT,
  current_version,
  glue::glue("split_cumulatives_check_{full_or_summary}_{current_version}.pdf")
)

message(paste0("Writing split check plots to ", fp))

pdf(file = fp, onefile = T)

for (loc in unique(split_dt$location_id)) {
  cumsum_plt <- ggplot(
    data = split_dt[location_id == loc & count_type %in% cumsum_cols],
    aes(x = date, y = count, color = count_type),
    alpha = 0.5
  ) +
    geom_point(
      data = split_dt[location_id == loc & count_type == "wrong_count"],
      alpha = 0.25,
      size = 2
    ) +
    geom_point(size = 0.25) +
    labs(
      title = paste0(
        unique(split_dt[location_id == loc, .(location_name)]),
        " (", loc, ")"
      ),
      caption = paste0(
        current_version, " | ",
        "pre: ", pre_split_filename, " | ",
        "post: ", post_split_filename
      )
    )

  print(cumsum_plt)
}

dev.off()

message(glue::glue("Finished plotting {fp}"))
