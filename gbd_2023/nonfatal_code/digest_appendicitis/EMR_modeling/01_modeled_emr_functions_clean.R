###############################################################################
## Purpose: Functions for EMR regression using MR BRT
###############################################################################


get_emr_data <- function(model_id){
  odbc <- ini::read.ini('FILEPATH')
  con_def <- 'clinical_dbview'
  myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                              host = odbc[[con_def]]$SERVER,
                              username = odbc[[con_def]]$USER,
                              password = odbc[[con_def]]$PASSWORD)
  
  df <- dbGetQuery(
    conn = myconn,
    statement = paste0(
      "SELECT * FROM epi.t3_model_version_emr where model_version_id = ",
      model_id
    )
  )
  
  dbDisconnect(myconn)
  
  df <- df %>%
    dplyr::rename(
      nid = source_nid,
      underlying_nid = source_underlying_nid
    ) %>%
    dplyr::select(-outlier_type_id, -model_version_id, -model_version_dismod_id)
  
  setDT(df)
  
  return(df)
}

## Option to remove claims and/or hospital data
remove_clinical_or_location_data <- function(data, locations_to_remove = NULL){
  message("Starting removal of clinical or location data")
  tmp_dt <- data
  
  clinical_nids_file <- "FILEPATH"
  print(paste0("Pulling in clinical nids from ", clinical_nids_file))
  clinical_nids <- as.data.table(read.xlsx(clinical_nids_file))
  
  # Subset cols, drop year_id, merge_nid
  clinical_nids_cleaned <- clinical_nids %>%
    select(nid, merged_nid, source_name, clinical_data_type_id, description) %>%
    # Remove leading/trailing whitespace; convert "na" to NA
    mutate(description =  trimws(description, which = "both"),
           merged_nid = ifelse(merged_nid == "na", NA, merged_nid),
           nid = ifelse(!is.na(merged_nid), merged_nid, nid)) %>%
    mutate(nid = as.integer(nid)) %>%
    select(-merged_nid) %>%
    distinct()
  
  
  # Filter for only inpatient (1), claims (3), claims - flagged (5)
  clinical_nids_filtered <- clinical_nids_cleaned %>%
    filter(
      clinical_data_type_id %in% c(1, 3, 5)
    ) %>%
    select(-clinical_data_type_id)
  
  # Remove specific claims flagged data
  if (remove_russia_claims_flagged) {
    clinical_nids_filtered <- clinical_nids_filtered %>%
      filter(!(source_name == "RUS_SY" & description == "claims - flagged"))
    print("Removing Russia claims flagged data if present")
  }
  
  if (remove_mongolia_claims_flagged) {
    clinical_nids_filtered <- clinical_nids_filtered %>%
      filter(!(source_name == "MNG_H_INFO" & description == "claims - flagged"))
    print("Removing Mongolia claims flagged data if present")
  }
  
  if (remove_korea_claims_flagged) {
    clinical_nids_filtered <- clinical_nids_filtered %>%
      filter(!(source_name == "KOR_HIRA" & description == "claims - flagged"))
    print("Removing Korea claims flagged data if present")
  }
  
  # Reshape claims/inpatient
  clinical_nids_reshaped <- clinical_nids_filtered %>%
    # Treat claims flagged as claims
    mutate(description = ifelse(description == 'claims - flagged',
                                'claims',
                                description)) %>%
    mutate(presence = 1) %>%
    pivot_wider(
      id_cols = c("nid", "source_name"),
      names_from = description,
      values_from = presence,
      values_fill = 0
    ) %>%
    setDT()
  
  # Label country claims
  clinical_nids_labeled <- clinical_nids_reshaped %>%
    mutate(
      taiwan_claims = ifelse((claims == 1 & source_name == "TWN_NHI"), 1, 0),
      russia_claims = ifelse((claims == 1 & source_name == "RUS_SY"), 1, 0),
      poland_claims = ifelse((claims == 1 & source_name == "POL_NHF"), 1, 0),
      us_claims = ifelse((claims == 1 & source_name == "MS"), 1, 0),
      singapore_claims = ifelse((claims == 1 & source_name == "SGP"), 1, 0)
    )
  
  print("Left joining clinical metadata")
  
  merge_dt <- tmp_dt %>% left_join(
    clinical_nids_labeled[, c("nid","inpatient","taiwan_claims","russia_claims",
                              "singapore_claims","poland_claims","us_claims")] %>%
      distinct(),
    by = 'nid') %>%
    as.data.table()
  
  # Remove specified clinical data
  if (remove_marketscan == TRUE) {
    merge_dt <- merge_dt[us_claims != 1 | is.na(us_claims), ]
    print("U.S. Marketscan data removed")
  }
  if (remove_taiwan == TRUE) {
    merge_dt <- merge_dt[taiwan_claims != 1 | is.na(taiwan_claims), ]
    print("Taiwan claims data removed")
  }
  if (remove_poland == TRUE) {
    merge_dt <- merge_dt[poland_claims != 1 | is.na(poland_claims), ]
    print("Poland claims data removed")
  }
  if (remove_russia == TRUE) {
    merge_dt <- merge_dt[russia_claims != 1 | is.na(russia_claims), ]
    print("Russia claims data removed")
  }
  if (remove_singapore == TRUE) {
    merge_dt <- merge_dt[singapore_claims != 1 | is.na(singapore_claims), ]
    print("Singapore claims data removed")
  }
  if (remove_inpatient == TRUE) {
    merge_dt <- merge_dt[inpatient != 1 | is.na(inpatient), ]
    print("Inpatient data removed")
  }
  
  print(paste("Original data had ", nrow(tmp_dt), "rows"))
  print(paste("New data now has", nrow(merge_dt), "rows"))
  ## Drop locations from EMR input data if desired
  if(!is.null(locations_to_remove)) print(paste("Removing the following locations:", paste(locations_to_remove, collapse = ", ")))
  merge_dt <- merge_dt[ ! merge_dt$location_id %in% locations_to_remove, ]
  
  message("Finished removing data")
  return(merge_dt)
}

reformat_data_for_modeled_emr <- function(data) {
  message("Starting formatting")
  dt <- data
  
  ## Cap age_end at 100 because GBD estimates age 0 - 99
  print("Forcing age above 100 to be 100 because GBD estimates 0 - 99")
  dt[age_end > 100, age_end := 100]
  
  ## Add variables for midpoint age, sex (male=0, female=1), and midyear
  print("Merging midpoint age, midpoint year, and sex to 0 (male) 1 (female)")
  dt[, midage := (age_start + age_end) / 2]
  dt[, sex_binary := ifelse(sex_id == 2, 1, 0)]
  dt[, year_id := round((year_start + year_end) / 2)]
  
  ## Add study ID to include random effects to MR-BRT model
  print("Adding ID column by nid/location_id pair for MRBRT random effects")
  dt[, id := .GRP, by = c("nid", "location_id")]
  
  ## Convert EMR mean and standard error to log space
  print("Converting mean and standard error to log space. This will take a while")
  message("Current mean summary")
  print(summary(dt$mean))
  dt[, log_ratio := log(mean)]
  message("Log mean summary")
  print(summary(dt$log_ratio))
  
  message("Current se summary")
  print(summary(dt$standard_error))
  
  dt$delta_log_se <- mapply(FUN = function(mu, sigma) {
    msm::deltamethod(g = ~ log(x1), mean = mu, cov = sigma^2)
  }, mu = dt$mean, sigma = dt$standard_error)
  
  print(summary(dt$delta_log_se))
  
  message("Finished with reformatting")
  return(dt)
}

plot_input_haq_data <- function(data, plot_output_path, haq_data, locs){
  message("Starting plot of input data")
  tmp_dt <- as.data.table(data)
  haq <- haq_data
  
  #Visualize HAQ and age spread of input data
  max_year_id <- max(haq$year_id)
  haq2 <- haq[with(haq, order(haqi_mean)), ]
  haq2 <- as.data.table(haq2[haq2$year_id == max_year_id, ])
  haq2[, index := .I]
  
  # Get super region ids, will drop super region rows in the process
  haq2 <- merge(
    haq2,
    locs[, c("location_id", "super_region_id")],
    by = "location_id"
  )
  
  # Use super region ids to get location name short of super region
  haq2 <- merge(
    haq2,
    locs[, c("location_id", "location_name_short")],
    by.x = "super_region_id",
    by.y = "location_id", all.x = TRUE
  )
  
  print("Starting plot 1")
  gg1 <- ggplot() +
    geom_point(
      data = haq2,
      aes(
        x = location_name_short,
        y = haqi_mean,
        color = location_name_short
      ),
      position = position_jitter(w = .1, h = 0)
    ) +
    scale_color_discrete(name = "Super Region") +
    labs(y = "HAQ") +
    ggtitle(glue::glue("HAQ Distribution by Super Region {max_year_id}")) +
    theme_classic() +
    theme(
      legend.text = element_blank(),
      legend.title = element_blank(),
      text = element_text(size = 8)
    ) +
    theme(legend.position = "none") +
    xlab("Super Region")
  
  gg1
  ggsave(gg1,
         filename = paste0(plot_output_path, cause_name, "_haq_dist_", date, ".pdf"),
         width = 6, height = 6
  )
  
  tmp_dt[, age_grp := round(tmp_dt$midage, -1)]
  
  print("Starting plot 2")
  gg2 <- ggplot() +
    geom_point(data = tmp_dt,
               aes(x = haqi_mean,
                   y = mean,
                   color = as.factor(sex_binary)),
               alpha = 0.4) +
    facet_wrap(~ tmp_dt$age_grp) +
    scale_fill_manual(guide = "none", values = c("midnightblue", "purple")) +
    scale_color_discrete(name = "Sex", labels = c("Male", "Female")) +
    labs(x = "HAQ", y = "EMR") +
    ggtitle(paste0("EMR input distribution vs HAQ faceted by age")) +
    theme_classic() +
    theme(text = element_text(size = 10, color = "black"), legend.position = "bottom") +
    coord_cartesian(xlim = NULL, ylim = NULL)
  
  ggsave(gg2,
         filename = paste0(plot_output_path, cause_name, "_input_data_plot_", date, ".pdf"),
         width = 10, height = 8
  )
  
  message(paste("Plots saved to", plot_output_path))
  
}

map_emr_input <- function(train_data){
  message("Starting map function")
  ## Pull in EMR input data used in MR-BRT regression with trim
  print("Modify dataframe")
  df <- train_data
  
  df <- merge(
    df,
    locs[,c("location_id", "location_name", "region_name", "ihme_loc_id", "level")],
    by = "location_id")
  
  df[, c("underlying_nid", "year_start", "year_end", "age_start", "age_end",
         "measure_id", "mean", "lower", "upper", "standard_error", "sex_binary",
         "haqi_mean", "log_ratio", "delta_log_se")] <- NULL
  
  df_trimmed <- as.data.table(df[df$w == 0, ])
  df_total <- as.data.table(df)
  
  ## Create mapping variable - one count per row of data by location and trim
  print("Creating mapping variable - one count per row by location and trim")
  df_trimmed <- df_trimmed[, list(mapvar_trim = .N), by = .(location_id)]
  df_total <- df_total[, list(mapvar_total = .N), by = .(location_id)]
  df_merge <- merge(df_trimmed, df_total, by = "location_id", all.y = T)
  df_merge[is.na(mapvar_trim), mapvar_trim := 0]
  df_merge[, mapvar := round(mapvar_trim / mapvar_total * 100)]
  setnames(df_total, "mapvar_total", "mapvar")
  
  ## ---GBD MAP--------------------------------------------------------------------
  print("Sourcing GBD Map function")
  gbd_map_func <- "FILEPATH"
  
  source(gbd_map_func)
  pdf(file = paste0(plot_output_path, '/pct_trimmed_emr_input_map_subnational.pdf'),
      height = 4.15, width = 7.5,
      pointsize = 6.5)
  
  print("Saving % trimmed EMR input data map by subnationals")
  gbd_map(
    df_merge,
    limits = c(0, 1, 25, 50, 75, 100),
    sub_nat = "all",
    legend = TRUE,
    inset = TRUE,
    labels = c("0%", "1% to 25%", "25% to 50%", "50% to 75%", "75% to 100%"),
    pattern = NULL,
    col = "RdYlBu",
    col.reverse = TRUE,
    na.color = "white",
    title = paste0("% trimmed EMR input data by subnational location for ",
                   cause_name, ", trim =", trim, ", modeled on ", date),
    fname = NULL,
    legend.title = NULL,
    legend.columns = NULL,
    legend.cex = 1,
    legend.shift = c(0, 0)
  )
  
  dev.off()
  
  pdf(file = paste0(plot_output_path, '/pct_trimmed_emr_input_map_national.pdf'),
      height = 4.15, width = 7.5,
      pointsize = 6.5)
  
  print("Saving % trimmed EMR input data map by nationals")
  gbd_map(
    df_merge,
    limits = c(0, 1, 25, 50, 75, 100),
    sub_nat = "none",
    legend = TRUE,
    inset = TRUE,
    labels = c("0%", "1% to 25%", "25% to 50%", "50% to 75%", "75% to 100%"),
    pattern = NULL,
    col = "RdYlBu",
    col.reverse = TRUE,
    na.color = "white",
    title = paste0("% trimmed EMR input data by national location for ",
                   cause_name, ", trim =", trim, ", modeled on ", date),
    fname = NULL,
    legend.title = NULL,
    legend.columns = NULL,
    legend.cex = 1,
    legend.shift = c(0, 0)
  )
  
  dev.off()
  
  print("Saving total EMR input data map by subnationals")
  pdf(file = paste0(plot_output_path, 'FILEPATH'),
      height = 4.15, width = 7.5,
      pointsize = 6.5)
  gbd_map(
    df_total,
    limits = c(1, 25, 50, 100, 500, 1000000000),
    sub_nat = "all",
    legend = TRUE,
    inset = TRUE,
    labels = c("1 to 25 data points", "26 to 50 data points",
               "51 to 100 data points", "101 to 500 data points",
               "501+ data points"),
    pattern = NULL,
    col = "RdYlBu",
    col.reverse = TRUE,
    na.color = "white",
    title = paste0("Total EMR input data (trimmed and untrimmed) by subnational location for ",
                   cause_name, ", trim =", trim, ", modeled on ", date),
    fname = NULL,
    legend.title = NULL,
    legend.columns = NULL,
    legend.cex = 1,
    legend.shift = c(0, 0)
  )
  
  dev.off()
  
  print("Saving total EMR input data map by nationals")
  pdf(
    file = paste0(plot_output_path, "FILEPATH"),
    height = 4.15, width = 7.5,
    pointsize = 6.5
  )
  gbd_map(
    df_total,
    limits = c(1, 25, 50, 100, 500, 1000000000),
    sub_nat = "none",
    legend = TRUE,
    inset = TRUE,
    labels = c(
      "1 to 25 data points", "26 to 50 data points",
      "51 to 100 data points", "101 to 500 data points",
      "501+ data points"
    ),
    pattern = NULL,
    col = "RdYlBu",
    col.reverse = TRUE,
    na.color = "white",
    title = paste0("Total EMR input data (trimmed and untrimmed) by national location for ",
                   cause_name, ", trim =", trim, ", modeled on ", date),
    fname = NULL,
    legend.title = NULL,
    legend.columns = NULL,
    legend.cex = 1,
    legend.shift = c(0, 0)
  )
  
  dev.off()
  
  df_csv <- merge(df_merge, locs[, c("location_id", "location_name")], by = "location_id")
  df_csv <- df_csv[order(df_csv$mapvar, decreasing = TRUE), ]
  setnames(
    df_csv,
    old = c("mapvar_trim", "mapvar_total", "mapvar"),
    new = c("Total trimmed", "Total input", "% trimmed")
  )
  df_csv <- setcolorder(df_csv, c(
    "location_id", "location_name",
    "Total input", "Total trimmed", "% trimmed"
  ))
  print(paste("Saving a dataframe version at ", plot_output_path))
  write.csv(df_csv, paste0(plot_output_path, "FILEPATH"))
  message("Finised -----")
}

graph_mrbrt_results <- function(results, predicts, space){
  data_dt <- as.data.table(results)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  
  model_dt <- as.data.table(predicts)
  model_dt[, haqi_mean := round(haqi_mean, digits =2)]
  quant_haq <- round(quantile(model_dt$haqi_mean, probs = seq(0,1, by = 0.50)), digits = 2)
  
  # Must have 3 quant haq values to plot 3 lines on the graph (min, median, max)
  if (length(quant_haq) != 3) {
    stop("The length of quant_haq is not equal to 3. Please evaluate.")
  }
  
  haqi_unique <- unique(model_dt$haqi_mean)
  # Identify the closest value to the median and replace quant_haq median with it
  haqi_median_tmp <- haqi_unique[which.min(abs(haqi_unique - quant_haq[2]))]
  quant_haq[2] <- haqi_median_tmp[1]
  
  model_dt <- model_dt[haqi_mean %in% quant_haq]
  model_dt[, X_haqi_mean := as.character(haqi_mean)]
  model_dt[, sex := ifelse(sex_binary == 1, "Female", "Male")]
  setorder(model_dt, haqi_mean)
  
  # Set space specific transformations and labels
  if (space == "log") {
    func1 <- identity
    ylab1 <- "EMR, log scale"
  } else if (space == "normal") {
    func1 <- exp
    ylab1 <- "EMR"
  } else {
    stop("Invalid space argument. Please use 'log' or 'normal'.")
  }
  
  gg_subset <- ggplot() +
    geom_jitter(
      data = data_dt,
      aes(
        x = midage,
        y = func1(log_ratio),
        color = factor(excluded, levels = c(0, 1))
      ),
      width = 0.6, alpha = 0.2, size = 2
    ) +
    geom_line(
      data = model_dt,
      aes(
        x = midage,
        y = pred1,
        color = factor(haqi_mean, levels = quant_haq),
        linetype = sex
      ),
      linewidth = 1.5
    ) +
    labs(
      x = "Age",
      y = ylab1
    ) +
    ggtitle(paste0("MR-BRT model results overlay on EMR input data (", space, " space)")) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    scale_color_manual(
      name = "",
      values = c("#999999", "#E69F00", "#56B4E9", "#0072B2", "#009E73"),
      labels = c("Included EMR", "Trimmed EMR", "Min HAQ", "50th Percentile HAQ", "Max HAQ")
    ) +
    guides(
      colour = guide_legend(override.aes = list(
        shape = c(16, 16, NA, NA, NA),
        linetype = c(0, 0, 1, 1, 1),
        size = c(3, 3, 2, 2, 2)
      ))
    )
  
  return(gg_subset)
}

merge_haqi_covariate <- function(data, release_id) {
  dt <- copy(data)
  # Add column for haqi subsetted to location_id
  # use midyear for prevalence data to subset to haqi year_id
  print("Getting HAQi covariate")
  
  # TODO should the model version's location_set_id, and location_set_version_id
  # be passed into here or is release_id good enough?
  haq <- get_covariate_estimates(
    covariate_id = 1099, # Healthcare access and quality index
    release_id = release_id
  )
  
  # Subset columns, add row id
  haq <- haq %>%
    select(location_id, year_id, haqi_mean = mean_value) %>%
    dplyr::mutate(index = dplyr::row_number())
  
  
  # Initial row count
  n <- nrow(dt)
  
  dt <- merge(dt, haq, by = c("location_id", "year_id"), all.x = TRUE)
  
  # Check for any missing location year combinations that failed to merge a haq value
  missing_haq <- dt[is.na(haqi_mean), .(location_id, year_id)] %>%
    distinct()
  
  dt <- dt[!is.na(haqi_mean)]
  dt[, c("index", "year_id")] <- NULL
  
  if (nrow(dt) != n) {
    
    message("Error! Not all location-year combinations in the EMR data have a matching HAQI covariate value.\nTroubleshooting: ensure that all location_id values for the prevalence/incidence data in your preliminary DisMod model exist in the most up-to-date GBD location hierarchy")
    
    for (location in unique(missing_haq$location_id)) {
      for (year in missing_haq[location_id == location, year_id]) {
        message(paste0("Location ", location, " year ", year, " missing"))
      }
    }
    
    stop("Missing HAQI values detected.")
    
  }
  return(
    list(
      "dt" = dt,
      "haq" = haq
    )
  )
}

apply_mrbrt_ratios_and_format <- function(data, seq_of_dummy_emr, extractor) {
  final_dt <- as.data.table(data)
  seq_emr <- seq_of_dummy_emr
  
  final_dt[, log_pre_se := (log_upper - log_lower) / (2 * qnorm(0.975, 0, 1))]
  # pred_df[, standard_error := msm::deltamethod(~exp(x1), mean, log_pre_se^2), by=c("log_mean", "log_pre_se")]
  
  final_dt$standard_error <- mapply(FUN = function(mu, sigma) {
    msm::deltamethod(g = ~exp(x1), mean = mu, cov = sigma^2)
  }, mu = final_dt$log_mean, sigma = final_dt$log_pre_se)
  
  final_dt$mean <- exp(final_dt$log_mean)
  
  ## For uploader validation - add in required columns
  final_dt[, crosswalk_parent_seq := seq_emr]
  final_dt[, sex := ifelse(sex_binary == 1, "Female", "Male")]
  final_dt[, year_start := year_id]
  final_dt[, year_end := year_id]
  final_dt[, nid := 416752]
  final_dt[, year_issue := 0]
  final_dt[, age_issue := 0]
  final_dt[, sex_issue := 0]
  final_dt[, measure := "mtexcess"]
  final_dt[, source_type := "Facility - inpatient"]
  final_dt[, extractor := extractor]
  final_dt[, is_outlier := 0]
  final_dt[, measure_adjustment := 1]
  final_dt[, recall_type := "Point"]
  final_dt[, urbanicity_type := "Mixed/both"]
  final_dt[, unit_type := "Person"]
  final_dt[, representative_name := "Nationally and subnationally representative"]
  final_dt[, note_modeler := paste0("These data are modeled from CSMR and prevalence using HAQI as a predictor on ", date)]
  final_dt[, unit_value_as_published := 1]
  final_dt <- left_join(final_dt, locs[,c("location_id", "location_name")], by="location_id")
  final_dt[,c("sex_binary", "year_id", "log_mean", "log_pre_se", "log_lower", "log_upper")] <- NULL
  
  # Set standard error greater than 1 to 1, occurs in clinical data
  print("Setting standard error >1 = 1")
  final_dt <- as.data.table(final_dt)
  final_dt[standard_error >1, standard_error  := 1]
  
  message("Finished, returning reformatted data")
  return(final_dt)
  
}

#' Get model version id metadata in the epi.model_version table. For a given model_version_id
#' Return location_set_version_id, location_set_id, release_id, gbd_round_id
#'
#' @param model_id [integer] model_version_id
#'
#' @return [list] location_set_version_id, location_set_id, release_id, gbd_round_id
#' @export
#'
#' @examples
get_model_version_metadata <- function(model_id) {
  
  query_db <- function(db,query_string) {
    odbc <- ini::read.ini('FILEPATH')
    
    con <- DBI::dbConnect(RMySQL::MySQL(),
                          user = odbc[[db]]$user,
                          password = odbc[[db]]$password,
                          dbname = odbc[[db]]$database,
                          host = odbc[[db]]$server)
    query_results <- DBI::dbGetQuery(con, query_string)
    
    DBI::dbDisconnect(con)
    return(query_results)
  }
  
  # Get location_set_version_id and release_id associated with the model_version_id
  query_string <- glue::glue("
SELECT * FROM epi.model_version
where model_version_id = {model_id};
")
  
  model_version_id_metadata <- query_db(db = "epi", query_string = query_string)
  
  # Get location_set_id from the location_set_version_id
  query_string <- glue::glue("
SELECT * FROM shared.location_set_version
WHERE location_set_version_id = {model_version_id_metadata$location_set_version_id};
")
  
  location_set_version_metadata <- query_db(db = "shared", query_string = query_string)
  
  # Get GBD round release_ids
  query_string <- "
SELECT *
FROM shared.release
WHERE include_in_gbd_rotation = 1;
"
  gbd_round_metadata <- query_db(db = "shared", query_string = query_string)
  
  current_gbd_round_release_id <- max(gbd_round_metadata$release_id)
  
  return(
    list(
      location_set_version_id = model_version_id_metadata$location_set_version_id,
      location_set_id = location_set_version_metadata$location_set_id,
      release_id = model_version_id_metadata$release_id,
      gbd_round_id = model_version_id_metadata$gbd_round_id
    )
  )
}


identify_missing_locations_from_map <- function(dt, loc_hier, output_path) {
  
  # First check that all locations in data are in the location hierarchy
  if(any(!dt$location_id %in% loc_hier$location_id)) {
    stop("There are locations in the data that are not in the location hierarchy!")
  }
  
  # sub_nat = 'all'
  load("FILEPATH")
  all_map_missing_locs <- setdiff(dt$location_id, map@data$loc_id)
  
  # sub_nat = 'none'
  load("FILEPATH")
  sub_nat_none_dt <- dt[location_id %in% locs[level <= 3, location_id]]
  nat_map_missing_locs <- setdiff(dt$location_id, map@data$loc_id)
  
  # Join location metdata
  source("FILEPATH")
  loc_names <- get_ids('location')
  
  all_missing_locs <- c(all_map_missing_locs, nat_map_missing_locs)
  
  if (length(all_missing_locs) == 0) {
    message("No missing locations found")
  } else {
    message("Writing missing locations to file")
    
    all_missing_locs_dt <- data.table(
      location_id = all_missing_locs) %>%
      left_join(loc_names, by = "location_id")
    
    fp <- file.path(output_path, "FILEPATH")
    fwrite(all_missing_locs_dt, fp)
  }
  
}
