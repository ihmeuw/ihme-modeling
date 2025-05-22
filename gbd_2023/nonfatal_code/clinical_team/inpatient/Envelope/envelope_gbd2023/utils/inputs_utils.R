# GBD age group metadata
pull_age_metadata <- function(release_id, age_group_set_id) {
  age_metadata <- get_age_metadata(release_id = release_id, age_group_set_id = age_group_set_id)
  setnames(age_metadata,
    old = c("age_group_years_start", "age_group_years_end"),
    new = c("age_start", "age_end")
  )
  age_metadata <- age_metadata %>% select(age_group_id, age_start, age_end, age_group_name)
  return(age_metadata)
}

# All the existing age groups directly from the DB to assign age group ID to nonstandard age groups
pull_complete_age_metadata <- function() {
  # get all age group IDs
  # pull the most up-to-date inpatient run from the database to get source_name
  library(RMySQL)
  db_connection <- function() {
    myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
      dbname = "DBPATH",
      user = "USERNAME",
      password = "PASSWORD",
      host = "ADDRESS"
    )
    return(myconn)
  }

  library(DBI)
  myconn <- db_connection()
  sql_command <- "SELECT age_group_id, age_group_years_start, age_group_years_end, age_group_name FROM shared.age_group"

  complete_age_metadata <- dbGetQuery(myconn, sql_command)

  # close the connection
  dbDisconnect(myconn)

  complete_age_metadata <- as.data.table(complete_age_metadata)
  setnames(complete_age_metadata, c("age_group_id", "age_start", "age_end", "age_group_name"))

  # Remove age-standardized (age group ID 27) and duplicate age groups
  complete_age_metadata <- complete_age_metadata %>% 
    filter(age_group_id != 27) %>%  # keeping All Ages, excluding age-standardized
    filter(age_group_id != 49) %>% # keeping 12 to 23 months, excluding 1
    filter(age_group_id != 237) %>% # keeping 90 plus, excluding 90 to 124
    filter(age_group_id != 308) %>% # keeping 70+ years, excluding 70 plus - not recognized by Epi Uploader
    filter(age_group_id != 161) %>%  # keeping <1 year, excluding 0
    filter(age_group_id != 36) %>% # keeping 2 to 19, excluding 2 to 19, age standardized
    filter(age_group_id != 371) %>% # keeping 124, removing 124+
    filter(age_group_id != 38)# keeping 20 plus, removing 20 plus, age-standardized

  # Validate that no duplicate age_start-age_end combinations are present to avoid merging errors
  duplicated_age_groups <- complete_age_metadata %>%
    dplyr::group_by(age_start, age_end) %>%
    dplyr::filter(n() > 1) %>%
    dplyr::ungroup() %>%
    dplyr::select(age_group_id, age_start, age_end)
  
  # Warn if so
  if (nrow(duplicated_age_groups) > 0) {
    warning("Duplicate age groups detected. Please review the following age groups:")
    print(duplicated_age_groups)
  }
  
  return(complete_age_metadata)
}

# get GHDx nid, underlying_nid, series_nid, data_type_name, title
pull_ghdx_metadata <- function() {
  library(RMySQL)
  db_connection <- function() {
    myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
      dbname = "DBNAME",
      user = "USERNAME",
      password = "PASSWORD",
      host = "ADDRESS"
    )
    return(myconn)
  }

  library(DBI)
  myconn <- db_connection()
  lookup1 <- "SELECT nid AS underlying_nid, title AS underlying_field_citation_value FROM ghdx.node;"
  lookup2 <- "SELECT nid AS series_nid, title AS series_field_citation_value FROM ghdx.node;"

  bigquery <- "
SELECT
    n.nid
  , n.title AS field_citation_value
  , ttd.name data_type_name
  , fdfrn.field_references_node_target_id AS underlying_nid
  , fdfsos.field_series_or_system_target_id AS series_nid
FROM
  node n
  join field_data_field_type fdft on fdft.entity_id=n.nid    -- data type field for NIDs
  join taxonomy_term_data ttd on ttd.tid=fdft.field_type_tid -- data type definition
  left join field_data_field_references_node fdfrn on fdfrn.entity_id=n.nid
  left join field_data_field_series_or_system fdfsos on fdfsos.entity_id=n.nid
WHERE
      n.type='record'
  and n.status=1 -- active, published NIDs
;"

  dt1 <- dbGetQuery(myconn, bigquery) %>% as.data.table()
  dt2 <- dbGetQuery(myconn, lookup1) %>% as.data.table()
  dt3 <- dbGetQuery(myconn, lookup2) %>% as.data.table()

  dt4 <- merge(dt1, dt2, by = "underlying_nid", all.x = TRUE)
  dt5 <- merge(dt4, dt3, by = "series_nid", all.x = TRUE)

  # close the connection
  dbDisconnect(myconn)

  ghdx_metadata <- copy(dt5)
  ghdx_metadata <- unique(ghdx_metadata)

  # save ages in the global environment
  # assign("ghdx_db_output", ghdx_db_output, envir = .GlobalEnv)

  return(ghdx_metadata)
}

# format cols and filter ----
calc_uncertainty <- function(df, confidence = 0.95) {
  
  # Initialize columns
  df$lower <- 0
  df$upper <- 0
  df$standard_error <- 0
  
  # Split the data frame into two based on cases <= 5 and cases > 5
  df_sub5 <- df %>% filter(sample_size <= 5)
  df_over5 <- df %>% filter(sample_size > 5)
  
  # Calculate standard error for df_sub5
  df_sub5$standard_error <- ifelse(df_sub5$sample_size <= 5,
                                   ((5 - df_sub5$val * df_sub5$sample_size) / df_sub5$sample_size
                                    + df_sub5$val * df_sub5$sample_size * sqrt(5 / df_sub5$sample_size^2)) / 5,
                                   NA)
  
  # Calculate standard error for df_over5 - Poisson distribution
  if (nrow(df_over5) > 0) {
    df_over5$standard_error <- sqrt(df_over5$val / df_over5$sample_size)
  }
  
  # Combine the data frames
  df <- bind_rows(df_sub5, df_over5)
  
  # Calculate the lower and upper bounds of the confidence interval
  low_quantile <- (1 - confidence) / 2
  up_quantile <- 1 - low_quantile
  
  df$lower <- df$val + qnorm(low_quantile) * df$standard_error
  df[df$lower < 0, "lower"] <- 0
  
  df$upper <- df$val + qnorm(up_quantile) * df$standard_error
  
  df <- df %>% select(-c("lower", "upper"))
  
  return(df)
}

# Add country_id (level 3)
append_country_ids_names <- function(dt, location_metadata) {
  
  location_metadata <- get_location_metadata(release_id = release_id, 
                                             location_set_id = location_set_id)
  
  dt_locations <- unique(dt[, .(location_id)])
  dt_locations <- merge(dt_locations, location_metadata[, .(location_id, location_name, level, parent_id)],
                        by = "location_id", all.x = TRUE)
  cat("NA check in dt_locations:", sum(is.na(dt_locations)), "\n")
  level3 <- unique(dt_locations[level == 3, .(location_id, country_id = location_id)])
  level4 <- unique(dt_locations[level == 4, .(location_id, country_id = parent_id)])
  
  level5 <- dt_locations[level == 5, .(location_id, level_4_location_id = parent_id)] %>% unique()
  level5 <- merge(level5, location_metadata[level == 4, .(location_id, parent_id)], 
                  by.x = "level_4_location_id", by.y = "location_id", all.x = TRUE) %>%
    .[, .(location_id, country_id = parent_id)] %>%
    unique()
  
  all_levels <- rbind(level3, level4, level5, fill = TRUE) %>% unique()
  
  all_levels <- merge(all_levels, location_metadata[, .(location_id, location_name)],
                      by.x = "country_id", by.y = "location_id", all.x = TRUE) %>% 
    setnames("location_name", "country_name")
  
  cat("NA check in all_levels:", sum(is.na(all_levels)), "\n")
  
  # Merging back to the original data table to append country_id
  result_dt <- merge(dt, all_levels, by = "location_id", all.x = TRUE)
  
  return(result_dt)
}