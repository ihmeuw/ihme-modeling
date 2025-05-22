
# source libraries --------------------------------------------------------

library(data.table)

# get data to check -------------------------------------------------------

identify_rows_to_check <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(
    df$note_adjusted == "Not Specified" & 
      df$cluster_altitude > 400 &
      !(is.na(df$sample_size)) &
      df$sex_id != 3
  )
  
  inverse_i_vec <- setdiff(
    seq_len(nrow(df)),
    i_vec
  )
  
  return(list(
    good_df = df[inverse_i_vec, ],
    to_check_df = df[i_vec, ]
  ))
}

# label adjust type for specified rows ------------------------------------

assign_adjust_type <- function(input_df){
  df <- copy(input_df)
  
  df$adj_type <- NA_character_
  
  i_vec <- which(grepl("altitude|smoking", df$note_adjusted, ignore.case = TRUE))
  df$adj_type[i_vec] <- "survey_adjusted"
  
  i_vec <- which(df$note_adjusted == "None" | df$note_adjusted == "Not Specified")
  df$adj_type[i_vec] <- "unadjusted"
  
  return(df)
}

# load in gbd hb data -----------------------------------------------------

get_gbd_data <- function(gbd_rel_id, loc_id_vec, year_id_vec){
  me_id_list <- list(
    mean_adj_hb = c(10487, 750464), 
    total_anemia_prev = c(10507, 750466),
    mild_anemia_prev = c(10489, 750467),
    mod_anemia_prev = c(10490, 750469), 
    sev_anemia_prev = c(10491, 750468)
  ) 
  
  df <- data.table()
  for(i in names(me_id_list)){
    temp_df <- ihme::get_model_results(
      gbd_team = 'epi', 
      gbd_id = me_id_list[[i]][1],
      model_version_id = me_id_list[[i]][2],
      location_id = loc_id_vec,
      year_id = year_id_vec,
      sex_id = 1:2,
      release_id = gbd_rel_id
    )
    temp_df$me_type <- i
    df <- rbindlist(list(df, temp_df), use.names = T, fill = T)
  }
  
  age_df <- get_age_metadata(release_id = gbd_rel_id)
  age_df <- age_df[order(age_group_years_start)]
  age_df$ordinal_val <- seq_len(nrow(age_df))
  
  return(list(
    me = df,
    age = age_df
  ))
}

get_estimation_years <- function(input_df, release_id) {
  df <- data.table::copy(input_df)
  # assign year as approx midpoint between year start and year end
  df$year_id <- floor(as.numeric(df$year_end) + as.numeric(df$year_start)) / 2
  
  # get gbd demographic years
  est_years <- get_demographics(
    gbd_team = "epi",
    release_id = release_id
  )$year_id
  
  df$est_year_id <- unlist(lapply(seq_len(nrow(df)), 
                                  function(x) 
                                    closest(df$year_id[x], est_years)))
  
  return(df)
}

closest <- function(x, y) {
  y[which.min(abs(y - x))]
}

# impute each row using the me data ---------------------------------------

impute_each_row <- function(input_df, gbd_list, gbd_rel_id) {
  df <- data.table::copy(input_df)
  df <- get_estimation_years(df, gbd_rel_id)
  me_df <- data.table::copy(gbd_list$me)
  age_df <- data.table::copy(gbd_list$age)
  
  df$modeled_hb <- 0
  
  for (r in seq_len(nrow(df))) {
    assign("curr_row", df[r, ], envir = .GlobalEnv)
    
    age_vec <- get_age_bounds(
      input_age_df = age_df,
      lower_age = df$age_start[r],
      upper_age = df$age_end[r]
    )
    
    dis_year <- as.numeric(df$est_year_id[r])
    dis_loc <- as.numeric(df$location_id[r])
    dis_preg <- as.numeric(df$cv_pregnant[r])
    me_type <- as.character(df$me_type[r])
    
    i_vec <- which(
      me_df$year_id == dis_year &
        me_df$location_id == dis_loc &
        me_df$age_group_id %in% age_vec &
        me_df$me_type == me_type
    )
    temp_me_df <- me_df[i_vec, ]
    
    mean_val <- mean(temp_me_df$mean)
    
    if(dis_preg == 1) {
      mean_val <- mean_val * 0.92
    }
    
    df$modeled_value[r] <- mean_val
  }
  return(df)
}

get_age_bounds <- function(input_age_df, lower_age, upper_age) {
  age_df <- data.table::copy(input_age_df)
  
  i_vec <- which(
    age_df$age_group_years_start <= lower_age &
      age_df$age_group_years_end > lower_age
  )
  lower_age_ord <- age_df[["ordinal_val"]][i_vec]
  
  i_vec <- which(
    age_df$age_group_years_start <= upper_age &
      age_df$age_group_years_end > upper_age
  )
  upper_age_ord <- age_df[["ordinal_val"]][i_vec]
  
  ord_vec <- c(lower_age_ord:upper_age_ord)
  i_vec <- which(
    age_df$ordinal_val %in% ord_vec
  )
  
  return(age_df$age_group_id[i_vec])
}

# get percentage difference in actual versus modeled value ----------------

get_pct_diff <- function(input_df){
  df <- copy(input_df)
  
  df$pct_diff <- (df$modeled_value - df$mean) / df$mean * 100
  
  i_vec <- which(df$pct_diff < 5)
  inverse_i_vec <- setdiff(seq_len(nrow(df)), i_vec)
  
  df$adj_type <- NA_character_
  df$adj_type[i_vec] <- "unadjusted"
  df$adj_type[inverse_i_vec] <- "survey_adjusted"
  
  return(df)
}

# get_gbd_hb_results ------------------------------------------------------

main_get_gbd_hb_results <- function(input_df, gbd_rel_id){
  df <- copy(input_df)
  
  df_list <- identify_rows_to_check(input_df = df)
  
  df <- copy(df_list$good_df)
  to_check_df <- copy(df_list$to_check_df)
  
  df <- assign_adjust_type(input_df = df)
  
  df <- rbindlist(
    list(df, to_check_df),
    use.names = TRUE,
    fill = TRUE
  )
  
  return(df)
}
