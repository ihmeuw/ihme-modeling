
redistribute_ages_uniform <- function(df, age_starts, objective_columns) {
  
  #message(paste(unique(df$location_name), unique(df$date), sep=" | " ))
  
  range_columns <- c("age_start", "age_end")
  keep_columns <-c(range_columns, objective_columns)
  
  counter <- 0
  for (objective_column in objective_columns) {
    df_objective_column <- .get_df_rebin_column_uniform(df, age_starts, range_columns, objective_column)
    if (counter < 1) {
      df_rebin <- data.frame(df_objective_column)
    }else {
      df_rebin <- merge(df_rebin, df_objective_column, sort = FALSE)
    }
    counter <- counter + 1
  }
  
  for (colname in colnames(df)) {
    if (!(colname %in% keep_columns)) {
      df_rebin[colname] <- df[1,][colname]
    }
  }
  
  return(df_rebin)
}


# Local method per column
.get_df_rebin_column_uniform <- function(df, age_starts, range_columns, objective_column) {
  
  df <- as.data.frame(df)
  
  check <- any(c(duplicated(df$age_start), duplicated(df$age_end)))
  if (check) stop("Rebin stopped due to overlapping age groups. Ensure all age groups in object 'df' are discrete")
  
  LIMIT_YEAR <- 125
  age_from <- range_columns[1]
  age_to <- range_columns[2]
  # Get doses/year per row
  keep_columns <- c(range_columns, objective_column)
  df_doses_year <- df[keep_columns]
  df_doses_year <- sapply(df_doses_year, as.numeric)
  df_doses_year <- data.frame(df_doses_year)
  df_doses_year$doses_year <- df_doses_year[[objective_column]] / (df_doses_year[[age_to]] - df_doses_year[[age_from]] + 1)
  
  # Get ages_reference
  #age_reference <- seq(from = min(age_starts), to = LIMIT_YEAR, by = 1)
  age_reference <- seq(from = 0, to = LIMIT_YEAR, by = 1)
  
  # Make df_by_year
  df_by_year <- data.frame(age_reference)
  # Mean(doses/year) of all 0-125 years.
  df_by_year$doses <- NA
  
  for (i in 1:nrow(df_doses_year)) {
    row <- df_doses_year[i,]
    df_by_year["doses"][df_by_year$age_reference>=row[[age_from]] & df_by_year$age_reference<=row[[age_to]],] <- row$doses_year
  }
  
  na_obs <- which(is.na(df_by_year$doses))
  df_by_year[is.na(df_by_year$doses), 'doses'] <- 0
  
  # Important to check here bc age_starts may not include all age groups provided
  check <- round(sum(df_by_year$doses, na.rm=T)) == round(sum(df[[objective_column]], na.rm=T))
  if (!check) stop('Number doses per age year does not sum properly')
  
  #df_by_year$doses[na_obs] <- NA
  
  #test <- df_by_year
  #extrapolate extra row not presented in original using near approx.
  
  # Uncertain what this is for, but appears setting NA to zero help make this unnecessary
  #df_na_rows <- df_by_year[is.na(df_by_year$doses),]
  #
  #if (nrow(df_na_rows) > 0) {
  #  for (i in 1:nrow(df_na_rows)) {
  #    row <- df_na_rows[i,]
  #    from_vector <- abs(df_doses_year[[age_from]] - row$age_reference )
  #    to_vector <- abs(df_doses_year[[age_to]] - row$age_reference )
  #    index_group <- which.min(from_vector + to_vector)
  #    df_by_year$doses[i] <- df_doses_year$doses_year[index_group]
  #  }
  #}
  
  #cbind(test, df_by_year)
  
  #set max limit only for users purposes.
  if (max(age_starts) <= LIMIT_YEAR) {
    age_starts <- c(age_starts, LIMIT_YEAR + 1)
  }
  
  # Prepare new bins.-make the new rebin
  new_bins <- age_starts - 1
  df_by_year$income <- cut(df_by_year$age_reference, breaks = new_bins)
  df_result <- aggregate(df_by_year$doses, by=list(income=df_by_year$income), FUN=function(x) {sum(x)})
  
  
  ###### reconstruct dataframe columns #####
  
  levels <- levels(df_result$income)
  level_ranges <- (strsplit(levels, split = ","))
  
  level_matrix <- sapply(level_ranges, unlist)
  level_matrix <- gsub("\\(|]", "", level_matrix)# gsub replacement.
  level_matrix <- t(level_matrix)
  
  # Fix intervals.
  from_column <- as.numeric(c(level_matrix[, 1])) + 1
  to_column <- as.numeric(c(level_matrix[, 2]))
  df_rebin <- data.frame(from_column, to_column, df_result$x)
  
  #name propery col.
  names(df_rebin) <- keep_columns
  
  
  #######################################################################################
  #######################################################################################
  ## Check
  #sum(df_by_year$count) == sum(df_doses_year$no)
  
  sum_original <- sum(as.numeric(df[[objective_column]]), na.rm=T)
  sum_rebin <- sum(as.numeric(df_rebin[[objective_column]]), na.rm=T)
  
  if (sum_original == sum_rebin) {
    
    #message(sprintf("Rebin for column=%s sum balanced. Sum for this columns= %f", objective_column, sum_original))
    message(glue::glue('Rebin for {objective_column} was successful. Original total = {sum_original}, Rebin total = {sum_rebin}'))
    
  } else{
    
    #####################SUM NOT EQUAL IF YOU EXTRAPOLATED IN THE BINS############
    ## EXTRAPOLATED VALUES, FILLED BY NEAR RANGE.#################################
    age_starts_string <- paste(age_starts, collapse = ",")
    message_extrapolate <- paste('Take care result column=', objective_column, ' sum is not equal, you are extrapolating in the rebin.
    New bins: [', age_starts_string,'] are outside of ranges from original df.', collapse = ",")
    warning(message_extrapolate)
    
    #stop(glue::glue('Rebin for {objective_column} failed. Original total = {sum_original}, Rebin total = {sum_rebin}'))
    
  }
  
  # Summary row. optional
  #df_rebin <- .add_summarice_row(df_rebin, age_starts, "total_administred" )
  return(df_rebin)
}

