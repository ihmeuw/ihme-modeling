
redistribute_ages_pop_weighted <- function(df, age_starts, objective_columns) {

  #message(paste(unique(df$location_name), unique(df$date), sep=" | " ))

  range_columns <- c("age_start", "age_end")
  keep_columns <-c(range_columns, objective_columns)

  pop_dt <- ALL_POP[location_id == unique(df$location_id)]

  # rename population data table columns to make it easier to manage
  setnames(pop_dt, c("age_group_years_start", "age_group_years_end"), range_columns)

  counter <- 0
  for (objective_column in objective_columns) {
    df_objective_column <- .get_df_rebin_column_pop_weighted(df, pop_dt, age_starts, range_columns, objective_column)
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
.get_df_rebin_column_pop_weighted <- function(df, pop_dt, age_starts, range_columns, objective_column) {

  #LIMIT_YEAR <- 125

  dt <- as.data.table(df, TRUE)


  # Instead of passing around these variables, we will manage them as global in this scope
  AGE_START <- range_columns[1]
  AGE_END <- range_columns[2]
  OBJ_COLUMN <- objective_column

  # Create empty data table
  final_out <- data.table()

  nrows_counter <- 1
  NROWS <- nrow(dt)
  NROWS <- c(1:NROWS)

  for (i in c(1:length(age_starts))){
    next_index <- i + 1
    ages <- age_starts[i:next_index]

    start <-ages[1]
    end <- ages[2]

    if (start == 0) {
      start <- ages[1]
    } else {
      start <- ages[1] + 1
    }

    result <- .get_bin_dt(dt, nrows_counter, pop_dt, start, end)
    slice_dt <- result[[1]]
    nrows_counter <- result[[2]]

    tmp_dt <- data.table(
      age_start=start,
      age_end=end,
      total=sum(slice_dt[[OBJ_COLUMN]])
    )

    setnames(tmp_dt, "total", objective_column)
    final_out <- rbind(final_out, tmp_dt)

    if (end == tail(age_starts, n=1)){
      break
    }
  }

  return(final_out)
}


#----utilities----
.quit_summarize_and_na_rows <- function(df) {
  sel <-  c(which(is.na(df$age_start)),
            which(is.na(df$age_end)),
            which(df$age_start == 0 & df$age_end == 125))
  df <- df[-sel,]

}

.add_summarice_row <- function(df_rebin, age_starts, column) {
  total_df <- data.frame(0, max(age_starts), sum(df_rebin[[column]]))
  names(total_df) <- colnames(df_rebin)
  df_rebin <- rbind(total_df, df_rebin)
  return(df_rebin)
}

.create_age_tables <- function(row) {
  year_data <- row[[OBJ_COLUMN]] / row$divisor
  start <- row[[AGE_START]]
  end <- row[[AGE_END]] - 1
  dt <- data.table(location_id = row$location_id,
                   age_start = row[[AGE_START]],
                   age_end = row[[AGE_END]],
                   year_data = year_data,
                   age = start:end,
                   divisor = row$divisor
  )
  return(dt)
}

.unbin_data_table <- function(dt) {
  final_out <- data.table()
  for (i in 1:nrow(dt)) {
    tmp_dt <- .create_age_tables(dt[i])

    final_out <- rbind(final_out, tmp_dt)
  }
  return(final_out)
}

.get_bin_dt <- function(dt, nrows_counter, data_by_year, start, end){
  slice_pop <- data.table()
  looping <- TRUE
  while (looping) {
    result <- .get_total_in_bin(dt, nrows_counter)
    bin_total <- result[1]
    bin_start <- result[2]
    bin_end <- result[3]

    if (end <= bin_end) {
      slice_pop_tmp <- .calculate_total_for_new_bin(data_by_year, bin_total, bin_start, bin_end - 1)
      slice_pop_tmp <- slice_pop_tmp[age_start %in% c(start:end)]
      slice_pop <- rbind(slice_pop, slice_pop_tmp)
      looping <- FALSE
    } else {
      slice_pop_tmp <- .calculate_total_for_new_bin(data_by_year, bin_total, bin_start, bin_end - 1)
      slice_pop_tmp <- slice_pop_tmp[age_start %in% c(start:end)]
      slice_pop <- rbind(slice_pop, slice_pop_tmp)
      nrows_counter <- nrows_counter + 1
      start <- bin_end
    }
  }
  return(list(slice_pop, nrows_counter))
}

# check which vaccine bin total we should work with.
.get_total_in_bin <- function(tmp_dt, counter) {
  bin_row <- tmp_dt[NROWS[counter]]
  bin_start <- bin_row[[AGE_START]]
  bin_end <- bin_row[[AGE_END]]
  bin_total <- bin_row[[OBJ_COLUMN]]

  return(c(bin_total, bin_start, bin_end))
}

# Knowing the bin that we will work on, we need to slice those ages in population
.calculate_total_for_new_bin <- function(data_by_year, bin_total, start, end) {
  slice_dt <- data_by_year[age_start %in% c(start:end)]
  slice_total <- sum(slice_dt$population)
  slice_dt[, eval(OBJ_COLUMN) := ..bin_total * (population / ..slice_total)]

  return(slice_dt)
}

