
# source libraries --------------------------------------------------------

library(data.table)

sample_description_map <- fread(file.path(getwd(), "vmnis//src/sample_description_map.csv"))
sample_description_map[sample_description_map == ""] <- NA_character_

# specify group/specificity/group_review ----------------------------------

assign_group_review <- function(input_df){
  df <- copy(input_df)

  k_df <- sample_description_map |>
    dplyr::filter(adult_flag == 0) |>
    dplyr::select(sample_size_description)
  
  #' this following code block does:
  #' 1) updates age_end and age_demographer
  #' 2) sets site_memo = 'national' for all locations that don't have site_memo 
  #' specified
  #' 3)  given that there isn't nesting with in age groups < 20 (or at least no 
  #' biological reason for such thing), we set all age groups < 20 years old to 
  #' have the same population sample_size_description so they are all treated 
  #' the same way
  #' 4) removes the current group, specificity, and group_review columns
  df <- df |>
    dplyr::mutate(
      age_demog_end = dplyr::case_when(
        age_demographer == 0 & 
          age_end > 1 & 
          age_start > 1 &
          age_start %% 1 == 0 &
          age_end %% 1 == 0 
        ~ age_end - 1,
        .default = age_end
      ),
      site_memo = dplyr::case_when(
        is.na(site_memo) | site_memo == '' ~ 'national',
        .default = site_memo
      ),
      sample_size_description = dplyr::case_when(
        sample_size_description %in% k_df$sample_size_description ~ 'Children (all)',
        .default = sample_size_description
      ),
      group = NULL,
      specificity = NULL,
      group_review = NULL
    )
  
  #' get all unique combos of NID, measure type (i.e. mean hemoglobin, total
  #' anemia prevalence, etc.), and IHME location ID
  combo_df <- unique(df[,.(nid, var, ihme_loc_id)])
  
  final_df <- data.table() # place holder for newly set group_review rows
  # for each unique combo in combo_df
  for(r in seq_len(nrow(combo_df))){
    # subset that combo and define group/specificity/group_review
    subset_df <- df |>
      dplyr::filter(
        nid == combo_df$nid[r] & 
          var == combo_df$var[r] &
          ihme_loc_id == combo_df$ihme_loc_id[r]
      ) |>
      define_gsgr() 
    
    final_df <- rbindlist(
      list(final_df, subset_df),
      use.names = TRUE,
      fill = TRUE
    )
  }
  final_df <- final_df |>
    dplyr::mutate(
      specificity = dplyr::case_when(
        !(is.na(group)) ~ sample_size_description,
        .default = NA_character_
      )
    )
  
  return(final_df)
}

define_gsgr <- function(df){
  #' identify all of the unique age groups and sex combinations within each site
  #' memo, urbanicity, and sample size description combos and then merge on the
  #' nested sample_size_description list
  sample_unit_df <- df |>
    dplyr::select(site_memo, urbancity_type, sample_size_description) |>
    unique() |>
    dplyr::mutate(
      contains_sex_equals_both = FALSE,
      contains_male_and_female = FALSE,
      contains_mixed_urban_rural = FALSE,
      contains_both_urban_and_rural = FALSE,
      min_age = -1,
      max_age = 999
    ) |> 
    dplyr::left_join(
      y = sample_description_map |> 
        dplyr::select(sample_size_description, hierarchy_value, adult_flag),
      by = 'sample_size_description'
    ) |>
    dplyr::arrange(adult_flag, site_memo, hierarchy_value)
  
  #' define the least granular group within each combo defined above and assign
  #' a group value
  sample_unit_df <- get_least_granular_groups(
    df = df,
    sample_unit_df = sample_unit_df
  ) |>
    dplyr::mutate(urbancity_type = NULL) |>
    unique() |>
    define_group_value()
  
  # for debugging
  #assign('sample_unit_data', sample_unit_df, envir = .GlobalEnv)
  
  #' merge on the newly identified groups to the vmnis data set, define the
  #' number of splits each row would have in the vmnis data set, and the row
  #' with most splits/that matches most closely to the most granular group
  #' parameters is defined as the reference_row. 
  df <- dplyr::inner_join(
      x = df,
      y = sample_unit_df,
      by = intersect(
        colnames(df),
        colnames(sample_unit_df)
      )
    ) |>
    get_age_sex_split_metrics() |>
    define_reference_point()
  
  # define group_review (tryCatch is being used to see where warnings occur)
  df <- tryCatch(
    define_group_review(df),
    warning = function(w){
      assign('warn_df', df, envir = .GlobalEnv)
      stop(w)
    }
  )
  
  return(df)
}

get_least_granular_groups <- function(sample_unit_df, df){
  for(r in seq_len(nrow(sample_unit_df))){
    temp_df <- df |>
      dplyr::filter(
        site_memo == sample_unit_df$site_memo[r] &
          sample_size_description == sample_unit_df$sample_size_description[r]
      )
    
    if('Both' %in% temp_df$sex){
      sample_unit_df$contains_sex_equals_both[r] <- TRUE
    }
    if(all(c('Male', 'Female') %in% temp_df$sex)){
      sample_unit_df$contains_male_and_female[r] <- TRUE
    }
    if('Mixed/both' %in% temp_df$urbancity_type){
      sample_unit_df$contains_mixed_urban_rural[r] <- TRUE
    }
    if(all(c('Urban', 'Rural') %in% temp_df$urbancity_type)) {
      sample_unit_df$contains_both_urban_and_rural <- TRUE
    }
    
    sample_unit_df$min_age[r] <- min(temp_df$age_start)
    sample_unit_df$max_age[r] <- max(temp_df$age_end)
  }
  return(sample_unit_df)
}

define_group_value <- function(sample_unit_df){
  group_counter <- 0
  sample_unit_df <- get_urbanicity_types(sample_unit_df)
  sample_unit_df$group <- group_counter
  new_df <- data.table()
  if(all(sample_unit_df$site_memo != 'national')){
    site_memo_vec <- unique(sample_unit_df$site_memo)
    for(site in site_memo_vec){ 
      temp_df <- sample_unit_df |>
        dplyr::filter(site_memo == site) |>
        dplyr::mutate(assign_group_review = TRUE)
      
      group_list <- identify_population_group(temp_df, group_counter)
      
      group_counter <- group_list$count
      
      new_df <- rbindlist(
        list(new_df, group_list$dat),
        use.names = TRUE,
        fill = TRUE
      )
    }
  }else{
    subnat_df <- sample_unit_df |>
      dplyr::filter(site_memo != "national")
    
    if(nrow(subnat_df) > 0){
      group_counter <- group_counter + 1
      subnat_df$group <- group_counter
      subnat_df$assign_group_review <- FALSE
    }
    
    nat_list <- sample_unit_df |>
      dplyr::filter(site_memo == "national") |>
      identify_population_group(group_counter)
    
    nat_df <- nat_list$dat |>
      dplyr::mutate(assign_group_review = TRUE)
    group_counter <- nat_df$count
    
    new_df <- rbindlist(
      list(nat_df, subnat_df),
      use.names = TRUE,
      fill = TRUE
    )
  }
  new_df <- update_urbanicity_groupings(new_df)
  return(new_df)
}

get_urbanicity_types <- function(input_dat){
  final_df <- data.table()
  for(r in seq_len(nrow(input_dat))){
    dat <- input_dat |> dplyr::slice(r)
    if(
      isTRUE(unique(dat$contains_mixed_urban_rural)) && 
      isTRUE(unique(dat$contains_both_urban_and_rural))
    ) {
      dat <- rbind(
        dat |> dplyr::mutate(urbancity_type = 'Mixed/both', keep_urbanicity = 1),
        dat |> dplyr::mutate(urbancity_type = 'Urban', keep_urbanicity = 0),
        dat |> dplyr::mutate(urbancity_type = 'Rural', keep_urbanicity = 0)
      )
    } else if (
      isFALSE(unique(dat$contains_mixed_urban_rural)) && 
      isTRUE(unique(dat$contains_both_urban_and_rural))
    ) {
      dat <- rbind(
        dat |> dplyr::mutate(urbancity_type = 'Mixed/both', keep_urbanicity = 0),
        dat |> dplyr::mutate(urbancity_type = 'Urban', keep_urbanicity = 1),
        dat |> dplyr::mutate(urbancity_type = 'Rural', keep_urbanicity = 1)
      )
    } else if (
      isTRUE(unique(dat$contains_mixed_urban_rural)) && 
      isFALSE(unique(dat$contains_both_urban_and_rural))
    ) {
      dat <- rbind(
        dat |> dplyr::mutate(urbancity_type = 'Mixed/both', keep_urbanicity = 1),
        dat |> dplyr::mutate(urbancity_type = 'Urban', keep_urbanicity = 0),
        dat |> dplyr::mutate(urbancity_type = 'Rural', keep_urbanicity = 0)
      )
    } else {
      dat <- rbind(
        dat |> dplyr::mutate(urbancity_type = 'Mixed/both', keep_urbanicity = 1),
        dat |> dplyr::mutate(urbancity_type = 'Urban', keep_urbanicity = 1),
        dat |> dplyr::mutate(urbancity_type = 'Rural', keep_urbanicity = 1)
      )
    }
    final_df <- rbindlist(
      list(final_df, dat),
      use.names = TRUE,
      fill = TRUE
    )
  }
  return(final_df)
}

update_urbanicity_groupings <- function(dat){
  max_group <- max(dat$group)
  group_vec <- unique(dat$group)
  for(g in group_vec){
    g_dat <- dat |> dplyr::filter(group == g)
    if(
      isFALSE(unique(g_dat$contains_mixed_urban_rural)) && 
      isTRUE(unique(g_dat$contains_both_urban_and_rural))
    ) {
      dat <- dat |>
        dplyr::mutate(
          group = dplyr::case_when(
            group == g & urbancity_type == 'Rural' ~ max_group + 1,
            .default = group
          )
        )
    }
    max_group <- max(dat$group)
  }
  return(dat)
}

identify_population_group <- function(temp_df, group_counter){
  og_df <- temp_df
  og_df$group <- NULL
  temp_df$urbancity_type <- NULL
  temp_df$keep_urbanicity <- NULL
  temp_df <- unique(temp_df)
  for(r in seq_len(nrow(temp_df))){
    parents <- get_parents(temp_df$sample_size_description[r])
    if(length(parents) > 0){
      found_parent <- FALSE
      for(p in parents){
        if(p %in% temp_df$sample_size_description){
          parent_index <- match(p, temp_df$sample_size_description)
          temp_df$group[r] <- temp_df$group[parent_index]
          found_parent <- TRUE
          break
        }
      }
      if(!found_parent){
        group_counter <- group_counter + 1
        temp_df$group[r] <- group_counter 
      }
    }else{
      group_counter <- group_counter + 1
      temp_df$group[r] <- group_counter 
    }
  }
  temp_df <- merge.data.frame(
    x = temp_df,
    y = og_df,
    by = intersect(colnames(temp_df), colnames(og_df))
  )
  return(list(
    dat = temp_df,
    count = group_counter
  ))
}

get_parents <- function(val){
  level_cols <- paste('level', 1:4, sep = "_")
  keep_row <- unlist(as.vector(
    sample_description_map[
      sample_size_description == val, 
      level_cols, 
      with = FALSE
    ]
  ))
  keep_row <- keep_row[!(is.na(keep_row))]
  return(keep_row)
}

get_age_sex_split_metrics <- function(df){
  df$num_sex_groups <- ifelse(
    df$sex == 'Both', 2, 1
  )
  age_group_id_vec <- agesexsplit::get_age_group_ids(
    age_start = df$age_start,
    age_end = df$age_demog_end,
    release_id = 16
  )
  df$num_age_groups <- sapply(age_group_id_vec, \(x){
    length(x)
  }, simplify = TRUE)
  
  df$total_groups <- df$num_sex_groups * df$num_age_groups
  
  df$age_diff <- df$age_end - df$age_start
  
  df <- df |> 
    dplyr::group_by(group, hierarchy_value) |>
    dplyr::mutate(
      reference_point = dplyr::case_when(
        total_groups == max(total_groups) &
          age_diff == max(age_diff) ~ 1, 
        .default = 0
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::group_by(group) |>
    dplyr::mutate(
      reference_point = dplyr::case_when(
        hierarchy_value != min(hierarchy_value) ~ 0,
        .default = reference_point
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::arrange(group, total_groups)
  
  return(df)
}

define_reference_point <- function(dat){ # ADD something about sample_size
  dat$temp_row_val <- seq_len(nrow(dat))
  dat$reference_point <- 0
  for(g in unique(dat$group)){
    temp_df <- dat |>
      dplyr::filter(group == g & keep_urbanicity == 1) |>
      dplyr::filter(hierarchy_value == min(hierarchy_value))
    reference_rows <- c()
    if (isTRUE(unique(temp_df$contains_sex_equals_both))) {
      s_df <- temp_df |>
        dplyr::filter(
          sex == 'Both' & age_start == min_age & age_end == max_age
        )
      if(nrow(s_df) == 0){
        s_df <- temp_df |>
          dplyr::filter(
            sex == 'Both' & age_start >= min_age & age_end <= max_age
          )
      }
      if(
        nrow(s_df) > 1 &&
        length(unique(s_df$age_start)) == 1 &&
        length(unique(s_df$age_end)) == 1 &&
        all(s_df$age_start == unique(s_df$age_start)) && 
        all(s_df$age_end == unique(s_df$age_end))
      ) {
        s_df <- s_df |>
          dplyr::slice(which.max(s_df$sample_size))
      }
      reference_rows <- s_df$temp_row_val
    } else if(isTRUE(unique(temp_df$contains_male_and_female))) {
      sex_vec <- c('Male', 'Female')
      for(s in sex_vec){
        s_df <- temp_df |>
          dplyr::filter(sex == s & age_start == min_age & age_end == max_age)
        if(nrow(s_df) == 0){
          s_df <- temp_df |>
            dplyr::filter(
              sex == s & age_start >= min_age & age_end <= max_age
            )
        }
        if(
          nrow(s_df) > 1 &&
          length(unique(s_df$age_start)) == 1 &&
          length(unique(s_df$age_end)) == 1 &&
          all(s_df$age_start == unique(s_df$age_start)) && 
          all(s_df$age_end == unique(s_df$age_end))
        ) {
          s_df <- s_df |>
            dplyr::slice(which.max(s_df$sample_size))
        }
        reference_rows <- append(reference_rows, s_df$temp_row_val)
      }
    } else {
      s_df <- temp_df |>
        dplyr::filter(age_start == min_age & age_end == max_age)
      if(nrow(s_df) == 0) {
        s_df <- temp_df |>
          dplyr::filter(age_start >= min_age & age_end <= max_age)
      }
      if(
        nrow(s_df) > 1 &&
        length(unique(s_df$age_start)) == 1 &&
        length(unique(s_df$age_end)) == 1 &&
        all(s_df$age_start == unique(s_df$age_start)) && 
        all(s_df$age_end == unique(s_df$age_end))
      ) {
        s_df <- s_df |>
          dplyr::slice(which.max(s_df$sample_size))
      }
      reference_rows <- append(reference_rows, s_df$temp_row_val)
    }
    dat <- dat |>
      dplyr::mutate(
        reference_point = dplyr::case_when(
          temp_row_val %in% reference_rows & group == g ~ 1,
          .default = reference_point
        )
      )
  }
  dat$temp_row_val <- NULL
  return(dat)
}

define_group_review <- function(dat){
  group_id_vec <- unique(dat$group)
  final_df <- data.table()
  for(g in group_id_vec){
    temp_df <- dat |> 
      dplyr::filter(group == g) |>
      dplyr::mutate(
        group_review = 0,
        group_row_id = dplyr::row_number()
      )
    non_gr_urbanicity <- temp_df |> dplyr::filter(keep_urbanicity == 0)
    temp_df <- temp_df |> dplyr::filter(keep_urbanicity == 1)
    assign('to_gr_df', temp_df, envir = .GlobalEnv)
    hierarchy_vec <- unique(sort(temp_df$hierarchy_value, decreasing = TRUE))
    reference_row <- temp_df |>
      dplyr::filter(reference_point == 1)
    assigned_group_review <- FALSE
    if (nrow(temp_df) == 1) {
      print(g)
      print('cond 0')
      temp_df$group <- NA_integer_
      temp_df$group_review <- NA_integer_
    } else if(all(temp_df$assign_group_review)){
      if(nrow(reference_row) > 0){
        for(h in hierarchy_vec){
          print(paste(g, h))
          h_df <- temp_df |>
            dplyr::filter(hierarchy_value == h) |>
            dplyr::arrange(age_diff, age_start)
          if (
            isTRUE(unique(reference_row$contains_sex_equals_both)) && # contains sex = BOTH
              isTRUE(unique(reference_row$contains_male_and_female)) # and has both Males and Females present
          ){
            male_df <- h_df |>
              dplyr::filter(sex == 'Male' & reference_point != 1) |>
              get_most_granular_ages(reference_row)
            female_df <- h_df |>
              dplyr::filter(sex == 'Female' & reference_point != 1)|>
              get_most_granular_ages(reference_row)
            both_df <- h_df |>
              dplyr::filter(sex == 'Both' & reference_point != 1)|>
              get_most_granular_ages(reference_row)
            
            if(
              nrow(male_df) > 0 && nrow(female_df) > 0 &&
              all(male_df$age_start >= min(reference_row$age_start)) && 
              all(male_df$age_end <= max(reference_row$age_end)) && 
              sum(male_df$age_diff) == sum(reference_row$age_diff) &&
              all(female_df$age_start >= min(reference_row$age_start)) && 
              all(female_df$age_end <= max(reference_row$age_end)) && 
              sum(female_df$age_diff) == sum(reference_row$age_diff) &&
                dplyr::near(
                  sum(male_df$sample_size) + sum(female_df$sample_size),
                  sum(reference_row$sample_size),
                  tol = ceiling(sum(reference_row$sample_size) * 0.02)
                )
            ) {
              print('cond 1')
              assigned_group_review <- TRUE
              good_rows <- c(male_df$group_row_id, female_df$group_row_id)
              temp_df <- temp_df |>
                dplyr::mutate(
                  group_review = dplyr::case_when(
                    group_row_id %in% good_rows ~ 1,
                    .default = group_review
                  )
                )
            } else if(
              nrow(both_df) > 0 &&
              all(both_df$age_start >= min(reference_row$age_start)) && 
              all(both_df$age_end <= max(reference_row$age_end)) && 
                dplyr::near(
                  sum(both_df$sample_size),
                  sum(reference_row$sample_size),
                  tol = ceiling(sum(reference_row$sample_size) * 0.02)
                )
            ) {
              print('cond 2')
              assigned_group_review <- TRUE
              good_rows <- c(both_df$group_row_id)
              temp_df <- temp_df |>
                dplyr::mutate(
                  group_review = dplyr::case_when(
                    group_row_id %in% good_rows ~ 1,
                    .default = group_review
                  )
                )
            }
          } else if (
            isTRUE(unique(reference_row$contains_sex_equals_both)) && # contains sex = Both
              isFALSE(unique(reference_row$contains_male_and_female))  # and does not have Males and Females present
          ){
            both_df <- h_df |>
              dplyr::filter(sex == 'Both' & reference_point != 1) |>
              get_most_granular_ages(reference_row)
            if (
              nrow(both_df) > 0 &&
              all(both_df$age_start >= min(reference_row$age_start)) && 
              all(both_df$age_end <= max(reference_row$age_end)) && 
              dplyr::near(
                sum(both_df$sample_size),
                sum(reference_row$sample_size),
                tol = ceiling(sum(reference_row$sample_size) * 0.02)
              )
            ) {
              print('cond 3')
              assigned_group_review <- TRUE
              good_rows <- c(both_df$group_row_id)
              temp_df <- temp_df |>
                dplyr::mutate(
                  group_review = dplyr::case_when(
                    group_row_id %in% good_rows ~ 1,
                    .default = group_review
                  )
                )
            }
          } else if (
            isFALSE(reference_row$contains_sex_equals_both) &&
              isTRUE(reference_row$contains_male_and_female)  
          ){
            for(s in c('Male', 'Female')){
              s_reference <- reference_row |> dplyr::filter(sex == s)
              s_df <- h_df |>
                dplyr::filter(reference_point != 1 & sex == s) |>
                get_most_granular_ages(s_reference)
              if(
                all(s_df$age_start >= min(s_reference$age_start)) && 
                all(s_df$age_end <= max(s_reference$age_end)) && 
                dplyr::near(
                  sum(s_df$sample_size),
                  sum(s_reference$sample_size),
                  tol = ceiling(sum(s_reference$sample_size) * 0.02)
                )
              ) {
                print('cond 4')
                assigned_group_review <- TRUE
                good_rows <- c(s_df$group_row_id)
                temp_df <- temp_df |>
                  dplyr::mutate(
                    group_review = dplyr::case_when(
                      group_row_id %in% good_rows ~ 1,
                      .default = group_review
                    )
                  )
              }
            }
          } else { # only one sex present
            h_df <- h_df |>
              dplyr::filter(reference_point != 1) |>
              get_most_granular_ages(reference_row)
            if(
              all(h_df$age_start >= min(reference_row$age_start)) && 
              all(h_df$age_end <= max(reference_row$age_end)) && 
              dplyr::near(
                sum(h_df$sample_size),
                sum(reference_row$sample_size),
                tol = ceiling(sum(reference_row$sample_size) * 0.02)
              )
            ) {
              print('cond 5')
              assigned_group_review <- TRUE
              good_rows <- c(h_df$group_row_id)
              temp_df <- temp_df |>
                dplyr::mutate(
                  group_review = dplyr::case_when(
                    group_row_id %in% good_rows ~ 1,
                    .default = group_review
                  )
                )
            }
          }
          if(assigned_group_review) break
        }
        if(!assigned_group_review) {
          print('cond 6')
          temp_df <- temp_df |>
            dplyr::mutate(
              group_review = dplyr::case_when(
                reference_point == 1 ~ 1,
                .default = 0
              )
            )
        }
      } else {
        print('cond 7')
        sex_vec <- if(isTRUE(unique(temp_df$contains_sex_equals_both))) {
          'Both'
        } else if (isTRUE(unique(temp_df$contains_male_and_female))) {
          c('Male', 'Female')
        } else {
          unique(temp_df$sex)
        }
        print(min(temp_df$min_age))
        print(max(temp_df$max_age))
        print(sex_vec)
        print(urbanicity_types)
        i_vec <- which(
          temp_df$age_start >= min(temp_df$min_age) &
            temp_df$age_end <= max(temp_df$max_age) & 
            temp_df$sex %in% sex_vec 
        )
        group_id_values <- temp_df$group_row_id[i_vec]
        print(g)
        print(group_id_values)
        temp_df <- temp_df |>
          dplyr::mutate(
            group_review = dplyr::case_when(
              group_row_id %in% group_id_values ~ 1,
              .default = group_review
            )
          )
      }
    }
    if(all(temp_df$sample_size_description == 'Pregnant women')) {
      temp_df <- process_pregnancy_groups(temp_df)
    }
    if(length(unique(temp_df$measure_method)) > 1) {
      temp_df <- process_measure_method(temp_df)
    }
    final_df <- rbindlist(
      list(final_df, temp_df, non_gr_urbanicity),
      use.names = TRUE,
      fill = TRUE
    )
  }
  return(final_df)
}

process_pregnancy_groups <- function(dat) {
  if(all(0:1 %in% dat$cv_trimester)){
    dat <- dat |> 
      dplyr::mutate(
        group_review = dplyr::case_when(
          cv_trimester == 1 ~ 0,
          .default = group_review
        )
      )
  }
  return(dat)
}

process_measure_method <- function(dat) {
  measure_method_vec <- unique(dat$measure_method)
  if('Venipuncture' %in% measure_method_vec) {
    dat <- dat |> 
      dplyr::mutate(
        group_review = dplyr::case_when(
          measure_method != 'Venipuncture' ~ 0,
          .default = group_review
        )
      )
  } else if ('Capillary puncture' %in% measure_method_vec) {
    dat <- dat |> 
      dplyr::mutate(
        group_review = dplyr::case_when(
          measure_method != 'Capillary puncture' ~ 0,
          .default = group_review
        )
      )
  }
  return(dat)
}

get_most_granular_ages <- function(dat, reference_rows){
  final_df <- data.table()
  dat <- dat |>
    dplyr::arrange(age_start, age_end, age_diff)
  for(r in seq_len(nrow(dat))){
    if(nrow(final_df) == 0) {
      final_df <- dat |> dplyr::slice(r)
    }else{
      if(
        all(dat$age_start[r] > final_df$age_start) &&
        all(dat$age_end[r] > final_df$age_end)
      ) {
        final_df <- rbind(final_df, dat |> dplyr::slice(r))
      }
    }
  }
  return(final_df)
}
