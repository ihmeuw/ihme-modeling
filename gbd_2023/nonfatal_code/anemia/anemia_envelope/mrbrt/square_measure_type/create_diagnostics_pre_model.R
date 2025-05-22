library(ggplot2)
library(data.table)

map_file_name <- file.path(getwd(), "mrbrt/square_measure_type/config_map.csv")
config_map <- fread(map_file_name) |>
  dplyr::arrange(anemia_category, ref)

main_df <- rbindlist(lapply(
  list.files(
    path = "FILEPATH",
    full.names = TRUE
  ),
  fread
), use.names = TRUE, fill = TRUE)

main_df[main_df==""] <- NA
main_df <- main_df[!(is.na(ihme_loc_id))]

age_df <- ihme::get_age_metadata(release_id = 16)
anemia_map <- fread(file.path(getwd(), "mrbrt/square_measure_type/anemia_map.csv"))

plot_diagnostics <- function(input_df, ref_var, alt_var, model_cat){
  df <- copy(input_df)
  
  ref_df <- df[var == ref_var]
  alt_df <- df[var == alt_var]
  
  merge_cols <- c(
    "nid",
    "survey_name",
    "ihme_loc_id",
    "year_start",
    "year_end",
    "survey_module",
    "file_path",
    "age_start",
    "age_end",
    "sex_id",
    "cv_pregnant",
    "sample_size",
    "nstrata",
    "nclust"
  )
  
  if(nrow(ref_df) > 0 && nrow(alt_df) > 0) {
  
    merge_df <- merge.data.table(
      x = ref_df,
      y = alt_df,
      by = merge_cols,
      suffixes = c('.ref', '.alt')
    )
    
    merge_df <- unique(merge_df)
    
    if(grepl("hemog", ref_var)){
      offset_value_min <- merge_df |>
        dplyr::filter(mean.alt > 0) |>
        purrr::chuck('mean.alt') |>
        min() / 2
      offset_value_max <- (1 - merge_df |>
                             dplyr::filter(mean.alt < 1) |>
                             purrr::chuck('mean.alt') |>
                             max()) / 2
      
      merge_df <- merge_df |>
        dplyr::mutate(
          mean.alt = dplyr::case_when(
            mean.alt >= 1 ~ (1 - offset_value_max),
            mean.alt <= 0 ~ offset_value_min,
            .default = mean.alt
          )
        )
      
      merge_df <- merge_df[
        !(is.na(mean.ref)) &
          !(is.na(mean.alt)) &
          !(is.na(standard_error.ref)) &
          !(is.na(standard_error.alt)) &
          mean.ref > 25 & mean.ref < 250 &
          mean.alt > 0 & mean.alt < 1 &
          standard_error.ref > 0.00001 
      ]
      
      #merge_df$mean.ref <- log(merge_df$mean.ref)
      merge_df$logit_mean_prev <- nch::logit(merge_df$mean.alt)
      merge_df$logit_se_prev <- nch::logit_se(
        mean_vec = merge_df$mean.alt,
        se_vec = merge_df$standard_error.alt
      )
    }else{
      merge_df <- merge_df[
        !(is.na(mean.ref)) &
          !(is.na(mean.alt)) &
          !(is.na(standard_error.ref)) &
          !(is.na(standard_error.alt)) &
          mean.alt > 25 & mean.alt < 250 &
          standard_error.ref > 0.0001 
      ]
      
      #merge_df$mean.ref <- nch::logit(merge_df$mean.ref)
      merge_df$logit_mean_prev <- log(merge_df$mean.alt)
      merge_df$logit_se_prev <- nch::log_se(
        mean_vec = merge_df$mean.alt,
        se_vec = merge_df$standard_error.alt
      )
    }
    
    merge_df$age_end[merge_df$age_end == 1 & merge_df$age_start == 1] <- 2
    merge_df$age_group_id <- NA_integer_
    merge_df$temp_age_end <- merge_df$age_end - 0.0001 # just bump down age_end so age_group_id can be assigned
    for(r in seq_len(nrow(age_df))){
      i_vec <- which(merge_df$age_start >= age_df$age_group_years_start[r] &
                       merge_df$temp_age_end < age_df$age_group_years_end[r])
      merge_df$age_group_id[i_vec] <- age_df$age_group_id[r]
    }
    merge_df$temp_age_end <- NULL
    
    merge_df <- merge.data.table(
      x = merge_df,
      y = anemia_map,
      by = c("age_group_id", "sex_id", "cv_pregnant")
    )
    
    merge_df <- merge_df[anemia_category == model_cat]
      plot_title <- if(grepl('hemog', ref_var)){
        "Mean Hemoglobin vs. Logit Anemia Prevalence"
      }else{
        "Anemia Prevalence vs. Log Mean Hemoglobin"
      }
      
    diagnostic_plot1 <- ggplot(merge_df,aes(x = mean.ref, y = logit_mean_prev)) +
      geom_point(aes(colour = standard_error.ref, size = logit_se_prev), alpha = 0.5) +
      scale_colour_gradientn(colours = terrain.colors(10)) +
      geom_smooth(method = "loess", se = FALSE)+
      theme_classic() +
      labs(title = plot_title,
           subtitle = paste(ref_var, alt_var, model_cat, sep = " - "),
           x= if(grepl('hemog', ref_var)) "Mean Hemoglobin (g/L)" else 'Anemia Prevalence',
           y= if(grepl('hemog', ref_var)) "Logit Anemia Prevalence" else "Log Mean Hemoglobin (g/L)") +
      theme(axis.line = element_line(colour = "black"))
    
    plot(diagnostic_plot1)
  } else {
    n_row_ref <- nrow(ref_df)
    n_row_alt <- nrow(alt_df)
    
    message(paste(ref_var, alt_var))
    message('nrow ref = ', n_row_ref)
    message('nrow alt = ', n_row_alt)
  }
}

pdf("FILEPATH/pre_diagnostics.pdf", width = 12, height = 10)

for(r in seq_len(nrow(config_map))) {
  plot_diagnostics(
    input_df = main_df,
    ref_var = config_map$ref[r],
    alt_var = config_map$alt[r],
    model_cat = config_map$anemia_category[r]
  )
}

dev.off()
