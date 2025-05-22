library(ggplot2)
library(gridExtra)

# get cluster args --------------------------------------------------------

if(interactive()) {
  map_file_path <- file.path(getwd(), 'diagnostics/param_maps/final_param_map.yaml')
  adj_type <- 'who'
  output_dir <- 'FILEPATH'
} else {
  command_args <- commandArgs(trailingOnly = TRUE)
  map_file_path <- command_args[1]
  adj_type <- command_args[2]
  output_dir <- command_args[3]
}

# load in param maps ------------------------------------------------------

id_map <- yaml::read_yaml(map_file_path)[[adj_type]]

# load in ME data ---------------------------------------------------------

df_list <- lapply(names(id_map), \(x) {
  data.table::rbindlist(list(
    ihme::get_model_results(
      gbd_team = id_map[[x]][['id_type']],
      gbd_id = id_map[[x]][['modelable_entity_id']],
      model_version_id = id_map[[x]][['current_model_version_id']],
      location_set_id = 35,
      release_id = id_map[[x]][['release_id']]
    ) |>
      dplyr::mutate(Model = 'Current ME Results'),
    ihme::get_model_results(
      gbd_team = id_map[[x]][['id_type']],
      gbd_id = id_map[[x]][['modelable_entity_id']],
      model_version_id = id_map[[x]][['previous_model_version_id']],
      location_set_id = 35,
      release_id = id_map[[x]][['release_id']]
    ) |>
      dplyr::mutate(Model = 'Previous ME Results')
  ), use.names = TRUE, fill = TRUE)
})

names(df_list) <- names(id_map)


# format df list ----------------------------------------------------------

age_df <- ihme::get_age_metadata(release_id = 16) |>
  dplyr::arrange(age_group_years_start) |>
  dplyr::mutate(age_group_name = factor(age_group_name, levels = age_group_name)) |>
  dplyr::select(age_group_id, age_group_name)

for(i in names(df_list)) {
  df_list[[i]] <- df_list[[i]] |>
    data.table::merge.data.table(
      y = age_df,
      by = 'age_group_id'
    ) |>
    dplyr::mutate(
      Sex = dplyr::case_when(
        sex_id == 1 ~ 'Male',
        .default = 'Female'
      )
    )
}

# plot results ------------------------------------------------------------

if(!interactive()) {
  file_date <- stringr::str_remove_all(Sys.Date(), pattern = '-')
  out_file_path <- file.path(
    output_dir,
    paste(adj_type, file_date, 'model_results.pdf', sep = '_')
  )
  
  pdf(
    file = out_file_path,
    width = 30,
    height = 35
  )
}

loc_vec <- unique(df_list$mean_hemoglobin$location_id)
num_facet_cols <- 5

for(loc in loc_vec) {
  
  ### MALE MEAN HB ###
  male_mean_hb_plot <- ggplot(
    data = df_list$mean_hemoglobin[sex_id == 1 & location_id == loc],
    aes(x = year_id)
  ) +
    geom_ribbon(
      data = df_list$mean_hemoglobin[sex_id == 1 & location_id == loc & Model == 'Current ME Results'],
      aes(ymin = lower, ymax = upper), fill = 'lightblue', alpha = 0.1
    ) +
    geom_line(aes(y = mean, linetype = Model), colour = 'black') +
    scale_linetype_manual(
      name = 'Model',
      breaks = c('Current ME Results', 'Previous ME Results'),
      values = c('Current ME Results' = 'solid', 'Previous ME Results' = 'dashed')
    ) +
    labs(
      title = paste('Mean Hemoglobin - Males -', nch::name_for(type = 'location', id = loc)),
      x = 'Year',
      y = 'Hemoglobin (g/L)'
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30)) +
    facet_wrap('age_group_name', ncol = num_facet_cols)

  ### MALE MILD ANEMIA PREVALENCE ###
  male_mild_anemia_plot <- ggplot(
    data = df_list$mild_anemia[sex_id == 1 & location_id == loc],
    aes(x = year_id)
  ) +
    geom_line(aes(y = mean, linetype = Model), colour = '#20a39e') +
    geom_ribbon(
      data = df_list$mild_anemia[sex_id == 1 & location_id == loc & Model == 'Current ME Results'],
      aes(ymin = lower, ymax = upper), fill = '#20a39e', alpha = 0.1
    ) +
    scale_linetype_manual(
      name = 'Model',
      breaks = c('Current ME Results', 'Previous ME Results'),
      values = c('Current ME Results' = 'solid', 'Previous ME Results' = 'dashed')
    ) +
    labs(
      title = paste('Mild Anemia Prevalence - Males -', nch::name_for(type = 'location', id = loc)),
      x = 'Year',
      y = 'Anemia Prevalence'
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30)) +
    facet_wrap('age_group_name', ncol = num_facet_cols)
  
  ### MALE MODERATE ANEMIA PREVALENCE ###
  male_moderate_anemia_plot <- ggplot(
    data = df_list$moderate_anemia[sex_id == 1 & location_id == loc],
    aes(x = year_id)
  ) +
    geom_line(aes(y = mean, linetype = Model), colour = '#ffba49') +
    geom_ribbon(
      data = df_list$moderate_anemia[sex_id == 1 & location_id == loc & Model == 'Current ME Results'],
      aes(ymin = lower, ymax = upper), fill = '#ffba49', alpha = 0.1
    ) +
    scale_linetype_manual(
      name = 'Model',
      breaks = c('Current ME Results', 'Previous ME Results'),
      values = c('Current ME Results' = 'solid', 'Previous ME Results' = 'dashed')
    ) +
    labs(
      title = paste('Moderate Anemia Prevalence - Males -', nch::name_for(type = 'location', id = loc)),
      x = 'Year',
      y = 'Anemia Prevalence'
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30)) +
    facet_wrap('age_group_name', ncol = num_facet_cols)
  
  ### MALE SEVERE ANEMIA PREVALENCE ###
  male_severe_anemia_plot <- ggplot(
    data = df_list$severe_anemia[sex_id == 1 & location_id == loc],
    aes(x = year_id)
  ) +
    geom_line(aes(y = mean, linetype = Model), colour = '#ef5b5b') +
    geom_ribbon(
      data = df_list$severe_anemia[sex_id == 1 & location_id == loc & Model == 'Current ME Results'],
      aes(ymin = lower, ymax = upper), fill = '#ef5b5b', alpha = 0.1
    ) +
    scale_linetype_manual(
      name = 'Model',
      breaks = c('Current ME Results', 'Previous ME Results'),
      values = c('Current ME Results' = 'solid', 'Previous ME Results' = 'dashed')
    ) +
    labs(
      title = paste('Severe Anemia Prevalence - Males -', nch::name_for(type = 'location', id = loc)),
      x = 'Year',
      y = 'Anemia Prevalence'
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30)) +
    facet_wrap('age_group_name', ncol = num_facet_cols)
  
  ### FEAMLE MEAN HB ###
  female_mean_hb_plot <- ggplot(
    data = df_list$mean_hemoglobin[sex_id == 2 & location_id == loc],
    aes(x = year_id)
  ) +
    geom_ribbon(
      data = df_list$mean_hemoglobin[sex_id == 2 & location_id == loc & Model == 'Current ME Results'],
      aes(ymin = lower, ymax = upper), fill = 'lightblue', alpha = 0.1
    ) +
    geom_line(aes(y = mean, linetype = Model), colour = 'black') +
    scale_linetype_manual(
      name = 'Model',
      breaks = c('Current ME Results', 'Previous ME Results'),
      values = c('Current ME Results' = 'solid', 'Previous ME Results' = 'dashed')
    ) +
    labs(
      title = paste('Mean Hemoglobin - Females -', nch::name_for(type = 'location', id = loc)),
      x = 'Year',
      y = 'Hemoglobin (g/L)'
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30)) +
    facet_wrap('age_group_name', ncol = num_facet_cols)
  
  ### FEMALE MILD ANEMIA PREVALENCE ###
  female_mild_anemia_plot <- ggplot(
    data = df_list$mild_anemia[sex_id == 2 & location_id == loc],
    aes(x = year_id)
  ) +
    geom_line(aes(y = mean, linetype = Model), colour = '#20a39e') +
    geom_ribbon(
      data = df_list$mild_anemia[sex_id == 2 & location_id == loc & Model == 'Current ME Results'],
      aes(ymin = lower, ymax = upper), fill = '#20a39e', alpha = 0.1
    ) +
    scale_linetype_manual(
      name = 'Model',
      breaks = c('Current ME Results', 'Previous ME Results'),
      values = c('Current ME Results' = 'solid', 'Previous ME Results' = 'dashed')
    ) +
    labs(
      title = paste('Mild Anemia Prevalence - Females -', nch::name_for(type = 'location', id = loc)),
      x = 'Year',
      y = 'Anemia Prevalence'
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30)) +
    facet_wrap('age_group_name', ncol = num_facet_cols)
  
  ### FEMALE MODERATE ANEMIA PREVALENCE ###
  female_moderate_anemia_plot <- ggplot(
    data = df_list$moderate_anemia[sex_id == 2 & location_id == loc],
    aes(x = year_id)
  ) +
    geom_line(aes(y = mean, linetype = Model), colour = '#ffba49') +
    geom_ribbon(
      data = df_list$moderate_anemia[sex_id == 2 & location_id == loc & Model == 'Current ME Results'],
      aes(ymin = lower, ymax = upper), fill = '#ffba49', alpha = 0.1
    ) +
    scale_linetype_manual(
      name = 'Model',
      breaks = c('Current ME Results', 'Previous ME Results'),
      values = c('Current ME Results' = 'solid', 'Previous ME Results' = 'dashed')
    ) +
    labs(
      title = paste('Moderate Anemia Prevalence - Females -', nch::name_for(type = 'location', id = loc)),
      x = 'Year',
      y = 'Anemia Prevalence'
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30)) +
    facet_wrap('age_group_name', ncol = num_facet_cols)
  
  ### FEMALE SEVERE ANEMIA PREVALENCE ###
  female_severe_anemia_plot <- ggplot(
    data = df_list$severe_anemia[sex_id == 2 & location_id == loc],
    aes(x = year_id)
  ) +
    geom_line(aes(y = mean, linetype = Model), colour = '#ef5b5b') +
    geom_ribbon(
      data = df_list$severe_anemia[sex_id == 2 & location_id == loc & Model == 'Current ME Results'],
      aes(ymin = lower, ymax = upper), fill = '#ef5b5b', alpha = 0.1
    ) +
    scale_linetype_manual(
      name = 'Model',
      breaks = c('Current ME Results', 'Previous ME Results'),
      values = c('Current ME Results' = 'solid', 'Previous ME Results' = 'dashed')
    ) +
    labs(
      title = paste('Severe Anemia Prevalence - Females -', nch::name_for(type = 'location', id = loc)),
      x = 'Year',
      y = 'Anemia Prevalence'
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30)) +
    facet_wrap('age_group_name', ncol = num_facet_cols)
  
  plot_list <- list(
    male_mean_hb_plot,
    female_mean_hb_plot,
    male_mild_anemia_plot,
    female_mild_anemia_plot,
    male_moderate_anemia_plot,
    female_moderate_anemia_plot,
    male_severe_anemia_plot,
    female_severe_anemia_plot
  )
  
  if(length(plot_list) > 0){
    suppressWarnings(do.call("grid.arrange", c(plot_list, ncol = 2))) 
  }else{
    message(paste("No data for location_id =", loc))
  }
    
}

if(!interactive()) {
  dev.off()
}
