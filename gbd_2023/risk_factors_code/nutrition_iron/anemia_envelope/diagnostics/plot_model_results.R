
# source libraries --------------------------------------------------------

library(data.table)
library(ggplot2)
library(gridExtra)

box::use(
  helper_functions/load_model_xwalk_data,
  helper_functions/merge_severity_data
)

# load in cluster args ----------------------------------------------------

if(interactive()){
  param_map <- yaml::read_yaml(
    file.path(getwd(), 'diagnostics/param_maps/id_param_map.yaml')
  )
  adj_type <- 'final'
  gbd_rel_id <- 16
  output_dir <- 'FILEPATH'
}else{
  command_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- command_args[1]
  adj_type <- command_args[2]
  gbd_rel_id <- command_args[3]
  output_dir <- command_args[4]
  
  param_map <- yaml::read_yaml(map_file_name)
}

sex_id <- 1:2

me_list <- param_map[[adj_type]]

# load in model/xwalk data ------------------------------------------------

if(interactive()){
  data_list <- readRDS('FILEPATH/data_list.rds')
}else{
  data_list <- load_model_xwalk_data$load_model_xwalk_data(
    me_list = me_list,
    sex_id = sex_id
  )
}

data_list <- merge_severity_data$merge_severity_data(input_data_list = data_list)

# get demographics to plot ------------------------------------------------

plot_demographics_map <- load_model_xwalk_data$get_demographics_data(
  release_id = gbd_rel_id
) 

# determine plot labels ---------------------------------------------------

adj_titles <- list(
  raw = "Unadjusted",
  adj = "Survey Elevation Adjusted",
  who = "WHO Elevation Adjusted",
  brinda = "BRINDA Elevation Adjusted",
  final = "Final EPIC/COMO Elevation Adjusted Hb"
)

plot_adj_title <- adj_titles[[adj_type]]

plot_titles <- list(
  paste(plot_adj_title, "Mean Hemoglobin"),
  paste(plot_adj_title, "Total Anemia Prevalence"),
  paste(plot_adj_title, "Moderate + Severe Anemia Prevalence"),
  paste(plot_adj_title, "Severe Anemia Prevalence")
)
names(plot_titles) <- names(me_list)

y_axis_titles <- list(
  'Mean Hemoglobin (g/L)',
  'Anemia Prevalence',
  'Anemia Prevalence',
  'Anemia Prevalence'
)
names(y_axis_titles) <- names(me_list)

subtitle_preamble <- list(
  current = if(adj_type == 'final') 'Post Ensemble' else 'Current',
  previous = if(adj_type == 'final') 'ST-GPR' else 'Previous'
)

# plot data ---------------------------------------------------------------

curr_date <- stringr::str_replace_all(
  string = as.character(Sys.Date()),
  pattern = "-",
  replacement = ""
)

if(!interactive()){
  pdf_file_name <- file.path(
    output_dir,
    paste0(adj_type, "_", sex_id, "_", curr_date, '.pdf')
  )
  
  pdf(
    file = pdf_file_name,
    height = 15,
    width = 30
  )
}

MIN_HB <- 25
MAX_HB <- 220

MIN_PREVALENCE <- 0
MAX_PREVALENCE <- 1

for(loc in plot_demographics_map$location_id){
  for(sex in sex_id) {
    hb_df <- copy(data_list$mean_hemoglobin[location_id == loc & sex_id == sex])
    anemia_df <- copy(data_list$anemia[location_id == loc & sex_id == sex])
    hb_df$`Adjustment Type` <- factor(hb_df$`Adjustment Type`, levels = c('Original/Unadjusted', 'Age/Sex Split', 'Measure Adjusted', 'Elevation Adjusted'))
    anemia_df$`Adjustment Type` <- factor(anemia_df$`Adjustment Type`, levels = c('Original/Unadjusted', 'Age/Sex Split', 'Measure Adjusted', 'Elevation Adjusted'))
    
    if(nrow(hb_df) > 0){
      lowest_bound <- if(all(is.na(hb_df$lower_xwalk))) {
        NA_real_
      } else {
        min(c(hb_df$lower_xwalk, hb_df$lower_current, hb_df$lower_previous), na.rm = TRUE)
      }
      lowest_bound <- if(!(is.na(lowest_bound)) && lowest_bound < MIN_HB) MIN_HB else NA_real_
      
      highest_bound <- if(all(is.na(hb_df$upper_xwalk))) {
        NA_real_
      } else {
        max(c(hb_df$upper_current, hb_df$upper_xwalk, hb_df$upper_previous), na.rm = TRUE)
      }
      highest_bound <- if(!(is.na(highest_bound)) && highest_bound > MAX_HB) MAX_HB else NA_real_
      
      p <- ggplot(hb_df) +
        geom_ribbon(aes(x = year_id, ymin = lower_current, ymax = upper_current), fill = 'lightblue', alpha = 0.4) +
        geom_line(aes(x = year_id, y = mean_current, linetype = paste(subtitle_preamble$current, 'ME Results')), colour = 'black') 
      if(!(all(is.na(hb_df$mean_previous)))){
        p <- p +
          geom_line(aes(x = year_id, y = mean_previous, linetype = paste(subtitle_preamble$previous, 'ME Results')), colour = 'darkgrey') +
          scale_linetype_manual(
            name = 'Model',
            breaks = c(paste(subtitle_preamble$current, 'ME Results'), paste(subtitle_preamble$previous, 'ME Results')),
            values = c('solid', 'dashed')
          )
      }
      if(!(all(is.na(hb_df$mean_xwalk)))){
        p <- p +
          geom_point(data = hb_df[from_xwalk == 1], aes(x = og_year_id, y = mean_xwalk, colour = `Adjustment Type`)) +
          geom_linerange(data = hb_df[from_xwalk == 1], aes(x = og_year_id, ymin = lower_xwalk, ymax = upper_xwalk, colour = `Adjustment Type`)) +
          scale_color_manual(
            name = 'Adjustment Type',
            values = c(
              'Original/Unadjusted' = '#a1c181', 
              'Age/Sex Split' = '#fcca46', 
              'Measure Adjusted' = '#9b2915', 
              'Elevation Adjusted' = '#f17cf7'
            )
          ) 
      }
      if(nrow(hb_df[is_outlier == 1]) > 0){
        p <- p +
          geom_text(
            data = hb_df[is_outlier == 1],
            aes(x = og_year_id, y = mean_xwalk, label = ifelse(is_outlier == 1, "X", "")), 
            color = "red", vjust = -0.1, size = 3
          )
      }
      
      p <- p  +
        labs(
          title = paste(plot_adj_title, 'Mean Hemoglobin', unique(hb_df$Sex), unique(hb_df$Location), sep = " - "),
          subtitle = paste0(
            subtitle_preamble$current,
            ' ME ID = ', 
            unique(hb_df$modelable_entity_id), " (", unique(hb_df$model_version_id),") ",
            'and ', subtitle_preamble$previous,' ME ID = ', 
            unique(hb_df$modelable_entity_id.previous_me), " (", unique(hb_df$model_version_id.previous_me), ")"
          ),
          x = 'Year',
          y = 'Mean Hemoglobin (g/L)'
        ) +
        ylim(lowest_bound, highest_bound) +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 30)) +
        facet_wrap('age_group_name', ncol = 5)
      
      if(sex == 1) {
        p_m <- p
      } else {
        p_f <- p
      }
    }
    
    if(nrow(anemia_df) > 0){
      lowest_bound <- if(all(is.na(anemia_df$lower_xwalk))) {
        NA_real_
      } else {
        min(c(anemia_df$lower_xwalk, anemia_df$lower_current, anemia_df$lower_previous), na.rm = TRUE)
      }
      lowest_bound <- if(!(is.na(lowest_bound)) && lowest_bound < MIN_PREVALENCE) MIN_PREVALENCE - 0.1 else NA_real_
      
      highest_bound <- if(all(is.na(anemia_df$upper_xwalk))) {
        NA_real_
      } else {
        max(c(anemia_df$upper_current, anemia_df$upper_xwalk, anemia_df$upper_previous), na.rm = TRUE)
      }
      highest_bound <- if(!(is.na(highest_bound)) && highest_bound > MAX_PREVALENCE) MAX_PREVALENCE + 0.1 else NA_real_
      
      q <- ggplot(anemia_df) +
        geom_line(aes(x = year_id, y = mean_current, linetype = paste(subtitle_preamble$current, 'ME Results'), colour = `Severity`)) +
        geom_ribbon(aes(x = year_id, ymin = lower_current, ymax = upper_current, fill = `Severity`), alpha = 0.1)
      if(!(all(is.na(anemia_df$mean_previous)))){
        q <- q +
          geom_line(aes(x = year_id, y = mean_previous, linetype = paste(subtitle_preamble$previous, 'ME Results'), colour = `Severity`)) +
          scale_linetype_manual(
            name = 'Model',
            breaks = c(paste(subtitle_preamble$current, 'ME Results'), paste(subtitle_preamble$previous, 'ME Results')),
            values = c('solid', 'dashed')
          )
      }
      if(!(all(is.na(anemia_df$mean_xwalk)))){
        q <- q +
          geom_point(data = anemia_df[from_xwalk == 1], aes(x = og_year_id, y = mean_xwalk, shape = `Adjustment Type`, colour = `Severity`), position=position_dodge(0.98), size = 2) +
          geom_linerange(data = anemia_df[from_xwalk == 1], aes(x = og_year_id, ymin = lower_xwalk, ymax = upper_xwalk, colour = `Severity`), position=position_dodge(0.98))
      }
      
      q <- q +
        scale_shape_manual(
          name = 'Adjustment Type',
          values = c(
            'Original/Unadjusted' = 16, 
            'Age/Sex Split' = 15, 
            'Measure Adjusted' = 18, 
            'Elevation Adjusted' = 17
          )
        ) +
        scale_color_manual(
          name = 'Severity',
          breaks = c('Total Anemia', 'Mod + Sev Anemia', 'Severe Anemia'),
          values = c('Total Anemia' = '#20a39e', 'Mod + Sev Anemia' = '#ffba49', 'Severe Anemia' = '#ef5b5b')
        ) + 
        scale_fill_manual(
          name = 'Severity',
          breaks = c('Total Anemia', 'Mod + Sev Anemia', 'Severe Anemia'),
          values = c('Total Anemia' = '#20a39e', 'Mod + Sev Anemia' = '#ffba49', 'Severe Anemia' = '#ef5b5b')
        )
      
      if(nrow(anemia_df[is_outlier == 1]) > 0){
        q <- q +
          geom_text(
            data = anemia_df[is_outlier == 1],
            aes(x = og_year_id, y = mean_xwalk, label = ifelse(is_outlier == 1, "X", "")), 
            color = "red", vjust = -0.5, size = 3
          )
      }
      
      q <- q  +
        labs(
          title = paste(plot_adj_title, 'Anemia Prevalence', unique(anemia_df$Sex), unique(anemia_df$Location), sep = " - "),
          subtitle = if(adj_type == 'final') 'Post Ensemble GBD 2023 vs. ST-GPR 2023' else 'Bested GBD 2023 vs. bested GBD 2021 models',
          x = 'Year',
          y = 'Anemia Prevalence'
        ) +
        ylim(lowest_bound, highest_bound) +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 30)) +
        facet_wrap('age_group_name', ncol = 5)
      
      if(sex == 1) {
        q_m <- q
      } else {
        q_f <- q
      }
    } 
  }
  
  plot_list <- list(p_m, p_f, q_m, q_f)
  
  if(length(plot_list) > 0){
    suppressWarnings(do.call("grid.arrange", c(plot_list, ncol = 2))) 
  }else{
    message(paste("No data for location_id =", loc))
  }
}

if(!interactive()){
  dev.off()
}
