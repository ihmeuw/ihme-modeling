# Plotting functions ------------------------------------------------------

total_pop_plot <- function(plot_data, loc_name, baseline_subtitle, colour_scale_values) {
  p <- ggplot(data = plot_data, aes(x = year_id, y = mean, color = outlier_type, fill = fill_type,
                                    shape = source_group, alpha = data_stage,
                                    group = outlier_type)) +
    geom_line(data=plot_data[data_stage == "estimate"], size = 1, alpha = 0.75) +
    geom_point(data=plot_data[data_stage != "estimate"], size = 4, stroke = 1) +
    scale_alpha_manual(values = c(0.66, 1, 0.66, 0.33), limits = c("estimate", "pes_corrected", "unheaped", "raw")) +
    scale_colour_manual(values = colour_scale_values) +
    scale_fill_manual(values = colour_scale_values, guide = FALSE) +
    scale_shape_manual(values = c(21, 22, 24, 23, 3, 25), limits = c("Custom", "IPUMS", "DYB", "Mitchell", "Backwards ccmpp baseline", "Adjusted split location")) +
    theme_bw() +
    scale_y_continuous(name="Population", labels = scales::comma) +
    scale_x_continuous(name="Year") +
    labs(title = paste(loc_name, ". Total Population by source"), subtitle = baseline_subtitle)
  print(p)
}

age_pattern_plot <- function(plot_data, subtitle, colour_scale_values) {
  p <- ggplot(data = plot_data, aes(x = age_group_years_start, y = mean, color = outlier_type, fill = fill_type,
                                    shape = source_group, alpha = data_stage,
                                    group = interaction(outlier_type, source_group, data_stage))) +
    geom_line(size = 1) +
    geom_point(data=plot_data[data_stage != "estimate"], size = 4, stroke = 1) +
    scale_alpha_manual(values = c(0.66, 1, 0.66, 0.33), limits = c("estimate", "pes_corrected", "unheaped", "raw")) +
    scale_colour_manual(values = colour_scale_values) +
    scale_fill_manual(values = colour_scale_values, guide = FALSE) +
    scale_shape_manual(values = c(21, 22, 24, 23, 3, 25), limits = c("Custom", "IPUMS", "DYB", "Mitchell", "Backwards ccmpp baseline", "Adjusted split location")) +
    theme_bw() +
    scale_y_continuous(name="Population / Age group width", labels = scales::comma) +
    scale_x_continuous(name="Start of age group", breaks = seq(0, 100, 10)) +
    labs(subtitle = subtitle)
  return(p)
}

baseline_age_pattern_plot <- function(plot_data, loc_name, baseline_subtitle, colour_scale_values) {
  years <- unique(plot_data$year_id)
  years <- unique(round(seq.int(min(years), max(years), length.out = 3)))
  year_colours <- brewer.pal(11, "RdYlBu")[c(2, 8, 11)]
  names(year_colours) <- paste0("Backwards CCMPP ", years)

  plot_data <- plot_data[year_id %in% years]
  plot_data[outlier_type == "backwards ccmpp", colour := paste0("Backwards CCMPP ", year_id)]
  plot_data[outlier_type != "backwards ccmpp", colour := as.character(outlier_type)]

  colour_scale_values <- c(year_colours, colour_scale_values)

  p <- ggplot(plot_data, aes(x = age_group_years_start, y = mean)) +
    geom_point(data = plot_data[outlier_type == "backwards ccmpp"], aes(shape = missing_triangle, alpha = missing_triangle, fill = colour), colour = "black", size = 5) +
    geom_line(aes(group = colour, colour = colour), size = 1, alpha = 0.5) +
    scale_colour_manual(values = colour_scale_values, limits = unique(plot_data$colour), name = "1950 estimate source") +
    scale_fill_manual(values = colour_scale_values, limits = unique(plot_data$colour), name = "1950 estimate source", guide=FALSE) +
    scale_alpha_manual(values = c(0.5, 0.8), limits = c(F, T), name = "Age group in\nmissing triangle") +
    scale_shape_manual(values = c(21, 24), limits = c(F, T), name = "Age group in\nmissing triangle") +
    scale_y_continuous(name="Population / Age group width", labels = scales::comma) +
    scale_x_continuous(name="Start of age group", breaks = seq(0, 100, 10)) +
    labs(title = paste0(loc_name, ". Backwards ccmpp diagnostics."), subtitle = baseline_subtitle) +
    theme_bw()
  print(p)
}
