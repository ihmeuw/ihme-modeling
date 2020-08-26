plot_time_series <- function(plot_data, scale_values,
                             plot_title, plot_subtitle, plot_y_var,
                             facet_var = NULL, facet_scales = NULL,
                             data_shape_var = NULL, data_shape_values = NULL, data_shape_label_name = NULL,
                             data_size = 2, drop_source_uncertainty = "", line_size = 1) {

  fill_values <- c(scale_values[names(scale_values) == "data"], "white")
  names(fill_values) <- c("Included", "Excluded")
  fill_values <- c(fill_values, scale_values)

  # if data isn't already marked as best, make all of them best
  if (!"outlier_type" %in% names(plot_data)) plot_data[, outlier_type := "best"]
  if (!"drop" %in% names(plot_data)) plot_data[, drop := F]
  plot_data[, drop := factor(drop, levels = c(F, T), labels = c("Included", "Excluded"))]
  plot_data[, data_alpha := outlier_type == "not outliered"] # make non best data more transparent

  # make prior lines dashed
  linetype_values <- scale_values
  linetype_values[grepl("prior", names(linetype_values))] <- "dashed"
  linetype_values[!grepl("prior", names(linetype_values))] <- "solid"

  # produce plot if data exists (doesn't some times for new locations for the GBD round)
  p <- NULL
  if (nrow(plot_data) > 0) {
    p <- ggplot(data = plot_data, aes_string(x = "year_id", y = "mean", group = "source", color = "source", alpha = "data_alpha")) +
      geom_ribbon(data=plot_data[!source %in% drop_source_uncertainty], aes_string(ymin = "lower", ymax = "upper", fill = "source"), alpha = 0.15, color = NA, show.legend = F) +
      geom_line(data=plot_data[!grepl("data", source)], aes_string(linetype = "source"), size = line_size, alpha = 0.75) +
      geom_point(data=plot_data[grepl("data", source)], aes_string(shape = data_shape_var, fill = "drop"), size = data_size, colour = scale_values[names(scale_values) == "data"]) +
      scale_alpha_manual(values = c(.35, 1), limits = c(F, T)) +
      scale_colour_manual(values = scale_values, limits = names(scale_values), name = "Source") +
      scale_linetype_manual(values = linetype_values, limits = names(linetype_values), name = "Source") +
      scale_shape_manual(values = data_shape_values, limits = names(data_shape_values), name = data_shape_label_name) +
      scale_fill_manual(values = fill_values, limits = names(fill_values), name = "Data point used in model") +
      theme_bw() +
      guides(alpha = F, fill = F) +
      scale_y_continuous(name = plot_y_var, labels = scales::comma) +
      scale_x_continuous(name = "Year") +
      labs(title = plot_title, subtitle = plot_subtitle)

    # add on optional facet
    if (!is.null(facet_var)) p <- p + facet_wrap(facet_var, scales = facet_scales)
  }
  return(p)
}
