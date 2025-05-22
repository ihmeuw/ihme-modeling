library(ggplot2)
library(data.table)

mrbrt_files <- list.files(
  path = "FILEPATH",
  pattern = "*\\.csv$",
  full.names = TRUE
)

good_date <- Sys.Date() - 3
old_files <- c()
for(i in mrbrt_files) {
  file_date <- as.Date(fs::file_info(i)$modification_time)
  if(file_date < good_date) {
    old_files <- append(old_files, i)
  }
}

mrbrt_files <- setdiff(mrbrt_files, old_files)
ordinal_vec <- sapply(mrbrt_files, \(x) {
  stringr::str_split(
    string =  fs::path_file(x),
    pattern = '_',
    simplify = TRUE
  )[1] |>
    as.integer()
}, simplify = TRUE)

mrbrt_file_df <- data.frame(
  ordinal_val = ordinal_vec,
  file_name = mrbrt_files
) |>
  dplyr::arrange(ordinal_val)

plot_diagnostics <- function(df, ref_var, alt_var, anemia_cateogry){
  
  plot_title <- if(grepl('hemog', ref_var)){
    "Mean Hemoglobin vs. Logit Anemia Prevalence"
  }else{
    "Anemia Prevalence vs. Log Mean Hemoglobin"
  }
  
  model_plot1 <- ggplot(df,aes(x = mean.ref)) +
    geom_point(aes(y = logit_mean_prev, colour = standard_error.ref, size = logit_se_prev), alpha = 0.5) +
    scale_colour_gradientn(colours = terrain.colors(10)) +
    geom_line(aes(y = pred_logit_mean), colour = 'blue', linewidth = 1) +
    theme_classic() +
    labs(title = plot_title,
         subtitle = paste(ref_var, alt_var, anemia_cateogry, sep = " - "),
         x= if(grepl('hemog', ref_var)) "Mean Hemoglobin (g/L)" else 'Anemia Prevalence',
         y= if(grepl('hemog', ref_var)) "Logit Anemia Prevalence" else "Log Mean Hemoglobin (g/L)") +
    theme(axis.line = element_line(colour = "black"))
  
  plot(model_plot1)
}

pdf("FILEPATH/post_diagnostics.pdf", width = 12, height = 10)

for(i in mrbrt_file_df$file_name){
  df <- fread(i)
  
  plot_diagnostics(
    df = df,
    ref_var = unique(df$var.ref),
    alt_var = unique(df$var.alt),
    anemia_cateogry = unique(df$anemia_category)
  )
}

dev.off()
