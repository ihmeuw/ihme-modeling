
# source libraries --------------------------------------------------------

library(data.table)
library(ggplot2)

# define file paths -------------------------------------------------------

elevation_file_list <- list.files(
  path = "FILEPATH",
  pattern = "\\.csv$",
  full.names = TRUE
)

# plot results ------------------------------------------------------------

pdf("FILEPATH/mrbrt_diagnostics.pdf")

for(i in elevation_file_list){
  df <- fread(i)
  subtitle_label <- stringr::str_split(i, "/", simplify = TRUE)
  subtitle_label <- subtitle_label[length(subtitle_label)]
  subtitle_label <- stringr::str_remove(subtitle_label, "-\\.csv")
  p <- ggplot(df, aes(x = elevation_number_cat)) + 
    geom_point(aes(y = diff_mean, colour = as.factor(anemia_category), shape = as.factor(raw_prev_category))) +
    geom_line(aes(y = pred_logit_mean, colour = as.factor(anemia_category), shape = as.factor(raw_prev_category))) + 
    theme_classic() +
    labs(title = paste("Elevation Category vs. Change in Anemia Prevalence - MR-BRT Model"),
         subtitle = subtitle_label,
         x="Elevation Category",
         y="Logit Difference of Anemia Prevalence") +
    theme(axis.line = element_line(colour = "black"))
  plot(p)
}

dev.off()