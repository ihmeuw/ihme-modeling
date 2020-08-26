# NAME
# February 2014
# Plot HIV-specific mortality from:
  # Weibull, ALPHA, ZAF
  # UNAIDS Compartmental model
  # Statistical model
  # Our compartmental model

#################################################################################
##Set up
#################################################################################
rm(list=ls())
library(ggplot2)
library(RColorBrewer)
library(gridExtra)
graphics.off()

# Overall plot theme to have white background
theme_set(theme_bw())

if (Sys.info()[1] == "Linux") {
  root <- "ADDRESS"
} else {
  root <- "ADDRESS"
}


data_dir <- paste0(root,"FILEPATH")
graph_dir <- paste0(root,"FILEPATH")


#################################################################################
##Data
#################################################################################
df <- read.csv(paste0(data_dir,"FILEPATH/compare_models.csv"))

#################################################################################
##Example plot
#################################################################################

df$source <- factor(df$source, levels = c("ALPHA East Africa/ZAF Miners",
                                          "UNAIDS Weibull",
                                          "IHME Statistical Model",
                                          "IHME Compartmental Model",
                                          "UNAIDS Compartmental Model"))
sources <- unique(df$source)
colours <- c("black", "red1", "green3", "dodgerblue3", "red3")
names(colours) <- levels(sources)
colScale <- scale_colour_manual(name="Source", values = colours)

p <- ggplot(df, aes(yr_since_sc, surv_mean, colour=source)) +
  xlab("Years since seroconversion") + ylab("HIV relative survival") +
  colScale +
  scale_y_continuous(limits = c(0,1), breaks = seq(0, 1, by=0.1)) +
  scale_x_continuous(limits = c(0, 12), breaks = seq(0, 12, by=1)) +
  theme(plot.title = element_text(size = 12)) +
  guides(colour=guide_legend(override.aes=list(shape=c(16, 16, NA, NA, NA),linetype=c(0, 0, 1, 1, 1)), ncol=2)) +
  theme(legend.position = "bottom") + theme(legend.key = element_blank())


pdf(file = paste0(graph_dir,"FILEPATH"), height = 6, width = 8)

for(a in unique(df$age)) {

 #  a <- "15_25"

  temp <- df[df$age==a,]

  a <- gsub("_", "-", a)
  if (a=="15-25") {
    a <- "15-24"
  } else if(a=="25_35") {
    a
  }
  if(a=="45-100") {
    a <- "45+"
  }


  p.all <- p %+% temp
  p.all <- p.all + geom_line(aes(x=yr_since_sc, y=surv_mean)) +
                   geom_line(aes(x=yr_since_sc, y=surv_lower), linetype=2) +
                   geom_line(aes(x=yr_since_sc, y=surv_upper), linetype=2) +
                   geom_point(aes(x=yr_since_sc, y=surv)) +
                   ggtitle(paste("Comparison of HIV relative survival estimates: Age group", a, sep = " "))

  print(p.all)

}
dev.off()

