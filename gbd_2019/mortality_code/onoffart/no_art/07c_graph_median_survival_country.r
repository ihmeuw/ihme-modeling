# NAME
# April 2014
# Graph median survival from compartmental model and country-specific from selected curve

#################################################################################
##Set up
#################################################################################
rm(list=ls())
library(ggplot2)
library(RColorBrewer)
library(gridExtra)
library(xtable)
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

model.draws <- read.csv(paste0(data_dir,"FILEPATH"))
matched.draws <- read.csv(paste0(data_dir,"FILEPATH"))

#################################################################################
# Graph of compartmental model median survival distributions
#################################################################################
model.draw.stats <- model.draws[,!names(model.draws) %in% c("draw", "median_survival")]
model.draw.stats <- model.draw.stats[!duplicated(model.draw.stats),]

model.draw.stats$age <- ordered(factor(model.draw.stats$age),
                                levels = c("15_25", "25_35", "35_45", "45_100"),
                                labels = c("15-24", "25-34", "35-44", "45+"))
matched.draws$age <- ordered(factor(matched.draws$age),
                             levels = c("15_25", "25_35", "35_45", "45_100"),
                             labels = c("15-24", "25-34", "35-44", "45+"))

p <- ggplot(data = model.draw.stats, aes(x=age, y=median_surv_mean)) +
  geom_point() +
  geom_errorbar(aes(x=age, ymax=median_surv_upper, ymin=median_surv_lower)) +
  scale_y_continuous(limits = c(0,30), breaks = seq(0, 30, by=5)) +
  xlab("Age group") +
  ylab("Median survival (years) and 95% CI") +
  ggtitle("Median survival distributions from compartmental model")

pdf(file = paste0(graph_dir,"FILEPATH"), height = 8.5, width = 11)
print(p)
dev.off()

#################################################################################
# Graph of country-specific median survival distributions overlaid on median survival distributions
#################################################################################
countries <- unique(matched.draws$iso3)

pdf(file = paste0(graph_dir,"FILEPATH"), height = 8.5, width = 11)
for(i in countries) {
  # i <- "ZAF"
  cdata <- matched.draws[matched.draws$iso3==i,]
  cp <- p + geom_errorbar(data=cdata, aes(x=age, ymax=median_surv_upper, ymin=median_surv_lower), colour='red') +
    geom_point(data=cdata, aes(x=age, y=median_surv_mean), colour='red') +
    ggtitle(paste("Median survival comparison: ", i, sep=""))

  print(cp)
}
dev.off()

#################################################################################
# Histogram of country-specific survival means, by age, with line for pre-matching mean survival
#################################################################################
hist <- ggplot(matched.draws, aes(x=median_surv_mean)) + geom_histogram(binwidth=0.2, colour="black", fill="white") +
  facet_grid(age ~ .) +
  geom_vline(data=model.draw.stats, aes(xintercept=median_surv_mean),
             linetype="dashed", size=1, colour="red") +
  xlab("Average Median Survival (Red = Pre-Matched Average)") +
  ggtitle("Age-specific histograms of post-matching median survival country averages")

pdf(file = paste0(graph_dir,"FILEPATH"), height = 8.5, width = 11)
print(hist)
dev.off()

#################################################################################
# Histogram of country-specific survival means, by age
#################################################################################

ages <- unique(matched.draws$age)

pdf(file = paste0(graph_dir,"FILEPATH"), height = 8.5, width = 11)
for(a in ages) {

  hdata <- matched.draws[matched.draws$age==a,]
  ldata <- model.draw.stats[model.draw.stats$age==a,]

  hist <- ggplot(data=hdata, aes(x=median_surv_mean, group=super_region_name)) +
    geom_histogram(binwidth=0.2, colour="black", fill="white") +
    facet_wrap(~ super_region_name, ncol=2) +
    geom_vline(data=ldata, aes(xintercept=median_surv_mean),linetype="dashed", size=1, colour="red") +
    xlab("Average Median Survival (Red = Pre-Matched Average)") +
    ggtitle(paste("Region-specific histograms of post-matching median survival country averages: Age group ", a, sep=""))

  print(hist)

}
dev.off()


#################################################################################
# Table of numbers
#################################################################################
table.data <- matched.draws[, names(matched.draws) %in% c("iso3", "age", "median_surv_mean", "median_surv_lower", "median_surv_upper")]
table.data <- table.data[with(table.data, order(iso3, age)),]
matched.draws.table <- xtable(table.data)
print(matched.draws.table, tabular.environment="longtable", floating=FALSE, include.rownames=FALSE)
