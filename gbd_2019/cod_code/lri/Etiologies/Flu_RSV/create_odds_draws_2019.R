###############################################################################
## Create draws for odds ratios used in influenza/RSV PAF calculation #########
###############################################################################

library(matrixStats)
library(plyr)
library(msm)
library(viridis)

age_meta <- read.csv("filepath")
  age_meta <- subset(age_meta, age_pull == 1)
  age_meta <- age_meta[,c("age_group_id","age_start","age_end")]
  age_meta$age <- floor((age_meta$age_start + age_meta$age_end) / 2)

##---------------------------------------------------------------------
## UPDATE! values between the two studies listed below 
## were interpolated such that we have odds ratios for every age-year!
## --------------------------------------------------------------------
## Values come from this paper: Aetiological role of common respiratory viruses in acute lower respiratory infections in children under five years: A systematic review and meta-analysis
    # found here: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4593292/
#
# odds_values <- data.frame(risk = c("eti_lri_rsv","eti_lri_flu"), modelable_entity_id = c(1269,1259),
#                           odds_mean = c(9.79, 5.1), odds_lower = c(4.98, 3.19), odds_upper = c(19.27, 8.14), age="Under 5")
#
# odds_values$odds_std <- with(odds_values, (odds_upper - odds_lower) / 2 / qnorm(0.975))

##-----------------------------------------------------------------------
## Interpolated odds ratios
odds_int <- read.csv("filepath")
    odds_int$odds_mean <- odds_int$mean_new
    odds_int$odds_std <- with(odds_int, (upper_new - lower_new) / 2 / qnorm(0.975))

# Get some of the ages to merge correctly
  age_meta$age <- ifelse(age_meta$age < 7, 2, age_meta$age)
  age_meta$age <- ifelse(age_meta$age > 62, 65, age_meta$age)

odds_values <- odds_int[,c("age","lri","mean_new","lower_new","upper_new","odds_mean","odds_std")]

## Convert to log space for draws
  odds_values$ln_odds_mean <- log(odds_values$odds_mean)
  odds_values$ln_odds_std <-   sapply(1:nrow(odds_values), function(i) {
    ratio_i <- odds_values[i, "odds_mean"]
    ratio_se_i <- odds_values[i, "odds_std"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })

## Sample draws
  for(i in 0:999){
    odds_values[,paste0("rr_",i)] <- exp(rnorm(length(odds_values$age), mean=odds_values$ln_odds_mean, sd=odds_values$ln_odds_std))
  }

odds_values <- join(age_meta, odds_values, by="age")
odds_values$modelable_entity_id <- ifelse(odds_values$lri == "rsv", 1269, 1259)
odds_values$modelable_entity_name <- ifelse(odds_values$lri == "rsv", "eti_lri_rsv", "eti_lri_flu")

## Save
  write.csv(odds_values, "filepath")

##--------------------------------------------------------------
## Plot to show the potential impact of changing ORs
plot_df <- expand.grid(proportion=c(0.01,0.02,0.05,0.1,0.15,0.2,0.3,0.5,1), odds=seq(1,12,0.1))
plot_df$af <- plot_df$proportion * (1 - 1 / plot_df$odds)

ggplot(plot_df, aes(x=odds, y=af, col=factor(proportion))) + geom_line(lwd=1.25) + scale_color_viridis("Proportion", discrete=T) + theme_bw(base_size=15) +
  xlab("Odds ratio") + ylab("Attributable fraction") + geom_vline(xintercept = 5.1) + geom_vline(xintercept = 8.3)
