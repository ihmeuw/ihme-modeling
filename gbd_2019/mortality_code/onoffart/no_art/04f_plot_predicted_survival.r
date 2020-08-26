# NAME
# February 2014
# Plot predicted draws from logit hazard regression with random effects on study

#################################################################################
##Set up
#################################################################################
rm(list=ls())
library(ggplot2)
library(RColorBrewer)
library(gridExtra)
graphics.off()

if (Sys.info()[1] == "Linux") {
  root <- "ADDRESS"
} else {
  root <- "ADDRESS"
}

# Overall plot theme to have white background
theme_set(theme_bw())

data_dir <- paste0(root,"FILEPATH")
graph_dir <- paste0(root,"FILEPATH")


#################################################################################
##BRING IN DATA AND FORMAT
#################################################################################
surv.preds <- read.csv(paste0(data_dir,"FILEPATH"))
surv.preds <- subset(surv.preds, yr_since_sc <= 12)

# LOGIT HAZARD, RANDOM EFFECT STUDY NO WEIGHTS NO SEX
df.re <- read.csv(paste0(data_dir,"FILEPATH/logit_hazard_draws_re_study.csv"))
df.re <- subset(df.re, select = c(age_cat, yr_since_sc, draw, y))
df.re$surv <- 1-df.re$y
df.re <- df.re[with(df.re, order(draw, age_cat, yr_since_sc)),]

p <- ggplot(surv.preds, aes(yr_since_sc, surv, group=draw)) +
  geom_line() +
  xlab("Years since seroconversion") + ylab("HIV relative survival") +
  scale_y_continuous(limits = c(0,1), breaks = seq(0, 1, by=0.1)) +
  scale_x_continuous(limits = c(0, 12), breaks = seq(0, 12, by=1)) +
  theme(plot.title = element_text(size = 8))
plot(p)


pdf(file = paste0(graph_dir,"FILEPATH/comp_model_pred_survival_draws_re_compare.pdf"), height = 5, width = 5)

for(a in unique(surv.preds$age)) {

  ## COMPARTMENTAL MODEL PREDICTED SURVIVAL CURVES
  temp <- surv.preds[surv.preds$age==a,]
  p.cm <- p %+% temp
  p.cm <- p.cm+ ggtitle(paste(a,"comp model preds", sep="_"))

  ## LOGIT HAZARD, RANDOM EFFECT STUDY NO WEIGHTS
  temp <- df.re[df.re$age_cat==a,]
  p.re <- p %+% temp
  p.re <- p.re+ ggtitle(paste(a,"study random effect regression draws", sep="_"))

  grid.arrange(p.cm, p.re)

}
dev.off()




