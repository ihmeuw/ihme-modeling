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

# Overall plot theme to have white background
theme_set(theme_bw())
root <- ifelse(Sys.info()[1]=="Windows", "ADDRESS", "ADDRESS")

setwd(paste0(root,"FILEPATH"))

#################################################################################
##BRING IN DATA AND FORMAT
#################################################################################
# LOGIT HAZARD, RANDOM EFFECT STUDY NO WEIGHTS NO SEX
df.re <- read.csv("FILEPATH/logit_hazard_draws_re_study.csv")
df.re <- subset(df.re, select = c(age_cat, yr_since_sc, draw, y))
df.re$surv <- 1-df.re$y
df.re <- df.re[with(df.re, order(draw, age_cat, yr_since_sc)),]


#################################################################################
##SET UP EXAMPLE PLOT
#################################################################################
p <- ggplot(df.re, aes(yr_since_sc, surv, group=draw)) +
  geom_line() +
  xlab("Years since seroconversion") + ylab("HIV relative survival") +
  scale_y_continuous(limits = c(0,1), breaks = seq(0, 1, by=0.1)) +
  scale_x_continuous(limits = c(0, 12), breaks = seq(0, 12, by=1)) +
  theme(plot.title = element_text(size = 12))

#################################################################################
##RANDOM EFFECT NO SEX
#################################################################################

pdf(file = "FILEPATH/logit_hazard_draws_re_study.pdf", height = 6, width = 8)

for(a in unique(df.re$age_cat)) {

    # # LOGIT HAZARD, RANDOM EFFECT STUDY NO WEIGHTS
    temp <- df.re[df.re$age_cat==a,]
    p.re <- p %+% temp
    a <- gsub("_", "-", a)

    if(a=="15-25") {
      a <- "15-24"
    }
    else if(a=="25-35") {
      a<-"25-34"
    }
    else if(a=="35-45") {
      a<- "35-44"
    }
    else if(a=="45-100") {
      a <- "45+"
    }
    p.re <- p.re+ ggtitle(paste("HIV relative survival curve draws from statistical model: ", a, sep=""))
    print(p.re)

}
dev.off()


#
# #################################################################################
# ## COMPARE FIXED PREDICTION CODE WITH OLD CODE FOR RANDOM EFFECTS MODEL
# #################################################################################
# pdf(file = "FILEPATH/logit_hazard_draws_re_study_compare_to_fixed.pdf", height = 5, width = 5)
#
# for(a in unique(df.re$age_cat)) {
#
#   # # LOGIT HAZARD, RANDOM EFFECT STUDY NO WEIGHTS
#   temp <- df.re[df.re$age_cat==a,]
#   p.re <- p %+% temp
#   p.re <- p.re+ ggtitle(paste(a,"no weights, study random effect, new", sep="_"))
#
#   # # LOGIT HAZARD, RANDOM EFFECT STUDY NO WEIGHTS - OLD MODEL WITH PREDICTION ERROR
#   temp <- df.re.fixed[df.re.fixed$age_cat==a,]
#   p.re.old <- p %+% temp
#   p.re.old <- p.re.old + ggtitle(paste(a,"no weights, study random effect, fixed", sep="_"))
#
#   grid.arrange(p.re, p.re.old)
#
#
#
# }
# dev.off()


