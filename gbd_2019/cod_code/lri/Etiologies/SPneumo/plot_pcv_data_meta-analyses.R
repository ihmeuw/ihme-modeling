### Produce some plots for PCV meta-analyses ###
library(ggplot2)
library(scales)
library(plyr)

data <- read.csv("/home/j/temp/ctroeger/LRI/Files/pcv_fordismod_inputdata_readable.csv")
data <- read.csv("/home/j/temp/ctroeger/LRI/Files/pcv_fordismod_2019.csv")
#data <- subset(data, GBD_round!=2019)
mod <- read.csv("/home/j/temp/ctroeger/LRI/dismod/Usable/reshaped_draws_spneumo.csv")
age_info <- read.csv("/home/j/temp/ctroeger/Misc/age_mapping.csv")

mod <- join(mod, age_info, by="age_group_id")
#data$mean_paf <- data$paf_mean_estimate
data$studytype <- data$study_type

ggplot() +
  geom_line(data=mod, aes(x=age_start, y=mean)) + geom_ribbon(data=mod, aes(x=age_start, ymin=lower, ymax=upper), alpha=0.3) +
  geom_point(data=data, aes(x=(age_end + age_start)/2, y=mean_paf, col=factor(studytype)), size=2) +
  geom_point(data=data, aes(x=(age_end + age_start)/2, y=mean_paf), col="black", pch=1, size=2.1) +
  geom_errorbar(data=data, aes(x=(age_end + age_start)/2, ymax=paf_upper, ymin=paf_lower, col=factor(studytype)), alpha=0.5) +
  geom_errorbarh(data=data, aes(y=mean_paf, x=age_start, xmin=age_start, xmax=age_end, col=factor(studytype)), alpha=0.5) +
  scale_y_continuous("Attributable fraction", limits=c(-1,1), labels=percent) + theme_bw() + scale_color_discrete("", labels=c("Impact","RCT")) +
  scale_x_continuous("Age", limits=c(0,100)) + geom_hline(yintercept=0, lty=2)

###################################
## Make a plot of study on x-axis
du5 <- subset(data, age_end <6)

du5$plot_order <- 1:length(du5$iso3)
du5$xaxis <- factor(du5$first)
du5$xaxis <- reorder(du5$xaxis, du5$plot_order)
ggplot(data=du5, aes(x=plot_order, y=mean_paf, ymin=paf_lower, ymax=paf_upper, col=as.factor(studytype))) + geom_point(position=position_dodge(width=0.8)) +
  geom_errorbar(position=position_dodge(width=0.8)) + scale_x_continuous("", breaks=du5$plot_order, labels=(du5$xaxis)) +
  theme_minimal() + geom_hline(yintercept=0, lty=2) + scale_y_continuous("Attributable fraction", labels=comma) +
  scale_color_discrete("", labels=c("RCT","Impact")) +
  theme(axis.text.x=element_text(angle=45, hjust=1))

#######################################################
## compare data source types ##
#######################################################
data2 <- read.csv("/home/j/temp/ctroeger/LRI/Files/pcv_fordismod_2019.csv")
ggplot(data2, aes(x=study_type, y=ve_invasive/100, col=study_type)) + geom_boxplot() + theme_minimal() +
  scale_y_continuous("VE against vaccine type invasive disease", label=percent) +
  geom_hline(yintercept=0, lty=2) +
  xlab("") + scale_color_discrete("")
ggplot(data2, aes(x=study_type, y=ve_all_invasive/100, col=study_type)) + geom_boxplot() + theme_minimal() +
  scale_y_continuous("VE against all invasive disease", label=percent) +
  geom_hline(yintercept=0, lty=2) +
  xlab("") + scale_color_discrete("")
ggplot(data2, aes(x=study_type, y=ve_pneumonia/100, col=study_type)) + geom_boxplot() + theme_minimal() +
  scale_y_continuous("VE against all pneumonia", label=percent) +
  geom_hline(yintercept=0, lty=2) +
  xlab("") + scale_color_discrete("")
ggplot(data, aes(x=studytype, y=mean_paf, col=studytype)) + geom_boxplot() + theme_minimal() +
  scale_y_continuous("Attributable fraction", limits=c(-0.5,1), labels=percent) +
  geom_hline(yintercept=0, lty=2) +
  xlab("") + scale_color_discrete("")

