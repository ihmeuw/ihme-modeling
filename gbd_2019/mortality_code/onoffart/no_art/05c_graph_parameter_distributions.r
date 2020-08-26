# NAME
# February 2014
# Box plot of parameters

#################################################################################
##Set up
#################################################################################
rm(list=ls())
library(ggplot2)
library(RColorBrewer)
library(gridExtra)
library(plyr)

graphics.off()

# Overall plot theme to have white background
theme_set(theme_bw())

if (Sys.info()[1] == "Linux") {
  root <- "ADDRESS"
} else {
  root <- "ADDRESS"
}


comp_dir <- paste0(root,"FILEPATH")
graph_dir <- paste0(root,"FILEPATH")

#################################################################################
##BRING IN DATA AND FORMAT
#################################################################################
pars <- read.csv(paste0(comp_dir,"FILEPATH/compare_pars_ihme_unaids.csv"))
pars$cd4 <- ordered(factor(pars$cd4), levels = c(">500", "350-500", "250-349", "200-249", "100-199", "50-99","<50"))

#################################################################################
##95% CONFIDENCE INTERVAL
#################################################################################
p <- ggplot(pars, aes(cd4, log_par_mean)) +
  geom_point(aes(colour=source)) +
  scale_colour_manual(name="Source", values=c("black", "red")) +
  geom_errorbar(aes(cd4, ymax = log_par_upper, ymin = log_par_lower)) +
  xlab("CD4 Bin") +
  theme(plot.title = element_text(size = 12))

plot(p)

pdf(file = paste0(graph_dir,"FILEPATH"), height = 6, width = 8)
for(t in unique(pars$type)) {
  for(a in unique(pars$age)) {

    temp <- pars[pars$age==a & pars$type == t,]

    a <- gsub("_", "-", a)
    if (a=="15-25") {
      a <- "15-24"
    } else if(a=="25_35") {
      a
    }
    if(a=="45-100") {
      a <- "45+"
    }
    p.temp <- p %+% temp +
      ggtitle(paste("95% CIs for model parameters:", a, t, sep=" "))

    if(t=="mortality") {
        p.temp <- p.temp +  scale_y_continuous(limits = c(-10,1), breaks = seq(-10, 1, by=1)) +
                  ylab(paste("Natural log risk of HIV relative death in 0.1 year interval"))
    } else if (t=="progression"){
        p.temp <- p.temp +  scale_y_continuous(limits = c(-5,-1), breaks = seq(-5, -1, by=1)) +
          ylab(paste("Natural log risk of progression in 0.1 year interval"))
    }

    print(p.temp)


  }
}
dev.off()


#################################################################################
##Just look at progression parameters and use facet.grid to make panels
#################################################################################
prog <- pars[pars$type=='progression',]

prog$age <- factor(prog$age, labels=c("15-24", "25-34", "35-44", "45+"))
prog$ind <- factor(prog$age, labels=c("15-24", "25-34", "35-44", "45+"))

p <- ggplot(prog, aes(cd4, log_par_mean, group=ind)) +
  geom_point(aes(colour=source)) +
  scale_colour_manual(name="Source", values=c("black", "red")) +
  geom_errorbar(aes(cd4, ymax = log_par_upper, ymin = log_par_lower)) +
  xlab("CD4 Bin") +
  ylab("Natural log risk of progression in 0.1 year interval") +
  ggtitle("95% CIs for GBD CD4 progression parameters compared to UNAIDS") +
  facet_wrap(~ age, ncol=2) +
  theme(plot.title = element_text(size = 15)) +
  theme(axis.title.y = element_text(size=15)) +
  theme(legend.text = element_text(size=15))

pdf(file = paste0(graph_dir,"FILEPATH"), height = 8.5, width = 11)
print(p)
dev.off()

#################################################################################
##Mortality parameters with facet.grid to make panels
#################################################################################
mort <- pars[pars$type=='mortality',]

mort$age <- factor(mort$age, labels=c("15-24", "25-34", "35-44", "45+"))
mort$ind <- factor(mort$age, labels=c("15-24", "25-34", "35-44", "45+"))

p <- ggplot(mort, aes(cd4, log_par_mean, group=ind)) +
  geom_point(aes(colour=source)) +
  scale_colour_manual(name="Source", values=c("black", "red")) +
  geom_errorbar(aes(cd4, ymax = log_par_upper, ymin = log_par_lower)) +
  xlab("CD4 Bin") +
  ylab("Natural log risk of HIV-relative death in 0.1 year interval") +
  ggtitle("95% CIs for GBD HIV-relative mortality parameters compared to UNAIDS") +
  facet_wrap(~ age, ncol=2) +
  theme(plot.title = element_text(size = 15)) +
  theme(axis.title.y = element_text(size=15)) +
  theme(legend.text = element_text(size=15))

pdf(file = paste0(graph_dir,"FILEPATH"), height = 8.5, width = 11)
print(p)
dev.off()

