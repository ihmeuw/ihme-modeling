### Calculating the age and pathogen-specific odds ratios for GEMS and TAC reanalysis ###
## Note that internal IHME filepaths have been replaced with FILEPATH ##
## Thanks for reading! ##

## Import packages ##
library(coxme)
library(plyr)
library(reshape2)
library(mvtnorm)
library(matrixStats)

#### The most important changes from GBD 2015 are that we are estimating odds ratios for only two age groups: 
## Under-1 and Over-1. The second big change is that the odds are forced to be above 1. This affects Aeromonas and Entamoeba
## in the Under-1 age group and Campylobacter in the Over-1 age group. #####

#### We have decided to use FIXED effects only from a case definition using the lowest point where a local maximum occurs for accuracy ####
accuracy <- read.csv("FILEPATH/min_loess_accuracy.csv") # bimodal accuracy (adenovirus eg)
accuracy <- subset(accuracy, pathogen!="tac_EAEC")

gems <- read.csv("FILEPATH/gems_final.csv")

## Create new variable 'binary_[pathogen]' but cutting at Ct value
pathogens <- as.vector(accuracy$pathogen)
cts <- as.vector(accuracy$ct.inflection)

tac.results <- gems[,pathogens]
tac <- gems[,pathogens]
for(i in 1:12){
  value <- cts[i]
  path <- pathogens[i]
  obs <- tac.results[,path]
  tac.results[,path] <- ifelse(obs<value,1,0)
}

colnames(tac.results) <- paste0("binary_",substr(pathogens,5,20))

gems <- data.frame(gems, tac.results)

#### Main Regression ####
## Create variables for interaction of age and pathogen status ###
binary <- colnames(tac.results)
gems2 <- gems
for(b in 1:12){
  apath <- binary[b]
  age1 <- ifelse(gems[,apath]==1,ifelse(gems$age==1,1,0),0)
  age2 <- ifelse(gems[,apath]==1,ifelse(gems$age>=2,1,0),0)
  out <- data.frame(age1,age2)
  colnames(out) <- c(paste0(substr(apath,8,25),"age1"),paste0(substr(apath,8,25),"age2"))
  gems2 <- data.frame(gems2, out)
}
#  GEMS 'site.names' incorrect
gems2$site.names <- revalue(gems2$site.names, c("Karachi"="Pakistan", "Gambia"="The Gambia"))

### Big loop to model for each pathogen, then calculate uncertainty for each site/age combination ###
## Trick coxme to run a mixed effects conditional logistic regression model
gems2$time <- 1
# empty data frame for saving 
## recode age groups to be above, below 1 year ##
gems2$age_new <- ifelse(gems2$age>=2,2,1)

## Get metadata about etiologies. ##
rei <- read.csv("FILEPATH/eti_rr_me_ids.csv")
rei <- rei[!is.na(rei$id),]
rei <- subset(rei, rei_id!=183)
rei <- rei[order(rei$reg_name),]
rei$pathogen <- rei$reg_name
# coefficients are the same whether mixed effects with only random intercepts or a fixed effects model.
output <- data.frame()
out.odds <- data.frame()
for(b in 1:12){ 
  # pathogen
  path.now <- binary[b]
  # all other pathogens
  paths.in <- binary[binary!=path.now]

  paths.age <- c(paste0("(",substr(path.now,8,25),"age1|site.names)"), paste0("(",substr(path.now,8,25),"age2|site.names)"))
  form <- as.formula(paste("Surv(time, case.control) ~ ", paste(paths.in, collapse="+"), paste0("+ ",path.now,":factor(age_new)"," +"),
                           paste(paths.age, collapse="+"), "+ strata(CASEID)"))
  fitme <- coxme(form, data=gems2)

  betas <- fitme$coefficients[12:13]
  matrix <- sqrt(vcov(fitme))
  se <- c(matrix[12,12], matrix[13,13])
  odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2))
  
  out.odds <- rbind.data.frame(out.odds, odds)
}

out.odds <- join(out.odds, rei, by="pathogen")

## Set age groups for plotting now ##
out.odds$age <- rep(c("Below 1","Over 1"),12)

## Separate draws by significance ##
backup <- out.odds
sig <- subset(out.odds, pvalue<0.05)
insig <- subset(out.odds, pvalue>=0.05)

exp.odds <- c()
for(i in seq(1,1000,1)){

## How should "Inf" values be handled? ##
  draw <- exp(rnorm(length(insig$lnor), mean=insig$lnor-1, sd=insig$errors))+1
  insig[,paste0("odds_",i)] <- ifelse(draw=="Inf",1000,draw)
}
for(i in seq(1,1000,1)){
  sig[,paste0("odds_",i)] <- exp(rnorm(length(sig$lnor), mean=sig$lnor, sd=sig$errors))
}

out.odds <- rbind.data.frame(sig, insig)

for(i in seq(1,1000,1)){
  exp.odds <- cbind(exp.odds, out.odds[,paste0("odds_",i)])
}

out.odds$mean_or <- rowMeans(exp.odds)
out.odds$std_or <- rowSds(exp.odds)
out.odds$lower_bound <- rowQuantiles(exp.odds, probs=0.025)
out.odds$upper_bound <- rowQuantiles(exp.odds, probs=0.975)

#### Very nice, congratulations! ####

## Last, you need to expand to all age groups and save for use! ##
age_meta <- read.csv("FILEPATH/age_mapping.csv")
age_meta <- subset(age_meta, rei==1)

out.odds$under_1 <- ifelse(out.odds$age=="Below 1",1,0)

final.df <- merge(age_meta, out.odds, by="under_1", all.x=T, all.y=T)

write.csv(final.df, "FILEPATH/odds_ratios_gbd_2016.csv")

######### That's all, folks! ############

