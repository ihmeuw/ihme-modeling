#############################################################
## Run regression models for GEMS and MALED odds ratios ##
## This code uses the combined GEMS and MALED data to 
## estimate the odds of diarrhea given the 12 etiologies
## measured in both for GBD among children 0-1 years and 
## older than 1 years.
#############################################################
library(lme4)
library(ggplot2)
library(plyr)

# File created in 'combine_compare_maled_gems.R' file
df <- read.csv("filepath")

# File created in 'pooled_sen-spe_maled_gems.R' file
cut_df <- read.csv("filepath")

### Set standard names ###
base_names <- c("adenovirus","aeromonas","campylobacter","cryptosporidium","ehist","epec","etec","norovirus","rotavirus","salmonella","shigella","cholera")
pcr_names <- paste0("tac_",base_names)
binary_names <- paste0("binary_",base_names)

#############################################################
## Create binary indicators ##
for(t in base_names){
  ct <- cut_df$combined_max[cut_df$pathogen==t]
  positive <- ifelse(df[,paste0("tac_",t)] < ct, 1, 0)
  df[,paste0("binary_",t)] <- positive
}

## Make sure that there are only two age groups ##
df$age_group <- ifelse(df$age_years==0,1,2)

## Create variables for interaction of age and pathogen status ###
for(b in 1:12){
  apath <- binary_names[b]
  age1 <- ifelse(df[,apath]==1,ifelse(df$age_group==1,1,0),0)
  age2 <- ifelse(df[,apath]==1,ifelse(df$age_group>=2,1,0),0)
  out <- data.frame(age1,age2)
  colnames(out) <- c(paste0(apath,"_age1"),paste0(apath,"_age2"))
  df <- data.frame(df, out)
}

###########################################################################################################
## Make a loop for odds ratios. They are a little different from GBD 2016 because here we will use GLMM ##

## Test out on GEMS first ##
output <- data.frame()
out.gems <- data.frame()
for(b in binary_names){ 
  print(b)
  # pathogen
  path.now <- b
  # all other pathogens
  paths.other <- binary_names[binary_names!=path.now]
  
  path_age_random <- c(paste0("(",path.now,"_age1|site)"), paste0("(",path.now,"_age2|site)"))
  
## Ehist doesn't have any cases in children under-1. Calculate a single value for this ##
  if(b != "binary_ehist"){
    form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_group)")," + ",
                             paste(paths.other, collapse= "+"), " + ", 
                             paste(path_age_random, collapse="+"), "+ (1|pid)"))
    
    g <- glmer(form, data=subset(df, source=="GEMS"), family="binomial")

    betas <- fixef(g)[13:14]
    matrix <- sqrt(vcov(g))
    se <- c(matrix[13,13], matrix[14,14])
    odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), age_years=c("Under 1","Over 1"), age_group=c(1,2))
  } else {
    form <- as.formula(paste("case ~ ", path.now," + ",
                             paste(paths.other, collapse= "+"), "+ (1|pid) + ",paste0("(",path.now,"|site)")))
    
    g <- glmer(form, data=subset(df, source=="GEMS"), family="binomial")

    betas <- fixef(g)[2]
    matrix <- sqrt(vcov(g))
    se <- c(matrix[2,2])
    odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), age_years=c("Under 1","Over 1"), age_group=c(1,2))
  }  
  out.gems <- rbind.data.frame(out.gems, odds)
}
out.gems$study <- "GEMS"
write.csv(out.gems, "filepath", row.names=F)

###################################################################
## Yay, next do it for MALED ##
output <- data.frame()
out.maled <- data.frame()
for(b in binary_names){ 
  print(b)
  # pathogen
  path.now <- b
  # all other pathogens
  paths.other <- binary_names[binary_names!=path.now]
  
  path_age_random <- c(paste0("(",path.now,"_age1|site)"), paste0("(",path.now,"_age2|site)"))
  
  form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_group)")," + ",
                           paste(paths.other, collapse= "+"), " + ", 
                           paste(path_age_random, collapse="+"), "+ (1|pid)"))
  
  g <- glmer(form, data=subset(df, source=="MALED"), family="binomial")
  
  betas <- fixef(g)[13:14]
  matrix <- sqrt(vcov(g))
  se <- c(matrix[13,13], matrix[14,14])
  odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), age_years=c("Under 1","Over 1"), age_group=c(1,2))
  
  out.maled <- rbind.data.frame(out.maled, odds)
}
out.maled$study <- "MALED"
write.csv(out.maled, "filepath", row.names=F)

###################################################################
## Lastly, both! ##
output <- data.frame()
out.both <- data.frame()
for(b in binary_names){ 
  print(b)
  # pathogen
  path.now <- b
  # all other pathogens
  paths.other <- binary_names[binary_names!=path.now]
  
  path_age_random <- c(paste0("(",path.now,"_age1|site)"), paste0("(",path.now,"_age2|site)"))
  
  form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_group)")," + ",
                           paste(paths.other, collapse= "+"), " + ", 
                           paste(path_age_random, collapse="+"), "+ (1|pid) + (1|source)"))
  
  g <- glmer(form, data=df, family="binomial")
  
  betas <- fixef(g)[13:14]
  matrix <- sqrt(vcov(g))
  se <- c(matrix[13,13], matrix[14,14])
  odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), age_years=c("Under 1","Over 1"), age_group=c(1,2))
  
  out.both <- rbind.data.frame(out.both, odds)
}
out.both$study <- "Both"
write.csv(out.both, "filepath", row.names=F)

#####################################################################
final.df <- rbind(out.gems, out.maled, out.both)
final.df$odds <- exp(final.df$lnor)
#final.df$errors <- ifelse(final.df$errors>10,10,final.df$errors)

#####################################################################
## Create draw matrix ##
#####################################################################

# Separate significant, non-significant ORs #
sig <- subset(final.df, pvalue < 0.05)
insig <- subset(final.df, pvalue >= 0.05)

###### Create draws #######
exp.odds <- c()
for(i in seq(1,1000,1)){
  ## Handle Inf values by setting to 1000 ##
  draw <- exp(rnorm(length(insig$lnor), mean=insig$lnor-1, sd=insig$errors))+1
  insig[,paste0("odds_",i)] <- ifelse(draw=="Inf",1000,draw)
}
for(i in seq(1,1000,1)){
  sig[,paste0("odds_",i)] <- exp(rnorm(length(sig$lnor), mean=sig$lnor, sd=sig$errors))
}

out.odds <- rbind.data.frame(sig, insig)


###### Summarize ########
for(i in seq(1,1000,1)){
  exp.odds <- cbind(exp.odds, out.odds[,paste0("odds_",i)])
}

out.odds$odds <- rowMeans(exp.odds)
out.odds$odds_lower <- apply(exp.odds, 1, quantile, probs = c(0.025))
out.odds$odds_upper <- apply(exp.odds, 1, quantile, probs = c(0.975))

## Save ##
eti <- read.csv('filepath')
# Expand to all age groups and save for use #
age_meta <- read.csv("filepath")
age_meta <- subset(age_meta, rei==1)
age_meta$age_years <- ifelse(age_meta$age_group_id <5, "Under 1", "Over 1")

out.odds <- merge(age_meta, out.odds, by="age_years", all.x=T, all.y=T)
out.odds$regression_name <- out.odds$pathogen
out.odds <- join(out.odds, eti, by=c("regression_name"))
out.odds$age_years <- ifelse(out.odds$age_group_id<5, "Under 1", "Over 1")

write.csv(out.odds, "filepath", row.names=F)

small_odds <- data.frame(pathogen=out.odds$pathogen, age_years=out.odds$age_years, age_group_id=out.odds$age_group_id, source=out.odds$study, lnor=out.odds$lnor, lnse=out.odds$errors,
                         odds=out.odds$odds, odds_lower=out.odds$odds_lower, odds_upper=out.odds$odds_upper, rei_name=out.odds$rei_name, rei_id=out.odds$rei_id)
write.csv(small_odds, "filepath")

ggplot(final.df, aes(x=pathogen, y=odds, col=study, shape=age_years)) + geom_point(position=position_dodge(width=0.9)) + scale_y_continuous(limits=c(0,15)) + 
  geom_hline(yintercept=1) + theme_bw()

ggplot(out.odds, aes(x=pathogen, y=odds, col=source, shape=age_years)) + geom_point(position=position_dodge(width=0.9), size=3) + scale_y_continuous("Odds ratio", limits=c(0,15)) + 
  geom_errorbar(aes(ymin=odds_lower, ymax=odds_upper), position=position_dodge(width=0.9)) + 
  geom_hline(yintercept=1) + facet_wrap(~age_years) + theme_bw() + xlab("") + theme(axis.text.x=element_text(hjust=1, angle=90)) + guides(shape=F)

small_odds <- subset(small_odds, age_group_id %in% c(2,5))
i1 <- data.frame(pathogen=small_odds$pathogen[small_odds$source=="GEMS"], odds_gems=small_odds$odds[small_odds$source=="GEMS"], 
                 age=small_odds$age_years[small_odds$source=="GEMS"], se_gems=small_odds$lnse[small_odds$source=="GEMS"])
i2 <- data.frame(pathogen=small_odds$pathogen[small_odds$source=="MALED"], odds_maled=small_odds$odds[small_odds$source=="MALED"], 
                 age=small_odds$age_years[small_odds$source=="MALED"], se_maled=small_odds$lnse[small_odds$source=="MALED"])
i3 <- data.frame(pathogen=small_odds$pathogen[small_odds$source=="Both"], odds_both=small_odds$odds[small_odds$source=="Both"], 
                 age=small_odds$age_years[small_odds$source=="Both"], se_both=small_odds$lnse[small_odds$source=="Both"])
sdf <- join(i1, i2, by=c("age","pathogen"))
sdf <- join(sdf, i3, by=c("age","pathogen"))

#######################################################################################################
pdf("filepath", height=6, width=8)
  ggplot(sdf, aes(x=odds_gems, y=odds_maled, col=pathogen)) + 
    geom_point(size=3, alpha=0.7) + facet_wrap(~age) + scale_x_continuous("GEMS", limits=c(0,20)) + 
    scale_y_continuous("MALED", limits=c(0,20)) + theme_bw() +
    geom_abline(intercept=0, slope=1)
  ggplot(sdf, aes(x=odds_maled, y=odds_both, col=pathogen)) + 
    geom_point(size=3, alpha=0.7) + facet_wrap(~age) + scale_x_continuous("MALED", limits=c(0,20)) + 
    scale_y_continuous("Both", limits=c(0,20)) + theme_bw() +
    geom_abline(intercept=0, slope=1)
  ggplot(sdf, aes(x=odds_gems, y=odds_both, col=pathogen)) + 
    geom_point(size=3, alpha=0.7) + facet_wrap(~age) + scale_x_continuous("GEMS", limits=c(0,20)) + 
    scale_y_continuous("Both", limits=c(0,20)) + theme_bw() +
    geom_abline(intercept=0, slope=1)
  
  ## Plot uncertainty ##
  diag <- read.csv("filepath")
  diag$gems_se <- ifelse(diag$gems_se>2,2,diag$gems_se)
  diag$maled_se <- ifelse(diag$maled_se>2,2,diag$maled_se)
  diag$combined_se <- ifelse(diag$combined_se>2,2,diag$combined_se)
  
  ggplot(sdf, aes(x=se_gems, y=se_maled, col=pathogen, shape=as.factor(age))) + theme_bw() + geom_point(size=3) + scale_x_continuous("Standard Error GEMS", limits=c(0,2)) +
    scale_y_continuous("Standard Error MALED", limits=c(0,2)) + guides(shape=F) + geom_abline(intercept=0, slope=1) + ggtitle("Standard error of odds ratio predictions")
  ggplot(sdf, aes(x=se_both, y=se_gems, col=pathogen, shape=as.factor(age))) + theme_bw() + geom_point(size=3) + scale_x_continuous("Standard Error combined GEMS/MALED", limits=c(0,2)) +
    scale_y_continuous("Standard Error GEMS", limits=c(0,2)) + guides(shape=F) + geom_abline(intercept=0, slope=1) + ggtitle("Standard error of odds ratio predictions")
  ggplot(sdf, aes(x=se_both, y=se_maled, col=pathogen, shape=as.factor(age))) + theme_bw() + geom_point(size=3) + scale_x_continuous("Standard Error combined GEMS/MALED", limits=c(0,2)) +
    scale_y_continuous("Standard Error MALED", limits=c(0,2)) + guides(shape=F) + geom_abline(intercept=0, slope=1) + ggtitle("Standard error of odds ratio predictions")
  
  ###################################################################
  ## Compare with GBD 2016 ##
  ###################################################################
  old <- read.csv('filepath')
  old <- subset(old, age_group_id %in% c(2,5))
  small_odds$under_1 <- ifelse(small_odds$age_years=="Under 1",1,0)
  s <- subset(small_odds, age_group_id %in% c(2,5) & source=="GEMS")
  cdf <- join(old, s, by=c("rei_id","under_1"))
  
  ggplot(data=cdf, aes(x=mean_or, y=odds, col=pathogen)) + geom_point(size=3, alpha=0.6) + 
    theme_bw() + scale_x_continuous("GBD 2016 Coxme", limits=c(0,20)) + scale_y_continuous("GBD 2017 Glmer", limits=c(0,20)) +
    facet_wrap(~under_1) + geom_abline(intercept=0, slope=1) 
dev.off()

#######################################################################
#######################################################################
#######################################################################
## Test where study is random intercept ##
#######################################################################
df$source_age <- paste0(df$source,"_",df$age_group)
output <- data.frame()
out.gems <- data.frame()
for(b in binary_names){ 
  print(b)
  # pathogen
  path.now <- b
  # all other pathogens
  paths.other <- binary_names[binary_names!=path.now]
  
  path_age_random <- c(paste0("(",path.now,"_age1|site)"), paste0("(",path.now,"_age2|site)"))

  form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_group)")," + ",
                           paste(paths.other, collapse= "+"), " + ", 
                           "(1|source_age) + (1|pid)"))
  
  g <- glmer(form, df, family="binomial", link="logit")
  
  betas <- fixef(g)[13:14]
  ranef <- ranef(g)$source
  ranef_gems <- ranef$`(Intercept)`[1:2]
  ranef_maled <- ranef$`(Intercept)`[3:4]
  gems_betas <- betas + ranef_gems
  maled_betas <- betas + ranef_maled
  matrix <- sqrt(vcov(g))
  se <- c(matrix[13,13], matrix[14,14])
  odds <- data.frame(lnor=betas, ranef=c(ranef_gems, ranef_maled), errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), 
                     age_years=c("Under 1","Over 1"), age_group=c(1,2),
                     source=c("GEMS","GEMS","MALED","MALED"))
  
  out.gems <- rbind.data.frame(out.gems, odds)
}

write.csv("filepath")
