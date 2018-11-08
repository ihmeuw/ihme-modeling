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
df <- read.csv("FILEPATH/gems_maled_etiology_results.csv")

# File created in 'pooled_sen-spe_maled_gems.R' file
cut_df <- read.csv("FILEPATH/combined_gems_maled_cutoffs.csv")

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
  
## Ehist doesn't have any cases in children under-1! Calculate a single value for this ##
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
write.csv(out.gems, "FILEPATH/odds_results_gems_update.csv", row.names=F)

###################################################################
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
write.csv(out.maled, "FILEPATH/odds_results_maled_list2.csv", row.names=F)

###################################################################
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
write.csv(out.both, "FILEPATH/odds_results_gems_maled_update.csv", row.names=F)

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
  draw <- exp(rnorm(length(insig$lnor), mean=insig$lnor-1, sd=insig$errors))+1
  insig[,paste0("odds_",i)] <- ifelse(draw=="Inf",1000,draw)
  #insig[,paste0("odds_",i)] <- ifelse(draw>100,100,draw)
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
eti <- read.csv('FILEPATH/eti_rr_me_ids.csv')
age_meta <- read.csv("/FILEPATH/age_mapping.csv")
age_meta <- subset(age_meta, rei==1)
age_meta$age_years <- ifelse(age_meta$age_group_id <5, "Under 1", "Over 1")

out.odds <- merge(age_meta, out.odds, by="age_years", all.x=T, all.y=T)
out.odds$regression_name <- out.odds$pathogen
out.odds <- join(out.odds, eti, by=c("regression_name"))
out.odds$age_years <- ifelse(out.odds$age_group_id<5, "Under 1", "Over 1")

write.csv(out.odds, "FILEPATH/full_odds_results_maled-list2_gems_both.csv", row.names=F)

