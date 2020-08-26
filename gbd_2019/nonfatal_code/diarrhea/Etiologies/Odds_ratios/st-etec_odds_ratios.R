#############################################################
## Only estimate for ST-ETEC!!
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
# Change to ST-ETEC only in GBD 2019
base_names <- c("adenovirus","aeromonas","campylobacter","cryptosporidium","ehist","epec","st_etec","norovirus","rotavirus","salmonella","shigella","cholera")
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
## Make a loop for odds ratios. Use GLMM ##
## ONLY do ST-ETEC
#binary_names <- "binary_st_etec"
## GEMS first ##
output <- data.frame()
out.gems <- data.frame()
  # pathogen
  path.now <- "binary_st_etec"
  # all other pathogens
  paths.other <- binary_names[binary_names!=path.now]
  
  path_age_random <- c(paste0("(",path.now,"_age1|site)"), paste0("(",path.now,"_age2|site)"))

    form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_group)")," + ",
                             paste(paths.other, collapse= "+"), " + ",
                             paste(path_age_random, collapse="+"), "+ (1|pid)"))
    
    g <- glmer(form, data=subset(df, source=="GEMS"), family="binomial")
    
    betas <- fixef(g)[13:14]
    matrix <- sqrt(vcov(g))
    se <- c(matrix[13,13], matrix[14,14])
    odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), age_years=c("Under 1","Over 1"), age_group=c(1,2))

  out.gems <- rbind.data.frame(out.gems, odds)

out.gems$study <- "GEMS"

###################################################################
## Now for MALED ##
output <- data.frame()
out.maled <- data.frame()

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

out.maled$study <- "MALED"

###################################################################
## Lastly, both! ##
output <- data.frame()
out.both <- data.frame()

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

out.both$study <- "Both"


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
  ## handle Inf by setting to 1000 ##
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
# expand to all age groups and save for use #
age_meta <- read.csv("filepath")
age_meta <- subset(age_meta, rei==1)
age_meta$age_years <- ifelse(age_meta$age_group_id <5, "Under 1", "Over 1")

out.odds <- merge(age_meta, out.odds, by="age_years", all.x=T, all.y=T)
out.odds$regression_name <- out.odds$pathogen
out.odds <- join(out.odds, eti, by=c("regression_name"))
out.odds$age_years <- ifelse(out.odds$age_group_id<5, "Under 1", "Over 1")

write.csv(out.odds, "filepath", row.names=F)
