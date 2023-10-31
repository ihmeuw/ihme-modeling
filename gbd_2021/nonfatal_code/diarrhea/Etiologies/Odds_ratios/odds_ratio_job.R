#############################################################
## Run regression models for GEMS and MALED odds ratios ##
## This code uses the combined GEMS and MALED data to 
## estimate the odds of diarrhea given the 12 etiologies
## measured in both for GBD among children 0-1 years and 
## older than 1 years.
#############################################################

etiology <- commandArgs(trailingOnly=TRUE)[1] 
study <- commandArgs(trailingOnly=TRUE)[2] 
output_path <- commandArgs(trailingOnly=TRUE)[3] 

library(lme4)
library(ggplot2)
library(plyr)


df <- read.csv("/FILEPATH/")
cut_df <- read.csv("/FILEPATH/")

### Set standard names ###
base_names <- c("adenovirus","aeromonas","campylobacter","cryptosporidium","ehist","epec","etec","norovirus","rotavirus","salmonella","shigella","cholera","astrovirus","sapovirus")
pcr_names <- paste0("tac_",base_names)
binary_names <- paste0("binary_",base_names)
b <- paste0("binary_",etiology)

#############################################################
## Create binary indicators ##
for(t in base_names){
  if(t == "etec"){
    t2 <- "st_etec"
  } else{
    t2 <- t
  }
  ct <- cut_df$combined_max[cut_df$pathogen==t2]
  positive <- ifelse(df[,paste0("tac_",t)] < ct, 1, 0)
  df[,paste0("binary_",t)] <- positive
}

##Ensure there are only two age groups ##
df$age_group <- ifelse(df$age_years==0,1,2)

## Create variables for interaction of age and pathogen status ###
for(n in 1:length(binary_names)){
  apath <- binary_names[n]
  age1 <- ifelse(df[,apath]==1,ifelse(df$age_group==1,1,0),0)
  age2 <- ifelse(df[,apath]==1,ifelse(df$age_group>=2,1,0),0)
  out <- data.frame(age1,age2)
  colnames(out) <- c(paste0(apath,"_age1"),paste0(apath,"_age2"))
  df <- data.frame(df, out)
}

if(study %in% c("GEMS","MALED")){
  df <- subset(df,source==study)
}

output <- data.frame()

print(b)
# pathogen
path.now <- b
# all other pathogens
paths.other <- binary_names[binary_names!=path.now]

path_age_random <- c(paste0("(",path.now,"_age1|site)"), paste0("(",path.now,"_age2|site)"))

form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_group)")," + ",
                         paste(paths.other, collapse= "+"), " + ",
                         paste(path_age_random, collapse="+"), "+ (1|pid)"))

g <- glmer(form, data=df, family="binomial")

betas_names <- paste0(b,c(":factor(age_group)1",":factor(age_group)2"))
betas_all <- fixef(g)
betas <- betas_all[which(names(betas_all) %in% betas_names)]

matrix <- sqrt(vcov(g))
row_inds <- c(which(rownames(matrix)==betas_names[1]),
              which(rownames(matrix)==betas_names[2]))
col_inds <- c(which(colnames(matrix)==betas_names[1]),
              which(colnames(matrix)==betas_names[2]))
se <- c(matrix[row_inds[1],col_inds[1]],matrix[row_inds[2],col_inds[2]])

odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), age_years=c("Under 1","Over 1"), age_group=c(1,2))

output <- rbind.data.frame(output, odds)

if(study %in% c("GEMS","MALED")){
  output$study <- study
} else{
  output$study <- "Both"
}

filepath <- paste0(output_path,"/odds_results_gems_update_",etiology,"_",study,".csv")
write.csv(output, filepath, row.names=F)