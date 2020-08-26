## Data prep for MAL-ED and qPCR results ##
library(plyr)
library(zoo)
library(ggplot2)
library(lme4)

micro <- read.csv("filepath")
tac <- read.csv("filepath")

## GBD pathogens ##
gbd.names <- c("adeno","aeromonas","campy","crypto","ehist","etec","nrvgeno2","rota","salmonella","shigella","epec","vibrio")
keep.micro <- micro[,c("pid","agedays","flag_diarrhea",gbd.names)]
keep.micro[is.na(keep.micro)] <- 0

## Some data prep for TAC results ##
tac$adenovirus <- tac$adenovirus_40_41
tac$campylobacter <- tac$campylobacter_jejuni_coli
tac$ehist <- tac$e_histolytica
tac$ETEC <- ifelse(tac$ST_ETEC < tac$LT_ETEC, tac$ST_ETEC, tac$LT_ETEC)
tac$norovirus <- tac$norovirus_gii

tac.names <- c("adenovirus", "aeromonas", "c_difficile", "campylobacter", "cryptosporidium", "ehist", "tEPEC", "ETEC", "norovirus","rotavirus","salmonella","shigella_eiec","v_cholerae")

keep.tac <- tac[,c("pid","month_ss","agedays","stooltype","stooltype7","stooltype14","stooltype28","srfspectype","pilot","list1","list2","flag24","matchid", tac.names)]
keep.tac$case <- ifelse(keep.tac$stooltype7=="D1",1, ifelse(keep.tac$stooltype7=="M1",0,NA))

keep.tac$keep_month <- ifelse(keep.tac$month_ss %in% c(3,6,9,12,15,18,21,24), 1, 0)
keep.tac$keep_overall <- ifelse(keep.tac$case==1, 1, ifelse(keep.tac$keep_month==1,1,0))

keep.tac$site <- substr(keep.tac$pid, 1,2)
write.csv(keep.tac, "filepath")

maled <- join(keep.micro, keep.tac, by=c("pid", "agedays"))

##############################################################
### Maximizes accuracy (correctly assigns case-control status) for GEMS reanalysis using TAC data ###
#### Create loess curves for each, find loess maximum #####
n <- 1
span <- 0.3
interval <- 0.2
type <- "maled"
loess.out <- data.frame()
pdf(paste0("filepath"))
  source("filepath/plot_max_accuracy.R")
dev.off()

keep.tac <- subset(keep.tac, list1==1)
loess.out <- data.frame()
pdf(paste0("filepath"))
  source("filepath/plot_max_accuracy.R")
dev.off()
maled_table <- loess.out

ggplot(data=subset(keep.tac, !is.na(case) & adenovirus<35), aes(x=adenovirus, fill=as.factor(case))) + geom_histogram(binwidth=1, position=position_dodge(width=0.9)) + theme_bw()
ggplot(data=subset(keep.tac, !is.na(case) & adenovirus<35), aes(x=adenovirus, fill=as.factor(case))) + geom_density(alpha=0.5) + theme_bw()

###################################################################
## Create new variable 'binary_[pathogen]' but cutting at Ct value
accuracy <- read.csv("filepath")
maled_table <- read.csv("filepath")

tac.results <- keep.tac[,tac.names]
output <- data.frame(site_name = keep.tac$site)
for(p in tac.names){
  output[,paste0("binary_",p)] <- ifelse(tac.results[,p] < maled_table$ct_gbd_gems[maled_table$pathogen==p], 1, 0)
  output[,paste0("dassociated_",p)] <- ifelse(tac.results[,p] < maled_table$ct_or_2[maled_table$pathogen==p], 1, 0)
  output[,paste0("maledacc_",p)] <- ifelse(tac.results[,p] < maled_table$ct_inflection[maled_table$pathogen==p], 1, 0)
}
keep.tac <- data.frame(keep.tac, output)
write.csv(keep.tac, "filepath")

#### Main Regression ####
## Create variables for interaction of age and pathogen status ###
binary <- colnames(tac.results)
df <- keep.tac
df$age_new <- ifelse(df$agedays<365,1,2)
for(b in 1:12){
  apath <- binary[b]
  age1 <- ifelse(df[,apath]==1,ifelse(df$agedays<=365,1,0),0)
  age2 <- ifelse(df[,apath]==1,ifelse(df$agedays>365,1,0),0)
  out <- data.frame(age1,age2)
  colnames(out) <- c(paste0(substr(apath,8,25),"age1"),paste0(substr(apath,8,25),"age2"))
  df <- data.frame(df, out)
}


## subset data for analysis to list1
# monthly stool only in c(3,6,9,12,15,18,21,24)
df_mod <- subset(df, list1==1 & keep_overall==1)

output <- data.frame()
out.odds <- data.frame()
for(b in 1:12){ 
  # pathogen
  path.now <- binary[b]
  # all other pathogens
  paths.in <- binary[binary!=path.now]
  
  paths.age <- c(paste0("(",substr(path.now,8,25),"age1|site)"), paste0("(",substr(path.now,8,25),"age2|site)"))
  #form <- as.formula(paste("case ~ ", paste(paths.in, collapse="+"), paste0("+ ",path.now,":factor(age_new)"," +"),
  #                         "(1|site) + (1|pid)"))
  form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_new)"), "+ (1|pid)"))
  # form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_new)"), "+", paste0("(",path.now,"|site)"), "+ (1|pid)"))
  # form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_new)"),"+", paste(paths.in, collapse="+"), "+ (1|pid)"))
  # form <- as.formula(paste("case ~ ", paste0(path.now,":factor(age_new)"), "+", 
  #                          paste(paths.in, collapse="+"), "+", paste0("(",path.now,"|site)"), "+ (1|pid)"))
  
  fitme <- glmer(form, data=df_mod, family="binomial")
  
  betas <- fitme@beta[2:3]
  matrix <- sqrt(vcov(fitme))
  se <- c(matrix[2,2], matrix[3,3])
  odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), age=c("1 and under","1-2 Years"))
  
  out.odds <- rbind.data.frame(out.odds, odds)
}

out.odds$lower <- exp(out.odds$lnor - out.odds$errors*1.96)
out.odds$odds <- exp(out.odds$lnor)
write.csv(out.odds, "filepath")
pdf("filepath")
  ggplot(data=out.odds, aes(x=pathogen, y=exp(lnor))) + facet_grid(~age) + geom_point() + theme_bw() +
    geom_errorbar(aes(ymin=lower, ymax=exp(lnor)), width=0) + geom_hline(yintercept=1, lty=2) + theme(axis.text.x=element_text(angle=90, hjust=1)) +
    ylab("Odds ratio")
dev.off()

## Compare with GEMS ##
gems <- read.csv("filepath")
gems <- subset(gems, age_group_id %in% c(4,5))
gems$age <- ifelse(gems$age_group_id==5, "1+ Years", "1 and under")
gems$pathogen <- ifelse(gems$pathogen=="TEPEC","tEPEC", as.character(gems$pathogen))
gems$pathogen <- ifelse(gems$pathogen=="entamoeba", "ehist", as.character(gems$pathogen))

out.odds$age <- ifelse(out.odds$age=="1 and under", "1 and under", "1+ Years")
comp_df <- rbind(data.frame(pathogen = out.odds$pathogen, age = out.odds$age, odds = exp(out.odds$lnor), lower= out.odds$lower, source="MAL-ED"),
                 data.frame(pathogen = gems$pathogen, age = gems$age, odds=exp(gems$lnor), lower = gems$lower_bound, source="GEMS"))
pdf("filepath")
  ggplot(data=comp_df, aes(x=pathogen, y=odds, col=source)) + facet_grid(~age) + geom_point(position=position_dodge(width=0.9)) + theme_bw() +
    geom_errorbar(aes(ymin=lower, ymax=odds), width=0, position=position_dodge(width=0.9)) + 
    geom_hline(yintercept=1, lty=2) + theme(axis.text.x=element_text(angle=90, hjust=1)) +
    scale_y_continuous("Odds ratio", limits=c(0,20))
dev.off()

## Aeromonas, Entamoeba, and Salmonella are not statistically significant, try running without age split ##
output <- data.frame()
out.prob <- data.frame()
for(b in c("binary_aeromonas","binary_ehist","binary_salmonella")){ 
  # pathogen
  path.now <- b
  # all other pathogens
  paths.in <- binary[binary!=path.now]
  form <- as.formula(paste("case ~ ", path.now, "+ (1|pid)"))
  fitme <- glmer(form, data=df_mod, family="binomial")
  
  betas <- fitme@beta[2]
  matrix <- sqrt(vcov(fitme))
  se <- c(matrix[2,2])
  odds <- data.frame(lnor=betas, errors=se, pathogen = substr(path.now, 8, 25), pvalue = signif(1 - pchisq((betas/se)^2, 1), 2), age=c("0-2 Years"))
  
  out.prob <- rbind.data.frame(out.prob, odds)
}
out.prob$odds <- exp(out.prob$lnor)
