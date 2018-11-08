#################################################################
## GEMS and MALED sensitivity/specificity analyses ##
#################################################################
#################################################################
library(dplyr)
library(ggplot2)
library(boot)
library(lme4)

list1_data <- read.csv("FILEPATH/gems_maled_etiology_results_list1.csv")
df <- list1_data

### Set standard names ###
base_names <- c("adenovirus","aeromonas","campylobacter","cryptosporidium","ehist","epec","etec","norovirus","rotavirus","salmonella","shigella","cholera")
pcr_names <- paste0("tac_",base_names)
lab_names <- paste0("lab_",base_names)

##########################################################################################
## Create a dataframe with new, combined cut points for maximum accuracy of diagnostics ##
accuracy <- read.csv("FILEPATH/min_loess_accuracy.csv")
maled_table <- read.csv("FILEPATH/maled_diagnostic_accuracy.csv")

source('FILEPATH/max_accuracy_functions.R')
interval <- 0.05
combined <- c()
for(p in pcr_names){
  pathogen <- p
  output <- data.frame()
  out.maled <- data.frame()
  out.gems <- data.frame()
  ming <- floor(min(df[df$case==1 & df$source=="GEMS",pathogen], na.rm=T)) + 1
  minm <- floor(min(df[df$case==1 & df$source=="MALED",pathogen], na.rm=T)) + 1
  min <- max(ming, minm)
  is_case <- subset(df, !is.na(case))$case
  for(i in seq(min,35,interval)){
    a <- cross_table(value=i, data=subset(df, !is.na(case)), pathogen)
    a.maled <- cross_table(value=i, data=subset(df, !is.na(case) & source=="MALED"), pathogen)
    a.gems <- cross_table(value=i, data=subset(df, !is.na(case) & source=="GEMS"), pathogen)
    output <- rbind(output, data.frame(a, ct=i, type="Total"))
    out.maled <- rbind(out.maled, data.frame(a.maled, ct=i, type="MALED"))
    out.gems <- rbind(out.gems, data.frame(a.gems, ct=i, type="GEMS"))
  }
  output_total <- rbind(output, out.maled, out.gems)
  l <- loess(accuracy ~ ct,  data=output)
  output$p <- predict(l, data.frame(ct=seq(min,35,interval)))
  infl <- c(FALSE, diff(diff(output$p)>0)!=0)
  
  if(length(infl[infl==T])==0){
    combined_max <- vgems
  } else {
    combined_max <- output$ct[output$p==max(output$p[infl==T])]
  }
  
  yloc <- max(output$accuracy)
  
  vgems <- accuracy$ct.inflection[accuracy$loop_tac==pathogen]
  vmaled <- maled_table$ct_inflection[maled_table$loop_tac==pathogen]
  combined <- rbind(combined, data.frame(pathogen=substr(pathogen,5,20), combined_max))
}
write.csv(combined, "FILEPATH/combined_gems_maled_cutoffs.csv", row.names = F)

df <- list2_data
case.df <- subset(df, case==1 & !is.na(tac_adenovirus))

sens <- data.frame(pathogen=base_names, measure="sensitivity")
spec <- data.frame(pathogen=base_names, measure="specificity")
for(i in 1:1000){
  cases <- sample_n(case.df, length(case.df$pid), replace=T)
  new.boot <- cases
  df.out <- data.frame()
  for(j in 1:12){
    tac <- pcr_names[j]
    lab <- lab_names[j]
    positive <- ifelse(new.boot[,tac]<combined$list1[j],1,0)
    r <- table(new.boot[,lab], positive)
    sensitivity <- r[2,2]/(r[1,2]+r[2,2])
    specificity <- r[1,1]/(r[1,1]+r[2,1])
    sens[j,2+i] <- sensitivity
    colnames(sens)[2+i] <- paste0("draw_",i)
    spec[j,2+i] <- specificity
    colnames(spec)[2+i] <- paste0("draw_",i)
  }
}

adjustment <- rbind.data.frame(sens, spec)
write.csv(adjustment, "FILEPATH/combined_gems_maled_sens_spec_list1.csv")
