#################################################################
## GEMS and MALED sensitivity/specificity analyses ##
#################################################################
## List1 data only are being used to determine the cutoff point
## for the molecular diagnostics because of consistency with
## the results from GEMS. For the sensitivity/specificity, it
## doesn't matter because all the diarrhea samples in MALED
## are in List1 data.
#################################################################
library(dplyr)
library(ggplot2)
library(boot)
library(lme4)

list1_data <- read.csv("filepath")
df <- list1_data

### Set standard names ###
# For GBD 2019, change ETEC to ST-ETEC
base_names <- c("adenovirus","aeromonas","campylobacter","cryptosporidium","ehist","epec","st_etec","norovirus","rotavirus","salmonella","shigella","cholera")
pcr_names <- paste0("tac_",base_names)
lab_names <- paste0("lab_",base_names)

##########################################################################################
## Create a dataframe with new, combined cut points for maximum accuracy of diagnostics ##
accuracy <- read.csv("filepath")
maled_table <- read.csv("filepath")

source('filepath/max_accuracy_functions.R')
interval <- 0.02
combined <- c()
total_output <- data.frame()
for(p in 1:length(pcr_names)){
  pathogen <- pcr_names[p]
  print(pathogen)
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

  output_total$pathogen <- base_names[p]
  total_output <- rbind(total_output, output_total)
}
write.csv(combined, "filepath", row.names = F)
write.csv(total_output, "filepath", row.names=F)


## Using the values from 'list1' in GBD 2017 and replacing the value for
# ETEC with ST-ETEC from the code above. C
res_2019 <- read.csv("filepath")
combined <- read.csv("filepath")

combined$list1 <- ifelse(combined$pathogen == "etec", res_2019$combined_max[res_2019$pathogen=="st_etec"], combined$list1)

df <- list1_data
case.df <- subset(df, case==1 & !is.na(tac_adenovirus))

sens <- data.frame(pathogen=base_names, measure="sensitivity")
spec <- data.frame(pathogen=base_names, measure="specificity")
for(i in 1:1000){
  ##### Cases only #####
  ## several options to define sample:
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
write.csv(adjustment, "filepath")

#############################################################################
## Compare with Sensitivity/Specificity from GEMS only ##
comb <- read.csv("filepath")

ggplot(data=comb, aes(x=mean_sensitivity, y=combined_sensitivity, col=pathogen)) + geom_point(size=3) + geom_abline(intercept=0, slope=1) + theme_bw() +
  scale_x_continuous("GEMS only", limits=c(0.15,1)) + scale_y_continuous("Combined", limits=c(0.15,1)) + 
  ggtitle("Sensitivity") + geom_text(aes(label=substr(pathogen, 1,4)), col="black") +
  geom_errorbar(aes(ymin=c_sen_lower, ymax=c_sen_upper)) + geom_errorbarh(aes(xmin=g_sen_lower, xmax=g_sen_upper))

ggplot(data=comb, aes(x=mean_specificity, y=combined_specificity, col=pathogen)) + geom_point(size=3) + geom_abline(intercept=0, slope=1) + theme_bw() +
  scale_x_continuous("GEMS only", limits=c(0.7,1)) + scale_y_continuous("Combined", limits=c(0.7,1)) + ggtitle("Specificity") + 
  geom_text(aes(label=substr(pathogen, 1,4)), col="black")
