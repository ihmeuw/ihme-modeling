### Produce estimates of sensitivity and specificity of
### laboratory diagnostics to new qPCR case definition among
### cases only. 
## Note that internal IHME filepaths have been replaced with FILEPATH ##
## Thanks for reading! ##

library(dplyr)
library(matrixStats)
library(epiR)

## Using bimodal accuracy (affects adenovirus)
accuracy <- read.csv("FILEPATH/min_loess_accuracy.csv")
gems <- read.csv("FILEPATH/gems_pathogen_modified.csv")

pathogens <- c("tac_adenovirus","tac_aeromonas","tac_campylobacter","tac_cryptosporidium",
               "tac_entamoeba", 
               "tac_TEPEC", "tac_norovirus", "tac_rotavirus","tac_salmonella", "tac_shigella_eiec","tac_ETEC","tac_v_cholerae")
names.gems <- c("F18_RES_ADENO4041", "F16_AEROMONAS", "F16_CAMPY_JEJUNI_COLI", "F18_RES_CRYPTOSPOR",
                "F18_RES_ENTAMOEBA", 
                "F17_tEPEC", "F19_NOROVIRUS", "F18_RES_ROTAVIRUS", "F16_SALM_NONTYPHI", "F16_SHIG_SPP", "F17_ETEC", "F16_VIB_CHOLERAE")


sub.gems <- subset(gems, !is.na(case.control))
sub.gems <- sub.gems[,c(pathogens,"case.control","age","site.names",names.gems)]


sens <- data.frame(pathogen=substr(pathogens,5,20), measure="sensitivity")
spec <- data.frame(pathogen=substr(pathogens,5,20), measure="specificity")

for(i in 1:1000){
##### Cases only?! #####
## several options to define sample:  
  cases <- sample_n(subset(sub.gems, case.control==1), 5304, replace=T)
  new.boot <- cases
  df.out <- data.frame()
  for(j in 1:12){
    tac <- pathogens[j]
    lab <- names.gems[j]
    positive <- ifelse(new.boot[,tac]<accuracy[j,"ct.inflection"],1,0)
    r <- table(new.boot[,lab], positive)
    sensitivity <- r[2,2]/(r[1,2]+r[2,2])
    specificity <- r[1,1]/(r[1,1]+r[2,1])
    sens[j,2+i] <- sensitivity
    colnames(sens)[2+i] <- paste0("draw_",i)
    spec[j,2+i] <- specificity
    colnames(spec)[2+i] <- paste0("draw_",i)
  }
}
### This one is better because it doesn't require a split in STATA do file
adjustment <- rbind.data.frame(sens, spec)

## Maximum accuracy
write.csv(adjustment, "FILEPATH/loess_sens_spec_adjustment.csv")

####### END Main program ########

