#####################################################
## The purpose of this code is to calculate a seasonality
# adjustment for the LRI survey data.
######################################################

#Read in dataset, rename columns
library(mgcv)
library(plyr)
library(survey)
library(ggplot2)
library(boot)
library(plyr)
library(data.table)
library(lme4)

################################################
## Part 1 ##
## Get monthly prevalence by survey ##
lri$nid_new <- ifelse(is.na(lri$nid_str), paste0(lri$ihme_loc_id,"_",lri$year), lri$nid)
nid <- unique(lri$nid_new)
nid <- nid[!is.na(nid)]
nid <- nid[nid!=""]
df.month <- data.frame()
num <- 1
for(n in nid){
  print(paste0("Collapsing survey ", num, " of ", length(nid)))
  temp <- subset(lri, nid_new==n)
   if(length(unique(temp$psu))>1){
     if(!is.na(max(temp$psu))){
       if(max(temp$int_month, na.rm=T)!="-Inf"){
        cv_diag_valid_good <- ifelse(min(temp$chest_symptoms, na.rm=T)==0,1,0)
        ## Set svydesign
        dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
        month.prev <- svyby(~any_lri, ~int_month, dclus, svymean, na.rm=T)
        month.prev$ihme_loc_id <- unique(temp$ihme_loc_id)
        month.prev$year_id <- max(temp$end_year, na.rm=T)
        month.prev$cv_diag_valid_good <- cv_diag_valid_good
        month.prev$sample_size <- svyby(~any_lri, ~ int_month, dclus, unwtd.count, na.rm=T)$count
        month.prev$region_name <- unique(temp$region_name)
        month.prev$super_region_name <- unique(temp$super_region_name)
        month.prev$nid <- unique(temp$nid_new)
        colnames(month.prev)[1] <- "survey_month"
        df.month <- rbind.data.frame(df.month, month.prev)
       }
     }
   }
  num <- num + 1
}

write.csv(df.month, "FILEPATH")
