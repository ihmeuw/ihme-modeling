#####################################################
## The purpose of this code is to calculate a seasonality
# adjustment for the diarrhea survey data. The code can be run
# independently but is currently sourced in the "diarrhea_collapse_in_process.R"
######################################################

#### Import the things you'll need ####
library(survey)
library(ggplot2)
library(boot)
library(plyr)
library(data.table)
library(lme4)
locs <- read.csv("FILEPATH/ihme_loc_metadata_2017.csv")

## Must have a data frame called "diarrhea" ##
# diarrhea <- read.csv(~...)

#######################################
## Do a couple data management things 
#######################################
## Make sure 'had_diarrhea' exists ##
diarrhea$had_diarrhea[is.na(diarrhea$had_diarrhea)] <- 9

## Some areas have age greater than 5 ##
diarrhea <- subset(diarrhea, age_year<=5)

## Fill missing values ##
diarrhea$psu <- as.numeric(diarrhea$psu)
diarrhea <- subset(diarrhea, !is.na(psu))
diarrhea$pweight[is.na(diarrhea$pweight)] <- 1

################################################
## Part 1 ##
## Get monthly prevalence by survey ##
diarrhea_reserve <- diarrhea
diarrhea <- subset(diarrhea, !is.na(int_month))

diarrhea$new_nid <- paste0(diarrhea$nid,"_",diarrhea$ihme_loc_id)
nid <- unique(diarrhea$new_nid)

df.month <- data.frame()
for(n in nid){
  temp <- subset(diarrhea, new_nid==n & had_diarrhea!=9)
  #table(temp$had_diarrhea, temp$survey_month)
  if(length(unique(temp$psu))>1){
    ## Set svydesign
    dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
    month.prev <- svyby(~had_diarrhea, ~int_month, dclus, svymean, na.rm=T)
    month.prev$ihme_loc_id <- unique(temp$ihme_loc_id)
    month.prev$year_id <- max(unique(temp$year_end))
    month.prev$sample_size <- svyby(~had_diarrhea, ~int_month, dclus, unwtd.count, na.rm=T)$count
    month.prev$region_name <- unique(temp$region_name)
    month.prev$super_region_name <- unique(temp$super_region_name)
    month.prev$nid <- unique(temp$nid)
    df.month <- rbind.data.frame(df.month, month.prev)
  }
}

write.csv(df.month, "FILEPATH/month_tabulations_2017.csv")
diarrhea <- diarrhea_reserve

######################################################################################################################
## Part 2 ##
## Develop seasonality model ##
## Zeros and missings cause problems ##
tab <- df.month
tab$mean <- tab$had_diarrhea
tab$month <- tab$int_month
tab$country <- tab$ihme_loc_id
tab$year_id <- as.numeric(tab$year_id)
tab <- subset(tab, !is.na(mean))
tab <- subset(tab, mean!=0)
tab <- subset(tab, mean!=1)

tab$ln_mean <- logit(tab$mean)

tab <- join(tab, locs, by="ihme_loc_id")

## Great, now construct a loop to fit a seasonal curve to each region ##
## Use a single model with a sine/cosine function ##
## Also fit a GAM model ##
## Note that GBD 2017 used the GAM model ##

  pred.df <- data.frame()
  curves <- data.frame()
  regions <- unique(tab$region_name)
  regions <- regions[regions!="East Asia"]
  regions <- regions[!is.na(regions)]
  
  for(r in regions){
    sub <- subset(tab, region_name==r)
    sub$year_id <- as.numeric(sub$year_id)
    sub <- subset(sub, se>0)
    k <- 4 
    pred.ov <- data.frame(month = 1:12, year_id=mean(sub$year_id), country=sub$country[1])
    
  # Some regions only have data from a single country  
    if(length(unique(sub$country))>1){
      mod <- lmer(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + year_id + (1|country), weights=1-(se^2), data=sub)
      pred.ov$pred <- exp(predict(mod, data.frame(pred.ov), re.form=NA))
    #  gmod <- gam(ln_mean ~ s(month, k=k, bs="cc") + year_id + factor(country), data=sub) # weighting is causing problems, can add back in like 'weights=log(sample_size)'
      gmod <- gam(ln_mean ~ s(month, k=k, bs="cc") + year_id + factor(country), weights=1-(se^2), data=sub)
    } else {
      mod <- lm(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + year_id, weights=1-(se^2), data=sub)
      pred.ov$pred <- exp(predict(mod, data.frame(pred.ov)))
      gmod <- gam(ln_mean ~ s(month, k=k, bs="cc") + year_id, weights=1-(se^2), data=sub)
    }
    
    pred.ov$predgam <- exp(predict(gmod, data.frame(pred.ov)))
    pred.ov$mean.pred <- mean(pred.ov$pred)
    pred.ov$meangam <- mean(pred.ov$predgam)
    pred.ov$scalar <- 1-(pred.ov$pred - pred.ov$mean.pred)/pred.ov$pred
    pred.ov$gamscalar <- 1-(pred.ov$predgam - pred.ov$meangam)/pred.ov$predgam
    pred.ov$amplitude <- max(pred.ov$pred) - min(pred.ov$pred)
    pred.ov$peak <- pred.ov$month[pred.ov$pred==max(pred.ov$pred)]
    pred.ov$nadir <- pred.ov$month[pred.ov$pred==min(pred.ov$pred)]
    pred <- join(sub, pred.ov, by="month")
    pred$final_mean <- pred$mean * pred$scalar

    pred.df <- rbind.data.frame(pred.df, pred)
    pred.ov$region_name <- r
    pred.ov$super_region_name <- unique(sub$super_region_name)
    curves <- rbind.data.frame(curves, pred.ov)
  }
# end loop

write.csv(pred.df, paste0("FILEPATH/seasonal_diarrhea_prediction_2017.csv"))
write.csv(curves, "FILEPATH/monthly_scalars_2017.csv")