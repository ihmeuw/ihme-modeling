## Take Diarrhea Survey Data, account for seasonality ##
############### Steps ##################
# 1). Get summary prev by month
# 2). Develop simple region seasonal model
# 3). Determine scalar for month and geography
# 4). Apply scalar to raw data
# 5). Tabulate month-adjusted prevalence
## Internal IHME filepaths have been replaced with FILEPATH ##
## Thanks for reading! ##

#### Import the things you'll need ####
library(survey)
library(ggplot2)
library(boot)
library(plyr)
library(data.table)
library(lme4)

## Append all files that were extracted ##
filenames <- list.files(path="FILEPATH/Diarrhea/2017_02_28/", full.names=T)
keep.names <- c("ihme_loc_id","year_start","psu","nid.x","survey_name","year_end","strata","pweight","sex_id","age_year","age_month","int_month","urban",
                "had_diarrhea","had_diarrhea_recall_period_weeks","svy_area1","location_name")
d.input <- data.frame()
for(f in filenames){
  df <- data.frame(fread(f))
  df <- df[,keep.names]
  df <- subset(df, age_year <= 4)
  df$had_diarrhea[is.na(df$had_diarrhea)] <- 9
  df$tabulate <- ave(df$had_diarrhea, df$nid.x, FUN= function(x) min(x))
  df <- subset(df, tabulate!=9)
  d.input <- rbind.fill(d.input, df)
}

locs <- read.csv("FILEPATH/ihme_loc_metadata_2016.csv")[,c("location_name","location_id","ihme_loc_id","region_name","super_region_name")]
diarrhea <- data.frame(d.input)[,keep.names]
diarrhea <- join(diarrhea, locs, by="ihme_loc_id")

## Make sure 'had_diarrhea' exists ##
diarrhea$had_diarrhea[is.na(diarrhea$had_diarrhea)] <- 9
diarrhea$nid <- diarrhea$nid.x
diarrhea$tabulate <- ave(diarrhea$had_diarrhea, diarrhea$nid, FUN= function(x) min(x))
diarrhea <- subset(diarrhea, tabulate!=9)

## Some areas have age greater than 5 ##
diarrhea <- subset(diarrhea, age_year<=4)

## Fill missing values ##
full <- diarrhea

diarrhea$psu <- as.numeric(diarrhea$psu)
diarrhea <- subset(diarrhea, !is.na(psu))
diarrhea$pweight[is.na(diarrhea$pweight)] <- 1


################################################
## Part 2 ##
## Get monthly prevalence by survey ##
# month not always matched, this is a temporary solution

diarrhea$survey_month <- diarrhea$int_month

diarrhea <- subset(diarrhea, !is.na(int_month))

nid <- unique(diarrhea$nid)

df.month <- data.frame()
for(n in nid){
  temp <- subset(diarrhea, nid.x==n & had_diarrhea!=9)
  #table(temp$had_diarrhea, temp$survey_month)
  if(length(unique(temp$psu))>1){
    ## Set svydesign
    dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
    month.prev <- svyby(~had_diarrhea, ~survey_month, dclus, svymean, na.rm=T)
    month.prev$ihme_loc_id <- unique(temp$ihme_loc_id)
    month.prev$year_id <- max(unique(temp$year_end))
    month.prev$sample_size <- svyby(~had_diarrhea, ~survey_month, dclus, unwtd.count, na.rm=T)$count
    month.prev$region_name <- unique(temp$region_name)
    month.prev$super_region_name <- unique(temp$super_region_name)
    month.prev$nid <- unique(temp$nid)
    df.month <- rbind.data.frame(df.month, month.prev)
  }
}

write.csv(df.month, "FILEPATH/month_tabulations.csv")
##################################################
## Part 3 ##
## Develop seasonality model ##
## Zeros and missings cause problems ##
tab <- df.month
tab$mean <- tab$had_diarrhea
tab$month <- tab$survey_month
tab$country <- tab$ihme_loc_id
tab$year_id <- as.numeric(tab$year_id)
tab <- subset(tab, !is.na(mean))
tab <- subset(tab, mean!=0)
tab <- subset(tab, mean!=1)

tab$ln_mean <- logit(tab$mean)

## Great, now construct a loop to fit a seasonal curve to each region ##
## Note that presently I am including year_id in the model itself ##
pdf(paste0("FILEPATH/seasonal_fit_plots_",date,".pdf"))
pred.df <- data.frame()
curves <- data.frame()
for(r in  unique(tab$region_name[!is.na(tab$region_name)])){
  sub <- subset(tab, region_name==r)
  pred.ov <- data.frame(month = 1:12, year_id=2015)
  if(length(unique(sub$ihme_loc_id))>1){
    mod <- lmer(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + year_id + (1|ihme_loc_id), weights=sample_size, data=sub)
    pred.ov$pred <- inv.logit(predict(mod, data.frame(pred.ov), re.form=NA))
  } else {
    mod <- lm(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + year_id, weights=sample_size, data=sub)
    pred.ov$pred <- inv.logit(predict(mod, data.frame(pred.ov)))
  }
  pred.ov$mean.pred <- mean(pred.ov$pred)
  pred.ov$scalar <- 1-(pred.ov$pred - pred.ov$mean.pred)/pred.ov$pred
  pred.ov$amplitude <- max(pred.ov$pred) - min(pred.ov$pred)
  pred.ov$peak <- pred.ov$month[pred.ov$pred==max(pred.ov$pred)]
  pred.ov$nadir <- pred.ov$month[pred.ov$pred==min(pred.ov$pred)]
  pred <- join(sub, pred.ov, by="month")
  pred <- subset(pred, month < 13)
  pred$final_mean <- pred$mean * pred$scalar
  f <- ggplot(data=pred, aes(x=month, y=mean, col=country)) + geom_point(aes(size=sample_size)) + scale_size(range=c(1,4)) + guides(size=F) + 
    geom_line(data=pred.ov, aes(x=month, y= pred), col="black", lwd=1.25) + geom_hline(yintercept=pred$mean.pred, lty=2) + theme_bw() +
    ylab("Model Input Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Country") +
    ggtitle(paste0(r, "\nDiarrhea Seasonality"))
  print(f)
  rm <- melt(pred[,c("month","country","mean","final_mean")], id.vars=c("month", "country"))

  rm$shift <- ifelse(rm$variable=="mean",rm$month-0.1,rm$month+0.1)
  g <- ggplot() + geom_point(data=rm, aes(x=shift, y=value, col=country, shape=variable), size=3) + scale_x_continuous("Month", breaks=1:12, labels=1:12) +
    geom_line(data=pred.ov, aes(x=month, y=pred), col="black",lwd=1.25) + geom_hline(yintercept=pred$mean.pred, lty=2) + scale_color_discrete("Country") +
    theme_bw() + ylab("Prevalence") + scale_shape_manual("", labels=c("Unadjusted","Adjusted for\nseasonality"), values=c(16,18)) + ggtitle(paste0(r, "\nDiarrhea prevalence adjusted for seasonality"))
  print(g)

  pred.df <- rbind.data.frame(pred.df, pred)
  pred.ov$region_name <- r
  pred.ov$super_region_name <- unique(sub$super_region_name)
  curves <- rbind.data.frame(curves, pred.ov)
}
for(s in unique(curves$super_region_name)){
  o <- ggplot(data=subset(curves, super_region_name==s), aes(x=month, y=pred, col=region_name)) + geom_line(lwd=1.25) + theme_bw() + 
    ylab("Modeled Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Region Name") +
    ggtitle(s)
  print(o)
  o <- ggplot(data=subset(curves, super_region_name==s), aes(x=month, y=1/scalar, col=region_name)) + 
    geom_hline(yintercept=1, col="black", lty=2) + geom_line(lwd=1.25) + theme_bw() + 
    ylab("Modeled Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Region Name") +
    ggtitle(s)  
  print(o)
}
dev.off()
# end loop

write.csv(pred.df, paste0("FILEPATH/seasonal_diarrhea_prediction_",date,".csv"))
write.csv(curves, "FILEPATH/monthly_scalars.csv")
########################################################

########################################################

########################################################
## Part 4. We have a month/country scalar, now apply to raw data 
########################################################
diarrhea <- full
pred.df <- read.csv("FILEPATH/seasonal_diarrhea_prediction_1_31_2017.csv")

scalar <- curves[,c("scalar","month","region_name")]
scalar$survey_month <- scalar$month
diarrhea$survey_month <- diarrhea$int_month
diarrhea <- join(diarrhea, scalar, by=c("region_name","survey_month"))

## Don't forget subnationals in Kenya, S Africa, India
##############################################################
## Kenya is a problem, subset out, will deal with later
# Create dummy nid for each subnat
diarrhea$urban_name <- ifelse(diarrhea$urban==1,"Urban","Rural")
subnats <- diarrhea[diarrhea$ihme_loc_id %in% c("ZAF","BRA","KEN","MEX","IDN","IND"),]
subnat.f <- subset(subnats, is.na(admin_1))
subnats <- subset(subnats, !is.na(admin_1))

subnats$nid.new <- paste0(subnats$admin_1,"_",subnats$nid)
subnats$subname <- ifelse(subnats$ihme_loc_id=="IND", paste0(subnats$admin_1,", ",subnats$urban_name), subnats$admin_1)

national <- diarrhea[!(diarrhea$ihme_loc_id %in% c("ZAF","BRA","KEN","MEX","IDN","IND")),]
national <- rbind(national, subnat.f)
national$nid.new <- national$nid
national$subname <- "none"

diarrhea <- rbind(national, subnats)

nid.new <- unique(diarrhea$nid.new)

## Round ages
diarrhea$age_year <- floor(diarrhea$age_year)
## Set missing sex to both
diarrhea$sex_id[is.na(diarrhea$sex_id)] <- 3
## prep diarrhea column (missing is set to 9) ##
diarrhea$had_diarrhea[diarrhea$had_diarrhea==9] <- NA
diarrhea$pweight[is.na(diarrhea$pweight)] <- 1

############################################################################################
## Yay! Ready for loop ... or are we?! ##
############################################################################################

df.final <- data.frame()
for(n in nid.new){
  temp <- subset(diarrhea, nid.new==n)
# Drop if had_diarrhea is missing #
  temp <- subset(temp, !is.na(had_diarrhea))

# It is possible some surveys will be empty so skip those #    
  if(nrow(temp)>0){
    
# If all pweights are 1 (i.e. missing), set psu to 1:nrow #
  row.count <- 1:nrow(temp)
  if(min(temp$pweight)==1){
      temp$psu <- row.count
  }
  
# If all psu are missing, set psu to 1:nrow and pweight to 1 #
  if(is.na(min(temp$psu))){
    temp$psu <- row.count
    temp$pweight <- 1
  }

# If all psu are the same, set psu to 1:nrow and pweight to 1 #
  if(length(unique(temp$psu))==1){
    temp$psu <- row.count
    temp$pweight <- 1
  }
  
    temp$recall_period <- ifelse(is.na(temp$had_diarrhea_recall_period_weeks),2,temp$had_diarrhea_recall_period_weeks)
    recall_period <- max(temp$recall_period, na.rm=T)
    
    loop.dummy <- max(temp$scalar)

# Does survey month exist? #
    if(is.na(loop.dummy)){
      temp$scalar_diarrhea <- temp$had_diarrhea
    } else {
      temp$scalar_diarrhea <- temp$had_diarrhea * temp$scalar
    }
    
# Set svydesign #
    dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
    
# Get survey prevalence #
    prev <- svyby(~scalar_diarrhea, ~sex_id + age_year, dclus, svymean, na.rm=T)
    prev$sample_size <- svyby(~scalar_diarrhea, ~sex_id + age_year, dclus, unwtd.count, na.rm=T)$count
    prev$base_mean <- svyby(~had_diarrhea, ~sex_id + age_year, dclus, svymean, na.rm=T)$had_diarrhea
    prev$ihme_loc_id <- unique(temp$ihme_loc_id)
    prev$location <- unique(temp$subname)
    prev$year_start <- min(temp$year_start)
    prev$year_end <- max(temp$year_end)
    prev$nid <- unique(temp$nid)
    prev$sex <- ifelse(prev$sex_id==2,"Female","Male")
    prev$age_start <- prev$age_year
    prev$age_end <- prev$age_year + 1
    prev$recall_period <- recall_period
    prev$survey <- unique(temp$survey_name)
    prev$note_modeler <- paste0("Diarrhea prevalence adjusted for seasonality. The original value was ", round(prev$base_mean,4)," See C:/Users/USER/Documents/Programming/GBD_2016/diarrhea_seasonality_adj.R for detail.")
    df.final <- rbind.data.frame(df.final, prev)
  }
}

ggplot(data=df.final, aes(x=base_mean, y=scalar_diarrhea)) + geom_point() + geom_abline(intercept=0, slope=1, col="red")

## Export for review ##
write.csv(df.final, "FILEPATH/diarrhea_seasonal_subs_v3.csv")

