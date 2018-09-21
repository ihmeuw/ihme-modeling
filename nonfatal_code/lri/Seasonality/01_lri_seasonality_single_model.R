###################################
## Seasonality model accounting for 
## case specificity and year in the 
## season model ##
###################################

#### Import the things you'll need ####
library(survey)
library(ggplot2)
library(boot)
library(plyr)
library(data.table)
library(lme4)

load("FILEPATH/LRI/2017_03_06.Rda")
lri <- combined_data

## Add some missing values ##
lri$child_sex <- lri$sex_id
lri$child_sex[is.na(lri$child_sex)] <- 3
lri$age_yr <- floor(lri$age_year)
## Drop ages greater than 5
lri <- subset(lri, age_yr<=5)

lri$nid <- lri$nid_str
locs <- read.csv("FILEPATH/ihme_loc_metadata.csv")
lri <- join(lri, locs, by="ihme_loc_id")

lri$pweight[is.na(lri$pweight)] <- 1

###############################################
# ## Part 1 ##
# lri$cv_diag_valid_good <- ifelse(!is.na(lri$chest_symptoms),1,0)
# lri$cv_diag_valid_poor <- ifelse(lri$cv_diag_valid_good==0,1,0)

## Surveys must have difficulty breathing! ##
# Change the NA values to 9
lri$chest_symptoms[is.na(lri$chest_symptoms)] <- 9
lri$had_fever[is.na(lri$had_fever)] <- 9
lri$diff_breathing[is.na(lri$diff_breathing)] <- 9
lri$had_cough[is.na(lri$had_cough)] <- 9

lri$tabulate <- ave(lri$diff_breathing, lri$nid, FUN= function(x) min(x))
lri <- subset(lri, tabulate!=9)

## For seasonality, do not adjust data for specificity but do include fever (malaria)? ##
lri$any_lri <- ifelse(lri$chest_symptoms==1,1,ifelse(lri$diff_breathing==1 & lri$chest_symptoms!=0,1,0))
################################################
## Part 2 ##
## Get monthly prevalence by survey ##
nid <- unique(lri$nid)
df.month <- data.frame()
for(n in nid){
  temp <- subset(lri, nid==n)
  if(length(unique(temp$psu))>1){
    if(!is.na(max(temp$psu))){
      if(!is.na(max(temp$int_month))){
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
        month.prev$nid <- unique(temp$nid)
        colnames(month.prev)[1] <- "survey_month"
        df.month <- rbind.data.frame(df.month, month.prev)
      }
    }
  }
}

write.csv(df.month, "FILEPATH/df_month.csv")
##################################################
## Part 3 ##
## Develop seasonality model ##
## Zeros and missings cause problems ##
tab <- df.month
tab$mean <- tab$any_lri
tab$month <- tab$survey_month
tab$country <- tab$ihme_loc_id
tab <- subset(tab, !is.na(mean))
tab <- subset(tab, mean!=0)
tab <- subset(tab, mean!=1)

tab$ln_mean <- log(tab$mean)

## Kosovo is not matched ##
tab$region_name[tab$ihme_loc_id=="KOS"] <- "Eastern Europe"

#################################
## Part 4, determine scalar 
#################################

## Great, now construct a loop to fit a seasonal curve to each region ##
## Use a single model with a sin/cosin function ##
pdf("FILEPATH/seasonal_fit_plots_model-adj_year-period2.pdf")
pred.df <- data.frame()
curves <- data.frame()
for(r in unique(tab$region_name)){
  sub <- subset(tab, region_name==r)
  sub$year_id <- as.numeric(sub$year_id)
  sub <- subset(sub, se>0)
  pred.ov <- data.frame(month = 1:12, cv_diag_valid_good=mean(sub$cv_diag_valid_good), year_id=mean(sub$year_id))
  if(length(unique(sub$country))>1){
    mod <- lmer(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + cv_diag_valid_good + year_id + (1|country), weights=se, data=sub)
    pred.ov$pred <- exp(predict(mod, data.frame(pred.ov), re.form=NA))
  } else {
    mod <- lm(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + cv_diag_valid_good + year_id, weights=1/se^2, data=sub)
    pred.ov$pred <- exp(predict(mod, data.frame(pred.ov)))
  }
  pred.ov$mean.pred <- mean(pred.ov$pred)
  pred.ov$scalar <- 1-(pred.ov$pred - pred.ov$mean.pred)/pred.ov$pred
  pred.ov$amplitude <- max(pred.ov$pred) - min(pred.ov$pred)
  pred.ov$peak <- pred.ov$month[pred.ov$pred==max(pred.ov$pred)]
  pred.ov$nadir <- pred.ov$month[pred.ov$pred==min(pred.ov$pred)]
  pred <- join(sub, pred.ov, by="month")
  pred$final_mean <- pred$mean * pred$scalar
  f <- ggplot(data=pred, aes(x=month, y=mean, col=country)) + geom_point(aes(size=1/se^2)) + scale_size(range=c(1,4)) + guides(size=F) + 
    geom_line(data=pred.ov, aes(x=month, y= pred), col="black", lwd=1.25) + geom_hline(yintercept=pred$mean.pred, lty=2) + theme_bw() +
    ylab("Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Country") +
    ggtitle(paste0(r, "\nSeasonality from Case Definition- and Year-Adjusted Input Data"))
  print(f)
  g <- ggplot(data=pred, aes(x=month, col=country)) + geom_point(aes(y=mean, size=1/se^2), pch=1) + 
    geom_point(aes(y=final_mean, size=1/se^2)) + guides(size=F) + 
    geom_line(data=pred.ov, aes(x=month, y=pred), col="black",lwd=1.25) + geom_hline(yintercept=pred$mean.pred, lty=2) + scale_color_discrete("Country") +
    theme_bw() + ggtitle(paste0(r,"\nOpen circles are original prev, closed are seasonally-adjusted")) + ylab("Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12)
  print(g)
  pred.df <- rbind.data.frame(pred.df, pred)
  pred.ov$region_name <- r
  pred.ov$super_region_name <- unique(sub$super_region_name)
  curves <- rbind.data.frame(curves, pred.ov)
}
sr <- unique(curves$super_region_name)
sr <- sr[!is.na(sr)]
for(s in sr){
  o <- ggplot(data=subset(curves, super_region_name==s), aes(x=month, y=pred, col=region_name)) + geom_line(lwd=1.25) + theme_bw() + 
    ylab("Modeled Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Region Name") +
    ggtitle(paste0("Comparative Seasonality by Super-Region\n",s))
  print(o)
}
dev.off()
# end loop

write.csv(curves, "FILEPATH/month_region_scalar.csv")
write.csv(pred.df, "FILEPATH/seasonal_lri_prediction.csv")
