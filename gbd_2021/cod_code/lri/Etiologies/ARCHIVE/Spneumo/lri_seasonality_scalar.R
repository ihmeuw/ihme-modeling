#######################################################################
## The purpose of this code is to determine a monthly scalar
## for LRI symptoms from the surveys extracted via UbCov. 
## This file must be run after "lri_month_tabulation.R" and
## can be sourced in the "lri_collapse_survey_data.R" script. 
#######################################################################
library(lme4)
library(plyr)
library(ggplot2)
## LRI Seasonality adjustment ##
tab <- read.csv("filepath")

tab$mean <- tab$any_lri
tab$month <- tab$survey_month
tab$country <- substr(tab$ihme_loc_id,1,3)
tab <- subset(tab, !is.na(mean))
tab <- subset(tab, mean!=0)
tab <- subset(tab, mean!=1)

## SE is causing a problem where extremely small
tab$se <- ifelse(tab$se < 1e-6, 1e-6, tab$se)

tab$ln_mean <- log(tab$mean)

## Kosovo is not matched ##
tab$region_name[tab$ihme_loc_id=="KOS"] <- "Eastern Europe"

#################################
## Determine scalar 
#################################
## Creates a loop for each GBD region, finds a curve by month
## and prints the output in a PDF for review 
## Use a single model with a sine/cosine function ##
## Also fit a GAM model ##

pdf("filepath")
    pred.df <- data.frame()
    curves <- data.frame()
    regions <- unique(tab$region_name)
    regions <- regions[regions!="Oceania"]
    regions <- regions[!is.na(regions)]
    
    for(r in regions){
      sub <- subset(tab, region_name==r)
      sub$year_id <- as.numeric(sub$year_id)
      sub <- subset(sub, se>0)
      k <- 4 
      pred.ov <- data.frame(month = 1:12, year_id=floor(mean(sub$year_id)), country=sub$country[1], cv_diag_valid_good=1)
      if(length(unique(sub$country))>1){
        mod <- lmer(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + year_id + (1|country), weights=1-(se^2), data=sub)
        pred.ov$pred <- exp(predict(mod, data.frame(pred.ov), re.form=NA))
        gmod <- gam(ln_mean ~ s(month, k=k, bs="cc") + cv_diag_valid_good + year_id + factor(country), weights=1-(se^2), data=sub)
        
      } else {
        mod <- lm(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + cv_diag_valid_good + year_id, weights=1-(se^2), data=sub)
        pred.ov$pred <- exp(predict(mod, data.frame(pred.ov)))
        gmod <- gam(ln_mean ~ s(month, k=k, bs="cc") + cv_diag_valid_good + year_id, weights=1-(se^2), data=sub)
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
      
      mplot <- melt(pred.ov[,c("predgam","pred","month")], id.vars="month")
      
      f <- ggplot(data=pred, aes(x=month, y=mean)) + geom_point(aes(size=sample_size, col=country)) + scale_size(range=c(1,4)) + guides(size=F) + 
        geom_line(data=mplot, aes(x=month, y=value, lty=variable), lwd=1.25) + geom_hline(yintercept=pred$mean.pred, lty=2) + geom_hline(yintercept=pred$meangam) + theme_bw() +
        ylab("Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Country") +
        ggtitle(paste0(r, "\nSeasonality from Case Definition- and Year-Adjusted Input Data"))
      print(f)
    
      pred.df <- rbind.data.frame(pred.df, pred)
      pred.ov$region_name <- r
      pred.ov$super_region_name <- unique(sub$super_region_name)
      curves <- rbind.data.frame(curves, pred.ov)
    }
    
    sr <- unique(curves$super_region_name)
    sr <- sr[!is.na(sr)]
    
    for(s in sr){
      sub <- subset(curves, super_region_name==s)
      mo <- melt(sub[,c("region_name","scalar","gamscalar","month")], id.vars=c("region_name","month"))
      o <- ggplot(data=mo, aes(x=month,y=value,col=region_name, lty=variable)) + geom_line(lwd=1.25) + theme_bw() + 
        ylab("Modeled Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Region Name") +
        ggtitle(paste0("Comparative Seasonality by Super-Region\n",s))
      print(o)
    }

dev.off()
# end loop

write.csv(curves, "filepath")
write.csv(pred.df, "filepath")
