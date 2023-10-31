library(data.table)
library(scales)
library(ggplot2)

ss <- fread("/ihme/covid-19-2/model-inputs/best/Symptom survey_USA/mask5days_ts.csv")
ss[, mask_use := prop_mask5days_all]
ss <- ss[!is.na(mask_use)]
ss[, source := "Facebook - US"]
ss[, date := as.Date(date, "%d.%m.%Y")]

# Pull in activities responses
act <- fread("/ihme/covid-19-2/model-inputs/best/Symptom survey_USA/mask24hrs_ts.csv")
act[, date := as.Date(date, "%d.%m.%Y")]
act <- act[N_mask24hrs > 0]

ggplot(ss, aes(x=(date), group = date, y=N_mask5days)) + geom_boxplot() + theme_bw() + 
  xlab("") + ylab("Sample size")

min(ss$N_mask5days)
median(ss$N_mask5days)
max(ss$N_mask5days)

dt <- fread("/ihme/covid-19/mask-use-outputs/2020_10_12.01/mask_use.csv")
dt_dat <- fread("/ihme/covid-19/mask-use-outputs/2020_10_12.01/used_data.csv")
dt_dat[, date := as.Date(date)]

scat_dt <- merge(ss, dt_dat, by=c("location_id","date"))

dt_dat[, mask_use := prop_always]


## Don't include Puerto Rico but aggregate facebook
fb_nat <- ss[location_id != 385, lapply(.SD, function(x) sum(x)), by="date", .SDcols = c("mask5days_all","N_mask5days")]
fb_nat[, source := "Facebook"]
fb_nat[, mask_use := mask5days_all / N_mask5days]

us_nat <- rbind(dt_dat[location_id == 102], fb_nat, fill=T)

pdf("/ihme/covid-19/mask-use-outputs/best/new_us_facebook_data.pdf")
  ggplot(scat_dt[location_id != 385], aes(x=prop_always, y=mask_use)) + geom_point() + ggtitle("Data matched by state and date") + 
    xlab("PREMISE") + ylab("Facebook") + theme_bw() + geom_abline(intercept = 0, slope = 1, col="purple")

  ggplot(us_nat[!(source %in% c(NA, "Weighted mean"))], aes(x=as.Date(date), y=mask_use, col=source)) + geom_point() + theme_bw() +
    ggtitle("Always mask use, aggregate USA, by source") + xlab("") + ylab("Mask use") + scale_color_discrete("")
  
  for(l in unique(ss$location_id[ss$location_id!=891])){
    p <- ggplot() + geom_point(data=ss[location_id==l], aes(x=as.Date(date), y=mask_use, col="Facebook", size = N_mask5days), alpha = 0.5) +
      geom_point(data=dt_dat[location_id==l], aes(x=as.Date(date), y=mask_use, col="PREMISE", size=N), alpha = 0.5) + 
      geom_line(data = dt[location_id == l & date < "2020-10-01"], aes(x = as.Date(date), y=mask_use)) + 
      xlab("") + scale_y_continuous("Mask use", labels = percent) + ggtitle(unique(dt_dat[location_id==l, location_name])) + 
      theme_bw() + scale_color_manual("Survey", values = c("Facebook" = "#0033CC", "PREMISE" = "#FF3399")) +
      scale_size_continuous("Sample size")
    print(p)
  }
dev.off()

## Plotting mask use in various activities
act <- fread("/ihme/covid-19/mask-use-outputs/best/facebook_24hrs_mask_activities_usa.csv")

act_long <- melt(act[, c("date","state","mask24hrs_work","mask24hrs_grocery","mask24hrs_restaurant","mask24hrs_otherperson",   
                         "mask24hrs_10pluspeople","mask24hrs_publictransit")],
                 id.vars = c("date","state"))
act_ss <- melt(act[, c("date","state","yes24hrs_work","yes24hrs_grocery","yes24hrs_restaurant","yes24hrs_otherperson",   
                         "yes24hrs_10pluspeople","yes24hrs_publictransit")],
                 id.vars = c("date","state"))
setnames(act_ss, "value", "sample_size")
act_ss[, variable := gsub("yes24hrs","mask24hrs", variable)]

act_long <- merge(act_long, act_ss, by=c("date","state","variable"))

pdf("/ihme/covid-19/mask-use-outputs/best/new_us_facebook_data_mask_activities.pdf")
  for(l in unique(act_long$state)){
    p <- ggplot(act_long[state==l], aes(x=as.Date(date), y=value/sample_size)) + geom_point(aes(size = sample_size, col=variable), pch= 19, alpha=0.7) +
      theme_bw() + ggtitle("Did you wear a mask during the following activities (24hrs)", subtitle = l) + 
      scale_x_date("Date", limits = c(as.Date("2020-08-01"),NA)) + 
      scale_y_continuous("Mask use", limits = c(0,1)) + 
      scale_color_discrete("Activity", labels = c("Work","Grocery","Restaurant","Other people","10 plus people","Public transit")) +
      #stat_smooth(method = "loess", se = F) + 
      scale_size_continuous("") +
      geom_point(data=dt_dat[location_name == l], aes(x=as.Date(date), y=mask_use, size=N), col="black", pch = 17, alpha = 0.7) + 
      geom_point(data=ss[state == l], aes(x=as.Date(date), y=mask_use, col="Facebook", size = N_mask5days), pch = 18, col = "black", alpha = 0.7) +
      scale_shape_manual("Source", values = c(19, 17, 18), labels = c("Activity","Premise","Facebook"))
    print(p)
  }
dev.off()

act_long <- melt(act[, c("date","state","prop_work","prop_grocery","prop_restaurant","prop_otherperson",   
                         "prop_10pluspeople","prop_publictransit")],
                 id.vars = c("date","state"))
act_long <- merge(act_long, act[,c("state","date","N")], by=c("state","date"))

pdf("/ihme/covid-19/mask-use-outputs/best/new_us_facebook_data_all_activities.pdf")
for(l in unique(act_long$state)){
  p <- ggplot(act_long[state==l], aes(x=as.Date(date), y=value)) + geom_point(aes(size = N, col=variable), pch= 19, alpha=0.5) +
    theme_bw() + ggtitle("Did you do the following activities (24hrs)", subtitle = l) + 
    scale_x_date("Date") + 
    scale_y_continuous("Proportion", limits = c(0,1)) + 
    scale_color_discrete("Activity", labels = c("Work","Grocery","Restaurant","Other people","10 plus people","Public transit")) +
    #stat_smooth(method = "loess", se = F) + 
    scale_size_continuous("")
  print(p)
  m <- ggplot(act_long[state==l], aes(x=variable, y=value, col=variable)) + geom_boxplot() + xlab("") + 
    scale_y_continuous("Proportion", limits = c(0,1)) + theme_bw() + theme(axis.text.x = element_text(angle=45, hjust = 1)) +
    guides(col = F) + ggtitle("Did you do the following activities (24hrs)", subtitle = l) +
    scale_x_discrete("Activity", labels = c("Work","Grocery","Restaurant","Other people","10 plus people","Public transit"))
  print(m)
}
dev.off()

## Anticipating being shifted to level of Facebook for PREMISE ##
library(boot)
## Find average relationship 
scat_dt <- scat_dt[location_id != 385]
scat_dt[, xw_ratio := mask_use / prop_always]
scat_dt[, logit_ratio := logit(mask_use) - logit(prop_always)]

country_crosswalk <- scat_dt[, lapply(.SD, function(x) median(x)), by="location_id", .SDcols = c("xw_ratio","logit_ratio")]

ggplot(scat_dt, aes(x=date, y=ratio, col=location_name, group=location_name)) + geom_line() +
  facet_wrap(~location_name) + ylab("Weekly ratio") + guides(col=F) + theme_minimal() +
  geom_hline(yintercept = 1, lty=2)

premise_dt <- dt_dat[source == "Premise"]
premise_dt <- merge(premise_dt, country_crosswalk, by="location_id")
logit_premise <- copy(premise_dt)

premise_dt[, prop_always := prop_always * xw_ratio]
logit_premise[, logit_prop_always := logit(prop_always)]
logit_premise[, prop_always := inv.logit(logit(prop_always) + logit_ratio)]
premise_dt[, source := "Adjusted Premise"]
logit_premise[, source := "Logit adjusted Premise"]

ss[, prop_always := mask_use]
ss[, location_name := state]
final_dt <- rbind(dt_dat[source == "Premise"], premise_dt, logit_premise, ss, fill = T)

ggplot(final_dt, aes(x=as.Date(date), y=prop_always, col=source)) + geom_point() + 
  facet_wrap(~location_name) + theme_bw() + xlab("") + ylab("Mask use")


