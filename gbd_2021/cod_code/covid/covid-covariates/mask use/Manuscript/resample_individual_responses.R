##############################################################
## Do some analysis of dropping individuals randomly to assess
## the amount of change in the mask use estimate. 
##############################################################
library(data.table)
library(dplyr)
library(ggplot2)
set.seed(2119)
## install ihme.covid (always use newest version)
tmpinstall <- system("mktemp -d --tmpdir=/tmp", intern = TRUE)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T, ref = "get_latest_output_dir")
##

dt <- fread("FILEPATH/summary_individual_responses_global.gz")
dt <- dt[!is.na(location_id)]
full_dt <- copy(dt)

# Need a few locations for examples
locs <- c("Chile","Mozambique","Thailand","Lebanon","Mexico","Japan","United Kingdom","France")
dt <- dt[location_name %in% locs]
dt[, N := mask_7days_all + mask_7days_most + mask_7days_half + 
     mask_7days_some + mask_7days_none]
dt <- dt[N == 1]
dt[, worried_all := ifelse(worried == "All", 1, 0)]

# Going to need a function to resample by location, keep X number
resample_respondents <- function(dt, number){
  print(paste0("Resampling ", number))
  fun_out <- data.table()
  for(l in unique(dt$location_id)){
    for(d in unique(dt[location_id == l, date])){
      if(number > nrow(dt[location_id==l & date == d])){
        num_sample <- nrow(dt[location_id==l & date == d])
      } else {
        num_sample <- number
      }
      tmp <- sample_n(dt[location_id == l & date == d], num_sample, replace = FALSE)
      fun_out <- rbind(fun_out, tmp)
    }
  }
  fun_out <- fun_out[, lapply(.SD, function(x) sum(x)), by=c("location_id","location_name","date"), .SDcols = c("N", "mask_7days_all","worried_all","yes_24hrs_work")]
  return(fun_out)
}

all <- dt[, lapply(.SD, function(x) sum(x)), by=c("location_id","location_name","date"), .SDcols = c("N", "mask_7days_all","worried_all","yes_24hrs_work")]
all[, daily_count := "All"]

resamps <- data.table()
for(n in c(25, 50, 100, 200, 300, 400, 500, 1000)){
  mk <- resample_respondents(dt, number = n)
  mk$daily_count <- n
  resamps <- rbind(resamps, mk)
}
backup_samps <- copy(resamps)

resamps <- rbind(resamps, all)
resamps[, mask_use := mask_7days_all / N]
resamps[, worried := worried_all / N]
resamps[, work24hrs := yes_24hrs_work / N]
# Has to be greater than 0
#resamps[, mask_use := ifelse(mask_use == 0, 0.001, mask_use)]
resamps <- resamps[order(location_id, daily_count, date)]
resamps[, daily_count := factor(daily_count, levels = c("All","25","50","100","200","300","400","500","1000"))]

remask <- resamps[mask_use > 0 & mask_use < 1]
reworried <- resamps[worried > 0]
rework <- resamps[work24hrs > 0]

remask[, smooth_prop_always := ihme.covid::barber_smooth(mask_use, n_neighbors = 5, times = 10), by = c("location_id", "daily_count")]
reworried[, smooth_worried := ihme.covid::barber_smooth(worried, n_neighbors = 5, times = 10), by = c("location_id", "daily_count")]
rework[, smooth_work := ihme.covid::barber_smooth(work24hrs, n_neighbors = 5, times = 10), by = c("location_id", "daily_count")]

all_data <- remask[daily_count == "All"]
all_data[, fit := smooth_prop_always]

remask <- merge(remask, all_data[,c("fit","date","location_id")], by=c("date","location_id"))
remask[, error := smooth_prop_always - fit]
hist(remask$error)

pdf("FILEPATH/resample_size_facebook.pdf", height=7, width=9)
loc <- 101
for(loc in unique(resamps$location_id)){
  p1 <- ggplot(remask[location_id == loc & daily_count != "All"], aes(x=as.Date(date), col = daily_count)) + 
    # geom_point(data=remask[location_id == loc & daily_count == "All"], aes(y=mask_use, size = N), col="black", pch=1) + 
    # geom_point(aes(y=mask_use), alpha=0.3) + 
    geom_line(aes(y=smooth_prop_always), lwd=1.25) + 
    geom_line(data=remask[location_id == loc & daily_count == "All"], aes(y=smooth_prop_always), col="black", lwd=1.25) + 
    ggtitle(paste0(unique(remask[location_id == loc, location_name]),": Respondents always wearing a mask"), 
            subtitle = "Black line is fit with all available respondents") + 
    scale_color_discrete("Resampled size") + ylab("Mask use (%)") + xlab("") + 
    theme_bw()
  print(p1)
}  
ggplot(remask, aes(x=daily_count, y=error, col=location_name)) + geom_boxplot() + theme_bw() +
  facet_wrap(~location_name) + ylab("Model residual error") + xlab("Resample size") + guides(col = F) + 
  theme(axis.text.x = element_text(angle = 90, hjust=1))

for(loc in unique(resamps$location_id)){
  p1 <- ggplot(reworried[location_id == loc & daily_count != "All"], aes(x=as.Date(date), col = daily_count)) + 
    # geom_point(data=reworried[location_id == loc & daily_count == "All"], aes(y=worried, size = N), col="black", pch=1) + 
    # geom_point(aes(y=worried), alpha=0.3) + 
    geom_line(aes(y=smooth_worried), lwd=1.25) + 
    geom_line(data=reworried[location_id == loc & daily_count == "All"], aes(y=smooth_worried), col="black", lwd=1.25) + 
    ggtitle(paste0(unique(reworried[location_id == loc, location_name]),": Respondents that are very worried about COVID-19"), 
            subtitle = "Black line is fit with all available respondents") + 
    scale_color_discrete("Resampled size") + ylab("Very worried about COVID-19 (%)") + xlab("") + 
    theme_bw()
  print(p1)
  
  # p1 <- ggplot(rework[location_id == loc & daily_count != "All"], aes(x=as.Date(date), col = daily_count)) + 
  #   # geom_point(data=rework[location_id == loc & daily_count == "All"], aes(y=work24hrs, size = N), col="black", pch=1) + 
  #   # geom_point(aes(y=work24hrs), alpha=0.3) + 
  #   geom_line(aes(y=smooth_work), lwd=1.25) + 
  #   geom_line(data=rework[location_id == loc & daily_count == "All"], aes(y=smooth_work), col="black", lwd=1.25) + 
  #   ggtitle(unique(rework[location_id == loc, location_name]), subtitle = "Worked outside home 24 hours") + 
  #   scale_color_discrete("Resampled size") + ylab("Worked outside home") + xlab("") + 
  #   theme_bw()
  # print(p1)
}
dev.off()

#######################################################################
## Map average sample sizes:
lsvid <- 771
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/generic_map_function_national.R")
mdt <- fread("FILEPATH/used_data.csv")
premise <- mdt[source=="Premise"]
usm <- fread("FILEPATH/symptoms_ts.csv")
usm[, N := symptoms_24hrs_yes + symptoms_24hrs_no]
usm[, date := as.Date(date, "%d.%m.%Y")]
usm <- usm[date > "2020-08-01"]
usm[, location_name := state]

mdt <- mdt[date > "2020-08-01" & source %in% c("Facebook")]
#mdt[, N := ifelse(source == "Premise", N/7, N)]

sdt <- rbind(mdt[, lapply(.SD, function(x) mean(x, na.rm=T)), by=c("location_name","location_id"), .SDcols = "N"],
             usm[, lapply(.SD, function(x) mean(x, na.rm=T)), by=c("location_name","location_id"), .SDcols = "N"])


sdt[,bin := cut(N, breaks = c(0,50,100,200,350,450,1000,100000),
                labels = c("0-49","50-99","100-199","200-349","350-449","450-999",">1000"))]
loc_id <- 1
#sdt <- rbind(sdt, data.table(location_id = 570, location_name = "Washington", N = NA, bin = NA))
sdt[, date := "2020-09-11"]
brief_cols <- c("darkgray", colorRampPalette(rev(c("#F74C00",  "#FEA02F",  "#EBD9C8","#007A7A",  "#002043")))(7))

generic_map_function(sdt, title="Average daily sample size since August 1\nFacebook symptoms surveys", colors = brief_cols)

## Data Directory
data_dir_m <- "FILEPATH"
data_dir_m <- "FILEPATH"
df <- file.info(list.files(data_dir_m, full.names = T, pattern="2020."))
data_dir <- rownames(df)[which.max(df$mtime)]

us.impact.file<-list.files(data_dir, pattern="us_covid")
#### PREMISE DATA SETS - only need "Impact" for Masks
us_premise_impact <-fread(paste0(data_dir,"/", us.impact.file))
us_premise_impact[, date := as.Date(created)]
us_premise_impact[, n := 1]

us_recent <- us_premise_impact[date >= "2020-08-01"]

tab <- us_recent[, lapply(.SD, function(x) sum(x)), by="why_do_you_not_wear_a_mask_when_you_leave_your_home", .SDcols="n"]
tab <- tab[!why_do_you_not_wear_a_mask_when_you_leave_your_home %in% c("i","")]
tab[, total := sum(n)]
tab[, prop := n / total]
tab

table(us_premise_impact$"why_do_you_not_wear_a_mask_when_you_leave_your_home")

ggplot(premise, aes(x=as.Date(date), y=N, group=location_name)) + geom_line(alpha=0.2) + theme_bw() + 
  xlab("Date") + ylab("Sample size") + ggtitle("PREMISE weekly sample size among US states")

us_premise_impact[, week := format(date, "%W")]
week_map <- data.table(date = seq(as.Date("2020-04-01"), as.Date("2020-09-18"), 7))
week_map[, week := format(date, "%W")]
tab <- us_premise_impact[, lapply(.SD, function(x) sum(x)), by=c("week","why_do_you_not_wear_a_mask_when_you_leave_your_home"), .SDcols="n"]
wearers <- tab[why_do_you_not_wear_a_mask_when_you_leave_your_home == ""]
wearers[, mask_wearers := n]

tab <- tab[!why_do_you_not_wear_a_mask_when_you_leave_your_home %in% c("i","")]
tab[, total := sum(n), by="week"]
tab[, prop := n / total]
tab <- merge(tab, week_map, by="week")
tab <- merge(tab, wearers[,c("week","mask_wearers")], by="week")

tab[, respondents := total + mask_wearers]
tab[, never_wear := total / respondents]

tgh_colors <- c("#CCCC33","#99CCFF","#9999FF","#FF9900")
tab$group <- tab$why_do_you_not_wear_a_mask_when_you_leave_your_home
tab[, group := factor(group, labels = c("Uncomfortable","Not effective","Don't own","Others do not"))]
ggplot(tab, aes(x=as.Date(date), y=prop, col = group)) + geom_point() + theme_bw() +
  stat_smooth(method = "loess", se = F) + xlab("") + scale_y_continuous("Proportion", labels = percent) + 
  ggtitle("Why aren't Americans wearing masks?", subtitle = "People who never wear masks") + 
  scale_color_manual("", values = c(tgh_colors), label = c("Uncomfortable","Not effective","Don't own","Others do not"))

ggplot(tab, aes(x=as.Date(date), y= n / respondents, col = group)) + geom_point() + theme_bw() +
  stat_smooth(method = "loess", se = F) + xlab("") + scale_y_continuous("Proportion", labels = percent) + 
  ggtitle("Why aren't Americans wearing masks?", subtitle = "People who report never wearing a mask") + 
  geom_point(aes(y = never_wear, col="All never wearers")) + stat_smooth(aes(y=never_wear), col='black', se = F) + 
  #scale_color_manual("", values = c(tgh_colors), label = c("Uncomfortable","Not effective","Don't own","Others do not"))
  scale_color_manual("", values = c("All never wearers" = "black", "Uncomfortable" = tgh_colors[1],"Not effective" = tgh_colors[2],
                                    "Don't own" = tgh_colors[3],"Others do not" = tgh_colors[4]))

ggplot(tab, aes(x=as.Date(date), y=never_wear)) + geom_point() + theme_bw() +
  stat_smooth(method = "loess", col="black") + xlab("") + 
  scale_y_continuous("Proportion", labels = percent, limits = c(0,0.25)) + 
  ggtitle("Americans who never wear masks") 



