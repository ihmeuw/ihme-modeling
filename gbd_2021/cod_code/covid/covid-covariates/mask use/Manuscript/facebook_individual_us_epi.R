#############################################################
## Facebook survey US and epidemiology ##
#############################################################

library(ggplot2)
library(data.table)
library(gridExtra)

us <- fread("/ihme/limited_use/LIMITED_USE/PROJECT_FOLDERS/COVID19/SYMPTOM_SURVEY/US/summary_individual_responses_us.gz")

## Pull in SEIR results
seir_version <- "2020_08_05.01"
seir_sum_daily <- fread(paste0("/ihme/covid-19/hospitalizations/inputs/seir/",seir_version,"/stats/Daily_deaths_summary_",seir_version,".csv"))
infections <- fread(paste0("/ihme/covid-19/hospitalizations/inputs/seir/",seir_version, "/daily_infections_summ.csv"))
population <- fread(paste0("/ihme/covid-19/model-inputs/best/age_pop.csv"))
population <- population[, lapply(.SD, function(x) sum(x)), by="location_id", .SDcols = "population"]


## What symptom questions are shared across the surveys?
  us_symptoms <- new_names <- c(
    'symptoms_24hrs_yes', 'symptoms_24hrs_no',
    'fever_24hrs', 'cough_24hrs', 'short_breath_24hrs',
    'difficulty_breathing_24hrs', 'tired_24hrs', 'congestion_24hrs',
    'runny_nose_24hrs', 'achy_24hrs', 'sore_throat_24hrs',
    'chest_pain_24hrs', 'nausea_24hrs', 'diarrhea_24hrs',
    'smell_taste_loss_24hrs', 'other_24hrs', 'none_24hrs')

  shared_symptoms <- c("fever","cough","difficulty_breathing","fatigue",
                       "sore_throat","chest_pain","smell_taste",
                       "runny_nose","nausea","any")
  us_s_names <- c("fever_24hrs","cough_24hrs","difficulty_breathing_24hrs",
                  "tired_24hrs","sore_throat_24hrs","chest_pain_24hrs",
                  "smell_taste_loss_24hrs","runny_nose_24hrs","nausea_24hrs","symptoms_24hrs_yes")

  setnames(us, us_s_names, shared_symptoms)

  us[, recent_test := ifelse(symptoms_24hrs_yes_tested_yes_positive == 1, 1,
                             ifelse(symptoms_24hrs_yes_tested_yes_negative == 1, 1,
                                    ifelse(symptoms_24hrs_yes_tested_yes_negative == 1, 1, 0)))]
  setnames(us, "symptoms_24hrs_yes_tested_yes_positive","test_positive")

  ## Rbind, collapse to location_id/date
  us$location_name <- us$state
  us$sex <- ifelse(us$female == 1, "female", "male")
  us$age_group[us$age_group == "24-34"] <- "25-34"
  us$age_group[us$age_group == "75+"] <- "> 75"

  # Make work and contacts
  us[, work := ifelse(outside_home_work_yes == 1, "Yes", "No")]
  us[, avoid_contact := ifelse(avoid_contact_all == 1, "Yes", "No")]
  us[, direct_contact := ifelse(direct_contact_24hrs_yes == 1, "Yes", "No")]

  us[, sum_symptoms := fever + cough + difficulty_breathing + fatigue + sore_throat + chest_pain + smell_taste + runny_nose + nausea]
  us[, recent_test := ifelse(is.na(recent_test), 0, recent_test)]
  us[, test_positive := ifelse(is.na(test_positive), 0, test_positive)]

  # Focus on finances / feelings / behaviors
  us[, avoid_contact := ifelse(avoid_contact_all == 1, "All", ifelse(avoid_contact_most == 1, "Most", ifelse(avoid_contact_some == 1, "Some", "None")))]
  us[, work_outside_home := ifelse(outside_home_work_yes == 1, "Yes", "No")]
  us$n <- 1

  # dt <- data.frame(us)
  # dt <- dt[, c("state","date","age_group","sex","location_id","work","work_outside_home","avoid_contact", "recent_test","test_positive",
  #              "direct_contact","nervous","worried","depressed","sum_symptoms","finances",shared_symptoms,"n")]
  dt <- data.table(us)

## Now we can do the mental health symptoms
  dt[, all_nervous := ifelse(nervous == "All", 1, 0)]
  dt[, all_worried := ifelse(worried == "All", 1, 0)]
  dt[, all_depressed := ifelse(depressed == "All", 1, 0)]

  mh <- dt[, lapply(.SD, function(x) sum(x, na.rm=T)), by = c("state","date","age_group","sex","location_id"),
           .SDcols = c("n",
                       names(dt)[names(dt) %like% "depressed_"],
                       names(dt)[names(dt) %like% "finance_"],
                       names(dt)[names(dt) %like% "avoid_contact_"],
                       names(dt)[names(dt) %like% "worried_"],
                       names(dt)[names(dt) %like% "nervous_"])]
  mhp <- dt[, lapply(.SD, function(x) mean(x, na.rm=T)), by = c("state","date","age_group","sex","location_id"),
           .SDcols = c(names(dt)[names(dt) %like% "depressed_"],
                       names(dt)[names(dt) %like% "finance_"],
                       names(dt)[names(dt) %like% "avoid_contact_"],
                       names(dt)[names(dt) %like% "worried_"],
                       names(dt)[names(dt) %like% "nervous_"])]

  mhp <- merge(mhp, mh[,c("n","location_id","date","age_group","sex")], by = c("location_id","date","age_group","sex"))
  mhp <- mhp[!is.na(location_id) & age_group != "" & n > 10]
  mhp <- merge(mhp, seir_sum_daily[,c("location_id","date","deaths_mean")], by=c("location_id","date"))
  mhp <- merge(mhp, infections[,c("location_id","date","inf_mean")], by=c("location_id","date"))
  mhp <- merge(mhp, population, by="location_id")
  mhp[, mrate := deaths_mean / population * 10000]
  mhp[, incidence := inf_mean / population * 100]

## Figure out how to plot on the same scales.
  st_name <- "New York"
  ggplot(mhp[state==st_name], aes(x=as.Date(date))) + theme_bw() +
    geom_line(aes(y=mrate)) +
    geom_point(aes(y=depressed_all), alpha=0.2) +
    stat_smooth(aes(y=depressed_all), se=F, method = "loess") +
    facet_grid(sex ~ age_group) + ggtitle("Depressed all the time")
  ggplot(mhp[state==st_name], aes(x=as.Date(date))) + theme_bw() +
    geom_line(aes(y=mrate)) +
    geom_line(aes(y=incidence), lty=2) +
    geom_point(aes(y=worried_very), alpha=0.2) +
    stat_smooth(aes(y=worried_very), se=F, method = "loess", col="green") +
    facet_grid(sex ~ age_group) + ggtitle("Very worried about COVID-19")

##################################################################################################
## Info for Kevin O'Rourke
  library(scales)
  tgh_colors <- c("#CCCC33","#99CCFF","#9999FF","#FF9900")

  # Survey seemed to change
  dt <- dt[date <= "2020-09-08"]
  mh <- dt[, lapply(.SD, function(x) sum(x, na.rm=T)), by = c("date"),
           .SDcols = c("n",
                       names(dt)[names(dt) %like% "depressed_"],
                       names(dt)[names(dt) %like% "finance_"],
                       names(dt)[names(dt) %like% "avoid_contact_"],
                       names(dt)[names(dt) %like% "worried_"],
                       names(dt)[names(dt) %like% "nervous_"])]
  mht <- dt[, lapply(.SD, function(x) sum(x, na.rm=T)), by = c("date", "sex","age_group"),
            .SDcols = c("n",
                        names(dt)[names(dt) %like% "depressed_"],
                        names(dt)[names(dt) %like% "finance_"],
                        names(dt)[names(dt) %like% "avoid_contact_"],
                        names(dt)[names(dt) %like% "anxious_"],
                        names(dt)[names(dt) %like% "worried_"])]
  mhw <- dt[, lapply(.SD, function(x) weighted.mean(x, w = weight, na.rm=T)), by = c("date", "sex","age_group"),
            .SDcols = c(names(dt)[names(dt) %like% "depressed_"],
                        names(dt)[names(dt) %like% "finance_"],
                        names(dt)[names(dt) %like% "avoid_contact_"],
                        names(dt)[names(dt) %like% "anxious_"],
                        names(dt)[names(dt) %like% "worried_"])]
  mhw[, weighted_depressed_none := depressed_none]

  long_depressed <- melt(mh[,c("date","n","depressed_none","depressed_some","depressed_most","depressed_all")], id.vars = c("date","n"))
  long_depressed[, proportion := value / n]
  ggplot(long_depressed, aes(as.Date(date), y=proportion, col=variable)) + stat_smooth(method="loess", se=F) +
    geom_point(alpha=0.2) + theme_bw()
#
#   long_depressed <- melt(mht[,c("date","sex","age_group","n","depressed_none","depressed_some","depressed_most","depressed_all")], id.vars = c("date","n","sex","age_group"))
#   long_depressed[, proportion := value / n]
#   long_depressed <- long_depressed[age_group != ""]
#   long_depressed[, age_group := factor(age_group, levels = c("18-24","25-34","35-44","45-54","55-64","65-74","> 75"))]
#   ggplot(long_depressed[!is.na(sex)], aes(as.Date(date), y=proportion, col=variable)) + stat_smooth(method="loess", se=F) +
#     geom_point(alpha=0.2) + theme_bw() + facet_grid(sex~age_group)

  long_depressed <- melt(mht[,c("date","sex","age_group","n","depressed_none","depressed_some","depressed_most","depressed_all")], id.vars = c("date","n","sex","age_group"))
  long_depressed[, proportion := value / n]
  long_depressed <- long_depressed[age_group != ""]
  long_depressed[, age_group := factor(age_group, levels = c("18-24","25-34","35-44","45-54","55-64","65-74","> 75"))]
  long_depressed[, sex_name := ifelse(sex == "female","Females","Males")]

  jpeg("/home/j/temp/ctroeger/COVID19/Posts/mental_health_depressed_lines.jpeg", width = 6600, height = 4000)
    ggplot(long_depressed[!is.na(sex)], aes(as.Date(date), y=proportion, col=variable)) +
      stat_smooth(method="loess", se=F, lwd = 10) +
      geom_point(alpha=0.2, size = 20) + facet_grid(sex_name~age_group) +
      theme_minimal(base_size = 140, base_family = "bold") +
      guides(color = guide_legend(override.aes = list(size=30))) +
      theme(legend.position = "bottom") + scale_color_manual("", values = tgh_colors, labels = c("None","Some","Most","All")) +
      scale_y_continuous("Percent", labels = percent) + scale_x_date("", date_breaks = "2 month", date_labels = "%b") +
      theme(legend.key = element_rect(color="black"), axis.text.x = element_text(angle = 45, hjust=1))
  dev.off()

## Alt, weekly bars
  dt[, date := as.Date(date)]
  dt[, week := format(date, "%W")]
  week_map <- data.table(date = seq(as.Date("2020-04-01"), as.Date("2020-09-18"), 7))
  week_map[, week := format(date, "%W")]
  mh <- dt[, lapply(.SD, function(x) sum(x, na.rm=T)), by = c("week"),
           .SDcols = c("n",
                       names(dt)[names(dt) %like% "depressed_"],
                       names(dt)[names(dt) %like% "finance_"],
                       names(dt)[names(dt) %like% "avoid_contact_"],
                       names(dt)[names(dt) %like% "worried_"],
                       names(dt)[names(dt) %like% "nervous_"])]
  mh[, n := depressed_none + depressed_some + depressed_most + depressed_all]

  long_depressed <- melt(mh[,c("week","n","depressed_none","depressed_some","depressed_most","depressed_all")], id.vars = c("week","n"))
  long_depressed[, proportion := value / n]
  long_depressed <- merge(long_depressed, week_map, by="week")

  jpeg("/home/j/temp/ctroeger/COVID19/Posts/mental_health_depressed_bar.jpeg", width = 4000, height = 3500)
    ggplot(long_depressed, aes(as.Date(date), y=proportion, fill=variable)) + geom_bar(stat="identity") +
      theme_minimal(base_size = 100) +
      scale_y_continuous("Percent", labels = percent) + scale_x_date("") +
      theme(legend.position = "bottom") +
      guides(fill = guide_legend(override.aes = list(size=30))) +
      scale_fill_manual("", values = tgh_colors, labels = c("None","Some","Most","All")) +
      theme(legend.key = element_rect(color="black"))
  dev.off()

## Anxious
  mh <- dt[, lapply(.SD, function(x) sum(x, na.rm=T)), by = c("date"),
           .SDcols = c("n",
                       names(dt)[names(dt) %like% "depressed_"],
                       names(dt)[names(dt) %like% "finance_"],
                       names(dt)[names(dt) %like% "avoid_contact_"],
                       names(dt)[names(dt) %like% "anxious_"],
                       names(dt)[names(dt) %like% "worried_"])]

  long_depressed <- melt(mh[,c("date","n","anxious_none","anxious_some","anxious_most","anxious_all")], id.vars = c("date","n"))
  long_depressed[, proportion := value / n]
  ggplot(long_depressed, aes(as.Date(date), y=proportion, col=variable)) + stat_smooth(method="loess", se=F) +
    geom_point(alpha=0.2) + theme_bw()

# Age/sex alternative
  long_depressed <- melt(mht[,c("date","sex","age_group","n","anxious_none","anxious_some","anxious_most","anxious_all")], id.vars = c("date","n","sex","age_group"))
  long_depressed[, proportion := value / n]
  long_depressed <- long_depressed[age_group != ""]
  long_depressed[, age_group := factor(age_group, levels = c("18-24","25-34","35-44","45-54","55-64","65-74","> 75"))]
  long_depressed[, sex_name := ifelse(sex == "female","Females","Males")]

  jpeg("/home/j/temp/ctroeger/COVID19/Posts/mental_health_anxious_lines.jpeg", width = 6600, height = 4000)
    ggplot(long_depressed[!is.na(sex)], aes(as.Date(date), y=proportion, col=variable)) +
      stat_smooth(method="loess", se=F, lwd = 10) +
      geom_point(alpha=0.2, size = 20) + facet_grid(sex_name~age_group) +
      theme_minimal(base_size = 140, base_family = "bold") +
      guides(color = guide_legend(override.aes = list(size=30))) +
      theme(legend.position = "bottom") + scale_color_manual("", values = tgh_colors, labels = c("None","Some","Most","All")) +
      scale_y_continuous("Percent", labels = percent) + scale_x_date("", date_breaks = "2 month", date_labels = "%b") +
      theme(legend.key = element_rect(color="black"), axis.text.x = element_text(angle = 45, hjust=1))
  dev.off()

## Finances
  long_depressed <- melt(mh[,c("date","n","finance_threat_none","finance_threat_little","finance_threat_moderate","finance_threat_substantial")],
                         id.vars = c("date","n"))
  long_depressed[, proportion := value / n]
  ggplot(long_depressed, aes(as.Date(date), y=proportion, col=variable)) + stat_smooth(method="loess", se=F) +
    geom_point(alpha=0.2) + theme_bw()

  long_depressed <- melt(mht[,c("date","sex","age_group","n","finance_threat_none","finance_threat_little","finance_threat_moderate","finance_threat_substantial")],
                         id.vars = c("date","n","sex","age_group"))
  long_depressed[, proportion := value / n]
  long_depressed <- long_depressed[age_group != ""]
  long_depressed[, age_group := factor(age_group, levels = c("18-24","25-34","35-44","45-54","55-64","65-74","> 75"))]
  long_depressed[, sex_name := ifelse(sex == "female","Females","Males")]

  jpeg("/home/j/temp/ctroeger/COVID19/Posts/mental_health_financial_lines.jpeg", width = 6600, height = 4000)
    ggplot(long_depressed[!is.na(sex)], aes(as.Date(date), y=proportion, col=variable)) +
      stat_smooth(method="loess", se=F, lwd = 10) +
      geom_point(alpha=0.2, size = 20) + theme_minimal(base_size = 140, base_family = "bold") +
      guides(color = guide_legend(override.aes = list(size=30))) +
      facet_grid(sex_name~age_group) +
      theme(legend.position = "bottom") + scale_color_manual("", values = tgh_colors, labels = c("None","Little","Moderate","Substantial")) +
      scale_y_continuous("Percent", labels = percent) + scale_x_date("", date_breaks = "2 month", date_labels = "%b") +
      theme(legend.key = element_rect(color="black"), axis.text.x = element_text(angle = 45, hjust=1))
  dev.off()

## Summary tabulations
  dt[, month := format(date, "%B")]
  mht <- dt[, lapply(.SD, function(x) sum(x, na.rm=T)), by = c("month", "sex","age_group"),
            .SDcols = c("n",
                        names(dt)[names(dt) %like% "depressed_"],
                        names(dt)[names(dt) %like% "finance_"],
                        names(dt)[names(dt) %like% "avoid_contact_"],
                        names(dt)[names(dt) %like% "anxious_"],
                        names(dt)[names(dt) %like% "worried_"])]
  mht <- mht[!is.na(sex)]
  mht <- mht[age_group != ""]

  dep_vars <- c("depressed_none","depressed_some","depressed_most","depressed_all")
  mht[,(dep_vars) := lapply(.SD, function(x) x/n), .SDcols = dep_vars]

  mht[month %in% c("April","September"), ]

  jpeg("/home/j/temp/ctroeger/COVID19/Posts/mental_health_sample_sizes.jpeg", width = 4000, height = 3600)
    ggplot(mh, aes(x=as.Date(date), y=n)) +
      geom_point(size = 30, alpha=0.8, col=tgh_colors[3]) +
      theme_minimal(base_size = 115, base_family = "bold") + scale_y_continuous("Respondents", label = comma) +
      scale_x_date("")
  dev.off()

  mht <- dt[, lapply(.SD, function(x) sum(x, na.rm=T)), by = c("date", "sex","age_group"),
            .SDcols = c("n",
                        names(dt)[names(dt) %like% "depressed_"],
                        names(dt)[names(dt) %like% "finance_"],
                        names(dt)[names(dt) %like% "avoid_contact_"],
                        names(dt)[names(dt) %like% "anxious_"],
                        names(dt)[names(dt) %like% "worried_"])]
  mht <- mht[!is.na(sex)]
  mht <- mht[age_group != ""]
  mht <- mht[order(date, sex, age_group)]

  write.csv(mht, "/home/j/temp/ctroeger/COVID19/Posts/facebook_survey_mh_tabulations.csv", row.names = F)
