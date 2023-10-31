###########################################################################
## Import and minor prep for mask use data ##
## Hopefully there are no important reasons to revisit this script often.
###########################################################################

##---------------------------------------------------------------------------
## YouGov data
source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/yougov_processing.R"))
yougov <- fread(paste0(output_dir, "/yougov_always.csv"))
message(paste0("The last date of data from yougov was ", unique(yougov$max_date)[1]))
setnames(yougov, "value","prop_always")
yougov[, date := as.Date(date)]
yougov <- yougov[, .(location_name, location_id, date, prop_always, source, N)]

yougov <- subset(yougov, source %in% c("Adjusted Website","Week Binned GitHub Always"))

##--------------------------------------------------------------------------
## Premise
## If update premise is false, just moves old PREMISE data over
if(update_premise == F){
  system(paste0("cp FILEPATH", best_mask_outputs,"/Weekly_Premise_Mask_Use.csv ", output_dir, "/Weekly_Premise_Mask_Use.csv"))
  system(paste0("cp FILEPATH", best_mask_outputs,"/metadata.yaml ", output_dir, "/metadata.yaml"))
}

dt <- fread(paste0(output_dir,"/Weekly_Premise_Mask_Use.csv"))

# I don't know why but PREMISE data changed in some states, I don't want that to happen.
## No longer exists, folder has been archived (6/1/2021)
# consistency_premise <- fread("/ihme/covid-19/mask-use-outputs/2020_10_06.01/Weekly_Premise_Mask_Use.csv")
# consistency_premise <- consistency_premise[state %in% c("Alabama","Alaska","California","Colorado","Florida",
#                                                         "Georgia","Illinois","Indiana","Iowa","Kansas",
#                                                         "Michigan","Minnesota","Missouri","New Jersey",
#                                                         "New York","North Carolina","North Dakota","Oregon",
#                                                         "Pennsylvania","South Carolina","Tennessee","Texas",
#                                                         "Virginia","Washington","Wisconsin") & week == 25]
# 
# dt <- dt[!(state %in% consistency_premise$state & week == 25)]
# dt <- rbind(dt, consistency_premise)
dt[, source := "Premise"]

# Drop Low values in New England (per Emm, Steve 10/6/2020)
dt <- dt[!(Division == "New England" & week == 19)]
# Drop low values in week 20
dt <- dt[week != 20]
# Drop small sample size
dt <- dt[question_count > 30]

# Convert week to date
dt[, date := as.Date("2020-05-12") - (4 - week) * 7]

# Merge on location_id
dt[, location_name := state]
dt[, state := NULL]
dt[, N := question_count]
dt <- merge(dt[, .(location_name, date, prop_always, source, N)], 
            hierarchy[!(location_name == "Georgia" & parent_id != 102), .(location_name, location_id)])

# Include national aggregate PREMISE data
dt_nat <- copy(dt)
dt_nat[, count := prop_always * N]
dt_nat <- dt_nat[, lapply(.SD, function(x) sum(x)), by="date", .SDcols = c("N","count")]
dt_nat[, location_name := "United States of America"]
dt_nat[, location_id := 102]
dt_nat[, source := "Premise"]
dt_nat[, prop_always := count / N]
dt_nat[, count := NULL]

dt <- rbind(dt, dt_nat)

##--------------------------------------------------------------------------
## Facebook data

# Global data (Facebook)
#global_c <- fread("/ihme/covid-19-2/model-inputs/best/symptom_survey/GLOBAL_d_c.csv")
#global_s <- fread("/ihme/covid-19-2/model-inputs/best/symptom_survey/GLOBAL_d_r.csv")
global_c <- fread("FILEPATH/d_c.csv")
global_s <- fread("FILEPATH/d_r.csv")
global <- rbind(global_c, global_s, fill = T)
global[, date := as.Date(date)]

# US data  
#fb_us <- fread("/ihme/covid-19-2/model-inputs/best/symptom_survey/US_d_f.csv")
fb_us <- fread("FILEPATH/d_f.csv")
fb_us[, date := as.Date(date)]
setnames(fb_us, c("denom_nm","prop_nm"), c("N","mask_use"))
# Shifts recall period and variable name
us7 <- fb_us[variable == "C14" & value == 1 & date < "2021-02-09"]
us5 <- fb_us[variable == "C14a" & value == 1]

fb_us <- rbind(us5[, c("location_id","state","date","mask_use","N")], 
               us7[, c("location_id","state","date","mask_use","N")])
fb_us <- fb_us[order(location_id, date)]

fb_us <- fb_us[state != "Puerto Rico"]
fb_us[location_id == 891, location_id := 531]
fb_us[location_id == 531, state := "District of Columbia"]
fb_us <- fb_us[!is.na(mask_use)]
fb_us[, source := "Facebook - US"]
fb_us <- fb_us[, max_date := max(date), by = "location_id"]
fb_us[, location_name := state]
fb_us <- fb_us[date < max_date]


# Read in old Facebook data, before data revision
## No longer exists, folder has been archived (6/1/2021)
# prev_fb <- fread("/ihme/covid-19-2/mask-use-outputs/2020_09_08.01/facebook_premise_data.csv")
# prev_fb <- prev_fb[source=="Facebook"]
# prev_fb[, date := as.Date(date)]
