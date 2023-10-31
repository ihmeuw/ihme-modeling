
library(data.table)
library(ggplot2)
library(lme4)
library(plm)
library(viridis)

## Get hierarchy
source("FILEPATH/get_location_metadata.R")
hierarchy <- get_location_metadata(location_set_version_id = 746, location_set_id = 115)

## Pull in, format data
version <- "2020_09_28.01" #"best"

## Read in smoothed mask use estimates
mask_use <- fread(paste0("FILEPATH",version,"/mask_use.csv"))
  mask_use[, date := as.Date(date)]
setnames(mask_use, "mask_use","smoothed_mask_use")

## Read in mask data
mask_data <- fread(paste0("FILEPATH",version,"/used_data.csv"))
mask_data$mask_use <- mask_data$prop_always
mask_data[, date := as.Date(date)]
mask_data <- subset(mask_data, source %in% c("Facebook","Premise","Adjusted Website","Week Binned GitHub Always"))
mask_data <- merge(mask_data, mask_use[,c("location_id","date","smoothed_mask_use")], by=c("location_id","date"), all.x=T)

## Read in daily infections
seir_version <- "2020_09_16.04"
inf <- fread(paste0("FILEPATH",seir_version,"FILEPATH/daily_infections.csv")) # Should be current reference
  setnames(inf, "mean", "daily_infections")
deaths <- fread(paste0("FILEPATH",seir_version,"FILEPATH/daily_deaths.csv"))
  setnames(deaths, "mean", "daily_deaths")
inf[, date := as.Date(date)]
deaths[, date := as.Date(date)]

## Read in full_data
fd <- fread("FILEPATH/full_data.csv")
fd[, date := as.Date(Date)]
fd[, daily_confirmed := Confirmed - shift(Confirmed), by="location_id"]
fd[, daily_reported_deaths := Deaths - shift(Deaths), by="location_id"]

## Read in population
pop <- fread("FILEPATH/age_pop.csv")
pop <- pop[, lapply(.SD, sum), by=location_id, .SDcols = "population"]

seir <- merge(deaths, inf[,c("date","location_id","daily_infections")], by=c("date","location_id"))
seir <- merge(seir, pop, by="location_id")

seir[, incidence := daily_infections / population]
seir[, mort_rate := daily_deaths / population]
seir[, cuml_inc := cumsum(daily_infections), by="location_id"]
seir[, cuml_deaths := cumsum(daily_deaths), by="location_id"]

# Anticipating some requests from Steve/Chris
mask_data[, date := as.Date(date)]
mask_data <- mask_data[order(location_id, source, date)]
mask_data[, non_mask_use := 1 - mask_use]
mask_data[, change_use := mask_use - shift(mask_use), by=c("location_id","source")]
mask_data[, previous_non_use := shift(non_mask_use), by=c("location_id","source")]
mask_data[, first_diff_ratio := change_use / previous_non_use]

# Two data frames, one with observed, one with smoothed.
mask_data <- merge(mask_data, seir, by=c("location_id","date"), all.x=T)
mask_data <- merge(mask_data, fd[,c("location_id","date","Confirmed","daily_confirmed","Deaths","daily_reported_deaths")],
                   by=c("location_id","date"))

mask_use <- merge(mask_use, seir, by=c("location_id","date"), all.x=T)
mask_use <- merge(mask_use, fd[,c("location_id","date","Confirmed","daily_confirmed","Deaths","daily_reported_deaths")],
                   by=c("location_id","date"))

## Add mandates
  library(openxlsx)
  # Probably want a separate script to process
  mask_mandates_data <- read.xlsx("FILEPATH/closure_criteria_sheet_masks.xlsx")
  mask_mandates_data$date <- as.Date(mask_mandates_data$date, format = "%d.%m.%Y")
  mask_mandates_data <- data.table(mask_mandates_data)
  
  ## Manually add Victoria, AUS https://www.dhhs.vic.gov.au/updates/coronavirus-covid-19/face-coverings-mandatory-melbourne-and-mitchell-shire
  vic <- data.table(location_id = 60403, location_name = "Victoria", date = as.Date("2020-07-23"),
                    non_compliance_penalty = "Fine", targeted_ihme = "General public")
  
  mask_mandates_data <- rbind(mask_mandates_data, vic, fill=T)
  
  # What I think is, keep if only 1 by location, if multiple, choose more general (general public)
  mask_mandates_data[, row := 1]
  mask_mandates_data[, date := as.Date(date, format = "%d.%m.%Y")]
  mask_mandates_data[, location_rows := sum(row), by=location_id]
  mask_mandates_data[, public_in_name := ifelse(targeted_ihme %in% c("General public","General Public","general public","Public","public",
                                                                     "Public places","public places","Public spaces", "public spaces","All Public Spaces",
                                                                     "All public spaces","All persons"),1,
                                                ifelse(targeted %in% c("General public","General Public","general public","Public","public",
                                                                       "Public places","public places","Public spaces", "public spaces","All Public Spaces",
                                                                       "All public spaces","Mandate2","Mandate3"),1,0))]
  mask_mandates_data[, keep_row := ifelse(location_rows == 1, 1, ifelse(public_in_name == 1, 1, 0))]
  
  mask_mandates_data[, date_first_mandate := min(date), by=location_id]
  
  mask_mandates <- mask_mandates_data[keep_row == 1]
  mask_mandates[, first_date := min(date), by=location_id]
  mask_mandates <- mask_mandates[date == first_date] # Keep only the earlier mandate
  
  mask_mandates[, man_date := date]
  mask_mandates[, has_mandate := 1]

##
  
  mask_data <- merge(mask_data, 
                   mask_mandates[,c("location_id","man_date","has_mandate","date_first_mandate")], 
                   by="location_id", all.x=T) 
  mask_data[, man_date := as.character(man_date)]
  mask_data[, man_date := as.Date(man_date)]
  mask_data[, mandate := ifelse(date >= man_date, 1, 0), by=location_id]
  mask_data[, mandate := ifelse(mandate == TRUE, 1, 0)]
  mask_data[, mandate := ifelse(is.na(mandate), 0, mandate)]
  mask_data[, has_mandate := ifelse(is.na(has_mandate), 0, 1)]
  
##----------------------------------------------------------------------------
# Regressions

# Need a dummy variable so case/death doesn't explain time trends
mask_data[, first_data_date := min(date), by="location_id"]
mask_data[, data_days := as.numeric(date - first_data_date)]
mask_data[, epi_days := as.numeric(date - as.Date(first_case_date))]
mask_use <- merge(mask_use, unique(mask_data[,c("location_id","first_data_date")]), by="location_id")
mask_use[, data_days := as.numeric(date - first_data_date)]
mask_data[, lag_inc := shift(incidence, 7), by="location_id"]
mask_data[, lag_mort_rate := shift(mort_rate, 7), by="location_id"]
mask_data[, lag_inc14 := shift(incidence, 14), by="location_id"]
mask_data[, lag_mort_rate14 := shift(mort_rate, 14), by="location_id"]
mask_data[, cumul_infections := cumsum(daily_infections), by="location_id"]
mask_data[, cumul_inf_rate := cumul_infections / population]
mask_data[, cuml_death_rate := cuml_deaths / population]

# What do you want to consider as a predictor?
cor_data <- mask_data[, c("mask_use","data_days","epi_days","Confirmed","Deaths", "daily_confirmed","daily_reported_deaths",
                          "daily_deaths","daily_infections","incidence","mort_rate", "lag_inc","lag_mort_rate")]
cor_data <- mask_data[, c("mask_use","data_days","epi_days","daily_deaths","daily_infections","incidence","mort_rate", "cumul_infections","cuml_deaths",
                          "cumul_inf_rate","cuml_death_rate")]
cormat <- round(cor(cor_data, use = "complete.obs"),2)
# Get upper triangle of the correlation matrix
get_lower_tri <- function(cormat){
  cormat[upper.tri(cormat)]<- NA
  return(cormat)
}
cormat <- get_lower_tri(cormat)
cormat <- melt(cormat, na.rm=T)

ggplot(data = cormat, aes(x=Var1, y=Var2, fill=value)) + theme_bw() + 
  geom_tile(color = "white") + geom_text(aes(label = value)) + scale_fill_viridis("Correlation") +
  theme(axis.text.x = element_text(angle=90, hjust=1))

mask_data[, lag_inc := lag_inc * 100000]

summary(lmer(prop_always ~ epi_days + has_mandate + lag_mort_rate + (1 | location_id), data=mask_data))
summary(lmer(prop_always ~ epi_days + has_mandate + lag_mort_rate14 + (1 | location_id), data=mask_data))
summary(lmer(prop_always ~ epi_days + has_mandate + lag_inc + (1 | location_id), data=mask_data))
summary(lmer(prop_always ~ epi_days + has_mandate + (lag_inc | location_id), data=mask_data))



mmod1 <- lmer(prop_always ~ epi_days + has_mandate + lag_mort_rate + (1 | location_id), data=mask_data)
summary(mmod1)

mod1 <- lm(prop_always ~ epi_days + lag_inc + lag_mort_rate, data=mask_data) # Random effects absorbing too much info?
summary(mod1)


# Predict it
loc <- c(570, 76, 128, 123, 4659, 11, 60360, 528, 93) 
mask_data$pred_me_mod <- predict(mmod1, newdata = mask_data, allow.new.levels = T)
mask_data$pred_lm_mod <- predict(mod1, newdata= mask_data)
# mask_data$pred_pmod1 <- predict(pmod1, newdata = mask_data)

ggplot(mask_data[location_id %in% loc], aes(x=as.Date(date))) + geom_point(aes(y=prop_always, shape=factor(has_mandate))) + 
  geom_line(aes(y=pred_me_mod, col="Mixed effects"), lwd = 1.25) +
  geom_line(aes(y=pred_lm_mod, col="Linear model"), lwd = 1.25) +
  facet_wrap(~location_name) +
  scale_color_manual(values = c("Mixed effects" = "blue", "Linear model" = "orange")) + theme_bw()


##-----------------------------------------------------------------------------------------------------
## 21 day OOS predictions

model_df <- mask_data[date <= Sys.Date() - 42]
mmod1 <- lmer(prop_always ~ epi_days + has_mandate + lag_mort_rate + (1 | location_id), data=model_df)
#mmod1 <- lmer(prop_always ~ epi_days + has_mandate + (lag_inc | location_id), data=model_df)

mask_data$pred_42days <- predict(mmod1, newdata = mask_data, allow.new.levels = T)

# Line up model predictions with end of Barber smooth
first_model_pred <- mask_data[date == Sys.Date() - 42 & source != "Adjusted Website", c("location_id","pred_42days")]
last_bsmooth <- mask_data[date == Sys.Date() - 42 & source != "Adjusted Website", c("location_id","smoothed_mask_use")]
setnames(first_model_pred, "pred_42days", "first_model_pred")
setnames(last_bsmooth, "smoothed_mask_use","last_bsmooth")
pred_data <- merge(mask_data, first_model_pred, by="location_id")
pred_data <- merge(pred_data, last_bsmooth, by="location_id")

#mask_data[, last_bsmooth := tail(prop_always,1), by="location_id"]
pred_data[, shifted_pred_42days := pred_42days + (last_bsmooth - first_model_pred)]

pdf("FILEPATH/epidemic_regression_predictions.pdf")
  for(loc in unique(pred_data$location_id)){
    p <- ggplot(pred_data[location_id %in% loc], aes(x=as.Date(date))) + geom_point(aes(y=prop_always, shape=factor(has_mandate))) + 
      geom_line(aes(y=pred_42days, col="42 day OOS"), lwd = 1.25) +
      geom_line(aes(y=smoothed_mask_use, col = "Barber smooth"), lwd=1.25) + 
      geom_line(data=pred_data[date > Sys.Date() - 42 & location_id %in% loc], aes(y = shifted_pred_42days, col = "Shifted 42 day OOS"), lwd = 1.25) + 
      facet_wrap(~location_name) + xlab("") + ylab("Mask use") + 
      geom_vline(xintercept = Sys.Date() - 42, lty=2) + 
      scale_shape_discrete("Mandate", labels = c("No","Yes")) + 
      scale_color_manual("", values = c("42 day OOS" = "blue", "Barber smooth" = "purple", "Shifted 42 day OOS" = "goldenrod")) + theme_bw()
    print(p)
  }

dev.off()




