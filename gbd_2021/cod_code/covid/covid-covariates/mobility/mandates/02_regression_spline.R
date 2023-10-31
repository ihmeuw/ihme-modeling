## Purpose: Fit mandate regression and forecast percent of mandates\
## -------------------------------------------------------------------------
# requires R version 3.6.3 or higher

# Source packages
library(data.table)
library(lme4)
library(ggplot2)
library(splines)
library(arm)

if (R.Version()$major == "3") {
  library(mrbrt001, lib.loc = "FILEPATH/")
} else if (R.Version()$major == "4") {
  library(mrbrt002, lib.loc = "FILEPATH/") 
}
library('ihme.covid', lib.loc = 'FILEPATH')
source("FILEPATH/get_location_metadata.R")
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Define file paths
error_path <- paste0("FILEPATH/",Sys.info()['user'],"/errors/")
output_path <- paste0("FILEPATH/",Sys.info()['user'],"/output/")
pneumo_path <- "FILEPATH/reference_scenario.csv"
pop_path <- "FILEPATH/all_populations.csv"
vacc_path <- "FILEPATH/slow_scenario_vaccine_coverage.csv"
save_root <- "FILEPATH"
loc_set_file <-  "FILEPATH/location_sets.csv"
save_dir <- ihme.covid::get_latest_output_dir(root = save_root)
version <- basename(save_dir)
start_time <- Sys.time()


#---- Set args ---------------------------
# These arguments shouldn't change
latitude_adj <- TRUE
vacc_threshold <- 0.75  # What proportion of people do we think need to be vaccinated before all mandates come off?
decay <- 'exponential'  # linear or exponential / how to determine the scalar for vaccine coverage
parent_locs_to_model <- c(71, 11, 196) #Australia, Indonesia, South Africa
permissible_locs <- c() #locs that are in hierarchy but don't have to be in regression prep

# Location hierarchy args
loc_sets <- fread(loc_set_file)

lsid <- loc_sets[note == 'use',ls]
lsvid <-loc_sets[note == 'use',lsv]
rid <- loc_sets[note == 'use',ri]

lsid_pub <- loc_sets[note == 'production',ls]
lsvid_pub <-loc_sets[note == 'production',lsv]
rid_pub <- loc_sets[note == 'production',ri]

# Define args if interactive. Parse args otherwise.
if (interactive()){
  code_dir <- paste0("FILEPATH") 
  compare_version <- ""
  method <- 'vaccine_coverage' #vaccine_coverage or neither
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--code_dir", type = "character", help = "Root of repository.")
  parser$add_argument("--compare_version", type="character", help="Mandates version to include in the comparison plot")
  parser$add_argument("--method", type="character", choices = c("neither", "vaccine_coverage"), help="Vaccine adjustment method; 'neither' or 'vaccine_coverage'")
  
  args <- parser$parse_args(get_args())
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  print("Arguments passed were:")
  for (arg in names(args)){
    print(paste0(arg, ": ", get(arg)))
  }
}

repo_loc <- paste0(code_dir, "/sd_mandates/")
source(paste0(repo_loc, "/helper_plot_functions.R"))

# Part 1 --- Load and prep the data for regression ------------------------------------------------------
metadata_path <- file.path(save_dir, "02_regression_spline.metadata.yaml")

# Pull in hierarchy for plotting
hierarchy <- get_location_metadata(lsid, lsvid, release_id = rid) 
hierarchy[location_id == 35, location_name:= "Georgia (country)"]
hierarchy[location_id == 53620, location_name := "Punjab (Pakistan)"]

# Update hierarchy to make desired locs most-detailed and drop their child locs
child_locs_to_drop <- NULL
for(parent in parent_locs_to_model){
  child_locs <- hierarchy[parent_id==parent, location_id]
  child_locs_to_drop <- c(child_locs_to_drop, child_locs)
}
hierarchy <- hierarchy[!(location_id %in% child_locs_to_drop)]
hierarchy[location_id %in% parent_locs_to_model, most_detailed := 1]

# Build in "national id" for hierarchical modelling
hierarchy[level == 3, national_id := location_id]
hierarchy[level == 4, national_id := parent_id]
hierarchy[level >= 5, national_id := tstrsplit(path_to_top_parent, ",", keep = 4)]
hierarchy[, national_id := as.character(national_id)]
hierarchy[, region_id := as.character(region_id)]
hierarchy[, super_region_id := as.character(super_region_id)]

write.csv(hierarchy, paste0(save_dir, '/hierarchy.csv'), row.names = F)

# Locs for pub:
locs_for_pub <- get_location_metadata(lsid_pub, lsvid_pub, release_id = rid_pub)

# Pull data
all_mandates_df <- fread(paste0(save_dir, "/all_data_prepped.csv"))
pneumo_df <- fread(pneumo_path)
pneumo_df[, date:=as.Date(date)]
pop_df <- fread(pop_path)[sex_id == 3 & age_group_id == 22]

# get rid of draw columns in pneumonia covariate if present
if(length(names(pneumo_df)) > 4){
  draw_cols <- grep("draw_", colnames(pneumo_df), value = T)
  pneumo_df[, (draw_cols):=NULL]
  setnames(pneumo_df, old = 'value', new = 'pneumonia_reference') 
}

# add China w/o Macao to the pneumonia data if we are modeling it
if(44533 %in% hierarchy$location_id){
  chn_wo_macao <- pneumo_df[location_id == 6,]
  chn_wo_macao[,location_id := 44533]
  pneumo_df <- rbind(pneumo_df, chn_wo_macao)
  rm(chn_wo_macao)
}

# Add an extra year of pneumonia estimates
one_year <- pneumo_df[substr(date,1,4)=='2022']
one_year <- one_year[, date := gsub('2022', '2023', date)]
one_year <- one_year[date!='2023-01-01']
one_year[, date:=as.Date(date)]
pneumo_df <- rbind(pneumo_df, one_year)
pneumo_df <- pneumo_df[order(location_id, date)]

# SD data date
etl_version <- unique(all_mandates_df$etl_version)
data_date <- as.Date(gsub("_","-",paste0(strsplit(etl_version, "")[[1]][1:10],  collapse = "")))

# Vaccine coverage data
vacc_df <- fread(vacc_path)[,.(location_id, date, cumulative_all_vaccinated)]
vacc_df <- vacc_df[, date:=as.Date(date)]
# add population
vacc_df <- merge(vacc_df, pop_df[,.(location_id, population)])
# calculate cumulative vaccine coverage rate
vacc_df <- vacc_df[, vacc_coverage := cumulative_all_vaccinated/population]
# one month lag
vacc_df <- vacc_df[, lagged_vacc := shift(vacc_coverage, n=30, fill=0, type='lag'), by='location_id']
vacc_df <- vacc_df[, .(location_id, date, vacc_coverage, lagged_vacc)]
fwrite(vacc_df, paste0(save_dir,'/vaccine_coverage.csv'))

# Check all locations expected are there
model_locs <- hierarchy[most_detailed==1 & !(location_id %in% permissible_locs), location_id]
if( length(setdiff(model_locs, all_mandates_df$location_id))>0){
  stop(paste0("Missing these locations from prepped data: "),
       paste0(setdiff(model_locs, all_mandates_df$location_id), collapse = ","))
}

# Prep date vars
all_mandates_df[, date:=as.Date(date)]
all_mandates_df[, day:= as.integer(date - min(all_mandates_df$date))]

# Calculate percent of mandates in effect
mandates_in_reg <- c("stay_home", "educational_fac", "all_noness_business", "any_gathering_restrict", "any_business")
num_mandates <- length(mandates_in_reg)
all_mandates_df[, num_mandates_on := stay_home+educational_fac+all_noness_business+any_gathering_restrict+any_business, by = c("location_id","date")] 
all_mandates_df[, percent_mandates := num_mandates_on/num_mandates]

# Add location info
all_mandates_df <- merge(all_mandates_df, hierarchy[,.(location_id, super_region_id, super_region_name, region_id, region_name, national_id)], by="location_id", all.x=TRUE)
super_region_mapping <- hierarchy[level==1, .(location_id, location_name)]
all_mandates_df[, location_id:=as.factor(location_id)]
all_mandates_df[, super_region_id:=as.factor(super_region_id)]
reference_loc_id <- as.numeric(levels(all_mandates_df$location_id)[1])

# Merge on pneumonia
pneumo_df[, date:=as.Date(date)]
pneumo_df <- pneumo_df[date>=min(all_mandates_df$date)] #only need pneumo data for dates where we have mandate data
setnames(pneumo_df, "pneumonia_reference", "pneumonia")
all_mandates_df[, location_id := as.integer(as.character(location_id))]
all_mandates_df <- merge(all_mandates_df, pneumo_df[,.(date, location_id, pneumonia)], by=c("location_id", "date"), all.x=T)

# Aggregate pneumonia up hierarchy
pneumo_df <- merge(pneumo_df, pop_df[, .(location_id, population)], by = "location_id", all.x=T)
pneumo_df[, observed := NULL]

for(r in unique(hierarchy$region_id)){
  child_locs <- hierarchy[level==3 & region_id == r, location_id]
  
  pneumo_df_r <- pneumo_df[location_id %in% child_locs, 
                           .(pneumonia = weighted.mean(pneumonia, population), population=sum(population), location_id = r), 
                           by = "date"]
  
  pneumo_df <- rbind(pneumo_df, pneumo_df_r)
}

for(sr in unique(hierarchy$super_region_id)){
  child_locs <- hierarchy[level==2 & super_region_id == sr, location_id]
  
  pneumo_df_sr <- pneumo_df[location_id %in% child_locs, 
                            .(pneumonia = weighted.mean(pneumonia, population), population=sum(population), location_id = sr), 
                            by = "date"]
  
  pneumo_df <- rbind(pneumo_df, pneumo_df_sr)
}

# global
child_locs <- hierarchy[level==1, location_id]

pneumo_df_g <- pneumo_df[location_id %in% child_locs, 
                         .(pneumonia = weighted.mean(pneumonia, population), population=sum(population), location_id = 1), 
                         by = "date"]

pneumo_df <- rbind(pneumo_df, pneumo_df_g)


# Down-weight last 2 weeks of new data
# Introduced in order to stabilize the cascading splines model from week to week
# Data are weighted according to a linear function such that today's data are .05 of their full value
# and data from TWO weeks ago are back to full value
wt <- data.table("date" = seq(min(all_mandates_df$date), data_date + 7, by="day"),
                 "weight" = 1)
wt[date > (data_date - 14), weight := NA] 
wt[date >= data_date, weight := .05]      
# gradually decrease the weight from 1 to .05 starting from 2 weeks prior to the data date
weights <- 1 + seq(1,13,1) * (.05 - 1)/14
dates <- seq(min(wt[is.na(weight), date]), max(wt[is.na(weight), date]), by="day")
wt2 <- data.frame("date" = dates,
                  "weight" = weights)
wt <- wt[!is.na(weight)]
wt <- rbind(wt, wt2)
wt <- wt[order(date)]

# Part 2 --- Fit regression ----------------------------------------------------------------

df <- copy(all_mandates_df[!is.na(percent_mandates)])
df <- merge(df, wt, by="date", all.x=TRUE)
if(NROW(df[is.na(weight)])>0){
  stop("Some dates do not have weights assigned. Check date range in the wt table.")
}

# offset and convert to logit
df[percent_mandates == 0, percent_mandates := 0.01]
df[percent_mandates == 1, percent_mandates := 0.99]
df[, logit_val := logit(percent_mandates)]
df[, logit_se := sqrt(1/weight)]  #weighted SE = sqrt(SE^2/weight), we assume SE=1 so this simplifies to sqrt(1/weight)

dat_loc <- MRData()
dat_loc$load_df(
  data = df,
  col_obs = "logit_val", col_obs_se = "logit_se",
  col_covs = list("day", "pneumonia"), col_study_id = "location_id"
)

# specify prior bounds on the slopes of each spline
prior_matrix <- do.call("rbind", list(rep(-Inf, 4), rep(Inf, 4)))
prior_matrix[2,4] <- 0 #the last segment is flat or decreasing

# determine knot placement
# we want the knots to be at: 0, 68, 135, 203, {data date+7} 
knots <- c(0,67.5,135,202.5,max(df$day))/max(df$day)

# set up global model (use country & state level data to fit a global trend)
mod3 <- MRBRT(
  data = dat_loc,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE), 
    LinearCovModel(
      alt_cov = list("day"),
      use_re = FALSE,
      use_spline = TRUE,
      spline_knots = array(knots), 
      spline_degree = 3L,  #try linear or quadratic
      spline_knots_type = 'domain',
      spline_l_linear = FALSE,
      spline_r_linear = TRUE,
      prior_spline_maxder_uniform = prior_matrix  # prior that forces the linear tail to be flat or decreasing
    ),
    LinearCovModel("pneumonia", use_re = FALSE)
  )
)
mod3$fit_model(inner_print_level = 0L, inner_max_iter = 2000L)

# assign the global estimate of the pneumonia beta coeff as the prior for subsequent stages in the cascade
pneumo_beta <- tail(mod3$beta_soln, 1)
mod3$cov_models[[3]]$prior_beta_uniform <- matrix(c(pneumo_beta, pneumo_beta))
#mod3$cov_models[[3]]$prior_beta_uniform <- array(c(pneumo_beta, pneumo_beta))

# cascading splines
fit1 <- run_spline_cascade(
  stage1_model_object = mod3,
  df = df,
  col_obs = "logit_val",
  col_obs_se = "logit_se",
  col_study_id = "location_id",
  stage_id_vars = c("super_region_id", "region_id", "national_id", "location_id"),
  gaussian_prior = FALSE,  #uniform prior
  inner_print_level = 2L,
  thetas = c(0.125, 0.0625, 0.025, 0.0125), #tightest thetas
  output_dir = save_dir,
  model_label = "mrbrt_cascade_test",
  overwrite_previous = TRUE
)

# prep prediction df
df_pred <- hierarchy[,.(super_region_id, region_id, location_id, national_id, location_name, level, most_detailed)]
pneumo_df[, day := as.integer(date - min(date))]
pneumo_df[, location_id := as.integer(location_id)]
df_pred <- merge(df_pred, pneumo_df[,.(location_id, day, pneumonia)], by = c("location_id"))

df_pred[location_id == 1 | location_id == national_id | location_id == region_id | location_id == super_region_id, location_id := NA]

# make predictions
preds1 <- predict_spline_cascade(
  fit = fit1,
  newdata = df_pred
)

preds1 <- as.data.table(preds1)
preds1[, pred := invlogit(pred)]
preds1 <- preds1[order(level, super_region_id, day)]

with(preds1, plot(day, pred, type = "n", xlab = "day"))
for (id in rev(unique(preds1$cascade_prediction_id))) {
  with(
    filter(preds1, cascade_prediction_id == id),
    lines(
      x = day,
      y = pred,
      lwd = ifelse(cascade_prediction_id == "stage1__stage1", 3, 0.5),
      col = ifelse(cascade_prediction_id == "stage1__stage1", "darkorange", "black")
    )
  )
}

# add predictions to the main data table
preds1[is.na(location_id), location_id := national_id]
preds1[, location_id:=as.integer(location_id)]
all_mandates_df[, location_id:=as.integer(as.character(location_id))]
all_mandates_df <- merge(all_mandates_df, preds1[!is.na(location_id), .(location_id, day, pred)], by = c("location_id", "day"))


# Save projections for global, super region, and region 
regions <- preds1[level < 3, .(super_region_id, region_id, location_name, level, day, pred)]
write.csv(regions, paste0(save_dir, "/global_and_regional_preds.csv"), row.names = F)


# Part 3 --- Intercept shift results ---------------------------

# Fix observed/projected gap by scaling based on ratio of max obs data and min proj date
all_mandates_df[!is.na(percent_mandates), max_date_obs := max(date), by = "location_id" ]
tmp <- unique(all_mandates_df[date==max_date_obs, .(location_id, percent_mandates)])
all_mandates_df[is.na(percent_mandates), min_date_proj := min(date), by = "location_id"]
tmp2 <- unique(all_mandates_df[date==min_date_proj, .(location_id, pred)])
tmp <- merge(tmp, tmp2, by = c("location_id"))
tmp[percent_mandates == 1, percent_mandates := 0.99]
tmp[percent_mandates == 0, percent_mandates := 0.01]
tmp[, continuity_ratio := percent_mandates / pred]

# Merge ratio onto draws and adjust values
all_mandates_df <- merge(all_mandates_df, tmp[,.(location_id, continuity_ratio)], by = c("location_id"))
#mem save
rm(tmp)
all_mandates_df[is.na(percent_mandates), pred_adj := pred * continuity_ratio]
all_mandates_df[pred_adj >= (1-1e-9), pred_adj:= 1]
all_mandates_df[pred_adj <= (0+1e-9), pred_adj:= 0]
print("Predictions have been intercept shifted")


# Part 4 --- Adjust mandates based on forecasted vaccine coverage -----------

# Add vaccine coverage to the mandates dataset
all_mandates_df <- merge(all_mandates_df, vacc_df[,.(location_id, date, vacc_coverage, lagged_vacc)],
                         by=c('location_id', 'date'), all.x = T)

#calculate the scalar based on the vaccine coverage threshold (i.e. how many vaccines until govts lift all mandates?)
# linear means a linear relationship between the vaccine coverage and the scalar (for every unit increase in coverage, the scalar decreases by 1/vac_threshold)
if (decay=='linear'){
  # calculate the slope
  slope = -1/vacc_threshold
  # calculate the scalar
  all_mandates_df <- all_mandates_df[, scalar:=1+lagged_vacc*slope]
  # don't let the scalar go negative, even as vaccine coverage surpasses the threshold
  all_mandates_df <- all_mandates_df[scalar<0, scalar:=0]
} else if (decay=='exponential'){
  #all_mandates_df <- all_mandates_df[, scalar:=dexp(lagged_vacc, rate=1)] #this is almost right but has wrong x-int
  #use the form y=a^x, where y=scalar and x=coverage
  a <- ifelse(vacc_threshold==0.5, 5e-5, ifelse(vacc_threshold==0.75, .001, NA))
  if(is.na(a)){print("STOP. This is a new vaccine threshold. Need to specify a new parameter to satisfy this threshold.")}
  # calculate the scalar
  all_mandates_df <- all_mandates_df[, scalar:= a^lagged_vacc]
  # set the scalar to 0 after threshold has been reached
  all_mandates_df <- all_mandates_df[lagged_vacc>=vacc_threshold, scalar:=0]
}
# don't adjust for vaccine coverage prior to Feb 15 (i.e. set scalar equal to 1)
all_mandates_df <- all_mandates_df[date<='2021-02-15', scalar:=1]

# apply the scalar to adjust predicted mandates
# note: leave the first projected day unadjusted to avoid a discontinuity
all_mandates_df <- all_mandates_df[, pred_adj_vac:=pred_adj]
all_mandates_df <- all_mandates_df[!is.na(scalar) & !is.na(min_date_proj) & min_date_proj!=date, pred_adj_vac:=pred_adj_vac*scalar]

# if scalar is missing, set adjusted value to NA
all_mandates_df <- all_mandates_df[date>='2022-01-01' & is.na(scalar), pred_adj_vac:=NA]

# gradually ramp down from percent on last day of data to first value of pred_adj_vac over 4 weeks
#(a) first, obtain the last observed value of pct mandates and the projected value 28 days after the first forecast
start_end_vals <- all_mandates_df[date==min_date_proj | date==min_date_proj + 28, .(location_id, date, pred_adj_vac)]
start_end_vals <- start_end_vals[, n := seq_len(.N), by=location_id] #add a counter
start_end_vals <- dcast(start_end_vals, location_id ~ n, value.var="pred_adj_vac") #reshape wide
setnames(start_end_vals, c('1','2'),c('start_val', 'end_val'))
# (b) if the difference between the two days is <0.02, no need to smooth the transition
start_end_vals <- start_end_vals[, adj_flag := ifelse(abs(start_val - end_val)<0.02, 0, 1)]
# (c) calculate the slope for the linear ramp down
start_end_vals <- start_end_vals[, slope := (end_val - start_val)/28]
# (d) calculate the number of days since min_date_proj
all_mandates_df <- all_mandates_df[, n_days := date - min_date_proj]
# if n_days is outside the range of 1-28, set it to NA
all_mandates_df <- all_mandates_df[n_days>28, n_days:=NA]
# (e) combine information in order to apply the ramp down
all_mandates_df <- merge(all_mandates_df, start_end_vals[, .(location_id, start_val, slope, adj_flag)], by='location_id', all.x=T)
# (f) finally, apply the ramp down
all_mandates_df <- all_mandates_df[(! is.na(n_days)) & adj_flag==1, pred_adj_vac := start_val + (slope * n_days)]


if(method=='vaccine_coverage'){
  all_mandates_df <- all_mandates_df[, pred_adj := pred_adj_vac]
} # otherwise leave pred_adj as defined

# save full results
write.csv(all_mandates_df, file.path(save_dir, "full_results.csv"), row.names = F)


# Part 5 --- Plot -------------------------------------------------------------

all_mandates_df[,location_name := factor(location_name, levels = unique(hierarchy$location_name))]
all_mandates_df <- merge(all_mandates_df, preds1[level == 0, .(day, global = pred)], by = "day")

super_region_df <- all_mandates_df[, .(percent_mandates = mean(percent_mandates)), by = c("day", "super_region_name")][!is.na(percent_mandates)]

pdf(file.path(save_dir, "spline_plots.pdf"), width = 14, height = 10)

gg <- ggplot(preds1[level == 0], aes(x = day))+
  geom_line(data = super_region_df, aes(y = percent_mandates, color = super_region_name))+
  geom_line(aes(y = pred))+
  geom_vline(data = data.table(knots = mod3$cov_models[[2]]$spline_knots), aes(xintercept = knots), alpha = 0.4, linetype = 3)+
  scale_x_continuous(breaks = c(0, 183, 366, 548, 730),
                     labels = c("Jan-20", "Jul-20", "Jan-21", "Jul-21", "Dec-21"))+
  scale_y_continuous(limits=c(0,1),
                     breaks= seq(0,1,0.2)) +
  theme_bw()+
  labs(title = "Stage 1 spline fit",
       x = "Time",
       y = "Percent of mandates")

print(gg)

gg <- ggplot(preds1[level == 0], aes(x = day))+
  geom_line(data = super_region_df, aes(y = percent_mandates, color = super_region_name))+
  geom_line(data = preds1[level == 1], aes(y = pred, color = location_name), size =0.5)+
  geom_line(aes(y = pred))+
  geom_vline(data = data.table(knots = mod3$cov_models[[2]]$spline_knots), aes(xintercept = knots), alpha = 0.4, linetype = 3)+
  scale_x_continuous(breaks = c(0, 183, 366, 548, 730),
                     labels = c("Jan-20", "Jul-20", "Jan-21", "Jul-21", "Dec-21"))+
  scale_y_continuous(limits=c(0,1),
                     breaks= seq(0,1,0.2)) +
  theme_bw()+
  labs(title = "Stage 1 + Stage 2 spline fits",
       x = "Time",
       y = "Percent of mandates")

print(gg)

# US

gg <- ggplot(all_mandates_df[national_id == 102], aes(x=day, y = percent_mandates))+
  geom_line(aes(color = location_name), show.legend = F)+
  geom_line(aes(color = location_name, y = pred_adj), show.legend = F, linetype = 2)+
  geom_line(aes(y = global), color = "black", show.legend = F)+
  geom_line(data = preds1[national_id == 102 & !is.na(location_id)], aes(y = pred, color = location_name), show.legend = F)+
  facet_wrap(~location_name, scales = "free")+
  scale_x_continuous(breaks = c(0, 183, 366, 548, 730),
                     labels = c("Jan-20", "Jul-20", "Jan-21", "Jul-21", "Dec-21"))+
  scale_y_continuous(limits=c(0,1),
                     breaks= seq(0,1,0.2)) +
  labs(title = paste0("United States", ": location-specific spline fits and global model"))+
  theme_bw()

print(gg)

for(r in unique(all_mandates_df$region_name)){
  rid <- hierarchy[location_name == r & level == 2, location_id]
  
  gg <- ggplot(all_mandates_df[region_name == r], aes(x=day, y = percent_mandates))+
    geom_line(aes(color = location_name), show.legend = F)+
    geom_line(aes(color = location_name, y = pred_adj), show.legend = F, linetype = 2)+
    geom_line(aes(y = global), color = "black", show.legend = F)+
    geom_line(data = preds1[region_id == rid & most_detailed == 1], aes(y = pred, color = location_name), show.legend = F)+
    facet_wrap(~location_name, scales = "free")+
    scale_x_continuous(breaks = c(0, 183, 366, 548, 730),
                       labels = c("Jan-20", "Jul-20", "Jan-21", "Jul-21", "Dec-21"))+
    scale_y_continuous(limits=c(0,1),
                       breaks= seq(0,1,0.2)) +
    labs(title = paste0(r, ": location-specific spline fits and global model"))+
    theme_bw()
  
  print(gg)
}


dev.off()


# Part 6 --- save -----------------------------------------------------------

# Check all locations we need are still there
if( length(setdiff(model_locs, all_mandates_df$location_id))>0){
  stop(paste0("Cannot save results - missing these locations from final data: "),
       paste0(setdiff(model_locs, all_mandates_df$location_id), collapse = ","))
}

# save projections only for mobility
all_mandates_out <- all_mandates_df[is.na(percent_mandates)]
all_mandates_out[, percent := pred_adj]
all_mandates_out[, etl_version := etl_version]
setcolorder(all_mandates_out, c("location_id", "date", "percent", "etl_version" ))
write.csv(all_mandates_out, paste0(save_dir, "/results_for_mobility.csv"), row.names = F)
print("Results for mobility are saved!")
print(start_time - Sys.time())


# Appendix - comparison plots ---------------------------------------

# read in previous version
prev_ver <- fread(paste0(save_root,compare_version,'/full_results.csv'))
prev_ver[,location_name := factor(location_name, levels = unique(hierarchy$location_name))]
super_region_df_old <- prev_ver[, .(percent_mandates = mean(percent_mandates)), by = c("day", "super_region_name")][!is.na(percent_mandates)]
prev_regions <- fread(paste0(save_root,compare_version, "/global_and_regional_preds.csv"))

# retrieve the old ETL version
data_old <- fread(paste0(save_root, compare_version, "/all_data_prepped.csv"))
etl_old <- unique(data_old$etl_version)
rm(data_old)
print(paste("The new mandates are using etl", etl_version, "and the old mandates used etl", etl_old))

# ready to plot
pdf(file.path(save_dir, paste0("mandate_compare_plots_v",compare_version,".pdf")), width = 17, height = 15)

# compare global forecasts
gg <- ggplot(regions[level==0], aes(x = day, y = pred)) +
  geom_line(color='dodgerblue') +
  geom_line(data=prev_regions[level==0]) +
  scale_x_continuous(breaks = c(0, 183, 366, 548, 730),
                     labels = c("Jan-20", "Jul-20", "Jan-21", "Jul-21", "Dec-21")) +
  scale_y_continuous(limits=c(0,1),
                     breaks= seq(0,1,0.2)) +
  theme_bw() +
  theme(plot.title = element_text(size=18),
        axis.title = element_text(size=16),
        axis.text = element_text(size=14),
        plot.caption = element_text(size=14)) +
  labs(title = paste('Global:',version,'vs',compare_version),
       x = 'Time',
       y = 'Percent of mandates',
       caption = paste0("The black line is the previous version (",compare_version,"). ",
                        "The blue line is the current version (",version,")."))
print(gg)

# compare super region forecasts
gg <- ggplot(regions[level==1], aes(x=day, y=pred)) +
  geom_line(color='dodgerblue') +
  geom_line(data=prev_regions[level==1]) +
  facet_wrap(~location_name) +
  scale_x_continuous(breaks = c(0, 183, 366, 548, 730),
                     labels = c("Jan-20", "Jul-20", "Jan-21", "Jul-21", "Dec-21")) +
  scale_y_continuous(limits=c(0,1),
                     breaks= seq(0,1,0.2)) +
  theme_bw() +
  theme(plot.title = element_text(size=18),
        axis.title = element_text(size=16),
        axis.text = element_text(size=14),
        plot.caption = element_text(size=14)) +
  labs(title = paste('Super regions:',version,'vs',compare_version),
       x = 'Time',
       y = 'Percent of mandates',
       caption = paste0("The black line is the previous version (",compare_version,"). ",
                        "The blue line is the current version (",version,")."))
print(gg)

# compare region forecasts
gg <- ggplot(regions[level==2], aes(x=day, y=pred)) +
  geom_line(color='dodgerblue') +
  geom_line(data=prev_regions[level==2]) +
  facet_wrap(~location_name) +
  scale_x_continuous(breaks = c(0, 183, 366, 548, 730),
                     labels = c("Jan-20", "Jul-20", "Jan-21", "Jul-21", "Dec-21")) +
  scale_y_continuous(limits=c(0,1),
                     breaks= seq(0,1,0.2)) +
  theme_bw() +
  theme(plot.title = element_text(size=18),
        axis.title = element_text(size=16),
        axis.text = element_text(size=14),
        plot.caption = element_text(size=14)) +
  labs(title = paste('Regions:',version,'vs',compare_version),
       x = 'Time',
       y = 'Percent of mandates',
       caption = paste0("The black line is the previous version (",compare_version,"). ",
                        "The blue line is the current version (",version,")."))
print(gg)

# Prep data for parallelized plots 
locs <- locs_for_pub[level %in% c(3,4), .(location_id)]
write.csv(locs, paste0(save_dir, "/regression_spline_jobs.csv"), row.names=F) 
saveRDS(all_mandates_df, paste0(save_dir, "/all_mandates_df.rds"))
saveRDS(prev_ver, paste0(save_dir, "/prev_ver.rds"))

# Write a settings file with other arguments needed for PDF array job 
settings = data.table(save_dir = save_dir, 
                      version = version, 
                      sd_compare_version = compare_version)

# Need to write this file to a central location, not attached to "save_dir". 
write.csv(settings, paste0("/share/temp/sgeoutput/", Sys.info()[['user']], "/regression_spline_settings.csv"), row.names=F) 
write.csv(settings, paste0(save_dir, "/regression_spline_settings.csv"), row.names=F) # Save a copy for metadata

if (R.Version()$major == "4") {
  
  # Source the pdf_array_job function.
  ihme.covid::pdf_array_job(jobs_file=paste0(save_dir, "/regression_spline_jobs.csv"),
                            r_script=paste0(getwd(), '/sd_mandates/02a_qc_plots.R'), 
                            final_out_dir=save_dir,
                            write_file_name='mandate_compare_by_location.pdf')
  
}

dev.off()

yaml::write_yaml(
  list(
    script = "02_regression_spline.R",
    lsvid = lsvid,
    output_dir = save_dir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)