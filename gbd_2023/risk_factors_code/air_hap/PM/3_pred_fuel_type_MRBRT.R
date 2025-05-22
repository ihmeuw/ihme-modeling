#-------------------Header------------------------------------------------

# Purpose: use MR-BRT to run log-linear mapping model

#------------------Set-up--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

project <- "-P USERNAME "
sge.output.dir <- " -o FILEPATH -e FILEPATH "

# load packages, install if missing
lib.loc <- paste0(h_root, "R/", R.Version()$platform, "/", R.Version()$major, ".", R.Version()$minor)
dir.create(lib.loc, recursive=T, showWarnings = F)
.libPaths(c(lib.loc, .libPaths()))

packages <- c("data.table", "magrittr", "ggplot2", "openxlsx", "metafor", "pbapply", "Metrics", "readr")

for(p in packages){
  if(p %in% rownames(installed.packages()) == F){
    install.packages(p)
  }
  library(p, character.only = T)
}

'%ni%' <- Negate("%in%")

in_date <- "082124" # GBD 2023
date <- format(Sys.Date(), "%m%d%y")
disagg <- F

draw_cols <- paste0("draw_",1:1000)

#------------------Directories and shared functions-----------------------------
home_dir <- file.path("FILEPATH")  # GBD 2022/2023
in_dataset <- paste0(home_dir,"/FILEPATH/lmer_input_",in_date,".csv")

# out
dir.create(paste0(home_dir, "/FILEPATH/", date), recursive=T)
dir.create(paste0(home_dir, "/FILEPATH/", date), recursive=T)

# central functions
source(file.path(central_lib, "FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(location_set_id=35, release_id = 16)  # GBD 2022/2023
source(file.path(central_lib, "FILEPATH/get_covariate_estimates.R"))

dt <- fread(in_dataset)

# make measure_group labels for modeling
dt[, group_female:=ifelse(measure_group=="female",1,0)]
dt[, group_male:=ifelse(measure_group=="male",1,0)]
dt[, group_child:=ifelse(measure_group=="child",1,0)]
dt[, group_indoor:=ifelse(measure_group=="indoor",1,0)]

# set clean!= 1, 0 to .5
dt[clean>0 & clean <1, clean:=.5]

# log-transform pm excess
dt[, log_pm_excess:=log(pm_excess)]

# create a stand-in for standard error because we don't have one,
# so we'll just weight by sample size
dt[, weight:=1/sqrt(sample_size)]


# add on LDI
ldi <- get_covariate_estimates(covariate_id = 57,
                               location_id = c(unique(dt$location_id)),
                               year_id = c(unique(dt$year_id)),
                               location_set_id = 35,
                               release_id = 16)  # GBD 2022/2023

setnames(ldi, "mean_value", "ldi")
dt <- merge(dt, ldi[, .(location_id, year_id,ldi)], by=c("location_id", "year_id"), all.x=T)


ldi <- get_covariate_estimates(covariate_id = 57,
                               year_id = 1990:2023,  # GBD 2022/2023
                               location_id = locs[level>=3 & level==0, location_id],
                               location_set_id=35,
                               release_id = 16)  # GBD 2022/2023


ldi <- ldi[, .(location_id, year_id, ldi=mean_value)]
ldi <- ldi[, merge:=1]


# load MR-BRT functions
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")


# -------------------------  Run log-linear model to get fuel-type preds ----------------------------------------------------------------------------------

# load the data frame

data <- mr$MRData()

data$load_df(
  dt,
  col_obs = "log_pm_excess",
  col_obs_se = "weight",
  col_covs = list("ldi","solid","crop","coal","dung","wood",
                  "group_female","group_male","group_child","measure_24hr"),
  col_study_id = "S.No")


# fit the model
linear_cov_model = mr$LinearCovModel(
  alt_cov=c("ldi"),
  use_re=T,
  use_spline=T,
  use_re_mid_point=T,
  spline_degree = 2L, # 2L is quadratic
  spline_knots = array(seq(0, 1, length.out = 3)),
  spline_knots_type = 'frequency',
  spline_l_linear = T,
  prior_spline_num_constraint_points = 100L,
  prior_spline_monotonicity = "decreasing",
  prior_spline_convexity = "concave"
)

# not including "indoor" because it is the reference

model <- mr$MRBRT(
  data = data,
  cov_models = list(linear_cov_model,
                    mr$LinearCovModel("intercept", use_re=F),
                    mr$LinearCovModel("crop", use_re=F),
                    mr$LinearCovModel("coal", use_re=F),
                    mr$LinearCovModel("dung", use_re=F),
                    mr$LinearCovModel("wood", use_re=F),
                    mr$LinearCovModel("group_female", use_re=F),
                    mr$LinearCovModel("group_male", use_re=F),
                    mr$LinearCovModel("group_child", use_re=F),
                    mr$LinearCovModel("measure_24hr", use_re=F)),
  inlier_pct = 1)

model$fit_model(inner_print_level = 5L, inner_max_iter = 50000L)

# get the coefficients for the appendix
# get the draws of the coef
n_samples <- as.integer(100)
samples <- model$sample_soln(
  sample_size = n_samples)

coef <- as.data.table(samples[[1]])

# remove the first 2 columns
coef <- coef[, -.(1,2)]

# rename the columns
new_names<-c("intercept","crop","coal","dung","wood","female","male","child","measure_hr")
colnames(coef) <- new_names

# Calculating mean, lower, and upper bounds
types <- c("mean", "lower", "upper")
final_coef <- data.table(type = rep(types, each = length(new_names)),
                       column = rep(new_names, times = length(types)))

for (col_name in new_names) {
  # Calculate statistics
  mean_val <- mean(coef[[col_name]], na.rm = T)
  lower_val <- quantile(coef[[col_name]], probs = 0.025, na.rm = T)
  upper_val <- quantile(coef[[col_name]], probs = 0.975, na.rm = T)

  # Fill in the values in the final data table
  final_coef[type == "mean", (col_name) := mean_val]
  final_coef[type == "lower", (col_name) := lower_val]
  final_coef[type == "upper", (col_name) := upper_val]
}

final_coef$column <- NULL

final_coef <- unique(final_coef)

# now add LDI
ldi_mean <- mean(samples[[2]])
ldi_lower <- quantile(samples[[2]], probs = 0.025, na.rm = T)
ldi_upper <- quantile(samples[[2]], probs = 0.975, na.rm = T)
ldi_vals <- c(ldi_mean,ldi_lower,ldi_upper)

ldi_tbl <- data.table(ldi=ldi_vals)

# cbind to the final_coef table
final_coef <- cbind(final_coef, ldi_tbl)

# save to file
write_excel_csv(final_coef, paste0(home_dir, "FILEPATH/crosswalk_coef.csv"))


# make a data.table to plot the model
long <- melt.data.table(dt, id.vars=c("location_name","location_id","region_name","super_region_name","ldi","pm_excess","measure_group","measure_24hr","sample_size"),
                        measure.vars=c("coal","wood","crop","dung","clean"), variable.name="fuel", value.name="fuel_weight")

# scale 1/2 fuel studies based on fuel type
long[fuel_weight==0.5 & fuel=="solid", pm_excess:=exp(log(pm_excess)+.5*(model$beta_soln[2]))]
long[fuel_weight==0.5 & fuel!="solid", pm_excess:=exp(log(pm_excess)-.5*(model$beta_soln[2]))]


# Prep square for predicting out later

df_preds <- data.table(expand.grid(crop=c(1,0),
                                   coal=c(1,0),
                                   dung=c(1,0),
                                   wood=c(1,0),
                                   group_female=1,
                                   group_male=0,
                                   group_child=0,
                                   measure_24hr=1,
                                   merge=1))

df_preds[, group_sum:=crop+coal+wood+dung]
df_preds <- df_preds[group_sum==1]  # only take the rows that are just for one fuel
df_preds[, group_sum:=NULL]

df_preds <- merge(df_preds, ldi, by="merge", allow.cartesian=T)
df_preds[, merge:=NULL]

dat_preds <- mr$MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = list("ldi","crop","coal","dung","wood","group_female","group_male","group_child","measure_24hr")

)

df_preds$preds <- model$predict(dat_preds)
df_preds <- as.data.table(df_preds)
df_preds[, pm_excess:=exp(preds)]

df_preds[crop==1, fuel:="crop"]
df_preds[coal==1, fuel:="coal"]
df_preds[dung==1, fuel:="dung"]
df_preds[wood==1, fuel:="wood"]

df_preds[group_female==1, measure_group:="female"]

##### diagnostics for GBD 2023
dt_plot <- dt

dt_plot[crop==1, fuel:="crop"]
dt_plot[coal==1, fuel:="coal"]
dt_plot[dung==1, fuel:="dung"]
dt_plot[wood==1, fuel:="wood"]

# drop rows with NA in fuel
dt_plot <- dt_plot[!is.na(fuel)]

dt_plot$source <- 'input data'

# plot ldi and pm_excess for predictions and input data
ggplot()+
  geom_line(data=df_preds, aes(x=ldi, y=pm_excess, color=fuel), size=1)+
  geom_point(data=dt_plot, aes(x=ldi, y=pm_excess, color=fuel), size=1)+
  ylim(0,max(df_preds$pm_excess))+
  theme_classic()

# save
ggsave(paste0(home_dir, "FILEPATH/ldi_pm_excess_model_", date,".png"), width=8, height=5)

# make draws
samples <- mrbrt002::core$other_sampling$sample_simple_lme_beta(sample_size = 1000L, model = model)

# generate table of betas for reporting
model_betas <- data.table(mean = apply(samples, 2, mean),
                          lower = apply(samples, 2, quantile, 0.025),
                          upper = apply(samples, 2, quantile, 0.975))

dat_preds <- mr$MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = list("ldi","crop","coal","dung","wood","group_female","group_male","group_child","measure_24hr")
)

gamma_draws <- matrix(rep(model$gamma_soln, times=1000), ncol = 1)

draws <- model$create_draws(
  data = dat_preds,
  beta_samples = samples,
  gamma_samples = gamma_draws,
  random_study = F
)

draws <- exp(draws)  # exp the draws, as they were modeled in log space
draws <- as.data.table(draws)
draws <- setNames(draws, c(paste0("draw_", 1:1000)))

# diagnostics for GBD 2023
row_means <- as.data.table(rowMeans(draws, na.rm = T))
row_medians <- as.data.table(apply(draws, 1, median, na.rm = T))

out <- cbind(df_preds,draws)
setnames(out, "pm_excess", "predict_pm")


# make preds for males and children
# read in the betas already made for the disaggregated mapping model
betas <- readRDS(paste0(home_dir, "FILEPATH/crosswalk_betas.RDS"))

# calculate pm estimates for male and child by scaling draws of female by draws of ratios

male <- copy(out)
male[, measure_group:="male"]
male[, c(draw_cols):=lapply(1:1000, function(x){exp(betas["groupmale", x]) * get(paste0("draw_",x))})]

child <- copy(out)
child[, measure_group:="child"]
child[, c(draw_cols):=lapply(1:1000, function(x){exp(betas["groupchild", x]) * get(paste0("draw_",x))})]

out <- rbind(out, male)
out <- rbind(out, child)
out <- setnames(out, "measure_group", "grouping")

#### UPDATES FOR GBD 2023
#### MODEL IS UNSTABLE AT HIGH LDI VALUES. REPLACING PREDICTIONS WITH LDI > 40,000 WITH PREDICTIONS AT LDI = 40,000
max_ldi <- 40000  # where we decided to cap the predictions
out_filter <- out %>% dplyr::filter(ldi < max_ldi)  # these will remain the same

new_max_ldi <- dplyr::filter(out_filter, ldi == max(out_filter$ldi)) %>%  # these will replace rows where ldi is greater than max_ldi
  dplyr::select(c('fuel', 'grouping', starts_with('draw_')))

out_greater_ldi <- out %>% dplyr::filter(ldi > max_ldi) %>%  # these draws will be replaced
  dplyr::select(-c(starts_with('draw_')))

# replacing the draws
out_greater_ldi <- merge(out_greater_ldi, new_max_ldi, by=c("fuel", "grouping"))

# combine to get final data
out_final <- rbind(out_filter, out_greater_ldi)

# check to make sure distribution looks okay
test <- dplyr::select(out_final, starts_with('draw_')) %>% rowMeans()
hist(test)

# save the draws
out <- out_final

# save all the fuel types (these take a long time)
crop_out <- out[fuel=="crop"]
write.csv(crop_out, file = paste0(home_dir, "/FILEPATH/", date, "/lm_pred_crop_", date, ".csv"), row.names = F)
print("Done saving crop!")

coal_out <- out[fuel=="coal"]
write.csv(coal_out, file = paste0(home_dir, "/FILEPATH/", date, "/lm_pred_coal_", date, ".csv"), row.names = F)
print("Done saving coal!")

dung_out <- out[fuel=="dung"]
write.csv(dung_out, file = paste0(home_dir, "/FILEPATH/", date, "/lm_pred_dung_", date, ".csv"), row.names = F)
print("Done saving dung!")

wood_out <- out[fuel=="wood"]
write.csv(wood_out, file = paste0(home_dir, "/FILEPATH/", date, "/lm_pred_wood_", date, ".csv"), row.names = F)
print("Done saving wood!")


# summary
crop_sum <- copy(crop_out)
crop_sum[, lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
crop_sum[, mean := apply(.SD, 1, mean), .SDcols=draw_cols]
crop_sum[, median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
crop_sum[, upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
crop_sum[, conf_int := upper-lower]
crop_sum[, c(draw_cols) :=NULL]
print("Done summarizing crop!")

coal_sum <- copy(coal_out)
coal_sum[, lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
coal_sum[, mean := apply(.SD, 1, mean), .SDcols=draw_cols]
coal_sum[, median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
coal_sum[, upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
coal_sum[, conf_int := upper-lower]
coal_sum[, c(draw_cols) :=NULL]
print("Done summarizing coal!")

dung_sum <- copy(dung_out)
dung_sum[, lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
dung_sum[, mean := apply(.SD, 1, mean), .SDcols=draw_cols]
dung_sum[, median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
dung_sum[, upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
dung_sum[, conf_int := upper-lower]
dung_sum[, c(draw_cols) :=NULL]
print("Done summarizing dung!")

wood_sum <- copy(wood_out)
wood_sum[, lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
wood_sum[, mean := apply(.SD, 1, mean), .SDcols=draw_cols]
wood_sum[, median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
wood_sum[, upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
wood_sum[, conf_int := upper-lower]
wood_sum[, c(draw_cols) :=NULL]
print("Done summarizing wood!")

# save summary files
write.csv(crop_sum, file = paste0(home_dir, "/FILEPATH/", date, "/lm_map_crop_", date, ".csv"), row.names = F)
print("Done saving crop!")
write.csv(coal_sum, file = paste0(home_dir, "/FILEPATH/", date, "/lm_map_coal_", date, ".csv"), row.names = F)
print("Done saving coal!")
write.csv(dung_sum, file = paste0(home_dir, "/FILEPATH/", date, "/lm_map_dung_", date, ".csv"), row.names = F)
print("Done saving dung!")
write.csv(wood_sum, file = paste0(home_dir, "/FILEPATH/", date, "/lm_map_wood_", date, ".csv"), row.names = F)
print("Done saving wood!")

summary <- rbindlist(list(coal_sum, crop_sum, dung_sum, wood_sum), use.names = T)

summary_female <- dplyr::filter(summary, grouping == 'female')

# plot summary means with confidence intervals
ggplot(summary_female, aes(x=ldi, y=mean)) +
  geom_point(size = 0.5, color = 'red') +
  facet_wrap(~fuel, scales="free_y") +
  labs(title="Mean PM Excess by LDI for Females with 95% Confidence Intervals", x="LDI", y="Mean PM Excess") +
  theme_minimal()

ggsave(paste0(home_dir, "/FILEPATH/ldi_pm_excess_median_draws_female_", date, ".png"), width=8, height=5)

ggplot(summary_female, aes(x=ldi, y=median)) +
  geom_point(size = 0.5, color = 'red') +
  facet_wrap(~fuel, scales="free_y") +
  labs(title="Median PM Excess by LDI for Females with 95% Confidence Intervals", x="LDI", y="Median PM Excess") +
  theme_minimal()

ggsave(paste0(home_dir,"/FILEPATH/ldi_pm_excess_mean_draws_female_",date,".png"), width=8, height=5)

# compare with GBD 2021 here they are...
# old output data
old_crop <- fread('FILEPATH/lm_map_crop_121620.csv')
old_coal <- fread('FILEPATH/lm_map_coal_121620.csv')
old_dung <- fread('FILEPATH/lm_map_dung_121620.csv')
old_wood <- fread('FILEPATH/lm_map_wood_121620.csv')

old_data <- rbind(old_crop, old_coal, old_dung, old_wood)
rm(old_crop, old_coal, old_dung, old_wood)


ggplot(data = summary[year_id %in% c(1990,2005,2019,2022,2023) & location_id<500], aes(x=ldi, y=mean, ymin=lower, ymax=upper, color=grouping, fill=grouping)) + geom_point() + geom_ribbon(alpha=.2) + scale_y_log10()

# Compare all fuel types
plot <- ggplot(summary[grouping!="indoor" & year_id%in%c(1990,2005,2019,2022, 2023) & location_id <500], aes(x=ldi, y=mean, ymin=lower, ymax=upper, color=grouping, fill=grouping)) +
  geom_point()+
  geom_ribbon(alpha=0.2) +
  facet_wrap(~fuel,nrow=1) +
  xlim(0,15000)+
  ylim(0,1660)+
  labs(title="Fuel-type mapping values", x="LDI", y="Median PM2.5 exposure")
print(plot)

# Compare to GBD 2019

gbd20 <- fread("FILEPATH/lm_map_121620.csv")

plot <- merge(gbd20, summary, by=c("location_id", "year_id", "grouping"))
plot <- plot[year_id %in% c(1990,2000,2010,2020,2021,2022,2023)]
ggplot(plot[grouping != "indoor"],aes(x=median.x, y = median.y))+
  geom_point() + geom_abline(slope=1, intercept=0)+
  facet_wrap(~grouping) +
  labs(title="GBD23 vs GBD21 HAP Mapping Values", x="GBD 2021", y="GBD 2023")

dev.off()