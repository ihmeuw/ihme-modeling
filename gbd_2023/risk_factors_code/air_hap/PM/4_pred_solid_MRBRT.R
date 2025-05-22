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
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","ggplot2","openxlsx","metafor","pbapply","Metrics","car")

for(p in packages){
  if(p %in% rownames(installed.packages()) == F){
    install.packages(p)
  }
  library(p, character.only = T)
}

'%ni%' <- Negate("%in%")

in_date <- "082124"
date <- format(Sys.Date(), "%m%d%y")
disagg <- F

draw_cols <- paste0("draw_",1:1000)

#------------------Directories and shared functions-----------------------------

home_dir <- file.path("FILEPATH")
in_dataset <- paste0(home_dir, "/FILEPATH/lmer_input_", in_date, ".csv")

# out
dir.create(paste0(home_dir, "/FILEPATH/", date), recursive=T)
dir.create(paste0(home_dir, "/FILEPATH/", date), recursive=T)

# central functions
source(file.path(central_lib, "/FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(location_set_id=35, gbd_round_id=9, release_id = 16)
source(file.path(central_lib, "/FILEPATH/get_covariate_estimates.R"))

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
# dt[,weight:=1/sample_size]

# add on
ldi <- get_covariate_estimates(covariate_id = 57,
                               location_id = c(unique(dt$location_id)),
                               year_id = c(unique(dt$year_id)),
                               location_set_id = 35,
                               gbd_round_id = 9,
                               release_id = 16)
setnames(ldi, "mean_value", "ldi")

dt <- merge(dt, ldi[, .(location_id, year_id, ldi)], by=c("location_id", "year_id"), all.x=T)


ldi <- get_covariate_estimates(covariate_id = 57,
                               year_id = 1990:2023,
                               location_id = locs[level>=3 & level==0, location_id],
                               location_set_id = 35,
                               gbd_round_id = 9,
                               release_id = 16)

ldi <- ldi[, .(location_id, year_id, ldi=mean_value)]
ldi <- ldi[, merge:=1]


# load MR-BRT functions
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")

test_dir <- "FILEPATH"
dir.create(test_dir, recursive=T)

pdf(file = paste0(test_dir, "/FILEPATH", date, ".pdf"), width=8, height=5, onefile = T)


# -------------------------  Run log-linear model to get solid preds ----------------------------------------------------------------------------------

# load the data frame
data <-  mr$MRData()

data$load_df(
  dt,
  col_obs = "log_pm_excess",
  col_obs_se = "weight",
  col_covs = list("ldi","solid","crop","coal","dung","wood",
                  "group_female","group_male","group_child","measure_24hr"),
  col_study_id = "S.No")

linear_cov_model = mr$LinearCovModel(
  alt_cov = c("ldi"),
  use_re = T,
  use_spline = T,
  use_re_mid_point = T,
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
                    mr$LinearCovModel("solid", use_re=F),
                    mr$LinearCovModel("group_female", use_re=F),
                    mr$LinearCovModel("group_male", use_re=F),
                    mr$LinearCovModel("group_child", use_re=F),
                    mr$LinearCovModel("measure_24hr", use_re=F)),
  inlier_pct = 1)

model$fit_model(inner_print_level = 5L, inner_max_iter = 50000L)

# make a data.table to plot
long <- melt.data.table(dt, id.vars=c("location_name","location_id","region_name","super_region_name","ldi","pm_excess","measure_group","measure_24hr","sample_size","S.No"),
                        measure.vars=c("clean","solid"), variable.name="fuel", value.name="fuel_weight")

# scale 1/2 fuel studies based on fuel type
long[fuel_weight==0.5 & fuel=="solid", pm_excess:=exp(log(pm_excess)+.5*(model$beta_soln[2]))]
long[fuel_weight==0.5 & fuel!="solid", pm_excess:=exp(log(pm_excess)-.5*(model$beta_soln[2]))]

# Prep square for predicting out later

df_preds <- data.table(expand.grid(solid=1,
                                   group_female=c(1,0),
                                   group_male=c(1,0),
                                   group_child=c(1,0),
                                   measure_24hr=1,
                                   merge=1))

df_preds[, group_sum:=group_female+group_male+group_child]
df_preds <- df_preds[group_sum<=1]  # only take the rows that are just for one grouping or for indoor
df_preds[, group_sum:=NULL]

df_preds <- merge(df_preds, ldi, by="merge", allow.cartesian=T)

dat_preds <- mr$MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = list("ldi","solid","group_female","group_male","group_child","measure_24hr")
)

df_preds$preds <- model$predict(dat_preds)
df_preds <- as.data.table(df_preds)
df_preds[, pm_excess:=exp(preds)]

df_preds[group_female==1, measure_group:="female"]
df_preds[group_male==1, measure_group:="male"]
df_preds[group_child==1, measure_group:="child"]
df_preds[group_female==0&group_male==0&group_child==0, measure_group:="indoor"]
df_preds[, fuel:="solid"]

long[, new:=ifelse(S.No==509,1,0)]

ggplot()+
  geom_line(data=df_preds[measure_group=="female"], aes(x=ldi,y=pm_excess,color=measure_group)) +
  geom_line(data=df_preds[measure_group=="male"], aes(x=ldi,y=pm_excess,color=measure_group)) +
  geom_line(data=df_preds[measure_group=="child"], aes(x=ldi,y=pm_excess,color=measure_group)) +
  geom_line(data=df_preds[measure_group=="indoor"], aes(x=ldi,y=pm_excess,color=measure_group)) +
  geom_point(data=long, aes(x=ldi, y=pm_excess, color=measure_group, alpha=fuel_weight, shape=as.factor(new), size=sample_size)) +
  guides(alpha = "none", color = "legend", shape = "none", size = "none") +
  scale_y_log10(limits=c(0.1,30000)) +
  xlim(0,15000) +
  scale_alpha(range=c(0,1)) +
  scale_shape_manual(values=c(1,19)) +
  theme_classic() +
  labs(title="log(pm_excess) ~ ldi, solid, measure_group, measure_24hr (fixed effects) + S.No (random effects))")


# make draws
samples <- mrbrt002::core$other_sampling$sample_simple_lme_beta(sample_size = 1000L, model = model)

# make draws (we only want female for this)
df_preds <- df_preds[group_female == 1]

dat_preds <- mr$MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = list("ldi","solid","group_female","group_male","group_child","measure_24hr")
)

gamma_draws <- matrix(rep(model$gamma_soln, times=1000), ncol = 1)

draws <- model$create_draws(
  data = dat_preds,
  beta_samples = samples,
  gamma_samples = gamma_draws,
  random_study = F
)

draws <- exp(draws)  # exp the draws
draws <- as.data.table(draws)
draws <- setNames(draws,c(paste0("draw_", 1:1000)))

# diagnostics for GBD 2023
row_means <- as.data.table(rowMeans(draws, na.rm = T))
row_medians <- as.data.table(apply(draws, 1, median, na.rm = T))

out <- cbind(df_preds,draws)
setnames(out, "pm_excess", "predict_pm")


# make preds for males and children
# read in the betas you already made for the disaggregated mapping model
betas <- readRDS(paste0(home_dir, "/FILEPATH/crosswalk_betas.RDS"))

# calculate pm estimates for male and child by scaling draws of female by draws of ratios

male <- out[measure_group=="female"]
male[, measure_group:="male"]
male[, c(draw_cols):=lapply(1:1000, function(x){exp(betas["groupmale", x]) * get(paste0("draw_",x))})]

child <- out[measure_group=="female"]
child[, measure_group:="child"]
child[, c(draw_cols):=lapply(1:1000, function(x){exp(betas["groupchild", x]) * get(paste0("draw_",x))})]

out <- rbind(out, male)
out <- rbind(out, child)
out <- setnames(out, "measure_group", "grouping")
names(out)  # check for all the required column names

#### UPDATES FOR GBD 2023
#### MODEL IS UNSTABLE AT HIGH LDI VALUES. REPLACING PREDICTIONS WITH LDI > 40,000 WITH PREDICTIONS AT LDI = 40,000
max_ldi <- 40000   # where we decided to cap the predictions
out_filter <- out %>% dplyr::filter(ldi < max_ldi)  # these will remain the same

new_max_ldi <- dplyr::filter(out_filter, ldi == max(out_filter$ldi)) %>%  # these will replace rows where ldi is greater than max_ldi
  dplyr::select(c('fuel', 'grouping', starts_with('draw_')))

out_greater_ldi <- out %>% dplyr::filter(ldi > max_ldi) %>%  # these draws will be replaced
  dplyr::select(-c(starts_with('draw_')))

# replacing the draws
out_greater_ldi <- merge(out_greater_ldi, new_max_ldi, by = c("fuel", "grouping"))

# combine to get final data
out_final <- rbind(out_filter, out_greater_ldi)

# check to make sure distribution looks okay
test <- dplyr::select(out_final, starts_with('draw_')) %>% rowMeans()
hist(test)

# save the draws
out <- out_final

write.csv(out, file = paste0(home_dir, "/FILEPATH/", date, "/lm_pred_", date, ".csv"), row.names = F)

# make a summary file
summary <- copy(out)
summary[, lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
summary[, mean := apply(.SD, 1, mean), .SDcols=draw_cols]
summary[, median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
summary[, upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
summary[, conf_int := upper-lower]
summary[, c(draw_cols) :=NULL]

ggplot(data=summary[year_id %in% c(1990,2005,2019,2022, 2023) & location_id<500],
       aes(x=ldi, y=mean, ymin=lower, ymax=upper, color=grouping, fill=grouping))+
  geom_point() +
  geom_ribbon(alpha=.2) +
  ylim(0, 920) +
  xlim(0, 15000)

write.csv(summary, file = paste0(home_dir, "/FILEPATH/", date, "/lm_map_", date, ".csv"), row.names = F)

# make more diagnostic plots
# Compare to GBD 2019

gbd20 <- fread("FILEPATH/lm_map_122220.csv")
gbd23 <- fread("FILEPATH/lm_map_082124.csv")

plot <- merge(gbd20,gbd23,by=c("location_id","year_id","grouping"))
plot <- plot[year_id %in% c(1990, 2000, 2019, 2020, 2021, 2022, 2023)]

plot <- merge(plot, locs,by="location_id")

ggplot(plot, aes(x=median.x, y=median.y, color=super_region_name, label=ihme_loc_id))+
  geom_point()+geom_abline(slope=1, intercept=0)+
  facet_wrap(~grouping) +
  theme_bw() +
  labs(title="HAP map comparison to previous model", x="GBD 2021", y="GBD 2023")


dev.off()