# Author: USERNAME
# Date: DATE
# Description: BRFSS prevalence to mean xwalk

# ==============================================================================

rm(list=ls())

# Packages
library(data.table)
library(ggplot2)
library(usmap)
library(gridExtra)
library(RMySQL)
library(openxlsx)

# Shared functions
central <- "/FILEPATH/"
source(paste0(central, 'get_location_metadata.R'))
source(paste0(central, 'get_bundle_data.R'))

# Parameters
release_id <- 16
me <- "sbp" # "sbp" or "chl"

# Filepaths
collapsed_data_path <- paste0("/FILEPATH/")
plot_output <- paste0("/FILEPATH/")

##############################################################################################################
################################## # Load data ###############################################################
##############################################################################################################

collapse_output_cols <- c("mean", "standard_error", "design_effect", "sample_size", "nclust", "nstrata")

# load NHANES
nhanes <- rbindlist(lapply(list.files(collapsed_data_path, pattern='nhanes', full.names = T), fread))
setnames(nhanes, ifelse(me=='sbp', 'bp_group', 'chol_group'), 'diag_group')
nhanes[var=='sbp_mean_1_2_3', var := 'sbp']
nhanes[var==ifelse(me=='sbp', 'high_bp', 'high_chol'), var := 'prevalence']
nhanes <- dcast(nhanes, as.formula(paste0(paste0(setdiff(names(nhanes), c(collapse_output_cols, 'var')), collapse = "+"), " ~ var")), 
                value.var = collapse_output_cols)
setnames(nhanes, names(nhanes), gsub("mean_", "", names(nhanes)))
nhanes[, ages_grouped := ifelse(age_start < 80, age_start, 80)]

# load BRFSS
brfss <- rbindlist(lapply(list.files(collapsed_data_path, pattern='brfss', full.names = T), fread))
brfss <- brfss[ihme_loc_id != 'USA']
setnames(brfss, ifelse(me=='sbp', 'bp_group', 'chol_group'), 'diag_group')
brfss[var==ifelse(me=='sbp', 'high_bp', 'high_chol'), var := 'prevalence']
demo_groups <- setdiff(names(brfss), c(collapse_output_cols, 'var'))
brfss <- dcast(brfss, as.formula(paste0(paste0(demo_groups, collapse = "+"), " ~ var")), 
               value.var = collapse_output_cols)
setnames(brfss, names(brfss), gsub("mean_", "", names(brfss)))
brfss[, ages_grouped := ifelse(age_start < 80, age_start, 80)]

# load location metadata
loc_meta <- get_location_metadata(22, release_id = release_id)

##############################################################################################################
################################## # Fit model ###############################################################
##############################################################################################################

pdf(paste0(plot_output, "nhanes_", me, "_model_fit.pdf"), width=12, height=7)

# Data plots
p<-ggplot(data=nhanes, aes(x=prevalence, y=get(me), color=factor(ages_grouped)))+
  geom_point()+
  geom_line(stat="smooth", method = "lm", alpha=.3, se=F)+
  facet_wrap(diag_group~factor(sex_id, levels=c(1,2), labels=c("Male", "Female")), scales='free') +
  theme_classic()+
  labs(title = paste0("Prevalence of high ", me, " vs mean ", me, " in NHANES"), color='Age group',
       x = paste0("Measured prevalence of high ", me), y = paste0("Measured mean ", me))
print(p)

# OOS RMSE function
run.oos.rmse <- function(df, prop_train, model, reps) {
  lapply(1:reps, function(x) {
    # Split
    set.seed(80)
    train_index <- sample(seq_len(nrow(df)), size = floor(prop_train * nrow(df)))
    train <- df[train_index]
    test <- df[-train_index]
    # Model
    test_mod <- lm(model, data=train)
    # Predict
    prediction <- predict(test_mod, newdata=test, allow.new.levels=TRUE)
    # RMSE
    var <- paste0(model[[2]])
    rmse <- sqrt(mean((prediction-test[[var]])^2, na.rm=T))
    return(rmse)
  }) %>% unlist %>% mean
}

xwalk_mods <- NULL
form <- as.formula(paste0(me, " ~ prevalence + ages_grouped + prevalence*ages_grouped"))

for(sx in unique(nhanes$sex_id)){
  for(gp in unique(nhanes$diag_group)){
    
    message(paste0('\n', gp, ', ', ifelse(sx==1, 'Males', 'Females'), ':'))
    
    x <- nhanes[sex_id==sx & diag_group == gp & !is.na(get(me)),]
    x[, wt := get(paste0('sample_size_', me))]
    
    mod <- lm(form, data=x, weights=wt)
    print(summary(mod))
    xwalk_mods[[paste0(sx)]][[paste0(gp)]]  <- mod
    
    prediction <- predict(mod, interval="confidence", level=0.95)
    x <- cbind(x, prediction)
    
    x[, resid := get(me)-fit]
    rmse <- sqrt(mean((x$resid)^2, na.rm=T))
    oos.rmse <- run.oos.rmse(df=x, prop_train=0.8, model=form, reps=10)
    print(round(oos.rmse, 2))
    
    p <- ggplot(data=x, aes(x=prevalence, y=get(me), color=factor(ages_grouped))) +
      geom_point() + geom_line(aes(y=fit)) + theme_classic() +
      geom_ribbon(aes(ymin=lwr, ymax=upr, fill=factor(ages_grouped)), color=NA, alpha=0.1) +
      labs(title = paste0('Model fit on NHANES data for ', gp, ', ', ifelse(sx==1, 'Males', 'Females')), 
           subtitle = paste0('In-sample RMSE: ', round(rmse, 2), ' | Out-of-sample RMSE: ', round(oos.rmse, 2)),
           color = 'Age group', fill = 'Age group', x = paste0("Measured prevalence of high ", me), y = paste0("Measured mean ", me))
    print(p)
    
    p <- ggplot(data=x, aes(x=prevalence, y=resid, color=factor(ages_grouped))) +
      geom_point() + theme_classic() +
      geom_hline(yintercept=0, linetype="dotted") +
      labs(title = paste0('Residuals for ', gp, ', ', ifelse(sx==1, 'Males', 'Females')), 
           subtitle = paste0('In-sample RMSE: ', round(rmse, 2), ' | Out-of-sample RMSE: ', round(oos.rmse, 2)),
           color = 'Age group', fill = 'Age group', x = paste0("Measured prevalence of high ", me), y = paste0("Measured mean ", me, " - model fit"))
    print(p)
    
  }
}

dev.off()

# save coefficients
coefs <- rbindlist(lapply(1:2, function(i) 
  rbindlist(lapply(xwalk_mods[[i]], function(x) as.data.table(as.list(coef(x)))), 
            idcol='diagnosis_group')), 
  idcol='sex_id')
write.csv(coefs, paste0(plot_output, me, '_model_coefs.csv'), row.names = F)

##############################################################################################################
################################## # Crosswalk BRFSS #########################################################
##############################################################################################################

pdf(paste0(plot_output, "brfss_", me, "_model_results.pdf"), width=12, height=7)

xwalked_brfss <- data.table()

for(sx in unique(brfss$sex_id)){
  for(gp in unique(brfss$diag_group)){
    
    message(paste0('\n', gp, ', ', ifelse(sx==1, 'Males', 'Females')))
    
    x <- brfss[sex_id==sx & diag_group == gp,]
    x[, wt := sample_size_prevalence]
    
    mod <- xwalk_mods[[sx]][[gp]]
    
    pred <- predict.lm(mod, x, interval="prediction", level=0.95, weights=x$wt)
    x <-cbind(x, pred)
    
    p <- ggplot(data=x, aes(x=prevalence, y=fit, color=as.factor(ages_grouped))) +
      geom_point() +
      geom_errorbar(aes(ymin=lwr, ymax=upr), alpha=0.025) +
      labs(title = paste0('Predicted BRFSS: ', gp, ', ', ifelse(sx==1, 'Males', 'Females')),
           x = paste0("Corrected prevalence of high ", me), y = paste0("Modeled mean ", me),
           color = 'Age group') +
      theme_minimal()
    print(p)
    
    p <- ggplot(data=x, aes(x=fit, fill=as.factor(ages_grouped))) +
      geom_density(alpha=0.5) +
      theme_classic() +
      labs(title = paste0('Distribution by age group: ', gp, ', ', ifelse(sx==1, 'Males', 'Females')), x = paste0('Mean ', me),
           fill = 'BRFSS', color = 'NHANES') + theme(legend.box = "horizontal") +
      geom_density(data=nhanes[sex_id==sx & diag_group==gp], fill=NA, aes(x=get(me), color=as.factor(ages_grouped)))
    print(p)

    p <- ggplot(data=x, aes(x=fit, fill='BRFSS')) +
      geom_density(alpha=0.5) +
      theme_classic() +
      labs(title = paste0('Overall distribution: ', gp, ', ', ifelse(sx==1, 'Males', 'Females')), x = paste0('Mean ', me), fill=NULL) +
      geom_density(data=nhanes[sex_id==sx & diag_group==gp], aes(x=get(me), fill='NHANES'), alpha=0.5)
    print(p)
    
    xwalked_brfss <- rbind(xwalked_brfss, x)
    
  }
}

xwalked_brfss[, mean := fit]
xwalked_brfss[, sample_size := sample_size_prevalence]
xwalked_brfss[, standard_error := (upr-lwr)/(qnorm(0.975)*2)]
xwalked_brfss[, terminal_age := max(age_end), by='nid']
xwalked_brfss[year_start >= 2013 & age_end == terminal_age, age_end := max(xwalked_brfss$age_end)]

# collapse across diagnosed/undiagnosed groups
nhanes_collapsed <- nhanes[, list(mean = weighted.mean(get(paste0(me)), get(paste0("sample_size_", me)))),
                           by = setdiff(demo_groups, 'diag_group')]

final <- xwalked_brfss[, list(mean = weighted.mean(mean, sample_size),
                              standard_error = sqrt(sum(standard_error^2)),
                              sample_size = sum(sample_size)),
                       by = setdiff(demo_groups, 'diag_group')]

# Density plots by label
p <- ggplot(xwalked_brfss, aes(x = mean, fill = diag_group)) + geom_density(alpha=0.5) +
  facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Males", "Females"))) + 
  theme_bw() + labs(x=paste0('Mean ', me), fill="BRFSS", color="NHANES",
                    title='Distribution by diagnosis group and sex') +
  geom_density(data=final, aes(x=mean, fill='All'), alpha=0.5) +
  geom_density(data=nhanes, aes(x=get(me), color=diag_group, fill=NULL)) +
  geom_density(data=nhanes_collapsed, aes(x=mean, color='All', fill=NULL))
print(p)

# Density plots by age
p <- ggplot(final, aes(x = mean, fill = factor(age_start))) + geom_density(alpha=0.5) +
  facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Males", "Females"))) + 
  theme_bw() + labs(x=paste0('Mean ', me), fill="BRFSS", color="NHANES",
                    title='Distribution by age start and sex') +
  geom_density(data=nhanes_collapsed, aes(color=factor(age_start), fill=NULL)) +
  theme(legend.box = "horizontal")
print(p)

# Age series
plot_dt <- rbind(
  xwalked_brfss[, list(source = 'BRFSS', ihme_loc_id = 'USA',
    mean = weighted.mean(mean, sample_size)),
                by = c('ages_grouped', 'sex_id')],
  nhanes[, list(source = 'NHANES', ihme_loc_id = 'USA',
    mean = weighted.mean(get(paste0(me)), get(paste0("sample_size_", me)))),
    by = c('ages_grouped', 'sex_id')]
)

p <- ggplot(plot_dt, aes(x=factor(ages_grouped), y=mean, color=source, group=source)) + 
  geom_line() + facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Males", "Females"))) + 
  theme_bw() + labs(x="Age group", y=paste0('Mean ', me), color=NULL, 
                    title=paste0("Predicted vs measured ", me, ", averaged by age and sex"))
print(p)

plot_dt <- rbind(plot_dt,
                 xwalked_brfss[, list(source = 'BRFSS',
                                      mean = weighted.mean(mean, sample_size)),
                               by = c('ages_grouped', 'sex_id', 'ihme_loc_id')]
)

p <- ggplot(plot_dt[ihme_loc_id != 'USA'], aes(x=factor(ages_grouped), y=mean, linetype=source, color=ihme_loc_id, group=interaction(source, ihme_loc_id))) + 
  geom_line() + facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Males", "Females")), scales='free_y') + guides(color="none") +
  theme_bw() + labs(x="Age group", y=paste0('Mean ', me), color=NULL, linetype=NULL,
                    title=paste0("Predicted vs measured ", me, ", averaged by age, sex, and state"), caption=paste0('National-level ', me, ' shown in black')) +
  geom_line(data=plot_dt[ihme_loc_id == 'USA'], color='black', linewidth=1)
print(p)

# Year series
plot_dt <- rbind(
  xwalked_brfss[, list(source = 'BRFSS', ihme_loc_id = 'USA',
                       mean = weighted.mean(mean, sample_size)),
                by = c('year_start', 'sex_id')],
  nhanes[, list(source = 'NHANES', ihme_loc_id = 'USA',
                mean = weighted.mean(get(paste0(me)), get(paste0("sample_size_", me)))),
         by = c('year_start', 'sex_id')]
)

p <- ggplot(plot_dt, aes(x=year_start, y=mean, color=source, group=source)) + 
  geom_line() + facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Males", "Females"))) + 
  theme_bw() + labs(x="Year start", y=paste0('Mean ', me), color=NULL, 
                    title=paste0("Predicted vs measured ", me, ", averaged by year and sex"))
print(p)

plot_dt <- rbind(plot_dt,
                 xwalked_brfss[, list(source = 'BRFSS',
                                      mean = weighted.mean(mean, sample_size)),
                               by = c('year_start', 'sex_id', 'ihme_loc_id')]
)

p <- ggplot(plot_dt[ihme_loc_id != 'USA'], aes(x=year_start, y=mean, linetype=source, color=ihme_loc_id, group=interaction(source, ihme_loc_id))) + 
  geom_line() + facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Males", "Females")), scales='free_y') + guides(color="none") +
  theme_bw() + labs(x="Yeart start", y=paste0('Mean ', me), color=NULL, linetype=NULL,
                    title=paste0("Predicted vs measured ", me, ", averaged by age, sex, and state"), caption=paste0('National-level ', me, ' shown in black')) +
  geom_line(data=plot_dt[ihme_loc_id == 'USA'], color='black', linewidth=1)
print(p)

# Maps
brfss_plot <- xwalked_brfss[!ihme_loc_id %in% c('USA', 'USA_531'), 
                            list(mean = weighted.mean(mean, sample_size)),
                            by = c('ihme_loc_id', 'sex_id')]
brfss_plot <- merge(brfss_plot, loc_meta[,.(ihme_loc_id, location_name)], by='ihme_loc_id')
setnames(brfss_plot, 'location_name', 'state')

p_sx1 <- plot_usmap(data = brfss_plot[sex_id==1], values = "mean") +
  theme(legend.position = "right") + 
  scale_fill_distiller(palette='RdYlBu', direction=-1) +
  labs(title=paste0("Males: BRFSS modeled estimates, averaged by sex and state"), 
       fill=me)

p_sx2 <- plot_usmap(data = brfss_plot[sex_id==2], values = "mean") +
  theme(legend.position = "right") + 
  scale_fill_distiller(palette='RdYlBu', direction=-1) +
  labs(title=paste0("Females: BRFSS modeled estimates, averaged by sex and state"), 
       fill=me)

p <- grid.arrange(p_sx1, p_sx2)
print(p)

# Compare to data currently in bundle
if(me=='sbp'){
  bundle <- get_bundle_data(4787)
} else {
  bundle <- get_bundle_data(4904)
}

bundle_brfss <- bundle[data_type==3 & age_start_orig >= 20,.(nid, ihme_loc_id, year_start, year_end, sex, age_start_orig, age_end_orig, val, sample_size, standard_error, variance)]
setnames(bundle_brfss, c('age_start_orig', 'age_end_orig', 'val'), c('age_start', 'age_end', 'mean'))
bundle_brfss[, sex_id := ifelse(sex=='Male', 1, 2)]

comp <- merge(final, bundle_brfss, by=c('nid', 'ihme_loc_id', 'year_start', 'year_end', 'sex_id', 'age_start'), suffixes = c('_new', '_old'))
ggplot(comp, aes(x=mean_old, y=mean_new, color=as.factor(age_start))) + 
  facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Male", "Female"))) + geom_point() + theme_bw() + geom_abline(slope=1, intercept=0) + 
  labs(x=paste0('BRFSS ', me, ' data currently in the bundle'), y=paste0('New BRFSS ', me, ' data'), color='Age start',
       caption='NHANES mean values showed along plot margins', title=paste0('Survey years: ', paste0(sort(unique(comp$year_start)), collapse=', '))) +
  geom_rug(data=nhanes[year_start >= min(comp$year_start) & year_start <= max(comp$year_start)], aes(x=get(me), y=get(me)))
  
bundle_brfss[, ages_grouped := ifelse(age_start < 80, age_start, 80)]
plot_dt <- rbind(
  bundle_brfss[, list(source = 'BRFSS:\nData currently in the bundle',
                    mean = weighted.mean(mean, standard_error)),
               by = c('ages_grouped', 'sex_id')],
  xwalked_brfss[year_start >= min(comp$year_start) & year_start <= max(comp$year_start), 
                list(source = 'BRFSS:\nNew data',
                     mean = weighted.mean(mean, standard_error)),
                by = c('ages_grouped', 'sex_id')],
  nhanes[year_start >= min(comp$year_start) & year_start <= max(comp$year_start), 
         list(source = 'NHANES', mean = weighted.mean(get(paste0(me)), get(paste0("standard_error_", me)))),
         by = c('ages_grouped', 'sex_id')]
)

p <- ggplot(plot_dt, aes(x=factor(ages_grouped), y=mean, color=source, group=source)) + 
  geom_line() + facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Males", "Females"))) + 
  theme_bw() + labs(x="Age group", y=paste0('Mean ', me), color=NULL, 
                    title=paste0("Predicted vs measured ", me, ", averaged by age and sex"),
                    subtitle=paste0('Survey years: ', paste0(sort(unique(comp$year_start)), collapse=', ')))
print(p)

# Scatter mean estimates vs raw, self-reported prevalence
brfss_path <- "/FILEPATH/"
brfss_micro <- data.table()
brfss_files <- list.files(brfss_path)
for(file in brfss_files){
  print(paste0(file, ' | ', which(file==brfss_files), ' out of ', length(brfss_files)))
  temp <- as.data.table(read_dta(paste0(brfss_path, file)))
  temp <- temp[,(intersect(c('nid', 'admin_1_id', 'year_start', 'year_end', 'pweight', 'sex_id', 'age_year', 'hypertension', 'hyperchol', 'ever_check_chol'), names(temp))), with=F]
  brfss_micro <- rbind(brfss_micro, temp, fill=T)
  rm(temp)
}
brfss_micro[ever_check_chol == 0, hyperchol := 0]
brfss_micro <- brfss_micro[age_year >= 20 & age_year <= 125 & sex_id %in% c(1, 2)]
brfss_micro[, age_start := floor(age_year/5)*5]
setnames(brfss_micro, ifelse(me=='sbp', 'hypertension', 'hyperchol'), 'prevalence')
brfss_micro_collapsed <- brfss_micro[, list(prevalence = weighted.mean(prevalence, pweight, na.rm=T)),
                                     by = c('nid', 'age_start', 'sex_id', 'year_start', 'admin_1_id')]

plot_dt <- merge(final, brfss_micro_collapsed, by.x = c('nid', 'age_start', 'sex_id', 'year_start', 'ihme_loc_id'), by.y = c('nid', 'age_start', 'sex_id', 'year_start', 'admin_1_id'))

p <- ggplot(plot_dt, aes(x=prevalence, y=mean, color=as.factor(age_start))) + 
  facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Male", "Female"))) + geom_point() + theme_bw() + 
  labs(x=paste0('BRFSS raw, self-reported prevalence of high ', me), y=paste0('BRFSS modeled mean ', me), color='Age start')
print(p)

plot_dt <- plot_dt[, list(mean = weighted.mean(mean, sample_size), prevalence = weighted.mean(prevalence, sample_size)),
                   by = c('age_start', 'sex_id')]
plot_dt <- melt(plot_dt, id.vars = c('age_start', 'sex_id'))

p <- ggplot(plot_dt, aes(x=factor(age_start), y=value, color=factor(sex_id, levels=c(1,2), labels=c("Males", "Females")), group = interaction(variable, sex_id))) + 
  geom_line() + facet_wrap(~variable, scales='free_y') + 
  theme_bw() + labs(x="Age group", y=paste0(me), color=NULL, title=paste0('BRFSS raw, self-reported prevalence of high ', me, ' vs modeled mean ', me))
print(p)

dev.off()

##############################################################################################################
######################################## # Save data #########################################################
##############################################################################################################

# check for and drop duplicates
################################
if(nrow(unique(final))!=nrow(final)){
  message('Duplicates detected within newly extracted data and will be removed:')
  dups <- final[duplicated(final)]
  print(dups[, .N, by=c('ihme_loc_id', 'nid')])
  final <- unique(final)
}

# convert sex_id to sex
################################
final[sex_id == 1, sex := "Male"]
final[sex_id == 2, sex := "Female"]
final[, sex_id := NULL]

# add age_group_ids
################################
final[, age_start_orig := age_start]
final[, age_end_orig := age_end]

db_con <- read.csv("/FILEPATH/database_info.csv",
                   stringsAsFactors = FALSE)

con <- dbConnect(dbDriver("MySQL"),
                 username = db_con$user,
                 password = db_con$password,
                 host = "ADDRESS")

a <- as.data.table(dbGetQuery(con, 'QUERY'))
dbDisconnect(con)

age_ids <- a[last_updated_action!='DELETE', c("age_group_id", "age_group_years_start", "age_group_years_end"), with = FALSE]
setnames(age_ids, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
# adjust for age_demographer
age_ids[age_end <= 1 & age_end > 0, age_end := age_end - .001]
age_ids[age_end > 1, age_end := age_end - 1]
# remove duplicate age_start and age_end combinations
age_ids[, age_range := paste0(age_start, '-', age_end)]
age_ids <- age_ids[!duplicated(age_ids$age_range)]

orig_nrow <- nrow(final)
final <- merge(final, age_ids[,.(age_start, age_end, age_group_id)], by = c('age_start', 'age_end'))
if(nrow(final)!=orig_nrow){
  stop(paste0((nrow(final)-orig_nrow), ' rows lost/gained when adding age_id column'))
} else {
  message('Successfully mapped age_group_id to all rows of data')
}

# add year ids
################################
final[, year_start_orig := year_start]
final[, year_end_orig := year_end]
final[, year_id := floor((year_start+year_end)/2)]

# add location ids
################################
final <- merge(final, loc_meta[, .(ihme_loc_id, location_id, location_name)], by = "ihme_loc_id")
final[, c("representative_name", "urbanicity_type") := list("Representative for subnational location only", "Mixed/both")]

# Final formatting
################################
final[, extractor := Sys.info()[["user"]]]
final[, note_SR := paste0("BRFSS xwalk performed on ", Sys.Date())]
final[, measure := "continuous"]
final[, c("seq", "underlying_nid") := NA]
final[, is_outlier := 0]
final[, source_type := 'Survey - other/unknown']
setnames(final, 'mean', 'val')
final[, variance := standard_error^2]
final[, me_name := paste0(me)]
final[, data_type := 3] # data_type 1 = microdata, data_type 2 = literature, data_type 3 = BRFSS
final[, cvd_cv_urbanicity := 3]

bundle_mapping <- fread('/FILEPATH/var_bundle_map.csv')[var==me]
folder <- unique(bundle_mapping$rei)[1]
folder <- paste0(folder, '/', unique(bundle_mapping$bundle_id_)[1])
output_path <- paste0('/FILEPATH/', folder, '/FILEPATH/')
output_name <- paste0("brfss_xwalk_", Sys.Date())

write.csv(final, paste0(output_path, output_name, ".csv"), row.names=FALSE, na="")
message(paste0("Output saved to ", output_path, output_name, ".csv"))

# Save xlsx
################################
# Merge to bundle ids
final <- merge(final, bundle_mapping[,.(var, bundle_id_, bundle_name)], by.x = 'me_name', by.y = "var")
setnames(final, 'bundle_id_', 'bundle_id')

# Drop unnecessary columns
final[, c('survey_name', 'survey_module') := NULL]

# Save by bundle, for upload
output_path <- paste0('/FILEPATH/', folder, '/FILEPATH/')
output_name <- paste0('brfss_xwalked_', me, '_data_to_upload_',  Sys.Date(), '.xlsx')

# Save file
message(paste0("\nSaving file for epi uploader: ", output_path, output_name))
write.xlsx(x=final, file=paste0(output_path, output_name), sheetName = "extraction")

# Save file with only old BRFSS seq values to remove from bundle
wipe_old_brfss <- bundle[data_type==3,.(seq)] 
write.xlsx(x=wipe_old_brfss, file=paste0(output_path, paste0('wipe_old_brfss_xwalk_',  Sys.Date(), '.xlsx')), sheetName="extraction")
