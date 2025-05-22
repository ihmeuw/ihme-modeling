# Author: USERNAME
# Date: DATE
# Description: Crosswalk self-reported diagnosis of hypertension/hypercholesterolemia in BRFSS to measured elevated BP/cholesterol

# ================================================================================================================================

rm(list=ls())

# Packages
library(data.table)
library(haven)
library(ggplot2)
library(dplyr)
library(mvtnorm)
library(RColorBrewer)
library(boot)
library(splines)
library(effects, lib.loc = '/FILEPATH')
library(usmap)

set.seed(826)

# Filepaths
brfss_path <- "/FILEPATH/"
nhanes_path <- "/FILEPATH/"
output_path <- "/FILEPATH/"

##############################################################################################################
################################### # Load data ##############################################################
##############################################################################################################

keep_vars <- c('file_path', 'nid', 'ihme_loc_id', 'survey_module', 'survey_name', 'year_start', 'year_end', 
               'strata', 'psu', 'pweight', 'sex_id', 'age_year', 'admin_1_id', 'admin_1_mapped',
               'hypertension', 'hyperchol', 'ever_check_chol', 'health_insurance', 
               'sbp_1', 'sbp_2', 'sbp_3', 'dbp_1', 'dbp_2', 'dbp_3', 'chl', 'chl_unit', 'hdl', 'hdl_unit', 'ldl', 'ldl_unit', 'tgl', 'tgl_unit',
               'diabetes', 
               'g_ever_smoking', 'g_current_smoking',
               'bmi', 'bmi_rep', 'pregnant',
               'edu_level_categ')

# load BRFSS data
#########################################################################################
brfss_data <- data.table()
brfss_files <- list.files(brfss_path)
for(file in brfss_files){
  print(paste0(file, ' | ', which(file==brfss_files), ' out of ', length(brfss_files)))
  temp <- as.data.table(read_dta(paste0(brfss_path, file)))
  temp <- temp[,(intersect(keep_vars, names(temp))), with=F]
  brfss_data <- rbind(brfss_data, temp, fill=T)
  rm(temp)
}

# additional data processing
brfss_data <- brfss_data[age_year >= 20 & age_year <= 125 & sex_id %in% c(1, 2)]
brfss_data[, age_group := cut(age_year, breaks=c(seq(from=20, to=80, by=5), 125), right=F)]

brfss_data[, education := case_when(
  edu_level_categ %in% c("0", "0-8", "1-8", "9-11") ~ "less than HS",
  edu_level_categ == "12" ~ "HS grad",
  edu_level_categ %in% c("12-13", "13-15", "14") ~ "some college",
  edu_level_categ %in% c("16", "16-18", "17-18") ~ "college grad"
)]
brfss_data[, education := factor(education, levels=c("less than HS", "HS grad", "some college", "college grad"))]

brfss_data[, current_smoke := case_when(
  g_ever_smoking == 0 ~ 0,
  g_ever_smoking == 1 & g_current_smoking == 0 ~ 0,
  g_ever_smoking == 1 & g_current_smoking == 1 ~ 1
)]
brfss_data[, current_smoke := as.factor(current_smoke)]

brfss_data[, diabetes := as.factor(diabetes)]

brfss_data[, health_insurance := as.factor(health_insurance)]

brfss_data[ever_check_chol == 0, hyperchol := 0]
brfss_data[hyperchol == 1, ever_check_chol := 1]

brfss_data[, bp_group := ifelse(hypertension == 0, 'Not Diagnosed', ifelse(hypertension == 1, 'Diagnosed', NA))]
brfss_data[, chol_group := ifelse(hyperchol == 0, 'Not Diagnosed', ifelse(hyperchol == 1, 'Diagnosed', NA))]

brfss_data <- brfss_data[admin_1_id %like% "USA_"]

# create data plots
pdf(paste0(output_path, "extracted_brfss_data.pdf"), height=7, width=12)

ggplot(brfss_data[,.N, by=c('age_year', 'year_start', 'year_end', 'sex_id')], 
       aes(x = year_start, y = age_year, fill = N)) + geom_tile() + theme_bw() + facet_wrap(~paste0('sex_id ', sex_id))

for(var in setdiff(names(brfss_data), c('file_path', 'nid', 'year_start', 'year_end'))){
  print(var)
  if(var %in% c('admin_1_id', 'admin_1_mapped')){
    p <- ggplot(brfss_data[, .N, by=c(var, 'year_start', 'year_end')], aes(x=as.factor(get(var)), y=N, fill=paste0(year_start, '-', year_end))) + geom_col() + 
      theme_bw() + theme(axis.text.x = element_text(angle = 25, hjust = 1), legend.position = 'bottom') + guides(fill=guide_legend(ncol=10)) +
      labs(x = paste0(var), title = paste0(var, " by survey year"), fill=NULL)
  } else if (class(brfss_data[, get(var)])=="factor" | class(brfss_data[, get(var)])=="character" | class(brfss_data[, get(var)])=="logical" |
             length(unique(brfss_data[, get(var)])) < 10){
    p <- ggplot(brfss_data[, .N, by=c(var, 'year_start', 'year_end')], aes(x=as.factor(get(var)), y=N, fill=as.factor(get(var)))) + geom_col() + 
      facet_wrap(~paste0(year_start, '-', year_end), scales = "free") + theme_bw() + 
      labs(x = paste0(var), title = paste0(var, " by survey year"), fill=NULL)
    if(length(unique(na.omit(brfss_data[, get(var)]))) > 3){
      p <- p + theme(axis.text.x = element_text(angle = 25, hjust = 1))
    }
  } else {
    p <- ggplot(brfss_data, aes(x=paste0(year_start, '-', year_end), y=get(var))) + geom_boxplot(fill="cornflowerblue") + 
      theme_bw() + theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      labs(x = NULL, y = paste0(var), title = paste0(var, " by survey year"))
  } 
  print(p)
}
dev.off()

# load NHANES data
#########################################################################################
nhanes_data <- data.table()
nhanes_files <- list.files(nhanes_path)
for(file in nhanes_files){
  print(paste0(file, ' | ', which(file==nhanes_files), ' out of ', length(nhanes_files)))
  temp <- as.data.table(read_dta(paste0(nhanes_path, file)))
  temp <- temp[,(intersect(keep_vars, names(temp))), with=F]
  nhanes_data <- rbind(nhanes_data, temp, fill=T)
  rm(temp)
}

# additional data processing
nhanes_data <- nhanes_data[age_year >= 20 & age_year <= 125 & sex_id %in% c(1, 2)]
nhanes_data[, age_group := cut(age_year, breaks=c(seq(from=20, to=80, by=5), 125), right=F)]

nhanes_data[, education := case_when(
  edu_level_categ %in% c("0", "0-8", "1-8", "9-11") ~ "less than HS",
  edu_level_categ == "12" ~ "HS grad",
  edu_level_categ %in% c("12-13", "13-15", "14") ~ "some college",
  edu_level_categ %in% c("16", "16-18", "17-18") ~ "college grad"
)]
nhanes_data[, education := factor(education, levels=c("less than HS", "HS grad", "some college", "college grad"))]

nhanes_data[, current_smoke := case_when(
  g_ever_smoking == 0 ~ 0,
  g_ever_smoking == 1 & g_current_smoking == 0 ~ 0,
  g_ever_smoking == 1 & g_current_smoking == 1 ~ 1
)]
nhanes_data[, current_smoke := as.factor(current_smoke)]

nhanes_data[, diabetes := as.factor(diabetes)]

nhanes_data[, health_insurance := as.factor(health_insurance)]

nhanes_data[ever_check_chol == 0, hyperchol := 0]
nhanes_data[hyperchol == 1, ever_check_chol := 1]

nhanes_data[, bp_group := ifelse(hypertension == 0, 'Not Diagnosed', ifelse(hypertension == 1, 'Diagnosed', NA))]
nhanes_data[, chol_group := ifelse(hyperchol == 0, 'Not Diagnosed', ifelse(hyperchol == 1, 'Diagnosed', NA))]

nhanes_data[, sbp_mean_1_2_3 := rowMeans(.SD, na.rm=T), .SDcols = paste0('sbp_', 1:3)]
nhanes_data[, dbp_mean_1_2_3 := rowMeans(.SD, na.rm=T), .SDcols = paste0('dbp_', 1:3)]

nhanes_data[!is.na(sbp_mean_1_2_3) & !is.na(dbp_mean_1_2_3), high_bp := as.integer(sbp_mean_1_2_3 >= 140 | dbp_mean_1_2_3 >= 90)]
nhanes_data[!is.na(sbp_mean_1_2_3) & is.na(dbp_mean_1_2_3), high_bp := as.integer(sbp_mean_1_2_3 >= 140)]
nhanes_data[is.na(sbp_mean_1_2_3) & !is.na(dbp_mean_1_2_3), high_bp := as.integer(dbp_mean_1_2_3 >= 90)]

nhanes_data[, high_chol := as.integer(chl >= 6.2)]

# create data plots
pdf(paste0(output_path, "extracted_nhanes_data.pdf"), height=7, width=12)

ggplot(nhanes_data[,.N, by=c('age_year', 'year_start', 'year_end', 'sex_id')], 
       aes(x = year_start, y = age_year, fill = N)) + geom_tile() + theme_bw() + facet_wrap(~paste0('sex_id ', sex_id))

for(var in setdiff(names(nhanes_data), c('file_path', 'nid', 'year_start', 'year_end'))){
  print(var)
  if (class(nhanes_data[, get(var)])=="factor" | class(nhanes_data[, get(var)])=="character" | class(nhanes_data[, get(var)])=="logical" |
      length(unique(nhanes_data[, get(var)])) < 10){
    p <- ggplot(nhanes_data[, .N, by=c(var, 'year_start', 'year_end')], aes(x=as.factor(get(var)), y=N, fill=as.factor(get(var)))) + geom_col() + 
      facet_wrap(~paste0(year_start, '-', year_end), scales = "free") + theme_bw() + 
      labs(x = paste0(var), title = paste0(var, " by survey year"), fill=NULL)
    if(length(unique(na.omit(nhanes_data[, get(var)]))) > 3){
      p <- p + theme(axis.text.x = element_text(angle = 25, hjust = 1))
    }
  } else {
    p <- ggplot(nhanes_data, aes(x=paste0(year_start, '-', year_end), y=get(var))) + geom_boxplot(fill="cornflowerblue") + 
      theme_bw() + theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      labs(x = NULL, y = paste0(var), title = paste0(var, " by survey year"))
  } 
  print(p)
}
dev.off()

##############################################################################################################
############################ # Correct self-reported BMI #####################################################
##############################################################################################################

# calculate mean measured BMI from NHANES by year, sex, and age
nhanes_bmi <- nhanes_data[(is.na(pregnant) | pregnant == 0) & !is.na(bmi) & pweight > 0]
nhanes_bmi <- nhanes_bmi[, list(bmi = weighted.mean(bmi, pweight)), by=c('year_start', 'year_end', 'sex_id', 'age_group')]

# match up BRFSS years with NHANES year cycles
brfss_bmi <- brfss_data[(is.na(pregnant) | pregnant == 0) & !is.na(bmi_rep) & pweight > 0 & 
                          year_start >= min(nhanes_bmi$year_start) & year_end <= max(nhanes_bmi$year_end)]
brfss_bmi[, new_year_start := 2*floor((year_start-1)/2)+1]
brfss_bmi[, new_year_end := new_year_start + 1]
brfss_bmi[new_year_start >= 2017, `:=` (new_year_start = 2017, new_year_end = 2020)]

# re-weigh weights before merging with NHANES data to account for survey year combinations (as per BRFSS documentation)
brfss_bmi[, indiv_yr_obs := .N, by=c('year_start', 'year_end')]
brfss_bmi[, comb_yr_obs := .N, by=c('new_year_start', 'new_year_end')]
brfss_bmi[, pweight := pweight * (indiv_yr_obs/comb_yr_obs)]

# calculate mean reported BMI from BRFSS by year, sex, and age
brfss_bmi[, c('year_start', 'year_end') := NULL]
setnames(brfss_bmi, c('new_year_start', 'new_year_end'), c('year_start', 'year_end'))
brfss_bmi <- brfss_bmi[, list(bmi_rep = weighted.mean(bmi_rep, pweight)), by=c('year_start', 'year_end', 'sex_id', 'age_group')]

# fit model based on age-sex-year specific means of measured and reported BMI
bmi_data <- merge(nhanes_bmi, brfss_bmi, by=c("year_start", "year_end", "sex_id", "age_group"))
correct_bmi_fit <- lapply(1:2, function(s) {
  lm(bmi ~ bmi_rep, data=bmi_data[sex_id==s,]) 
})
summary(correct_bmi_fit[[1]])
summary(correct_bmi_fit[[2]])

# save coefficients
bmi_coefs <- rbindlist(lapply(correct_bmi_fit, function(x) as.data.table(as.list(coef(x)))), 
                       idcol='sex_id')
write.csv(bmi_coefs, paste0(output_path, 'self_rpt_bmi_xwalk_coefs.csv'), row.names = F)

# define function for applying the correction
correct_bmi <- function(newdata, sex_id, correct_bmi_fit, simulate=F, n.sims=0) {
  fit <- correct_bmi_fit[[sex_id]]
  model <- formula(paste("~", fit$call$formula[3]))
  if (!simulate) {
    fe <- as.matrix(fit$coef)
    X <- model.matrix(model, model.frame(model, newdata, na.action="na.pass"))[, rownames(fe)]
    pred <- X %*% fe
  } else {
    if (n.sims == 0) fe <- as.matrix(fit$coef)
    if (n.sims > 0) fe <- t(rmvnorm(n.sims, fit$coef, vcov(fit)))
    X <- model.matrix(model, model.frame(model, newdata, na.action="na.pass"))[, rownames(fe)]
    pred <- X %*% fe
    # calculate error term and add on to predictions
    sigma <- matrix(rnorm(dim(pred)[1]*dim(pred)[2],mean=0,sd=summary(fit)$sigma), dim(pred)[1], dim(pred)[2])
    pred <- pred + sigma
    if (n.sims > 0) pred <- lapply(1:n.sims, function(p) as.numeric(pred[,p]))
  }
  pred
}

# apply correction to individual-level BRFSS data
brfss_data[, bmi := correct_bmi(.SD, sex_id[1], correct_bmi_fit, F), by='sex_id']

pdf(paste0(output_path, "bmi_self-report_correction.pdf"), width=10, height=7)

# plot BRFSS vs NHANES data with linear fit
s1_coefs <- as.data.table(as.list(correct_bmi_fit[[1]]$coefficients))
s2_coefs <- as.data.table(as.list(correct_bmi_fit[[2]]$coefficients))
model_coefficients <- rbind(data.table(sex_id = 1, s1_coefs), data.table(sex_id = 2, s2_coefs))

# plot colored by year
ggplot(bmi_data, aes(bmi_rep, bmi, color=paste0(year_start, '-', year_end))) + 
  geom_point(size=2) + theme_bw() + geom_abline(aes(intercept=0, slope=1)) +
  facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Male", "Female"))) +
  labs(x="Reported BMI (BRFSS)", y="Measured BMI (NHANES)", title="NHANES/BRFSS Mean Data and Fitted Model by Year", 
       caption="Solid line represents reference line, dashed line represents model fit", color='Year') + 
  scale_color_brewer(palette='Spectral') + scale_shape_manual(values=c(16, 3)) +
  geom_abline(data = model_coefficients, aes(intercept = `(Intercept)`, slope = bmi_rep), linetype='dashed')

# plot colored by age
ggplot(bmi_data, aes(bmi_rep, bmi, color=age_group)) + 
  geom_point(size=2) + theme_bw() + geom_abline(aes(intercept=0, slope=1)) +
  facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Male", "Female"))) +
  labs(x="Reported BMI (BRFSS)", y="Measured BMI (NHANES)", title="NHANES/BRFSS Mean Data and Fitted Model by Age", 
       caption="Solid line represents reference line, dashed line represents model fit", color='Age Group') + 
  scale_shape_manual(values=c(16, 3)) +
  geom_abline(data = model_coefficients, aes(intercept = `(Intercept)`, slope = bmi_rep), linetype='dashed')

# plot distributions
brfss_plot <- brfss_data[, c("sex_id", "bmi_rep", "bmi")]
setnames(brfss_plot, c("bmi_rep", "bmi"), c("BRFSS self-report", "BRFSS corrected"))
brfss_plot <- melt(brfss_plot, id.vars = 'sex_id', variable.name = "type", value.name = "bmi")
nhanes_plot <- nhanes_data[, c("sex_id", "bmi")]
nhanes_plot[, type := "NHANES measured"]
ggplot(rbind(nhanes_plot, brfss_plot), aes(x=bmi, color=type)) + facet_wrap(~factor(sex_id, levels=c(1,2), labels=c("Male", "Female")), scales='free_y') + 
  geom_density() + theme_bw() + xlim(15, 60) + theme(legend.position = 'bottom') +
  scale_color_manual(values=c("blue", "black", "red")) + labs(color=NULL)

# plot bmi time series by sex for both NHANES and BRFSS
nhanes_plot <- nhanes_data[, list(source = "NHANES", type = 'measured',
                                  bmi = weighted.mean(bmi, pweight, na.rm=T)), 
                           by=c('year_start', 'year_end', 'sex_id')]
brfss_plot <- brfss_data[, list(source = "BRFSS", reported = weighted.mean(bmi_rep, pweight, na.rm=T), corrected = weighted.mean(bmi, pweight, na.rm=T)), 
                          by=c('year_start', 'year_end', 'sex_id')]
brfss_plot <- melt(brfss_plot, id.vars = c('year_start', 'year_end', 'sex_id', 'source'), variable.name = 'type', value.name = 'bmi')

ggplot(rbind(nhanes_plot, brfss_plot), aes(x=year_start, y=bmi, shape=source, linetype=source, colour=type, group=interaction(type, source))) + theme_bw() +
  geom_point(size=3) + geom_line() + facet_wrap(~factor(sex_id, levels=1:2, labels=c("Males", "Females"))) + scale_shape_manual(values=c(16, 2)) +
  scale_colour_manual(values=c("darkgreen", "red", "pink2")) +
  labs(x="Year Start", y="Mean BMI", color=NULL, shape=NULL, linetype=NULL)

# plot bmi age series by sex for both NHANES and BRFSS
nhanes_plot <- nhanes_data[, list(source = "NHANES", type = 'measured',
                                  bmi = weighted.mean(bmi, pweight, na.rm=T)), 
                           by=c('age_group', 'sex_id')]
brfss_plot <- brfss_data[, list(source = "BRFSS", reported = weighted.mean(bmi_rep, pweight, na.rm=T), corrected = weighted.mean(bmi, pweight, na.rm=T)), 
                         by=c('age_group', 'sex_id')]
brfss_plot <- melt(brfss_plot, id.vars = c('age_group', 'sex_id', 'source'), variable.name = 'type', value.name = 'bmi')

ggplot(rbind(nhanes_plot, brfss_plot), aes(x=age_group, y=bmi, shape=source, linetype=source, colour=type, group=interaction(type, source))) + theme_bw() +
  geom_point(size=3) + geom_line() + facet_wrap(~factor(sex_id, levels=1:2, labels=c("Males", "Females"))) + scale_shape_manual(values=c(16, 2)) +
  scale_colour_manual(values=c("darkgreen", "red", "pink2")) + theme(axis.text.x = element_text(angle = 30, hjust = 1)) +
  labs(x="Age Group", y="Mean BMI", color=NULL, shape=NULL, linetype=NULL)

dev.off()

rm(nhanes_plot, nhanes_bmi, brfss_plot, brfss_bmi, bmi_data)

##############################################################################################################
####################### # Correct self-reported hypertension diagnosis #######################################
##############################################################################################################

nhanes_bp <- nhanes_data[!is.na(high_bp) & !is.na(bp_group) & pweight > 0, 
                         list(year_start, year_end, pweight, sex_id, age_year, age_group, education, bmi, current_smoke, diabetes, health_insurance, bp_group, hypertension, high_bp)]
nhanes_bp[, pweight := round(pweight * length(pweight)/sum(pweight), 5), by=c('year_start', 'year_end')]
nhanes_bp[age_year > 80, age_year := 80]

# model true blood pressure status from predictors
pdf(paste0(output_path, "bp_self_report_xwalk_model_fit.pdf"), width=14, height=11)
mod <- as.formula("high_bp ~ education + bmi + diabetes + health_insurance + current_smoke + year_start + bs(age_year)")
high_bp_fit <- NULL

# fit the model
high_bp_fit <- lapply(1:2, function(sx) {
  sapply(unique(nhanes_bp$bp_group), function(gp) {
    assign('sub', nhanes_bp[bp_group == gp & sex_id == sx], envir = .GlobalEnv)
    pweight <- sub[,pweight]
    fit <- glm(mod, family=binomial(link="logit"), data=sub, weights=pweight)
    message(paste0('\n', gp, ', ', ifelse(sx==1, 'Males', 'Females'), ':'))
    print(summary(fit))
    plot(allEffects(fit), main = paste0(gp, ', ', ifelse(sx==1, 'Males', 'Females')))
    fit
  }, simplify = FALSE, USE.NAMES = TRUE)
})

dev.off()

# save coefficients
bp_coefs <- rbindlist(lapply(1:2, function(i) 
  rbindlist(lapply(high_bp_fit[[i]], function(x) as.data.table(as.list(coef(x)))), 
            idcol='diagnosis_group')), 
  idcol='sex_id')
write.csv(bp_coefs, paste0(output_path, 'self_rpt_bp_xwalk_coefs.csv'), row.names = F)

# function to predict true blood pressure status from fitted model
pred_high_bp <- function(newdata, high_bp_fit, bp_group, sex_id, n.sims) {
  fit <- high_bp_fit[[sex_id]][[bp_group]]
  model <- formula(paste("~", fit$formula[3]))
  if (n.sims == 0) fe <- as.matrix(fit$coef)
  if (n.sims > 0) fe <- t(rmvnorm(n.sims, fit$coef, vcov(fit)))
  
  X <- model.matrix(model, model.frame(model, newdata, na.action="na.pass"))[, rownames(fe)]
  
  #X <- X[, rownames(fe)[rownames(fe) %in% dimnames(X)[[2]]]]
  #fe <- fe[rownames(fe)[rownames(fe) %in% dimnames(X)[[2]]],]
  
  pred <- inv.logit(X %*% fe)
  if (n.sims > 0) pred <- lapply(1:n.sims, function(p) as.numeric(pred[,p] >= runif(nrow(pred), 0, 1))) # its faster to use runif() this way then to loop through and use rbinom()
  pred
}

predvars <- paste("bp_pred", 1:10, sep="")

# apply model to NHANES
nhanes_bp[, (predvars) := pred_high_bp(.SD, high_bp_fit, bp_group[1], sex_id[1], 10), by='bp_group,sex_id']
nhanes_bp <- nhanes_bp[!is.na(bp_pred1),]

# apply model
brfss_data[, og_age_year := age_year]
brfss_data[age_year > 80, age_year := 80]
brfss_data[!is.na(bp_group), (predvars) := pred_high_bp(.SD, high_bp_fit, bp_group[1], sex_id[1], 10), by='bp_group,sex_id']
brfss_data[, age_year := og_age_year]
brfss_data[, og_age_year := NULL]
brfss_data[, high_bp := rowMeans(.SD, na.rm=T), .SDcols=predvars]
brfss_bp <- brfss_data[!is.na(bp_pred1),]

pdf(paste0(output_path, "bp_self_report_xwalk_model_results.pdf"), width=12, height=7)

# plot high BP time series by sex and diagnosis for both NHANES and BRFSS
plot_dt <- rbind(
  nhanes_bp[, c(list(source = "NHANES",
                     measured = weighted.mean(high_bp, pweight),
                     reported = weighted.mean(hypertension, pweight)), 
                pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
            by=c('year_start', 'year_end', 'sex_id', 'bp_group')],
  nhanes_bp[, c(list(source = "NHANES", bp_group = 'All',
                     measured = weighted.mean(high_bp, pweight),
                     reported = weighted.mean(hypertension, pweight)), 
                pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
            by=c('year_start', 'year_end', 'sex_id')],
  brfss_bp[, c(list(source = "BRFSS",
                    reported = weighted.mean(hypertension, pweight)), 
               pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
           by=c('year_start', 'year_end', 'sex_id', 'bp_group')],
  brfss_bp[, c(list(source = "BRFSS", bp_group = 'All',
                    reported = weighted.mean(hypertension, pweight)), 
               pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
           by=c('year_start', 'year_end', 'sex_id')]
  , fill=T)

plot_dt <- melt(plot_dt, id.vars = c('year_start', 'year_end', 'sex_id', 'bp_group', 'source'), value.name = 'high_bp', na.rm = T)
plot_dt[, type := factor(ifelse(grepl("pred", variable), "pred", as.character(variable)), 
                         levels=c("reported", "measured", "pred"),
                         labels=c("self-reported", "measured", "corrected"))]
plot_dt[, transp := ifelse(type=='corrected', .5, 1)]

ggplot(plot_dt, aes(x=year_start, y=high_bp, shape=source, linetype=source, colour=type, group=interaction(variable, source))) + 
  geom_point(size=3, alpha=plot_dt$transp) + geom_line(alpha=.5) + 
  facet_wrap(factor(sex_id, levels=1:2, labels=c("Males", "Females"))~bp_group) + theme_bw() +
  scale_colour_manual(values=c("darkgreen", "red", "pink2")) + scale_shape_manual(values=c(16, 2)) +
  labs(x="Year Start", y="Prevalence of high BP", color=NULL, shape=NULL, linetype=NULL)

# plot high BP age series by sex and diagnosis for both NHANES and BRFSS
plot_dt <- rbind(
  nhanes_bp[, c(list(source = "NHANES",
                     measured = weighted.mean(high_bp, pweight),
                     reported = weighted.mean(hypertension, pweight)), 
                pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
            by=c('age_group', 'sex_id', 'bp_group')],
  nhanes_bp[, c(list(source = "NHANES", bp_group = 'All',
                     measured = weighted.mean(high_bp, pweight),
                     reported = weighted.mean(hypertension, pweight)), 
                pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
            by=c('age_group', 'sex_id')],
  brfss_bp[, c(list(source = "BRFSS",
                    reported = weighted.mean(hypertension, pweight)), 
               pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
           by=c('age_group', 'sex_id', 'bp_group')],
  brfss_bp[, c(list(source = "BRFSS", bp_group = 'All',
                    reported = weighted.mean(hypertension, pweight)), 
               pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
           by=c('age_group', 'sex_id')]
, fill=T)
plot_dt <- melt(plot_dt, id.vars = c('age_group', 'sex_id', 'bp_group', 'source'), value.name = 'high_bp', na.rm = T)
plot_dt[, type := factor(ifelse(grepl("pred", variable), "pred", as.character(variable)), 
                             levels=c("reported", "measured", "pred"),
                             labels=c("self-reported", "measured", "corrected"))]
plot_dt[, transp := ifelse(type=='corrected', .5, 1)]

ggplot(plot_dt, aes(x=age_group, y=high_bp, shape=source, linetype=source, colour=type, group=interaction(variable, source))) + 
  geom_point(size=3, alpha=plot_dt$transp) + geom_line(alpha=.5) + 
  facet_wrap(factor(sex_id, levels=1:2, labels=c("Males", "Females"))~bp_group) + theme_bw() +
  scale_colour_manual(values=c("darkgreen", "red", "pink2")) + scale_shape_manual(values=c(16, 2)) +
  labs(x="Age Group", y="Prevalence of high BP", color=NULL, shape=NULL, linetype=NULL) +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))

# distribution plots by diagnosis and sex
brfss_plot <- rbind(brfss_bp[, list(source = "BRFSS", 
                                    high_bp = weighted.mean(high_bp, pweight)),
                              by=c('admin_1_mapped', 'year_start', 'sex_id', 'bp_group')],
                     brfss_bp[, list(source = "BRFSS", bp_group = 'All',
                                     high_bp = weighted.mean(high_bp, pweight)),
                              by=c('admin_1_mapped', 'year_start', 'sex_id')]
                    )

p <- ggplot(brfss_plot, aes(x=high_bp, fill=bp_group)) + geom_density(alpha=0.5) +
  facet_wrap(~factor(sex_id, levels=1:2, labels=c("Males", "Females")), scales = "free_y") + theme_bw() + 
  labs(x="Prevalence of high BP", fill='BRFSS', color='NHANES',
       title=paste0("Distribution of corrected BRFSS high BP, averaged by diagnosis group, sex, year, and state"))
print(p)

nhanes_plot <- rbind(nhanes_bp[, list(source = "NHANES", 
                                    high_bp = weighted.mean(high_bp, pweight)),
                             by=c('sex_id', 'year_start', 'bp_group')],
                    nhanes_bp[, list(source = "NHANES", bp_group = 'All',
                                    high_bp = weighted.mean(high_bp, pweight)),
                             by=c('sex_id', 'year_start')]
)
print(p + geom_density(data=nhanes_plot, aes(fill=NULL, color=bp_group)))

# distribution plots by age
brfss_plot <- brfss_bp[, list(source = "BRFSS", 
                              high_bp = weighted.mean(high_bp, pweight)),
                       by=c('admin_1_mapped', 'age_group', 'sex_id', 'year_start')]

p <- ggplot(brfss_plot, aes(x=high_bp, fill=age_group)) + geom_density(alpha=0.5) +
  theme_bw() + facet_wrap(~factor(sex_id, levels=1:2, labels=c("Males", "Females")), scales = "free_y", ncol=1) +
  labs(x="Prevalence of high BP", fill='BRFSS', color='NHANES',
       title=paste0("Distribution of corrected BRFSS high BP, averaged by age, sex, year, and state")) +
  theme(legend.box = "horizontal")
print(p)

nhanes_plot <- nhanes_bp[, list(source = "NHANES", 
                                high_bp = weighted.mean(high_bp, pweight)),
                         by=c('age_group', 'sex_id', 'year_start')]
print(p + geom_density(data=nhanes_plot, aes(fill=NULL, color=age_group)))

# maps
brfss_plot <- rbind(brfss_bp[admin_1_mapped!='District of Columbia', 
                             list(source = "BRFSS", 
                                  high_bp = weighted.mean(high_bp, pweight)),
                             by=c('admin_1_mapped', 'sex_id', 'bp_group')],
                    brfss_bp[admin_1_mapped!='District of Columbia', 
                             list(source = "BRFSS", bp_group = 'All',
                                  high_bp = weighted.mean(high_bp, pweight)),
                             by=c('admin_1_mapped', 'sex_id')]
)
brfss_plot[, state := admin_1_mapped]
brfss_plot[, state := gsub('N ', 'North ', state)]
brfss_plot[, state := gsub('S ', 'South ', state)]
brfss_plot[, state := gsub('W ', 'West ', state)]

for(gp in unique(brfss_plot$bp_group)){
  print(plot_usmap(data = brfss_plot[bp_group == gp], values = "high_bp") +
          theme(legend.position = "right") + 
          scale_fill_distiller(palette='RdYlBu', direction=-1) +
          facet_wrap(~factor(sex_id, levels=1:2, labels=c("Males", "Females"))) +
          labs(title=paste0(gp, 
                            "\nCorrected BRFSS high BP, averaged by diagnosis group, sex, and state"), 
               fill="Prevalence of\nhigh BP"))
}

dev.off()

rm(brfss_bp, brfss_plot, nhanes_bp, nhanes_plot, plot_dt)

##############################################################################################################
####################### # Correct self-reported hypercholesterolemia diagnosis ###############################
##############################################################################################################

nhanes_chol <- nhanes_data[!is.na(high_chol) & !is.na(chol_group) & pweight > 0, 
                         list(year_start, year_end, pweight, sex_id, age_year, age_group, education, bmi, current_smoke, diabetes, health_insurance, chol_group, hyperchol, high_chol)]
nhanes_chol[, pweight := round(pweight * length(pweight)/sum(pweight), 5), by=c('year_start', 'year_end')]
nhanes_chol[age_year > 80, age_year := 80]

# model true cholesterol status from predictors
pdf(paste0(output_path, "chol_self_report_xwalk_model_fit.pdf"), width=14, height=11)
mod <- as.formula("high_chol ~ education + bmi + diabetes + health_insurance + current_smoke + year_start + bs(age_year)")
high_chol_fit <- NULL

# fit the model
high_chol_fit <- lapply(1:2, function(sx) {
  sapply(unique(nhanes_chol$chol_group), function(gp) {
    assign('sub', nhanes_chol[chol_group == gp & sex_id == sx], envir = .GlobalEnv)
    pweight <- sub[,pweight]
    fit <- glm(mod, family=binomial(link="logit"), data=sub, weights=pweight)
    message(paste0('\n', gp, ', ', ifelse(sx==1, 'Males', 'Females'), ':'))
    print(summary(fit))
    plot(allEffects(fit), main = paste0(gp, ', ', ifelse(sx==1, 'Males', 'Females')))
    fit
  }, simplify = FALSE, USE.NAMES = TRUE)
})

dev.off()

# save coefficients
chol_coefs <- rbindlist(lapply(1:2, function(i) 
  rbindlist(lapply(high_chol_fit[[i]], function(x) as.data.table(as.list(coef(x)))), 
            idcol='diagnosis_group')), 
  idcol='sex_id')
write.csv(chol_coefs, paste0(output_path, 'self_rpt_chol_xwalk_coefs.csv'), row.names = F)

# function to predict true cholesterol status from fitted model
pred_high_chol <- function(newdata, high_chol_fit, chol_group, sex_id, n.sims) {
  fit <- high_chol_fit[[sex_id]][[chol_group]]
  model <- formula(paste("~", fit$formula[3]))
  if (n.sims == 0) fe <- as.matrix(fit$coef)
  if (n.sims > 0) fe <- t(rmvnorm(n.sims, fit$coef, vcov(fit)))
  
  X <- model.matrix(model, model.frame(model, newdata, na.action="na.pass"))[, rownames(fe)]
  
  #X <- X[, rownames(fe)[rownames(fe) %in% dimnames(X)[[2]]]]
  #fe <- fe[rownames(fe)[rownames(fe) %in% dimnames(X)[[2]]],]
  
  pred <- inv.logit(X %*% fe)
  if (n.sims > 0) pred <- lapply(1:n.sims, function(p) as.numeric(pred[,p] >= runif(nrow(pred), 0, 1))) # its faster to use runif() this way then to loop through and use rbinom()
  pred
}

predvars <- paste("chol_pred", 1:10, sep="")

# apply model to NHANES
nhanes_chol[, (predvars) := pred_high_chol(.SD, high_chol_fit, chol_group[1], sex_id[1], 10), by='chol_group,sex_id']
nhanes_chol <- nhanes_chol[!is.na(chol_pred1),]

# apply model
brfss_data[, og_age_year := age_year]
brfss_data[age_year > 80, age_year := 80]
brfss_data[!is.na(chol_group), (predvars) := pred_high_chol(.SD, high_chol_fit, chol_group[1], sex_id[1], 10), by='chol_group,sex_id']
brfss_data[, age_year := og_age_year]
brfss_data[, og_age_year := NULL]
brfss_data[, high_chol := rowMeans(.SD, na.rm=T), .SDcols=predvars]
brfss_chol <- brfss_data[!is.na(chol_pred1),]

pdf(paste0(output_path, "chol_self_report_xwalk_model_results.pdf"), width=12, height=7)

# plot high cholesterol time series by sex and diagnosis for both NHANES and BRFSS
plot_dt <- rbind(
  nhanes_chol[, c(list(source = "NHANES",
                     measured = weighted.mean(high_chol, pweight),
                     reported = weighted.mean(hyperchol, pweight)), 
                pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
            by=c('year_start', 'year_end', 'sex_id', 'chol_group')],
  nhanes_chol[, c(list(source = "NHANES", chol_group = 'All',
                     measured = weighted.mean(high_chol, pweight),
                     reported = weighted.mean(hyperchol, pweight)), 
                pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
            by=c('year_start', 'year_end', 'sex_id')],
  brfss_chol[, c(list(source = "BRFSS",
                    reported = weighted.mean(hyperchol, pweight)), 
               pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
           by=c('year_start', 'year_end', 'sex_id', 'chol_group')],
  brfss_chol[, c(list(source = "BRFSS", chol_group = 'All',
                    reported = weighted.mean(hyperchol, pweight)), 
               pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
           by=c('year_start', 'year_end', 'sex_id')]
  , fill=T)

plot_dt <- melt(plot_dt, id.vars = c('year_start', 'year_end', 'sex_id', 'chol_group', 'source'), value.name = 'high_chol', na.rm = T)
plot_dt[, type := factor(ifelse(grepl("pred", variable), "pred", as.character(variable)), 
                         levels=c("reported", "measured", "pred"),
                         labels=c("self-reported", "measured", "corrected"))]
plot_dt[, transp := ifelse(type=='corrected', .5, 1)]

ggplot(plot_dt, aes(x=year_start, y=high_chol, shape=source, linetype=source, colour=type, group=interaction(variable, source))) + 
  geom_point(size=3, alpha=plot_dt$transp) + geom_line(alpha=.5) + 
  facet_wrap(factor(sex_id, levels=1:2, labels=c("Males", "Females"))~chol_group) + theme_bw() +
  scale_colour_manual(values=c("darkgreen", "red", "pink2")) + scale_shape_manual(values=c(16, 2)) +
  labs(x="Year Start", y="Prevalence of high cholesterol", color=NULL, shape=NULL, linetype=NULL)

# plot high cholesterol age series by sex and diagnosis for both NHANES and BRFSS
plot_dt <- rbind(
  nhanes_chol[, c(list(source = "NHANES",
                     measured = weighted.mean(high_chol, pweight),
                     reported = weighted.mean(hyperchol, pweight)), 
                pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
            by=c('age_group', 'sex_id', 'chol_group')],
  nhanes_chol[, c(list(source = "NHANES", chol_group = 'All',
                     measured = weighted.mean(high_chol, pweight),
                     reported = weighted.mean(hyperchol, pweight)), 
                pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
            by=c('age_group', 'sex_id')],
  brfss_chol[, c(list(source = "BRFSS",
                    reported = weighted.mean(hyperchol, pweight)), 
               pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
           by=c('age_group', 'sex_id', 'chol_group')],
  brfss_chol[, c(list(source = "BRFSS", chol_group = 'All',
                    reported = weighted.mean(hyperchol, pweight)), 
               pred = lapply(predvars, function(x) weighted.mean(.SD[[x]], pweight, na.rm=T))),
           by=c('age_group', 'sex_id')]
  , fill=T)
plot_dt <- melt(plot_dt, id.vars = c('age_group', 'sex_id', 'chol_group', 'source'), value.name = 'high_chol', na.rm = T)
plot_dt[, type := factor(ifelse(grepl("pred", variable), "pred", as.character(variable)), 
                         levels=c("reported", "measured", "pred"),
                         labels=c("self-reported", "measured", "corrected"))]
plot_dt[, transp := ifelse(type=='corrected', .5, 1)]

ggplot(plot_dt, aes(x=age_group, y=high_chol, shape=source, linetype=source, colour=type, group=interaction(variable, source))) + 
  geom_point(size=3, alpha=plot_dt$transp) + geom_line(alpha=.5) + 
  facet_wrap(factor(sex_id, levels=1:2, labels=c("Males", "Females"))~chol_group) + theme_bw() +
  scale_colour_manual(values=c("darkgreen", "red", "pink2")) + scale_shape_manual(values=c(16, 2)) +
  labs(x="Age Group", y="Prevalence of high cholesterol", color=NULL, shape=NULL, linetype=NULL) +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))

# distribution plots by diagnosis and sex
brfss_plot <- rbind(brfss_chol[, list(source = "BRFSS", 
                                    high_chol = weighted.mean(high_chol, pweight)),
                             by=c('admin_1_mapped', 'year_start', 'sex_id', 'chol_group')],
                    brfss_chol[, list(source = "BRFSS", chol_group = 'All',
                                    high_chol = weighted.mean(high_chol, pweight)),
                             by=c('admin_1_mapped', 'year_start', 'sex_id')]
)

p <- ggplot(brfss_plot, aes(x=high_chol, fill=chol_group)) + geom_density(alpha=0.5) +
  facet_wrap(~factor(sex_id, levels=1:2, labels=c("Males", "Females")), scales = "free_y") + theme_bw() + 
  labs(x="Prevalence of high cholesterol", fill='BRFSS', color='NHANES',
       title=paste0("Distribution of corrected BRFSS high cholesterol, averaged by diagnosis group, sex, year, and state"))
print(p)

nhanes_plot <- rbind(nhanes_chol[, list(source = "NHANES", 
                                      high_chol = weighted.mean(high_chol, pweight)),
                               by=c('sex_id', 'year_start', 'chol_group')],
                     nhanes_chol[, list(source = "NHANES", chol_group = 'All',
                                      high_chol = weighted.mean(high_chol, pweight)),
                               by=c('sex_id', 'year_start')]
)
print(p + geom_density(data=nhanes_plot, aes(fill=NULL, color=chol_group)))

# distribution plots by age
brfss_plot <- brfss_chol[, list(source = "BRFSS", 
                              high_chol = weighted.mean(high_chol, pweight)),
                       by=c('admin_1_mapped', 'age_group', 'sex_id', 'year_start')]

p <- ggplot(brfss_plot, aes(x=high_chol, fill=age_group)) + geom_density(alpha=0.5) +
  theme_bw() + facet_wrap(~factor(sex_id, levels=1:2, labels=c("Males", "Females")), scales = "free_y", ncol=1) +
  labs(x="Prevalence of high cholesterol", fill='BRFSS', color='NHANES',
       title=paste0("Distribution of corrected BRFSS high cholesterol, averaged by age, sex, year, and state")) +
  theme(legend.box = "horizontal")
print(p)

nhanes_plot <- nhanes_chol[, list(source = "NHANES", 
                                high_chol = weighted.mean(high_chol, pweight)),
                         by=c('age_group', 'sex_id', 'year_start')]
print(p + geom_density(data=nhanes_plot, aes(fill=NULL, color=age_group)))

# maps
brfss_plot <- rbind(brfss_chol[admin_1_mapped!='District of Columbia', 
                             list(source = "BRFSS", 
                                  high_chol = weighted.mean(high_chol, pweight)),
                             by=c('admin_1_mapped', 'sex_id', 'chol_group')],
                    brfss_chol[admin_1_mapped!='District of Columbia', 
                             list(source = "BRFSS", chol_group = 'All',
                                  high_chol = weighted.mean(high_chol, pweight)),
                             by=c('admin_1_mapped', 'sex_id')]
)
brfss_plot[, state := admin_1_mapped]
brfss_plot[, state := gsub('N ', 'North ', state)]
brfss_plot[, state := gsub('S ', 'South ', state)]
brfss_plot[, state := gsub('W ', 'West ', state)]

for(gp in unique(brfss_plot$chol_group)){
  print(plot_usmap(data = brfss_plot[chol_group == gp], values = "high_chol") +
          theme(legend.position = "right") + 
          scale_fill_distiller(palette='RdYlBu', direction=-1) +
          facet_wrap(~factor(sex_id, levels=1:2, labels=c("Males", "Females"))) +
          labs(title=paste0(gp, 
                            "\nCorrected BRFSS high cholesterol, averaged by diagnosis group, sex, and state"), 
               fill="Prevalence of\nhigh cholesterol"))
}

dev.off()

rm(brfss_chol, brfss_plot, nhanes_chol, nhanes_plot, plot_dt)

##############################################################################################################
######################################### # Save data ########################################################
##############################################################################################################

for(me in c('bp', 'chol')){
  
  # save BRFSS
  data <- brfss_data[!is.na(get(paste0('high_', me))) & !is.na(get(paste0(me, '_group'))), 
                     c('file_path', 'nid', 'ihme_loc_id', 'survey_module', 'survey_name', 'year_start', 'year_end', 
                       'strata', 'psu', 'pweight', 'sex_id', 'age_year', 'admin_1_id', 'admin_1_mapped',
                       paste0('high_', me), paste0(me, '_group')), with=F]
  
  for(yr in unique(data$year_start)){
    x <- data[year_start==yr,]
    write.csv(x, file=paste0(output_path, "FILEPATH/brfss_xwalked_micro_", yr, ".csv"), row.names = F)
  }
  
  # save NHANES
  data <- nhanes_data[!is.na(get(paste0('high_', me))) & !is.na(get(paste0(me, '_group'))), 
                     c('file_path', 'nid', 'ihme_loc_id', 'survey_module', 'survey_name', 'year_start', 'year_end', 
                       'strata', 'psu', 'pweight', 'sex_id', 'age_year', ifelse(me=='bp', 'sbp_mean_1_2_3', 'chl'),
                       paste0('high_', me), paste0(me, '_group')), with=F]
  
  for(yr in unique(data$year_start)){
    x <- data[year_start==yr,]
    write.csv(x, file=paste0(output_path, "FILEPATH/nhanes_micro_", yr, ".csv"), row.names = F)
  }
  
}
