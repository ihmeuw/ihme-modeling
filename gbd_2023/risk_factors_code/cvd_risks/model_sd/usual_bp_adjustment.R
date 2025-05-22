# Author: USERNAME
# Date: DATE
# Description: Produces age-specific ratios for usual blood pressure adjustment

# ==============================================================================

rm(list=ls())

# Packages
library(data.table)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(Metrics)
library(viridis)
library(RColorBrewer)

# Shared functions
central <- "/FILEPATH/"
source(paste0(central, 'get_age_metadata.R'))

# Usual BP functions
source('/FILEPATH/usual_bp_adjustment_functions.R')

# Filepaths
long_folder <- '/FILEPATH/'
output_folder <- '/FILEPATH/'

# Arguments
release <- 16 # gbd release id
ave_meas <- F  # average over measurement @ each time point?
form <- "sbp~bs(age_year)+factor(sex_id)+(1|id)" # model formula

##############################################################################################################
################################# # Get age metadata #########################################################
##############################################################################################################

age_meta <- get_age_metadata(release_id = release)

##############################################################################################################
################################# # Compile data #############################################################
##############################################################################################################

# subset to currently utilized longitudinal surveys
files <- list.files(long_folder, full.names=F, recursive = T)
files <- files[grepl('CHN/CHRLS|IDN/FLS|USA/NHANES|ZAF/NIDS', files)]

# this is for matching studies up, so that they can be formatted together
files <- data.table(files=paste0(long_folder, files), 
                    id=unlist(lapply(strsplit(files, "_|\\."), "[", 1)), 
                    yr=unlist(lapply(strsplit(files, "_|\\."), "[", 2)))

# add in longitudinal data on limited use
lu_files <- fread(paste0(long_folder, 'limited_use_filepaths.csv'))
files <- rbind(files, lu_files)
files <- files[order(id, yr)]

loaded_data <- load_data(files, ave_meas)

full<-rbindlist(loaded_data$full, fill=T)
meta<-rbindlist(loaded_data$meta, fill=T)
rm(loaded_data)

table(full$survey_name, full$followup)
print(meta)
write.csv(meta, paste0(output_folder, 'usual_bp_metadata.csv'))

##############################################################################################################
################################# # Validate data ############################################################
##############################################################################################################

full <- validate_data(full)

##############################################################################################################
################################# # Data diagnostics #########################################################
##############################################################################################################

pdf(file=paste0(output_folder, "long_data_diagnostics", ifelse(ave_meas, "_ave_meas", ""), ".pdf"), width=12, height=7)

full[, survey_span := paste0(sort(unique(year_start)), collapse = ' | '), by = 'survey_name']

# BP histograms
ggplot(full[!is.na(age_year) & !is.na(sbp) & !is.na(sex_id) & !is.na(id) & sbp>80 & sbp<270 & age_year >= 25], 
       aes(x = sbp, color = as.factor(followup))) +
  geom_density() + theme_bw() + facet_wrap(~paste0(survey_name, '\n', survey_span), scales = 'free') + 
  labs(x='Measured SBP', color = 'Followup period')

# BP by age
ggplot(full[!is.na(age_year) & !is.na(sbp) & !is.na(sex_id) & !is.na(id) & sbp>80 & sbp<270 & age_year >= 25], 
       aes(x=age_year, y=sbp, color=factor(sex_id, levels=c(1,2), labels=c('Male', 'Female')))) +
  facet_wrap(~paste0(survey_name, '\n', survey_span), scales = 'free') + geom_point(alpha=.1) +
  geom_smooth(se=F) + theme_bw() + labs(x='Age', y='Measured SBP', color=NULL)

full[, survey_span := NULL]

# coefficient of variation by age
plot_var <- full[!is.na(age_year) & !is.na(sbp) & !is.na(sex_id) & !is.na(id) & sbp>80 & sbp<270 & age_year >= 25]
plot_var[, merge_age_year := age_year]
plot_var <- plot_var[age_meta, on = .(merge_age_year >= age_group_years_start, merge_age_year < age_group_years_end),
                     nomatch = 0][, c(names(plot_var), 'age_group_id', 'age_group_name'), with=F]
plot_var[, merge_age_year := NULL]
plot_var[followup==1, baseline_age := age_group_id]
plot_var <- plot_var[order(survey_name, id, followup)]
plot_var[, baseline_age := nafill(baseline_age, type='locf'), by = c('survey_name', 'id')]
plot_var[, follow_up_mean := mean(sbp), by=c('survey_name', 'id', 'followup')]
plot_var[, max_followup := max(followup), by = c('survey_name')]
plot_var[, n_followup := length(unique(followup)), by=c('survey_name', 'id')]

plot_var_all_fu <- unique(plot_var[n_followup == max_followup,.(survey_name, id, followup, age_year, year_start, year_end, baseline_age, follow_up_mean)])
plot_var_all_fu <- merge(plot_var_all_fu, age_meta[,.(age_group_id, age_group_name, age_group_years_start)], by.x='baseline_age', by.y='age_group_id')
plot_var_all_fu[, n_ids := length(unique(id)), by = c('age_group_years_start', 'survey_name')]
plot_var_all_fu <- plot_var_all_fu[n_ids > 100]
plot_var_all_fu[, temporal_cv := sd(follow_up_mean)/mean(follow_up_mean), by=c('survey_name', 'id')]
plot_var_all_fu[, survey_span := paste0(sort(unique(year_start)), collapse = ' | '), by = 'survey_name']

ggplot(plot_var_all_fu, aes(x = age_group_years_start, y = temporal_cv)) + theme_bw() + geom_boxplot(aes(group = age_group_years_start)) + 
  facet_wrap(~paste0(survey_name, '\n', survey_span)) + geom_smooth(method = "loess") + 
  labs(x = 'Age at baseline', y = 'Coefficient of variation', title = 'Variation between mean survey period BP readings within an individual')

plot_var_subset_fu <- unique(plot_var[max_followup > 2 & n_followup == max_followup,.(survey_name, id, followup, age_year, year_start, year_end, baseline_age, follow_up_mean)])
plot_var_subset_fu <- merge(plot_var_subset_fu, age_meta[,.(age_group_id, age_group_name, age_group_years_start)], by.x='baseline_age', by.y='age_group_id')
plot_var_subset_fu[, n_ids := length(unique(id)), by = c('age_group_years_start', 'survey_name')]
plot_var_subset_fu <- plot_var_subset_fu[n_ids > 100]
for(i in 1:(length(unique(plot_var_subset_fu$followup))-1)){
  temp <- plot_var_subset_fu[followup %in% c(i, (i+1))]
  temp[, temporal_cv := sd(follow_up_mean)/mean(follow_up_mean), by=c('survey_name', 'id')]
  temp[, survey_span := paste0(sort(unique(year_start)), collapse = ' | '), by = 'survey_name']
  temp[, followup_comps := length(unique(followup))==2, by='survey_name']
  p <- ggplot(temp[followup_comps==T], aes(x = age_group_years_start, y = temporal_cv)) + theme_bw() + geom_boxplot(aes(group = age_group_years_start)) + 
    facet_wrap(~paste0(survey_name, '\n', survey_span)) + geom_smooth(method = "loess") + 
    labs(x = 'Age at baseline', y = 'Coefficient of variation', title = 'Variation between mean survey period BP readings within an individual')
  print(p)
}

dev.off()

##############################################################################################################
################################# # Run regression ###########################################################
##############################################################################################################

pdf(file=paste0(output_folder, 'usual_BP', ifelse(ave_meas, "_ave_meas", ""), '.pdf'), width=9.5)

stack <- list()

for(surv in unique(full$survey_name)){
  
  survey_dt <- full[survey_name==surv & !is.na(age_year) & !is.na(sbp) & !is.na(sex_id) & !is.na(id) & sbp>80 & sbp<270,]
  
  message(paste0("\nComputing for ", surv, "\n"))

  # create categorical age groups
  survey_dt[, merge_age_year := age_year]
  survey_dt <- survey_dt[age_meta, on = .(merge_age_year >= age_group_years_start, merge_age_year < age_group_years_end),
             nomatch = 0][, c(names(survey_dt), 'age_group_id', 'age_group_name'), with=F]
  survey_dt[, merge_age_year := NULL]
  
  # subset to modeled age groups
  survey_dt <- survey_dt[age_year >= 25]
  
  # fit the model
  mod <- lmer(form, survey_dt)
  summary(mod)
  
  # extract individual intercepts
  indiv_intercept <- coef(mod)$id['(Intercept)']
  indiv_intercept <- cbind(data.table(id = as.numeric(rownames(indiv_intercept))), data.table(indiv_intercept))
  indiv_intercept[, type := 'Model individual intercepts']
  indiv_intercept <- rbind(indiv_intercept, survey_dt[,.(`(Intercept)` = mean(sbp), type = 'Measured mean BP by individual'), by='id'])
  indiv_intercept <- merge(indiv_intercept, unique(survey_dt[followup==1, .(id, age_group_id, age_group_name)]), by='id')
  
  # get coefficients
  coefs <- coef(summary(mod))[,'Estimate']
  coefs <- as.data.table(as.list(coefs))
  final_coefs <- data.table()
  for(age in unique(floor(survey_dt$age_year))){
    for(sx in c(1,2)){
      coefs[, age_group_yr := age]
      coefs[, sex_id := sx]
      final_coefs <- rbind(final_coefs, coefs)
    }
  }
  final_coefs <- cbind(final_coefs, bs(final_coefs$age_group_yr))
  
  # predict on data
  survey_dt[, preds := predict(mod, newdata=survey_dt)]
  survey_dt[, diff := preds-sbp]
  base <- survey_dt[followup==1]
  
  # get rmse
  rmse <- sqrt(mean(base$diff^2))
  rmse
  
  # get variance
  adj_factors <- base[, .(ratio = sqrt(var(preds)/var(sbp)), sample_size = length(unique(id))), by='age_group_id']
  adj_factors <- adj_factors[sample_size > 100]
  
  # plot density plots of pre-and post-modeled sbp
  plot_dt <- data.table::melt(base, id.vars = c('id', 'followup', 'survey_name', 'sex_id', 'age_year', 'year_start', 'year_end', 'nid', 'k', 'age_group_id', 'age_group_name'),
                  measure.vars=c('sbp', 'preds'))
  p <- ggplot(plot_dt, aes(x=value, color=factor(variable, levels=c('sbp', 'preds'), labels=c('Measured', 'Modelled')))) + 
    theme_classic() + labs(color = NULL, x = 'SBP', title = paste0('Baseline ', surv)) + scale_color_manual(values=c('black', 'cornflowerblue')) +
    geom_density(linewidth=1.5)
  
  print(p)
  print(p + facet_wrap(~age_group_name, scales='free'))
  
  # show all relevant values for a single individual
  survey_dt[, N := .N, by='id']
  ex_id <- c(unique(survey_dt[N == max(survey_dt$N) & id %in% unique(base$id) & age_year==35, id])[10],
             unique(survey_dt[N == max(survey_dt$N) & id %in% unique(base$id) & age_year==50, id])[10],
             unique(survey_dt[N == max(survey_dt$N) & id %in% unique(base$id) & age_year==65, id])[10],
             unique(survey_dt[N == max(survey_dt$N) & id %in% unique(base$id) & age_year==80, id])[10])
  measured <- survey_dt[id %in% ex_id,]
  measured[, type := 'Measured']
  measured[, info := paste0('ID ', id, ', Followup ', followup, ' (', year_start, '-', year_end, ') : age ', floor(age_year), ', sex_id ', sex_id)]
  measured[, overall_info := paste0(sort(unique(info)), collapse='\n'), by='id']
  measured[, overall_info := factor(overall_info, levels = unique(measured[order(age_year), overall_info]))]
  
  aver <- measured[, list(aver = mean(sbp), type = 'Measured Average'), by=c('id', 'followup', 'overall_info')]
  
  preds <- unique(survey_dt[id %in% ex_id, .(id, followup, year_start, year_end, nid, age_year, sex_id, preds)])
  preds <- data.table::melt(preds, id.vars=c('id', 'followup', 'year_start', 'year_end', 'nid', 'age_year', 'sex_id'))
  preds[, type := 'Modelled']
  preds[, info := paste0('ID ', id, ', Followup ', followup, ' (', year_start, '-', year_end, ') : age ', floor(age_year), ', sex_id ', sex_id)]
  preds[, overall_info := paste0(sort(unique(info)), collapse='\n'), by='id']
  preds[, overall_info := factor(overall_info, levels = unique(preds[order(age_year), overall_info]))]
  
  p<-ggplot(data=measured, aes(x=factor(followup), y=sbp, color=type)) + facet_wrap(~overall_info, scales='free_y', nrow=2) +
    geom_point(size=3, aes(shape=k)) +
    xlab("Measurement period") +
    theme_classic() + labs(shape=NULL, color=NULL, y = 'SBP', title = paste0('Example individuals: ', surv)) +
    geom_point(data=preds, aes(y=value), size=3, shape=ifelse(preds$followup==1, 16, 4))+
    geom_line(data=preds, aes(y=value, group=variable))+
    geom_point(data=aver, aes(y=aver), size=3) + scale_color_manual(values=c('black', 'red', 'cornflowerblue')) +
    scale_shape_manual(values=c(8, 17, 0))
  print(p)
  
  # plot coefficients
  final_coefs[sex_id==1, pred_sbp := `(Intercept)` + (`bs(age_year)1` * `1`) + (`bs(age_year)2` * `2`) + (`bs(age_year)3` * `3`)]
  final_coefs[sex_id==2, pred_sbp := `(Intercept)` + (`bs(age_year)1` * `1`) + (`bs(age_year)2` * `2`) + (`bs(age_year)3` * `3`) + `factor(sex_id)2`]
  final_coefs[, type := 'Modelled']
  
  measured_means <- copy(survey_dt)
  measured_means[, age_year := floor(age_year)]
  measured_means <- measured_means[, list(mean = mean(sbp, na.rm=T), N = .N), by = c('sex_id', 'age_year')]
  measured_means[, type := 'Measured']
  
  p <- ggplot(final_coefs, aes(x = age_group_yr, y = pred_sbp, color = factor(sex_id, levels=c(1,2), labels=c('Male', 'Female')))) + 
    theme_bw() + geom_point(aes(shape = type), alpha = .6) +
    labs(x = 'Age', y = 'Predicted SBP', color = NULL, shape = NULL, linetype = NULL,
         title = paste0(surv, ' coefficients')) +
    geom_line(data = measured_means, aes(x = age_year, y = mean, linetype = type, color = factor(sex_id, levels=c(1,2), labels=c('Male', 'Female')))) +
    scale_shape_manual(values=c(0, 8))
  print(p)
  
  # plot individual intercepts
  p <- ggplot(indiv_intercept, aes(x=`(Intercept)`, color=type)) + geom_density(linewidth=1.5) + theme_bw() + 
    labs(color=NULL, x='SBP', title = paste0(surv, ' individual intercepts from model and mean measured BP readings by individual'), subtitle='Baseline sample') + 
    scale_color_manual(values=c('black', 'cornflowerblue')) + theme(legend.position = 'bottom')
  print(p)
  print(p + facet_wrap(~age_group_name, scales='free') + labs(subtitle='Baseline sample, faceted by age group at baseline'))
  
  
  stack[[surv]] <- cbind(adj_factors, survey=surv)
}

##############################################################################################################
################################# # Combine across surveys ###################################################
##############################################################################################################

final <- rbindlist(stack)
final <- merge(final, age_meta[,.(age_group_id, age_group_years_start)], by='age_group_id')
setnames(final, "age_group_years_start", "age_start")

collapsed <- final[, .(ratio=mean(ratio), sample_size=sum(sample_size)), by=c('age_group_id', 'age_start')]

# fill in for oldest ages
max_age <- 80 #max(collapsed$age_start)
oldest_ratio <- collapsed[age_start == max_age, ratio]
new_age_groups_ids <- age_meta[age_group_years_start > max_age, .(age_group_id, age_group_years_start)]
setnames(new_age_groups_ids, 'age_group_years_start', 'age_start')
new_age_groups_ids[, `:=` (ratio = oldest_ratio, survey = 'Collapsed')]

collapsed <- rbind(collapsed[age_start <= max_age], new_age_groups_ids, fill=T)

# plot collapsed age-specific ratios 
p<-ggplot(data=collapsed, aes(x=age_start, y=ratio))+
  geom_line(linewidth=2)+
  xlab("Age")+
  ylab("Adjustment factor")+
  theme_classic()+
  theme(text=element_text(size=20))+
  geom_hline(yintercept = c(0,1))
if(max(collapsed$ratio)<1){
  p <- p + ylim(0,1)
} else {
  p <- p + ylim(0,NA)
}
print(p)

# plot age- and survey-specific ratios
collapsed[, survey := "Collapsed"]
final_comb <- rbind(final, collapsed)
final_comb[, survey:=factor(survey, levels=c('Collapsed', sort(unique(final$survey))))]

p<-ggplot(data=final_comb, aes(x=age_start, y=ratio, color=survey))+
  geom_point()+
  geom_line()+
  ggtitle("Correction factors by survey")+
  xlab("Age")+
  ylab("Adjustment factor")+
  theme_classic()+
  guides(color=guide_legend(nrow=2, byrow=T))+
  theme(legend.position="bottom", legend.title=element_blank())+
  scale_color_manual(values=c('black', brewer.pal(length(unique(final$survey)), 'Dark2')))
if(max(final_comb$ratio)<1){
  print(p + ylim(0,1) + geom_hline(yintercept = c(0,1)))
} else {
  print(p + ylim(0,NA) + geom_hline(yintercept = c(0,1)))
}
print(p)

dev.off()

# save final ratios
write.csv(collapsed, file=paste0(output_folder, 'sbp_usual_bp_adj_ratios.csv'), row.names=F)
