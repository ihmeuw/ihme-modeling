## ******************************************************************************
##
## Purpose: Estimate prevalence at 7 days and 28 days by doing a life table 
##          calculation on estimated birth prevalence and EMR in the EN period, then LN period. 
## Input:   For enceph specifically: birth prevalence, EMR in ENN and LNN periods
##          From demographics team: all-cause mortality rate and a_x, which is the average
##          years lived in a given age interval by people who die in this interval
##
## Output:  prevalence in ENN, LNN, and at 28 days
## Created: 2020-05-12
## Last updated: 2020-07-28
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h <-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<- "PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

out_dir <- "PATHNAME"

pacman::p_load(data.table, openxlsx, tidyr, ggplot2, plotly)
library('Metrics', lib.loc = 'PATHNAME')
source("PATHNAME/get_population.R")
source("PATHNAME/get_location_metadata.R")
source("PATHNAME/get_life_table.R")
source("PATHNAME/get_crosswalk_version.R")
source("PATHNAME/get_covariate_estimates.R")
source("PATHNAME/get_model_results.R")
source("PATHNAME/get_demographics.R")
source("PATHNAME/utility.r")


# constants
t_7 <- 7/365
t_28 <- (28/365) - t_7
type <- 'exp' #enter 'exp' or 'life_table' to choose which calculation method to use

locs <- get_location_metadata(22, gbd_round_id = 7)[,.(location_id,path_to_top_parent,level,location_name,is_estimate)]
locs <- locs[is_estimate == 1]
locs <- locs[(location_id %in% c(44533,44793:44800)) == FALSE]

#pull a_x, all-cause mortality, and population at birth. These inputs do not have any uncertainty.
life_table <- get_life_table(location_id = unique(locs$location_id), gbd_round_id = 7, decomp_step = 'iterative', 
                                   with_hiv = 1, with_shock = 1, year_id = c(1990:2022), life_table_parameter_id = c(1,2),
                                   age_group_id = c(2,3), sex_id = c(1,2))
life_table[life_table_parameter_id == 1, param := 'amr']
life_table[life_table_parameter_id == 2, param := 'a']
life_table <- dcast(life_table, location_id + year_id + sex_id + run_id ~ param + age_group_id, value.var = 'mean')
life_table$run_id <- NULL

pop_birth <- get_population(age_group_id=164, location_id=unique(locs$location_id), year_id=c(1990:2022), sex_id=c(1,2), with_ui=TRUE, gbd_round_id=7,
                            decomp_step = 'iterative')
pop_birth$age_group_id <- NULL
pop_birth$run_id <- NULL
setnames(pop_birth, 'population', 'pop_birth')
dt <- merge(life_table, pop_birth, by = c('location_id', 'year_id', 'sex_id'), all.x = TRUE)

#calculate total population at relevant ages
dt <- as.data.table(dt)
dt[, pop_7 := pop_birth - (pop_birth * amr_2 * t_7)]
dt[, pop_28 := pop_7 - (pop_7 * amr_3 * t_28)]
dt[, pop_person_years_2 := (pop_7 * t_7) + (pop_birth * amr_2 * t_7 * a_2)]
dt[, pop_person_years_3 := (pop_28 * t_28) + (pop_7 * amr_3 * t_28 * a_3)]

#save population data so don't have to rerun every time
write.csv(dt, row.names=FALSE, file = paste0(out_dir, 'pulled_demographics_data.csv'))

dt <- fread(paste0(out_dir, 'pulled_demographics_data.csv'))

#pull enceph specific numbers - need to start running at draw level
#test data
#cases_birth <- data.table(location_id = 101, sex_id = 2, year_id = 2019, prev_birth = 0.02, emr_2 = 0.6, emr_3 = 0.2)

#load prev from best ST-GPR birth prev model. this data is location/year/age/sex specific.
run_id <- 159986
draw_path <- paste0('PATHNAME','/draws_temp_0/')
draw_names <- paste0('draw_', 0:999)
files <- list.files(draw_path)

fread_stgpr <- function(file) {
  dt <- fread(paste0(file))
  return(dt)
}

stgpr <- rbindlist(lapply(paste0(draw_path, files),FUN = fread_stgpr),
                   use.names = TRUE)
#prev <- stgpr[location_id %in% c(8,101,217)]
prev <- copy(stgpr)
prev$age_group_id <- NULL

prev[, split.id := .I]
metadata <- prev[, .(split.id, location_id, year_id, sex_id)]

prev_long <- as.data.table(melt.data.table(prev, id.vars = names(prev)[!grepl("draw", names(prev))], 
                                           measure.vars = patterns("draw"),
                                           variable.name = 'draw.id'))
setnames(prev_long, 'value', 'prev_birth')

prev_long <- merge(prev_long, dt, by = c('location_id', 'year_id', 'sex_id'), all.x = TRUE)

#load EMR data from best MR-BRT model. This data is location/year/sex/age specific (2 age groups, 2 sexes)
input_dir <- "PATHNAME"
emr <- fread(file = input_dir)
emr <- emr[year_id >= 1990]

emr2 <- emr[age_group_id == 2]
emr3 <- emr[age_group_id == 3]
emr2$age_group_id <- NULL
emr3$age_group_id <- NULL

#testing with means instead of draws
# draw_cols <- paste0("draw_", 0:999)
# emr2[,draw_cols] <- 0 
# emr3[,draw_cols] <- 0 

emr2_long <- as.data.table(melt.data.table(emr2, id.vars = names(emr2)[!grepl("draw", names(emr2))], 
                                           measure.vars = patterns("draw"),
                                           variable.name = 'draw.id'))
setnames(emr2_long, 'value', 'emr_2')

emr3_long <- as.data.table(melt.data.table(emr3, id.vars = names(emr3)[!grepl("draw", names(emr3))], 
                                           measure.vars = patterns("draw"),
                                           variable.name = 'draw.id'))
setnames(emr3_long, 'value', 'emr_3')

#more testing with means
# emr2_long[, emr_2 := emr]
# emr3_long[, emr_3 := emr]

prev_emr <- merge(prev_long, emr2_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'), all.x = TRUE)
prev_emr <- merge(prev_emr, emr3_long, by = c('location_id', 'year_id', 'sex_id', 'draw.id'), all.x = TRUE)

#calculate enceph prevalence
if (type == 'life_table') {
  prev_emr[, cases_birth := prev_birth * pop_birth]
  prev_emr[, deaths_enn := cases_birth * emr_2 * t_7]
  prev_emr[, cases_7 := cases_birth  - deaths_enn]
  
  prev_emr[, deaths_lnn := cases_7 * emr_3 * t_28]
  prev_emr[, cases_28 := cases_7 - deaths_lnn]
  
  prev_emr[, cases_person_years_2 := (cases_7 * t_7) + (deaths_enn * a_2)]
  prev_emr[, cases_person_years_3 := (cases_28 * t_28) + (deaths_lnn * a_3)]
  
  prev_emr[, prev_enn := cases_person_years_2 / pop_person_years_2]
  prev_emr[, prev_lnn := cases_person_years_3 / pop_person_years_3]
  prev_emr[, prev_28 := cases_28 / pop_28]
}

#instantaneous rate method
if (type == 'exp') {
  prev_emr[, cases_birth := prev_birth * pop_birth]
  prev_emr[, deaths_enn := cases_birth * (1 - exp(-emr_2 * t_7))]
  prev_emr[, cases_7 := cases_birth  - deaths_enn]
  
  prev_emr[, deaths_lnn := cases_7 * (1 - exp(-emr_3 * t_28))]
  prev_emr[, cases_28 := cases_7 - deaths_lnn]
  
  prev_emr[, cases_person_years_2 := (cases_7 * t_7) + (deaths_enn * a_2)]
  prev_emr[, cases_person_years_3 := (cases_28 * t_28) + (deaths_lnn * a_3)]
  
  prev_emr[, prev_enn := cases_person_years_2 / pop_person_years_2]
  prev_emr[, prev_lnn := cases_person_years_3 / pop_person_years_3]
  prev_emr[, prev_28 := cases_28 / pop_28]
  
}

# melt age columns into separate rows
prev_emr_long <- as.data.table(melt.data.table(prev_emr, id.vars = c('location_id', 'year_id', 'sex_id', 'draw.id', 'split.id'), 
                                               measure.vars = c('prev_birth', 'prev_enn', 'prev_lnn', 'prev_28'),
                                               variable.name = 'age_group'))
prev_emr_long[age_group == 'prev_birth', age_group := '0']
prev_emr_long[age_group == 'prev_enn', age_group := '0-6']
prev_emr_long[age_group == 'prev_lnn', age_group := '7-27']
prev_emr_long[age_group == 'prev_28', age_group := '28']

#cast draws back wide
prev_emr_draws <- dcast(prev_emr_long, location_id + year_id + sex_id + age_group ~ draw.id, value.var = 'value')
prev_emr_draws <- as.data.table(prev_emr_draws)
prev_emr_draws[age_group == '0', age_group_id := 164]
prev_emr_draws[age_group == '0-6', age_group_id := 2]
prev_emr_draws[age_group == '7-27', age_group_id := 3]
prev_emr_draws[age_group == '28', age_group_id := 999]

write.csv(prev_emr_draws, row.names = FALSE, file = paste0(out_dir,'prevalence_draws_trim10_',type,'_0810.csv'))

#format columns for the severity split script
prev_emr_draws$age_group <- NULL
write.csv(prev_emr_draws, row.names = FALSE, file = paste0(out_dir,'prevalence_draws_trim10_',type,'_0810_tobesplit.csv'))

# save prevalence and emr results for birth, EN, LN, and 28 days to ME 2525
source("PATHNAME/get_demographics.R"  )
template <- get_demographics("epi")
template_locs <- template[[1]]
template_ages <- template[[3]]
template_yrs <- template[[4]]

out_dir <- "PATHNAME"
prev_emr_draws <- fread('PATHNAME/prevalence_draws_trim10_exp_0810_tobesplit.csv')
prev_emr_draws$measure_id <- 5
prev_emr_draws[age_group_id == 999, age_group_id := 388]

input_dir <- 'PATHNAME/neonatal_enceph_modeled_emr_draws_0728.csv'
emr_draws <- fread(file = input_dir)
emr_draws$measure_id <- 6


draws <- rbind(prev_emr_draws, emr_draws)

for (loc_i in template_locs) {
  for (year_i in template_yrs) {
    for (sex_i in c(1,2)) {
      write.csv(draws[location_id == loc_i & year_id == year_i & sex_id == sex_i,], 
                file = paste0(out_dir, '/',loc_i,'_',year_i,'_',sex_i,'.csv'),
                row.names = FALSE)
    }
  }
}

## QSUB
job_flag <- '-N save_pre_severity'
project_flag <- paste0("-P proj_neonatal")
thread_flag <- "-l fthread=20"
mem_flag <- "-l m_mem_free=30G"
runtime_flag <- "-l h_rt=02:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q all.q"
next_script <- "PATHNAME"
errors_flag <- paste0("-e PATHNAME", Sys.getenv("USER"), "PATHNAME")
outputs_flag <- paste0("-o PATHNAME", Sys.getenv("USER"), "PATHNAME")
shell_script <- "-cwd /PATHNAME/r_shell_ide.sh"
me_id <- 2525
filepath <- paste0(out_dir, '/') 
desc <- 'prev_and_emr_results_from_life_table_082420_just_prevalence'
bun_id <- 338
xwalk_id <- 18551
meas_id <- c(5)

qsub <- paste("qsub ", job_flag, project_flag, thread_flag, mem_flag, runtime_flag, jdrive_flag, queue_flag,
              errors_flag, outputs_flag, shell_script, next_script, me_id, desc, filepath, bun_id, xwalk_id, meas_id)
system(qsub)




#collapse back into means for vetting and save
prev_results <- prev_emr[, .(prev_birth = mean(prev_birth),
                             prev_enn = mean(prev_enn),
                             prev_lnn = mean(prev_lnn),
                             prev_28 = mean(prev_28)), by = (split.id)] %>% merge(metadata, by = 'split.id')
write.csv(prev_results, row.names = FALSE, file = paste0('PATHNAME'))
  
# generate diagnostic plots
#test one off
prev_means_long <- as.data.table(melt.data.table(prev_results, 
                                                 id.vars = c('location_id', 'year_id', 'sex_id', 'split.id'), 
                                               measure.vars = c('prev_birth', 'prev_enn', 'prev_lnn', 'prev_28'),
                                               variable.name = 'age_group'))
prev_means_long[age_group == 'prev_birth', age_group := '0']
prev_means_long[age_group == 'prev_enn', age_group := '0-6']
prev_means_long[age_group == 'prev_lnn', age_group := '7-27']
prev_means_long[age_group == 'prev_28', age_group := '28']
write.csv(prev_means_long, row.names = FALSE, file = paste0(out_dir,'prevalence_means_long_trim10_',type,'_0810.csv'))

gg <- ggplot(data=prev_means_long[location_id == 8 & year_id %in% c(1990,2005,2015)], aes(x=age_group, y=value)) + 
  geom_boxplot() + facet_grid(year_id ~ sex_id) +
  theme_bw() +
  labs(title=paste0('Prevalence Age-Series'),
       x="Age", y="Prevalence") #+ scale_y_continuous(trans = "log10")

print(gg)

#launch full location set of plots
#Job specifications
username <- Sys.getenv("USER")
m_mem_free <- "-l m_mem_free=80G"
fthread <- "-l fthread=4"
runtime_flag <- "-l h_rt=06:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q i.q"
shell_script <- "-cwd /PATHNAME/r_shell_ide.sh"

### change to your own repo path if necessary
script <- ("PATHNAME/full_age_prevalence_diagnostics.R")
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")

job_name <- paste0("-N", " enceph_plots")
job <- paste("qsub", m_mem_free, fthread, runtime_flag,
             jdrive_flag, queue_flag,
             job_name, "-P proj_neonatal", outputs_flag, errors_flag, shell_script, script, out_dir)

system(job)


# scatter 28d prevalence between gbd rounds
out_dir <- "PATHNAME"
new_results <- fread(file = paste0(out_dir,'prevalence_means_long_trim10_exp_0810.csv'))
new_results <- new_results[age_group == "28"]

# draw_cols <- paste0("draw_", 0:999)
# old_results <- old_results[, lapply(.SD, as.numeric), .SDcols = draw_cols]
# old_results_labels <- old_results[, .(age_group_id, sex_id, year_id, location_id)]
# old_results <- cbind(old_results_labels, old_results2)
# 
# old_results$mean <- rowMeans(old_results[,draw_cols,with=FALSE], na.rm = T)
# old_results$lower <- old_results[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
# old_results$upper <- old_results[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
# old_results <- old_results[,-draw_cols,with=FALSE]
# old_results <- old_results[age_group_id != 'age_group_id']

old_results <- fread(file = 'PATHNAME/all_28_day_means.csv')
old_results <- unique(old_results[, .(sex_id, location_id, year_id, mean, lower, upper)])

old_data <- get_crosswalk_version(crosswalk_version_id = 12515)
old_data <- old_data[measure == 'prevalence']
old_data <- old_data[age_start > 0.076]
old_data[sex == 'Female', sex_id := 2]
old_data[sex == 'Male', sex_id := 1]
old_data[, year_id := year_start]
old_data$age_group <- "28"
old_data <- old_data[, .(location_id, year_id, sex_id, age_group, mean, lower, upper)]

compare <- merge(new_results, old_results, by = c('location_id', 'year_id', 'sex_id'))

locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7,
                              decomp_step = 'iterative')
compare <- merge(compare, locs, by = 'location_id', all.x = TRUE)

out_dir <- 'PATHNAME'
pdf(file = paste0(out_dir, "/scatter_dismodbest_v_",run_id,"_bysex_28d.pdf"),
    width = 12, height = 8)

for (year_i in sort(unique(compare$year_id))) {
  
  gg <- ggplot() +
    geom_point(data = compare[year_id == year_i], 
               aes(x = mean, y = value, 
                   color = region_name, 
                   text = paste0(location_name, ', dismod: ', round(mean,4),
                                 ', stgpr: ', round(value,4))),
               alpha = 0.5) +
    geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
    facet_wrap(~ sex_id) + 
    labs(title = paste0('Neonatal Enceph Prevalence at 28 days - Method Comparison, Year: ',year_i),
         x = 'DisMod GBD2019 Best (Model Version ID 427685)',
         y = paste0('ST-GPR, Run ID: ', run_id))
  print(gg)
  #ggplotly(p = gg, tooltip = 'text')
}

for (year_i in sort(unique(compare$year_id))) {
  gg1 <- ggplot() +
    geom_point(data = compare[year_id == year_i], 
               aes(x = mean, y = value, 
                   color = region_name, 
                   text = paste0(location_name, ', dismod: ', round(mean,4),
                                 ', stgpr: ', round(value,4))),
               alpha = 0.5) +
    geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
    scale_x_log10() +
    scale_y_log10() +
    facet_wrap(~ sex_id) + 
    labs(title = paste0('Neonatal Enceph Prevalence at 28 days - Method Comparison, Year: ',year_i),
         x = 'DisMod GBD2019 Best (Model Version ID 427685)',
         y = paste0('ST-GPR, Run ID: ', run_id))
  print(gg1)
  #ggplotly(p = gg1, tooltip = 'text')
}

dev.off()



# scatter ENN and LNN prevalence input data between gbd rounds

# pull ENN and LNN prev data from old best crosswalk version
old_data <- get_crosswalk_version(crosswalk_version_id = 10625)
old_data <- old_data[measure == 'prevalence']

#start without the step4 data that you forgot to age split, and see if that scatter is
#informative enough
old_data <- old_data[age_start == 0.019 | age_end == 0.019]
old_data <- old_data[, .(location_id, year_start, year_end, age_start, age_end, sex, mean, is_outlier)]
old_data$version <- 'GBD2019'
old_data[, year_id := floor((year_start + year_end)/2)]

# pull new ENN and LNN prevalence data
new_data <- fread("PATHNAME")
new_data <- new_data[, .(location_id, year_id, age_start, age_end, sex, val, is_outlier)]
setnames(new_data, 'val', 'mean')
new_data$version <- 'GBD2020'

data <- rbind(new_data, old_data, fill = TRUE)
data[location_id %in% c(4911:4919,4921,4922,4927,4928), location_id := 90]

haqi <- get_covariate_estimates(1099, decomp_step='iterative')[,.(location_id,year_id,mean_value)]
setnames(haqi,"mean_value","haqi")

data <- merge(data, haqi, by = c('location_id', 'year_id'), all.x = TRUE)
data <- merge(data, locs, by = 'location_id', all.x = TRUE)

data[age_start == 0, age_group_label := 'Early Neonatal']
data[age_start > 0, age_group_label := 'Late Neonatal']

pdf(file = "PATHNAME",
    width = 12, height = 8)

  gg1 <- ggplot() +
    geom_point(data = data[is_outlier == 0], 
               aes(x = haqi, y = mean, 
                   color = region_name, 
                   text = paste0(location_name, ', haqi: ', round(haqi,4),
                                 ', data: ', round(mean,4))),
               alpha = 0.5) +
    geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
    facet_wrap(age_group_label ~ version) + 
    labs(title = paste0('Neonatal Enceph Prevalence Data, No Outliers'),
         x = 'HAQI',
         y = 'Prevalence')
  print(gg1)
  #ggplotly(p = gg1, tooltip = 'text')
  
  gg2 <- ggplot() +
    geom_point(data = data, 
               aes(x = haqi, y = mean, 
                   color = region_name, 
                   text = paste0(location_name, ', haqi: ', round(haqi,4),
                                 ', data: ', round(mean,4))),
               alpha = 0.5) +
    geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
    facet_wrap(age_group_label ~ version) + 
    labs(title = paste0('Neonatal Enceph Prevalence Data, Including Outliers'),
         x = 'HAQI',
         y = 'Prevalence')
  print(gg2)

dev.off()

# calculate RMSE between prevalence model estimates and prevalence data
# use data from above
# pull prev estimates from old dismod model
dismod <- get_model_results(gbd_team = 'epi', gbd_id = 2525, decomp_step='step4', status='best',
                                 gbd_round_id = 6, age_group_id = c(2,3), sex_id = c(1,2), measure_id = 5)
dismod <- dismod[, .(location_id, year_id, age_group_id, sex_id, mean, lower, upper)]
dismod$version <- 'GBD2019'

# pull prev estimates from new approach
template <- get_demographics(gbd_team = 'epi', gbd_round_id = 7)
temp_yrs <- template[[4]]
out_dir <- "PATHNAME"
new_results <- fread(file = paste0(out_dir,'prevalence_means_long_trim10_exp_0804.csv'))
new_results <- new_results[age_group %in% c('0-6','7-27') & year_id %in% temp_yrs]
new_results[age_group == '0-6', age_group_id := 2]
new_results[age_group == '7-27', age_group_id := 3]
new_results$version <- 'GBD2020'
setnames(new_results, 'value', 'mean')

est <- rbind(dismod, new_results, fill = TRUE)

# combine and plot
data[age_group_label == 'Early Neonatal', age_group_id := 2]
data[age_group_label == 'Late Neonatal', age_group_id := 3]
data[sex == 'Female', sex_id := 2]
data[sex == 'Male', sex_id := 1]

data[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
data[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]

data[est_year_id < 1990, est_year_id := 1990]
data[year_id == 2018 | year_id == 2019, est_year_id := 2019]
data[year_id > 2019, est_year_id := year_id]
data[, year_id := est_year_id]

data_and_est <- merge(data, est, by = c('location_id', 'year_id', 'sex_id','age_group_id','version'), all.x = TRUE)

rmse(data_and_est[version == 'GBD2019' & is_outlier == 0, mean.x], data_and_est[version == 'GBD2019' & is_outlier == 0, mean.y])
rmse(data_and_est[version == 'GBD2020'& is_outlier == 0, mean.x], data_and_est[version == 'GBD2020' & is_outlier == 0, mean.y])

