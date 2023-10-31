## ******************************************************************************
##
## Purpose: Prepare a dataset of excess mortality (EMR) data that is age and sex specific to model
##          in MR-BRT.
##          -Pull all the EMR data where the source covers both ENN and LNN
##          -Pool the data together by age group, and calculate the ratio of ENN / LNN
##          -Pull the EMR data that covers the whole neonatal period, and apply the age ratio to age-split
## Input:   bundle ID
## Output:  
## Created: 2020-06-23
## Last updated: 2020-07-01
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

pacman::p_load(data.table, openxlsx, msm)

source("PATHNAME/get_bundle_data.R")
source("PATHNAME/save_bundle_version.R")
source("PATHNAME/get_bundle_version.R")
source("PATHNAME/get_crosswalk_version.R")
source("PATHNAME/get_model_results.R")
source("PATHNAME/get_draws.R")
source("PATHNAME/get_location_metadata.R")
source("PATHNAME/upload_bundle_data.R")
source("PATHNAME/functions_agesex_split.R")

# estBetaParams <- function(mu, var) {
#   alpha <- ((1 - mu) / var - 1 / mu) * mu ^ 2
#   beta <- alpha * (1 / mu - 1)
#   return(params = list(alpha = alpha, beta = beta))
# }

#pull prevalence data (should all be age and sex specific at this point in processing)
dt <- fread(file = "PATHNAME/338_prevalence_neonatal_only_with_mrbrt_outliers.xlsx")
dt <- dt[age_end > 0]
#dt <- dt[is_outlier == 0]

dt[sex == 'Male', sex_id := 1]
dt[sex == 'Female', sex_id := 2]
setnames(dt, 'val', 'mean')
dt[, seq := crosswalk_parent_seq]

#calculate total counts of cases in males and females in ENN age group
case_weight_f <- sum(dt[sex == 'Female', cases]) / sum(dt$cases)
case_weight_m <- sum(dt[sex == 'Male', cases]) / sum(dt$cases)


#pull cod results for the corresponding demographic combinations
deaths <- get_model_results(gbd_team = 'cod', gbd_id = 382, decomp_step='step3', status='best',
                            gbd_round_id = 7, age_group_id = c(2,3), sex_id = c(1,2),
                            year_id = unique(dt$year_id), location_id = unique(dt$location_id),
                            measure_id = 1)

age_pool <- merge(dt, deaths, by = c('location_id', 'year_id', 'age_group_id', 'sex_id'), all.x = TRUE)

#calculate total counts of deaths in males, females, ENN, and LNN
m_f_total <- sum(age_pool[sex == 'Male', mean_death]) / sum(age_pool[sex == 'Female', mean_death])
m_f_2 <- sum(age_pool[sex == 'Male' & age_group_id == 2, mean_death]) / sum(age_pool[sex == 'Female'& age_group_id == 2, mean_death])
m_f_3 <- sum(age_pool[sex == 'Male' & age_group_id == 3, mean_death]) / sum(age_pool[sex == 'Female'& age_group_id == 3, mean_death])

death_weight_f <- sum(age_pool[sex == 'Female', mean_death]) / (sum(age_pool[, mean_death]))
death_weight_m <- sum(age_pool[sex == 'Male', mean_death]) / (sum(age_pool[, mean_death]))

death_weight_2_f <- sum(age_pool[sex == 'Female' & age_group_id == 2, mean_death]) / (sum(age_pool[sex == 'Female', mean_death]))
death_weight_3_f <- sum(age_pool[sex == 'Female' & age_group_id == 3, mean_death]) / (sum(age_pool[sex == 'Female', mean_death]))

death_weight_2_m <- sum(age_pool[sex == 'Male' & age_group_id == 2, mean_death]) / (sum(age_pool[sex == 'Male', mean_death]))
death_weight_3_m <- sum(age_pool[sex == 'Male' & age_group_id == 3, mean_death]) / (sum(age_pool[sex == 'Male', mean_death]))


#pull all the lit CFR data, and split any that is not age and sex specific
cfr_data <- get_bundle_version(bundle_version_id = 30299, fetch = 'all')
bundle_data <- copy(cfr_data)
cfr_data[, year_id := floor((year_start + year_end)/2)]
cfr_data[, `:=` (orig_sex = sex, orig_age_start = age_start, orig_age_end = age_end,
           orig_cases = cases, orig_sample_size = sample_size)]

#split both sex data first
needs_split <- copy(cfr_data[measure == 'mtexcess' & sex == 'Both'])
male_copy <- copy(needs_split[, sex := 'Male'])
female_copy <- needs_split[, sex := 'Female']
needs_split <- rbind(male_copy, female_copy)

needs_split[sex == 'Female', sample_size_adjusted := sample_size * case_weight_f]
needs_split[sex == 'Male', sample_size_adjusted := sample_size * case_weight_m]

needs_split[sex == 'Female', cases_adjusted := cases * death_weight_f]
needs_split[sex == 'Male', cases_adjusted := cases * death_weight_m]

needs_age_split <- needs_split[age_start == 0 & age_end > 0.07]
done <- fsetdiff(needs_split, needs_age_split, all = TRUE)

#then split into ENN and LNN
sex_specific <- copy(cfr_data[measure == 'mtexcess' & sex != 'Both'])
sex_specific[, cases_adjusted := cases]
needs_age_split <- rbind(needs_age_split, sex_specific, fill = TRUE)

needs_age_split[sex == 'Female', `:=` (deaths_2_adjusted = cases_adjusted * death_weight_2_f,
                                       deaths_3_adjusted = cases_adjusted * death_weight_3_f)]
needs_age_split[sex == 'Male', `:=` (deaths_2_adjusted = cases_adjusted * death_weight_2_m,
                                     deaths_3_adjusted = cases_adjusted * death_weight_3_m)]
needs_age_split[is.na(sample_size_adjusted), sample_size_adjusted := sample_size]
needs_age_split[, `:=` (cfr_2_adjusted = deaths_2_adjusted / sample_size_adjusted)]
needs_age_split[, `:=` (cfr_3_adjusted = deaths_3_adjusted / (sample_size_adjusted - (sample_size_adjusted * cfr_2_adjusted)))]

check <- rbind(needs_age_split, done, fill = TRUE)
check[, neonatal_cfr := cases / sample_size]
write.csv(check, file = 'PATHNAME/testing_7_24.csv')


#reshape into one row per age group
age_split <- melt(needs_age_split, measure.vars = c('deaths_2_adjusted', 'deaths_3_adjusted'))
age_split[variable == 'deaths_2_adjusted', `:=` (cases = value, age_start = 0, age_end = 7/365,
                                                 mean = cfr_2_adjusted, sample_size = sample_size_adjusted)]
age_split[variable == 'deaths_3_adjusted', `:=` (cases = value, age_start = 7/365, age_end = 28/365,
                                                 mean = cfr_3_adjusted)]
age_split[variable == 'deaths_3_adjusted', sample_size := cases / mean]

#clean up columns
done <- subset(done, select = -c(cases, sample_size))
setnames(done, c('sample_size_adjusted', 'cases_adjusted'), c('sample_size', 'cases'))
done[, mean := cases/sample_size]
age_split <- subset(age_split, select=-c(value, variable, cfr_2_adjusted, cfr_3_adjusted, sample_size_adjusted, cases_adjusted))

split_emr_data <- rbind(done, age_split, fill = TRUE)

#convert the split CFR data into EMR
split_emr_data[ , `:=` (mean = -log(1-mean)/(age_end - age_start))]

# clear out the original upper, lower and standard_error in order to let the epi uploader calculate these values
split_emr_data[, `:=` (lower = NA, upper = NA, standard_error = NA, uncertainty_type_value = NA)]

# add age group ids
split_emr_data[age_start == 0, age_group_id := 2]
split_emr_data[age_start > 0.019, age_group_id := 3]


#calculate EMR from all the PREVALENCE data by taking CSMR/prevalence
deaths <- get_draws(gbd_id_type = 'cause_id', source = 'codem', gbd_id = 382, decomp_step='step3', status='best',
                            gbd_round_id = 7, age_group_id = c(2,3), sex_id = c(1,2),
                            year_id = unique(dt$year_id), location_id = unique(dt$location_id),
                            measure_id = 1, metric_id = 1)
draw_cols <- paste0("draw_", 0:999)
deaths[, (draw_cols) := lapply(.SD, function(x) x / pop ), .SDcols = draw_cols]

deaths <- melt.data.table(deaths, id.vars = names(deaths)[!grepl("draw", names(deaths))], 
                      measure.vars = patterns("draw"),
                      variable.name = 'draw.id')

# copy the prevalence data and create empty draw columns
dt_draws <- copy(dt)
dt_draws[,draw_cols] <- 0 

#if standard error is 0, calculate it from sample_size
z <- qnorm(0.975)
dt_draws[(is.na(standard_error) | standard_error == 0) & (measure == "prevalence" | measure == "proportion"),
   standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]

dt_draws <- melt.data.table(dt_draws, id.vars = names(dt_draws)[!grepl("draw", names(dt_draws))], 
                                 measure.vars = patterns("draw"),
                                 variable.name = 'draw.id')

dt_draws <- merge(dt_draws, deaths, by = c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw.id'), all.x = TRUE)
setnames(dt_draws, c('value.x', 'value.y'), c('prev', 'csmr'))

# make 1000 copies of the original prevalence value 
dt_draws[, prev := mean]

#sample to use uncertainty in the clinical data
# dt_draws <- split(dt_draws, by = "seq")
# 
# # then apply this function to each of those data tables in that list
# dt_draws <- lapply(dt_draws, function(input_i){
#   
#   beta_params <- estBetaParams(mu = unique(input_i$mean), var = (unique(input_i$standard_error))^2)
# 
#   #Generate a random draw for each mean and se
#   set.seed(123)
#   input.draws <- rbeta(1000, shape1 = beta_params[[1]], shape2 = beta_params[[2]])
#   input_i[, prev := input.draws]
# 
#   #if mean is 0, set the draws to 0 (beta distribution doesn't handle this on its own)
#   input_i[mean == 0, prev := 0]
#   # 
#   # mean.vector <- input_i$mean
#   # se.vector <- input_i$standard_error
#   # #Generate a random draw for each mean and se 
#   # set.seed(123)
#   # input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
#   # input_i[, prev := input.draws]
#   # 
#   return(input_i)
#   
# })
# 
# dt_draws <- rbindlist(dt_draws)

dt_draws[, emr := csmr / prev]
dt_draws[prev == 0, emr := 0]

#collapse back into means
setnames(dt, c('mean', 'standard_error', 'lower', 'upper'), c('orig_mean', 'orig_standard_error', 'orig_lower', 'orig_upper'))
final <- dt_draws[, .(mean = mean(emr),
                standard_error = sd(emr),
                lower = quantile(emr, .025),
                upper = quantile(emr, .975),
                csmr_draws_mean = mean(csmr),
                prev_draws_mean = mean(prev)), by = seq] %>% merge(dt, by = "seq")
final[lower < 0, lower := 0]

final[orig_mean == 0, standard_error := orig_standard_error]

# cod_col_names <- names(deaths)
# cod_col_names <- cod_col_names[cod_col_names %in% c('year_id', 'location_id', 'age_group_id', 'sex_id') == FALSE]
# age_pool[, (cod_col_names) := NULL]

final[, `:=` (measure = 'mtexcess', modelable_entity_id = 25306)]
final <- subset(final, select = -c(parent_id, location_set_version_id, location_set_id, path_to_top_parent,
                                   level, is_estimate, most_detailed, sort_order, location_ascii_name, location_name_short,
                                   location_type, map_id, super_region_id, super_region_name, region_id, region_name, 
                                   local_id, developed, start_date, date_inserted, last_updated, last_updated_by, 
                                   last_updated_action, lancet_label))


#append the lit EMR and the clinical EMR together and format and save the dataset for MR-BRT modeling
final <- rbind(final, split_emr_data, fill = TRUE)
final$origin_seq <- NULL
final$seq <- NA

#save all the data to a bundle, replacing anything with "mtexcess" as the measure
cols <- names(bundle_data)
bundle_data[, (cols[cols != 'seq']) := '']

final <- rbind(bundle_data, final, fill = TRUE)

# fill in year start and year end arbitrarily to pass epi upload functions.
final[is.na(year_start) & is.na(year_end) & !is.na(year_id), `:=` (year_start = year_id, year_end = year_id)]

#drop crosswalk parent seq and variance columns (came from the prevalence data)
final$crosswalk_parent_seq <- NULL
final$variance <- NULL

bun_id <- 337
write.xlsx(final, 'PATHNAME', sheetName = 'extraction')

upload_bundle_data(bundle_id = bun_id, decomp_step = 'iterative', gbd_round_id = 7, 
                   filepath = "PATHNAME")

# plot data
locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7,
                              decomp_step = 'iterative')
dt_plot <- merge(final, locs, by = 'location_id', all.x = TRUE)
dt_plot <- dt_plot[!is.na(nid)]
dt_plot$path_to_top_parent.x <- NULL
setnames(dt_plot, c('path_to_top_parent.y'), c('path_to_top_parent'))
dt_plot[is.na(clinical_data_type), clinical_data_type := 'lit']
dt_plot[, category := paste0(clinical_data_type, '_', sex)]

dt_plot[is.na(age_group_id) & age_start == 0, age_group_id := 2]
dt_plot[is.na(age_group_id) & age_start > 0, age_group_id := 3]
  
gg <- ggplot() +
  geom_point(data = dt_plot[year_id == year_i & region_id.y == 73], 
             aes(x = as.factor(age_group_id), y = mean, 
                 color = location_name.y, 
                 text = paste0(location_name.y, ', csmr: ', round(csmr_draws_mean,4),
                               ', prev: ', round(orig_mean,4))),
             alpha = 0.5) +
  facet_wrap(~ sex) +
  labs(title = paste0('Birth Prevalence of Neonatal Enceph - Model Comparison, Year: ',year_i),
       x = '',
       y = 'Excess Mortality Rate')
print(gg)
ggplotly(p = gg, tooltip = 'text')



pdf(paste0("PATHNAME",format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d"),"_nofacet.pdf"), width=20, height=10) 

for(l in c(0:5)) { 
  print(paste0("l = ",l))
  data <- copy(dt_plot)
  
  if(l == 0){
    data[, parent_loc_id := 0]
    data[, page_loc_id := 1]
  } else if (l == 1) {
    data[, parent_loc_id := 1]
    data[, page_loc_id := 1]
  } else {
    data[, parent_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l - 1)]
    data[, parent_loc_id := as.integer(parent_loc_id)]
    data[, page_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l)]
    data[, page_loc_id := as.integer(page_loc_id)]
  }
  
  data[, plot_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l+1)]
  data[, plot_loc_id := as.integer(plot_loc_id)]
  data <- data[!is.na(plot_loc_id)]
  
  for(plot_loc in unique(na.omit(data$plot_loc_id))){
    print(paste0("plot_loc_id is ",plot_loc))
    page_data_est <- data[plot_loc_id == plot_loc, ]
    page_data_est <- merge(page_data_est, locs[, list(plot_loc_name = location_name, plot_loc_id = location_id)], all.x= T, by = "plot_loc_id")
    
    if(nrow(page_data_est) == 0){
      gg <- ggplot() + geom_blank() + ggtitle(paste0("No data for ",  unique(locs[location_id == plot_loc, location_name])))
      print(gg)
      next
    }
    
    title = paste0("EMR (Calculated and from Literature): ", unique(locs[location_id == plot_loc, location_name]))
    
    gg <- ggplot() +
      geom_boxplot(data = page_data_est, 
                 aes(x = as.factor(age_group_id), y = mean, fill = category, 
                     color = category),
                 alpha = 0.5) +
      #facet_wrap(~ location_name.y) +
      labs(title = title,
           x = 'Age Group',
           y = 'Excess Mortality Rate') +
      theme_minimal() +
      scale_fill_discrete(l = 70) + # a bit brighter
      scale_color_discrete(l = 50) # a bit darker
    print(gg)

  }
  
}

dev.off()
