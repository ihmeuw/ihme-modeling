
rm(list=ls())

args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
measure_name <- args[2]

print(args)

# set bundle id and measure name directly when testing
bun_id <- 260
measure_name <- "prevalence"

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "USERNAME"
  h <- paste0("FILEPATH")
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')
# library('testit', lib = "/homes/chikeda/r_350")

source("FILEPATH")



#source custom functions
source(paste0("FILEPATH"))

#-----------------------------------------------------------------------------------
### constants
save_dir <- "FILEPATH"
map <- fread("FILEPATH")

bun_version_map <- fread(paste0('FILEPATH'))
bun_version_map_step4 <- fread(paste0('FILEPATH'))

version_id <- bun_version_map[bundle_id == bun_id, bundle_version]

me_id <- map[bundle_id == bun_id, me_id]

age_map <- fread("FILEPATH")

#-----------------------------------------------------------------------------------
#load bundle data
#-----------------------------------------------------------------------------------
#temp_seqs <- 999001:(999000+nrow(bun_data_7))
#bun_data_7$seq <- temp_seqs

#bun_data <- rbind(bun_data_7, bun_data_8, fill = TRUE)

bun_data <- get_bundle_version(version_id, fetch = 'all', export = FALSE)


#next 6 lines for testing
#test_data <- get_bundle_data(260, gbd_round_id = 6, decomp_step = 'step2')
#test_data_2 <- get_bundle_data(260, gbd_round_id = 6, decomp_step = 'step1')

#missing_source <- test_data_2$nid[!test_data_2$nid %in% test_data$nid]

#inc_data <- test_data_2[measure == 'incidence']

#upload <- save_bundle_version(260, 'step2', FALSE)
#upload_2 <- save_bundle_version(405, 'step2', FALSE)

#test_version <- get_bundle_version(1688, export = FALSE)
#test_version_2 <- get_bundle_version(8492, export = TRUE)
########

mean_removes <- bun_data$seq[bun_data$mean > 0 & bun_data$mean <= 1e-10]

bun_data <- bun_data[!bun_data$seq %in% mean_removes]

#206 changes
#bun_data <- bun_data[!bun_data$clinical_data_type == '' & bun_data$age_start >= 30, is_outlier := 1]

#bun_data_India <- bun_data[grep("India", bun_data$field_citation_value)]
#bun_data_Nepal <- bun_data[grep("Nepal", bun_data$field_citation_value)]
#bun_data_Mexico <- bun_data[grep("Mexico", bun_data$field_citation_value)]
#bun_data_NZ <- bun_data[grep("New Zealand", bun_data$field_citation_value)]
#bun_data_Norway <- bun_data[grep("Norway", bun_data$field_citation_value)]
#bun_data_Chile <- bun_data[grep("Chile", bun_data$field_citation_value)]
#bun_data_Germany <- bun_data[grep("Germany", bun_data$field_citation_value)]
#bun_data_Turkey <- bun_data[grep("Turkey", bun_data$field_citation_value)]


#full_countries <- rbind(bun_data_India, bun_data_Mexico, bun_data_Nepal,
                        #bun_data_Turkey, bun_data_Germany, bun_data_Chile,
                        #bun_data_Norway, bun_data_NZ)
#full_countries_seqs <- full_countries$seq[!full_countries$clinical_data_type == '']

#bun_data <- bun_data[bun_data$seq %in% full_countries_seqs & age_start >= 1, is_outlier := 1]

#bun_data_Japan <- bun_data[grep('Japan', bun_data$field_citation_value)]
#bun_seqs_Japan <- unique(bun_data_Japan$seq[!bun_data_Japan$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_Japan & mean >= 0.001, is_outlier := 1]

#bun_data_AUS <- bun_data[grep("Austria", bun_data$field_citation_value)]
#bun_seqs_AUS <- unique(bun_data_AUS$seq[!bun_data_AUS$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_AUS, is_outlier := 1]

#bun_data_SWE <- bun_data[grep("Sweden", bun_data$field_citation_value)]
#bun_seqs_SWE <- unique(bun_data_SWE$seq[!bun_data_SWE$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_SWE, is_outlier := 1]

#bun_seqs_Mexico <- unique(bun_data_Mexico$seq[!bun_data_Mexico$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_Mexico & mean >= 0.0005, is_outlier := 1]

#bun_data_US <- bun_data[grep('United States', bun_data$field_citation_value)]
#bun_seqs_US <- unique(bun_data_US$seq[!bun_data_US$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_US & age_start >= 20, is_outlier := 1]

#bun_data_BRA <- bun_data[grep('Brazil', bun_data$field_citation_value)]
#bun_seqs_BRA <- unique(bun_data_BRA$seq[!bun_data_BRA$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_BRA & mean >= 0.0001, is_outlier := 1]

#bun_data_CHN <- bun_data[grep('China', bun_data$field_citation_value)]
#bun_seqs_CHN <- unique(bun_data_CHN$seq[!bun_data_CHN$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_CHN, is_outlier := 1]

#207 changes

#clinical_data <- bun_data$seq[!bun_data$clinical_data_type == '']

#bun_data <- bun_data[bun_data$seq %in% clinical_data & age_start >= 20, is_outlier := 1]

##Marketscan data for bundles 209-211

#bun_data <- bun_data[grep("MarketScan", bun_data$field_citation_value), cv_marketscan := 1]

#bun_data <- bun_data[bun_data$year_end == 2000 & cv_marketscan == 1, cv_marketscan_all_2000 := 1]
#bun_data <- bun_data[bun_data$year_end == 2010 & cv_marketscan == 1, cv_marketscan_all_2010 := 1]
#bun_data <- bun_data[bun_data$year_end == 2012 & cv_marketscan == 1, cv_marketscan_all_2012 := 1]

#bun_data <- bun_data[bun_data$cv_marketscan_all_2000 == 1, is_outlier := 1]
#bun_data <- bun_data[bun_data$age_start >= 70 & bun_data$cv_marketscan == 1, is_outlier := 1]

#209 changes

#bun_data_US <- bun_data[grep('United States', bun_data$field_citation_value)]
#bun_seqs_US <- unique(bun_data_US$seq[!bun_data_US$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_US & age_start >= 70, is_outlier := 1]
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_US & cv_marketscan_all_2000 == 1, is_outlier := 1]
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_US & clinical_data_type == 'inpatient', is_outlier := 1]

#bun_data_BRA <- bun_data[grep('Brazil', bun_data$field_citation_value)]
#bun_seqs_BRA <- unique(bun_data_BRA$seq[!bun_data_BRA$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_BRA & age_start >= 50, is_outlier := 1]

#bun_data_MEX <- bun_data[grep('Mexico', bun_data$field_citation_value)]
#bun_seqs_MEX <- unique(bun_data_MEX$seq[!bun_data_MEX$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_MEX & age_start >= 40, is_outlier := 1]

#bun_data_ITA <- bun_data[grep('Italy', bun_data$field_citation_value)]
#bun_seqs_ITA <- unique(bun_data_ITA$seq[!bun_data_ITA$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_ITA & age_start >= 70, is_outlier := 1]

#bun_data <- bun_data[bun_data$nid %in% c(116023, 96471), is_outlier := 1]

#210 changes
#bun_data_US <- bun_data[grep('United States', bun_data$field_citation_value)]
#bun_seqs_US <- unique(bun_data_US$seq[!bun_data_US$clinical_data_type == ''])
#bun_data_BRA <- bun_data[grep('Brazil', bun_data$field_citation_value)]
#bun_seqs_BRA <- unique(bun_data_BRA$seq[!bun_data_BRA$clinical_data_type == ''])
#bun_data <- bun_data[!bun_data$clinical_data_type == '' & bun_data$age_start >= 50 & !bun_data$seq %in% bun_seqs_US & !bun_data$seq %in% bun_seqs_BRA, is_outlier := 1]
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_BRA & age_start >= 70, is_outlier := 1]
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_US & age_start >= 60, is_outlier := 1]

#bun_data_Japan <- bun_data[grep('Japan', bun_data$field_citation_value)]
#bun_seqs_Japan <- unique(bun_data_Japan$seq[!bun_data_Japan$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_Japan & mean >= 0.0001, is_outlier := 1]

#bun_data_Iran <- bun_data[grep('Iran', bun_data$field_citation_value)]
#bun_seqs_Iran <- unique(bun_data_Iran$seq[!bun_data_Iran$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_Iran & mean >= 0.0004, is_outlier := 1]

#bun_data_Jordan <- bun_data[grep('Jordan', bun_data$field_citation_value)]
#bun_seqs_Jordan <- unique(bun_data_Jordan$seq[!bun_data_Jordan$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_Jordan & mean >= 0.0004, is_outlier := 1]

#bun_data_ENG <- bun_data[grep('England', bun_data$field_citation_value)]
#bun_seqs_ENG <- unique(bun_data_ENG$seq[!bun_data_ENG$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_ENG & age_start >= 1, is_outlier := 1]

#bun_data_BRA <- bun_data[grep('Brazil', bun_data$field_citation_value)]
#bun_seqs_BRA <- unique(bun_data_BRA$seq[!bun_data_BRA$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_BRA & age_start >= 50, is_outlier := 1]

#211 changes

#bun_data_US <- bun_data[grep('United States', bun_data$field_citation_value)]
#bun_seq_US_states <- unique(bun_data_US$seq[!bun_data_US$location_id == 102])
#bun_data <- bun_data[bun_data$seq %in% bun_seq_US_states & age_start >= 80, is_outlier := 1]

#bun_data_BRA <- bun_data[grep('Brazil', bun_data$field_citation_value)]
#bun_seqs_BRA <- unique(bun_data_BRA$seq[!bun_data_BRA$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_BRA & age_start >= 50, is_outlier := 1]

#bun_data_TAI <- bun_data[grep('Taiwan', bun_data$field_citation_value)]
#bun_seqs_TAI <- unique(bun_data_TAI$seq[!bun_data_TAI$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_TAI & age_start >= 50, is_outlier := 1]

#bun_data_AUS <- bun_data[grep('Austria', bun_data$field_citation_value)]
#bun_seqs_AUS <- unique(bun_data_AUS$seq[!bun_data_AUS$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_AUS & age_start >= 50, is_outlier := 1]

#bun_data_ECU <- bun_data[grep('Ecuador', bun_data$field_citation_value)]
#bun_seqs_ECU <- unique(bun_data_ECU$seq[!bun_data_ECU$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_ECU & age_start >= 45, is_outlier := 1]

#bun_data_ITA <- bun_data[grep('Italy', bun_data$field_citation_value)]
#bun_seqs_ITA <- unique(bun_data_ITA$seq[!bun_data_ITA$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_ITA & age_start >= 70, is_outlier := 1]

#bun_data_NZL <- bun_data[grep('New Zealand', bun_data$field_citation_value)]
#bun_seqs_NZL <- unique(bun_data_NZL$seq[!bun_data_NZL$clinical_data_type == ''])
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_NZL & age_start >= 50, is_outlier := 1]

#bun_data_KEN <- bun_data[grep('Kenya', bun_data$field_citation_value)]
#bun_seqs_KEN <- unique(bun_data_KEN$seq)
#bun_data <- bun_data[bun_data$seq %in% bun_seqs_KEN & age_start >= 30, is_outlier := 1]

#bun_data <- bun_data[bun_data$location_id %in% c(35498, 4644, 173) & age_start >= 30, is_outlier := 1]

#narrow_age_column
bun_data$narrow_age <- 1
bun_data$narrow_age <- ifelse(bun_data$age_end - bun_data$age_start >= 4, 0, 1)
bun_data$narrow_age <- ifelse(bun_data$age_start >= 5 * (bun_data$age_start%/% 5) & bun_data$age_end <= 5 * (bun_data$age_start%/% 5) + 4, 1, 0)

#212 changes

#bun_data <- bun_data[bun_data$nid %in% c(128591), is_outlier := 1]

#bun_data <- pull_bundle_data(measure_name = measure_name, bun_id = bun_id, bun_data = bun_data)

bun_data <- bun_data[!is.na(mean)]

bun_data <- bun_data[bun_data$narrow_age == 1, orig_age_start := age_start]
bun_data <- bun_data[bun_data$narrow_age == 1, orig_age_end := age_end]

#Subsetting bundles 262 data 
#rural_urban_locs <- c(115740, 115750, 129099, 129106, 129107, 129108, 129126, 129149, 347702, 347752)
#bun_data <- bun_data[bun_data$nid %in% rural_urban_locs, site_memo := paste(urbanicity_type, site_memo, sep = ', ')]

#specificity_locs <- c(140021)
#bun_data <- bun_data[bun_data$nid %in% specificity_locs, site_memo := paste(case_definition, site_memo, sep = ', ')]

#subnationals <- c(247562, 247296)
#bun_data <- bun_data[bun_data$nid %in% subnationals, site_memo := paste(note_sr, site_memo, sep = ', ')]

#-----------------------------------------------------------------------------------
#subset data into an aggregate dataset and a fully-specified dataset
#-----------------------------------------------------------------------------------
data <- divide_data(input_data = bun_data)

good_data <- data[need_split == 0]

aggregate <- fsetdiff(data, good_data, all = TRUE)

if (nrow(aggregate)==0) { 
  print(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
  stop(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
}

#-----------------------------------------------------------------------------------
#expand the aggregate dataset into its constituent age and sexes
#-----------------------------------------------------------------------------------
expanded <- expand_test_data(agg.test = aggregate)

#Jordan Changes for age cols
expanded <- expanded[expanded$narrow_age == 0, orig_age_start := age_start]
expanded <- expanded[expanded$narrow_age == 0, orig_age_end := age_end]
#expanded <- expanded[, agg_age_mid := (agg_age_start + agg_age_end) / 2]

#-----------------------------------------------------------------------------------
#merge populations and age group ids onto the expanded dataset
#-----------------------------------------------------------------------------------
if ("age_group_id" %in% names(expanded) == TRUE) {
  expanded$age_group_id <- NULL
}

expanded <- add_pops(expanded)
print("Loaded populations")

#-----------------------------------------------------------------------------------
# Pull model results from DisMod to use as age/sex weights
#-----------------------------------------------------------------------------------
#label each row with the closest dismod estimation year for matching to dismod model results
#round down
expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
#round up
expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]

expanded[est_year_id < 1990, est_year_id := 1990]
expanded[year_id == 2017, est_year_id := 2017]

#' Pull draw data for each age-sex-yr for every location in the current aggregated test data 
#' needed to be split.
weight_draws <- pull_model_weights(me_id, measure_name)
print("Pulled DisMod results")

#' Append draws to the aggregated dataset
draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
               all.x=TRUE)

#' Take all the columns labeled "draw" and melt into one column, row from expanded now has 1000 
#' rows with same data with a unique draw. Draw ID for the equation
draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                         variable.name = "draw.id", value.name = "model.result")


#' SAMPLE FROM THE RAW INPUT DATA: 
#' Save a dataset of 1000 rows per every input data point that needs to be split.
#' Keep columns for mean and standard error, so that you now have a dataset with 
#' 1000 identical copies of the mean and standard error of each input data point.
orig.data.to.split <- unique(draws[, .(split.id, draw.id, mean, standard_error)])

#' Generate 1000 draws from the input data (assuming a normal distribution), and replace the identical means
#' in orig.data.to.split with draws of that mean
#' Currently setting all negative values to 0
set.seed(123)
mean.vector <- orig.data.to.split$mean
se.vector <- orig.data.to.split$standard_error
input.draws <- rnorm(length(mean.vector), mean.vector, se.vector)
orig.data.to.split[, input.draw := input.draws]

p.sample <- "zero"
orig.data.to.split[input.draw < 0 & p.sample == "reflect", input.draw := input.draw * -1]
orig.data.to.split[input.draw < 0 & p.sample == "zero", input.draw := 0]
if (measure_name == 'prevalence') {orig.data.to.split[input.draw > 1] <- 1}

#' Now each row of draws has a random draw from the distribution N(mean, SE) of the original
#' data point in that row
draws <- merge(draws, orig.data.to.split[,.(split.id, draw.id, input.draw)], by = c('split.id','draw.id'))

####################################################################################

#' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
#' based on the modeled prevalence
draws[, numerator := model.result * population]

#' This is the denominator, the sum of all the numerators by both draw and split ID. The number of cases in the aggregated age/sex 
#' group.
#' The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point
draws[, denominator := sum(numerator), by = .(split.id, draw.id)]

#' Calculate the actual estimate of the split point from the input data (mean) and a unique draw from the modelled 
#' prevalence (model.result)
draws[, estimate := input.draw * model.result / denominator * pop.sum]

draws[, sample_size_new := sample_size * population / pop.sum]

if (bun_id %in% c(437,438)) {
  draws[, estimate := cases / sample_size_new]
}

# If the numerator and denominator is zero, set the estimate to zero
draws[numerator == 0 & denominator == 0, estimate := 0]

#ariz_draws <- draws[draws$location_name == 'Arizona']


# Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
#' Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
final <- draws[, .(mean.est = mean(estimate),
                   sd.est = sd(estimate),
                   upr.est = quantile(estimate, .975),
                   lwr.est = quantile(estimate, .025),
                   sample_size_new = unique(sample_size_new),
                   cases.est = mean(numerator),
                   agg.cases = mean(denominator)), by = expand.id] %>% merge(expanded, by = "expand.id")


#if the mean is zero, calculate the standard error using Wilson's formula instead of the standard deviation of the mean
z <- qnorm(0.975)
final[sd.est == 0 & mean.est == 0 & measure == "prevalence", 
      sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]

final <- final[final$mean.est >= 1, mean.est := 0.999]
final <- final[final$upr.est >= 1, upr.est := 0.999]


final[, se.est := sd.est]
final[, agg.sample.size := sample_size]
final[, sample_size := sample_size_new]
final[,sample_size_new:=NULL]

#' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
final[mean==0, mean.est := 0]
final[, case_weight := cases.est / agg.cases]
final$agg.cases <- NULL
setnames(final, c("mean", "standard_error", "cases"), c("agg.mean", "agg.std.error", "agg.cases"))
setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
setnames(final, 'seq','parent_seq')
final[, seq := integer()] 

#drop rows that don't match the cause sex-restrictions
if (bun_id == 437) { final <- final[sex_id == 2] }
if (bun_id == 438) { final <- final[sex_id == 1] }

#-----------------------------------------------------------------------------------
#' Save off just the split data for troubleshooting/diagnostics
#-----------------------------------------------------------------------------------
split_data <- final[, c('nid','parent_seq','age_start','age_end','sex_id','mean',
                        'standard_error','case_weight','sample_size',
                        'agg_age_start','agg_age_end','agg_sex_id', 'agg.mean',
                        'agg.std.error','agg.cases','agg.sample.size',
                        'population','pop.sum',
                        'age_group_id','age_demographer','n.age','n.sex',
                        'location_id','year_start','year_end','est_year_id')]
split_data <- split_data[order(nid)]

write.csv(split_data, 
          file = "FILEPATH",
	  row.names = FALSE)


#-----------------------------------------------------------------------------------
#' Append split data back onto fully-specified data and save the fully split bundle
#-----------------------------------------------------------------------------------
good_data <- good_data[,names(bun_data),with = FALSE]
#good_data <- good_data[, agg_age_mid := (age_start + age_end) / 2]

good_data[, parent_seq := integer()]

final[, sex := ifelse(sex_id == 1, "Male", "Female")]
final[, `:=` (lower = lwr.est, upper = upr.est,
              cases = NA, effective_sample_size = NA)]

final <- final[,names(good_data),with = FALSE]

full_bundle <- rbind(good_data, final)

write.csv(full_bundle, 
          file = paste0("FILEPATH"),
          row.names = FALSE)
