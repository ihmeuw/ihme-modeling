##############################
## Purpose: First stage
## Details: Fit first stage model 
##          Linear model w/ female education 
##          Source random intercept by nid
##          Use reference source random intercepts to adjust non-reference data
##          Generate stage 1 predictions
#############################

library(data.table)
library(lme4)
library(arm)
library(assertable)
library(readr)
library(splines)
library(mortdb)
library(mortcore)
rm(list=ls())

if (interactive()){
  user <- "USERNAME"
  version_id <- 'Run id'
  gbd_year <- '2023'
  year_start <- '1950'
  year_end <- '2024'
  loop <- '1'
  model_age <- '45'
  super_reg <- 'Others'
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--loop', type = 'integer')
  parser$add_argument('--model_age', type = 'integer')
  parser$add_argument('--super_reg', type = 'integer')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  loop <- args$loop
  model_age <- args$model_age
  super_reg <- args$super_reg
}

super_reg_map <- fread('FILEPATH/super_regs.csv')
super_reg <- super_reg_map[super_reg_id == super_reg]$super_reg_name

gbd_round <- mortdb::get_gbd_round(gbd_year = gbd_year)

input_dir <- "FILEPATH"
output_dir <- "FILEPATH"
gpr_dir <- "FILEPATH"
loop1_dir <- "FILEPATH"

super_regs <- super_reg
if(super_reg == 'Others') {
  super_regs <- c('Southeast Asia, East Asia, and Oceania', 
                  'Latin America and Caribbean', 'North Africa and Middle East', 
                  'South Asia')
}

## Read in model_locs
all_locs <- fread(paste0(loop1_dir, '/loc_map.csv'))[is_estimate == 1]
if (model_age == 20) {
  model_locs <- all_locs[super_region_name %in% super_regs, ihme_loc_id]
} else {
  model_locs <- fread(paste0(loop1_dir, '/model_locs.csv'))
  model_locs <- model_locs[super_region_name %in% super_regs, ihme_loc_id]
}

chn_subs <- all_locs[grepl("CHN_", ihme_loc_id) & level > 3]$ihme_loc_id

## Read in input data
input_data <- fread(paste0(input_dir, '/input_data.csv'))
data <- input_data[age == model_age & super_region_name %in% super_regs]

## Add ASFR20 covariate if needed
if (model_age != 20) {
  assertable::check_files(paste0('gpr_', all_locs$ihme_loc_id, '_20.csv'), 
                          folder = gpr_dir, continual = F, display_pct = 0)
  asfr20 <- assertable::import_files(list.files(gpr_dir, full.names = T, 
                                                pattern = '.*20.csv'), 
                                     FUN = fread)
  asfr20[,mean := logit(mean)]
  setnames(asfr20, 'mean', 'asfr20')
  asfr20[, year_id := floor(year)]
  data <- merge(data, asfr20[,.(year_id, ihme_loc_id, asfr20)], 
                by=c('year_id', 'ihme_loc_id'))
}

###############################################
##############   Model Fitting   ###############################################
###############################################

## Create variable to uniquely identify sources
data[,loc_source := paste0(ihme_loc_id,  '_', nid)]
data[grepl('VR', source_name) & method_name == 'Dir-Unadj',
     loc_source := paste0(ihme_loc_id, '_', source_name, '_', 'ASFR')]
data[grepl('VR', source_name) & method_name == 'TB-split',
     loc_source := paste0(ihme_loc_id, '_', source_name, '_', 'TB')]
data[grepl('Census', source_name) & method_name == 'TB-split',
     loc_source := paste0(ihme_loc_id, '_', source_name, '_', 'TB')]

## Break COL into 3 series
data[ihme_loc_id == 'COL' & year_id < 1970 & grepl('VR', source_name)
     & method_name == 'Dir-Unadj', loc_source := 'COL_VR_incomplete']
data[ihme_loc_id == 'COL' & year_id %in% 1970:1990 & grepl('VR', source_name)
     & method_name == 'Dir-Unadj', loc_source := 'COL_VR_1970_1990']
data[ihme_loc_id == 'COL' & year_id > 1990 & grepl('VR', source_name)
     & method_name == 'Dir-Unadj', loc_source := 'COL_VR_post1990']

## Break BOL into 2 series
data[ihme_loc_id == 'BOL' & year_id < 2000 & grepl('VR', source_name)
     & method_name == 'TB-split', loc_source := 'BOL_VR_pre2000_TB']
data[ihme_loc_id == 'BOL' & year_id >= 2000 & grepl('VR', source_name)
     & method_name == 'TB-split', loc_source := 'BOL_VR_post2000_TB']

## Break BGD into 2 series
data[ihme_loc_id == 'BGD' & year_id < 2014 & grepl('VR', source_name)
     & method_name == 'Dir-Unadj', loc_source := 'BGD_VR_pre2014']
data[ihme_loc_id == 'BGD' & year_id >= 2014 & grepl('VR', source_name)
     & method_name == 'Dir-Unadj', loc_source := 'BGD_VR_post2014']

## Break GHA into 2 series
data[ihme_loc_id == 'GHA' & year_id < 2000 & grepl('VR', source_name)
     & method_name == 'TB-split', loc_source := 'GHA_VR_pre2000_TB']
data[ihme_loc_id == 'GHA' & year_id >= 2000 & grepl('VR', source_name)
     & method_name == 'TB-split', loc_source := 'GHA_VR_post2000_TB']

## Break UZB into 3 series even though dates are arbitrary cutoffs based on old
## completeness
data[ihme_loc_id == 'UZB' & year_id < 1979 & grepl('VR', source_name)
     & method_name == 'Dir-Unadj', loc_source := 'UZB_VR_incomplete_TB']
data[ihme_loc_id == 'UZB' & year_id %in% 1979:1991 & grepl('VR', source_name)
     & method_name == 'Dir-Unadj', loc_source := 'UZB_VR_complete_TB']
data[ihme_loc_id == 'UZB' & year_id > 1991 & grepl('VR', source_name)
     & method_name == 'Dir-Unadj', loc_source := 'UZB_VR_incomplete_TB']

## Break CHN subnat TB into 2 series
data[grepl('CHN_', ihme_loc_id) & year_id < 2013 & method_name == 'TB-split',
     loc_source := paste0(ihme_loc_id, '_', 'VR_pre2013_TB')]
data[grepl('CHN_', ihme_loc_id) & year_id >= 2013 & method_name == 'TB-split',
     loc_source := paste0(ihme_loc_id, '_', 'VR_post2013_TB')]

# Break TLS into two VR series
data[ihme_loc_id == 'TLS' & year_id > 1980 & grepl('VR', source_name),
     loc_source := paste0(ihme_loc_id, '_', 'VR_post1980')]

# Break WSM into two VR series
data[ihme_loc_id == 'WSM' & year_id > 2000 & grepl('VR', source_name),
     loc_source := paste0(ihme_loc_id, '_', 'VR_post2000')]

# Break CHN subnat source into two
data[ihme_loc_id %in% chn_subs & year_id %in% 2002:2012 & nid == 137357,
     loc_source := paste0(ihme_loc_id, '_', nid)]

# Break DZA CBH
data[ihme_loc_id == 'DZA' & year_id < 1990 & method_name == 'CBH',
     loc_source := paste0(ihme_loc_id, '_', 'CBH_pre1990')]
data[ihme_loc_id == 'DZA' & year_id >= 1990 & method_name == 'CBH',
     loc_source := paste0(ihme_loc_id, '_', 'CBH_post1990')]

## Combine all ECU VR
data[ihme_loc_id == 'ECU' & grepl('VR', source_name) & method_name == 'Dir-Unadj',
     loc_source := 'ECU_VR']

## Break CHN census
data[ihme_loc_id == 'CHN_44533' & nid == 268219 & year_id == 1990, 
     loc_source := paste0(loc_source, '_census')]
data[ihme_loc_id == 'CHN_44533' & nid == 319831 & year_id == 1981, 
     loc_source := paste0(loc_source, '_census')]

## First fit model on standard locations
std_locs <- get_locations(gbd_type = 'standard_modeling', level = 'all', 
                          gbd_year = gbd_year)
nat_parents <- all_locs[level == 4, parent_id]
std_locs <- all_locs[location_id %in% unique(c(std_locs$location_id, 
                                               nat_parents, 44533))]

std_data <- data[location_id %in% std_locs[,location_id]]

knotsdf <- data.table(region_name = c(rep('High-income', 5), 
                                      rep('Sub-Saharan Africa', 5), 
                                      rep('Others', 5), 
                                      rep('Central Europe, Eastern Europe, 
                                          and Central Asia', 5)), 
                      age = rep(seq(25, 45, 5), 4), 
                      knot = c(NA, -2.25, -2, -2.25, -2.25, -1.75, -1.25, -1.3, 
                               -1.5, -1.75, -1.5, -1.3, -1.3, -2, -2.5, -1.5, 
                               -2, -1.75, -1.75, -2))
knots <- knotsdf[age == model_age & region_name == super_reg, knot]
if (length(knots) == 0) knots <- NULL
if (length(knots) != 0) { if(is.na(knots)) { knots <- NULL }}

## Model form
if(model_age == 20){
  form <-  paste0('logit_asfr_data ~ fem_edu  + (1|loc_source)')
} else if (model_age != 20 & super_reg == 'High-income' ) {
  form <-  paste0('logit_asfr_data ~  bs(asfr20, degree = 1, knots = ', 
                  paste(knots, collapse = ', '), ') + (1|loc_source)')
} else {
  form <- paste0('logit_asfr_data ~ fem_edu +  bs(asfr20, degree = 1, knots = ', 
                 paste(knots, collapse = ', '), ') + (1|loc_source)')
}

#fit model on standard locations to get fixed effects
set.seed(4242)
std_mod <- lmer(formula=as.formula(form), data=std_data[outlier == 0])

#subtract coefficients from standard locations model, fit just a random 
#intercept model to get REs
data[, pred := predict(std_mod, newdata = data, re.form = ~0, 
                       allow.new.levels = T )]

std_intercept <- fixef(std_mod)[1]
data[, fe_data := logit_asfr_data - pred + std_intercept]

re_mod <- lmer(fe_data ~ 0 + (1|loc_source), data=data[outlier == 0])

loc_source_re <- data.table('loc_source' = rownames(lme4::ranef(re_mod)$loc_source), 
                            're' = lme4::ranef(re_mod)$loc_source[,1])

fe_output <- t(t(fixef(std_mod)))
fe_row_names <- row.names(fe_output)
fe_output <- as.data.table(cbind(fe_row_names, fe_output))
setnames(fe_output, names(fe_output), c("fixef_var", "fixef_value"))
readr::write_csv(fe_output, paste0(output_dir, '/', super_reg,
                                   '_age_', model_age, 
                                   '_coefficients.csv'))

re_output <- ranef(std_mod)$loc_source
re_row_names <- row.names(re_output)
re_output <- as.data.table(cbind(re_row_names, re_output))
setnames(re_output, names(re_output), c("loc_source", "ranef_value"))
readr::write_csv(re_output, paste0(output_dir, '/', super_reg, 
                                   '_age_', model_age, 
                                   '_loc_source_ranef.csv'))

###############################################
###########   Source Adjustments   #############################################
###############################################

## Identify all unique sources
all_sources <- unique(data[outlier == 0,.(ihme_loc_id, source_name, method_name, 
                                          loc_source)])
ref_source_list <- copy(all_sources)
complete_vr_locs <- ref_source_list[source_name == 'VR_complete']$ihme_loc_id
complete_vr_locs <- unique(complete_vr_locs)

## Choose complete VR when available
ref_source_list <- ref_source_list[(source_name == 'VR_complete' & 
                                      ihme_loc_id %in% complete_vr_locs) | 
                                     !ihme_loc_id %in% complete_vr_locs]

## chose all CBH sources by default if no vr_complete
ref_source_list <- ref_source_list[grepl('VR_complete', source_name) | 
                                     method_name == 'CBH']

## custom reference sources
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'KGZ' &
                                                        grepl('VR', source_name)])
ref_source_list <- ref_source_list[!ihme_loc_id == 'TJK']
ref_source_list <- ref_source_list[!ihme_loc_id == 'COL']
ref_source_list <- ref_source_list[!ihme_loc_id == 'NIU']
ref_source_list <- ref_source_list[!ihme_loc_id == 'LSO']
ref_source_list <- ref_source_list[!ihme_loc_id == 'MWI']
ref_source_list <- ref_source_list[!ihme_loc_id == 'ECU']
ref_source_list <- ref_source_list[!ihme_loc_id == 'KIR']
ref_source_list <- ref_source_list[!ihme_loc_id == 'MHL']
ref_source_list <- ref_source_list[!ihme_loc_id == 'NRU']
ref_source_list <- ref_source_list[!ihme_loc_id == 'ZAF']
ref_source_list <- ref_source_list[!ihme_loc_id == 'CIV']
ref_source_list <- ref_source_list[!ihme_loc_id == 'BTN']
ref_source_list <- ref_source_list[!ihme_loc_id == 'IND']
ref_source_list <- ref_source_list[!grepl('ZAF_', ihme_loc_id)]
ref_source_list <- ref_source_list[!(grepl('CHN_', ihme_loc_id) & 
                                       ihme_loc_id != 'CHN_44533')]

## remove specific sources
ref_source_list <- ref_source_list[!(ihme_loc_id == 'BGR' & 
                                       method_name == 'SBH-split')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'YEM' & 
                                       method_name == 'TB-split')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'BGD' & 
                                       method_name == 'TB-split')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'IND_4863' & 
                                       grepl('VR', source_name))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'PAK' & 
                                       source_name == 'WFS')]

ref_source_list <- ref_source_list[!grepl('CHN', ihme_loc_id)]
ref_source_list <- ref_source_list[!grepl('BRA', ihme_loc_id)]

cbh_locs <- c('TJK', 'COL', 'LSO', 'BGD', 'MWI', 'KOR', 'TKM', 'ECU', 'BLZ', 
              'TTO', 'SAU', 'MDV', 'BWA', 'ZAF', 'THA', 'KGZ', 'WSM', 'CPV',
              'STP', 'JAM')
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id %in% cbh_locs
                                                      & method_name == 'CBH'])

ref_source_list <- rbind(ref_source_list, all_sources[grepl('BRA', ihme_loc_id) & 
                                                        method_name == 'CBH'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('ZAF', ihme_loc_id) & 
                                                        method_name == 'CBH'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('PHL', ihme_loc_id) & 
                                                        method_name == 'CBH' & 
                                                        grepl('DHS', source_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'CIV' & 
                                                        method_name == 'CBH' & 
                                                        grepl('DHS', source_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'CIV' &
                                                        source_name == 'DHS' &
                                                        method_name == 'CBH'])
ref_source_list <- rbind(ref_source_list, all_sources[loc_source == 'CIV_218611' 
                                                      & method_name == 'CBH'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'OMN' &
                                                        grepl('CBH', method_name)])
ref_source_list <- rbind(ref_source_list, 
                         all_sources[ihme_loc_id %in% c('IND_4849', 'IND_43880', 
                                                        'IND_43916') & 
                                       grepl('CBH', method_name) &
                                       grepl('DHS', source_name, ignore.case = T)])
ref_source_list <- rbind(ref_source_list,
                         all_sources[loc_source == "DZA_CBH_pre1990"])
for (loc in c('KAZ', 'UZB', 'GTM')) {
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == loc &
                                                        grepl('CBH', method_name)])
}

sbh_locs <- c('IDN', 'MMR', 'VNM', 'AGO', 'COG', 'GNQ', 'BDI', 'ERI', 'KEN', 
              'RWA', 'GMB', 'BTN', 'THA', 'NIC', 'PER', 'CIV') # , 'WSM')
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id %in% sbh_locs
                                                      & method_name == 'SBH-split'])

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'BRA' & 
                                                        method_name == 'SBH-split'
                                                      & grepl('Other', source_name)])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('CHN_', ihme_loc_id) 
                                                      & method_name == 'SBH-split'])
ref_source_list <- ref_source_list[!(ihme_loc_id == 'CHN_44533' & 
                                       method_name == 'SBH-split')]
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'MDV' & 
                                                        method_name == 'SBH-split' & 
                                                        grepl('DHS', source_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'MEX' & 
                                                        source_name == 'MICS' &
                                                        method_name == 'SBH-split'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('MEX_', ihme_loc_id) & 
                                                        source_name == 'MICS' &
                                                        method_name == 'SBH-split'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('MEX_', ihme_loc_id) & 
                                                        source_name == 'Census' &
                                                        method_name == 'SBH-split'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('MEX_', ihme_loc_id) & 
                                                        source_name == 'Other' &
                                                        method_name == 'SBH-split'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'CPV' & 
                                                        grepl('DHS', source_name) & 
                                                        method_name == 'SBH-split'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'SDN' &
                                                        grepl('SBH', method_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'TGO' &
                                                        grepl('SBH', method_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'TJK' &
                                                        grepl('SBH', method_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'BRA_4762' &
                                                        grepl('SBH', method_name)])
ref_source_list <- ref_source_list[loc_source != "AFG_563"]

tb_locs <- c('NIU', 'FJI', 'GUM', 'ASM', 'GEO', 'KAZ', 'DEU', 'BRN', 'ATG', 
             'BLZ', 'BMU', 'GRD', 'JAM', 'LCA', 'VCT', 'TTO', 'LBY', 'SYR',
             'ARE', 'PLW', 'TKL', 'MYS', 'LKA', 'THA', 'SMR', 'CPV', 'GTM',
             'MNE', 'TUN', 'CHN_354')
chn_subs <- all_locs[grepl('CHN', ihme_loc_id) & level >= 5, ihme_loc_id]
tb_locs <- c(tb_locs, chn_subs)

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id %in% tb_locs &
                                                        method_name == 'TB-split'])

if(model_age == "45") {
  
  tb_US_locs <- c("USA_524", "USA_527", "USA_555", "USA_530", "USA_534", "USA_535")
  ref_source_list <- ref_source_list[!(ihme_loc_id %in% tb_US_locs & grepl("complete_TB", loc_source))]
  
}

ref_source_list <- rbind(ref_source_list, all_sources[grepl('GBR_', ihme_loc_id) 
                                                      & method_name == 'TB-split'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('MEX', ihme_loc_id) 
                                                      & method_name == 'TB-split'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'UZB' 
                                                      & (loc_source == 'UZB_VR_complete_TB' |
                                                           loc_source == 'UZB_VR_incomplete_TB')])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'BOL' 
                                                      & loc_source == 'VR_post2000_TB'])

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'COK' & 
                                                        source_name == 'Census' & 
                                                        method_name == 'Dir-Unadj'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'GIN' & 
                                                        source_name == 'Census' & 
                                                        method_name == 'Dir-Unadj'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'CHN_44533' & 
                                                        source_name == 'Census' & 
                                                        method_name == 'Dir-Unadj'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'PNG' &
                                                        source_name == 'Census' &
                                                        method_name == 'Dir-Unadj'])

ref_source_list <- rbind(ref_source_list, all_sources[grepl('ITA_', ihme_loc_id) & 
                                                        method_name == 'Dir-Unadj'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('NOR_', ihme_loc_id) &
                                                        method_name == 'Dir-Unadj'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('GBR_', ihme_loc_id) &
                                                        method_name == 'Dir-Unadj'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('MEX_', ihme_loc_id) &
                                                        method_name == 'Dir-Unadj'])

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'TCD' &
                                                        source_name == 'MICS'])
ref_source_list <- ref_source_list[loc_source != "TCD_76701"]
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'CIV' & 
                                                        source_name == 'Census'])


ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'SGP' & 
                                                        source_name == 'Statistical Report'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'KWT' & 
                                                        source_name == 'Statistical Report'])

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'MDV' &
                                                        grepl('VR', source_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'TLS' &
                                                        loc_source == 'TLS_VR_post1980'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'CHN_361' &
                                                        source_name == 'VR_complete' &
                                                        method_name == 'TB-split'])
if(loop == 2) ref_source_list <- rbind(ref_source_list, all_sources[loc_source == 'USA_VR_incomplete_TB'])

ref_source_list <- ref_source_list[!(grepl('IND_', ihme_loc_id) & 
                                       grepl('VR', source_name))]

ref_source_list <- ref_source_list[!(ihme_loc_id == 'PHL_53578' & 
                                       grepl('VR', source_name))]

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'NIC' & 
                                                        source_name == 'VR_complete'])

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'CHN_44533' & 
                                                        source_name == 'VR_complete'])

ref_source_list <- rbind(ref_source_list, all_sources[grepl('RUS_', ihme_loc_id) 
                                                      & grepl('VR', source_name)])

ref_source_list <- rbind(ref_source_list, all_sources[grepl('JPN_', ihme_loc_id) 
                                                      & grepl('VR', source_name)])

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'MEX' 
                                                      & method_name == 'Dir-Unadj' 
                                                      & grepl('VR', source_name)])

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'ECU'
                                                      & source_name == 'VR_complete'])

dir_locs <- c('HRV', 'PRT', 'KAZ', 'GRC', 'CYP', 'KOR', 'GEO', 'MNE', 'ROU', 
              'SMR', 'ATG', 'BLZ', 'BMU', 'CUB', 'GRD', 'JAM', 'LCA', 'VCT',
              'TTO', 'OMN', 'SAU', 'SYR', 'ARE', 'YEM', 'NIU', 'MNP', 'PLW',
              'TLK', 'MYS', 'LKA', 'BFA', 'BTN', 'THA', 'LBY', 'GTM', # 'BGD', Test remove BGD
              'NIC', 'BIH', 'TUN', 'URK_44934')
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id %in% dir_locs &
                                                        method_name == 'Dir-Unadj'])

ref_source_list <- rbind(ref_source_list, all_sources[grepl('CHN_', ihme_loc_id) 
                                                      & method_name == 'Dir-Unadj'])

vr_locs <- c('ALB', 'HRV', 'HUN', 'MDA', 'UKR', 'CHL', 'ISL', 'LUX', 'PRT', 
             'JAM', 'LCA', 'VCT', 'VIR', 'PAN', 'ASM', 'COK', 'FJI', 'GUM',
             'SYC', 'LKA', 'EST', 'TUR', 'BEL', 'CHE', 'CRI', 'CZE', 'DNK',
             'FIN', 'FRA', 'GRL', 'IRL', 'KOR', 'LTU', 'MUS', 'NLD', 'NOR',
             'SWE', 'SWE_4940', 'SWE_4944', 'TWN', 'ISR', 'AND', 'BGR',
             'AUT', 'ESP', 'SVK', 'SVN') # , 'WSM')
vr_locs <- c(vr_locs, grep("ITA", model_locs, value = TRUE))
vr_locs <- c(vr_locs, grep("JPN", model_locs, value = TRUE))
vr_locs <- c(vr_locs, grep("RUS", model_locs, value = TRUE))

ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id %in% vr_locs &
                                                        grepl('VR', source_name)])

ref_source_list <- rbind(ref_source_list, all_sources[grepl('IND', ihme_loc_id)
                                                      & source_name == 'SRS'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id %in% 
                                                        c('IND_44538', 'IND_44539',
                                                          'IND_44540') &
                                                        method_name == 'CBH' &
                                                        source_name == 'Other'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id %in% c('IND_44538',
                                                                         'IND_44539','IND_44540') &
                                                        source_name == 'Other' & 
                                                        method_name == 'CBH'])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'IND_4849' & 
                                                        method_name == 'CBH' & 
                                                        grepl('DHS', source_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'IND_4863' & 
                                                        method_name == 'CBH' & 
                                                        grepl('DHS', source_name)])
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'IND' & 
                                                        method_name == 'CBH' & 
                                                        loc_source == 'IND_268219'])
ref_source_list <- rbind(ref_source_list, all_sources[grepl("IND", ihme_loc_id) & 
                                                        method_name == 'CBH' & 
                                                        grepl('DHS', source_name)])

ref_source_list <- rbind(ref_source_list, all_sources[loc_source == 'BRB_153002'])
ref_source_list <- rbind(ref_source_list, all_sources[loc_source == 'GEO_431847'])
ref_source_list <- rbind(ref_source_list, all_sources[loc_source == 'ZAF_164898'])

ref_source_list <- rbind(ref_source_list, all_sources[loc_source %in% 
                                                        c('CHN_44533_353530', 
                                                          'CHN_44533_353533',
                                                          'CHN_44533_357371', 
                                                          'CHN_44533_415956', 
                                                          'CHN_44533_426273',
                                                          'CHN_44533_319831', 
                                                          'CHN_44533_319831_census', 
                                                          'CHN_44533_268219_census')])

ref_source_list <- ref_source_list[loc_source != 'SGP_VR_post2000_TB']
ref_source_list <- ref_source_list[loc_source != 'SGP_VR_post2000_TB-split']
ref_source_list <- ref_source_list[!(ihme_loc_id == "EGY" & grepl("VR", source_name))]
ref_source_list <- rbind(ref_source_list, all_sources[loc_source %in% c('HND_426273', 
                                                                        'HND_429799', 
                                                                        'HND_429800')])
ref_source_list <- rbind(ref_source_list, all_sources[loc_source %in% c('MEX_43761', 
                                                                        'MEX_105262', 'MEX_140201', 
                                                                        'MEX_20326', 'MEX_8480', 
                                                                        'MEX_105806', 'MEX_8634', 
                                                                        'MEX_25293', 'MEX_43776', 
                                                                        'MEX_8635', 'MEX_25317', 
                                                                        'MEX_23982', 'MEX_25335', 
                                                                        'MEX_24006', 'MEX_93321', 
                                                                        'MEX_240604', 'MEX_294574',
                                                                        'MEX_264590') & 
                                                        method_name == 'SBH-split']) 
if(model_age==15) ref_source_list <- rbind(ref_source_list, 
                                           all_sources[loc_source %in% c('YEM_13829', 
                                                                         'YEM_21068', 'YEM_105262', 
                                                                         'YEM_13795', 'YEM_13816', 
                                                                         'YEM_112500')])
ref_source_list <- rbind(ref_source_list, all_sources[grepl('CHN_', ihme_loc_id) 
                                                      & grepl('VR_post2013_TB', loc_source)])
ref_source_list <- ref_source_list[!(ihme_loc_id %in% chn_subs & grepl('137357', loc_source))]
if(model_age==35) ref_source_list <- rbind(ref_source_list,
                                           all_sources[ihme_loc_id == 'FJI'])

vr_phl <- ref_source_list[grepl('PHL_', ihme_loc_id) & grepl('VR', loc_source), unique(ihme_loc_id)]
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id %in% vr_phl & method_name == 'TB-split'])
ref_source_list <- ref_source_list[!(grepl('PHL_', ihme_loc_id) & grepl('incomplete', source_name))]
ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'PHL_53578' & grepl('VR_complete', source_name)])
if(model_age == 45){
  ref_source_list <- rbind(ref_source_list, all_sources[ihme_loc_id == 'PHL_53564' &
                                                          grepl('VR', source_name)])
}

ref_source_list <- unique(ref_source_list)

## If no complete VR or CBH, make locations agnostic
missing_locs <- all_sources$ihme_loc_id[!all_sources$ihme_loc_id %in% ref_source_list$ihme_loc_id]
agnostic_locs <- all_sources[ihme_loc_id %in% missing_locs]
ref_source_list <- rbind(ref_source_list, agnostic_locs)

## Get reference source random intercepts
ref_source_list[,ref_source := 1]

data <- merge(data, ref_source_list, by = c('ihme_loc_id', 'source_name', 'method_name', 'loc_source'), all.x=T)
data[is.na(ref_source), ref_source := 0]

## Take average of reference sources for any location w/ >1 reference sources
ref_source_re <- merge(ref_source_list, loc_source_re, by='loc_source')
ref_source_re[, re := mean(re), by='ihme_loc_id']
ref_source_re <- unique(ref_source_re[,.(ihme_loc_id, re)])

data <- merge(data, loc_source_re, by = 'loc_source', all.x=T)

data <- merge(data, ref_source_re, by = 'ihme_loc_id', all.x=T)

setnames(data, c('re.x', 're.y'), c('loc_source_re', 'ref_source_re'))

data[is.na(ref_source_re), ref_source_re := loc_source_re, ]

## Calculate adjustments
data[, adjustment := ref_source_re - loc_source_re]

## Don't adjust incomplete VR down
data[grepl('VR_incomplete', loc_source) & adjustment < 0, adjustment := 0]

## Custom
data[ihme_loc_id == 'BWA' & source_name == 'Statistical Report' & adjustment < 0, adjustment := 0]

data[ihme_loc_id == 'CHN_44533' & method_name == 'TB-split' & adjustment < 0, adjustment := 0]

data[grepl('ZAF_', ihme_loc_id) & method_name == 'SBH-split' & adjustment < 0, adjustment := 0]

data[grepl('NGA', ihme_loc_id) & grepl('SBH', method_name) & adjustment < 0, adjustment := 0]

data[ihme_loc_id == 'PAK' & year_id == 1970 & method_name == 'Dir-Unadj', adjustment := 0]

data[ihme_loc_id == 'UKR_44934' & source_name == "VR_incomplete", adjustment := 0]

data[ihme_loc_id == 'MKD' & grepl("Stat", source_name), adjustment := 0]

data[ihme_loc_id == 'BOL' & grepl("VR", source_name) & adjustment < 0, adjustment := 0]

data[ihme_loc_id == 'PSE' & grepl("SBH", method_name) & adjustment < 0, adjustment := 0]

data[ihme_loc_id == 'TKM' & grepl("VR", source_name) & year_id %in% 1950:1969, adjustment := 0]

data[ihme_loc_id == "TON" & source_name == "VR_incomplete" & year_id >= 2013, adjustment := 0]

data[ihme_loc_id == "BGD" & source_name == "VR_incomplete" & age_group_id == 8 & year_id >= 2014, adjustment := 0]

data[ihme_loc_id == "GBR_434" & age == model_age & adjustment < 0, adjustment := 0]

data[ihme_loc_id == "IND_4857" & adjustment < 0 & grepl("VR", source_name), adjustment := 0]

data[grepl("USA", ihme_loc_id), adjustment := 0]

if(super_reg == 'Others') {
  bra_vr_2019 <- data[grepl('BRA', ihme_loc_id) & year_id == 2019 & grepl("VR", source_name)]
  bra_vr_2019 <- bra_vr_2019[, .(ihme_loc_id, year_id = 2020, rep_adj = adjustment)]
  
  data <- merge(data, 
                bra_vr_2019, by = c("ihme_loc_id", "year_id"),
                all.x = TRUE)
  data[!is.na(rep_adj), adjustment := rep_adj]
  data[, rep_adj := NULL]
}

ref_data <- data[ref_source == 1 & outlier == 0]
ref_data[, ref_asfr_data := mean(logit_asfr_data), by=c('ihme_loc_id', 'year_id')]
data <- merge(data, unique(ref_data[,.(ihme_loc_id, year_id, ref_asfr_data)]), by=c('ihme_loc_id', 'year_id'), all.x=T)
data[!ref_source & !is.na(ref_asfr_data), adjust := (logit_asfr_data > ref_asfr_data) == (loc_source_re > ref_source_re)]
data[adjust == F, adjustment := 0]
data[,c('ref_asfr_data', 'adjust') := NULL]

## Don't adjust reference sources
data[ref_source == 1, adjustment := 0]

data[outlier == 1, adjusted_asfr_data := logit_asfr_data]
data[outlier == 0, adjusted_asfr_data := logit_asfr_data + adjustment]

data <- data[!is.na(adjusted_asfr_data)]

readr::write_csv(data, paste0(input_dir, super_reg, '_age_', model_age, '_data.csv'))

###############################################
#################   Predict   ##################################################
###############################################

pred <- data.table(expand.grid(year_id = year_start:year_end, ihme_loc_id = model_locs, age = as.integer(as.character(model_age))))
fem_edu <- fread(paste0(loop1_dir, 'fem_edu.csv'))
pred <- merge(pred, fem_edu, by=c('age', 'year_id', 'ihme_loc_id'), all.x=T)
if (model_age != 20) pred <- merge(pred, asfr20[,.(year_id, ihme_loc_id, asfr20)], by=c('year_id', 'ihme_loc_id'))

if(nrow(pred[is.na(fem_edu)])>0) stop(paste('Missing female education covariate for the following locations:', 
                                            paste(pred[is.na(fem_edu), unique(ihme_loc_id)], collapse=', ')))

## Make predictions
pred[, stage1_pred := predict(std_mod, newdata = pred, re.form = ~0, allow.new.levels = T)]

## Save stage 1 predictions
readr::write_csv(pred, paste0(output_dir, super_reg, '_age_', model_age, '_stage1_results.csv'))
