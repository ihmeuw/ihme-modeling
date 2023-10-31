#############################################################
##Purpose:Prep MR-BRT at bundle-level
##Notes: 
##Updates: Aggregate ICG, prep and split by bundle into workable datasets
#
###########################################################

# Master function -----
user <- Sys.info()[7]
prep_vers <- 29
run <- 25

library(data.table)
library(readr)
library(readstata13)
library(tidyverse)
library(crosswalk, lib.loc = FILEPATH)

write_folder <- FILEPATH

ages <- get_age_metadata(19, gbd_round_id = 7) %>% 
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
age_1 <- data.table(age_group_id = 28, age_start = 0, age_end = 1)
ages <- ages %>% rbind(age_1, fill = TRUE)

bundle_map <- bundle_icg()
locs <- get_location_metadata(35, gbd_round_id = 7, decomp_step = 'step2')

# Load bundles to keep from Taiwan data with mapping changes - will need to be updated for future versions of the map.
keepers <- fread(paste0(write_folder,'bundle_keepers.csv'))
keepers <- keepers$V1
b_swaps <- fread(paste0(write_folder,'bundle_swaps.csv'))
setnames(b_swaps,c('V1','V2'),c('old','new'))
b_swaps <- b_swaps[old != 0]

hosp_dt <- data.frame()
twn <- data.frame()
pol <- data.frame()
ms <- data.frame()

# Aggregate neonatal bundles -----
neonatal_collapser <- function(df){
  df_chromo <- df[bundle_id %in% c(436, 3029)]
  cols <- names(df_chromo)
  cols <- str_subset(cols,"inp_pri_claims_cases|cases_alt", negate=TRUE)
  df_chromo <- df_chromo[, .(inp_pri_claims_cases = sum(inp_pri_claims_cases), cases_alt = sum(cases_alt))
               ,by=cols]
  df_chromo[, index := 1]
  df_chromo[, bundle_id := NULL]
  
  buns <- data.table(bundle_id = c(436,437,438,439,638,3029), index = c(1,1,1,1,1,1))
  df_chromo <- merge(df_chromo, buns, by='index', allow.cartesian = TRUE)
  df_chromo[, index := NULL]
  
  df <- df[!(bundle_id %in% c(436,437,438,439,638,3029))]
  df <- rbind(df, df_chromo)
  
  return(df)
}

# Hospital data -----
hosp_files <- Sys.glob(FILEPATH)
filereader <- lapply(hosp_files, function(filepath){
  a <- tryCatch({
    df <- fread(filepath)
    outcome <- paste0(filepath, ' worked!')
    print(outcome)
    print(ncol(df))
    print(nrow(df))
    output <- list(data = df, logs = outcome)
    return(output)
  }, 
  error = function(cond){
    outcome <- paste0(filepath, ' broke')
    message(outcome)
    output <- list(data = NULL, logs = outcome)
    return(output)
  })
} )

hosp_dt <- rbindlist(lapply(filereader, function(list) list$data))
logs <- unlist(lapply(filereader, function(list) list$logs), use.names = FALSE)
logs <- data.frame(logs)
write_csv(logs, paste0(write_folder, 'fileloading.csv'))

hosp_dt[is.na(hosp_dt)] <- 0
if(nrow(hosp_dt) > 0){
  hosp_dt <- age_binner(hosp_dt)
}

hosp_dt[,age := NULL]
hosp_dt[,age_end := NULL]
hosp_dt <- hosp_dt[,lapply(.SD,sum),by=c('location_id','year_start','year_end','sex_id','bundle_id','age_start')]
hosp_dt <- melt(hosp_dt, id.vars = c('location_id','year_start','year_end','sex_id','bundle_id','age_start',
                                     'inp_pri_claims_cases'),variable.name = 'dorm_alt', value.name = 'cases_alt')
hosp_dt <- hosp_dt[,.(inp_pri_claims_cases = sum(inp_pri_claims_cases), cases_alt = sum(cases_alt)),
                   by=c('sex_id','age_start','year_start','year_end','location_id','bundle_id','dorm_alt')]

hosp_dt <- merge(hosp_dt, ages[age_group_id > 4, c('age_start', 'age_group_id', 'age_end')], by = 'age_start')
# Make mid-point of ages
hosp_dt[, age_midpoint := (age_start+age_end)/2]
hosp_dt <- merge(hosp_dt, locs[, c('location_id', 'parent_id')], by = 'location_id') %>%
  .[parent_id == 9, source := 'PHL'] %>%
  .[parent_id == 72, source := 'NZL'] %>%
  .[parent_id == 102, source := 'HCUP']

# Need to download the envelope and population to get an estimate of inpatient sample size
pops <- get_population(year_id = unique(hosp_dt$year_start), sex_id = c(1,2), location_id = unique(hosp_dt$location_id), age_group_id = unique(hosp_dt$age_group_id),
                       gbd_round_id = 7, decomp_step = 'step2') %>% setnames('year_id', 'year_start')

pops[, sample_size := population]
hosp_dt <- merge(hosp_dt, pops[, c('sex_id', 'age_group_id', 'year_start', 'location_id', 'sample_size')], 
                 by = c('sex_id', 'age_group_id', 'year_start', 'location_id'))
hosp_dt[, year_end := NULL]

# Poland ----
pol_files <- Sys.glob(FILEPATH)
pol <- lapply(pol_files, function(x){
  pdt <- fread(x)
  return(pdt)
})
pol <- rbindlist(pol, use.names = TRUE)
pol <- age_binner(pol)
pol[,age := NULL]
pol <- melt(pol, id.vars = c('bundle_id','year_start','year_end','sex_id','location_id','age_start','age_end','inp_pri_claims_cases'),
            variable.name = 'dorm_alt',value.name = 'cases_alt')
pol <- merge(pol, ages[age_group_id > 4,c('age_start','age_end','age_group_id')], by=c('age_start','age_end'))
pol_pop <- get_population(age_group_id = unique(pol$age_group_id), year_id = unique(pol$year_start), location_id = unique(pol$location_id), sex_id = c(1,2),
                          gbd_round_id = 7, decomp_step = 'step2')
pol_pop[,run_id := NULL]
setnames(pol_pop,c('year_id','population'),c('year_start','sample_size'))
pol <- merge(pol, pol_pop, by = c('year_start','age_group_id','sex_id','location_id'))
pol[,age_midpoint := (age_start + age_end)/2]
pol[, source := 'POL'][, parent_id := 51]
pol[is.na(pol)] <- 0

pol <- pol[,.(inp_pri_claims_cases = sum(inp_pri_claims_cases), cases_alt = sum(cases_alt), sample_size = sum(sample_size)),
           by=c('sex_id','age_group_id','age_start','age_midpoint','age_end','year_start','location_id','parent_id','source','bundle_id','dorm_alt')]

# Taiwan -----
taiwan <- fread(FILEPATH)
taiwan[age_ihmec == '06d', age_ihmec := '0.07671233']
taiwan[age_ihmec == '28d', age_ihmec := '0.50136986']
taiwan[, age_ihmec := as.numeric(age_ihmec)]
taiwan <- taiwan[!is.na(age_ihmec)]
taiwan[age_ihmec > 1, age_ihmec := age_ihmec + 1]
taiwan[age_ihmec == 96, age_ihmec := 125]
setnames(taiwan, 'age_ihmec', 'age_end')
taiwan <- merge(taiwan, ages[age_group_id != 28, c('age_start', 'age_end', 'age_group_id')], by = 'age_end', all.x = TRUE)
taiwan[is.na(age_start), `:=`(age_end = 0.01917808, age_start = 0, age_group_id = 2)]
taiwan[, age_midpoint := (age_start + age_end)/2]
taiwan[age_midpoint == 110, age_midpoint := 97.5]
taiwan[, location_id := 8][,source := 'TWN']
setnames(taiwan, 'sex', 'sex_id')

# Aggregate over subnationals
taiwan[taiwan == '.'] <- 0
taiwan$bundle_id <- as.numeric(taiwan$bundle_id)
taiwan[bundle_id == 66107, bundle_id := 6107][bundle_id == 66110, bundle_id := 6110][bundle_id == 6607, bundle_id := 6077]
taiwan <- taiwan[, lapply(.SD, as.numeric), by = c('age_end', 'age_start', 'age_group_id', 'sex_id', 'location_id', 'age_midpoint','source'),
       .SDcols = c('bundle_id','inp_pri_indv_cases', 'inp_any_indv_cases', 'inp_otp_any_adjusted_otp_only_indv_cases', 'inp_pri_claims_cases')]
taiwan <- taiwan[, lapply(.SD, sum, na.rm = TRUE), 
                 by = c('age_end', 'age_start', 'age_group_id', 'bundle_id', 'sex_id', 'location_id', 'age_midpoint','source'),
               .SDcols = c('inp_pri_indv_cases', 'inp_any_indv_cases', 'inp_otp_any_adjusted_otp_only_indv_cases', 'inp_pri_claims_cases')]
taiwan <- merge(taiwan, b_swaps, by.x = 'bundle_id', by.y = 'old', all.x = TRUE)
taiwan[is.na(new), bundle_id := bundle_id]
taiwan[!is.na(new), bundle_id := new]
taiwan[,new := NULL]
taiwan <- taiwan[bundle_id %in% keepers]

# standard error using the total number as sample size
taiwan <- melt(taiwan, id.vars=c('sex_id','age_group_id','age_start','age_midpoint','age_end','location_id','source','bundle_id','inp_pri_claims_cases'),
               variable.name = 'dorm_alt',value.name = 'cases_alt')
taiwan[,parent_id := 5]
# only have 2016 taiwan data
tpop <- get_population(age_group_id = unique(taiwan$age_group_id), location_id = 8, year_id =  2016, sex_id = c(1,2), gbd_round_id = 7, decomp_step = 'step2')
taiwan <- merge(taiwan,tpop[,c('age_group_id','sex_id','population')],by=c('age_group_id','sex_id'))
setnames(taiwan,'population','sample_size')

taiwan[, year_start := 2016]

# Marketscan -----
# Download and format data
ms <- fread(FILEPATH)
ms[, facility_id := NULL]
# Test removal of 2000
ms <- ms[year_start != 2000]
ms <- dcast(ms, age_end + age_start + bundle_id + location_id + sex_id + year_end + year_start ~ estimate_type, value.var = 'val', fun.aggregate = sum)
ms[is.na(ms)] <- 0

# Get sample size, need Marketscan sample size files and an egeoloc map to get to location id
egeoloc_map <- fread(FILEPATH) # For merging
enrolee_files <- Sys.glob(FILEPATH)
enrolees <- rbindlist(lapply(enrolee_files, read.dta13)) %>% 
  merge(egeoloc_map, by = 'egeoloc') %>% 
  setnames('age_start', 'age') %>% age_binner()
enrolees[, sample_size := sum(sample_size), by = c('age_start', 'sex', 'year', 'location_id')] # Aggregate over 5-year age groups
enrolees <- unique(enrolees[, c('age_start', 'sex', 'year', 'location_id', 'sample_size')]) %>% setnames(c('sex', 'year'),c('sex_id', 'year_start'))
enrolees[, sex_id := as.numeric(sex_id)]
ms <- merge(ms, enrolees, by = c('age_start', 'sex_id', 'year_start', 'location_id'))

# Drop US national data
ms <- ms[location_id != 102]
ms <- melt(ms, id.vars = c('age_start','age_end','sex_id','year_start','year_end','location_id','bundle_id',
                           'inp_pri_claims_cases','sample_size'), variable.name = 'dorm_alt', value.name = 'cases_alt')
ms <- ms[,.(inp_pri_claims_cases = sum(inp_pri_claims_cases), cases_alt = sum(cases_alt), sample_size = sum(sample_size)),
         by = c('age_start','age_end','sex_id','location_id','bundle_id','dorm_alt','year_start','year_end')]

ms <- merge(ms, ages[age_group_id > 4, c('age_start', 'age_group_id')], by = 'age_start')
ms[, age_midpoint := (age_start + age_end)/2]
ms[, source := 'Marketscan']
ms[, parent_id := 102]
ms[, year_end := NULL]

# Combine and prep -----
df <- rbind(ms, hosp_dt)
df <- rbind(df, pol)
df <- rbind(df, taiwan)

df <- df[,.(inp_pri_claims_cases = sum(inp_pri_claims_cases), cases_alt = sum(cases_alt), sample_size = sum(sample_size)),
         by = c('age_start','age_end','age_group_id','age_midpoint','sex_id','location_id','parent_id','source','bundle_id','dorm_alt')]
df <- neonatal_collapser(df)

# Calculate variance on each point, using wilson's method 
# std_error = sqrt(1 / sample_size * cf * (1 - cf) + 1 / (4 * sample_size^2) * 1.96^2)
# Just need to calculate the total claims to get a cause fraction
df[, total_inp_pri_claims_cases := sum(inp_pri_claims_cases), by = c('age_group_id','location_id','sex_id','source')]
df[sample_size < 1, sample_size := 1]
df[, af := inp_pri_claims_cases/total_inp_pri_claims_cases]
# Matching Wilson's approx 
df[, se_ref := sqrt(((af*(1-af))/sample_size + 1.96^2/(4*sample_size^2))/(1 + 1.96^2/sample_size)^2)]
df <- df[se_ref > 0]
df[, se_alt := se_ref]
df[, s_loc := paste0(source,location_id)]

# Age-sex restrictions
rests <- get_bundle_restrictions() %>% as.data.table()
rests <- melt(rests, id.vars = c('bundle_id','yld_age_start','yld_age_end','map_version'), variable.name = 'sex', value.name = 'sex_present')
rests[sex == 'male', sex_id := 1][sex == 'female', sex_id := 2]

df <- merge(df, rests, by = c('bundle_id','sex_id'))
df <- df[sex_present != 0]
df <- df[age_start >= yld_age_start]

# Special handling of age max for Taiwan <1 age data
df[, rm := 0]
df[(yld_age_end >= 1) & (age_start > yld_age_end), rm := 1]
df[(yld_age_end < 1) & age_start >= 1, rm := 1]
df <- df[rm != 1]
df[, rm := NULL]
df[age_midpoint == 110, age_midpoint := 97.5]

cwprep <- function(df,cf){
  sink(paste0(write_folder,cf,'_summary.txt'))
  print(paste0('Input data from run_id ',run))
  print('Rows by source: ')
  nrow(hosp_dt)
  nrow(twn)
  nrow(pol)
  nrow(ms)
  print(paste0('Total rows: ',nrow(df)))
  print(paste0('Losing ',nrow(df[inp_pri_claims_cases == 0]),' rows w/ 0s in the denominator'))
  df <- df[inp_pri_claims_cases != 0]
  print(paste0('Losing ',nrow(df[cases_alt == 0]),' rows w/ 0s in the numerator'))
  df <- df[cases_alt != 0]
  sink()
  
  if(cf == 'cf1'){
    df[, cf := cases_alt/inp_pri_claims_cases]
    df <- df[!is.nan(cf)][cf != 0]
    transformation <- crosswalk::delta_transform(mean = df$cf, sd = df$se_ref, transformation = 'linear_to_logit') %>% as.data.table()
    setnames(transformation,c('mean_logit','sd_logit'),c('cf','cf_se'))
    df[,cf := NULL]
    df <- cbind(df, transformation)
  }else{
    dat_diff <- as.data.frame(cbind(
      crosswalk::delta_transform(
        mean = df$cases_alt, 
        sd = df$se_alt,
        transformation = 'linear_to_log'),
      crosswalk::delta_transform(
        mean = df$inp_pri_claims_cases, 
        sd = df$se_ref,
        transformation = 'linear_to_log')
    ))
    names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")
    
    df[, c("cf", "cf_se")] <- calculate_diff(
      df = dat_diff, 
      alt_mean = "mean_alt", alt_sd = "mean_se_alt",
      ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
  }
  
  df <- df[!is.na(cf) & cf != Inf]
  df <- df[!is.na(cf_se)]
  return(df)
}

df <- df[dorm_alt %in% c('inp_pri_indv_cases','inp_any_indv_cases','inp_otp_any_adjusted_otp_only_indv_cases')]

df[dorm_alt == 'inp_pri_indv_cases' & inp_pri_claims_cases == cases_alt, cases_alt := 0.99*cases_alt]

df <- split(df, by='dorm_alt')[1:3]
prepped_data <- lapply(names(df), function(x){
  if(x == 'inp_pri_indv_cases'){
    cf <- 'cf1'
  }else if(x == 'inp_any_indv_cases'){
    cf <- 'cf2'
  }else{
    cf <- 'cf3'
  }
  print(x)
  data <- cwprep(df[[x]],cf)
  return(data)
})

prepped_data <- rbindlist(prepped_data)
prepped_data[dorm_alt == 'inp_pri_indv_cases', correction := 'cf1']
prepped_data[dorm_alt == 'inp_any_indv_cases', correction := 'cf2']
prepped_data[dorm_alt == 'inp_otp_any_adjusted_otp_only_indv_cases', correction := 'cf3']
prepped_data[, env := 1][, env_group := 1]
prepped_data <- prepped_data[,.(age_start,age_midpoint,age_group_id,sex_id,source,location_id,s_loc,parent_id,env,env_group,
                                bundle_id,correction,total_inp_pri_claims_cases,sample_size,inp_pri_claims_cases,
                                se_ref,dorm_alt,cases_alt,se_alt,cf,cf_se)]

prepped_data <- split(prepped_data, by=c('bundle_id','correction'))
dir.create(paste0(write_folder,'cf1/'), showWarnings = FALSE)
dir.create(paste0(write_folder,'cf2/'), showWarnings = FALSE)
dir.create(paste0(write_folder,'cf3/'), showWarnings = FALSE)
lapply(names(prepped_data), function(x){
  bun_id <- str_split_fixed(x,'\\.',2)[[1]]
  cf <- str_split_fixed(x,'\\.',2)[[2]]
  fwrite(prepped_data[[x]], paste0(write_folder,cf,'/',bun_id,'.csv'))
})

