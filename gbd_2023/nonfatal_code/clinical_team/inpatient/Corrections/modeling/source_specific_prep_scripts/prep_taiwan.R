#############################################################
# Purpose: prep taiwan CF inputs
# Notes:
# For Taiwan data we can only use data for bundles that have not had mapping changes between when we last
# received TWN data (2019) and current
#############################################################

# Get settings / controls ####
source("~/00a_prep_setup.R")
twn = data.frame()

if(prep_twn == TRUE) {
# Get TWN bundles to keep and swap ####
  twn_bundles_dir = paste0("FILEPATH")
  if (!dir.exists(twn_bundles_dir)) dir.create(twn_bundles_dir)
  
  ## If using a new map verson, this following code will need to be rerun ####
  if (!"bundle_keepers.csv" %in% list.files(twn_bundles_dir)){
    odbc <- ini::read.ini("FILEPATH")
    conn_def <- "USERNAME"
    con <- RMySQL::dbConnect(RMySQL::MySQL(),
                                host = odbc[[conn_def]]$SERVER,
                                username = odbc[[conn_def]]$USER,
                                password = odbc[[conn_def]]$PASSWORD)
    keep_bundle_ids = dbGetQuery(con, paste0("QUERY"))
    dbDisconnect(con)
    write.csv(keep_bundle_ids, file = paste0(twn_bundles_dir, 'bundle_keepers.csv'), row.names = FALSE)

    keep_bundle_ids = as.data.table(read.csv("FILEPATH"))
    keep_bundle_ids = keep_bundle_ids[, c('bundle_id'), with=FALSE]
    write.csv(keep_bundle_ids, file = paste0(twn_bundles_dir, 'bundle_keepers.csv'), row.names = FALSE)
  }
  
  keepers = fread(paste0(twn_bundles_dir,'bundle_keepers.csv'))
  b_swaps = fread(paste0(twn_bundles_dir,'bundle_swaps.csv'))
  if (length(b_swaps)>2) stop("There should only be two columns in this data table - check if row names column is present because then the following code will not work correctly:")
  setnames(b_swaps,c('V1','V2'),c('old','new'))
  b_swaps = b_swaps[old != 0]

  # Taiwan data ####
  ## Create write folder ####
  write_folder = paste0('FILEPATH')
  if (!dir.exists(write_folder)) dir.create(write_folder)
  
  ## Read in data ####
  taiwan = fread('FILEPATH')
  
  ## Clean up age data ####
  taiwan[age_ihmec == '06d', age_ihmec := '0.07671233']
  taiwan[age_ihmec == '28d', age_ihmec := '0.50136986']
  taiwan[, age_ihmec := as.numeric(age_ihmec)]
  taiwan = taiwan[!is.na(age_ihmec)]
  taiwan[age_ihmec > 1, age_ihmec := age_ihmec + 1]
  taiwan[age_ihmec == 96, age_ihmec := 125]
  setnames(taiwan, 'age_ihmec', 'age_end')
  ## Merge on age_group_id ####
  taiwan = merge(taiwan, ages[age_group_id != 28, c('age_start', 'age_end', 'age_group_id')], by = 'age_end', all.x = TRUE)
  taiwan[is.na(age_start), `:=`(age_end = 0.01917808, age_start = 0, age_group_id = 2)]
  taiwan[, age_midpoint := (age_start + age_end)/2]
  taiwan[age_midpoint == 110, age_midpoint := 97.5]
  ## Other data clean up ####
  taiwan[, location_id := 8][,source := 'TWN']
  setnames(taiwan, 'sex', 'sex_id')
  
  ## Clean up bundle ids ####
  taiwan[taiwan == '.'] = 0
  taiwan$bundle_id = as.numeric(taiwan$bundle_id)
  taiwan[bundle_id == 66107, bundle_id := 6107][bundle_id == 66110, bundle_id := 6110][bundle_id == 6607, bundle_id := 6077]
  
  ## Aggregate over the incorrect subnationals ####
  taiwan = taiwan[, lapply(.SD, as.numeric), by = c('age_end', 'age_start', 'age_group_id', 'sex_id', 'location_id', 'age_midpoint','source'),
                   .SDcols = c('bundle_id','inp_pri_indv_cases', 'inp_any_indv_cases', 'inp_otp_any_adjusted_otp_only_indv_cases', 'inp_pri_claims_cases')]
  taiwan = taiwan[, lapply(.SD, sum, na.rm = TRUE),
                   by = c('age_end', 'age_start', 'age_group_id', 'bundle_id', 'sex_id', 'location_id', 'age_midpoint','source'),
                   .SDcols = c('inp_pri_indv_cases', 'inp_any_indv_cases', 'inp_otp_any_adjusted_otp_only_indv_cases', 'inp_pri_claims_cases')]
  
  ## Swap old bundle ids for new ones and keep the bundles that haven't changed mapping ####
  taiwan = merge(taiwan, b_swaps, by.x = 'bundle_id', by.y = 'old', all.x = TRUE)
  taiwan[is.na(new), bundle_id := bundle_id]
  taiwan[!is.na(new), bundle_id := new]
  taiwan[,new := NULL]
  taiwan = taiwan[bundle_id %in% keepers$bundle_id]
  
  ## Reshape long by estimate type ####
  taiwan = melt(taiwan, 
                id.vars=c('sex_id','age_group_id','age_start','age_midpoint','age_end','location_id','source','bundle_id'),
                variable.name = 'estimate_type',value.name = 'val')
  # Map on the estimate ids/dno classifications and keep just estimates we want for CFs
  twn_ids = merge(taiwan, dno_mapping, by = 'estimate_type')

  # standard error using the total number as sample size
  # only have 2016 taiwan data
  tpop = get_population(age_group_id = unique(twn_ids$age_group_id), 
                        location_id = 8,
                        year_id =  2016, 
                        sex_id = c(1,2), 
                        release_id=9)
  twn_ids = merge(twn_ids, tpop[,c('age_group_id','sex_id','population')], by=c('age_group_id','sex_id'))
  
  ## Clean up ####
  # Create columns for parent_id
  twn_ids[,parent_id := 5]
  twn_ids[, year_start := 2016]
  setnames(twn_ids, 'estimate_id', 'estimate')
  twn_ids[, estimate_type := NULL]
  setnames(twn_ids, "year_start", "year_id")
  
  ## Save data by estimate ID ####
  lapply(unique(twn_ids$estimate), function(x){
    write_data = twn_ids[estimate == x, ]
    fwrite(write_data, paste0(write_folder,'/',x,'.csv'))
  })
  
}
