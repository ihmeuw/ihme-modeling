#############################################################
# Purpose: prep CMS Medicare CF inputs
#############################################################

# Get settings / controls ####
source("~/00a_prep_setup.R")

##### File paths  #####
directory = getwd()
read_folder = paste0("FILEPATH")
write_folder = (paste0("FILEPATH"))
if (!dir.exists(write_folder)) dir.create(write_folder, recursive = TRUE)

##### CMS data  #####
if (prep_cms == TRUE & grepl('FILEPATH', getwd())){
  ###### Read in data #####
  all_cms_files = Sys.glob(paste0(read_folder, 'mdcr*'))

  est_ids = unique(dno_mapping$estimate_id)
  files = lapply(est_ids, function(x){ all_cms_files[grepl(x, all_cms_files)]})
  cms_files = unlist(files)
  
  cms_data = data.table()
  
  cms_filereader = lapply(cms_files, function(filepath){
    a = tryCatch({
      df = as.data.table(read_parquet(filepath))
      # sum over age groups
      df = age_binner(df)
      df[, age := NULL]
      df = df[, .(val = sum(val, na.rm = TRUE)), by=c('location_id','year_start','year_end','sex_id','bundle_id','age_start', 'estimate_id', 'estimate_type')]
      outcome = paste0(filepath, ' worked!')
      output = list(data = df, logs = outcome)
      return(output)
    },
    error = function(cond){
      outcome = paste0(filepath, ' broke')
      output = list(data = NULL, logs = outcome)
      
      return(output)
    })
  })
  
  had_error = sapply(cms_filereader, function(x) class(x)=="try-error")
  if(length(which(had_error == TRUE)) > 0) print("Check filereader, some files not read correctly")
  
  cms_data = rbindlist(lapply(cms_filereader, function(list) list$data), use.names = TRUE, fill = TRUE)
  cms_data = dcast.data.table(cms_data, location_id + year_start + year_end + sex_id + age_start + bundle_id ~ estimate_type, value.var = 'val')
 
  # Set NAs to 0s
  cms_data[is.na(inp_pri_indv_cases), inp_pri_indv_cases := 0]
  cms_data[is.na(inp_pri_claims_cases), inp_pri_claims_cases := 0]
  cms_data[is.na(inp_any_indv_cases), inp_any_indv_cases := 0]
  cms_data[is.na(inp_otp_any_adjusted_otp_only_indv_cases), inp_otp_any_adjusted_otp_only_indv_cases := 0]
  cms_data[is.na(inp_pri_claims_5_percent), inp_pri_claims_5_percent := 0]
  cms_data[is.na(inp_pri_indv_5_percent_cases), inp_pri_indv_5_percent_cases := 0]
  cms_data[is.na(inp_any_indv_5_percent_cases), inp_any_indv_5_percent_cases := 0]
  
  # Reshape long
  cms_data_ids=melt(cms_data, id.vars = c('location_id','year_start','year_end','sex_id','bundle_id','age_start'), variable.name = 'estimate', value.name = 'val')

  # Map on the estimate ids/dno classifications
  cms_data_ids = merge(cms_data_ids, dno_mapping, by.x =c('estimate'), by.y = c('estimate_type'), all.x = TRUE)
  if(nrow(cms_data_ids[is.na(dno_name)]) > 0) print("Need to check the matching up of estimate names/ids")
  
  # Merge on age_group_id and make a midpoint of ages
  cms_data_ids=merge(cms_data_ids, ages[age_group_id > 4, c('age_start', 'age_group_id', 'age_end')], all.x = TRUE, by = 'age_start')
  cms_data_ids[, age_midpoint := (age_start+age_end)/2]
  
  # Use location ids to merge on parent ids from gbd locations and include source column
  cms_data_ids=merge(cms_data_ids, locs[, c('location_id', 'parent_id')], all.x = TRUE, by = 'location_id')
  cms_data_ids[, source := 'Medicare']

  # simplify data set
  cms_data_ids[, year_end := NULL]
  setnames(cms_data_ids, "year_start", "year_id")
  cms_data_ids[, estimate := NULL]
  setnames(cms_data_ids, "estimate_id", "estimate")
  
  # Save data by estimate ID ####
  lapply(unique(cms_data_ids$estimate), function(x){
    write_data = cms_data_ids[estimate == x, ]
    fwrite(write_data, paste0(write_folder,'/',x,'.csv'))
  })
}

