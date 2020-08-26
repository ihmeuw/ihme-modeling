################################################################################
## DESCRIPTION: Rescales Dismod categorical prevalences of physical activity by by draw to sum to 1 ##
## INPUTS: Dismod draws of categorical prevalence (6 MEs) ##
## OUTPUTS: Scaled draws of categorical prevalence (6 MEs) ##
## AUTHOR: 
## DATE: 
################################################################################

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
code_dir <- if (os == "Linux") "FILEPATH" else if (os == "Windows") ""

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH/primer.R'))
source(paste0(code_dir, 'FILEPATH/activity_utilities.R'))
source(paste0('FILEPATH/interpolate.R'))
library(stringr)

## SCRIPT SPECIFIC FUNCTIONS

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Location ID to be rescaled",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--activity_version_id", help = "Version number for whole run",
                    default = 1, type = "integer")
parser$add_argument("--gbd_round_id", help = "The current GBD round ID",
                    default = 6, type = "integer")
parser$add_argument("--resub", help = "Whether or not this job is being resubmitted",
                    default = 0, type = "integer")

parser$add_argument("--decomp_step", help = "Which Decomp step this is for",
                    default = "step1", type = "character")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

## LOAD RELEVANT DATA

## Load configurations for run
load_config(paste0(code_dir, 'FILEPATH'), c('gbd', 'activity', 'demo_validation'))

## LOAD TASK INFORMATION
task_id <- ifelse(interactive(), 721, Sys.getenv("SGE_TASK_ID"))
task_map <- fread(sprintf('%s/v%d/task_map.csv', data_dir, activity_version_id))
task_var <- ifelse(!as.logical(resub), 'task_num', 'resub_task_num')
loc_id <- task_map[get(task_var) == task_id, location_id]
loc_map <- fread(sprintf('%s/v%d/loc_map.csv', data_dir, activity_version_id))[location_id == loc_id]
version_map <- fread(paste0(code_dir, 'FILEPATH'))[activity_version_id == get("activity_version_id", .GlobalEnv)]

## LOAD RELEVANT DATA
activity_variant_dfs <- {}

for(activity_variant in activity_variants){
  
  # MEs for physical activity
  me_table <- data.table(modelable_entity_id = c(9356:9361, 23714:23719), activity_variant = c(rep('gpaq', 6), rep('ipaq', 6)), measure = rep(grep('prev', activity_measures, value = T), 2))
  me_table <- me_table[activity_variant == get('activity_variant', .GlobalEnv)]
  me_ids <- me_table$modelable_entity_id
  
  me_versions <- version_map[, grep(activity_variant, names(version_map), value = T), with = F] %>% transpose %>% unlist
  prev_map <- data.table(modelable_entity_id = me_ids,
                         parent_me_id = c(0, me_ids[5], me_ids[6], me_ids[6], 0, me_ids[5]),
                         level = c(1, 2, 3, 3, 1, 2),
                         most_detailed = c(1, 1, 1, 1, 0, 0))
  
  prev_dfs <- {}
  
  for(me_id in 1:6){
    
    message(sprintf('Pulling and interpolating draws for me_id %d version %d', me_ids[me_id], me_versions[me_id]))
    
    start <- Sys.time()
    
    prev_dfs[[me_id]] <- interpolate(source = 'epi', gbd_id_type = 'modelable_entity_id', gbd_id = me_ids[me_id], 
                                     location_id = loc_id, reporting_year_start = 1990, reporting_year_end = tail(prod_year_ids,1), measure_id = 18, age_group_id = detailed_age_ids, sex_id = detailed_sex_ids,
                                     gbd_round_id = gbd_round_id, decomp_step = decomp_step, version_id = me_versions[me_id], num_workers = 10)
    
    message(sprintf('Interpolation for me_id %d version %d took %s seconds', me_ids[me_id], me_versions[me_id], as.character(round(Sys.time() - start))))
    
  }
  
  prev_df <- rbindlist(prev_dfs) %>% melt(., measure = patterns('draw_'), variable.name = 'draw'); prev_df[, draw := as.numeric(str_extract(draw, '[:digit:]{1,3}'))]
  
  ## BODY
  id_vars <- c('location_id', 'age_group_id', 'sex_id', 'year_id', 'measure_id', 'metric_id', 'draw')
  
  prev_df <- merge(prev_df, prev_map, by = 'modelable_entity_id')
  
  # Scale down levels
  for(lev in unique(prev_map$level)){
    
    message(sprintf("Scaling level %d to parent", lev))
    
    if(lev == 1){
      
      scaled_prev_df <- prev_df[level == 1]
      scaled_prev_df[, scalar := 1/sum(value), by = id_vars]
      scaled_prev_df[, value := value * scalar]
      
    } else {
      
      pdf <- scaled_prev_df[modelable_entity_id %in% unique(prev_df[level == lev, parent_me_id])]
      pdf[, names(pdf)[!names(pdf) %in% c('modelable_entity_id', id_vars, 'value')] := NULL]
      setnames(pdf, c('modelable_entity_id', 'value'), c('parent_me_id', 'parent_value'))
      cdf <- prev_df[level == lev]
      cdf <- merge(cdf, pdf, by = c(id_vars, 'parent_me_id'))
      
      cdf[, scalar := parent_value/sum(value), by = id_vars][, value := value * scalar][, parent_value := NULL]
      
      scaled_prev_df <- rbind(scaled_prev_df, cdf, use.names = T, fill = T)
    }
  }
  
  # Value assertions for internal consistency
  stopifnot(nrow(prev_df) == nrow(scaled_prev_df))
  assert_df <- scaled_prev_df[most_detailed == T, .(tol = sum(value)-1), by = id_vars]
  assert_values(assert_df, colnames = 'tol', test = 'lte', test_val = 1e-4)
  
  ## PREP FOR SAVE
  scaled_prev_df[, names(scaled_prev_df)[!names(scaled_prev_df) %in% c('modelable_entity_id', id_vars, 'value')] := NULL]
  scaled_prev_df <- merge(scaled_prev_df, me_table, by = 'modelable_entity_id')
  scaled_prev_df[, activity_version_id := activity_version_id]
  setcolorder(scaled_prev_df, c('activity_version_id', 'location_id', 'age_group_id', 'sex_id', 'year_id', 'activity_variant', 'modelable_entity_id', 'measure', 'measure_id', 'metric_id', 'draw', 'value'))
  
  ## VALIDATE
  activity.validate(scaled_prev_df, loc_id, age_ids = detailed_age_ids, sex_ids = detailed_sex_ids, year_ids =  prod_year_ids, prod_draws, measures = me_table$measure)
  
  activity_variant_dfs[[activity_variant]] <- scaled_prev_df; rm(scaled_prev_df)
  
}

activity_variant_dfs <- rbindlist(activity_variant_dfs)

## SAVE OUTPUTS
save_list <- split(activity_variant_dfs, by = c('measure', 'activity_variant'))
mclapply(save_list, function(df) write.csv(df, sprintf('%s/v%d/%s/scaled_%s_%d.csv', data_dir, activity_version_id, unique(df$measure), unique(df$activity_variant), loc_id), row.names = F), mc.cores = 5)

activity_variant_df <- activity_variant_dfs[modelable_entity_id %in% c(9356:9359, 23714:23717)]
activity_variant_df[, measure := gsub('prev_', 'mean', measure)][, measure := gsub('_', '', measure)][, measure := gsub('mean', 'mean_', measure)]
activity_variant_df[, c('activity_version_id', 'modelable_entity_id', 'measure_id', 'metric_id') := NULL]
agg_pops <- fread(sprintf('%s/v%d/agg_pops.csv', data_dir, activity_version_id))[location_id == loc_id]
activity_variant_df <- merge(activity_variant_df, agg_pops[, .(location_id, age_group_id, sex_id, year_id, pop = mean)], by = c('location_id', 'age_group_id', 'sex_id', 'year_id'))
activity_variant_df[age_group_id %in% c(30:32, 235), age_group_id := 21]
activity_variant_df <- activity_variant_df[, .(value = weighted.mean(value, pop)), by = .(location_id, age_group_id, sex_id, year_id, measure, draw, activity_variant)]
activity_variant_df <- dcast(activity_variant_df[order(location_id, age_group_id, sex_id, year_id, measure, draw)], ...~measure + draw, value.var = 'value')
activity_variant_df[, gpaq := as.integer(activity_variant_df$activity_variant == 'gpaq')][, activity_variant := NULL]

write.csv(activity_variant_df, sprintf('%s/v%d/format_met_prediction/%d.csv', data_dir, activity_version_id, loc_id), row.names = F)
