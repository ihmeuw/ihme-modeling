## ******************************************************************************
## Purpose: Clean processed registry and lit bundle data and clean to pass bundle upload validations
## Input:   Bundle ID(s)
## Output:  Uploads bundle data
## ******************************************************************************


args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
gbd_round_id <- args[2]
decomp_step <- args[3]



os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('openxlsx')
library('dplyr')
library('data.table')
library('stringr')



source("FILEPATH")
source("FILEPATH")

'%ni%' <- Negate('%in%')
map <- fread("FILEPATH") %>% as.data.table()
cong_map_dt <- map[type == 'congenital' & me_name != '']

new_lit_bundles <- c(628, 630, 632, 634, 636, 2978)

get_upper_lower <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[!is.na(mean) & !is.na(standard_error) & is.na(lower), lower := mean - (1.96 * standard_error)]
  dt[!is.na(mean) & !is.na(standard_error) & is.na(upper), upper := mean + (1.96 * standard_error)]
  return(dt)
}


##' ************************************************************
##' 1.  APPEND REGISTRY AND LIT DATA AND CLEAN FOR VAIDATIONS
##' ************************************************************


print(bun_id)
# Read in registry data
bun_data_reg <- read.xlsx(paste0("FILEPATH")) %>% as.data.table()
bun_data_reg <- bun_data_reg[, cv_literature := 0]

# Read in data where we have new extractions
if (bun_id %in% new_lit_bundles){
  bun_data_new_lit <- read.xlsx(paste0("FILEPATH")) %>% as.data.table()
  bd_notuk <- copy(bun_data_new_lit)
  bd_notuk <- bd_notuk[location_id != 95]
  ## Re-mapping UK heart lit extractions
  bduk <- copy(bun_data_new_lit)
  bduk <- bduk[location_id == 95]
  bduk_expanded <- bduk[rep(seq_len(nrow(bduk)), each = 4), ]
  
  bduk_expanded$duplicate <- rep(1:4, times = (nrow(bduk_expanded)/4))
  bduk_expanded <- bduk_expanded[is.na(mean), mean := cases/sample_size]
  bduk_expanded <- bduk_expanded[, c("location_id", "location_name", "ihme_loc_id") := NULL]
  
  uk_map <- read.xlsx("FILEPATH")
  
  # calculate new means for the next 4 rows of each original data point, setting each UK location to one of the four regions
  bduk_weights <- merge(bduk_expanded, uk_map, by = 'duplicate')
  bduk_weights <- bduk_weights[, new_weighted_mean := mean * pop_weight]
  bduk_weights <- bduk_weights[, duplicate := NULL]
  bduk_weights <- bduk_weights[, mean := NULL]
  setnames(bduk_weights, "new_weighted_mean", "mean")
  bun_data_new_lit <- bduk_weights
  
  #append not UK data
  bun_data_new_lit <- rbind(bun_data_new_lit, bd_notuk, fill = TRUE)
  print(paste0("New lit has ", length(unique(bun_data_new_lit$nid)), " unique NIDs"))
}

# Read in remaining lit extractions
bun_data_old_lit <- read.xlsx(paste0("FILEPATH")) %>% as.data.table()

bun_data_old_lit[, flag_literature := NULL]
if (bun_id == 602){
  bun_data_old_lit <- bun_data_old_lit[, cv_lit := NULL]
}
if (bun_id != 602){
  setnames(bun_data_old_lit, 'cv_lit', 'cv_literature')
}

# Append data
if (bun_id %in% new_lit_bundles){
  bun_data <- rbindlist(list(bun_data_reg, bun_data_old_lit, bun_data_new_lit), fill = TRUE) #%>% as.data.table()
} else {
  bun_data <- rbindlist(list(bun_data_reg, bun_data_old_lit), fill = TRUE) %>% as.data.table()
}

#### Begin cleaning data for validations
df <- copy(bun_data)

# if mean is negative set to zero (this is bc means were originally zero prior to getting adjusted and then when we subtracted the offset*beta it became negative)
df <- df[mean < 0, mean:= 0]
df$standard_error <- as.numeric(df$standard_error)
df <- get_upper_lower(df)
df$lower <- as.numeric(df$lower)
df$upper <- as.numeric(df$upper)
df <- df[lower < 0, lower := 0]
df <- df[upper > 1, upper := .999] # this is for newzealand ultra high data
df <- df[check == -1, check := 0] # not sure what this is but it can't be negative

df <- df[underlying_nid == '.', underlying_nid := '']
df <- df[, underlying_nid := as.numeric(underlying_nid)]


# Read in csmr data for bundle 3029
if (bun_id == 3029){
  df_csmr <- get_bundle_data(bundle_id = 3029, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
  df_csmr <- df_csmr[nid == 347976]
  
  #aggregate old norway data into new subnat groups
  norway_map <- fread(input = paste0("FILEPATH"))
  setnames(norway_map, 'gbd19_loc_id', 'location_id')
  df_csmr <- merge(df_csmr, norway_map, by = 'location_id', all.x = TRUE)
  df_csmr[!is.na(gbd20_loc_id), `:=`(cases = sum(.SD$cases),
                                     sample_size = sum(.SD$sample_size)),
          by=c('gbd20_loc_id', 'year_start', 'sex', 'age_start', 'age_end','nid')]
  #replace old norway data with new subnat rows
  norway_data <- df_csmr[!is.na(gbd20_loc_id)]
  df_csmr <- df_csmr[is.na(gbd20_loc_id)]
  norway_data <- norway_data[!duplicated(norway_data[,c('gbd20_loc_id', 'year_start', 'sex', 'age_start', 'age_end', 'nid')]),]
  df_csmr <- rbind(df_csmr, norway_data)
  df_csmr[!is.na(gbd20_loc_id), location_id := gbd20_loc_id]
  df_csmr$gbd20_loc_id <- NULL
  
  df <- rbind(df, df_csmr, fill = TRUE)
}

# For heart bundles, correct extraction errors, remove SS for NID 4241717 bc its throwing an invalid bound validation error and mean/upper/lower is present
if (bun_id %in% c(628, 634, 630, 636, 632)){
  df <- df[nid == 423706, nid := 423705]
  df <- df[unit_type == 1, unit_type := 'Person']
  df <- df[representative_name == 'Nationally representative', representative_name := 'Nationally representative only']
  df <- df[is.na(urbanicity_type), urbanicity_type := "Unknown"]
  df <- df[nid == 424171, sample_size := 'NA']
  df <- df[nid == 424171, cases := 'NA']
}


# Remove invalid rows where SS < cases (ignoring rows that have SS = NA and mtspecific rows)
df_na <- df[is.na(sample_size)]
df1 <- df[!(sample_size < cases & measure != 'mtspecific')]
df <- rbind(df_na, df1)


# Delete note_SR (a weird column somehow added, we want note_sr) 
df <- df[, note_SR := NULL]

# Set is_outlier to zero (no rows should be outlier = 1 at this point)
df <- df[, is_outlier := 0]

# Delete cv_isolated_only (these rows are already group review zero)
df <- df[, cv_isolated_only := NULL]

# Assign lit extraction error rows with no source type to 'Survey - other/unknown'
df[cv_literature == 1 & is.na(source_type) | source_type == '', source_type := 'Survey - other/unknown']

# If mean, lower, and upper are not blank, and uncertainty_type_value is blank, set uncertainty_type_value to 95
df[mean != '' & lower != '' & upper != '' & uncertainty_type_value == '' | is.na(uncertainty_type_value), uncertainty_type_value := 95 ]

# If mean, lower, and upper are blank (or NA), and uncertainty_type_value is not blank, set uncertainty_type_value to NA
df[(mean == '' | is.na(mean)) & (lower == '' | is.na(lower)) & (upper == '' | is.na(upper)) & uncertainty_type_value != '', uncertainty_type_value := 'NA']

# Set blank or NA recall type to 'Point'
df[recall_type == '' | is.na(recall_type), recall_type := 'Point']

# Clear seq column
df <- df[, seq := 'NA']

# Set note_sr, case_definition, case_diagnostics, and case_name to max character length of 100
df <- df[, note_sr := substr(note_sr, 0, 96)]

df <- df[, case_definition := substr(case_definition, 0, 96)]

df <- df[, case_diagnostics := substr(case_diagnostics, 0, 96)]

df <- df[, case_name := substr(case_name, 0, 96)]

# Set any cv columns with NAs to zeros

df1 <- df %>% select(starts_with("cv_"))
df <- df %>% select(-contains("cv_"))

df1[df1 == ''] <- 0
df1[is.na(df1)] <- 0

df <- cbind(df, df1)

# Set lower minimum to zero for Klinefelter (the only bundle that didn't need splitting therefore didn't receive this correction in age/sex script)
if (bun_id == 438){
  df <- df[lower < 0, lower := 0]
}

# Correct literature data (bc registry should have already been fixed) mortality data means (mtwith value)
df_mortality <- df[measure == 'mtwith']

# Write out mtwith rows with age end of zero to unusable data directory to re-extract later
reextract_mtwith <- df_mortality[age_end == 0 | age_end == 	0.00000000]
write.xlsx(reextract_mtwith, paste0("FILEPATH"), sheetName="extraction")
df_mortality <- df_mortality[age_end != 0 | age_end != 	0.00000000]


# Create a proportion dead variable (cases/SS) and a number variable if there is mtwith at multiple age levels for the same source-- does this need to be year specific?
df_mortality <- df_mortality[, proportion_dead := (cases/sample_size)]
df_mortality <- df_mortality[, number := .N, by = .(age_end, nid)]

# Drop rows that have 0% mortality (proportion dead == 0)
df_mortality <- df_mortality[!(proportion_dead == 0 | proportion_dead == 0.00000000)]

# Drop rows that have 100% mortality (proportion dead == 1) but SS < than 10
df_mortality <- df_mortality[!(proportion_dead == 1 & sample_size <10)]

# Drop rows that have 100% mortality (proportion dead == 1) but number of age groups > 1
df_mortality <- df_mortality[!(proportion_dead == 1 & number > 1)]

# For rows that have proportion dead == 1 but don't fall in above categories, set mortality to 95% instead and recalculate appropriate mtwith value
df_mortality[proportion_dead == 1, proportion_dead := .95]
test <- df_mortality[, mean := ((-log(1-proportion_dead))/(age_end - age_start))]

# Set mtwith lower, upper, uncertainty_type_value to NA, remove proportion_dead and number variables that we created above
df_mortality[, lower := NA]
df_mortality[, upper := NA]
df_mortality[, uncertainty_type_value := NA]
df_mortality[, proportion_dead := NULL]
df_mortality[, number := NULL]

# Append prevalence and revised mortality
df_prev <- df[measure != 'mtwith']
df <- rbind(df_prev, df_mortality)
df$check <- NA


# Write out a file for upload to the same archive file (writes over the original)
write.xlsx(df, file = paste0("FILEPATH"), sheetName="extraction")

##'************************************************************
##'2.  UPLOAD BUNDLE DATA AND WRITE OUT BUNDLE SUMMARY
##'************************************************************

upload_bundle_data(bundle_id=bun_id, decomp_step=decomp_step, gbd_round_id=gbd_round_id, filepath=paste0("FILEPATH", decomp_step, "/raw_", bun_id, ".xlsx"))

# Check the bundle data for row counts and mean values 
bun_summary <- data.table()
bun_summary$rows <- nrow(df)
df <- df[measure == 'prevalence']
bun_summary$min <- min(df$mean)
bun_summary$max <- max(df$mean)
bun_summary$mean <- mean(df$mean)
bun_summary$bundle <- bun_id
bun_summary$bundle_name <- cong_map_dt[bundle_id == bun_id]$bundle_name

write.xlsx(bun_summary, paste0("FILEPATH", decomp_step, '_', bun_id, '_', Sys.Date(), '_summary.xlsx'))

