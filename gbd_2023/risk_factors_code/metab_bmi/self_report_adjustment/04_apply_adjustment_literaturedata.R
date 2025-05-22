################################################################################
## DESCRIPTION: Applies literature data self-report adjustment
## INPUTS: prevalence literature data
## OUTPUTS: prevalence literature data with applied literature self-report adjustment
## AUTHOR: 
## DATE:
################################################################################


# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- paste0(code_dir, "FILEPATH")

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))

## Load configuration for meta data variables
load_config(paste0(code_dir, 'FILEPATH'), c('gbd', 'qsub', 'bmi'))

# Set arguments
args <- commandArgs(trailingOnly = TRUE)
data_dir <- args[1]
lit_overweight_file <- args[2]
lit_obese_file <- args[3]
lit_underweight_file <- args[4]
lit_severe_underweight_file <- args[5]

# Load helper datasets
locs <- get_location_metadata(location_set_id = location_set_id, release_id=release_id)
# age meta data
ages <- get_age_map(type = "all")
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

# Population estimates
pops <-
  get_population(
    age_group_id = prod_age_ids, 
    location_id = unique(locs$location_id),
    year_id = prod_year_ids,
    sex_id = c(1:3),
    release_id=release_id
  )
pops <- merge(pops, ages[, .(age_group_id, age_start, age_end)], by = "age_group_id")

## logit conversion calculations from crosswalk functions
logit <- function(p) log(p/(1-p))
inv_logit <- function(mean) 1/(1+exp(-mean))

offset_val <- 1e-05

# Read in tracking sheet
tracking <- fread("FILEPATH/data_tracker.csv")

# crosswalk adjustment factors
xwalk <- list.files("FILEPATH", pattern="xwalk_prev|xwalk_prop", full.names=T)
xwalk <- xwalk[grep("2024-10-10", xwalk)]

for( meas in c("ow","ob","uw", "suw")){
  # Read in data
  if(meas=="ow") df <- read.xlsx(lit_overweight_file) %>% as.data.table()
  if(meas=="ob") df <- read.xlsx(lit_obese_file) %>% as.data.table()
  if(meas=="uw") df <- read.xlsx(lit_underweight_file) %>% as.data.table()
  if(meas=="suw") df <- read.xlsx(lit_severe_underweight_file) %>% as.data.table()
  
  ## remove report lines that are missing both sex and age
  df <- subset(df, !(age_start==999 & age_end==999 & sex=="Both"))
  
  ## remove report lines that are pre-1980
  df <- subset(df, year_id>=1980)
  
  # Find out the duplicated rows
  print(paste0("There are ", sum(duplicated(df[,c("age_group_id","sex","nid","year_start","location_name","location_id")])), 
               " rows of duplicate data in the overweight files. Removing them should change the number of rows from ",
               nrow(df), " to ", nrow(df)-sum(duplicated(df)), "."))
  
  ## Apply crosswalk
  
  ## a) Pull in MRBRT crosswalk results
  xwalks <- xwalk[grep(paste0("_",meas,"_"), xwalk)]
  model <- fread(xwalks[1])
  model2 <- fread(xwalks[2])
  
  model[sex_id==1, gamma := unique(model2$gamma[model2$sex_id==1 & !is.na(model2$gamma)])]
  model[sex_id==2, gamma := unique(model2$gamma[model2$sex_id==2 & !is.na(model2$gamma)])]
  
  ##Separate measured and self-report literature data
  split_meas <-  subset(as_split, diagnostic=="measured")
  split_sr <- subset(as_split, diagnostic=="self-report")
  split_sr <- merge(split_sr, unique(locs[,c("location_id","region_id","region_name","super_region_name")]), by="location_id", all.x=T)
  
  ## Combine stgpr ratio and individual level data -- use appropriate crosswalk model based on number of sources in dataset
  cw_reg <- merge(split_sr, model[,c("region_id", "age_group_id", "sex_id", "pred","pred_se","gamma")], 
                  by=c("region_id","age_group_id","sex_id"), all.x=T) 
  
  ## get cases without a match in the model
  unmatched <- cw_reg[is.na(pred)]
  if(nrow(unmatched)>0){
    ##Use neighboring age group
    unmatched[, ag_orig := age_group_id]
    unmatched[!ag_orig %in% c(6,34), age_group_id := ag_orig-1][ag_orig==30, age_group_id := 20][ag_orig==235, age_group_id :=  32]
    unmatched[ag_orig==6, age_group_id := 7][ag_orig==34, age_group_id := 6]
    cw_reg2 <- merge(unmatched[,-c("pred","pred_se")], model[,c("region_id", "age_group_id", "sex_id", "pred","pred_se")], 
                     by=c("region_id","age_group_id","sex_id"), all.x=T) 
    cw_reg2[,age_group_id:=ag_orig][, ag_orig:= NULL]
    
    cw_reg <- rbind(cw_reg[!is.na(pred)], cw_reg2, fill=T)
  }
  
  ##combine with measured to get all data
  df_comb <- rbind(split_meas, cw_reg,fill=T) %>% as.data.table()

  ## Create adjusted BMI for reported values
  ## convert mean to log and adjusting using beta value. return to linear
  df_comb[,val_logit := val][,se_logit := sqrt(variance)]
  offset_val <- 1e-05
  df_comb[val_logit <= 0, val_logit := offset_val][val_logit >=1, val_logit := 1-offset_val]
  df_comb[se_logit <= 0, se_logit := 0.001][(is.na(se_logit) | is.infinite(se_logit)) & sample_size<100, se_logit := 0.5][(is.na(se_logit) | is.infinite(se_logit)) & sample_size>=100, se_logit := 0.1]
  df_comb[, se_logit := se_logit/(val_logit*(1.0 - val_logit))][, val_logit := logit(val_logit)]
  df_comb[, val_logit_adj := ifelse(diagnostic == 'self-report', val_logit - pred, val_logit)]
  df_comb[, se_logit_adj := ifelse(diagnostic == 'self-report', sqrt(se_logit^2 + pred_se^2 + gamma), se_logit)]
  df_comb[diagnostic == 'self-report', val_adj := inv_logit(val_logit_adj)] 
  df_comb[diagnostic == 'self-report', se_adj := (exp(val_logit_adj)/(1.0 + exp(val_logit_adj))^2)*se_logit_adj] 
  df_comb[,val_original := val][diagnostic == 'self-report',val := val_adj]
  df_comb[,se_original := sqrt(variance)][diagnostic == 'self-report',standard_error := se_adj]
  df_comb[standard_error>1000, standard_error := sqrt(val *( 1 - val) / sample_size)]
  df_comb[, `:=` (val_logit = NULL, val_logit_adj = NULL, val_adj = NULL, se_logit = NULL, se_logit_adj = NULL, se_adj = NULL, pred = NULL)]
  df_comb[, variance_orig := variance][,variance := standard_error^2]
  
  summary(df_comb$val)
  summary(df_comb$val_original)
  
  df_comb[diagnostic == 'self-report', `:=` (lower = val - 1.96 * standard_error, upper = val + 1.96 * standard_error)]
  df_comb <- df_comb[, -c("pred", "pred_se","gamma")]
  
  ##Final cleaning
  df_comb[, measure := "proportion"]
  df_comb[val>1, val := 1]
  df_comb[variance<0, variance := 0.02] ## around median
  df_comb[is.infinite(variance) | is.na(variance), variance := 0.2] ## on higher range
  df_comb <- subset(df_comb, !is.na(val))
  
  # Save them
  if(meas=="ow") measure <- "prev_overweight"
  if(meas=="ob") measure <- "prop_obese"
  if(meas=="uw") measure <- "prev_underweight"
  if(meas=="suw")measure <- "prop_severe_underweight"

  if(!dir.exists(paste0(data_dir, measure, "/FILEPATH"))){
    dir.create(paste0(data_dir, measure, "/FILEPATH"))
    save_dir <- paste0(data_dir, measure, "/FILEPATH/")
  } else {
    save_dir <- paste0(data_dir, measure, "/FILEPATH/")
  }
  
  fwrite(df_comb, paste0(save_dir, "lit_sradjusted_", Sys.Date(), ".csv"))
    
}

