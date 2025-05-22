################################################################################################
##Adjust data 
################################################################################################

user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())
j <- "FILEPATH"
h_root <- "FILEPATH"
j_work <- "FILEPATH"
xwalk_temp <- "FILEPATH"
cause <- "herpes"
out_dir <- "FILEPATH"
out_dir
#SET UP-------------------------------------------------------------------------------------------------------------------
source(paste0(h_root,"FILEPATH"))
source("FILEPATH")
source("FILEPATH")
source_shared_functions("get_bundle_version")
# plot_dir<-paste0(output_dir, "plots/")
# if(!dir.exists(plot_dir)){dir.create(plot_dir,recursive = T)}
require(data.table)
require(stringr)
require(magrittr)
require(assertthat)

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))

library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(openxlsx)
library(metafor)

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortdb, lib = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(Hmisc, lib.loc = paste0(j, "FILEPATH"))

library(ggplot2)
library(ggridges, lib = "FILEPATH")
source("FILEPATH")
draws <- paste0("draw_", 0:999)


#PREP INPUT DATA -------------------------------------------------------------------------------------------------------------------
get_data <- function(fpath){
  dat_original <- data.table(read_excel(path = fpath, guess_max = 1048576))
  dt <- dat_original[!(is.na(nid)), ]
  dt_prev <- dt[measure == "prevalence",]
  dt_prev <- dt_prev[sex != "Both", ]
  return(list(dt = dt, dt_prev = dt_prev))
}

get_db_data <- function(bvid){
  data <- get_bundle_version(bvid, export = FALSE, transform = TRUE)
  dt_prev <- data[sex != "Both" & measure == "prevalence"]
  return(list(dt = data, dt_prev = dt_prev))
}

#cv_keep <- c("culture", "other", "pcr") #update
## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

#GET SEX RATIOS
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence")]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end", cv_keep) #, cv_keep
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean","standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean, na.rm = TRUE) 
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid", "location_id")]
  return(sex_dt)
}
calc_sex_ratios <- function(dt){
  ratio_dt <- copy(dt)
  ratio_dt[, midage := (age_start + age_end)/2]
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  ratio_dt[, log_ratio := log(ratio)]
  ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) {
    mean_i <- ratio_dt[i, "ratio"]
    se_i <- ratio_dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  ratio_dt <- ratio_dt[!is.nan(ratio_se), ]
  return(ratio_dt)
}

#PREP DATA FOR SEX RUN ----------------------------------------------------------------------------------------------------
# bundle_data <- get_data(fpath = "FILEPATH")) #use if pulling from flat file
# bundle_prev <- bundle_data$dt_prev

db_data <- get_db_data(bvid = #) #use if pulling from epi database 

sex_prep <- function(df){
  sex_dt <- get_cases_sample_size(df)
  sex_dt <- get_se(sex_dt)
  sex_dt <- calculate_cases_fromse(sex_dt)
  sex_matches <- find_sex_match(sex_dt)
  mrbrt_sex_dt <- calc_sex_ratios(sex_matches)
  mrbrt_sex_dt <- mrbrt_sex_dt[log_se != 0,]
  return(mrbrt_sex_dt)
}

run_sex_dt <- sex_prep(df = db_data$dt_prev)

covs <-  c(cv_keep)
cov_list <- lapply(cv_keep, function(x) cov_info(x, "X"))
#PLOT INPUT DATA----------------------------------------------------------------------------------------------------------------------------
plot_ratios <- function(dt) { #update xform_val to log ratio and so on
  loc_data <- get_location_metadata(location_set_id = 9, gbd_round_id = 6) #id 9 is dismod/epi data
  loc_data_sr <- loc_data[ ,c("location_id", "super_region_id", "super_region_name", "location_name")]
  run_sex_plot <- merge(dt, loc_data_sr, all.x = TRUE, by = "location_id")
  sex_age <- ggplot(run_sex_plot, aes(x=age_start, y=log_ratio)) +geom_point() +ylim(-5,5)
  print(sex_age) 
  sex_hist <- ggplot(run_sex_plot, aes(x=log_ratio, fill = super_region_name)) + facet_wrap(~super_region_name) +
  geom_histogram(binwidth = 0.1, alpha = 0.5)
  print(sex_hist)
  sex_nid <- ggplot(run_sex_plot, aes(x=nid, y=log_ratio)) +geom_point()
  return(list(by_age = sex_age, by_sr = sex_hist, by_nid = sex_nid))
}
plots <- plot_ratios(dt = run_sex_dt)
sex_by_age <- plots$by_age
sex_by_sr <- plots$by_sr


#RUN MRBRT SEX MODEL & BETA PREDICTIONS ----------------------------------------------------------------------------------------------------
model_name <- #insert name

sex_model <- run_mr_brt(
  output_dir = out_dir,
  model_label = model_name,
  data = run_sex_dt,
  mean_var = "log_ratio",
  se_var = "log_se",
  study_id = "id", #changes the way variance is calculated
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = TRUE, max_iter = 400
)

plot_mr_brt(sex_model)

sex_predictions <- predict_mr_brt(sex_model, write_draws = T)
results <- sex_model
predicts <- sex_predictions
saveRDS(sex_model, "FILEPATH")

#READ IN AND USE THE BEST MODEL----------------------------------------------------------------------------------------------------
model_love <- #insert name
read_sex_model <- readRDS( paste0(out_dir, model_love,"/",model_love, ".xlsx"))

#GET METADATA FOR ACTUAL SPLIT -----------------------------------------------------------------------------------------------------------------

age_dt <- get_age_metadata(12)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]

get_closest_age <- function(i, var, start = T, dt){
  age <- dt[i, get(var)]
  if (start == T){
    age_dt[, age_group_years_start][which.min(abs(age_dt[, age_group_years_start] - age))]
  } else if (start == F){
    age_dt[, age_group_years_end][which.min(abs(age_dt[, age_group_years_end] - age))]
  }
}

get_row <- function(n, dt, pop_dt){
  row_dt <- copy(dt)
  row <- row_dt[n]
  # pops_sub <- pop_dt[location_id == row[, location_id] & year_id == row[, midyear] &
  #                      age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]]
  pops_sub <- pop_dt[location_id == row[, location_id] & year_id == row[, midyear]]
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}

split_data <- function(dt){
  dt[ ,crosswalk_parent_seq := NA]
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0) | (sex == "Both" & (group_review == 1 | is.na(group_review)) & !measure %in% c("incidence", "prevalence"))]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review %in% c(1,NA)) & measure %in% c("prevalence", "incidence")]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  tosplit_dt[ ,X_midage := age_start]
  #preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1),write_draws = T) #change newdata depending on if running with covs or not
  #pred_draws <- as.data.table(preds$model_draws)
  #pred_draws <- data.table(read.csv(paste0(out_dir, "FILEPATH"))) #if transferring to step 2
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  # pops <- get_population(location_id = tosplit_dt[, unique(location_id)], sex_id = 1:3, decomp_step = "step2",
  #                        year_id = tosplit_dt[, unique(midyear)], single_year_age = T, status ="latest")
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)], sex_ids = 1:3)
  pops[age_group_years_end == 125, age_group_years_end := 99]
  #tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops), mc.cores = 9))
  tosplit_dt <- tosplit_dt[!is.na(both_N)] 
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = c("merge", "X_midage"), allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  split_dt[ , crosswalk_parent_seq := seq] #added this for xwalk version upload purposes
  split_dt[!is.na(crosswalk_parent_seq), seq := NA]
  male_dt <- copy(split_dt)
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  male_dt <- dplyr::select(male_dt, names(dt))
  female_dt <- copy(split_dt)
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = "", lower = "",
                    cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                          ratio_se, ")"))]
  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- dplyr::select(female_dt, names(dt))
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(list(final = total_dt, graph = split_dt))
}


predict_sex <- split_data(dt = db_data$dt) #update depending on if measure being estimated

split <- predict_sex$graph
total <- predict_sex$final
View(split)
View(total)


write.xlsx(split, file = paste0(out_dir, "split_dt.xlsx"))
write.xlsx(total, file = paste0(out_dir, "total_dt.xlsx"))

graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure %in% c("prevalence", "incidence"), .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error,nid)])
  #graph_dt <- graph_dt[!is.na(mean),] 
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = TRUE)
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt <-graph_dt[!is.nan(N),] 
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  #ages <- c(60, 70, 80, 90)
  #graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    #facet_wrap(~super_region_name) +
    labs(x = "Both Sex Mean", y = "Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex-Split Means Compared to Both-Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "purple", "Female" = "orange")) +
    theme_classic()
  return(list(plot = gg_sex, dt = graph_dt))
}


#LEAVE IN THIS ORDER
pdf(paste0(out_dir, "FILEPATH"))
graph_predictions(dt = split)$plot
dev.off()

###########################################################
#AGE SPLITTING CODE FOR NONFATAL MODELS

#FUNCTION SPECIFICATIONS
## split_mvid = model_version_id of model to use for age-splitting (typically previous round's best MVID)
## gbd_round_id = the gbd round that your age_pattern_model will stem from
## age = what ages your cause applies to (ex. c(9:20, 30:33, 235))
## location_pattern_id = location/region id of the geographic are whose pattern you want to use
###########################################################

#SETUP------------------------------------------------------------------------------------------------------
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())
pkg_lib <- "FILEPATH"

##SOURCE FUNCTIONS-------------------------------------------------------------------------------------------
source("FILEPATH")
source_shared_functions(functions = c("get_draws", "get_population", "get_location_metadata",
                                      "get_age_metadata", "get_ids", "get_crosswalk_version",
                                      "save_crosswalk_version", "get_best_model_versions"))
pacman::p_load(data.table, openxlsx, ggplot2, magrittr, tidyr)


##ARGS & DIRS--------------------------------------------------------------------------------------------------
cause_name <- "herpes"
bundle_id <- #insert bundleID
#bundle_version <- #insert bvid
draws <- paste0("draw_", 0:999)

temp_dir <- "FILEPATH"
xwalk_temp <- "FILEPATH"
out_dir <- "FILEPATH"

#get age tables
gbd20_ages <- get_ages()
gbd19_ages <- source("FILEPATH")$value
print(gbd19_ages)

#PREP CROSSWALKED AND/OR SEX_SPLIT DT TO BE AGE_SPLIT--------------------------------------------------------------------------------------------------
database <- F
filepath <- T
memory <- F

prep_for_split <- function(bvid, to_as_fpath, dt_in_memory){
  if (database == T){
    dt = get_bundle_version(bundle_version_id = bvid, fetch = "all", export = FALSE, transform = TRUE)
  } else if (filepath == TRUE) {
    dt <-  data.table(read.xlsx(to_as_fpath))
  } else if (memory == TRUE){
    dt <- copy(dt_in_memory)
  }

  print(paste0("There are ",nrow(dt[measure == "prevalence"]), " prevalence rows in the dt."))
  print(paste0("# of 'Both' sex rows: ", nrow(dt[sex == "Both" & measure == "prevalence"])))
  print(paste0("# of rows that need to be age-split: ", nrow(dt[(age_end - age_start > 5) & measure == "prevalence"])))

  dt[measure %in% c("prevalence", "proportion") & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  dt[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := as.numeric(sample_size) * as.numeric(mean)]
  #set it up so that seqs will line up correctly
  dt[ ,age_seq := seq]

  dt[is.na(age_seq), age_seq := crosswalk_parent_seq]
  dt[, id_seq := 1:nrow(dt)]
  return(dt)
}

for_AS_fpath <- paste0("FILEPATH")
print(for_AS_fpath)
prepped_dt <- prep_for_split(bvid = bundle_version, to_as_fpath = for_AS_fpath, dt_in_memory = full_bv_xwalked)


#SPECIFY LAST COUPLE OF PARAMETERS------------------------------------------------------------------------------------------------------------------------
dt_to_agesplit  <- copy(prepped_dt)
gbd_round_id        <- c(6)
age                 <- c(2:20, 30:32, 235)

#BEGIN TO RUN THE FUNCTION---------------------------------------------------------------------------------------------------------------------------------
age_split <- function(split_meid, gbd_round_id = 6, year_id = 2010, age,
                          location_pattern_id, measures = c("prevalence", "incidence"), measure_ids = c(5,6)){

  print(paste0("getting data"))
  all_age <- copy(dt_to_agesplit)

  ## FORMAT DATA
  print(paste0("formatting data"))
  all_age <- all_age[measure %in% measures,]

  tri_cols <- c("group", "specificity", "group_review")
  if (tri_cols[1] %in% names(all_age)){
    all_age <- all_age[group_review %in% c(1,NA),] 
  } else {
    all_age[ ,`:=` (group = as.numeric(), specificity = "", group_review = as.numeric())]
  }

  all_age <- all_age[(age_end-age_start)>5,]
  all_age <- all_age[!mean ==0, ] ##don't split points with zero prevalence
  all_age[sex=="Female", sex_id := 2]
  all_age[sex=="Male", sex_id :=1]
  all_age[, sex_id := as.integer(sex_id)]
  all_age[measure == "proportion", measure_id := 18]
  all_age[measure == "prevalence", measure_id := 5]
  all_age[measure == "incidence", measure_id := 6]
  all_age[, year_id := year_start] 

  ## CALC CASES AND SAMPLE SIZE
  print(paste0("CALC CASES AND SAMPLE SIZE"))
  all_age <- all_age[cases!=0,] ##don't want to split points with zero cases
  all_age_original <- copy(all_age)

  ## ROUND AGE GROUPS
  print(paste0("ROUND AGE GROUPS"))
  all_age_round <- copy(all_age)
  all_age_round[, age_start := age_start - age_start %%5]
  all_age_round[, age_end := age_end - age_end %%5 + 4]
  all_age_round <- all_age_round[age_end > 99, age_end := 99]

  ## EXPAND FOR AGE
  print(paste0("EXPAND FOR AGE"))
  all_age_round[, n_age:=(age_end+1 - age_start)/5]
  all_age_round[, age_start_floor:=age_start]
  all_age_round[ ,cases := as.numeric(cases)]
  all_age_round[, drop := cases/n_age] ##drop the data points if cases/n_age is less than 1 
  all_age_round <- all_age_round[!drop<1,]
  seqs_to_split <- all_age_round[, unique(id_seq)] #changed from seq to id seq
  all_age_parents <- all_age_original[id_seq %in% seqs_to_split] ##keep copy of parents to attach on later (difference here would be the n/cases thing)
  expanded <- rep(all_age_round$id_seq, all_age_round$n_age) %>% data.table("id_seq" = .) #duplicate rows by the number of groups for the given age
  split <- merge(expanded, all_age_round, by="id_seq", all=T)
  split[,age.rep:= (1:.N - 1), by =.(id_seq)]
  split[,age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4 ]

  ## GET SUPER REGION INFO
  print("getting super regions")
  super_region <- get_location_metadata(location_set_id = 22, gbd_round_id = 6) #change round depending on if you have an age pattern model or not
  super_region <- super_region[, .(location_id, super_region_id)]
  split <- merge(split, super_region, by = "location_id")
  super_regions <- unique(split$super_region_id) ##get super regions for dismod results

  ## GET AGE GROUPS #THIS IS WHERE YOU REMERGE
  all_age_total <- merge(split, gbd19_ages, by = c("age_start", "age_end"), all.x = T)
  all_age_total <- data.table(all_age_total)

  ## CREATE AGE GROUP ID 1
  all_age_total[age_start == 0 & age_end == 4, age_group_id := 1] 
  all_age_total <- all_age_total[age_group_id %in% age] ##don't keep where age group id isn't estimated for cause

  ##GET LOCS AND POPS
  pop_locs <- unique(all_age_total$location_id)
  pop_years <- unique(all_age_total$year_id)

  ## GET AGE PATTERN
  print("GET AGE PATTERN")

  locations <- location_pattern_id
  print("getting age pattern")
  draws <- paste0("draw_", 0:999)
  print(split_meid)

  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = split_meid,
                           measure_id = measure_ids, location_id = locations, source = "epi",
                           status = "best", sex_id = unique(all_age$sex_id),
                           age_group_id = age, year_id = 2010) ##imposing age pattern

  global_population <- as.data.table(get_population(location_id = locations, year_id = 2010, sex_id = 2,
                                                age_group_id = age))
  global_population <- global_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

  print("formatting age pattern")

  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5,6)]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, global_population, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5,6)]
  age_pattern <- rbind(age_pattern, age_1)

  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id %in% c(5, 18), sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  age_pattern[is.nan(cases_us), cases_us := 0]

  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, rate_dis := cases_us/sample_size_us]
  sex_3[measure_id %in% c(5, 18), se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
  sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
  sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
  sex_3[is.nan(se_dismod), se_dismod := 0]
  sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
  sex_3[, sex_id := 3]
  age_pattern <- rbind(age_pattern, sex_3)

  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]

  ## MERGE AGE PATTERN
  age_pattern1 <- copy(age_pattern)
  all_age_total <- merge(all_age_total, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))

  ## GET POPULATION INFO
  print("getting populations for age structure")
  populations <- as.data.table(get_population(location_id = pop_locs, year_id = pop_years,
                                              sex_id =unique(all_age$sex_id), age_group_id = age))
  age_1 <- copy(populations) 
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5,6)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5,6)]
  populations <- rbind(populations, age_1)  
  total_age <- merge(all_age_total, populations, by = c("location_id", "sex_id", "year_id", "age_group_id"))

  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("creating new age split points")
  total_age[, total_pop := sum(population), by = "id_seq"]
  total_age[, sample_size := (as.numeric(population) / as.numeric(total_pop)) * as.numeric(sample_size)]
  total_age[, cases_dis := sample_size * rate_dis]
  total_age[, total_cases_dis := sum(cases_dis), by = "id_seq"]
  total_age[, total_sample_size := sum(sample_size), by = "id_seq"]
  total_age[, all_age_rate := total_cases_dis/total_sample_size]
  total_age[, ratio := mean / all_age_rate]
  total_age[, mean := ratio * rate_dis]
  total_age[, cases := sample_size * mean] 

  ######################################################################################################
  ## FORMATTING
  total_age[, specificity := paste0(specificity, ", age-split child")]
  total_age[ ,specificity := "age-split child"]
  total_age[, group := 1]
  total_age[, group_review := 1]

  blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "standard_error", "cases") 
  total_age[ ,(blank_vars) := NA] 
  total_age[, crosswalk_parent_seq := age_seq]
  total_age[ , seq := NA]

  ## ADD PARENTS
  all_age_parents[, crosswalk_parent_seq := NA]
  all_age_parents[ ,measure_id := NULL]
  all_age_parents[, group_review := 0]
  all_age_parents[, group := 1]
  all_age_parents[, specificity := paste0(specificity, ", age-split parent")]
  all_age_parents[ ,specificity := "age-split parent"]

  #MORE FORMATTING
  total_age[, setdiff(names(total_age), names(all_age_parents)) := NULL] ## make columns the same
  total <- rbind(all_age_parents, total_age)
  total <- total[mean > 1, group_review := 0]
  total$sex_id <- NULL
  total$year_id <- NULL

  #RBIND TO THE NON AGE-SPLIT DATA POINTS
  non_split <- dt_to_agesplit[!(id_seq %in% seqs_to_split)]
  all_points <- rbind(non_split, total, fill = TRUE)
  print("finished.")
  return(all_points)
}
agesplit_data <- age_split(split_meid = split_meid, year_id = 2010, age, location_pattern_id = location_pattern_id, measures = c("prevalence", "incidence"), measure_ids = c(5.6))


agesplit_data

