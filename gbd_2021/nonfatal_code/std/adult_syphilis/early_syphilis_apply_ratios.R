##############################################################################################333
#' Purpose: Sex-split, adjust, and age-split syphilis seroprevalence
################################################################################################

#SETUP-----------------------------------------------------------------------------------------------
#rm(list=ls())
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
user <- Sys.info()["user"]
source("FILEPATH")
library(mortdb, lib = "FILEPATH")
pacman::p_load(data.table, openxlsx, ggplot2, dplyr, writexl)

#SOURCE FUNCTIONS
source_shared_functions(functions = c("get_bundle_version", "save_crosswalk_version", "get_crosswalk_version",
                                      "save_bulk_outlier", "get_draws", "get_population", "get_location_metadata",
                                      "get_age_metadata", "get_ids"))

#ARGS & DIRS
cause <- "Early Syphilis Infection"
syphilis_dir <- "FILEPATH"
draws <- paste0("draw_", 0:999)

#GET & PREP DATA TO BE SEX-SPLIT
bundle_version <- 26528
database <- T
filepath <- F

sex_split_prep <- function(bvid, prepped_for_as_fpath){
  if (database == T){
    dt = get_bundle_version(bundle_version_id = bvid, fetch = "all", export = FALSE, transform = TRUE)
  } else {
    dt <-  data.table(read.xlsx(prepped_for_as_fpath))
  }

  print(paste0("There are ",nrow(dt), " rows in the dt."))
  print(paste0(nrow(dt[group_review ==0]), " group_review = 0 rows were removed."))
  dt <- dt[group_review %in% c(1,NA)]

  print(paste0("Now there are ",nrow(dt), " rows in the dt."))
  print(paste0("# of 'Both' sex rows, prevalence ", nrow(dt[sex == "Both" & measure == "prevalence"]) ))
  print(paste0("# of 'Both' sex rows, incidence: ", nrow(dt[sex == "Both" & measure == "incidence"]) ))


  dt[measure %in% c("prevalence", "proportion") & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  dt[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := sample_size * mean]


  return(dt)
}

bv_fpath <- NA
print(bv_fpath)
for_ss <- sex_split_prep(bvid = bundle_version, bv_fpath)

#READ IN SEX COEFFICIENTS
sex_model <- "no_covs_prev_only_2019_06_24"
sex_summaries <- data.table(read.csv("FILEPATH"))
sex_draws <- data.table(read.csv("FILEPATH"))

#SEX SPLIT HELPER FUNCTION
get_row <- function(n, dt, pop_dt){
  row_dt <- copy(dt)
  row <- row_dt[n]
  pops_sub <- pop_dt[location_id == row[, location_id] & year_id == row[, midyear]]
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}

#SPLIT THE DATA
split_data <- function(dt, ss_draws){
  dt[ ,crosswalk_parent_seq := NA]
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & !(measure %in% c("incidence", "prevalence")))]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review %in% c(1,NA)) & (measure %in% c("prevalence", "incidence"))]

  #check for correct separation
  print(nrow(nosplit_dt) + nrow(tosplit_dt) == nrow(dt))

  #add covariate columns
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]

  #format the draws dt
  pred_draws <- copy(ss_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]

  #estimate the ratios
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)

  #get sex weights (switch to the more generalizable one)
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)], sex_ids = 1:3)
  pops[age_group_years_end == 125, age_group_years_end := 99]

  #add sex weights to the tosplit_dt
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops), mc.cores = 9))
  tosplit_dt <- tosplit_dt[!is.na(both_N)] 
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]

  #apply ratios for sex-splitting
  split_dt <- merge(tosplit_dt, pred_draws, by = c("merge"), allow.cartesian = T) 
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  split_dt[ , crosswalk_parent_seq := seq] 
  split_dt[!is.na(crosswalk_parent_seq), seq := NA]

  #get & format male values
  male_dt <- copy(split_dt)
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  male_dt <- dplyr::select(male_dt, names(dt))

  #get & format female values
  female_dt <- copy(split_dt)
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = "", lower = "",
                    cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                          ratio_se, ")"))]
  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- dplyr::select(female_dt, names(dt))

  #combine the two
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))

  #total_dt is full data set with both-sex points now sex-specific points. crosswalk ratios should be applied to total dt
  #split_dt is only the both data points that have been split into male and female
  return(list(final = total_dt, graph = split_dt))
}

predict_sex <- split_data(dt = for_ss, ss_draws = sex_draws)

split <- predict_sex$graph
write_xlsx(split, path = "FILEPATH", col_names = TRUE,format_headers = FALSE)

total <- predict_sex$final
full_ss_fpath <- "FILEPATH"
write.xlsx(x = total, file = full_ss_fpath, col.names = TRUE)

#GRAPH THE SEX-SPLIT FOR VETTING
graphing_ss <- copy(split)

graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure %in% c("prevalence", "incidence"), .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error,nid)])
  graph_dt <- graph_dt[!is.na(mean),]
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
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    labs(x = "Both Sex Mean", y = "Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex-Split Means Compared to Both-Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "purple", "Female" = "orange")) +
    theme_classic()
  return(list(plot = gg_sex, dt = graph_dt))
}

ss_plot <- graph_predictions(dt = graphing_ss)$plot
ss_plot

#WRITE PDF TO FILE
pdf("FILEPATH")
ss_plot
dev.off()

################################################################
#APPLY CROSSWALK COEFFICIENTS###################################
################################################################

#READ IN CROSSWALK COEFFICIENTS
xwalk_model <- "step2_allcovs_notrim"
xwalk_summaries <- data.table(read.csv("FILEPATH"))
xwalk_draws <- data.table(read.csv("FILEPATH"))

#GET THE DATASET TO BE CROSSWALKED (should already be sex-split)
print(full_ss_fpath)
for_xwalk <- data.table(read.xlsx(full_ss_fpath))
for_xwalk[is.na(cv_blood_donor) ,cv_blood_donor := 0]
for_xwalk[is.na(cv_dx_nontreponly) ,cv_dx_nontreponly := 0]
for_xwalk[is.na(cv_treponemal_only) ,cv_treponemal_only := 0]
unique(for_xwalk[ ,c("cv_blood_donor", "cv_dx_nontreponly", "cv_treponemal_only")])

#GET ADJUSTMENT FACTORS FOR EACH UNIQUE COMBINATION OF COVARIATES
get_preds_adjustment <- function(adj_draws){

  pred_dt <- copy(xwalk_draws)
  pred_dt[, logadj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, logadj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept") := NULL]
  setnames(pred_dt, names(pred_dt), gsub("^X_", "", names(pred_dt)))

  pred_dt$adjust_for <- sapply(1:nrow(pred_dt), function(i){
    names(pred_dt[i])[which(pred_dt[i] == 1, arr.ind=T)[, "col"]]})
  pred_table <- copy(pred_dt)
  pred_table[ ,adjust_for := as.character(adjust_for)]
  return(pred_dt)
}

pred_table <- get_preds_adjustment(adj_draws = xwalk_draws)
pred_table

##APPLY RATIOS TO SEX-SPLIT DATASET
make_adjustment <- function(data_dt, ratio_dt){

  #get xwalking covs
  cvs <- names(ratio_dt)[grepl("^cv", names(ratio_dt))]

  #write in something later to ensure all cvs are filled in properly, some type of lapply get statement for each cov in cvs
  dt <- merge(data_dt, ratio_dt, by = cvs, all.x = T)

  #add in name of change here
  adjust_dt <- copy(dt[measure %in% c("prevalence") & !is.na(logadj) & mean != 0])
  noadjust_dt <- copy(dt[!(measure %in% c("prevalence")) | is.na(logadj) | mean == 0])
  noadjust_dt[, c("logadj", "logadj_se") := NULL]

  #check for correct separation
  print(nrow(noadjust_dt) + nrow(adjust_dt) == nrow(dt))

  ## ADJUST MEANS
  adjust_dt[, logmean := log(mean)]
  adjust_dt$log_se <- sapply(1:nrow(adjust_dt), function(i){ #
    mean_i <- adjust_dt[i, "mean"]
    se_i <- adjust_dt[i, "standard_error"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  adjust_dt[, logmean_adj := logmean - logadj]
  adjust_dt[, logmean_adj_se := sqrt(logadj_se^2 + log_se^2)]
  adjust_dt[, mean_adj := exp(logmean_adj)]
  adjust_dt$standard_error_adj <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- adjust_dt[i, "logmean_adj"]
    se_i <- adjust_dt[i, "logmean_adj_se"]
    deltamethod(~exp(x1), mean_i, se_i^2)
  })

  #format all crosswalked data
  full_dt <- copy(adjust_dt)

  full_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  full_dt[, `:=` (mean = mean_adj, standard_error = standard_error_adj, upper = "", lower = "", seq = "",
                  cases = "", sample_size = "", uncertainty_type_value = "", effective_sample_size = "",
                  note_modeler = paste0(note_modeler, " | crosswalked with log(ratio): ", round(logadj, 2), " (",
                                        round(logadj_se, 2), ")"))]
  extra_cols <- setdiff(names(full_dt), names(noadjust_dt))
  full_dt[, c(extra_cols) := NULL]
  final_dt <- rbind(noadjust_dt, full_dt, fill = T)
  return(list(epidb = final_dt, vetting_dt = adjust_dt))
}

#WRITE FULL XWALKED, SS DT TO FILE
#this dataset now needs to be agesplit
full_ss_xwalked_dt <- make_adjustment(data_dt = for_xwalk, ratio_dt = pred_table)$epidb
full_ss_xwalked_fpath <- "FILEPATH"
print(full_ss_xwalked_fpath)
write.xlsx(x = full_ss_xwalked_dt, file = full_ss_xwalked_fpath, col.names = TRUE)

#XWALKED DT ONLY
xwalked_only_dt <- make_adjustment(data_dt = for_xwalk, ratio_dt = pred_table)$vetting_dt

#SCATTER ADJUSTED DATAPOINTS
#some of this data, ofc, has also been sex-split

plotting_dt <- copy(xwalked_only_dt)
nrow(plotting_dt[mean_adj > 1 & measure %in% c("prevalence")])

pdf("FILEPATH")
ggplot(plotting_dt, aes(x=mean, y=mean_adj))  +
  #xlim(0,0.005)+ ylim(0,0.05) + #update
  geom_point(size=1.6, alpha = 0.6) + geom_abline(intercept = 0, slope = 1, color="black",
                                                  linetype="dashed", size=.5) +
  #facet_wrap(adjust_for) +
  labs(color="Case Definition(s)", x="Unadjusted Prevalence", y="Adjusted Prevalence", title = paste0(cause, ": Crosswalked Data, BV ", bundle_version))

dev.off()

################################################################
#AGE-SPLIT THE DATA#############################################
################################################################

#GET AGES
age_table <- get_ages()
print(age_table)

old_ages <- source("FILEPATH")$value
print(old_ages)
names(old_ages)
draws <- paste0("draw_", 0:999)

#PREP CROSSWALKED AND/OR SEX_SPLIT DT TO BE AGE_SPLIT
database <- F
filepath <- T

prep_for_split <- function(bvid, prepped_for_as_fpath){
  if (database == T){
    dt = get_bundle_version(bundle_version_id = bvid, fetch = "all", export = FALSE, transform = TRUE)
  } else {
    dt <- data.table(read.xlsx(prepped_for_as_fpath))
  }

  print(paste0("There are ",nrow(dt), " rows in the dt."))
  print(paste0("# of 'Both' sex rows: ", nrow(dt[sex == "Both"]) ))
  print(paste0("# of rows that need to be age-split: ", nrow(dt[age_end - age_start > 10 & measure == "prevalence"])))

  dt[measure %in% c("prevalence", "proportion") & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  dt[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := sample_size * mean]

  dt[ ,age_seq := seq] 
  dt[is.na(age_seq), age_seq := crosswalk_parent_seq] 
  dt[, id_seq := 1:nrow(dt)]
  return(dt)
}

prepped_for_as_fpath <- copy(full_ss_xwalked_fpath)
print(prepped_for_as_fpath)
for_agesplit <- prep_for_split(bvid = bundle_version, prepped_for_as_fpath)

#AGE-SPLITTING FUNCTION SPECIFICATIONS
## me_id = the modelable_entity_id of the cause you are splitting (find this in the drop-down menu of epi-viz)
## split_mvid = model_version_id of model to use for age-splitting (typically previous round's best MVID)
## gbd_round_id = the gbd round that your age_pattern_model will stem from
## age = what ages your cause applies to (ex. c(9:20, 30:33, 235))
## region_pattern = T/F - do you want to use the age_pattern from a particular region
## location_pattern_id = T/F - location/region id of the geographic are whose pattern you want to use

tot_xwalked_bundle  <- copy(for_agesplit)
me_id               <- 3948
gbd_round_id        <- 6
year_ids            <- 2019
age                 <- c(2:20, 30:32, 235)
location_pattern_id <- 100
measures <- c("prevalence")
print(measures)

age_split <- function(tot_xwalked_bundle, me_id, split_mvid, gbd_round_id, year_ids = 2019, age,
                      region_pattern, location_pattern_id, measures){

  print(paste0("getting data"))

  all_age <- copy(tot_xwalked_bundle)

  ## FORMAT DATA
  print(paste0("formatting data"))
  all_age <- all_age[measure %in% measures,]
  #unique(all_age[ ,c("group", "specificity", "group_review")])

  all_age <- all_age[group_review %in% c(1,NA),] 
  dim(all_age)

  all_age <- all_age[(age_end-age_start)>4,]
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
  all_age <- all_age[!cases==0,] ##don't want to split points with zero cases
  all_age_original <- copy(all_age)

  ## ROUND AGE GROUPS
  print(paste0("ROUND AGE GROUPS"))
  all_age_round <- copy(all_age)
  all_age_round[, age_start := age_start - age_start %%5]
  all_age_round[, age_end := age_end - age_end %%5 + 4]
  all_age_round <- all_age_round[age_end > 99, age_end := 99]

  ## EXPAND FOR AGE
  print(paste0("EXPAND FOR AGE"))
  all_age_round[, n.age:=(age_end+1 - age_start)/5]
  all_age_round[, age_start_floor:=age_start]
  all_age_round[, drop := cases/n.age] ##drop the data points if cases/n.age is less than 1 
  all_age_round <- all_age_round[!drop<1,]
  seqs_to_split <- all_age_round[, unique(id_seq)] #changed from seq to id seq
  all_age_parents <- all_age_original[id_seq %in% seqs_to_split] 
  expanded <- rep(all_age_round$id_seq, all_age_round$n.age) %>% data.table("id_seq" = .) 
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
  all_age_total <- merge(split, old_ages, by = c("age_start", "age_end"), all.x = T)
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
  print(me_id)

  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = me_id,
                           measure_id = 5, location_id = locations, source = "epi",
                           status = "best", sex_id = c(1,2), gbd_round_id = 6,
                           age_group_id = age, year_id = 2010, decomp_step = "step4") ##imposing age pattern

  global_population <- as.data.table(get_population(location_id = locations, year_id = 2010, sex_id = c(1,2),
                                                    age_group_id = age, gbd_round_id =  6, decomp_step = "step4"))
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
                                              sex_id =c(1,2), age_group_id = age, gbd_round_id =  6, decomp_step = "step4")) 
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
  total_age[, sample_size := (population / total_pop) * sample_size]
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
  total_age[, group := 1]
  total_age[, group_review := 1]

  blank_vars <- c("lower", "upper", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", "standard_error", "cases") 
  total_age[ ,(blank_vars) := NA] 
  total_age[is.na(crosswalk_parent_seq),`:=` (crosswalk_parent_seq = age_seq, seq = NA)]
  unique(total_age$seq) 

  ## ADD PARENTS
  #all_age_parents[, crosswalk_parent_seq := NA]
  all_age_parents[ ,measure_id := NULL]
  all_age_parents[, group_review := 0]
  all_age_parents[, group := 1]
  all_age_parents[, specificity := paste0(specificity, ", age-split parent")]

  total_age[, setdiff(names(total_age), names(all_age_parents)) := NULL] ## make columns the same
  total <- rbind(all_age_parents, total_age)
  nrow(total[mean > 1])
  total <- total[mean > 1, group_review := 0]
  total$sex_id <- NULL
  total$year_id <- NULL
}

sub_txb <- tot_xwalked_bundle[!(id_seq %in% seqs_to_split)] #subset to the original data points that didn't need to be age split
txb_as_upload <- rbind(sub_txb, total, fill = TRUE)
dim(txb_as_upload)

txb_as_upload








