################
## Purpose: Sex Split Hearing Data
###############

#SETUP------------------------------------------------------------------------------------------
user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())
j <- "FILEPATH"
xwalk_temp <- paste0(j,"FILEPATH", user, "FILEPATH")
j_hwork <- paste0(j, "FILEPATH")

library(data.table)
library(openxlsx)
library(haven)
library(mlr)
install.packages("mlr",lib.loc = paste0(j, "FILEPATH", user, "FILEPATH"))  
pacman::p_load(data.table, openxlsx, ggplot2)
library(mortdb, lib = "FILEPATH")
library(msm, lib.loc = paste0(j, "FILEPATH"))
library(Hmisc, lib.loc = paste0(j, "FILEPATH"))
out_dir <- paste0(xwalk_temp,"FILEPATH")
source("FILEPATH")
source("FILEPATH")
#SEX MATCH FUNCTIONS------------------------------------------------------------------------------------------
raw_dt <- as.data.table(subset_bundle)
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]                                                   
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]                         
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt) ###were filled anyway
}
#
# ##calculate std based on uploader formulas
get_se <- function(raw_dt){                                                                   
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)     ###same as before, cols were already filled
}
calculate_cases_fromse <- function(raw_dt){                                                
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt) ###cases are already filled
}

#sex match function fixed for hearing
find_hsex_match <- function(dt){
   sex_dt <- copy(dt)
   sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]               
   match_vars <- c("age_start", "age_end", "location_id", "measure", "year_start", "year_end", "nid")      
   sex_dt[, match_n := .N, by = match_vars]          
   sex_dt <- sex_dt[match_n == 2 & mean != 0]       
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
   ratio_dt<- as.data.table(ratio_dt)
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



#GET SEX RATIOS------------------------------------------------------------------------------------------
full_fpath <- "FILEPATH"
dt <- data.table(read.xlsx(full_fpath))
dt<- as.data.table(subset_bundle)
dim(dt)
hsex_dt <- get_cases_sample_size(dt) 
hsex_dt <- get_se(hsex_dt)
hsex_matches <- find_hsex_match(hsex_dt) 
mrbrt_hsex_dt <- calc_sex_ratios(hsex_matches) 
mrbrt_hsex_dt <- mrbrt_hsex_dt[log_se != 0,] 
dim(mrbrt_hsex_dt)

#PLOT SEX RATIOS BY THRESH, AGE ------------------------------------------------------------------------------------------
plot_ratios <- function(dt) { 
   sex_age <- ggplot(mrbrt_hsex_dt, aes(x=midage, y=log_ratio)) +geom_point()
   print(sex_age)
   sex_thresh <- ggplot(mrbrt_hsex_dt, aes(x=log_ratio)) +
     geom_histogram(binwidth = 0.1, alpha = 0.5)
   print(sex_thresh)
   return(list(by_age = sex_age, by_thresh = sex_thresh))
 }
 plots <- plot_ratios(dt = mrbrt_hsex_dt)
 sex_by_age <- plots$by_age 
 sex_by_thresh <- plots$by_thresh #conclusion: covariate on threshes higher than 0_to_19

#UPDATE SEX DT BASED ON PLOT ANALYSIS ------------------------------------------------------------------------------------------

# #RUN MODEL, GET PREDICTIONS ------------------------------------------------------------------------------------------

hmodel_name <- paste0("tr_80_94_", date)

hsex_model <- run_mr_brt(
 output_dir = paste0(xwalk_temp,"FILEPATH"),
 model_label = hmodel_name, #update name
 data = mrbrt_hsex_dt,
 mean_var = "log_ratio",
 se_var = "log_se",
 study_id = "id",
 method = "trim_maxL",
 trim_pct = 0.15,
 max_iter = 400,
 #covs = cov_list,
 overwrite_previous = TRUE,
 remove_x_intercept = FALSE
)
hsex_model$model_coefs
exp(hsex_model$model_coefs[,"beta_soln"]) ###positive, .85
#if negative, means females are lower and that raw ratio is a decimal. this is what I expect
#if positive, means males are lower and that raw ratio is greater than 1.


results <- hsex_model
hsex_predicts <- predict_mr_brt(hsex_model, write_draws = T)
saveRDS(hsex_model, paste0(out_dir, hmodel_name,"/",hmodel_name, ".xlsx"))
saveRDS(hsex_predicts, paste0(out_dir, hmodel_name,"FILEPATH",hmodel_name, ".xlsx"))

#INSERT FUNCTIONS TO SPLIT DATA, GRAPH WITH AND EXTEND DATASET WITH
#SPLIT DATA ------------------------------------------------------------------------------------------

totadj_dt <- copy(dt)

#pull in split data function 
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
  pops_sub <- pop_dt[location_id == row[, location_id] & year_id == row[, midyear]]
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}

draws<- paste0("draw_", 0:999)

split_data <- function(dt, model, newdata){
  dt[ ,crosswalk_parent_seq := NA]   
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)] 
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)] 
  preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T) 
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]   
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws] 
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2) 
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)  
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)], sex_ids = 1:3)
  pops[age_group_years_end == 125, age_group_years_end := 99]
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops), mc.cores = 9)) 
  tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)] 
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]  
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL] 
  split_dt[ , crosswalk_parent_seq := seq] 
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

#returns total_dt and split_dt
predict_sex <- split_data(dt = totadj_dt, model = hsex_model) 

split <- predict_sex$graph
total <- predict_sex$final
final_dir <- paste0(hxwalk_temp, "FILEPATH")
write.xlsx(predict_sex$graph, file = paste0(final_dir, "FILEPATH"), sheetName = "FILEPATH")
write.xlsx(predict_sex$final, file = paste0(final_dir, "FILEPATH"), sheetName = "extraction")

#GRAPH SPLIT ------------------------------------------------------------------------------------------
graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])    
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
    ggtitle("20-34 Sex-Split Means Compared to Both-Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "purple", "Female" = "orange")) +
    theme_classic()
  return(list(plot = gg_sex, dt = graph_dt))
}

pdf(paste0(out_dir, hmodel_name, "FILEPATH"))
graph_predictions(dt = split)$plot
dev.off()

#CLEAN DATA TO SAVE XWALK VERSION
source("FILEPATH")
dismod_locs <- get_location_metadata(location_set_id = 9)$location_id
update_locs <- setdiff(total$location_id, dismod_locs)

location_split <- function(){
  loc_map <- unique(total[location_id %in% update_locs, c("nid", "location_id", "location_name", "sex", "year_start")])
  setnames(loc_map, "location_id", "parent_id")
  parent_locs <- unique(loc_map$location_id)
  child_locs <- loc_df[parent_id %in% parent_locs, c("location_id", "parent_id", "location_name")]
  chp_locs <- merge(loc_map, child_locs, by = "parent_id", allow.cartesian = TRUE)  
  loc_dt <- total[location_id %in% update_locs & group_review %in% c(1,NA)]
  noloc_dt <- total[!(location_id %in% update_locs) | group_review == 0]
  pops <- get_population(location_id = unique(chp_locs$location_id), location_set_id = 35,
                         year_id = 2010, gbd_round_id = 6, sex_id = c(1,2), decomp_step = "step2")
  pops[sex_id == 1, sex := "Male"]
  pops[sex_id == 2, sex := "Female"]
  pops[ ,c("age_group_id", "year_id", "run_id", "sex_id") := NULL]
  dt_pops <- merge(chp_locs, pops, by = c("location_id", "sex"))
  dt_pops[ ,pop_sum := sum(population), by = c( "parent_id","sex")]
  dt_pops[ ,samp_split := population/pop_sum]
  dt_pops[ ,sum_split := sum(samp_split), by = c("parent_id", "sex")]
  setnames(dt_pops, old = c("location_id", "location_name.y"), new=c("child_loc_id", "child_loc_name"))
  setnames(dt_pops, "parent_id", "location_id")
  dt_pops[ ,location_name.x := NULL]
  dt_pops[ ,sum_split := NULL]
  dt_pops[ ,pop_sum := NULL]
  dt_pops[ ,population := NULL]
  dt_pops <- unique(dt_pops, by= c("sex", "location_id", "nid", "year_start", "child_loc_id"))

  full_locs <- merge(loc_dt, dt_pops, by = c("sex", "nid", "location_id", "year_start"), allow.cartesian = TRUE)
  full_locs[ ,loc_seq := seq]
  full_locs[is.na(loc_seq), loc_seq := crosswalk_parent_seq]
  full_locs[ , `:=` (cases = NA, new_sample = samp_split * sample_size, crosswalk_parent_seq = loc_seq)]
  full_locs[ ,sample_size := new_sample]
  full_locs[ ,`:=` (location_name = child_loc_name, step2_location_year = "GBR level 3 split to L4, and regions split to L6 ", seq = NA)]
  noloc_dt[ ,step2_location_year := NA]
  extra_cols <- setdiff(names(full_locs), names(noloc_dt))

  full_locs[ ,c(extra_cols) := NULL]
  total_loc_dt <- rbind(full_locs, noloc_dt)
  return(total_loc_dt)
}

write.xlsx(total_loc_dt, file = paste0(hxwalk_temp, "FILEPATH"), sheetName = "extraction")


xversion_data <- total_loc_dt[sex != "Both", ]
xversion_data <- total_loc_dt[group_review %in% c(1,NA),] #irrelevant for pid
write.xlsx(xversion_data, file = paste0(hxwalk_temp, "FILEPATH"), sheetName = "extraction")

#SAVE CROSSWALK VERSION
save_hxwalk <- save_crosswalk_version(bundle_version_id = 9749, data_filepath = paste0(hxwalk_temp, "FILEPATH"), description = "Logit adjusted non-GBD threshold, sex split, locsplit L3 to L4, L5 to L6")


save_hxwalk


library(g)
