
user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILENAME", user, "FILENAME")

install.packages("data.table",lib=path)

library(dplyr)
library(tidyr)

rm(list = ls())

os <- .Platform$OS.type
if (os == "FILENAME") {
  prefix <- "FILENAME"
} else {
  prefix <- "FILENAME"
}		

#load shared functions 

source(sprintf("FILENAME",prefix))
source(sprintf("FILENAME",prefix))
source(sprintf("FILENAME",prefix))
source(sprintf("FILENAME",prefix))
source(sprintf("FILENAME",prefix))
source(sprintf("FILENAME",prefix))
source("FILENAME")

### INPUT DATA 
dt<-read.csv("FILENAME")

#remove records with no #tested or mean
dt2<-dt[!is.na(dt$mean) & !is.na(dt$cases),]


#drop is_outlier =1
dt2<-dt2[dt2$is_outlier==0,]

#dataset for splitting here
input_data_bv_data<-dt2[!is.na(dt2$sample_size),]

# Helper Function

draw_summaries <- function(x, new_col, cols_sum, se = F) {
  
  x[, (paste0(new_col, "_lower")) := apply(.SD, 1, quantile, probs = .025, na.rm =T), .SDcols = (cols_sum)]
  x[, (paste0(new_col, "_mean"))  := rowMeans(.SD), .SDcols = (cols_sum)]
  x[, (paste0(new_col, "_upper")) := apply(.SD, 1, quantile, probs = .975, na.rm =T), .SDcols = (cols_sum)]
  if (se == T) x[, paste0(new_col, "_se") := (get(paste0(new_col, "_upper"))-get(paste0(new_col, "_lower")))/3.92]
  if (se == T) x[get(paste0(new_col, "_se")) == 0, paste0(new_col, "_se") := apply(.SD, 1, sd, na.rm=T), .SDcols = (cols_sum)]
  x <- x[, !cols_sum, with = F]
  return(x)
  
}

add_population_cols <- function(data, gbd_round, decomp_step){
  
  if (!("sex_id" %in% names(data))) { data[, sex_id := ifelse(sex == "Male", 1, ifelse(sex == "Female", 2, 3))]}
  
  cat("Returning data with columns of population by sex aggregated over custom age bin for floor of average of year_start and year_end; update to full year for more robustness")
  
  data[, year_id := floor((year_end + year_start)/2)]
  
  sexes <- 1:3
  locs  <- unique(data[, location_id])
  years <- unique(data[, year_id])
  
  age_dt <- fread("FILENAME")
  
  ages_over_1 <- age_dt[age_start >=1 , age_group_id]
  under_1     <- age_dt[age_start <1, age_group_id]
  over_95     <- age_dt[age_start >=95 , age_group_id]
  
  age_dt[ , age_group_name:=NULL]
  age_dt[age_start<1 , age_group_id:=0]
  age_dt[age_start<1 , age_end:=0]
  age_dt[age_start<1 , age_start:=0]
  age_dt<-unique(age_dt)
  
  # Pull population all but under 1 and over 95
  
  pop <- get_population(age_group_id = ages_over_1,
                        single_year_age = TRUE,
                        location_id = locs,
                        year_id = years,
                        sex_id = sexes,
                        gbd_round_id = gbd_round,
                        decomp_step = decomp_step,
                        status = "best")
  
  pop[,run_id:=NULL]
  
  # Pull pop over 95
  
  pop_over_95 <- get_population(age_group_id = 235,
                                location_id = locs,
                                year_id = years,
                                sex_id = sexes,
                                gbd_round_id = gbd_round,
                                decomp_step = decomp_step,
                                status = "best")
  
  pop_over_95[, run_id:=NULL]
  pop_over_95[, population:=population/5]
  pop_over_95_template<-expand.grid(age_group_id=age_dt[age_start>=95,age_group_id],sex_id=sexes,location_id=locs,year_id=years)
  pop_over_95[, age_group_id:=NULL]
  pop_over_95 <- merge(pop_over_95, pop_over_95_template,by=c("sex_id","location_id","year_id"))
  
  # Pull pop under 1
  
  pop_under_1 <- get_population(age_group_id = under_1,
                                location_id = locs,
                                year_id = years,
                                sex_id = sexes,
                                gbd_round_id = gbd_round,
                                decomp_step = decomp_step,
                                status = "best")
  
  pop_under_1 <- pop_under_1[,.(population=sum(population)),by=c("location_id","year_id","sex_id")]
  pop_under_1[,age_group_id:=0]
  
  pops <- rbindlist(list(pop, pop_under_1, pop_over_95), use.names = T)
  pops <- dcast.data.table(pops, ... ~ sex_id, value.var = "population")
  setnames(pops, c("1", "2", "3"), c("male_population", "female_population", "both_population"))
  
  # split out data 
  
  data[, split_seq:=1:.N]
  data_ss <- copy(data)
  data_ss[, n := (age_end+1 - age_start)]
  expanded <- data.table("split_seq" = rep(data_ss[,split_seq], data_ss[,n]))
  data_ss <- merge(expanded, data_ss, by="split_seq", all=T)
  data_ss[,age_rep:= 1:.N - 1, by =.(split_seq)]
  data_ss[,age_start:= age_start+age_rep]
  data_ss[,age_end:=  age_start]
  
  # merge on age group ids and pops
  
  cat("at mergeing data tables in pop")
  
  data_ss <- merge(data_ss, age_dt, by=c("age_start","age_end"))
  ### 
  data_ss <- merge(data_ss, pops, by = c("location_id","year_id","age_group_id"),all.x=T)
  
  data_ss <- data_ss[, .("male_population" = sum(male_population),
                         "female_population" = sum(female_population),
                         "both_population" = sum(both_population)),
                     by = "split_seq"]
  
  data <- merge(data, data_ss, by="split_seq", all=T)
  
  cat("past all merges")
  
  data[, split_seq := NULL]
  
  return(data)
  
}

#pull in data to be split

###DEFINE ARGUMENTS###
 mr_brt_fit_obj <- fit1
 decomp_step <- "iterative"
 all_data <- as.data.table(input_data_bv_data)

 source(sprintf("FILENAME",prefix))
 
apply_sex_crosswalk <- function(mr_brt_fit_obj, all_data, decomp_step){
  
  all_data[, crosswalk_parent_seq := seq]
  
  cat("\n\n")
  
  # set what data should be crosswalked
  
  all_data[, sex_split := ifelse(sex == "Both", 1, 0)]
  
  # pre-format
  
  if (!("sex_id" %in% names(all_data))) { all_data[, sex_id := ifelse(sex == "Male", 1, ifelse(sex == "Female", 2, 3))]}
  orig_df <- copy(all_data)
  unadj_df <- all_data[sex_split == 1]
  
  # get fit from mr-brt object
  
  pred1 <- predict_mr_brt(mr_brt_fit_obj, newdata = unadj_df, write_draws = TRUE)
  
  cat("\nChecking if prediction outputs made -- if not there is an error in the fit.\n\n")
  
  check_for_preds(pred1)
  pred_object <- load_mr_brt_preds(pred1)
  
  # predict mr-brt draws
  
  preds <- as.data.table(pred_object$model_summaries)
  draws <- as.data.table(pred_object$model_draws)
  
  # merge predictions to data to be split
  
  setnames(draws, paste0("draw_", 0:999), paste0("ratio_", 0:999))
  draws   <- draws[, .SD, .SDcols = paste0("ratio_", 0:999)]
  unadj_df <- cbind(unadj_df, draws)
  
  # prepare to split data -- need population to get female, male, both populations
  # calling custom function
  
  cat("\nCalling custom add population function.\n\n")
  
  unadj_df <- add_population_cols(data = unadj_df, gbd_round = 6, decomp_step = decomp_step)
  unadj_df[, ratio_m_f := male_population / female_population]
  
  # exponentiate predicted draws
  
  unadj_df[, paste0("ratio_", 0:999) := lapply(0:999, function(x) exp(get(paste0("ratio_", x))))]
  
  # get 1,000 "draws" of prevalence in log space
  
  cat("\n\nInput data of 0 prevalence is split into male and female rows with 0's and the points orginal standard error -- not robust\n")
  
  unadj_df_0 <- unadj_df[mean == 0]
  unadj_df   <- unadj_df[mean > 0]
  
  cat("\n To incorporate standard error of point, use mean and standard error as parameters in binomial distribution to make 1,000 'draws' of prevalence to apply the 1,000 MR-BRT draws\n")

  unadj_df[, paste0("both_prev_", 0:999) := lapply(0:999, function(x){
    rbinom(n = nrow(unadj_df), size = round(sample_size), prob = mean) / round(sample_size)
  })]

  # sex-split at 1000 draw-level
  
  unadj_df[, paste0("prev_female_", 0:999) := lapply(0:999, function(x) get(paste0("both_prev_", x))*((both_population)/(female_population+get(paste0("ratio_", x))*male_population)))]
  unadj_df[, paste0("prev_male_", 0:999)   := lapply(0:999, function(x) get(paste0("ratio_", x))*get(paste0("prev_female_", x)))]
  unadj_df <- draw_summaries(unadj_df, "male",   paste0("prev_male_", 0:999), T)
  unadj_df <- draw_summaries(unadj_df, "female", paste0("prev_female_", 0:999), T)
  
  # clean - add 0's, separate tables, bind original sex-spec with sex-split
  
  cols_rem <- names(unadj_df)[names(unadj_df) %like% "ratio_|prev_"]
  unadj_df <- unadj_df[, .SD, .SDcols = -cols_rem]
  
  # add 0's back in
  
  cols_rem <- names(unadj_df_0)[names(unadj_df_0) %like% "ratio_|prev_"]
  unadj_df_0 <- unadj_df_0[, .SD, .SDcols = -cols_rem]
  
  unadj_df_0[, ":="(male_lower = lower,
                    female_lower = lower,
                    male_mean = mean,
                    female_mean = mean,
                    female_upper = upper,
                    male_upper = upper,
                    male_se = standard_error,
                    female_se = standard_error)]
  
  unadj_df <- rbind(unadj_df, unadj_df_0)
  
  # male table
  
  male <- copy(unadj_df)
  male <- male[, .SD, .SDcols = -names(unadj_df)[names(unadj_df) %like% "female"]]
  
  male[, `:=` (mean = NULL, standard_error = NULL, upper = NULL, lower = NULL)]
  male[, sex := "Male"]
  male[, m_per_pop := male_population/both_population]
  male[, sample_size := m_per_pop * sample_size]
  male[, m_per_pop := NULL]
  setnames(male, c("male_lower", "male_mean", "male_upper", "male_se"), c("lower", "mean", "upper", "standard_error"))
  
  # female table
  
  female <- copy(unadj_df)
  
  female[, `:=` (male_lower = NULL, male_mean = NULL, male_upper = NULL, male_se = NULL)]
  female[, `:=` (mean = NULL, standard_error = NULL, upper = NULL, lower = NULL)]
  female[, sex := "Female"]
  female[, f_per_pop := female_population/both_population]
  female[, sample_size := f_per_pop * sample_size]
  female[, f_per_pop := NULL]
  female[, female_population := NULL]
  setnames(female, c("female_lower", "female_mean", "female_upper", "female_se"), c("lower", "mean", "upper", "standard_error"))
  
  # all sex-split table
  
  sex_spec <- rbind(male, female)
  sex_spec[, sex_split := 1]
  sex_spec[, uncertainty_type := "Standard error"]
  sex_spec[, cases := mean * sample_size]
  sex_spec[, specificity := "; Sex split using ratio from MR-BRT"]
  
  # compute ui
  
  sex_spec[, lower := mean-1.96*standard_error]
  sex_spec[, upper := mean+1.96*standard_error]
  sex_spec[lower < 0, lower := 0]
  
  # append sex-split estimates
  
  sex_adj_df <- copy(orig_df)
  sex_adj_df <- sex_adj_df[sex_split == 0]
  
  if (length(setdiff(names(sex_spec), names(sex_adj_df))) != 0){
    cat(paste0("\nDropping col ", as.character(setdiff(names(sex_spec), names(sex_adj_df))), " from sex-split data to merge with all-sex"))
    drop_cols <- setdiff(names(sex_spec), names(sex_adj_df))
    rem_cols  <- names(sex_spec)[!(names(sex_spec) %in% drop_cols)]
    sex_spec <- sex_spec[, ..rem_cols]
  }
  
  sex_adj_df <- rbind(sex_adj_df, sex_spec,fill=TRUE)
  sex_adj_df <- sex_adj_df[order(nid, ihme_loc_id, year_start, sex, age_start)]
  sex_adj_df[, seq := NA]
  
  cat("\n\nSex-Splitting complete. Returned data table with original sex-specific estimates and newly sex-split estimates.\n\nSex-split estimates can be differentiated by the sex_split column. Seqs are blanked. Crosswalk_parent_seqs from seqs in input data.\n ")
  sex_adj_df[, sex_id := ifelse(sex == "Male", 1, 2)]
  return(sex_adj_df)
  
}

#set these values to enable the age split code
sex_adj_df$group_review<-1
sex_adj_df$is_outlier<-0
sex_adj_df$year_issue<-0


write.csv(sex_adj_df,"FILENAME")
write.xlsx(sex_adj_df,"FILENAME")
