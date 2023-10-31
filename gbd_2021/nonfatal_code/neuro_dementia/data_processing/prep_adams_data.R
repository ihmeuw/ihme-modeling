##########################################################################
### Author: USERNAME
### Project: GBD Nonfatal Estimation
### Purpose: Prep ADAMS Data for IRT
##########################################################################

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library("arules", lib.loc = paste0("FILEPATH"))
date <- gsub("-", "_", Sys.Date())

# SET UP OBJECTS ----------------------------------------------------------

repo_dir <- paste0("FILEPATH")
adams_dir <- paste0("FILEPATH")

# FUNCTIONS ---------------------------------------------------------------

create_interval_var <- function(dt, variable, new_variable, groups = 10){
  dt[, c(new_variable) := as.numeric(discretize(get(variable), method = "interval", breaks = groups,
                                                labels = 1:groups))]
  n <- nrow(dt[!is.na(get(new_variable))])
  cats <- sort(dt[!is.na(get(new_variable)), unique(get(new_variable))])
  for (cat in cats){
    cats <- sort(dt[!is.na(get(new_variable)), unique(get(new_variable))]) 
    if (nrow(dt[get(new_variable) == cat])/n < 0.05 & cat == min(cats)){ 
      dt[get(new_variable) == cat, c(new_variable) := min(cats[!cats == cat])]
    }
    if (nrow(dt[get(new_variable) == cat])/n < 0.05 & !cat %in% c(max(cats), min(cats))){
      dt[get(new_variable) == cat, c(new_variable) := cat + 1]
    }
    if (nrow(dt[get(new_variable) == cat])/n < 0.05 & cat == max(cats)){
      dt[get(new_variable) == cat, c(new_variable) := max(cats[!cats == cat])]
    }
  }
  dt[, c(new_variable) := as.numeric(as.factor(get(new_variable)))]
  return(dt)
}

# GET DATA ----------------------------------------------------------------

message('getting data')

adamsa <- as.data.table(readr::read_rds(paste0("FILEPATH")))
adams_vars <- as.data.table(read.xlsx(paste0("FILEPATH")))
keep_vars <- c("hhid", "pn", "sweight_cross", "strata", "cluster")
for (x in 1:nrow(adams_vars)){
  if (adams_vars[, type][x] == "one"){
    keep_vars <- c(keep_vars, adams_vars[, vars][x])
  } else if (adams_vars[, type][x] == "grep"){
    vars <- names(adamsa)[grepl(adams_vars[, vars][x], names(adamsa))]
    keep_vars <- c(keep_vars, vars)
  }
}
adams_sub <- copy(adamsa)
adams_sub <- dplyr::select(adams_sub, keep_vars)


# DEFINE VARSETS AND FORMAT -----------------------------------------------

message('getting varsets')

test_vars_adams <- keep_vars

## MMSE
mmse_varset_adams <- names(adamsa)[grepl("mmse", names(adamsa))]
mmse_varset_adams <- mmse_varset_adams[!mmse_varset_adams %in% c("mmse_totalscore", "mmse_11_numtrials")]
correct_mmse_adams <- paste0("mmse_", c(16, 17, 19, 21))
adamsa[, paste0(correct_mmse_adams, "_recode") := lapply(.SD, function(x) grepl("Correct", x)), .SDcols = correct_mmse_adams]
mmse_3items <- paste0("mmse_20", c("f", "l", "c"))
adamsa[, mmse_20_num := apply(.SD, 1, sum), .SDcols = mmse_3items]
mmse_varset_adams <- c(mmse_varset_adams[!mmse_varset_adams %in% c(correct_mmse_adams, mmse_3items)], paste0(correct_mmse_adams, "_recode"), "mmse_20_num")

## TICS MINUS COUNTING/SERIAL 7
tics_varset_adams <- names(adamsa)[grepl("tics", names(adamsa))]
tics_varset_adams <- tics_varset_adams[!grepl("total|animalfluen|countback|serial", tics_varset_adams)] 

## COUNTING
count_varset_adams <- names(adamsa)[grepl("tics_countback|tics_completed", names(adamsa))]
count_varset_adams <- count_varset_adams[!grepl("2$", count_varset_adams)] 

## ANIMAL FLUENCY 
setnames(adamsa, "animalfluen_total", "animals_total")
animal_varset_adams <- names(adamsa)[grepl("animal", names(adamsa))]
animal_varset_adams <- animal_varset_adams[!grepl("[0-9]$|total$", animal_varset_adams)] ## ONLY TOTAL ANIMAL FLUENCY

## SERIAL 7s
serial7_varset_adams <- names(adamsa)[grepl("tics_completed|tics_serial", names(adamsa))]
serial7_varset_adams <- serial7_varset_adams[!grepl("serial7_[0-9]$", serial7_varset_adams)]

## IMMEDIATE WORD RECALL
imrecall_varset_adams <- c(paste0("imword_trl", 1:3), "imword_completed")

## DELAYED WORD RECALL
delrecall_varset_adams <- c("delword_completed", "delword_total")

## DIGIT SPAN 
ds_varset_adams <- names(adamsa)[grepl("digitsp", names(adamsa))]
ds_varset_adams <- ds_varset_adams[!ds_varset_adams %in% c(ds_varset_adams[grepl("[0-9]$", ds_varset_adams)], "digitsp_total")] ## TOTAL FWD AND BWD DIGITSPAN
adamsa[digitsp_fwd_total >= 13, digitsp_fwd_total := 13] 
adamsa[digitsp_bwd_total >= 10, digitsp_bwd_total := 10] 


## ADL
adl_varset_adams <- names(adamsa)[grepl("^(adl|iadl)", names(adamsa))]
## reverse coding
reverse_code <- function(item){
  reverse <- rep(0, length(NA))
  reverse[item == T] <- F
  reverse[item == F] <- T
  reverse <- as.logical(reverse)
  return(reverse)
}
adamsa[, (adl_varset_adams) := lapply(.SD, reverse_code), .SDcols = adl_varset_adams]


## IQCODE
iqcode_varset_adams <- names(adamsa)[grepl("iqcode", names(adamsa))]
iqcode_convert_adams <- data.table(cat = c("much worse", "a bit worse", "not much change", "a bit better", "much better"),
                                   num = c(1, 1, 2, 3, 3))
for (var in iqcode_varset_adams){
  adamsa <- merge(adamsa, iqcode_convert_adams, by.x = var, by.y = "cat", all.x = T)
  setnames(adamsa, "num", paste0(var, "_num"))
}
iqcode_varset_adams <- paste0(iqcode_varset_adams, "_num")


# ## TRAIL A
adamsa <- create_interval_var(adamsa, "traila_time", "traila_timecat") ## BREAK INTO CATEGORIES
adamsa[!is.na(traila_timecat), traila_timecat := length(unique(traila_timecat)) - traila_timecat] ## REVERSING THE SCALE
adamsa[!is.na(traila_numincomplete), traila_timecat := 1] 
traila_varset_adams <- paste0("traila_", c("completed", "timecat"))

# ## TRAIL B
adamsa <- create_interval_var(adamsa, "trailb_time", "trailb_timecat") ## BREAK INTO CATEGORIES
adamsa[!is.na(trailb_timecat), trailb_timecat := length(unique(trailb_timecat)) - trailb_timecat] ## REVERSING THE SCALE
adamsa[!is.na(trailb_numincomplete), trailb_timecat := 1] 
trailb_varset_adams <- paste0("trailb_", c("complete", "timecat"))

## SYMBOL DIGIT TEST (NOT INCLUDING ERROR QUESTION)
adamsa <- create_interval_var(adamsa, "symdig_total", "symdig_cat")
symdig_varset_adams <- c("symdig_complete", "symdig_cat")


# USE VARSETS TO SUBSET ---------------------------------------------------

## SUBSET DATA
variable_sets_adams <- ls(pattern = "varset_adams")
test_vars_adams <- c("hhid", "pn", "sweight_cross")
for (set in variable_sets_adams){
  test_vars_adams <- c(test_vars_adams, get(set))
}
test_dt_adams <- dplyr::select(adamsa, test_vars_adams)

message('fixing missing values')


cog_impair_stop <- c("subject could not do test due to cognitive impairment",
                     "tech stopped test due to congitive impairment")
for (set in variable_sets_adams){
  if (length(get(set)[grepl("complete", get(set))]) > 0){
    vars <- get(set)
    complete_var <- vars[grepl("complete", vars)]
    convert <- vars[!grepl("complete", vars)]
    type_dt <- data.table(vars = convert,
                          type = sapply(dplyr::select(test_dt_adams, convert), class))
    for (var in convert){
      if (type_dt[vars == c(var), type] == "logical"){
        test_dt_adams[get(complete_var) %in% cog_impair_stop, c(var) := F]
      } else if (type_dt[vars == c(var), type] == "numeric") {
        test_dt_adams[get(complete_var) %in% cog_impair_stop, c(var) := 0]
      }
    }
  }
}

message('standardizing names')

## STANDARDIZE NAMES
test_dt_adams <- dplyr::select(test_dt_adams, names(test_dt_adams)[!grepl("complete", names(test_dt_adams))])
new_names_adams <- gsub("_num$|_recode$|_cat$|cat$", "", names(test_dt_adams))
setnames(test_dt_adams, names(test_dt_adams), new_names_adams)
setnames(test_dt_adams, c("hhid", "sweight_cross"), c("hh_id", "pweight"))

## WRITE FORMATTED TESTING DATA
readr::write_rds(test_dt_adams, paste0("FILEPATH"))
readr::write_rds(adamsa, paste0("FILEPATH"))