##########################################################################
### Author: USERNAME
### Project: GBD Nonfatal Estimation
### Purpose: Prep HRS Data for IRT
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
hrs_dir <- paste0("FILEPATH")

# FUNCTIONS ---------------------------------------------------------------

create_interval_var <- function(dt, variable, new_variable, groups = 10){
  dt[, c(new_variable) := as.numeric(discretize(get(variable), method = "interval", breaks = groups,
                                                labels = 1:groups))]
  n <- nrow(dt[!is.na(get(new_variable))])
  cats <- sort(dt[!is.na(get(new_variable)), unique(get(new_variable))])
  for (cat in cats){
    cats <- sort(dt[!is.na(get(new_variable)), unique(get(new_variable))]) ## recalculate categories based on what we have
    if (nrow(dt[get(new_variable) == cat])/n < 0.05 & cat == min(cats)){ ## if less than 5% of data and not the max, subtract 1
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
files <- list.files(hrs_dir)
file2000 <- files[grepl("2000", files)]
hrs <- as.data.table(haven::read_dta(paste0("FILEPATH")))
hrs <- hrs[!is.na(age_year)] 

# FIX SKIP PATTERN --------------------------------------------------------

message('fixing skip pattern')

hrs[difficulty_walk_svrl_blks == "no", difficulty_walk_1block := "no"] ## IF YOU CAN WALK SEVERAL BLOCKS YOU CAN WALK ONE (SKIP LOGIC)
hrs_gateway_adl <- c("difficulty_walk_svrl_blks", "difficulty_walk_1block", "difficulty_sit_2hrs", "difficulty_getting_up_chair",
                     "difficulty_climb_1flight_stairs", "difficulty_stooping", "difficulty_reaching_arms", "difficulty_pull_push_lrg_obj",
                     "difficulty_lifting_weights", "difficulty_picking_up_dime")
hrs_adl <- names(hrs)[grepl("^adl", names(hrs))]
hrs_adl_minusdress <- hrs_adl[!hrs_adl == "adl_dressing"]
hrs[, sum := apply(.SD, 1, function(x) sum(x == "no")), .SDcols = hrs_gateway_adl]
hrs[sum == 10, (hrs_adl) := apply(.SD, 1, function(x) x = "no"), .SDcols = hrs_adl]
hrs[sum == 9 & adl_dressing == "no", (hrs_adl_minusdress) := apply(.SD, 1, function(x) x = "no"), .SDcols = hrs_adl_minusdress]

# CONVERT AND FORMAT VARIABLE LISTS ---------------------------------------

message('converting variables')

## ADL
convert_adl <- data.table(label = c("", "yes", "no", "don't do", "can't do", "dk (don't know); na (not ascertained)"),
                          binary = c(NA, F, T, F, F, NA))
adl_varset_hrs <- names(hrs)[grepl("^adl|^iadl", names(hrs))]
for (var in adl_varset_hrs){
  hrs <- merge(hrs, convert_adl, by.x = var, by.y = "label", all.x = T)
  setnames(hrs, "binary", paste0(var, "_bin"))
}
adl_varset_hrs <- paste0(adl_varset_hrs, "_bin")

## IQCODE 
iqcode_hrs <- names(hrs)[grepl("iqcode", names(hrs))]
iqcode_hrs <- iqcode_hrs[stringr::str_count(iqcode_hrs, "_") == 1]
iqcode_na_vars <- c("", "dk (don't know); na (not ascertained)", "rf (refused)")
iqcode_nochange_vars <- c("does not apply; r doesn't do activity")
for (var in iqcode_hrs){
  hrs[, paste0(var, "_recode") := get(var)]
  hrs[get(var) %in% iqcode_na_vars, paste0(var, "_recode") := NA]
  hrs[get(var) %in% iqcode_nochange_vars, paste0(var, "_recode") := "not much changed"]
  hrs[get(var) == "gotten worse", paste0(var, "_recode") := get(paste0(var, "_worse"))]
  hrs[get(var) == "improved", paste0(var, "_recode") := get(paste0(var, "_better"))]
  hrs[get(paste0(var, "_recode")) %in% iqcode_na_vars, paste0(var, "_recode") := NA]
}
iqcode_convert_hrs <- data.table(cat = c("much worse", "a bit worse", "not much changed", "a bit improved", "much improved"),
                                 num = c(1, 1, 2, 3, 3))
for (var in iqcode_hrs){
  hrs <- merge(hrs, iqcode_convert_hrs, by.x = paste0(var, "_recode"), by.y = "cat", all.x = T)
  setnames(hrs, "num", paste0(var, "_num"))
}
iqcode_varset_hrs <- paste0(iqcode_hrs, "_num")

## IMMEDIATE WORD RECALL
imrecall_hrs <- names(hrs)[grepl("word_recall_immediate", names(hrs))]
recall_na_vars <- c("", "dk (don't know); na (not ascertained)", "rf (refused)")
recall_wrong_vars <- c(paste0(c("1st", "2nd", "3rd"), " WRONG WORD"), "none remembered")
recall_convert <- function(x){
  x[x %in% recall_na_vars] <- NA
  x[x %in% recall_wrong_vars] <- 0
  x[!is.na(x) & !x == 0] <- 1
  x <- as.numeric(x)
  return(x)
}
hrs[, paste0(imrecall_hrs, "_numeric") := lapply(.SD, recall_convert), .SDcols = imrecall_hrs]
hrs[, imword_trl1 := apply(.SD, 1, function(x) sum(x == 1, na.rm = T)), .SDcols = paste0(imrecall_hrs, "_numeric")]
hrs[is.na(word_recall_immediate_1_numeric), imword_trl1 := NA]
imrecall_varset_hrs <- "imword_trl1"


## DELAYED WORD RECALL
delrecall_hrs <- names(hrs)[grepl("word_recall_delayed", names(hrs))]
hrs[, paste0(delrecall_hrs, "_numeric") := lapply(.SD, recall_convert), .SDcols = delrecall_hrs]
hrs[, delword_total := apply(.SD, 1, function(x) sum(x == 1, na.rm = T)), .SDcols = paste0(delrecall_hrs, "_numeric")]
hrs[is.na(word_recall_delayed_1_numeric), delword_total := NA]
delrecall_varset_hrs <- "delword_total"


## COUNTING
count_hrs <- names(hrs)[grepl("countback", names(hrs)) & grepl("1$", names(hrs))]
count_na_vars_hrs <- c("", "rf (refused)")
count_wrong_vars_hrs <- c("incorrect", "want to start over", "wants to start over")
recode_count <- function(x){
  x[tolower(x) %in% count_na_vars_hrs] <- NA
  x[tolower(x) %in% count_wrong_vars_hrs] <- F
  x[!is.na(x) & !x == F] <- T
  x <- as.logical(x)
  return(x)
}
hrs[, paste0(count_hrs, "_recode") := lapply(.SD, recode_count), .SDcols = count_hrs]


counting_varset_hrs <- paste0(count_hrs, "_recode")

## SERIAL 7s - GETTING RID OF A LOT OF EXTRA INFORMATION BY DICHOTOMIZING
serial7_hrs <- names(hrs)[grepl("serial7", names(hrs))]
serial7_ans <- data.table(num = c(1, 2, 3, 4, 5),
                          ans = c(93, 86, 79, 72, 65))
serial7_convert <- function(number){
  correct_ans <- serial7_ans[num == number, ans]
  x <- hrs[, get(paste0("tics_serial7_", number))]
  y <- rep(0, length(x))
  y[x == correct_ans] <- 1
  if (!number == 1){
    other_correct <- hrs[, get(paste0("tics_serial7_", number-1))]-7
    y[(x == other_correct & !is.na(x))] <- 1
  }
  y[is.na(x)] <- NA
  return(y)
}
hrs[, paste0("tics_serial7_", 1:5, "_recode") := lapply(1:5, serial7_convert)]
hrs[, tics_serial7_total := apply(.SD, 1, sum), .SDcols = paste0("tics_serial7_", 1:5, "_recode")]
serial7_varset_hrs <- paste0("tics_serial7_total")

## OTHER TICS
other_tics_hrs <- paste0("tics_", c("cutpaper", "desertplant", "president_us", "vicepresident"))
other_na_hrs <- c("", "dk (don't know); na (not ascertained)", "rf (refused)")
other_tics_convert <- function(x){
  x[x %in% other_na_hrs] <- NA
  x[x == "not correct"] <- F
  x[!is.na(x) & !x == F] <- T
  x <- as.logical(x)
  return(x)
}
hrs[, paste0(other_tics_hrs, "_recode") := lapply(.SD, other_tics_convert), .SDcols = other_tics_hrs]
other_tics_varset_hrs <- paste0(other_tics_hrs, "_recode")

## MMSE
mmse_hrs <- names(hrs)[grepl("mmse", names(hrs))]
mmse_na_hrs <- c("", "dk (don't know); na (not ascertained)", "rf (refused)")
mmse_convert <- function(x){
  x[x %in% mmse_na_hrs] <- NA
  x[grepl("not ok", x)] <- F
  x[!is.na(x) & !x == F] <- T
  x <- as.logical(x)
  return(x)
}
hrs[, paste0(mmse_hrs, "_recode") := lapply(.SD, mmse_convert), .SDcols = mmse_hrs]
mmse_varset_hrs <- paste0(mmse_hrs, "_recode")

## MEANING
meaning_hrs <- names(hrs)[grepl("meaning", names(hrs))]
meaning_na_hrs <- c("", "RF (refused)")
meaning_convert <- function(x){
  x[x %in% meaning_na_hrs] <- NA
  x[x == "Don't know; answer incorrect"] <- 0
  x[x == "Answer partially correct"] <- 1
  x[x == "Answer perfectly correct"] <- 2
  x <- as.numeric(x)
  return(x)
}
hrs[, paste0(meaning_hrs, "_recode") := lapply(.SD, meaning_convert), .SDcols = meaning_hrs]
meaning_varset_hrs <- paste0(meaning_hrs, "_recode")

## INFORMANT - LEAVING OUT FOR NOW
inform_hrs <- names(hrs)[grepl("inform_", names(hrs))]

# SUBSET DATA -------------------------------------------------------------

message('subsetting')

## SAVE FULL HRS SAMPLE BUT SUBSET TO THOSE WITH COGNITIVE DATA
hrs_all <- copy(hrs)
hrs <- hrs[age_year >= 65] ## DID NOT ADMINISTER ALL COGNITIVE TESTS TO THOSE UNDER 65

variable_sets_hrs <- ls(pattern = "varset_hrs")
test_vars_hrs <- c("hh_id", "pn", "pweight")
for (set in variable_sets_hrs){
  test_vars_hrs <- c(test_vars_hrs, get(set))
}
test_dt_hrs <- dplyr::select(hrs, test_vars_hrs)
new_names_hrs <- gsub("_bin|_num|_recode", "", names(test_dt_hrs))
setnames(test_dt_hrs, names(test_dt_hrs), new_names_hrs)

## GET RID OF ITEMS WITH STANDARDIZED LOADING OF LESS THAN 0.3
test_dt_hrs[, c("meaning_fabric", "meaning_compassion", "meaning_domestic") := NULL]

## WRITE DATA
readr::write_rds(test_dt_hrs, paste0("FILEPATH"))