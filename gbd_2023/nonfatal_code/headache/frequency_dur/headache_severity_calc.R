
##########################################################################
## Purpose: Get Severity Splits for Headache
##########################################################################

## SET-UP
rm(list=ls())

pacman::p_load(data.table, ggplot2, boot, readr, openxlsx)

## SET OBJECTS
date <- gsub("-", "_", Sys.Date())
freq_dir <- 'FILEPATH'
save_dir <- 'FILEPATH'
draws <- paste0("draw_", 0:999)


## READ IN MEAN AND SE OF PROPORTION OF TIME IN ICTAL STATE FOR THOSE WITH HEADACHE
timesym <- fread(paste0(freq_dir, "/pTIS_by_sex_and_age.csv"))


## RELABELLING TO EASE THE USE OF LOOPS
timesym[age == "<35", age := 1]
timesym[age == "35-49", age := 2]
timesym[age == ">50", age := 3]
timesym[diagnosis == "definite migraine", diagnosis := "def_mig"]
timesym[diagnosis == "probable migraine", diagnosis := "prob_mig"]
timesym[diagnosis == "definite tth", diagnosis := "def_tth"]
timesym[diagnosis == "probable tth", diagnosis := "prob_tth"]


## CALCULATING THE SIZE-VARIABLES TO BE USED AS ARGUMENTS IN THE rbinom() FUNCTION TO CREATE 1000 DRAWS
for (a in 1:3) {
  for (d in c("moh", "def_mig", "prob_mig", "def_tth", "prob_tth")){
    for (s in c("male", "female")) {
      nam1 <- paste(d, "_", "size", "_", s, "_", a, sep = "")
      assign(nam1, (timesym[diagnosis == d & sex == s & age == a, time_ictal_mean] * (1 - timesym[diagnosis == d & sex == s & age == a, time_ictal_mean])) / ((timesym[diagnosis == d & sex == s & age == a, time_ictal_se])^2))
    }
  }
}


## ROUNDING THE SIZE-VARIABLES
for (b in c("prob_mig_size_", "def_mig_size_", "prob_tth_size_", "def_tth_size_", "moh_size_")) {
  for (s in c("male", "female")) {
    for (i in 1:3) {
      variable_name <- paste0(b, s, "_", i)
      assign(variable_name, round(get(variable_name), digits = 0))
    }
  }
}


## CREATE NEW DATATABLE WHICH WILL CONTAIN 1000 DRAWS
dt <- data.table(variable = draws)


## CREATE THE 1000 DRAWS FOR EACH HEADACHE TYPE*AGE GROUP*SEX COMBINATION
for (d in c("moh", "def_mig", "prob_mig", "def_tth", "prob_tth")) {
  for (s in c("male", "female")) {
    for (a in 1:3) {
      # CONSTRUCT COLUMN NAMES
      timesym_name <- sprintf("timesym%s_%s_%d", d, s, a)
      size_var_name <- sprintf("%s_size_%s_%d", d, s, a)
      
      # CALCULATE THE rbinom() ARGUMENTS
      prob <- timesym[diagnosis == d & sex == s & age == a, time_ictal_mean]
      size <- get(size_var_name)
      
      # ASSIGN THE CALCULATED VALUE OF THE DRAWS TO THE NEWLY CREATED "timesym2" DATATABLE USING rbinom()
      dt[, (timesym_name) := rbinom(n = 1000, prob = prob, size = size)]
      # DIVIDE THE DRAW-VALUES BY THEIR CORRESPONDING SIZE-VARIABLE TO GET FINAL ESTIMATES OF PROPORTION OF TIME IN ICTAL STATE
      dt[, (timesym_name) := get(timesym_name) / size, .SDcols = timesym_name]
    }
  }
}


## SAVE DRAWS IN .rds FORMAT
write_rds(dt, paste0(save_dir, date, ".rds"))