rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- FILEPATH
  h <- FILEPATH
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- FILEPATH
  h<-FILEPATH
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(stringr)


args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- ifelse(is.na(args[1]), FILEPATH args[1])
p <- ifelse(is.na(args[2]), "ga_cat", args[2])
r <- ifelse(is.na(args[3]), "38_40", args[3])

task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 4, as.integer(Sys.getenv("SGE_TASK_ID")))
param_map <- fread(param_map_filepath)

iso3 <- param_map[task_id, iso3]
age <- param_map[task_id, age]
sex <- param_map[task_id, sex]

print(paste(iso3, age, sex, p, r))

# --------------------------
# FUNCTIONS

get_data <- function(iso3, age, sex){
  
  if(iso3 == "USA"){
    filepath <- FILEPATH 
    data <- readRDS(filepath)
    
  } else{
    filepath <- FILEPATH
    data <- readRDS(filepath)
    data[, sex_id := sex]
    
  }
  
  data <- data[age_group_id == age, ]  
  data[, ga_bw := paste0(ga_cat, "_", bw_cat)]
  
  return(data)
  
  print("Retrieved data")
  
}

get_regression_template <- function(predictor = "ga_bw"){
  
  predictor <- standardize_predictor(predictor)
  
  me_map <- fread(FILEPATH)
  
  me_map <- me_map[, list(ga_cat, bw_cat, ga, bw)]
  me_map[, ga_bw := paste0(ga_cat, "_", bw_cat)]
  
  if(predictor=="ga_cat"){
    regression_template <- unique(me_map[, list(ga_cat, ga)])  
  } else if(predictor == "bw_cat"){
    regression_template <- unique(me_map[, list(bw_cat, bw)])
  } else if(predictor == "ga_bw") {
    regression_template <- me_map 
  } else{
    warning("select predictor: ga_cat, bw_cat, or ga_bw")
  }

  return(regression_template)
    
}


standardize_predictor <- function(predictor){
  
  if(predictor %in% c("ga", "ga_cat")){
    predictor = "ga_cat"  
  } else if (predictor %in% c("bw", "bw_cat")){
    predictor = "bw_cat"
  } 
  return(predictor)
  
}


set_reference_for_regression <- function(data, predictor, reference){
  
  predictor <- standardize_predictor(predictor)
  
  regression_template <- get_regression_template(predictor)
  
  all_values <- regression_template[, predictor, with = F] %>% unlist %>% unname
  
  non_reference_levels <- all_values[!(all_values == reference)]
  
  ordered_factors <- c(reference, non_reference_levels)
  
  if(predictor == "ga_cat"){
    data[, ga_cat := factor(ga_cat, levels = ordered_factors)]  
  } else if(predictor == "bw_cat"){
    data[, bw_cat := factor(bw_cat, levels = ordered_factors)]
  } else if(predictor == "ga_bw"){
    data[, ga_bw := factor(ga_bw, levels = ordered_factors)]
  }
  
  return(data)
  
}


run_regression <- function(data, predictor){
  
  predictor <- standardize_predictor(predictor)
  
  if(predictor=="bw_cat"){
    
    model <- glm(formula = death~bw_cat, family = binomial(link = "logit"), data = data)  
    
  } else if(predictor=="ga_cat"){
    
    model <- glm(formula = death~ga_cat, family = binomial(link = "logit"), data = data)  
    
  } else if(predictor == "ga_bw"){
    
    print("Starting to run the bivariate model")
    model <- glm(formula = death~ga_bw, family = binomial(link = "logit"), data = data)
    print("Completed bivariate model")
    
  } else{
    
    warning("specify bw or ga univariate model predictor")
    
  }

  return(model)
  
  # Status message
  print("Regression Run")
  
}


store_betas_and_std_err <- function(model, predictor, reference){

  store <- data.table(names(coef(model)), coef(model), coef(summary(model))[, "Std. Error"])  
  
  setnames(store, c("V1", "V2", "V3"), c(predictor, "oddsratio", "std_error"))
  
  replace_intercept <- paste0(predictor, reference)
  
  store[grep(pattern = "Intercept", x = store[, predictor, with = F]), (predictor) :=  replace_intercept] 
  
  store[, (predictor) := sapply(strsplit(store[[predictor]], split=predictor, fixed=TRUE), function(x) (x[2]))]
  
  return(store)
  
  # Status message
  print("Betas & std error stored")
  
}
 
add_sample_size_per_bin <- function(data, store, predictor){
  
  predictor = standardize_predictor(predictor)
  
  sample_size <- data[, .(num_at_age_start = .N, 
                           deaths = sum(death),
                           case_fatality = sum(death)/.N), by = predictor]
  
  store <- merge(sample_size, store, by = predictor)
  
  return(store)
  
  # Status message
  print("Sample size per bin stored")
  
}


calculate_relative_risk <- function(store, predictor, reference){
  
  store[, intercept := store[get(predictor) == reference, "oddsratio"]]
  
  store[get(predictor) == reference, oddsratio := 0]
  
  store[, ln_mortality_odds := intercept + oddsratio]
  
  store[, mortality_odds := exp(ln_mortality_odds)]
  
  store[, mortality_risk := mortality_odds / (1+mortality_odds)]
  
  min_num_at_age_start <- 0.005 * store[, sum(num_at_age_start, na.rm =T)]
  
  min_risk <- store[num_at_age_start > min_num_at_age_start, min(mortality_risk)]  
  
  store[, relative_risk := mortality_risk / min_risk]
  
  return(store)
  
  # Status message
  print("Relative Risk calculated")
  
}






# --------------------------
# Run Code: 

print("run_regressions.R starting")

data <- get_data(iso3, age, sex)

data <- set_reference_for_regression(data, predictor = p, reference = r)

model <- run_regression(data, predictor = p)

store <- store_betas_and_std_err(model = model, predictor = p, reference = r) 

store <- add_sample_size_per_bin(data = data, store = store, predictor = p) 

store <- calculate_relative_risk(store = store, predictor = p, reference = r) 

filepath <- FILEPATH 

write.csv(store, filepath, na = "", row.names = F) 


# Status message
print("run_regressions.R completed")