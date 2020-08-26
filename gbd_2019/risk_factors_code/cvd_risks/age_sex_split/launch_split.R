##Purpose: Launch age sex splitting

if(!exists("rm_ctrl")){
  rm(list=objects())
}
os <- .Platform$OS.type
if (os=="windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
}

date<-gsub("-", "_", Sys.Date())
library(StanHeaders)
library(rstan)
library(ggplot2)
library(data.table)
library(TMBhelper, lib='FILEPATH')

################### SCRIPTS #########################################
code_root <- 'FILEPATH'
central<-"FILEPATH"

source(paste0(code_root, "prep_split_data.R"))
source(paste0(code_root, "setup_kos.R"))

source(paste0(code_root, "data_tests.R"))
source(paste0(code_root, "get_recent.R"))
source(paste0(code_root, "model_helper_functions.R"))
source(paste0(code_root, "get_user_input.R"))
source("FILEPATH/delta_smooth_mer.R")

source(paste0(central, "get_population.R"))
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_age_metadata.R"))

################### PATHS AND ARGS #########################################
if(!exists("me")){
  me <- get_me_from_user()
  decomp_step <- get_step_from_user()
}

step_num <- gsub('step', '', decomp_step)

save_output <- T

validate <- F
kos <- 1
st <- F

##sy: bayes model args
if(me=="sbp"){
  age_smooth <- 1.5 
  age_mesh <- seq(from=0, to=100, by=5) ##sy: ages to allow linear deviation between
}
if(me=="ldl"){
  age_smooth <- 0.05 
  age_mesh <- c(0, seq(from=10, to=100, by=5) )
}

m_name <- paste0(me, "_tmb_test")
plot_results <- F 

sd_mod_path <- paste0("FILEPATH", me, "_sd_mod.Rdata")
sd_mod_path <- paste0("FILEPATH", me, "_models/")

data_folder <- paste0("FILEPATH", me, "_to_split/")
output_path <- paste0("FILEPATH", me, "_to_clean/")
plot_output<-paste0("FILEPATH", me, "_age_sex_pattern_", date, ".pdf")


################### CONDITIONALS #########################################

if(validate==F){
  input_folder <- paste0("FILEPATH", me, "/input_data/")
  kos <- 1
  mod_name <- m_name
} else {
    input_folder <- paste0("FILEPATH", me, "/ko_input_data/")
}

################### SETUP DATA #########################################
cleaned_data <- setup_split_data(me, m_name,
                               data_folder=data_folder,
                               output_folder=input_folder,
                               validate=validate, kos=kos, decomp_step = step_num)
pre_split_data <- cleaned_data$data

split_data_list <- prep_split_data(pre_split_data, step_num)


if(validate==T){
  saveRDS(cleaned_data[[1]], paste0(output_path, mod_name, "_", ko, "_data.rds"))
  saveRDS(cleaned_data[[2]], paste0(output_path, mod_name, "_orig_data.rds"))
}else{
  saveRDS(cleaned_data[[1]], paste0(output_path, mod_name, "_data.rds"))
}

################### CREATE PATTERNS #########################################

#data<-cleaned_data[[1]]
training <- split_data_list$training

training[, pop_mean:=weighted.mean(data, sample_size), by=.(nid, sex_id, location_id, year_id)]

## make binary for female and interaction
training[, female:=sex_id-1]
training[, age_female:=age_start*female]

## make interaction for pop_mean
training[, scaled_pop_mean:=(pop_mean-mean(pop_mean))/sd(pop_mean)]
training[, scaled_age:=(age_start-mean(age_start))/sd(age_start)]
training[, age_popmean:=scaled_age*scaled_pop_mean]

## create mesh points for pop_mean after scaling
training[, scaled_pop_mean:=(pop_mean-mean(pop_mean))/sd(pop_mean)]
mesh_points <- list(
  age_start = age_mesh,
  age_female = age_mesh
  )

fit1 <- delta_smooth_mer(response="data", data=training, fixefs="female", ranefs=NULL, delta_vars=c("age_start", "age_female"), 
                       ses=training$standard_error, mesh_points = mesh_points, est_tau = T, tau = c(age_smooth, age_smooth))

################### PLOT AGE TREND #########################################

## This prediction is just for plotting purposes
age_preds <- rep(mesh_points$age_start, times=2)
female <- c(rep(0, times=length(mesh_points$age_start)), rep(1, times=length(mesh_points$age_start)))
age_preds <- data.table(age_start=age_preds, female=female)
age_preds[, age_female:=age_start*female]
age_preds[, age_popmean:=4]
age_preds[, scaled_pop_mean:=4]
age_preds[, nid:=NA]

preds <- predict_delta_mer(fit1, age_preds, return_draws=F, upper_lower=T)

age_preds <- cbind(age_preds, preds)

age_means <- training[, .(age_mean=mean(data), age_mean_wtd=weighted.mean(data, 1/standard_error)), by=c("age_start", "female")]
age_preds <- merge(age_preds, age_means, by=c("age_start", "female"))

pdf(plot_output)
p <- ggplot(data=age_preds, aes(x=age_start))+
      geom_point(data=training, aes(y=data), alpha=0.1)+
      geom_line(aes(y=pred), color="cornflowerblue")+
      geom_point(aes(y=age_mean), color="red")+
      geom_point(aes(y=age_mean_wtd), color="orange")+
      geom_ribbon(aes(ymin=lower, ymax=upper), fill="cornflowerblue", alpha=0.6)+
      facet_wrap(~female)+
      ylab(me)+
      xlab("Age")+
      ggtitle("Global age patterns")+
      theme_bw()
print(p)
dev.off()

################### SETUP TO PREDICT SPLIT DATA #########################################
## create necessary variables for prediction, this will need to be updated if any variables are added
split_copy <- split_data_list$split_copy
split_copy[, female:=sex_id-1]
split_copy[, age_female:=age_start*female]
split_copy[, wt:=population/total_pop]

## predict mer mod for split_copy
est_draws <- predict_delta_mer(fit1, new_data=split_copy, return_draws=T)

## get sum of exposure times sub populations for whole split_id, at the draw level (predicted overall mean)
pop_sums <- cbind(split_copy[, .(wt, split_id)], est_draws)
n_draws <- length(grep("draw", names(pop_sums)))-1

## get weighted mean for each age group
pop_sums[, (paste0("draw", 0:n_draws)):=lapply(0:n_draws, function(x){
  get(paste0("draw", x)) * wt
})]

## then sum. This holds draws of the sum of the predictions for each age group, weighted by population
pop_sums[, (paste0("draw", 0:n_draws)):=lapply(0:n_draws, function(x){
  sum(get(paste0("draw", x)))
}), by=split_id]
pop_sumst <- t(pop_sums[, grep("draw", names(pop_sums)), with=F]) ##sy: transpose so draws are long


################### PREDICT SPLIT DATA #########################################
## save draws of the predicted draw and the sum of the predicted draws, weighted by pops
draw_list <- list(
  est_draw = t(est_draws), ## transpose so draws are long
  r_group = pop_sumst
)

## pass in the mean and standard error
data_list <- list(
  means = split_copy$data,
  ses = split_copy$standard_error
)

pred_math <- "rnorm(n=length(means), mean=means, sd=ses) * est_draw/r_group"

split_predictions <- predict_draws(pred_math, draw_list, data_list, return_draws=F)

split_predictions <- cbind(split_copy, split_predictions)

################### CLEAN, MERGE METADATA #########################################
## split sample size
split_predictions[, sample_size:=sample_size*wt]

## drop unnecessary columns
split_predictions <- split_predictions[, .(split_id, nid, year_id, location_id, sex_id, age_group_id, sample_size, pred, se)]
setnames(split_predictions, c("pred", "se"), c("data", "standard_error"))
split_predictions[, cv_split:=1]

## merge onto training the data that wasn't used to train or to split
if(nrow(split_data_list$no_split)>0){
  training <- rbind(training, split_data_list$no_split, fill=T)
}

## create age_group_id for training data
training[, age_group_id := round((age_start/5)+5)]  ##sy:haven't tested this yet
training[age_start>=80, age_group_id:=30]
training[age_start>=85, age_group_id:=31]
training[age_start>=90, age_group_id:=32]
training[age_start>=95, age_group_id:=235]

## drop unnecessary cols
training[, cv_split:=0]
training<-training[, names(split_predictions), with=F]


full <- rbind(split_predictions, training)

## merge on metadata
metadata <- split_data_list$metadata

full <- merge(full, metadata, by="split_id", all.x=T)

miss_ids <- setdiff(metadata$split_id, full$split_id)

if(length(miss_ids)>0){
  message(length(miss_ids), " rows dropped in splitting process. Verify that this is correct.")
  Sys.sleep(4)
}


################### SAVE #########################################
if(save_output==T){
    ## check missing for values that can be missing and values that can't
    invisible(sapply(c("data", "standard_error", "sample_size"), check_missing, full))
    invisible(sapply(c("data", "ihme_loc_id", "sex_id", "year_id", "age_group_id"), check_missing, full, warn=F))
  
    ## check classes
    invisible(sapply(c("nid", "sex_id", "year_id", "age_group_id"), check_class, full, class="integer"))
    invisible(sapply(c("data", "standard_error"), check_class, full, class="numeric"))
  
    ## summary
    message("Summary of mean for ", me)
    print(summary(full$data))
    message("Summary of se for ", me)
    print(summary(full$standard_error))
  
  
    ## combine output split points with data that didn't need to be age/sex split
    write.csv(full, file=paste0(output_path, "to_clean_decomp", step_num, "_", date, ".csv"), row.names=F)
    message("Output saved here: ", paste0(output_path, "to_clean_decomp", step_num, "_", date, ".csv"))
  
}