



topic <- "anemia"
varname <- "hemoglobin"
input_dir <- FILEPATH
code_dir <- FILEPATH
plot_dir <- FILEPATH
XMIN = 0 
XMAX = 220
dw_r = c(1, 13, 40)
threshold_csv <- FILEPATH
max_files = 100
min_obs = 100 
distclass = "classQ"
max_iter = 500
plot = FALSE
XSHIFT = NA
if(plot) pdf(file = paste0(plot_dir, "/mv2P_fits_ensemble_ClassQ_4.pdf"), width = 16, height = 8)


###################
### Setting up ####
###################

require(data.table)
setwd(code_dir)
source("DistList_mv2P.R")
threshold_map <- fread(threshold_csv)
dists <- get(distclass)
distnames <- names(dists) 
  
  ### DEFINE TESTING/TRAINING DATA
  files <- list.files(path = input_dir, pattern = "csv", recursive = FALSE)
    #Reduce to max length 
    set.seed(2) 
    if(!is.na(max_files)) files <- sample(files, max_files, replace = FALSE)

  
  # ---- ANEMIA - 
  if(topic == "anemia") {
      ### DEFINE TESTING SET - HALF NHANES AND HALF DHS
      testing_i <- list.files(path = input_dir, pattern = "NHANES", recursive = FALSE)
      training_i <- list.files(path = input_dir, pattern = "DHS", recursive = FALSE)
      #Make testing set half DHS  
      set.seed(1)  
      train_2_test <- sample(training_i, length(testing_i), replace = FALSE)
      remove_i <- which(training_i %in% train_2_test)
      #Testing Set 
      testing_set <- c(testing_i, train_2_test)
      #Training set 
      training_set <- training_i[-remove_i]
      if(!is.na(max_files)) training_set <- sample(training_set, max_files, replace = FALSE)
  }
  

#Load data into memory - need all groups
#Initialize

train_data <- list()
train_t <- list()
train_survey <- list()
train_group <- list()

test_data <- list()
test_t <- list()
test_survey <- list()
test_group <- list()

files <- c(training_set, testing_set)

num <- length(files)
setwd(input_dir)

for (i in 1:num) {
  
  print(paste0("LOADING ", files[i], " (", i, " of ", num, ")"))
  
  ## load
  df <- fread(files[i])
  if("V1" %in% names(df)) df$V1 <- NULL
  #In case age_year is continuous, round
  df[, age_year := round(age_year, 0)]
  #Shifts
  if(!is.na(XSHIFT)) df[[varname]] = df[[varname]] + XSHIFT
  
  ## Set groups
  merge_vars <- unique(threshold_map$merge_vars)
  merge_vars <- unlist(strsplit(gsub(" ","", merge_vars), split=","))
  df <- merge(df, threshold_map, by = merge_vars, all.x = TRUE)
    
  groups <- unique(df$group)
  
  for(grp in groups){
    
    print(paste("     ", grp))
    
    df1 <- df[group == grp]
    
    if(nrow(df1) > min_obs) {
      
      name <- paste(unique(df1$ihme_loc_id), unique(df1$survey_name), unique(df1$year_start), sep = "_")
      
      
      if(files[i] %in% training_set) {
        j = length(train_data) + 1
        train_data[[j]] <- df1[[varname]]
        train_t[[j]] <- gsub(" ","", unique(df1$t))
        train_survey[[j]] <- name
        train_group[[j]] <- grp
      }
      
      
      if(files[i] %in% testing_set) {
        j = length(test_data) + 1
        test_data[[j]] <- df1[[varname]]
        test_t[[j]] <- gsub(" ","", unique(df1$t))
        test_survey[[j]] <- name
        test_group[[j]] <- grp
      }
     
      
    


#TRAIN

data <- train_data
thresh <- train_t
dists <- get(distclass)
distnames <- names(dists) 
n_dist <- length(dists)

F = function(w){
  
  num <- length(data)
  sum_e = 0   
  
  for (i in 1:num) {
    
    td <- data[[i]]
    t = as.numeric(unlist(strsplit(thresh[[i]],split=",")))
   
    #WEIGHTS - Make w positive and sum to 1
      w = sapply(w, function(x) abs(x))
      w = w * (1 / sum(w))
      names(w) = distnames
    
    #PARAMETERS
      param <- list()
      for(dist in distnames){
        mv2p <- dists[[dist]][["mv2par"]]
        param[[dist]] <- mv2p(mean(td), var(td))
      }
    
    #ERRORS
      #For each thresh
      errors =  sapply(t, function(x) abs(sum(unlist(lapply(distnames, function(d) w[[d]] * dists[[d]][["tailF"]](param[[d]], x, lt = TRUE)))) - 
                                         sum(td < x)/length(td)))
      #Adjust by DW ratio
      sum(dw_r * errors)
      sum_e = sum_e + sum(dw_r * errors)
      
    
  } # next survey 
  print("weights")
  print(w)
  print("error")
  print(sum_e)
  sum_e
  
} #end function

xi = rep(1/n_dist,n_dist)
if(topic == "anemia" & distclass == "classP") xi = rep(1/n_dist, n_dist)
est = abs(optim(xi, F, control = list(maxit=max_iter))$par)
est = est * (1 / sum(est))
est = round(est, 6)







