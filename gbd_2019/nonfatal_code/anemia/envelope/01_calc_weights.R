#testing 
topic <- "anemia"
varname <- "hemoglobin"
input_dir <- "FILEPATH"
code_dir <- "FILEPATH"
plot_dir <- "FILEPATH"
XMIN = 0 
XMAX = 220
dw_r = c(1, 13, 40)
threshold_csv <- "FILEPATH/ensemble_thresh_map.csv"
max_files = 100
min_obs = 100 
distclass = "classQ"
max_iter = 500
plot = FALSE
XSHIFT = NA
if(plot) pdf(file = paste0(plot_dir, "/mv2P_fits_ensemble_ClassQ_4.pdf"), width = 16, height = 8)


if(F){
est = calc_weights(topic = topic, input_dir = input_dir, code_dir = code_dir, XMIN = XMIN, XMAX = XMAX, dw_r = dw_r, 
             min_obs = min_obs, threshold_csv = threshold_csv, max_files = max_files, distclass = distclass, max_iter = max_iter, XSHIFT = NA)


}

calc_weights <- function(topic, input_dir, code_dir, XMAX, dw_r, min_obs = 100, threshold_csv, max_files = NA, distclass = "classP", max_iter = 500){

###################
### Setting up ####
###################

require(data.table)
setwd(code_dir)
source("FILEPATH/DistList_mv2P.R")
threshold_map <- fread(threshold_csv)
dists <- get(distclass)
distnames <- names(dists) 
  
  ### DEFINE TESTING/TRAINING DATA
  files <- list.files(path = input_dir, pattern = "csv", recursive = FALSE)
    #Reduce to max length 
    set.seed(2) 
    if(!is.na(max_files)) files <- sample(files, max_files, replace = FALSE)
    
  # ---- DEFAULT - 80% training, 20% testing 
  if(topic != "anemia"){
    set.seed(1)  
    testing_set <- sample(files, 0.2 * length(files), replace = FALSE)
    remove_i <- which(files %in% testing_set)
    training_set <- files[-remove_i]
  }
  
  # ---- ANEMIA - special 
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
     
      
      
      ## graph 
      if(plot){
        td = df1[[varname]]
        param <- list()
        for(dist in distnames){
          mv2p <- dists[[dist]][["mv2par"]]
          param[[dist]] <- mv2p(mean(td), var(td))
        }
        
        #xx = 0:220
        xx = seq(from = XMIN, to = XMAX, length.out = 100)
        
        hist(td, probability=T, 30, xlim = c(XMIN,XMAX), xlab = varname, main = paste(files[i], grp, "(n =", length(td), ")"))
        lines(xx, dgamma(xx,param[["gamma"]][[1]], param[["gamma"]][[2]]), col = "green", lwd = 2)
        lines(xx, dmirgamma(xx,param[["mirgamma"]][[1]], param[["mirgamma"]][[2]]), lwd = 2, col = "blue") 
        lines(xx, dweibull(xx,param[["weibull"]][[1]], param[["weibull"]][[2]]), lwd = 2, col = "red") 
        lines(xx, dmgumbel(xx,param[["mgumbel"]][[1]], param[["mgumbel"]][[2]]), lwd = 2, col = "pink") 
        lines(xx, dmirlnorm(xx,param[["mirlnorm"]][[1]], param[["mirlnorm"]][[2]]), lwd = 2, col = "purple")
        lines(xx, dlnorm(xx,param[["lnorm"]][[1]], param[["lnorm"]][[2]]), lwd = 2, col = "orange") 
        
        #ensemble weigthed to optimized global weights - Class P
        if(FALSE) {
        est = c(0, 0.4, 0, 0.6, 0) # anemia 
        lines(xx, 
              est[1] * dweibull(xx,param[["weibull"]][[1]], param[["weibull"]][[2]]) +
                est[2] * dgamma(xx,param[["gamma"]][[1]], param[["gamma"]][[2]]) +
                est[3] * dmirgamma(xx,param[["mirgamma"]][[1]], param[["mirgamma"]][[2]]) +
                est[4] * dmgumbel(xx,param[["mgumbel"]][[1]], param[["mgumbel"]][[2]]) +
                est[5] * dmirlnorm(xx,param[["mirlnorm"]][[1]], param[["mirlnorm"]][[2]]), lwd = 4, col = "gray")
        }
        # est = c(0.361, 0.004, 0.035, 0.309, 0.010, 0.281) #anemia
        #ensemble weigthed to optimized global weights - Class Q
        est = c(0, 0.055, 0.001, 0.567, 0.008, 0.369) # WHZ
        lines(xx, 
              est[1] * dweibull(xx,param[["weibull"]][[1]], param[["weibull"]][[2]]) +
                est[2] * dgamma(xx,param[["gamma"]][[1]], param[["gamma"]][[2]]) +
                est[3] * dmirgamma(xx,param[["mirgamma"]][[1]], param[["mirgamma"]][[2]]) +
                est[4] * dmgumbel(xx,param[["mgumbel"]][[1]], param[["mgumbel"]][[2]]) +
                est[5] * dmirlnorm(xx,param[["mirlnorm"]][[1]], param[["mirlnorm"]][[2]]) +
                est[6] * dlnorm(xx,param[["lnorm"]][[1]], param[["lnorm"]][[2]]), lwd = 4, col = "black")
        
        
        #Normal dist 
        lines(xx, dnorm(xx,10, 1), lwd = 1, col = "gray") 
        
        cols = c("green", "blue", "red", "pink", "purple", "orange", "gray", "black")
        legend("topright",legend= c("Gamma","Mirror Gamma","Weibull","Mirror Gumbel", "Mirror Lognormal", "Lognormal", "Z score Ref", "Class Q Ensemble"),col=cols,pch=16,bty="o",ncol=1,cex=1,pt.cex=1.5)
        
        for (thresh in as.numeric(unlist(strsplit(gsub(" ","", unique(df1$t)),split=",")))) {
          segments(thresh, 0, thresh, 1, col = 'gray', lwd=2)
        }
        #segments(t_mild, 0, t_mild, 0.04, col = 'yellow', lwd=2)
        #segments(t_mod, 0, t_mod, 0.04, col = 'orange', lwd=2)
        #segments(t_sev, 0, t_sev, 0.04, col = 'red', lwd=2)
        
        p <- recordPlot()
        p 
      }
      
      
      
      
      
      
       
    }
    
  } # next group 
} # next survey 

if(plot) dev.off()


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
if(topic == "anemia" & distclass == "classP") xi = c(0, 0.4, 0, 0.6, 0) 
est = abs(optim(xi, F, control = list(maxit=max_iter))$par)
est = est * (1 / sum(est))
est = round(est, 6)

est


} #end calc_weights function

F(est)
F(c(0,0.4,0,0.6,0))

#Full test data set
data = test_data
F(est)
#Is it more stable than the weibull? 
F(c(1,0,0,0,0))
#What about 2 dist ens
F(c(0,0.4,0,0.6,0))

#Test of just NHANES
data = test_data[1:58]
F(est)
#Is it more stable than the weibull? 
F(c(1,0,0,0,0))
#What about 2 dist ens
F(c(0,0.4,0,0.6,0))
F(c(0.01,0.39,0,0.6,0))
