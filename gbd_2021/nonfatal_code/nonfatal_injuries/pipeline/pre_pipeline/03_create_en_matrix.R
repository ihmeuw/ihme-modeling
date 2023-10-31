
## Parallelized Script for EN_DATA Matrices by e-code and PLATFORMS

##################################################################
# SETUP 
# - Defines file paths
# - Loads Libraries
# - Loads EN data 
# - Defines constants to be used in the script
##################################################################

# Load Libraries
library(foreign)
library(data.table)
library(magrittr)
library(DirichletReg)
# source the custom predict function for Dirichet Regression that allows
# you to input new covariates into the model
source(FILEPATH)

# Load Data
EN_DATA <- read.csv(FILEPATH)

# Constants
N_DRAWS <- 1000
DRAW_COLUMNS <- paste0("draw_", 0:(N_DRAWS - 1))

ECODES <- unique(EN_DATA$ecode)
NCODES <- grep("^N", names(EN_DATA), value = TRUE)
PLATFORMS <- c(0, 1) 


COVARIATES = c("high_income", "inpatient", "age", "sex")
COVARIATE_TABLE <- data.table(
  expand.grid(list("high_income" = c(0, 1),
                   "sex" = c(1, 2),
                   "age" = c(0, 1, seq(5, 95, by = 5))))
)

##################################################################
# CREATE THE MATRICES
##################################################################

for(ecode in ECODES){
  if (ecode == "") {next}
  for(platform in PLATFORMS){
    if (ecode != "inj_medical") {
      print(paste0("Beginning ", ecode, " for platform: ", platform))
      
      data <- EN_DATA[(EN_DATA$inpatient == 1) & (EN_DATA$ecode == ecode),]
      
      empty_ncodes = colSums(data[,NCODES]) == 0
      
      # Run the Dirichlet Regression:
      DRdata = DR_data(data[, NCODES[!empty_ncodes]])
      regression = DirichReg(DRdata ~ high_income + sex + age, data, verbosity = 1)
      
      # Regression results
      converged = regression$optimization[[1]]
      results = confint.DirichletRegModel(regression)
      coeffs = results$coef
      stderror = results$se
      coeffs_ul = unlist(coeffs)
      
      # Create Draws
      #
      # create a data table of parameters and standard errors
      params = data.table(coeffs_ul, stderror)
      draws = matrix(nrow = length(coeffs_ul), ncol = N_DRAWS,NA) # simulate the draws
      
      # take draws from a normal distribution of parameters and standard errors
      for(PARAMETERS in 1:nrow(params)){
        draws[PARAMETERS,] = rnorm(N_DRAWS, params[[PARAMETERS,1]],params[[PARAMETERS,2]])
      }
      
      predict_draws = matrix(nrow = nrow(COVARIATE_TABLE)*length(NCODES[!empty_ncodes]), ncol = N_DRAWS, NA)
      
      # predict based on draws of the parameters
      for(DRAW in 1:N_DRAWS){
        ncoef = draws[,DRAW]
        names(ncoef) = names(coeffs_ul)
        ncoef = relist(ncoef, coeffs)
        
        prediction = predict.DirichletRegModel.newcoef(regression, COVARIATE_TABLE, newcoef = ncoef)
        prediction = prediction %>% data.table
        setnames(prediction, 1:length(NCODES[!empty_ncodes]), NCODES[!empty_ncodes])
        
        prediction = prediction %>% melt
        predict_draws[,DRAW] = prediction[,value]
        if(DRAW %% 25 == 0) cat("Draw ",DRAW," Complete","\n")
      } # finished with draws loop
      
      # create data frame with all information
      predict_draws = predict_draws %>% data.table
      setnames(predict_draws,1:N_DRAWS,DRAW_COLUMNS)
      predict_draws = data.table(ecode = rep(ecode, nrow(predict_draws)),
                                 ncode = rep(NCODES[!empty_ncodes], each = nrow(COVARIATE_TABLE)),
                                 inpatient = rep(platform, nrow(predict_draws)),
                                 COVARIATE_TABLE,
                                 predict_draws)
      
      # add on the zero models IF there are any
      if(!any(empty_ncodes)){
        complete = predict_draws
      } else {
        zero_models = data.table(matrix(nrow = length(NCODES[empty_ncodes])*nrow(COVARIATE_TABLE), ncol = 1000, 0))
        setnames(zero_models, 1:1000,DRAW_COLUMNS)
        zero_models = data.table(ecode = rep(ecode, nrow(zero_models)),
                                 ncode = rep(NCODES[empty_ncodes], each = nrow(COVARIATE_TABLE)),
                                 inpatient = rep(platform, nrow(zero_models)),
                                 COVARIATE_TABLE,
                                 zero_models)
        
        complete = rbind(predict_draws, zero_models)
      }
    } else {
      complete = expand.grid(list("ecode" = "inj_medical",
                                     "ncode" = NCODES,
                                     "inpatient" = PLATFORMS[i],
                                     "high_income" = c(0, 1),
                                     "sex" = c(1, 2),
                                     "age" = c(0, 1, seq(5, 95, by = 5)))) %>% data.table
      
      complete[, (DRAW_COLUMNS) := 0]
      complete[ncode == "N46", (DRAW_COLUMNS) := 1]
    }
    
    if(platform == 1){
      plat <- "inp"
    } else {
      plat <- "otp"
    }
    
    # write CSV with the 1000 draws output
    filepath <- paste0(FILEPATH)
    
    print(paste0("Writing draws file to ", FILEPATH))
    fwrite(complete, FILEPATH, row.names = F, showProgress = TRUE)
    print(FILEPATH"))
    

  }
}

