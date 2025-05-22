## Parallelized Script for EN_DATA Matrices by e-code and PLATFORMS

##################################################################
# SETUP 
# - Defines file paths
# - Loads Libraries
# - Loads EN data 
# - Defines constants to be used in the script
##################################################################

rm(list=ls())

# File Paths
root_dir        <- "FILEPATH"
code_dir        <- "FILEPATH"
input_data_path <- paste0(root_dir, "/FILEPATH.csv")
output_dir      <- paste0(root_dir, "/output_sept")
# Create output dirs
output_dirs <- c(
  paste0(output_dir, "/01_draws/"),
  paste0(output_dir, "/01_draws/inp/"),
  paste0(output_dir, "/01_draws/otp/"),
  paste0(output_dir, "/02_summary/"),
  paste0(output_dir, "/02_summary/inp/"),
  paste0(output_dir, "/02_summary/otp/"),
  paste0(output_dir, "/04_diagnostics/")
)
for ( dir in output_dirs ) {
  if (!file.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
}

# Load Libraries
library(foreign)
library(data.table)
library(magrittr)
library(DirichletReg)
# source the custom predict function for Dirichlet Regression that allows
# you to input new covariates into the model
source(paste0(code_dir,"/FILEPATH.R"))

# Load Data
EN_DATA <- read.csv(input_data_path)
EN_DATA <- subset(EN_DATA, ecode == "inj_suicide")

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
      
      data <- EN_DATA[(EN_DATA$inpatient == platform) & (EN_DATA$ecode == ecode),]
      
      empty_ncodes = colSums(data[,NCODES]) == 0
      
      # Run the Dirichlet Regression:
      # https://cran.r-project.org/web/packages/DirichletReg/vignettes/DirichletReg-vig.pdf
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
    filepath <- paste0(output_dir, "FILEPATH.csv")
    
    print(paste0("Writing draws file to ", filepath))
    fwrite(complete, filepath, row.names = F, showProgress = TRUE)
    print(paste0("Finished with e-code ", ecode, " and PLATFORMS ", plat, " draws file."))
    
    # Summary Statistics
    complete[, mean :=   apply(complete[, ..DRAW_COLUMNS], 1, mean)]
    complete[, ll   :=   apply(complete[, ..DRAW_COLUMNS], 1, quantile, probs=0.025)]
    complete[, ul   :=   apply(complete[, ..DRAW_COLUMNS], 1, quantile, probs=0.975)]
    complete = complete[,c("ncode", COVARIATES, "mean","ll","ul"), with=F]
    
    # write CSV with the summary output
    filepath <- paste0(output_dir, "FILEPATH.csv")
    
    print(paste0("Writing summary file to ", filepath))
    fwrite(complete, filepath, row.names = F, showProgress = TRUE)
    print(paste0("Finished with e-code ", ecode, " and PLATFORMS ", plat, " summary file."))
  }
}

#################################################################################
# FOR MODEL-VETTING PURPOSES, GET THE RAW PROPORTIONS AND COMPARE THEM TO THE
# FITTED VALUES
############################################################################################

dir <- paste0(output_dir, "/02_summary/inp")
files <- grep(".csv", list.files(dir), value = T)

ecodes <- gsub(".csv", "", files)
ecodes <- ecodes[!grepl("en_matrix", ecodes)]
ecodes <- ecodes[ecodes != "inj_medical" & !ecodes %in% grep("summ", ecodes, value = TRUE)]

                                        # define function to read in data for an E-code
read.e <- function(ecode){
    
    path.inp <- paste0(output_dir, "/02_summary/inp/", ecode, ".csv")
    data.inp <- path.inp %>% fread
    
    path.otp <- paste0(output_dir, "/02_summary/otp/", ecode, ".csv")
    data.otp <- path.otp %>% fread
    
    data <- rbind(data.inp, data.otp, use.names = TRUE)
    
    data[, e_code := ecode]
    
    return(data)
}

                                        # get all of the fitted values
fitted <- lapply(ecodes, read.e) %>% rbindlist(use.names = TRUE)
fitted <- fitted[, list(ncode, high_income, inpatient, age, sex, ecode, mean)]
setnames(fitted, "mean", "fitted")

                                        # get the raw proportions from 02_appended.csv
raw <- paste0(input_data_path) %>% fread
NCODES <- grep("N", names(raw), value = TRUE)

                                        # calculate proportions
raw <- raw[, (NCODES) := lapply(.SD, function(x) x / raw[['totals']]), .SDcols = NCODES]

                                        # reshape both data tables and merge
raw <- melt(raw, id.vars = c("age", "sex", "high_income", "inpatient", "ecode"), measure.vars = patterns("^N"))
setnames(raw, c("variable", "value"), c("ncode", "raw"))

diag_dir <- paste0(output_dir, "/04_diagnostics/")
master <- merge(raw, fitted, by = c("age", "sex", "high_income", "inpatient", "ecode", "ncode"))

fwrite(master, paste0(diag_dir, "/FILEPATH.csv"))

