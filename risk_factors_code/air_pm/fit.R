#----HEADER-------------------------------------------------------------------------------------------------------------

# Project: RF: air_pm
# Purpose: Prep relative risk data for fitting of the IER curve

#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  mcmc.cores <- 10 #number of MCMC chains to run in parallel 
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  if (length(arg)==0) {
    arg <- c("power2_simsd_source", 16, 40, 1000) #toggle for targeted run
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  mcmc.cores <- 1 #cant run MCMC chains in parallel on local machine
  arg <- c("power2_simsd_source", 15, 1, 1000)
  
}

# load packages, install if missing
pacman::p_load(data.table, ggplot2, grid, magrittr, parallel, reshape2, rstan, shinystan, stringr)
rstan_options(auto_write=T) #allow you to autosave bare stan program to the HD so that it does not need to be recompiled
options(mc.cores=mcmc.cores) #allow you to execute multiple MCMC in parallel

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# set parameters from input arguments
model <- arg[1]
version <- arg[2] %>% as.numeric
cores.provided <- as.numeric(arg[3])
draws.required <- as.numeric(arg[4])
#***********************************************************************************************************************

#----IN/OUT-------------------------------------------------------------------------------------------------------------
##in##
data.dir <- file.path(home.dir, 'FILEPATH', version)
tmrel.dir <- file.path(home.dir, 'FILEPATH')
#define tmrel file(draws) based on current model version
tmrel.file <- ifelse(version<2,
                     file.path(tmrel.dir, "tmrel_gbd2013.csv"),
                     file.path(tmrel.dir, "tmrel_gbd2015.csv")) %>% fread()
tmrel.mean <-  ifelse(version!=4, mean(tmrel.file[[1]]), 0)

##out##
output.dir <- file.path(home.dir, 'FILEPATH', paste0(version, model))
dir.create(output.dir, recursive=T)
graphs.dir <- file.path(home.dir, 'FILEPATH', paste0(version, model))
dir.create(graphs.dir, recursive=T)
#***********************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#IER functions#
ier.function.dir <- file.path(h_root, 'FILEPATH')
# this pulls the stan model information based on the current model
file.path(ier.function.dir, "models.R") %>% source()
# these helper functions will extract the model results, save them to CSV, and create diagnostic plots
file.path(ier.function.dir, "model_diagnostics.R") %>% source()

#AiR PM functions#
air.function.dir <- file.path(h_root, 'FILEPATH')
# this pulls the miscellaneous helper functions for air pollution
file.path(air.function.dir, "misc.R") %>% source()

#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source()
# this pulls the current locations list
file.path(central.function.dir, "get_locations.R") %>% source()
#***********************************************************************************************************************

#----DEFINE MODEL-------------------------------------------------------------------------------------------------------
##model fitting specifications##
#4 chains and 1000 draws required are the standard run, which will turn these settings out to:
#4 chains of 10k samples - drop first 10k(warmup) of each, and keep each 20th(thin) sample after that
chains <- 4
iter <- draws.required*10
warmup <- iter/2
thin <- warmup/draws.required*chains
#vector of exposure levels that you want to predict for
test.exposure <- c(seq(1,300,1), seq(350,30000,50))

#bring in list of ages and causes to output data for
age.cause <- ageCauseLister(full.age.range = T, gbd.version="GBD2016", lri.version='none')
#note, run full ages when satisfied with model fit
#age.cause <- age.cause[33:34,] #toggle to test only on LRI split
#age.cause <- age.cause[1:3,] #toggle to test only on ihd
#***********************************************************************************************************************

#----PREP---------------------------------------------------------------------------------------------------------------
#prep data for each cause
#define custom function to prep data into a list of model.data objects for STAN
prepData <- function(age.cause.number,
                     pred.values,
                     ...) {
  
  this.cause <- age.cause[age.cause.number, 1]
  this.age <- age.cause[age.cause.number, 2]
  
  # print current loop status
  message("Prepping data for ", paste0(this.cause,' ',this.age))
  
  # load data
  load(file.path(data.dir, paste0(this.cause,'_',this.age,'.RData')))
  
  # test data filters, toggle to test a run of the model with exclusions
  #draw.data <- draw.data[log_se > 0,]
  
  #create a stan model data object
  model.data <- list(name=paste0(this.cause,'_',this.age),
                     NID=draw.data$nid,
                     N=dim(draw.data)[1],
                     log_rr=draw.data$log_rr,
                     log_rr_sd=draw.data$log_se,
                     source=as.numeric(factor(draw.data$source)),
                     source_string=draw.data$source,
                     S=length(unique(draw.data$source)),
                     exposure=draw.data$conc,
                     cf_exposure=draw.data$conc_den,
                     tmrel=draw.data$tmrel[1],
                     T=length(pred.values),
                     test_exposure=pred.values)
  
  return(model.data)
  
}

#loop through and prep all your model datasets for each age/cause pair
all.data <- lapply(1:nrow(age.cause), prepData, pred.values=test.exposure)
#***********************************************************************************************************************

#----MODEL--------------------------------------------------------------------------------------------------------------
#master function that will fit your rStan model using the select parameters
#then it will call on the helper functions to extract draws of the params and predictions (and create diagnostic plots of both)
fitModel <- function(model.data) {
  
  model.name <- model.data$name
  
  message("fitting model for ", model.name, " using the ", model, " model")
  
  #fit the model
  mod_fit <- stan(model_name=model.name,
                  file=returnModel(model, model.data$S)[['model']],
                  data=model.data,
                  iter=iter, warmup=warmup, chains=chains, thin=thin,
                  init=returnModel(model, model.data$S)[['initialize']])
  
  
  pdf(file.path(graphs.dir, paste0(model.name, '_all_plots.pdf')), width=12, height=9)
  
  draws <- extractDraws(mod_fit,
                        name=model.name,
                        sources=unique(sort(model.data$source_string)),
                        model.parameters=returnModel(model, model.data$S)[['parameters']])
  
  pred <- extractPred(mod_fit,
                      name=model.name)
  
  dev.off()
  
  output <- list("draws"=draws, "pred"=pred)
  
  return(output)
  
}

#loop over your list of datasets to fit a model and run diagnostics for every cause/age pair
all.draws <- mclapply(all.data,
                      fitModel,
                      mc.cores=(cores.provided)/mcmc.cores)#divide cores by # of MCMC chains already running parallel
#***********************************************************************************************************************