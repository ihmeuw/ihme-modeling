#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: air_pm
# Purpose: Return model code for various models in STAN
#***********************************************************************************************************************

returnModel <- function(model, source.num, cohort.num) {
  
  home <- "FILEPATH"
  
  root <- ifelse(Sys.info()["sysname"] == "Linux", "FILEPATH", home)
  model.dir <- file.path(root, "FILEPATH")

  if (model == "power2") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = 50, beta = 0.1, gamma = 0.25)
    }
    
    parameters <- c('alpha', 'beta', 'gamma')
    
  }
  
  if (model == "power2_simsd") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = 50, beta = 0.1, gamma = 0.25, delta = 5)
    }
    
    parameters <- c('alpha', 'beta', 'gamma', 'delta')
    
  }
  
  if (model == "power2_simsd_source") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = 50, beta = 0.1, gamma = 0.25, delta = rep(5, source.num))
    }
    
    parameters <- c('alpha', 'beta', 'gamma', 'delta')
    
  }
  
  if (model == "simplified") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(beta = .5, rho = 0.25)
    }
    
    parameters <- c('beta', 'rho')
    
  }
  
  if (model == "simrel") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(beta = .5, rho = 0.25, tmrel = 0.25)
    }
    
    parameters <- c('beta', 'rho', 'tmrel')
    
  }
  
  if (model == "simsd") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(beta = .5, rho = 0.25, tmrel = 0.25, delta = rep(5,source.num))
    }
    
    parameters <- c('beta', 'rho', 'tmrel', 'delta')
    
  }
  
  if (model == "loglin") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(beta = .5, gamma = 0.25)
    }
    
    parameters <- c('beta', 'gamma')
    
  }
  
  if (model == "power2_simsd_constraints") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = 50, beta = 0.1, gamma = 0.25, delta = 5)
    }
    
    parameters <- c('alpha', 'beta', 'gamma', 'delta')
    
  }
  
  if (model == "power2_simsd_source_free_gamma") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = .5, beta = 0.1, gamma = 1, delta = rep(5, source.num))
    }
    
    parameters <- c('alpha', 'beta', 'gamma', 'delta')
    
  }
  
  if (model == "linear") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(b = 0.1)
    }
    
    parameters <- c('b')
    
  }
  
  if (model == "power2_simsd_source_phaseII") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = 50, gamma = 0.25, delta = rep(5, source.num))
    }
    
    parameters <- c('alpha', 'gamma', 'delta')
    
  }
  
  if (model == "power2_simsd_source_re") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = 50, beta = 0.1, gamma = 0.25, delta = rep(5, source.num), 
           sigma_alpha = 0.1, alpha_c=rep(50,cohort.num),
           sigma_beta = 0.1, beta_c=rep(0.1,cohort.num),
           sigma_gamma = 0.1, gamma_c=rep(0.25,cohort.num))
    }
    
    parameters <- c('alpha', 'beta', 'gamma', 'delta')
    
  }
  
  if (model == "power2_simsd_cohort") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = 50, beta = 0.1, gamma = 0.25, delta = rep(5, cohort.num))
    }
    
    parameters <- c('alpha', 'beta', 'gamma', 'delta')
    
  }
  
  if (model == "power2_simsd_source_priors") {
    
    # read in model file
    model.file <- file.path(model.dir, paste0("model_", model, ".stan"))
    
    # prime the initialization function with some numbers you think are reasonable to start
    # the step shouldnt matter that much as long as the model starts to converge
    init.f <- function() {
      list(alpha = 50, beta = 0.1, gamma = 0.25, delta = rep(5, source.num))
    }
    
    parameters <- c('alpha', 'beta', 'gamma', 'delta')
    
  }
  
  
    
  model.info <- list("model"=model.file, "initialize"=init.f, 'parameters'=parameters)
  
  return(model.info)
  
  }
  