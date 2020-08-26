os <- .Platform$OS.type

repo = "FILEPATH"
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)
library(copula)
library(VineCopula)
library(actuar, lib.loc = my_libs)

source("FILEPATH/edensity.R")
source("FILEPATH/pdf_families.R")
source(file.path("FILEPATH/lbwsg_copula_functions.R"))

fit_copula <- function(data, family = NA, param_filepath){
  
  fit.data <- copy(data)
  fit.data <- fit.data[, lapply(.SD, pobs)]
  
  RVM <- RVineStructureSelect(fit.data, familyset = family)
   
  if(!is.na(param_filepath)) { 
    
    pairs <- combn(1:ncol(fit.data), m = 2)
    
    cop_params <- lapply(1:ncol(pairs), function(j){
      
      x <- pairs[,j][1]
      y <- pairs[,j][2]
      
      y_x <- data.table(vars = paste0("var", y, "_var", x), 
                        family = RVM$family[y,x], 
                        par1 = RVM$par[y,x], 
                        par2 = RVM$par2[y,x], 
                        tau = RVM$tau[y,x], 
                        total_AIC = RVM$AIC, 
                        total_BIC = RVM$BIC)
      
      return(y_x)
      
    }) %>% rbindlist
    
    cop_params[, N := nrow(fit.data)]
    
    saveRDS(cop_params, param_filepath) 
    
  } 
  
  return(RVM)
  
}


data <- readRDS("FILEPATH")

copula <- fit_copula(data = data[, .(var1 = gestweek, var2 = bw)], param_filepath = "FILEPATH", family = NA)
saveRDS(copula, "FILEPATH")