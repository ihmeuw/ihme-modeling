

rm(list=ls())


os <- .Platform$OS.type

if (os=="windows") {
  j<- "FILEPATH"
  h <- "FILEPATH"
  my_libs <- NULL
  
  
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- "FILEPATH"
}



## Define Functions



allsexes <- c(1, 2)
allager5 <- 3:4

me_map <- fread("FILEPATH")

for(sss in allsexes){
  
  for(ggg in c("post_gpr_prior", "post_gpr_no_prior", "no_gpr")){
    
    for(aaa in allager5){
      
      dt <- fread("FILEPATH")
      
      if(ggg == "post_gpr_prior" | ggg == "post_gpr_no_prior"){
        
        dt <- dt[!is.na(sample),]
        
        dt <- dt[, c("ga", "bw", "sex", "sample", "exposure", "gpr_ln_mortality_odds", "gpr_ln_odds_std_error")]
        
        setnames(dt, c("gpr_ln_mortality_odds", "gpr_ln_odds_std_error"), c("ln_mortality_odds", "ln_odds_std_err"))
        
      }else{
        
        dt <- dt[, c("ga", "bw", "sex", "sample", "exposure", "ln_mortality_odds", "ln_odds_std_error")]  
      }
      
      dt[, mortality_odds := exp(ln_mortality_odds)]
      
      dt[, mortality_risk := mortality_odds / (1+mortality_odds)]
      
      reference_risk <- dt[ga == 40 & bw == 4500, mortality_risk]
      
      dt[, relative_risk := mortality_risk / reference_risk]
      
      dt[relative_risk < 1, relative_risk := 1]
      
      dt <- merge(dt, me_map[, c("ga", "bw")] )
      
      write.csv(dt, "FILEPATH", na = "", row.names = F)
      
      
    }
    
  }
  
}




########### Create RR draws for USA


convert_to_risk <- function(x){ 
  x = x / (1 + x)  
  return(x) 
}

newvars <- paste0("rr_",c(0:999))

year_list <- c(1990, 1995, 2000, 2005, 2010, 2016)

cause_id_list <- fread("FILEPATH")[["cause_id"]]

for(sss in c(1, 2)){
  
  rr_data_store <- data.table()
  rr_final <- data.table()
  
  for(aaa in 3:4){
    
    rr_data <- fread("FILEPATH")  
    
    ## aaa minus 1 because aaa (ager5) 3 = 0-6 days death, and age_group_id 2 = 0-6 days
    
    rr_data[, cause_id := 000][, age_group_id := aaa - 1][, mortality := 1][, morbidity := 0]
    
    rr_data <- merge( rr_data, me_map[, c("ga", "bw", "categorical_parameter")] )
    
    rr_data <- rr_data[, c("cause_id", "age_group_id", "categorical_parameter", "mortality", "morbidity", "ga", "bw", "sex", "ln_mortality_odds", "ln_odds_std_err")]
    
    setnames(rr_data, "categorical_parameter", "parameter")
    
    rr_data[, (newvars) := as.list(exp(rnorm(1000,ln_mortality_odds,ln_odds_std_err))), by = ln_mortality_odds]
    
    rr_data[, (newvars) := lapply(.SD, "convert_to_risk"), .SDcols = newvars]
    
    mins <- rr_data[ga == 40 & bw == 4500, mget(newvars)]
    
    rr_data[, (newvars) := mget(newvars) / mins]
    
    ## Remove reference category (TMREL)
    rr_data <- rr_data[!(ga == 40 & bw == 4500), ]
    
    rr_data <- rr_data[, -c("ga", "bw", "sex", "ln_mortality_odds", "ln_odds_std_err")]
    
    l <- list(rr_data_store, rr_data)
    
    rr_data_store <- rbindlist(l)
    

  }
  
  for(ccc in cause_id_list){
    
    rr_cause <- copy(rr_data_store)
    rr_cause[, cause_id := ccc]
    
    rr_final <- rbindlist(list(rr_cause, rr_final))
    
    
  }
  
  
  for(yyyy in year_list){
    
    if(sss == 1) sex_id = 1 else sex_id = 2
    
    write.csv(rr_final, "FILEPATH", row.names = F)  
    
  }
  
}




