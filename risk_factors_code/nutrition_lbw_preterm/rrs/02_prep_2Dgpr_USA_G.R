
rm(list=ls())

os <- .Platform$OS.type

if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  my_libs <- NULL
  
  
} else {
  j<- "FILEPATH"
  h<- "FILEPATH"
  my_libs <- "FILEPATH"
}


library(reshape2)
library(data.table)

allsexes <- c(1, 2)
allager5 <- 3:4

for(sss in allsexes){
  
  for(aaa in allager5){
    
    ## Data prep
    
    joint_ln_odds <- fread("FILEPATH")
    setnames(joint_ln_odds, "std_error", "ln_odds_std_error")
    joint_ln_odds[, exposure := sample / sum(sample)]
    
    joint_ln_odds <- joint_ln_odds[, c("ga", "bw", "ga_bw","sex", "ln_mortality_odds", "ln_odds_std_error", "sample", "exposure")]
    
    ga_ln_odds <- fread("FILEPATH")
    setnames(ga_ln_odds, c("ln_oddsratio", "ln_odds_std_error"), c("ga_ln_oddsratio", "ga_ln_odds_std_error"))
    ga_ln_odds <- ga_ln_odds[, c("ga", "ga_ln_oddsratio", "ga_ln_odds_std_error", "intercept")]
    ga_ln_odds[, intercept_ln_std_error := ga_ln_odds[ga_ln_oddsratio == 0, ][["ga_ln_odds_std_error"]] ]
    ga_ln_odds[, ga := as.integer(ga)]
    
    
    bw_ln_odds <- fread("FILEPATH")
    setnames(bw_ln_odds, c("ln_oddsratio", "ln_odds_std_error"), c("bw_ln_oddsratio", "bw_ln_odds_std_error"))    
    bw_ln_odds <- bw_ln_odds[, c("bw", "bw_ln_oddsratio", "bw_ln_odds_std_error")]
    bw_ln_odds[, bw := as.integer(bw)]    
    
    
    ## Delete unnecessary MEs 
    
    mes_to_delete <- fread("FILEPATH")
    joint_ln_odds <- merge(joint_ln_odds, mes_to_delete, all = T, by = c("ga", "bw"))
    joint_ln_odds <- joint_ln_odds[is.na(discard), ] 
    
    ## Copy new dataset for second dataset "prior". Only necessary MEs are kept
    
    prior_ln_odds <- copy(joint_ln_odds[, c("ga", "bw", "sex")])
    prior_ln_odds <- merge(ga_ln_odds, prior_ln_odds, by = c("ga"))    
    prior_ln_odds <- merge(prior_ln_odds, bw_ln_odds, by = c("bw"))
    prior_ln_odds[, sex := sss]
    
    
    ## Calculate ln(odds) data points
    prior_ln_odds[, ln_mortality_odds := intercept + bw_ln_oddsratio + ga_ln_oddsratio]

    
    ## Propogate standard error
    prior_ln_odds[, ln_odds_std_error := ga_ln_odds_std_error + bw_ln_odds_std_error + intercept_ln_std_error]
    
    write.csv(joint_ln_odds, "FILEPATH", row.names = F, na = "") # save for prior
    
    write.csv(joint_ln_odds, "FILEPATH", row.names = F, na = "") # save for final outputs
    
    prior_ln_odds <- prior_ln_odds[, list(ga, bw, sex, ln_mortality_odds, ln_odds_std_error)]
    prior_ln_odds[, sample := NA]  
    
    joint_ln_odds <- joint_ln_odds[, list(ga, bw, sex, ln_mortality_odds, ln_odds_std_error, sample, exposure)]
    
    l <- list(prior_ln_odds, joint_ln_odds)
    
    stacked_ln_odds <- rbindlist(l, use.names = T, fill = T)
    
    write.csv(stacked_ln_odds, "FILEPATH", row.names = F, na = "")
    
  }
  
}

