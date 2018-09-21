
rm(list=ls())

os <- .Platform$OS.type

if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  my_libs <- NULL
  
  
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- "FILEPATH"
}


library(reshape2)
library(data.table)

allsexes <- c(1, 2)
allager5 <- 3:4

amer_dt <- fread("FILEPATH")
amer_dt[, region := "amer"]
ssa_dt <- fread("FILEPATH")
ssa_dt[, region := "ssa_dt"]
sa_dt <- fread("FILEPATH")
sa_dt[, region := "sa_dt"]

## Average then convert to ln_odds
regions_dt <- rbindlist(list(amer_dt, ssa_dt, sa_dt), use.names = T, fill = T)

no_gpr_1_ager_3_RRs <- fread("FILEPATH")
no_gpr_1_ager_3_RRs[, ager5 := 3]
no_gpr_1_ager_4_RRs <- fread("FILEPATH")
no_gpr_1_ager_4_RRs[, ager5 := 4]
no_gpr_2_ager_3_RRs <- fread("FILEPATH")
no_gpr_2_ager_3_RRs[, ager5 := 3]
no_gpr_2_ager_4_RRs <- fread("FILEPATH")
no_gpr_2_ager_4_RRs[, ager5 := 4]

no_gpr_dt <- rbindlist(list(no_gpr_1_ager_3_RRs, no_gpr_1_ager_4_RRs, no_gpr_2_ager_3_RRs, no_gpr_2_ager_4_RRs), fill = T, use.names = T)
no_gpr_dt[, region := "usa"]

average <- rbindlist(list(regions_dt[, list(sex, ager5, region, relative_risk, ga, bw)], no_gpr_dt[, list(sex, ager5, region, relative_risk, ga, bw)]), use.names = T, fill = T)
average[, relative_risk := lapply(.SD, mean, na.rm = T), by = list(sex, ager5, ga, bw), .SDcols = "relative_risk"]

average <- unique(average[, list(sex, ager5, relative_risk, ga, bw)])

new_no_gpr_dt <- merge(average, no_gpr_dt[, list(sex, ager5, ga, bw, sample, exposure, mortality_risk, ln_odds_std_error)], by = c("sex", "ager5", "ga", "bw"))

min_mortality_risk <- new_no_gpr_dt[ga == 40 & bw == 4500, mortality_risk, by = list(sex, ager5)]
setnames(min_mortality_risk, "mortality_risk", "min_mortality_risk")

new_no_gpr_dt <- merge(new_no_gpr_dt, min_mortality_risk, by = c("sex", "ager5"))

new_no_gpr_dt[, mortality_risk := relative_risk * min_mortality_risk]

new_no_gpr_dt[, mortality_odds :=  mortality_risk / (1-mortality_risk)]

new_no_gpr_dt[, ln_mortality_odds :=  log(mortality_odds)]

new_no_gpr_dt[bw != 500, ga_bw := paste0("gestweek_", ga, "_birthweight_", bw)]
new_no_gpr_dt[bw == 500, ga_bw := paste0("gestweek_", ga, "_birthweight_0", bw)]

for(sss in allsexes){
  
  for(aaa in allager5){
    
    ## Data prep
 
    joint_ln_odds <- new_no_gpr_dt[sex == sss & ager5 == aaa, ]
    joint_ln_odds <- joint_ln_odds[, c("ga", "bw", "ga_bw","sex", "ln_mortality_odds", "ln_odds_std_error", "sample")]
    
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
    
    joint_ln_odds <- joint_ln_odds[, list(ga, bw, sex, ln_mortality_odds, ln_odds_std_error, sample)]
    
    l <- list(prior_ln_odds, joint_ln_odds)
    
    stacked_ln_odds <- rbindlist(l)
    
    write.csv(stacked_ln_odds, "FILEPATH", row.names = F, na = "")
    
  }
  
}

