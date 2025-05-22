######################################################
##                                                  ##
## Description: Stillbirth transformation functions ##
##   1. Stillbirth counts (sb)                      ##
##   2. Stillbirth rates (sbr)                      ##
##   3. Log ratio of stillbirth rate to neonatal    ##
##      mortality rate (log_rat)                    ##
##   4. Neonatal mortality rate (qx)                ##
##   5. Birth numbers (births)                      ##
##                                                  ##
######################################################


sb_to_sbr <- function(sb, births) {
  new <- sb/(sb + births)
  return(new)
}

sbr_to_sb <- function(sbr, births) {
  new <- (sbr * births)/(1 - sbr)
}

lograt_to_sbr <- function(log_rat, qx) {
  new <- exp(log_rat) * qx
  return(new)
}

lograt_to_sb <- function(log_rat, qx, births) {
  sbr <- lograt_to_sbr(log_rat, qx)
  new <- sbr_to_sb(sbr, births)
  return(new)
}

sbr_to_lograt <- function(sbr, qx) {
  new <- log(sbr/qx)
  return(new)
}

sb_to_lograt <- function(sb, qx, births) {
  sbr <- sb/(sb + births)
  new <- sbr_to_lograt(sbr, qx)
  return(new)
}
