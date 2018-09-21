##########################################################
## Database connections not elsewhere defined (i.e., not 
## a shared function)
##########################################################
# Dependencies


et.dbQuery <- function(host_name, query_string) {
  # MySQL wrapper
  con <- dbConnect(dbDriver("MySQL"), 
                   username="USERNAME", 
                   password="PASSWORD", 
                   host=paste0("ADDRESS"))
  results <- dbGetQuery(con, query_string)
  dbDisconnect(con)
  return(data.table(results))
}

et.getAgeWeights <- function(gbdrid = 4, agids = c(2:20, 30:32, 235)) {
  # Return population age-standard weights for particular ages, ensuring they sum to 1
  df <- et.dbQuery("SQL QUERY")
  
  if (sum(df$age_group_weight_value) < 0.9999) {
    warning("Age weights do not some to 1 for ages selected, RESCALING! User should consider whether this is desirable...")
    df <- df[, age_group_weight_value := age_group_weight_value / sum(age_group_weight_value)]
  } else if (sum(df$age_group_weight_value) > 1.0001) stop("You've requested a series of ages that are incompatible (sum to ", sum(df$age_group_weight_value), ")")
  return(df)
}

et.getAgeMetadata <- function(agids = c(2:20, 30:32, 235)) {
  # Return population age-standard weights for particular ages, ensuring they sum to 1
  df <- et.dbQuery("SQL QUERY")
  return(df)
}
