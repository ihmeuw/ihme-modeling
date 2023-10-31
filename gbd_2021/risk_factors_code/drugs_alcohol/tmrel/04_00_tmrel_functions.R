################
# 4.0. TMREL functions
################

agg_burden <- function(file, avg = F, decade = F, for_viz = T){
  
  message(file)
  
  single_cause <- fread(paste0(path, file))
  
  if (for_viz){
    single_cause <- single_cause[sex_id == 3 | location_id == 1 | age_group_id %in% c(22,27)]
  }
  
  setnames(single_cause, old = c("variable", "value"), new = c("draw", "dalys"))
  
  if (avg == F & decade == T){
    
    single_cause[year_id %in% c(1990:1999), decade := "1990s"]
    single_cause[year_id %in% c(2000:2009), decade := "2000s"]
    single_cause[year_id %in% c(2010:2019), decade := "2010s"]
    
    single_cause[,dalys_decade := mean(dalys), by = c("location_id", "age_group_id", "sex_id", "cause_id", "decade", "draw")]
    
    single_cause[,c("year_id", "dalys")] <- NULL
    single_cause <- unique(single_cause)
    setnames(single_cause, old = c("dalys_decade", "decade"), new = c("dalys", "year_id"))
  } 
  
  if (avg){
    single_cause <- copy(single_cause) %>%
      .[,mean := mean(dalys), by = c(specific, "cause_id")] %>%
      .[,lower := quantile(dalys, 0.025), by = c(specific, "cause_id")] %>%
      .[,upper := quantile(dalys, 0.975), by = c(specific, "cause_id")] %>%
      .[,draw := NULL] %>%
      .[,dalys := NULL] %>% unique
  }
  return(single_cause)
}

grab_and_format <- function(filepath){
  
  df <- fread(filepath)
  df <- df[, .(exposure, draw, rr)]
  
  c <- regmatches(filepath, regexpr("\\d{3}", filepath))
  s <- as.numeric(gsub("^rr_\\d{3}|.csv|_", "", filepath))
  
  df <- df[, `:=`(sex_id = s, cause_id = c)]
  
  if (is.na(s)){
    df$sex_id <- NULL
  }
  
  return(df)
}

weighted_rr <- function(l, sex, age, for_viz = F){
  
  message(l )
  
  total <- fread('FILEPATH')
  
  if (for_viz){
    total <- total[sex_id == 3 | location_id == 1 | age_group_id %in% c(22,27)]
  }
  return(total)
}
