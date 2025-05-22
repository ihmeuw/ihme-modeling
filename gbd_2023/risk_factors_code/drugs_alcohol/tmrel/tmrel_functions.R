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
    single_cause[year_id %in% c(2020:2029), decade := "2020s"]
    
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

weighted_rr <- function(d = NULL, l = NULL, s = NULL, a = NULL , for_viz = F){
  
  
  message(d)
  total <- fread(paste0(input_path,"/",d,".csv"))
  total <- total[sex_id == s & age_group_id == a]
  
  if (for_viz){
    total <- total[sex_id == 3 | location_id == 1 | age_group_id %in% c(22,27)]
  }
  return(total)
}


clean_daly_draws <- function(df){
  
  cause_names[,c("acause", "cause_description")] <- NULL
  cause_names$cause_count  <- c(1:nrow(cause_names))
  
  df <- merge(df, cause_names, by = "cause_id", all.x = T) %>%
    merge(., age_name, by = "age_group_id", all.x = T) %>%
    merge(., sex_name, by = "sex_id", all.x = T) %>%
    merge(., locs[,c("location_id", "location_name")], by = "location_id", all.x = T) %>%
    .[,age_count := age_group_id - 7] %>%
    .[age_group_id %in% c(30:32),age_count := age_group_id - 16] %>%
    .[age_group_id %in% c(235),age_count := 17] %>%
    .[,cause_group := cause_name] %>%
    .[cause_id %in% c(297, 322), cause_group := "TB & LRI"]%>%
    .[cause_name %like% c("tuberc"), cause_group := "TB & LRI"]%>%
    .[cause_name %like% c("cancer"), cause_group := "Cancers"]%>%
    .[cause_name %like% c("Cirrhosis"), cause_group := "Cirrhosis"]%>%
    .[cause_name %like% c("injuries") | cause_id %in% c(718, 724), cause_group := "Injuries"]%>%
    .[cause_name %like% c("Physical"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("Drowning"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("heat"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("Poison"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("enomous"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("harm"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("mechanical"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("iolence"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("Falls"), cause_group := "Injuries"]%>%
    .[cause_name %like% c("Ischemic") | cause_id %in% c(496, 498, 500), cause_group := "Cardiovascular Disease"]%>%
    .[cause_name %in% c("Idiopathic epilepsy", "Pancreatitis"), cause_group := "Other causes"]%>%
    .[cause_name %like% c("Diabetes"), cause_group := "Diabetes mellitus type 2"]
  
  df <- df[!cause_name %like% "forces of nature"]
  
  return(df)  
}

