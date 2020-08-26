





rm(list = ls())

library(data.table)
library(dplyr)
library(foreign)
library(stringr)

library(haven)
library(RMySQL)
library(parallel)



if (Sys.info()[1] == "Linux") {
  j <- "FILENAME"
  h <- paste0("FILENAME", Sys.info()[7])
  k <- "FILENAME"
} else if (Sys.info()[1] == "Windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "K:"
}





source(paste0(k, "FILEPATH"))





loadCauses <- function() {
  db_con = fread(paste0(j, "FILEPATH"))

  
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  df <- dbGetQuery(con, sprintf("SQL"))
  
  
  
  dbDisconnect(con)
  return(data.table(df))
}


loadChildCauses <- function(parent_id) {
  db_con = fread(paste0(j, "FILEPATH"))

  
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  df <- dbGetQuery(con, sprintf("SQL",
                                paste0(parent_id, collapse = ",")))
  dbDisconnect(con)
  return(data.table(df))
}

odbc <- ini::read.ini("FILEPATH")
con_def <- 'clinical_NAME'


get_bundle_restrictions <- function(){
  myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                              host = odbc[[con_def]]$SERVER,
                              username = odbc[[con_def]]$USER,
                              password = odbc[[con_def]]$PASSWORD)
  df <- dbGetQuery(myconn, sprintf("SQL"))
  chk_df <- dbGetQuery(myconn, sprintf("SQL"
  ))
  dbDisconnect(myconn)

  
  stopifnot(length(unique(chk_df$bundle_id)) == length(unique(df$bundle_id)))
  return(data.table(df))
}

applyRestrictions <- function(df, application_point) {
  
  restrict <- get_bundle_restrictions()
  restrict[, c('map_version') := NULL]


  
  stopifnot(!0.1 %in% unique(restrict$yld_age_start))

  
  df$age_end = df$age_start + 4
  df[age_start == 0]$age_end = 1
  df[age_start == 1]$age_end = 4

  
  pre <- nrow(df)
  df <- merge(df, restrict, all.x = TRUE, by = 'bundle_id')
  stopifnot(pre == nrow(df))

  if (application_point == "start") {
    case_cols <- names(df)[grep("_cases$", names(df))]
  } else if (application_point == "end") {
    
    case_cols <- names(df)[grep("value$", names(df))]

  } else {break}

  
  if (application_point == "start") {
    df[ which(df$male == 0 & df$sex_id == 1), case_cols] <- NA
    df[ which(df$female == 0 & df$sex_id == 2), case_cols] <- NA
    stopifnot(sum(df[bundle_id == 74 & sex_id == 1]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
    stopifnot(sum(df[bundle_id == 198 & sex_id == 2]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
  } else if (application_point == "end") {
    df[ which(df$male == 0 & df$sex == 1), case_cols] <- NA
    df[ which(df$female == 0 & df$sex == 2), case_cols] <- NA
    stopifnot(sum(df[bundle_id == 74 & sex == 1]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
    stopifnot(sum(df[bundle_id == 198 & sex == 2]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)
  }

  df[ which(df$age_end < df$yld_age_start), case_cols] <- NA
  df[ which(df$age_start > df$yld_age_end), case_cols] <- NA

  
  stopifnot(sum(df[bundle_id == 292 & age_start < 20]$otp_any_indv_cases, na.rm = TRUE) == 0)
  stopifnot(sum(df[bundle_id == 92 & age_start > 10]$inp_otp_any_claims_cases, na.rm = TRUE) == 0)


  
  df[, c('male', 'female', 'yld_age_start', 'yld_age_end', 'age_end') := NULL]

  return(df)
}


get_parent_injuries <- function(df) {
  pc_injuries <- fread("FILEPATH")
  setnames(pc_injuries, 'Level1-Bundle ID', 'bundle_id')

  pc_df <- data.table()
  for (parent in pc_injuries[parent == 1]$e_code) {
    children <- pc_injuries[child == 1]$`baby sequela`[grepl(parent, pc_injuries[child == 1]$`baby sequela`)]
    temp_pc_df = pc_injuries[`baby sequela` %in% children]
    temp_pc_df$parent_bid <- pc_injuries$bundle_id[pc_injuries$e_code == parent]
    pc_df = rbind(pc_df, temp_pc_df)
  }
  pc_df <- pc_df[, c('bundle_id', 'parent_bid'), with = FALSE]
  
  inj_rows <- nrow(df[bundle_id %in% pc_df$bundle_id])
  
  pc_df = merge(pc_df, df, by = 'bundle_id', all.x = TRUE, all.y = FALSE)

  all_children <- unique(pc_df$bundle_id)
  all_parents <- unique(pc_df$parent_bid)
  
  pc_df[, bundle_id := NULL]
  setnames(pc_df, 'parent_bid', 'bundle_id')
  

  df = rbind(df, pc_df)
  
  return(df)
}


newcollapser <- function(df) {
  
  
  case_cols <- names(df)[grep("_cases$", names(df))]

  df = df[, lapply(.SD, sum, na.rm = TRUE), .SDcols = case_cols, by = .(age_start, sex_id, bundle_id)]

  return(df)
}






cause_map <- loadCauses()




create_scalars <- function(run_id) {

  rank_name <- "bundle_id"
  
  counter <- 0

  
  final_df = data.table()

  
  
  
  df <- fread(paste0("FILEPATH"))

  
  df <- dcast(df, age_end + age_start + bundle_id + location_id + sex_id + year_end + year_start ~ estimate_type, value.var = 'val')

  
  pre_shape <- nrow(df)
  df <- get_parent_injuries(df)
  stopifnot(pre_shape < nrow(df))

  df <- newcollapser(df)

  df <- applyRestrictions(df, application_point = "start")

  
  anence_bundles <- c(610, 612, 614)
  chromo_bundles <- c(436, 437, 438, 439, 638)
  poly_synd_bundles <- c(602, 604, 606, 799)
  cong_bundles <- c(622, 624, 626, 803)
  cong2_bundles <- c(616, 618)

  neonate_bundles <- c(80, 81, 82, 500)

  anence <- df[bundle_id %in% anence_bundles]
  chromo <- df[bundle_id %in% chromo_bundles]
  poly_synd <- df[bundle_id %in% poly_synd_bundles]
  cong <- df[bundle_id %in% cong_bundles]
  cong2 <- df[bundle_id %in% cong2_bundles]
  neonate <- df[bundle_id %in% neonate_bundles]

  anence$bundle_id <- 610
  chromo$bundle_id <- 2000
  poly_synd$bundle_id <- 607
  cong$bundle_id <- 4000
  cong2$bundle_id <- 5000
  neonate$bundle_id <- 6000

  anence <- newcollapser(anence)
  chromo <- newcollapser(chromo)
  poly_synd <- newcollapser(poly_synd)
  cong <- newcollapser(cong)
  cong2 <- newcollapser(cong2)
  neonate <- newcollapser(neonate)

  
  child_neo_causes <- unique(loadChildCauses(380))  
  
  cause_bundle_map <- cause_map[cause_id %in% child_neo_causes$cause_id]
  
  neonate_bundles <- unique(df$bundle_id)[unique(df$bundle_id) %in% cause_bundle_map$bundle_id]

  chromo_buns <- rep(c(436, 437, 438, 638), 1, each = nrow(chromo))
  cong_buns <- rep(cong_bundles, 1, each = nrow(cong))
  cong2_buns <- rep(cong2_bundles, 1, each = nrow(cong2))
  neonate_buns <- rep(neonate_bundles, 1, each = nrow(neonate))

  chromo <- do.call("rbind", replicate(length(unique(chromo_buns)), chromo, simplify = FALSE))
  cong <- do.call("rbind", replicate(length(cong_bundles), cong, simplify = FALSE))
  cong2 <- do.call("rbind", replicate(length(cong2_bundles), cong2, simplify = FALSE))
  neonate <- do.call("rbind", replicate(length(neonate_bundles), neonate, simplify = FALSE))

  chromo$bundle_id <- chromo_buns
  cong$bundle_id <- cong_buns
  cong2$bundle_id <- cong2_buns
  neonate$bundle_id <- neonate_buns

  
  df <- df[!bundle_id %in% c(610, 436, 437, 438, 638, 607, 622, 624, 626, 803, 616, 618, neonate_bundles)]

  df <- rbind(df, anence, chromo, poly_synd, cong, cong2, neonate, fill = TRUE)

  for (cause in unique(df[[rank_name]])) {
    
    all_null  = NULL
    type_null = NULL
    sex_null = NULL
    
    counter <- counter + 1

    print(paste0(" ", round((counter/length(unique(df[[rank_name]])))*100, 2), "% Complete"))

    
    dummy_grid <- expand.grid('age_start' = c(0,1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95),
                              'sex' = c(1,2))
    df_sub <- df[bundle_id == cause]
    setnames(df_sub, "sex_id", "sex")
    
    df_sub <- merge(dummy_grid, df_sub, all.x = TRUE)
    df_sub <- data.table(df_sub)

    cases_df <- copy(df_sub)

    
    
    

    
    
    
    df_sub <- df_sub[, outpatient := get("otp_any_indv_cases") / get("otp_any_claims_cases")]

    
    df_sub <- melt(df_sub[, c("sex", "age_start", "outpatient"), with = FALSE],
                   id.vars = c("sex", "age_start"),
                   measure.vars = c("outpatient"),
                   variable.name = "type",
                   value.name = "value")
    
    df_sub$smoothed_value <- as.numeric(NA)

    
    
    
    corr_level = "outpatient"

    for (sex_id in c(1,2)) {
      
      if ( cause %in% c(79, 3419, 646, 74, 75, 423, 77, 422, 667, 76) & rank_name == "bundle_id") {
        if (sex_id == 1) {next}

        
        dat = df_sub[type == corr_level & sex == sex_id]
        dat$value[is.infinite(dat$value)] <- NA
        smoothobj <- loess(log(value) ~ age_start,
                           data = dat,
                           span = 0.6)

        
        df_sub[type == corr_level & sex == 2, smoothed_value := exp(predict(smoothobj, dat))]

        if (sex_id == 2) {
          
          
          max_age = max(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
          upper_val <- df_sub$smoothed_value[df_sub$age_start==max_age & df_sub$sex==sex_id & df_sub$type==corr_level]
          
          df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start>max_age & df_sub$type==corr_level] <- upper_val

          
          min_age <- min(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
          lower_val <- df_sub$smoothed_value[df_sub$age_start==min_age & df_sub$sex==sex_id & df_sub$type==corr_level]
          df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start<min_age & df_sub$type==corr_level] <- lower_val
        }

      } else {
        
        dat <- df_sub[type == corr_level & sex == sex_id]
        dat$value[is.infinite(dat$value)] <- NA
        dat$value[dat$value == 0] <- NA
        
        if (sum(!is.na(dat$value)) <= 2) {
          all_null <- cause
          type_null <- c(type_null, corr_level)
          sex_null <- c(sex_null, sex_id)

          next
        }
        else if (sum(!is.na(dat$value)) <= 5) {
          smoothobj <- loess(log(value) ~ age_start, data = dat, span = 1)
          
          df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
        }
        else if (sum(!is.na(dat$value)) <= 10) {
          smoothobj <- loess(log(value) ~ age_start, data = dat, span = .75)
          
          df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
        } else {
          smoothobj <- loess(log(value) ~ age_start,
                             data = dat,
                             span = 0.5)
          
          df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
        }
        
        
        max_age = max(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex == sex_id & df_sub$type == corr_level])
        upper_val <- df_sub$smoothed_value[df_sub$age_start == max_age & df_sub$sex == sex_id & df_sub$type == corr_level]

        df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex == sex_id & df_sub$age_start > max_age & df_sub$type == corr_level] <- upper_val

        
        min_age <- min(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
        lower_val <- df_sub$smoothed_value[df_sub$age_start==min_age & df_sub$sex==sex_id & df_sub$type==corr_level]
        df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start<min_age & df_sub$type==corr_level] <- lower_val

      } 
    } 
    

    if (cause %in% all_null) {
      
      
      for (asex in sex_null) {
        if ("outpatient" %in% type_null) {
          print("Hey we made it into this outpatient if statement")
          df_sub[sex == asex & type == "outpatient"]$smoothed_value <-
            sum(cases_df[sex == asex]$otp_any_indv_cases[!is.na(cases_df[sex == asex]$otp_any_claims_cases) &
                                                           cases_df[sex == asex]$otp_any_claims_cases != 0], na.rm = TRUE) /
            sum(cases_df[sex == asex]$otp_any_claims_cases, na.rm = TRUE)
        }
      } 
    } 

    
    df_sub[type == "injury_cf" & smoothed_value > 1]$smoothed_value <- 1
    df_sub[type == "indv_cf" & smoothed_value > 1]$smoothed_value <- 1
    
    df_sub[type == "outpatient" & smoothed_value > 1]$smoothed_value <- 1

    
    df_sub <- df_sub[is.infinite(smoothed_value), smoothed_value := NA]

    
    df_sub[[rank_name]] = cause
    
    final_df = rbind(final_df, df_sub)

  } 

  
  final_df = applyRestrictions(final_df, application_point = "end")

  write_fold = paste0("FILENAME", run_id, "FILENAME")

  
  save_path <- paste0(write_fold, "FILEPATH")
  print(paste0("Saving file ", save_path, "..."))
  write.csv(final_df, save_path, row.names = FALSE)
  
  save_path <- paste0(write_fold, "FILEPATH")
  print(paste0("Saving file ", save_path, "..."))
  write.csv(final_df, save_path, row.names = FALSE)

  
  wide_df <- data.table::dcast(final_df, sex+age_start+bundle_id~type, value.var = 'smoothed_value')
  wide_df <- wide_df[, c("sex", "age_start", "bundle_id", "outpatient"), with = FALSE]
  save_path <- paste0("FILEPATH")
  print(paste0("Saving file ", save_path, "..."))
  write.csv(wide_df, save_path, row.names = FALSE)

} 






run_id <- 3
create_scalars(run_id=run_id)
