# Util script used in ntd_ebola validate parameters

####################################################################
#' [00_validate_parameters]
####################################################################

#' [Zero negatives in vector]
# This function was used to zero negative draws
zero_negs <- function(vector){
  for (i in 1:length(vector)){
    if (vector[i] < 0){
      vector[i] <- 0
    }
  }
  return(vector)
}

tabler <- function(matrix, group){
  # takes rnorm output and forms a table with col names and label, as a matrix
  matrix        <- as.data.table(t(matrix))
  names(matrix) <- paste0("draw_", 0:999)
  col           <- data.table(group = group)
  table         <- cbind(col, matrix)
  return(table)
}

#' [Make cfr draws]
# Reads cfr scalars and outputs draws into run folder's draw folder
# Do not need if have cdc case data
make_cfr_draws <- function(scalar_file, epi_demo, draws_dir){
  
  cfr_raw <- fread(scalar_file) 
  
  under_15_mean <- cfr_raw$CFR_mean[cfr_raw$Grouping=="<15 yrs"]
  under_15_se   <- (cfr_raw$CFR_UCI[cfr_raw$Grouping=="<15 yrs"]-cfr_raw$CFR_LCI[cfr_raw$Grouping=="<15 yrs"])/(2*1.96)
  
  over_45_mean  <- cfr_raw$CFR_mean[cfr_raw$Grouping==">=45 yrs"]
  over_45_se    <- (cfr_raw$CFR_UCI[cfr_raw$Grouping==">=45 yrs"]-cfr_raw$CFR_LCI[cfr_raw$Grouping==">=45 yrs"])/(2*1.96)
  
  middle_mean   <- cfr_raw$CFR_mean[cfr_raw$Grouping=="15-44 yrs"]
  middle_se     <- (cfr_raw$CFR_UCI[cfr_raw$Grouping=="15-44 yrs"]-cfr_raw$CFR_LCI[cfr_raw$Grouping=="15-44 yrs"])/(2*1.96)
  
  all_mean      <- cfr_raw$CFR_mean[cfr_raw$Grouping == "All cases"]
  all_se        <- (cfr_raw$CFR_UCI[cfr_raw$Grouping=="All cases"]-cfr_raw$CFR_LCI[cfr_raw$Grouping=="All cases"])/(2*1.96)
  
  #for each of the GBD age_group_ids, create 1,000 draws all-age added too
  age_sex         <- expand.grid(age_group_id = c(epi_demographics$age_group_id, 22), sex_id = epi_demographics$sex_id)
  blank_cfr_param <- as.data.table(matrix(0, nrow = nrow(age_sex), ncol = 1000))
  
  names(blank_cfr_param)<- paste0("draw_", 0:999)
  cfr_param<-cbind(age_sex, blank_cfr_param)
  
  
  under_15_draw <- rnorm(1000, under_15_mean, under_15_se)
  over_45_draw  <- rnorm(1000, over_45_mean, over_45_se)
  middle_draw   <- rnorm(1000, middle_mean, middle_se)
  all_draw      <- rnorm(1000, all_mean, all_se)
  
  for(j in 1:nrow(cfr_param)){
    if(cfr_param$age_group_id[j] %in% c(2,3,4,5,6,7)){
      cfr_param[j,3:1002]<-under_15_draw
    }
    if(cfr_param$age_group_id[j] %in% c(8,9,10,11,12,13)){
      cfr_param[j,3:1002]<-middle_draw
    }
    if(cfr_param$age_group_id[j] %in% c(14,15,16,17,18,19,20,30,31,32,235)){
      cfr_param[j,3:1002]<-over_45_draw
    }
    if(cfr_param$age_group_id[j] %in% c(22)){
      cfr_param[j,3:1002]<-all_draw
    }
  }
  
  fwrite(cfr_param, "FILENAME"), row.names = FALSE)
}

#' [Make acute draws]
# Reads in scalars and creates draws file
# Mean is the mean, se is calculated by upper - lower / (2 * 1.96)
# Normal distribution with mean and se

make_acute_draws <- function(scalar_file, draws_dir){
  
  acute_duration_raw <- fread(scalar_file)
  
  acute_death_mean     <- acute_duration_raw[grouping == "onset_to_death", mean]
  acute_death_se       <- acute_duration_raw[grouping == "onset_to_death", mean_upper - mean_lower]/(2*1.96)
  
  acute_death_draw     <- rnorm(1000, acute_death_mean, acute_death_se) / 365          #parameters in day space - need to be in years
  
  acute_survival_mean  <- acute_duration_raw[grouping == "onset_to_discharge", mean]
  acute_survival_se    <- acute_duration_raw[grouping == "onset_to_discharge", mean_upper - mean_lower]/(2*1.96)
  
  acute_survival_draw  <- rnorm(1000, acute_survival_mean, acute_survival_se) / 365   #parameters in day space - need to be in years
  acute_duration       <- as.data.table(rbind(acute_death_draw, acute_survival_draw))
  names(acute_duration)<- paste0("draw_", 0:999)
  
  col                  <- data.table(group = c("acute_death", "acute_survival"))        
  acute_duration       <- cbind(col, acute_duration)
  
  fwrite(acute_duration, paste0(draws_dir, "FILEPATH"), row.names = FALSE)
}

#' [Make chronic draws]

make_chronic_draws <- function(scalar_file, draws_dir){
  # Reads in scalar and creates draws
  
  evd_data <- fread(scalar_file)
  evd_data <- evd_data[study != 'Wendo_2001']    #exclude Wendo 2001 data due to limited metadata and therefore outlier
  
  #using means
  dataset  <- data.table(y = NA, x = NA)
  
  for (i in 1:nrow(evd_data)){
    input_expanded   <-  cbind( y = mean(c(evd_data$value_min[i], evd_data$value_max[i])), x= log(mean(c(evd_data$time_start[i], evd_data$time_end[i]))))
    dataset          <-  rbind(dataset, input_expanded)
  }
  
  dataset        <- na.omit(dataset)
  model          <- lm(y~x, data=dataset)
  
  #as per INTERNAL-REFERENCE  duration
  years          <- seq(0, 20, 0.05)
  
  #for each year, calculate the value with CI
  new_data       <- data.frame(x=log(years))
  
  intercept      <-  coef(summary(model))[1,1]
  intercept_ste  <-  coef(summary(model))[1,2]
  mean_ste       <-  coef(summary(model))[2,2]
  mean           <-  coef(summary(model))[2,1]
  
  mean_draws     <-  runif(1000, mean-1.96*mean_ste, mean+1.96*mean_ste)
  intercept_draws<-  runif(1000, intercept-1.96*intercept_ste, intercept+1.96*intercept_ste)
  
  results        <-  list()  
  result_draw    <-  data.frame(x=years,y=rep(NA,length(years) ))
  
  for (i in 1:1000){
    
    for(j in 1:nrow(result_draw)){
      result_draw$y[j]  <-  (mean_draws[i]*log(result_draw$x[j]))+intercept_draws[i]
      result_draw$y[1]  <-1
    }
    results[[i]]<-result_draw
  }
  
  for (i in 1:1000){
    for (j in 1:nrow(results[[i]]-1)){
      
      results[[i]]$value[j]  <-  ((results[[i]]$x[j+1]-results[[i]]$x[1])*(results[[i]]$y[j]-results[[i]]$y[j+1]))/2
      
    }
    results[[i]]$value[nrow(results[[i]])]  <-  0
  }
  duration  <-  NA
  
  for (i in 1:1000){
    duration[i]  <-  sum(results[[i]]$value)
  }
  
  mean(duration)
  quantile(duration, probs=c(0.05, 0.5,0.95))
  
  central_tendency<-result_draw
  for(j in 1:nrow(central_tendency)){
    central_tendency$y[j]  <-  (mean*log(central_tendency$x[j]))+intercept
    central_tendency$y[1]  <-1
  }
  
  
  year1<-duration
  for(i in 1:length(duration)){
    if(year1[i]>1){year1[i]<-1}
  }
  
  year2<-duration-1
  for(i in 1:length(duration)){
    if(year2[i]<0){year2[i]<-0}
  }
  
  year1 <- tabler(year1, "year1")
  year2 <- tabler(year2, "year2")
  
  write.csv(year1, "FILEPATH"))
  write.csv(year2, "FILEPATH"))
}

#' [Make underreporting draws]
# Reads in scalar and creates draws. Upper and Lower variables are stand-in near maximum and near minimums
# for distribution. This is acheived by setting them to the third standard deviation (making near 99.9% draws within them).
# Underlying data needs to be changed and better documented to address this

make_underreport_draws <- function(scalar_file, draws_dir){
  
  underreporting_raw <- fread(scalar_file)
  
  #cases underreporting
  under_cases_mean <- underreporting_raw[group == "cases", mean]
  under_cases_se   <- underreporting_raw[group == "cases", max - min]/(6) #max and min are near max and near mins (3rd sd)
  under_cases_draw <- tabler(rnorm(n = 1000, mean = under_cases_mean, sd = under_cases_se), "underreporting_cases")
  fwrite(under_cases_draw, "FILENAME"))
  
  #deaths underreporting
  under_deaths_mean <- underreporting_raw[group == "deaths", mean]
  under_deaths_se   <- underreporting_raw[group == "deaths", max - min]/(6) #max and min are near max and near mins (3rd sd)
  under_deaths_draw <- tabler(rnorm(n = 1000, mean = under_deaths_mean, sd = under_deaths_se), "underreporting_deaths")
  fwrite(under_deaths_draw, "FILENAME"))
}

####################################################################
#' [01_estimate_deaths (age-sex functions use in 02 as well)]
####################################################################

#' [make draw files with all zeroes]
# This is useful both for non-endemic locations, but also to update specific location-year areas with endemic results

make_non_end_draw <- function(loc_id, epi_demo, measure_id){
  
  age_sex_epi <- expand.grid(age_group_id = epi_demographics$age_group_id, 
                             location_id  = loc_id,
                             year_id      = 1980:2019,
                             sex_id       = epi_demographics$sex_id,
                             measure_id   = measure_id)
  
  draws_blank <- as.data.table(matrix(0, nrow = nrow(age_sex_epi), ncol = 1000))
  names(draws_blank) <- paste0("draw_", 0:999)
  
  non_end <- as.data.table(cbind(age_sex_epi, draws_blank))
  
  return(non_end)
  
}

#' [calculate all-sex, age-specific proportions for age splitting]
# Calculates the observed all-sex, age-specific proportion based on the data

calc_as_proportion <- function(data){
  
  sex_age  <- data[age_group_id != 22]
  tot      <- sum(sex_age[, mean])
  proportions   <- sex_age[, .("proportions" = sum(mean)/tot), by = c("age_group_id", "sex_id")]
  
  return(proportions)
}


#' [calculate sex-specific proportions for age splitting]

calc_ss_proportion <- function(data, s_id){ 
  
  sex_age  <- data[age_group_id != 22 & sex_id == s_id]
  tot      <- sum(sex_age[, mean])
  proportions   <- sex_age[, .("proportions" = sum(mean)/tot ), by = c("age_group_id")]
  
  return(proportions)
}

#' [handy row replicator for use below]

rep_row <- function(row , n){
  x <- row[rep(1,n)]
  return(x)
}

#' [split all-sex all-age data by the provided proportions]

as_age_split <- function(all_age_data, all_age_proportions){
  
  for (obs in 1:nrow(all_age_data)){
    row <- all_age_data[obs]
    rows <- rep_row(row, nrow(all_age_proportions))
    rows[, c("sex_id", "age_group_id") := NULL]
    rows <- cbind(rows, all_age_proportions)
    rows[, mean  := as.numeric(mean) * proportions]
    rows[, proportions := NULL]
    if (obs == 1){
      split <- rows
    } else {
      split <- rbind(split, rows)
    }
  }
  
  return(split)
}

#' [split sex-specific all-age data by the provided proportions]

ss_age_split <- function(ss_all_age_data, ss_age_proportions){
  
  for (obs in 1:nrow(ss_all_age_data)){
    row <- ss_all_age_data[obs]
    rows <- rep_row(row, nrow(ss_age_proportions))
    rows[, c("age_group_id") := NULL]
    rows <- cbind(rows, ss_age_proportions)
    rows[, mean  := as.numeric(mean) * proportions]
    rows[, proportions := NULL]
    if (obs == 1){
      split <- rows
    } else {
      split <- rbind(split, rows)
    }
  }
  
  return(split)
}

#' [gives the names without the draw variables]
names_not_draws <- function(dt){
  names(dt)[!(str_detect(names(dt), "^draw*"))]
}

#' [applies ur]

apply_ur <- function(data, ur){
  
  if("year_start" %in% names(data)){
    data[, ":="(year_id = as.character(year_start), location_id = as.character(location_id),
              sex_id = as.character(sex_id), age_group_id = as.character(age_group_id))]
  } else {
    data[, ":="(year_id = as.character(year_id), location_id = as.character(location_id),
                sex_id = as.character(sex_id), age_group_id = as.character(age_group_id))]
  }
  
  data[, paste0("draw_", 0:999) := mean] 
  data <- melt(data, measure.vars = paste0("draw_", 0:999), variable.name = "draw", value.name = "value")
  ur   <- melt(ur, measure.vars = paste0("draw_", 0:999), variable.name = "draw", value.name = "ur_value")
  data <- merge(data, ur, by = "draw")
  data[, value := value * ur_value]

  data <- dcast(data, age_group_id + sex_id + location_id + year_id ~ draw, value.var = "value", fun.aggregate = sum)
  
  return(data)
}

#' [returns csvs with data added based on new_data]

inserter <- function(zero_draw_file, new_data, loc, m_id){
  
  cat("\n Starting on", loc, "\n")
  id_cols <- c("age_group_id", "sex_id", "year_id", "measure_id", "location_id")
  
  rows     <- new_data
  csv      <- zero_draw_file
  num <- 0
  
  for (ind in 1:nrow(rows)){
  
    num <- num + 1
    row <- rows[ind]
    
    if (!("year_start" %in% names(row))){
      row[, year_start := year_id]
    }
    
    csv[age_group_id == row[, age_group_id] &             #updating csv as go
          sex_id == as.character(row[, sex_id]) &         #as a list
          year_id == as.character(row[, year_start]) &
          location_id == as.character(row[, location_id]),
        paste0("draw_", 0:999) := row[, paste0("draw_", 0:999)]]
    
    cat("Finished adding row", num, "of", nrow(rows), "for" , loc, "for measure_id ", m_id, "\n")
  }
  
  return(csv)
}

####################################################################
#' [02_estimate_non-fatal]
####################################################################    

#' [converts a data.table of cases into prevalence through cases/population * duration]

cases_to_prev <- function(data, duration, pop_decomp_step, round_id){
  
  if (!("year_id" %in% names(data))){
    data[, year_id := year_start]
  }
  
  # hange year id 1994:2019 to blank
  pop  <- get_population(age_group_id = "all", year_id = unique(data[,year_id]), decomp_step = pop_decomp_step, sex_id = c(1,2), location_id = as.character(data[, location_id]), gbd_round_id = round_id)
  data[, c("age_group_id", "location_id",  "year_id", "sex_id") := lapply(.SD, as.integer), .SDcols = c("age_group_id", "location_id",  "year_id", "sex_id")]
  data <- merge(data, pop, by = c("age_group_id", "location_id",  "year_id", "sex_id")) 
  
  data <- melt(data, measure.vars = paste0("draw_", 0:999), variable.name = "draw", value.name = "value")
  
  # convert to incidence
  data[, value := value / population]
  
  # convert to prevalence
  duration <- melt(duration, measure.vars = paste0("draw_", 0:999), variable.name = "draw", value.name = "dur_value")
  data     <- merge(data, duration, by = "draw")
  data[, value := value * dur_value]
  data     <- dcast(data, age_group_id + sex_id + location_id + year_id ~ draw, value.var = "value")
  
  return(data)
}

#' [aggregates upscaled case count values by age, sex, location, year, draw]

agg_asldy <- function(data){
  
  if (!("year_id" %in% names(data))){
    data[, year_id := year_start]
  }
  
  cols         <- c("age_group_id", "sex_id", "location_id", "year_id", paste0("draw_", 0:999))
  cols_in_data <- names(data)[names(data) %in% cols]
  
  if (!(all(cols_in_data %in% cols))){
    stop("Data does not have age_group_id, sex_id, location_id, location_name, year_id (or year_start), or one of the draw_ 0:999 variables")
  }
  
  data <- data[, ..cols]
  
  data <- melt(data, measure.vars = paste0("draw_", 0:999), variable.name = "draw", value.name = "value")
  data <- data[, .("value" = sum(value)), by = c("age_group_id", "sex_id", "location_id", "year_id", "draw")]
  data <- dcast(data, age_group_id + sex_id + location_id + year_id ~ draw, value.var = "value")
  
  return(data)
}
