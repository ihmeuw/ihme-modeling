###
# Purpose: This script is used to crosswalk the diabetes bundle to the fpg bundle. 
# The script uses the crosswalk file to find the corresponding fpg value for the 
# diabetes value. The script also checks for any issues in the crosswalk and outputs 
# a file with the issues. ie. if there are any diabetes values that do not have a 
# corresponding fpg value or if there are any fpg values that do not have a corresponding diabetes value. 
# The script also checks for any diabetes values that have more than one corresponding fpg value.
# 
# Inputs: diabetes budndle version , fpg bundle version, function value( db_to_fpg or fpg_to_db)
###

# print what the input paramaters are when sourced
cat("example input:\n xwalk <- diabetes_fpg_xwalk(fpg_ver = 43404, db_ver = 43303, 
    function_val = 'db_to_fpg', output_path = '/ihme/code/user/db/')\n",
("db_ver:optional: diabetes crosswalk version id, this is defaulted to null
    to allow for the ability to only specify the fpg bundle version for get_se\n"),
("fpg_ver:required: fpg bundle version id(get_se) or crosswalk version id\n"),
("function_val:required: db_to_fpg or fpg_to_db or get_se\n"),
("output_path:optional: file directory to write diagnostic plots to, defaulted to '~/' the users home directory"))




x_walk_file<- setDT(read.csv('FILEPATH'))
# check if x_walk_file sex columns has 3 as a value
if(!(3 %in% x_walk_file$sex)){
  temp<- copy(x_walk_file)
  temp <-temp[, sex := 3][, lapply(.SD, mean, na.rm = TRUE), by = .(age, mean)]
  x_walk_file<- rbind(x_walk_file,temp)

}

# function_val <- "fpg_to_db"
# function_val <- "get_se"

# a<- db_bundle_data[nid == 448541,]
format_db_to_fpg<- function(temp_row,input_list){
  ## function that formats the temp_row to be used in the crosswalk
  ## formats columns in fpg bundle but not in the diabetes bundle
  ## takes in input list which is the list of the mean and sd from the crosswalk file
  new_mean <- input_list[[1]]
  new_sd <- input_list[[2]]
  temp_row <- setDT(temp_row)
  temp_row <- temp_row[,orig_year_start := year_start]
  temp_row <- temp_row[,orig_year_end := year_end]
  temp_row <- temp_row[,orig_age_start := age_start]
  temp_row <- temp_row[,orig_age_end := age_end]
  temp_row <- temp_row[,age_group_id := 999]
  temp_row <- temp_row[,variance := new_sd^2]
  temp_row <- temp_row[,unit_id := "mmol/l"]
  mid_year <- round((temp_row$year_start + temp_row$year_end)/2)
  temp_row <- temp_row[,year_id := mid_year]
  temp_row <- temp_row[,standardized_case_definition := "from_db"]
  temp_row <- temp_row[,seq := NA]
  temp_row <- temp_row[,val := new_mean]
  temp_row <- temp_row[,standard_error := new_sd]
  temp_row <- temp_row[,input_type_id := "1"]
  temp_row <- temp_row[,measure := "prevalence"]
  return(temp_row)
  
}
format_fpg_to_db<- function(temp_row,input_list){
  ## function that formats the temp_row to be used in the crosswalk
  ## formats columns in diabetes bundle but not in the fpg bundle
  ## takes in input list which is the list of the mean , upper and lower 
  ## from the crosswalk file
  new_mean_val <- input_list[[1]]
  upper_val <- input_list[[3]]
  lower_val <- input_list[[4]]
  temp_row <- setDT(temp_row)
  temp_row <- temp_row[,year_start := orig_year_start]
  temp_row <- temp_row[,year_end := orig_year_end]
  temp_row <- temp_row[,age_start := orig_age_start]
  temp_row <- temp_row[,age_end := orig_age_end]
  temp_row <- temp_row[,standardized_case_definition := "from_fpg"]
  temp_row <- temp_row[,seq := NA]
  temp_row <- temp_row[,orig_mean_fpg := val]
  temp_row <- temp_row[,mean := new_mean_val]
  temp_row <- temp_row[,upper := upper_val]
  temp_row <- temp_row[,lower := lower_val]
  temp_row <- temp_row[,measure := "continuous"]

  return(temp_row)
  
}

plot_mean_val<- function(data,function_val,start_age, end_age,dt_row_var,post_xwalk_var ){
  ## function that plots the mean and val column for each region and nid
  
  all <- ggplot(data = data) +
    geom_point(aes(x=data[["val"]], y=data[["mean"]])) +
    labs(x = "fpg", y = "prevalence",title="All Data prev vs fpg", 
         subtitle = paste0("crosswalk from: ", function_val)) +
    facet_wrap(~sex) +
    theme_minimal()
  print(all)
  all_prev <- ggplot(data = data) +
    geom_point(aes(x=data[["mid_age"]], y=data[["mean"]])) +
    labs(x = "Mid age", y = "prevalence",title="prevalence by mid_age", 
         subtitle = paste0("crosswalk from: ", function_val)) +
    facet_wrap(~sex) +
    theme_minimal()
  print(all_prev)
  all_fpg <- ggplot(data = data) +
    geom_point(aes(x=data[["mid_age"]], y=data[["val"]])) +
    labs(x = "Mid age", y = "mean FPG",title="FPG mean by mid_age", 
         subtitle = paste0("crosswalk from: ", function_val)) +
    facet_wrap(~sex) +
    theme_minimal()
  print(all_fpg)
  for(i in unique(data$region_name)){
    data_nid <- data[region_name == i]
    title <- paste0("Region Name:", unique(data_nid$region_name))
    #plot the mean and val column each page being a different region, and faceted by nid
    gg <- ggplot(data = data_nid) +
      geom_point(aes(x=data_nid[["val"]], y=data_nid[["mean"]], color=sex)) +
      labs(x = "fpg", y = "prevalence",title=title, 
           subtitle = paste0("crosswalk from: ", function_val)) +
      facet_wrap(~nid) +
      theme_minimal()
    print(gg)
  }
  dev.off()
}

format_se<- function(temp_row,input_list){
  # assign standard error and variance to the temp_row
  temp_row <- setDT(temp_row)
  new_sd <- input_list[[2]]
  temp_row <- temp_row[,standard_error := new_sd]
  temp_row <- temp_row[,variance := new_sd^2]
  temp_row <- temp_row[,upper := (val + (1.96* new_sd))]
  temp_row <- temp_row[,lower := (val - (1.96* new_sd))]
  temp_row <- temp_row[,note_SR := paste0(note_SR, "| imputed variance")]
  return(temp_row)
  
}
x_walk_helper <- function(temp_row, start_age,end_age, start_year,end_year ,sex_val,
                         x_walk_match,x_walk_target,dt_row_var, diagnostic_cols){
  ##
  #Helper function to db_fpg_xwalk(). This processes all the conditional statements
  #specific to the direction of cross walking. assuring age and year are within
  #bounds, and that the crosswalk is valid.
  ##
    age_val<- temp_row[[start_age]]
    mean_val <- temp_row[[dt_row_var]]
    issues <- data.table()
    # check  if age range is within bounds
    if((temp_row[[end_age]] - temp_row[[start_age]]) > 5) {
      age_val <- round((temp_row[[end_age]] + temp_row[[start_age]]) / 2 / 5) * 5
      print(paste("seq: ", temp_row$seq, 
                  ", Age range exceeds 5 years . Mid point value used: ", age_val))
      need_age_temp<- temp_row[,diagnostic_cols,with = FALSE]
      need_age_temp<- need_age_temp[,issue := "age range exceeds 5 years . Mid point value used"]
      need_age_temp<- need_age_temp[,answer := age_val] 
      issues<- rbind(issues, need_age_temp)
    }else if(age_val %% 5 != 0) {
      age_val <- round(age_val / 5) * 5
      print(paste("seq: ", temp_row$seq, ", Age_start is not a multiple of 5. Rounded to nearest 5: ", age_val))
      need_age_temp<- temp_row[,diagnostic_cols,with = FALSE]
      need_age_temp<- need_age_temp[,issue := "age_start is not a multiple of 5. Rounded to nearest 5"]
      need_age_temp<- need_age_temp[,answer := age_val]
      issues<- rbind(issues, need_age_temp)
    }
    # check if age is greater than 95 
    # sets age to 95 if it is
    if(age_val > 95) {
      age_val <- 95
      print(paste("seq: ", temp_row$seq, ", Age is greater than 95. Age_start set to 95: ", age_val))
      need_age_temp<- temp_row[,diagnostic_cols,with = FALSE]
      need_age_temp<- need_age_temp[,issue := "age is less than 95. Age_start set to 95"]
      need_age_temp<- need_age_temp[,answer := age_val]
      issues<- rbind(issues, need_age_temp)  
    }
    subset_data <- x_walk_file[x_walk_file$age == age_val & x_walk_file$sex == sex_val,]
    min_diff <- min(abs(subset_data[[x_walk_match]] - mean_val))
    equal_dat <- subset_data[abs(subset_data[[x_walk_match]] - mean_val) == min_diff, ]
    # check if there are multiple rows in x_walk that match 
    if(nrow(equal_dat) > 1) {      
      print(paste("seq: ", temp_row$seq, ", has multiple rows in x_walk, ", toString(equal_dat$row.names)))
      need_mean_temp<- temp_row[,diagnostic_cols,with = FALSE]
      need_mean_temp<- need_mean_temp[,issue := "multiple rows in x_walk getting mean of list"]
      need_mean_temp<- need_mean_temp[,answer := list(equal_dat[[x_walk_target]])] 
      new_mean<- mean(equal_dat[[x_walk_target]])
      new_sd<- mean(equal_dat$sd)
      new_upper<- mean(equal_dat$prev_upper)
      new_lower<- mean(equal_dat$prev_lower)
      issues<- rbind(issues, need_mean_temp)
    }
    # check if there is no exact match in x_walk
    else if(equal_dat[[x_walk_match]] != mean_val) {
      need_mean_temp<- temp_row[,diagnostic_cols,with = FALSE]
      need_mean_temp<- need_mean_temp[,issue := "no exact match found in x_walk. Closest value used"]
      need_mean_temp<- need_mean_temp[,answer := (equal_dat[[x_walk_target]])] 
      new_mean <- (equal_dat[[x_walk_target]])
      new_sd <- (equal_dat$sd)
      new_upper <- (equal_dat$prev_upper)
      new_lower <- (equal_dat$prev_lower)
      issues<- rbind(issues, need_mean_temp)
      print(paste("seq: ", temp_row$seq, ", no exact match found in x_walk. Closest value used: ", equal_dat[[x_walk_match]]))
    }
    # check if the mean value is 0
    else if (mean_val == 0){
     stop(paste("seq: ", temp_row$seq, ", mean value is 0. Cannot crosswalk"))
    }
    else{
      new_mean <- equal_dat[[x_walk_target]]
      new_sd <- equal_dat$sd
      new_upper <- equal_dat$prev_upper
      new_lower <- equal_dat$prev_lower
    }
    end<- list(mean = new_mean, sd = new_sd, prev_upper = new_upper, prev_lower = new_lower, issues = issues)
    return(end)
    
}


diabetes_fpg_xwalk<- function(fpg_ver,db_ver = NULL, function_val,output_path = "~/" ){
  # if the last charachter of output_path is not /, add / but only if function_val is not get_se
  if (substr(output_path, nchar(output_path), nchar(output_path)) != "/" & function_val != 'get_se') {
    output_path <- paste0(output_path, "/")
  }
  if(function_val != 'get_se'){
  plot_text<-paste0("plots are saved to the output directory: ",
                      output_path,"crosswalk_diagnostic_plots_",
                      function_val,".pdf\n")
  db_data<- get_crosswalk_version(crosswalk_version_id  = db_ver)
  db_data <- db_data[db_data$standardized_case_definition != 'from_fpg']
  db_data <- db_data[db_data$measure == "prevalence"]
  db_bundle_id <- get_elmo_ids(crosswalk_version_id = db_ver)
  db_bundle_id <- unique(db_bundle_id$bundle_id)
  db_bundle<- get_bundle_data(bundle_id = db_bundle_id)
  col_names_db <- colnames(db_bundle)
  fpg_data<- get_crosswalk_version(crosswalk_version_id  = fpg_ver)
  fpg_data <- fpg_data[fpg_data$standardized_case_definition != 'from_db']
  fpg_bundle_id <- get_elmo_ids(crosswalk_version_id  = fpg_ver)
  fpg_bundle_id <- unique(fpg_bundle_id$bundle_id)
  fpg_bundle<- get_bundle_data(bundle_id = fpg_bundle_id)
  col_names_fpg <- colnames(fpg_bundle)
  }else{
    plot_text <- "no plots saved for get_se function\n"
    fpg_data<- get_bundle_version(bundle_version_id = fpg_ver)
    fpg_bundle_id <- get_elmo_ids(bundle_version_id = fpg_ver)
    fpg_bundle_id <- unique(fpg_bundle_id$bundle_id)
    fpg_bundle<- get_bundle_data(bundle_id = fpg_bundle_id)
  }
  
  
  # set sex column to be the same for bundles
  x_walk_file<- setDT(x_walk_file)
  x_walk_file[, sex := ifelse(sex == 1, "Male", ifelse(sex == 2, "Female", ifelse(sex == 3, "Both", sex)))]
  # initialize function specific variables in universal variables to be used in the fucntion 
  #without redundancy
  prev_0_row <-data.table()
  if (function_val == "fpg_to_db" || function_val == "get_se") {
    start_age <- "orig_age_start"
    end_age <- "orig_age_end"
    start_year <- "orig_year_start"
    end_year <- "orig_year_end"
    x_walk_match<- "mean"
    dt_row_var<- "val"
    x_walk_target <-"prev_mean"
    post_xwalk_var<- "mean"
    dt_copy<- fpg_data
    dt_copy<- setDT(dt_copy)
    col_names_fpg <- colnames(fpg_data)
    diagnostic_cols<-c('seq','nid','orig_age_start','orig_age_end','sex','val')
    if (function_val == "fpg_to_db") {
      dt_copy <- dt_copy[diabetes_fpg_crosswalk == 1]
      cols_not_in_fpg <- col_names_db[!(col_names_db %in% colnames(fpg_data))]
      cols_not_in <- cols_not_in_fpg
      cols_target <- col_names_db
      # add orig_mean_fpg to col_target
      if (!("orig_mean_fpg" %in% col_names_db)){
      cols_target <- c(cols_target, "orig_mean_fpg")
      }
      format_fun <- "format_fpg_to_db"
      dt_copy <- dt_copy[(sex %in% c("Male","Female")) &
                           dt_copy[[end_age]] >=25,]
    }else{
      # filter to variance is 999 and val has a value
      dt_copy <- dt_copy[variance == 999 & !is.na(val )& dt_copy[[end_age]] >=25,]
      cols_not_in <-  c()
      cols_target <- col_names_fpg
      format_fun <- "format_se"
      }
    }else if (function_val== "db_to_fpg") {
    start_age <- "age_start"
    end_age <- "age_end"
    start_year <- "year_start"
    end_year <- "year_end"
    x_walk_match<- "prev_mean"
    dt_row_var<- "mean"
    x_walk_target<- "mean"
    post_xwalk_var<- "val"
    dt_copy<- db_data
    dt_copy<- setDT(dt_copy)
    dt_copy <- dt_copy[diabetes_fpg_crosswalk == 1]
    col_names_fpg <- colnames(fpg_bundle)
    cols_not_in_db <- col_names_fpg[!(col_names_fpg %in% col_names_db)]
    cols_not_in <- cols_not_in_db
    cols_target <- col_names_fpg
    format_fun <- "format_db_to_fpg"
    diagnostic_cols<-c('seq','nid','age_start','age_end','sex','mean')
    dropped_prev_0<-nrow(dt_copy[diabetes_fpg_crosswalk == 1&
                         (sex %in% c("Male","Female")) &
                         dt_copy[[end_age]] >=25 & 
                         dt_copy[[dt_row_var]]  ==0,])
    prev_0_row<- data.table(issue = "prev_mean is 0, dropped", number_of_rows= dropped_prev_0)
    dt_copy <- dt_copy[dt_copy[[dt_row_var]] != 0,]
    dt_copy <- dt_copy[(sex %in% c("Male","Female")) &
                         dt_copy[[end_age]] >=25,]
  # stops function if function_val is not one of the three options
  }else{
    stop("function_val must be either db_to_fpg, fpg_to_db, or get_se")
  }
  # filter to only rows requiring crosswalk and have valid sex and age
  
  output<- data.table()
  issues<- data.table()
  plot_data <- data.table()
  # loops through each row requiring function to be applied
  
  for (i in 1:nrow(dt_copy)) {
    temp_row <- dt_copy[i,]
    sex_val <- temp_row$sex
    print(temp_row$seq)
    x_walked_vars <- x_walk_helper(temp_row,start_age, end_age, start_year, end_year,sex_val,
                                   x_walk_match,x_walk_target,dt_row_var,diagnostic_cols)
    issues<- rbind(issues, x_walked_vars$issues)
    #remove issues from x_walked_vars
    x_walked_vars$issues <- NULL
    for (j in seq_along(cols_not_in)) {
        temp_row[,(cols_not_in)[j] := NA]
    }
    temp_row<- get(format_fun)(temp_row,x_walked_vars)
    if(!(function_val == "get_se")){
      cols<- c("seq","nid","region_name",dt_row_var,post_xwalk_var,"sex",start_age,end_age)
      plot_temp<- temp_row[,cols, with = FALSE]
      plot_temp<- plot_temp[,mid_age := 
                              round(as.numeric(plot_temp[[start_age]]) + as.numeric(plot_temp[[end_age]]))/2]
      plot_data<- rbind(plot_data,plot_temp)
    }
    temp_row <- temp_row[,cols_target, with = FALSE]
    output<- rbind(output,temp_row)
  }
  # if functionval is db_to_fpg or fp_to_db,
  if (function_val == "db_to_fpg" || function_val == "fpg_to_db") {
    pdf(paste0(output_path,"crosswalk_diagnostic_plots_",function_val,".pdf"), width = 18, height = 8.5)
    plot_mean_val(plot_data, function_val,start_age, end_age,dt_row_var,post_xwalk_var)
  }else{
    seqs<- unique(output$seq)
    #get rows in fpg_bundle_data that are not in output
    not_in_output<- fpg_data[!(seq %in% seqs),]
    #bind to output
    output<- rbind(output, not_in_output)
  }
  
  summary<- issues[, .N, by = .(issue)]
  #rename N to number of rows
  setnames(summary, "N", "number_of_rows")
  summary<- rbind(summary, prev_0_row)
  final<- list(x_walk = output, issues = issues, summary = summary)
  cat("Returned values: This function returns a list containing the crosswalked data (x_walk)\n",
      "any issues that arose during crosswalking (issues), and,\n", 
      "a summary of those issues (summary).\n",
      paste0(plot_text),
      "Assign list values to variable ie. variable <- returned$x_walk\n")
  return(final)
}


