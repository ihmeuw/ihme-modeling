
divide_data <- function(input_data){
  #' @description Divides the full dataset into "train" and "test" sets. The train set
  #' consists of all rows which are in proper age and sex groups. The test is all others 
  #' (both aggregate age groups and both sex data).
  #' @param input_data data.table. All input data.
  #' @return a list of two elements, the first is the training data, the second is the test 
  #' data as described above
  #' Assumes that age_demographer column in bundle is properly filled out
  
  data <- copy(input_data)
  data[sex == "Both", sex_id := 3]
  data[sex == "Male", sex_id := 1]
  data[sex == "Female", sex_id := 2]
  
  #convert anything not in demographer notation into demographer notation
  data[age_demographer == 0 & age_end >= 1, age_end := age_end - 0.001]
  
  #Round age groups to the nearest age-group boundary
  data$age_start <- as.double(data$age_start)
  data[age_start < 0.02, age_start := 0]
  data[age_start >= 0.02 & age_start < 0.07, age_start := 0.01917808]
  data[age_start >= 0.07 & age_start < 1, age_start := 0.07671233]
  data[age_start >= 1 & age_start < 5, age_start := 1]
  data[age_start >= 5, age_start := age_start - age_start %% 5]
  data[age_start > 95, age_start := 95]
  
  data[age_end == 0, age_end := 0]
  data[age_end > 0.019 & age_end <= 0.02, age_end := 0.01917808]
  data[age_end > 0.02 & age_end <= 0.077, age_end := 0.07671233]
  data[age_end > 0.077 & age_end < 1, age_end := 0.999]
  data[age_end >= 1 & age_end < 5, age_end := 4]
  data[age_end >= 5, age_end := age_end - age_end %% 5 + 4]
  data[age_end > 95, age_end := 99]
    
  data[, need_split := 1]
  data[age_start == 0 & (age_end == 0.019 | age_end == 0) & (sex_id %in% c(1,2)), need_split := 0]
  data[age_start == 0.02 & age_end == 0.077 & (sex_id %in% c(1,2)), need_split := 0]
  data[age_start == 0.077 & age_end == 0.999 & (sex_id %in% c(1,2)), need_split := 0]
  data[age_start == 1 & age_end == 4 & (sex_id %in% c(1,2)), need_split := 0]
  data[age_start > 0 & (age_end - age_start) == 4 & (sex_id %in% c(1,2)), need_split := 0]
  
  data
}
