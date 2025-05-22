
args <- commandArgs(trailingOnly = TRUE)
year_i <- args[1]
out_dir <- args[2]
trim <- args[3]

pacman::p_load(data.table)

draws <- fread("PATHNAME", year_i, '.csv')

# split the data table into a list, where each data point to be split is now its own data table 
draws <- split(draws, by = "split.id")

# then apply this function to each of those data tables in that list
draws <- lapply(draws, function(input_i){
  mean.vector <- input_i$emr
  se.vector <- input_i$emr_se
  #Generate a random draw for each mean and se 
  set.seed(123)
  input.draws <- rnorm(length(mean.vector), as.numeric(mean.vector), as.numeric(se.vector))
  input_i[, value := input.draws]
  
  return(input_i)
  
})

draws <- rbindlist(draws)

#dcast back into rows of draws
final <- dcast(draws, location_id + year_id + sex_id + age_group_id ~ draw.id, value.var = 'value')

write.csv(final, row.names = FALSE, 
          file = paste0(out_dir, 'PATHNAME', '/collapsed_', year_i, '.csv'))