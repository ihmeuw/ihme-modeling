#library(data.table)
#library(dplyr)
#library(gsheet, lib.loc = "FILEPATH")

# The dataframe must have age bin columns age_start and age_end
# The first row should be the consolidated all-ages total.
#
# Example:
# age_start	age_end		total_administered
# 0	          125	      12
# 18	        24		    2
# 25	        49		    2
# 50	        59		    2
# 60	        69		    2
# 70	        79		    2
# 80	        125		    2



# This function runs the age redistribution funcs below on each location-age combo and parallelized for speed
run_redistribute_ages <- function(df,                 # data frame with vaccine hesitancy survey counts. Function assumes columns are present: location_id. data. age_start, age_end, plus objective data columns
                                  age_starts,         # Integer vector of ages defining the beginning of each new age interval
                                  include_all_ages=FALSE,
                                  objective_columns,  # Character vector naming each of the data columns to redistribute
                                  population_weighted=FALSE,
                                  n_cores=1             # Number of cores to use when running location-dates in parallel. By default 1.
) {
  
  # Load 1-year population bins.
  if (population_weighted) ALL_POP <- fread(file='FILEPATH/pop_one_year.csv')
  
  df <- as.data.frame(df)
  
  if (!('age_group' %in% colnames(df))) df$age_group <- stringr::str_c(df$age_start, '-', df$age_end)
  
  if (identical(age_starts, 0)) {
    
    message('age_starts = 0 and will return results for all age groups combined')
    sel <-  which(df$age_start == 0 & df$age_end == 125)
    
  } else {
    
    message('Redistributing to age groups:')
    age_groups <- foreach(i=seq_along(age_starts), .combine = c) %do% if (i == length(age_starts)) paste0(age_starts[i], "+") else paste0(age_starts[i], '-', age_starts[i+1]-1)
    if (include_all_ages) age_groups <- c(age_groups, paste(min(age_starts), 125, sep='-'))
    message(paste(age_groups, collapse=" | "))
    
    
    sel <-  c(which(is.na(df$age_start)),
              which(is.na(df$age_end)),
              which(df$age_start == 0 & df$age_end == 125))
  }
  
  if (length(sel) > 0) df <- df[-sel,]
  
  
  ### Getting NULL out of this chunk here, but I bet the bug is further down...
  #x <- df[df$location_id == 523 & df$date == as.Date('2021-11-14'),]
  
  rebin <- do.call(
    rbind,
    pbmcapply::pbmclapply(
      X=split(df, list(df$location_id, df$date)),
      mc.cores=n_cores,
      FUN=function(x) {
        
        tryCatch( {
          
          if (population_weighted) {
            
            out <- redistribute_ages_pop_weighted(df=x,
                                                  age_starts=age_starts,
                                                  objective_columns=objective_columns)
            
          } else {
            
            out <- redistribute_ages_uniform(df=x,
                                             age_starts=age_starts,
                                             objective_columns=objective_columns)
          }
          
          
          if (include_all_ages) {
            tmp <- out[1,]
            tmp$age_start <- min(out$age_start)
            tmp$age_end <- max(out$age_end)
            tmp[,objective_columns] <- colSums(out[,objective_columns])
            out <- rbind(out, tmp)
            
            # Check all age
            checks <- colSums(x[,objective_columns]) == colSums(tmp[,objective_columns])
            checks <- checks[!is.na(checks)]
            if (any(checks == FALSE)) warning(glue('Redistributed all-ages do not sum to original (possibly because original contains unused age groups)'))
            
          }
          
          return(out)
          
        }, error = function(e) {
          
          message(paste('ERROR:', unique(x$location_id), unique(x$age_group), conditionMessage(e)))
          
        })
      }
    )
  )
  
  
  row.names(rebin) <- NULL
  rebin$age_group <- stringr::str_c(rebin$age_start, '-', rebin$age_end)
  rebin <- rebin[order(rebin$location_id, rebin$age_group, rebin$date),]
  return(rebin)
}

