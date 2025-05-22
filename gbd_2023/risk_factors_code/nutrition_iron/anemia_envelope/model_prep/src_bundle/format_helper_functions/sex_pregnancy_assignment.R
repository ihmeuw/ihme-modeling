
# add sex id/sex to df ----------------------------------------------------

add_sex_id <- function(input_df){
  df <- copy(input_df)
  
  sex_list <- list(
    Male = 1,
    Female = 2,
    Both = 3
  )
  
  for(i in names(sex_list)){
    i_vec <- which(
      is.na(df$sex) & 
        !(is.na(df$sex_id)) & 
        df$sex_id == sex_list[[i]]
    )
    df$sex[i_vec] <- i
    
    i_vec <- which(
      is.na(df$sex_id) & 
        !(is.na(df$sex)) & 
        df$sex == i
    )
    df$sex_id[i_vec] <- as.integer(sex_list[[i]])
  }
  
  if(any(is.na(df$sex_id) | is.na(df$sex))){
    stop("Sex IDs/Sex columns are not all defined. Please updated and rerun.")
  }
  
  return(df)
}


# update trimester to fit cv bundle constraints ---------------------------

update_trimester <- function(trimester_col){
  i_vec <- which(!(is.na(trimester_col)) & trimester_col > 1)
  trimester_col[i_vec] <- 1
  return(trimester_col)
}
