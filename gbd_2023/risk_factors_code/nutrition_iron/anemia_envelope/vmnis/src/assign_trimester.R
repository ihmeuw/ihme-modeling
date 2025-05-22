
# source libraries --------------------------------------------------------

library(data.table)

# function to assign trimester --------------------------------------------

assign_trimester <- function(input_df){
  df <- copy(input_df)
  trimester_df <- data.table(
    one = c("1", "first", "one"),
    two = c("2", "second", "two"),
    three = c("3", "third", "three")
  )
  df$trimester <- 0
  df$cv_trimester <- 0
  
  i_vec <- which(
    df$sex == 'Female' &
      (
        (
          !(is.na(df$specificity)) &
            grepl("trimester", df$specificity, ignore.case = TRUE)
        ) |
          (
            !(is.na(df$note_sr)) &
              grepl("trimester", df$note_sr, ignore.case = TRUE)
          )
      )
  )
  df$cv_trimester[i_vec] <- 1
  
  for(c in colnames(trimester_df)){
    col_vals <- paste(trimester_df[[c]], collapse = "|")
    i_vec <- which(df$cv_trimester == 1 &
      (
        grepl(col_vals, df$specificity, ignore.case = TRUE)
         | grepl(col_vals, df$note_sr, ignore.case = TRUE)
      )
    )
    df$trimester[i_vec] <- as.numeric(trimester_df[[c]][1])
  }
  
  return(df)
}
