check_me_names <- function(data) {
  
  data <- as.data.table(data)
  
  df_mes <- unique(data$me_name)
  extra_mes <- df_mes[!(df_mes %in% mes)]
  
  if (length(extra_mes) > 0) {
    # Ask if user would like to update invalid MEs
    update_mes_prompt <- paste0('This script doesn\'t recognize ', extra_mes, 
                                '. Would you like to replace all these ME names? (y/[n]): ')
    update_mes <- readline(prompt=update_mes_prompt)
    
    # If the user does want to replace them
    if (tolower(update_mes) == 'y') {
      
      message(paste0('You must replace each unrecognized ME with one of the following: ',
                     paste(mes, collapse=', '), '.'))
      # For each invalid ME
      for (extra_me in extra_mes) {
        replace_prompt <- sprintf('Replace %s with: ', extra_me)
        new_me <- extra_me
        # Make sure replacement is valid
        while (!(new_me %in% mes)) {
          new_me <- readline(prompt=replace_prompt)
          if (!(new_me %in% mes)) {
            message('The new ME must be one of the following: ',
                    paste(mes, collapse=', '))
          }
        }
        # Then update the data
        data[me_name==eval(extra_me),me_name:=get('new_me')]
      }
      invisible(return(data))
      # Otherwise that data will get dropped
    } else {
      continue_prompt <- "Rows with invalid MEs will be dropped. Continue with script? (y/[n]): "
      continue <- readline(prompt=continue_prompt)
      if (tolower(continue) != 'y') {
        exit_script('Correct the invalid ME values in the bundle before re-running script!')
      }
    } 
  } else { # return data without changes
    message('All MEs in dataset are processed properly by this script!')
    invisible(return(data))
  }
}