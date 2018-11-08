
get_settings <- function(main_dir) {

  # load settings
  settings <- read.csv(paste0(main_dir, "/settings.csv"), stringsAsFactors=F, header=F)

  # loop over settings
  for (var in 1:nrow(settings)) {
    arg_name <- settings[var, 1]
    arg_value <- settings[var, 2]

    # assign settings in the global environment
    exp <- try(assign(arg_name, eval(parse(text=arg_value)), envir=.GlobalEnv), silent=T)
    if(class(exp) == "try-error") assign(arg_name, arg_value, envir=.GlobalEnv)
  }
  return("Settings loaded")
}
