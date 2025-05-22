################################################################################
## Name of Script: utilities.r
## Description: Loads functions for interacting with directories
## Arguments: see individual functions
## Output: get_path returns a string-formatted filepath. a 'process' must be specified for non-common paths (see function)

## Contributors: INDIVIDUAL_NAME
################################################################################
library(here)
if (!exists("code_repo"))  {
  code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
  if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
print(code_repo)
require(yaml)


## Create 'not in' statement
`%ni%` <- Negate(`%in%`)


is_filepath <- function(path){
  ##  Takes  a path as input and determines if the path leads to a file or not ##
  ## Arguments: (path:string)
  ##
  dir_path = dirname(path)
  base = basename(path)
  if (grepl("\\.", dir_path)) {
    stop(paste0("Please fix this path: ", path, "). Periods should not exist in directory names."))
  }
  ## assumes that path is a file if tail contains a period
  if (grepl("\\.", base)) {
    return(TRUE)
  } else return(FALSE)
}


ensure_dir <- function(somePath){
  
  
  ##
  success = tryCatch ({
    if ( is_filepath(somePath) ){
      directory_path = dirname(somePath)
      dir.create(directory_path, recursive=TRUE, showWarnings=FALSE)
    } else {
      dir.create(somePath, recursive=TRUE, showWarnings=FALSE)
    }}, error = function(e){
      print(paste("Error creating directory", somePath))
    }
  )
}

get_gbd_parameter <- function(parameter_name){
    
    ## returns a dict of parameters if parameter_name = 'list'
    ##
    path_file = file.path(code_repo,'gbd_parameters.yaml')
    parameter_dict <- yaml.load_file(path_file)
    if (parameter_name %ni% c(names(parameter_dict), "list")) {
        stop(paste0("ERROR: could not find specified parameter, ", parameter_name,
             ", in list of parameters"))
    } else if (parameter_name == "list") {
        return(names(parameter_dict))
    } else {
        return(parameter_dict[parameter_name])
    }
}


set_roots <- function() {
    ##
    ##
    if (!exists("grh.__roots_list")) {
        sysinf <- Sys.info()
        os.name <- sysinf['sysname']
        current_gbd_round = get_gbd_parameter('current_gbd_name')
        j <- ifelse( os.name == "Windows", "J:", "FILEPATH" )
        if ( os.name == "Windows") {
            h <- "H:"
        } else if (os.name == "Darwin") {
            h <- paste0("FILEPATH", Sys.info()['user'][1])
        } else {
            h <- paste0("FILEPATH", Sys.info()['user'][1])
        }
        i <- ifelse( os.name == "Windows", "I:" , "FILEPATH")
        j_temp <- file.path( j, 'FILEPATH', current_gbd_round)
        scratch <- file.path("FILEPATH", current_gbd_round)
        workspace <- ifelse( os.name == "Windows", j_temp, scratch)
        if (!exists("code_repo")) {
            code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
        }
        roots = list(
            "j"=j,
            "h"=h,
            "i"=i,
            "j_temp"=j_temp,
            "scratch"=scratch,
            "workspace"=workspace,
            "code_repo"=code_repo,
            "storage"= paste0(j, "FILEPATH", current_gbd_round)
        )
        grh.__roots_list <<- roots
  }
  return(grh.__roots_list)
}


get_root <- function(root_name) {
  ## returns the string-formatted filepath of the requested root, or returns
  ##      a list of possible roots if the "list_roots" is passed
  ##
  roots = set_roots()
  if (root_name %in% names(roots)) {
    return(roots[[root_name]])
  } else if (root_name == "list roots") {
    return(names(roots))
  } else {
    return("incorrect root name requested")
  }
}


if (!exists("get_path_listing")){
REDACTED
  source(file.path(get_root('code_repo'), 'r_utils/get_path_helpers.r'))
}


get_path <- function(key, process="common", base_folder="storage"){
  
  ##    accepts an optional specified root to override the generic [base_folder])
  
  ###
  
  all_roots = get_root("list roots")
  if (key %in% all_roots) {
    return(get_root(key))
  }
  
  acceptable_roots = all_roots[length(all_roots) > 1]
  if (base_folder %ni% acceptable_roots) {
    stop(paste0("USER ERROR: to use base_folder specification with a get_path key," +
                  "the root must be among the folowing \n ", acceptable_roots, ", process_paths"
    ))
  }
  
  path_obj <- get_path_listing(base_folder, process)
  if (key %ni% names(path_obj)) {
    stop("Requested path does not exist")
  }
  requested_path <- path_obj[[key]]
  if (length(requested_path) == 0) {
    stop("Requested path does not exist")
  }
  return(requested_path)
}


get_cluster_setting <- function(setting_key){
    yaml_file <- get_path("cluster_settings")
    yaml_data <- yaml.load_file(yaml_file)
    # Quit if the requested process is does not exist
    if (setting_key %ni% names(yaml_data)) {
            stop(paste("ERROR: Incorrect cluster setting specified.",
                        "No matching process setting_key for  '",
                        process, "  'in cluster_settings"))
    }
    refs <- substitute_roots(yaml_data)
    return(refs[[setting_key]])
}


################################################################################
##			Append Functions
################################################################################
append_csv <- function(root, output_path, output_name) {
  
  ## Run bash script to append
  script <- paste0(ubcov_path("shells_root"), "/append_csv.sh")
  cmd <- paste("sh", script, root, output_path, output_name, sep = " ")
  system(cmd)
  
  print(paste0("Append complete. Ouput location: ", output_path, "/", output_name, ".csv"))
}


append_load <- function(root, output_path, output_name, rm=FALSE) {
  
  ## Append files and load
  append_csv(root, output_path, output_name)
  df <- fread(paste0(output_path, "/", output_name, ".csv"))
  
  ## If remove, clear file
  if (rm) unlink(paste0(output_path, "/", output_name, ".csv"))
  
  return(df)
}

append_pdf <- function(input, output, rm=FALSE) {
  cmd <- paste0("/usr/bin/ghostscript -dBATCH -dSAFER -dNOGC -DNOPAUSE -dNumRenderingThreads=4 -q -sDEVICE=pdfwrite -sOutputFile=", output, " ", input)
  system(cmd)
  if (rm) unlink(dirname(input[1]), recursive=T)
}
