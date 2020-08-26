#####################################################################################################################################################################################
#####################################################################################################################################################################################
#' @Author:
#' @DateUpdated: 12/17/2018
#' @Description: This helper function formats the arguments for a rscript to be able to parse them by a specific flag (e.g. --date DATE_ARGUMENT)
#' 
#' @param root_j_temp base directory on j for finished.txt files
#' @param root_tmp_dir base directory on clustertmp that holds intermediate draws, and ODE inputs/outputs
#' @param date timestamp of current run (i.e. 2014_01_17)
#' @param step_num step number of this step (i.e. 01a)
#' @param step_name name of current step (i.e. first_step_name)
#' @param hold_steps steps to wait for before running
#' @param last_steps step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
#' @param code_dir directory where the code lives
#' @param in_dir directory for external inputs
#' @param etiology only used for step 04b_save_results and 09_collate_outputs
#' @param group only used for step 04b_save_results and 09_collate_outputs, potential values are: hearing, vision, and epilepsy
#' @param healthstate only used for step 04b_save_results and 09_collate_outputs
#' @param meid only used for 09_collate_outputs, meid for custom model
#'
#' @return a string that concatenates --flag1 argument1 --flag2 argument2 ...
#' 
#' @Example: my_args <- argparser(
#'                          root_j_dir = "filepath",
#'                          root_tmp_dir = "filepath",
#'                          date = "2018_12_20",
#'                          step_num = "02a", 
#'                          step_name = "cfr_draws", 
#'                          hold_steps = NULL, 
#'                          last_steps = NULL, 
#'                          code_dir = "filepath,
#'                          in_dir = "filepath"
#'                      )
#####################################################################################################################################################################################
#####################################################################################################################################################################################

argparser <-
  function(root_j_dir, root_tmp_dir, date, step_num, step_name, hold_steps = NULL,
           last_steps = NULL, location = NULL, code_dir, in_dir, out_dir = NULL,
           tmp_dir = NULL, group = NULL, healthstate = NULL, 
           meid = NULL, ds = NULL) 
    {
        
        my_root_j_dir   <- ifelse(!is.null(root_j_dir),   paste("--root_j_dir", root_j_dir),     "")
        my_root_tmp_dir <- ifelse(!is.null(root_tmp_dir), paste("--root_tmp_dir", root_tmp_dir), "")
        my_date         <- ifelse(!is.null(date),         paste("--date", date),                 "")
        my_step_num     <- ifelse(!is.null(step_num),     paste("--step_num", step_num),         "")
        my_step_name    <- ifelse(!is.null(step_name),    paste("--step_name", step_name),       "")
        my_hold_steps   <- ifelse(!is.null(hold_steps),   paste("--hold_steps", hold_steps),     "")
        my_last_steps   <- ifelse(!is.null(last_steps),   paste("--last_steps", last_steps),     "")
        my_loc          <- ifelse(!is.null(location),     paste("--location", location),         "")
        my_code_dir     <- ifelse(!is.null(code_dir),     paste("--code_dir", code_dir),         "")
        my_in_dir       <- ifelse(!is.null(in_dir),       paste("--in_dir", in_dir),             "")
        my_out_dir      <- ifelse(!is.null(out_dir),      paste("--out_dir", out_dir),           "")
        my_tmp_dir      <- ifelse(!is.null(tmp_dir),      paste("--tmp_dir", tmp_dir),           "")
        my_group        <- ifelse(!is.null(group),        paste("--group", group),               "")
        my_healthstate  <- ifelse(!is.null(healthstate),  paste("--state", healthstate),         "")
        my_meid         <- ifelse(!is.null(meid),         paste("--meid", meid),                 "")
        my_ds           <- ifelse(!is.null(ds),           paste("--ds", ds),                     "")
        
        args <-paste(my_root_j_dir, my_root_tmp_dir, my_date, my_step_num, my_step_name, 
                     my_hold_steps, my_last_steps, my_loc, my_code_dir, my_in_dir, 
                     my_out_dir, my_tmp_dir, my_group, my_healthstate, 
                     my_meid, my_ds)
        return(args)
  }