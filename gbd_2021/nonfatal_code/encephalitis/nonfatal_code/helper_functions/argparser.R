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
#' @param ds indicate which decomp step to use in shared functions
#' @param new_cluster binary variable to indicate 1 - submiting jobs on cluster_fair or 0 - submiting jobs on cluster_prod / dev
#'
#' @return a string that concatenates --flag1 argument1 --flag2 argument2 ...
#' 
#####################################################################################################################################################################################
#####################################################################################################################################################################################

argparser <-
  function(root_j_dir, root_tmp_dir, date, step_num, step_name, hold_steps = NULL,
           last_steps = NULL, location = NULL, code_dir, in_dir, out_dir = NULL,
           tmp_dir = NULL, etiology = NULL, group = NULL, healthstate = NULL, 
           meid = NULL, ds = NULL, new_cluster = NULL) 
    {
        # if (is.null(root_j_dir))   stop("Did not specify root_j_dir")
        # if (is.null(root_tmp_dir)) stop("Did not specify root_tmp_dir")
        # if (is.null(date))         stop("Did not specify date")
        # if (is.null(step_num))     stop("Did not specify step_num")
        # if (is.null(step_name))    stop("Did not specify step_name")
        # if (is.null(hold_steps))   stop("Did not specify hold_steps")
        # if (is.null(last_steps))   stop("Did not specify last_steps")
        # if (is.null(code_dir))     stop("Did not specify code_dir")
        # if (is.null(in_dir))       stop("Did not specify in_dir")
        
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
        my_etiology     <- ifelse(!is.null(etiology),     paste("--cause", etiology),            "")
        my_group        <- ifelse(!is.null(group),        paste("--group", group),               "")
        my_healthstate  <- ifelse(!is.null(healthstate),  paste("--state", healthstate),         "")
        my_meid         <- ifelse(!is.null(meid),         paste("--meid", meid),                 "")
        my_ds           <- ifelse(!is.null(ds),           paste("--ds", ds),                     "")
        my_new_cluster  <- ifelse(!is.null(new_cluster),  paste("--new_cluster", new_cluster),   "") 
        
        args <-paste(my_root_j_dir, my_root_tmp_dir, my_date, my_step_num, my_step_name, 
                     my_hold_steps, my_last_steps, my_loc, my_code_dir, my_in_dir, 
                     my_out_dir, my_tmp_dir, my_etiology, my_group, my_healthstate, 
                     my_meid, my_ds, my_new_cluster)
        return(args)
  }