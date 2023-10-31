#' @author 
#' @date 2020/06/15
#' @description Save outputs from CWModel to an RDS object
#' @function save_crosswalk_RDS
#' @param results CW model
#' @param model_name model name
#' @return model RDS object that contains all selected items

save_crosswalk_RDS <- function(results, path, model_name){
  names <- c("beta", 
             "beta_sd", 
             "constraint_mat",
             "cov_mat",
             "cov_models",
             "cwdata",
             "design_mat",
             "fixed_vars",
             "gamma",
             "gold_dorm", 
             "lt",
             "num_vars",
             "num_vars_per_dorm",
             "obs_type",
             "order_prior",
             "random_vars",
             "relation_mat",
             "var_idx",
             "vars",
             "w")
  
  model <- list()
  
  for (name in names){
    if(is.null(results[[name]])) {
      message(name, " is NULL in original object, will not be included in RDS")
    }
    model[[name]] <- results[[name]]
  }
  
  saveRDS(model, paste0(path, model_name, ".rds"))
  message("RDS object saved to ", paste0(path, model_name, ".RDS"))
  
  return(model)
}
