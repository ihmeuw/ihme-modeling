# OUTLIER CLINICAL DATA WHERE MEAN == 0
outlier_clinical_data <- function(raw_dt) {
  #' outlier clinical data
  #' 
  #' @description outlier clinical data in bundle version where mean = 0 and 
  #'              sets is_outlier = 1 and comments in note_modeler column
  #' 
  #' @param raw_dt data.table. bundle version data table
  #' @return dt data.table. bundle version data with mean = 0 clinical data 
  #'                        outliered
  #' 
  #' @examples outlier_clinical_data(bundle_version_dt)
  #' 
  dt <- copy(raw_dt)
  n <- nrow(dt[clinical_data_type %in% c("inpatient", "claims") & mean == 0])
  print(paste0("Outliering ", n, " rows of clinical data with mean = 0"))
  dt <- dt[, note_modeler:=as.character(note_modeler)]
  dt[clinical_data_type %in% c("inpatient", "claims") & mean == 0, 
     `:=` (is_outlier = 1, 
           note_modeler = paste0(note_modeler, " | outliering since mean = 0 is implaussibly low"))]
  return(dt)
}