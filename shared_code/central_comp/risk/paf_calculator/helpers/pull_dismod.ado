cap program drop pull_dismod
program define pull_dismod
   syntax, model_version_id(integer) [clear]

quiet {

   * Check if absence of clear option would delete dataset
   quiet count
   if r(N) != 0 & "`clear'" == "" {
      local error_msg = "No, data would be lost"
      noisily di as error `"`error_msg'"' //"
      error(999)
   }

  adopath + "FILEPATH"
  create_connection_string, server(ADDRESS)
    local epi_string = r(conn_string)

  import delimited using "FILEPATH/data.csv", asdouble varname(1) clear
  capture drop standard_error
  gen standard_error = meas_stdev 
  preserve
    clear
    # delim ;
    odbc load, exec("
      SELECT effective_sample_size as sample_size, model_version_dismod_id AS a_data_id 
      FROM epi.t3_model_version_dismod
      WHERE model_version_id = `model_version_id'") `epi_string' clear;
    # delim cr
    cap destring a_data_id, replace
    tempfile sample_size
    save `sample_size', replace
  restore
  merge 1:1 a_data_id using `sample_size', nogen
  gen std_dev = standard_error * sqrt(sample_size)
  gen log_std_dev = log(std_dev)
  gen log_meas_value = log(meas_value)
  gen outlier = (std_dev / meas_value > 2) | (std_dev / meas_value < .1)
  drop if outlier==1
  
}

end
//END of file