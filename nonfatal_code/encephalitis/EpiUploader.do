run "/home/j/temp/central_comp/libraries/current/stata/upload_epi_data.ado"
run "/home/j/temp/central_comp/libraries/current/stata/get_epi_data.ado"

local date 03_17_17
//download and delete data for epilepsy
//get_epi_data, bundle_id(401) clear
//keep bundle_id seq
//export excel "/home/j/WORK/12_bundle/encephalitis/401/03_review/02_upload/delete_all_`date'.xlsx", sheet("extraction") sheetreplace firstrow(variables)
//upload_epi_data, bundle_id(401) filepath("/home/j/WORK/12_bundle/encephalitis/401/03_review/02_upload/delete_all_`date'.xlsx") clear
//upload new epilepsy data
upload_epi_data, bundle_id(401) filepath("/home/j/WORK/12_bundle/encephalitis/gbd2016/04_big_data/2821/dm_custom_input_encephalitis__epilepsy_2017_03_22.xlsx") clear

//download and delete data for moderate to severe impairment due to epilepsy
//get_epi_data, bundle_id(400) clear
//keep bundle_id seq
//export excel "/home/j/WORK/12_bundle/encephalitis/400/03_review/02_upload/delete_all_`date'.xlsx", sheet("extraction") sheetreplace firstrow(variables) 
//upload_epi_data, bundle_id(400) filepath("/home/j/WORK/12_bundle/encephalitis/400/03_review/02_upload/delete_all_`date'.xlsx") clear
//upload new moderate to severe impairment
upload_epi_data, bundle_id(400) filepath("/home/j/WORK/12_bundle/encephalitis/gbd2016/04_big_data/2815/dm_custom_input_encephalitis_long_modsev_2017_03_22.xlsx") clear
