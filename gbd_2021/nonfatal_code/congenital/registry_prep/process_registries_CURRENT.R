#### process raw registries 
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
source("FILEPATH")

case_map <- as.data.table(read.xlsx("FILEPATH"))

congmalf_filepath <- paste0("FILEPATH")
icbdsr_filepath <-  paste0("FILEPATH")
nbdpn_filepath <- paste0("FILEPATH")
worldatlas_filepath <- paste0("FILEPATH")
china_filepath <- paste0("FILEPATH")
china_mortality_filepath <- paste0("FILEPATH")

drop_cases <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
setnames(drop_cases, "registry", "registry_name")
drop_cases <- drop_cases[registry_name == "icbdms", registry_name := "icbdsr"]

# map just tells which columns in the case_map to use
registry_column_map <- fread("FILEPATH")

# this is based on scoping document and total to component defects 
id_cases <- fread("FILEPATH")
congenital_registries <- c("congmalf", "icbdsr", "nbdpn", "worldatlas", "china", "china_mortality")

for(registry in congenital_registries) {
  registry_filepath <- get(paste0(registry, "_filepath"))
  registry_data <- as.data.table(read.xlsx(registry_filepath))
  print(registry)
  
  reg_cols <- registry_column_map[registry_name == registry, unique(registry_columns)]
  drop <- drop_cases[registry_name == registry]
  id_case <- id_cases[registry_name == registry]
  
  if(registry == "nbdpn"){
    print(registry)
    print("nbdpn ss calculation for is.na(ss)")
    registry_data[is.na(sample_size), sample_size:= cases/mean]
    registry_data[is.na(sample_size), mean := NA]
    registry_data[case_name %like% "Hypoplastic", case_name := "Hypoplastic left heart syndrome"]
  } else{
    print(registry)
    registry_data <- copy(registry_data)
  }
  
  
  if(registry == "worldatlas"){
    print("worldatlas fix underlying_NID column")
    setnames(registry_data, "underlying_NID", "underlying_nid")
  } else {
    print(registry)
    registry_data <- copy(registry_data)
  }
  
  registry_data[, c(grep('X', colnames(registry_data), value = T)) := NULL]
  registry_data[, id := paste0(location_id, "_", year_start)] # creates a unique id based on location and year so can determine which data is duplicate
  
  #abwall
  abwall <- copy(registry_data)
  abwall_id <- id_case[case_type == "abwall", case_name]
  abwall <- abwall[case_name %in% abwall_id]
  
  gas_omph <- copy(registry_data)
  gas_omph_id <- id_case[case_type == "gas_omph", case_name]
  gas_omph <- gas_omph[case_name %in% gas_omph_id]
  gas_omph <- gas_omph[!id %in% unique(abwall$id)]
  
  #-ophthalmos
  thalmous <- copy(registry_data)
  thalmous_id <- id_case[case_type == "thalmous", case_name]
  thalmous <- thalmous[case_name %in% thalmous_id]
  
  anop <- copy(registry_data)
  anop_id <- id_case[case_type == "anop", case_name]
  anop <- anop[case_name %in% anop_id]
  anop <- anop[!id %in% unique(thalmous$id)]
  
  # -otia
  otia <- copy(registry_data)
  otia_id <- id_case[case_type == "otia", case_name]
  otia <- otia[case_name %in% otia_id]
  
  anmic <- copy(registry_data)
  anmic_id <- id_case[case_type == "anmic", case_name]
  anmic <- anmic[case_name %in% anmic_id]
  anmic <- anmic[!id %in% unique(otia$id)]
  
  # - lira
  lira <- copy(registry_data)
  lira_id <- id_case[case_type == "lira", case_name]
  lira <- lira[case_name %in% lira_id]
  
  lira_comp <- copy(registry_data)
  lira_comp_id <- id_case[case_type == "lira_comp", case_name]
  lira_comp <- lira_comp[case_name %in% lira_comp_id]
  lira_comp <- lira_comp[!id %in% unique(lira$id)]
  
  final <- registry_data[!case_name %in% id_case$case_name]
  final <- rbind(final, abwall, gas_omph, thalmous, anop, otia, anmic, lira, lira_comp)
  
  # merge case_map by case_name
  keep_cols <- c(reg_cols, "Level1-Bundel.ID")
  registry_map <- case_map[, c(keep_cols), with = F]
  
  data <- data.table()
  for(col in c(reg_cols)){
    loop_keeps <- c(col, "Level1-Bundel.ID")
    loop_map <- copy(registry_map)
    loop_map <- loop_map[, c(loop_keeps), with = F]
    loop_map <- unique(loop_map)
    setnames(loop_map, loop_keeps, c("case_name", "bundle_id"))
    loop_map <- loop_map[!is.na(case_name)]
    loop_map <- loop_map[!case_name %in% unique(drop$case_name)]
    loop_data <- copy(registry_data)
    loop_data <- loop_data[case_name %in% loop_map$case_name]
    
    test <- merge(loop_data, loop_map, by = 'case_name', all.x = TRUE  , allow.cartesian = TRUE)
    data <- rbind(data, test)
  }
  
  data[, id := NULL]
  
  # adjust for sex specific cases
  data[(case_name %like% c("Hypospadias") | case_name %in% c("Epispadias")) & Denom_SexSpecific_Disorders == "Both", 
       Denom_SexSpecific_Disorders := "Male"]
  
  sex_specific <- copy(data)
  sex_specific <- sex_specific[Denom_SexSpecific_Disorders %in% c("Single_sex", NA) & !case_name %like% "determin"]
  
  indeterminate <- copy(data)
  indeterminate <- indeterminate[case_name %like% "determin"]
  
  both <- copy(data)
  both <- both[Denom_SexSpecific_Disorders %in% c("Male", "Female")]
  
  both <- both[, year_id := year_start]
  
  if(nrow(both) != 0){
    
    pops <- get_population(age_group_id = 164, location_id = unique(both$location_id), year_id = unique(both$year_start), 
                           sex_id = c(1,2,3), gbd_round_id = 7, decomp_step = "step3")
    pops[, c("age_group_id", "run_id") := NULL]
    
    pops <- dcast(pops, formula = location_id + year_id ~ sex_id, value.var = c("population"))
    pops <- data.table(pops)
    pops[, male := pops$"1"/pops$"3"]
    pops[, female := pops$"2"/ pops$"3"]
    pops[, c("1", "2", "3") := NULL]
    
    test <- merge(both, pops, by = c('year_id', 'location_id'))
    
    test <- test[Denom_SexSpecific_Disorders == "Male", sample_size := sample_size * male]
    test <- test[Denom_SexSpecific_Disorders == "Female", sample_size := sample_size * female]
    
    test <- test[Denom_SexSpecific_Disorders == "Male", sex := 'Male']
    test <- test[Denom_SexSpecific_Disorders == "Female", sex := 'Female']
    
    test[, c("year_id", "male", "female") := NULL]
    
    data <- rbind(test, sex_specific)
    
  } else{
    
    data <- copy(sex_specific)
    
  }
  
  if(nrow(indeterminate) != 0){
    
    indeterminate[, ratio := 0.5]
    indeterminate_final <- data.table()
    
    for(row in 1:nrow(indeterminate)){
      male_row <- copy(indeterminate[row])
      
      for(j in c("sample_size", "cases")){
        set(male_row, i=NULL, j=j, value= male_row[[j]] * male_row[["ratio"]])
      }
      male_row[, sex:= "Male"]
      
      female_row <- copy(indeterminate[row])
      for(j in c("sample_size", "cases")){
        set(female_row, i=NULL, j=j, value= female_row[[j]] * female_row[["ratio"]])
      }
      female_row[, sex:= "Female"]
      
      both <- rbind(male_row, female_row)
      indeterminate_final <- rbind(indeterminate_final, both)
      
    }
    
    indeterminate_final[, ratio := NULL]
    
    data <- rbind(data, indeterminate_final)
    
  } else {
    
    data <- copy(data)
    
  }
  
  
  #adjust mtwith data
  if(registry == "china_mortality"){
    print("adjusting mtwith data")
    data <- data[age_end != 0 | age_end !=   0.00000000]
    data <- data[, proportion_dead := (cases/sample_size)]
    ## creates a number variable if there is mtwith at multiple age levels for the same source
    data <- data[, number := .N, by = .(age_end, nid)]
    # drop rows that have 100% mortality (proportion dead == 1) but SS < than 10
    data <- data[!(proportion_dead == 1 & sample_size <10)]
    # drop rows that have 100% mortality (proportion dead == 1) but number of age groups > 1
    data <- data[!(proportion_dead == 1 & number > 1)]
    # for rows that have proportion dead == 1 but don't fall in above categories, set mortality to 95% instead
    data[proportion_dead == 1, proportion_dead := .95]
    data[, mean := ((-log(1-proportion_dead))/(age_end - age_start))]
    data[, lower := 'NA']
    data[, upper := 'NA']
    data[, uncertainty_type_value := 'NA']
    data[, proportion_dead := NULL]
    data[, number := NULL]  
    
  } else {
    print("no mtwith data")
    data[is.na(mean), mean := cases/sample_size] 
  }
  
  #assign raw_cv_includes_chromos
  data <- data[raw_cv_includes_chromos == "1", cv_excludes_chromos := "0"]
  data <- data[raw_cv_includes_chromos == "0", cv_excludes_chromos := "1"]
  data <- data[raw_cv_includes_chromos == "x", cv_excludes_chromos := '']
  
  data[, seq := NA]
  data[, c(grep('DELETE', colnames(data), value = T)):=NULL]
  
  write.xlsx(data, paste0("FILEPATH"),
             row.names = FALSE, sheetName = "extraction")
  
}

