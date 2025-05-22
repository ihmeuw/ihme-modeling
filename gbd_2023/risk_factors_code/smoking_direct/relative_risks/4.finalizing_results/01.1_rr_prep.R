library(data.table)
i <- commandArgs(trailingOnly = T)[1]
reason <- commandArgs(trailingOnly = T)[2]
rr_files <- commandArgs(trailingOnly = T)[3]
final_file_path <- commandArgs(trailingOnly = T)[4]
RO_pairs <- commandArgs(trailingOnly = T)[5]
file_path <- commandArgs(trailingOnly = T)[6]

if(reason == "continuous"){
  RO_pairs <- fread(RO_pairs)
  rr_files <- paste0(rr_files, i, ".csv")
  temp <- fread(rr_files)
  message(paste0(i))
  setDT(temp)
  setDT(RO_pairs)
  RO <- RO_pairs[cause_name == i]
  temp <- temp[, draw := paste0("draw_", draw)]
  temp <- temp[, rr := ifelse(rr < 0, 0.01, rr)]
  temp <- dcast.data.table(temp, exposure + age_group_id + sex_id ~ draw, value.var = "rr")
  temp <- temp[, `:=` (location_id = 1, mortality = 1, morbidity = 1, year_id = 2021,
                       cause_id = RO$cause_id, rei_id = RO$rei_id, modelable_entity_id = RO$modelable_entity_id,
                       parameter = NA)]
  
  # keep only 1000 levels (1-99.9) for exposure
  temp <- temp[exposure!=100,]

  assign(i, temp)
  print(paste(i, "DONE"))
  write.csv(get(i), paste0(file_path, RO$cause_id, ".csv"), row.names = F)
  message("All Done")
  
} else if(reason == "dichotomous"){
  RO_pairs <- fread(RO_pairs)
  rr_files <- paste0(rr_files, i, ".csv")
  temp <- fread(rr_files)
  file <- copy(temp)
  setDT(temp)
  setDT(file)
  RO <- RO_pairs[cause_name == i]
  temp <- temp[, draw := paste0("draw_", draw)]
  temp <- dcast.data.table(temp, cause_id + age_group_id + sex_id ~ draw, value.var = "rr")
  temp <- temp[, `:=` (location_id = 1, mortality = 1, morbidity = 1, year_id = 2021,
                       rei_id = RO$rei_id, modelable_entity_id = RO$modelable_entity_id,
                       exposure = NA, parameter = "cat1")]
  try <- copy(file)
  if(!(try$draw[3] %like% "draw_")){
    try <- try[, draw := paste0("draw_", draw)]
  }
  try <- try[, rr := 1]
  try <- dcast.data.table(try, cause_id + age_group_id + sex_id ~ draw, value.var = "rr")
  try <- try[, `:=` (location_id = 1, mortality = 1, morbidity = 1, year_id = 2021,
                     rei_id = RO$rei_id, modelable_entity_id = RO$modelable_entity_id,
                     exposure = NA, parameter = "cat2")]
  final_fractures <- rbindlist(list(temp, try), use.names = TRUE)
  write.csv(final_fractures, paste0(file_path, RO$cause_id, ".csv"), row.names = F)
  message("All Done")
  
} else if(reason == "final"){
  files <- list.files(file_path, full.names = F)
  causes_produced <- gsub(".csv","",files)
  all_rr <- data.table()
  for(i in causes_produced){
    temp <- fread(paste0(file_path, i, ".csv"))
    if(i == "502"){
      pad <- copy(temp)
    }
    all_rr <- rbindlist(list(all_rr, temp), use.names = T)
    print(i)
  }
  write.csv(all_rr, paste0(final_file_path, "all_draws.csv"), row.names = F)
  message("All RR draws saved")
  
  # calculate the unmediated RR draws for PAD, cause_id: 502
  pad <- melt(pad, exposure + age_group_id + sex_id ~ draw, value.var = "rr")
  idvars <- c("exposure","age_group_id","sex_id","location_id","mortality",
              "morbidity","year_id","cause_id","rei_id","modelable_entity_id", "parameter")
  pad_wide <- melt(pad, id.vars = idvars, measure.vars = paste0("draw_",0:999))
  setnames(pad_wide, c("variable","value"),c("draw","rr"))
  
  # load the mediation factor draws for PAD
  mf_matrix <- fread(FILEPATH)[rei_id==99 & cause_id==502]
  mf_matrix <- melt(mf_matrix, id.vars = c('rei_id', 'med_id', 'cause_id'), 
                    measure.vars = paste0("draw_", 0:999)) %>% data.table
  setnames(mf_matrix, c("variable","value"),c("draw","mf"))

  # merge the mediation factor draws with the unmediated RR draws
  pad_wide <- merge(pad_wide, mf_matrix[,.(draw, mf)], by = "draw", all.x = T) %>% data.table
  setDT(pad_wide)
  
  # calculate the unmediated RR draws for PAD
  pad_wide[, rr := (rr-1)*(1-mf)+1]
  pad_wide[, mf := NULL]
  
  # format the unmediated draws
  pad_final <- dcast.data.table(pad_wide, exposure + age_group_id + sex_id + 
                                  location_id + mortality + morbidity + year_id + 
                                  cause_id + rei_id + modelable_entity_id + parameter ~ draw, value.var = "rr")
  
  # save the final unmediated draws
  write.csv(pad_final, paste0(gsub("wide_GBD2021/","wide_GBD2021_um/",final_file_path), "all_draws.csv"), row.names = F)
  message("Unmediated RR draws for PAD saved")
}

