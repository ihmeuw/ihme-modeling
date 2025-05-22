##########################################


#
##########################################

mir_version = REDACTED



all_eye_rb <- list.files(paste0("FILEPATH",mir_version,"/neo_eye_rb"), full.names = T)
all_eye_rb <- all_eye_rb[grepl(paste0("mir_v",mir_version,"_draw"), all_eye_rb)]


all_eye_other <- list.files(paste0("FILEPATH",mir_version,"/neo_eye_other"), full.names = T)
all_eye_other <- all_eye_other[grepl(paste0("mir_v",mir_version,"_draw"), all_eye_other)]

# read in files for each subcause
read_eye_rb = lapply(all_eye_rb, function(x){
  fread(x)
})
read_eye_rb = rbindlist(read_eye_rb, fill = T)

read_eye_other = lapply(all_eye_other, function(x){
  fread(x)
})
read_eye_other = rbindlist(read_eye_other, fill = T)

# bind to total parent cause
total_eye = rbind(read_eye_rb,read_eye_other, fill = T)
total_eye$acause = "neo_eye"

# write combined total draw file
write.csv(total_eye, paste0("FILEPATH",mir_version,"FILEPATH",mir_version,"_combined_total_draws.csv"))

# pull to create combined mean file
eye_rb_means = fread(paste0("FILEPATH",mir_version,"FILEPATH",mir_version,".csv"))
eye_other_means = fread(paste0("FILEPATH",mir_version,"FILEPATH",mir_version,".csv"))

total_eye_wo_draws = rbind(eye_rb_means, eye_other_means, fill=T)
total_eye_wo_draws$acause = "neo_eye"

#write combined mean file
write.csv(total_eye_wo_draws, paste0("FILEPATH",mir_version,"FILEPATH",mir_version,"_combined.csv"))

