library(data.table)

age_df <- ihme::get_age_metadata(release_id = 9)
sex_id <- 1:2
preg_id <- 0:1

anemia_map_df <- as.data.table(expand.grid(age_group_id = age_df$age_group_id,
                                           sex_id = sex_id,
                                           cv_pregnant = preg_id))
anemia_map_df <- anemia_map_df[cv_pregnant==0 | (cv_pregnant==1 & sex_id==2 & age_group_id >= 8 & age_group_id <= 15)]
anemia_map_df <- merge.data.table(anemia_map_df,age_df[,.(age_group_id,age_group_name,age_group_years_start)], by = "age_group_id")
anemia_map_df <- anemia_map_df[order(age_group_years_start)]
anemia_map_df$age_group_years_start <- NULL

write.csv(anemia_map_df, file.path(getwd(), "mrbrt/elevation_adjust/anemia_map.csv"), row.names = F)
