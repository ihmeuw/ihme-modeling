
# source libraries --------------------------------------------------------

library(data.table)
library(ggplot2)

# define input file paths -------------------------------------------------

bundle_map <- fread(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))
output_dir <- 'FILEPATH'

loc_df <- ihme::get_location_metadata(location_set_id = 35, release_id = 16)
age_df <- ihme::get_age_metadata(release_id = 16)

setnames(
  age_df,
  old = c('age_group_years_start', 'age_group_years_end'),
  new = c('age_start', 'age_end')
)

bunlde_rows <- which(bundle_map$name %like% "WHO|BRINDA" & bundle_map$cv_pregnant == 0)

for(r in bunlde_rows){
  full_file_path <- file.path(
    output_dir,
    bundle_map$id[r],
    'elevation_adj_measure2.csv'
  )
  
  print(full_file_path)
  
  if(file.exists(full_file_path)){
    
    file_name <- paste0("FILEPATH/full_diagnostics_", bundle_map$id[r], ".pdf")
    pdf(file_name, width = 15, height = 8)
  
    df <- fread(full_file_path)
    df$age_start <- NULL
    df$age_end <- NULL
    df$location_name <- NULL
    bad_df <- df[sex_id == 3 | group_review == 0]
    print(paste("Num bad rows = ", nrow(bad_df)))
    df <- df[sex_id != 3 & (is.na(group_review) | group_review != 0)]
    
    df <- merge.data.table(
      x = df,
      y = age_df[,.(age_group_id, age_start, age_end)],
      by = 'age_group_id'
    )
    
    if(nrow(df) > 0){
      
      df <- merge.data.table(
        x = df,
        y = loc_df[,.(location_id, location_name)],
        by = 'location_id', 
        all.x = TRUE
      )
      
      df$Sex <- ifelse(
        df$sex_id == 1,
        'Male',
        'Female'
      )
      
      df <- df[imputed_elevation_adj_row == 1, `Imputation Type` := 'Elevation Imputed']
      df <- df[imputed_measure_row == 1, `Imputation Type` := 'Measure Imputed']
      df <- df[to_split == 1, `Imputation Type` := 'Age/Sex Split Imputed']
      df <- df[is.na(`Imputation Type`), `Imputation Type` := 'Original/Unadjusted Point']
      
      for(i in unique(df$location_id)){
        
        high_age_df <- df[age_start >= 2 & location_id == i]
        low_age_df <- df[age_start <= 2 & location_id == i]
        
        p <- ggplot(high_age_df, aes(x = age_start, y = val, colour = `Imputation Type`, shape = `Sex`)) +
          geom_point() +
          labs(
            title = paste("Split Trends -", unique(high_age_df$location_name)),
            subtitle = paste("Bundle:", bundle_map$name[r]),
            x = "Age Start",
            y = if(bundle_map$measure[r] == 'continuous') "Hemoglobin (g/L)" else "Prevalence"
          ) +
          theme_minimal()
        
        if(bundle_map$cv_pregnant[r] == 0){
          q <- ggplot(low_age_df, aes(
              x = if(bundle_map$measure[r] == 'continuous') age_start else age_start,
              y = val, 
              colour = `Imputation Type`, 
              shape = `Sex`
            )) +
            geom_point() +
            labs(
              title = paste("Split Trends -", unique(low_age_df$location_name)),
              subtitle = paste("Bundle:", bundle_map$name[r]),
              x = if(bundle_map$measure[r] == 'continuous') "Log of Age Midpoint" else "Squareroot Age Start",
              y = if(bundle_map$measure[r] == 'continuous') "Hemoglobin (g/L)" else "Prevalence"
            ) +
            theme_minimal()
          
          gridExtra::grid.arrange(p, q, nrow = 1)
        }else{
          plot(p)
        }
      }
    }
    
    dev.off()
  }else{
    print("File does not exist!!")
  }
}
