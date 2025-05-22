
# source libraries --------------------------------------------------------

library(data.table)
library(ggplot2)

# define input file paths -------------------------------------------------

bundle_map <- fread(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))
output_dir <- 'FILEPATH'

loc_df <- ihme::get_location_metadata(location_set_id = 35, release_id = 16) |>
  dplyr::filter(level == 1) |>
  setnames(old = 'location_name', new = "Super Region")

pdf("FILEPATH/age_sex_diagnostics.pdf")

for(r in seq_len(nrow(bundle_map))){
  age_sex_file_path <- file.path(
    output_dir,
    bundle_map$id[r],
    'age_sex_split_mrbrt.csv'
  )
  
  print(age_sex_file_path)
  
  if(file.exists(age_sex_file_path)){
  
    df <- fread(age_sex_file_path)
    df <- df[to_split == 1]
    
    if(nrow(df) > 0){
    
      df <- merge.data.table(
        x = df,
        y = loc_df[,.(location_id, `Super Region`)],
        by.x = 'super_region_id',
        by.y = 'location_id'
      )
      
      high_age_df <- df[age_start >= 2]
      low_age_df <- df[age_start <= 2]
      
      p <- ggplot(high_age_df, aes(x = age_start, y = val, colour = `Super Region`, shape = as.factor(sex_id))) +
        geom_point() +
        labs(
          title = "Split Trends",
          subtitle = paste("Bundle:", bundle_map$name[r]),
          x = "Age Start",
          y = if(bundle_map$measure[r] == 'continuous') "Hemoglobin (g/L)" else "Prevalence"
        ) +
        theme_minimal()
      
      plot(p)
      
      p <- ggplot(high_age_df, aes(x = age_start, y = mean_diff, colour = `Super Region`, shape = as.factor(sex_id))) +
        geom_point() +
        labs(
          title = "Difference in orginal vs split mean",
          subtitle = paste("Bundle:", bundle_map$name[r]),
          x = "Age Start",
          y = if(bundle_map$measure[r] == 'continuous') "Hemoglobin Difference (g/L)" else "Prevalence Difference"
        ) +
        theme_minimal()
      
      plot(p)
      
      if(bundle_map$cv_pregnant[r] == 0){
        p <- ggplot(low_age_df, aes(x = sqrt_age_start, y = val, colour = `Super Region`, shape = as.factor(sex_id))) +
          geom_point() +
          labs(
            title = "Split Trends",
            subtitle = paste("Bundle:", bundle_map$name[r]),
            x = "Squareroot Age Start",
            y = if(bundle_map$measure[r] == 'continuous') "Hemoglobin (g/L)" else "Prevalence"
          ) +
          theme_minimal()
        
        plot(p)
        
        p <- ggplot(low_age_df, aes(x = sqrt_age_start, y = mean_diff, colour = `Super Region`, shape = as.factor(sex_id))) +
          geom_point() +
          labs(
            title = "Difference in orginal vs split mean",
            subtitle = paste("Bundle:", bundle_map$name[r]),
            x = "Squareroot Age Start",
            y = if(bundle_map$measure[r] == 'continuous') "Hemoglobin Difference (g/L)" else "Prevalence Difference"
          ) +
          theme_minimal()
        
        plot(p)
      }
    }
  }else{
    print("File does not exist!!")
  }
}

dev.off()