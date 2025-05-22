
# source libraries --------------------------------------------------------

library(data.table)
library(haven)
library(googlesheets4)
library(stringr)

# check to see if google needs to be run ----------------------------------

check_google_coords <- function(df){
  admin_file_name <- file.path(getwd(), "extract/brinda/param_maps/google_admin_coords.csv")
  if(file.exists(admin_file_name)){
    current_admin_df <- fread(admin_file_name)
    if(all(df$cb_basic_id %in% current_admin_df$ubcov_id)){
      return(FALSE)
    }
  }
  return(TRUE)
}

# get admin names ---------------------------------------------------------

get_location_metadata <- memoise::memoise(
  ihme::get_location_metadata,
  cache = cachem::cache_layered(
    cachem::cache_mem(),
    cachem::cache_disk("FILEPATH")
  )
)

get_admin_names <- function(df){
  admin_df <- data.table()
  loc_df <- get_location_metadata(location_set_id = 35, release_id = 16)[
      ,.(location_id, ihme_loc_id, location_name)
    ]
  for(r in seq_len(nrow(df))){
    extract_df <- read_dta(
      file = df$extracted_file_name[r],
      col_select = dplyr::starts_with("admin_") & 
        dplyr::ends_with(as.character(1:6))
    )
    admin_list <- get_admin_values(extract_df)
    if(!(is.null(admin_list))){
      country_name <- get_country_name(
        ihme_loc_id = df$ihme_loc_id[r],
        loc_df = loc_df
      )
      temp_df <- data.table(
        ubcov_id = df$cb_basic_id[r],
        extract_path = df$extracted_file_name[r],
        year_end = df$year_end[r],
        country_name = country_name,
        admin_level = admin_list$admin_column,
        admin_name = admin_list$admin_values
      )
      admin_df <- rbindlist(
        list(admin_df, temp_df),
        use.names = TRUE,
        fill = TRUE
      )
    }
  }
  return(admin_df)
}

get_admin_values <- function(df){
  admin_str <- "admin_"
  admin_vec <- rev(as.integer(
    str_remove_all(string = colnames(df), pattern = admin_str)
  ))
  for(i in admin_vec){
    temp_col_name <- paste0(admin_str, i)
    admin_col <- df[[temp_col_name]]
    if(all(is.na(suppressWarnings(as.numeric(admin_col))))){
      return(list(
        admin_values = unique(admin_col),
        admin_column = temp_col_name
      ))
    }
  }
  return(NULL)
}

get_country_name <- function(ihme_loc_id, loc_df){
  country_index <- match(
    as.character(substr(ihme_loc_id, 1, 3)), 
    as.character(loc_df$ihme_loc_id)
  )
  return(loc_df$location_name[country_index])
}


# extract coordinates from google sheets ----------------------------------

extract_coordinates_from_gs <- function(df){
  gs_vec <- c("google_sheet_urls")
  
  to_return_df <- data.table()
  for(gs_url in gs_vec){
    main_sheet_name <- "template_sheet"
    num_sheets <- ceiling(nrow(df)/1000)
    MAX_ROWS <- 1000
    for(i in 1:num_sheets){
      new_sheet_name <- paste(Sys.getenv("USER"), i, sep = "_")
      start_index <- (i - 1)*MAX_ROWS + 1
      end_index <- i*MAX_ROWS
      if(i == num_sheets) end_index <- nrow(df)
      temp_df <- df[start_index:end_index,]
      sheet_copy(from_ss = gs_url, from_sheet = main_sheet_name,
                 to_ss = gs_url, to_sheet = new_sheet_name)
      range_write(ss=gs_url,sheet = new_sheet_name,data = temp_df,range = "A2",col_names = F)
    }
    
    #give the google sheet 2 minutes to find all of the coordinates 
    # (it can sometimes be slow, so this almost guarantees that it will return all coordinates)
    sleepy_time <- 120
    Sys.sleep(time = sleepy_time)
    
    for(i in 1:num_sheets){
      new_sheet_name <- paste(Sys.getenv("USER"),i,sep = "_")
      temp_df <- as.data.table(read_sheet(ss = gs_url,sheet = new_sheet_name))
      to_return_df <- rbindlist(
        list(to_return_df, temp_df),
        use.names = TRUE,
        fill = TRUE
      )
      sheet_delete(ss = gs_url,sheet = new_sheet_name)
    }
    to_return_df <- to_return_df[1:nrow(df),]
    temp_df <- to_return_df[is.na(coords) | coords %like% "Loading"]
    if(!(is.null(temp_df)) && nrow(temp_df)>0){
      print("Google sheet errored out, trying next one:")
      print(gs_url)
      to_return_df <- NULL
    }else{
      break
    }
  }
  coords_df <- to_return_df[coords!="#ERROR!"]
  error_df <- to_return_df[is.na(coords) | coords=="#ERROR!" | coords %like% "Loading"]
  
  check_coords_df(coords_df)
  
  return(list(good_df = coords_df,bad_df = error_df))
}

check_coords_df <- function(df){
  for(i in unique(df$ubcov_id)){
    deez_coords <- unique(df[ubcov_id==i,coords])
    if(length(deez_coords)==1){
      message("All coordinates for UbCov ID:",i,"are identical. Check output and rerun!")
    }
  }
}

# main google sheet function ----------------------------------------------

get_google_admin_coordinates <- function(input_df){
  df <- copy(input_df)
  message("Getting admin names...")
  admin_df <- get_admin_names(df = df)
  message("Getting coordinates from google...")
  google_coords_list <- extract_coordinates_from_gs(df = admin_df)
  
  if(nrow(google_coords_list$good_df) > 0){
    write.csv(
      x = google_coords_list$good_df,
      file = file.path(getwd(), "extract/brinda/param_maps/google_admin_coords.csv"),
      row.names = FALSE
    )
  }
  
  if(nrow(google_coords_list$bad_df) > 0){
    write.csv(
      x = google_coords_list$bad_df,
      file = file.path(getwd(), "extract/brinda/param_maps/error_admin_coords.csv"),
      row.names = FALSE
    )
  }
  
  i_vec <- df$cb_basic_id %in% google_coords_list$good_df$ubcov_id
  df <- df[i_vec, ]
  
  return(df)
}
