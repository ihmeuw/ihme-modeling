
# load in command args ----------------------------------------------------

if(interactive()){
  task_id <- 8
  map_file_name <- file.path(getwd(), "model_prep/param_maps/measure_preg_df.csv")
  output_dir <- "FILEPATH"
}else{
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  command_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- command_args[1]
  output_dir <- command_args[2]
}

measure_preg_df <- read.csv(map_file_name)

current_elevation_adjust_type <- measure_preg_df$elevation_adj_type[task_id]
current_cv_pregnant <- measure_preg_df$cv_pregnant[task_id]

bundle_map <- read.csv(file.path(getwd(), "model_prep/param_maps/bundle_map.csv")) |>
  dplyr::filter(
    elevation_adj_type == current_elevation_adjust_type &
      cv_pregnant == current_cv_pregnant
  )

ihme_loc_df <- ihme::get_location_metadata(location_set_id = 35, release_id = 16)

# load in current data and then outlier based on 2021 ---------------------

outlier_anemia_data <- function(bundle_dat, loc_df, elevation_type, cv_preg, data_dir) {
  bun_id_list <- get_id_list(bundle_dat)
  aslyn_combos_to_outlier <- main_outlier_function(bun_id_list, loc_df, data_dir)
  
  if(interactive()) {
    assign(
      x = 'aslyn_combos',
      value = aslyn_combos_to_outlier,
      envir = .GlobalEnv
    )
  }  
  
  # get unique age/sex/location/year/nid values
  # load back in data with the current elevation adj/cv_preg and outlier the combos in the df
  
  apply_outliers(
    bun_id_list = bun_id_list,
    outlier_combo_df = aslyn_combos_to_outlier,
    data_dir = data_dir
  )
}

get_id_list <- function(bundle_dat) {
  list(
    mean_hemoglobin = bundle_dat |>
      dplyr::filter(measure == 'continuous') |>
      purrr::chuck('id'),
    total_anemia = bundle_dat |>
      dplyr::filter(grepl('anemia_anemic_', var)) |>
      purrr::chuck('id'),
    modsev_anemia = bundle_dat |>
      dplyr::filter(grepl('anemia_mod_sev_', var)) |>
      purrr::chuck('id'),
    severe_anemia = bundle_dat |>
      dplyr::filter(grepl('anemia_severe_', var)) |>
      purrr::chuck('id')
  )
}

main_outlier_function <- function(bun_id_list, loc_df, data_dir) {
  lapply(bun_id_list, \(bun_id) {
    file_name <- file.path(
      data_dir,
      bun_id,
      'post_dedup.fst'
    )
    fst::read.fst(path = file_name, as.data.table = TRUE) |>
      filter_group_review() |>
      dplyr::mutate(is_outlier = 0) |>
      outlier_nids(loc_df = loc_df) |>
      dplyr::filter(is_outlier == 1) |>
      dplyr::select(
        nid, age_group_id, sex_id, year_id, location_id, group, site_memo
      ) |>
      data.table::setDT()
  }) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
    unique() |>
    dplyr::mutate(apply_outlier = 1)
}

filter_group_review <- function(df){
  i_vec <- which(df$group_review == 1 | is.na(df$group_review))
  
  df <- df[i_vec, ]
  
  return(df)
}

outlier_nids <- function(df, loc_df){
  
  df <- df |>
    dplyr::mutate(
      ui_value = dplyr::case_when(
        measure == 'continuous' ~ variance,
        .default = sqrt(variance)
      ),
      ui_ratio = ui_value / val,
      required_sample_size = dplyr::case_when(
        grepl('anemia_anemic', var) ~ 5,
        grepl('anemia_mod_sev', var) ~ 5,
        grepl('anemia_severe', var) ~ 5,
        .default = 5
      )
    )
  
  uk_locs <- loc_df |>
    dplyr::filter(grepl(',95,', path_to_top_parent))
  
  i_vec <- which(
    (df$location_id %in% uk_locs$location_id & ((df$imputed_measure_row == 1 & df$val == 0) | (df$measure == 'proportion' & df$val > 0.8))) |
      (df$nid == 144314 & df$location_id == 34) | # unverified VMNIS data from Azerbaijan
      (df$nid == 139870 & df$site_memo != 'Dundgovi, Gobi-Altay, Hentiy, Selenge, Ulaanbaatar') |
      (df$nid == 143269 & df$site_memo != 'An Giang,Bac Kan,Bac Ninh,Dak Lak,Ha Noi City,Thua Thien - Hue') | 
      (grepl('KEN_', df$ihme_loc_id) & df$age_group_id %in% c(31, 32, 235)) | # improbable older age trends in Kenya driving high anemia trends in whole subregion
      (df$location_id == 131 & df$year_id < 1995) | # inconsistent VMNIS data before 1995 in Nicaragua
      (df$location_id == 140 & df$imputed_measure_row == 1) | # bad imputation for Bahrain
      (df$location_id == 4626 & df$nid %in% c(144231, 144232)) | # from birth data from Bristol and avon only, incorrectly age ID'd
      (df$location_id == 154 & df$age_end > 55) | # Tunisia bad imputation for older age groups
      (df$location_id == 101 & df$nid %in% c(143777, 143780, 143781)) | # Canada data from facility, Toronto iron supplementation x-study, Montreal only study looking at anemia in kids
      (df$location_id == 68 & df$nid %in% c(144177, 144178, 144314)) | # South Korea urban only data, data from facility, unverified VMNIS data
      (df$location_id == 71 & df$nid %in% c(143697, 143699)) | # non-representative data from Austrailia in VMNIS (data from perth only)
      (df$location_id == 532 & df$nid == 144249) | # Florida x-sectional study on the influence of cow's milk on anmia status
      (df$location_id == 543) | # All Maryland data is from hospital nor nationally representative
      (df$location_id == 558 & df$nid == 144256) | # Hospital data from ohio
      (df$location_id == 78 & df$nid != 143846) | # lot's of non-nationally representative data in Denmark
      (df$location_id == 40 & df$nid == 144314) | # duplicate data for Turkmenistan, old data not processed properly for group review
      (df$location_id == 130 & df$nid == 8618 & df$age_group_id %in% c(2, 3, 388, 389)) | # implausible under 2 data in Mexico
      (grepl('MEX', df$ihme_loc_id) & df$nid == 144069) | # Mexico survey looking at impacts of supplementation on nutrition
      (df$location_id == 4649 & df$sex_id == 1 & df$age_group_id == 31) | # unrealistic data point for 85-90yo males
      (df$location_id == 132 & df$nid == 144314) | # Panama data from GBD 2021 data set that's not cleaned up/verified
      (df$location_id == 133 & df$nid != 144289) | # Venezuela data with that is not nationally representative
      (df$location_id == 143 & df$nid == 143996) | # Iraq data that is only from Baghdad
      (df$location_id == 157 & df$nid == 144299) | # Yemen data that is only for one city from VMNIS
      #(df$location_id == 161 & !(df$nid %in% c(261683, 55956))) | # Bangledesh data that is not nationally representative --> inliered due to better processing
      (df$location_id == 416 & df$urbancity_type != 'Urban') | # Urban data for Tuvalu got through. outlier for now
      (df$location_id == 72 & df$nid == 144093) | # non-representative data for new zealand from VMNIS 
      (df$location_id == 555 & df$nid == 144279) | # hospital data from NY, NY
      (df$location_id == 97 & df$nid != 56715) | # Argentina data that is not nationally representative 
      #(df$location_id == 162 & df$nid != 425208) | # Bhutan data that is not nationally representative --> inliered due to better processing
      (df$nid == 119643) | # China data that only contains urban data from one province 
      (df$nid == 143808) | # China data that only contains anemic children
      (df$location_id == 48 & df$nid %in% c(143906)) | # Hungary facility data
      (df$location_id == 63 & df$nid == 144314) | # Duplicate Ukraine data
      (df$location_id == 189 & df$nid %in% c(144196, 110784, 144190)) | # VMNIS data from Tanzania that isn't fully representative, conflicts with DHS
      (grepl('JPN', df$ihme_loc_id) & df$nid %in% c(144314, 144010)) | # Duplicate Japan data, facility data
      (df$location_id == 217 & df$nid == 77393 & df$age_group_id == 2) | # improperly coded value for ENN in SLE
      (grepl('NGA', df$ihme_loc_id) & df$nid == 144103) | # Nigeria facility data
      (df$location_id == 18 & df$nid %in% c(144203, 144199, 144207)) | # Thailand data from only a few regions, vegetarians vs non vegetarians (non veggie team = more iron)
      (df$location_id == 8 & df$nid == 143810) | # Non nationally representative data from Taiwan
      (df$location_id == 95 & df$nid == 13265 & df$age_group_id == 2) | # UK data point that has 100% total prevalence. Not representative
      (df$nid == 143905) | # facility data Sweden
      (df$nid == 21039 & df$location_id == 41 & df$sex_id == 1 & df$age_group_id %in% 10:20) | # Uzbekistan males that are far too high
      (df$location_id == 202 & df$nid == 144314) | # Cameroon data from VMNIS that wasn't fully processed
      (df$location_id == 25 & df$nid != 139971) | # Micronesia data that is facility data
      (df$location_id == 152 & df$nid %in% c(144147, 144148)) | # Saudi Arabia data that is from hospitals/not nationally representative
      #(df$location_id == 123 & !(df$nid %in% c(359163, 359146, 139818))) | # Outliering data in Peru that is not DHS/nantionally representative
      (df$location_id == 126 & df$nid %in% c(143821, 143822)) | # removing non-nationally representative data from costa rica
      (df$location_id == 28 & df$nid == 142784) | # solomon island data that has implausible early age group data
      (df$nid == 264956 & df$age_group_id == 2) | # indonesia data with unclear definition of under 1 age group
      (df$nid == 2223 & df$age_group_id == 2) | # far too high of ENN prevalence with too small of variance from collapse code
      (df$nid == 57990 & df$age_group_id == 2) | # unreliable ENN data
      (df$location_id == 175 & df$nid == 144314) | # unproccessed VMNIS data for Burundi
      (df$location_id == 191 & df$nid == 144314) | # unproccessed VMNIS data for Zambia
      (grepl('BRA', df$ihme_loc_id) & df$nid %in% c(143752, 143753)) | # Facility data from Brazil
      (df$location_id == 102 & df$nid == 144277) | # US data from WIC
      #(df$from_2021 == 1 & df$vmnis == 1) | # lot's of unreliable extracted data from 2021
      (df$location_id == 199 & df$nid == 77393 & df$age_group_id == 2) | # low ENN data point in Sierra lione
      (df$location_id == 97 & df$nid == 137334 & df$age_group_id == 2) | # low ENN data point in Argentina
      (df$location_id == 180 & df$nid == 58006 & df$age_group_id == 2) | # low ENN data point in Kenya
      (df$location_id == 163 & df$nid == 467681 & df$age_group_id == 2) | # too high of ENN prev in India DHS survey
      (grepl('IND', df$ihme_loc_id) & df$nid %in% c(23219, 165390, 233917)) | # India survey with super low hb values, including 2014 Anthropometric and Bio-Chemical Survey
      (df$nid == 22680 & df$location_id == 4724 & (df$imputed_elevation_adj_row == 1 | df$val == 1)) | # East Java bad imputations for elevation
      (!(grepl('mod_sev|severe', df$var)) & df$val > 0 & df$ui_ratio > 2) | 
      #(df$val > 0 & (df$ui_ratio > 2)) | 
      (!(df$age_group_id %in% c(2, 3, 388, 389)) & !(is.na(df$sample_size)) & 
         !((df$sample_size >= df$required_sample_size) | (!is.na(df$orig_sample_size) & df$orig_sample_size >= df$required_sample_size))) | 
      (df$variance < 1 * 10 ^ (-8)) |
      (df$year_id < 1980) |
      (grepl('anemia', df$var) & (df$val > 1 | df$val < 0))
  )
  df$is_outlier[i_vec] <- 1
  
  if('post_impute_prev' %in% colnames(df)) {
    print('post impute prev')
    i_vec <- which(!(is.na(df$bad_prevalence_row)) & df$bad_prevalence_row == 1)
    df$is_outlier[i_vec] <- 1
  }
  
  return(df)
}

apply_outliers <- function(bun_id_list, outlier_combo_df, data_dir) {
  for(bun_id in bun_id_list) {
    file_name <- file.path(
      data_dir,
      bun_id,
      'post_dedup.fst'
    )
    
    dat <- fst::read.fst(file_name, as.data.table = TRUE)
    dat$is_outlier <- 0
    if('apply_outlier' %in% colnames(dat)) dat$apply_outlier <- NULL
    
    merge_cols <- intersect(colnames(dat), colnames(outlier_combo_df))
    print(merge_cols)
    
    dat <- dat |>
      data.table::merge.data.table(
        y = outlier_combo_df,
        by = merge_cols,
        all.x = TRUE
      ) |>
      dplyr::mutate(
        is_outlier = dplyr::case_when(
          !(is.na(apply_outlier)) & apply_outlier == 1 ~ 1,
          .default = is_outlier
        )
      )
    
    if(interactive()) {
      assign(
        x = paste0('bun_', bun_id),
        value = dat,
        envir = .GlobalEnv
      )
    } else {
      file_name <- file.path(
        data_dir,
        bun_id,
        'final_data.fst'
      )
      
      fst::write.fst(
        x = dat,
        path = file_name
      ) 
    }
  }
}

# run functions -----------------------------------------------------------

outlier_anemia_data(
  bundle_dat = bundle_map,
  loc_df = ihme_loc_df,
  elevation_type = current_elevation_adjust_type,
  cv_preg = current_cv_pregnant,
  data_dir = output_dir
)
