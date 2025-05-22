
DataContainer <- setRefClass("DataContainer",
  fields = list(
    release_id = "integer",
    start_year = "integer",
    end_year = "integer",
    output_dir = "ANY",
    population_estimate_version = "ANY",
    version_id = "integer",

    external_inputs = "list",
    file_paths = "list",
    submodel_file_paths = "list",
    data_cache = "list"
  ),
  methods = list(
    initialize = function(release_id = NULL, start_year = ,
                          end_year = , output_dir = NULL,
                          version_id = NULL,
                          population_estimate_version = NULL,
                          external_inputs = NULL) {
      # Check to make sure that all variables have been filled in
      assertthat::assert_that(!is.null(gbd_round_id))
      assertthat::assert_that(!is.null(start_year))
      assertthat::assert_that(!is.null(end_year))
      assertthat::assert_that(!is.null(version_id))

      # Convert to integers and save in object instance
      release_id <<- as.integer(release_id)
      start_year <<- as.integer(start_year)
      end_year <<- as.integer(end_year)
      population_estimate_version <<- as.character(population_estimate_version)
      version_id <<- as.integer(version_id)
      output_dir <<- output_dir
      external_inputs <<- as.list(external_inputs)

      # Set the filepaths
      data_dir <- paste0("FILEPATH")
      file_paths[['location']] <<- paste0("FILEPATH")
      file_paths[['population']] <<- paste0("FILEPATH")
      file_paths[['ldi']] <<- paste0("FILEPATH")
      file_paths[['maternal_education']] <<- paste0("FILEPATH")
      file_paths[['hiv']] <<- paste0("FILEPATH")
      file_paths[['input_5q0']] <<- paste0("FILEPATH")
      file_paths[['raw_adjusted_5q0']] <<- paste0("FILEPATH")
      file_paths[['model_input']] <<- paste0("FILEPATH")
      file_paths[['mlt_hiv']] <<- paste0("FILEPATH")
      file_paths[['output_5q0']] <<- paste0("FILEPATH")
      file_paths[['covid_inf_pc']] <<- paste0("FILEPATH")
      submodel_file_paths[['references_applied']] <<- "FILEPATH"
      submodel_file_paths[['stage_1_input']] <<- "FILEPATH"
      submodel_file_paths[['stage_1_model']] <<- "FILEPATH"
      submodel_file_paths[['stage_1_prediction']] <<- "FILEPATH"
      submodel_file_paths[['bias_adjusted']] <<- "FILEPATH"
      submodel_file_paths[['spacetime_data']] <<- "FILEPATH"
      submodel_file_paths[['spacetime_prediction']] <<- "FILEPATH"
      submodel_file_paths[['spacetime_output']] <<- "FILEPATH"
    },
    get = function(key) {
      # Make sure location data is in data_cache to begin with
      if (!('location' %in% names(data_cache))) {
        if(!file.exists(.self$file_paths[['location']])) {
        print("Pulling mortality location hierarchy...")
        # Pull core mortality location hierarchy
        location_data <- get_location_metadata(location_set_id = 82, release_id = .self$release_id)
		location_data <- location_data[location_data$location_id != 6,]
        islands <- c("GUY", "TTO", "BLZ", "JAM", "ATG", "BHS", "BMU",
        "BRB", "DMA", "GRD", "VCT", "LCA", "PRI")
        location_data[location_data$ihme_loc_id %in% islands, 'region_name'] <- "CaribbeanI"
        # Save to file
        data.table::fwrite(location_data, file = .self$file_paths[['location']],
        row.names = FALSE, na = "")
        # Save to cache
        data_cache[['location']] <<- location_data
        } else {
          print("read from location flat file")
		      data_cache[['location']] <<- data.table::fread(.self$file_paths[['location']])
        }
      }

      # Get the data from the in-memory cache
      if (key %in% names(data_cache)) {
        print("Served from cache")
      } else {
        print(paste("Fetching", key))
        if (key %in% names(file_paths)) {
          if (file.exists(.self$file_paths[[key]])) {
            data <- data.table::fread(.self$file_paths[[key]])
            data_cache[[key]] <<- data
          } else {
            if (key == 'population') {
              # Get the population data
              year_ids <- c(start_year:end_year)
              sex_ids <- c(1, 2, 3)
              age_group_ids <- c(1, 2, 3, 4, 5, 22, 24, 28)
              assertthat::assert_that(!is.null(.self$population_estimate_version))
              pop_data <- get_mort_outputs(model_name="population", model_type="estimate", run_id=.self$population_estimate_version, sex_ids=sex_ids, year_ids=year_ids, age_group_ids=age_group_ids,location_set_id=21)
              # Reformat columns
              setnames(pop_data, old = "mean", new = "pop")
              pop_data <- pop_data[, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'pop')]
              location_data <- data_cache[['location']]
              location_data <- location_data[location_data$location_id != 44849, ]
              location_data <- location_data[location_data$location_id != 420, ]
              location_data <- location_data[location_data$location_id != 428, ]
              location_ids <- location_data$location_id
              index_ids <- list(location_id = location_ids, year_id = year_ids,
                                sex_id = sex_ids, age_group_id = age_group_ids)
              pop_data <- merge(pop_data, location_data[, c('ihme_loc_id', 'location_id')],
                                by = c('location_id'))
              assertable::assert_ids(pop_data, index_ids)
              # Save to file
              .self$save(pop_data, 'population')
              # Save to cache
              data_cache[['population']] <<- pop_data
            } else if (key == 'ldi') {
              # Get the LDI covariate data
              assertthat::assert_that(!is.null(.self$external_inputs[['ldi_pc']]))

              data <- get_covariate_estimates(covariate_id = 57, model_version_id  = .self$external_inputs[['ldi_pc']], release_id = .self$release_id)

              # Check that data only is for sex_id == 3 and age_group_id == 22
              assertthat::assert_that(unique(data$sex_id) == 3)
              assertthat::assert_that(unique(data$age_group_id) == 22)
			  assertthat::assert_that(nrow(data[data$mean_value < 0]) == 0)
              # Rename columns
              data <- data[, c('location_id', 'year_id', 'model_version_id', 'mean_value')]
              data.table::setnames(data, old = "mean_value", new = "ldi")
              data.table::setnames(data, old = "model_version_id",
                                   new = "ldi_model_version_id")


              # Save to file
              .self$save(data, 'ldi')
              # Save to cache
              data_cache[[key]] <<- data
            } else if (key == 'maternal_education') {
              # Get the maternal education covariate data
              assertthat::assert_that(!is.null(.self$external_inputs[['maternal_edu']]))
              data <- get_covariate_estimates(covariate_id = 463, model_version_id = .self$external_inputs[['maternal_edu']], release_id = .self$release_id)

              # Check that data only is for sex_id == 3 and age_group_id == 22
              assertthat::assert_that(unique(data$sex_id) == 3)
              assertthat::assert_that(unique(data$age_group_id) == 22)
              # Rename columns
              data <- data[, c('location_id', 'year_id', 'model_version_id', 'mean_value')]
              data.table::setnames(data, old = "mean_value", new = "maternal_edu")
              data.table::setnames(data, old = "model_version_id",
                                   new = "maternal_edu_model_version_id")

              # Save to file
              .self$save(data, 'maternal_education')
              # Save data to cache
              data_cache[[key]] <<- data
            } else if (key == 'covid_inf_pc') {
              # Get the covid infections covariate data
              assertthat::assert_that(!is.null(.self$external_inputs[['covid_inf_pc']]))
              data <- get_covariate_estimates(covariate_id = 2535, model_version_id = .self$external_inputs[['covid_inf_pc']], release_id = .self$release_id)

              # Check that data only is for sex_id == 3 and age_group_id == 22
              assertthat::assert_that(unique(data$sex_id) == 3)
              assertthat::assert_that(unique(data$age_group_id) == 22)
              # Rename columns
              data <- data[, c('location_id', 'year_id', 'model_version_id', 'mean_value')]
              data.table::setnames(data, old = "mean_value", new = "covid_inf_pc")
              data.table::setnames(data, old = "model_version_id",
                                   new = "covid_inf_pc_model_version_id")
              # Save to file
              .self$save(data, 'covid_inf_pc')
              # Save data to cache
              data_cache[[key]] <<- data
            } else if (key == 'hiv') {
              # Get the HIV covariate data
              assertthat::assert_that(!is.null(.self$external_inputs[['hiv_child_cdr']]))
              data <- get_covariate_estimates(covariate_id = 214, model_version_id = .self$external_inputs[['hiv_child_cdr']], release_id = .self$release_id)

              # Check that data only is for sex_id == 3 and age_group_id == 22
              assertthat::assert_that(unique(data$sex_id) == 3)
              assertthat::assert_that(unique(data$age_group_id) == 22)
              # Rename columns
              data <- data[, c('location_id', 'year_id', 'model_version_id', 'mean_value')]
              data.table::setnames(data, old = "mean_value", new = "hiv")
              data.table::setnames(data, old = "model_version_id",
                                   new = "hiv_model_version_id")

              # Add in data from 1950 to 1970
              location_ids <- c()
              year_ids <- c()
              for (l in unique(data$location_id)) {
                for (y in c(1950:1969)) {
                  location_ids <- c(location_ids, l)
                  year_ids <- c(year_ids, y)
                }
              }
              dt <- data.table(location_id = location_ids, year_id = year_ids)
              hiv_version_id <- data$hiv_model_version_id[1]
              filler_data <- data.table(location_id = location_ids, year_id = year_ids,
                                        hiv = 0, hiv_model_version_id = hiv_version_id)
              data <- rbind(data, filler_data, use.names=T, fill = T)
              # Save to file
              .self$save(data, 'hiv')
              # Save to cache
              data_cache[[key]] <<- data
            } else {
              stop(paste(paste0('"', key, '"'), "is missing a function for this data container"))
            }
          }
        } else {
          stop(paste(paste0('"', key, '"'), "is not a valid key for this data container"))
        }
      }
      return(data_cache[[key]])
    },
    get_submodel = function(submodel_id, key) {
      file_path <- paste0("FILEPATH")
      if (key == 'stage_1_model') {
        load(data, file = file_path)
      } else {
        data <- data.table::fread(file_path)
        return(data)
      }
    },
    save = function(data, key) {
      valid_keys <- c('input_5q0', 'raw_adjusted_5q0', 'model_input',
                      'population', 'ldi', 'maternal_education', 'hiv',
                      'mlt_hiv', 'covid_inf_pc')
      if (key %in% valid_keys) {
        data.table::fwrite(data, file = .self$file_paths[[key]],
                  row.names = FALSE, na = "")
      } else {
        stop(paste(paste0('"', key, '"'), "is not a valid save key for this data container"))
      }
    },
    save_submodel = function(data, submodel_id, key) {
      if (key %in% names(submodel_file_paths)) {
        dir.create(paste0("FILEPATH"),
                   showWarnings = FALSE)
        dir.create(paste0("FILEPATH"),
                   showWarnings = FALSE)
        file_path <- paste0("FILEPATH")
        if (key == 'stage_1_model') {
          return(file_path)
        } else {
          data.table::fwrite(data, file = file_path, row.names = FALSE, na = "")
        }
      } else {
        stop(paste(paste0('"', key, '"'), "is not a valid key for this data container"))
      }
    }
  )
)
