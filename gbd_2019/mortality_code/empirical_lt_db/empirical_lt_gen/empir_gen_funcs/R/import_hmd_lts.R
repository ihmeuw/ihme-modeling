#' Import all HMD lifetables, format them long, and reunite East and West Germany for HMD
#'
#' @return data.table with variables ihme_loc_id, sex, age (numeric, values 0, 1, 5 (5) 110), source (character), qx
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

import_hmd_lts <- function() {

    root <- "FILEPATH"

    format_hmd_ages <- function(dt) {
        dt <- copy(dt)
        suppressWarnings(dt <- setDT(tidyr::separate(dt, "age", into = c("new_age"))))
        dt[, age_int := as.integer(new_age)]
        dt[, c("new_age") := NULL]
        setnames(dt, "age_int", "age")
        return(dt)
    }

    hmd_male_folder <- paste0(root, "FILEPATH/HMD/mltper_5x1")
    hmd_female_folder <- paste0(root, "FILEPATH/HMD/fltper_5x1")

    male_files <- list.files(hmd_male_folder)
    female_files <- list.files(hmd_female_folder)

    import_country_filename <- function(filename) {
        country_name <- strsplit(filename, ".", fixed = T)[[1]][1]
        result <- fread(filename)
        result <- result[!is.na(Year)]
        result[, iso3 := country_name]
        return(result)
    }

    male_results <- assertable::import_files(filenames = male_files, folder = hmd_male_folder, FUN = import_country_filename, multicore = T, mc.cores = 4)
    female_results <- assertable::import_files(filenames = female_files, folder = hmd_female_folder, FUN = import_country_filename, multicore = T, mc.cores = 4)

    male_results[, sex := "male"]
    female_results[, sex := "female"]

    results <- rbindlist(list(male_results, female_results), use.names = T)
    setnames(results, "Lx", "nlx")
    setnames(results, colnames(results), tolower(colnames(results)))
    results <- format_hmd_ages(results)
    results[, iso3 := sub('.*\\/', '', iso3)]

    ## Drop BEL results that have missing values for all lifetable characteristics
    results <- results[iso3 != "BEL" | (! year %in% c(1914:1918))]

    for(var in c("mx", "qx", "ax", "lx", "dx", "nlx", "tx", "ex")) {
        results[, (var) := as.numeric(get(var))]
    }

    ## Reunite East and West Germany
    east_exposure <- fread(paste0(hmd_male_folder, "/../Exposures_5x1/DEUTE.Exposures_5x1.txt"))
    east_exposure[, iso3 := "DEUTE"]

    west_exposure <- fread(paste0(hmd_male_folder, "/../Exposures_5x1/DEUTW.Exposures_5x1.txt"))
    west_exposure[, iso3 := "DEUTW"]

    exposures <- rbindlist(list(east_exposure, west_exposure), use.names = T)
    setnames(exposures, colnames(exposures), tolower(colnames(exposures)))
    exposures <- exposures[!is.na(year)]
    exposures <- format_hmd_ages(exposures)

    exposures[, total := NULL]

    ## Reshape long by sex
    exposures <- melt(exposures, id.vars = c("iso3", "year", "age"))
    setnames(exposures, c("variable", "value"), c("sex", "population"))

    ## Merge on HMD
    exposures <- merge(exposures, results, by = c("iso3", "sex", "year", "age"))
    exposures[, total_pop := sum(population), by = c("year", "sex", "age")]

    ## Pop-weight mx
    exposures[, mx_weighted := mx * population / total_pop]

    ## Death-weight ax
    exposures[, ax_weighted := ax * (mx * population / sum(mx * population)), by = c("year", "sex", "age")]

    ## Collapse the sum of mx and ax
    exposures <- exposures[, list(mx = sum(mx_weighted), ax = sum(ax_weighted)), by = c("year", "sex", "age")]
    exposures[, iso3 := "DEU"]

    ## Generate qx using mx and ax, and other parameters
    gen_age_length(exposures)
    exposures[, qx := mx_ax_to_qx(mx, ax, age_length)]

    id_vars <- c("iso3", "year", "sex", "age")
    results <- rbindlist(list(results[, .SD, .SDcols = c(id_vars, "qx")], exposures[, .SD, .SDcols = c(id_vars, "qx")]), use.names = T)
    results[iso3 == "FRATNP", iso3 := "FRA"]
    results[iso3 == "GBR_NP", iso3 := "GBR"]
    results[iso3 == "NZL_NP", iso3 := "NZL"]

    ## Drop countries with documented problems per HMD
    results <- results[iso3 != "UKR" | year >= 1970]
    results <- results[iso3 != "BLR" | year >= 1970]
    results <- results[iso3 != "BGR" | year >= 1970]
    results <- results[iso3 != "EST" | year <= 2000]
    results <- results[iso3 != "IRL" | (year <= 1949 | year >= 1986)]
    results <- results[iso3 != "ITA" | (year <= 1971 | year >= 1906)]
    results <- results[iso3 != "LVA" | (year <= 1958 | year >= 1970)]
    results <- results[iso3 != "LTV" | (year <= 1958 | year >= 1970)]
    results <- results[iso3 != "PRT" | (year <= 1939 | year >= 1971)]
    results <- results[iso3 != "RUS" | (year <= 1958 | year >= 1970)]
    results <- results[iso3 != "SVK" | (year <= 1949 | year >= 1962)]
    results <- results[iso3 != "ESP" | (year <= 1907 | year >= 1961)]
    results <- results[iso3 != "FIN" | year != 1918]

    results[, source := "HMD"]

    return(results)
}

