#' @title Manual outliering for raw VR
#'
#' @description Adjust outliering based on requests for individual location/years
#'
#' @param dataset \[`data.table()`\]\cr
#'   VR dataset containing standard id, series name, and outlier columns
#'
#' @return \[`data.table()`\]\cr
#'   Dataset where specified rows have been outliered

loc_map <- demInternal::get_locations()

manual_outliering_raw_vr <- function(dataset) {

  dt <- copy(dataset)

  # TWN 8
  dt[location_id == 8 & age_group_id %in% c(20, 30) & year_id == 1957, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 8 & age_group_id %in% 2:3 & year_id <= 1994, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MYS 13, outlier WHO & use DYB/Custom
  dt[location_id == 13 & age_group_id %in% c(6:20, 30, 34, 238) & year_id == 1998 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & age_group_id %in% c(6:20, 30, 34, 238) & year_id == 1998 & !series_name == "CRVS", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 13 & age_group_id %in% c(6:20, 30) & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & age_group_id %in% c(6:20, 30) & !series_name == "WHO" & year_id >= 1999 & year_id <= 2020, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 13 & age_group_id %in% c(32, 235) & year_id == 2019, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & age_group_id %in% c(16:20, 30) & year_id %in% 1980:1981, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MMR 15
  dt[location_id == 15, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PHL 16
  dt[location_id == 16 & age_group_id == 235 & year_id %in% 1997:2021, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 16 & age_group_id == 32 & year_id %in% 1997:1998 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 16 & age_group_id %in% c(18:20, 30:32, 235) & year_id %in% 1950:1959, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LKA 17
  dt[location_id == 17 & age_group_id %in% 8:11 & sex_id == 1 & year_id == 1996, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 17 & age_group_id == 9 & sex_id == 2 & year_id == 1978 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 17 & age_group_id == 9 & sex_id == 2 & year_id == 1978 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 17 & age_group_id == 9 & sex_id == 2 & year_id == 2010, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 17 & age_group_id %in% 12:15 & sex_id == 2 & year_id == 2005, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 17 & age_group_id %in% c(20, 30:32, 235) & year_id %in% 1950:1957, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # THA 18
  dt[location_id == 18 & age_group_id %in% 2:3 & year_id < 1990, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 18 & age_group_id == 238 & year_id == 1998, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PNG 26
  dt[location_id == 26, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ARM 33
  dt[location_id == 33 & age_group_id %in% 6:17 & year_id == 1988, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 33 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outlier 85+; "))]
  # AZE 34
  dt[location_id == 34 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(20, 30:32, 235) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 34 & year_id %in% 1992:1994 & age_group_id %in% 8:11 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 34 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GEO 35
  dt[location_id == 35 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(31:32, 235) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KAZ 36
  dt[location_id == 36 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(18:20, 30:32, 235) & series_name == "Custom" & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 36 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(20, 30:32, 235) & series_name == "Custom" & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KGZ 37
  dt[location_id == 37 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(17:20, 30:32) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 37 & year_id %in% 2000:2003 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 37 & year_id %in% 2014:2016 & age_group_id == 32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MNG 38
  dt[location_id == 38 & year_id == 1994 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TJK 39
  dt[location_id == 39 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(20, 30:32, 235) & series_name == "Custom" & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 39 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(18:20, 30:32, 235) & series_name == "Custom" & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 39 & year_id == 1993 & age_group_id %in% 8:17 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TKM 40
  dt[location_id == 40 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(18:20, 30:32, 235) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # UZB 41
  dt[location_id == 41 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(18:20, 30:32, 235) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 41 & year_id %in% 1981:1982, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ALB 43
  dt[location_id == 43 & year_id == 1997 & age_group_id %in% 9:12 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 43 & year_id == 2010, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 43 & age_group_id %in% c(2:3, 388:389, 34, 238), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BIH 44
  dt[location_id == 44 & age_group_id %in% c(13:20, 30) & year_id %in% c(1992:1995), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44 & age_group_id %in% c(15, 30) & year_id %in% c(1998:1999), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BGR 45
  dt[location_id == 45 & age_group_id %in% 31:32 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # HRV 46
  dt[location_id == 46 & age_group_id %in% 7:12 & year_id %in% 1991:1993 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 46 & age_group_id %in% 9:10 & year_id == 1995 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CZE 47
  dt[location_id == 47 & nid == 287600 & year_id == 1994, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # HUN 48
  dt[location_id == 48 & age_group_id == 235 & year_id %in% c(2008, 2019) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 48 & age_group_id == 235 & year_id == 2022 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 48 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 48 & year_id == 1950 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MKD 49
  dt[location_id == 49 & age_group_id == 235 & year_id %in% c(2017, 2018, 2021) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 49 & age_group_id == 235 & year_id %in% c(2006, 2016, 2017, 2021) & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MNE 50
  dt[location_id == 50 & year_id %in% 1993:1994 & age_group_id == 20, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 50 & year_id == 2017 & age_group_id == 235 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 50 & year_id %in% 1992:1994 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SRB 53
  dt[location_id == 53 & age_group_id %in% c(6:20, 30) & year_id %in% 1992:1994, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53 & age_group_id %in% c(32, 235) & year_id %in% 2015:2019, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SVN 55
  dt[location_id == 55 & age_group_id == 32 & year_id %in% c(1985:1996, 1998, 2011), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 55 & age_group_id %in% 19:20 & year_id %in% 1950:1961, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 55 & age_group_id == 30 & year_id %in% 1950:1971, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # EST 58
  dt[location_id == 58 & age_group_id %in% 8:12 & year_id == 1994 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 58 & age_group_id == 10 & year_id == 1989 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 58 & year_id == 1958 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 58 & age_group_id == 235 & year_id == 1972 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LVA 59
  dt[location_id == 59 & age_group_id == 32 & year_id %in% c(1999, 2006), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 59 & age_group_id == 32 & year_id %in% c(1970, 2010) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 59 & age_group_id == 235 & year_id %in% c(1960:1979, 2019) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 59 & age_group_id == 235 & year_id %in% c(1960:1979, 2009, 2010, 2017) & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 59 & age_group_id == 31 & year_id %in% c(1999, 2006), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LTU 60
  dt[location_id == 60 & age_group_id %in% c(31, 32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 60 & year_id == 1958 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MDA 61
  dt[location_id == 61 & age_group_id %in% c(18:20, 30:32, 235) & year_id %in% c(1958, 1969) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # RUS 62
  dt[location_id == 62 & age_group_id == 235 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # UKR 63
  dt[location_id == 63 & age_group_id %in% c(32, 235) & year_id %in% 1983:1984, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BRN 66
  dt[location_id == 66 & year_id <= 1964, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 66 & age_group_id %in% c(19, 20, 30:32, 235) & year_id %in% 1965:1969, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # JPN 67
  dt[location_id == 67 & age_group_id == 30 & year_id %in% c(1979, 1981:1984) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 67 & age_group_id == 30 & year_id == 1980 & sex_id == 2 & series_name == "CRVS", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 67 & age_group_id == 31 & deaths == 0, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KOR 68
  dt[location_id == 68 & year_id %in% 1960:1969 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 68 & year_id < 1977 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 68 & year_id <= 1994 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SGP 69
  dt[location_id == 69 & year_id >= 1980 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 69 & year_id %in% c(1980:2017, 2020:2023) & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 69 & year_id == 2017 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # AUS 71
  dt[location_id == 71 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # AND 74
  dt[location_id == 74 & year_id %in% 1950:1959, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - outlier 1950s; "))]
  # AUT 75
  dt[location_id == 75 & age_group_id %in% c(32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BEL 76, early neonatal
  dt[location_id == 76 & age_group_id == 2 & year_id %in% 1986:1987, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 76 & age_group_id == 238 & year_id == 1987, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 76 & year_id < 1990 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CYP 77, males
  dt[location_id == 77 & age_group_id %in% c(9:14, 18:19) & year_id == 1974 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 77 & age_group_id == 17 & year_id %in% c(1974:1975) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DEN 78, 12 to 23 months
  dt[location_id == 78 & age_group_id == 238 & year_id == 1998, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # FIN 79
  dt[location_id == 79 & age_group_id %in% c(31:32, 235) & year_id %in% 1950:1951 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # FRA 80
  dt[location_id == 80 & age_group_id %in% c(34, 238) & year_id == 2016, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 80 & age_group_id %in% c(31:32, 235) & year_id == 1951 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DEU 81
  dt[location_id == 81 & age_group_id == 3 & year_id == 2016, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 81 & age_group_id %in% c(31:32, 235) & year_id < 1990 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GRC 82
  dt[location_id == 82 & age_group_id %in% 15:16 & year_id == 1954, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ISR 85
  dt[location_id == 85 & year_id <= 1968 & age_group_id %in% 6:19 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 85 & year_id <= 1968 & age_group_id %in% 6:19 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # ITA 86
  dt[location_id == 86 & year_id == 1950 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LUX 87
  dt[location_id == 87 & year_id == 1997 & age_group_id == 32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 87 & year_id == 2018 & age_group_id %in% c(11:17, 19) & sex_id == 1 & nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 87 & year_id == 2018 & age_group_id %in% 31:32 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NLD 89
  dt[location_id == 89 & year_id %in% 1996:1997 & age_group_id %in% c(34, 238), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NOR 90
  dt[location_id == 90 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outlier off trend data; "))]
  dt[location_id == 90 & year_id %in% 1950:1985 & series_name == "HMD", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outlier off trend data; "))]
  dt[location_id == 90 & year_id %in% 1986:2016 & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outlier off trend data; "))]
  dt[location_id == 90 & year_id %in% 1991:2016 & nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PRT 91
  dt[location_id == 91 & year_id %in% c(1971:1973, 1975:1979) & age_group_id %in% c(34, 238), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 91 & year_id < 1990 & age_group_id == 235 & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # ESP 92
  dt[location_id == 92 & year_id < 1980 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SWE 93
  dt[location_id == 93 & year_id == 1950 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHE 94
  dt[location_id == 94 & year_id == 1950 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ARG 97
  dt[location_id == 97 & year_id %in% 1966:1967 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 97 & year_id %in% 1969:1970 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - negatively effecting age pattern; "))]
  dt[location_id == 97 & year_id %in% 1966:1967 & age_group_id %in% 6:8 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # CHL 98
  dt[location_id == 98 & year_id < 1968 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 98 & year_id %in% 1990:1996 & age_group_id %in% 31:32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CAN 101
  dt[location_id == 101 & year_id %in% c(2005:2009, 2014:2015) & age_group_id %in% c(238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BHS 106
  dt[location_id == 106 & year_id == 2003 & age_group_id == 20 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CUB 109
  dt[location_id == 109 & year_id == 1961 & age_group_id %in% 8:9 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 109 & year_id <= 1980 & age_group_id %in% c(18:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DMA 110
  dt[location_id == 110 & year_id < 1970 & age_group_id %in% c(20, 30) & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DOM 111
  dt[location_id == 111 & year_id %in% 2011:2018 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 111 & year_id %in% 2013:2018 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 111 & year_id %in% 2014:2016 & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # GRD 112
  dt[location_id == 112 & year_id <= 1970 & age_group_id %in% c(16:20, 30:32, 160, 234, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 112 & year_id %in% 1989:1993 & age_group_id %in% c(20, 30), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GUY 113
  dt[location_id == 113 & year_id == 2000 & sex_id == 1 & age_group_id == 6, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 113 & year_id == 2000 & sex_id == 2 & age_group_id %in% c(6, 12:15), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # HTI 114
  dt[location_id == 114, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # JAM 115
  dt[location_id == 115 & year_id %in% c(2005, 2009), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TTO 119
  dt[location_id == 119 & year_id <= 1955 & age_group_id %in% c(20, 30), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 119 & year_id <= 1980 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ECU 122
  dt[location_id == 122 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # COL 125
  dt[location_id == 125 & year_id == 1980 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SLV 127
  dt[location_id == 127 & year_id %in% 1979:1984, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GTM 128
  dt[location_id == 128 & year_id == 1976 & age_group_id %in% 7:11 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 128 & year_id == 1978 & age_group_id %in% c(2:3, 388:389), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 128 & year_id == 1980 & age_group_id %in% 9:10 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 128 & year_id == 1981 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # HND 129
  dt[location_id == 129 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 129 & year_id %in% 2005:2015, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MEX 130
  dt[location_id == 130 & year_id %in% 1950:1990 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NIC 131
  dt[location_id == 131 & year_id %in% 1978:1979, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PAN 132
  dt[location_id == 132 & year_id == 2020 & age_group_id == 34, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # VEN 133
  dt[location_id == 133 & year_id == 2019 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BRA 135
  dt[location_id == 135 & year_id == 2019 & age_group_id %in% 31:32 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PRY 136
  dt[location_id == 136 & year_id == 1950 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 136 & year_id == 1951 & age_group_id %in% 18:19, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 136 & year_id == 1959 & age_group_id == 8, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DZA 139
  dt[location_id == 139 & year_id %in% 2005:2006 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 139 & year_id < 1970 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - off trend; "))]
  # EGY 141
  dt[location_id == 141 & series_name == "WHO" & year_id <= 1964, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 141 & series_name == "DYB" & year_id <= 1964, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 141 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IRN 142
  dt[location_id == 142 & year_id == 1999 & age_group_id == 30, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 142 & year_id %in% c(1996, 2000:2001) & age_group_id == 14 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 142 & year_id == 1999 & age_group_id == 14 & sex_id == 1 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 142 & year_id == 1999 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 142 & year_id <= 1992, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IRQ 143 - outlier WHO, unoutlier custom in 2015-16
  dt[location_id == 143 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 143 & series_name == "Custom" & year_id %in% 2015:2016, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 143 & year_id < 1980, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 143 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # JOR 144
  dt[location_id == 144 & year_id %in% 2004:2006 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MAR 148
  dt[location_id == 148 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - do not use WHO; "))]
  dt[location_id == 148 & !series_name == "WHO", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - do not use WHO; "))]
  # PSE 149 - outlier dyb
  dt[location_id == 149 & series_name == "DYB" & year_id >= 2013, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 149 & series_name == "DYB" & year_id <= 2007, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 149 & series_name == "DYB" & age_group_id %in% 12:13 & year_id == 2002 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 149 & series_name == "CRVS" & year_id <= 2007, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # QAT 151
  dt[location_id == 151 & year_id == 1982 & age_group_id == 20, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SAU 152 - outlier who, unoutlier dyb
  dt[location_id == 152 & year_id %in% 1999:2002, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 152 & year_id == 2009 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 152 & year_id == 2009 & series_name == "CRVS", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 152 & year_id == 2016, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SYR 153
  dt[location_id == 153 & year_id == 1980 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id <= 1985 & age_group_id %in% c(2:3, 388:389), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id == 2000 & sex_id == 1 & age_group_id == 13, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id == 2004 & sex_id == 1 & age_group_id == 14, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id == 2004 & sex_id == 2 & age_group_id == 14, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id == 2010 & sex_id == 2 & age_group_id == 30, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id %in% 1980:1989 & age_group_id %in% 6:9, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TUN 154 - outlier dyb from 1981-2000
  dt[location_id == 154 & year_id %in% 1981:2000 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TUR 155
  dt[location_id == 155 & year_id == 1999, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 155 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 155 & year_id == 2023, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BGD 161
  dt[location_id == 161 & year_id >= 2012 & series_name == "CRVS" & deaths < 1000, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 161 & year_id %in% c(2002, 2013:2014), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IND 163
  dt[location_id == 163 & series_name != "SRS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 163 & series_name == "SRS" & age_group_id == 19 & sex_id == 1 & year_id == 2006, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PAK 165
  dt[location_id == 165, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 165 & year_id == 1968, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # COG 170
  dt[location_id == 170, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MDG 181
  dt[location_id == 181, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MWI 182, outlier SSA
  dt[location_id == 182, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MOZ 184
  dt[location_id == 184 & nid == 459445, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ZMB 191, outlier SSA
  dt[location_id == 191, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BWA 193
  dt[location_id == 193 & age_group_id %in% c(28, 5:7, 31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LSO 194, outlier SSA
  dt[location_id == 194, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NAM 195
  dt[location_id == 195 & year_id == 2011 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 195 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ZAF 196
  dt[location_id == 196 & year_id %in% 1980:1982 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 196 & year_id == 2017 & age_group_id == 32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ZWE 198, outlier SSA
  dt[location_id == 198, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GHA 207, outlier SSA
  dt[location_id == 207, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GIN 208, outlier SSA
  dt[location_id == 208, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MLI 211, outlier SSA
  dt[location_id == 211, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NGA 214, outlier SSA
  dt[location_id == 214, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHN_354 (Hong Kong)
  dt[location_id == 354 & year_id %in% 1950:1954 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id < 1960 & age_group_id %in% 16:19, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id < 1977 & age_group_id %in% c(20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id == 2012 & nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id == 2017 & age_group_id == 31, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHN_361 (Macao)
  dt[location_id == 361 & age_group_id %in% c(3, 238) & year_id == 1994, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NIU 374
  dt[location_id == 374 & year_id == 2009 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # VIR 422
  dt[location_id == 422 & year_id %in% 2018:2019 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GBR subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "GBR_", location_id] & year_id %in% 2014:2016 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ZAF subnats
  dt[location_id %in% 482:490 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHN subnats
  dt[location_id %in% 491:521 & series_name == "DSP" & extraction_source == "cod", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 491:521 & series_name == "DSP" & extraction_source == "noncod", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id %in% 491:521 & age_group_id %in% c(2:3, 388:389, 238, 34, 31:32, 235) & series_name == "DSP", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 491:521 & year_id <= 2003, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 492 & age_group_id == 28 & year_id == 2005 & series_name == "DSP" & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 515 & age_group_id == 28 & year_id == 2005 & series_name == "DSP" & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 505 & deaths == 0, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 491:521 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 491:521 & age_group_id %in% 2:3 & year_id %in% 2004:2005, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MEX subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "MEX", location_id] & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4650 & age_group_id %in% 7:14 & year_id %in% 2008:2012 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4650 & age_group_id %in% 8:11 & year_id == 2010 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BRA subnats
  dt[location_id %in% c(4751, 4755, 4761, 4764:4766, 4769) & year_id == 1979, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% c(4758, 4767) & year_id %in% 1979:1980, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% c(4754, 4759) & year_id %in% 1979:1981, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4776 & year_id == 1990, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IND subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "IND_", location_id] & series_name != "SRS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4873 & series_name == "SRS" & age_group_id %in% c(8, 20) & year_id == 2003, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # JPN subnats
  dt[location_id %in% c(35426, 35427) & age_group_id %in% c(7:20, 30:32) & year_id == 2011, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 35430 & age_group_id %in% c(7:8, 10) & year_id == 2011 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 35430 & age_group_id %in% c(7:8, 10:13, 16:17, 20, 30:31) & year_id == 2011 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 35451 & age_group_id %in% c(6:20, 30) & year_id == 1995, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHN 44533
  dt[location_id == 44533 & series_name == "DSP" & age_group_id %in% c(2:3, 388:389, 238, 34, 31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44533 & year_id <= 1995 & series_name == "DSP", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44533 & year_id == 2011 & series_name == "DSP", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44533 & year_id == 2019 & series_name == "DSP" & age_group_id == 19, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NZL subnats
  dt[location_id %in% c(44850, 44851) & year_id <= 1994, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IRN subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & age_group_id %in% c(2:3, 388:389, 238, 34) & !title %like% "Iran Death Registration System", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outlier; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & year_id %in% 2001:2016 & age_group_id %in% c(2:3, 388:389, 238, 34) & title %like% "Iran Death Registration System" & !(year_id == 2016 & extraction_source == "noncod"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outlier; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & !age_group_id %in% c(2:3, 388:389, 238, 34) & !source_type_id == 56, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outlier; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & year_id %in% 2011:2020 & !age_group_id %in% c(2:3, 388:389, 238, 34) & source_type_id == 56, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outlier; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN_", location_id] & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN_", location_id] & age_group_id == 31 & year_id %in% c(2001:2009), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% loc_map[ihme_loc_id %in% c("IRN_44887", "IRN_44884", "IRN_44864"), location_id] & age_group_id %in% c(19:20, 30:32, 235) & year_id %in% c(2000:2010), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # RUS subnats
  dt[location_id == 44985 & age_group_id %in% c(6:9) & year_id == 1995 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44985 & age_group_id %in% c(6:14) & year_id == 1995 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44944 & age_group_id %in% c(6:7) & year_id == 2004, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44944 & age_group_id == 8 & year_id == 1995 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44944 & age_group_id == 8 & year_id == 2004 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PHL subnats
  dt[location_id == 53585 & age_group_id %in% c(34, 238, 6:8) & year_id == 2013 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53585 & age_group_id %in% c(34, 238, 6:16) & year_id == 2013 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 53533:53614 & year_id <= 1979, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 53533:53614 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53614 & age_group_id == 32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53598 & year_id %in% 2019:2021, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NGA subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "NGA_", location_id], ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]

  return(dt)
}

#' @title Manual outliering for final VR
#'
#' @description Adjust outliering based on requests for individual location/years
#'
#' @param dataset \[`data.table()`\]\cr
#'   VR dataset containing standard id, series name, and outlier columns
#'
#' @return \[`data.table()`\]\cr
#'   Dataset where specified rows have been outliered

manual_outliering_final_vr <- function(dataset) {

  dt <- copy(dataset)

  # TWN 8
  dt[location_id == 8 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 8 & year_id == 1957 & age_group_id %in% c(20, 30), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 8 & year_id <= 1981 & age_group_id == 32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 8 & year_id <= 1994 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MYS 13
  dt[location_id == 13 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered  - source prioritization; "))]
  dt[location_id == 13 & year_id %in% 1950:1979 & age_group_id %in% c(16:20, 30:32, 235) & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & year_id %in% 1980:1981, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & year_id <= 1981 & age_group_id %in% c(18:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & year_id == 1982 & nid == 32162, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & year_id == 1998 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - source prioritization; "))]
  dt[location_id == 13 & year_id == 1998 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - source prioritization; "))]
  dt[location_id == 13 & year_id %in% 1999:2020 & age_group_id %in% c(31:32, 235) & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & year_id %in% 1999:2020 & !series_name == "WHO", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered  - source prioritization; "))]
  dt[location_id == 13 & year_id %in% 2005:2008 & age_group_id %in% c(34, 238) & !series_name == "WHO" & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered  - source prioritization; "))]
  dt[location_id == 13 & year_id == 2019 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 13 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MDV 14
  dt[location_id == 14 & year_id <= 1983 & age_group_id %in% c(18:20, 30:32), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 14 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MMR 15
  dt[location_id == 15, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PHL 16
  dt[location_id == 16 & year_id %in% 1950:1964 & age_group_id %in% c(18:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 16 & year_id %in% 1965:1969 & age_group_id %in% c(19:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 16 & year_id %in% 1979:1991 & age_group_id %in% c(20, 30:32, 235) & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 16 & year_id %in% 1997:1998 & age_group_id == 32 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 16 & year_id %in% c(2012:2013, 2015:2016, 2018) & !series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 16 & year_id %in% c(2012:2013, 2015:2016, 2018) & series_name == "DYB" & !(age_group_id %in% 30:32 & !onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 16 & year_id == 2017, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 16 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LKA 17
  dt[location_id == 17 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 17 & year_id %in% 1950:1957 & age_group_id == 20, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 17 & year_id == 1996 & age_group_id %in% 8:11 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 17 & year_id == 2005 & age_group_id %in% 12:15 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 17 & year_id == 2010 & age_group_id == 9 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # THA 18
  dt[location_id == 18 & year_id <= 1978 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 18 & year_id %in% 1979:1987 & !nid == 233896, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 18 & year_id %in% 1979:1987 & nid == 233896 & !(age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 18 & year_id == 1998 & age_group_id == 238, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 18 & year_id %in% 1950:1974 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 18 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # FJI 22
  dt[location_id == 22 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KIR 23
  dt[location_id == 23 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MHL 24
  dt[location_id == 24 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # FSM 25
  dt[location_id == 25 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PNG 26
  dt[location_id == 26, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # WSM 27
  dt[location_id == 27 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 27 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SLB 28
  dt[location_id == 28 & year_id %in% 2017:2018 & age_group_id %in% c(18:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TON 29
  dt[location_id == 29 & year_id == 1990, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 29 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ARM 33
  dt[location_id == 33 & age_group_id %in% 6:17 & year_id == 1988, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # AZE 34
  dt[location_id == 34 & year_id %in% 1992:1994 & age_group_id %in% 8:11 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 34 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(20, 30:32, 235) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 34 & year_id %in% c(1991:2004, 2008:2009) & age_group_id %in% c(34, 238) & onemod_process_type == "standard gbd age group data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 34 & year_id %in% c(1991:2000) & age_group_id %in% c(34, 238) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 34 & year_id %in% c(2001:2004, 2008:2009) & age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 34 & year_id == 2007 & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 34 & year_id == 2007 & nid == 287600 & !(age_group_id %in% c(34, 238) & onemod_process_type == "standard gbd age group data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # GEO 35
  dt[location_id == 35 & year_id <= 1980 & age_group_id %in% c(20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 35 & year_id %in% 1995:2001, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KAZ 36
  dt[location_id == 36 & year_id %in% 2008:2012 & age_group_id %in% c(2:3, 388:389), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered disjointed source; "))]
  dt[location_id == 36 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(18:20, 30:32, 235) & series_name == "Custom" & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 36 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(20, 30:32, 235) & series_name == "Custom" & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KGZ 37
  dt[location_id == 37 & year_id %in% 1950:2003 & age_group_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 37 & year_id == 2017 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - source prioritization; "))]
  dt[location_id == 37 & year_id == 2017 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - source prioritization; "))]
  dt[location_id == 37 & age_group_id %in% c(17:20, 30:32) & year_id %in% c(1958, 1969, 1978) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MNG 38
  dt[location_id == 38 & year_id == 1965, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TJK 39
  dt[location_id == 39 & age_group_id %in% 8:17 & year_id == 1993 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 39 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 39 & age_group_id %in% c(20, 30:32, 235) & year_id %in% c(1958, 1969, 1978) & series_name == "Custom" & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 39 & age_group_id %in% c(18:20, 30:32, 235) & year_id %in% c(1958, 1969, 1978) & series_name == "Custom" & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TKM 40
  dt[location_id == 40 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(18:20, 30:32, 235) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 40 & year_id %in% 1999:2011 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # UZB 41
  dt[location_id == 41 & year_id %in% c(1958, 1969, 1978) & age_group_id %in% c(18:20, 30:32, 235) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 41 & year_id %in% 1981:1982, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ALB 43
  dt[location_id == 43 & year_id %in% 2005:2009 & !nid == 233896, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 43 & year_id %in% 2005:2009 & nid == 233896 & !(age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 43 & year_id <= 1970 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 43 & year_id == 1997 & age_group_id %in% 9:12 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 43 & year_id <= 2010 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 43 & year_id == 2010, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BIH 44
  dt[location_id == 44 & year_id %in% 1992:1995 & age_group_id %in% c(13:20, 30), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44 & year_id %in% 1998:1999 & age_group_id %in% c(15, 30), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44 & year_id == 2023, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BGR 45
  dt[location_id == 45 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 45 & year_id %in% 2022:2023 & age_group_id %in% c(32, 235) & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CZE 47
  dt[location_id == 47 & year_id == 1994 & nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 47 & year_id %in% 1950:1954 & age_group_id %in% c(30:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 47 & year_id %in% 1950:1959 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # HUN 48
  dt[location_id == 48 & age_group_id == 235 & year_id %in% c(2008, 2019) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 48 & age_group_id == 235 & year_id == 2022 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 48 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 48 & year_id == 1950 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MNE 50
  dt[location_id == 50 & year_id <= 1989 & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 50 & year_id == 1991 & series_name == "Custom" & age_group_id %in% c(2:3, 388:389), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 50 & year_id %in% 1992:1994 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 50 & year_id == 2017 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ROU 52
  dt[location_id == 52 & age_group_id %in% c(31:32, 235) & year_id %in% 1956:1958 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 52 & year_id %in% 1959:1968 & nid == 134110, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - source prioritization; "))]
  dt[location_id == 52 & year_id %in% 1959:1968 & nid == 18447, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - source prioritization; "))]
  dt[location_id == 52 & age_group_id %in% c(2:3, 388:389, 34, 238) & year_id <= 1968, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 52 & age_group_id %in% c(34, 238) & year_id == 1963 & nid == 18447 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - age sex split duplicate; "))]
  # SRB 53
  dt[location_id == 53 & year_id %in% 1992:1994 & age_group_id %in% c(6:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53 & year_id %in% 1995:1997 & age_group_id %in% c(15:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53 & year_id %in% 2015:2019 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53 & year_id %in% 2023, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SVN 55
  dt[location_id == 55 & year_id %in% 1950:1964 & age_group_id %in% c(17:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 55 & year_id == 2011 & age_group_id == 32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # EST 58
  dt[location_id == 58 & age_group_id %in% c(8:12) & year_id == 1994 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 58 & age_group_id == 10 & year_id == 1989 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 58 & year_id == 1958 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 58 & age_group_id == 235 & year_id == 1972 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LVA 59
  dt[location_id == 59 & age_group_id == 32 & year_id %in% c(1970, 2010) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 59 & age_group_id == 235 & year_id %in% c(1960:1979, 2019) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 59 & age_group_id == 235 & year_id %in% c(1960:1979, 2009, 2010, 2017) & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 59 & year_id == 1958 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LTU 60
  dt[location_id == 60 & age_group_id %in% c(31, 32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 60 & year_id == 1958 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MDA 61
  dt[location_id == 61 & year_id %in% c(1958, 1969) & age_group_id %in% c(18:20, 30:32, 235) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 61 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # UKR 63
  dt[location_id == 63 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 63 & year_id == 1958, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 63 & year_id %in% 1983:1984 & age_group_id == 32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BRN 66
  dt[location_id == 66 & age_group_id %in% c(15:20, 30:32, 235) & year_id <= 1964, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 66 & age_group_id %in% c(19, 20, 30:32, 235) & year_id %in% 1965:1969, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # JPN 67
  dt[location_id == 67 & age_group_id == 30 & year_id %in% c(1979, 1981:1984) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 67 & age_group_id == 30 & year_id == 1980 & sex_id == 2 & series_name == "CRVS", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 67 & age_group_id == 31 & deaths == 0, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KOR 68
  dt[location_id == 68 & year_id %in% 1960:1969 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 68 & year_id < 1977 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 68 & year_id <= 1994 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SGP 69
  dt[location_id == 69 & year_id >= 1980 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 69 & year_id %in% c(1980:2017, 2020:2023) & series_name == "Custom" & !(year_id == 1992 & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # AUS 71
  dt[location_id == 71 & year_id <= 1967 & age_group_id %in% c(2:3, 388:389) & nid == 134110, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NZL 72
  dt[location_id == 72 & year_id %in% 1950:2016 & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 72 & year_id %in% 1950:2016 & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 72 & year_id %in% 1950:2016 & nid == 287600 & age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # AND 74
  dt[location_id == 74 & year_id %in% 1950:1959, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - outlier 1950s; "))]
  dt[location_id == 74 & year_id %in% 1990:2010 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # AUT 75
  dt[location_id == 75 & age_group_id %in% c(32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BEL 76, early neonatal
  dt[location_id == 76 & year_id %in% 1986:1987 & age_group_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 76 & year_id == 1987 & age_group_id == 238, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 76 & year_id < 1990 & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CYP 77, males
  dt[location_id == 77 & year_id == 1974 & age_group_id %in% c(9:14, 18:19) & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 77 & year_id %in% 1974:1975 & age_group_id == 17 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DNK 78
  dt[location_id == 78 & year_id == 1998 & age_group_id == 238, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 78 & year_id %in% c(1951:2012, 2015:2016, 2018) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 78 & year_id %in% c(1951:2012, 2015:2016, 2018) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 78 & year_id %in% c(1965, 1967, 1969:1998, 2001:2012, 2015:2016, 2018) & age_group_id %in% c(34, 238) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 78 & year_id %in% c(2017, 2019:2020) & !series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 78 & year_id %in% c(2017, 2019:2020) & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 78 & year_id %in% 2021:2022 & !series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 78 & year_id %in% 2021:2022 & series_name == "HMD", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # FIN 79
  dt[location_id == 79 & year_id %in% 1950:1951 & age_group_id %in% c(31:32, 235)  & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 79 & year_id %in% c(1952:2016, 2018) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 79 & year_id %in% c(1952:2016, 2018) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 79 & year_id %in% 1952:1995 & age_group_id %in% c(34, 238)& nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # FRA 80
  dt[location_id == 80 & year_id == 1951 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 80 & year_id == 2016 & age_group_id %in% c(34, 238), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DEU 81
  dt[location_id == 81 & year_id < 1990 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 81 & year_id == 2016 & age_group_id == 3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 81 & year_id %in% 1980:1989 & !nid == 242294, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 81 & year_id %in% 1980:1989 & nid == 242294, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 81 & year_id %in% 2020:2022 & !nid == 400760, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 81 & year_id %in% 2020:2022 & nid == 400760, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # GRC 82
  dt[location_id == 82 & year_id == 1954 & age_group_id %in% 15:16 , ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 82 & year_id %in% c(1961:1999, 2014) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use noncod; "))]
  dt[location_id == 82 & year_id %in% c(1961:1999, 2014) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use noncod; "))]
  dt[location_id == 82 & year_id %in% 1961:1999 & age_group_id %in% c(34, 238) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 82 & year_id %in% 2000:2017 & !nid == 397988, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use noncod; "))]
  dt[location_id == 82 & year_id %in% 2000:2017 & nid == 397988, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use noncod; "))]
  dt[location_id == 82 & year_id == 2018 & !nid == 426273, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 82 & year_id == 2018 & nid == 426273, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 82 & year_id %in% 2019:2020 & !nid == 492415, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 82 & year_id %in% 2019:2020 & nid == 492415, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # ISR 85
  dt[location_id == 85 & year_id %in% c(1975:2013, 2015) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use noncod; "))]
  dt[location_id == 85 & year_id %in% c(1975:2013, 2015) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use noncod; "))]
  dt[location_id == 85 & age_group_id %in% c(34, 238) & year_id %in% c(1975:2013, 2015) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 85 & year_id <= 1969 & series_name != "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 85 & year_id <= 1969 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 85 & year_id == 1998 & age_group_id == 31, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ITA 86
  dt[location_id == 86 & year_id == 1950 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LUX 87
  dt[location_id == 87 & age_group_id == 32 & year_id == 1997, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 87 & age_group_id %in% c(11:17, 19) & year_id == 2018 & sex_id == 1 & nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 87 & age_group_id %in% 31:32 & year_id == 2018 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NLD 89
  dt[location_id == 89 & year_id %in% 1996:1997 & age_group_id %in% c(34, 238)  & onemod_process_type == "standard gbd age group data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 89 & year_id %in% c(1950:2013, 2016, 2018, 2020) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use noncod; "))]
  dt[location_id == 89 & year_id %in% c(1950:2013, 2016, 2018, 2020) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use noncod; "))]
  dt[location_id == 89 & year_id %in% c(1950:2013, 2016, 2018, 2020) & nid == 287600 & age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NOR 90
  dt[location_id == 90 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outlier off trend data; "))]
  dt[location_id == 90 & year_id %in% c(1950:1985, 2021:2022) & series_name == "HMD", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outlier off trend data; "))]
  dt[location_id == 90 & year_id %in% c(1986:2016, 2018:2020, 2023) & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outlier off trend data; "))]
  dt[location_id == 90 & year_id %in% 1991:2016 & nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 90 & year_id == 2017 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # PRT 91
  dt[location_id == 91 & year_id < 1990 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ESP 92
  dt[location_id == 92 & year_id < 1980 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 92 & year_id < 1975 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use HMD; "))]
  dt[location_id == 92 & year_id < 1975 & series_name == "HMD", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use HMD; "))]
  dt[location_id == 92 & year_id %in% 1975:2015 & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use noncod; "))]
  dt[location_id == 92 & year_id %in% 1975:2015 & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use noncod; "))]
  dt[location_id == 92 & year_id %in% 1975:2015 & age_group_id %in% c(34, 238)  & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 92 & year_id >= 2016 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 92 & year_id >= 2016  & !year_id == 2020 & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered "))]
  dt[location_id == 92 & year_id == 2017 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 92 & year_id == 2021 & series_name == "HMD", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # SWE 93
  dt[location_id == 93 & year_id == 1950 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 93 & year_id %in% 1969:1979 & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use noncod; "))]
  dt[location_id == 93 & year_id %in% 1969:1979 & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use noncod; "))]
  dt[location_id == 93 & year_id %in% 1973:1979 & nid == 287600 & age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHE 94
  dt[location_id == 94 & year_id == 1950 & age_group_id %in% c(31:32, 235) & series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 94 & year_id %in% 1951:2015 & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use noncod; "))]
  dt[location_id == 94 & year_id %in% 1951:2015 & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered- use noncod; "))]
  dt[location_id == 94 & year_id %in% 1951:2015 & nid == 287600 & age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GBR 95
  dt[location_id == 95 & year_id %in% 1981:1991 & age_group_id %in% c(2:3, 388:389) & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use HMD; "))]
  dt[location_id == 95 & year_id %in% 1981:2013 & !series_name == "HMD", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use HMD; "))]
  dt[location_id == 95 & year_id %in% 1981:2013 & series_name == "HMD", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use HMD; "))]
  # ARG 97
  dt[location_id == 97 & year_id %in% 1966:1967 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 97 & year_id %in% 1966:1967 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 97 & year_id %in% 1969:1970 & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 97 & year_id %in% 1969:1970 & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 97 & year_id %in% 1969:1970 & age_group_id %in% c(34, 238)  & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHL 98
  dt[location_id == 98 & year_id %in% 1990:1996 & age_group_id %in% 31:32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 98 & year_id %in% c(1955:1984, 1986:1992, 1994:2015, 2017:2018) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 98 & year_id %in% c(1955:1984, 1986:1992, 1994:2015, 2017:2018) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 98 & year_id %in% c(1955:1982, 1986:1992, 1994:2015, 2017:2018) & age_group_id %in% c(34, 238) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CAN 101
  dt[location_id == 101 & year_id %in% 2014:2015 & age_group_id %in% c(34, 238) &  onemod_process_type == "standard gbd age group data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 101 & age_group_id %in% c(34, 238) & year_id %in% c(2014:2015) & nid == 387779 & onemod_process_type == "age-sex split data", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # USA 102
  dt[location_id == 102 & year_id %in% 1950:1958 & age_group_id %in% c(2:3, 388:389), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 102 & year_id %in% 1959:1967 & !series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 102 & year_id %in% 1959:1967 & series_name == "CRVS", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 102 & year_id == 2018 & nid == 478568, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 102 & year_id == 2018 & nid == 456291, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # BHS 106, 0s in 75 to 79
  dt[location_id == 106 & year_id == 2003 & age_group_id == 20 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BRB 107
  dt[location_id == 107 & year_id %in% c(1960:1964, 1966:1978, 2000:2013) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 107 & year_id %in% c(1960:1964, 1966:1978, 2000:2013) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 107 & age_group_id %in% c(34, 238) & year_id %in% c(1960:1964, 1966:1978, 2000:2013) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BLZ 108
  dt[location_id == 108 & age_group_id %in% c(20, 30:32, 235) & year_id <= 1963, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CUB 109
  dt[location_id == 109 & age_group_id %in% c(8:9) & year_id == 1961 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 109 & age_group_id %in% c(18:20, 30:32, 235) & year_id <= 1980, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 109 & year_id %in% c(1964, 1968:1978, 1997:2009) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 109 & year_id %in% c(1964, 1968:1978, 1997:2009) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 109 & age_group_id %in% c(34, 238) & year_id %in% c(1964, 1968:1978, 1997:2009) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DMA 110
  dt[location_id == 110 & age_group_id %in% c(20, 30) & year_id < 1970 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 110 & year_id %in% c(1961:1962, 1967:1978, 2000:2015) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 110 & year_id %in% c(1961:1962, 1967:1978, 2000:2015) & nid == 287600, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 110 & age_group_id %in% c(34, 238) & year_id %in% c(1967:1978, 2001:2015) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DOM 111
  dt[location_id == 111 & year_id %in% 2011:2018 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 111 & year_id %in% 2013:2018 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 111 & year_id %in% 2014:2016 & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 111 & year_id %in% c(1970:1979, 1990, 1996:2001, 2003:2006) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 111 & year_id %in% c(1970:1979, 1990, 1996:2001, 2003:2006) & nid == 287600 & !(age_group_id %in% c(238, 34) & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 111 & year_id %in% 2014:2023 & age_group_id %in% c(2:3, 388:389, 238, 34) & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GRD 112
  dt[location_id == 112 & year_id <= 1970 & age_group_id %in% c(16:20, 30:32, 160, 234, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 112 & year_id %in% 1974:1978 & age_group_id %in% c(2:3, 388:389) & nid == 18447, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 112 & year_id %in% 1989:1993 & age_group_id %in% c(20, 30), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GUY 113
  dt[location_id == 113 & year_id == 2000 & age_group_id == 6 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 113 & year_id == 2000 & age_group_id %in% c(6, 12:15) & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 113 & year_id == 2000 & age_group_id %in% c(6, 12:15) & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 113 & year_id == 2020, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 113 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # HTI 114
  dt[location_id == 114, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # JAM 115
  dt[location_id == 115 & year_id %in% 1968:1975 & age_group_id %in% c(2:3, 388:389) & nid == 18447, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 115 & year_id %in% 1975:2005 & age_group_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 115 & year_id %in% c(2005, 2009), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TTO 119
  dt[location_id == 119 & year_id <= 1955 & age_group_id %in% c(20, 30), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 119 & year_id %in% 1999:2010 & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 119 & year_id %in% 1999:2010 & nid == 287600 & !(age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 119 & year_id == 2011 & !series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 119 & year_id == 2011 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # BOL 121
  dt[location_id == 121 & age_group_id %in% c(2:3, 388:389, 34, 238), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 121 & year_id %in% 2000:2003, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ECU 122
  dt[location_id == 122 & year_id %in% 1950:1967 & age_group_id %in% c(18:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PER 123
  dt[location_id == 123 & year_id %in% 1950:1964 & age_group_id %in% c(18:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # COL 125
  dt[location_id == 125 & year_id == 1979, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 125 & year_id == 1980 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SLV 127
  dt[location_id == 127 & year_id %in% 1979:1984 & !(year_id %in% 1981:1984 & age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - DH2 shocks subtracted; "))]
  # GTM 128
  dt[location_id == 128 & age_group_id %in% 9:10 & year_id == 1980 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 128 & age_group_id %in% 7:11 & year_id == 1976 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 128 & year_id == 1981 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # HND 129
  dt[location_id == 129 & year_id %in% 1982:1983 & age_group_id %in% c(2:3, 388:389) & nid == 265138, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 129 & year_id %in% 2005:2015, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MEX 130
  dt[location_id == 130 & year_id %in% 1950:1953 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NIC 131
  dt[location_id == 131 & year_id %in% 1950:1987 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 131 & year_id %in% 1978:1979, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PAN 132
  dt[location_id == 132 & age_group_id == 34 & year_id == 2020, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # VEN 133
  dt[location_id == 133 & year_id == 2019 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BRA 135, 0 in 90 to 94
  dt[location_id == 135 & age_group_id %in% 31:32 & year_id == 2019 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PRY 136
  dt[location_id == 136 & year_id == 1950 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 136 & year_id == 1951 & age_group_id %in% 18:19, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 136 & year_id == 1959 & age_group_id == 8, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 136 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # DZA 139
  dt[location_id == 139 & year_id < 1970 & age_group_id %in% c(2:3, 388:389, 238, 34, 19:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - off trend; "))]
  dt[location_id == 139 & year_id %in% 2005:2006 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # EGY 141
  dt[location_id == 141 & year_id <= 1967 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 141 & year_id <= 1964 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 141 & year_id <= 1967 & age_group_id %in% c(15:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 141 & year_id <= 1968 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 141 & year_id %in% c(2011:2013, 2015:2016, 2018:2019) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 141 & year_id %in% c(2011:2013, 2015:2016, 2018:2019) & nid == 287600 & !(year_id %in% c(2011, 2015:2016, 2018:2019) & age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 141 & year_id == 2014 & underlying_nid == 289636, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 141 & year_id == 2014 & is.na(underlying_nid) & !(age_group_id %in% c(34, 238) & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 141 & year_id == 2017 & !nid == 426273, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 141 & year_id == 2017 & nid == 426273, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 141 & year_id == 2022, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 141 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IRN 142
  dt[location_id == 142 & year_id %in% c(1983, 1984, 1986, 1991) & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 142 & year_id <= 1992, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 142 & year_id == 1999 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 142 & year_id == 1999 & age_group_id == 30, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 142 & year_id %in% c(1996, 2000:2001) & age_group_id == 14 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IRQ 143
  dt[location_id == 143 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 143 & year_id <= 1970 & age_group_id %in% c(2:3, 388:389, 238, 34, 19:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 143 & year_id >= 2008 & age_group_id %in% c(6:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 143 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # JOR 144
  dt[location_id == 144 & year_id %in% 2004:2006 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 144 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 144 & year_id >= 2019 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KWT 145
  dt[location_id == 145 & age_group_id %in% c(20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LBN 146
  dt[location_id == 146 & year_id %in% 2004:2015 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LBY 147
  dt[location_id == 147 & year_id %in% c(1972:1996, 2011, 2016:2017), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MAR 148
  dt[location_id == 148 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - do not use WHO; "))]
  dt[location_id == 148 & !series_name == "WHO", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - do not use WHO; "))]
  dt[location_id == 148 & year_id %in% c(1950:1998, 2000:2005) & sex_id == 1 & age_group_id %in% c(18:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 148 & year_id %in% c(1950:1998, 2000:2005) & sex_id == 2 & age_group_id %in% c(12:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 148 & year_id == 1999 & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 148 & year_id == 1999 & age_group_id %in% 19:20 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 148 & year_id %in% c(2005, 2020, 2021) & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PSE 149 - outlier dyb
  dt[location_id == 149 & year_id >= 2013 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 149 & year_id <= 2007 & !age_group_id %in% c(20, 30:32, 235) & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 149 & year_id == 2002 & series_name == "DYB" & age_group_id %in% 12:13 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 149 & year_id <= 2007 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 149 & year_id %in% 2008:2011 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # OMN 150
  dt[location_id == 150 & year_id <= 2006 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # QAT 151
  dt[location_id == 151 & year_id == 1982 & age_group_id == 20, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SAU 152
  dt[location_id == 152 & year_id == 2009 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 152 & year_id == 2009 & series_name == "CRVS" & !(age_group_id %in% c(238, 10, 16, 20) & onemod_process_type == "age-sex split data"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 152 & year_id %in% c(2001:2002, 2016), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 152 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 152 & year_id %in% c(1950:1998, 2013:2015, 2017) & age_group_id %in% c(20, 30), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 152 & year_id %in% 1999:2000 & age_group_id %in% c(2:3, 388:389, 238, 34, 6:10), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 152 & year_id %in% 2003:2010 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SYR 153
  dt[location_id == 153 & year_id %in% 1973:1978 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id %in% 1973:1978 & !series_name == "WHO", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 153 & year_id == 1980 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id <= 1985 & age_group_id %in% c(2:3, 388:389), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id %in% 1980:1989 & age_group_id %in% 6:9, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id == 2000 & sex_id == 1 & age_group_id == 13, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id == 2004 & sex_id == 1 & age_group_id == 14, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id == 2004 & sex_id == 2 & age_group_id == 14, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 153 & year_id == 2010 & sex_id == 2 & age_group_id == 30, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TUN 154
  dt[location_id == 154 & year_id %in% 1981:2000 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 154 & year_id %in% c(2009, 2013), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 154 & year_id %in% 2016:2021 & !series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 154 & year_id %in% 2016:2021 & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # TUR 155
  dt[location_id == 155 & year_id == 1999, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 155 & year_id == 2000 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 155 & year_id == 2023, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ARE 156
  dt[location_id == 156 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BGD 161
  dt[location_id == 161 & year_id >= 2012 & series_name == "CRVS" & deaths < 1000, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 161 & year_id == 2012 & series_name == "SRS" & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 161 & year_id %in% c(2002, 2013:2014), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 161 & year_id %in% c(2001, 2013:2014) & age_group_id %in% c(17:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BTN 162
  dt[location_id == 162 & year_id == 2022, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IND 163
  dt[location_id == 163 & series_name != "SRS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 163 & series_name == "SRS" & year_id %in% c(1970:1971, 2020), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 163 & series_name == "SRS" & year_id == 1972 & age_group_id %in% c(16:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 163 & series_name == "SRS" & year_id == 2014 & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 163 & series_name == "SRS" & age_group_id == 19 & sex_id == 1 & year_id == 2006, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PAK 165, unoutliering
  dt[location_id == 165, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 165 & year_id == 1968, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # COG 170
  dt[location_id == 170, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # KEN 180
  dt[location_id == 180, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MDG 181
  dt[location_id == 181 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MWI 182, outlier SSA
  dt[location_id == 182, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MOZ 184, outlier SSA
  dt[location_id == 184, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SYC 186
  dt[location_id == 186 & year_id %in% 1950:1980 & age_group_id %in% 31:32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 186 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ZMB 191, outlier SSA
  dt[location_id == 191, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BWA 193
  dt[location_id == 193 & age_group_id %in% c(2:3, 388:389, 238, 34, 5:7, 31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # LSO 194, outlier SSA
  dt[location_id == 194, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NAM 195
  dt[location_id == 195 & year_id == 2011 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 195 & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ZAF 196
  dt[location_id == 196 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 196 & year_id %in% 1993:1995 & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 196 & year_id %in% 1980:1982 & series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 196 & year_id %in% 1997:2015 & series_name == "CRVS" & !onemod_process_type == "age-sex split data", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id == 196 & year_id == 2017 & age_group_id == 32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ZWE 198, outlier SSA
  dt[location_id == 198, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GHA 207, outlier SSA
  dt[location_id == 207, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GIN 208, outlier SSA
  dt[location_id == 208, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MLI 211, outlier SSA
  dt[location_id == 211, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NER 213, outlier SSA
  dt[location_id == 213, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NGA 214, outlier SSA
  dt[location_id == 214, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # SLE 217, outlier SSA
  dt[location_id == 217, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # ASM 298
  dt[location_id == 298 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # COK 320
  dt[location_id == 320 & age_group_id %in% c(20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GUM 351
  dt[location_id == 351 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHN_354 (Hong Kong)
  dt[location_id == 354 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id %in% 1950:1954 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id < 1960 & age_group_id %in% 16:19, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id < 1977 & age_group_id %in% c(20, 30:32), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id %in% 1984:1995 & age_group_id %in% 31:32, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & year_id == 2017 & age_group_id == 31, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 354 & nid == 287600 & year_id == 2012, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHN_361 (Macao)
  dt[location_id == 361 & year_id %in% 1954:1960, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 361 & year_id %in% 1966:1967 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 361 & year_id < 1970 & age_group_id %in% c(20, 30:32), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 361 & year_id == 1994 & series_name == "WHO", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use DYB; "))]
  dt[location_id == 361 & year_id == 1994 & series_name == "DYB", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered - use DYB; "))]
  dt[location_id == 361 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MCO 367
  dt[location_id == 367 & year_id %in% 2011:2013 & !series_name == "Custom", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 367 & year_id %in% 2011:2013 & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  # NRU 369
  dt[location_id == 369 & age_group_id %in% c(20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NIU 374
  dt[location_id == 374 & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 374 & year_id %in% c(1954:1955, 1973), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 374 & year_id == 2009 & series_name == "DYB", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MNP 376
  dt[location_id == 376 & age_group_id %in% c(20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PLW 380
  dt[location_id == 380 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PRI 385
  dt[location_id == 385 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TKL 413
  dt[location_id == 413 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # TUV 416
  dt[location_id == 416 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # VIR 422
  dt[location_id == 422 & year_id %in% 2018:2019 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # GBR subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "GBR_", location_id] & year_id %in% 2014:2016 & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 433 & year_id %in% c(1968:1978, 1980:2017) & !nid == 287600, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; ")) ]
  dt[location_id == 433 & year_id %in% c(1968:1978, 1980:2017) & nid == 287600 &
       !(year_id %in% c(2001, 2003:2004, 2006:2009, 2011:2015, 2017) & age_group_id == 32 & onemod_process_type == "age-sex split data") &
       !(year_id %in% c(2001, 2003:2004, 2008:2009, 2011, 2013:2015) & age_group_id == 235 & onemod_process_type == "age-sex split data"),
     ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; ")) ]
  dt[location_id == 433 & year_id %in% c(1968:1978, 1980:2017) & age_group_id %in% c(34, 238) & nid == 287600 & onemod_process_type == "age-sex split data", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; ")) ]
  dt[location_id %in% c(4749, 4618:4624) & year_id %in% 1981:1985 & age_group_id %in% 388:389, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - spike in age split data; "))]
  # ZAF subnats
  dt[location_id %in% 482:490 & year_id %in% 1993:1995 & age_group_id %in% 30:31, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 482:490 & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHN subnats
  dt[location_id %in% 491:521 & series_name == "DSP" & extraction_source == "cod", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 491:521 & series_name == "DSP" & extraction_source == "noncod", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id %in% 491:521 & year_id <= 2003, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 492 & year_id == 2005 & age_group_id == 28 & series_name == "DSP" & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 505 & deaths == 0, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 515 & year_id == 2005 & age_group_id == 28 & series_name == "DSP" & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 491:521 & series_name == "CRVS", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id %in% 491:521 & year_id %in% 2004:2005 & age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 491:521 & year_id %in% c(2013:2014, 2019) & series_name == "DSP" & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 491:521 & !year_id %in% c(2004:2010, 2012, 2015) & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # MEX subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "MEX", location_id] & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "MEX", location_id] & year_id <= 1990 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4649 & year_id %in% 1991:1993 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4650 & year_id %in% 2008:2012 & age_group_id %in% 7:14 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4650 & year_id == 2010 & age_group_id %in% 8:11 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4652 & year_id %in% 1991:2002 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4654 & year_id %in% 1991:2003 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4662 & year_id %in% 1991:1995 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4665 & year_id %in% 1991:1993 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4667 & year_id %in% 1991:2001 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # BRA subnats
  dt[location_id %in% c(4751, 4761, 4764:4766, 4769) & year_id == 1979, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4754 & year_id %in% 1979:1981, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4755 & year_id %in% 1979:1985, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4758 & year_id %in% c(1979:1986, 1990:1991), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4759 & year_id %in% 1979:1984, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4763 & year_id %in% 1979:1982, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4767 & year_id %in% 1979:1982, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4776 & year_id == 1990, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4771 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IND subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "IND_", location_id] & series_name != "SRS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 4873 & series_name == "SRS" & age_group_id %in% c(8, 20) & year_id == 2003, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NOR subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "NOR_", location_id] & year_id == 1951, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "NOR_", location_id] & year_id %in% 1986:2016 & series_name == "Custom", ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "NOR_", location_id] & year_id %in% 1986:2016 & series_name == "CRVS", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - use age split; "))]
  # JPN subnats
  dt[location_id %in% c(35426, 35427) & age_group_id %in% c(7:20, 30:32) & year_id == 2011, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 35430 & age_group_id %in% c(7:8, 10) & year_id == 2011 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 35430 & age_group_id %in% c(7:8, 10:13, 16:17, 20, 30:31) & year_id == 2011 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 35451 & age_group_id %in% c(6:20, 30) & year_id == 1995, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # CHN 44533
  dt[location_id == 44533 & year_id %in% c(1950:1995, 2011) & series_name == "DSP", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44533 & year_id %in% c(2013:2014, 2019) & series_name == "DSP" & age_group_id %in% c(30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44533 & year_id == 2019 & series_name == "DSP" & age_group_id == 19, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NZL subnats
  dt[location_id %in% 44850:44851 & year_id <= 1994, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # IRN subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & age_group_id %in% c(2:3, 388:389, 238, 34) & !title %like% "Iran Death Registration System", ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outlier; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & year_id %in% 2001:2016 & age_group_id %in% c(2:3, 388:389, 238, 34) & title %like% "Iran Death Registration System" & !(year_id == 2016 & extraction_source == "noncod"), ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outlier; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & !age_group_id %in% c(2:3, 388:389, 238, 34) & !source_type_id == 56, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outlier; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & year_id %in% 2011:2020 & !age_group_id %in% c(2:3, 388:389, 238, 34) & source_type_id == 56, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outlier; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN", location_id] & age_group_id %in% c(32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "IRN_", location_id] & year_id %in% 2001:2009 & age_group_id == 31, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% loc_map[ihme_loc_id %in% c("IRN_44887", "IRN_44884", "IRN_44864"), location_id] & year_id %in% 2000:2010 & age_group_id %in% c(19:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # UKR subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "UKR_", location_id] & age_group_id == 235, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # RUS subnats
  dt[location_id == 44985 & age_group_id %in% 6:9 & year_id == 1995 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44985 & age_group_id %in% 6:14 & year_id == 1995 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44944 & age_group_id %in% 6:7 & year_id == 2004, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44944 & age_group_id == 8 & year_id == 1995 & sex_id == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44944 & age_group_id == 8 & year_id == 2004 & sex_id == 2, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 44903:44987 & series_name == "Custom" & year_id <= 1988 & deaths == 0, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 44987 & age_group_id %in% c(16:20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # PHL subnats
  dt[location_id == 53535 & year_id %in% 1968:1979, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53536 & year_id %in% 1970:1974, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53555 & year_id < 1980, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53579 & year_id == 1968 & sex_id == 1 & age_group_id == 15, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id == 53598 & year_id %in% 2019:2021, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% c(53611, 53613, 53614) & year_id < 1980, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% 53533:53614 & age_group_id %in% c(31:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  dt[location_id %in% loc_map[ihme_loc_id %like% "PHL", location_id] & year_id == 2023 & age_group_id %in% c(2:3, 388:389, 238, 34), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outlier; "))]
  dt[location_id == 53611 & age_group_id %in% c(20, 30:32, 235), ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]
  # NGA subnats
  dt[location_id %in% loc_map[ihme_loc_id %like% "NGA_", location_id], ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]

  return(dt)

}

#' @title Manual outliering for VR surveys
#'
#' @description Adjust outliering based on requests for individual location/years
#'
#' @param dataset \[`data.table()`\]\cr
#'   VR survey dataset containing standard id, series name, and outlier columns
#'
#' @return \[`data.table()`\]\cr
#'   Dataset where specified rows have been outliered
#'
#' @note
#'   Unlike VR we want to selectively UN-outlier location/source/years for now

loc_map <- demInternal::get_locations()

manual_outliering_vr_surveys <- function(dataset) {

  dt <- copy(dataset)

  # for now these should default to outliered
  dt[, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]

  # CAF 169
  dt[location_id == 169 & year_id == 1988, ':=' (outlier = 0, outlier_note = "")]
  # BDI 175
  dt[location_id == 175 & nid %in% c(1966, 1967), ':=' (outlier = 0, outlier_note = "")]
  # COM 176
  dt[location_id == 176 & nid == 3115, ':=' (outlier = 0, outlier_note = "")]
  # KEN 180
  dt[location_id == 180 & nid %in% c(133219, 7427), ':=' (outlier = 0, outlier_note = "")]
  # MDG 181
  dt[location_id == 181, ':=' (outlier = 0, outlier_note = "")]
  # MWI 182
  dt[location_id == 182 & nid %in% c(206387, 40186, 21393, 140967), ':=' (outlier = 0, outlier_note = "")]
  # MOZ 184
  dt[location_id == 184 & nid == 8891, ':=' (outlier = 0, outlier_note = "")]
  # RWA 185
  dt[location_id == 185 & nid == 205740, ':=' (outlier = 0, outlier_note = "")]
  # TZA 189
  dt[location_id == 189 & nid == 43207, ':=' (outlier = 0, outlier_note = "")]
  # UGA 190
  dt[location_id == 190 & nid %in% c(43328, 21014), ':=' (outlier = 0, outlier_note = "")]
  # ZMB 191
  dt[location_id == 191 & nid %in% c(21117, 58660), ':=' (outlier = 0, outlier_note = "")]
  # BWA 193
  dt[location_id == 193 & nid %in% c(1413, 314123, 21970), ':=' (outlier = 0, outlier_note = "")]
  # NAM 195
  dt[location_id == 195 & nid == 134132, ':=' (outlier = 0, outlier_note = "")]
  # SWZ 197
  dt[location_id == 197 & nid %in% c(52741, 237659), ':=' (outlier = 0, outlier_note = "")]
  # ZWE 198
  dt[location_id == 198 & nid %in% c(53065, 21163, 140967), ':=' (outlier = 0, outlier_note = "")]
  # BFA 201
  dt[location_id == 201 & nid %in% c(1960, 105387, 2933, 1955), ':=' (outlier = 0, outlier_note = "")]
  # CMR 202
  dt[location_id == 202 & nid %in% c(105633, 2068), ':=' (outlier = 0, outlier_note = "")]
  # CIV 205
  dt[location_id == 205 & nid %in% c(126910, 57471, 140967), ':=' (outlier = 0, outlier_note = "")]
  # GHA 207
  dt[location_id == 207 & nid == 237659, ':=' (outlier = 0, outlier_note = "")]
  # MLI 211
  dt[location_id == 211 & nid %in% c(140201, 40192, 40235, 237659), ':=' (outlier = 0, outlier_note = "")]
  # SEN 216
  dt[location_id == 216 & nid == 43142, ':=' (outlier = 0, outlier_note = "")]
  # TGO 218
  dt[location_id == 218 & nid == 32357, ':=' (outlier = 0, outlier_note = "")]
  # CHN subnats
  dt[source_type == "Census" & location_id %in% c(44533, 491:521), ':=' (outlier = 0, outlier_note = "")]
  dt[source_type == "SSPC", ':=' (outlier = 0, outlier_note = "")]

  return(dt)

}

#' @title Manual outliering for HHD data
#'
#' @description Adjust outliering based on general older age/adult rules
#'
#' @param dataset \[`data.table()`\]\cr
#'   H2 dataset containing adjusted mx, and outlier columns
#'
#' @return \[`data.table()`\]\cr
#'   Dataset where specified rows have been outliered

hhd_outliers <- function(dataset) {

  # prep h2 data
  agg_qx <- copy(dataset)

  agg_qx <- agg_qx[outlier == 0 & source_type_name %in% c("Census", "HHD", "Survey", "VR")]

  agg_qx[is.na(mx_adj), mx_adj := mx]

  agg_qx <- merge(agg_qx, age_map_extended, by = "age_group_id")

  agg_qx[, qx := demCore::mx_to_qx(mx = mx_adj, age_length = age_end - age_start)]

  agg_qx <- agg_qx[, .(location_id, year_id, sex_id, age_start, age_end, nid,
                       underlying_nid, source_type_name, qx)]

  agg50_agg_qx <- demCore::agg_lt(
    dt = agg_qx[
      age_start >= 15 & age_end <= 50,
      .(location_id, nid, underlying_nid, source_type_name, year_id, sex_id, age_start, age_end, qx)
    ],
    id_cols = c("location_id", "nid", "underlying_nid", "source_type_name", "year_id", "sex_id", "age_start", "age_end"),
    age_mapping = data.table(age_start = c(15), age_end = c(50)),
    missing_dt_severity = "warning"
  )
  agg80_agg_qx <- demCore::agg_lt(
    dt = agg_qx[
      age_start >= 50 & age_end <= 80,
      .(location_id, nid, underlying_nid, source_type_name, year_id, sex_id, age_start, age_end, qx)
    ],
    id_cols = c("location_id", "nid", "underlying_nid", "source_type_name", "year_id", "sex_id", "age_start", "age_end"),
    age_mapping = data.table(age_start = c(50), age_end = c(80)),
    missing_dt_severity = "warning"
  )

  setnames(agg50_agg_qx, "qx", "qx_15_35")
  setnames(agg80_agg_qx, "qx", "qx_50_30")

  agg50_agg_qx[, c("age_start", "age_end") := NULL]
  agg80_agg_qx[, c("age_start", "age_end") := NULL]

  agg_qx <- merge(
    agg50_agg_qx,
    agg80_agg_qx,
    by = c("location_id", "nid", "underlying_nid", "source_type_name", "year_id", "sex_id")
  )

  # Get VR bounds
  highest_30q50_vr <- max(agg_qx[source_type_name == "VR", qx_50_30])
  lowest_30q50_vr <- min(agg_qx[source_type_name == "VR", qx_50_30])

  # prep HDSS data
  hdss_data <- fread("FILEPATH")
  hdss_data <- hdss_data[sex_id == 3]

  lowest_30q50_hdss <- min(hdss_data$qx_50_30)
  highest_30q50_hdss <- max(hdss_data$qx_50_30)

  # identify nids to outlier:
  agg_qx <- agg_qx[source_type_name != "VR"]

  agg_qx <- agg_qx[
    (sex_id == 1 & qx_50_30 < lowest_30q50_hdss) | # 1
      (sex_id == 1 & qx_50_30 > highest_30q50_vr) | # 2
      (sex_id == 2 & qx_50_30 < lowest_30q50_vr) | # 3
      (sex_id == 2 & qx_50_30 > highest_30q50_hdss) | # 4
      (qx_50_30 < qx_15_35) # 5
  ]

  agg_qx[, c("qx_15_35", "qx_50_30") := NULL]
  agg_qx[, new_outlier := 1]

  dataset <- merge(
    dataset,
    agg_qx,
    by = c("location_id", "nid", "underlying_nid", "source_type_name", "year_id", "sex_id"),
    all.x = TRUE
  )

  dataset[
    !is.na(new_outlier) & outlier != new_outlier,
    outlier_note := "Meets HHD 30q50 outlier criteria"
  ]
  dataset[!is.na(new_outlier), outlier := 1]
  dataset[, new_outlier := NULL]

  return(dataset)

}
