#### Process EUROCAT Functions

#### create_raw_eurocat ####
create_raw_eurocat <- function(data_dir, data_years, registries){
  
  year_sheet <- create_unique_locs(data_dir, data_years, registries)
  
  final <- data.frame()
  for(reg_type in  registries){
    if(reg_type == "associate/"){
      years_list <- data_years[data_years != 2011]
    } else{
      years_list <- data_years
    }
    for(year in years_list){
      print(paste0(data_dir, reg_type,'1980_', year, '.XLSX'))
      eurocat_file <- as.data.table(read.xlsx(paste0(data_dir, reg_type,'1980_', year, '.XLSX'), 
                                              startRow = 3, na.strings = '-'))
      
      if(year == 2011){
        setnames(eurocat_file, c(1:12), c("registry_name", "case_name", "year_start", "sample_size",
                                          "cases_lb", "cases_fd", "cases_top", "cases_total", 
                                          "cases_lb_nonchromo", "cases_fd_nonchromo", "cases_top_nonchromo",
                                          "cases_total_nonchromo"))
      } else {
        eurocat_file <- eurocat_file[, c(1:8, 16:19)]
        setnames(eurocat_file, c(1:12), c("registry_name", "case_name", "year_start", "sample_size", 
                                          "cases_lb", "cases_fd", "cases_top", "cases_total", 
                                          "cases_lb_nonchromo", "cases_fd_nonchromo", "cases_top_nonchromo",
                                          "cases_total_nonchromo"))
      }
      
      eurocat_file[, c("case_name") := gsub(' §', '', case_name)]
      eurocat_file <- eurocat_file[, fill(eurocat_file, c("registry_name", "case_name"))]
      
      update_cols <- names(eurocat_file[,3:12])
      eurocat_file[, c(update_cols) := lapply(.SD, as.numeric), .SDcols = update_cols]
      
      ### drop NA's
      eurocat_file <- eurocat_file[!is.na(sample_size),]  ## drop if sample size is NA
      
      ### edit loc names
      if(reg_type == "full/"){
        eurocat_file[registry_name == "Tuscany - Italy", registry_name:= "Tuscany - Italy "]
        eurocat_file[registry_name == "Hainaut - Belgium", registry_name := "Hainaut - Belgium 2"]
        if(year %in% c(2011, 2012)){
          eurocat_file[registry_name == "Odense - Denmark", registry_name := "Odense - Denmark 2"]
          eurocat_file[registry_name == "Dublin - Ireland", registry_name := "Dublin - Ireland 2"]
        }
      }
      
      if(reg_type == "associate/" & year == 2012){
        eurocat_file[registry_name == "Spain Hospital Network", registry_name := "Spain Hospital Network 2"]
      }
      
      eurocat_file[,registry_name := gsub('\\d+',"",registry_name)][,registry_name := trimws(registry_name)]
      eurocat_file <- eurocat_file[!grepl('%', registry_name)]
      
      ### subset to relevant countries
      year_sheet_split <- copy(year_sheet)
      loc_sheet <- year_sheet_split[type == "locs"]
      case_sheet <- year_sheet_split[type == "cases"]
      
      file_year = year
      
      if(file_year == 2011){
        case_2011 <- case_sheet[year == file_year]
        eurocat_file <- eurocat_file[case_name %in% unique(case_2011$var)]
      } else if(file_year == 2016){
        case_2016 <- case_sheet[year == file_year]
        eurocat_file <- eurocat_file[case_name %in% unique(case_2016$var)]
      } else {
        unique_countries <- loc_sheet[year == file_year & registry == reg_type ]
        unique_countries <- unique_countries[!is.na(var)] 
        ctry_list <- unique(unique_countries$var)
        eurocat_file <- eurocat_file[registry_name %in% ctry_list]
      }
      
      final <- rbind(final, eurocat_file)
    }
  }
  return(final)
}

#### create_unique_locs ####
create_unique_locs <- function(data_dir, data_years, registries){
  ### returns a sheet of all unique countries and the most recent year file that they appear in
  unique_locs <- data.frame()
  for(reg_type in  registries){
    if(reg_type == "associate/"){
      years_list <- data_years[data_years != 2011]
    } else{
      years_list <- data_years
    }
    for(year in years_list){
      eurocat_file <- as.data.table(read.xlsx(paste0(data_dir, reg_type,'1980_', year,'.XLSX'), startRow = 3))
      eurocat_file[, c('Anomaly.^') := gsub(' §', '', eurocat_file$'Anomaly.^')]
      
      print(paste0(data_dir, reg_type,'1980_', year, '.XLSX'))
      
      setnames(eurocat_file, c(1:2), c("registry_name", "anomaly"))
      
      eurocat_file <- eurocat_file[, list(registry_name, anomaly)]
      eurocat_file <- eurocat_file[, fill(eurocat_file, c("registry_name", "anomaly"))]
      
      year_data <- copy(eurocat_file)
      year_data[, c("year", "registry") := list(year, reg_type)]
      year_data <- unique(year_data)
      setnames(year_data, c("registry_name", "anomaly"), c("locations", "case"))
      for(country in unique(year_data$locations)) {
        print(paste0(country, " ~~ ", year, " ~~ ", reg_type, " ~~ ", 
                     nrow(year_data[locations == country])))
      }
      
      ### create df of unique registries, per registry_type/year
      if(reg_type == "full/"){
        year_data[locations == "Tuscany - Italy", locations := "Tuscany - Italy "]
        year_data[locations == "Hainaut - Belgium", locations := "Hainaut - Belgium 2"]
        if(year %in% c(2011, 2012)){
          year_data[locations == "Odense - Denmark", locations := "Odense - Denmark 2"]
          year_data[locations == "Dublin - Ireland", locations := "Dublin - Ireland 2"]
        }
      }
      
      if(reg_type == "associate/" & year == 2012){
        year_data[locations == "Spain Hospital Network", locations := "Spain Hospital Network 2"]
      }
      
      year_data[, locations := gsub('\\d+', "", locations)][,locations := trimws(locations)]
      year_data <- year_data[!grepl('%', locations)]
      
      unique_locs <- rbind(unique_locs, year_data)
    }
  }
  
  unique_locs <- unique_locs[!is.na(locations)]
  
  locs <- unique_locs[ , .SD[which.max(year)], by = locations]
  locs[, case := NULL][, type := "locs"]
  setnames(locs, "locations", "var")
  
  cases <- unique_locs[ , .SD[which.max(year)], by = case]
  cases[, locations := NULL][, type := "cases"]
  setnames(cases, "case", "var")
  
  final <- rbind(locs, cases)
  return(final)
}

#### format_locations ####
format_locations <- function(final){
  ### merge in nids, create site_memo and location_name
  ### Returns: data ready for subnat splitting
  nid_map <- fread("FILEPATH")
  nid_map[, c("V1", "nid_2017", "underlying_nid_2017") := NULL]
  
  final <- merge(final, nid_map, by = "registry_name", all.x = TRUE)
  
  #registry_name is changed to site_memo
  hyphened <- final[registry_name %like% " - "]
  hyphened[, c("site_memo", "country") := tstrsplit(registry_name, " - ", fixed = TRUE)]
  hyphened[, c("registry_name", "country") := NULL]
  setcolorder(hyphened, c(16, 1:15))
  
  non_hyphen <- final[!registry_name %like% " - "]
  setnames(non_hyphen, "registry_name", "site_memo")
  
  final <- rbind(hyphened, non_hyphen)
  return(final)
}

#### create_italy_subnational ####
create_italy_subnationals <- function(format_locs, gbd_round, step){
  italy <- copy(format_locs)
  italy <- italy[location_name == "Italy"]
  italy[, location_name := NULL]
  
  before_sample <- italy[, sum(sample_size)]
  print(paste0("pre-split sample size: ",before_sample))
  before_case <- italy[!is.na(cases_lb), sum(cases_lb)]
  print(paste0("pre-split cases: ", before_case))
  
  
  ne_ita <- italy[site_memo == "North East Italy"]
  italy <- italy[site_memo != "North East Italy"]
  
  italy[site_memo == "Emilia Romagna", site_memo := "Emilia-Romagna"]
  italy[site_memo == "Tuscany", site_memo := "Toscana"]
  italy[site_memo == "Sicily", site_memo := "Sicilia"]
  italy[site_memo == "Milan Area", site_memo := "Lombardia"]
  setnames(italy, "site_memo", "location_name")
  italy[,note_SR:= paste0("Previously mapped to Italy, remapped in GBD round ", gbd_round," for added subnational locations")]
  italy[, site_memo := "Italy"]
  
  loc_names <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round, decomp_step = paste0("step", step))
  loc_names <- loc_names[ , list(location_name, location_id, ihme_loc_id)]
  
  italy <- merge(italy, loc_names, by = "location_name", all.x = TRUE)
  
  ### handle NE Italy by creating population ratio to apply to each point
  pre_pop <- get_population(location_id = c(35501, 35500), sex_id = c(3), year_id = c(1980:1989),
                            gbd_round_id = gbd_round, age_group_id = 164, decomp_step = paste0("step", step))
  post_pop <- get_population(location_id = c(35501, 35498, 35499, 35500), sex_id = c(3), 
                             year_id = c(1990:2017), gbd_round_id = gbd_round, age_group_id = 164, 
                             decomp_step = paste0("step",step ))
  pop <- rbind(pre_pop, post_pop)
  pop[, c("run_id", "sex_id", "age_group_id") := NULL]
  
  total_pops<- pop[, .(total = sum(population)), by = year_id]
  pop <- merge(pop, total_pops, by = 'year_id', all.x = TRUE)
  pop[, ratio := population/total]
  pop[, c("population", "total") := NULL]
  setnames(pop, "year_id", "year_start")
  
  final_ne_ita <- data.frame()
  for(loc in c(35501, 35498, 35499, 35500)){
    loop_ne_ita <- copy(ne_ita)
    loop_ne_ita[, site_memo := "NE Italy"][, location_id := loc]
    loop_ne_ita <- merge(loop_ne_ita, loc_names, by = 'location_id', all.x = TRUE)
    loop_ne_ita <- merge(loop_ne_ita, pop, by = c('year_start', 'location_id'), all.x = TRUE)
    
    # multiplies sample_size and colnames with cases with ratio
    for(j in c('sample_size', grep('cases', colnames(loop_ne_ita), value = TRUE))){
      set(loop_ne_ita, i=NULL, j=j, value= loop_ne_ita[[j]] * loop_ne_ita[['ratio']])
    }
    loop_ne_ita[, ratio:= NULL]
    
    for(n in c(1:nrow(loop_ne_ita))){
      if(loop_ne_ita$year_start[n] %in% c(1980:1989)){
        loop_ne_ita$note_SR[n] <- paste0("Split from North East Italy registry; Veneto and 
        Friuli Venezia Giulia; using gbd round ", gbd_round, " decomp Step ",step, " populations")
      }
      else{
        loop_ne_ita$note_SR[n] <- paste0("Split from North East Italy registry; Veneto, 
        Friuli Venezia Giulia and Trentino Alto Adige (consisting of Provincia autonoma di Bolzano 
        and Provincia autonoma di Trento; using gbd round ", gbd_round, " decomp Step ", step, " populations")
      }
    }
    
    if(loc %in% c(35498, 35499)){ #thes loc ids only entered region after 1989
      loop_ne_ita <- loop_ne_ita[year_start > 1989]
    }
    
    final_ne_ita <- rbind(final_ne_ita, loop_ne_ita)
  }
  
  italy <- rbind(italy, final_ne_ita)
  italy[, c("location_id", "ihme_loc_id") := NULL] # to be merged in later
  italy[, smaller_site_unit := 0]
  italy[, representative_name := "Representative for subnational location only"]
  
  after_sample <- italy[, sum(sample_size)]
  print(paste0("post-split sample size: ",after_sample))
  after_case <- italy[!is.na(cases_lb), sum(cases_lb)]
  print(paste0("post-split cases: ", after_case))
  
  return(italy)
}

#### handle_poland ####
handle_poland <- function(format_locs, map_dir, gbd_round, step){
  pol_data <- copy(format_locs)
  pol_data <- pol_data[location_name == "Poland"]
  
  pol_data_to_split <- pol_data[!(site_memo %in% "Wielkopolska" & year_start > 2009) ]
  pre_split_sample <- pol_data_to_split[, sum(sample_size)]
  pre_split_cases <- pol_data_to_split[!is.na(cases_lb), sum(cases_lb)]
  print(paste0("pre split sample: ", pre_split_sample))
  print(paste0("pre split cases: ", pre_split_cases))
  
  
  wielkopolska_data <- pol_data[site_memo %like% "Wielkopolska" & year_start > 2009]
  wielkopolska_data[, location_name := "Wielkopolskie"]
  wielkopolska_data[, note_SR := "Not split from Poland registry; Since 2010 the Wielkopolska data is sent separately"]
  wielkopolska_data[, representative_name := "Representative for subnational location only"]
  
  pol_map <- as.data.table(read.xlsx(paste0(map_dir, "poland_mapping.xlsx")))
  
  locnames <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round, decomp_step = paste0("step", step))
  locnames <- locnames[parent_id == 51, list(location_name, location_id, ihme_loc_id)] #poland 
  
  final_map <- data.table()
  final_nat_pop <- data.frame()
  
  # creates a total population based on subnationals included in the poland registry at certain years
  for(n in c(1:nrow(pol_map))){
    if(pol_map$site_memo[n] == "Wielkopolska"){
      year_end <- 2009
    } else {
      year_end <- 2017
    }
    
    pol_loop <- data.frame(site_memo = pol_map$site_memo[n],
                           country = pol_map$country[n],
                           location_name = pol_map$location_name[n],
                           year_start = c(pol_map$year_start[n]:year_end))
    pol_loop <- merge(pol_loop, locnames, by = "location_name", all.x = TRUE)
    final_map <- rbind(final_map, pol_loop)
    
    loop_nat_pop <- get_population(age_group_id = 164, 
                                   location_id = unique(pol_loop$location_id), 
                                   year_id = c(min(pol_loop$year_start):max(pol_loop$year_start)), 
                                   sex_id = 3, 
                                   gbd_round_id = gbd_round, 
                                   decomp_step = paste0("step", step))
    loop_nat_pop <- loop_nat_pop[, list(year_id, population)]
    setnames(loop_nat_pop, c("population", "year_id"), c("nat_pop", "year_start"))
    
    final_nat_pop <- rbind(final_nat_pop, loop_nat_pop)
  }
  
  final_nat_pop <- final_nat_pop[, .(total = sum(nat_pop)), by = year_start]
  
  # determines subnational population by  year
  subnat_pops <- get_population(age_group_id = 164, location_id = unique(final_map$location_id), 
                                year_id = unique(final_map$year_start), sex_id = 3, 
                                gbd_round_id = gbd_round, decomp_step = paste0("step", step))
  subnat_pops <- subnat_pops[, list(location_id, year_id, population)]
  setnames(subnat_pops, "year_id", "year_start")
  
  final_map <- merge(final_map, subnat_pops, by = c("location_id", "year_start"), all.x = TRUE)
  final_map <- merge(final_map, final_nat_pop, by = "year_start", all.x = TRUE)
  
  #creates a ratio using subnational population/ total population by year
  final_map[, ratio := population/total]
  final_map[, c("population", "total", "country") := NULL]
  final_map[, location_name := as.character(location_name)]
  
  final_pol_data <- data.table()
  for(region in unique(final_map$location_name)){
    loop_map <- copy(final_map)
    loop_map <- loop_map[location_name == region]
    
    loop_data <- copy(pol_data)
    loop_data <- loop_data[site_memo == unique(loop_map$site_memo)]
    loop_data[, note_SR := paste0("Split from ", unique(loop_map$site_memo), " EUROCAT Registry; ",
                                  region, " registry entering into Polish registry in ", 
                                  min(loop_map$year_start), "; using GBD Round ", gbd_round, " decomp Step ", step, " populations")]
    loop_data[,  c("location_name", "site_memo") := NULL]
    
    loop_data <- merge(loop_data, loop_map, by = "year_start", all.y = TRUE)
    
    for(j in c('sample_size', grep('cases', colnames(loop_data), value = TRUE))){
      set(loop_data, i=NULL, j=j, value= loop_data[[j]] * loop_data[['ratio']])
    }
    loop_data[, ratio := NULL]
    final_pol_data <- rbind(final_pol_data, loop_data)
  }
  
  
  final_pol_data[, c("location_id", "ihme_loc_id") := NULL]
  final_pol_data[, site_memo := "Poland"][year_start > 2006, representative_name := "Nationally representative only"]
  final_pol_data[is.na(representative_name), representative_name := "Representative for subnational location only"]
  final_pol_data <- rbind(final_pol_data, wielkopolska_data)
  final_pol_data[, smaller_site_unit := 0]
  
  return(final_pol_data)
}

#### split_poland ####
split_poland <- function(format_locs, map_dir, gbd_round, step){
  pol_data <- copy(format_locs)
  pol_data <- pol_data[location_name == "Poland"]
  pol_data[, location_name := NULL]
  
  pol_data_to_split <- pol_data[!(site_memo %in% "Wielkopolska" & year_start > 2009) ]
  pre_split_sample <- pol_data_to_split[, sum(sample_size)]
  pre_split_cases <- pol_data_to_split[!is.na(cases_lb), sum(cases_lb)]
  print(paste0("pre split sample: ", pre_split_sample))
  print(paste0("pre split cases: ", pre_split_cases))
  
  
  wielkopolska_data <- pol_data[site_memo %like% "Wielkopolska" & year_start > 2009]
  wielkopolska_data[, location_name := "Wielkopolskie"]
  wielkopolska_data[, note_SR := "Not split from Poland registry; Since 2010 the Wielkopolska data is sent separately"]
  wielkopolska_data[, representative_name := "Representative for subnational location only"]
  
  pol_map <- as.data.table(read.xlsx(paste0(map_dir, "poland_mapping.xlsx")))
  
  locnames <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round, decomp_step = paste0("step", step))
  locnames <- locnames[parent_id == 51, list(location_name, location_id, ihme_loc_id)] #poland 
  
  final_map <- data.table()
  final_nat_pop <- data.frame()
  
  # creates a total population based on subnationals included in the poland registry at certain years
  for(n in c(1:nrow(pol_map))){
    if(pol_map$site_memo[n] == "Wielkopolska"){
      year_end <- 2009
    } else {
      year_end <- 2017
    }
    
    pol_loop <- data.frame(site_memo = pol_map$site_memo[n],
                           country = pol_map$country[n],
                           location_name = pol_map$location_name[n],
                           year_start = c(pol_map$year_start[n]:year_end))
    pol_loop <- merge(pol_loop, locnames, by = "location_name", all.x = TRUE)
    final_map <- rbind(final_map, pol_loop)
    
    loop_nat_pop <- get_population(age_group_id = 164, 
                                   location_id = unique(pol_loop$location_id), 
                                   year_id = c(min(pol_loop$year_start):max(pol_loop$year_start)), 
                                   sex_id = 3, 
                                   gbd_round_id = gbd_round, 
                                   decomp_step = paste0("step", step))
    loop_nat_pop <- loop_nat_pop[, list(year_id, population)]
    setnames(loop_nat_pop, c("population", "year_id"), c("nat_pop", "year_start"))
    
    final_nat_pop <- rbind(final_nat_pop, loop_nat_pop)
  }
  
  final_nat_pop <- final_nat_pop[, .(total = sum(nat_pop)), by = year_start]
  
  # determines subnational population by  year
  subnat_pops <- get_population(age_group_id = 164, location_id = unique(final_map$location_id), 
                                year_id = unique(final_map$year_start), sex_id = 3, 
                                gbd_round_id = gbd_round, decomp_step = paste0("step", step))
  subnat_pops <- subnat_pops[, list(location_id, year_id, population)]
  setnames(subnat_pops, "year_id", "year_start")
  
  pop <- merge(final_nat_pop, subnat_pops, by = "year_start")
  pop[, ratio := population/total]
  pop[, c("population", "total") := NULL]
  
  final_pol_data <- data.table()
  for(loc in unique(final_map$location_id)){
    loop_pol_data <- copy(pol_data_to_split)
    loop_pol_data[, location_id := loc]
    loop_pol_data <- merge(loop_pol_data, locnames, by = 'location_id', all.x = TRUE)
    loop_pol_data <- merge(loop_pol_data, pop, by = c('year_start', 'location_id'), all.x = TRUE)
    loop_pol_data[, note_SR := paste0("Split from ", site_memo, " EUROCAT Registry; ",
                                      location_name, " registry entering into Polish registry in ", 
                                      final_map[location_id == loc, min(year_start)], "; using GBD Round ", gbd_round, " decomp Step ", step, " populations")]
    
    # multiplies sample_size and colnames with cases with ratio
    for(j in c('sample_size', grep('cases', colnames(loop_pol_data), value = TRUE))){
      set(loop_pol_data, i=NULL, j=j, value= loop_pol_data[[j]] * loop_pol_data[['ratio']])
    }
    
    loop_pol_data[, ratio:= NULL]
    loop_pol_data <- loop_pol_data[year_start %in% c(final_map[location_id == loc, min(year_start)]:final_map[location_id == loc, max(year_start)])]
    final_pol_data <- rbind(final_pol_data, loop_pol_data)
    
  }
  
  post_split_sample <- final_pol_data[, sum(sample_size)]
  post_split_cases <- final_pol_data[!is.na(cases_lb), sum(cases_lb)]
  print(paste0("post split sample: ", post_split_sample))
  print(paste0("post split cases: ", post_split_cases))
  
  
  final_pol_data[, c("location_id", "ihme_loc_id") := NULL]
  final_pol_data[, site_memo := "Poland"][year_start > 2006, representative_name := "Nationally representative only"]
  final_pol_data[is.na(representative_name), representative_name := "Representative for subnational location only"]
  final_pol_data <- rbind(final_pol_data, wielkopolska_data)
  final_pol_data[, smaller_site_unit := 0]
  
  return(final_pol_data)
}


#### create_cvs ####
create_cvs <- function(format_locs){
  data <- copy(format_locs)
  all_cases <- grep('cases_', colnames(data), value=TRUE)
  
  reference <- copy(data)
  reference <- reference[!is.na(cases_lb)] #subsets rows to where there are cases live birth
  cv_livestill <- copy(reference)
  cv_excludes_chromos <- copy(reference)
  
  reference[, c('cv_livestill', 'cv_excludes_chromos') := 0]
  reference[, cases := cases_lb]
  
  #cv_livestill includes live and still births
  cv_livestill <- cv_livestill[!is.na(cases_fd)] #based on cases where fetal death is known
  cv_livestill[, c('cv_excludes_chromos') := 0]
  cv_livestill[, c('cv_livestill') := 1]
  cv_livestill[, cases := cases_lb + cases_fd]
  
  ###################
  # looks at cases if chromo anomalies were excluded
  cv_excludes_chromos <- cv_excludes_chromos[!is.na(cases_lb_nonchromo)] #should be no n/a
  cv_excludes_chromos[, c('cv_livestill') := 0]
  cv_excludes_chromos[, c('cv_excludes_chromos') := 1]
  cv_excludes_chromos[, cases := cases_lb_nonchromo]
  
  final <- rbind(reference, cv_livestill, cv_excludes_chromos, fill = T)
  
  all_cases_cv <- paste0("cv_", all_cases)
  
  #change all case names to cv_X
  setnames(final, all_cases, all_cases_cv)
  
  
  return(final)
  
}



#### split_uk####
split_uk <- function(format_locs, gbd_round, step){
  uk_map <- fread("FILEPATH")
  uk_map[year_end == 2016, year_end := 2017]
  uk_data <- copy(format_locs)
  uk_data <- uk_data[location_name == "United Kingdom"]
  uk_data[, location_name := NULL]
  
  pre_split_sample <- uk_data[, sum(sample_size)]
  pre_split_cases <- uk_data[!is.na(cases_lb), sum(cases_lb)]
  print(paste0("pre split sample: ", pre_split_sample))
  print(paste0("pre split cases: ", pre_split_cases))
  
  
  
  #subsets out locations not in England (Glasgow and Wales)
  glas_wales <- copy(uk_data)
  glas_wales <- glas_wales[site_memo %in% c('Glasgow', 'Wales')]
  glas_wales <- glas_wales[, location_name := site_memo]
  glas_wales <- glas_wales[site_memo == 'Glasgow', location_name := 'Scotland']
  glas_wales[, site_memo := "UK"]
  glas_wales[, note_SR := "Data directly from UK registry entry"]
  
  #subsets this out to later deal with historical changes within Thames Valley
  thames <- copy(uk_data)
  thames <- thames[site_memo == 'Thames Valley']
  
  uk_data <- uk_data[!site_memo %in% c('Glasgow', 'Wales', 'Thames Valley')]
  
  utla_final <- data.table()
  for(loc in unique(uk_data$site_memo)){
    loop_data <- copy(uk_data)
    loop_data <- loop_data[site_memo == loc]
    
    #get UTLA populaton based on site memo 
    pops <- get_population(age_group_id = 164, location_id = uk_map[site_memo == loc, unique(location_id)], 
                           year_id = c(uk_map[site_memo == loc, unique(year_start)]
                                       : uk_map[site_memo == loc, unique(year_end)]), 
                           sex_id = 3, gbd_round_id = gbd_round, decomp_step = paste0("step", step))
    pops[, c("run_id", "age_group_id", "sex_id") := NULL]
    pops <- pops[, sum := sum(population), by = year_id]
    pops <- pops[, ratio := population/sum] # makes a ratio of UTLA pop/ total site_memo pop
    pops[, c("population", "sum") := NULL]
    setnames(pops, "year_id", "year_start")
    
    utla_data <- data.table()
    for(utla in unique(pops$location_id)){
      test <- merge(loop_data, pops[location_id == utla], by = 'year_start')
      test <- merge(test, uk_map[location_id == utla & site_memo == loc, .(location_name, location_id)], by = 'location_id')
      test[, note_SR := paste0("Split from ", unique(test$site_memo), " registry; using GBD round ", gbd_round, " decomp Step ", step,"  populations")]
      
      # multiplies UTLA ratio to samples size and case names 
      for(j in c('sample_size', grep('cases', colnames(test), value = TRUE))){
        set(test, i=NULL, j=j, value= test[[j]] * test[['ratio']])
      }
      utla_data <- rbind(utla_data, test)
    }
    utla_final <- rbind(utla_final, utla_data)
    
  }
  
  utla_final[, c("location_id", "ratio") := NULL]
  
  #subsets oxfordshire data from thames data
  ox_data <- copy(thames)
  ox_data <- ox_data[year_start < 2005]
  ox_data[, note_SR := paste0("Split from Thames Valley registry; only contained Oxfordshire data, others added in 2005; using gbd round ", gbd_round, " decomp Step ",  step, " populations")]
  ox_data[,location_name := "Oxfordshire"]
  
  other_thames <- copy(thames)
  other_thames <- other_thames[year_start > 2004]
  
  #like above get UTLA populaton based on site memo
  thames_pop <- get_population(age_group_id = 164, location_id = uk_map[site_memo == "Thames Valley", unique(location_id)], 
                               year_id = c(2005:2017), sex_id = 3, gbd_round_id = gbd_round, decomp_step = paste0("step", step))
  thames_pop[, c("run_id", "age_group_id", "sex_id") := NULL]
  thames_pop <- thames_pop[, sum := sum(population), by = year_id]
  thames_pop[, ratio := population/sum] # makes a ratio of UTLA pop/ total site_memo pop
  thames_pop <- thames_pop[, year_start := year_id][, year_id := NULL]
  thames_pop[, c("population", "sum"):= NULL]
  
  thames_final <- data.table()
  for(utla in unique(thames_pop$location_id)){
    test <- merge(other_thames, thames_pop[location_id == utla], by = 'year_start')
    test <- merge(test, uk_map[location_id == utla, .(location_name, location_id)], by = 'location_id')
    test[, note_SR := paste0("Split from ", unique(test$site_memo), " registry; 1991-2004 only contained Oxfordshire data, others added in 2005; using GBD round ", gbd_round, " decomp Step ", step, " populations")]
    
    for(j in c('sample_size', grep('cases', colnames(test), value = TRUE))){
      set(test, i=NULL, j=j, value= test[[j]] * test[['ratio']])
    }
    
    thames_final <- rbind(thames_final, test)
  }
  
  thames_final[, c("population", "sum", "location_id", "ratio") := NULL]
  thames_final <- rbind(thames_final, ox_data)
  
  
  final <- rbind(thames_final, glas_wales, utla_final)
  
  post_split_sample <- final[, sum(sample_size)]
  post_split_cases <- final[!is.na(cases_lb), sum(cases_lb)]
  print(paste0("post split sample: ", post_split_sample))
  print(paste0("post split cases : ", post_split_cases))
  
  final <- final[, smaller_site_unit := 0]
  final <- final[, representative_name := 'Representative for subnational location only']
  
  return(final)
}


#### merge_level1_bun_id ####
merge_level1_bun_id <- function(format_locs){
  data = copy(format_locs)
  keep_cols <- c(grep('eurocat', colnames(case_map), value = TRUE), 'eurocat_column_1', 
                 'Level1-Bundel.ID')
  case_map_level1 <- copy(case_map)
  case_map_level1 <- case_map_level1[, c(keep_cols), with = FALSE]
  
  drop_cases <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  drop_cases <- drop_cases[registry == 'eurocat']
  
  final <- data.table()
  for(col in c(1:3)){
    use_col <- paste0('eurocat_', col)
    loop_keeps <- c(use_col, 'Level1-Bundel.ID')
    loop_map <- copy(case_map)
    loop_map <- loop_map[eurocat_column_1 == col]
    loop_map <- loop_map[, c(loop_keeps), with = F]
    loop_map <- unique(loop_map)
    setnames(loop_map, use_col, "case_name")
    loop_map <- loop_map[!case_name %in% unique(drop_cases$case_name)]
    
    loop_data <- copy(data)
    loop_data <- loop_data[case_name %in% loop_map$case_name]
    
    test <- merge(loop_data, loop_map, by = 'case_name', all.x = TRUE)
    final <- rbind(final, test)
    
  }
  setnames(final, "Level1-Bundel.ID", "bundle_id")
  return(final)
}

#### merge_level2_bun_id ####
merge_level2_bun_id <- function(format_locs){
  data = copy(format_locs)
  keep_cols <- c(grep('eurocat', colnames(case_map), value = TRUE), 'eurocat_column_2', 'Level2-Bundel.ID')
  case_map <- case_map[, c(keep_cols), with = FALSE]
  
  drop_cases <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  drop_cases <- drop_cases[registry == 'eurocat']
  
  final <- data.table()
  for(col in c(1:3)){
    use_col <- paste0('eurocat_', col)
    loop_keeps <- c(use_col, 'Level2-Bundel.ID')
    loop_map <- copy(case_map)
    loop_map <- loop_map[eurocat_column_2 == col]
    loop_map <- loop_map[, c(loop_keeps), with = F]
    loop_map <- unique(loop_map)
    setnames(loop_map, use_col, 'case_name')
    loop_map <- loop_map[!case_name %in% unique(drop_cases$case_name)]
    
    loop_data <- copy(data)
    loop_data <- loop_data[case_name %in% loop_map$case_name]
    
    test <- merge(loop_data, loop_map, by = 'case_name', all.x = TRUE)
    final <- rbind(final, test)
    
  }
  setnames(final, "Level2-Bundel.ID", "total_bundle_id")
  return(final)
}

#### merge_level1 ####
merge_level1 <- function(format_locs){
  
  data <- copy(format_locs)
  keep_cols <- c(grep('eurocat', colnames(case_map), value = TRUE), 'Level1-Bundel.ID', 'Level2-Bundel.ID')
  case_map <- case_map[, c(keep_cols), with = FALSE]
  
  drop_cases <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  drop_cases <- drop_cases[registry == "eurocat"]  
  
  final <- data.table()
  for(col in c(1:4)){
    if(col == 1 | col == 4){
      use_col <- paste0("eurocat_", col)
      print(use_col)
      loop_keeps <- c(use_col, 'Level1-Bundel.ID')
      loop_map <- copy(case_map)
      loop_map <- loop_map[, c(loop_keeps), with = F]
      loop_map <- unique(loop_map)
      setnames(loop_map, use_col, "case_name")
      loop_map <- loop_map[!case_name %in% unique(drop_cases$case_name)]
      loop_map <- loop_map[`Level1-Bundel.ID` != 0][!is.na(case_name)]
      
      loop_data <- copy(data)
      loop_data <- loop_data[case_name %in% loop_map$case_name]
      
      test <- merge(loop_data, loop_map, by = "case_name", allow.cartesian = T) 
      
    } else {
      use_col <- paste0("eurocat_", col)
      print(use_col)
      loop_keeps <- c(use_col, "Level1-Bundel.ID", "Level2-Bundel.ID")
      loop_map  <- copy(case_map)
      loop_map <- loop_map[, c(loop_keeps), with = F]
      loop_map <- unique(loop_map)
      setnames(loop_map, use_col, "case_name")
      loop_map <- loop_map[!case_name %in% unique(drop_cases$case_name)]
      loop_map <- loop_map[`Level1-Bundel.ID` != 0]
      loop_map <- loop_map[!is.na(case_name)]
      loop_map <- loop_map[`Level2-Bundel.ID` == 0, list(case_name, `Level1-Bundel.ID`)]
      
      loop_data <- copy(data)
      loop_data <- loop_data[case_name %in% loop_map$case_name]
      
      test <- merge(loop_data, loop_map, by = "case_name", allow.cartesian = T )
      
    }
    final <- rbind(final, test)
    
  }
  setnames(final, "Level1-Bundel.ID", "bundle_id")
  return(final)
}


#### merge_level2####
merge_level2 <- function(format_locs){
  
  data <- copy(format_locs)
  keep_cols <- c(grep('eurocat', colnames(case_map), value = TRUE), 'Level1-Bundel.ID', 'Level2-Bundel.ID')
  case_map <- case_map[, c(keep_cols), with = FALSE]
  
  drop_cases <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
  drop_cases <- drop_cases[registry == "eurocat"]
  drop_cases <- drop_cases[!case_name %like% "Nervous"]
  
  final <- data.table()
  for(col in c(2:5)){
    if(col == 2){
      use_col <- paste0("eurocat_", col)
      print(use_col)
      loop_keeps <- c(use_col, "Level1-Bundel.ID", "Level2-Bundel.ID")
      loop_map <- copy(case_map)
      loop_map <- loop_map[, c(loop_keeps), with = F]
      loop_map <- unique(loop_map)
      setnames(loop_map, use_col, "case_name")
      loop_map <- loop_map[!case_name %in% unique(drop_cases$case_name)]
      loop_map <- loop_map[!is.na(case_name)]
      loop_map <- loop_map[`Level2-Bundel.ID` != 0]
      
      loop_map <- loop_map[, list(case_name, `Level2-Bundel.ID`)]
      loop_map <- unique(loop_map)
      
      loop_data <- copy(data)
      loop_data <- loop_data[case_name %in% loop_map$case_name]
      
      test <- merge(loop_data, loop_map, by = "case_name", allow.cartesian = T) 
      
    } else {
      use_col <- paste0("eurocat_", col)
      print(use_col)
      loop_keeps <- c(use_col, "Level2-Bundel.ID")
      loop_map  <- copy(case_map)
      loop_map <- loop_map[, c(loop_keeps), with = F]
      loop_map <- unique(loop_map)
      setnames(loop_map, use_col, "case_name")
      loop_map <- loop_map[!case_name %in% unique(drop_cases$case_name)]
      loop_map <- loop_map[!is.na(case_name)]
      loop_map <- loop_map[`Level2-Bundel.ID` != 0]
      
      loop_data <- copy(data)
      loop_data <- loop_data[case_name %in% loop_map$case_name]
      
      test <- merge(loop_data, loop_map, by = "case_name", allow.cartesian = T)
      
    }
    final <- rbind(final, test)
    
  }
  setnames(final, "Level2-Bundel.ID", "bundle_id")
  return(final)
}


#### sex specific denom ####
sex_specific_denom <- function(final, gbd_round, step){
  data <- copy(final)
  
  # assign sex
  data[, sex := "Both"]
  data[case_name == "Turner syndrome", sex := "Female"]
  data[case_name == "Klinefelter syndrome", sex := "Male"]
  data[case_name %like% "spadia", sex := "Male"]
  
  data_both <- data[sex == "Both" & !case_name %like% "determin" ]
  data_sex_spec <- data[!sex == "Both"]
  
  # get locations
  data_sex_spec <- data_sex_spec[, year_id := year_start]
  pops <- get_population(age_group_id = 164, location_id = unique(data_sex_spec$location_id), year_id = unique(data_sex_spec$year_start), 
                         sex_id = c(1,2,3), gbd_round_id = gbd_round, decomp_step = paste0("step", step))
  pops[, c("age_group_id", "run_id") := NULL]
  
  pops <- dcast(pops, formula = location_id + year_id ~ sex_id, value.var = c("population"))
  pops <- data.table(pops)
  pops[, male := pops$"1"/pops$"3"]
  pops[, female := pops$"2"/ pops$"3"]
  pops[, c("1", "2", "3") := NULL]
  
  data_sex_spec <- merge(data_sex_spec, pops, by = c("year_id", "location_id"))
  data_sex_spec <- data_sex_spec[sex == "Male", sample_size := sample_size * male]
  data_sex_spec <- data_sex_spec[sex == "Female", sample_size := sample_size * female]
  
  data_sex_spec[, c("year_id", "male", "female") := NULL]
  
  indeterminate <- data[case_name %like% "determin"]
  indeterminate[, ratio := 0.5]
  
  indeterminate_final <- data.table()
  for(row in 1:nrow(indeterminate)){
    male_row <- copy(indeterminate[row])
    for(j in c('sample_size', grep('cases', colnames(male_row), value = TRUE))){
      set(male_row, i=NULL, j=j, value= male_row[[j]] * male_row[["ratio"]])
    }
    male_row[, sex:= "Male"]
    
    female_row <- copy(indeterminate[row])
    
    for(j in c('sample_size', grep('cases', colnames(female_row), value = TRUE))){
      set(female_row, i=NULL, j=j, value= female_row[[j]] * female_row[["ratio"]])
    }
    
    female_row[, sex:= "Female"]
    
    both <- rbind(male_row, female_row)
    indeterminate_final <- rbind(indeterminate_final, both)
    
  }
  
  indeterminate_final[, ratio := NULL]
  
  data <- rbind(data_both, data_sex_spec, indeterminate_final)
  
  return(data)
  
}



