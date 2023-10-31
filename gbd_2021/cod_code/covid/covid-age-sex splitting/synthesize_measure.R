synthesize_measure <- function(measure, gbd_seir_outputs_dir, prod_seir_outputs_dir,
  rake_locs, prod_gbd_locs, sub_locs, fhs, apply_scalars, gbd_hierarchy) {
  
  if (fhs==T){ #FHS methods
    if (measure == "admissions"){ 
      gbd_daily <- fread(file.path(gbd_seir_outputs_dir, paste0("reference/output_draws/daily_admissions.csv")))
    } else if (measure %in% c("infections")) {
      gbd_daily <- fread(file.path(gbd_seir_outputs_dir, paste0("reference/output_draws/daily_", measure, ".csv")))
    } else if (measure == 'deaths'){
      if (apply_scalars == T){
        gbd_daily <- fread(file.path(gbd_seir_outputs_dir, paste0("reference/output_draws/unscaled_daily_", measure, ".csv")))
      } else {
        gbd_daily <- fread(file.path(gbd_seir_outputs_dir, paste0("reference/output_draws/daily_", measure, ".csv")))
      }
    }else {
      stop("`synthesize_measure` only accepts c('admissions', 'deaths', 'infections').  Please use one of these options or add a measure to synthesize_measure.R ")
    }

    if("observed" %in% names(gbd_daily)) {
      gbd_daily[, observed := NULL]
    }
    
    #If prepping deaths that scalars will be applied to, for 5 locs fill in with regional rates applied to location-specific pop counts
    if (apply_scalars==T & measure=='deaths'){
      scalar_loc_fill <- c(133,131,39,40,189)
      region_ids <- unique(gbd_hierarchy[location_id %in% scalar_loc_fill]$parent_id)
      fill_ids <- gbd_hierarchy[parent_id %in% region_ids & !location_id %in% scalar_loc_fill][, .(location_id, parent_id)]
      loc_counts <- gbd_daily[location_id %in% fill_ids$location_id]
      loc_counts <- merge(loc_counts, fill_ids, by='location_id', all.x=T, allow.cartesian=T)
      region_counts <- loc_counts[,lapply(.SD, sum, na.rm=T),
                                  by = .(date, parent_id),
                                  .SDcols = paste0("draw_", 0:99)]
      setnames(region_counts, 'parent_id', 'location_id')
      
      #all age both sex pops for regions
      pop <- get_population(
        age_group_id = 22,
        sex_id = 3,
        location_id = -1,
        year_id = 2020,
        gbd_round_id = 7,
        decomp_step = "iterative",
      )
      pop_counts <- pop[location_id %in% fill_ids$location_id]
      pop_counts <- merge(pop_counts, fill_ids, by='location_id', all.x=T, allow.cartesian=T)
      region_pops <- pop_counts[, .(population = sum(population)), by='parent_id']
      setnames(region_pops, 'parent_id', 'location_id')
      
      region_rates <- merge(region_counts, region_pops, by='location_id', allow.cartesian=T)
      region_rates[, paste0('draw_', 0:99):=lapply(.SD, function(x) x/population), .SDcols=paste0('draw_', 0:99)]
      region_rates[, population:=NULL]
      pop_fill <- pop[location_id %in% scalar_loc_fill, .(population, location_id)]
      pop_fill <- merge(pop_fill, gbd_hierarchy[, .(location_id, parent_id)], by='location_id', all.x=T, all.y=F)
      scalar_fill <- merge(pop_fill, region_rates, by.x='parent_id', by.y='location_id', allow.cartesian=T)
      scalar_fill[, paste0('draw_', 0:99):=lapply(.SD, function(x) x*population), .SDcols=paste0('draw_', 0:99)]
      scalar_fill[, c('parent_id', 'population'):=NULL]
    }
    
    #locations missing from FHS seir outputs that should be filled in using regional rates applied to location-specific populations
    regional_fill <- c(23,28,29,30,39,66,162,175,176,183,215,349,376,380,37,121,168,7,40) 
    
    if (apply_scalars==T & measure=='deaths'){ #if these conditions are met, then loc ids 39 and 40 were already filled using above code (output=scalar_fill)
      regional_fill <- setdiff(c(23,28,29,30,39,66,162,175,176,183,215,349,376,380,37,121,168,7,40), c(133,131,39,40,189)) 
    } 
    
    regions <- c(unique(gbd_hierarchy[location_id %in% regional_fill]$parent_id), 44533)
    region_counts <- gbd_daily[location_id %in% regions]
    china_subnats <- gbd_hierarchy[parent_id==6 & !location_id %in% c(354,361)]$location_id
    
    #all age both sex pops for regions
    pop <- get_population(
      age_group_id = 22,
      sex_id = 3,
      location_id = -1,
      year_id = 2020,
      gbd_round_id = 7,
      decomp_step = "iterative",
    )
    
    #create aggregated 44533 pop
    china_mainland <- pop[location_id %in% china_subnats]
    china_mainland <- china_mainland[, .(population=sum(population)), by=.(age_group_id, sex_id, year_id, run_id)][, location_id:=44533]
    pop <- rbind(pop, china_mainland)
    
    region_rates <- merge(region_counts, pop[, .(location_id, population)], by='location_id', allow.cartesian=T)
    region_rates[, paste0('draw_', 0:99):=lapply(.SD, function(x) x/population), .SDcols=paste0('draw_', 0:99)]
    region_rates[, population:=NULL]
    pop_fill <- pop[location_id %in% c(regional_fill, china_subnats), .(population, location_id)]
    pop_fill <- merge(pop_fill, gbd_hierarchy[, .(location_id, parent_id)], by='location_id', all.x=T, all.y=F)
    pop_fill[parent_id==6, parent_id:=44533]
    estimate_fill <- merge(pop_fill, region_rates, by.x='parent_id', by.y='location_id', allow.cartesian=T)
    estimate_fill[, paste0('draw_', 0:99):=lapply(.SD, function(x) x*population), .SDcols=paste0('draw_', 0:99)]
    estimate_fill[, c('parent_id', 'population'):=NULL]
    
    #rake china subnationals excluding SARs to parent (loc_id==44533)
    p_dt <- gbd_daily[location_id == 44533]
    c_dt <- estimate_fill[location_id %in% china_subnats]
    draw_names <- grep("draw", names(c_dt), value = T)
    # Get daily sum of children
    sum_c_dt <- c_dt[, lapply(.SD, sum), by = date, .SDcols = draw_names]
    # Get daily scalar
    daily_scalar <- p_dt[, draw_names, with = F] / sum_c_dt[, draw_names, with = F]
    daily_scalar[is.na(daily_scalar)] <- 1
    # Apply daily scalar
    raked_c_dt <- cbind(
        c_dt[, .(location_id, date)],
        c_dt[, draw_names, with = F] * daily_scalar[rep(seq_len(nrow(daily_scalar)),length(unique(c_dt$location_id)))]
    )
    
    if (apply_scalars==T & measure=='deaths'){ #if these conditions are met, then we are saving a combination of estimates from: 
      #seir directly (gbd_daily), regional rate fills (estimate_fill), 
      #raked china subnats that were filled from nat rate (raked_c_dt) and 
      #5 locs that needed a special approach before scalars applied (scalar_fill)
      daily_dt <- rbindlist(
        list(
          gbd_daily[!location_id %in% c(scalar_loc_fill,regional_fill,china_subnats)], 
          estimate_fill[!location_id %in% china_subnats], 
          raked_c_dt,
          scalar_fill
        )
      )
    
      
    } else { #otherwise, skip the scalar fill approach and saved the rest
      daily_dt <- rbindlist(
        list(
          gbd_daily[! location_id %in% c(regional_fill,china_subnats)], 
          estimate_fill[!location_id %in% china_subnats], 
          raked_c_dt
        )
      )
      
      
    }
   
    #confirm no duplicate location-dates
    if ( sum(duplicated(daily_dt, by=c("location_id", "date"))) > 0 ){
      stop("daily_dt contains duplicates!")
    }

  } else { #GBD methods
    
    if (measure == "admissions"){ 
      gbd_daily <- fread(file.path(gbd_seir_outputs_dir, paste0("reference/output_draws/hospital_admissions.csv")))
      prod_daily <- fread(file.path(prod_seir_outputs_dir, paste0("reference/output_draws/hospital_admissions.csv")))
    } else if (measure %in% c("infections")) {
      gbd_daily <- fread(file.path(gbd_seir_outputs_dir, paste0("reference/output_draws/daily_", measure, ".csv")))
      prod_daily <- fread(file.path(prod_seir_outputs_dir, paste0("reference/output_draws/daily_", measure, ".csv")))
    } else if (measure == 'deaths'){
      if (apply_scalars == T){
        gbd_daily <- fread(file.path(gbd_seir_outputs_dir, paste0("reference/output_draws/unscaled_daily_", measure, ".csv")))
        prod_daily <- fread(file.path(prod_seir_outputs_dir, paste0("reference/output_draws/unscaled_daily_", measure, ".csv")))
      } else {
        gbd_daily <- fread(file.path(gbd_seir_outputs_dir, paste0("reference/output_draws/daily_", measure, ".csv")))
        prod_daily <- fread(file.path(prod_seir_outputs_dir, paste0("reference/output_draws/daily_", measure, ".csv")))
      }
    } else {
      stop("`synthesize_measure` only accepts c('admissions', 'deaths', 'infections').  Please use one of these options or add a measure to synthesize_measure.R ")
    }
    
    prod_daily <- prod_daily[, names(gbd_daily), with = F] # match draws
    
    #zero fill 
    zero_locs <- c(374,413) #Niue, Tokelau
    zero_dt <- copy(prod_daily[location_id==1])
    zero_dt[, paste0('draw_', 0:99):=0]
    
    fill_zero <- function(l){
      dt <- copy(zero_dt)[, location_id:=l]
      return(dt)
    }
    
    zeros_fill <- rbindlist(lapply(zero_locs, fill_zero))
    prod_daily <- rbind(prod_daily, zeros_fill)
    
    #If prepping deaths that scalars will be applied to, 5 locations need special handling. Estimate daily deaths
    #in these locs using the regional mortality rates applied to location-specific pop counts.
    if (apply_scalars==T & measure=='deaths'){
      regional_fill <- c(133,131,39,40,189) #Venezuela, Nicaragua, Tajikistan, Turkmenistan, Tanzania
      region_ids <- unique(gbd_hierarchy[location_id %in% regional_fill]$parent_id)
      fill_ids <- gbd_hierarchy[parent_id %in% region_ids & !location_id %in% regional_fill][, .(location_id, parent_id)]
      loc_counts <- prod_daily[location_id %in% fill_ids$location_id]
      loc_counts <- merge(loc_counts, fill_ids, by='location_id', all.x=T, allow.cartesian=T)
      region_counts <- loc_counts[,lapply(.SD, sum, na.rm=T),
                                  by = .(date, parent_id),
                                  .SDcols = paste0("draw_", 0:99)]
      setnames(region_counts, 'parent_id', 'location_id')
      
      #all age both sex pops for regions
      pop <- get_population(
        age_group_id = 22,
        sex_id = 3,
        location_id = -1,
        year_id = 2020,
        gbd_round_id = 7,
        decomp_step = "iterative",
      )
      pop_counts <- pop[location_id %in% fill_ids$location_id]
      pop_counts <- merge(pop_counts, fill_ids, by='location_id', all.x=T, allow.cartesian=T)
      region_pops <- pop_counts[, .(population = sum(population)), by='parent_id']
      setnames(region_pops, 'parent_id', 'location_id')
      
      region_rates <- merge(region_counts, region_pops, by='location_id', allow.cartesian=T)
      region_rates[, paste0('draw_', 0:99):=lapply(.SD, function(x) x/population), .SDcols=paste0('draw_', 0:99)]
      region_rates[, population:=NULL]
      pop_fill <- pop[location_id %in% regional_fill, .(population, location_id)]
      pop_fill <- merge(pop_fill, gbd_hierarchy[, .(location_id, parent_id)], by='location_id', all.x=T, all.y=F)
      estimate_fill <- merge(pop_fill, region_rates, by.x='parent_id', by.y='location_id', allow.cartesian=T)
      estimate_fill[, paste0('draw_', 0:99):=lapply(.SD, function(x) x*population), .SDcols=paste0('draw_', 0:99)]
      estimate_fill[, c('parent_id', 'population'):=NULL]
    }

    
    parent_hierarchy <- get_location_metadata(location_set_id=111, location_set_version_id = 1050, release_id=9)
    rake_dt <- rake_measure(rake_locs, gbd_daily,
                            prod_daily, gbd_hierarchy, parent_hierarchy)
 
    if("observed" %in% names(prod_daily)) {
      prod_daily[, observed := NULL]
    }
    if("observed" %in% names(gbd_daily)) {
      gbd_daily[, observed := NULL]
    }
    
    missing_from_prod <- setdiff(prod_gbd_locs, c(prod_daily$location_id, rake_locs))
    
    if (apply_scalars==T & measure=='deaths'){
    daily_dt <- rbindlist(
      list(
        prod_daily[location_id %in% prod_gbd_locs & !location_id %in% c(rake_locs, regional_fill)],
        estimate_fill,
        gbd_daily[location_id %in% c(sub_locs, missing_from_prod)],
        rake_dt
      )
    )
    } else {
      daily_dt <- rbindlist(
        list(
          prod_daily[location_id %in% prod_gbd_locs & !location_id %in% c(rake_locs)],
          gbd_daily[location_id %in% c(sub_locs, missing_from_prod)],
          rake_dt
        )
      )
    }
  }
  
  return(daily_dt)
}

rake_measure <- function(rake_locs, gbd_daily, prod_daily, 
  gbd_hierarchy, parent_hierarchy) {
  #' Prepare a table with parent child relationships for all most detailed 
  #' locations in a hierarchy and a vector of parents
  prep_parent_child <- function(parent_hierarchy, child_hierarchy, children) {
    parent_child <- rbindlist(lapply(children, function(l) {
      path_to_top <- rev(as.integer(
        strsplit(
          child_hierarchy[location_id == l]$path_to_top_parent,
          ","
        )[[1]]
      ))
      present <- which(path_to_top %in% parent_hierarchy[most_detailed == 1]$location_id)
      if(length(present) > 0) {
        p <- path_to_top[min(present)]
      } else {
        p <- NA
      }
      return(data.table(location_id = l, parent_id = as.integer(p)))
    }))
    # Other Union Territories as parent for Other Union Territories
    parent_child[is.na(parent_id), parent_id := 44538]
    return(parent_child)
  }

  p_c_dt <- prep_parent_child(parent_hierarchy, gbd_hierarchy, rake_locs) 
  raked_dt <- rbindlist(lapply(unique(p_c_dt$parent_id), function(p) {

    p_dt <- prod_daily[location_id == p]
    c_dt <- gbd_daily[location_id %in% p_c_dt[parent_id == p]$location_id]
    draw_names <- grep("draw", names(c_dt), value = T)
    if(nrow(p_dt) == 0) return(c_dt[, c("location_id", "date", draw_names), with = F])
    # Get daily sum of children
    dates_by_loc <- c_dt[, .(min_d = min(date)), by=.(location_id)]
    min_d <- max(dates_by_loc$min_d)
    p_dt <- p_dt[as.Date(date)>as.Date(min_d)-1]
    c_dt <- c_dt[as.Date(date)>as.Date(min_d)-1]
    sum_c_dt <- c_dt[, lapply(.SD, sum), by = date, .SDcols = draw_names]
    
    # Get daily scalar
    daily_scalar <- p_dt[, draw_names, with = F] / sum_c_dt[, draw_names, with = F]
    daily_scalar[is.na(daily_scalar)] <- 1

    # Apply daily scalar
    raked_c_dt <- cbind(
      c_dt[, .(location_id, date)],
      c_dt[, draw_names, with = F] * daily_scalar[rep(seq_len(nrow(daily_scalar)),length(unique(c_dt$location_id)))]
    )
    return(raked_c_dt)
  }))
  return(raked_dt)
}
