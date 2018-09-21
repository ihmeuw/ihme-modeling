###################
### Setting up ####
###################

source('utility.r')

###################################################################
# Raking blocks
###################################################################

##--PCOMP------------------------------------------------------------------------------

tag.p_comp <- function(df, threshold = 0.6) {
  # Levels of subnationals
  levels.subnat <- df[level>3]$level %>% unique %>% sort
  # Levels of subnat that are also parents
  levels.subnat.parent <- levels.subnat[1:(length(levels.subnat)-1)]
  # Generate the average estimate by parent, sex, and age
  df <- df[, p_mean := mean(gpr_mean), by=c("parent_id", "age_group_id", "sex_id")]   
  # Generate an indicator (p_comp) for subnationals which should be raked in p_complement space
  df <- df[, p_comp := ifelse(p_mean > threshold & level %in% levels.subnat, 1, NA)]
  # Tag p_comp for any child of any parent that is tagged with p_comp
  for (parent_level in levels.subnat.parent) {
    p_tag <- df[level==parent_level & p_comp == 1, .(location_id, age_group_id, sex_id, p_comp)] %>% unique
    setnames(p_tag, c("location_id", "p_comp"), c("parent_id", "p_comp_parent"))
    df <- merge(df, p_tag, by=c("parent_id", "age_group_id", "sex_id"), all.x=TRUE)
    df <- df[p_comp_parent==1, p_comp := 1]
    df$p_comp_parent <- NULL
  }
  # Clean
  df <- df[, p_mean := NULL]
  return(df)
}

apply.p_comp <- function(df, vars) {
  df<- df[p_comp==1, (vars) := lapply(.SD, function(x) 1 - x), .SDcols=vars]
  return(df)
}

##--RAKE------------------------------------------------------------------------------

calculate.rf <- function(df, lvl) {
  ## Calculate parent sum
  parent <- copy(df)
  parent <- parent[, parent_sum := gpr_mean * population]
  parent <- parent[, .(location_id, year_id, age_group_id, sex_id, parent_sum)]
  setnames(parent, "location_id", "parent_id") 
  ## Get sum of population weighted totals at level
  df <- df[, aggregated_sum := sum(gpr_mean * population, na.rm=T), by=c("parent_id", "year_id", "age_group_id", "sex_id")]
  ## Merge parent sum on
  df <- merge(df, parent, by=c("parent_id", "year_id", "age_group_id", "sex_id"), all.x=T)
  ## Get ratio of parent to aggregation
  df <- df[level==lvl, rake_factor := aggregated_sum/parent_sum]
  df <- df[, c("aggregated_sum", "parent_sum") := NULL]
  return(df)
}

rake.estimates <- function(df, lvl, vars, dont_rake) {
  df <- calculate.rf(df, lvl)
  ## Save unraked estimates if no draws
  if (draws == 0) df <- df[level == lvl & !is.na(rake_factor) & !is.element(parent_id, dont_rake), 
                                  paste0(vars, "_unraked") := lapply(.SD, function(x) x), .SDcols=vars]
  ## Rake particular level if theres a rake factor and parent isnt an element in dont_rake
  df <- df[level == lvl & !is.na(rake_factor) & !is.element(parent_id, dont_rake), 
            (vars) :=lapply(.SD, function(x) x/df[level == lvl & !is.na(rake_factor) & !is.element(parent_id, dont_rake), rake_factor]), .SDcols=vars]
  return(df)
}

##--AGGREGATE------------------------------------------------------------------------------

aggregate.estimates <- function(df, vars, parent_ids) {
  ## Drop locations being created by aggregation
  df <- df[!location_id %in% parent_ids]
  ## Subset to requested parent_ids
  agg <- df[parent_id %in% parent_ids] %>% copy
  key <- c("parent_id", "year_id", "age_group_id", "sex_id")
  ## Aggregate estimates to the parent_id [ sum(var * pop) /sum_pop ]
  agg <- agg[, sum_pop := sum(population), by=key]
  agg <- agg[, (vars) := lapply(.SD, function(x) x * agg[['population']]), .SDcols=vars]
  agg <- agg[, (vars) := lapply(.SD, sum), .SDcols=vars, by=key]
  ## De-duplicate so get one set of estimates
  agg <- unique(agg[, c("parent_id", "year_id", "age_group_id", "sex_id", "sum_pop", vars), with=F])
  ## Divide by sum_pop
  agg <- agg[, (vars) := lapply(.SD, function(x) x/agg[['sum_pop']]), .SDcols=vars]
  agg <- agg[, sum_pop := NULL]
  ## Rename parent_id -> location_id
  setnames(agg, "parent_id", "location_id")
  df <- rbind(df, agg, fill=T)
  return(df)
}

save.draws <- function(df, run_id, holdout=1) {

  ## Check that all locations exist
  locs <- get_location_hierarchy(location_set_version_id)[level >= 3 & level <7]$location_id
  missing.locs <- setdiff(locs, unique(df$location_id))
  if (length(missing.locs) != 0) stop(paste0("missing locations ", toString(missing.locs)))
  ## Restrict columns
  cols <- c("location_id", "year_id", "age_group_id", "sex_id", grep("draw_", names(df), value=T))
  df <- df[, cols, with=F]
  ## Create measure_id col
  if (measure_type=="continuous") df <- df[, measure_id := 19]
  if (measure_type=="proportion") df <- df[, measure_id := 18]
  ## Save by locs
  output_path <- paste0(run_root, "/draws_temp_", holdout)
  system(paste0("rm -rf ", output_path))
  system(paste0("mkdir -m 777 -p ", output_path))
  mclapply(locs, function(x) write.csv(df[location_id==x], paste0(output_path, "/", x, ".csv"), row.names=F, na=""), mc.cores=cores) %>% invisible
  print(paste0("Draw files saved to ", output_path))

}

###################################################################
# Raking Process
###################################################################

run.rake <- function(holdout_num=1,get.df=FALSE) {


  ####################
  # Setup
  ####################

  ## Load
  if (draws == 0) df <- model_load(run_id, "gpr", holdout = holdout_num)
  if (draws > 0) df <- fread(paste0(run_root, "/gpr_full_", holdout_num, ".csv")) 
  
  ## Identify which variable names you need to rake
  if (draws > 0) vars <- paste0("draw_", seq(0, draws-1)) else vars <- c("gpr_mean", "gpr_lower", "gpr_upper")

  ## Backtransform outputs
  df <- df[, (vars) := lapply(.SD, function(x) transform_data(x, data_transform, reverse=T)), .SDcols=vars]
    
  ## Set countries you don't want to rake for
  if (!is.blank(aggregate_ids)) aggregate_ids<-as.numeric(unlist(strsplit(gsub(" ", "", aggregate_ids), split = ",")))
  ## Aggregate for location 6 automatically
  aggregate_ids <- aggregate_ids[!is.na(aggregate_ids)] %>% unique

  ## Merge on populations
  source('FILEPATH')
  ## Aggregate for age_group_id 21 populations
  if (21%in%unique(df$age_group_id)) {
    pops <- get_population(location_set_version_id=149, 
                         location_id = paste(unique(df$location_id),collapse=" "),
                         year_id = paste(unique(df$year_id),collapse=" "),
                         age_group_id =     paste(c(unique(df$age_group_id), 30, 31, 32, 235),collapse=" "),
                         sex_id = paste(unique(df$sex_id),collapse=" "),
                         status = "best")
    pops<-pops[age_group_id>=30, age_group_id:=21]
    pops<-pops[, lapply(.SD, sum, na.rm=TRUE), by=c("age_group_id", "sex_id", "location_id", "year_id", "process_version_map_id") ]
  } else {
    pops <- get_population(location_set_version_id=location_set_version_id, 
                           location_id = paste(unique(df$location_id),collapse=" "),
                           year_id = paste(unique(df$year_id),collapse=" "),
                           age_group_id = paste(unique(df$age_group_id),collapse=" "),
                           sex_id = paste(unique(df$sex_id),collapse=" "),
                           status = "best")
  }
  df <- merge(df, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"))

  ## Merge on location hierarchy
  ## Re-source db_tools.r
  source(ubcov_path("db_tools"))
  locs <- get_location_hierarchy(location_set_version_id)[, .(location_id, parent_id, level)]
  df <- merge(df, locs, by="location_id")

  ## Calculate mean if draws
  if (draws > 0) df <- df[, gpr_mean := rowMeans(df[, (vars), with=F])]

  ## Identify which locations / levels to aggregate
  if (!is.blank(level_4_to_3_agg)) level_4_to_3_agg<-as.numeric(unlist(strsplit(gsub(" ", "", level_4_to_3_agg), split = ",")))
  level_4_to_3_agg <- level_4_to_3_agg[!is.na(level_4_to_3_agg)] %>% unique

  if (!is.blank(level_5_to_4_agg)) level_5_to_4_agg<-as.numeric(unlist(strsplit(gsub(" ", "", level_5_to_4_agg), split = ",")))
  level_5_to_4_agg <- level_5_to_4_agg[!is.na(level_5_to_4_agg)] %>% unique
      
  if (!is.blank(level_6_to_5_agg)) level_6_to_5_agg<-as.numeric(unlist(strsplit(gsub(" ", "", level_6_to_5_agg), split = ",")))
  level_6_to_5_agg <- level_6_to_5_agg[!is.na(level_6_to_5_agg)] %>% unique  

  ## Generate three raked data tables: "raked" is raked at all three levels, "raked_5_6" is raked at levels 5 and 6,  "raked_6" is just raked levels 6 to 5
  raked<-copy(df)
  raked_5_6<-copy(df)
  raked_6<-copy(df)
  for (lvl in c(4,5,6)) raked <- rake.estimates(df=raked, lvl=lvl, vars=vars, dont_rake=aggregate_ids)
  for (lvl in c(5,6)) raked_5_6 <- rake.estimates(df=raked_5_6, lvl=lvl, vars=vars, dont_rake=aggregate_ids)
  for (lvl in c(6)) raked_6 <- rake.estimates(df=raked_6, lvl=lvl, vars=vars, dont_rake=aggregate_ids)

  ## Merge location hierarchy
  hierarchy <- get_location_hierarchy(location_set_version_id)
  hierarchy <- hierarchy[, grep("location_id|level", names(hierarchy)), with=F]
  df<-merge(df, hierarchy, by=c("location_id", "level"))
  raked<-merge(raked, hierarchy, by=c("location_id", "level"))
  raked_5_6<-merge(raked_5_6, hierarchy, by=c("location_id", "level"))
  raked_6<-merge(raked_6, hierarchy, by=c("location_id", "level"))

  ## Get a df with just nats to rbind
  nats<-df[!(level_3 %in% c(102, 11, 93, 130, 135, 67, 152, 6, 163, 180, 95, 196))]
    
  ## Aggregate and rake level by level
  out<-NULL
  # Only level 4
  for (i in c(102, 11, 93, 130, 135, 67, 152, 196)) {
    if (i %in% level_4_to_3_agg) {
        temp <- df[level_3 == i & level == 4]
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=i)
        out<-rbind(out, temp, fill = TRUE)
    } else {
        temp <- raked[level_3 == i,]
        out<-rbind(out, temp, fill = TRUE)
    }
  }
  # Only levels 4 and 5
  for (i in c(6, 163, 180)) {
    if (!(i %in% level_4_to_3_agg) & !(i %in% level_5_to_4_agg)) {
        temp <- raked[level_3 == i,]
        out<-rbind(out, temp, fill = TRUE)
    } else if((i %in% level_4_to_3_agg) & !(i %in% level_5_to_4_agg)) {
        temp <- raked_5_6[level_3 == i & (level == 5 | level == 4),]
        if (i == 6) {
            hkg_mac <- df[location_id %in% c(354, 361)]
            temp<-rbind(hkg_mac, temp, fill = TRUE)
        }
        temp<-aggregate.estimates(df=temp, vars=vars, parent_ids=i)
        out<-rbind(out, temp, fill = TRUE)
    } else if((i %in% level_4_to_3_agg) & (i %in% level_5_to_4_agg)) {
        temp <- df[level_3 == i & level == 5]
        lvl_parents <- hierarchy[level_3==i & !is.na(level_4), level_4] %>% unique
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=lvl_parents)
        temp <- temp[,c("population", "parent_id", "level"):=NULL]
        temp <- merge(temp, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"))
        temp <- merge(temp, locs, by='location_id', all.x=T)
        if (i == 6) {
            hkg_mac <- df[location_id %in% c(354, 361)]
            temp<-rbind(hkg_mac, temp, fill = TRUE)
        }
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=i)
        out<-rbind(out, temp, fill = TRUE)
    } else {
        stop("You have specified an impossible rake / aggregation combination.")
    }
  }
 # Levels 4, 5, and 6
  for (i in c(95)) {
    if(!(i %in% level_4_to_3_agg) & !(i %in% level_5_to_4_agg) & !(i %in% level_6_to_5_agg)) {
        temp <- raked[level_3 == i,]
        out<-rbind(out, temp, fill = TRUE)
    } else if ((i %in% level_4_to_3_agg) & !(i %in% level_5_to_4_agg) & !(i %in% level_6_to_5_agg)) {
        temp <- raked_5_6[level_3 == i & (level == 6 | level == 5 | level == 4),]
        if (i == 95) {
            uk <- df[location_id %in% c(433, 434, 4636)]
            temp<-rbind(uk, temp, fill = TRUE)
        }
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=i)
        out<-rbind(out, temp, fill = TRUE)
    } else if ((i %in% level_4_to_3_agg) & (i %in% level_5_to_4_agg) & !(i %in% level_6_to_5_agg)) {
        temp <- raked_6[level_3 == i & (level == 6 | level == 5),]
        lvl_parents <- hierarchy[level_3==i & !is.na(level_4), level_4] %>% unique
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=lvl_parents)
        temp <- temp[,c("population", "parent_id", "level"):=NULL]
        temp <- merge(temp, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"))
        temp <- merge(temp, locs, by='location_id', all.x=T)
        if (i == 95) {
            uk <- df[location_id %in% c(433, 434, 4636)]
            temp<-rbind(uk, temp, fill = TRUE)
        }
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=i)
        out<-rbind(out, temp, fill = TRUE)
    } else if ((i %in% level_4_to_3_agg) & (i %in% level_5_to_4_agg) & (i %in% level_6_to_5_agg)) {
        temp <- df[level_3 == i & level == 6,]
        lvl_parents <- hierarchy[level_3==i & !is.na(level_5), level_5] %>% unique
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=lvl_parents)
        temp <- temp[,c("population", "parent_id", "level"):=NULL]
        temp <- merge(temp, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"))
        temp <- merge(temp, locs, by='location_id', all.x=T)
        lvl_parents <- hierarchy[level_3==i & !is.na(level_4), level_4] %>% unique
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=lvl_parents)
        temp <- temp[,c("population", "parent_id", "level"):=NULL]
        temp <- merge(temp, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"))
        temp <- merge(temp, locs, by='location_id', all.x=T)
        if (i == 95) {
            uk <- df[location_id %in% c(433, 434, 4636)]
            temp<-rbind(uk, temp, fill = TRUE)
        }
        temp <- aggregate.estimates(df=temp, vars=vars, parent_ids=i)
        out<-rbind(out, temp, fill = TRUE)
    } else {
        stop("You have specified an impossible rake / aggregation combination.")
    }
  }
  # Clean up workspace
  rm(raked_5_6, raked_6, raked)

  ## Bind on the nationals
  out<-rbind(out, nats, fill = TRUE)
  df <- copy(out)
  # Clean up workspace
  rm(out)

  #########################
  # Clean and save
  #########################
    
  ## Clean
  varlist <- c("location_id", "year_id", "age_group_id", "sex_id", vars, "rake_factor")
  if (!("rake_factor" %in% colnames(df))) {
    df<-df[, rake_factor:=NA]
  }
  df <- df[, varlist, with=F]
          
  ## Sort
  df <- df[order(location_id, year_id, age_group_id, sex_id)]

  if (!get.df) {

    if (draws == 0) model_save(df, run_id, 'raked', holdout = holdout_num)
    
    if (draws > 0) {
      # Save summary
        df_sum <- df
        draw_cols <- grep("draw_", names(df_sum), value = T)
        df_sum <- df_sum[, gpr_mean := rowMeans(.SD), .SD=draw_cols]
        df_sum <- df_sum[, gpr_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
        df_sum <- df_sum[, gpr_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
        df_sum <- df_sum[, c("location_id", "year_id", "age_group_id", "sex_id", "gpr_mean", "gpr_lower", "gpr_upper"), with=FALSE]
        model_save(df_sum, run_id, 'raked', holdout = holdout_num)
        # Clean workspace
        rm(df_sum)
      # Save draws
        save.draws(df, run_id, holdout = holdout_num)
        unlink(paste0(run_root, "/gpr_full_", holdout_num, ".csv"))
    }
    
  } else return(df)

}


