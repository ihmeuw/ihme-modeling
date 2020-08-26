## Life table vetting functions

library(data.table)
library(stringr)
library(ggplot2)
library(mortdb, lib = "FILEPATH/r-pkgs")
code_dir <- "FILEPATH/shared-functions"

## DIRECTORY:
# ==================================================================================
# get_life_table:           pulls empirical LTs from db
# ids_to_names:             utility function, merges on age, sex, outlier_type, parameter, and lt category names
# add_plot_nums:            adds unique identifier column plot_num for each unique life table
# parse_file_path:          takes a file_path column with format dir/{ihme_loc_id}_{year}_{sex}_{lt_category}_{source}_{smooth_width}.jpg
#                             and creates new columns for those components
# outlier_based_on_mv:      takes lt data.table and mv prediction directory and modifies outlier column in lt data.table accordingly
# outlier_based_on_csv:     takes lt.data.table and a csv with outlier calls and modifies outlier col in lt data.table accordingly
# plot_single_lt:           plotting function for one life table
# plot_to_compiled_pdfs:    plot a series of pdfs of 1000 life tables, sorted by outlier id, saves plot_num map file
# plot_for_machine_vision:  plot single .jpg lts in directories test/train/predict for machine vision
# map_life_tables:          map life table availability
# merge_plot_num_map:       merge on plot number mapping file
# smoothing_plots:          plot series of plots with all smooth_widths adjacent and colored based on outlier status

# get life table data from database
# ==================================================================================
get_life_table <- function(version='best', parameter_ids=c(3), smooth_widths=c(1)){
  message(paste0(Sys.time()," : getting life tables from database"))
  source(paste0(code_dir,"/query_cache_gmo.R"))
  library(mortdb, lib = "FILEPATH/r-pkgs")
  lt <- query_cache_gmo(model_name = "life table empirical",
                        model_type = "data",
                        run_id = version,
                        life_table_parameter_ids = parameter_ids)
  # some subsetting and formatting
  lt <- lt[!age_group_id %in% c(1, 199)]
  if(nrow(lt[is.na(smooth_width)])<0.1*nrow(lt)){
    lt[is.na(smooth_width), smooth_width := -999]
    lt <- lt[smooth_width %in% smooth_widths]
  }
  lt[, log_mean := log(mean)]
  return(lt)
}

# merge names to ids
# ==================================================================================
ids_to_names <- function(lt, age=T, sex=T, outlier_type=T, parameter=T, category=T){

  message(paste0(Sys.time()," : merging ids to names"))
  if(age==T){
    source(paste0(code_dir,"/get_age_map.r"))
    age_map <- data.table(get_age_map(type = "lifetable"))
    lt <- merge(lt, age_map[, c('age_group_id', 'age_group_name','age_group_years_start')], all.x = T)
    setnames(lt,'age_group_years_start','age_start')
    lt[, age_group_name := factor(age_group_name,
                                  levels = c("<1 year", "1 to 4", "5 to 9", "10 to 14", "15 to 19",
                                              "20 to 24", "25 to 29", "30 to 34", "35 to 39",
                                              "40 to 44", "45 to 49", "50 to 54", "55 to 59",
                                              "60 to 64", "65 to 69", "70 to 74", "75 to 79",
                                              "80 to 84", "85 to 89", "90 to 94", "95 to 99",
                                              "100 to 104", "105 to 109", "110 plus"),
                                  ordered = T)]
  }

  if(sex==T){
    lt[sex_id==1, sex:='male']
    lt[sex_id==2, sex:='female']
    lt[sex_id==3, sex:='both']
  }

  if(outlier_type==T){
    source(paste0(code_dir,"/get_mort_ids.R"))
    outlier_types <- get_mort_ids("outlier_type")
    lt <- merge(lt, outlier_types, by = "outlier_type_id")
  }

  if(parameter==T){
    lt[life_table_parameter_id == 3, parameter_name := "qx"]
    lt[life_table_parameter_id == 4, parameter_name := "mx"]
  }

  if(category==T){
    lt_category_names <- data.table(get_mort_ids(type = "life_table_category"))
    lt <- merge(lt,lt_category_names,by='life_table_category_id')
  }

  return(lt)
}

# add plot numbers by unique combinations of source, location, year, sex
# ================================================================================
add_plot_nums <- function(dt){

  cols <- intersect(names(dt),
                    c('source_name','source_type','ihme_loc_id','year','year_id','sex','sex_name','smooth_width'))
  dt[, plot_num := .GRP, by = cols]
  message('Added plot_num by:')
  message(paste(cols,collapse=""))
  return(dt)
}


# file or file_path to ihme_loc_id, year, sex, category, source
# ===============================================================================
parse_file <- function(dt){
  dt[, len:=str_count(file,"_")]
  if(nrow(dt[len==4])>0){
    dt[len==4, c('ihme_loc_id','year_id','sex','source_name','smooth_width'):=tstrsplit(file,"_",fixed=T)]
  }
  if(nrow(dt[len==5])>0){
    dt[len==5, c('parent_iso','location_id','year_id','sex','source_name','smooth_width'):=tstrsplit(file,"_",fixed=T)]
    dt[len==5, ihme_loc_id:=paste0(parent_iso,"_",location_id)]
    dt[,c('location_id','parent_iso') := NULL]
  }
  dt[,c('len','file'):=NULL]
  dt[,smooth_width:=tstrsplit(smooth_width,".",fixed=T,keep=1)]
  dt[,year_id:=as.integer(year_id)]
  dt[,smooth_width:=as.integer(smooth_width)]
  return(dt)
}
parse_file_path <- function(dt){
  len <- str_count(dt$file_path[1] ,'/') + 1
  dt[, file:=tstrsplit(file_path,'/',fixed=T, keep=len)]
  dt <- parse_file(dt)
  return(dt)
}


# correct outlier column according to machine vision output
# ================================================================================
outlier_based_on_mv <- function(dt, mv_dir, threshold_pct=0.2){

  message(paste0(Sys.time()," : outliering based on machine vision"))

  prediction_files <- list.files(mv_dir, pattern='elt_predictions_')
  mv <- data.table()
  for(p in prediction_files){
    temp <- fread(paste0(mv_dir,'/',p))
    mv <- rbind(mv,temp,fill=T)
  }
  if(nrow(mv[actual=='outlier'])>0){
    mv[actual=='outlier',actual:=1]
    mv[actual=='nonoutlier',actual:=0]
    mv[!actual %in% c(1,0), actual:=3]
  }
  mv[predicted<threshold_pct, outlier:=0]
  mv[predicted>(1-threshold_pct), outlier:=1]
  # use outlier=2 for undecided status to be reviewed
  mv[predicted>threshold_pct & predicted<(1-threshold_pct), outlier:=2]

  # also review cases with disagreement between database and this algorithm
  # if(nrow(mv[actual==1])>0) mv[actual!=outlier & outlier!=2, outlier:=3]
  mv <- parse_file_path(mv)
  setnames(mv,'outlier','outlier_mv')

  dt[,`:=`(year_id=as.numeric(year_id), life_table_category_id=as.numeric(life_table_category_id))]
  mv[,`:=`(year_id=as.numeric(year_id), life_table_category_id=as.numeric(life_table_category_id))]
  dt <- merge(dt,mv,by=c('ihme_loc_id','year_id','sex','life_table_category_id','source_name','smooth_width'),all.x=T)

  dt[!is.na(outlier_mv), outlier:=outlier_mv]
  dt[,outlier_mv:=NULL]

  dt <- dt[order(predicted)]

  return(dt)
}

# correct outlier column according to other file
# =================================================================================
outlier_based_on_csv <- function(dt, file.path, id.col, outlier.col){

  message(paste0(Sys.time()," : outliering based on ",file.path))

  decisions <- as.data.table(read.csv(file.path))
  setnames(decisions, id.col, 'plot_num')
  setnames(decisions, outlier.col, 'outlier_decision')
  if('outlier_decision' %in% names(dt)){
    message('outlier_decision already exists in names(dt)')
  }
  decisions <- decisions[,c('plot_num','outlier_decision')]
  dt <- merge(dt, decisions, by='plot_num', all.x=T)
  dt[,outlier:=as.integer(outlier)]
  dt[,outlier_decision:=as.integer(outlier_decision)]
  dt[!is.na(outlier_decision),outlier:=outlier_decision]
  dt[,outlier_decision:=NULL]
  return(dt)
}


# outlier where 1q0 < 4q1
# =================================================================================
outlier_from_1q0 <- function(dt){

  message(paste0(Sys.time()," : outliering based on 1q0"))

  # age_group_id = 28 : 0-1
  # age_group_id = 5  : 1-4
  # mortality in 0-1 should be greater than mort for 1-4

  if(!('plot_num' %in% names(dt))){
    dt <- add_plot_nums(dt)
    rm_plot_num_at_end <- T
  } else {
    rm_plot_num_at_end <- F
  }
  temp <- copy(dt)
  temp <- temp[age_group_id %in% c(5,28), .(plot_num, mean, age_group_id)]
  temp[age_group_id == 28, grp:='zero_to_one']
  temp[age_group_id == 5, grp:='one_to_four']
  temp <-  unique(temp)
  temp <- dcast(temp, plot_num ~ grp, value.var='mean')
  temp <- temp[zero_to_one < one_to_four]
  outlier_plot_nums <- unique(temp$plot_num)

  zeros  <- length(unique(dt[outlier==0 & plot_num %in% outlier_plot_nums]$plot_num))
  ones   <- length(unique(dt[outlier==1 & plot_num %in% outlier_plot_nums]$plot_num))
  others <- length(unique(dt[outlier!=0 & outlier!=1 & plot_num %in% outlier_plot_nums]$plot_num))

  message(paste0(Sys.time()," : ", zeros,  " non-outliers outliered based on 1q0 and 4q1"))
  message(paste0(Sys.time()," : ", ones,   " LTs already outliered, found to have 1q0 < 4q1"))
  message(paste0(Sys.time()," : ", others, " unknown LTs outliered based on 1q0 and 4q1"))

  dt[plot_num %in% outlier_plot_nums, outlier:=1]
  if(rm_plot_num_at_end == T) dt[,plot_num:=NULL]
  return(dt)
}


# plot a single life table
# =================================================================================
plot_single_lt <- function(dt, plot_num=NA, include_title=T){
  library(ggplot2)
  y_limits <- c(-10, 0)
  y_breaks <- c(-10, -5, -2.5, -1, -.5)
  if(include_title==T){
    subtitle <- paste(dt$ihme_loc_id[1],
                      dt$year_id[1],
                      dt$sex[1],
                      dt$source_name[1],
                      dt$outlier[1],
                      sep='_')
    title <- plot_num
  } else {
    subtitle <- NA
    title <- NA
  }
  if("age_start" %in% names(dt)){
    dt[, age_var := age_start]
  } else if("age" %in% names(dt)){
    dt[, age_var := age]
  } else {
    print("Please include age or age_start as variable in dt")
  }
  plot <- ggplot(dt,
                 aes(x = age_var,
                     y = log_mean)) +
                geom_line(aes(group = 1))+
                labs(x = "Age Group", y = "qx: log(x)",
                     subtitle = subtitle, title=title) +
                scale_y_continuous(limits = y_limits, breaks = y_breaks) +
                scale_x_continuous(breaks=unique(dt$age_start)) +
                theme_minimal()
  print(plot)
}


# plot compiled pdfs separated by outlier status
# =================================================================================
plot_to_compiled_pdfs <- function(dt, dir){

  if(!'plot_num' %in% names(dt)) dt <- add_plot_nums(dt)
  map <- unique(dt[,.(ihme_loc_id,year_id,sex,source_name,outlier,plot_num)],by='plot_num')
  write.csv(map,paste0(dir,'/plot_number_mapping.csv'),row.names=F)

  for(outlier_status in unique(dt$outlier)){

    temp <- dt[outlier==outlier_status]
    plot_nums <- unique(temp$plot_num)
    print(paste0(length(plot_nums)," life tables for outlier id ",outlier_status))
    count_max <- ceiling(length(plot_nums)/1000)

    for(count in 1:count_max){
      print(paste0(Sys.time(),' Working on ',dir,'/lifetables_outlier_id_',outlier_status,'_',count,'.pdf'), width=7,height=4)
      pdf(paste0(dir,'/lifetables_outlier_id_',outlier_status,'_',count,'.pdf'))
      start_index <- (count-1)*1000+1
      stop_index <- min(count*1000, length(plot_nums))
      for(i in plot_nums[start_index:stop_index]){
        plot_dt <- temp[plot_num==i]
        plot_single_lt(plot_dt, plot_num=i, include_title=T)
      }
      dev.off()
    }
  }
}

# plot to single jpgs for machine vision
# =================================================================================
plot_for_machine_vision <- function(dt, dir){

  message(paste0(Sys.time()," : beginning plotting for machine vision"))

  # make directories
  dir.create(paste0(dir,'/predict'), recursive=T, showWarnings=F)
  dir.create(paste0(dir,'/test/outlier'), recursive=T, showWarnings=F)
  dir.create(paste0(dir,'/test/non_outlier'), recursive=T, showWarnings=F)
  dir.create(paste0(dir,'/train/outlier'), recursive=T, showWarnings=F)
  dir.create(paste0(dir,'/train/non_outlier'), recursive=T, showWarnings=F)
  # add plot_num if it isn't in dt
  if(!'plot_num' %in% names(dt)) dt <- add_plot_nums(dt)
  total_n_plots <- length(unique(dt$plot_num))

    p <- function(i){
          temp <- dt[plot_num==i]
              plot_set <- temp$plot_set[1]
              outlier_path <- temp$outlier_path[1]
              loc <- temp$ihme_loc_id[1]
              year <- temp$year_id[1]
              sex_name <- temp$sex[1]
              category_id <- temp$life_table_category_id[1]
              source_type <- temp$source_name[1]
              smooth_width <- temp$smooth_width[1]
          plot_single_lt(temp, include_title=F)
          if(outlier_path=='unknown'){
            decade <- floor(year/10)*10
            dir.create(paste0(dir,'/',plot_set,'/unknown',smooth_width,'_',decade), recursive=T, showWarnings=F)
            filepath <- paste0(dir,'/',plot_set,'/unknown',smooth_width,'_',decade,'/',loc,'_',year,'_',
                               sex_name,'_',category_id,'_',source_type,'_',smooth_width,'.jpg')
          } else {
          filepath <- paste0(dir,'/',plot_set,'/',outlier_path,'/',loc,'_',year,'_',
                       sex_name,'_',category_id,'_',source_type,'_',smooth_width,'.jpg')
          }
          ggsave(filepath, width = 20, height = 20, units = "cm", dpi = 320)
          #print(paste0(i,"/",total_n_plots," : ",filepath))
    }

    message('Generating and saving plots')
    time_start <- Sys.time()
    mclapply(unique(dt$plot_num), p)
    time_end <- Sys.time()
    time_elapsed <- round(difftime(time_end,time_start,units='mins'),3)
    message(paste0('Done! time elapsed: ', time_elapsed, ' minutes'))
}

# map life table availability
# ==================================================================================
map_life_tables <- function(lt, rm_outliers=T, dir){
  source('FILEPATH/central-functions/GBD_WITH_INSETS_MAPPING_FUNCTION.R')
  if(rm_outliers==T) lt <- lt[outlier==0,]
  status <- ifelse(rm_outliers==T,'excluding','including')
  lt <- unique(lt,by=c('year_id','source_name','ihme_loc_id'))
  lt[,mapvar:=.N, by=c('ihme_loc_id')]
  lt <- unique(lt, by='ihme_loc_id')
  lt <- lt[,.(ihme_loc_id,mapvar)]
  maxmapvar <- round(max(lt$mapvar)/10,0)*10
  cutpoints <- seq(0,maxmapvar,10)
  cutpoints[1] <- 1
  gbd_map(data=lt,
          limits=cutpoints,
          legend.title='source-years',
          title=paste0('Count of empirical life tables: ',status,' outliers'),
          fname=paste0(dir,'/elt_count_',status,'_outliers_19.pdf'))
}



# merge map and shiny table output for decisions
# =============================================================
merge_plot_num_map <- function(dt, map_dir){
  # dir must contain plot_number_mapping.csv
  # merge_cols options are:
  # ihme_loc_id, year_id, sex, source_name, plot_num
  map <- fread(paste0(map_dir,'/plot_number_mapping.csv'))
  map[,outlier:=NULL]
  merge_cols <- intersect(names(dt), names(map))
  message('merging by:')
  message(paste(unlist(merge_cols), collapse=' '))
  dt <- merge(dt,map,by=merge_cols,all.x=T)
  return(dt)
}


# plot machine vision outputs
# =============================================================
plot_mv_decisions <- function(dir, save_code2_for_shiny = F){
  lt <- get_life_table()
  lt <- ids_to_names(lt)
  lt <- outlier_based_on_mv(lt, mv_dir=dir)
  lt <- outlier_from_1q0(lt)
  lt <- add_plot_nums(lt)
  if(save_code2_for_shiny==T){
    to_review_2 <- lt[outlier==2]
    write.csv(to_review_2, paste0(dir,'/to_review_for_shiny_2.csv'))
  }
  plot_to_compiled_pdfs(lt, dir)
}


# save all smoothing levels to a smooth directory
# ========================================================
save_smooth <- function(dir){
  lt <- get_life_table(smooth_widths = c(3,5,7))
  lt <- ids_to_names(lt)
  lt[, outlier_path:='unknown']
  lt[, plot_set:='smooth']
  plot_for_machine_vision(lt, dir)
}


# plot all smoothing levels w/ decisions
# =============================================
smoothing_plots <- function(version_id){
  # read in all decisions
  # pull all life tables
  # merge on decisions to life tables
  # create indicator var for min smoothing that isn't outlier
  # plot all smoothing widths for each loc-year-source

  if(version_id == "custom"){
    lt <- get_life_table(version = 231, smooth_widths=c(1,3,5,7))
    lt <- lt[ihme_loc_id %like% "USA"]
    lt <- ids_to_names(lt)
    # only keep LTs where all smooth widths are outliered
    lt[, included := ifelse(life_table_category_name == "outlier", 0, 1)]
    lt[, count := sum(included), by = c("age_group_id", "year_id", "ihme_loc_id", "sex_id", "source_name")]
    lt <- lt[count == 0]
  } else {

    # get all life tables
    lt <- get_life_table(version=version_id, smooth_widths=c(1,3,5,7))
    lt <- ids_to_names(lt)

  }

  # add column for which smooth width was chosen
  if(nrow(lt[life_table_category_id!="outlier"]) > 0){
    smooth_id <- copy(lt)
    smooth_id <- as.data.table(smooth_id)
    smooth_id <- smooth_id[life_table_category_name!="outlier"]
    smooth_id[, smooth_to_use:=min(smooth_width, na.rm=T), by=c('ihme_loc_id','year_id','sex','source_name')]
    smooth_id <- smooth_id[,.(smooth_to_use,ihme_loc_id,year_id,sex,source_name)]
    smooth_id <-  unique(smooth_id)
    lt <- merge(lt,smooth_id,by=c('ihme_loc_id','year_id','sex','source_name'),all.x=T, allow.cartesian=T)
    lt[is.na(smooth_to_use),smooth_to_use:=0]
  } else {
    lt[, smooth_to_use := 0]
  }

  # add id, indexed at 0
  lt <- lt[order(smooth_to_use)]
  lt[, plot_num := .GRP, by=c("ihme_loc_id","year_id","sex","source_name")]
  lt[, plot_num := plot_num - 1]
  n <- length(unique(lt$plot_num))
  n_groups <- round(n/1000,0)

  cols <- c('blue', 'green', 'red', 'black')
  names(cols) <- c('universal','location-specific','outlier','other')
  y_limits <- c(-10, 0)
  y_breaks <- c(-10, -5, -2.5, -1, -.5)

  if(version_id == "custom"){
    dir <- "FILEPATH/custom-save-folder/"
  } else {
    dir <- paste0("FILEPATH/empirical_lt/", version_id, "/diagnostics")
  }
  dir.create(dir)

  plot_num_map <- lt[,.(plot_num,ihme_loc_id,year_id,sex,source_name)]
  plot_num_map <- unique(plot_num_map)
  write.csv(plot_num_map, paste0(dir,"/plot_num_map.csv"), row.names=F)

  # plot
  for(start_i in seq(0,n_groups*1000,1000)){
    end_i <- min(start_i+999,n)
    pdf(paste0(dir,'/elt_smoothing_plots_',start_i,'.pdf'), width=13, height=7)
    for(i in start_i:end_i){

      print(paste0(start_i,': ', i-start_i,'/',end_i-start_i))

      lt_plot <- lt[plot_num==i]
      loc <- lt_plot$ihme_loc_id[1]
      year <- lt_plot$year_id[1]
      source <- lt_plot$source_name[1]
      ss <- lt_plot$sex[1]
      smooth_to_use <- lt_plot$smooth_to_use[1]

      gg <- ggplot(data=lt_plot,
                 aes(x=age_start,
                     y=log(mean),
                       color=life_table_category_name)) +
            geom_line() + facet_wrap('smooth_width') +
            theme_bw() + ylab('log qx') + scale_color_manual(values=cols) +
            scale_y_continuous(limits = y_limits, breaks = y_breaks) +
            ggtitle(paste(i,":", loc, year, source, ss, sep=' '))

      plot(gg)
    }
    dev.off()
  }
} # end of function

# save file for shiny
# ============================================
save_for_shiny <- function(){
  # read in all decisions
  files <- list.files('FILEPATH/empirical_lt/machine_vision/', pattern='elt_predictions', full.names=T)
  dt <- data.table(file_path=NA, actual=NA, predicted=NA)
  for(f in files){
    temp <- fread(f)
    dt <- rbind(dt, temp, fill=T)
  }
  dt <- dt[!is.na(predicted)]
  dt <- parse_file_path(dt)
  dt <- dt[,.(predicted,ihme_loc_id,year_id,sex,life_table_category_id,source_name,smooth_width)]
  dt[,predicted:=ifelse(predicted>0.5, 1, 0)]
  dt <- dt[smooth_width==1 & predicted==0]
  dt$ran_num <- runif(nrow(dt),0,1)
  dt <- dt[ran_num < 0.20]
  dt <- dt[1:4000]
  dt <- add_plot_nums(dt)
  lt <- get_life_table()
  lt <- ids_to_names(lt)
  lt <- merge(lt,dt,by=c('ihme_loc_id','year_id','sex','life_table_category_id','source_name','smooth_width'), all.y=T, all.x=F, allow.cartesian=T)
  lt <- lt[order(plot_num)]
  write.csv(lt, 'FILEPATH/empirical_lt/machine_vision/to_review_for_shiny.csv', row.names=F)
}


# workflow to outlier based on csv and save to files
# ======================================================================
wf_05_10_19 <- function(){
  lt <- get_life_table()
  lt <- ids_to_names(lt)
  lt[,outlier:=NA]
  # apply original 6000 decisions
  initial <- read.csv("FILEPATH/empirical_lt/vetting/final_decisions_revised.csv")
  initial <- as.data.table(initial)
  initial <- initial[,.(outlier_decision,ihme_loc_id,year_id,sex,source_name)]
  lt <- merge(lt,initial,by=c('ihme_loc_id','year_id','sex','source_name'),all.x=T)
  lt[,outlier:=as.integer(outlier)]
  lt[,outlier_decision:=as.integer(outlier_decision)]
  lt[!is.na(outlier_decision),outlier:=outlier_decision]
  lt[,outlier_decision:=NULL]

  lt <- merge_plot_num_map(lt, "FILEPATH/empirical_lt/vetting/plot-map-dir")
  lt <- outlier_based_on_csv(lt, "FILEPATH/empirical_lt/vetting/outlier-info.csv", "id", "outlier")
  lt[,plot_num:=NULL]

  lt <- merge_plot_num_map(lt, "FILEPATH/empirical_lt/vetting/plot-map-dir")
  lt <- outlier_based_on_csv(lt, "FILEPATH/empirical_lt/vetting/outlier-info.csv", "id", "outlier_decision")
  lt[,plot_num:=NULL]

  lt <- merge_plot_num_map(lt, "FILEPATH/empirical_lt/vetting/plot-map-dir")
  lt <- outlier_based_on_csv(lt, "FILEPATH/empirical_lt/vetting/outlier-info.csv","plot_num","outlier")
  lt[,plot_num:=NULL]
  # set-up for outlier_path and plot_set
  lt[is.na(outlier),outlier:=3]
  lt <- add_plot_nums(lt)
  plots <- unique(lt,by='plot_num')
  plots[outlier==0, outlier_path:='non_outlier']
  plots[outlier==1, outlier_path:='outlier']
  plots[outlier>1, outlier_path:='unknown']
  plots$ran_num <- runif(nrow(plots),0,1)
  plots[outlier_path!='unknown', plot_set:=ifelse(ran_num>0.2,"train","test")]
  plots[outlier_path=="unknown", plot_set:="predict"]
  plots <- plots[,c('plot_num','plot_set','outlier_path')]
  lt <- merge(lt,plots,by='plot_num')

  print(table(plots$outlier, useNA='ifany'))
  print(table(plots$outlier_path, useNA='ifany'))
  print(table(plots$plot_set, useNA='ifany'))

  plot_for_machine_vision(lt,
                          "FILEPATH/empirical_lt/plot-dir")
}

wf_05_29_19 <- function(){
  lt <- get_life_table(version=157, smooth_widths=c(1,3,5,7))
  lt <- ids_to_names(lt)
  lt <- add_plot_nums(lt)
  lt[,plot_set:="predict"]
  lt[,outlier_path:="unknown"]
  plot_for_machine_vision(lt,
                          "FILEPATH/empirical_lt/plot-dir")

}

wf_09_02_19 <- function(){
  lt <- get_life_table("recent", smooth_widths=c(1,3,5,7))
  lt <- lt[ihme_loc_id %like% "USA"]
  lt <- ids_to_names(lt)
  # only keep LTs where all smooth widths are outliered
  lt[, included := ifelse(life_table_category_name == "outlier", 0, 1)]
  lt[, count := sum(included), by = c("age_group_id", "year_id", "ihme_loc_id", "sex_id", "source_name")]
  lt <- lt[count == 0]
  # plot for vetting
}

# END
