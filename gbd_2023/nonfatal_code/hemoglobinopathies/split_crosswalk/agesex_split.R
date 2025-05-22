#gets path to j and h drives depending on operating system
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}


library('openxlsx')
library('dplyr')
library('data.table')
library('matrixStats')
library('ggplot2')
library('msm')
'%ni%' <- Negate('%in%')

### change to your own repo path if necessary CHANGES THIS LATER
invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))


grid_arrange_shared_legend <- function(..., ncol = length(list(...)), nrow = 1, position = c("bottom", "right"), title = title) {
  library('gridExtra')
  library('grid')
  plots <- list(...)
  position <- match.arg(position)
  g <- ggplotGrob(plots[[1]] + theme(legend.position = position))$grobs
  legend <- g[[which(sapply(g, function(x) x$name) == "guide-box")]]
  lheight <- sum(legend$height)
  lwidth <- sum(legend$width)
  gl <- lapply(plots, function(x) x + theme(legend.position="none"))
  gl <- c(gl, ncol = ncol, nrow = nrow)
  
  combined <- switch(position,
                     "bottom" = arrangeGrob(do.call(arrangeGrob, gl),
                                            legend,
                                            ncol = 1,
                                            heights = unit.c(unit(1, "npc") - lheight, lheight)),
                     "right" = arrangeGrob(do.call(arrangeGrob, gl),
                                           legend,
                                           ncol = 2,
                                           widths = unit.c(unit(1, "npc") - lwidth, lwidth)))
  
  grid.newpage()
  grid.draw(combined)
  
  # return gtable invisibly
  invisible(combined)
  
}

create_pre_diagnostics <- function(bun_data, bun_id, measure_name, outdir, release_id) {
  ### Diagnostic 1- original standard error to mean
  loc_meta <- get_location_metadata(release_id = release_id, location_set_id = 9)
  bun_data_plot <- bun_data[, .(location_id, sex, mean, standard_error, age_start, age_end, nid,
                                is_outlier, clinical_data_type)]
  bun_data_plot <- merge(bun_data_plot, loc_meta[, .(location_id, super_region_name, location_name)], by = 'location_id')
  bun_data_plot <- bun_data_plot[is_outlier == 1, outlier_name := 'Outliered']
  bun_data_plot <- bun_data_plot[is_outlier == 0, outlier_name := 'Not outliered']
  
  bun_data_plot <- bun_data_plot[, se_rel_mean := standard_error/mean]
  bun_data_plot <- bun_data_plot[se_rel_mean <1, se_mean_ratio:= "<=1"]
  bun_data_plot <- bun_data_plot[se_rel_mean >=1 & se_rel_mean <5, se_mean_ratio:= "1-5"]
  bun_data_plot <- bun_data_plot[se_rel_mean >=5 & se_rel_mean <10, se_mean_ratio:= "5-10"]
  bun_data_plot <- bun_data_plot[se_rel_mean >=10 & se_rel_mean <15, se_mean_ratio:= "10-15"]
  bun_data_plot <- bun_data_plot[se_rel_mean >=15, se_mean_ratio:= "15+"]
  bun_data_plot <- bun_data_plot[clinical_data_type %ni% c('inpatient', 'claims'), clinical_data_type := 'Not clinical']
  
  bun_data_plot$se_mean_ratio <- factor(bun_data_plot$se_mean_ratio, levels = c("<=1", "1-5", "5-10", "10-15", "15+"))
  
  library(scales)
  
  diagnostic_1 <- bun_data_plot %>% ggplot() +
    geom_point(data= bun_data_plot, aes(x = mean, y = standard_error,
                                        color = se_mean_ratio,
                                        shape = as.factor(is_outlier),
                                        text =  paste('mean: ', prettyNum(signif(bun_data_plot$mean, digits = 3)),
                                                      'standard_error: ', prettyNum(signif(bun_data_plot$standard_error, digits = 3)),
                                                      '<br> Location: ', bun_data_plot$location_name,
                                                      'Clinical data?: ', bun_data_plot$clinical_data_type))) +
    labs(x = "Mean", y = "Standard error", shape = 'Outlier', color = 'Standard error/mean') +
    scale_shape_manual(values=c(16, 4))+
    # scale_color_brewer(palette="Blues")+
    ggtitle(paste0("Raw mean vs Standard Error, bundle ", bun_id)) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    geom_abline()+
    scale_x_log10(breaks = trans_breaks("log10", function(x) 10^x),
                  labels = trans_format("log10", math_format(10^.x))) +
    scale_y_log10(breaks = trans_breaks("log10", function(x) 10^x),
                  labels = trans_format("log10", math_format(10^.x))) +
    theme(text = element_text(size = 10, color = "black")) +
    facet_grid(super_region_name~clinical_data_type, labeller = label_wrap_gen(width=60))
  
  
  pdf(file = paste0(outdir, "diag1_", bun_id,"_scatter_raw_mean_v_se.pdf"),
      width = 12, height = 8)
  print(diagnostic_1)
  dev.off()
  
}


create_post_diagnostics <- function(bun_id, measure_name, outdir, release_id, new_split, map) {
  
  bun_metadata <- map[bundle_id == bun_id]
  
  loc_meta <- get_location_metadata(location_set_id = 9, release_id = release_id)
  
  new_split$orig_sex_id <- as.factor(new_split$orig_sex_id)
  new_split[, orig_age_range := paste0(orig_age_start, '-', orig_age_end)]
  new_split[, se_relative_size := orig.standard_error / orig.mean]
  new_split[se_relative_size > 1, ratio_se_to_mean := 'SE/mean > 1']
  new_split[se_relative_size <= 1, ratio_se_to_mean := 'SE/mean <= 1']
  new_split <- merge(new_split, loc_meta, by = 'location_id')
  new_split[, log.orig.mean := log(orig.mean)]
  new_split[, log.reaggregated := log(reagg_mean)]
  new_split[orig.mean == 0, `:=` (reagg_mean = mean,
                                  log.reaggregated = 0, 
                                  log.orig.mean = 0)]
  new_split[is.nan(log.reaggregated), `:=` (log.reaggregated = log(abs(reagg_mean)))]
  
  new_split[is.na(clinical_data_type) | clinical_data_type == '', clinical_data_type := 'Not clinical']
  new_split$is_outlier <- as.factor(new_split$is_outlier)
  new_split$clinical_data_type <- as.factor(new_split$clinical_data_type)
  
  
  pdf(file = paste0(outdir, "diag2_",bun_id,"_scatter_original_v_reaggregated.pdf"),
      width = 12, height = 8)
  ## Difference in Original and Reaggregated Mean (Linear Space)
  gg <- ggplot(data = new_split, aes(x = orig.mean, y = reagg_mean, alpha = 0.2, 
                                     color = se_relative_size,
                                     text = paste0('seq: ',crosswalk_parent_seq, ', ',
                                                   location_name, ', reagg mean: ', round(reagg_mean,6),
                                                   ', orig mean: ', round(orig.mean,6),
                                                   ', orig se: ', round(orig.standard_error,6)))) +
    geom_point() +
    labs(title = paste0('Difference in Original and Reaggregated Mean, Bundle: ', bun_metadata$bundle_name, " (", bun_id,")"),
         x = 'Original Mean',
         y = 'Mean Reaggregated from Split Data') +
    geom_abline(slope = 1, alpha = 0.1) +
    theme_bw()
  print(gg)
  #ggplotly(p = gg, tooltip = 'text')
  
  ## Difference in Original and Reaggregated Mean (Log Space)
  gg1 <- ggplot(data = new_split, aes(x = log.orig.mean, y = log.reaggregated, alpha = 0.2, 
                                      color = se_relative_size,
                                      text = paste0('seq: ',crosswalk_parent_seq, ', ',
                                                    location_name, ', reagg mean: ', round(reagg_mean,6),
                                                    ', orig mean: ', round(orig.mean,6),
                                                    ', orig se: ', round(orig.standard_error,6)))) +
    geom_point() +
    labs(title = paste0('Difference in Original and Reaggregated Mean (Log Space), Bundle: ', bun_metadata$bundle_name, " (", bun_id,")"),
         x = 'Original Mean',
         y = 'Mean Reaggregated from Split Data') +
    geom_abline(slope = 1, alpha = 0.1) +
    theme_bw()
  print(gg1)
  #ggplotly(p = gg1, tooltip = 'text')
  
  
  ## Ratio of reagg:orig on x axis and coeff of variation (se/mean)
  new_split <- new_split[, coeff_var := standard_error/reagg_mean]
  # View(new_split[orig.mean/reagg_mean >= .9, .(coeff_var, (orig.mean/reagg_mean))])
  mean_coeff_var_at90 <- round(mean(new_split[orig.mean/reagg_mean >= .9]$coeff_var), digits=3)
  mean_coeff_var_at95 <- round(mean(new_split[orig.mean/reagg_mean >= .95]$coeff_var), digits=3)
  coeff_var_dt <- data.table(average_cv_90 = mean_coeff_var_at90, average_cv_95 = mean_coeff_var_at95, bundle = bun_id)
  write.csv(coeff_var_dt,file = paste0(outdir, bun_id,"_", measure_name,"_coeff_var_values.csv"), row.names=F )
  
  # coeff_var_90 <- round(quantile(new_split$coeff_var, probs = c(.9),na.rm=T), digits = 3) 
  
  caption <- paste0('*', nrow(new_split[clinical_data_type == 'claims']), ' claims rows, ', 
                    nrow(new_split[clinical_data_type == 'inpatient']), ' inpatient rows, ',
                    nrow(new_split[clinical_data_type == 'Not clinical']), ' Non clinical rows')
  caption2 <- paste0('** Original mean: Reagg mean >= .9, average coefficient of variation: ', mean_coeff_var_at90)
  title <- gsub('(.{1,60})(\\s|$)', '\\1\n', bun_metadata$bundle_name)
  
  gg2 <- ggplot(data = new_split, aes(x = orig.mean/reagg_mean, y = coeff_var, alpha = 0.2, 
                                      color = clinical_data_type,
                                      shape = is_outlier,
                                      text = paste0('seq: ',crosswalk_parent_seq, ', ',
                                                    location_name, ', reagg mean: ', round(reagg_mean,6),
                                                    ', orig mean: ', round(orig.mean,6),
                                                    ', orig se: ', round(orig.standard_error,6)))) +
    geom_point() +
    scale_shape_manual(values=c(16,4))+
    labs(title = title,
         subtitle='Ratio of reagg:orig mean vs. Coefficent of variation',
         x = 'Original Mean/Reaggregated Mean',
         y = 'Coefficient of variation (split SE/split mean)',
         color = 'Difference in Orig and Reagg Mean (%)',
         shape = 'Outlier',
         caption = '') +
    geom_abline(slope = 1, alpha = 0.1) +
    theme(text = element_text(size = 10, color = "black"))
  # print(gg2)
  
  ## Ratio of reagg:orig on x axis and coeff of variation (se/mean) -- LOG
  gg3 <- ggplot(data = new_split, aes(x = orig.mean/reagg_mean, y = coeff_var, alpha = 0.2, 
                                      color = clinical_data_type,
                                      shape = is_outlier,
                                      text = paste0('seq: ',crosswalk_parent_seq, ', ',
                                                    location_name, ', reagg mean: ', round(reagg_mean,6),
                                                    ', orig mean: ', round(orig.mean,6),
                                                    ', orig se: ', round(orig.standard_error,6)))) +
    geom_point() +
    scale_shape_manual(values=c(16,4))+
    # scale_x_log10()+
    scale_y_log10()+
    labs(title = '',
         subtitle='Ratio of reagg:orig mean vs. Coefficent of variation (log y scale)',
         x = 'Original Mean/Reaggregated Mean',
         y = 'Coefficient of variation (split SE/split mean) - log',
         color = 'Clinical data type',
         shape = 'Outlier',
         caption = paste0(caption, '\n', caption2)) +
    geom_abline(slope = 1, alpha = 0.1) +
    theme(text = element_text(size = 10, color = "black"))
  
  
  # print(gg3)
  #print(grid_arrange_shared_legend(gg2, gg3, ncol = 2, nrow = 1, position = 'right'))
  invisible(dev.off())
  
  
  #also output a list of data points where the reagg means are more than 20% different
  #than the original input means
  newer_split <- new_split[mean_diff_percent > 20]
  write.xlsx(newer_split[order(-mean_diff_percent)],
             file = paste0(outdir, bun_id,"_", measure_name,"_large_diffs_reaggregation.xlsx"))
  
  append_pdf <- function(dir, starts_with) {
    files <- list.files(dir, pattern = paste0("^", starts_with), full.names = T)
    files <- paste(files, collapse = " ")
    cmd <- paste0("/usr/bin/ghostscript -dBATCH -dSAFER -dNOGC -DNOPAUSE -dNumRenderingThreads=4 -q -sDEVICE=pdfwrite -sOutputFile=",
                  dir, "/", starts_with, "_full.pdf ", files)
    system(cmd)
  }
  
  
  append_pdf(dir=outdir, starts_with = 'diag')
  
}

# Source functions and set up
user <- Sys.getenv("USER")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
'%ni%' <- Negate('%in%')


stop_quietly <- function() {
  opt <- options(show.error.messages = FALSE)
  on.exit(options(opt))
  stop()
}

##---------------------------------------------------------------------------------------------------------------
adjust_under1 <- function(df, bundle_id, age_group_set_id, release_id, old_release_id){
  
  # Format data 
  df <- data.table(df)
  df[,index := 1:.N] # Create index reference variable in full data set
  under1_data <- df[clinical_data_type=="inpatient" & age_end <= 1 & age_start != 1, 
                    .(age_start,age_end,sex,location_id,year_start,year_end,nid,mean,index)] # Subset to <1 inpatient data for adjustment
  
  if (nrow(under1_data)==0){
    print("There is no under-1 inpatient data in your data set!")
    return(df)
  }
  
  # Merge on age and population
  under1_data[, year_id := floor((year_start + year_end)/2)] # create a reference year_id for population 
  under1_data[, sex_id := ifelse(sex=="Male",1,2)] # create sex_id for easier use
  
  ages <- get_age_metadata(age_group_set_id = age_group_set_id, release_id = release_id)[,.(age_group_id,age_group_years_start)]
  pops <- get_population(age_group_id=c(2,3,388,389,164), location_id=unique(under1_data$location_id), 
                         year_id=unique(under1_data$year_id), sex_id=c(1,2), release_id = old_release_id)[,-c("run_id")]
  under1_data <- merge(under1_data,ages,by.x="age_start",by.y="age_group_years_start",all.x=TRUE)
  under1_data[age_start==0 & age_end==0, age_group_id:=164] # fix for birth prevalence because age_metadata doesn't return age group
  under1_data <- merge(under1_data,pops,by=c("location_id","age_group_id","year_id","sex_id"),all.x=TRUE)
  
  under1_data <- under1_data[,`:=` (age_start=NULL, age_end=NULL, sex=NULL, year_start=NULL, year_end=NULL)]
  
  
  # Transform data to count space and save index info
  under1_data[, mean:=mean*population]
  ref <- under1_data[,.(location_id,age_group_id,sex_id,year_id,nid,index,population)]
  
  # Cast wide by age
  under1_data <- dcast(under1_data, location_id + sex_id + year_id + nid ~ age_group_id, value.var="mean")
  # data <- dcast(data, ... ~ age_group_id, value.var="mean")
  
  # Check for no missing ages
  if (nrow(under1_data[is.na("2") | is.na("3") | is.na("388") | is.na("389")]) > 0){
    print(paste0("Error: Dataset is not square by under-1 age groups! Check for rows missing some of the under-1 age groups 
          at FILEPATH"))
    next
  }
  
  under1_data <- as.data.table(under1_data)
  # Make the under-1 cohort adjustment
  under1_data[, `:=` (`2` = `2` + ((1/3)*`3`) + ((1/(5/12*52))*`388`) + ((1/(6/12*52))*`389`),
                      `3` = `3` + ((3/(5/12*52))*`388`) + ((3/(6/12*52))*`389`),
                      `388` = `388` + ((5/6)*`389`))]
  under1_data[, `164` := `2`*52]
  
  # Melt down by age
  under1_data <- melt(under1_data, id.vars=c("location_id","sex_id","year_id","nid"),
                      variable.name="age_group_id",value.name="mean")
  
  # Merge index and pop back on
  under1_data[,age_group_id := as.numeric(levels(age_group_id))[age_group_id]] # revert from factor
  under1_data <- merge(under1_data,ref,by=c("location_id","age_group_id","sex_id","year_id","nid"),all.x=TRUE)
  
  # Revert to rate space
  under1_data[, mean := mean/population]
  
  # Merge on to original data
  under1_data <- under1_data[,.(index,mean)]
  setnames(under1_data,"mean","mean_u1_adjust")
  
  df[, which(duplicated(names(df))) := NULL]
  df <- merge(df,under1_data,by="index",all.x=TRUE)
  
  plot_df <- df[!is.na(mean_u1_adjust), .(location_id,age_start,age_end,sex,mean,mean_u1_adjust)]
  
  
  # Replace with new vals and clean up columns
  df[!is.na(mean_u1_adjust), mean := mean_u1_adjust]
  df[!is.na(mean_u1_adjust), lower := NA] #let uploader calculate 
  df[!is.na(mean_u1_adjust), upper := NA] #let uploader calculate 
  df[!is.na(mean_u1_adjust) & !is.na(cases), cases := mean*sample_size]
  df[, `:=` (index=NULL, mean_u1_adjust=NULL)]
  
  # Write-out
  output_dir <- paste0("FILEPATH",bundle_id)
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive=TRUE)
  
  return(df)
}

## END
##---------------------------------------------------------------------------------------------------------------

create_birth_lit <- function(df, bundle_id, age_group_set_id, release_id, diagnostics=TRUE){
  orig_df <- copy(df)
  orig_nrows <- nrow(orig_df)
  print(paste0("Original df had ", orig_nrows, " rows"))
  # Format data
  df <- data.table(df)
  df[,index := 1:.N] # Create index reference variable in full data set
  print(unique(df$cv_literature))
  data <- df[cv_literature == 1 & age_end <= (7/365),
             .(age_start,age_end,sex,location_id,site_memo,year_start,year_end,nid,mean,lower,upper,index)] # Subset to only <= ENN 0-6 days lit data for adjustment
  
  index_vals_all_ENN_and_birth <- data$index
  
  if (nrow(data[age_end != 0])==0) {
    #print("There is no ENN lit data in your data set!")
    return(orig_df)
  }else{
    # Merge on age and population
    data[, year_id := floor((year_start + year_end)/2)] # create a reference year_id for population
    data[, sex_id := ifelse(sex=="Male",1,2)] # create sex_id for easier use
    ages <- get_age_metadata(age_group_set_id = age_group_set_id, release_id= release_id)[,.(age_group_id,age_group_years_start)]
    data <- merge(data,ages,by.x="age_start",by.y="age_group_years_start",all.x=TRUE)
    data[age_start==0 & age_end==0, age_group_id:=164] # fix for birth prevalence because age_metadata doesn't return age group
    
    
    data <- data[,`:=` (age_start=NULL, age_end=NULL, sex=NULL, year_start=NULL, year_end=NULL)]
    
    # Cast wide by age
    data <- dcast(data, ... ~ age_group_id, value.var=c("mean", "lower", "upper"))
    if (is.null(data$mean_164) | is.null(data$mean_164)){
      return(orig_df)
    }
    
    # Create birth rows if an ENN row exists but a birth point does not for a given loc/sex/nid/site memo
    nrows_to_make <- nrow(data[is.na(mean_164) & !(is.na(mean_2))])
    index_vals_to_duplicate <- data[is.na(mean_164) & !(is.na(mean_2))]$index # Find the index vals of rows we need to duplicate in original df and set to birth
    
    print(paste0("Creating ", nrows_to_make, " rows of birth data from ENN data"))
    ENN_duplicated <- df[index %in% index_vals_to_duplicate]
    ENN_duplicated$age_end <- 0
    df_copy_birth <- copy(ENN_duplicated)
    ENN_duplicated <- ENN_duplicated[, c('mean', 'lower', 'upper', 'cases', 'sample_size') := NA] #setting to NA because will fill in later
    df <- rbind(df, ENN_duplicated) # appending new birth rows to original df
    
    if (nrow(df) != orig_nrows + nrows_to_make){
      print(paste0("Error, your new df has the incorrect number of rows"))
      break
    }else{
      print(paste0("New df has ", nrow(df), " rows, equal to ", orig_nrows, " orig rows + ", nrows_to_make))
    }
    
    df_copy_birth <- df_copy_birth[,.(index,mean,lower,upper, age_end)]
    setnames(df_copy_birth,c("mean","lower","upper"),c("mean_birth_adjust","lower_birth_adjust","upper_birth_adjust"))
    
    new <- merge(df,df_copy_birth,by=c("index","age_end"),all=TRUE)
    
    if (nrow(new) != nrow(df)){
      print(paste0("Error, your new df has the incorrect number of rows"))
      break
    }else{
      print(paste0("New df has ", nrow(new), " rows, equal to ", orig_nrows, " orig rows + ", nrows_to_make))
    }
    
    #subset to data for diagnostic
    duplicated <- new[index %in% index_vals_to_duplicate, .(location_id,age_start,age_end,sex,mean,mean_birth_adjust)] #birth data duplicated from ENN
    duplicated_birth <- duplicated[age_end == 0]
    duplicated_birth[, mean := mean_birth_adjust]
    duplicated_birth$mean_birth_adjust <- NULL
    duplicated_birth$variable <- 'Duplicated from ENN'
    
    non_dupe <- new[index %in% index_vals_all_ENN_and_birth & index %ni% index_vals_to_duplicate & age_end ==0, .(location_id,age_start,age_end,sex,mean,mean_birth_adjust)] # non_dupe ENN and birth data
    non_dupe$variable <- 'Non duplicated data'
    non_dupe <- non_dupe[age_end == 0 & is.na(mean), mean := mean_birth_adjust]
    non_dupe$mean_birth_adjust <- NULL
    
    all <- new[index %in% index_vals_all_ENN_and_birth & age_end ==0, .(location_id,age_start,age_end,sex,mean,mean_birth_adjust)] # all ENN and birth data
    all$variable <- 'All birth lit data*'
    all <- all[is.na(mean), mean := mean_birth_adjust]
    all$mean_birth_adjust <- NULL
    
    
    plot_df <- rbind(duplicated_birth, non_dupe, all, fill = T)
    
    
    # Generate diagnostics if specified
    if (diagnostics){
      #Job specifications
      username <- Sys.getenv("USER")
      m_mem_free <- "-l m_mem_free=10G"
      fthread <- "-l fthread=4"
      runtime_flag <- "-l h_rt=04:00:00"
      jdrive_flag <- "-l archive"
      queue_flag <- "-q long.q"
      shell_script <- "-cwd FILEPATH -s "
      
      script <- paste0("FILEPATH/adjust_u1_diagnostics.R")
      errors_flag <- paste0("-e FILEPATH")
      outputs_flag <- paste0("-o FILEPATH")
      
      print(paste0("Launching diagnostics for bundle ",bundle_id))
      print("could not launch diagnostics - diagnostics script still under construction")
      job_name <- paste0("-N", " cascade_diagnostics_", bundle_id)
      job <- paste("qsub", m_mem_free, fthread, runtime_flag, jdrive_flag, queue_flag, job_name, "-P proj_nch", 
                   outputs_flag, errors_flag, shell_script, script, bundle_id, release_id) 
      
      system(job)
    }
    
    
    # Replace with new vals and clean up columns
    new[!is.na(mean_birth_adjust), mean := mean_birth_adjust]
    new[!is.na(mean_birth_adjust), lower := lower_birth_adjust] 
    new[!is.na(mean_birth_adjust), upper := upper_birth_adjust] 
    new[!is.na(mean_birth_adjust), uncertainty_type_value := 95] 
    new[!is.na(mean_birth_adjust), cases := NA] #let uploader calculate
    new[!is.na(mean_birth_adjust), sample_size := NA] #let uploader calculate
    new[, c('index', 'mean_birth_adjust', 'lower_birth_adjust', 'upper_birth_adjust') :=NULL]
    
    return(new)
  }
}
##---------------------------------------------------------------------------------------------------------------



### Suite of functions for use in do_dismod_split.R and do_agesex_split.R
#     - expand_test_data
#     - add_pops
#     - pull_model_weights
#     - gen_cases_sample_size
#     - gen_se
#     - calculate_cases_fromse
#     - get_upper_lower
#     - pull_bundle_data
#     - pool_across_years

####################################################################################


# subset out birth prevalence (age_start and age_end = 0) because it does not need age expansion,
# it only needs sex expansion
subset_out_prevalence_data <- function(df){
  birth_prev <- df[age_start == 0 & age_end == 0]
  birth_prev[, `:=` (agg_age_start = age_start, 
                     agg_age_end = age_end,
                     orig_age_start = age_start,
                     orig_age_end = age_end,
                     n.age = 1,
                     need_split = 0,
                     age_group_id = 164)]
  df <- df[age_end != 0]
  return(list(df, birth_prev))
}

split_age_group_if_needed <- function(df_row, age_map){
  # Find age_map's order where age_start is closest to but less than or equal to t's agg_age_start  
  # Find age_map's order where age_end is closest to but greater than t's agg_age_end 
  pre_expansion_data <- copy(df_row)
  order_start <- age_map[age_start == max(age_map[age_start_compute <= df_row$agg_age_start, age_start]), order]
  if (df_row$agg_age_end > 124) { df_row$agg_age_end <- 124 }
  order_end <- age_map[age_end_compute == min(age_map[age_end_compute > df_row$agg_age_end, age_end_compute]), order]
  df_row[, agg_age_end := orig_age_end]
  df_row <- cbind(df_row, age_map[order %in% order_start:order_end, .(age_start, age_end, age_group_id)])
  df_row[, n.age := length(order_start:order_end)]
  df_row[n.age == 1, `:=` (age_start = agg_age_start, age_end = agg_age_end)]
  df_row[length(order_start:order_end) == 1, need_split := 0]
  df_row[, orig_age_start := agg_age_start] # adding these orig columns because we use them later with the split data and want to be able to know the original data ages
  df_row[, orig_age_end := agg_age_end]
  df_row[need_split == 0, age_start := age_map[order == order_start, age_start]] # this is for the data that doesn't need splitting but is slightly off from the GBD age group
  df_row[need_split == 0, age_end := age_map[order == order_end, age_end]] # this is for the data that doesn't need splitting but is slightly off from the GBD age group
  df_row[length(order_start:order_end) > 1, need_split := 1]
  return(df_row)
}


get_age_map <- function(release_id, age_group_set_id){
  age_map       <- get_age_metadata(release_id=release_id, age_group_set_id=age_group_set_id)[, .(age_group_id, age_group_years_start, age_group_years_end)]
  age_map       <- age_map[order(age_group_years_start),] 
  age_map$order <- seq.int(nrow(age_map)) + 1
  setnames(age_map, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  age_map$age_start_compute <- age_map$age_start
  age_map$age_end_compute   <- round(age_map$age_end, 3)
  age_map$age_end_compute[age_map$age_end_compute < age_map$age_end] <-   age_map$age_end_compute[age_map$age_end_compute < age_map$age_end] + 0.001
  
  return(age_map)
}


expand_age <- function(df, age_map){
  df[, orig_age_end := age_end]
  

  df[age_demographer == 1 & age_end <1, age_demographer := 0] ## age demographer must always be 1 for under 1 ages 
  if (nrow(df[age_demographer == 0 & age_start == age_end & age_end >= 1]) >0){
    message('You have rows that have been incorrectly extracted based on age demographer.')
  }
  df[age_demographer == 0 & age_end > 1,`:=`(age_demographer=1, age_end = age_end - 1)] ## for over 1 data where age_demographer ==0, change age_demographer to 1 and subtract 1 to get all data in "normal" age notation

  
  setnames(df, c("age_start", "age_end"), c("agg_age_start", "agg_age_end"))
  
  df <- split(df, by = "split.id")
  
  
  df <- lapply(df, split_age_group_if_needed, age_map=age_map)
  df <- rbindlist(df)
  
  return(df)
}

expand_sex <- function(df, birth_prev){
  df <- rbind(df, birth_prev, fill = TRUE)
  
  df[, sex_split_id := paste0(split.id, "_", age_start)]
  
  sex_specific <- df[n.sex == 1]
  sex_specific[, sex_id := agg_sex_id]
  df <- df[n.sex == 2]
  df[, need_split := 1]
  
  pre_expansion_data <- copy(df)
  expanded <- rep(df[,sex_split_id], df[,n.sex]) %>% data.table("sex_split_id" = .)
  df <- merge(expanded, df, by="sex_split_id", all=T)
  
  if (nrow(df[agg_sex_id ==3]) > 0) {
    df <- df[agg_sex_id==3, sex_id := 1:.N, by=sex_split_id]
  }
  
  df$sex_id <- as.double(df$sex_id)
  df[is.na(sex_id), sex_id:=agg_sex_id]
  
  df <- rbind(df, sex_specific, use.names = TRUE, fill = TRUE)
  return(df)
}

check_sex_restrictions <- function(df, bun_id){
  # hard-code sex restrictions for Turner (437 - should only be females) and Kleinfelter (438 - should only be males)
  if (bun_id == 437) {
    df[sex_id == 1, cases := 0]
  }
  if (bun_id == 438) {
    df[sex_id == 2, cases := 0]
  }
  return(df)
}

expand_age_and_sex <- function(df, release_id, bun_id, age_group_set_id){
  df$sex_id <-1
  df$sex_id[df$sex=="Female"]<-2
  df$sex_id[df$sex=="Both"]<-3
  setnames(df, c("sex_id"), c("agg_sex_id"))
  
  df$n.sex = ifelse(df$agg_sex_id==3, 2, 1)
  
  df$split.id <- seq.int(nrow(df))
  
  birth_prev_and_after_dfs <- subset_out_prevalence_data(df)
  df         <- birth_prev_and_after_dfs[[1]]
  birth_prev <- birth_prev_and_after_dfs[[2]]
  
  age_map <- get_age_map(release_id, age_group_set_id)
  
  df <- expand_age(df, age_map)
  df <- expand_sex(df, birth_prev)
  df <- check_sex_restrictions(df, bun_id)
  
  return(df)
}

####################################################################################
add_pops <- function(expanded, release_id, old_release_id){
  
  #pull populations for every year and location of data that need to be split
  pops <- get_population(year_id = unique(expanded$est_year_id), sex_id = c(1,2), 
                         location_id = unique(expanded$location_id), 
                         age_group_id = unique(expanded$age_group_id),
                         release_id = old_release_id) ##needs to temporarily be old release until 2021 is fixed
  
  pops$run_id <- NULL
  setnames(pops, 'year_id', 'est_year_id')
  
  #round age_start and age_end for neonatal in the aggregate dataset, in preparation for the merge
  expanded$population <- NULL ## make sure there is no population column in the original dataset that we are about to merge populations onto
  expanded <- merge(expanded, pops, by = c("age_group_id", "sex_id", "location_id", "est_year_id"),
                    all.x = TRUE)
  
  #' Create an expand ID for tracking the draws. 1000 draws for each expand ID
  expanded[,expand.id := .I]
  
  # Split ID is related to each orignial aggregated test data point. So add up the population of each individual
  #' group within each split ID. That is the total population of the aggregated age/sex group
  expanded[, pop.sum := sum(population), by = split.id]
  
  return(expanded)
  
  print("Expanding done")
}
####################################################################################

####################################################################################

pull_model_weights <- function(expanded, model_id, measure_name, old_release_id, me_id){
  if (measure_name == "prevalence"){
    measure_id = 5
  } else if (measure_name == "continuous"){
    measure_id = 19
  }else if (measure_name == "indidence") {
    measure_id = 6
  }else if (measure_name == "proportion") {
    measure_id = 18
  }else if (measure_name == "mtexcess"){
    measure_id = 9
  }else {
    print(paste0("measure name ", measure_name, " is not recognized"))
    stop_quietly()
  }

  wt_data <- get_draws(gbd_id_type = 'modelable_entity_id', 
                       gbd_id=me_id, 
                       source='epi',
                       measure_id = measure_id, 
                       location_id = unique(expanded$location_id),
                       sex_id = unique(expanded$sex_id), 
                       year_id = unique(expanded$est_year_id),
                       #age_group_id=unique(expanded$age_group_id),
                       release_id = old_release_id)
  
  if (nrow(wt_data)==0) { 
    print(paste("ME ID",model_id,"does not have DisMod results for",measure_name))
    stop(paste("ME ID",model_id,"does not have DisMod results for",measure_name))
  }
  
  wt_data[, c("measure_id", "metric_id", 'model_version_id', 'modelable_entity_id') := NULL]
  
  setnames(wt_data, 'year_id', 'est_year_id')
  
  return(wt_data)
}
####################################################################################

####################################################################################
## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[(is.na(standard_error) | standard_error == 0) & is.numeric(lower) & is.numeric(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[(is.na(standard_error) | standard_error == 0) & (measure == "prevalence" | measure == "proportion"),
     standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

get_upper_lower <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[!is.na(mean) & !is.na(standard_error) & is.na(lower), lower := mean - (1.96 * standard_error)]
  dt[!is.na(mean) & !is.na(standard_error) & is.na(upper), upper := mean + (1.96 * standard_error)]
  return(dt)
}
####################################################################################

####################################################################################
pull_bundle_data <- function(measure_name, bun_id, bun_data, cause_root_path, release_id){
  bun_data_other_measure <- bun_data[measure != measure_name]
  bun_data <- bun_data[measure == measure_name]
  bun_data <- bun_data[!(nid == 135199 & sex == "Both")]
  bun_data <- bun_data[location_id %in% c(44551, 44547, 44553, 44541, 44549, 44550, 44545, 44546, 44552, 44548, 44542, 44543, 44544), location_id := 152]
  
  ### Fill in any blank values in mean, sample size, or cases, based on the available values
  bun_data[!is.numeric(standard_error), standard_error := as.numeric(standard_error)]
  bun_data[!is.numeric(upper), upper := as.numeric(upper)]
  bun_data[!is.numeric(lower), lower := as.numeric(lower)]
  bun_data <- get_cases_sample_size(raw_dt = bun_data)
  bun_data <- get_se(raw_dt = bun_data)
  bun_data <- calculate_cases_fromse(raw_dt = bun_data)
  bun_data <- get_upper_lower(raw_dt = bun_data)
  return(list(bun_data,  bun_data_other_measure))
}

####################################################################################

####################################################################################



#*******************************************************************************
#*******************************************************************************
#* Description: This function gets the path to where data for the cause being age-sex split is
#* stored.
get_cause_root_path <- function(bun_id, map, measure_name){ 
  cause_root_path <- "/share/mnch/"
  bun_type <- map[(map$bundle_id==bun_id), type]
  born_this_way = c("congenital", "hemoglobinopathies")
  
  if (is.element(bun_type, born_this_way)){
    cause_root_path <- paste0(cause_root_path, "BornThisWay/")
  }
  
  if (bun_type == "hemoglobinopathies"){
    cause_root_path <- paste0(cause_root_path, "Hemog/")
  }
  else if (bun_type == "congenital"){
    cause_root_path <- paste0(cause_root_path, "Congenital/")
  }
  else{
    cause_root_path <- paste0(cause_root_path, bun_type, "/")
  }
  
  if (dir.exists(cause_root_path)){
    return(cause_root_path)
  }
  else{
    print("ERROR: not an mnch bundle type")
    stopifnot(dir.exists(cause_root_path))
  }
}
#*******************************************************************************
#*******************************************************************************


#*******************************************************************************
#*******************************************************************************
#* This function checks for missing values, (eg. standard error) and calculate 
#* them based on available columns.
#* It also subsets the data down to just the measure that was supplied as an 
#* argument
handle_missing_values <- function(df){
  df_nas <- df[is.na(mean)]
  df <- df[!is.na(mean)]
  return (list(df, df_nas))
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
#* Deal with duplicates - collapse rows from a single source/age/loc/year (e.g. 
#* only difference is site memo or case name)
handle_duplicates <- function(df, source_cols){
  cols <- colnames(df)
  cols <- cols[!cols %in% source_cols]
  df <- df[, cases := sum(cases), by = cols]
  df <- unique(df, by = c(cols, 'cases'))
  return(df)
}
#*******************************************************************************
#*******************************************************************************


#*******************************************************************************
#*******************************************************************************
get_me_id <- function(bun_id, map){
  meid_colname <- 'me_id'
  if(map[(map$bundle_id==bun_id), type] %in% c('cgf', 'lbwsg', 'breastfeeding')){
    meid_colname <- paste0("age_sex_specific_", meid_colname)
  } 
  me_id <- map[bundle_id == bun_id, meid_colname, with=FALSE]
  return(me_id)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
### Drop any location data where the location_id is not in the model location set.
remove_invalid_locs <- function(df, type, release_id, location_set_id, silent){
  model_locs <- get_location_metadata(location_set_id=location_set_id, 
                                      release_id=release_id)
  
  invalid_loc_bun_data <- df[location_id %ni% model_locs$location_id]
  df <- df[location_id %in% model_locs$location_id]
  if (!silent) {print(paste0("Removed NIDs ", unique(invalid_loc_bun_data$nid), " from bun data bc of invalid locs ", unique(invalid_loc_bun_data$location_id)))}
  
  return(df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
handle_restrictions <- function(df,
                                restricted_col,
                                restricted_col_accpetable_values,
                                change_or_delete){
  df <- as.data.frame(df)
  if (change_or_delete == "change"){
    df[, restricted_col] <- restricted_col_accpetable_values[1]
  }
  if (change_or_delete == "delete"){
    df <- df[(df[, restricted_col] %in% restricted_col_accpetable_values), ]
  }
  df <- as.data.table(df)
  return(df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
# Subset out rows (currently Nigeria DHS subnats) to append back in later, 
# we aren't age splitting these rows, DisMod will do it

handle_dismod_split_rows <- function(bun_data, nosplit_bun_ids, 
                                     nosplit_country_prefix, nosplit_extractor){
  nosplit_df=data.frame(matrix(ncol = 0, nrow = 0))
  if (bun_data$bundle_id[1] %in% nosplit_bun_ids){
    nosplit_df <- bun_data[ihme_loc_id %like% nosplit_country_prefix & 
                             extractor == nosplit_extractor]
    nosplit_df <- nosplit_df[, age_start := 0.501]
    nosplit_df <- nosplit_df[sex == "Both", sex_id := 3]
    bun_data <- bun_data[!(ihme_loc_id %like% nosplit_country_prefix & 
                             extractor == nosplit_extractor)]
  }
  return(list(bun_data, nosplit_df))
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
# Expand each row into its constituent GBD age and sex groups. Subset off data
# that is already GBD-age and sex-specific, and therefore does not need to be
# split ("good_data")
expand_data <- function(df, release_id, measure_name, age_group_set_id, silent){
  
  expanded <- expand_age_and_sex(df, release_id, df$bundle_id[1], age_group_set_id)
  
  good_data <- expanded[need_split == 0]
  expanded <- fsetdiff(expanded, good_data, all = TRUE)
  if (!silent) {print(paste0("Expanded data to split has ", length(unique(expanded$nid)), " unique NIDs"))}
  
  if (nrow(expanded)==0) {
    good_data[, crosswalk_parent_seq := seq]
    good_data[, seq := '']
    good_data[, needed_split := 0]
  }
  good_data <- good_data[,c(names(df), 
                            'orig_age_start', 
                            'orig_age_end', 'age_group_id'),
                         with = FALSE]
  good_data[, crosswalk_parent_seq := seq]
  good_data[, seq := '']
  good_data[, needed_split := 0]
  expanded[, needed_split  := 1]
  return(list(expanded, good_data))
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
merge_with_pop_data <- function(df, release_id, old_release_id){
  #assign year as approx midpoint between year start and year end
  df$year_id <- floor((as.numeric(df$year_end)+as.numeric(df$year_start))/2)
  #round to nearest five years
  df$est_year_id  <- df$year_id - df$year_id%%5 + 5*(round(df$year_id%%5/5))
  #assign 1990 to all years prior to 1990
  df$est_year_id[df$est_year_id < 1990] <- 1990
  #assign 2019 to both 2018 and 2019
  df$est_year_id[df$year_id == 2018 | df$year_id == 2019] <- 2019
  #no need to use rounded year after 2019
  df$est_year_id[df$year_id > 2019] <- df$year_id[df$year_id > 2019]
  df <- add_pops(df, release_id = release_id, old_release_id = old_release_id)
  return(df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
transform_data <- function(df){
  #' logit transform the original data mean and se
  if (unique(df$measure) == 'continuous'){
    df$mean_transformed           <- log(1+df$mean)
  } else{
    df$mean_transformed <- log(df$mean / (1-df$mean))
  }
  df$standard_error_transformed <- sapply(1:nrow(df), function(i) {
    mean_i <- as.numeric(df[i, "mean"])
    se_i <- as.numeric(df[i, "standard_error"])
    if (df$measure == 'continuous'){
      return(deltamethod(~log(1+x1), mean_i, se_i^2))
    }else{
      return(deltamethod(~log(x1/(1-x1)), mean_i, se_i^2))
    }
  })
  df[is.infinite(mean_transformed), `:=` (mean_transformed = 0, standard_error_transformed = 0)]
  return(df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
merge_with_draws <- function(df, me_id, measure_name, old_release_id, silent, nas_okay){
  weight_draws <- pull_model_weights(df, 
                                     me_id, 
                                     measure_name, 
                                     old_release_id=old_release_id,
                                     me_id=me_id)
  
  if(!silent){print(paste0('Number of rows in dataset where draw_0 is NA: ',nrow(weight_draws[is.na(draw_0)])))}
  
  draws <- merge(df, 
                 weight_draws, 
                 by=c("age_group_id", "sex_id", "location_id", "est_year_id"),
                 all.x=TRUE)
  na_ind <- 0
  if(any(is.na(draws$draw_0)) & nas_okay==FALSE){
    print("!!!*******ERROR*******!!!")
    print("draws could not be created for all of the age/sex/location/year combinations in your data")
    print("make sure that the data available for your old_release_id is as specific as the data for your release_id")
    print("if you wish to ignore this error, call age sex splitting again with the argument nas_okay=TRUE")
    print("and NAs will be reported for any unmatched age/sex/location/year combination")
    na_ind <- 1
  }
  
  draws <- melt.data.table(draws, 
                           id.vars=names(draws)[!grepl("draw", names(draws))], 
                           measure.vars=patterns("draw"),
                           variable.name="draw.id", value.name = "model.result")
  draws$na_ind <- na_ind
  return(draws)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
sample_from_raw_input <- function(df){
  orig.data.to.split <- unique(df[, .(split.id, draw.id, mean_transformed, standard_error_transformed)])
  orig.data.to.split <- split(orig.data.to.split, by = "split.id")
  orig.data.to.split <- lapply(orig.data.to.split, function(input_i){
    mean.vector <- input_i$mean_transformed
    se.vector <- input_i$standard_error_transformed
    set.seed(123)
    input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
    input_i[, input.draw := input.draws]
    return(input_i)
  })
  orig.data.to.split <- rbindlist(orig.data.to.split)
  orig.data.to.split <- orig.data.to.split[,.(split.id, draw.id, input.draw)]
  df <- merge(df, 
              orig.data.to.split, 
              by = c('split.id','draw.id'))
  return(df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
get_case_counts_from_draws <- function(draw_df, expanded_df){
  #' Calculate count of cases for a specific age-sex-loc-yr
  #' based on the modeled prevalence
  draw_df[, numerator := model.result * population]
  #' Calculate count of cases across all the age/sex groups that cover an original aggregate data point,
  #' based on the modeled prevalence.
  #' (The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point)
  draw_df[, denominator := sum(numerator), by = .(split.id, draw.id)]
  #' Calculate the weight for a specific age/sex group as: model prevalence of specific age/sex group (logit_split) divided by
  #' model prevalence of aggregate age/sex group (logit_aggregate)
  if (unique(draw_df$measure) == 'continuous'){
    draw_df[, logit_split := log(1+model.result)]
    draw_df[, logit_aggregate := log(1+(denominator/pop.sum))]
  }else{
    draw_df[, logit_split := log(model.result / (1- model.result))]
    draw_df[, logit_aggregate := log( (denominator/pop.sum) / (1 - (denominator/pop.sum)))]
  }
  draw_df[, logit_weight := logit_split - logit_aggregate]
  #' Apply the weight to the original mean (in draw space and in logit space)
  draw_df[, logit_estimate := input.draw + logit_weight]
  draw_df[ ,estimate := logit_estimate]
  #' If the original mean is 0, or the modeled prevalance is 0, set the estimate draw to 0
  draw_df[is.infinite(logit_weight) | is.nan(logit_weight), estimate := 0]
  draw_df[, sample_size_new := sample_size * population / pop.sum]
  #' Save weight and input.draws in linear space to use in numeric check (recalculation of original data point)
  draw_df <- draw_df[, weight := model.result / (denominator/pop.sum)]
  # Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
  #' Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
  final_df <- draw_df[, .(mean.est = mean(estimate),
                          sd.est = sd(estimate),
                          upr.est = quantile(estimate, .975,  na.rm=TRUE),
                          lwr.est = quantile(estimate, .025,  na.rm=TRUE),
                          sample_size_new = unique(sample_size_new),
                          cases.est = mean(numerator),
                          orig.cases = mean(denominator),
                          orig.standard_error = unique(standard_error),
                          mean.input.draws = mean(input.draw),
                          mean.weight = mean(weight),
                          mean.logit.weight = mean(logit_weight)), by = expand.id] %>% merge(expanded_df, by = "expand.id")
  return(final_df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
log_to_linear <- function(log_df){
  #convert from log space to linear space
  linear_df <- log_df
  linear_df$sd.est   <- sapply(1:nrow(linear_df), function(i) {
    mean_i    <- as.numeric(linear_df[i, "mean.est"])
    mean_se_i <- as.numeric(linear_df[i, "sd.est"])
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
  })
  linear_df[, mean.est := exp(mean.est) / (1+exp(mean.est))]
  linear_df[, lwr.est  := exp(lwr.est) / (1+exp(lwr.est))]
  linear_df[, upr.est  := exp(upr.est) / (1+exp(upr.est))]
  linear_df[, mean.input.draws := exp(mean.input.draws) / (1+exp(mean.input.draws))]
  #' If the original mean is 0, recode the mean estimate to 0, and calculate the
  #'  adjusted standard error using Wilson's formula and the split sample sizes.
  linear_df[mean == 0, mean.est := 0]
  linear_df[mean == 0, lwr.est := 0]
  linear_df[mean == 0, mean.input.draws := NA]
  linear_df[mean == 1, mean.est := 1]
  linear_df[mean == 1, upr.est := 1]
  linear_df[mean == 1, mean.input.draws := NA]
  
  z <- qnorm(0.975)
  linear_df[(mean.est == 0 | mean.est == 1) & (measure == "prevalence" | measure == "proportion"),
            sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]

  linear_df[(mean.est == 0 | mean.est == 1) & measure == "incidence",
            sd.est := ((5-mean.est*sample_size_new)/sample_size_new+mean.est*sample_size_new*sqrt(5/sample_size_new^2))/5]
  linear_df[mean.est == 0, upr.est := mean.est + 1.96 * sd.est]
  linear_df[mean.est == 1, lwr.est := mean.est - 1.96 * sd.est]
  
  return(linear_df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
update_metrics <- function(df){
  df[, se.est := sd.est]
  df[, orig.sample.size := sample_size]
  df[, sample_size := sample_size_new]
  df[,sample_size_new:=NULL]
  
  df[, case_weight := cases.est / orig.cases]
  df$orig.cases <- NULL
  df$standard_error <- NULL
  setnames(df, c("mean", "cases"), c("orig.mean", "orig.cases"))
  setnames(df, c("mean.est", "se.est"), c("mean", "standard_error"))
  setnames(df, 'seq','crosswalk_parent_seq')
  df[, seq := '']
  
  df[, sex := ifelse(sex_id == 1, "Male", "Female")]
  df[, `:=` (lower = lwr.est, upper = upr.est,
             cases = mean * sample_size, effective_sample_size = NA)]
  df$cases.est <- NULL
  df[is.nan(case_weight), `:=` (mean = NaN, cases = NaN)]
  
  #setnames(df, c('agg_age_start','agg_age_end','agg_sex_id'),
  #         c('orig_age_start', 'orig_age_end', 'orig_sex_id'))
  df$agg_age_start <- NULL
  df$agg_age_end <- NULL
  setnames(df, c('agg_sex_id'), c('orig_sex_id'))
  
  return(df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
get_change_in_mean <- function(df){
  #recalculate the original mean as a check
  df[, reagg_mean := sum(cases) / sum(sample_size), by = .(split.id)]
  df[, mean_diff_percent := (abs(reagg_mean - orig.mean)/orig.mean) * 100]
  df[reagg_mean == 0 & orig.mean == 0, mean_diff_percent := 0]
  return(df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
get_split_data_only <- function(df){
  split_df <- df[, c('split.id', 'nid','crosswalk_parent_seq','age_start','age_end','sex_id','mean',
                     'standard_error','cases','sample_size',
                     'orig_age_start','orig_age_end','orig_sex_id', 'orig.mean',
                     'reagg_mean', 'mean_diff_percent','mean.input.draws',
                     'orig.standard_error','orig.cases','orig.sample.size',
                     'population','pop.sum',
                     'age_group_id','age_demographer','n.age','n.sex',
                     'location_id','est_year_id', 'year_start','year_end','is_outlier', 'clinical_data_type')]
  split_df <- split_df[order(split.id)]
  return(split_df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
get_age_summary <- function(df){
  age_summary <- df[n.age > 1, .N, by = .(orig_age_start, orig_age_end, age_demographer, n.age, age_start, age_end)]
  age_summary <- age_summary[order(orig_age_start)]
  return(age_summary)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
do_cascades <- function(df, release_id, cascade, lit_cascade, old_release_id, age_group_set_id){
  if (cascade == TRUE){
    df <- adjust_under1(df=df, bundle_id=df$bundle_id[1], age_group_set_id=age_group_set_id,
                        release_id=release_id, old_release_id=old_release_id)
  }
  
  if (lit_cascade == TRUE){
    df <- create_birth_lit(df=df,  bundle_id=df$bundle_id[1], age_group_set_id=age_group_set_id,
                           release_id=release_id, diagnostics=TRUE)
  }
  return(df)
}
#*******************************************************************************
#*******************************************************************************



#*******************************************************************************
#*******************************************************************************
reset_age_start_and_end_for_unsplit <- function(df){
  df <- df[needed_split == 0, age_start := orig_age_start]
  df <- df[needed_split == 0, age_end := orig_age_end]
  return(df)
}
#*******************************************************************************
#*******************************************************************************

process_bundle_data <- function(bundle_data, measure_name, bun_id, 
                                cause_root_path, outdir, release_id, source_cols, 
                                nosplit_bun_ids, nosplit_country_prefix, 
                                nosplit_extractor, 
                                bundles_restricted_to_birth_age, debug, silent){
  if (debug == TRUE){
    print(paste0("for debugging, reducing ", nrow(bundle_data), "to 500 rows"))
    bundle_data <- bundle_data[bundle_data$nid == 295194,]
  }
  
  bundle_data <- impute_missing_metrics(bundle_data)
  bundle_data_list <- pull_bundle_data(measure_name=measure_name, 
                                       bun_id=bundle_data$bundle_id[1], 
                                       bun_data=bundle_data, 
                                       cause_root_path=cause_root_path, 
                                       release_id)
  if (!silent) {print(paste0("Bundle version subset to ",measure_name, " only has ", length(unique(bundle_data$nid)), " unique NIDs"))}
  
  bundle_data               <- bundle_data_list[[1]]
  bundle_data_other_measure <- bundle_data_list[[2]]
  
  bundle_data_other_measure$lower[bundle_data_other_measure$lower < 0] <- 0
  bundle_data_other_measure$mean[bundle_data_other_measure$mean < 0] <- 0
  bundle_data_other_measure$urbanicity_type[bundle_data_other_measure$urbanicity_type == ""] <- "Unknown"
  bundle_data_other_measure$recall_type[bundle_data_other_measure$recall_type == ''] <- "Point"
  if (nrow(bundle_data_other_measure) > 0){bundle_data_other_measure[, 'unit_type'] <- "Person"}
  
  
  bun_data_and_nas <- handle_missing_values(bundle_data)
  bundle_data      <- bun_data_and_nas[[1]]
  bundle_data_nas  <- bun_data_and_nas[[2]]
  bundle_data      <- handle_duplicates(bundle_data, source_cols)
  if(!silent){print(paste0("After collapsing rows with only case name difference, data has ", length(unique(bundle_data$nid)), " unique NIDs"))}
  
  
  ## Remove any remaining invalid locations
  bundle_data <- remove_invalid_locs(bundle_data, type, release_id, location_set_id=35, silent)
  print(paste0("Bun data with invalid locs removed has ", length(unique(bundle_data$nid)), " unique NIDs"))
  
  #bundle_data      <- mark_outliers(bundle_data, outliering_factor)
  bundle_data$bundle_id[(is.na(bundle_data$bundle_id))] <- bun_id
  split_nosplit    <- handle_dismod_split_rows(bun_data=bundle_data, 
                                               nosplit_bun_ids=nosplit_bun_ids, 
                                               nosplit_country_prefix="NGA_", 
                                               nosplit_extractor="hjt")
  bundle_data   <- split_nosplit[[1]]
  nosplit_df    <- split_nosplit[[2]]
  
  
  if (bundle_data$bundle_id[1] %in% bundles_restricted_to_birth_age){
    bundle_data <- handle_restrictions(df=bundle_data, 
                                       restricted_col="age_start", 
                                       restricted_col_accpetable_values=c(0), 
                                       change_or_delete="change")
    
    bundle_data <- handle_restrictions(df=bundle_data, 
                                       restricted_col="age_end", 
                                       restricted_col_accpetable_values=c(0), 
                                       change_or_delete="change")
  }
  
  dfs_list <- list(bundle_data, bundle_data_nas, nosplit_df, bundle_data_other_measure)
  return(dfs_list)
}

process_expanded_data <- function(expanded_data, release_id, old_release_id){
  expanded_data <- merge_with_pop_data(df=expanded_data,
                                       release_id=release_id, 
                                       old_release_id=old_release_id)
  expanded_data <- transform_data(expanded_data)
  return(expanded_data)
}

process_final <- function(final_data, good_data, bundles_restricted_to_males, bundles_restricted_to_females, squeeze){
  final_data <- log_to_linear(final_data)
  final_data <- update_metrics(final_data)
  
  if (final_data$bundle_id[1] %in% bundles_restricted_to_males){
    final_data <- handle_restrictions(df=final_data, 
                                      restricted_col="sex_id", 
                                      restricted_col_accpetable_values=c(1), 
                                      change_or_delete="delete")
  }
  if (final_data$bundle_id[1] %in% bundles_restricted_to_females){
    final_data <- handle_restrictions(df=final_data, 
                                      restricted_col="sex_id", 
                                      restricted_col_accpetable_values=c(2), 
                                      change_or_delete="delete")
  }
  
  final_data <- get_change_in_mean(final_data)
  
  if (squeeze == TRUE){
    final_data$pre_squeeze_mean <- final_data$mean
    final_data$pre_squeeze_cases <- final_data$cases
    final_data <- squeeze_data(final_data)
  }
  
  
  return(final_data)
}

process_full_bundle <- function(final, good_data, nosplit_df, bundle_data_other_measure, release_id, old_release_id, cascade, lit_cascade, age_group_set_id){
  final<- final[,c(names(good_data), 'sex_id'), with=FALSE]
  good_data$sex_id <- ifelse(good_data$sex=="Male",1,2)
  #add age group id here
  full_bundle <- rbind(good_data, final, fill=TRUE)
  full_bundle <- do_cascades(full_bundle, release_id, cascade, lit_cascade, old_release_id, age_group_set_id)
  full_bundle <- rbind(full_bundle, nosplit_df, fill=TRUE)
  if (!is.null(bundle_data_other_measure)){
    full_bundle <- rbind(full_bundle, bundle_data_other_measure, fill=TRUE)
  }
  full_bundle <- reset_age_start_and_end_for_unsplit(full_bundle)
  return(full_bundle)
}


process_inputs <- function(bv_id, me_id_map_path, me_id_map_filename, measure_name, type, cascade, lit_cascade, debug, silent){ 
  type <- tolower(type)
  if (type == "mrbrt"){
    type="mr_brt"
  }
  if (type != "dismod" & type != "stgpr" & type != "mr_brt"){
    print("Error: type must be stgpr or dismod or mr_brt")
    stop_quietly()
  }
  if (debug != TRUE & debug != FALSE){
    print("Error: debug must be TRUE or FALSE")
    stop_quietly()
  }
  if (cascade != TRUE & cascade != FALSE){
    print("Error: cascade must be TRUE or FALSE")
    stop_quietly()
  }
  if (lit_cascade != TRUE & lit_cascade != FALSE){
    print("Error: cascade must be TRUE or FALSE")
    stop_quietly()
  }
  if (!dir.exists(me_id_map_path)){
    print(paste0("Error: directory ", me_id_map_path, " does not exist"))
    stop_quietly()
  }
  if (!file.exists(paste0(me_id_map_path, me_id_map_filename))){
    print(paste0("Error: file ", me_id_map_path, me_id_map_filename, " does not exist"))
    stop_quietly()
  }
  measure_name <- tolower(measure_name)
  #should this next line be conditioned on a bundle being congenital or something?
  if (measure_name == "incidence"){measure_name <- "prevalence"}
  if (measure_name %ni% c("prevalence", "proportion", "mtexcess","continuous" )){
    print(paste0("Error: measure name must be proportion, prevalence, incidence, or mtexcess"))
    stop_quietly()
  }
  
  if (!file.exists(paste0(me_id_map_path, me_id_map_filename))){
    print(paste0("there is no file ", me_id_map_filename, " at location ", me_id_map_path))
  } else{
    map <- fread(paste0(me_id_map_path, me_id_map_filename))
  }
  
  bun_data <- get_bundle_version(bundle_version_id = bv_id, fetch = 'all')
  if (!silent){print(paste0("Bundle version has ", length(unique(bun_data$nid)), " unique NIDs"))}
  
  if (is.null(bun_data)){
    print(paste0("Call to get_bundle_version() with bv_id ", bv_id, "failed."))
    print("aborting age sex splitting, please check your bv_id.")
    stop_quietly()
  }
  
  if (is.null(bun_data$age_demographer)){
    print("Bundle lacks an age_demographer column, which is necessary for age sex splitting")
    stop_quietly()
  }
  if (!is.null(bun_data$age_group_id )){
    print("Bundle already had an age_group_id column - column will be discarded and replaced")
    bun_data$age_group_id <- NULL
  }
  
  if (!is.null(bun_data$is_outlier )){
    bun_data$is_outlier <- 0
  }
  
  return (list(bun_data, map, measure_name, type, cascade, lit_cascade, debug))
}


handle_bundle_type_differences <- function(bundle_data, type, stgpr_location_set_id, dismod_location_set_id){
  if (type=="stgpr"){ 
    #change this so that it can be over-ridden by a parameter
    if (is.null(bundle_data$bundle_id)){bundle_data$bundle_id <- bundle_data$stgpr_bundle}
    bundle_data$bundle_id <- max(unique(bundle_data$bundle_id)[!is.na(unique(bundle_data$bundle_id))])
    bundle_data$location_set_id    <- stgpr_location_set_id
    bundle_data$mean   <- bundle_data$val
    bundle_data$age_demographer = 1
    suppressWarnings(bundle_data$age_group_id <- NULL)
    bundle_data$age_start    <- NULL
    bundle_data$age_end      <- NULL
    setnames(bundle_data,c("orig_age_start", "orig_age_end"), c("age_start", "age_end"))
  } 
  if (type=="dismod"){
    bundle_data$bundle_id <- max(unique(bundle_data$bundle_id)[!is.na(unique(bundle_data$bundle_id))])
    bundle_data$location_set_id    <- dismod_location_set_id
  }
  if (type=="mr_brt"){
    print("mr_brt age sex splitting not finished yet")
    stop_quietly()
  }
  
  return(bundle_data)
}

impute_missing_metrics <- function(bundle_data){
  if (is.null(bundle_data$cases)){
    bundle_data$cases <- bundle_data$mean * bundle_data$sample_size
  }
  if (is.null(bundle_data$sample_size)){
    bundle_data$sample_size <- bundle_data$cases / bundle_data$mean
  }
  if (is.null(bundle_data$mean)){
    bundle_data$mean <- bundle_data$cases / bundle_data$sample_size
  }
  
  return(bundle_data)
}

export_files <- function(outdir, full_bundle, bun_data_nas, good_data, split_df, age_summary, pre_diagnostics_df, bun_id, measure_name, release_id, map){
  if (!dir.exists(outdir)) {
    print("!!!*****ERROR*****!!!")
    print(paste0("Directory ", outdir, " does not exist"))
    stop()
  }
  write.xlsx(full_bundle,  file = paste0(outdir, bun_id,'_',measure_name, '_agesex_split_data.xlsx'), sheetName = 'extraction', showNA = FALSE)
  write.csv(bun_data_nas, file = paste0(outdir, bun_id,'_',measure_name, '_bun_data_nas.csv'),                   row.names=FALSE)
  write.csv(split_df,     file = paste0(outdir, bun_id,'_',measure_name, '_split_only.csv'),                     row.names=FALSE)
  write.csv(good_data,    file = paste0(outdir, bun_id,'_',measure_name, '_unsplit_only.csv'),                   row.names=FALSE)
  write.csv(age_summary,  file = paste0(outdir, bun_id,'_',measure_name, '_summary_age_ranges_post_split.csv'), row.names=FALSE)
  create_pre_diagnostics(bun_data=pre_diagnostics_df, bun_id=bun_id, measure_name=measure_name, outdir=outdir, release_id=release_id)
  create_post_diagnostics(bun_id=bun_id, measure_name=measure_name, outdir=outdir, release_id=release_id, new_split=split_df, map=map)
}

squeeze_data <- function(df){
  #1. Get estimated pre expansion cases from mean Sample size
  #not necessary because orig.cases is in final data set?
  
  #2. Get post everything cases from mean and sample size post expansion
  squeeze_df1 <- df[,c('cases', 'orig.cases', 'orig_index')]
  squeeze_df2 <- aggregate(cases ~ orig_index, data=squeeze_df1, sum)
  squeeze_df1$cases <- NULL
  squeeze_df  <- merge(squeeze_df1, squeeze_df2, by = c('orig_index'), all.x = TRUE)
  
  #3. Get squeeze factor from ratio cases to estimated cases
  squeeze_df$squeeze_factor <- squeeze_df$orig.cases/squeeze_df$cases
  squeeze_df$squeeze_factor[squeeze_df$orig.cases==0] <- 0 #divide by zero creates NaNs, which are replaced by 0 here
  squeeze_df <- squeeze_df[,c('orig_index', 'squeeze_factor')]
  squeeze_df <- unique(squeeze_df) #duplicate index/factor pairs bad for merging
  
  #4. Apply squeeze factor to cases and mean
  df  <- merge(df, squeeze_df, by = c('orig_index'), all.x = TRUE)
  df$cases <- df$cases*df$squeeze_factor
  df$mean  <- df$mean*df$squeeze_factor
  
  return(df)
  
}

age_sex_split <- function(bv_id,
                          measure_name,
                          type,
                          release_id,
                          cascade,
                          lit_cascade,
                          outdir=NULL,
                          old_release_id=NULL,
                          me_id_map_path="FILEPATH",
                          me_id_map_filename="FILEPATH",
                          squeeze=TRUE,
                          silent=FALSE,
                          nas_okay=FALSE,
                          debug=FALSE){
  # #   
  #' @description takes a bundle of data, and splits data from age and sex groupings comprised of multiple IHME age or sex groupings, and splits them into their composite groups
  #' @param bv_id                  numeric
  #' @param measure_name           string (must be prevalence, incidence, or proportion)
  #' @param type                   string (must be "dismod" or "stgpr," "mr_brt" coming in the future)
  #' @param release_id               numeric
  #' @param cascade                  boolean
  #' @param lit_cascade              boolean 
  #' @param outdir                   string  - OPTIONAL - DEFAULTS TO NULL
  #' @param old_release_id           numeric - OPTIONAL - DEFAULTS TO NULL
  #' @param me_id_map_path           string  - OPTIONAL - DEFUALTS TO "/share/mnch/crosswalks/"
  #' @param me_id_map_filename       string  - OPTIONAL - DEFAULTS TO "all_me_bundle.csv"
  #' @param dismod_location_set_id   numeric - OPTIONAL - DEFAULTS TO 9
  #' @param stgpr_location_set_id    numeric - OPTIONAL - DEFAULTS TO 35
  #' @param debug                    boolean - OPTIONAL - DEFUALTS TO FALSE
  #' @return dataframe with age and sex data split into composite IHME age and sex groupings
  
  #assign constants
  source_cols=c('mean', 'cases', 'sample_size', 'standard_error', 'upper', 'lower', 'case_name', 'case_definition', 'seq')
  nosplit_bun_ids=c(209, 210)
  nosplit_extractor="hjt"
  nosplit_country_prefix="NGA_"
  bundles_restricted_to_birth_age=c(610)
  bundles_restricted_to_males=c(438)
  bundles_restricted_to_females=c(437)
  dismod_location_set_id=9
  stgpr_location_set_id=35
  if (is.null(old_release_id)){old_release_id <- release_id}
  age_group_set_id <- 24
  
  #measure_name, type, cascade, lit_cascade, debug, me_id_map_path, me_id_map_filename
  inputs <- process_inputs(bv_id, me_id_map_path, me_id_map_filename, measure_name, type, cascade, lit_cascade, debug, silent)
  
  bun_data           <- inputs[[1]]
  map                <- inputs[[2]]
  measure_name       <- inputs[[3]]
  type               <- inputs[[4]]
  cascade            <- inputs[[5]]
  lit_cascade        <- inputs[[6]]
  debug              <- inputs[[7]]
  
  bun_data <- handle_bundle_type_differences(bun_data, type, stgpr_location_set_id, dismod_location_set_id)
  bun_id   <- bun_data$bundle_id[1]
  
  cause_root_path <- get_cause_root_path(bun_id=bun_id, map=map, measure_name=measure_name)
  me_id <- get_me_id(bun_id, map)
  
  processed_bun_data_dfs <- process_bundle_data(bun_data, measure_name, bun_id, cause_root_path, outdir, release_id, source_cols, nosplit_bun_ids, nosplit_country_prefix, nosplit_extractor, bundles_restricted_to_birth_age, debug, silent)
  bun_data                  <- processed_bun_data_dfs[[1]]
  bun_data_nas              <- processed_bun_data_dfs[[2]]
  nosplit_df                <- processed_bun_data_dfs[[3]]
  bundle_data_other_measure <- processed_bun_data_dfs[[4]]
  pre_diagnostics_df <- copy(bun_data)
  
  bun_data$orig_index <- seq.int(nrow(bun_data))
  
  suppressWarnings(expanded_and_good_data <- expand_data(bun_data, release_id, measure_name, age_group_set_id, silent))
  suppressWarnings(expanded  <- expanded_and_good_data[[1]])
  suppressWarnings(good_data <- expanded_and_good_data[[2]])
  
  if (nrow(expanded)==0){
    create_pre_diagnostics(bun_data=pre_diagnostics_df, bun_id=bun_id, measure_name=measure_name, outdir=outdir, release_id=release_id)
    print(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
    return(good_data)
  }
  suppressWarnings(expanded  <- process_expanded_data(expanded, release_id, old_release_id))
  draws <- merge_with_draws(expanded, me_id, measure_name, old_release_id, silent, nas_okay)
  if((draws$na_ind[1] == 1) & nas_okay==FALSE){
    stop_quietly()
  }
  draws <- sample_from_raw_input(draws)
  final <- get_case_counts_from_draws(draws, expanded)
  if(!silent){print(paste0("Split data has ", length(unique(final$nid)), " unique NIDs"))}
  
  
  
  final <- process_final(final, good_data, bundles_restricted_to_males, bundles_restricted_to_females, squeeze)
  
  suppressWarnings(full_bundle <- process_full_bundle(final, good_data, nosplit_df, bundle_data_other_measure, release_id, old_release_id, cascade, lit_cascade, age_group_set_id))
  if(!silent){
    print(paste0('Full bundle less than or equal to age start = 2 age table:'))
    print(table(full_bundle[age_start <=2]$age_start, full_bundle[age_start <=2]$age_end))
  }
  
  
  split_df  <- get_split_data_only(final)
  
  age_summary <- get_age_summary(split_df)
  if(!is.null(outdir)) {suppressWarnings(export_files(outdir, full_bundle, bun_data_nas, good_data, split_df, age_summary, pre_diagnostics_df, bun_id, measure_name, release_id, map))}
  
  return(full_bundle)
}
