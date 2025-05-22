#### This creates a diagnostic plot, per bundle, of the raw, split, and crosswalked data.
### if crosswalked = TRUE, then diagnostic will pull in a crosswalk version. otherwise it will just compare the raw bundle version data to the split data



create_diagnostics <- function(bun_id, crosswalked) {
  
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  library(gtools)
  
  source("FILEPATH")
  '%ni%' <- Negate('%in%')
  invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))
  
  
  map <- fread("FILEPATH/all_me_bundle.csv")
  measure_name <- 'prevalence'
  step <- 'iterative'
  
  #get metadata for bundle    
  bun_metadata <- map[bundle_id == bun_id]
  print(bun_metadata$bundle_name)
  
  bundle <- bun_id
  
  
  bv <-  fread(paste0("FILEPATH/gbd2020_", step, "_", bun_id, "_CURRENT.csv"))%>% as.data.table
  bundle_version_id <- bv$bundle_version_id
  ######## Get unsplit data #############
  # get raw data from bundle version
  raw_data <- get_bundle_version(bundle_version_id = bundle_version_id, fetch = 'all')
  unsplit_full <- copy(raw_data)
  unsplit_full$version <- "Unsplit full bundle"
  raw_data <- raw_data[clinical_data_type %ni% c('claims', 'inpatient') & cv_literature == 1, version := "Unsplit Lit"]
  raw_data <- raw_data[clinical_data_type %ni% c('claims', 'inpatient') & cv_literature == 0, version := "Unsplit Registry"]
  raw_data <- raw_data[clinical_data_type %in% c('claims', 'inpatient'), version := "Unsplit Clinical"]
  
  
  ###### Get split data ##############
  # read in split lit and split nigeria dhs data
  
  save_dir <- paste0('FILEPATH/', bun_id, '_prevalence_CURRENT/')
  split_data_file <- paste0(save_dir, bun_id, '_prevalence_full.xlsx')
  split_data <- read.xlsx(split_data_file) %>% as.data.table()
  
  split_data <- split_data[clinical_data_type %ni% c('claims', 'inpatient') & cv_literature == 1, version := "Split Lit"]
  split_data <- split_data[clinical_data_type %ni% c('claims', 'inpatient') & cv_literature == 0, version := "Split Registry"]
  split_data <- split_data[clinical_data_type %in% c('claims', 'inpatient'), version := "Split Clinical"]
  
  
  ########### Get crosswalked and outliered data #####
  # read in final uploaded crosswalked data
  if (crosswalked == TRUE){
    cw_data <- read.xlsx(paste0("FILEPATH/", bun_id, "_crosswalked_outliered_split_data_for_modeling_", step, "_CURRENT.xlsx")) %>% as.data.table()
    cw_data <- cw_data[, version := "Crosswalked and Outliers Applied"]
  }
  
  # create a single dt for plotting
  if (crosswalked == TRUE){
    dt <- rbind(unsplit_full, raw_data, split_data, cw_data, fill = TRUE)
  } else {
    dt <- rbind(unsplit_full, raw_data, split_data, fill = TRUE)
  }
  
  
  #label age ranges
  setorder(dt, age_start, na.last=FALSE)
  dt <- dt[, age_span := paste0(age_start, "-", age_end)]
  
  dt <- dt[measure=="prevalence",]
  
  dt <- dt[clinical_data_type %ni% c('claims','inpatient'), clinical_data_type := 'not clinical']
  
  #generate plot
  if (crosswalked == TRUE){
    pdf(file = paste0('FILEPATH/crosswalked_bundle_', bun_id,".pdf"),
        width = 15, height = 18)
  } else{
    pdf(file = paste0('FILEPATH/agesex_split_bundle_', bun_id,".pdf"),
        width = 15, height = 18)
  }
  
  
  dt$age_span <- factor(dt$age_span, levels = unique(dt$age_span))
  dt$version <- factor(dt$version, levels = c('Unsplit Lit', 'Split Lit', 'Unsplit Registry', 'Split Registry', 'Unsplit Clinical', 
                                              'Split Clinical', 'Unsplit full bundle', 'Crosswalked and Outliers Applied'))
  
  
  
  gg_by_sex <- ggplot(data = dt, aes(x = as.factor(age_span), y = mean, color = as.factor(sex))) +
    geom_boxplot() +
    scale_x_discrete(breaks=unique(dt$age_span), labels=unique(as.character(dt$age_span))) +
    labs(title = paste0('By sex, bundle: ', bun_metadata$bundle_name, " (", bun_id,")"),
         x = 'Age Range (years)',
         y = 'mean') +
    theme(legend.position = 'bottom') +
    theme(axis.text.x = element_text(angle = 45)) +
    scale_y_log10() +
    facet_wrap( ~version, ncol = 2, scales = "free_x") 
  
  gg_by_source <- ggplot(data = dt, aes(x = as.factor(age_span), y = mean, color = as.factor(clinical_data_type))) +
    geom_boxplot() + scale_color_brewer(palette="Dark2") +
    scale_x_discrete(breaks=unique(dt$age_span), labels=unique(as.character(dt$age_span))) +
    labs(title = paste0('By source, bundle: ', bun_metadata$bundle_name, " (", bun_id,")"),
         x = 'Age Range (years)',
         y = 'mean') +
    theme(legend.position = 'bottom') +
    theme(axis.text.x = element_text(angle = 45)) +
    scale_y_log10() +
    facet_wrap( ~version, ncol = 2, scales = "free_x") 
  
  print(gg_by_sex)
  print(gg_by_source)
  dev.off()
  
}


if (sys.nframe() == 0L) {  #if it's being called as a script as opposed to being sourced
  args <- commandArgs(trailingOnly = TRUE)
  bun_id <- args[1]
  crosswalked <- as.logical(args[2])
  print(args)
  create_diagnostics(bun_id, crosswalked=crosswalked)
}