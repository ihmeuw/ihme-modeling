os <- .Platform$OS.type

if (os=="windows") {
  j<- FILEPATH
  h <- FILEPATH
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- FILEPATH
  h<- FILEPATH
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)
library(metafor, lib.loc = my_libs)

ME_map <- fread(FILEPATH)


get_rr_data <- function(age = 2, sex = 1){
  
  files <- list.files(FILEPATH, full.names = T, recursive = F)
  files <- files[grepl(files, pattern = ".csv")]
  
  f1 <- lapply(c("AFRICA", "ASIA", "USA"), function(loc){
    
    file <- files[grepl(files, pattern = loc) & grepl(files, pattern = paste0(loc, "_", sex, "_", age))]
    
    return(file)
    
  }) %>% unlist
  
  f2 <- lapply(c("JPN", "NOR", "NZL", "TWN", "SGP"), function(loc){
    
    file <- files[grepl(files, pattern = loc) & grepl(files, pattern = paste0(loc, "_", 3, "_", age))]
    
    return(file)
    
  }) %>% unlist
  
  
  rrs <- lapply(c(f1, f2), function(x){
    
    dt <- fread(x)

    return(dt)
    
  }) %>% rbindlist(use.names = T, fill = T)
  
  rrs <- rrs[, list(ga, bw, ga_bw, dim_x1, dim_x2, gpr_y_obs, gpr_y_std)]
  
  return(rrs)

}



meta_analyze_rrs <- function(rrs, age, sex){

  ga_bw_combos <- unique(rrs$ga_bw)
  
  out.list <- lapply(ga_bw_combos, function(x) {
    
    print(x)
    
    rrs_subset <- rrs[ga_bw == x, ]
    
    rma <- rma.uni(yi = rrs_subset$gpr_y_obs, 
                   sei = rrs_subset$gpr_y_std,
                   measure="GEN")
    
    output <- data.table(ga_bw = x, 
                         y_obs = rma$beta,
                         y_std = rma$se) 
    
    return(output)
    
  }) %>% rbindlist(use.names = T, fill = T)
  
  
  setnames(out.list, "y_obs.V1", "y_obs")
  
  meta_rrs <- merge(out.list, ME_map[, list(ga_bw, ga, bw)]) 
  
  setnames(meta_rrs, c("ga", "bw"), c("dim_x1", "dim_x2"))
  
  write.csv(meta_rrs, FILEPATH, row.names = F, na = "")
  
}




# --------------------------
# MAIN SCRIPT

for(age in c(2,3)){
  
  for(sex in c(1,2)){
    
    rr_data <- get_rr_data(age = age, sex = sex)
    
    meta_analyze_rrs(rr_data, age = age, sex = sex)
    
  }
  
}

