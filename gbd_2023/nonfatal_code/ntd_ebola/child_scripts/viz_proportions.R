# Purpose : viz proportions from ebola code

library(ggplot2)
library(gridExtra)
source(paste0("FILEPATH/get_age_metadata.R"))

params_dir   <- "FILEPATH/params/"
data_dir   <- paste0(params_dir, "/data_for_params")

# age metadata
age_md <- get_age_metadata(gbd_round_id = "ADDRESS", age_group_set_id = ID)
setnames(age_md, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_md <- age_md[,.(age_group_id, age_start, age_end)]

# import data
ssa_o <- fread(paste0(interms_dir, "/FILEPATH"))
sfa_o <- fread(paste0(interms_dir, "/FILEPATH"))
sma_o <- fread(paste0(interms_dir, "/FILEPATH"))

ssa_n <- fread(paste0(interms_dir, "/FILEPATH"))
sfa_n <- fread(paste0(interms_dir, "/FILEPATH"))
sma_n <- fread(paste0(interms_dir, "/FILEPATH"))

viz <- function(data, s_id, version){
  
  if (!("age_start" %in% names(data))){
    data <- merge(data, age_md, by = "age_group_id", all.x = TRUE)  
    if (nrow(data[age_group_id %in% c(4,5)]) >= 2){
      data[age_group_id == 4, c("age_start", "age_end") := .(0.0767, 1)]
      data[age_group_id == 5, c("age_start", "age_end") := .(1, 5)]
  }}
  
  if (!("sex_id" %in% names(data))){
    data[, sex_id := s_id]
  }
  
  p <- ggplot(data, aes(as.factor(round(age_start,3)), ratios, fill = as.factor(sex_id))) +
    geom_bar(stat = "identity", position = "dodge") +
    labs(x = "age_start", y = "proportion", title = paste0("Version: ", version), fill = "sex_id") +
    theme_bw()
  
  return(p)
}
  
pdf(paste0(interms_dir, "/FILEPATH"))

p_f  <- viz(data = sfa_o, s_id = 2, version = "gbd2019 age proportion for all-age female data")
p1_f <- viz(data = sfa_n, s_id = 2, version = "current age proportion for all-age female data")
sfa_comp <- grid.arrange(p_f,p1_f,ncol = 1)

p_m  <- viz(data = sma_o, s_id = 1, version = "gbd2019 proportion for all-age male data")
p1_m <- viz(data = sma_n, s_id = 1, version = "current age proportion for all-age male data")
sma_comp <- grid.arrange(p_m,p1_m,ncol = 1)

p <- viz(data = ssa_o, s_id = NA, version = "gbd2019 proportion for all-age all-sex data")
p1 <- viz(data = ssa_n, s_id = NA, version = "current age proportion for all-age all-sex data")
ssa_comp <- grid.arrange(p,p1,ncol = 1)

dev.off()
