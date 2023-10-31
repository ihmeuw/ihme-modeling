
process_new_gavi_allocations <- function(data_root,           # file path to the gavi_scenarios dir
                                         gavi_dose_scenario,  # character scalar: 'high' (12+), 'med' (18+), or 'low' (50+)
                                         location_hierarchy   # data.table of the location hierarchy
){
  
  if (!(gavi_dose_scenario %in% c('low', 'medium', 'high', 'all'))) stop('Unrecognized GAVI scenario') 
  
  gavi <- model_inputs_data$load_new_gavi_data(data_root, "low")
  
  set_distribution_dates <- function(x, 
                                     date_1="2021-06-01", 
                                     date_2="2021-12-31", 
                                     date_3="2022-12-31"
  ) {
    x[, date := fifelse(variable %like% "june", as.Date(date_1),
                        fifelse (variable %like% "2021", as.Date(date_2), as.Date(date_3)))]
  }
  
  ## AstraZeneca
  
  gavi$june_2021_az_doses <- gavi$june_2021_doses * gavi[,"proportion_AZ"]
  gavi$dec_2021_az_doses <- gavi$dec_2021_doses * gavi[,"proportion_AZ"]
  gavi$dec_2022_az_doses <- gavi$dec_2022_doses * gavi[,"proportion_AZ"]
  
  az <- data.table(melt(gavi[, c("iso3","country_name","june_2021_az_doses","dec_2021_az_doses","dec_2022_az_doses")], id.vars = c("iso3","country_name")))
  az <- set_distribution_dates(az)
  az[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  az$merge_name <- "AZD1222"
  az$company <- "AstraZeneca"
  
  ## Phizer
  
  gavi$june_2021_pz_doses <- gavi$june_2021_doses * gavi[,"proportion_Pfizer"]
  gavi$dec_2021_pz_doses <- gavi$dec_2021_doses * gavi[,"proportion_Pfizer"]
  gavi$dec_2022_pz_doses <- gavi$dec_2022_doses * gavi[,"proportion_Pfizer"]
  
  pz <- data.table(melt(gavi[, c("iso3","country_name","june_2021_pz_doses","dec_2021_pz_doses","dec_2022_pz_doses")], id.vars = c("iso3","country_name")))
  pz <- set_distribution_dates(pz)
  pz[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  pz$merge_name <- "BNT-162"
  pz$company <- "Pfizer/BioNTech"
  
  ## Janssen
  
  # I don't know who Dan Hogan is, but at one point he said to divide J&J by 2
  
  gavi$june_2021_jj_doses <- gavi$june_2021_doses * gavi[,"proportion_JJ"] / 2
  gavi$dec_2021_jj_doses <- gavi$dec_2021_doses * gavi[,"proportion_JJ"] / 2
  gavi$dec_2022_jj_doses <- gavi$dec_2022_doses * gavi[,"proportion_JJ"] / 2
  
  jj <- data.table(melt(gavi[, c("iso3","country_name","june_2021_jj_doses","dec_2021_jj_doses","dec_2022_jj_doses")], id.vars = c("iso3","country_name")))
  jj <- set_distribution_dates(jj)
  jj[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  jj$merge_name <- "Janssen"
  jj$company <- "Janssen"
  
  # Moderna
  gavi$june_2021_md_doses <- gavi$june_2021_doses * gavi[,"proportion_Moderna"]
  gavi$dec_2021_md_doses <- gavi$dec_2021_doses * gavi[,"proportion_Moderna"]
  gavi$dec_2022_md_doses <- gavi$dec_2022_doses * gavi[,"proportion_Moderna"]
  
  md <- data.table(melt(gavi[, c("iso3","country_name","june_2021_md_doses","dec_2021_md_doses","dec_2022_md_doses")], id.vars = c("iso3","country_name")))
  md <- set_distribution_dates(md)
  md[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  md$merge_name <- "Moderna"
  md$company <- "Moderna"
  
  ## Novavax
  
  gavi$june_2021_nx_doses <- gavi$june_2021_doses * gavi[,"proportion_Novavax"]
  gavi$dec_2021_nx_doses <- gavi$dec_2021_doses * gavi[,"proportion_Novavax"]
  gavi$dec_2022_nx_doses <- gavi$dec_2022_doses * gavi[,"proportion_Novavax"]
  
  nx <- data.table(melt(gavi[, c("iso3","country_name","june_2021_nx_doses","dec_2021_nx_doses","dec_2022_nx_doses")], id.vars = c("iso3","country_name")))
  nx <- set_distribution_dates(nx)
  nx[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  nx$merge_name <- "Novavax"
  nx$company <- "Novavax"
  
  nx <- nx[complete.cases(nx),]
  
  ## Tianjin CanSino
  
  gavi$june_2021_cs_doses <- gavi$june_2021_doses * gavi[,"proportion_Ad5-nCOV"]
  gavi$dec_2021_cs_doses <- gavi$dec_2021_doses * gavi[,"proportion_Ad5-nCOV"]
  gavi$dec_2022_cs_doses <- gavi$dec_2022_doses * gavi[,"proportion_Ad5-nCOV"]
  
  cs <- data.table(melt(gavi[, c("iso3","country_name","june_2021_cs_doses","dec_2021_cs_doses","dec_2022_cs_doses")], id.vars = c("iso3","country_name")))
  cs <- set_distribution_dates(cs)
  cs[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  cs$merge_name <- "Tianjin CanSino"
  cs$company <- "Tianjin CanSino"
  
  cs <- cs[complete.cases(cs),]
  
  ## CNBG Wuhan
  
  gavi$june_2021_sp_doses <- gavi$june_2021_doses * gavi[,"proportion_BBIBP-CorV"]
  gavi$dec_2021_sp_doses <- gavi$dec_2021_doses * gavi[,"proportion_BBIBP-CorV"]
  gavi$dec_2022_sp_doses <- gavi$dec_2022_doses * gavi[,"proportion_BBIBP-CorV"]
  
  sp <- data.table(melt(gavi[, c("iso3","country_name","june_2021_sp_doses","dec_2021_sp_doses","dec_2022_sp_doses")], id.vars = c("iso3","country_name")))
  sp <- set_distribution_dates(sp)
  sp[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  sp$merge_name <- "CNBG Wuhan"
  sp$company <- "CNBG Wuhan"
  
  sp <- sp[complete.cases(sp),]
  
  ## CoronaVac
  
  gavi$june_2021_cv_doses <- gavi$june_2021_doses * gavi[,"proportion_Coronavac"]
  gavi$dec_2021_cv_doses <- gavi$dec_2021_doses * gavi[,"proportion_Coronavac"]
  gavi$dec_2022_cv_doses <- gavi$dec_2022_doses * gavi[,"proportion_Coronavac"]
  
  cv <- data.table(melt(gavi[, c("iso3","country_name","june_2021_cv_doses","dec_2021_cv_doses","dec_2022_cv_doses")], id.vars = c("iso3","country_name")))
  cv <- set_distribution_dates(cv)
  cv[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  cv$merge_name <- "CoronaVac"
  cv$company <- "Sinovac"
  
  cv <- cv[complete.cases(cv),]
  
  ## Sputnik V
  
  gavi$june_2021_sv_doses <- gavi$june_2021_doses * gavi[,"proportion_Sputnik V"]
  gavi$dec_2021_sv_doses <- gavi$dec_2021_doses * gavi[,"proportion_Sputnik V"]
  gavi$dec_2022_sv_doses <- gavi$dec_2022_doses * gavi[,"proportion_Sputnik V"]
  
  sv <- data.table(melt(gavi[, c("iso3","country_name","june_2021_sv_doses","dec_2021_sv_doses","dec_2022_sv_doses")], id.vars = c("iso3","country_name")))
  sv <- set_distribution_dates(sv)
  sv[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  sv$merge_name <- "Sputnik V"
  sv$company <- "Gamaleya Research Institute"
  
  sv <- sv[complete.cases(sv),]
  
  ## Other
  
  gavi$june_2021_ot_doses <- gavi$june_2021_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  gavi$dec_2021_ot_doses <- gavi$dec_2021_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  gavi$dec_2022_ot_doses <- gavi$dec_2022_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  
  ot <- data.table(melt(gavi[, c("iso3","country_name","june_2021_ot_doses","dec_2021_ot_doses","dec_2022_ot_doses")], id.vars = c("iso3","country_name")))
  ot <- set_distribution_dates(ot)
  ot[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  ot$merge_name <- "Other"
  ot$company <- "Other"
  
  ot <- ot[complete.cases(ot),]
  
  gavi_scenario_low <- rbind(az, pz, jj, md, nx, cs, sp, sv, ot)
  gavi_scenario_low$gavi_scenario <- "low"
  
  ###
  ### MEDIUM
  ###
  
  gavi <- model_inputs_data$load_new_gavi_data(data_root, "medium")
  
  ## AstraZeneca
  
  gavi$june_2021_az_doses <- gavi$june_2021_doses * gavi[,"proportion_AZ"]
  gavi$dec_2021_az_doses <- gavi$dec_2021_doses * gavi[,"proportion_AZ"]
  gavi$dec_2022_az_doses <- gavi$dec_2022_doses * gavi[,"proportion_AZ"]
  
  az <- data.table(melt(gavi[, c("iso3","country_name","june_2021_az_doses","dec_2021_az_doses","dec_2022_az_doses")], id.vars = c("iso3","country_name")))
  az <- set_distribution_dates(az)
  az[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  az$merge_name <- "AZD1222"
  az$company <- "AstraZeneca"
  
  ## Phizer
  
  gavi$june_2021_pz_doses <- gavi$june_2021_doses * gavi[,"proportion_Pfizer"]
  gavi$dec_2021_pz_doses <- gavi$dec_2021_doses * gavi[,"proportion_Pfizer"]
  gavi$dec_2022_pz_doses <- gavi$dec_2022_doses * gavi[,"proportion_Pfizer"]
  
  pz <- data.table(melt(gavi[, c("iso3","country_name","june_2021_pz_doses","dec_2021_pz_doses","dec_2022_pz_doses")], id.vars = c("iso3","country_name")))
  pz <- set_distribution_dates(pz)
  pz[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  pz$merge_name <- "BNT-162"
  pz$company <- "Pfizer/BioNTech"
  
  ## Janssen
  
  # I don't know who Dan Hogan is, but at one point he said to divide J&J by 2
  
  gavi$june_2021_jj_doses <- gavi$june_2021_doses * gavi[,"proportion_JJ"] / 2
  gavi$dec_2021_jj_doses <- gavi$dec_2021_doses * gavi[,"proportion_JJ"] / 2
  gavi$dec_2022_jj_doses <- gavi$dec_2022_doses * gavi[,"proportion_JJ"] / 2
  
  jj <- data.table(melt(gavi[, c("iso3","country_name","june_2021_jj_doses","dec_2021_jj_doses","dec_2022_jj_doses")], id.vars = c("iso3","country_name")))
  jj <- set_distribution_dates(jj)
  jj[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  jj$merge_name <- "Janssen"
  jj$company <- "Janssen"
  
  # Moderna
  gavi$june_2021_md_doses <- gavi$june_2021_doses * gavi[,"proportion_Moderna"]
  gavi$dec_2021_md_doses <- gavi$dec_2021_doses * gavi[,"proportion_Moderna"]
  gavi$dec_2022_md_doses <- gavi$dec_2022_doses * gavi[,"proportion_Moderna"]
  
  md <- data.table(melt(gavi[, c("iso3","country_name","june_2021_md_doses","dec_2021_md_doses","dec_2022_md_doses")], id.vars = c("iso3","country_name")))
  md <- set_distribution_dates(md)
  md[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  md$merge_name <- "Moderna"
  md$company <- "Moderna"
  
  ## Novavax
  
  gavi$june_2021_nx_doses <- gavi$june_2021_doses * gavi[,"proportion_Novavax"]
  gavi$dec_2021_nx_doses <- gavi$dec_2021_doses * gavi[,"proportion_Novavax"]
  gavi$dec_2022_nx_doses <- gavi$dec_2022_doses * gavi[,"proportion_Novavax"]
  
  nx <- data.table(melt(gavi[, c("iso3","country_name","june_2021_nx_doses","dec_2021_nx_doses","dec_2022_nx_doses")], id.vars = c("iso3","country_name")))
  nx <- set_distribution_dates(nx)
  nx[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  nx$merge_name <- "Novavax"
  nx$company <- "Novavax"
  
  nx <- nx[complete.cases(nx),]
  
  ## Tianjin CanSino
  
  gavi$june_2021_cs_doses <- gavi$june_2021_doses * gavi[,"proportion_Ad5-nCOV"]
  gavi$dec_2021_cs_doses <- gavi$dec_2021_doses * gavi[,"proportion_Ad5-nCOV"]
  gavi$dec_2022_cs_doses <- gavi$dec_2022_doses * gavi[,"proportion_Ad5-nCOV"]
  
  cs <- data.table(melt(gavi[, c("iso3","country_name","june_2021_cs_doses","dec_2021_cs_doses","dec_2022_cs_doses")], id.vars = c("iso3","country_name")))
  cs <- set_distribution_dates(cs)
  cs[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  cs$merge_name <- "Tianjin CanSino"
  cs$company <- "Tianjin CanSino"
  
  cs <- cs[complete.cases(cs),]
  
  ## CNBG Wuhan
  
  gavi$june_2021_sp_doses <- gavi$june_2021_doses * gavi[,"proportion_BBIBP-CorV"]
  gavi$dec_2021_sp_doses <- gavi$dec_2021_doses * gavi[,"proportion_BBIBP-CorV"]
  gavi$dec_2022_sp_doses <- gavi$dec_2022_doses * gavi[,"proportion_BBIBP-CorV"]
  
  sp <- data.table(melt(gavi[, c("iso3","country_name","june_2021_sp_doses","dec_2021_sp_doses","dec_2022_sp_doses")], id.vars = c("iso3","country_name")))
  sp <- set_distribution_dates(sp)
  sp[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  sp$merge_name <- "CNBG Wuhan"
  sp$company <- "CNBG Wuhan"
  
  sp <- sp[complete.cases(sp),]
  
  ## CoronaVac
  
  gavi$june_2021_cv_doses <- gavi$june_2021_doses * gavi[,"proportion_Coronavac"]
  gavi$dec_2021_cv_doses <- gavi$dec_2021_doses * gavi[,"proportion_Coronavac"]
  gavi$dec_2022_cv_doses <- gavi$dec_2022_doses * gavi[,"proportion_Coronavac"]
  
  cv <- data.table(melt(gavi[, c("iso3","country_name","june_2021_cv_doses","dec_2021_cv_doses","dec_2022_cv_doses")], id.vars = c("iso3","country_name")))
  cv <- set_distribution_dates(cv)
  cv[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  cv$merge_name <- "CoronaVac"
  cv$company <- "Sinovac"
  
  cv <- cv[complete.cases(cv),]
  
  ## Sputnik V
  
  gavi$june_2021_sv_doses <- gavi$june_2021_doses * gavi[,"proportion_Sputnik V"]
  gavi$dec_2021_sv_doses <- gavi$dec_2021_doses * gavi[,"proportion_Sputnik V"]
  gavi$dec_2022_sv_doses <- gavi$dec_2022_doses * gavi[,"proportion_Sputnik V"]
  
  sv <- data.table(melt(gavi[, c("iso3","country_name","june_2021_sv_doses","dec_2021_sv_doses","dec_2022_sv_doses")], id.vars = c("iso3","country_name")))
  sv <- set_distribution_dates(sv)
  sv[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  sv$merge_name <- "Sputnik V"
  sv$company <- "Gamaleya Research Institute"
  
  sv <- sv[complete.cases(sv),]
  
  ## Other
  
  gavi$june_2021_ot_doses <- gavi$june_2021_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  gavi$dec_2021_ot_doses <- gavi$dec_2021_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  gavi$dec_2022_ot_doses <- gavi$dec_2022_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  
  ot <- data.table(melt(gavi[, c("iso3","country_name","june_2021_ot_doses","dec_2021_ot_doses","dec_2022_ot_doses")], id.vars = c("iso3","country_name")))
  ot <- set_distribution_dates(ot)
  ot[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  ot$merge_name <- "Other"
  ot$company <- "Other"
  
  ot <- ot[complete.cases(ot),]
  
  gavi_scenario_med <- rbind(az, pz, jj, md, nx, cs, sp, sv, ot)
  gavi_scenario_med$gavi_scenario <- "medium"
  
  
  ###
  ### HIGH
  ###
  
  gavi <- model_inputs_data$load_new_gavi_data(data_root, "high")
  
  ## AstraZeneca
  
  gavi$june_2021_az_doses <- gavi$june_2021_doses * gavi[,"proportion_AZ"]
  gavi$dec_2021_az_doses <- gavi$dec_2021_doses * gavi[,"proportion_AZ"]
  gavi$dec_2022_az_doses <- gavi$dec_2022_doses * gavi[,"proportion_AZ"]
  
  az <- data.table(melt(gavi[, c("iso3","country_name","june_2021_az_doses","dec_2021_az_doses","dec_2022_az_doses")], id.vars = c("iso3","country_name")))
  az <- set_distribution_dates(az)
  az[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  az$merge_name <- "AZD1222"
  az$company <- "AstraZeneca"
  
  ## Phizer
  
  gavi$june_2021_pz_doses <- gavi$june_2021_doses * gavi[,"proportion_Pfizer"]
  gavi$dec_2021_pz_doses <- gavi$dec_2021_doses * gavi[,"proportion_Pfizer"]
  gavi$dec_2022_pz_doses <- gavi$dec_2022_doses * gavi[,"proportion_Pfizer"]
  
  pz <- data.table(melt(gavi[, c("iso3","country_name","june_2021_pz_doses","dec_2021_pz_doses","dec_2022_pz_doses")], id.vars = c("iso3","country_name")))
  pz <- set_distribution_dates(pz)
  pz[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  pz$merge_name <- "BNT-162"
  pz$company <- "Pfizer/BioNTech"
  
  ## Janssen
  
  # I don't know who Dan Hogan is, but at one point he said to divide J&J by 2
  
  gavi$june_2021_jj_doses <- gavi$june_2021_doses * gavi[,"proportion_JJ"] / 2
  gavi$dec_2021_jj_doses <- gavi$dec_2021_doses * gavi[,"proportion_JJ"] / 2
  gavi$dec_2022_jj_doses <- gavi$dec_2022_doses * gavi[,"proportion_JJ"] / 2
  
  jj <- data.table(melt(gavi[, c("iso3","country_name","june_2021_jj_doses","dec_2021_jj_doses","dec_2022_jj_doses")], id.vars = c("iso3","country_name")))
  jj <- set_distribution_dates(jj)
  jj[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  jj$merge_name <- "Janssen"
  jj$company <- "Janssen"
  
  # Moderna
  gavi$june_2021_md_doses <- gavi$june_2021_doses * gavi[,"proportion_Moderna"]
  gavi$dec_2021_md_doses <- gavi$dec_2021_doses * gavi[,"proportion_Moderna"]
  gavi$dec_2022_md_doses <- gavi$dec_2022_doses * gavi[,"proportion_Moderna"]
  
  md <- data.table(melt(gavi[, c("iso3","country_name","june_2021_md_doses","dec_2021_md_doses","dec_2022_md_doses")], id.vars = c("iso3","country_name")))
  md <- set_distribution_dates(md)
  md[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  md$merge_name <- "Moderna"
  md$company <- "Moderna"
  
  ## Novavax
  
  gavi$june_2021_nx_doses <- gavi$june_2021_doses * gavi[,"proportion_Novavax"]
  gavi$dec_2021_nx_doses <- gavi$dec_2021_doses * gavi[,"proportion_Novavax"]
  gavi$dec_2022_nx_doses <- gavi$dec_2022_doses * gavi[,"proportion_Novavax"]
  
  nx <- data.table(melt(gavi[, c("iso3","country_name","june_2021_nx_doses","dec_2021_nx_doses","dec_2022_nx_doses")], id.vars = c("iso3","country_name")))
  nx <- set_distribution_dates(nx)
  nx[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  nx$merge_name <- "Novavax"
  nx$company <- "Novavax"
  
  nx <- nx[complete.cases(nx),]
  
  ## Tianjin CanSino
  
  gavi$june_2021_cs_doses <- gavi$june_2021_doses * gavi[,"proportion_Ad5-nCOV"]
  gavi$dec_2021_cs_doses <- gavi$dec_2021_doses * gavi[,"proportion_Ad5-nCOV"]
  gavi$dec_2022_cs_doses <- gavi$dec_2022_doses * gavi[,"proportion_Ad5-nCOV"]
  
  cs <- data.table(melt(gavi[, c("iso3","country_name","june_2021_cs_doses","dec_2021_cs_doses","dec_2022_cs_doses")], id.vars = c("iso3","country_name")))
  cs <- set_distribution_dates(cs)
  cs[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  cs$merge_name <- "Tianjin CanSino"
  cs$company <- "Tianjin CanSino"
  
  cs <- cs[complete.cases(cs),]
  
  ## CNBG Wuhan
  
  gavi$june_2021_sp_doses <- gavi$june_2021_doses * gavi[,"proportion_BBIBP-CorV"]
  gavi$dec_2021_sp_doses <- gavi$dec_2021_doses * gavi[,"proportion_BBIBP-CorV"]
  gavi$dec_2022_sp_doses <- gavi$dec_2022_doses * gavi[,"proportion_BBIBP-CorV"]
  
  sp <- data.table(melt(gavi[, c("iso3","country_name","june_2021_sp_doses","dec_2021_sp_doses","dec_2022_sp_doses")], id.vars = c("iso3","country_name")))
  sp <- set_distribution_dates(sp)
  sp[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  sp$merge_name <- "CNBG Wuhan"
  sp$company <- "CNBG Wuhan"
  
  sp <- sp[complete.cases(sp),]
  
  ## CoronaVac
  
  gavi$june_2021_cv_doses <- gavi$june_2021_doses * gavi[,"proportion_Coronavac"]
  gavi$dec_2021_cv_doses <- gavi$dec_2021_doses * gavi[,"proportion_Coronavac"]
  gavi$dec_2022_cv_doses <- gavi$dec_2022_doses * gavi[,"proportion_Coronavac"]
  
  cv <- data.table(melt(gavi[, c("iso3","country_name","june_2021_cv_doses","dec_2021_cv_doses","dec_2022_cv_doses")], id.vars = c("iso3","country_name")))
  cv <- set_distribution_dates(cv)
  cv[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  cv$merge_name <- "CoronaVac"
  cv$company <- "Sinovac"
  
  cv <- cv[complete.cases(cv),]
  
  ## Sputnik V
  
  gavi$june_2021_sv_doses <- gavi$june_2021_doses * gavi[,"proportion_Sputnik V"]
  gavi$dec_2021_sv_doses <- gavi$dec_2021_doses * gavi[,"proportion_Sputnik V"]
  gavi$dec_2022_sv_doses <- gavi$dec_2022_doses * gavi[,"proportion_Sputnik V"]
  
  sv <- data.table(melt(gavi[, c("iso3","country_name","june_2021_sv_doses","dec_2021_sv_doses","dec_2022_sv_doses")], id.vars = c("iso3","country_name")))
  sv <- set_distribution_dates(sv)
  sv[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  sv$merge_name <- "Sputnik V"
  sv$company <- "Gamaleya Research Institute"
  
  sv <- sv[complete.cases(sv),]
  
  ## Other
  
  gavi$june_2021_ot_doses <- gavi$june_2021_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  gavi$dec_2021_ot_doses <- gavi$dec_2021_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  gavi$dec_2022_ot_doses <- gavi$dec_2022_doses * apply(gavi[,c("proportion_Other","proportion_BECOV2","proportion_GX-19","proportion_Recombinant SARS-CoV-2 vaccine")], 1, sum, na.rm = TRUE)
  
  ot <- data.table(melt(gavi[, c("iso3","country_name","june_2021_ot_doses","dec_2021_ot_doses","dec_2022_ot_doses")], id.vars = c("iso3","country_name")))
  ot <- set_distribution_dates(ot)
  ot[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  ot$merge_name <- "Other"
  ot$company <- "Other"
  
  ot <- ot[complete.cases(ot),]
  
  gavi_scenario_high <- rbind(az, pz, jj, md, nx, cs, sp, sv, ot)
  gavi_scenario_high$gavi_scenario <- "high"
  
  
  # Make combo of ALL arg
  gavi_out <- rbind(gavi_scenario_low, 
                    gavi_scenario_med,
                    gavi_scenario_high)
  
  gavi_out <- merge(gavi_out, 
                    location_hierarchy[,c("ihme_loc_id","location_id","location_name")],
                    by.x = "iso3", 
                    by.y = "ihme_loc_id")
  
  gavi_out[, c("country_name","variable","value") := NULL]
  gavi_out$location <- "GAVI"
  gavi_out[, named_doses := quarter_doses]
  
  
  if (gavi_dose_scenario == "low") {
    
    return(gavi_out[gavi_scenario == 'low',])
    
  } else if (gavi_dose_scenario == "medium"){
    
    return(gavi_out[gavi_scenario == 'medium',])
    
  } else if (gavi_dose_scenario == "high"){

    return(gavi_out[gavi_scenario == 'high',])
    
  } else if (gavi_dose_scenario == "all"){
    
    return(gavi_out)
    
  }

  
}
 