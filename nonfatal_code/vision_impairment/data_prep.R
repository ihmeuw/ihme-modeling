
pacman::p_load(data.table, xlsx)
code_dir <- FILEPATH
input_dir <- FILEPATH
out_dir <- FILEPATH

#Toggles for data prep and/or upload
aggregate_ages = T
aggregate_severities = T 
calculate_other = T
calculate_bc = T 
save_trach = T
save_for_shiny = F
map_me_bundle = F
save_excel = F


###LOAD DATA
setwd(input_dir)
data <- fread(FILEPATH, na.strings="", stringsAsFactors = FALSE)

#Subsets for now 
data <- data[location_id != "#USER"]
data <- data[!(is.na(nid))]
data <- data[diagcode != "NVI"]

#Fix "nonstandard" coding
data[diagcode=="PHT",diagcode:="PTH"]
data[diagcode_suffix=="SEV+MOD",diagcode_suffix:="MOD+SEV"]

df <- copy(data)

#ASSERT UNIQUE
unique_vars <- c("nid", "ihme_loc_id", "location_id", "site_memo", "year_start","year_end", "age_start", "age_end", "sex", "diagcode", "diagcode_suffix", "cv_best_corrected", "urbanicity_type")
anyDuplicated(df, by = unique_vars)
if(anyDuplicated(df, by = unique_vars) > 0){
  duplicated(df, by = unique_vars)
  
  df$rn<-seq.int(nrow(df))
  dup <- as.data.table(df[ ,  .I[duplicated(df, by = unique_vars)] ])
  setnames(dup, "V1", "rn")
  dupes <- merge(dup, df, by = "rn")
  dupedf <- merge(df, dupes, by = unique_vars, all.y = T)
  setorder(dupedf, nid, ihme_loc_id, site_memo, year_start, sex, age_start)
  #stop(print(paste0("NOT UNIQUELY ID BY ", unique_vars)))
}


if(aggregate_ages) { 
  
  #Calculate cases & ss for age aggregates 
  df_agg <- copy(df)
  df_agg[,age_group := paste0(age_start, "-",age_end)]
  df_agg5 <- df_agg[nid %in% unique(df_agg[age_group=="50-99",nid])]
  df_agg5 <- df_agg5[age_group != "50-99"]
  df_agg5 <- df_agg5[diagcode=="ALL"]
  df_agg5[,cases := as.numeric(cases)]
  df_agg5[,sample_size := as.numeric(sample_size)]
  df_agg5 <- df_agg5[, lapply(.SD, sum), .SDcols = c("cases", "sample_size"), by = eval(unique_vars[!(unique_vars %in% c("age_start","age_end"))])]
  df_agg5[, "age_start" := 50]
  df_agg5[, "age_end" := 99]
  
  df_agg4 <- df_agg[nid %in% unique(df_agg[age_group=="40-99",nid])]
  df_agg4 <- df_agg4[age_group != "40-99"]
  df_agg4 <- df_agg4[diagcode=="ALL"]
  df_agg4[,cases := as.numeric(cases)]
  df_agg4[,sample_size := as.numeric(sample_size)]
  df_agg4 <- df_agg4[, lapply(.SD, sum), .SDcols = c("cases", "sample_size"), by = eval(unique_vars[!(unique_vars %in% c("age_start","age_end"))])]
  df_agg4[, "age_start" := 40]
  df_agg4[, "age_end" := 99]
  df_agg <- rbind(df_agg5,df_agg4, fill=T)
  df_agg[,group := 1]
  df_agg[,specificity := "age-collapsed for xwalk purposes only"]
  df_agg[,group_review := 0]
  grouprevvars <- c("group", "specificity" , "group_review")
  
  df_merge <- merge(df,df_agg, by = c(unique_vars, grouprevvars))
  df_merge[, match := 1]
  df_agg <- merge(df_agg, df_merge, by = c(unique_vars, grouprevvars), all.x = TRUE)
  df_agg <- df_agg[is.na(match)]
  df_agg <- df_agg[,c(unique_vars, grouprevvars, "cases", "sample_size", "mean"), with = FALSE]
  df <- rbind(df, df_agg, fill = T)
  
}




df[is.na(mean),mean := as.numeric(cases) / as.numeric(sample_size)]
df_cases = df[,c(unique_vars, "cases"), with = F]
df <- df[,c(unique_vars, "mean"), with = F]
df_long <- copy(df) # in case no further transformations are desired 

if(aggregate_severities) {

  df_wide <- dcast(df, ... ~ diagcode_suffix, value.var = "mean", drop = T, fill = NA)
  
  df_wide[!(is.na(c("DVB","SEV","MOD"))), "DVB+SEV+MOD" := DVB + SEV + MOD]
  df_wide[!(is.na(c("DVB","SEV"))), "DVB+SEV" := DVB + SEV]
  df_wide[!(is.na(c("MOD","SEV"))), "MOD+SEV" := SEV + MOD]
  
  df_long <- melt(df_wide, id.vars = unique_vars[unique_vars != "diagcode_suffix"], variable.name = "diagcode_suffix", value.name = "mean") 
  df_long <- df_long[!is.na(mean)]
  
  df_wide_trach <- dcast(df_cases[diagcode=="ALL"], ... ~ diagcode_suffix, value.var = "cases", drop = T, fill = NA)
  df_wide_trach[!(is.na(c("DVB","SEV","MOD"))), "DVB+SEV+MOD" := DVB + SEV + MOD]
  df_wide_trach[!(is.na(c("DVB","SEV"))), "DVB+SEV" := DVB + SEV]
  df_wide_trach[!(is.na(c("MOD","SEV"))), "MOD+SEV" := SEV + MOD]
  df_long_trach <- melt(df_wide_trach, id.vars = unique_vars[unique_vars != "diagcode_suffix"], variable.name = "diagcode_suffix", value.name = "sample_size") 
  df_long_trach <- df_long_trach[!is.na(sample_size)]
  df_long_trach[,diagcode:="TRACH"]
}


if(calculate_other){ #Also calculates GBDRE
  #FILL IN MISSING CAUSES: OTHER
  df_wide <- dcast(df_long, ... ~ diagcode, value.var = "mean", drop = T, fill = NA)
  
  usable <- function(diag){ifelse(is.na(diag),0,diag)}
  
  df_wide[!(is.na(RE)) & is.na(APH), "GBDRE" := RE]
  df_wide[!(is.na(RE)) & !(is.na(APH)), "GBDRE" := RE + APH]
  
  df_wide[!(is.na(CAT)) & !(is.na(GL)) & !(is.na(MAC)) & !(is.na(DR)) & cv_best_corrected == 1, "GBDOTH" := ALL - CAT - GL - MAC - DR - usable(TRACH) - usable(ONC)] # SHOULD INCLUDE - TRACH - ONC - DR BUT NAs make OTH missing if incl 
  df_wide[!(is.na(CAT)) & !(is.na(GL)) & !(is.na(MAC)) & !(is.na(DR)) & !(is.na(GBDRE)) & cv_best_corrected == 0, "GBDOTH" := ALL - GBDRE - CAT - GL - MAC - DR - usable(TRACH) - usable(ONC)]
  
  df_wide[GBDOTH>-0.01&GBDOTH<0,GBDOTH:=0]
  
  
  
  
  df_long <- melt(df_wide, id.vars = unique_vars[unique_vars != "diagcode"], variable.name = "diagcode", value.name = "mean", na.rm = T)
  df_long <- df_long[!is.na(mean)]
}

#Make df with trachoma proportions
  df_trach <- df_wide[!(is.na(TRACH))&!is.na(ALL)]
  df_trach[,mean:=TRACH/ALL]
  df_trach[,c("GBDRE","CAT","GL","APH","RE","ONC","MAC","DR","POST","PTH","GBDOTH","ALL","TRACH"):=NULL]
  df_trach[,diagcode:="TRACH"]
  df_trach[,measure:="proportion"]
  
 
if(calculate_bc){  
  #REFRACTIVE ERROR                      
  
  for (suffix in unique(df_long[,diagcode_suffix]))  {
    
    #Only calculate BC from RE for sources that don't report best-corrected vision 
    df_match <- dcast(df_long, ... ~ cv_best_corrected, value.var="mean", drop=T, fill=NA)
    setnames(df_match, c("1", "0"), c("best_corrected", "presenting"))
    df_match <- df_match[diagcode=="ALL" & diagcode_suffix==suffix & !is.na(best_corrected) & !is.na(presenting)]
    nids_bc <- unique(df_match$nid)
    df_re <- df_long[diagcode == "GBDRE" & diagcode_suffix==suffix & !(nid %in% nids_bc)]
    
    #Refractive error
    setnames(df_re, "mean", "GBDRE")
    df_re <- df_re[,c(unique_vars, "GBDRE"), with = F]
    #Convert for merging to pres envelope
    df_re[,cv_best_corrected := 0]
    df_re[,diagcode := "ALL"]
    #Merge refractive error to uncorrected vision
    df_long_re <- merge(df_long, df_re, by = unique_vars)
    df_long_re[, mean := mean - GBDRE]
    df_long_re[, cv_best_corrected := 1]
    df_long_re[, GBDRE := NULL]
    
    df_long <- rbind(df_long, df_long_re)
    
  }
  
  #BC/PRES ratio 
  df_bcpres <- dcast(df_long, ... ~ cv_best_corrected, value.var = "mean", drop = T, fill = NA)
  df_bcpres <- df_bcpres[diagcode == "ALL"]
  setnames(df_bcpres, c("1", "0"), c("best_corrected", "presenting"))
  df_bcpres[,bc_pres := best_corrected / presenting] 
  setnames(df_bcpres, c("best_corrected", "presenting"), c("1", "0"))
  df_bcpres <- df_bcpres[!(is.na(bc_pres))]    
}




df <- copy(df_long)

###Merge back on metadata
metadata_3_vars <- unique_vars
metadata_3 <- unique(data, by = metadata_3_vars)
metadata_3[,note_modeler := NULL]
metadata_3[,note_modeler := "calculated via RAAB_prep_upload.R data manipulation"]
metadata_3 <- metadata_3[,c(metadata_3_vars, "cases", "page_num", "table_num", "case_name", "case_definition", "case_diagnostics", "group", "specificity",  
                            "note_modeler", "note_SR", "is_outlier", "design_effect"), with = F]
df <- merge(df, metadata_3, by = metadata_3_vars, all.x = T)

metadata_3_trach <- metadata_3
metadata_3_trach[,cases:=NA]
df_trach <- merge(df_trach, metadata_3_trach, by = metadata_3_vars, all.x = T) 



#Metadata Level 2: merge on unique vars but not diagcode and bc 
metadata_2_vars <- unique_vars[!(unique_vars %in% c("diagcode", "diagcode_suffix", "cv_best_corrected"))]
metadata_2 <- unique(data, by = metadata_2_vars)
metadata_2 <- metadata_2[,c(metadata_2_vars, "sample_size", "smaller_site_unit", "age_demographer",
                            "response_rate"), with = F]
df <- merge(df, metadata_2, by = metadata_2_vars, all.x = T)

metadata_2_trach <- metadata_2
metadata_2_trach[,sample_size:=NULL] 
trach_merge_vars <- colnames(df_long_trach)
trach_merge_vars <- trach_merge_vars[!(trach_merge_vars %in% c("sample_size"))]
df_trach <- merge(df_trach, df_long_trach, by = trach_merge_vars, all.x = T)
df_trach <- merge(df_trach, metadata_2_trach, by = metadata_2_vars, all.x = T) 

#Metadata Level 1: just on nid
metadata_1_vars <- unique_vars[!(unique_vars %in% c("age_start", "age_end", "sex", "diagcode", "diagcode_suffix", "cv_best_corrected"))]
metadata_1 <- unique(data, by = metadata_1_vars)
metadata_1 <- metadata_1[,c(metadata_1_vars, "row_num", "parent_id", "input_type", "underlying_nid", "underlying_field_citation_value", "field_citation_value", "file_path", 
                            "source_type", "sex_issue", "year_issue", "age_issue", "extractor", "unit_type", "unit_value_as_published",
                            "measure", "representative_name", "recall_type", "recall_type_value", "sampling_type"), with = F]
df <- merge(df, metadata_1, by = metadata_1_vars, all.x = T)
df[is.na(cases), cases := mean * sample_size]

merge_metadata_1_vars <- colnames(metadata_1)[!(colnames(metadata_1) %in% c("measure"))]
df_trach <- merge(df_trach, metadata_1[,c(merge_metadata_1_vars),with=F], by = metadata_1_vars[!(metadata_1_vars %in% c("measure"))], all.x = T)


if(save_trach){
  
  df_trach <- rbindlist(list(df[diagcode=="TRACH"],df_trach),use.names = TRUE,fill = TRUE)
  df_trach[, "cv_nonstandard_severity_def" := 0]
  df_trach[, "cv_diag_loss" := 1]
  df_trach[, c("bundle_id","modelable_entity_id","modelable_entity_name","seq", "location_name", "lower", "upper", "standard_error", "effective_sample_size", "uncertainty_type", "uncertainty_type_value", 
         "group_review", "data_sheet_filepath") := NA]
    write.csv(df_trach, "raab_trachoma_update.csv", row.names = FALSE, na="")
}


if(map_me_bundle){
  #MAP TO MODELABLE_ENTITIES AND BUNDLES
  me_diagcode_map <- fread(paste0(code_dir, "/me_diagcode_map.csv"))
  
  df[diagcode!="ALL",ihme_diagcode := paste(diagcode, diagcode_suffix, sep = "-")] 
  df[diagcode=="ALL",ihme_diagcode := paste(diagcode, diagcode_suffix, cv_best_corrected, sep = "-")]
  df[,c("modelable_entity_id", "modelable_entity_name") := NULL]
  
  df_upload <- merge(df, me_diagcode_map, by = "ihme_diagcode")
  df_upload[,c("diagcode1_prefix", "diagcode2_prefix", "ihme_diagcode") := NULL]
  df_upload[,c("seq") := NA]
  
}


if(save_excel){
  #Save combined sheet 
  write.xlsx(df_upload, paste0(out_dir, "/raab_extraction_compiled.xlsx") , sheetName = "extraction",  row.names = FALSE, showNA=FALSE)
  #SAVE FOR UPLOAD 
  bundles <- unique(df_upload$bundle_id)

  for(bundle in bundles){
    df_bundle <- df_upload[bundle_id == bundle]
    file <- paste0(out_dir, "/raab_bundle_", bundle, ".xlsx")
    print(paste("Saving", file))
    write.xlsx(df_bundle, file , sheetName = "extraction",  row.names = FALSE, showNA=FALSE)
  
      
    }
    
  }
  
}