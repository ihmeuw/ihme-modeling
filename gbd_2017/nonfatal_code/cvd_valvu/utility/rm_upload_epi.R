## function to pull in old epi data, delete, and upload new


rm_upload_epi<-function(b_id, new_data_folder, temp_folder="FILEPATH", match=F,
                        by_vars=c("location_id", "year_start", "year_end", "sex", "age_start", "age_end", "nid"), rm_only=F, rm_conditional=NULL){
  ################### SETUP #########################################
  ######################################################
  os <- .Platform$OS.type
  if (os=="windows") {
    j<- "FILEPATH"
    h<-"FILEPATH"
  } else {
    j<- "FILEPATH"
    h<-"FILEPATH"
    lib<-"FILEPATH"
  }
  library(openxlsx)
  
  
  central<-paste0(j, "FILEPATH")
  source(paste0(central, "get_epi_data.R"))
  source(paste0(central, "upload_epi_data.R"))
  source(paste0(j, "FILEPATH/get_recent.R"))  ## my function for getting most recent data
  
  
  ################### GET EXISTING DATA #########################################
  ######################################################
  message("Getting existing bundle...")
  epi_data<-get_epi_data(b_id)
  message("Done getting bundle")
  

  

  if(rm_only==T){
    ################### ONLY DELETE EXISTING DATA #########################################
    ######################################################
    if(is.null(rm_conditional)){stop("No removal condition(s) specified!")}
  
    drop_seqs<-data.table(seq=epi_data[eval(parse(text=(rm_conditional))), seq])
    
    if(nrow(drop_seqs)>0){
      message("Dropping ", nrow(drop_seqs), " where condition: `", rm_conditional,"' is met.")
      
      message(" Deleting previously uploaded data..")
      drop_seq_path<-paste0(temp_folder, "junk_upload_", b_id, "_", date, ".xlsx")
      
      openxlsx::write.xlsx(drop_seqs, file=drop_seq_path, sheetName = "extraction", row.names=F)
      
      upload_epi_data(b_id, drop_seq_path)
      unlink(drop_seq_path)
    }else{
      message(" No old data to delete")
    }
    print("Done")
    ################### DELETE OLD AND UPLOAD NEW  #########################################
    ######################################################
  }else{
    ################### DELETE EXISTING DATA #########################################
    ######################################################
    ## delete data if it's going to be replaced
    ## get NIDs 
    cleaned_data_folder<-new_data_folder
    cleaned_data_path<-get_recent(cleaned_data_folder, pattern=".xlsx", path=T)
    cleaned_data<-get_recent(cleaned_data_folder, pattern=".xlsx")
    epi_data<-epi_data[measure %in% unique(cleaned_data$measure)]
    
    if(match==T){
      message("by_vars for merging the match: ", paste0(by_vars, sep=", "))
      epi_data<-epi_data[, c("seq", by_vars), with=F]
      cleaned_data[, seq:=NULL]
      
      full<-merge(cleaned_data, epi_data, by=by_vars, all.x=T)
      
      message(nrow(full[is.na(seq)]), " rows where seq is missing, these will be newly added rows to the bundle")
      
      # full<-full[!is.na(seq)]
      ## overwrite svaed
      openxlsx::write.xlsx(full, file=cleaned_data_path, sheetName = "extraction", row.names=F)
    }
    
    
    
    ## if not matching data, just deleting any NIDs that have already been extracted
    if(match==F){
      nids<-unique(cleaned_data$nid)
      drop_seqs<-as.data.table(epi_data[nid %in% nids, .(seq=seq)])
      if(nrow(drop_seqs)>0){
        message(" Deleting previously uploaded data..")
        drop_seq_path<-paste0(temp_folder, "junk_upload_", b_id, "_", date, ".xlsx")
        
        openxlsx::write.xlsx(drop_seqs, file=drop_seq_path, sheetName = "extraction", row.names=F)
        
        upload_epi_data(b_id, drop_seq_path)
        unlink(drop_seq_path)
      }else{
        message(" No old data to delete")
      }
    }
    ################### UPLOAD NEW DATA #########################################
    ######################################################
    message("Uploading most recent cleaned file for :", cleaned_data_path)
    #cleaned_data[, X__1:=NULL]
    upload_epi_data(b_id, cleaned_data_path)
  
    #nrow(cleaned_data[is.na(upper) & !is.na(uncertainty_type_value)])
  }
}