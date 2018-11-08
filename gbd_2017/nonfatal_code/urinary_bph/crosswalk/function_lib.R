## Function library ##

set_j<-function(){
  if (Sys.info()['sysname'] == 'Linux') {
    j_root <- 'FILEPATH' 
  } else { 
    j_root <- 'FILEPATH'
  }
}

set_h<-function(){
  if (Sys.info()['sysname'] == 'Linux') {
    h_root <- 'FILEPATH' 
  } else { 
    h_root <- 'FILEPATH'
  }
}

## Shared functions ##
source_shared_functions<-function(functions){
  j_root<-set_j()
  functions<-paste0(j_root,"FILEPATH",functions,".R")
  for (func in functions){
    source(func)
  }
}

## Packages ## 
  require(data.table)
  require(stringr)
  require(magrittr)

## Qsubbing ## 
  h_root<-set_h()
  source(paste0(h_root,"FILEPATH/qsub_function.R"))

## Utility ## 

# mround_floor & ceiling: round ages (x) up or down to GBD age groups (choose between
# 1 and 5 year age group by specifying base)  
  mround_floor <- function(x,base){ 
    if (0<=x&x<0.01917808){return(0)}
    if (0.01917808<=x&x<0.07671233){return(0.01917808)}
    if (0.07671233<=x&x<1){return(0.07671233)}
    if (1<=x&x<4){return(1)}
    else{
      base*floor(x/base)
    }
  } 
  
  mround_ceiling <- function(x,base){ 
    if (0<x&x<=0.01917808){return(0.01917808)}
    if (0.01917808<x&x<=0.07671233){return(0.07671233)}
    if (0.07671233<x&x<=1){return(1)}
    if (1<x&x<=4){return(4)}
    if (x%%5==0){
      ceiling((base*ceiling(x/base)))+4
    }
    else{
      ceiling((base*ceiling(x/base)))-1
    }
  } 

# Capitalize the first letter of first word (if each =F) or every word (if each = T) in 
# a string 
  simpleCap <- function(s, each=F) {
    if (each==T){
      s <- strsplit(s, " ")[[1]]
    }
    paste(toupper(substring(s, 1,1)), substring(s, 2),
          sep="", collapse=" ")
  }
  
  
## Epi Functions ##
  
# Order epi cols will ensure all necessary epi columns are in a data.table/data.frame, filling
# those that were missing with NAs 

  order_epi_cols<-function(dt,cv_cols=NULL,suppress_output=F,add_columns=T,delete_columns=T){
    dt<-as.data.table(dt)
    orig_cols<-names(dt)
    note_cols<-c("note_modeler","note_sr1","note_sr2")
    note_cols<-note_cols[note_cols%in%orig_cols]
    col_ord<-c("seq","bundle_id","bundle_name","nid","underlying_nid","field_citation_value","underlying_field_citation_value",
               "input_type","page_num","table_num","source_type","location_id","location_name","smaller_site_unit", "site_memo",
               "sex","sex_issue","year_start","year_end","year_issue","age_start","age_end","age_issue","age_demographer",
               "measure","mean","lower","upper","standard_error","effective_sample_size","cases","sample_size","design_effect",
               "unit_type","unit_value_as_published","measure_issue","measure_adjustment","uncertainty_type","uncertainty_type_value",
               "representative_name","urbanicity_type",	"recall_type","recall_type_value","sampling_type","response_rate","case_name",
               "case_definition",	"case_diagnostics",	"group","specificity","group_review",note_cols,"extractor","is_outlier")
    if (!is.null(cv_cols)){col_ord<-append(x=col_ord,values=cv_cols)}
    add_cols<-col_ord[which(!col_ord%in%names(dt))]
    if (length(add_cols)>0) {
      if (add_columns==T){
        verb<-ifelse(length(add_cols)==1,"was","were")
        article<-ifelse(length(add_cols)==1,"this col","these cols")
        if (suppress_output==F){
          print(paste(simpleCap(paste(add_cols,collapse=",")), verb, "missing from original dataframe. Added", 
                      article,"and set value to NA"))
        }
        dt[,(add_cols):=NA]
      }else{
        print(paste("Dataset is missing the following standard column(s):",paste(add_cols,collapse=",")))
      }
    }
    deleted_cols<-base::setdiff(orig_cols,col_ord)
    if (delete_columns==T){
      if (length(deleted_cols)>0){
          if (suppress_output==F){
            print(paste("Deleted", deleted_cols, "from dataset")) 
          }
        }
      dt[,(deleted_cols):=NULL]
    }else{
      print(paste("Dataset has the following extra columns(s):",deleted_cols))
    }
    if (add_columns==T & delete_columns==T){
      dt<-setcolorder(x = dt,neworder = col_ord)
    }
    if (add_columns==F & delete_columns==T){
      col_ord<-col_ord[!col_ord%in%add_cols]
      dt<-setcolorder(x = dt,neworder = col_ord)
    }
    if (add_columns==T & delete_columns==F){
      col_ord<-append(col_ord,deleted_cols)
      dt<-setcolorder(x = dt,neworder = col_ord)
    }
    if (add_columns==F & delete_columns==F){
      col_ord<-append(col_ord,deleted_cols)
      col_ord<-col_ord[!col_ord%in%add_cols]
      dt<-setcolorder(x = dt,neworder = col_ord)
    }
    return(dt)
  }

# Pull map for age group id to age start/age end for 1 or 5 year age groups 
  get_age_map<-function(age_group,add_under_1=F){
    source_shared_functions("get_ids")
    if(!age_group%in%c(1,5)){
      stop("Can only generate age map for 1 or 5 year age groups")
    }
    age_dt<- get_ids(table='age_group')
    if (age_group==1){
      keep<-c(2:4,49:147)
    }else{
      keep<-c(2:20,30:32,235)
    }
    age_dt<- age_dt[age_group_id %in% keep]
    if (age_group==1){
      age_dt[,age_start:=c(0,0.01917808,0.07671233,1,seq(0,99,age_group)[3:length(seq(0,99,age_group))])]
      age_dt[age_start>=age_group,age_end:=age_start]
      age_dt[age_start<1,age_end:=c(0.01917808,0.07671233,1)]
    }else{
      age_dt[,age_start:=c(0,0.01917808,0.07671233,1,seq(0,99,age_group)[2:length(seq(0,99,age_group))])]
      age_dt[age_start>=age_group,age_end:=age_start+4]
      age_dt[age_start<age_group,age_end:=c(0.01917808,0.07671233,1,4)]
    }
    if(add_under_1==T){
      under_1<-data.frame(age_group_id=28,age_group_name="Under 1",age_start=0,age_end=0.999)
      age_dt<-age_dt[!age_group_id%in%c(2:4)]
      age_dt<-rbindlist(list(age_dt,under_1),use.names = T)
    }
    return(age_dt)
  }
  
# Check for missing locs, print names of missing locations and return datatable containing missing location_ids/names
  check_missing_locs<-function(indir,filepattern,team){
    setwd(indir)
    source_shared_functions(c("get_demographics","get_location_metadata"))
    
    # which locations should you have? 
    demographics<-get_demographics(team)
    locs<-demographics$location_id

    # which locations do you have 
    filepattern<-gsub(pattern = "\\..*",replacement = "",x = filepattern) #remove file extension 
    index<-grep(x = strsplit(x=strsplit(x = filepattern,split = "\\{")[[1]],split="\\}"),pattern = "location")-1
    saved_locs<-strsplit(x = gsub(pattern="\\..*",replacement="",x=list.files()),split = "_")
    saved_locs<-sapply(saved_locs, `[[`, index)

    missing<-setdiff(locs,saved_locs)
    loc_dt<-get_location_metadata(22)
    missing_loc_dt<-loc_dt[location_id%in%missing][,.(location_id,location_name)]
    if(length(missing)>=1){
      print(paste("Missing the following locations:",paste(missing_loc_dt[,location_name],collapse=",")))
      return(missing_loc_dt)
    }else{
      print("Not missing any locations")
      return(FALSE)
    }
  }

  get_epi_path<-function(bundle_id,acause,upload){
    if (upload==T){folder<-"02_upload"}else{folder<-"01_download"}
    path<-paste0(j_root,"WORK/12_bundle/",acause,"/",bundle_id,"/03_review/",folder,"/")
    return(path)
  }
  
  read_request<-function(bundle_id,request_id,acause){
    path<-get_epi_path(bundle_id,acause,upload=F)
    file<-paste0(path,"request_",request_id,".xlsx")
    file<-as.data.table(read.xlsx(file))
    return(file)
  }
  
  read_upload<-function(bundle_id,file_name,acause){
    path<-get_epi_path(bundle_id,acause,upload=T)
    file<-paste0(path,file_name,".xlsx")
    file<-as.data.table(read.xlsx(file))
    return(file)
  }
  
  write_upload<-function(object,bundle_id,acause,file_name){
    path<-get_epi_path(bundle_id,acause,upload=T)
    file<-paste0(path,file_name,".xlsx")
    write.xlsx(object,file,sheetName="extraction")
  }