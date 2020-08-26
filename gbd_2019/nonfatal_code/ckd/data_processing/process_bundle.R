#--------------------------------------------------------------
# Project: CKD data processing pipeline
# Purpose: Process and upload crosswalk versions for CKD bundles
#--------------------------------------------------------------


# setup -------------------------------------------------------------------

user <- Sys.info()["user"]
share_path <- "FILEPATH"
code_general <- "FILEPATH"

# load packages
require(data.table)

# source functions
source(paste0(code_general, "/function_lib.R"))
source(paste0(share_path,"FILEPATH/data_processing_functions.R"))
source_shared_functions(c("get_bundle_version","save_crosswalk_version","get_location_metadata"))

# set objects 
args<-commandArgs(trailingOnly = T)
bid<-as.numeric(args[1])
bvid<-as.numeric(args[2])
acause<-args[3]
ds<-args[4]
gbd_rnd<-as.numeric(args[5])
output_file_name<-args[6]
description<-args[7]
cv_ref_map_path<-args[8]
thrds<-as.numeric(args[9])
print(thrds)

# set data table threads
setDTthreads(thrds)

# load bundle version data
bdt<-get_bundle_version(bundle_version_id = bvid,export = F,transform = T)

# set output path 
bdt_xwalk_path<- "FILEPATH"
if(!dir.exists(bdt_xwalk_path)) dir.create(bdt_xwalk_path)

# set name for dt that gets dropped
drop_name<-paste0("dropped_data_bvid_",bvid,".xlsx")

# deal with weird slashes in albuminuria cv_cols
col_fix<-any(grepl("/",names(bdt)))
if (col_fix){
  message("renaming any columns with a / in them")
  fix_col_names<-grep("/",names(bdt),value=T)
  new_col_names<-sub(pattern = "/",replacement = "_",fix_col_names)
  setnames(bdt,fix_col_names,new_col_names)
}

#  -----------------------------------------------------------------------


#  age-sex split from literature data -------------------------------------

if ("age_sex_lit" %in% names(bdt)){
  bdt<-age_sex_split_lit(bdt,split_indicator = "age_sex_lit",return_full = T)
  
  # assign the original age seq as the crosswalk_parent_seq
  bdt[!is.na(parent_seq_age),crosswalk_parent_seq:=parent_seq_age]
  
  # calculate standard error for any age-sex split rows missing SE 
  bdt[(age_sex_lit==1&is.na(standard_error)),
      standard_error:=wilson_se(prop=mean,n=sample_size)]
}

#   -----------------------------------------------------------------------


# apply sex splits --------------------------------------------------------

bdt<-apply_mrbrt_sex_split(input_dt = bdt,acause = acause, bid = bid,
                           bvid = bvid, trim_pct =0.1, ds = ds, 
                           gbd_rnd = gbd_rnd)

#   -----------------------------------------------------------------------


# apply cross walks -------------------------------------------------------

cv_ref_map<-fread(cv_ref_map_path)[bundle_id==bid]
for (a in cv_ref_map[,alt]){
  bdt<-apply_mrbrt_xwalk(input_dt = bdt, bid = bid, alt_def = a, 
                         cv_ref_map_path = cv_ref_map_path)
}

#   -----------------------------------------------------------------------


# clean up ----------------------------------------------------------------

# drop any data marked with group reveiw 0 
drop_rows<-nrow(bdt[group_review%in%c(0)])
dropped_dt<-copy(bdt[group_review%in%c(0)])
dropped_dt[,drop_reason:="group_review value of 0"]
message(paste("dropping",drop_rows,"of", nrow(bdt),"where group_review is marked 0"))
bdt<-bdt[!(group_review%in%c(0))]

# clear effective sample size where design effect is null
message("clearing effective_sample_size where design effect is NA")
bdt[is.na(design_effect),effective_sample_size:=NA]

# reset weird slashes in cv_ cols for albuminuria
if (col_fix){
  message("resetting col names with /s in them to their original titles")
  setnames(bdt, new_cols_names, fix_col_names)
}

# drop any loc ids that can't be uploaded to dismod
message("dropping locations not in the DisMod hierarchy")
dismod_locs<-get_location_metadata(9)[,location_id]
dropped_dt<-rbindlist(
  list(dropped_dt,copy(
    bdt[!(location_id%in%dismod_locs)])[,drop_reason:="loc_id not supported in Dismod modelling"]),
  use.names=T,fill=T)
bdt<-bdt[location_id%in%dismod_locs]

# write data
writexl::write_xlsx(list(extraction=bdt),paste0(bdt_xwalk_path,output_file_name))
writexl::write_xlsx(list(extraction=dropped_dt),paste0(bdt_xwalk_path,drop_name))

save_crosswalk_version(bvid,data_filepath = paste0(bdt_xwalk_path,output_file_name),
                       description = description)

#   -----------------------------------------------------------------------


