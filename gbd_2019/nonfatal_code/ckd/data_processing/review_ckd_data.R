#--------------------------------------------------------------
# Project: Nonfatal CKD
# Purpose:  Pull and compile all data in CKD bundles
#--------------------------------------------------------------

# setup -------------------------------------------------------
user <- Sys.info()["user"]
share_path <- "FILEPATH"
code_general <- "FILEPATH"

source(paste0("FILEPATH/function_lib.R"))
source_shared_functions("get_epi_data")

mark_error<-function(dt,seqs,bid,src_nid,note){
  if(nrow(dt[seq%in%seqs&bundle_id==bid&nid==src_nid])<1){
    stop("No data identified by this seq/bundle_id/nid combination")
  }
  dt[seq%in%seqs&bundle_id==bid&nid==src_nid,flag_error:=1]
  dt[seq%in%seqs&bundle_id==bid&nid==src_nid&flag_error==1,flag_error_note:=note]
}


bundle_ids<-list(182,183,184,185,186,670,760)

dt_list<-lapply(bundle_ids,get_epi_data)
dt_list<-rbindlist(dt_list,use.names = T,fill=T)

mark_error(dt_list,5110,184,237936,"Duplicative data. Already have transplant incidence from
           ERA-EDTA for Turkey, 2008. USRDS number looks to be low and likely represents
           incidence of pre-emptive transplant rather than total number of TXs in the year")

# HUNT data - not flagging in albuminuria bundle b/c we don't have albuminuria measurement 
# in the microdata files
mark_error(dt_list,c(64341:64342),182,269883,"Duplicative data. Delete after decomp 3 when tabulated
           HUNT microdata is uplaoded from ubcov extraction (NID 327780)")
mark_error(dt_list,c(35488:35489),183,269883,"Duplicative data. Delete after decomp 3 when tabulated
           HUNT microdata is uplaoded from ubcov extraction (NID 327780)")
mark_error(dt_list,2403,760,269883,"Duplicative data. Delete after decomp 3 when tabulated
           HUNT microdata is uplaoded from ubcov extraction (NID 327780)")

# Leiden Study, Netherlands - age_start is incorrect
mark_error(dt_list,2525,760,328192,"Change age_start from 88 to 85")

# Irish National Kidney Disease Surveillance Programme - CKD EPI covariate is not tagged properly for 
# a few rows
mark_error(dt_list,seqs = c(1:3),182,212428,"Set cv_ckd_ckd_epi to 1. These data represent prevalence
           reported using CKD-EPI equation.")
mark_error(dt_list,seqs = c(3:5),183,212428,"Set cv_ckd_ckd_epi to 1. These data represent prevalence
           reported using CKD-EPI equation.")
mark_error(dt_list,seqs = c(3:5),183,212428,"Set cv_ckd_ckd_epi to 1. These data represent prevalence
           reported using CKD-EPI equation.")
mark_error(dt_list,seqs = c(3:5),183,212428,"Set age_start to 18 not 15. Remove note_modeler related 
           to age-splitting - these data are not age split")

# KNHANES lit study - duplicative after KNHANES microdata is added
mark_error(dt_list,seqs= c(8:9), 182, 212429, "Duplicative data. Delete after decomp 4 when tabulated 
           KNHANES microdata is uploaded from ubcov extraction")
mark_error(dt_list,seqs= c(10:11), 183, 212429, "Duplicative data. Delete after decomp 4 when tabulated 
           KNHANES microdata is uploaded from ubcov extraction")
mark_error(dt_list,seqs= c(1:2), 186, 212429, "Duplicative data. Delete after decomp 4 when tabulated 
           KNHANES microdata is uploaded from ubcov extraction")
mark_error(dt_list,seqs= c(440:441,3972,4019,4139,4183,4264,4331,4343,4399,4409,4424,4459,4473,4476,
                           4520,4557,4578), 760, 212429, "Duplicative data. Delete after decomp 4 when tabulated 
           KNHANES microdata is uploaded from ubcov extraction")

# Zhejiang Study - stage 4/5 data flipped
mark_error(dt_list,seqs=12,183,212430,"Stage 5 data input as stage 4. Swap this row with data from seq
           3, bundle 186.")
mark_error(dt_list,seqs=3,186,212430,"Stage4 data input as stage 5. Swap this row with data from seq
           12, bundle 183.")

# SLAN Ireland, Browne - Age-sex specific stage 3-5 data not extracted. 
mark_error(dt_list,seqs=4878,760,212435,"Number of cases extracted as 131 - should be 130 per table 3,
           bottom panel. Replace this all age estimate with age-specific values from this table:
           31 for < 65, 99 for => 65. Also extract sex-specific data from second paragraph of results
           section then age-sex split.")
mark_error(dt_list,seqs=20,182,212435,"Unsure where the 130 cases came from. Prevalence should extracted
           as 11.5% (~126 cases) not 11.8%")

# India SEEK study - CKD-EPI data not extracted
mark_error(dt_list,43,182,210572,"Study gives prevalence for CKD-EPI and MDRD. CKD-EPI not extracted. Extract
           and replace MDRD with CKD-EPI. Mark CKD-EPI covariate.")
mark_error(dt_list,40,183,210572,"Study gives prevalence for CKD-EPI and MDRD. CKD-EPI not extracted. Extract
           and replace MDRD with CKD-EPI. Mark CKD-EPI covariate.")
mark_error(dt_list,30,186,210572,"Study gives prevalence for CKD-EPI and MDRD. CKD-EPI not extracted. Extract
           and replace MDRD with CKD-EPI. Mark CKD-EPI covariate.")
mark_error(dt_list,c(377:378),670,210572,"Study gives prevalence for CKD-EPI and MDRD. CKD-EPI not extracted. Extract
           and replace MDRD with CKD-EPI. No sex-specificity for CKD-EPI. Mark CKD-EPI covariate.")
mark_error(dt_list,c(4871:4873),760,210572,"Study gives prevalence for CKD-EPI and MDRD. CKD-EPI not extracted. Extract
           and replace MDRD with CKD-EPI. No sex-specificity for CKD-EPI. Mark CKD-EPI covariate.")

# EPRICE Spain - Envelope data not properly extracted
mark_error(dt_list,seqs=c(364:369),760,129274,"Missing rows for original, raw CKD envelope extraction. Re-extract raw
           envelope data then run age-sex split from lit. Also remove note about being age-split.")

# Lancet CKD in China study - extract age-sex specific envelope data from the appendix
mark_error(dt_list,seqs=358,760,129263,"Replace this row with age-sex specific data from the appendix. Need to use
           web-plot digitizer. Appendix url: https://ars.els-cdn.com/content/image/1-s2.0-S0140673612600336-mmc1.pdf")

# Beijing CKD, Zhang - extract age-specific data from figure 1 
mark_error(dt_list,seqs=727,182,125728,"Replace this row with age-sopecific data from Figure 1. Age-specific sample
           sizes are available in table 4.")

# Thai Air Force study - cov marked incorrectly
mark_error(dt_list,736,182,125716,"Mark cv_mdrd = 1")
mark_error(dt_list,332,760,125716,"Mark cv_mdrd = 1")

# UK HSE - fix ckd_epi cov 
mark_error(dt_list,c(1172:1203),670,95630,"Mark cv_ckd_ckd_epi = 1")
mark_error(dt_list,c(1204:1235),670,95631,"Mark cv_ckd_ckd_epi = 1")

# CKD in China, Chen - group review mislabeled
mark_error(dt_list,793,182,125717,"Mark group_review = 1")

# Singh CKD in Delhi - mark MDRD and extract age/sex data
mark_error(dt_list,759,182,125718,"Mark cv_mdrd = 1")
mark_error(dt_list,333,760,125718,"Mark cv_mdrd = 1. Extract age-sex specific data from Fig 1b. Need to
           use web-plot digitizer")

# De Francisco Spain - fix ckd epi cov
mark_error(dt_list,2764,760,126035,"Mark cv_ckd_ckd_epi = 0. This is an MDRD point.")

# Taiwan Stage 3 Article - extract age/sex data
mark_error(dt_list,791,182,126042,"Replace this row with age-sex specific data from table 2.")

# ACTIFE Germany study - 
mark_error(dt_list,c(382:383),670,129261,"Extract CKDEPI albuminuria instead of MDRD")

# CRYSTAL Study St. Petersburg 
mark_error(dt_list,2773,186,129268,"Set age_start = 75, age_end=99")
mark_error(dt_list,2774,186,129268,"Set sex = Male")
mark_error(dt_list,2775,186,129268,"Set age_start = 75, age_end=99 and sex = Male")
mark_error(dt_list,4854,760,129268,"Set age_start = 75, age_end=99, and sample_size = 230. Clear other information 
           (lower, upper, SE, etc)")
mark_error(dt_list,4855,760,129268,"Set sex = Male and sample_size = 98. Clear other uncertainty information")
mark_error(dt_list,4856,760,129268,"Set age_start = 75, age_end=99, sex = Male, and sample_size = 72. Clear other 
           uncertainty information")

# ELSA-Brazil 
mark_error(dt_list,340,670,269845,"Extract age & sex specific data from Table 1 (as ACR > 30 only. Then age-sex split.
           Also age_end max should be 74")
mark_error(dt_list,64327,182,269845,"set age_end=74")
mark_error(dt_list,35477,183,269845,"set age_end=74")
mark_error(dt_list,784,186,269845,"set age_end=74")
mark_error(dt_list,c(486,2879,4007,4096,4173,4273), 760, 269845, "Extracted CKD col from Table 1 but should have extracted CKD-ACR
           < 30 alone (or GFR < 60 + Both). Also didn't ever age/sex split this data. Do that after re-extracting")

write.csv(dt_list[flag_error==1],paste0("FILEPATH/compiled_ckd_data_gbd2017_errors.csv"),
          row.names = F,na = "")


