############################################################################################################
## Purpose: Merge on HRH cadres based on occ coding system
###########################################################################################################

## clear memory
rm(list=ls())

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

pacman::p_load(data.table,magrittr,parallel,stringr)


###################################################################### still need to make new mappings work

## in/out
in.dir <- file.path(j, "FILEPATH")
bypass.dir <- file.path(j,"FILEPATH")
hrh.dir <- file.path(j, "FILEPATH")

in.files <- list.files(in.dir)
bypass.files <- list.files(bypass.dir)
out.root <- file.path(j, "FILEPATH")

## load hrh mappings
hrh_88 <- fread(file.path(hrh.dir,"isco88_hrh_merge.csv"))
hrh_88[,occ_code := as.character(occ_code)]
hrh_08 <- fread(file.path(hrh.dir,"isco08_hrh_merge.csv"))
hrh_08[,occ_code := as.character(occ_code)]
hrh_nco <- fread(file.path(hrh.dir,"nco_3d_hrh_merge.csv"))
hrh_nco[,occ_code := str_pad(as.character(occ_code),3,"left","0")]
hrh_csco <- fread(file.path(hrh.dir,"csco_3d_hrh_merge.csv"))
hrh_csco[,occ_code := str_pad(as.character(occ_code),3,"left","0")]
hrh_idn <- fread(file.path(hrh.dir,"idn_3d_hrh_merge.csv"))
hrh_idn[,occ_code := str_pad(as.character(occ_code),3,"left","0")]
hrh_bra1991 <- fread(file.path(hrh.dir,"bra1991_3d_hrh_merge.csv"))
hrh_bra1991[,occ_code := str_pad(as.character(occ_code),3,"left","0")]
hrh_bra2000 <- fread(file.path(hrh.dir,"bra2000_4d_hrh_merge.csv"))
hrh_bra2000[,occ_code := str_pad(as.character(occ_code),4,"left","0")]
hrh_mex <- fread(file.path(hrh.dir,"mex_4d_hrh_merge.csv"))
hrh_mex[,occ_code := str_pad(as.character(occ_code),4,"left","0")]
hrh_mex1992 <- fread(file.path(hrh.dir,"mex1992_4d_hrh_merge.csv"))
hrh_mex1992[,occ_code := str_pad(as.character(occ_code),4,"left","0")]
hrh_mexcuo <- fread(file.path(hrh.dir,"mexcuo_4d_hrh_merge.csv"))
hrh_mexcuo[,occ_code := str_pad(as.character(occ_code),4,"left","0")]
hrh_bwa <- fread(file.path(hrh.dir,"bwa_3d_hrh_merge.csv"))
hrh_bwa[,occ_code := str_pad(as.character(occ_code),3,"left","0")]
hrh_usa2000 <- fread(file.path(hrh.dir,"usa2000_3d_hrh_merge.csv"))
hrh_usa2000[,occ_code := str_pad(as.character(occ_code),3,"left","0")]
hrh_usa2010 <- fread(file.path(hrh.dir,"usa2010_4d_hrh_merge.csv"))
hrh_usa2010[,occ_code := str_pad(as.character(occ_code),4,"left","0")]
hrh_bracbo <- fread(file.path(hrh.dir,"bra_cbo_dom_4d_hrh_merge.csv"))
hrh_bracbo[,occ_code := str_pad(as.character(occ_code),4,"left","0")]
hrh_bolcob09 <- fread(file.path(hrh.dir,"bolcob09_5d_hrh_merge.csv"))
hrh_bolcob09[,occ_code := str_pad(as.character(occ_code),5,"left","0")]
hrh_bolcob98 <- fread(file.path(hrh.dir,"bolcob98_5d_hrh_merge.csv"))
hrh_bolcob98[,occ_code := str_pad(as.character(occ_code),5,"left","0")]
hrh_bolceob <- fread(file.path(hrh.dir,"bolceob_7d_hrh_merge.csv"))
hrh_bolceob[,occ_code := str_pad(as.character(occ_code),7,"left","0")]
hrh_tha <- fread(file.path(hrh.dir,"tha_4d_hrh_merge.csv"))
hrh_tha[,occ_code := str_pad(as.character(occ_code),4,"left","0")]


other_codes <- list()
too_short <- list()
for (file in c(bypass.files,in.files)) {
  print(file)

  ## read in file
  if (file %in% bypass.files){
    df <- fread(file.path(bypass.dir,file))
  } else {
    df <- fread(file.path(in.dir,file))
    df[,V1 := NULL]
  }

  ## pad leading 0's where they should exist in occ_codes
  if ("occ_length" %in% names(df)){
    df[,occ_code := str_pad(occ_code,occ_length,"left",pad="0")]
  } else {
    df[,occ_code := as.character(occ_code)]
  }

  ## look at original coding system of survey to determine what mapping (if any) to use
  if ("occ_code_type" %in% names(df)){
    df[,occ_code_type := iconv(occ_code_type,"WINDOWS-1252","UTF-8")]

    if (grepl("nco",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## currently assuming 3 digits for all NCO codes. Otherwise have to re-extract surveys to have
      ## occ_length variable
      df[,occ_code := str_pad(occ_code,3,"left",pad="0")]

      ## map NCO
      df <- merge(df, hrh_nco, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "NCO"]

      ## create binary variables for each category for which mapping is informative
      ## (NCO not informative for certain categories which we aren't able to map to)
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_clinic := ifelse(!is.na(me_name) & me_name == "clinic_officers",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_midass := ifelse(!is.na(me_name) & me_name == "midwifery_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_audio := ifelse(!is.na(me_name) & me_name == "audiologists",1,0)]
      df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]
      df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("tha_ipums_1990",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_census"

      ## map CSCO
      df <- merge(df, hrh_tha, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "THA_IPUMS_1990"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

      df[,isco_88_322 := ifelse(!is.na(me_name) & me_name == "isco_88_322",1,0)]

    } else if (grepl("csco",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_3"

      ## map CSCO
      df <- merge(df, hrh_csco, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "CSCO"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]
      df[,hrh_amb := ifelse(!is.na(me_name) & me_name == "ambulance_workers",1,0)]
      df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

      df[,isco_88_222 := ifelse(!is.na(me_name) & me_name == "isco_88_222",1,0)]
      df[,isco_88_322 := ifelse(!is.na(me_name) & me_name == "isco_88_322",1,0)]

    } else if (grepl("idn",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_3"

      ## map IDN coding system
      df <- merge(df, hrh_idn, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "IDN"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_clinic := ifelse(!is.na(me_name) & me_name == "clinic_officers",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_midass := ifelse(!is.na(me_name) & me_name == "midwifery_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

      df[,isco_88_513 := ifelse(!is.na(me_name) & me_name == "isco_88_513",1,0)]

    } else if (grepl("bra1991",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 1991 Census coding system
      df <- merge(df, hrh_bra1991, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "BRA1991"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_midass := ifelse(!is.na(me_name) & me_name == "midwifery_associates",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("bra2000",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_bra2000, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "BRA2000"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_amb := ifelse(!is.na(me_name) & me_name == "ambulance_workers",1,0)]
      df[,hrh_envir := ifelse(!is.na(me_name) & me_name == "environmental_hygienists",1,0)]
      df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("usa_ipums",tolower(unique(df$occ_code_type)))){

      if (df[,unique(occ_length)] == 4) {
        out.end <- "hrh_88_4"
        df <- merge(df, hrh_usa2010, by=c("occ_code"),all.x=T)
      } else {
        out.end <- "hrh_88_3"
        df <- merge(df, hrh_usa2000, by=c("occ_code"),all.x=T)
      }

      df[,occupational_code_type := "IPUMS_USA"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_clinic := ifelse(!is.na(me_name) & me_name == "clinic_officers",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_audio := ifelse(!is.na(me_name) & me_name == "audiologists",1,0)]
      df[,hrh_amb := ifelse(!is.na(me_name) & me_name == "ambulance_workers",1,0)]
      df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("mex_to_isco_hrh",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_mex, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "MEX2000"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("mex_mco_1992",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_mex1992, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "MEX1992"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("mex_cuo",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_mexcuo, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "MEXCUO"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_clinic := ifelse(!is.na(me_name) & me_name == "clinic_officers",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_midass := ifelse(!is.na(me_name) & me_name == "midwifery_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]
      df[,hrh_envir := ifelse(!is.na(me_name) & me_name == "environmental_hygienists",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("bwa",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_bwa, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "BWA"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]

    } else if (grepl("bra_cbo_dom",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_bracbo, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "BRA_CBO_DOM"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_clinic := ifelse(!is.na(me_name) & me_name == "clinic_officers",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_envir := ifelse(!is.na(me_name) & me_name == "environmental_hygienists",1,0)]
      df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("bol_cob_98",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_bolcob98, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "BOL_COB_98"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("bol_cob_09",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_bolcob09, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "BOL_COB_09"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_clinic := ifelse(!is.na(me_name) & me_name == "clinic_officers",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
      df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_midass := ifelse(!is.na(me_name) & me_name == "midwifery_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_audio := ifelse(!is.na(me_name) & me_name == "audiologists",1,0)]
      df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]
      df[,hrh_amb := ifelse(!is.na(me_name) & me_name == "ambulance_workers",1,0)]
      df[,hrh_envir := ifelse(!is.na(me_name) & me_name == "environmental_hygienists",1,0)]
      df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("bol_ceob",tolower(unique(df$occ_code_type)))){
      out.end <- "hrh_88_4"

      ## map BRA 2000 Census coding system
      df <- merge(df, hrh_bolceob, by=c("occ_code"), all.x = T)
      df[,occupational_code_type := "BOL_CEOB"]

      ## create binary variables for each category for which mapping is informative
      df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
      df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
      df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
      df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
      df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
      df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
      df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
      df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
      df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
      df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]
      df[,hrh_envir := ifelse(!is.na(me_name) & me_name == "environmental_hygienists",1,0)]
      df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]
      df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

    } else if (grepl("isco|4d|3d|tasco|sasco|nasco",tolower(unique(df$occ_code_type)))){
      if (df$occupational_code_type[1] == "ISCO 88"){
        df <- merge(df, hrh_88, by=c("occupational_code_type","occ_code"), all.x = T)

        if (nchar(round(mean(as.integer(df$occ_code)))) == 4){
          out.end <- "hrh_88_4"

          ## CATEGORIES ONLY DIRECTLY ASCERTAINABLE IN 4-DIGIT 88 ##

          df[,hrh_any := ifelse(!is.na(me_name) & !grepl("isco",me_name),1,0)]

          ## HRH cadres
          df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
          df[,hrh_clinic := ifelse(!is.na(me_name) & me_name == "clinic_officers",1,0)]
          df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
          df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
          df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
          df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
          df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
          df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
          df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
          df[,hrh_midass := ifelse(!is.na(me_name) & me_name == "midwifery_associates",1,0)]
          df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
          df[,hrh_pcare := ifelse(!is.na(me_name) & me_name == "personal_care_workers",1,0)]
          df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
          df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
          df[,hrh_audio := ifelse(!is.na(me_name) & me_name == "audiologists",1,0)]
          df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]
          df[,hrh_amb := ifelse(!is.na(me_name) & me_name == "ambulance_workers",1,0)]
          df[,hrh_envir := ifelse(!is.na(me_name) & me_name == "environmental_hygienists",1,0)]
          df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]

          ## isco 88 non-HRH residual categories within same 3-digit code as a HRH cadre
          df[,isco_88_222_other := ifelse(!is.na(me_name) & me_name == "isco_88_222_other",1,0)]
          df[,isco_88_244_other := ifelse(!is.na(me_name) & me_name == "isco_88_244_other",1,0)]
          df[,isco_88_313_other := ifelse(!is.na(me_name) & me_name == "isco_88_313_other",1,0)]
          df[,isco_88_321_other := ifelse(!is.na(me_name) & me_name == "isco_88_321_other",1,0)]
          df[,isco_88_322_other := ifelse(!is.na(me_name) & me_name == "isco_88_322_other",1,0)]
          df[,isco_88_324_other := ifelse(!is.na(me_name) & me_name == "isco_88_324_other",1,0)]
          df[,isco_88_513_other := ifelse(!is.na(me_name) & me_name == "isco_88_513_other",1,0)]

        } else if (nchar(round(mean(as.integer(df$occ_code)))) == 3){
          out.end <- "hrh_88_3"

          ## isco 88 3-digit envelopes
          df[,isco_88_222 := ifelse(!is.na(me_name) & me_name == "isco_88_222",1,0)]
          df[,isco_88_244 := ifelse(!is.na(me_name) & me_name == "isco_88_244",1,0)]
          df[,isco_88_313 := ifelse(!is.na(me_name) & me_name == "isco_88_313",1,0)]
          df[,isco_88_321 := ifelse(!is.na(me_name) & me_name == "isco_88_321",1,0)]
          df[,isco_88_322 := ifelse(!is.na(me_name) & me_name == "isco_88_322",1,0)]
          df[,isco_88_324 := ifelse(!is.na(me_name) & me_name == "isco_88_324",1,0)]
          df[,isco_88_513 := ifelse(!is.na(me_name) & me_name == "isco_88_513",1,0)]

          ## Combined nursing and midwife associates (consistent categorization in both coding systems)
          df[,hrh_nurse_mid_ass := ifelse(!is.na(me_name) & me_name == "nurse_mid_associates",1,0)]

        }

        ## HRH CATEGORIES DIRECTLY ASCERTAINABLE IN BOTH 3- AND 4-DIGIT 88 ##
        df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

      } else if (df$occupational_code_type[1] == "ISCO 08"){
        df <- merge(df, hrh_08, by=c("occupational_code_type","occ_code"), all.x = T)

        if (nchar(round(mean(as.integer(df$occ_code)))) == 4){
          out.end <- "hrh_08_4"

          ## CATEGORIES ONLY DIRECTLY ASCERTAINABLE IN 4-DIGIT 08 ##

          df[,hrh_any := ifelse(!is.na(me_name) & !grepl("isco",me_name),1,0)]

          ## HRH cadres
          df[,hrh_clinic := ifelse(!is.na(me_name) & me_name == "clinic_officers",1,0)]
          df[,hrh_opt := ifelse(!is.na(me_name) & me_name == "optometrists",1,0)]
          df[,hrh_dent := ifelse(!is.na(me_name) & me_name == "dentists",1,0)]
          df[,hrh_pharm := ifelse(!is.na(me_name) & me_name == "pharmacists",1,0)]
          df[,hrh_diet := ifelse(!is.na(me_name) & me_name == "dieticians",1,0)]
          df[,hrh_radio := ifelse(!is.na(me_name) & me_name == "radiographers",1,0)]
          df[,hrh_pharmtech := ifelse(!is.na(me_name) & me_name == "pharmaceutical_technicians",1,0)]
          df[,hrh_nurseass := ifelse(!is.na(me_name) & me_name == "nursing_associates",1,0)]
          df[,hrh_midass := ifelse(!is.na(me_name) & me_name == "midwifery_associates",1,0)]
          df[,hrh_dentass := ifelse(!is.na(me_name) & me_name == "dental_assistants",1,0)]
          df[,hrh_pcare := ifelse(!is.na(me_name) & me_name == "personal_care_workers",1,0)]
          df[,hrh_psych := ifelse(!is.na(me_name) & me_name == "psychologists",1,0)]
          df[,hrh_therap := ifelse(!is.na(me_name) & me_name == "therapists",1,0)]
          df[,hrh_audio := ifelse(!is.na(me_name) & me_name == "audiologists",1,0)]
          df[,hrh_amb := ifelse(!is.na(me_name) & me_name == "ambulance_workers",1,0)]
          df[,hrh_envir := ifelse(!is.na(me_name) & me_name == "environmental_hygienists",1,0)]
          df[,hrh_medtech := ifelse(!is.na(me_name) & me_name == "medical_technicians",1,0)]

          ## isco 08 non-HRH residual categories within same 3-digit code as a HRH cadre
          df[,isco_08_263_other := ifelse(!is.na(me_name) & me_name == "isco_08_263_other",1,0)]
          df[,isco_08_314_321_other := ifelse(!is.na(me_name) & me_name == "isco_08_314_321_other",1,0)]
          df[,isco_08_224_226_325_532_other := ifelse(!is.na(me_name) & me_name == "isco_08_224_226_325_532_other",1,0)]

        } else if (nchar(round(mean(as.integer(df$occ_code)))) == 3){
          out.end <- "hrh_08_3"

          ## isco 88 3-digit envelopes
          df[,isco_08_263 := ifelse(!is.na(me_name) & me_name == "isco_08_263",1,0)]
          df[,isco_08_314_321 := ifelse(!is.na(me_name) & me_name == "isco_08_314_321",1,0)]
          df[,isco_08_224_226_325_532 := ifelse(!is.na(me_name) & me_name == "isco_08_224_226_325_532",1,0)]

          ## Combined nursing and midwife associates (consistent categorization in both coding systems)
          df[,hrh_nurse_mid_ass := ifelse(!is.na(me_name) & me_name == "nurse_mid_associates",1,0)]

        }

        ## HRH CATEGORIES DIRECTLY ASCERTAINABLE IN BOTH 3- AND 4-DIGIT 08 ##
        df[,hrh_phys := ifelse(!is.na(me_name) & me_name == "physicians",1,0)]
        df[,hrh_trad := ifelse(!is.na(me_name) & me_name == "traditional_practitioners",1,0)]
        df[,hrh_nurseprof := ifelse(!is.na(me_name) & me_name == "nursing_professionals",1,0)]

      }
    }
  }

  ## censuses take so long to collapse that they need to be in separate folder
  if (grepl("census",tolower(file))) out.end <- "hrh_census"

  if (!"me_name" %in% names(df)){
    other_codes <- append(other_codes,file)
  } else if (all(is.na(df$me_name))) {
    too_short <- append(too_short,file)
  } else if (!grepl("OECD_PISA",file)) {
    df <- df[age_year >= 15 & age_year <= 69]
    if (grepl("WHO_SAGE",file)) df <- df[age_year >= 15 & age_year <= 49]

    write.csv(df,file.path(out.root,out.end,"new",file), row.names = F)

    ## now that we're retaining unknown codes that are detailed enough to provide occ_major info,
    ## may be biasing hrh estimates lower if such unknowns appear in hrh-related occ_majors
    ## (in this case, mostly occ_majors 2 or 3)
    if (grepl("isco",tolower(unique(df$occ_code_type))) & any(df$occ_code == "299" | df$occ_code == "2999" | df$occ_code == "399" | df$occ_code == "3999")) {
      print(paste0(file," HAS UNKNOWN OCC-MAJOR 2/3 CODES RETAINED FROM MAPPING STEP. NEED TO SPLIT"))
    }
  }
}

print("SURVEYS NOT MAPPED TO ISCO 08 OR 88:")
print(other_codes)
print("ISCO 08/88 CODES THAT WERE <3 DIGITS:")
print(too_short)
