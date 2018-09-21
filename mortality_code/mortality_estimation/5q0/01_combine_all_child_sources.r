# ----------------------------------------------------------------------------
# Description: This file combines all child mortality data into one dataset with consistent formatting.
# ----------------------------------------------------------------------------


# ----------------------------------------------------------------------------
# Setting up R
# ----------------------------------------------------------------------------

  rm(list=ls())
  root <- "FILEPATH"
  library(foreign)
  library(grid)
  library(lattice)
  library(plyr)
  library(haven)
  library(data.table)

  plot.graphs = TRUE

# read in location names 
  source("FILEPATH/shared/functions/get_locations.r")
  locations <- read.csv(paste0(root, "FILEPATH/locations.csv"), stringsAsFactors = F)
  ## need to fill in NAs in the 2013 locations, also the database lies and says we didn't do PRI or BMU
  locations$local_id_2013[is.na(locations$local_id_2013)] <- locations$ihme_loc_id[is.na(locations$local_id_2013)]


  DIRECTORY.LIBRARIES <- "FILEPATH/libraries/"
  ## Demographic methods and graph code
  source(paste(DIRECTORY.LIBRARIES,"gbd_envelopes_library.R",sep=""))
  source(paste(DIRECTORY.LIBRARIES, "graph5q0_nosave.R", sep=""))


## append function to combine datasets (fancy version of rbind)
  append.data <- function(data, add.data) {
      if (TRUE) {
        data = data
        add.data= add.data
      }
      if (add.data$in.direct[1] != "direct" | (add.data$in.direct[1] == "direct" & add.data$data.age[1] != "new") | is.na(add.data$in.direct[1])) {
        # add in variables that are specific to CBH data variance
        add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
         
      }
      
      # add extra columns if needed 
      if (!("ihme.loc.id" %in% names(add.data))){
        add.data$ihme.loc.id <- NA
      }
      
      if (!("iso3" %in% names(add.data))){
        add.data$iso3 <- NA
      }

      add.data$NID <- NULL  # take this out when we want to use NID's
      
      if (!("compiling.entity" %in% names(add.data))){
        add.data$compiling.entity <- NA
      }
      
      # only keep the variables that we want in the final dataset
      names <- c("iso3","ihme.loc.id","t","q5","source","source.date","in.direct","compiling.entity","data.age","sd.q5","log10.sd.q5")
      add.data <- add.data[,names]
      data <- data[,names]
      data <- rbind(data, add.data)
      return(data)
  }
  
                            
# ----------------------------------------------------------------------------
# Directories
# ----------------------------------------------------------------------------

  setwd("FILEPATH/01. Add child only data")
  sbh.data.dir<-"01. summary birth histories/"
  direct.data.dir<-"02. direct birth histories/"
  agg.data.dir<-"03. aggregate estimates/"
  ssc.data.dir<-"FILEPATH/Single survey or census data - for Growth Balance/"


# ----------------------------------------------------------------------------
# Filenames
# ----------------------------------------------------------------------------

  iso3.data<-"T country to iso3.csv"                            # ISO 3 to Country name file
  t.data<-"EST_T_VARIOUS_v5Q0 12-21-09.dta"               # original dataset from Lancet paper

#----------------------------------------------------------
#Summary birth history datasets

  random.sbh<-"EST_RANDOM_v5Q0_IHME.dta"                      # MOSTLY FROM UN POP DIVISION
  cdcrhs.sbh<-"EST_CDCRHS_v5Q0_IHME.dta"                      # CDC REPRODUCTIVE HEALTH SURVEYS (CDC RHS)
  dhs.sbh<-"EST_DHS_v5Q0_IHME.dta"                            # DEMOGRAPHIC AND HEALTH SURVEYS (DHS)
  dybtab.sbh<-"EST_DYB_CENSUSTAB_v5Q0_IHME.dta"               # DYB TABULATED CENSUS
  ipums.sbh<-"EST_IPUMS_v5Q0_IHME.dta"                        # IPUMS
  mics.sbh<-"EST_MICS_v5Q0_IHME.dta"                          # MULTIPLE INDICATOR CLUSTER SURVEYS (MICS)
  wfs.sbh<-"EST_WFS_v5Q0_IHME.dta"                            # WORLD FERTILITY SURVEYS (WFS)
  chncensus.sbh<-"EST_CHN_CENSUS_1990-2000_v5Q0_IHME.dta"     # CHINA: CENSUS 1990-2000
  chn2010census.sbh <- "EST_CENSUS_2010_CHN_v5Q0_IHME.dta"    # CHINA: CENSUS 2010
  inddlhs.sbh<-"EST_IND_DLHS_v5Q0_IHME.dta"                   # INDIA: DLHS
  susenas.sbh<-"EST_SUSENAS_v5q0_IHME.dta"				            # INDONESIA: SUSENAS
  ifhs.sbh<-"EST_IFHS_v5q0_IHME.dta"						              # IRAQ Fertility and Health Survey
  tlsdhs.sbh<-"EST_TLS DHS_v5Q0_IHME.dta"					            # subset of IDN DHS for Timor Leste; also 2003 TLS DHS
  lsms.sbh<-"EST_LSMS_v5Q0_IHME.dta"                          # LSMS surveys with summary birth histories
  papfam.sbh<-"EST_PAPFAM_v5Q0_IHME.dta"				             	# PAPFAM: Papfam countries summary birth history
  marcensus.sbh<-"EST_MAR_CENSUS_2004_v5Q0_IHME.dta"		      # summary birth history from 2004 Morocco Census
  bhutancensus.sbh<-"EST_BTN_CENSUS_2005_v5Q0_IHME.dta"      	# summary birth history from 2005 bhutan census
  kiribaticensus.sbh<-"EST_KIR_CENSUS_2005_v5Q0_IHME.dta"     # summary birth history from 2005 kiribati census
  idncen.sbh<-"EST_IDNcen_v5Q0_IHME.dta"						          # IDN 2000 census summary birth histories
  irncen.sbh<-"EST_IRN_CENSUS_v5Q0_IHME.dta"					        # IRN 2006 census summary birth histories
  irndhs.sbh<-"EST_IRN_DHS_2000_v5Q0_IHME.dta"		            # IRN 2000 DHS from M
  afgmics2.sbh<-"EST_AFG_MICS2_v5Q0_IHME.dta"                 # Add in summary birth Histories AFG MICS2
  afgahs.sbh<-"EST_AHS2006_v5Q0_IHME.dta"                     # Add in AFG 2006 AHS summar birth histories
  mis.sbh<-"EST_MIS_v5Q0_IHME.dta"                            # summary birth histories from the malaria indicators survey
  papchild.sbh<-"EST_PAPCHILD_v5Q0_IHME.dta"                  # summary birth histories from the PAPCHILD surveys
  dlhs3.sbh<-"EST_DLHS3_v5Q0_IHME.dta"                        # summary birth histories from the India DLHS III
  supas.sbh<-"EST_SUPAS_v5q0_IHME.dta"                        # summary birth histories from Indonesia SUPAS
  stpmics3.sbh<-"EST_STP_MICS3_v5q0_IHME.dta"                 # summary birth histories from the MICS3 final report for STP
  mmrfrhs.sbh <- "EST_FRHS_v5Q0_IHME.dta"                     # summary birth histories from the Myanmar Fertility and Reproductive Health Surveys
  lkacensus.sbh <- "EST_LKA_CENSUS_2001_v5Q0_IHME.dta"        # summary birth histories from the Sri Lanka 2001 CENSUS (tabulated data)
  sommics2.sbh <- "EST_SOM_MICS2_v5Q0_IHME.dta"               # summary birth histories from the 1999 Somalia MICS2 survey (tabulated data)
  dhsspecial.sbh <- "EST_DHS_SP_v5Q0_IHME.dta"                # summary birth histories from DHS Special surveys
  dhsinterim.sbh <- "EST_DHS_ITR_v5Q0_IHME.dta"               # summary birth histories from DHS Interim surveys
  dhsindepth.sbh <- "EST_DHS_IN_v5Q0_IHME.dta"                # summary birth histories from DHS In-depth surveys
  ais.sbh <- "EST_AIS_v5Q0_IHME.dta"                          # summary birth histories from AIS surveys
  pnad.sbh <- "EST_PNAD_v5Q0_IHME.dta"                        # summary birth histories from PNAD Brazil surveys
  vnmpcfps.sbh <- "EST_VNM_PCFPS_2008_v5Q0_IHME.dta"          # summary birth histories from the 2008 VNM Population Change and Family Planning Survey (tabulated data)
  saubulletin.sbh <- "EST_SAU_BULLETIN_2007_v5Q0_IHME.dta"    # summary birth histories from the 2007 SAU Demographic Bulletin (tabulated data)
  mex2010cen.subnat.sbh <- "EST_MEX_CENSUS_2010_v5Q0_IHME.dta"       # summary birth histories from the 2010 Mexico census (microdata) subnational 
  ind2001cen.sbh <- "EST_IND_CENSUS_2001_v5Q0_IHME.dta"        # summary birth histories from the 2001 India census at national level
  paksocial.sbh <- "EST_PAK_SOCIAL_SURVEY_v5Q0_IHME.dta"      # summary birth histories from the 2001-2002, 2005-2006, and 2007-2008 social and living measurement survey - PAK
  paksocial99.sbh <- "EST_PAK_SOCIAL_1999_v5Q0_IHME.dta"      # mac only sbh from the 1998-1999 social and living measurement survey - PAK
  bra2010census.sbh <- "EST_BRA_2010_CENSUS_v5Q0_IHME.dta"    # brazil 2010 census sbh 
  idn.ifls.sbh <- "EST_IDN_IFLS_v5Q0_IHME.dta"                # IDN IFLS sbh surveys in 1993, 1997, 2000, and 2007(8) 
  tur.dhs.sbh <- "EST_TUR_DHS_v5Q0_IHME.dta"                  # TUR DHS sbh 2008 (not macro)
  tur.ims.sbh <- "EST_TUR_IMS_v5Q0_IHME.dta"                  # TUR Infant Mortality Survey, April 2011
  tur.89ds.sbh <- "EST_TUR_89DS_v5Q0_IHME.dta"                # TUR 1989 Demographic Survey
  afg.natl.risk.sbh <- "EST_AFG_NRVA_SURVEY_v5Q0_IHME.dta"    # National risk and vulnerability assessment survey
  vut2009cen.sbh <- "EST_VUT_2009_CENSUS_NSO_v5Q0_IHME.dta"   # VUT 2009 Census from National Statistics Office download
  ecu2010cen.sbh <- "EST_ECU_2010_CENSUS_v5Q0_IHME.dta"       #2010 Ecuador Census 
  bol2000surv.sbh <- "EST_BOL_HH_SURVEY_2000_v5Q0_IHME.dta"   # Bolivia HH Survey 2000
  bwa.mics2000.sbh <- "EST_BWA_MICS_2000_v5Q0_IHME.dta"   # Botswana MICS 2000 microdata
  fsm2000census.sbh <- "EST_FSM_2000_CENSUS_v5Q0_IHME.dta"    # FSM 2000 census, tabulated data
  fsm1973census.sbh <- "EST_FSM_1973_CENSUS_v5Q0_IHME.dta"    # FSM 1973 census, tabulated data
  mexenigh2010.sbh <- "EST_MEX_ENIGH_2010_v5Q0_IHME.dta"        # MEX ENIGH 2010 microdata
  mexenadid2006.sbh <- "EST_MEX_ENADID_2006_v5Q0_IHME.dta"      # MEX ENADID 2006 microdata
  ihds.0405.sbh <- "EST_IND_human_development_survey_04_05_v5Q0_IHME.dta" #IND Human Development Survey
  imira.2004.sbh <- "EST_IRQ_IMIRA_v5Q0_IHME.dta"              #IRQ IMIRA 2004                                               
  ssd_hhs_2010.sbh <- "EST_SSD_HH_HEALTH_v5Q0_IHME.dta"         #SDN Southern Household Health Survey 2010 
  cpv.dhs2005.sbh <- "EST_CPV_DHS2005_REPORT_21442_v5Q0_IHME.dta"  # CPV DHS 2005 report sbh
  chn.prov.census82.sbh <- "EST_CHN_PROVINCE_1982_SBH_CENSUS_v5q0_IHME.dta" #Provincial level census sbh
  chn.prov.census90.sbh <- "EST_CHN_PROVINCE_1990CENSUS_v5q0_IHME.dta" #Provincial level census sbh
  chn.prov.census00.sbh <- "EST_CHN_PROVINCE_2000CENSUS_v5q0_IHME.dta" #Provincial level census sbh   
  chn.prov.census10.sbh <- "EST_CHN_PROVINCE_2010CENSUS_v5q0_IHME.dta" #Provincial level census sbh
  ago.mics1.1996.sbh <- "EST_AGO_MICS1report_v5Q0_IHME.dta"         # AGO MICS1 report sbh
  rwa.eicv.sbh <- "EST_RWA_EICV_2006_v5Q0_IHME.dta"             #RWA EICV 06 entered by Ethan    
  ind.dhs.urbrur.sbh <- "EST_IND_DHS_urban_rural_v5Q0_IHME.dta"  #IND urban rural DHS split sbh
  ind.dlhs.urbrur.sbh <- "EST_IND_DLHS_URBAN_RURAL_v5Q0_IHME.dta" #IND urban rural DLHS split sbh
  ind.dlhs3.urbrur.sbh <- "EST_India_DLHS3_urban_rural_v5Q0_IHME.dta"  #IND state + urban rural DLHS3 2007 split sbh
  ind.census01.urbrur.sbh <- "EST_IND_CENSUS_2001_urban_rural_v5Q0_IHME.dta"  #IND 2001 census urban rural sbh
  ind.census91.urbrur.sbh <- "EST_IND_CENSUS_1991_urban_rural_v5Q0_IHME.dta"  #IND 1991 census urban rural sbh
  ind.census81.urbrur.sbh <- "EST_IND_CENSUS_1981_urban_rural_v5Q0_IHME.dta"  #IND 1981 census urban rural sbh
  mex.enadid2006.subnat.sbh <- "EST_MEX_ENADID_2006_SUBNATIONAL_v5Q0_IHME.dta" #Mexico ENADID 2006 by state 
  mex.enadid2009.subnat.sbh <- "EST_MEX_ENADID_2009_SUBNATIONAL_v5Q0_IHME.dta" #Mexico ENADID 2009 by state
  mex.ipums1990.subnat.sbh <- "EST_MEX_IPUMS_1990_SUBNATIONAL_v5Q0_IHME.dta" #Mexico IPUMS 1990 by state
  mex.ipums2000.subnat.sbh <- "EST_MEX_IPUMS_2000_SUBNATIONAL_v5Q0_IHME.dta" #Mexico IPUMS 2000 by state
  mex.ipums2005.subnat.sbh <- "EST_MEX_IPUMS_2005_SUBNATIONAL_v5Q0_IHME.dta" #Mexico IPUMS 2005 by state
  mex.enadid1992.subnat.sbh <- "EST_MEX_ENADID_1992_SUBNATIONAL_v5Q0_IHME.dta" #Mexico ENADID 1992 Subnational 
  jor.1988.child.mort.survey.sbh <- "EST_jor_1988_child_mort_survey_nid7273_v5Q0_IHME.dta"  # Jordan 1988 Child mortality survey, NID 7273
  jor.1990.child.mort.survey.sbh <- "EST_jor_1990_child_mort_survey_nid7274_v5Q0_IHME.dta"  # Jordan 1990 Child mortality survey, NID 7274
  khm.socioec.survy.1997.sbh <- "EST_khm_socioeconomic_survey_1997_nid30907_v5Q0_IHME.dta"  # Cambodia 1997 Socioeconomic Survey, nid 30907
  khm.socioec.survy.1999.sbh <- "EST_khm_socioeconomic_survey_1999_nid30842_v5Q0_IHME.dta"  # Cambodia 1999 Socioeconomic Survey, nid 30842                                                                                                           
  nga.general.hh.surv.sbh <- "EST_nga_general_hh_survey_nid24890_v5Q0_IHME.dta"             # Nigera General Household Survey 2006, nid 24890
  mng.rhs.1998.report.sbh <- "EST_MNG_RHS_1998_REPORT_NID43106_v5Q0_IHME.dta"     # Mongolia RHS 1998 Report, NID 43106 
  mkd.mics.2011.report.sbh <- "EST_MKD_MICS_2011_report_nid91324_v5Q0_IHME.dta"   # Macedonia MICS 2011 report SBH, NID 91324
  vnm.pcfps.2010.sbh <- "EST_VNM_PCFPS_2010_v5Q0_IHME.dta"                        # Vietnam 2010 Population change and family planning survey 
  vnm.pcfps.2011.sbh <- "EST_VNM_PCFPS_2011_v5Q0_IHME.dta"                        # Vietnam 2011 Population change and family planning survey 
  hnd.living.conditions.2004.sbh <- "EST_HND_SURVEY_OF_LIVING_CONDITIONS_2004_v5Q0_IHME.dta"  # Honduras Survey of Living Conditions 2004 NID 5009
  zaf.oct.hh.survey.sbh <- "EST_ZAF_OCT_HH_SURVEY_v5Q0_IHME.dta"                  # South Africa October Household Surveys 1993-1995, 1997-1998
  pry.int.hh.1997.sbh <- "EST_pry_integrated_hh_survey_1997_v5Q0_IHME.dta"            # 1997-1998 PRY Integrated Household Survey
  pry.int.hh.2000.sbh <- "EST_pry_integrated_hh_survey_2000_v5Q0_IHME.dta"            # 2000-2001 PRY Integrated Household Survey
  zmb.mis.2012.sbh <- "EST_zmb_mis_2012_v5Q0_IHME.dta"                                # 2012 MIS for Zambia (limited use)
  kwt.child.health.1987.sbh <- "EST_kwt_1987_child_health_survey_v5Q0_IHME.dta"       # 1987 Kuwait Child Health Survey
  idn.census.2010.sbh <- "EST_IDN_CENSUS_2010_v5Q0_IHME.dta"                         # 2010 IDN census 
  bwa.fhs.1996.sbh <- "EST_BWA_FHS_1996_v5Q0_IHME.dta"                               # BWA FHS 1996
  ken.census.2009.sbh <- "EST_KEN_CENSUS_2009_v5Q0_IHME.dta" 	#Ken 2009 full census
  blz.census.1991.sbh <- "EST_BLZ_CENSUS_1991_v5Q0_IHME.dta"                         # BLZ census 1991
  bfa.census.2006.sbh <- "EST_BFA_CENSUS_2006_v5Q0_IHME.dta"                         # bfa census  IPUMS 2006
  cri.census.2011.sbh <- "EST_CRI_CENSUS_2011_v5Q0_IHME.dta"                         # cri census 2011
  cmr.census.2005.sbh <- "EST_CMR_CENSUS_2005_v5Q0_IHME.dta"                         # cmr census 2005
  kaz.census.2009.sbh <- "EST_KAZ_CENSUS_2009_v5Q0_IHME.dta"                         # kaz census 2009
  zwe.census.2012.sbh <- "EST_ZWE_CENSUS_2012_v5Q0_IHME.dta"                         # zwe census 2012
  uga.ais.2004.rep.sbh <- "EST_UGA_AIS_2004_2005_REPORT_v5Q0_IHME.dta"               # uga aids indicator survey 2004-2005 report
  cod.mics1995.data.sbh <- "EST_COD MICS 1995_v5Q0_IHME.dta"                         # COD mics 1995 sbh (prepped separately from the other MICSs)
  bwa.fhs.2007.2008.sbh <- "EST_BWA_FHS_2007_2008_SBH_v5Q0_IHME.dta"                  # BWA Family health survey 2007
  phl.dhs2011.sbh <- "EST_PHL_DHS6_2011_v5Q0_IHME.dta"                                # PHL 2011 DHS
  png.census2000.sbh <- "EST_PNG_CENSUS_2000_v5Q0_IHME.dta"                          # PNG 2000 Census
  khm.2003.socsurv.sbh <- "EST_30963#khm_socioeconomic_survey_2003_2005_v5Q0_IHME.dta" # KHM 2003-2005 socioeconomic survey  
  tza.census.2002.sbh <- "EST_TZA_CENSUS_2002_v5Q0_IHME.dta"                         # TZA census 2002
  kir.nonst.dhs.2009.sbh <- "EST_KIR_DHS_2009_nonstandard_v5Q0_IHME.dta"              # KIR nonstandard DHS 2009
  tgo.census.2010.sbh <- "EST_TGO_CENSUS_2010_v5Q0_IHME.dta"                          # TGO census 2010 sbh
  ury.census.2011.sbh <- "EST_URY_CENSUS_2011_v5Q0_IHME.dta"                          # URY census 2011
  fji.census.2007.sbh <- "EST_FJI_CENSUS_2007_v5Q0_IHME.dta"                          # FJI census 2007
  fsm.census.1994.sbh <- "EST_FSM_CENSUS_1994_v5Q0_IHME.dta"                          # FSM census 1994
  alb.census.2011.sbh <- "EST_ALB_2011_CENSUS_v5Q0_IHME.dta"                          # ALB census 2011
  zaf.ipums2001.subnat.sbh <- "EST_ZAF_IPUMS_CENSUS_2001_v5Q0_IHME.dta"	# South Africa Ipums census 2001 SBH
  bra.pnad.subnat.sbh <- "EST_PNAD_subnat_v5Q0_IHME.dta"		# Brazil PNAD subnational 2004, 2005, 2006, 2007, 2008, 2009 SBH
  aze.census.2009.sbh <- "EST_AZE_CENSUS_2009_v5Q0_IHME.dta" 	# Azerbaijan 2009 census SBH
  bra.ipums2010.subnat.sbh <- "EST_BRA_IPUMS_2010_subnat_v5Q0_IHME.dta"		#Brazil 2010 IPUMS Census subnational SBH
  ken.mics.subnat.sbh <- "EST_KEN_MICS_subnat_v5Q0_IHME.dta" 	# Kenya subnational MICS SBH 2008, 2009, 2011
  bra.census2000.subnat.sbh <- "EST_BRA_CENSUS_2000_v5Q0_IHME.dta" 	# Brazil 2000 census subnational SBH
  zaf.oct.hh.survey.subnat.sbh <- "EST_ZAF_OCT_HH_SURVEY_subnat_v5Q0_IHME.dta" 		# South Africa October Household Survey Sunational SBH 1993, 1994, 1995, 1997, 1998
  zaf.dhs.1998.subnat.sbh <- "EST_ZAF_DHS_subnat_v5Q0_IHME.dta" 	#South Africa 1998 DHS subnational SBH 	
  bra.pnad.1992.2013.subnat.sbh <- "EST_BRA_PNAD_1992_2013_v5Q0_IHME.dta"	#BRA subnational PNAD 1992, 1996, 1997, 1998, 1999, 2002, 2003 SBH
  zaf.ipums1996.subnat.sbh <- "EST_ZAF_IPUMS_CENSUS_1996_v5Q0_IHME.dta"		# South Africa 1996 IPUMS census subnational SBH
  bra.dhs.subnat.sbh <- "EST_BRA_DHS_subnat_v5Q0_IHME.dta"		# Brazil DHS 1986, 1996 subnational SBH
  zaf.ids1993.subnat.sbh <- "EST_ZAF_IDS_1993_v5Q0_IHME.dta"	# South Africa KwaZulu and Natal Income Dynamics Study 1993 subnational SBH
  bra.ipums.subnat.sbh <- "EST_BRA_IPUMS_CENSUS_v5Q0_IHME.dta"		# Brazil IPUMS census 1960, 1970, 1980, 1991 subnational SBH
  mex.census1980.subnat.sbh <- "EST_MEX.CENSUS.1980_v5Q0_IHME.dta"                   #MEX Census 1980 on subnational level
  mex.census1990.subnat.sbh <- "EST_MEX.CENSUS.1990_v5Q0_IHME.dta"                   #MEX Census 1990 on subnational level
  ken.ipums1979.subnat.sbh <- "EST_KEN_IPUMS_CENSUS_1979_v5Q0_IHME.dta"              #KEN IPUMS 1979 on subnational level
  ken.ais2007.sbh <- "EST_KEN_AIS_2007_v5Q0_IHME.dta"                                #KEN AIS 2007 on national level
  ken.ais2007.subnat.sbh <- "EST_KEN_AIS_2007_subnat_v5Q0_IHME.dta"                  #KEN AIS 2007 on subnational level
  ken.mics2007.subnat.sbh <- "EST_KEN_MICS_2007_v5Q0_IHME.dta"                       #KEN MICS 2007 on subnational level
  zaf.ipums2007.subnat.sbh <- "EST_ZAF_IPUMS_2007_v5Q0_IHME.dta"                     #ZAF IPUMS Census 2007 on subnational level
  zaf.lsms1993.subnat.sbh <- "EST_ZAF_LSMS_1993_v5Q0_IHME.dta"                       #ZAF LSMS 1993 on subnational level
  ken.mics2000.subnat.sbh <- "EST_KEN_MICS_2000_v5Q0_IHME.dta"                       #KEN MICS 2000 on subnational level
  tha.mics2012.sbh <- "EST_148649#THA_MICS_2012_report_v5Q0_IHME.dta" 	# Thailand 2012 MICS report SBH
  afg.nrva2011.sbh <- "EST_AFG_NRVA_SURVEY_2011_v5Q0_IHME.dta"		# Afghanistan 2011-2012 National Vulnerability Assessment Survey SBH
  lso.demographic.survey2011.sbh <- "EST_LSO_DEMOGRAPHIC_SURVEY_2011_v5Q0_IHME.dta"	# Lesotho 2011 Demographic Survey SBH
  tuv.dhs2007.sbh <- "EST_TUV_DHS_2007_v5Q0_IHME.dta"		#Tuvalu 2007 DHS SBH
  zmb.mis2006.sbh <- "EST_ZMB_MIS_2006_v5Q0_IHME.dta" 	# Sambia 2006 Malaria indicator survey  SBH
  pse.dhs2004.sbh <- "EST_PSE_DHS_2004_v5Q0_IHME.dta"		# Palestine Demographic Health Survey (not Macro DHS) 2004 SBH
  plw.census2000.sbh <- "EST_PLW_CENSUS_2000_REPORT_NID9969_v5Q0_IHME.dta" 	#Palau 2000 Census report SBH
  plw.census1995.sbh <- "EST_PLW_CENSUS_1995_REPORT_NID9968_v5Q0_IHME.dta" 	# Palau 1995 Census report SBH
  bdi.priority.survey1998.sbh <- "EST_BDI_PRIORITY_SURVEY_1998-1999_v5Q0_IHME.dta" #Burundi Priority Survey 1998-1999 SBH
  mdv.vpa1998.sbh <- "EST_MDV_VPA_REPORT_1998_NID141627_v5Q0_IHME.dta" 	# Maldives Vulnerability and Poverty Assessment 1997-1998 SBH
  zaf.dhs1987.sbh <- "EST_ZAF_DHS_1987_v5Q0_IHME.dta"		#South Africa 1987 DHS SBH
  nru.dhs2007.sbh <- "EST_NRU_DHS_2007_v5Q0_IHME.dta"	# Nauru 2007 DHS SBH
  lao.rhs2005.sbh <- "EST_LAO_RHS_2005_v5Q0_IHME.dta"	# Laos Reproductive Health Survey 2005 SBH
  pak.cps1993.sbh <- "EST_PAK_CPS_1993_report_nid105386_v5Q0_IHME.dta"	# Pakistan 1993 Contraceptive Prevalence Survey SBH
  mex.nns.subnat.sbh <- "EST_MEX_NNS_subnat_1988_v5Q0_IHME.dta"		#Mexico National Nutrition Survey 1988 Subnational SBH
  ken.ipums1969.subnat.sbh <- "EST_KEN_IPUMS_CENSUS_1969_subnat_v5Q0_IHME.dta"	# Kenya IPUMS census 1969 subnational SBH
  ken.dhs.subnat.sbh <- "EST_KEN_DHS_subnat_v5Q0_IHME.dta" 		# Kenya subnational DHS 1989, 1993, 1998, 2003, 2009 SBH
  ecu.ecv2005.sbh <- "EST_ECU_ECV_2005-2006_v5Q0_IHME.dta"	# Ecuador Living Conditions Survey 2005-2006 SBH
  ecu.ecv1998.sbh <- "EST_ECU_ECV_1998-1999_v5Q0_IHME.dta"	# Ecuador Living Conditions Survey 1998-1999 SBH
  nga.nlss2003.sbh <- "EST_NGA_NLSS_2003-2004_v5Q0_IHME.dta" 	# Nigeria Living Standards Survey 2003-2004 SBH
  mwi.mdicp2001.sbh <- "EST_MWI_MDICP_2001_v5Q0_IHME.dta"		# Malawi Diffusiont and ideational change project (MDICP) 2001 SBH
  mwi.mdicp1998.sbh <- "EST_MWI_MDICP_1998_v5Q0_IHME.dta"		# Malawi Diffusiont and ideational change project (MDICP) 1998 SBH	
  tza.tkap1994.sbh <- "EST_TZA_TKAP_1994_v5Q0_IHME.dta"		# Tanzania Knowledge, Attitudes, and Practices Survey 1994 SBH
  tha.cps1984.sbh <- "EST_THA_CPS_1984_v5Q0_IHME.dta"		# Thailand Contraceptive Prevalence Survey 1984 SBH
  tha.cps1981.sbh <- "EST_THA_CPS_1981_v5Q0_IHME.dta"		# Thailand Contraception Prevalence Survey 1981 SBH
  ecu.ensanut2012.sbh <- "EST_ECU_ENSANUT_2012_v5Q0_IHME.dta"	# Ecuador ENSANUT 2012 SBH
  mex.enigh2010.subnat.sbh <- "EST_MEX_ENIGH_2010_subnat_v5Q0_IHME.dta"		# Mexico Household Income and Expenditre Survey 2010 SBH
  mex.census2005.subnat.sbh <- "EST_MEX_CENSUS_2005_subnat_v5Q0_IHME.dta"	# Mexico 2005 census subnational SBH
  mex.census2000.subnat.sbh <- "EST_MEX_CENSUS_2005_subnat_v5Q0_IHME.dta"	# Mexico 2000 census subanational SBH
  ind.dhs1992.subnat.sbh <- "EST_IND_DHS_1992-1993_subnat_v5Q0_IHME.dta"	# India 1992-1993 DHS subnational SBH
  nic.ndhs2011.sbh <- "EST_NIC_DHS_2011_2012_v5Q0_IHME.dta"		# Nicaragua National Demographic and Health Survey 2011-2012 SBH
  bra.ndhs2006.sbh <- "EST_BRA_ndhs_2006_v5Q0_IHME.dta" 		#Brazil National Demographic and Health Survey of Children and Women 2006-2007 SBH  
  pse.hs2000.sbh <- "EST_PSE_HS_2000_v5Q0_IHME.dta" 	# Palestine Health Survey 2000 SBH
  ind.census2011.sbh <- "EST_IND_CENSUS_2011_v5Q0_IHME.dta"		# India 2011 census, national, state, and state-urban/rural SBH
  ind.dlhs2007.state.sbh <- "EST_IND_DLHS_2007_STATE_v5Q0_IHME.dta"   #India DLHS3 2007 survey at the state level SBH (national and state-urban/rural are above)
  ind.dhs2005.state.sbh <- "EST_IND_DHS_2005_state_v5Q0_IHME.dta" #India DHS 2005 at state level SBH (national level included above)
  ind.dhs2005.urbrur.sbh <- "EST_IND_DHS_2005_urban_rural_v5Q0_IHME.dta" #India DHS 2005 at state-urban/rural level SBH (national level included above)
  ind.dhs1998.state.sbh <- "EST_IND_DHS_1998_state_v5Q0_IHME.dta" #India DHS 1998 at state level (national level included above)
  ind.dhs1998.urbrur.sbh <- "EST_IND_DHS_1998_urban_rural_v5Q0_IHME.dta" #India DHS 1998 at state-urban/rural (national level included above)
  ind.hds2004.state.sbh <- "EST_IND_HDS_2004_state_v5Q0_IHME.dta" #India Human Development Survey 2004-2005 on state level (national level included above)
  ind.hds2004.urbrur.sbh <- "EST_IND_HDS_2004_urban_rural_v5Q0_IHME.dta" #India Human Development Survey 2004-2005 on state-urban/rural level (national level included above)
  ind.census2001.state.sbh <- "EST_IND_Census_2001_state_v5Q0_IHME.dta" #India Census 2001 at the state level (national and state-urban/rural included above)
  ken.wms1994.sbh <- "EST_KEN_WMS_1994_v5Q0_IHME.dta"		# Kenya Welfare Monitoring Survey II 1994 National and subnational SBH
  ind.dhs1992.state.sbh <- "EST_IND_DHS_1992_1993_state_v5Q0_IHME.dta" #India DHS 1992 at state  level 
  ind.dhs1992.urbrur.sbh <- "EST_IND_DHS_1992_1993_urban_rural_v5Q0_IHME.dta" #India DHS 1992 at state urban rural level 
  per.dhs2014.sbh <- "EST_PER_DHS_2014_v5Q0_IHME.dta"	#Peru 2014 DHS  SBH
  cok.census2011.sbh <- "EST_COK_Census_2011_v5Q0_IHME.dta"		# from UNICEF comparison
  dji.ds1991.sbh <- "EST_DJI_Demosurvey_1991_v5Q0_IHME.dta"		# from UNICEF comparison
  mwi.ffs1984.sbh <- "EST_MWI_FFS_1984_v5Q0_IHME.dta"		# from UNICEF comparison
  nru.census2011.sbh <- "EST_NRU_CENSUS_2011_v5Q0_IHME.dta"		# from UNICEF comparison
  vnm.pcfps2013.sbh <- "EST_VNM_PCFPS_2013_v5Q0_IHME.dta"		# from UNICEF comparison
  gin.ds1955.sbh <- "EST_GIN_Demosur_1954_1955_v5Q0_IHME.dta"		# from UNICEF comparison
  arm.census2011.sbh <- "EST_ARM_Census_2011_v5Q0_IHME.dta"		# from UNICEF comparison
  ind.census1981.sbh <- "EST_IND_CENSUS_1981_v5Q0_IHME.dta"		# from UNICEF comparison
  mhl.census2011.sbh <- "EST_MHL_CENSUS_2011_v5Q0_IHME.dta"		# MHL 2011 Census SBH
  rwa.census2012.sbh <- "EST_RWA_CENSUS_2012_v5Q0_IHME.dta"		# RWA 2012 Census microdata
  kir.census2010.sbh <- "EST_KIR_CENSUS_2010_v5Q0_IHME.dta"		# Kiribati 2010 census report
  nam.census2011.sbh <- "EST_NAM_CENSUS_2011_v5Q0_IHME.dta"		# Namibia 2011 census report
  slb.census2009.sbh <- "EST_SLB_CENSUS_2009_v5Q0_IHME.dta"		# Solomon islands 2009 census report
  mmr.census2014.sbh <- "EST_MMR_CENSUS_2014_v5Q0_IHME.dta"		# Myanmar 2014 census report
  ton.ndhs2012.sbh <- "EST_TON_NDHS_2012_v5Q0_IHME.dta"			# Tonga 2012 national DHS report
  vut.ndhs2013.sbh <- "EST_VUT_NDHS_2013_v5Q0_IHME.dta"			# Vanuatu 2013 national DHS report
  tur.ndhs2013.sbh <- "EST_TUR_NDHS_2013_2014_v5Q0_IHME.dta"	# Turkey 2013-2014 National DHS report
  aze.ndhs2011.sbh <- "EST_AZE_NDHS_2011_v5Q0_IHME.dta"			# Azerbaijan  2011 national DHS report
  bdi.ds1970.sbh <- "EST_BDI_DS_1970-1971_v5Q0_IHME.dta"		# Burundi Demographic survey report
  mwi.popsurvey1970.sbh <- "EST_MWI_POP_CHANGE_SURVEY_1970-1971_v5Q0_IHME.dta"	# Malawi population change survey 1970-1971 report
  png.census1971.sbh <- "EST_PNG_CENSUS_1971_v5Q0_IHME.dta"		# Papua New Guinea 1971 Census report
  hnd.edenh1972.sbh <- "EST_HND_EDENH_1971_1972_v5Q0_IHME.dta"		# Honduras 1971-1971 Demographic survey report 
  civ.ds1978.sbh <- "EST_CIV_DS_1978_1979_v5Q0_IHME.dta"		# Cote d'ivoire Demographic survey 1978-1979 report
  com.census1980.sbh <- "EST_COM_CENSUS_1980_v5Q0_IHME.dta"		# Comoros 1980 census report
  bgd.cps1981.sbh <- "EST_BGD_CPS_1981_v5Q0_IHME.dta"		# Banglades 1981 Contraceptive Prevalence Survey report
  cri.cps1981.sbh <- "EST_CRI_CPS_1981_v5Q0_IHME.dta"		# Costa rica 1981 Contraceptive Prevalence Survey
  jor.ds1981.sbh <- "EST_JOR_DS_1981_v5Q0_IHME.dta"		# jordan demographic survey 1981 report sbh
  per.cps1981.sbh <- "EST_PER_CPS_1981_v5Q0_IHME.dta"	# Peru contraceptive prevalence survey 1981 report
  hnd.edenh1983.sbh <- "EST_HND_EDENH_1983_v5Q0_IHME.dta"	# honduras national demographic sruvey 1983 report
  tun.cps1983.sbh <- "EST_TUN_CPS_1983_v5Q0_IHME.dta"		# tunisia contraceptive prevalence survey 1983 report
  egy.cps1984.sbh <- "EST_EGY_CPS_1984_v5Q0_IHME.dta"		# egypt contraceptive prevalence survey 1984 report
  hnd.nsmch1984.sbh <- "EST_HND_NSMCH_1984_v5Q0_IHME.dta"	# honduras national survey of maternal and child health report
  pak.cps1984.sbh <- "EST_PAK_CPS_1984_1985_v5Q0_IHME.dta"	# Pakistan contraceptive prevalence survey 1984-1985 report
  are.chs1987.sbh <- "EST_ARE_CHS_1987_1988_v5Q0_IHME.dta"	# United Arab Emirates Child health survey 1987-1988 report
  cub.nfs1987.sbh <- "EST_CUB_NFS_1987_v5Q0_IHME.dta"		# Cuba National Fertility survey 1987 report
  sau.chs1987.sbh <- "EST_SAU_CHS_1987_1988_v5Q0_IHME.dta"	# Saudi Arabia Child Health Survey 1987 report
  mrt.census1988.sbh <- "EST_MRT_CENSUS_1988_v5Q0_IHME.dta" # Mauritania 1988 census 
  tur.phs1988.sbh <- "EST_TUR_PHS_1988_v5Q0_IHME.dta"		# Turkey 1988 Population Health Survey 
  bhr.chs1989.sbh <- "EST_BHR_CHS_1989_v5Q0_IHME.dta"		# Bahrain 1989 Child Health Survey
  eth.ffs1990.sbh <- "EST_ETH_FFS_1990_1991_v5Q0_IHME.dta"	# Ethiopia National Family and Fertility Survey 1990
  mdv.census1990.sbh <- "EST_MDV_CENSUS_1990_v5Q0_IHME.dta"	# Maldives 1990 census 
  npl.nfhs1991.sbh <- "EST_NPL_NFHS_1991_v5Q0_IHME.dta"		# Nepal 1991 National Family Planning and Health Survey
  png.dhs1991.sbh <- "EST_PNG_DHS_1991_v5Q0_IHME.dta"		# papua New Guinea 1991 DHS
  lka.dhs1993.sbh <- "EST_LKA_DHS_1993_v5Q0_IHME.dta"		# Sri Lanka 1993 DHS
  png.dhs1996.sbh <- "EST_PNG_DHS_1996_1997_v5Q0_IHME.dta"	# papua New Guinea 1996-1997
  irq.icmms1999.sbh <- "EST_IRQ_ICMMS_1999_v5Q0_IHME.dta"	# Iraq Child and maternal mortality survey
  jor.afs1999.sbh <- "EST_JOR_AFS_1999_v5Q0_IHME.dta"		# Jordan 1999 Annual Fertility Survey
  slb.census1999.sbh <- "EST_SLB_CENSUS_1999_v5Q0_IHME.dta"		# Solomon Islands 1999 Census
  tkm.dhs2000.sbh <- "EST_TKM_DHS_2000_v5Q0_IHME.dta"		# Turkmenistan 2000 DHS 
  lso.ds2001.sbh <- "EST_LSO_DS_2001_v5Q0_IHME.dta"		# Lesotho 2001 Demographic Survey
  mng.rhs2003.sbh <- "EST_MNG_RHS_2003_v5Q0_IHME.dta"		#Mongolia 2003 Reporductive Health Survey
  gmb.cps1990.sbh <- "EST_GMB_CPS_1990_v5Q0_IHME.dta"		# Gambia 1990 Contraceptive Prevalence Survey
  cpv.census2000.sbh <- "EST_CPV_CENSUS_2000_v5Q0_IHME.dta"		# Cape verde 2000 census
  hnd.census2001.sbh <- "EST_HND_CENSUS_2001_v5Q0_IHME.dta"		# Honduras 2001 Census
  egy.dhssp2015.sbh <- "EST_EGY_DHS_SP_2015_v5Q0_IHME.dta"		# Egypt 2015 Special DHS 
  ind.dlhs4.sbh <- "EST_IND_DLHS4_2012_2014_v5Q0_IHME.dta"  # India DLHS4 2012-2014 sbh National/Subnational/Urban-Rural
  ind.ahs.2010.2013.sbh <- "EST_IND_AHS_2010_2013_v5Q0_IHME.dta" # India Annual Health Survey 2010-2013 subnational and urban/rural
  bgd.census2011.sbh <- "EST_BGD_CENSUS_2011_v5Q0_IHME.dta"	# Bangaldesh 2011 Census report SBH
  mex.int.census2015.sbh <- "EST_MEX_INT_CENSUS_SURVEY_2015_v5Q0_IHME.dta" 	# Mexico 2015 intercensal survey
  mex.enadid2014.sbh <- "EST_MEX_ENADID_2014_v5Q0_IHME.dta"		# Mexico ENADID 2014
  eri.phs2010.sbh <- "EST_ERI_PHS_2010_v5Q0_IHME.dta"   # Eritrea Population and Health Survey 2010 
  ken.dhs.province.sbh <- "EST_KEN_DHS_oldprovince_v5Q0_IHME.dta" #Kenya DHS at province level 
  ken.pmar32015.sbh <- "EST_KEN_PMA_2015_v5Q0_IHME.dta" #Kenya PMA r3 2015 
  uga.pmar32015.sbh <- "EST_UGA_PMA_2015_v5Q0_IHME.dta" #Uganda PMA r3 2015 
  gha.pmar342015.sbh <- "EST_PMA_GHA_2014_2015_v5Q0_IHME.dta" #Ghana PMA r3+r4 2014/2015
  idn.census.province.sbh <- "EST_IDN_CENSUS_PROVINCE_v5Q0_IHME.dta" # Indonesia Census data at the province level
  idn.dhs.province.sbh <- "EST_IDN_DHS_PROVINCE_v5Q0_IHME.dta" # Indonesia DHS data at the province level
  idn.efls.province.sbh <- "EST_IDN_EFLS_PROVINCE_v5Q0_IHME.dta" # Indonesia EFLS data at the province level
  idn.fls.province.sbh <- "EST_IDN_FLS_PROVINCE_v5Q0_IHME.dta" # Indonesia FLS data at the province level
  idn.supas.province.sbh <- "EST_IDN_SUPAS_PROVINCE_v5Q0_IHME.dta" # Indonesia SUPAS data at the province level
  idn.susenas.province.sbh <- "EST_IDN_SUSENAS_PROVINCE_v5Q0_IHME.dta" # Indonesia SUSENAS data at the province level
  mdg.mics.2012.sbh <- "EST_MDG_SOUTH_MICS_2012_v5Q0_IHME.dta" # Madagascar MICS 2012
 
#----------------------------------------------------------
#Complete birth history datasets

  cdcrhs.direct<-"EST_CDCRHS_v5Q0_IHME.dta"						              ## CDC REPRODUCTIVE HEALTH SURVEYS (CDC RHS)
  dhs.direct<-"EST_DHS_v5Q0_IHME_BY_SURVEY.dta"								                ## DEMOGRAPHIC AND HEALTH SURVEYS (DHS) includes WFS (DHS and WFS were pooled together for direct estimates)
  irndhs.direct<-"EST_IRNDHS_v5Q0_IHME.dta"        			     	      ## Iran DHS 
  ifhs.direct<-"EST_IFHS_v5q0_IHME.dta"						           	      ## Iraq Fertility and Health Survey
  tlsdhs.direct<-"EST_TLS_DHS_v5Q0_IHME_BY_SURVEY.dta"				       		      ## subset of IDN DHS for Timor Leste back in time
  papfam.direct<-"EST_PAPFAM_v5Q0_IHME.dta"					         	      ## PAPFAM countries complete birth histories (5 year blocks instead of 2)
  idncen.direct<-"EST_IDNcen_v5Q0_IHME.dta"							            # IDN 2000 census
  tls2003.direct<-"EST_TLS_2003_v5q0_IHME.dta"						          ## 2003 TLS survey complete birth histories (not DHS)
  papchild.direct<-"EST_PAPCHILD_direct_v5Q0_IHME.dta"              ## PAPCHILD complete birth histories
  enhogar.direct<-"EST_ENHOGAR_v5Q0_IHME.dta"                       ## complete birth histories from the ENHOGAR surveys in DOM
  mics.direct<-"EST_MICS_v5Q0_IHME.dta"                             ## MICS complete birth histories
  ais.direct <- "EST_AIS_v5Q0_IHME.dta"                             ## AIS complete birth histories
  mis.direct <- "EST_MIS_v5Q0_IHME.dta"                             ## MIS complete birth histories
  dhsindepth.direct <- "EST_DHS_IN_v5Q0_IHME.dta"                   ## DHS In-depth complete birth histories
  dhsspecial.direct <- "EST_DHS_SP_v5Q0_IHME.dta"                   ## DHS Special complete birth histories
  lsms.direct <- "EST_LSMS_v5Q0_IHME.dta"                           ## LSMS (ROU 1994 only) complete birth histories
  wfs.direct <- "EST_WFS_v5Q0_IHME.dta"                             ## WFS complete birth histories
  gf.direct <- "EST_GF_v5q0_IHME.dta"                               ## Global Fund HH Survey directs
  ifls.direct <- "EST_IFLS_v5q0_IHME.dta"                           # Indonesia Family Life Survey cbh  93, 97, 00, 07/8
  supas.direct <- "EST_SUPAS_direct_v5Q0_IHME.dta"                  ## SUPAS (Intercensal Population Survey) IDN cbh 1995, 2005
  tur.dhs.direct <- "EST_TUR_DHS_v5Q0_IHME.dta"                     ## TUR DHS 2008 (not macro) cbh
  ind.hds.direct <- "EST_IND_human_development_survey_04_05_v5Q0_IHME.dta" ## India Human Development Survey 04-05 cbh
  imira.direct <-"EST_IMIRA_v5q0_IHME.dta"					                    ## IRAQ: Iraq Multiple Indicator Rapid Assessment
  ind.dhs.urban.rural.direct <- "EST_IND_DHS_urban_rural_v5Q0_IHME_BY_SURVEY.dta" ## IND urban rural direct
  zaf.oct.hh.survey.direct <- "EST_ZAF_OCT_HH_SURVEY_v5Q0_IHME.dta"     ## South Africa October Household Survey 1993-1995, 1997-1998 direct
  bwa.fhs.2007.2008.direct <- "EST_BWA_FHS_2007_2008_v5Q0_IHME_22125.dta"
  pak.ihs.1998.1999.direct <- "EST_PAK_IHS_v5Q0_IHME.dta"		# Pakistan Integrated health survey 1998-1999 (LSMS)
  nic.endesa.2011.2012.direct <- "EST_NIC_ENDESA_2011-2012_v5Q0_IHME.dta"		# Nicaragua ENDESA 2011-2012
  pse.dhs.2004.direct <- "EST_PSE_DHS_2004_v5Q0_IHME.dta" 		# Palestine Demographic survey 2004 
  ecu.ensanut.2012.direct <- "EST_ECU_ENSANUT_2012_v5Q0_IHME.dta" 	# Ecuador National health and examination survey 2012
  bra.dhs.subnat.direct <- "EST_BRA_DHS_subnat_v5Q0_IHME.dta"		# Brazil subnational DHS 1986, 1991, 1996
  zaf.oct.hh.survey.subnat.direct <- "EST_ZAF_OCT_HH_SURVEY_subnat_v5Q0_IHME.dta"	# South Africa October Household survey subnational estimates
  ken.mics.subnat.direct <- "EST_KEN_subnat_MICS_v5Q0_IHME.dta"		# Kenya subnational MICS surveys 2011, 2008 and 2008 CBH
  ken.dhs.subnat.direct <- "EST_KEN_DHS_subnat_v5Q0_IHME.dta"		# Kenya subnational DHS 1988-1989, 1993, 1998, 2003, 2008-2009 CBH
  zaf.dhs.subnat.direct <- "EST_ZAF_DHS_subnat_v5Q0_IHME.dta"		# South Africa subnational 1998 DHS CBH
  mex.enadid.subnat.direct <- "EST_MEX_ENADID_1992_v5Q0_IHME.dta"	# Mexico National Survey of Demographic Dynamics 1992 subnational CBH
  pak.social.lsms.direct <- "EST_PAK_LSMS_v5Q0_IHME.dta"		# Pakistan Social and Living Standards Measurement Survey 2007-2008 , 2005-2006 CBH
  pak.ihs.direct <- "EST_PAK_IHS_2001-2002_v5Q0_IHME.dta" 	# Pakistan Integrated Household Survey 2001-2002 CBH
  per.dhs.2013.direct <- "EST_PER_DHS_v5Q0_IHME.dta"		#peru dhs 2013 CBH
  per.dhs.2014.direct <- "EST_PER_DHS_2014_v5Q0_IHME.dta"		#peru dhs 2014 CBH
  pse.hsurvey.2000.direct <- "EST_PSE_HEALTH_SURVEY_2000_v5Q0_IHME.dta"		# PSE 2000 MICS / HH survey CBH 
  idn.census.province.cbh <- "EST_IDN_CENSUS_PROVINCE_v5Q0_IHME.dta"
  idn.dhs.province.cbh <- "EST_IDN_DHS_PROVINCE_v5Q0_IHME.dta"
  idn.efls.province.cbh <- "EST_IDN_EFLS_PROVINCE_v5Q0_IHME.dta"
  idn.fls.province.cbh <- "EST_IDN_FLS_PROVINCE_v5Q0_IHME.dta"
  idn.supas.province.cbh <- "EST_IDN_SUPAS_PROVINCE_v5Q0_IHME.dta"
      
#######----------------------------------------------------------
#Aggregate q5 estimate datasets
  ined.agg<-"EST_INED_VARIOUS_1965-1989_v5q0.dta"			              # INED SURVEYS
  indbaht.agg<-"EST_IND_BHAT_SRS_1988-1994_v5Q0.dta"                # INDIA: SRS FROM MARI BAHT 1998 PAPER
  irnm.agg<-"EST_IRN_1965-2000_v5Q0_M.dta"                			# IRAN: ASSORTED DATA FROM COLLABORATOR
  irqkohli.agg<-"EST_IRQ_KOHLI_DSSSRS_1973-1974_v5Q0.dta"           # IRAQ: DSS/SRS FROM KOHLI 1976 PAPER
  omnhill.agg<-"EST_OMN_HILL_1960-1995_v5Q0.dta"                    # OMAN: ASSORTED DATA FROM HILL REPORT
  pngdhs.agg<-"EST_PNG_DHS_PRELIM_REPORT_2006_v5Q0.dta"             # PNG: DHS 1996 PRELIMINARY REPORT FROM COLLABORATOR
  ined.agg<-"EST_TABUTIN_INED_ALG_EGY_TUN_MAR_1965-1995_v5q0.dta"   # ALGERIA, EGYPT, MOROCCO, TUNISIA: INED DATA FROM TABUTIN 1991 PAPER
  ind90s.agg<-"USABLE_SRS_IND_1990-1997_v5q0.dta" 			            # INDIA: SRS 1990-1997
  ind8395.agg<-"EST_IND_SRS_1983_1995_v5Q0.dta"             		    # INDIA: SRS 1983-1995
  bgd0003.agg<-"EST_BGD_SRS_2000-2006_v5q0.dta"	 		                 # BGD: SRS 2001-2003
  ind07.agg<-"EST_IND_SRS_2007_v5q0.dta"
  vutcensus.agg <- "EST_VUT_CENSUS-SPC_1989-1999_v5Q0.dta" 		      # VUT CENSUS DATA
  dhsfinal.agg<-"EST_DHS_FINAL_REPORT_v5Q0.dta"                     # DHS Final Reports
  pbs.agg <- "EST_PSE_1990-2006_v5Q0.dta"                           # 5q0s extracted from "Maternal and child health in the occupied Palestinian territory" published 2009 in the Lancet from the Palestine Bureau of Statistics
  nam2001.agg<-"EST_NAM_CENSUS_2001_5q0.dta"
  bgdbbs.agg<-"EST_BGD_BBS_SRS_2006-2007_v5q0.dta"			            # BANGLADESH: SRS FROM BANGLADESH BUREAU OF STATISTICS
  susenas1996.agg<-"EST_SUSENAS_1996_v5q0.dta"						          # SUSENAS HH Deaths Module - 1996 survey
  susenas1998.agg<-"EST_SUSENAS_1998_v5q0.dta"						          # SUSENAS HH Deaths Module - 1998 survey
  susenas2000.agg<-"EST_SUSENAS_2000_v5q0.dta"						          # SUSENAS HH Deaths Module - 2000 survey
  transmonee.agg<-"EST_TransMONEE_EUR_1989-2012_v5Q0.dta"			      # 5q0 estimates from the TransMONEE database
  chnsur2011.agg<- "EST_CHN_MCHSS_1996-2013_5q0.dta"                # 5q0 from National Maternal and Child Health Surveillance System 00-10
  srsbgd.agg<- "EST_BGD_SRS_REPORT_v5Q0.dta"							          # 5q0 estimates from the BanglUSER SRS reports
  bencensus.agg<- "EST_BEN_CENSUS_1992_2000_v5q0.dta"               # 5q0s added from Benin Census
  afgmics2.agg<- "CRUDE_INT_MICS2_AFG_2003_v5q0.dta"                # One 5q0 estimate from MICS 2 AFG report (t=2003)
  vnmpcfps.agg<- "EST_PCFPS_VNM_HHDeaths_v5q0.dta"                  # 5q0 calculated from age specific deaths and pop (HH) Vietnam PCFPS Unadjusted
  mngstatyb.agg<- "EST_MNG_STATYB1999_1998_v5q0.dta"                # 5q0 From 1999 MNG Statistical Yearbook, for 1998 (report)
  mngstatyb1999.agg<- "EST_MNG_STATYB1999_1998_v5q0.dta"            # From MNG Stat YB 1999, for 1998 (Report)
  mngstatyb2001.agg<- "EST_MNG_STATYB2001_1999_v5q0.dta"            # 5q0 From 2001 MNG Stat YB, for 1999 (Report)
  mngstatyb2002.agg<- "EST_MNG_STATYB2002_2000-2002_v5q0.dta"       # 5q0 from 2002 MNG Staty YB, Points for 2000, 2001, 2002 (Report)
  mmrmoh.agg<- "EST_MOH_MMR_1991_1998_2003_5q0.dta"                 # From ministry of health data
  statcompiler.agg<- "EST_VAR_STATCOMPILER_v5q0.dta"                # Complete BH 5q0 from Statcompiler database not analyzed by IHME (Macro DHS, MIS, AIS, Special, Interim, and RHS Surveys).
  venenpofam.agg <- "EST_VEN_ENPOFAM_1998_v5q0.dta"                 # One datapoint from ENPOFAM report 1998
  yemstatreport.agg <- "EST_YEM_ANNUAL_REPORT_2006_2007_v5q0.dta"   # Two datapoints form YEM 2006-2007 Annual Stat Reports
  agoispw.agg <- "EST_AGO_ISPW_2008_v5q0.dta"                       # One datapoint from AGO 2008-2009 Integrated SVY on pop welfare
  chnu5svy.agg <-"EST_CHN_U5SVY_1991_v5q0.dta"                      # One point from 1991; Svy Conducted'92. CHN Survey on deaths of children under the age of 5.
  lkadhs.agg <-"EST_LKA_DHS_1993_2000_v5q0.dta"                     # DHS final reports for 2000 survey, 1993 survey.
  gulf.agg <- "CRUDE_EST_GLOBAL_GULF HEALTH SURVEYS_1991-1995_v5q0.csv" # 5q0 estimates from Gulf Family Health Surveys
  mics4.agg <- "EST_COG_GNB_MICS4_2010_REPORT_v5Q0.dta"             # GNB preliminary report from MICS4
  INDsrs2009.agg <- "EST_IND_SRS_2009_v5q0.dta"                     # SRS for 2009
  dza.vr.agg <- "EST_DZA_VR_2010_2011_REPORT_LIFETABLES_v5q0.dta"   # 5q0 estimates for DZA from the 2010/2011 VR reports
  irn.midhs2010.from.m.agg <- "EST_IRN_2010_MIDHS_from_M.DTA" 		# IRN: MIDHS 2010 5q0 estimates from collaborator
  chn.nsscm.91.agg <- "EST_CHN_NSSCM_1991_5q0.dta"                  # CHN National Sample Survey on Child Mortality 1991
  bwa.fhs.07.agg <- "EST_BWA_FHS_2007_5q0.dta"                      # BWA FHS 2007 report
  bwa.ds.06.agg <- "EST_BWA_DS_2006_5q0.dta"                        # BWA DS 2006 report indirect estimates
  cpv.dhs.05.agg <- "EST_CPV_DHS_2005_5q0.dta"                      # Cape Verde (CPV) DHS 2005 - only report available
  wsm.dhs.09.agg <- "EST_WSM_DHS_Report_2009_5q0.dta"               # WSM DHS 2009 - only report so far 
  vnm.hsy.08.agg <- "EST_VNM_HSY_2008_5q0.dta"                      # VNM Health Statistics Yearbook 2008
  gmb.mics4.10.agg <- "EST_GMB_MICS4_2010_report_5q0.dta"           # Gambia 2010 MICS4 preliminary report
  lso.census.06.agg <- "EST_LSO_CENSUS_2006_5q0.dta"                # LSO 2006 Census HH point
  mmr.mics.09.agg <- "EST_MMR_MICS_2009-2010_5q0.dta"               # MMR MICS 2009-2010 report
  pak.ds.agg <- "EST_PAK_DS_1991-2005_5q0.dta"                      # PAK Demographic Survey Reports 91, 92, 95, 96, 97, 99, 03, 05
  syr.mics2.agg <- "EST_SYR_MICS2_report_2000_5q0.dta"              # SYR MICS2 report
  yem.dhs1997.agg <- "EST_YEM_DHS_1997_REPORT_ESTIMATES.dta"        # YEM DHS 1997 report
  gin.dhs1992.agg <- "EST_GIN_DHS_1992_REPORT_ESTIMATES.dta"        # GIN DHS 1992 report
  sdn.mics2000.agg <- "EST_SDN_MICS_2000_REPORT_ESTIMATES.dta"      # SDN MICS 2000 report
  nga.dhs1999.agg <- "EST_NGA_DHS_1999_REPORT_ESTIMATES.dta"        # NGA DHS 1999 report
  ner.mics1996.agg <- "EST_NER_MICS_1996_REPORT_ESTIMATES.dta"      # NER MICS 1996 report
  sen.mics2000.agg <- "EST_SEN_MICS_2000_REPORT_5q0.dta"            # SEN MICS 2000 report
  lka.dhs20062007.agg <- "EST_LKA_DHS_2006_2007_PRELIM_REPORT_5q0.dta" #LKA DHS 2006-2007 Prelim report (final report not in system)
  dza.mics2000.agg <- "EST_DZA_MICS_2000_REPORT_ESTIMATES_5q0.dta"      # DZA MICS 2000 report
  dza.mics1995.agg <- "EST_DZA_MICS_1995_REPORT_ESTIMATES_5q0.dta"      # DZA MICS 1995 report  
  egy.dhs1997.agg <- "EST_EGY_DHS_1997_1998_REPORT_5q0.dta"         # EGY DHS 1997 report
  egy.dhs1998.agg <- "EST_EGY_DHS_1998_REPORT_5q0.dta"              # EGY DHS 1998 report
  mwi.mics1995.agg <- "EST_MWI_MICS_1995_REPORT_5q0.dta"            # MWI MICS 1995 report
  moz.mics1995.agg <- "EST_MOZ_MICS_1995_REPORT_5q0.dta"            # MOZ MICS 1995 report
  jor.rhs1983.agg <- "EST_JOR_RHS_1983_REPORT_5q0.dta"              # JOR RHS 1983 report
  hnd.rhs1987.agg <- "EST_HND_RHS_1987_REPORT_5q0.dta"              # HND RHS 1987 report
  geo.rhs2010.agg <- "EST_GEO_RHS_report_2010_5q0.dta"              # GEO RHS 2010 report
  chn.prov.mchs19962012.agg <- "EST_CHN_PROV_MCHS_1996_2012_5q0.dta"  # CHN MCHS 1996-2012
  chn.prov.moh19962012.agg <- "EST_CHN_PROV_MOH_1996_2012_5q0.dta"  # CNH MOH 1996-2012
  chn.moh19962012.agg <- "EST_CHN_MOH_1996_2012_5q0.dta"            # CHN MOH 1996-2012
  mng.rhs.1998.report.agg <- "MNG_RHS_1998_REPORT_NID43106.dta"     # MNG RHS 1998 report (CBH)
  sdn.papfam.2006.report.agg <- "SDN_PAPFAM_2006_REPORT_NID24143.dta"   # SDN PAPFAM 2006 (Household Health Survey)
  tjk.inf.child.2010.agg <- "TJK_SURVEY_ON_INFANT_CHILD_AND_MATERNAL_MORTALITY_2010.dta"     # TJK survey on infant and child mortality 2010
  mar.enpsf.2011.agg <- "EST_MAR_ENPSF_2011_REPORT_v5Q0.dta"        # MAR ENPSF 2011 cbh aggregate from report
  ind.urb.rur.srs.19831995.agg <- "EST_IND_URBAN_RURAL_SRS_1983_1995_5q0.dta"  # IND urban/rural estimates from SRS
  mar.multiround.survey.2009.agg <- "EST_MAR_MDSE_2009_q5.dta"       # MAR multi-round demographic survey 2009
  gnq.dhs.2011.report.agg <- "EST_GNQ_DHS_2011_q5.dta"              #report from GNQ 2011 dhs
  zaf.rms2012.agg <- "EST_ZAF_RAPID_MORTALITY_REPORT_2012.dta"      #ZAF RMS 2012 Report, estimates for 2009-2012
  sdn.mics2010.final.agg <- "EST_SDN_MICS4_2010_REPORT.dta"         # SDN MICS4 final report aggregate
  chn.prov.mchs.prov20002012.agg <- "EST_CHN_PROV_MCHS_PROV_2000_2014_5q0.dta"   # MCHS gave us provincial estimates in addition to the MCHS ones we aggregate to province by strata
  kir.nonst.dhs.2009.agg <- "EST_KIR_DHS_2009_nonstandard_5q0.dta"  # KIR non-standard 2009 report CBH
  aze.rhs2001.agg <- "EST_AZE_CDC_RHS_2001_v5Q0.dta"	# AZE CDC-RHS 2001 report
  bfa.census.96.agg <- "EST_BFA_CENSUS_1996_v5Q0.dta"	# BFA census report 1996
  bfa.census.06.agg <- "EST_BFA_CENSUS_2006_v5Q0.dta"	# BFA census report  2006
  bfa.ds.91.agg <- "EST_BFA_DS_1991_v5Q0.dta"	# BFA demographic survey 1991 report
  lao.rhs.2005.agg <- "EST_LAO_RHS_2005_v5Q0.dta" 	# Laos 2005 Reproductive health survey report 
  tuv.dhs.2007.agg <- "EST_TUV_DHS_2007_v5Q0.dta"	# Tuvalu DHS 2007 report
  mda.mics.2012.agg <- "EST_MDA_MICS_2012_v5Q0.dta" 	# Moldova MICS 2012 report
  irq.lcs.2004.agg <- "EST_IRQ_LCS_2004_v5Q0.dta"	# Iraq Living conditions survey report
  gha.dhs.2014.agg <- "EST_GHA_DHS_2014_v5Q0.dta"
  lso.dhs.2014.agg <- "EST_LSO_DHS_2014_v5Q0.dta"
  ben.mics.2014.agg <- "EST_BEN_MICS_2014_v5Q0.dta"
  dom.mics.2014.agg <- "EST_DOM_MICS_2014_v5Q0.dta"
  gnb.mics.2014.agg <- "EST_GNB_MICS_2014_v5Q0.dta"
  guy.mics.2014.agg <- "EST_GUY_MICS_2014_v5Q0.dta"
  sdn.mics.2014.agg <- "EST_SDN_MICS_2014_v5Q0.dta"
  slv.mics.2014.agg <- "EST_SLV_MICS_2014_v5Q0.dta"
  swz.mics.2014.agg <- "EST_SWZ_MICS_2014_v5Q0.dta"
  mng.mics.2013.agg <- "EST_MNG_MICS_2013_v5Q0.dta"		## Mongolia MICS 2013 preliminary report
  dza.mics.2013.agg <- "EST_DZA_MICS_2012-2013_v5Q0.dta"		## Algeria MICS  2012-2013  report
  bwa.census.2011.agg <- "EST_BWA_CENSUS_2011_v5Q0.dta"		# Botswana 2011 Census Analytical report 
  rwa.dhs2014.agg <- "EST_RWA_DHS_2014_2015_v5Q0.dta"		# Rwanda 2014-2015 DHS preliminary report
  lby.mics2003.agg <- "EST_LBY_MICS_2003_v5Q0.dta"		# Libya 2003 MICS report aggregate point
  mrt.mics1996.agg <- "EST_MRT_MICS_1996_v5Q0.dta"		# Maurtania 1996 MICS report aggregate point
  mdg.mics1995.agg <- "EST_MDG_MICS_1995_v5Q0.dta"		## Madagascar 1995 MICS report aggregate point
  ind.healthsurvey2010.agg <- "EST_IND_ANNUAL_HEALTH_SURVEY_2010_2011_v5Q0.dta" # India annual health survey 2010-2011
  ind.healthsurvey2011.agg <- "EST_IND_ANNUAL_HEALTH_SURVEY_2011_2012_v5Q0.dta" # India annual health survey 2011-2012
  ind.healthsurvey2012.agg <- "EST_IND_ANNUAL_HEALTH_SURVEY_2012_2013_v5Q0.dta" # India annual health survey 2013-2013
  tur.dhs.2013_2014.agg <- "EST_TUR_DHS_2013_2014_v5Q0.dta" ## Turkey DHS 2013-2014 report
  lbn.mics.2009.agg <- "EST_LBN_MICS_2009_v5Q0.dta" ## Lebanon MICS 2009 report
  are.statyb2013.agg <- "EST_ARE_STATYB_2013_v5Q0.dta" 	## United Arab Emirates VR from Stat yearbook 2009-2012
  ton.ndhs2012.agg <- "EST_TON_NDHS_2012_v5Q0.dta"		# Tonga 2012 national DHS aggregate point
  vut.ndhs2013.agg <- "EST_VUT_NDHS_2013_v5Q0.dta"		#Vanuatu 2013 national DHS aggregate point
  aze.ndhs2012.agg <- "EST_AZE_NDHS_2011_v5Q0.dta"		# Azerbaijan 2011 National DHS aggregate point
  pak.census1981.agg <- "EST_PAK_CENSUS_1981_v5Q0.dta" 		# Pakistan 1981 Census report SBH aggregate points
  hti.emmus1987.agg <- "EST_HTI_EMMUS_1987_v5Q0.dta"		# Haiti EMMUS 1987 aggregate points
  btn.nhs1994.agg <- "EST_BTN_HS_1994_v5Q0.dta"			# Bhutan National Health Survey 1994 aggregate points
  tcd.dhs2014.agg <- "EST_TCD_DHS_2014_2015_v5Q0.dta"	# Chad 2014-2015 DHS report aggregate points
  bhr.chs1989.agg <- "EST_BHR_CHS_1989_v5Q0.dta"		# Bahrain 1989 Child Health Survey aggregate points
  npl.nfhs1991.agg <- "EST_NPL_NFHS_1991_v5Q0.dta"		# Nepal 1991 national family planning and health survey
  mex.enadid1992.agg <- "EST_MEX_ENADID_1992_v5Q0.dta"	# Mexico ENADID 1992 aggregate points
  lka.dhs1993.agg <- "EST_LKA_DHS_1993_v5Q0.dta"		# Sri Lanka 1993 DHS aggregate points
  png.dhs1996.agg <- "EST_PNG_DHS_1996_1997_v5Q0.dta"	# Papua New Guinea 1996-1997 DHS aggregate points
  irq.icmms1999.agg <- "EST_IRQ_ICMMS_1999_v5Q0.dta"	# iraq child and maternal mortality survye 1999 aggregate points
  lbn.mics2000.agg <- "EST_LBN_MICS_2000_v5Q0.dta"		# Lebanon 2000 MICS indirect aggregate points
  tkm.dhs2000.agg <- "EST_TKM_DHS_2000_v5Q0.dta"		# Turkmenistan 2000 DHS aggregate points
  gmb.cps1990.agg <- "EST_GMB_CPS_1990_v5Q0.dta"		# Gambia 1990 Contraceptive Prevalence Survey aggregate points
  irq.cmns1991.agg <- "EST_IRQ_CMNS_1991_v5Q0.dta"		# Iraq international study team 1991 aggregate points
  sau.ltdicm1990.agg <- "EST_SAU_LTDICM_1990_v5Q0.dta"		# Saudi Arabia levels, trends and differentials of infant and child mortality 1990 indirect aggregate points
  gtm.dhs2015.agg <- "EST_GTM_2014_2015_DHS_v5Q0.dta"		# Guatemala 2014-2015 DHS report CBH
  ind.srs.1995_2013.agg <- "EST_IND_SRS_1995_2013_v5Q0.dta" # India SRS statistical reports 1995-2013 aggregate points 
  cog.mics5.2014_2015.agg <- "EST_COG_MICS5_2014_2015_v5Q0.dta" # Congo 2014-2015 MICS5 aggregate estimates
  omn.mics5.2014.agg <- "EST_OMN_MICS5_2014_v5Q0.dta" # Oman 2014 MICS5 aggregate estimates
  ind.dhs.2015.agg <- "EST_IND_DHS_2016_5q0.dta"	# India subnational DHS 2015-2016 direct aggregate estimates # updated 3/20/2017 by USER
  bgd.srs.2012_2014.agg <- "EST_BGD_SRS_2012-2014_v5Q0.dta"	# BanglUSER SRS 2012-2014 report direct aggregate estimates 
  arm.dhs.2015_2016.agg <- "EST_ARM_DHS_2015-2016_v5Q0.dta" #arm 2015-2016 dhs
  ago.dhs.2015_2016.agg <- "EST_AGO_DHS_2015-2016_v5Q0.dta" #ago dhs 2015-2016 
  eth.dhs.2016.agg <- "EST_ETH_2016_DHS_PRELIM_REPORT_5q0.dta" #eth dhs 2016 
  ind.srs.2014_2015.agg <- "EST_IND_SRS_2014_2015_5q0.dta" #ind srs for 2014 and 2015  # this is now being brought in the VR file 
#----------------------------------------------------------
#household deaths from single surveys and censuses adjusted with growth balance methods

  childmortality.ssc<-"04. ssc 5q0.dta"
  ifhs.ssc<-"IFHS pre and post conflict/04. IFHS adjusted 5q0.dta"
  
#----------------------------------------------------------
#Update VR

  pop.dir <- "FILEPATH/data"
  vr.dir <- "FILEPATH/data"
  births.dir <- "FILEPATH/results/"

  vr.file <-  "/d00_compiled_deaths.dta"
  pop.file <- "/d09_denominators.dta"

  births.file <- "births_gbd2016.dta" 
  ind.srs.births.file <- "FILEPATH/IND_SRS_births_Back_calculated.csv"
  ind.srs.births.urb.rur <- "FILEPATH/SRS_urban_rural_births_for_combineallchild.csv"

# ----------------------------------------------------------------------------
# Read in dataset for consistent country names
# ----------------------------------------------------------------------------

  country.to.iso3 = read.csv(paste(agg.data.dir,iso3.data,sep=""), stringsAsFactors = FALSE)


# ----------------------------------------------------------------------------
# 00. Add in collaborator's old dataset as framework
# ----------------------------------------------------------------------------

  data<-read.dta(paste(agg.data.dir,t.data,sep=""), convert.underscore=TRUE)

  # Merge in iso3 codes
  nrow(data)
  data = merge(unique(country.to.iso3[,c("country", "iso3")]), data, all.y = TRUE)
  nrow(data)
  index = is.na(data$iso3)
  unique(data[index, "country"])

  temp = unique(data[,c("iso3", "country")])
  colnames(temp)[colnames(temp) == "country"] = "laxo.country"
  country.to.iso3 = merge(country.to.iso3, temp, all = TRUE)
  index = is.na(country.to.iso3$laxo.country)
  unique(country.to.iso3[index, "country"])

  data = data[,colnames(data) != "country"]
  data$source = tolower(data$source)

  # add in variables that will be populated in the CBH section
  data$sd.q5 <- NA
  data$log10.sd.q5 <- NA
  data$ihme.loc.id <- NA
  data$NID <- NA

  data$in.direct[data$iso3=="MDV" & data$source=="census" & data$data.age=="old"] <- "indirect"
  data$source[data$iso3=="MDV" & data$source=="ministry of health vital statistics"] <-"ministry of health vital statistics - vital registration"


# ----------------------------------------------------------------------------
# 01. Add in summary birth history estimates
# ----------------------------------------------------------------------------

  # ----------------------------------------------------------------------------
  # RANDOM SOURCES

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,random.sbh,sep=""), convert.underscore=TRUE)

  # Remove old data from BGD Retrospective Survey of Fertility and Mortality
  temp = subset(data, iso3=="BGD")
  temp2 = subset(temp, source != "retrospective survey of fertility and mortality 1974")
  data = rbind(temp2, subset(data,iso3!="BGD"))
  # we don't want to add in WFS because we've also analyzed this in the WFS folder
  add.data <- add.data[add.data$source != "NATIONAL FERTILITY SURVEY (WFS),  OCTOBER 1974",]

  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # CDC REPRODUCTIVE HEALTH SURVEYS (CDC RHS) SUMMARY BIRTH HISTORY ESTIMATES

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,cdcrhs.sbh,sep=""), convert.underscore=TRUE)

  # The following code eliminates duplicates
  temp = subset(data, iso3=="GEO")
  temp2 = subset(temp, source!="reproductive maternal and child health")
  data = rbind(temp2, subset(data, iso3!= "GEO"))

  temp = subset(data, iso3=="ALB")
  temp2 = subset(temp, source!="rhs-cdc")
  data = rbind(temp2, subset(data, iso3!= "ALB"))

  temp = subset(data, iso3=="CPV")
  temp2 = subset(temp, source!="dsr")
  data = rbind(temp2, subset(data, iso3!= "CPV"))

  temp = subset(data, iso3=="ECU")
  temp2 = subset(temp, source!="eds")
  data = rbind(temp2, subset(data, iso3!= "ECU"))

  temp = subset(data, iso3=="SLV")
  temp2 = subset(temp, source!="ens")
  data = rbind(temp2, subset(data, iso3!= "SLV"))

  temp = subset(data, iso3=="GTM")
  temp2 = subset(temp, source!="rhs-cdc")
  data = rbind(temp2, subset(data, iso3!= "GTM"))

  temp = subset(data, iso3=="HND")
  temp2 = subset(temp, source!="encuesta nacional de epidemiología y salud familiar 1996")
  data = rbind(temp2, subset(data, iso3!= "HND"))

  temp = subset(data, iso3=="NIC")
  temp2 = subset(temp, source!="rhs-cdc")
  data = rbind(temp2, subset(data, iso3!= "NIC"))

  temp = subset(data, iso3=="PRY")
  temp2 = subset(temp, source!="endsr")
  temp2 = subset(temp2, source!="rhs-cdc")
  data = rbind(temp2, subset(data, iso3!= "PRY"))

  nrow(data)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # DEMOGRAPHIC AND HEALTH SURVEYS (DHS) INDIRECTS

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,dhs.sbh,sep=""), convert.underscore=TRUE)

  data = subset(data, !source %in% data$source[grep("(dhs)|(demographic and health)", tolower(data$source))] | !iso3 %in% add.data$iso3 | !in.direct == "indirect")
  nrow(data)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # DYB TABULATED CENSUS

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,dybtab.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # MULTIPLE INDICATOR CLUSTER SURVEYS (MICS)

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,mics.sbh,sep=""), convert.underscore=TRUE)
  data = subset(data, !(source %in% data$source[grep("(mics)|(multiple indicator cluster)", tolower(data$source))] & iso3 %in% add.data$iso3 & in.direct %in%
  data$in.direct[grep("(indirect)|(indirectstar)", tolower(data$in.direct))] ) );   nrow(data)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # WORLD FERTILITY SURVEYS (WFS)

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,wfs.sbh,sep=""), convert.underscore=TRUE)
  data = subset(data, !(source %in% data$source[grep("(world)|(wolrd)|(egyptian fertility survey)|(ghana fertility survey)|(jamaica fertility survey)|(jamica fertility survey)|(turkey fertility survey)|(WFS)", tolower(data$source))] & iso3 %in% add.data$iso3 & in.direct == "indirect")); nrow(data)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # CHINA: CENSUS 1990-2000

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,chncensus.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # ----------------------------------------------------------------------------
  # CHINA: CENSUS 2010

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,chn2010census.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # INDIA: DLHS

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,inddlhs.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # INDONESIA: SUSENAS

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,susenas.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # IRAQ: IFHS

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,ifhs.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # TLS DHS: Timor Leste region of IDN back in time; also new TLS DHS

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,tlsdhs.sbh,sep=""), convert.underscore=TRUE)
  # Want to classify the old points (TLS region of IDN) as nonstandard DHS
  add.data$source[add.data$source.date < 2003] <- "TLS_subIDN_DHS"
  data = append.data(data, add.data)
  head(data)

  # ------------------------------------------------------------------------------
  # LSMS summary birth histories

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,lsms.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ------------------------------------------------------------------------------
  # PAPFAM summary birth histories

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,papfam.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-----------------------------------------------------------------------------
  #2004 Morocco Census summary birth history

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,marcensus.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-----------------------------------------------------------------------------
  #2004 Bhutan Census summary birth history

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,bhutancensus.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-----------------------------------------------------------------------------
  #2004 Morocco Census summary birth history

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,kiribaticensus.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-----------------------------------------------------------------------------
  #2000 IDN census summary birth history

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,idncen.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-----------------------------------------------------------------------------
  #2006 IRN census summary birth history

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,irncen.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-----------------------------------------------------------------------------
  # 2000 IRN DHS

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,irndhs.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-----------------------------------------------------------------------------
  # MICS2 AFG 2000

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,afgmics2.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # IPUMS

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,ipums.sbh,sep=""), convert.underscore=TRUE)

  # Remove census estimates that are computed from tabulated census data since we are adding in these estimates from census microdata
  data$iso3.sourcedate = paste(data$iso3, data$source.date, sep=" ")	# creates a combined field for country and year of census so we don't take out ALL the tabulated census numbers from a country, just the ones that are getting replaced with IPUMS data
  add.data$iso3.sourcedate = paste(add.data$iso3, add.data$source.date, sep=" ")

  data = subset(data, !(source %in% data$source[grep("(census)|(conteo)", tolower(data$source))] & iso3.sourcedate %in% add.data$iso3.sourcedate & in.direct %in% data$in.direct[grep("(indirect)|(indirect, mac only)", tolower(data$in.direct))])); nrow(data)

  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # AFG AHS 2006 summary birth histories

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,afgahs.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # MIS summary birth histories

  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,mis.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ------------------------------------------------------------------------------
  # PAPCHILD summary birth histories

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,papchild.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ------------------------------------------------------------------------------
  # PAPCHILD summary birth histories

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,dlhs3.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ------------------------------------------------------------------------------
  # SUPAS summary birth histories

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,supas.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ------------------------------------------------------------------------------
  # STP MICS3 summary birth histories

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,stpmics3.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ------------------------------------------------------------------------------
  # MMR FRHS summary birth histories

  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mmrfrhs.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)


  # ----------------------------------------------------------------------------------
  # Sri Lanka 2001 Census summary birth histories
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,lkacensus.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #------------------------------------------------------------------------------------
  # SOM MICS2 1999 summary birth histories
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,sommics2.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-------------------------------------------------------------------------------------
  # DHS Special summary birth histories
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,dhsspecial.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -------------------------------------------------------------------------------------
  # DHS Interim summary birth histories
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,dhsinterim.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -------------------------------------------------------------------------------------
  # DHS In-depth summary birth histories
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,dhsindepth.sbh,sep=""), convert.underscore=TRUE)
    add.data <- add.data[add.data$iso3 != "PHL" & add.data$source.date != 1993,] 
    if (nrow(add.data)!=0) {
      data = append.data(data, add.data)
    } 
  head(data)

  # -------------------------------------------------------------------------------------
  # AIS summary birth histories
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ais.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -------------------------------------------------------------------------------------
  # PNAD Brazil summary birth histories
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,pnad.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------------
  # Vietnam 2008 Population Change and Family Planning Survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,vnmpcfps.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------------
  # Saudi Arabia 2007 Demographic Bulletin
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, saubulletin.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # ----------------------------------------------------------------------------------
  # Mexico 2010 Census microdata - subnational
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, mex2010cen.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # India 2001 Census tabulated
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, ind2001cen.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # Pakistan social and living standards measurement survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, paksocial.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # Pakistan social and living standards measurement survey - MAC only 1998-1999
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, paksocial99.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # Brazil 2010 census
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, bra2010census.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
  # -----------------------------------------------------------------------------------
  # IDN IFLS 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, idn.ifls.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 
  
  # -----------------------------------------------------------------------------------
  # TUR DHS 2008 (non - macro) 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, tur.dhs.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 
  
  # -----------------------------------------------------------------------------------
  # TUR 2011 Infant Mortality Survey 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, tur.ims.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 
  
  # -----------------------------------------------------------------------------------
  # TUR 1989 Demographic Survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, tur.89ds.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # AFG 2008 National Risk and Vulnerability Assessment Survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir, afg.natl.risk.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # VUT 2009 Census from the National Statistics Office download
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,vut2009cen.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # Ecuador 2010 Census
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ecu2010cen.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # Bolivia 2000 HH Survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,bol2000surv.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # Botswana MICS 2000 tabulated data
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,bwa.mics2000.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # FSM census 2000 tabulated data
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,fsm2000census.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # FSM census 1973 tabulated data
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,fsm1973census.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # MEX ENIGH 2010 Microdata
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mexenigh2010.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # MEX ENADID 2006 Microdata
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mexenadid2006.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # India Human Development Survey 04-05 Microdata
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ihds.0405.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # Iraq IMIRA 2004
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,imira.2004.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
 
  # -----------------------------------------------------------------------------------
  # SDN Southern HHS 2010 (South Sudan)
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ssd_hhs_2010.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  

  # -----------------------------------------------------------------------------------
  # CPV DHS 2005 report sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,cpv.dhs2005.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  

  # -----------------------------------------------------------------------------------
  # AGO 1996 MICS report sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ago.mics1.1996.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # RWA EICV 2006 report sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,rwa.eicv.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
  # -----------------------------------------------------------------------------------
  # CHN provincial level sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,chn.prov.census82.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # CHN provincial level sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,chn.prov.census90.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------------
  # CHN provincial level sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,chn.prov.census00.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # CHN provincial level sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,chn.prov.census10.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # IND urban rural DHS sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ind.dhs.urbrur.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
 
  # -----------------------------------------------------------------------------------
  # IND urban rural DLHS sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ind.dlhs.urbrur.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------------
  # IND urban rural DLHS sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ind.dlhs3.urbrur.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
    
  # -----------------------------------------------------------------------------------
  # IND urban rural 2001 Census sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ind.census01.urbrur.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
  
  # -----------------------------------------------------------------------------------
  # IND urban rural 1991 Census sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ind.census91.urbrur.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
  
  # -----------------------------------------------------------------------------------
  # IND urban rural 1981 Census sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ind.census81.urbrur.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)      

  # -----------------------------------------------------------------------------------
  # Jordan 1988 child mortality survey sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,jor.1988.child.mort.survey.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)          
  
  # -----------------------------------------------------------------------------------
  # Jordan 1990 child mortality survey sbh
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,jor.1990.child.mort.survey.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)        
  
  # -----------------------------------------------------------------------------------
  # Cambodia 1997 Socioeconomic Survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,khm.socioec.survy.1997.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
    
  # -----------------------------------------------------------------------------------
  # Cambodia 1999 Socioeconomic Survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,khm.socioec.survy.1999.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
  
  # -----------------------------------------------------------------------------------
  # Nigeria 2006 General Household Survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,nga.general.hh.surv.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)   
  
      
# -----------------------------------------------------------------------------------
#  MEX ENADID 2006 subnational 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mex.enadid2006.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

# -----------------------------------------------------------------------------------
#  MEX ENADID 2009 subnational 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mex.enadid2009.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

# -----------------------------------------------------------------------------------
#  MEX IPUMS Census 1990 subnational 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mex.ipums1990.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

# -----------------------------------------------------------------------------------
#  MEX IPUMS Census 2000 subnational 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mex.ipums2000.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

# -----------------------------------------------------------------------------------
#  MEX IPUMS Census 2005 subnational 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mex.ipums2005.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

# -----------------------------------------------------------------------------------
#  MEX ENADID 1992 subnational 
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mex.enadid1992.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
    
# -----------------------------------------------------------------------------------
#  MNG RHS 1998 report
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mng.rhs.1998.report.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

# -----------------------------------------------------------------------------------
#  MKD MICS 2011 report
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,mkd.mics.2011.report.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

# -----------------------------------------------------------------------------------
#  VNM 2010 population change and family planning survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,vnm.pcfps.2010.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
# -----------------------------------------------------------------------------------
#  VNM 2011 population change and family planning survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,vnm.pcfps.2011.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
# -----------------------------------------------------------------------------------
#  Honduras 2004 Survey of living conditions
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,hnd.living.conditions.2004.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)      
  
# -----------------------------------------------------------------------------------
#  South Africa October Household Surveys 1993-1995,1997-98
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,zaf.oct.hh.survey.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)      
  
# -----------------------------------------------------------------------------------
#  Paraguay 1997-1998 Integrated household survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,pry.int.hh.1997.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)     

# -----------------------------------------------------------------------------------
#  Paraguay 2000-2001 Integrated household survey
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,pry.int.hh.2000.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)        
  
# -----------------------------------------------------------------------------------
#  Zambia 2012 MIS SBH (limited use)
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,zmb.mis.2012.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)   
  
# -----------------------------------------------------------------------------------
#  Kuwait Child Health Survey 1987
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,kwt.child.health.1987.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)          
  
# -----------------------------------------------------------------------------------
#  IDN census 2010
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,idn.census.2010.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  

# -----------------------------------------------------------------------------------
#  BWA FHS 1996
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,bwa.fhs.1996.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 

# -----------------------------------------------------------------------------------
#  KEN census 2009
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,ken.census.2009.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 

# -----------------------------------------------------------------------------------
#  BLZ census 1991
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,blz.census.1991.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 
  
# -----------------------------------------------------------------------------------
#  BFA census 2006
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,bfa.census.2006.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)   

# -----------------------------------------------------------------------------------
#  CRI census 2011
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,cri.census.2011.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
# -----------------------------------------------------------------------------------
#  CMR census 2005
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,cmr.census.2005.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    

# -----------------------------------------------------------------------------------
#  KAZ census 2009
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,kaz.census.2009.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
  
# -----------------------------------------------------------------------------------
#  ZWE census 2012
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,zwe.census.2012.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
# -----------------------------------------------------------------------------------
#  UGA AIDS Indicator Survey 2004-2005 report
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,uga.ais.2004.rep.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
  
# -----------------------------------------------------------------------------------
#  COD MICS 1995 (prepped separately from the other MICS data)
  nrow(data)
  add.data <- read.dta(paste(sbh.data.dir,cod.mics1995.data.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
  
      

## -----------------------------------------------------------------------------------
##  BWA FHS 2007-2008 sbh 
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,bwa.fhs.2007.2008.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  
   
## -----------------------------------------------------------------------------------
##  Philippines 2011 DHS
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,phl.dhs2011.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)   
   
## -----------------------------------------------------------------------------------
##  Papua New Guinea SBH from 2000 census
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,png.census2000.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)       
   
## -----------------------------------------------------------------------------------
##  Cambodia 2003-2005 socioeconomic survey
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,khm.2003.socsurv.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)       
   
## -----------------------------------------------------------------------------------
##  Tanzania Census 2002
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,tza.census.2002.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)        
    
## -----------------------------------------------------------------------------------
##  Kiribati nonstandard DHS 2009
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,kir.nonst.dhs.2009.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)        

## -----------------------------------------------------------------------------------
##  Togo Census 2010
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,tgo.census.2010.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    
       
## -----------------------------------------------------------------------------------
##  Uruguay Census 2011
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ury.census.2011.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    
   
## -----------------------------------------------------------------------------------
##  Fiji Census 2007
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,fji.census.2007.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    
   
## -----------------------------------------------------------------------------------
##  Federated States of Micronesia Census 1994
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,fsm.census.1994.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)   
   
## -----------------------------------------------------------------------------------
##  Albania census 2011 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,alb.census.2011.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  South Africa 2001 IPUMS Census subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zaf.ipums2001.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  Brazil PNAD sunational SBH 2004, 2005, 2007, 2008, 2009
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,bra.pnad.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)   

## -----------------------------------------------------------------------------------
##  Azerbaijan 2009 Census SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,aze.census.2009.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    
 
## -----------------------------------------------------------------------------------
##  Brazil 2009 IPUMS census subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,bra.ipums2010.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)     

## -----------------------------------------------------------------------------------
##  Kenya MICS 2008, 2009, 2011 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ken.mics.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    

## -----------------------------------------------------------------------------------
##  Brazil 2000 Census subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,bra.census2000.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  
   
## -----------------------------------------------------------------------------------
##   South Africa October Household Survey 1993, 1994, 1995, 19797, 1998 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zaf.oct.hh.survey.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##   South Africa 1998  DHS subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zaf.dhs.1998.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)     
   
## -----------------------------------------------------------------------------------
##   BRA  1992, 1996, 1997, 1998, 1999, 2002, 2003 PNAD subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,bra.pnad.1992.2013.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data) 
   
## -----------------------------------------------------------------------------------
##  South Africa 1996 IPUMS Census subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zaf.ipums1996.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data) 

## -----------------------------------------------------------------------------------
##  Brazil 1986, 1996 DHS subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,bra.dhs.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)

## -----------------------------------------------------------------------------------
##  South Africa - KwaZulu and Natal Income Dynamics Study 1993 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zaf.ids1993.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)

## -----------------------------------------------------------------------------------
##  Brazil IPUMS census 1960, 1970, 1980, 1991 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,bra.ipums.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  
  
## -----------------------------------------------------------------------------------
##  Mexico Census 1980 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,mex.census1980.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  Mexico Census 1990 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,mex.census1990.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  Kenya IPUMS 1979 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ken.ipums1979.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  Kenya AIS 2007 on national level SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ken.ais2007.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  Kenya AIS 2007 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ken.ais2007.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  Kenya MICS 2007 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ken.mics2007.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  South Africa IPUMS census 2007 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zaf.ipums2007.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  South Africa LSMS 1993 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zaf.lsms1993.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  Kenya MICS 2000 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ken.mics2000.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)  

## -----------------------------------------------------------------------------------
##  Thailand 2012 MICS report SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,tha.mics2012.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)   
   
## -----------------------------------------------------------------------------------
##  Afghanistan National Risk and Vulnerability Assessment Survey 2011-2012 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,afg.nrva2011.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    
   
## -----------------------------------------------------------------------------------
##  Lesotho 2011 Demographic Survey SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,lso.demographic.survey2011.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data) 
   
## -----------------------------------------------------------------------------------
##  Tuvalu 2007 DHS SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,tuv.dhs2007.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    

## -----------------------------------------------------------------------------------
##  Zambia Malaria Indicator Survey 2006 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zmb.mis2006.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)   
   
## -----------------------------------------------------------------------------------
##  Palestine National Demographic and Health Survey 2004 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,pse.dhs2004.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)      
   
## -----------------------------------------------------------------------------------
##  Palau 2000 Cenus report SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,plw.census2000.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)        
   
## -----------------------------------------------------------------------------------
##  Palau 1995 Cenus report SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,plw.census1995.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data) 

## -----------------------------------------------------------------------------------
##  Burundi Priority Survey 1998-1999 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,bdi.priority.survey1998.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)

## -----------------------------------------------------------------------------------
##  Maldives Vulnerability and Poverty Assessment 1997-1998 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,mdv.vpa1998.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)
   
## -----------------------------------------------------------------------------------
##  South Africa National DHS 1987 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,zaf.dhs1987.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)   

## -----------------------------------------------------------------------------------
##  Nauru 2007 DHS SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,nru.dhs2007.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)
   
## -----------------------------------------------------------------------------------
##  Laos Reproductive Health Survey 2005 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,lao.rhs2005.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)   
   
## -----------------------------------------------------------------------------------
##  Pakistan 1993 Contraceptive prevalence Survey SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,pak.cps1993.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    
   
## -----------------------------------------------------------------------------------
##  Mexico National Nutrition Survey 1988 Subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,mex.nns.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    
     
## -----------------------------------------------------------------------------------
##  Kenya IPUMS census 1969 subnational SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ken.ipums1969.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)       

## -----------------------------------------------------------------------------------
##  Kenya subnational DHS 1989, 1993, 1998, 2003, 2009 SBH
   nrow(data)
   add.data <- read.dta(paste(sbh.data.dir,ken.dhs.subnat.sbh,sep=""), convert.underscore=TRUE)
   data = append.data(data, add.data)
   head(data)    
 
#  -----------------------------------------------------------------------------
#  Ecuador Living Conditions Survey 2005-2006 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,ecu.ecv2005.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    

#  -----------------------------------------------------------------------------
#  Ecuador Living Conditions Survey 1998-1999 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,ecu.ecv1998.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
      
#  -----------------------------------------------------------------------------
#  Nigeria Living Standards Survey 2003-2004 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,nga.nlss2003.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)   
  
#  -----------------------------------------------------------------------------
#  Malaxi Diffusion and Ideational Change Project (MDICP) 2001 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,mwi.mdicp2001.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
#  -----------------------------------------------------------------------------
#  Malawi Diffusion and Ideational Change Project (MDICP) 1998 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,mwi.mdicp1998.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
#  -----------------------------------------------------------------------------
#  Tanzania Knowledge, Attitudes, and Practices Survey 1994 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,tza.tkap1994.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)   
  
#  -----------------------------------------------------------------------------
#  Thailand Contraception Prevalence Survey 1984 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,tha.cps1984.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
#  -----------------------------------------------------------------------------
#  Thailand Contraception Prevalence Survey 1981 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,tha.cps1981.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  

#  -----------------------------------------------------------------------------
#  Ecuador ENSANUT 2012SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,ecu.ensanut2012.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

#  -----------------------------------------------------------------------------
#  Mexico Household Income and Expenditre Survey 2010 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,mex.enigh2010.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

#  -----------------------------------------------------------------------------
#  Mexico 2005 census subnational SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,mex.census2005.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

#  -----------------------------------------------------------------------------
#  Mexico 2000 census subanational SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,mex.census2000.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 

#  -----------------------------------------------------------------------------
#  India 1992-1993 DHS subnational SBH  
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,ind.dhs1992.subnat.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
#  -----------------------------------------------------------------------------
#  Nicaragua National Demographic and Health Survey 2011-2012 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,nic.ndhs2011.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
  
#  -----------------------------------------------------------------------------
#Brazil National Demographic and Health Survey of Children and Women 2006-2007 SBH 
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,bra.ndhs2006.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)      
  
#  -----------------------------------------------------------------------------
# Palestine Health Survey 2000 SBH
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,pse.hs2000.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)     
  
#  -----------------------------------------------------------------------------
# India 2011 Census National, state, and state-urban/rural SBH  
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,ind.census2011.sbh ,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
#  -----------------------------------------------------------------------------
# India DLHS 2007 state level SBH  
  nrow(data)
  add.data<-read.dta(paste(sbh.data.dir,ind.dlhs2007.state.sbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 

#  -----------------------------------------------------------------------------
# India 2005 DHS at the state level SBH  
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.dhs2005.state.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# India 2005 DHS at the state-urban/rural SBH  
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.dhs2005.urbrur.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# India Human Development Survey 2004-5 at the state level SBH  
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.hds2004.state.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# India 2001 Census at the state level SBH  
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.census2001.state.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# India DHS 1998 at the state level SBH 
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.dhs1998.state.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# India DHS 1998 at the state-urban/rural level SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.dhs1998.urbrur.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 
  
#  -----------------------------------------------------------------------------
# Kenya Welfare monitoring survey II 1994 subnational and national SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ken.wms1994.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)   

#  -----------------------------------------------------------------------------
# India DHS 1992-3 at the state level SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.dhs1992.state.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
# India DHS 1992-3 at the state-urban/rural level SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.dhs1992.urbrur.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
# Peru 2014 DHS report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,per.dhs2014.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
# Cook Islands 2011 Census SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,cok.census2011.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
# Djibouti demographic survey 1991 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,dji.ds1991.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)
 
#  -----------------------------------------------------------------------------
# Malawi Family formation survey 1984 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,mwi.ffs1984.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
# Nauru census 2011 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,nru.census2011.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Vietnam Population Change and Family Planning Survey 2013 report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,vnm.pcfps2013.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Guinea Demographic Survey 1954-1955 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,gin.ds1955.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Armenia Census 2011 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,arm.census2011.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  India Census 1981 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ind.census1981.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Marshall Islands Census 2011 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,mhl.census2011.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Rwanda Census 2012 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,rwa.census2012.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Kiribati 2010 census report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,kir.census2010.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Namibia 2011 Census report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,nam.census2011.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Solomon Islands 2009 census report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,slb.census2009.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Myanmar 2014 census report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,mmr.census2014.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Tonga 2012 national DHS report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,ton.ndhs2012.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Vanuatu 2013 national DHS report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,vut.ndhs2013.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
#  Turkey 2013-2014 national DHS report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,tur.ndhs2013.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)
 
#  -----------------------------------------------------------------------------
#  Azerbaijan 2011 national DHS report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,aze.ndhs2011.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)  

#  -----------------------------------------------------------------------------
#  Burundi Demographic survey 1970-1971 report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,bdi.ds1970.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)  

#  -----------------------------------------------------------------------------
#  malawi Population Change survey 1970-1071 report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,mwi.popsurvey1970.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 
 
#  -----------------------------------------------------------------------------
# Papua New Guinea 1971 Census report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,png.census1971.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Honduras National Demographic Survey report SBH 
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,hnd.edenh1972.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Cote d'ivoire Demographic Survey 1978-1979 report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,civ.ds1978.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Comoros 1980 census report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,com.census1980.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# BanglUSER 1981 Contraceptive Prevalence Survey report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,bgd.cps1981.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Costa Rica1981 Contraceptive Prevalence Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,cri.cps1981.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Jordan 1981 Demographic Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,jor.ds1981.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Peru 1981 Contraceptive Prevalence Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,per.cps1981.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Honduras 1983 National Demographic Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,hnd.edenh1983.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Tunisia 1983 Contraceptive Prevalence Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,tun.cps1983.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Egypt 1984 Contraceptive Prevalence Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,egy.cps1984.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Honduras Nationa Survey of maternal and Child Health SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,hnd.nsmch1984.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 
 
#  -----------------------------------------------------------------------------
#  Pakistan Contraceptive Prevalence Survey 1984-1985 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,pak.cps1984.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 
 
#  -----------------------------------------------------------------------------
#  United Arab Emirates Child health Survey 1987-1988 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,are.chs1987.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 
  
#  -----------------------------------------------------------------------------
#  Cuba national fertility survey 1987 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,cub.nfs1987.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Saudi Arabia Child Health Survey 1987 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,sau.chs1987.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Mauritania 1988 census SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,mrt.census1988.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)  

#  -----------------------------------------------------------------------------
#  Turkey 1988 Population Health Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,tur.phs1988.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)  
 
#  -----------------------------------------------------------------------------
#  Bahrain 1989 Child Health Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,bhr.chs1989.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)   

#  -----------------------------------------------------------------------------
#  Ethiopia National Family and Fertility Survey 1990 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,eth.ffs1990.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)   

#  -----------------------------------------------------------------------------
#  Maldives 1990 census SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,mdv.census1990.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Nepal 1991 National Family Planning and Health SurveySBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,npl.nfhs1991.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 
   
#  -----------------------------------------------------------------------------
#  papua New Guinea 1991 DHS SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,png.dhs1991.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Sri Lanka 1993 DHS SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,lka.dhs1993.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  papua New Guinea 1996-1997 SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,png.dhs1996.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Iraq Child and maternal mortality survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,irq.icmms1999.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Jordan 1999 Annual Fertility Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,jor.afs1999.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Solomon Islands 1999 Census SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,slb.census1999.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Turkmenistan 2000 DHS SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,tkm.dhs2000.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
#  Lesotho 2001 Demographic Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,lso.ds2001.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Mongolia 2003 Reporductive Health Survey SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,mng.rhs2003.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Gambia 1990 Contraceptive Prevalence SurveySBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,gmb.cps1990.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Cape Verde Census 2000 report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,cpv.census2000.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#  -----------------------------------------------------------------------------
# Honduras 2001 Census report SBH
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,hnd.census2001.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#  -----------------------------------------------------------------------------
# Egypt 2015 Special DHS 2015 microdata SBH 
nrow(data)
add.data<-read.dta(paste(sbh.data.dir,egy.dhssp2015.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)
    
#-----------------------------------------------------------------------------
# 2012-2014 IND DLHS4 (District Level Household Survey) SBH
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,ind.dlhs4.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#-----------------------------------------------------------------------------
# 2012-2014 IND Annual Health Survey 2010-2013 SBH
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,ind.ahs.2010.2013.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#-----------------------------------------------------------------------------
# BanglUSER 2011 Census report SBH
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,bgd.census2011.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)
  
#-----------------------------------------------------------------------------
# Mexico 2015 intercensal survey SBH
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,mex.int.census2015.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 
  
#-----------------------------------------------------------------------------
# Mexico 2014 ENADID SBH
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,mex.enadid2014.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#-----------------------------------------------------------------------------
# Eritrea 2010 Population and Health Survey SBH
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,eri.phs2010.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)  
  
#-----------------------------------------------------------------------------
# Kenya DHS province level
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,ken.dhs.province.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)  

#-----------------------------------------------------------------------------
# Kenya PMA r3 2015
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,ken.pmar32015.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)  

#-----------------------------------------------------------------------------
# Uganda PMA r3 2015
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,uga.pmar32015.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)  

#-----------------------------------------------------------------------------
# Ghana PMA r3+r4 2014/2015
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,gha.pmar342015.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data)

#-----------------------------------------------------------------------------
# Indonesia Census, data for provinces
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,idn.census.province.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#-----------------------------------------------------------------------------
# Indonesia DHS, data for provinces
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,idn.dhs.province.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#-----------------------------------------------------------------------------
# Indonesia efls, data for provinces
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,idn.efls.province.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#-----------------------------------------------------------------------------
# Indonesia fls, data for provinces
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,idn.fls.province.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#-----------------------------------------------------------------------------
# Indonesia supas, data for provinces
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,idn.supas.province.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#-----------------------------------------------------------------------------
# Indonesia susenas, data for provinces
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,idn.susenas.province.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 

#-----------------------------------------------------------------------------
# Madagascar MICS 2012 
nrow(data)
add.data <- read.dta(paste(sbh.data.dir,mdg.mics.2012.sbh,sep=""), convert.underscore=TRUE)
data = append.data(data, add.data)
head(data) 


# ----------------------------------------------------------------------------
# 02. Add in direct birth history estimates 
# ---------------------------------------------------------------------------- 
  
  # ----------------------------------------------------------------------------
  # CDC REPRODUCTIVE HEALTH SURVEYS (CDC RHS) DIRECTS

  nrow(data)
  add.data <- read.dta(paste(direct.data.dir,cdcrhs.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # DEMOGRAPHIC AND HEALTH SURVEYS (DHS) AND WORLD FERTILITY SURVEYS (WFS) DIRECTS: NOTE THESE WERE POOLED TOGETHER FOR DIRECT ESTIMATES

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,dhs.direct,sep=""), convert.underscore=TRUE)

  # Remove all WFS, not just WFS in countries that have DHS.

  data = subset(data, !(source %in% data$source[grep("(world)|(wolrd)|(egyptian fertility survey)|(ghana fertility survey)|(jamaica fertility survey)|(jamica fertility survey)|(turkey fertility survey)", tolower(data$source))] & in.direct == "direct")); nrow(data)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # IRAN DEMOGRAPHIC AND HEALTH SURVEY (DHS) DIRECTS

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,irndhs.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # IRAQ: IFHS

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,ifhs.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # TLS DHS: Timor Leste estimates back in time

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,tlsdhs.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # TLS 2003: Timor Leste 2003 survey (not DHS)

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,tls2003.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # PAPFAM: PAPFAM countries direct birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,papfam.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # IDN Census: IDN 2000 census direct birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,idncen.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  #-----------------------------------------------------------------------------
  # LSMS complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,lsms.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # PAPCHILD: complete birth histories from the PAPCHILD surveys

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,papchild.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # ----------------------------------------------------------------------------
  # ENHOGAR: complete birth histories from the ENHOGAR surveys

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,enhogar.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # MICS: complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,mics.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # AIS: complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,ais.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # MIS: complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,mis.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # DHS In-depth: complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,dhsindepth.direct,sep=""), convert.underscore=TRUE)
  
  # drop PHL indepth 1993 because we already use the regular 1993 dhs, and this survey asks the same women
    add.data <- add.data[add.data$iso3 != "PHL",] 
    if (nrow(add.data)!=0) {
      data = append.data(data, add.data)
    } 
  
  head(data)

  # -----------------------------------------------------------------------------
  # DHS Special: complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,dhsspecial.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # WFS complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,wfs.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # Global Fund HH surveys complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,gf.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------
  # Indonesia Family Life Survey complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,ifls.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # IDN SUPAS complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,supas.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------
  # TUR DHS complete birth histories

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,tur.dhs.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------
  # IND Human Development Survey 04-05 cbh

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,ind.hds.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------
  # IRQ Multiple Indicator Rapid Assessment 2004

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,imira.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------
  # IND DHS urban rural cbh

  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,ind.dhs.urban.rural.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
  # -----------------------------------------------------------------------------
  # South Africa October Household Survey 1993-1995, 1997-1998 cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,zaf.oct.hh.survey.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
 
#  # -----------------------------------------------------------------------------
#  # BWA FHS 2007-2008 cbh  -- will need to be outliered
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,bwa.fhs.2007.2008.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
  
#  -----------------------------------------------------------------------------
#  Pakistan Integrated Health Survey 1998-1999 cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,pak.ihs.1998.1999.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  

#  -----------------------------------------------------------------------------
#  Nicaragua  ENDESA 2011-2012 cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,nic.endesa.2011.2012.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  

#  -----------------------------------------------------------------------------
#  Palestine  DHS 2004 cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,pse.dhs.2004.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  

#  -----------------------------------------------------------------------------
#  Ecuador National Health and  Nutrition Survey 2012 cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,ecu.ensanut.2012.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 

#  -----------------------------------------------------------------------------
#  Brazil Subnational DHS surveys 1986, 1991, 1996 cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,bra.dhs.subnat.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 
 
#  -----------------------------------------------------------------------------
#  South Africa Subnational October Household surveys 1993, 1994, 1995, 1997, 1998 cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,zaf.oct.hh.survey.subnat.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 
  
#  -----------------------------------------------------------------------------
#  Kenya MICS 2011, 2009, 2008 subnational cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,ken.mics.subnat.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 
  
#  -----------------------------------------------------------------------------
#  Kenya DHS 1988-1989, 1993, 1998, 2003, 2008-2009 subnational cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,ken.dhs.subnat.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)   
 
#  -----------------------------------------------------------------------------
#  South Africa 1998 DHS subnational cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,zaf.dhs.subnat.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
 
#  -----------------------------------------------------------------------------
#  Mexico National Survey of Demographic Dynamics 1992 subnational cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,mex.enadid.subnat.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
 
#  -----------------------------------------------------------------------------
#  Pakistan Social and Living Standards Measurement Survey 2007-2008, 2005-2006 cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,pak.social.lsms.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data) 

#  -----------------------------------------------------------------------------
#  Pakistan Integrated Household Survey 2001-2002cbh
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,pak.ihs.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)   
  
#  -----------------------------------------------------------------------------
#  Peru DHS 2013 CBH
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,per.dhs.2013.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)  
    
#  -----------------------------------------------------------------------------
#  Peru DHS 2014 CBH
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,per.dhs.2014.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)    
  
#  -----------------------------------------------------------------------------
#  Palestine 2000 CBH
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,pse.hsurvey.2000.direct,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)   
  
  # -----------------------------------------------------------------------------
  # Indonesia census, data for provinces
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,idn.census.province.cbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # Indonesia DHS, data for provinces
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,idn.dhs.province.cbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # Indonesia efls, data for provinces
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,idn.efls.province.cbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # Indonesia fls, data for provinces
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,idn.fls.province.cbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)

  # -----------------------------------------------------------------------------
  # Indonesia supas, data for provinces
  nrow(data)
  add.data<-read.dta(paste(direct.data.dir,idn.supas.province.cbh,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data)
  head(data)
  
# ----------------------------------------------------------------------------
# 03. Add in aggregate estimates
# ----------------------------------------------------------------------------

  # ----------------------------------------------------------------------------
  # INDIA: SRS 1990-1997

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,ind90s.agg,sep=""), convert.underscore=TRUE)
  add.data<-subset(add.data,t>1995)     ## duplicates in SRS 5q0s
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # INDIA: SRS 1983-1995

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,ind8395.agg,sep=""), convert.underscore=TRUE)
  add.data<-add.data[,c(1,2,4,5:9)]
  names(add.data)[2:3]<-c("t","q5")

  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # BANGLADESH: SRS 2001-2003

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,bgd0003.agg,sep=""), convert.underscore=TRUE)
  head(add.data)

  add.data$compiling.entity = "new"
  add.data$data.age = "new"
  add.data$iso3 = add.data$country
  add.data$country[add.data$country=="BGD"]<-"BanglUSER"
  add.data$source = "SRS"
  add.data$source.date = add.data$source.year
  add.data$t = add.data$year
  add.data$in.direct = "NA"
  add.data <- add.data[add.data$t != 2003,]

  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # India SRS 2007

  add.data<-read.dta(paste(agg.data.dir,ind07.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)

  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  ## Namibia: Census 2001 5M0 plus IMR from government website

  add.data<-read.dta(paste(agg.data.dir,nam2001.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$in.direct = "hh"

  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # INED SURVEYS

  add.data<-read.dta(paste(agg.data.dir,ined.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # INDIA: SRS FROM MARI BHAT 1998 PAPER

  add.data<-read.dta(paste(agg.data.dir,indbaht.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # IRAN: ASSORTED DATA FROM M

  add.data<-read.dta(paste(agg.data.dir,irnm.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # IRAQ: DSS/SRS FROM KOHLI 1976 PAPER

  add.data<-read.dta(paste(agg.data.dir,irqkohli.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # OMAN: ASSORTED DATA FROM HILL REPORT

  add.data<-read.dta(paste(agg.data.dir,omnhill.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)
  
  # ----------------------------------------------------------------------------
  # PNG: DHS 1996 PRELIMINARY REPORT FROM COLLABORATOR


  add.data<-read.dta(paste(agg.data.dir,pngdhs.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # ALGERIA, EGYPT, MOROCCO, TUNISIA: INED DATA FROM TABUTIN 1991 PAPER

  add.data<-read.dta(paste(agg.data.dir,ined.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # VANUATU CENSUS DATA

  add.data<-read.dta(paste(agg.data.dir,vutcensus.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$in.direct = "hh"

  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # DHS FINAL REPORT

  add.data<-read.dta(paste(agg.data.dir,dhsfinal.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # Palestine Bureau of Statistics data

  add.data<-read.dta(paste(agg.data.dir,pbs.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # BANGLADESH: SRS FROM BANGLADESH BUREAU OF STATISTICS

  add.data<-read.dta(paste(agg.data.dir,bgdbbs.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data <- add.data[add.data$t != 2007,]
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # SUSENAS HOUSEHOLD DEATHS MODULE - 1996

  add.data<-read.dta(paste(agg.data.dir,susenas1996.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$in.direct = "hh"

  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # SUSENAS HOUSEHOLD DEATHS MODULE - 1998

  add.data<-read.dta(paste(agg.data.dir,susenas1998.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$in.direct = "hh"

  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # CHINA: National Maternal and Child Health Surveillance System 2000-2011
  
  add.data<-read.dta(paste(agg.data.dir,chnsur2011.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)


  # ----------------------------------------------------------------------------
  # BEN Census 1991 2002

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,bencensus.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # MICS2 AFG Single Point, 2003

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,afgmics2.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # MVNM PCFPS Single Point, 2006.5 (HH Survey 2007)

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,vnmpcfps.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # MNG Statistical Yearbook 1999, 1998 Point

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,mngstatyb.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # MNG Statistical Yearbook, 2001. Report: 1999 Estimate

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,mngstatyb2001.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # MNG Statistical Yearbook, 1999. Report: 1998 Estimate

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,mngstatyb1999.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # MNG Statistical Yearbook, 2002. Report: 2000, 2001, 2002 Estimates.

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,mngstatyb2002.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # MMR Ministry of Health data, misc. 5q0 point estimates.

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,mmrmoh.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # MACRO Statcompiler (DHS, MIS, AIS, RHS, INDEPTH, and Special Surveys) Directs

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,statcompiler.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # VEN ENPOFAM 1998- 1 point from report

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,venenpofam.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # YEM Annual Statistical Bulletin 2006-2007

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,yemstatreport.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # AGO Integrated survey on pop welfare 2008

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,agoispw.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # CHN Survey on Deaths of Children Under the Age of Five; conducted 1992, 1 year recall, plotted 1991.

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,chnu5svy.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # LKA DHS reports (2000, 1993).

  nrow(data)
  add.data<-read.dta(paste(agg.data.dir,lkadhs.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # -------------------------------------------------------------------------------
  # Gulf Family surveys

  add.data <- read.csv(paste(agg.data.dir,gulf.agg,sep=""))
  names(add.data) <- gsub("_", ".", names(add.data), fixed=T)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$Extracted.from <- add.data$Page <- NULL
  data = append.data(data, add.data)

  # -----------------------------------------------------------------------------------
  # GNB MICS4 2010 Preliminary Report
  
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir, mics4.agg, sep=""), convert.underscore = TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)
  
  # -----------------------------------------------------------------------------------
  # IND SRS 2009 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir, INDsrs2009.agg, sep=""), convert.underscore = TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  data = append.data(data, add.data)
  
  # -----------------------------------------------------------------------------------
  # DZA VR
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, dza.vr.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)   
   
    
  # ----------------------------------------------------------------------------
  # CHINA - 5q0s from published government census report and journal articles

  add.data = read.dta("FILEPATH/5q0s_08072009.dta", convert.underscore = TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x); nrow(add.data)

  add.data = subset(add.data, !(add.data$source %in% add.data$source[grep("(1995 1% population survey)|(2005 1% population survey)|(calculated using 1982 census)" , add.data$source)])); nrow(add.data) # Take out these observations because they are included in the 00. DEATHS file and we don't want to duplicate.

  add.data$q5 = add.data$v5q0
  add.data$source.date = floor(add.data$year)
  add.data$in.direct = "NA"
  add.data$in.direct[add.data$source =="journal publication using Sample survey on fertility nad birth control in China"] <- "direct"
  add.data$compiling.entity = "new"
  add.data$data.age = "new"
  add.data$country = "China (without Hong Kong and Macao)"
  add.data$t = add.data$year + .25

  data = append.data(data, add.data)

  # ----------------------------------------------------------------------------
  # Iran 2010 MIDHS, tables from report
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, irn.midhs2010.from.m.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data) 
   
  # ----------------------------------------------------------------------------
  # China 1991 NSSCM
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, chn.nsscm.91.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data) 
   
  # ----------------------------------------------------------------------------
  # BWA FHS 07 report
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, bwa.fhs.07.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)    
   
  # ----------------------------------------------------------------------------
  # BWA DS 06 report indirects
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, bwa.ds.06.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)    
   
  # ----------------------------------------------------------------------------
  # CPV DHS 2012 report 
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, cpv.dhs.05.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data) 
   
  # ----------------------------------------------------------------------------
  # WSM DHS 09 report 
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, wsm.dhs.09.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)
   
  # ----------------------------------------------------------------------------
  # VNM Health Statistics Yearbook 2008 report 
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, vnm.hsy.08.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)
      
  # -----------------------------------------------------------------------------------
  # Gambia 2010 MICS4 Report
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, gmb.mics4.10.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)
      
  # -----------------------------------------------------------------------------------
  #Lesotho 2006 Census
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, lso.census.06.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)    
      
  # -----------------------------------------------------------------------------------
  #MMR MICS 2009 Report
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, mmr.mics.09.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)    
           
  # -----------------------------------------------------------------------------------
  #PAK Demographic Surveys 91, 92,95,96,97,99,03,05
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, pak.ds.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data) 
           
  # -----------------------------------------------------------------------------------
  #SYR MICS2 report
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, syr.mics2.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)         
   
  # -----------------------------------------------------------------------------------
  # YEM 1997 DHS report
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, yem.dhs1997.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)     
   
  # -----------------------------------------------------------------------------------
  # GIN 1992 DHS report  
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, gin.dhs1992.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)       

  # -----------------------------------------------------------------------------------
  # SDN 2000 MICS report  
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, sdn.mics2000.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)         

  # -----------------------------------------------------------------------------------
  # NGA 1999 DHS report  
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, nga.dhs1999.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)         
   
  # -----------------------------------------------------------------------------------
  # NER 1996 MICS report  
   nrow(data)
   add.data <- read.dta(paste(agg.data.dir, ner.mics1996.agg, sep=""), convert.underscore = TRUE)
   add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
   add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
   data = append.data(data, add.data)   
  # -----------------------------------------------------------------------------------
  # SEN 2000 MICS report  
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir, sen.mics2000.agg, sep=""), convert.underscore = TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # LKA DHS 2006-2007 Preliminary report  
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir, lka.dhs20062007.agg, sep=""), convert.underscore = TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)
  
  # -----------------------------------------------------------------------------------
  # DZA MICS 2000 report sbh
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,dza.mics2000.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # DZA MICS 1995 report sbh
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,dza.mics1995.agg,sep=""), convert.underscore=TRUE)
  data = append.data(data, add.data) 
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  # -----------------------------------------------------------------------------------
  # EGY DHS 1997 report 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,egy.dhs1997.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  # -----------------------------------------------------------------------------------
  # EGY DHS 1998 report 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,egy.dhs1998.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  # -----------------------------------------------------------------------------------
  # MWI MICS 1995 report 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mwi.mics1995.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  # -----------------------------------------------------------------------------------
  # MOZ MICS 1995 report 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,moz.mics1995.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  # -----------------------------------------------------------------------------------
  # JOR RHS 1983 report 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,jor.rhs1983.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
 
  # -----------------------------------------------------------------------------------
  # HND RHS 1987 report 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,hnd.rhs1987.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  # -----------------------------------------------------------------------------------
  # GEO RHS 2010 report 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,geo.rhs2010.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  # AZE RHS 2001 report 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,aze.rhs2001.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  # CHN Provincial MCHS 1996-2012
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,chn.prov.mchs19962012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)
  # -----------------------------------------------------------------------------------
  # CHN Provincial MOH 1996-2012
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,chn.prov.moh19962012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  # CHN National MOH 1996-2012
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,chn.moh19962012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)
  
  # -----------------------------------------------------------------------------------
  # Mongolia 1998 RHS report  -- cbh agg
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mng.rhs.1998.report.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)
   
  # -----------------------------------------------------------------------------------
  # Sudan 2006 PAPFAM report (Household Health Survey) -- cbh agg
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,sdn.papfam.2006.report.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)     
    
  # -----------------------------------------------------------------------------------
  # TJK 2010 survey on infant, child, and maternal mortalitiy aggregate 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,tjk.inf.child.2010.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)       
  
  # -----------------------------------------------------------------------------------
  # MAR ENPSF 2011 report agg
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mar.enpsf.2011.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  # IND urban/rural SRS 1983-1995- note, more years should be added soon
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ind.urb.rur.srs.19831995.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # -----------------------------------------------------------------------------------
  # Morocco 2009 multi-round demographic survey
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mar.multiround.survey.2009.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # -----------------------------------------------------------------------------------
  # Equatorial Guinea 2011 DHS report aggregate CBH
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,gnq.dhs.2011.report.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # -----------------------------------------------------------------------------------
  # South Africa Rapid Mortality Surveillance report 2012
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,zaf.rms2012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # -----------------------------------------------------------------------------------
  # Sudan MICS 2010 final report
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,sdn.mics2010.final.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  

  # -----------------------------------------------------------------------------------
  # China provincial MCHS 2000-2012, MCHS gave us provincial level estiamtes, can compare to our aggregated provincial ones
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,chn.prov.mchs.prov20002012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)
  
  # -----------------------------------------------------------------------------------
  # Kiribati 2009 non-standard DHS report aggregate CBH points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,kir.nonst.dhs.2009.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)          
  
  # -----------------------------------------------------------------------------------
  # Burkina Faso 1996 Census  report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,bfa.census.96.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # Burkina Faso 2006 Census  report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,bfa.census.06.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # Burkina Faso 1991 Demographic Survey  report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,bfa.ds.91.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # Laos Reproductive health survey 2005 report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,lao.rhs.2005.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)    

  # -----------------------------------------------------------------------------------
  # Tuvalu DHS 2007 report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,tuv.dhs.2007.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # Moldova MICS 2012 report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mda.mics.2012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # Iraq Living Conditions Survey 2004 report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,irq.lcs.2004.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  

  # -----------------------------------------------------------------------------------
  # Ghana 2014 DHS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,gha.dhs.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  

  # -----------------------------------------------------------------------------------
  # Lesotho 2014 DHS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,lso.dhs.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  # Benin  2014 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ben.mics.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # DOM  2014 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,dom.mics.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # -----------------------------------------------------------------------------------
  # GNB  2014 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,gnb.mics.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  # Guyana  2014 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,guy.mics.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
 
  # -----------------------------------------------------------------------------------
  # Sudan 2014 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,sdn.mics.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
 
  # -----------------------------------------------------------------------------------
  # SLV 2014 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,slv.mics.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # -----------------------------------------------------------------------------------
  # Swaziland 2014 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,swz.mics.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)    

  # -----------------------------------------------------------------------------------
  # Mongolia 2013 MICS preliminary report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mng.mics.2013.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  # Algeria 2012-2013 MICS  report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,dza.mics.2013.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  # Botswana 2011 Census Analytical report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,bwa.census.2011.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  # Rwanda 2014-2015 DHS preliminary report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,rwa.dhs2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 

  # -----------------------------------------------------------------------------------
  # Libya 2003 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,lby.mics2003.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   
  
  # -----------------------------------------------------------------------------------
  # Mauritania 1996 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mrt.mics1996.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   
  
  # -----------------------------------------------------------------------------------
  # Madagascar 1995 MICS report aggregate points
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mdg.mics1995.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

	 
  # -----------------------------------------------------------------------------------
  # India Annual  Health Survey 2010-2011
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ind.healthsurvey2010.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  
  # -----------------------------------------------------------------------------------
  # India Annual  Health Survey 2011-2012
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ind.healthsurvey2011.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  
  # -----------------------------------------------------------------------------------
  # India Annual  Health Survey 2012-2013
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ind.healthsurvey2012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   


  # -----------------------------------------------------------------------------------
  # Turkey DHS 2013-2014 report
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,tur.dhs.2013_2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 

  # -----------------------------------------------------------------------------------
  # Lebanon MICS 2009 report
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,lbn.mics.2009.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   

  # -----------------------------------------------------------------------------------
  # United Arab Emirates 2013 Statistivcal yearbook report
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,are.statyb2013.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   
  
  # -----------------------------------------------------------------------------------
  # Tonga 2012 National DHS report
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ton.ndhs2012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  # Vanuatu 2013 National DHS report
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,vut.ndhs2013.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)     

  # -----------------------------------------------------------------------------------
  # Azerbaijan 2012 National DHS report
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,aze.ndhs2012.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)     

  # -----------------------------------------------------------------------------------
  # Pakistan 1981 Census report (SBH aggregate)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,pak.census1981.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)     

  # -----------------------------------------------------------------------------------
  # Haiti EMMUS 1987 report
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,hti.emmus1987.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Bhutan National Health Survey aggregate point
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,btn.nhs1994.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  # -----------------------------------------------------------------------------------
  #  Chad 2014-2015 DHS aggregate point
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,tcd.dhs2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Bahrain 1989 Child Health Survey aggregate point (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,bhr.chs1989.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Nepal 1991 national family planning and health survey aggregate point (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,npl.nfhs1991.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Mexico ENADID 1992 aggregate point (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,mex.enadid1992.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  

  # -----------------------------------------------------------------------------------
  #  Sri Lanka 1993 DHS aggregate point (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,lka.dhs1993.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Papua New Guinea 1996-1997 DHS aggregate point (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,png.dhs1996.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Iraq child and maternal mortality survey 1999 aggregate point (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,irq.icmms1999.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Lebanon 2000 MICS indirect  aggregate point (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,lbn.mics2000.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Turkmenistan 2000 DHS aggregate point (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,tkm.dhs2000.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  #  Gambia 1990 Contraceptive Prevalence Survey aggregate points (replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,gmb.cps1990.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  #  Saudi Arabia levels, trends and differentials of infant and child mortality 1990 indirect aggregate points(replacement for collaborator's data)
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,sau.ltdicm1990.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   
  
  # -----------------------------------------------------------------------------------
  #  Guatemala 2014-2015 DHS report aggregate CBH points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,gtm.dhs2015.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   
  
  # -----------------------------------------------------------------------------------
  #  India SRS statistical reports 1995-2013 aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ind.srs.1995_2013.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)
  
  # -----------------------------------------------------------------------------------
  #  Congo MICS5 2014-2015 aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,cog.mics5.2014_2015.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 

  # -----------------------------------------------------------------------------------
  #  Oman MICS5 2014 aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,omn.mics5.2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  #  India subnationa DHS 2015-2016 aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ind.dhs.2015.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)   
  
  # -----------------------------------------------------------------------------------
  #  BanglUSER SRS 2012-2014 report aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,bgd.srs.2012_2014.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)  
  
  # -----------------------------------------------------------------------------------
  #  Arm DHS 2015-2016 report aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,arm.dhs.2015_2016.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  #  Ago DHS 2015-2016 report aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ago.dhs.2015_2016.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  
  # -----------------------------------------------------------------------------------
  #  eth DHS 2015-2016 report aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,eth.dhs.2016.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data) 
  

  # # -----------------------------------------------------------------------------------
  # #  ind 2014/2015 srs report aggregate points 
  nrow(data)
  add.data <- read.dta(paste(agg.data.dir,ind.srs.2014_2015.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  data = append.data(data, add.data)

  
# ------------------------------------------------------------------------------------------
# 03. Add in update VR estimates and VR from other sources 
# ------------------------------------------------------------------------------------------
  
  # ------------------------------------------------------------------------------------------
  # update VR dataset
  
  # read in death, population and birth data
  vr <- read_dta(paste(vr.dir,vr.file,sep=""))
  # eliminate SRS for the India states from the VR file (we are using aggregate estimates)
   vr <- vr[!(grepl("IND_", vr$ihme_loc_id) & vr$deaths_source %in% c("SRS", "SRS_REPORT")),]

   vr <- vr[!(vr$ihme_loc_id=="GBR_4749" & vr$year < 1980),]
   
  pop <- read_dta(paste(pop.dir,pop.file,sep=""))
  
  pop <- pop[!(grepl("USA_", pop$ihme_loc_id) & pop$pop_source=="USA_CENSUS"),]
  births <- read_dta(paste(births.dir,births.file,sep=""))
  
  births <- births[births$sex == "both",c("ihme_loc_id", "year", "births")]
  births.srs <- read.csv(ind.srs.births.file) ## not in thousands
    names(births.srs) <- c("year", "births")
  ## delete 1992 from here
  births.srs <- births.srs[births.srs$year > 1992,]
  
  # replace ihme_loc_id = "CHN" with ihme_loc_id = "CHN_44533"
  data$iso3[data$iso3 == "CHN"] <- "CHN_44533"
  data$ihme.loc.id[data$iso3 == "CHN_44533"] <- "CHN_44533"
  vr$country[vr$ihme_loc_id == "CHN_44533"] <- "China (without Hong Kong and Macao)" 

  # take only both sexes (not males and females) and extract only the necessary age variables for 5q0
  vr <- vr[vr$sex == "both" & vr$year>=1950,c("ihme_loc_id","country","year","deaths_source","source_type",paste("DATUM",seq(0,4),"to",seq(0,4),sep=""),"DATUM1to4","DATUM0to4")]
  vr$orig_source_type <- vr$source_type  
  vr$source_type[grepl("VR", vr$source_type)] <- "VR"
  vr$source_type[grepl("SRS", vr$source_type)] <- "SRS"
  vr$source_type[grepl("DSP", vr$source_type)] <- "DSP" 
  pop <- pop[pop$sex == "both" & pop$ihme_loc_id != "",c("ihme_loc_id","year","source_type","c1_0to0","c1_1to4")]

  # take out BanglUSER SRS data points
  vr <- subset(vr, !(deaths_source=="SRS_LIBRARY" & ihme_loc_id=="BGD"))
  
  # Use finer age groups to get coarser age groups (e.g. 1to4, 0to4) 
  vr$DATUM1to4[is.na(vr$DATUM1to4)] <- apply(vr[is.na(vr$DATUM1to4),paste("DATUM",seq(1,4),"to",seq(1,4),sep="")],1,sum)
  vr$DATUM0to4[is.na(vr$DATUM0to4)] <- apply(vr[is.na(vr$DATUM0to4),c("DATUM0to0","DATUM1to4")],1,sum)
  
  # merge deaths and population
  vr$id <- paste(vr$ihme_loc_id, vr$source_type, vr$year, sep="_")
  pop$id <- paste(pop$ihme_loc_id, pop$source_type, pop$year, sep="_")
  
  vrpop1 <- merge(vr, pop, by=c("id", "ihme_loc_id", "source_type", "year"))
  vrpop2 <- merge(vr[!vr$id %in% vrpop1$id,c("country", "ihme_loc_id","year","source_type","deaths_source","DATUM0to0","DATUM1to4","DATUM0to4", "orig_source_type")], 
                  pop[pop$source_type=="IHME",c("ihme_loc_id","year","c1_0to0","c1_1to4")],
                  by=c("ihme_loc_id","year"),all.x=T)
  vrpop <- rbind(vrpop1[,names(vrpop2)], vrpop2)
  vrpop <- vrpop[!duplicated(vrpop),]
 
  # merge in births as well
  vrpop1 <- merge(vrpop[!(vrpop$ihme_loc_id == "IND" & vrpop$source_type == "SRS" & vrpop$year>1992),],births,by=c("ihme_loc_id","year"),all.x=TRUE)
  vrpop2 <- merge(vrpop[(vrpop$ihme_loc_id == "IND" & vrpop$source_type == "SRS" & vrpop$year>1992),],births.srs,by=c("year"),all.x=TRUE)
  vrpop <- rbind(vrpop1, vrpop2)

  vrpop$source_type[vrpop$ihme_loc_id == "TUR"] <- vrpop$orig_source_type[vrpop$ihme_loc_id == "TUR"]
  ## Add back SSA VR system for south africa as well
  vrpop$source_type[vrpop$ihme_loc_id == "ZAF"] <- vrpop$orig_source_type[vrpop$ihme_loc_id == "ZAF"]
  
  vrpop <- vrpop[,names(vrpop) != "orig_source_type"]
  
  # insert Thailand births from who
  THA_births <- read.csv("FILEPATH/THA_births")
  
  
  vrpop$births[vrpop$year <=2014 & vrpop$year >=2007 & vrpop$ihme_loc_id=="THA"] <- THA_births$births
  
  # calculate 5q0
  vr.q5 <- c()
  for(i in 1:nrow(vrpop)) {
    # Extract the ith row of data
    vrI <- c(as.numeric(vrpop[i,c("DATUM0to0","DATUM1to4")]),NA)
    names(vrI) <- c(0,1,5)
    popI <- c(as.numeric(vrpop[i,c("c1_0to0","c1_1to4")]),NA)
    names(popI) <- c(0,1,5)
    
    mortalityRate <- vrI/popI
    
    # First try to use the m -> q, if we can't then use deaths/births
    if(sum(is.na(mortalityRate[c("0","1")])) == 0 & !(vrpop[i,c("ihme_loc_id")]=="THA" & vrpop[i,c("year")] >= 2007 )) {   
      qs <- ConvertMortalityRateToProbability(mortalityRate,sex="both")
      q5I <- 1-((1-qs["0"])*(1-qs["1"]))
      vr.q5 <- c(vr.q5, q5I)
    } else {
      # deaths/births 
      q5I <- vrpop[i,"DATUM0to4"]/vrpop[i,"births"]
      vr.q5 <- c(vr.q5, q5I)
    }
  }
  vr.q5 <- as.vector(vr.q5)
  
  # Conform the new vr 5q0s to the rest of the data
  add.data <- vrpop
  add.data <- cbind(add.data, vr.q5)
  add.data <- cbind(add.data, vrpop$year)
  add.data <- add.data[,c("ihme_loc_id","year","source_type","vr.q5","vrpop$year")]


  names(add.data) <- c("ihme.loc.id","t","source","q5","source.date")
  
  add.data <- cbind(add.data,data.age="new")
  add.data <- cbind(add.data,compiling.entity="new")
  add.data <- cbind(add.data,in.direct=NA)
  
  add.data$q5 <- 1000*add.data$q5
  add.data <- unique(add.data)
 
  # Add it in
  data = append.data(data, add.data)
    
  # ----------------------------------------------------------------------------  
  # TransMONEE 5q0 estimates
  
  add.data<-read.dta(paste(agg.data.dir,transmonee.agg,sep=""), convert.underscore=TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data <- add.data[,colnames(add.data) != "country"]
  names(add.data) <- sub("year","t",names(add.data))
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  add.data$ihme.loc.id <- add.data$iso3
  lots_of_vr <- append.data(data[data$source == "VR",], add.data)
  add.data <- lots_of_vr[duplicated(lots_of_vr[,c("ihme.loc.id","t")]) == FALSE & lots_of_vr$source == "VR - TransMONEE",]
  
  data = append.data(data, add.data)
  
  # ----------------------------------------------------------------------------    
  #BanglUSER SRS 5q0 estimates
  
  add.data<-read.dta(paste(agg.data.dir,srsbgd.agg,sep=""), convert.underscore=TRUE) 
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)
  add.data$sd.q5 <- add.data$log10.sd.q5 <- NA
  add.data$ihme.loc.id <- add.data$iso3
  ## temporary addition until all sources have NIDs
  names <- c("iso3","ihme.loc.id","t","q5","source","source.date","in.direct","compiling.entity","data.age","sd.q5","log10.sd.q5")
  add.data <- add.data[,names]
  lots_of_vr<- lots_of_vr[,names]
  lots_of_srs <- rbind(data[data$source == "SRS",], add.data)
  add.data <- lots_of_srs[duplicated(lots_of_srs[,c("ihme.loc.id","t")]) == FALSE & lots_of_srs$source == "SRS vital registration, BGD Bureau of Statistics",]
  data = append.data(data, add.data)
 
  # ---------------------------------------------------------------------------- 
  # Nauru 2002 VR
  
  add.data = read.dta("FILEPATH/nauru2002.dta", convert.underscore = TRUE)
  add.data[] = lapply(add.data, function(x) if(is.factor(x)) as.character(x) else x)

  add.data$q5 = add.data$v5q0
  add.data$source.date = floor(add.data$year)
  add.data$in.direct = "NA"
  add.data$compiling.entity = "new"
  add.data$data.age = "new"
  add.data$country = "Nauru"
  add.data$t = add.data$year + .25
  add.data$ihme.loc.id <- add.data$iso3

  data = append.data(data, add.data)
  
#------------------------------------------------------------------------------------------
# 04. Add in household deaths from single surveys and censuses which have been adjusted with growth balance
#------------------------------------------------------------------------------------------
  data <- cbind(data, adjust=0)    # this is to allow us to mark only things adjusted with growth balance as adjusted   

  # ---------------------------------------------------------------------------- 
  # IFHS (growth balanced) 
  nrow(data)         
          
# ------------------------------------------------------------------------------  
# Merge in ihme.loc.id
# ------------------------------------------------------------------------------
  
local <- locations[locations$level_all == 1,]
local <- local[,c("local_id_2013", "ihme_loc_id")]
local <- plyr::rename(local, c("local_id_2013" = "iso3","ihme_loc_id" = "ihme.loc.id"))

## Map iso3's to ihme_loc_id's where ihme_loc_id is NA

## get rid of XIU/XIR data first because they shouldn't exist anymore
data <- data[!(data$iso3 %in% c("XIR","XIU")),]

data$iso3[data$iso3 == "ROM"] <- "ROU"


missings <- data[is.na(data$ihme.loc.id),]
missings$ihme.loc.id <- NULL

mapped <- data[!is.na(data$ihme.loc.id),]

missings <- merge(missings, local, by="iso3",all.x=T)
missings$ihme.loc.id[is.na(missings$ihme.loc.id)] <- missings$iso3[is.na(missings$ihme.loc.id)]

  
# put the mapped and unmapped together
data <- rbind(missings, mapped)
data$q5 <- as.numeric(data$q5)

##  -------------------------------------------------------------------------
##  Standardized China Source Names
##  Creating Over-Arching Source Names for clarity in the China 5q0 data
##  -------------------------------------------------------------------------

    #  VR - two sources from COD
     data$source[data$source == "WHO" & data$source.date <= 2000 & data$ihme.loc.id == "CHN_44533"] <- "VR_MOH_09C"
     data$source[data$source == "MOH_10C" & data$source.date >= 2002 & data$ihme.loc.id == "CHN_44533"] <- "VR_MOH_10C"
     
    # 1% Intra-Census Survey
    data$source[data$source == "DC" & (data$source.date == 1995 | data$source.date == 2005) & data$ihme.loc.id == "CHN_44533"] <- "1% Intra-Census Survey" 
    data$source[data$source == "one percent survey 1995"] <- "1% Intra-Census Survey"
    data$in.direct[data$ihme.loc.id == "CHN_44533" & data$source == "1% Intra-Census Survey" & data$in.direct != "indirect"] <- "hh"
  
    # 1 per 1000 Sample Survey on Population Change
    data$source[data$source == "SSPC" & data$ihme.loc.id == "CHN_44533"] <- "1 per 1000 Survey on Pop Change"
    data$source[data$source == "china 1 0/00 population sample survey, 1 july 1987"] <- "1 per 1000 Survey on Pop Change"
    data$source[data$source == "0/00 sample survey 1990-98"] <- "1 per 1000 Survey on Pop Change"
     # all estimates should be marked as hh is not indirects
    data$in.direct[data$source == "1 per 1000 Survey on Pop Change" & data$in.direct == "direct" & data$data.age == "old"] <- "hh"
    data$in.direct[is.na(data$in.direct)&data$source == "1 per 1000 Survey on Pop Change"] <- "hh"
    
    # need to delete duplicates data.age old for years 1994, 1996, 1997, 1998 - from the source(0/00 sample survey 1990-98)
    data <- data[!(data$source == "1 per 1000 Survey on Pop Change" & data$source.date == "1994" & data$data.age == "old"),]
    data <- data[!(data$source == "1 per 1000 Survey on Pop Change" & data$source.date == "1996" & data$data.age == "old"),]
    data <- data[!(data$source == "1 per 1000 Survey on Pop Change" & data$source.date == "1997" & data$data.age == "old"),]
    data <- data[!(data$source == "1 per 1000 Survey on Pop Change" & data$source.date == "1998" & data$data.age == "old"),]
    
    # Census 
    # remove census duplicates
    data <- data[!(data$source == "calculated using 1990 census"),] 
    # need to delete duplicate
    data <- data[!(data$source == "census, 1 july 1990"),]
    data$in.direct[is.na(data$in.direct)& data$ihme.loc.id == "CHN_44533" & data$source == "DYB"] <- "hh" 
    data$source[data$ihme.loc.id == "CHN_44533" & data$source == "DYB"] <- "census"     # hh deaths from census
    
    # National Maternal and Child Health Surveillance System
    data$source[data$source == "survey on deaths of children under the age of five in china, the national collaborative group for survey of deaths under five"] <- "Maternal and Child Health Surveillance System"
    # the source below is from collaborator's dataset, the point directly above is for 1992, this is a duplicate with a point estimate in the dataset below
    data$source[data$source == "child and maternal surveillance system 1991-2004"] <- "Maternal and Child Health Surveillance System"
    # delete the old duplicate point - for 1991, old data.age
    data <- data[!(data$source == "Maternal and Child Health Surveillance System" & data$source.date == "1991" & data$data.age == "old"),]
    # all the years below 2000-2004 are deleted based on a line in the scrub code(getting rid of duplciates)
    data <- data[!(data$source == "Maternal and Child Health Surveillance System" & data$source.date %in% c(1996:2013) & data$data.age == "old"),]
    # label all points in this source as in.direct na - this is a surveillance system not a complete bh survey (direct)
    data$in.direct[data$source == "Maternal and Child Health Surveillance System" & data$in.direct != "indirect"] <- NA
    # replacing the sourcedate for the maternal points to match t for information obtained in 2009 - this is a surveillance system, data is collected annually 
    data$source.date[data$source == "Maternal and Child Health Surveillance System" & data$source.date == "2009"] <- data$t[data$source == "Maternal and Child Health Surveillance System" & data$source.date == "2009"]

    # National Sample Survey on Fertility and Birth Control  
    data$source[data$source == "journal publication using Sample survey on fertility nad birth control in China"] <- "1 per 1000 Survey on Fertility and Birth Control"
    data$source[data$source == "female fertility in china: a 1 0/00 population survey, 31 june 1982"] <- "1 per 1000 Survey on Fertility and Birth Control"
    data$source[data$source == "fertility sampling survey 1992"] <- "1 per 1000 Survey on Fertility and Birth Control" 
    
    # changing the in.direct type to hh for the survey below, these are old points labeled as direct, are not actually CBH points
    data$in.direct[data$source == "national survey on fertility and birth control 1988"] <- "hh"
    data$source[data$source == "national survey on fertility and birth control 1988"] <- "1 per 1000 Survey on Fertility and Birth Control" 
    # also changing points with "direct" to "hh" for all estimates under this source
    data$in.direct[data$source == "1 per 1000 Survey on Fertility and Birth Control" & data$in.direct == "direct"] <- "hh"
        
    data$t[data$ihme.loc.id == "CHN_44533" & data$source == "1 per 1000 Survey on Fertility and Birth Control" & data$source.date == "1992"] <- 1991
    
# ------------------------------------------------------------------------------
# Duplicate birth histories
# ------------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Exclusion list
# ----------------------------------------------------------------------------

  data$t <- as.numeric(data$t)
  data$source[data$source == "UNDYB census"] <- "undyb census"

  # merge in country names
  data = merge(unique(country.to.iso3[!(is.na(country.to.iso3$iso3)),c("country","laxo.country", "iso3")]), data, all.y = TRUE,by="iso3")
 
  # apparently, when laxo.country exists, we want to have it replace country
  data$country[!is.na(data$laxo.country)] = data$laxo.country[!is.na(data$laxo.country)]
    
  #colnames(data)[colnames(data) == "
  # Clean up data
  data = subset(data, !is.na(t) & !is.na(q5)) 
  
  # Get rid of laxo.country
  data$laxo.country <- NULL
  
  # Get rid of duplicates from merge 
  data <- unique(data)
  
  # Get rid of duplicates (duplicates in terms of everyting (iso3, q5, t, source, etc.) but country) from countries with the same iso3 code but different country name
  data <- data[!duplicated(data[-2]),]
  
  # Throw out collaborator's vital registration data
  data = subset(data, !(data$source == "who vital registration" & (data$ihme.loc.id != "PLW" & data$ihme.loc.id != "TUV" & data$ihme.loc.id != "COK" & data$ihme.loc.id != "MCO")))
  data = subset(data, !(data$source == "who vital registration k" & (data$ihme.loc.id != "PLW" & data$ihme.loc.id != "TUV" & data$ihme.loc.id != "COK" & data$ihme.loc.id != "MCO")))
  data = subset(data, !(data$source == "vital registration" & (data$ihme.loc.id != "PLW" & data$ihme.loc.id != "TUV" & data$ihme.loc.id != "COK" & data$ihme.loc.id != "MCO")))
  data = subset(data, !(data$source == "vital registration, euphin" & (data$ihme.loc.id != "PLW" & data$ihme.loc.id != "TUV" & data$ihme.loc.id != "COK" & data$ihme.loc.id != "MCO")))
  data = subset(data, !(data$source == "social monitor vital registration" & (data$ihme.loc.id != "PLW" & data$ihme.loc.id != "TUV" & data$ihme.loc.id != "COK" & data$ihme.loc.id != "MCO")))
  data = subset(data, !(data$source == "vital registration and census" & (data$ihme.loc.id != "PLW" & data$ihme.loc.id != "TUV" & data$ihme.loc.id != "COK" & data$ihme.loc.id != "MCO")))
 
  # Drop censuses from collaborator's data if there are new data with the same source.date and more than 5 points
  print("dropping censuses from collaborator's data if there are new data with same source.data and more than 5 points (i.e. indirect)")
  
  census_dup <- table(data[grep("census",data$source),c("ihme.loc.id","source","source.date")])

  for(iii in 1:dim(census_dup)[1]) {
    if(iii %% 10 == 0) cat(paste(iii," of ",dim(census_dup)[1],"\n",sep="")); flush.console();
    for(jjj in 1:dim(census_dup)[3]) {
      if(length(grep("undyb census",rownames(census_dup[iii,,]))) != 0) {
          if(census_dup[iii,"undyb census",jjj] >= 5) {
            for(lll in 1:dim(census_dup)[2]) {
              if(rownames(census_dup[iii,,])[lll] != "undyb census" & census_dup[iii,lll,jjj] > 0) {
                data = subset(data, !(data$data.age != "new" & data$ihme.loc.id == rownames(census_dup)[iii] & data$source.date == colnames(census_dup[iii,,])[jjj] & data$source == rownames(census_dup[iii,,])[lll]))
                
              }
            }
          }
      }
    }
  }

## Eliminate these from the data set. They will not be graphed.

  scrub <- rep(F, length(data$t)) 
  scrub[data$t < 1400] <- T
  
  # Remove old data from collaborator's dataset that is now entered via statcompiler. 
  scrub[data$country == "Turkmenistan" & tolower(data$source) == "dhs" & data$data.age=="old" & data$in.direct=="direct" ] <- T
  scrub[data$country == "Azerbaijan" & tolower(data$source) == "reproductive health survey" & data$in.direct=="null" & data$data.age=="old" ] <- T
  scrub[data$country == "Romania" & tolower(data$source) == "rhs-cdc" & data$in.direct=="direct" & data$data.age=="old" ] <- T
  
  # Remove ambiguous sources 
  scrub[tolower(data$source) == "life tables" & data$data.age=="old" & data$in.direct=="null" ] <- T
  
  ##Remove old WFS that have been reanalyzed
  scrub[data$country == "Republic of Korea" & tolower(data$source) == "national fertility survey (wfs), october 1974"] <- T
  
  #Remove old data that has been reanalyzed
  scrub [data$country == "Niger" & tolower(data$source) == "demographic and health survey 2006 - preliminary"] <- T
  scrub [data$country == "Rwanda" & tolower(data$source) == "preliminary dhs" & data$sourcedate == 2005] <- T
  
  #Remove DHS Preliminary Report that has been reanalyzed
  scrub[data$ihme.loc.id == "ALB" & tolower(data$source) == "dhs preliminary report" & data$source.date=="2008" & data$data.age == "new" & data$in.direct == "direct"] <- T
  
  #'file has Benin Census included twice. Estimates are exactly the same.
  scrub[data$ihme.loc.id == "BEN" & tolower(data$source) == "census february 1992" & data$source.date=="1992" & data$data.age == "new"] <- T  
  
  #1988 'family health survey' 1988 sourcedate from collaborator's is actually a DHS, which has been reanalyzed. Remove both directs and indirects
  scrub[data$ihme.loc.id == "BWA" & tolower(data$source) == "family health survey" & data$source.date=="1988" & data$data.age == "old"] <- T  
  
  #DZA 1992 enquete nationale sur les objectifs is actually the same as PAPCHILD, which actually moonlights as MICS. Remove from collaborator's dataset
  scrub[data$ihme.loc.id == "DZA" & tolower(data$source) == "enquete nationale sur les objectifs" & data$source.date=="1992" & data$data.age == "old"] <- T  
  
  #ECU 2004 Prelim RHS from collaborator's old-- has been reanalyzed.
  scrub[data$ihme.loc.id == "ECU" & tolower(data$source) == "encuesta demografica y de salud materna e infantil 2004 - preliminary" & data$source.date=="2004" & data$data.age == "old"] <- T    

  #ECU 2004 RHS from collaborator's old- directs and indirects have been reanalyzed.
  scrub[data$ihme.loc.id == "ECU" & tolower(data$source) == "encuesta demografica y de salud materna e infantil" & data$source.date=="1989" & data$data.age == "old"] <- T
  
  #TLS Census 2004- old indirects replaced with new indirect. Dropped from collaborator's dataset.
  scrub[data$ihme.loc.id == "TLS" & tolower(data$source) == "census" & data$source.date=="2004" & data$data.age == "old"] <- T
  
  #1970 RWA Census has been reanalyzed. Remove from collaborator's old dataset.  
  scrub[data$ihme.loc.id == "RWA" & tolower(data$source) == "enquete demographique" & data$source.date=="1970" & data$data.age == "old"] <- T
  
  #1978 RWA Census has been reanalyzed. They are entered twice; once from UNDYB census, and another. 
  scrub[data$ihme.loc.id == "RWA" & tolower(data$source) == "rwanda 1978 census" & data$source.date=="1978" & data$data.age == "new"] <- T
  
  #Remove census 1991 old point with no indirect type- this has been reanalyzed. 
  scrub[data$ihme.loc.id == "RWA" & tolower(data$source) == "census 1991" & data$source.date=="1991" & data$data.age == "old"] <- T
  
  #Remove 1979 encuesta nacional de fecundidad old indirects from collaborator's old. These are actually WFS and have been reanalyzed.
  scrub[data$ihme.loc.id == "PRY" & tolower(data$source) == "encuesta nacional de fecundidad" & data$source.date=="1979" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  
  #Remove 1995 OMN Family Health Survey from collaborator's- has been reanalyzed. 
  scrub[data$ihme.loc.id == "OMN" & tolower(data$source) == "family health survey" & data$source.date=="1995" & data$data.age == "old" & data$in.direct == "direct"] <- T
  
  #Remove MRT "emip" from collaborator's old- this is actually DHS, and has been reanalyzed (both directs and indirects) 
  scrub[data$ihme.loc.id == "OMN" & tolower(data$source) == "emip" & data$source.date=="2000" & data$data.age == "old"] <- T
  
  #Remove MAR "enquete demographique, 9 may-11 november 1966" there is a duplicate for this in collaborator's dataset (both sources are old). 
  scrub[data$ihme.loc.id == "MAR" & tolower(data$source) == "enquete demographique, 9 may-11 november 1966" & data$source.date=="1966" & data$data.age == "old"] <- T

  #Remove BRA 2000 Census (null olds) from collaborator's old dataset. These have been reanalyzed. 
  scrub[data$ihme.loc.id == "BRA" & tolower(data$source) == "census" & data$source.date=="2000" & data$data.age == "old"] <- T
  
  #Remove EGY DHS Directs 2000, 2003 from collaborator's old dataset. These have been reanalyzed. Misspelling in sourcename is in original file. 
  scrub[data$ihme.loc.id == "EGY" & tolower(data$source) == "egpyt dhs" & data$source.date=="2000" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "EGY" & tolower(data$source) == "egpyt dhs" & data$source.date=="2003" & data$data.age == "old"] <- T
  
  # Already have these points in the database
  scrub[data$country == "BanglUSER" & tolower(data$source) == "srs" & data$t == 2002.5 & data$q5 < 60] <- T
  scrub[data$country == "Haiti" & tolower(data$source) == "enquete haitienne sur la prevalence de la contraception, 1983"] <- T
  scrub[data$country == "Indonesia" & tolower(data$source) == "census, 24 september 1971"] <- T
  scrub[data$country == "Iraq" & tolower(data$source) == "child mortality"] <- T
  scrub[data$country == "Iraq" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Malawi" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Malaysia" & tolower(data$source) == "world fertility survey, august-december 1974"] <- T
  scrub[data$country == "Mongolia" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Namibia" & tolower(data$source) == "census"] <- T
  scrub[data$country == "Nepal" & tolower(data$source) == "contraceptive prevalence survey"] <- T
  scrub[data$country == "Niger" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Ghana" & tolower(data$source) == "multiple indicator cluster survey" & data$source.date == "1984"] <- T
  scrub[data$country == "Gambia" & tolower(data$source) == "census" & data$source.date == "1973"] <- T
  scrub[data$country == "Sri Lanka" & tolower(data$source) == "census, 9 october 1971"] <- T
  scrub[data$country == "Sierra Leone" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Sudan" & tolower(data$source) == "census" & data$source.date == "1983"] <- T
  scrub[data$country == "Sudan" & tolower(data$source) == "census" & data$source.date == "1993"] <- T
  scrub[data$country == "Syrian Arab Republic" & tolower(data$source) == "census, 23 september 1970"] <- T
  scrub[data$country == "Syrian Arab Republic" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Tajikistan" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Thailand" & tolower(data$source) == "census"] <- T
  scrub[data$country == "Timor Leste" & tolower(data$source) == "census"] <- T
  scrub[data$country == "Turkey" & tolower(data$source) == "census" & (data$source.date == "1970" | data$source.date == "1980" | data$source.date== "1985") ] <- T
  scrub[data$country == "Turkmenistan" & tolower(data$source) == "tmd"] <- T
  scrub[data$country == "Uganda" & tolower(data$source) == "undyb census" & (data$source.date == "1991")] <- T
  scrub[data$country == "United Arab Emirates" & tolower(data$source) == "census, 31 december 1975"] <- T
  scrub[data$country == "United Republic of Tanzania" & tolower(data$source) == "census" & data$source.date == "1988"] <- T
  scrub[data$country == "Uruguay" & tolower(data$source) == "census 1985"] <- T
  scrub[data$country == "Uzbekistan" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Venezuela" & tolower(data$source) == "undyb census" & data$source.date == "1990"] <- T
  scrub[data$country == "Viet Nam" & tolower(data$source) == "undyb census" & data$source.date == "1999"] <- T
  scrub[data$country == "Zambia" & tolower(data$source) == "census" & data$source.date == "1980"] <- T
  scrub[data$ihme.loc.id == "IRN" & tolower(data$source) == "dhs rural"] <-T
  scrub[data$ihme.loc.id == "IRN" & tolower(data$source) == "dhs urban"] <-T
  scrub[data$ihme.loc.id == "AFG" & tolower(data$source) == "census 1979 sample"] <- T
  scrub[data$ihme.loc.id == "SWE" & tolower(data$source) == "undyb census"] <- T
  scrub[data$ihme.loc.id == "MLI" & tolower(data$source) == "census" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "HTI" & tolower(data$source) == "demographic and health survey 2005-2006 - preliminary" & data$source.date == "2005"] <-T      # we've analyzed this ourselves and can drop the preliminary report estimates
  scrub[data$ihme.loc.id == "HND" & tolower(data$source) == "encuesta nacional de epidemiología y salud familiar 2001" & data$source.date == "2001"] <-T   # same as CDC-RHS for the same year.
  scrub[data$ihme.loc.id == "IND" & tolower(data$source) == "national family health survey"] <- T                                                          # same as DHS, so dropping duplicates
  scrub[data$ihme.loc.id == "IDN" & tolower(data$source) == "annual national socio-economic survey"] <- T                                                  # same as SUSENAS, so dropping duplicates
  scrub[data$ihme.loc.id == "KEN" & tolower(data$source) == "census" & data$source.date == "1989" & data$data.age == "old"] <-T                            #we have this census from IPUMS as well.
  scrub[data$ihme.loc.id == "LSO" & tolower(data$source) == "census, 12 april 1986" & data$data.age == "old"] <- T                                         #we have this reanalyzed this census and have two copies.
  scrub[data$ihme.loc.id == "LSO" & tolower(data$source) == "demographic and health survey 2004-2005" & data$data.age == "old"] <- T                       #we have new and old copies of this DHS.
  scrub[tolower(data$source) == "tmd" & data$data.age == "old"] <- T                                                                                #these are calculated from VR--we are going to add these back in for all available countries for years we do not have VR data for already.
  scrub[data$ihme.loc.id == "MLI" & tolower(data$source) == "census 1987" & data$data.age == "old"] <- T                                                   #we have these cenuses for new and old dataage.
  scrub[data$ihme.loc.id == "MLI" & tolower(data$source) == "census, 1-6 december 1976" & data$data.age == "old"] <- T                                     # see above. 
  scrub[data$ihme.loc.id == "MEX" & tolower(data$source) == "encuesta nacional sobre fecundidad y salud" & data$source.date == "1987" & data$data.age == "old"] <- T # this is the same as the 1987 DHS 
  scrub[data$ihme.loc.id == "MEX" & tolower(data$source) == "encuesta nacional de fecundidad" & data$source.date == "1977" & data$data.age == "old"] <- T  # this is the same as the 1976 WFS.
  scrub[data$ihme.loc.id == "MOZ" & tolower(data$source) == "census" & data$source.date == "1997" & data$data.age == "old"] <- T                           #we have a new and old version of this census.
  scrub[data$ihme.loc.id == "IRQ" & tolower(data$source) == "child and maternal mortality survey 1999 (weighted average between the 2)" & data$source.date == "1999" & data$data.age == "old"] <- T # we're taking out 'weighted average' and leaving 'single year' for this source.
  scrub[data$ihme.loc.id == "ZAF" & tolower(data$source) == "living standard measurement study 1993" & data$source.date == "1993" & data$data.age == "old"] <- T #we have reanalyzed this.
  scrub[data$ihme.loc.id == "ZAF" & tolower(data$source) == "demographic and health survey 2003 - preliminary" & data$source.date == "2003" & data$data.age == "old"] <- T #we don't need preliminary and final report.
  scrub[data$ihme.loc.id == "LKA" & tolower(data$source) == "registrar" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "LKA" & tolower(data$source) == "demographic and health survey, january-march 1987" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "TJK" & tolower(data$source) == "living standard measurement study 1999"] <- T                                                 # drop indirect LSMS 1999 old Tajikistan
  scrub[data$ihme.loc.id == "TLS" & tolower(data$source) == "ifhs"] <- T                                                                                   # drop IFHS from Timor Leste
  scrub[data$ihme.loc.id == "ZWE" & tolower(data$source) == "demographic and health survey 2005-2006 - preliminary"] <- T                                  # drop data that has been reanalyzed since official report was released
  scrub[data$ihme.loc.id == "NGA" & tolower(data$source) == "preliminary dhs" & data$source.date=="2003"] <- T                                             # Drop this because it has been reanalyzed since it was officially released.
  scrub[data$ihme.loc.id == "NGA" & tolower(data$source) == "dhs preliminary report" & data$source.date=="2008"] <- T                                      # Drop this because it has been reanalyzed since it was officially released.
  scrub[data$ihme.loc.id == "BEN" & tolower(data$source) == "census february 1992" & data$source.date == "1992"] <- T                                      # we have two censuses from this year 
  
  #drop points from the old dataset where there is a single estimate from an indirect source
  scrub[data$ihme.loc.id == "DZA" & tolower(data$source) == "enquete fecondite" & data$source.date=="1970" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "DZA" & tolower(data$source) == "multiple indicator cluster survey 1995" & data$source.date=="1995" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "BDI" & tolower(data$source) == "census" & data$source.date=="1983" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "GNQ" & tolower(data$source) == "census 1983" & data$source.date=="1983" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "GMB" & tolower(data$source) == "contraceptive prevalence and fertility determinants survey 1990" & data$source.date=="1990" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "IDN" & tolower(data$source) == "census 2000" & data$source.date=="2000" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "NRU" & tolower(data$source) == "census 1992" & data$source.date=="1992" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "NER" & tolower(data$source) == "census 1988" & data$source.date=="1988" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "NRU" & tolower(data$source) == "census 1992" & data$source.date=="1992" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "MDA" & tolower(data$source) == "multiple indicator cluster survey 2000" & data$source.date=="2000" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "WSM" & tolower(data$source) == "demographic and vital statistics survey 2000" & data$source.date=="2000" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "SEN" & tolower(data$source) == "multiple indicator cluster survey 1996" & data$source.date=="1996" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "SEN" & tolower(data$source) == "multiple indicator cluster survey 2000" & data$source.date=="2000" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "SDN" & tolower(data$source) == "safe motherhood survey" & data$source.date=="1999" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  
  # this point is an extreme outlier and census produced other plausible points we use. 
  scrub[data$ihme.loc.id == "PNG" & tolower(data$source) == "undyb census" & data$year=="1965.627"]<-T

  # we re-estimated these using the new methods
  scrub[data$ihme.loc.id == "IRN" & tolower(data$source) == "dhs (M)"] <- T
  
  scrub[data$data.age == "old" & tolower(data$source) %in% c("multiple indicator cluster survey","mics","dhs","DHS","demographic health survey") & data$ihme.loc.id !="AFG" & data$ihme.loc.id != "TKM"] <- T

  # was outlier - drop instead 
  scrub[data$ihme.loc.id == "CIV" & tolower(data$source) == "census 1998"] <- T
  
  # We have analyzed Egypt DHS and need to remove old estimates from collaborator's dataset
  scrub[data$ihme.loc.id == "EGY" & tolower(data$source) == "egypt dhs" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "EGY" & tolower(data$source) == "preliminary dhs" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "ECU" & tolower(data$source) == "encuesta demografica y de salud familiar" & data$data.age == "old"] <- T # This is the same as DHS 1987.
  scrub[data$ihme.loc.id == "MDV" & tolower(data$source) == "pv" & data$data.age == "old"] <- T #These appear to be duplicates of VR data 
  scrub[data$ihme.loc.id == "MDV" & tolower(data$source) == "ministry of health vital statistics - vital registration" & data$data.age == "old" & data$t!="1995" & data$t!="1997" & data$t!="1998" & data$t!="1999"] <- T # these also appear to be duplicates of VR. Leaving in the years we don't have VR for

  # These have been reanalyzed.
  scrub[data$ihme.loc.id == "CHN_44533" & tolower(data$source) =="calculated using 1990 census" & data$data.age== "new"] <- T
  scrub[data$ihme.loc.id == "CHN_44533" & tolower(data$source) =="census, 1 july 1990" & data$data.age== "old"] <- T
  
  # This DHS has been reanalyzed. (Remove SLE DHS 2008 Preliminary Report)
  scrub[data$ihme.loc.id == "SLE" & tolower(data$source) =="dhs preliminary report" & data$source.date=="2008"] <- T
 
  # Remove MDA 2005 DHS preliminary report data points. We have analyzed the microdata.
  scrub[data$ihme.loc.id == "MDA" & tolower(data$source) == "preliminary dhs" & data$source.date=="2005"] <- T
  
  # scrub old papchild estimates from collaborator's files 
  scrub[data$ihme.loc.id == "SDN" & data$source == "pan arab for child development survey"] <- T
  scrub[data$ihme.loc.id == "MRT" & data$source == "mchs"] <- T
  scrub[data$ihme.loc.id == "EGY" & data$source == "eps"] <- T
  scrub[data$ihme.loc.id == "YEM" & data$source == "pap"] <- T
  scrub[data$ihme.loc.id == "DZA" & data$source == "enquete algerienne sur la sante de la mere/enfant"] <- T
  scrub[data$ihme.loc.id == "SYR" & data$source == "maternal and child health survey 1993"] <- T
  scrub[data$ihme.loc.id == "TUN" & data$source == "pap"] <- T
  scrub[data$ihme.loc.id == "LBY" & data$source == "maternal and child health survey 1995"] <- T
  scrub[data$ihme.loc.id == "LBN" & data$source == "maternal and child health survey 1996"] <- T
  scrub[data$ihme.loc.id == "MAR" & data$source == "pap"] <- T
  
  # there are duplicates sources in china that need to go
  scrub[data$ihme.loc.id == "CHN_44533" & data$source=="undyb census"  & data$in.direct=="indirect, MAC only"] <- T
  scrub[data$ihme.loc.id == "CHN_44533" & data$source=="child and maternal surveillance system 1991-2004" & (data$source.date=="2000" | data$source.date=="2001" | data$source.date=="2002" | data$source.date=="2003" | data$source.date=="2004")] <- T
  
  # PAPCHILD and DHS are the same survey in Yemen
  scrub[data$ihme.loc.id == "YEM" & grepl("PAPCHILD", data$source)] <- T 
  
  # Get rid of undyb IDN census for 1980 and 1990 because we've reanalyzed the microdata 
  scrub[data$ihme.loc.id == "IDN" & data$source=="undyb census" & data$in.direct=="indirect, MAC only" & (data$source.date==1980 | data$source.date==1990)] <- T 
    
  # Get rid of several points from DHS reports where we have reanalyzed the data
  scrub[data$ihme.loc.id == "CIV" & data$source == "demographic and health survey 2005 - preliminary" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "ARM" & data$source == "demographic and health survey 2005 - preliminary" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "MWI" & data$source == "demographic and health survey 2004" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "RWA" & data$source == "preliminary dhs" & data$in.direct == "direct"] <- T

  # Get rid of the old PNAD surveys for Brazil (this survey was not nationally representative until 2040)
  scrub[data$ihme.loc.id == "BRA" & data$source == "pesquisa nacional por amostra de domicilios, october-december 1973"] <- T
  scrub[data$ihme.loc.id == "BRA" & data$source == "pesquisa nacional por amostra de domicilios, october-december 1972"] <- T
  scrub[data$ihme.loc.id == "BRA" & data$source == "pesquisa nacional por amostra de domicilios, 28 november 1976"] <- T
 
  # deleting a single direct estimate from collaborator's dataset that seems to represent vr data and is too late to be an accurately classified as a WFS
  scrub[(data$ihme.loc.id == "JAM" & data$source == "contraceptive prevalence survey" & data$source.date == "1989")] <- T
  
  # Drop KHM 2008 Census (much too high- error in death numbers posted online?)
  scrub[data$ihme.loc.id == "KHM" & data$source == "census 2008" & data$in.direct == "hh" & data$source.date == "2008"] <- T 
  
  # Dropping the MOZ CDC-RHS survey (only women 15-24 were asked BH questions)
  scrub[data$ihme.loc.id == "MOZ" & data$source == "CDC RHS 2001" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "MOZ" & grepl("CDC-RHS", data$source) & data$in.direct == "direct"] <- T
  
  # Scrub ridiculous Tonga survey point
  scrub[data$ihme.loc.id == "TON" & data$source == "SURVEY"] <- T

  # Scrub non-nationally representative global fund household surveys
  scrub[data$source == "GLOBAL FUND 2008" & data$in.direct == "direct" & data$ihme.loc.id %in% c("BFA", "ETH", "ZMB")] <- T
 
  # Scrub Ethiopia 2007 HH deaths point - way too high 
  scrub[data$source == "Ethiopia 2007 Census" & data$source.date == "2007" & data$in.direct == "hh"] <- T
  
  # Scrub old Gulf Family Health Survey points
  scrub[data$ihme.loc.id == "ARE" & data$source == "gulf family health survey 1995" & data$data.age=="old"] <- T
  scrub[data$ihme.loc.id %in% c("BHR","QAT","SAU") & data$source == "gfh" & data$data.age=="old"] <- T
  scrub[data$ihme.loc.id == "OMN" & data$source == "Oman Family Health Survey (1995)" & data$data.age=="new"] <- T

  # India duplicates and problems
  scrub[data$ihme.loc.id == "IND" & data$source == "SRS vital registration, Bhat 1998"] <- T   # have SRS data from our main folder do not need estimates from this paper
  scrub[data$ihme.loc.id == "IND" & data$source == "India SRS" & floor(data$t) %in% c(1992, 1993, 1994, 1995)] <- T   # we have SRS data, India SRS represents an aggregate estimate pulled from an outside source
  scrub[data$ihme.loc.id == "IND" & data$source == "SRS" & data$source.date == 2007 & data$in.direct == "na"] <- T
  scrub[data$ihme.loc.id == "IND" & data$source == "SRS" & data$t == 1996.5] <- T
  
  # getting rid of Malawi duplicate census point
  scrub[data$ihme.loc.id == "MWI" & data$source == "CENSUS" & floor(data$t) == 1998 & round(data$q5) == 222] <- T
  
  ############## All additions after this line occured after the Lancet 2011 mdg paper was published ##################################
  # duplicate 1985 NIC survey
  scrub[data$ihme.loc.id == "NIC" & data$source.date == "1985" & data$data.age == "old"] <- T
  
  # added a summary birth history for SAU that only had a 1-year recall
  scrub[data$ihme.loc.id == "SAU" & data$source.date == "2006" & data$source == "Population Bulletin 2007"] <- T
  
  # INED points are really from surveys already included in our system - need to exlude points
  scrub[data$source == "INED via Tabutin (1991)"] <- T 
  
  # deleting duplicate estimates that have been discovered in the mortality book research process
  # 1. Report estimates for things we've added microdata estimates
  scrub[data$ihme.loc.id == "KHM" & data$source == "DHS Report" & substr(data$source.date, 1, 4)=="2010" & data$in.direct == "direct"] <- T 
  scrub[data$ihme.loc.id == "MWI" & data$source == "DHS Report" & substr(data$source.date, 1, 4)=="2010" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "BOL" & data$source == "DHS preliminary report" & data$source.date == "2008" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id %in% c("PRY", "SLV", "UKR") & data$source == "cdc rhs statcompiler"] <- T # we've analyzed the microdata for this 
  scrub[data$ihme.loc.id == "PHL" & data$source == "DHS preliminary report" & data$source.date == "2008" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "TLS" & data$source == "DHS final report" & data$source.date == "2003" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "PAK" & data$source == "living standards survey" & data$source.date == "1991" & data$in.direct == "indirect"] <- T
            
  # 2. Probable duplicates of other things from collaborator's dataset
  scrub[data$ihme.loc.id == "KHM" & data$source == "nhs" & data$source.date == "1998" & data$data.age == "old"] <- T  # this is a DHS 
  scrub[data$ihme.loc.id == "MDG" & data$source == "enquete demographique, 9 may-11 november 1966" & data$source.date == "1966" & data$in.direct == "indirect" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "MWI" & data$source == "demographic survey, november-december 1982" & data$data.age == "old"] <- T  
  scrub[data$ihme.loc.id == "HND" & data$source == "encuesta nacional de epidemiologia y salud familiar, september 1991-february 1992" & data$data.age == "old"] <- T # this is a CDCRHS
  scrub[data$ihme.loc.id == "BOL" & data$source == "encuesta demografica nacional, june-october 1975" & data$source.date == 1975 & data$data.age=="old"] <- T # new source by same name with same date
  scrub[data$ihme.loc.id == "BOL" & data$source == "encuesta nacional de poblacion y vivienda, september 1988" & data$source.date == "1988" & data$data.age=="old"] <- T  # new source by similiar name with same source date
  scrub[data$ihme.loc.id == "CRI" & data$source == "encuesta de fecundidad y salud" & data$source.date == "1986" & data$data.age == "old"] <- T # this is a CDCRHS
  scrub[data$ihme.loc.id == "DZA" & data$source == "enquete demographique, august 1969-march 1971" & data$data.age == "old" & data$source.date == "1970"] <- T # this is a duplicate of another old source
  scrub[data$ihme.loc.id == "GHA" & data$source == "ghana dhs" & data$data.age == "old" & data$source.date == "1988" & data$in.direct == "direct"] <- T # we have this DHS
  scrub[data$ihme.loc.id == "GTM" & data$source == "encuesta nacional de fecundidad, planificacion familiar y comunicacion, september 1977-august 1978" & data$source.date == "1977"] <- T # this is a CDCRHS
  scrub[data$ihme.loc.id == "LKA" & data$source == "DHS Summary of Findings Report" & data$source.date == "1993"] <- T # duplicate of a more complete series in collaborator's dataset
  scrub[data$ihme.loc.id == "MDV" & data$source == "ministry of health vital statistics - vital registration"] <- T # we have VR for all of these years   
  scrub[data$ihme.loc.id == "MNG" & data$source == "STATYB"] <- T # this is probably VR, which we have
  scrub[data$ihme.loc.id == "PER" & data$source == "census" & data$source.date == 1993 & data$data.age == "old"] <- T # this is probably indirect, and we have this from IPUMS 
  scrub[data$ihme.loc.id == "PRY" & data$source == "encuesta nacional de fecundidad" & data$source.date == "1979" & data$data.age == "old"] <- T # this is the WFS 
  scrub[data$ihme.loc.id == "RWA" & data$source == "enquete nationale sur la fecondite" & data$source.date == "1983" & data$data.age == "old"] <- T # this is a WFS 
  scrub[data$ihme.loc.id == "PAK" & data$source == "pakistan integrated household survey" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "AFG" & data$source == "multiple indicator cluster survey" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "MWI" & data$source == "CENSUS" & data$source.date == 1998 & floor(data$q5) == 221] <- T
  
  #3. MOH data for CHN, have decided that we do not have an appropriate population to match with the deaths sample for MOH data
  scrub[data$ihme.loc.id == "CHN_44533" & data$source %in% c("VR_MOH_09C", "VR_MOH_10C")] <- T 
  
  #4. Drop censuses from Chile so that it will be treated as a VR only country 
  scrub[data$ihme.loc.id == "CHL" & data$source != "VR"] <- T
  
  #5. Unidentifiable or otherwise suspect sources 
  scrub[data$ihme.loc.id == "COD" & data$source == "average of east and west from coghlan"] <- T 
  scrub[data$ihme.loc.id == "VUT" & data$source == "reproductive, maternal and child health"] <- T 
  scrub[data$ihme.loc.id == "SLV" & data$source == "ehs" & data$data.age == "old"] <- T 
  
  #6. Duplicates of things in OECD 
  scrub[data$ihme.loc.id == "BGD" & data$source == "population growth estimation experiment 1962-1965" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "DZA" & data$source == "enquete demographique" & data$data.age == "old"] <- T
  
  #7. Duplicate (keeping the old version, not the new, because the old version has a longer time series)
  scrub[data$ihme.loc.id == "PSE" & data$source == "palestine bureau of statistics survey" & data$source.date == "1995-1999"] <- T # duplicate of 'nhs'
  
  ## additional scrubs
  scrub[data$ihme.loc.id == "SLV" & data$source == "fsl"] <- T # duplicate of RHS
  scrub[data$ihme.loc.id == "ETH" & data$source == "ethiopia demographic survey"] <- T # rural only 
  scrub[data$ihme.loc.id == "LSO" & data$source == "rural household consumption and expenditure survey, march 1968-may 1969"] <- T # rural only 
  scrub[data$ihme.loc.id == "LBR" & data$source == "undyb census" & data$source.date == "1971"] <- T # scrubbing this for now as it's not apparent which part of the population
  scrub[data$ihme.loc.id == "LBR" & data$source == "SURVEY" & data$source.date == "1970"] <- T       # growth experiment this refers to, whereas it is apparent what the 'old'
                                                                                              # versions of the data refer to  
  scrub[data$ihme.loc.id == "MAR" & grepl("PAPFAM", data$source)] <- T  # same survey as the DHS
  scrub[data$ihme.loc.id == "QAT" & data$source == "child health survey"] <- T # same as the 'Gulf Child Health Survey Report'
  
  # drop the PNG DHS 2006 per collaborator this is an unreliable survey
  scrub[data$ihme.loc.id == "PNG" & data$source == "DHS Preliminary report, via collaborator" & data$source.date == 2006] <- T
  
  # drop the SSPC survey in China that is in 1995. These don't happen in the same years as inter-census surveys
  scrub[data$ihme.loc.id == "CHN_44533" & data$source == "1 per 1000 Survey on Pop Change" & data$data.age=="old"] <- T
  
  # drop DSS points
  scrub[data$source == "DSS"] <- T
  
  # replacing with IPUMS
  scrub[data$ihme.loc.id == "TUR" & data$data.age == "old" & data$source == "census 2000"] <- T
  
  # scrub older Oman VR 
  scrub[data$ihme.loc.id == "OMN" & data$source == "VR" & data$t <= 2004] <- T
  
  # scrub the 2007 Community Survey (questionable quality) 
  scrub[data$ihme.loc.id == "ZAF" & data$source == "Community Survey, IPUMS"] <- T
  scrub[data$ihme.loc.id == "ZAF" & data$source == "SURVEY" & data$source.date == "2007"] <- T
  
  # scrub collaborator's point from Tajikistan 2001.9
  scrub[data$ihme.loc.id == "TJK" & data$source == "demographic survey 2002" & floor(data$t) == 2001] <- T
  
  # YEM 2007 annual statistical report point, because it is an aggregate estimate from the 2006 MICS3
  scrub[data$ihme.loc.id == "YEM" & data$source == "Yemen Annual Statistical Report" & data$source.date == "2007" & data$t == 2006.5] <- T 

  # YEM dhs statcompiler point because it's a duplicate of the DHS 1997 report that was added
  scrub[data$ihme.loc.id == "YEM" & data$source == "dhs statcompiler" & data$source.date == "1997"] <- T   
  
  # drop jordan 1988 epi/cdd and child mortality survey that is "old" data because I've added in data from the actual report
  scrub[data$ihme.loc.id == "JOR" & data$source == "epi/cdd and child mortality survey 1988" & data$source.date == 1988 & data$in.direct == "indirect" & data$data.age == "old"] <- T   
  
  # Drop CHN subnat DSP 91-94 because there's something wrong with the data (all zeros)
  scrub[data$source == "DSP" & data$ihme.loc.id != "CHN_44533" & data$t < 1996] <- T

  #Sourth Africa October HH survey
  scrub[data$source == "ZAF_OCT_HH_SURVEY_1993-95_1997-98"] <- T
  scrub[grepl("ZAF_OCT_HH_SURVEY", data$source)] <- T

  #Bolivia - these points were already outliered
  scrub[data$ihme.loc.id == "BOL" & data$source == "ENCUESTA DEMOGRAFICA NACIONAL 1975"] <- T

  #CHINA SUBNATIONAL- Scrub MCHS data aggregated from site level because MCHS gave us new data we will use instead that they aggregated
  scrub[data$source == "China MCHS"] <- T
  
  # Scrub collaborator's data that were not in CME info - 9 points
  scrub[data$ihme.loc.id == "DZA" & data$source == "census 1998" & data$source.date == 1998 & data$t == 1994.1] <- T 
  scrub[data$ihme.loc.id == "HTI" & data$source == "enquete demographique a passages repetes, august 1971-november 1973" & data$source.date == 1973 & data$t == 1972.5] <- T 
  scrub[data$ihme.loc.id == "IRQ" & data$source == "international study team 1991 (direct method)" & data$source.date == 1991 & data$t == 1988.5] <- T 
  scrub[data$ihme.loc.id == "JOR" & data$source == "verbal autopsy study 1995-1996" & data$source.date == 1995 & data$t == 1995.5] <- T 
  scrub[data$ihme.loc.id == "JOR" & data$source == "population and housing census survey 1994" & data$source.date == 1994 & data$t == 1994.5] <- T 
  scrub[data$ihme.loc.id == "LAO" & data$source == "living conditions survey 1992-1993" & data$source.date == 1993 & data$t == 1990.5] <- T 
  scrub[data$ihme.loc.id == "SYR" & data$source == "multi purpose survey 1999" & data$source.date == 1999 & data$t == 1995.5] <- T 
  scrub[data$ihme.loc.id == "UGA" & data$source == "national integrated household survey 1992" & data$source.date == 1992 & data$t == 1990.5] <- T 
 
  # Scrub collaborator's data that we've replaced with new sources
  scrub[data$ihme.loc.id == "BDI" & data$source == "mrs" & data$source.date == 1970 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "MWI" & data$source == "population change survey, february 1970-january 1972" & data$source.date == 1970 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "PNG" & data$source == "census, 7 july 1971" & data$source.date == 1971 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "HND" & data$source == "encuesta demografica nacional retrospectiva, july-october 1972" & data$source.date == 1972 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "CIV" & data$source == "eds" & data$source.date == 1979 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "KEN" & data$source == "census" & data$source.date == 1989 & data$data.age == "old"] <- T 
  scrub[data$ihme.loc.id == "COM" & data$source == "census 1980" & data$source.date == 1980 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "BGD" & data$source == "contraceptive prevalence survey 1981" & data$source.date == 1981 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "CRI" & data$source == "encuesta de prevalencia anticonceptiva, january-april 1981" & data$source.date == 1981 & data$data.age == "old" & data$in.direct == "indirect"] <- T 

  # Scrub collaborator's data that we've replaced with new sources  
  scrub[data$ihme.loc.id == "JOR" & data$source == "demographic survey 1981" & data$source.date == 1981 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "PAK" & data$source == "census, 1 march 1981" & data$source.date == 1981 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "PER" & data$source == "encuesta nacional de prevalencia de anticonceptivos, august-december 1981" & data$source.date == 1981 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "HND" & data$source == "encuesta demografica nacional de honduras, july 1983-january 1984" & data$source.date == 1983 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "TUN" & data$source == "contraceptive prevalence survey" & data$source.date == 1983 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "EGY" & data$source == "egyptian contraceptive prevalence survey" & data$source.date == 1984 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "HND" & data$source == "encuesta nacional de salud materno infantil, february 1984-february 1985" & data$source.date == 1984 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  
  # Scrub more of collaborator's data that we've replaced with new sources
  scrub[data$ihme.loc.id == "PAK" & data$source == "contraceptive prevalence survey, october 1984-march 1985" & data$source.date == 1984 & data$data.age == "old" & data$in.direct == "indirect"] <- T  
  scrub[data$ihme.loc.id == "ARE" & data$source == "child health survey, march 1987-may 1988" & data$source.date == 1987 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "CUB" & data$source == "encuesta nacoinal de fecundidad, november-december 1987" & data$source.date == 1987 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "HND" & data$source == "encuesta nacional de epidemiologia y salud familiar, june-november 1987" & data$source.date == 1987 & data$data.age == "old" & data$in.direct == "direct"] <- T 
  scrub[data$ihme.loc.id == "HTI" & data$source == "enquete mortalite, morbidite et utilisation des services, may-september 1987" & data$source.date == 1987 & data$data.age == "old"] <- T 
  scrub[data$ihme.loc.id == "SAU" & data$source == "child health survey" & data$source.date == 1987 & data$data.age == "old" & data$in.direct == "indirect"] <- T 
  scrub[data$ihme.loc.id == "BTN" & data$source == "national health survey 1994" & data$source.date == 1994 & data$data.age == "old" & data$in.direct == "direct"] <- T 
  
  # Scrub more of collaborator's data that we've replaced with new sources
  scrub[data$ihme.loc.id == "MRT" & data$source == "census 1988" & data$source.date == 1988 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "TUR" & data$source == "turkey population and health survey" & data$source.date == 1988 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "BHR" & data$source == "child health survey" & data$source.date == 1989 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "ETH" & data$source == "national family and fertility survey" & data$source.date == 1990 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "MDV" & data$source == "census" & data$source.date == 1990 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "NPL" & data$source == "fertility and family planning survey" & data$source.date == 1991 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "PNG" & data$source == "demographic and health survey 1991" & data$source.date == 1991 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "MEX" & data$source == "encuesta nacional de la dinamica demografica" & data$source.date == 1992 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "LKA" & data$source == "demographic and health survey, july-september 1993" & data$source.date == 1993 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "BTN" & data$source == "national health survey 1994" & data$source.date == 1994 & data$data.age == "old" & data$in.direct == "direct"] <- T    
  scrub[data$ihme.loc.id == "PNG" & data$source == "demographic and health survey 1996" & data$source.date == 1996 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "PAK" & data$source == "demographic survey 1997" & data$source.date == 1997 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "IRQ" & data$source == "child and maternal mortality survey 1999 (single year)" & data$source.date == 1999 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "JOR" & data$source == "annual ferility survey 1999" & data$source.date == 1999 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "SLB" & data$source == "census 1999" & data$source.date == 1999 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "LBN" & data$source == "multiple indicator cluster survey 2000 - preliminary report" & data$source.date == 2000 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "TKM" & data$source == "dhs" & data$source.date == 2000 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "LSO" & data$source == "demographic survey 2001" & data$source.date == 2001 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "MNG" & data$source == "reproductive health survey" & data$source.date == 2003 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  scrub[data$ihme.loc.id == "GMB" & data$source == "contraceptive prevalence and fertility determinants survey 1990" & data$source.date == 1990 & data$data.age == "old"] <- T    
  scrub[data$ihme.loc.id == "IRQ" & data$source == "international study team 1991 (direct method)" & data$source.date == 1991 & data$data.age == "old" & data$in.direct == "direct"] <- T    
  scrub[data$ihme.loc.id == "SAU" & data$source == "levels, trends and differentials of infant and child mortality" & data$source.date == 1990 & data$data.age == "old" & data$in.direct == "indirect"] <- T    
  
  # Scrub collaborator's data that we've replaced with new sources  
  scrub[data$ihme.loc.id == "CPV" & data$source == "census 2000" & data$source.date == 2000 & data$data.age == "old"] <- T 
  scrub[data$ihme.loc.id == "HND" & data$source == "census 2001" & data$source.date == 2001 & data$data.age == "old"] <- T 
  
  # drop CHN_44533 VR point (we have DSP)
  scrub[data$ihme.loc.id=="CHN_44533" & data$source=="VR" & data$t == 2015] <- T
  
  data <- data[!scrub,]

  scre <- read.csv("FILEPATH/scrub_reintroduce.csv")
  scre$ihme.loc.id <- scre$iso3
  scre <- scre[,names(scre) != "scrub.reason"]
  
    # do not want the 2007 ZAF community survey in here, because it is calculated on the wrong population.
    numbertobeaddedback <- nrow(scre)
    scre <- scre[!(scre$ihme.loc.id=="ZAF" & scre$source == "SURVEY" & scre$source.date == 2007),]
    nrow(scre) == numbertobeaddedback - 1
  
  data <- rbind(data,scre)
  
#########################################################################################
  # Identify outliers. These will be plotted, but grayed out and not included in the analysis

 #This flags indirect calculations based on 15-19 and 20-24 year-old women.
 #For the from="k" data we drop the two most recent, since these do not note which age groups the data is based on.
 #Note that these points are excluded from ALL analysis, but will appear in plots as small gray points.
  kill0 <- c()
  for (i in 1:length(unique(data$ihme.loc.id))) {
   indir <- (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect" & !data$data.age == "new") | (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect, MAC only")
   
   indir[is.na(indir)] <- FALSE
   if( sum(indir) != 0 ) {
   i.survs <- unique(paste(data$source[indir],data$source.date[indir]))            #indirect survey-years in a country (values) (direct points may also be associated with the type)
   for (j in 1:length(i.survs)) {
    indir2 <- paste(data$source,data$source.date)==i.survs[j] & data$ihme.loc.id==unique(data$ihme.loc.id)[i]     #data points associated with the survey-year known to have at least one indirect point in the country of interest
    mark = unique(which(indir &indir2))                                                                                      
    if (length(mark)>0) point1 <- mark[data$t[mark]==max(data$t[mark])]               #Find the last two 
    if (length(mark)>1) point2 <- mark[data$t[mark]==max(data$t[mark[mark!=point1]])]  #observations

    in.direct.nomissing <- data$in.direct[point2]
    if(is.na(in.direct.nomissing)) in.direct.nomissing <- ""
    
    if (data$compiling.entity[point2]=="u" | data$source[point2] == "undyb census" | in.direct.nomissing == "indirect, MAC only") kill0 <- c(kill0,point1)                                         #Only drop most recent if source is UNICEF or source is undyb census
    else kill0 <- c(kill0,point1,point2)                                                                 #concatenate 
   }
   }
  }

  
##########################
  kill <- rep(F,length(data$q5))  
  #Outlier last two points from SBH analyses
    for (i in 1:length(unique(data$ihme.loc.id))) {
   indir <- (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect" & !data$data.age == "new") | (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect, MAC only")
   
   indir[is.na(indir)] <- FALSE
   if( sum(indir) != 0 ) {
   i.survs <- unique(paste(data$source[indir],data$source.date[indir]))            #indirect survey-years in a country (values) (direct points may also be associated with the type)
   for (j in 1:length(i.survs)) {
    indir2 <- paste(data$source,data$source.date)==i.survs[j] & data$ihme.loc.id==unique(data$ihme.loc.id)[i]     #data points associated with the survey-year known to have at least one indirect point in the country of interest
    mark = unique(which(indir &indir2))                                                                                      
    if (length(mark)>0) point1 <- mark[data$t[mark]==max(data$t[mark])]               #Find the last two 
    if (length(mark)>1) point2 <- mark[data$t[mark]==max(data$t[mark[mark!=point1]])]  #observations

    in.direct.nomissing <- data$in.direct[point2]
    if(is.na(in.direct.nomissing)) in.direct.nomissing <- ""
    
    if (data$compiling.entity[point2]=="u" | data$source[point2] == "undyb census" | in.direct.nomissing == "indirect, MAC only") kill0 <- c(kill0,point1)                                         #Only drop most recent if source is UNICEF or source is undyb census
    else kill0 <- c(kill0,point1,point2)                                                                 #concatenate 
   }
   }
  }                                                        
  kill[kill0] <- T 

  #Then outlier some points
  kill[data$ihme.loc.id == "CZE" & grepl("census|RHS", data$source)] <- T
  kill[data$ihme.loc.id == "BOL" & data$source == "ENCUESTA DEMOGRAFICA NACIONAL 1975"] <- T
  kill[data$ihme.loc.id == "BRA" & data$source == "LSMS"] <- T
  kill[data$ihme.loc.id == "CPV" & data$source == "census 2000" & data$t < 2000] <- T
  kill[data$ihme.loc.id == "CAF" & data$source == "census" & floor(data$t) == 1987] <- T
  kill[data$ihme.loc.id == "CAF" & data$source == "undyb census" & floor(data$t) == 1972] <- T
  kill[data$country == "Latvia" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Latvia" & tolower(data$source) == "undyb census"])] <- T
  kill[data$country == "Estonia" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country=="Estonia" & tolower(data$source)=="undyb census" & data$source.date == 1989])] <- T
  kill[data$ihme.loc.id == "RUS" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "RUS" & tolower(data$source) == "undyb census"])] <- T
  kill[data$ihme.loc.id == "AFG" & grepl("DHS SP", data$source)] <- T
  kill[data$ihme.loc.id == "PNG" & floor(data$t) == 1965 & data$source == "undyb census"] <- T
  kill[data$ihme.loc.id == "KOR" & floor(data$t) == 1983 & data$q5 > 80] <- T
  kill[data$ihme.loc.id == "DOM" & data$t < 1962 & data$source == "DHS" & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "IRN" & data$source == "Iran household survey" & data$q5 > 300] <- T
  # again for direct because the source names aren't the same between SBH and CBH
  kill[data$ihme.loc.id == "IRN" & data$source == "Iran household survey 2000" & data$q5 > 300] <- T
  kill[data$ihme.loc.id == "YEM" & data$source == "DHS" & data$in.direct == "direct" & data$t < 1966 & data$t > 1964] <- T
  kill[data$ihme.loc.id == "BTN" & data$q5 >300 & data$source == "demographic sample survey 1984"] <- T
  kill[data$ihme.loc.id == "SDN" & data$t == 2008 & data$source == "CENSUS"] <- T
  kill[data$ihme.loc.id == "BLR" & data$source == "Census, IPUMS"] <- T
  kill[data$ihme.loc.id == "KOR" & data$source == "VR" & data$t < 1970] <- T
  kill[data$ihme.loc.id == "COG" & (floor(data$t) == 1978 | floor(data$t) == 1979)] <- T
  kill[data$ihme.loc.id == "RWA" & data$q5 > 250 & data$t > 1994 & data$t < 1997] <- T
  kill[data$ihme.loc.id == "TON" & data$source == "VR" & data$t == 1957] <- T
  kill[data$ihme.loc.id == "TCD" & data$source == "DHS" & data$t <1971] <- T
  kill[data$ihme.loc.id == "SDN" & data$source == "Census, IPUMS" & data$source.date == 2008 & data$t <1990] <- T
  kill[data$ihme.loc.id == "TON" & data$source == "undyb census" & data$source.date == 1966] <- T
  kill[data$ihme.loc.id == "PAK" & grepl("DHS", data$source) & data$t < 1969 & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "DJI" & grepl("PAPFAM", data$source) & data$in.direct == "direct" & data$q5 > 150] <-T
  kill[data$ihme.loc.id == "GEO" & grepl("VR", data$source) & data$t == 1993] <- T
  
  #Kill hh points
  kill[data$source == "HOUSEHOLD"] <- T
  
  ##Kill almost all other hh points
    #First assign "", "NULL", "na" in.directs as NAs
    data$in.direct[data$in.direct == "" | data$in.direct == "NULL" | grepl("na", data$in.direct, ignore.case = T)] <- NA  
    
    #Old collaborator DZA points
    data$in.direct[data$source == "indirect"] <- "indirect"
    
    #These are old collaborator points; most are censuses; some others
    data$in.direct[data$in.direct == "null"] <- NA
    
    #Exceptions are DSP, SRS, India, China, and PAK_demographic_survey points
    kill[(is.na(data$in.direct) & !grepl("VR",data$source) & !grepl("SRS",data$source) & !grepl("DSP",data$source) & !(data$ihme.loc.id %in% c("IND","CHN_44533")) & !data$ihme.loc.id == "PRK") | (data$in.direct == "hh" & !(data$ihme.loc.id %in% c("CHN_44533","IND")) & !data$source == "PAK_demographic_survey")] <- T 
    
    #Exception for India, China subnational-- will have to find another indicator for this
 
    kill[(grepl("CHN_",data$ihme.loc.id) | grepl("IND_",data$ihme.loc.id)) & !(data$ihme.loc.id == "CHN_44533") & !grepl("indirect",data$in.direct)] <- F
    
    #Exception for two South Africa household points. 
    kill[data$ihme.loc.id == "ZAF" & data$source == "SURVEY" & floor(data$t) == 2006]<- F

    #Exception for South Africa Rapid Mortality Report
    kill[data$ihme.loc.id == "ZAF" & grepl("Rapid Mortality",data$source)] <- F
	
	# Exception for Eastern Cape (ZAF_482) keep in the 2000 and 2006 census per collaborator comments
	kill[data$ihme.loc.id == "ZAF_482" & data$source == "CENSUS" & (data$t == 2006 | data$t == 2000)]<- F

    #Exception for Tonga 2006 HH point as it's the only thing we have besides VR
    kill[data$ihme.loc.id == "TON" & data$t == 2006 & data$source == "CENSUS"] <- F
	
  #####
  
  #SSD survey with huge bump
  kill[data$ihme.loc.id == "SSD" & data$source == "SSD_HH_HEALTH_SURVEY_2010_32189"] <- T
  
  #Noisy DHS CBH series - these points are really high
  kill[data$ihme.loc.id == "KAZ" & grepl("DHS", data$source) & data$in.direct == "direct" & data$t > 1996 & data$t < 1998] <- T

  #Implausible bump in SRB SBH-take out whole series
  kill[data$ihme.loc.id == "SRB" & data$source == "MICS3"] <- T
  
  #This is a bad DHS. We don't include it in microdata estimates and shouldn't keep report estimates.
  kill[data$ihme.loc.id == "NGA" & data$source == "NGA_DHS_1999_report20555"] <- T

  #Early VR that makes the VR correction bad in NIC
  kill[data$ihme.loc.id == "NIC" & grepl("vr",data$source, ignore.case = T) & data$t < 1988] <- T
  
  #Bring back SAU points so that VR will be incomplete and increase data variance
  kill[data$ihme.loc.id == "SAU" & data$t > 2000] <- F
  
  #Outlier oldest 6 DHS points in MOZ
  kill[data$ihme.loc.id== "MOZ" & grepl("DHS", data$source) & data$in.direct == "direct" & data$t < 1976] <- T
  
  #Outlier new MLI DHS - too low. DHS seems to think this is potentially the case.
  kill[data$ihme.loc.id == "MLI" & data$source == "MLI DHS Preliminary Report 77388"] <- T 

  #Outlier AGO MICS1 1996 report - too low and very noisy
  kill[data$ihme.loc.id == "AGO" & data$source == "AGO_MICS1report"] <- T
  
  #Outlier TJK LSMS completes - too low
  kill[data$ihme.loc.id == "TJK" & data$source == "LSMS" & data$in.direct == "direct"] <- T
  
  #Haiti VR too low
  kill[data$ihme.loc.id == "HTI" & data$source == "VR"] <- T
    
  #South Africa (ZAF) Household survey 2007 sbh's.
  kill[data$ihme.loc.id == "ZAF" & data$source == "Community Survey, IPUMS" & data$source.date == 2007] <- T

  #South Africa DHS 2003
  kill[data$ihme.loc.id == "ZAF" & data$source == "DHS final report"] <- T
  
  #Oman VR (two points) that is actually MOH hospital deaths (and so too low)
  kill[data$ihme.loc.id == "OMN" & data$source == "VR" & (data$t %in% c(2001, 2003))] <- T

  # Outlier Hainan 1982 census point because we have deaths and not pop so it's using the wrong pop
  kill[data$ihme.loc.id == "CHN_499" & data$source == "CENSUS" & data$t == 1982] <- T

  #First hh point in PRK (north korea) 
  kill[data$ihme.loc.id == "PRK" & data$t < 2000] <- T

  kill[data$ihme.loc.id == "CHN_493" & data$source == "CENSUS" & data$t == 1982] <- T

  #MEX 2010 Census
  kill[data$ihme.loc.id == "MEX" & data$source == "Census" & data$source.date == 2010] <- T

  #TON VR 83, 90 - implausibly low, causing other VR to get adjusted up implausibly as well
  kill[data$ihme.loc.id == "TON" & data$source == "VR" & (data$t == 1990 | data$t == 1983)] <- T

  #IRQ VR before 1950 - 5q0 too low in comparison with other countries in the region,
  kill[data$ihme.loc.id == "IRQ" & data$source == "VR" & data$t < 1970] <- T

  #Turkenistan data post 1999; different VR system
  kill[data$ihme.loc.id == "TKM" & data$source == "VR - TransMONEE" & data$t >=1999] <- T

  #Cuba 
  kill[data$ihme.loc.id == "CUB" & !(data$source %in% c("VR", "undyb census"))] <- T


  #outlier Japan 2011 (tsunami)
  kill[data$ihme.loc.id == "JPN" & data$t == 2011] <- T
  # Outlier the Japanese earthquake in Iwate and  Miyagi in 2011 and Hyogo 1995 since these are shocks
  kill[data$ihme.loc.id == "JPN_35426" & floor(data$t) == 2011 & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "JPN_35427" & floor(data$t) == 2011 & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "JPN_35451" & floor(data$t) == 1995 & data$source == "VR"] <- T
  
  # outlier CRI 2011 census 
  kill[data$ihme.loc.id == "CRI" & data$source == "133760#CRI_CENSUS_2011" & data$t < 2010 & data$t > 1990] <- T
  
  # outlier ZAF 2011 household point
  kill[data$ihme.loc.id == "ZAF" & data$source == "CENSUS" & data$t ==2011] <- T

  #outlier
  kill[data$ihme.loc.id == "ZAF" & data$source == "CENSUS" & data$source.date == 2001] <- T
  kill[data$ihme.loc.id == "ZAF" & data$source == "Census, IPUMS" & data$source.date == 2001] <- T

  # outlier the Botswana CBH
  kill[data$ihme.loc.id == "BWA" & data$source == "22125#BWA_FHS_2007_2008" & data$in.direct == "indirect" & data$t >2006] <- T

  #outlier VR in Bolivia 2000-2004 - nearly zero
  kill[data$ihme.loc.id == "BOL" & data$source == "VR" & data$t >= 2000 & data$t <= 2005] <- T

  # outlier  mchs point in 2000/2001
  kill[data$ihme.loc.id == "CHN_501" & data$t == 2000 & grepl("MCHS", data$source)] <- T

  # outlier pre-2004 DSP 
  kill[data$ihme.loc.id == "CHN_517" & data$t < 2004 & grepl("DSP",data$source)] <- T
  
  # Subnationals: there are several surveys where the sample sizes are too small so the 5q0 is being estimated as 0. This is incorrect, so these points should be outliered. 
  # In many cases, these points were being used as the reference and so were pulling down the trend
  kill[grepl("KEN_", data$ihme.loc.id) & data$q5 == 0.0000000] <- T
  kill[grepl("IND_", data$ihme.loc.id) & data$q5 == 0.0000000] <- T
  kill[grepl("BRA_", data$ihme.loc.id) & data$q5 == 0.0000000] <- T
  
  # Outlier PNAD before 2005 in the Amazon regions of Brazil (north-west region) because the PNAD pre-2005 did not include the indigenous populations and is therefor not representative
  kill[data$ihme.loc.id %in% c("BRA_4770", "BRA_4750", "BRA_4752", "BRA_4771", "BRA_4763", "BRA_4753", "BRA_4776") & data$source.date < 2005 & grepl("PNAD",data$source)] <- T		 

  # Outlier the Kenya AIS 2012 indirect in Kenya and its subnationals
   kill[grepl("KEN", data$ihme.loc.id) & data$in.direct == "indirect" & grepl("133219#KEN_AIS_2007", data$source) & data$source.date == 2012] <- T

  # Outlier the CBH from the 2007 LSMS in TJK because all of the estimates are almost at 0 (too low)
  kill[data$ihme.loc.id == "TJK" & data$source == "LSMS 2007" & data$in.direct == "direct"] <- T
  
  # Outlier the last VR point in Uruguay (2010) because the estimates are too high
  kill[data$ihme.loc.id == "URY" & data$source == "VR" & data$source.date == 2010] <- T
  
  # Outlier the household points in China and provinces
  kill[grepl("CHN_", data$ihme.loc.id) & !(data$ihme.loc.id %in% c("CHN_354", "CHN_361")) & data$source %in% c("FFPS", "DC")] <- T
  
  # Outlier the low SBH census point in FSM 
  kill[data$ihme.loc.id == "FSM" & data$source == "138577#FSM_CENSUS_1994" & data$source.date == 1994 & data$t < 1979 & data$q5 < 22] <- T
  
  # Outlier really high India DHS household point that is changing the data source adjustment for HH points
  kill[grepl("IND", data$ihme.loc.id) & data$source == "HOUSEHOLD_DHS_1998-1999"] <- T
  
  # Outlier really low nigeria VR point in 2007
  kill[data$ihme.loc.id == "NGA" & data$source == "VR"] <- T
  
  # Outlier the BGD 2009 MICS, it's way too high
  kill[data$ihme.loc.id == "BGD" & data$source == "MICS3" & data$source.date == 2009] <- T
  
  # Outlier the Brazil 1986 DHS because it's not representative at the national level (they only asked urban areas in several states), and they only surveyed women 15-44
  kill[grepl("BRA", data$ihme.loc.id) & (data$source == "DHS 1986" | (data$source == "DHS" & data$source.date == 1986))] <- T  
  
   # Outlier the DHS CBH in Western Cape (DHS 1998) per collaborator instructions
  kill[data$ihme.loc.id == "ZAF_490" & grepl("DHS", data$source) & data$in.direct == "direct"] <- T
  
  # Outlier the Pakistan 1981 census points, the outlier was lost when collaborator's data was replaced, but the points are still too high
  kill[data$ihme.loc.id == "PAK" & data$source == "9924#PAK 1981 Census Report" & data$source.date == 1981 & data$t >= 1979] <- T
	
  # outlier BIH MICS points
  kill[data$ihme.loc.id=="BIH" & data$source== "MICS4"] <- T

  # outlier KEN_35629 points that are close to 0
  kill[data$ihme.loc.id=="KEN_35629" & data$source.date== 2003.91662597656] <- T

  # bring back Saudi subnational HH points
  kill[grepl("SAU_", data$ihme.loc.id) & data$source %in% c("CENSUS", "HOUSEHOLD")] <- F

  # bring back ZAF subnat census points
  kill[grepl("ZAF", data$ihme.loc.id) & data$source=="CENSUS"] <- F

  # outlier Greece census points
  kill[data$ihme.loc.id=="GRC" & data$source=="Census, IPUMS"] <- T

  # outlier BFA points that are 0 after 2010
  kill[data$ihme.loc.id=="BFA" & data$source == "MIS 2014"] <- T

  # outlier MNP 1989 point 
  kill[data$ihme.loc.id=="MNP" & data$source == "VR" & data$t==1989] <- T

  # outlier IND_43903 SBH series close to 0 
  kill[data$ihme.loc.id=="IND_43903" & data$source == "IND_DLHS" & data$source.date==2003] <- T
  
  # outlier IND_4842 SBH series close to 0 
  kill[data$ihme.loc.id=="IND_4842" & data$source == "IND_DLHS_2007_STATE_23258" & data$source.date==2008] <- T

  # outlier IND_4854 SBH series close to 0
  kill[data$ihme.loc.id=="IND_4854" & data$source == "IND_DLHS" & data$source.date==2003] <- T

  # outlier IND_4864 SBH series close to 0
  kill[data$ihme.loc.id=="IND_4864" & data$source == "IND_DHS_1992_1993_STATE_19787" & data$source.date==1992] <- T

  # outlier IND_4869 SBH series close to 0 
  kill[data$ihme.loc.id=="IND_4869" & data$source == "IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T

  # outlier the SWE 2011
  kill[grepl("SWE", data$ihme.loc.id) & data$t==2011] <- T

  # outlier "other" ARE point in 1983
  kill[data$ihme.loc.id=="ARE" & data$source == "13224#ARE_CHS_1987_1988" & floor(data$t)==1983] <- T
  
  # outlier KEN_35620 points near 0 at around 1990
  kill[data$ihme.loc.id=="KEN_35620" & data$source=="DHS 2003" & data$source.date == 1990.25] <- T

  # outlier India subnational sources that are low from 1980's
  kill[data$ihme.loc.id=="IND_43885" & data$source=="IND_DLHS" & data$source.date==2003] <- T
  kill[data$ihme.loc.id=="IND_43909" & data$source=="India_DLHS3_urban_rural" & data$source.date==2008] <- T
  kill[data$ihme.loc.id=="IND_43921" & data$source=="IND_DLHS" & data$source.date==2003] <- T
  kill[data$ihme.loc.id=="IND_4842"  & data$source=="IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T
  kill[data$ihme.loc.id=="IND_4849" & data$source=="IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T
  kill[data$ihme.loc.id=="IND_4850" & data$source=="IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T
  kill[data$ihme.loc.id=="IND_4864" & data$source=="IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T


  kill[data$ihme.loc.id=="IND_43942" & data$source=="165390_IND_DLHS4_2012_2014"] <- T

  
  # kill 
  kill[data$ihme.loc.id=="IND_43900" & data$source=="5291#IND Census 1981" & data$source.date==2011 & data$q5>500] <- T
  kill[data$ihme.loc.id=="IND_43919" & data$source=="5291#IND Census 1981" & data$source.date==2011 & data$q5 > 500] <- T


  kill[data$ihme.loc.id=="DOM" & data$source=="VR" & data$t %in% c(2011, 2012)] <- T
  kill[data$ihme.loc.id=="LKA" & data$source=="VR" & data$t==2009] <- T
  
  # outliering in the KEN subnationals
  kill[data$ihme.loc.id=="KEN_35656" & data$source== "DHS 1993"] <- T
  
  #outlier messed up GBR data point
  kill[data$ihme.loc.id=="GBR" & data$t==2000] <- T 
  
  # outlier Serbia VR 1998-2007 because it does not include Kosovo
  kill[data$ihme.loc.id=="SRB" &  data$source == "VR" & (data$t %in% seq(1998, 2007))] <- T


  kill[data$ihme.loc.id=="MLI" & data$source=="MIS 2015" & data$t>2011 & data$in.direct=="direct" ] <- T # outlier Mali points close to 0
  kill[data$ihme.loc.id=="GBR" & data$t>=2014] <- T # outlier gbr point
  
  kill[data$q5 > 500 | data$q5 < 0] <- T
  kill[data$ihme.loc.id=="PRK"] <- T  # for now we don't want to use any of the PRK points because they are too low
  kill[data$ihme.loc.id=="NER" & data$source=="VR"] <- T  # get rid of p0 poimnt
  kill[data$ihme.loc.id=="IRN" & data$source=="Iran household survey 2000"] <- T  # series was too high
  kill[data$ihme.loc.id=="SRB" & data$source=="CENSUS"] <- F  # bring in HH points in Serbia
  kill[data$ihme.loc.id=="MNE" & data$source=="CENSUS"] <- F  # bring in HH points in Montenegro
  kill[grepl("CHN_",data$ihme.loc.id) & data$source=="VR" & data$t==2015] <- T # outlier VR points in 2015 in china subnats 
  
  kill[data$ihme.loc.id=="SAU_44543" & data$t < 2010] <- T # outlier Riyadh data 
  
  kill[data$ihme.loc.id=="COG" & data$source=="VR" & data$t==2009] <- T # outlier the one COG VR point because it is low
  kill[data$ihme.loc.id=="CHN_519" & data$t==2015 & data$source=="DSP"] <- T
  kill[data$ihme.loc.id=="KEN_35646" & data$source=="KEN_DHS_subnat" & data$source.date==2014] <- T # outlier this series
  kill[data$ihme.loc.id=="KEN_44796" & data$source=="KEN_DHS_oldprovince" & data$source.date==2014] <- T # outlier this series 
  kill[data$ihme.loc.id=="LBN" & data$source=="VR" & data$t %in% c(2009, 2010)] <- T
  kill[data$ihme.loc.id=="IND_43875" & data$source=="165390_IND_DLHS4_2012_2014" & data$source.date==2013] <- T # outlier this series 
  kill[data$ihme.loc.id=="MNE" & data$source=="CENSUS"] <- T # outlier census in Montenegro
  kill[data$ihme.loc.id=="IRN" & data$source=="VR" & data$t==1991] <- T # outlier VR point that is higher than the others
  kill[data$ihme.loc.id=="IRN" & data$in.direct=="direct"] <- F # unoutlier the survey CBH series that are outliered
  kill[data$ihme.loc.id=="BWA" & data$source=="VR"] <- T # outlier BWA VR point
  kill[data$ihme.loc.id %in% c("ALB") & data$source=="VR" & data$t==2013] <- T # outlier last ALB VR datapoint
  kill[data$ihme.loc.id %in% c("BHR") & data$source=="VR" & data$t==2013] <- T # outlier last BHR VR datapoint
  kill[data$ihme.loc.id %in% c("HRV") & data$source=="VR" & data$t==2014] <- T # outlier last HRV VR datapoint
  kill[data$ihme.loc.id %in% c("TKM") & data$source=="VR" & ((data$t > 1999 & data$t < 2012) | data$t > 2013) ] <- T 
  kill[data$ihme.loc.id=="PSE" & data$source=="VR" & data$t < 2008] <- T # outlier early PSE VR
  kill[data$ihme.loc.id=="MDG" & data$source=="VR" & data$t < 2000 & data$t > 1980] <- T # outlier Madagascar VR in the 1980's and 1990's
  kill[data$ihme.loc.id=="MOZ" & data$source=="VR" & data$t==2001] <- T # outlier one MOZ VR point
  kill[data$ihme.loc.id=="GHA" & data$source=="VR" & data$t %in% c(2000, 2007)] <- T # outlier the GHA VR points
  kill[data$ihme.loc.id=="MLI" & data$source=="VR"  & data$t %in% c(1981, 1984)] <- T # outlier Mali VR points
  kill[data$ihme.loc.id %in% c("IND_43874", "IND_43884", "IND_43893", "IND_43894") & data$source=="VR" & data$t < 1999] <- T # outlier MCCD before 1999
 
   
######################  
  
  outlier <- rep(0,length(data$t))
  outlier[kill] <- 1
  
  data <- cbind(data,outlier)  
  
## Mortality shocks  
  shock <- rep(F,length(data$t))
  
  shock[data$ihme.loc.id=="ARG" & floor(data$t) == 1957  & data$outlier == 0] <- T
  shock[data$ihme.loc.id=="ARM" & floor(data$t) == 1988 & data$outlier == 0 & data$source == "VR"] <- T
  shock[data$ihme.loc.id=="BGD" & floor(data$t) == 1974 & data$outlier == 0] <- T
  shock[data$ihme.loc.id=="BGD" & floor(data$t) == 1971 & data$outlier == 0] <- T
  
  shock[data$ihme.loc.id == "COG" & (floor(data$t) == 1997 | floor(data$t) == 1998 | floor(data$t) == 1999) & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "JOR" & floor(data$t)== 1967 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "KHM" & floor(data$t)>=1975 & floor(data$t)<=1980 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "LBR" & floor(data$t) == 1990 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "LKA" & floor(data$t) == 1996 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "PAK" & floor(data$t) == 2005 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "PRT" & (floor(data$t) == 1975 | floor(data$t) == 1976) & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "RWA" & (floor(data$t) == 1994) & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "TJK" & floor(data$t) == 1993 & data$outlier == 0 & data$source == "VR"] <- T
  
  shock[data$ihme.loc.id == "PNG" & floor(data$t) == 1991] <- T
  shock[data$ihme.loc.id == "SLB" & floor(data$t) == 1999] <- T
  shock[data$ihme.loc.id == "KOR" & data$t >= 1950 & data$t < 1954] <- T
  
  # Mark the Japanese earthquake sin Iwate and Miyagi in 2011 and Hyogo 1995  as shocks 
  shock[data$ihme.loc.id == "JPN_35426" & floor(data$t) == 2011 & data$source == "VR"] <- T
  shock[data$ihme.loc.id == "JPN_35427" & floor(data$t) == 2011 & data$source == "VR"] <- T
  shock[data$ihme.loc.id == "JPN_35451" & floor(data$t) == 1995 & data$source == "VR"] <- T

  # mark shock in MNE for the Kosovo war
  shock[data$ihme.loc.id == "MNE" & floor(data$t) %in% seq(1997,1999)] <- T

    
  isShock <- rep(0,length(data$t))
  isShock[shock] <- 1
  
  data <- cbind(data,isShock)
  names(data)[names(data) == "isShock"] <- "shock"

  # need to include a CHN indirect point that has been outliered (1992.6 1% Intra-Census Survey with a source date of 1995
  # this point is the 2nd outlier out of 6 points, should not be outliered
  # ideally this would not be in the code here, for long term, figure out what's going wrong in the loop that checks all indirects in the kill code
  data$outlier[data$ihme.loc.id == "CHN_44533" & data$source == "1% Intra-Census Survey" & data$source.date == "1995" & data$t == "1992.6"] <- 0
  
  # ----------------------------------------------------------------------------
  # Clean up data, save, and plot
  # ----------------------------------------------------------------------------

  data = data[order(data$country, data$source, data$t),names(data)!="adjust"]
  data = subset(data, !is.na(ihme.loc.id) & !is.na(t) & !is.na(q5))
  
  data$source[data$source == "T's vr"] <- rep("T's vital registration",sum(data$source == "T's vr"))
  
  names(data)[names(data) == "t"] <- "year"
  data$q5 <- data$q5/1000
  
  # only keep IHME standard countries
  locs <- locations[locations$level_all == 1,]
  locs <- locs[,c("ihme_loc_id","location_name")]
  names(locs) <- c("ihme.loc.id","location_name")
  data <- merge(data,locs,by="ihme.loc.id")
  data <- data[,c("ihme.loc.id","location_name","year","q5","source","source.date","in.direct","compiling.entity","data.age","outlier","shock","sd.q5","log10.sd.q5")]
  
  data$in.direct[is.na(data$in.direct)] <- "NA"
  ## shift data at the beginning of a calendar year to the middle of the year unless it is a birth history that we have reanalyzed or results from a report
    ## data that is either not a birth history or is old
  data$year[data$year == floor(data$year) & ((data$in.direct != "direct" & data$in.direct != "indirect") | data$data.age != "new")] <-
      data$year[data$year == floor(data$year) & ((data$in.direct != "direct" & data$in.direct != "indirect") | data$data.age != "new")] + .5
  data <- data[order(data$ihme.loc.id, data$source, data$source.date, data$in.direct, data$year),]
  
  # keep only data after 1950 
  data <- data[data$year >= 1950,]
  

  ### Get rid of any of collaborator's old data #####
  data <- data[data$data.age=="new",]

  names(data)[names(data) == "ihme.loc.id"] <- "ihme_loc_id"
  
  ############################################################################################
  ## Creating old Andhra PrUSER from new Andhra Predesh and Telangana 
  #############################################################################################################
  nrow_data <- nrow(data)
  ind <- copy(data)
  ind <- data.table(ind)
  ind <- ind[ihme_loc_id %in% c("IND_4871", "IND_4841") ]  ## Andhra PrUSER and Telangana
  nrow <- nrow(ind)
  
  ## Case 1:
  ## AP and Telangana present and different in following sources:
        ## IND_DLHS for source.date 2003
        ## IND_DLHS_2007_STATE_23258
        ## IND_HDS_2004_STATE_26919
        ## 157050#IND 2015-2016 DHS Report
        ## HDS 2005, except for source.date < 1990
  
  ## SRS for 2014 and 2015 are included here
  ## In this case, we are going to population weight the AP and Telegana data to create old Andhra PrUSER
  ## And we'll keep the New AP and Telengana data as well
  
  # subsetting to the relevant data points
  sources_case1 <- c("IND_DLHS_2007_STATE_23258", "IND_HDS_2004_STATE_26919", "157050#IND 2015-2016 DHS Report", 
                     "2014 SRS Report", "2015 SRS Report",
                     "IND DHS 2015")
  ind1 <- copy(ind)
  ind1 <- ind1[(source == "IND_DLHS" & source.date ==2003) | (source == "HDS 2005" & source.date >=1990) | source %in% sources_case1]

  ind1_expected_nrow <- nrow(ind1)/2
  
  ind1[,year_id := floor(year)]
  
  # reading in population file
  ind_pop <- fread("FILEPATH/population_gbd2016_M.csv")
  ind_pop <- ind_pop[sex=="both" & age_group_id==1]
  ind_pop <- ind_pop[,.(ihme_loc_id, year_id, pop)]
  ind1 <- merge(ind1, ind_pop, by=c("ihme_loc_id", "year_id"), all.x=T)
  
  ## converting to mx space and pop-weighting
  ind1[,m5 := (-log(1-q5))/5]
  ind1[,m5 := m5 * pop]
  ind1[,source.date:=as.numeric(source.date)]
  
  for(i in c("ihme_loc_id", "year_id", "location_name", "source", "in.direct", "compiling.entity", "data.age", "outlier", "shock")){
    ind1[,i := as.character(i)]
  }
  
  setkey(ind1, year_id, source, in.direct, compiling.entity, data.age, outlier, shock)
  
  ind1 <- ind1[,.(year=mean(year), source.date=mean(source.date), pop=sum(pop), m5=sum(m5)), by=key(ind1)]
  ind1[,m5 := m5/pop]
  
  ## converting back to qx space
  ind1[,q5 := 1- exp(-5*m5)]
  
  ## Creating ihme_loc_ids and location names
  ind1[,location_name := "Old Andhra PrUSER"]
  ind1[,ihme_loc_id := "IND_44849"]
  
  ## Dropping and recreating relevant variables
  ind1[,c("pop", "m5"):=NULL]
  ind1[,sd.q5 := NA]
  ind1[,log10.sd.q5:= NA]
  ind1[,year_id := NULL]
  
  ## checking to make sure the number of rows is expected
  if(nrow(ind1)!= ind1_expected_nrow) stop("your AP and Telangana aggregation resulted in an unexpected number of rows")
  
  ## Appending the pop weighted and aggregated old AP onto the main data set
  ind <- rbind(ind, ind1, use.names=T)
    
  ## Case 2:
  ## Only AP present in following sources:
        ## 5291#IND Census 1981
        ## 60372#IND_CENSUS_2011
        ## DHS 1992
        ## DHS 1998
        ## DHS 2005
        ## IND_Census_2001_STATE_5314
        ## IND_DHS_1992_1993_STATE_19787
        ## IND_DHS_1998_STATE_19950
        ## IND_DHS_2005_STATE_19963
        ## IND_DLHS for source.date 1999
        ## HDS 2005 for source.date < 1990
  ## For these sources, simply change new AP to old AP, execpt for HDS 2005 pre-1990
  ind <- ind[!(source=="HDS 2005" & source.date<1990)]
  sources_case2 <- c("5291#IND Census 1981", "60372#IND_CENSUS_2011", "DHS 1992", "DHS 1998", "DHS 2005", "IND_Census_2001_STATE_5314", "IND_DHS_1992_1993_STATE_19787",
                     "IND_DHS_1998_STATE_19950", "IND_DHS_2005_STATE_19963" )
  ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999), ihme_loc_id:= "IND_44849"]
  ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999), location_name:= "Old Andhra PrUSER"]
  
  ## Case 3:
  ## AP and Telangana present and Telangana the same as AP in:
        ## SRS
  ## For SRS, we drop the Telangana data and use the AP data as old AP
  ind <- ind[!(source=="SRS" & location_name == "Telangana")]

  ind[source=="SRS", ihme_loc_id:= "IND_44849"]
  ind[source=="SRS", location_name:= "Old Andhra PrUSER"]

  ## ADD ALL THE CHECKS, append to "data" data frame
  expected_nrow_ind <- nrow + ind1_expected_nrow - 19 -2
  if(nrow(ind) != expected_nrow_ind) stop("You have an unexpected number of data points at the end of the AP fix")
  if(nrow(ind[ihme_loc_id == "IND_4871"])!=ind1_expected_nrow) stop("you have an unexpected number of telangana data points at the end of AP fix")
  if(nrow(ind[ihme_loc_id == "IND_4841"])!=ind1_expected_nrow) stop("you have an unexpected number of new AP data points at the end of AP fix")
  if(nrow(ind[ihme_loc_id == "IND_44849"])!=(expected_nrow_ind-2*ind1_expected_nrow)) stop("you have an unexpected number of old AP data points at the end of AP fix")
  
  data <- data[!data$ihme_loc_id %in% c("IND_4871", "IND_4841"),]
  data <- rbind(data, ind, use.names=T)
  if(nrow(data)!= nrow_data - nrow + expected_nrow_ind) stop ("after appending to data, somehow you have an unexpected number of rows post AP/Telangana fix")
  
  # also drop IND urban rural SRS
  data <- data[!((data$ihme_loc_id %in% c("IND_43902", "IND_43872", "IND_43908", "IND_43938") & data$source=="SRS") & data$year < 2014 ),]
  
  # set SRS reports to "SRS"
  data[data$source %in% c("2014 SRS Report", "2015 SRS Report")]$compiling.entity <- "new"
  data[data$source %in% c("2014 SRS Report", "2015 SRS Report")]$in.direct <- NA
  data[data$source %in% c("2014 SRS Report", "2015 SRS Report")]$source <- "SRS"
  
  ##############################################################################################################
  
  # create point id's for graphing later (all points not outliered or shock years)
  data$ptid <- rep(0, dim(data)[1])
  data$ptid[data$shock == 0 & data$outlier == 0] <- 1:length(data$ptid[data$shock == 0 & data$outlier == 0])
  
  old <- read.table("FILEPATH/raw.5q0.unadjusted.txt", header=T, stringsAsFactors = F)
  write.table(data, "FILEPATH/raw.5q0.unadjusted.txt", sep = "\t", col.names = TRUE, row.names = FALSE)
  write.table(data, paste("FILEPATH/raw.5q0.unadjusted - ", format(Sys.time(), format = "%m-%d-%y"), ".txt", sep = ""), sep = "\t", col.names = TRUE, row.names = FALSE)
  

 #########
 # Graph #
 #########

  data <- read.table(file="FILEPATH/raw.5q0.unadjusted.txt", sep="\t",header=TRUE)
  pdf(paste("FILEPATH/data_plots_", Sys.Date(), ".pdf", sep=""), onefile=T, width=9, height=6)
  
  count <- 0
  names(data)[1:2] <- c("iso3","country")
  data$location <- paste(data$country, data$iso3, sep = ", ")
  for(location in sort(unique(data$location))) {
    count <- count + 1 
     cat(paste(location, "; ", if(count %% 10 == 0) "\n", sep=""))
     flush.console()
    Graph5q0new(data[data$location == location,], graph.compare=F, justdata=T, xrange=c(1950,2016)) 
  }
  dev.off()  


                               