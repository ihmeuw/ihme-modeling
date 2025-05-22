
# Description: A library for setting reference categories for the national and subnational stage runs

require(data.table)

refcats.helper.displayReferenceSurveys = function(input) {
  data = input

  unique(data[,c("ihme_loc_id", "source1", "reference")])
  unique(data$ihme_loc_id)[!(unique(data$ihme_loc_id) %in% unique(data$ihme_loc_id[data$reference == 1]))]
}

refcats.helper.setReferenceSurveysToOne = function(input) {
  data = copy(input)

  for (ihme_loc_id in unique(data$ihme_loc_id)){
    ref_s <- unique(data$source1[data$reference == 1 & data$ihme_loc_id == ihme_loc_id])
    data$reference[data$ihme_loc_id == ihme_loc_id & data$source1 %in%  ref_s &
                     !(data$ihme_loc_id %like% "BRA" & data$year <= 2010)] <- 1
  }
  if("iso3" %in% names(data)) data$iso3 <- NULL

  return(data)
}

# Returns a modified data table with the set reference categories
refcats.setRefs = function(input, print_debug=F) {
  data = copy(input)

  data$reference[data$ihme_loc_id %in% c("GTM","PRY","BHR","ECU") & grepl("dhs .*direct|cdc-rhs .*direct|cdc rhs .*direct", data$source1) & !grepl("sp", data$source1) & !grepl("indirect", data$type)] <- 1
  data$reference[data$ihme_loc_id == "YEM" & grepl("papfam|dhs .*direct", data$source1) &! grepl("sp", data$source1) &! grepl("indirect", data$type)] <- 1
  data$reference[data$ihme_loc_id == "FSM" & grepl("vr|census_2000", data$source1)] <- 1
  data$reference[data$ihme_loc_id == "ZAF" & grepl("survey|dhs .*direct|rapidmortality", data$source1) &! grepl("sp", data$source1) &! grepl("indirect", data$type)] <- 1
  data$reference[data$ihme_loc_id == "CHN_44533" & (grepl("maternal and child health surveillance system", data$source1)) & data$data == 1] <- 1
  data$reference[data$ihme_loc_id == "GUY" & grepl("dhs .*direct", data$source1) &! grepl("sp", data$source1) &! grepl("indirect", data$type)] <- 1
  data$reference[data$ihme_loc_id == "IRQ" & grepl("mics", data$source)] <- 1
  data$reference[data$ihme_loc_id == "KAZ" & (data$source == "vr1" | grepl("mics",data$source))] <- 1
  data$reference[data$region_name == "CaribbeanI" & data$category == "vr_biased" & !data$to_correct] <- 1
  data$reference[data$ihme_loc_id == "CRI" & grepl("cdc-rhs|census|cdc rhs", data$source1)] <- 1

  # COD reference changed to Standard DHS
  data$reference[data$ihme_loc_id == "COD" & data$source.type == "Standard_DHS_CBH"] <- 1

  #Assign reference groups to China provinces
  mchs.provs <- c("CHN_491", "CHN_492", "CHN_493", "CHN_496", "CHN_497", "CHN_498", "CHN_500", "CHN_501", "CHN_503", "CHN_507", "CHN_508", "CHN_512", "CHN_513", "CHN_514", "CHN_515", "CHN_516", "CHN_499", "CHN_506", "CHN_510", "CHN_511", "CHN_517")
  moh.provs <- c("CHN_504", "CHN_494", "CHN_509", "CHN_505", "CHN_520")
  data$reference[(data$ihme_loc_id %in% moh.provs) & grepl("moh", data$source1, ignore.case = T)] <- 1
  data$reference[(data$ihme_loc_id %in% mchs.provs) & grepl("mchs", data$source1)] <- 1
  data$reference[data$ihme_loc_id == "CHN_521" & grepl("moh|mchs", data$source1)] <- 1
  data$reference[data$ihme_loc_id == "CHN_502" & grepl("mchs", data$source1)] <- 1
  data$reference[data$ihme_loc_id == "CHN_495" & data$source1 == "dsp_0"] <- 1
  data$reference[data$ihme_loc_id == "CHN_518" & grepl("census 2000|moh routine report",data$source1)] <- 1
  data$reference[data$ihme_loc_id == "CHN_519" & grepl("mchs|census 1990",data$source1)] <- 1

  data$reference[substr(data$ihme_loc_id,1,3) == "CHN" & data$source == "china_mchs_2017"] <- 1

  # Assign reference sources to Brazil States
  pnad.states <- c("BRA_4750","BRA_4754","BRA_4757","BRA_4758","BRA_4759","BRA_4760","BRA_4761","BRA_4762","BRA_4763","BRA_4766","BRA_4773","BRA_4774","BRA_4776")
  census.states <- c("BRA_4767","BRA_4770")
  dhs.states <- c("BRA_4752")
  data$reference[data$ihme_loc_id == "BRA_4753" & data$source_year == 2008 & data$source1 == "pnad_subnat 2008 indirect"] <- 1
  data$reference[data$ihme_loc_id == "BRA_4751" & data$source_year == 2005 & data$source1 == "pnad_subnat 2005 indirect"] <- 1
  data$reference[data$ihme_loc_id == "BRA_4771" & data$source_year == 2006 & data$source1 == "pnad_subnat 2006 indirect"] <- 1
  data$reference[data$ihme_loc_id == "BRA_4769" & data$source_year %in% c(2001,2002) & data$source1 %in% c("bra_pnad_1992_2013 2001 indirect, mac only", "bra_pnad_1992_2013 2002 indirect, mac only")] <- 1
  data$reference[(data$ihme_loc_id %in% pnad.states) & grepl("pnad", data$source1)] <- 1
  data$reference[(data$ihme_loc_id %in% census.states) & grepl("census", data$source1)] <- 1
  data$reference[(data$ihme_loc_id %in% dhs.states) & data$source.type == "Standard_DHS_CBH"] <- 1

  # assign reference source to Mozambique DHS CBH average
  data$reference[(data$ihme_loc_id == "MOZ") & data$source.type == "Standard_DHS_CBH"] <- 1

  # south sudan use the census as the reference
  data$reference[(data$ihme_loc_id == "SSD") & data$source == "census, ipums"] <- 1

  # Sudan make the reference MICS SBH
  data$reference[(data$ihme_loc_id == "SDN") & data$source1 == "mics direct"] <- 1

  # make india states reference sources the average of DHS summary birth history except where it's not available. Otherwise use DLHS SBH
  dlhs.states <- c("IND_43902", "IND_43938")
  data$reference[(data$ihme_loc_id %in% dlhs.states) & grepl("dlhs", data$source1) & data$type == "indirect"] <- 1
  data$reference[grepl("IND_", data$ihme_loc_id) & !(data$ihme_loc_id %in% dlhs.states) & grepl("dhs", data$source1) & data$type == "indirect"] <- 1

  # Laos use the MICS CBH as reference
  data$reference[data$ihme_loc_id == "LAO" & grepl("mics", data$source1) & data$type == "direct"] <- 1

  # TLS use the newer DHS as reference
  data$reference[data$ihme_loc_id == "TLS" & data$source1 == "dhs 2009-2010 direct"] <- 1
  data$reference[data$ihme_loc_id == "TLS" & data$source1 == "tls_dhs_2016_2016 direct"] <- 1

  # BWA use the census as reference
  data$reference[(data$ihme_loc_id == "BWA") & data$source.type == "Standard_DHS_CBH"] <- 1

  # Benin use the DHS CBH as reference
  data$reference[(data$ihme_loc_id == "BEN") & data$category == "dhs direct" & data$source != "dhs 2011-2012"] <- 1

  # Gambia use DHS CBH as reference
  data$reference[(data$ihme_loc_id == "GMB") & data$source.type == "Standard_DHS_CBH"] <- 1

  # Kyrgyzstan (KGZ)  use DHS CBH as reference
  data$reference[(data$ihme_loc_id == "KGZ") & data$source.type == "Standard_DHS_CBH"] <- 1

  # MLI use the higher DHS as reference
  data$reference[(data$ihme_loc_id == "MLI") & data$source.type == "Standard_DHS_CBH"] <- 1

  # SLE use the higher DHS CBH as reference
  data$reference[(data$ihme_loc_id == "SLE") & data$source.type == "Standard_DHS_CBH"] <- 1

  # Comoros use DHS 1996 direct as reference because the later survey has a flat trend
  data$reference[data$ihme_loc_id == "COM" & grepl("DHS_CBH", data$source.type)] <- 1

  #GHA with CBH source name updates the DHS SP is no longer marked as reference
  data$reference[(data$ihme_loc_id == "GHA") & data$category == "dhs direct"] <- 1

  # PER use DHS CBH as reference
  data$reference[(data$ihme_loc_id == "PER") & data$source.type == "Standard_DHS_CBH" & !(data$source1 == "dhs direct")] <- 1

  # add DHS and MICS SBH as reference in select KEN subnats
  data$reference[data$ihme_loc_id=="KEN_35617" & data$source1=="ken_dhs_subnat 2009 indirect"] <- 1
  data$reference[data$ihme_loc_id=="KEN_35625" & data$source1=="ken_mics_subnat_2008_2011 2011 indirect"] <- 1
  data$reference[data$ihme_loc_id=="KEN_35640" & data$source1== "155335#ken_mics_2007 2007 indirect"] <- 1
  data$reference[data$ihme_loc_id=="KEN_35641" & data$source1== "ken_dhs_subnat 2003 indirect"] <- 1
  data$reference[data$ihme_loc_id=="KEN_35653" & data$source1== "ken_dhs_subnat 2009 indirect"] <- 1
  data$reference[data$ihme_loc_id=="KEN_35662" & data$source1== "ken_dhs_subnat 2003 indirect"] <- 1
  # West Pokot to use the DHS 2009 (highest) SBH as reference
  data$reference[data$ihme_loc_id=="KEN_35663" & data$source1=="ken_dhs_subnat 2009 indirect"] <- 1

  # Kenya subnational
  dhs.2003 <- c("KEN_35618", "KEN_35626", "KEN_35628", "KEN_35632", "KEN_35633", "KEN_35656", "KEN_35657")
  dhs.2008 <- c("KEN_35619", "KEN_35629", "KEN_35645", "KEN_35658", "KEN_35660")
  dhs.1988 <- c("KEN_35621")
  dhs.1993 <- c("KEN_35631", "KEN_35636")
  dhs.1998 <- c("KEN_35655")
  data$reference[(data$ihme_loc_id %in% dhs.2003) & data$source.type == "Standard_DHS_CBH" & !(data$source1 == "dhs 2003 direct")] <- 1
  data$reference[(data$ihme_loc_id %in% dhs.2008) & data$source.type == "Standard_DHS_CBH" & !(data$source1 == "dhs 2008 direct")] <- 1
  data$reference[(data$ihme_loc_id == dhs.1988) & data$source.type == "Standard_DHS_CBH" & !(data$source1 == "dhs 1988 direct")] <- 1
  data$reference[(data$ihme_loc_id %in% dhs.1993) & data$source.type == "Standard_DHS_CBH" & !(data$source1 == "dhs 1993 direct")] <- 1
  data$reference[(data$ihme_loc_id == dhs.1998) & data$source.type == "Standard_DHS_CBH" & !(data$source == "dhs 1998")] <- 1
  data$reference[(data$ihme_loc_id == "KEN_35622") & data$source.type == "Standard_DHS_CBH" & !(data$source %in% c("dhs 1998", "dhs 2003"))] <- 1
  data$reference[(data$ihme_loc_id == "KEN_35648") & data$source.type == "Standard_DHS_CBH" & !(data$source %in% c("dhs 2003", "dhs 2008"))] <- 1

  # South Africa provinces switch to census 1996 as reference
  census.1996 <- c("ZAF_482", "ZAF_483", "ZAF_484", "ZAF_485", "ZAF_486", "ZAF_487", "ZAF_489", "ZAF_490")
  data$reference[(data$ihme_loc_id %in% census.1996) & grepl("census", data$source) & data$type == "indirect" & data$source_year == 1996] <- 1

  # for ZAF_483, add census HH points as part of reference
  data$reference[data$ihme_loc_id=="ZAF_483" &  data$source.type=="Census_HH"] <- 1

  # for ZAF_484, add census HH points as part of reference
  data$reference[data$ihme_loc_id=="ZAF_484" &  data$source.type=="Census_HH"] <- 1

  # Make Indonesia subnational assigned to the dhs direct category
  data$reference[grepl("IDN_", data$ihme_loc_id) & data$category == "dhs direct"] <- 1

  # MNG – Change reference to CBH only, do not include the SBH in the reference
  data$reference[data$ihme_loc_id == "MNG" & data$method == "CBH"] <- 1

  # TJK - Change reference to DHS
  data$reference[data$ihme_loc_id == "TJK" & data$source.type == "Standard_DHS_CBH"] <- 1

  # Use same reference source for TKM as last year
  data$reference[data$ihme_loc_id == "TKM" & data$source.type == "Standard_DHS_CBH"] <- 1
  data$reference[data$ihme_loc_id == "TKM" & data$source == "20956#tkm 2000 dhs report"] <- 1
  data$reference[data$ihme_loc_id == "TKM" & data$source1 == "tkm_mics_2015_2016 direct"] <- 1

  # UKR
  data$reference[data$ihme_loc_id=="UKR" & data$source.type == "Standard_DHS_CBH"] <- 1
  data$reference[data$ihme_loc_id=="UKR" & data$source == "vr_post1998"] <- 1

  #EGY
  data$reference[data$ihme_loc_id=="EGY" & data$source.type == "Standard_DHS_CBH"] <- 1


  # Manually mark Iran national sources
  iran_reference_sources <- c(
    "iran household survey", "iran household survey 2000",
    "survey by ministry of health and medical education ()",
    "irmidhs iran 2010")
  data$reference[data$ihme_loc_id == "IRN" & data$source %in% iran_reference_sources] <- 1
  data$reference[data$ihme_loc_id == "IRN" & data$vr == 1 & data$year >= 2015] <- 1

  # Manually mark Iran subnational sources
  iran_provincial_reference_types <- c("Standard_DHS_CBH", "Standard_DHS_SBH")
  data$reference[grepl("IRN_", data$ihme_loc_id) & data$source.type %in% iran_provincial_reference_types] <- 1
  data$reference[grepl("IRN_", data$ihme_loc_id) & data$vr == 1 & data$year >= 2015] <- 1


  #if no reference group assigned, use unbiased VR (except for TON and MWI)
  ref.ct <- unique(data$ihme_loc_id[data$reference == 1])
  data$reference[!(data$ihme_loc_id %in% ref.ct) & data$source.type == "vr_unbiased_VR/SRS/DSP" & data$vr == 1 & !(data$ihme_loc_id %in% c("TON", "MWI"))] <- 1
  ref.ct <- unique(data$ihme_loc_id[data$reference == 1])
  data$reference[!(data$ihme_loc_id %in% ref.ct) & data$source.type == "Standard_DHS_CBH"] <- 1

  #For countries w/o reference source or that already have a ref but we want to use the mean
  #, assign ref as mean of all sources since 1980 except biased VR
  ref.ct <- unique(data$ihme_loc_id[data$reference == 1])
  data$reference[data$source.type != "vr_biased_VR/SRS/DSP" & (data$ihme_loc_id %in% c("AFG","CAF","STP","LSO","SWZ","MHL","SLB","PNG") | !(data$ihme_loc_id %in% ref.ct)) & data$data == 1 & data$year>1980] <- 1

  ## Mexico states
  # don't use NNS subnat 1988 as reference
  data$reference[grepl("MEX_", data$ihme_loc_id) & data$source == "105806#mex_nns_subnat_1988"] <- 0
  # don't use ENIGH subnat 2010 as reference in MEX_4668
  data$reference[data$ihme_loc_id == "MEX_4668" & data$source == "93321#mex_enigh_2010_subnat"] <- 0

  ## SAU - use household points for the states in reference
  data$reference[grepl("SAU_", data$ihme_loc_id) & data$source.type %in% c("Census_HH", "Other_HH")] <- 1

  # add SAU VR after 2011 to the reference
  data$reference[grepl("SAU", data$ihme_loc_id) & grepl("vr", data$source, ignore.case = T)] <- 1

  # don't use Standard_DHS_CBH as reference, use MICS CBH instead
  data$reference[data$ihme_loc_id=="SDN" & data$source.type=="Standard_DHS_CBH"] <- 0
  data$reference[data$ihme_loc_id=="SDN" & data$source.type=="MICS_CBH"] <- 1

  # South Africa subnationals
  # Eastern Cape
  data$reference[data$ihme_loc_id=="ZAF_482"] <- 0
  data$reference[data$ihme_loc_id=="ZAF_482" & data$source=="43146#zaf_ipums_census_1996"] <- 1

  # Limpopo - get rid of everything but 1996 census as reference
  data$reference[data$ihme_loc_id=="ZAF_486"] <- 0
  data$reference[data$ihme_loc_id=="ZAF_486" & data$source=="43146#zaf_ipums_census_1996"] <- 1


  # Mpumalanga - get rid of everything but 1996 census as reference
  data$reference[data$ihme_loc_id=="ZAF_487"] <- 0
  data$reference[data$ihme_loc_id=="ZAF_487" & data$source=="43146#zaf_ipums_census_1996"] <- 1

  # North-West - get rid of everything but 1996 census as reference
  data$reference[data$ihme_loc_id=="ZAF_488"] <- 0
  data$reference[data$ihme_loc_id=="ZAF_488" & data$source=="43146#zaf_ipums_census_1996"] <- 1
  data$reference[data$ihme_loc_id=="ZAF_488" & data$source.type=="Census_HH"] <- 1


  data$reference[data$ihme_loc_id=="KEN_35661" & data$category=="dhs direct" & data$source=="DHS 2003"] <- 0
  data$reference[data$source1=="efls 2012 indirect"] <- 0

  data$reference[data$ihme_loc_id=="MDG"] <- 0
  data$reference[data$ihme_loc_id=="MDG" & data$source1 %in% c("dhs 1992 direct", "dhs 1997 direct", "dhs 2003-2004 direct", "dhs 2008-2009 direct", "dhs direct")] <- 1

  data$reference[data$ihme_loc_id=="IND_43931" & data$source1 %in% c("ind_dhs_1992_1993_urban_rural_19787 1992 indirect", "19787#ind_dhs_1992-1993_subnat 1993 indirect")] <- 0

  # make sure PAN does not have VR as reference
  data$reference[data$ihme_loc_id=="PAN" & data$source=="vr"] <- 0

  # make sure COL does not have VR as reference
  data$reference[data$ihme_loc_id=="PAN" & data$source=="vr"] <- 0

  # only make new Korean VR as reference
  data$reference[data$ihme_loc_id=="KOR" & data$source=="vr"] <- 0

  # add SRS to Bihar, Urban reference
  data$reference[data$ihme_loc_id=="IND_43875" & data$source=="srs"] <- 1

  # make the DHS2015 for India part of the reference
  data$reference[grepl("IND", data$ihme_loc_id) & data$source %in% c("ind dhs 2015")] <- 1

  # Make SRS for India part of the reference
  data$reference[grepl("IND", data$ihme_loc_id) & data$source %in% c("srs")] <- 1

  # # For TKM, add MICS_CBH as a reference source
  data$reference[data$ihme_loc_id=="TKM" & data$source.type=="MICS_CBH"] <- 1

  # LBY – use VR as another reference category
  data$reference[data$ihme_loc_id=="LBY" & data$vr == 1] <- 1

  # Make CBH DHS reference where we are splitting VR in addition to unbiased VR
  data$reference[data$ihme_loc_id=="ARM" & data$source.type == "Standard_DHS_CBH"] <- 1
  data$reference[data$ihme_loc_id=="PHL" & data$source.type == "Standard_DHS_CBH"] <- 1
  data$reference[data$ihme_loc_id=="JOR" & data$source.type == "Standard_DHS_CBH"] <- 1

  # Make KAZ post 2010 VR part of reference category
  data$reference[data$ihme_loc_id=="KAZ" & data$source == "vr_post2010"] <- 1

  # Make KGZ unbiased VR part of reference category
  data$reference[data$ihme_loc_id=="KGZ" & data$category == "vr_unbiased"] <- 1

  # Make ECU unbiased VR part of reference category
  data$reference[data$ihme_loc_id=="ECU" & data$category == "vr_unbiased"] <- 1

  # Make GEO unbiased VR part of reference category
  data$reference[data$ihme_loc_id=="GEO" & data$source == "vr_post2003"] <- 1

  # Add PAPCHILD and PAPFAM to
  data$reference[data$ihme_loc_id=="DZA" & (grepl("papfam", data$source) | grepl("papchild", data$source)) & data$method == "CBH"] <- 1

  # add mics 2016 to reference
  data$reference[data$ihme_loc_id=="PRY" & data$source == "pry_mics_2016_2016" & data$method == "CBH"] <- 1

  # Make sure JAM census is not reference
  data$reference[data$ihme_loc_id=="JAM" & data$category == "census"] <- 0

  # Adjusting WFS  down, mark WFS 1977 as reference so it doesn't get adjusted
  data$reference[data$ihme_loc_id=="LSO" & data$source == "wfs 1977" & data$method == "CBH"] <- 1

  # Add census SBH
  data$reference[data$ihme_loc_id=="PNG" & data$source.type == "Census_SBH" & data$year < 1970] <- 1

  #  use phl_census_2010 SBH and phl_dhs_2011 SBH as reference
  data$reference[grepl("PHL_", data$ihme_loc_id)] <- 0
  data$reference[grepl("PHL_", data$ihme_loc_id) & data$source == "phl_census_2010" & data$method == "SBH"] <- 1
  data$reference[grepl("PHL_", data$ihme_loc_id) & data$source == "phl_dhs_2011" & data$method == "SBH"] <- 1

  data$reference[data$ihme_loc_id%in%c("IRN_44885", "IRN_44869", "IRN_44883", "IRN_44876") & data$source == "vr"] <- 0

  # use dhs report as reference
  data$reference[data$ihme_loc_id=="ZMB" & data$source == "zmb_dhs_2018_report"] <- 1
  data$reference[data$ihme_loc_id=="NGA" & data$source == "nga_dhs_2018_report"] <- 1

  # use census sbh as reference
  data$reference[data$ihme_loc_id%in%c("BHR", "ARE", "SYR", "TON","SLB") & data$source.type == "Census_SBH"] <- 1
  data$reference[data$ihme_loc_id == "VUT" & data$year <=1965 & data$source.type == "Census_SBH"] <- 1
  data$reference[data$ihme_loc_id == "THA" & data$source.type == "Standard_DHS_CBH"] <- 1

  data$reference[data$ihme_loc_id%in%c("IND_4850","IND_4842","IND_4854","IND_4855","IND_4867","IND_4872","IND_4874") & data$source.type == "Standard_DHS_CBH"] <- 1
  data$reference[data$ihme_loc_id == "BWA" & data$source.type == "Census_SBH"] <- 1
  data$reference[data$ihme_loc_id == "IND_4854" & data$source.type =="Standard_DHS_CBH"] <-0

  # Reference updates.
  data[ihme_loc_id == "KOR" & source.type=="WFS_CBH", reference := 1]
  data[ihme_loc_id == "IND_4864" & source.type=="Census_SBH" & source_year == 2001, reference := 1]
  data[ihme_loc_id == "WSM" & source.type=="Census_SBH" & source_year < 1980, reference := 1]
  data[ihme_loc_id == "CHN_492" & source.type == "Census_SBH", reference := 1]
  data[ihme_loc_id == "IDN_4723" & source.type == "Census_SBH", reference := 1]
  data[ihme_loc_id %like% "CHN" & source.type == "Census_SBH" & source_year == 1982, reference := 1]
  data[ihme_loc_id == "CPY" & source.type == "Census_SBH" & year < 1970, reference := 1]
  data[ihme_loc_id == "WSM" & source_year==2016 & source=="vr", `:=`(reference=1, category='vr_unbiased',
                                                                     source.type = "vr_unbiased_VR/SRS/DSP",
                                                                     graphing.source = "vr_unbiased",
                                                                     source1 = "vr_unbiased")]
  data[ihme_loc_id == "IND_43880" & year > 2009 & source != "srs", reference := 0]
  data[(ihme_loc_id =="LSO" | ihme_loc_id == "BGD") & source.type == "WFS_SBH", reference := 1]

  # If no reference sources have been selected, and biased vr is present, use biased vr as reference
  is_DT <- is.data.table(data)
  if(!is_DT) data <- as.data.table(data)

  test <- data[data == 1]
  test[, nrefs := sum(reference), by = "ihme_loc_id"]
  locs_missing_reference <- unique(test[nrefs == 0]$ihme_loc_id)
  if(length(locs_missing_reference) > 0){
    warning(paste("these locs missing reference, and using biased vr as reference: ",
                  paste(locs_missing_reference, collapse=' ')))
    data[data == 1 & ihme_loc_id %in% locs_missing_reference & category == "vr_biased", reference := 1]
  }

  # set vr_no_overlap to vr_biased, but mark it so that it isn't run through the vr bias adjustment
  data$vr_no_overlap <- 0
  data[category %in% c("vr_no_overlap"), vr_no_overlap := 1]
  data[category %in% c("vr_no_overlap"), corr_code_bias := T]
  data[category %in% c("vr_no_overlap"), to_correct := T]
  data[category %in% c("vr_no_overlap"), source.type := "vr_biased_VR/SRS/DSP"]
  data[category %in% c("vr_no_overlap"), graphing.source := "vr_biased"]
  data[category %in% c("vr_no_overlap"), category := "vr_biased"]


  # All unbiased VR should be part of the reference
  data[category == "vr_unbiased", reference := 1]

  data[grepl("BRA_", ihme_loc_id) & grepl("dhs", source, ignore.case = T), reference := 0]
  data[grepl("BRA_", ihme_loc_id) & grepl("pnad", source, ignore.case = T), reference := 1]

  bras <- c("BRA", "BRA_4752", "BRA_4770", "BRA_4771")
  data[ihme_loc_id %in% bras, reference := 0]
  data[ihme_loc_id %in% bras & grepl("pnad", source, ignore.case = T), reference := 1]

  data[ihme_loc_id == "BRA_4758", reference := 0]

  data[grepl("BRA", ihme_loc_id) & grepl("BRA_HOUSEHOLD_SAMPLE_SUR_2015", source,
                                         ignore.case = T), reference := 1]

  data[ihme_loc_id %in% c("BRA_4765", "BRA_4773") & grepl("vr", source, ignore.case = T),
       reference := 1]

  # Add adjusted VR to reference
  data[grepl("BRA", ihme_loc_id) & source == "bra_sim",
       reference := 1]

  # Additional custom referencing
  data[ihme_loc_id == "QAT", reference := 1]
  data[ihme_loc_id == "NGA_25341" & source == "nga_dhs_1990_1990.0", reference := 1]

  # Add MEX_4657 VR back to reference
  data[ihme_loc_id == "MEX_4657" & source == "vr", reference := 1]
  data[ihme_loc_id %in% c("MEX_4643", "MEX_4646", "MEX_4647", "MEX_4649",
                          "MEX_4650", "MEX_4652", "MEX_4655", "MEX_4656",
                          "MEX_4659", "MEX_4660", "MEX_4661", "MEX_4663",
                          "MEX_4664", "MEX_4665", "MEX_4666", "MEX_4667",
                          "MEX_4668", "MEX_4669", "MEX_4670", "MEX_4671",
                          "MEX_4672", "MEX_4673", "MEX_4674") &
         year < 2013 & grepl("vr", source, ignore.case = TRUE), reference := 0]

  data[ihme_loc_id == "PHL_53611", reference := 0]
  data[ihme_loc_id == "PHL_53611" & grepl("dhs", source, ignore.case = TRUE)
       & method == "CBH", reference := 1]
  data[ihme_loc_id == "PHL" & grepl("vr", source, ignore.case = TRUE),
       reference := 0]

  data[grepl("BRA", ihme_loc_id) & grepl("vr", source, ignore.case = TRUE) &
         year >= 2019, reference := 1]

  data[ihme_loc_id == "IND_43895" & grepl("census", source, ignore.case = TRUE),
       reference := 1]
  data[ihme_loc_id == "IND_43936" &
         (grepl("census", source, ignore.case = TRUE) | method == "SBH" | method == "CBH") &
         year %in% 1980.5:2000.5 &
         (mort > 0.049), reference := 1]

  data[ihme_loc_id == "PAK_53619" & grepl("mics", source, ignore.case = TRUE) &
         year %in% 2010.5:2019.5,
       reference := 1]

  data[ihme_loc_id %like% "ZAF" & grepl("census", source, ignore.case = TRUE) & year >= 2000 & year <= 2011,
       reference := 0]
  data[ihme_loc_id == "LSO" & year == 2016.5 & grepl("mics", source, ignore.case = TRUE),
       reference := 0]
  data[grepl("PHL_", ihme_loc_id) & grepl("vr", source, ignore.case = TRUE),
       reference := 0]
  data["LBN" == ihme_loc_id & grepl("vr", source, ignore.case = TRUE),
       reference := 0]

  data["UZB" == ihme_loc_id & grepl("mics", source, ignore.case = TRUE) & year >= 2008, reference := 1]

  data[ihme_loc_id == "MEX_4654" & grepl("enadid", source), reference := 0]

  # make latest mics reference
  data[, n_ref := cumsum(reference), by = "ihme_loc_id"]
  data[, max_ref := max(n_ref), by = "ihme_loc_id"]
  data[source.type == "MICS_CBH", n_mics := .N, by = c("ihme_loc_id", "year")]
  data[
    (ihme_loc_id %in% c("BEN", "SSD", "TCD", "YEM", "WSM", "VNM", "NGA")) &
      n_ref == max_ref & n_mics == 1,
    reference := 1
  ]
  data[, c("n_ref", "max_ref", "n_mics") := NULL]

  if(!is_DT) data <- as.data.frame(data)

  if (print_debug) {
    refcats.helper.print_reference_surveys(data)
  }

  return(data.frame(refcats.helper.setReferenceSurveysToOne(data)))
}
