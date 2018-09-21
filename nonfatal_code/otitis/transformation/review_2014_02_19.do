// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:	This code template is used for making changes to actual values (mean/upper/lower/SE/ESS) for all lit/non-lit data: age/sex-splitting, ESS-splitting, other transformations
//			This assumes that all raw data has been prepped, checked, and corrected as needed through 01_lit/02_nonlit; this should not be used to make corrections to raw data
//			It's imperative that we preserve the raw data prior to transformation; raw is defined as any lit extractions and non-lit data imported to the database
//			The review process is designed to be run iteratively, as modelers will often want to try multiple transformations on the same data to get the results they desire
//			Each run will download the raw data without the transformations of the previous review upload, so that new transformations will be applied to the same raw data
//			Here is an overview of the steps involved:
//				1. Save this code template renamed with timestamp (do not just use YYYY_MM_DD file because we will push updates to the template), and fill in "functional" and "date" globals
//				2. Download raw data for transformation, which includes only raw data prepped through 01_lit/02_nonlit, and excludes all previously transformed data
//				3. Run transformations on data by duplicating raw rows, marking new rows as is_raw=adjusted, and raw rows to be excluded as is_raw=excluded_review
//				4. Check that data meet template specifications, and save output sheet in 05_upload for upload by data analyst
//				5. The next iteration of the review process will download and restore all the raw data that went into the previous iteration, and drop the adjusted data from that iteration
// Description:	 Excluding data from Aboriginal population, adding citation if citation is missing, correcting data entry errors, dropping duplicates, age/sex-splitting, ESS-splitting, converting period to point prevalence, calculating annual incidence based on cumulative incidence 

// set globals (required)
	// define the functional group for which you are prepping data (acause or impairment)
		global functional = "otitis"
	// define the date of the run (YYYY_MM_DD)
		global date = "DATE"
		
// prep stata (shouldn't need to modify except to increase memory)
	global process "03_review"
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
	}
	run "FILEPATH"   
	run "FILEPATH"  
	local code_dir "ADDRESS"
	local out_dir "ADDRESS"
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	cap log close
	log using "FILEPATH", replace

// download and load lit data (this will grab the latest changes made to the database including marking outliers)
	/*do "FILEPATH" */
	insheet using "FILEPATH", comma clear

// Exclude studies among Aboriginal population
replace data_status="excluded" if nid==103401
replace issues="Aboriginal population" if nid==103401

replace data_status="excluded" if nid==103403
replace issues="Aboriginal population" if nid==103403

replace data_status="excluded" if row_id==12 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==12 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==13 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==13 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==14 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==14 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==15 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==15 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==19 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==19 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==20 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==20 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==85 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==85 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==86 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==86 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==87 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==87 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==88 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==88 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==89 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==89 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==90 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==90 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==91 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==91 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if row_id==92 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="Aboriginal population" if row_id==92 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

// Exclude data points that were mistakenly extracted
replace data_status="excluded" if row_id==30 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==31 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace issues="non-representative sample" if row_id==30 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==31 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace data_status="excluded" if nid==103422 
replace issues="parameter was estimated using poisson regression" if nid==103422 

//exclude duplicates, replace data_status="" for data points that are no longer duplicates
replace data_status="excluded" if nid==116112 & case_name=="otitis media aggregate"
replace data_status="excluded" if nid==116112 & case_name=="OM with effusion" 
replace data_status="" if nid==116112 & case_name=="chronic suppurative OM"
replace data_status="" if nid==116112 & case_name=="accute OM"

replace data_status="excluded" if nid==116104 & case_name=="otitis media aggregate"
replace data_status="" if nid==116104 & case_name=="chronic suppurative OM" 
replace data_status="" if nid==116104 & case_name=="accute OM" 
replace data_status="" if nid==116104 & case_name=="chronic OM" 

replace data_status="" if nid==116103
replace data_status="excluded" if case_name=="OM with effusion"
replace data_status="excluded" if case_name=="otitis media aggregate"

// Exclude old MEPS data
replace data_status="excluded" if nid==97455
replace issues="old GBD 2010 MEPS data" if nid==97455

// Adding citations if citation is missing in GBD 2010 data
replace citation="Kim CS et al. Prevalence of otitis media and allied diseases in Korea. Journal of Korean Medical Science. 1993;8:34-40" if row_id==8 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==111 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace nid=103414 if citation=="Kim CS et al. Prevalence of otitis media and allied diseases in Korea. Journal of Korean Medical Science. 1993;8:34-40"
replace nid=103414 if row_id==71 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==72 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==73 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==74 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==75 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Biswas AC et al. Prevalence of CSOM among rural school going children. Mymensingh Med J. 2005;14:152-155" if row_id==9 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Kamal N et al. Prevalence of chronic suppurative otitis media among the children living in two selected slums of Dhaka City. BanglUSER Med. Res. Counc. Bull. 2004;30:95-104" if row_id==10 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Godinho RN et al. Precalence and impact of chronic otitis media in school age children in Brazil. First epidemiologic study concerning chronic otitis media in Latin America. International Journal of Pediatric Otorhinolaryngology. 2001;61:233-23" if row_id==11 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Kim CS et al. Prevalence of otitis media and allied diseases in Korea. Journal of Korean Medical Science. 1993;8:34-40" if row_id==16 & grouping=="chronic" | row_id==17 & grouping=="chronic" | row_id==18 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace site="" if row_id==16 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==17 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==18 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Hechavarria IF et al. Pesquisaje de otitis media cronica secretoria. Rev Cubana Cir 1989;28:287-293" if row_id==21 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Hechavarria IF et al. Pesquisaje de otitis media cronica secretoria. Rev Cubana Cir 1989;28:287-293" if row_id==22 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Hechavarria IF et al. Pesquisaje de otitis media cronica secretoria. Rev Cubana Cir 1989;28:287-293" if row_id==23 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Hechavarria IF et al. Pesquisaje de otitis media cronica secretoria. Rev Cubana Cir 1989;28:287-293" if row_id==24 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Hechavarria IF et al. Pesquisaje de otitis media cronica secretoria. Rev Cubana Cir 1989;28:287-293" if row_id==25 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Hechavarria IF et al. Pesquisaje de otitis media cronica secretoria. Rev Cubana Cir 1989;28:287-293" if row_id==26 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Alho OP, L‰‰r‰ E, Oja H. What is the natural history of recurrent acute otitis media in infancy? J Fam Pract. 1996 Sep;43(3):258ñ64" if row_id==27 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Sorri M, Jounio-Ervasti K. Otitis media in a cross-sectional population from northern Finland. Arctic Med Res. 1988;47 Suppl 1:653ñ6" if row_id==28 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==29 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace nid=103411 if row_id==28 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==29 & grouping=="chronic" & description=="GBD 2010 All uncertfix"


replace citation="Vartiainen E and J Vartiainen. Age and hearing function in patients with chronic otitis media. J of Otolaryngology 1995;24:336-9" if row_id==30 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Vartiainen E and J Vartiainen. Age and hearing function in patients with chronic otitis media. J of Otolaryngology 1995;24:336-9" if row_id==31 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Virolainen E et al. Prevalence of secretory otitis media in seven to eight year old school children. Annals of otology, rhinology & laryngology 1980;89 suppl:7-10" if row_id==32 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Berzon DB. Ear disease in a group general practice. A review of world communitites. Journal of Laryngology and Otology. 1983;97:817-824" if row_id==33 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Berzon DB. Ear disease in a group general practice. A review of world communitites. Journal of Laryngology and Otology. 1983;97:817-824" if row_id==34 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Berzon DB. Ear disease in a group general practice. A review of world communitites. Journal of Laryngology and Otology. 1983;97:817-824" if row_id==35 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Berzon DB. Ear disease in a group general practice. A review of world communitites. Journal of Laryngology and Otology. 1983;97:817-824" if row_id==36 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Berzon DB. Ear disease in a group general practice. A review of world communitites. Journal of Laryngology and Otology. 1983;97:817-824" if row_id==37 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Berzon DB. Ear disease in a group general practice. A review of world communitites. Journal of Laryngology and Otology. 1983;97:817-824" if row_id==38 & grouping=="chronic" & description=="GBD 2010 All uncertfix"


replace citation="Browning GG & S. Gatehouse. The prevalence of middle ear disease in the adult British population. Clin. Otolayngol. 1992; 17:317-321" if row_id==39 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Browning GG & S. Gatehouse. The prevalence of middle ear disease in the adult British population. Clin. Otolayngol. 1992; 17:317-321" if row_id==40 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Browning GG & S. Gatehouse. The prevalence of middle ear disease in the adult British population. Clin. Otolayngol. 1992; 17:317-321" if row_id==41 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Browning GG & S. Gatehouse. The prevalence of middle ear disease in the adult British population. Clin. Otolayngol. 1992; 17:317-321" if row_id==42 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Browning GG & S. Gatehouse. The prevalence of middle ear disease in the adult British population. Clin. Otolayngol. 1992; 17:317-321" if row_id==43 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Browning GG & S. Gatehouse. The prevalence of middle ear disease in the adult British population. Clin. Otolayngol. 1992; 17:317-321" if row_id==44 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Browning GG & S. Gatehouse. The prevalence of middle ear disease in the adult British population. Clin. Otolayngol. 1992; 17:317-321" if row_id==45 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="Browning GG & S. Gatehouse. The prevalence of middle ear disease in the adult British population. Clin. Otolayngol. 1992; 17:317-321" if row_id==46 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="McPherson B and CA Holborow. A study of deafness in West Africa: the Gambian hearing health project. Int J of Pediatric Otorhinolaryngology. 1985;10:115-135" if row_id==47 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
replace citation="McPherson B and CA Holborow. A study of deafness in West Africa: the Gambian hearing health project. Int J of Pediatric Otorhinolaryngology. 1985;10:115-135" if row_id==48 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Bandyopadhyay R et al. A comparative study of common ear morbidity pattern among the primary school children of an urban slum of Kolkata and rural area of Hooghly. Journal of the Indian Medical Association. 2005;103:428-432" if row_id==49 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==50 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==51 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==52 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==53 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==54 & grouping=="chronic" | row_id==55 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==56 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Jacob A et al. Hearing impairment and otitis media in a rural primary schook in South India. Int. J. Pediatr. Otorhinolaryngol. 1997;39:133-138" if row_id==57 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==58 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Cohen et al. The prevalence of middle ear pathologies in Jerusalem school children. The American journal of otology 1989;10:456-9" if row_id==59 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Podoshin L et al. Cholesteatoma: an epidemiologic study among members of Kibbutzim in Northern Israel. Ann Otol Rhinol Latyngol 1986;95:365-368" if row_id==60 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==61 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==62 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==63 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==64 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==65 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==66 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==67 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==68 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Hatcher J et al. A prevalence study of ear problems in school children in Kiambu district, Kenya, May 1992. Int" if row_id==69 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==70 & grouping=="chronic" & description=="GBD 2010 All uncertfix" 

replace citation="Hatcher J et al. A prevalence study of ear problems in school children in Kiambu district, Kenya, May 1992. Int" if row_id==69 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==70 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Kim CS et al. Prevalence of otitis media and allied diseases in Korea. Journal of Korean Medical Science. 1993;8:34-40" if row_id==71 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==72 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==73 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==74 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==75 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Ologe FE and CC Nwawolo. Chronic suppurative otitis media in school pupils in Nigeria. East African Medical Journal. 2003;80:130-4" if row_id==76 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==77 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==78 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==79 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Ologe FE and CC Nwawolo. Prevalence of chronic suppurative otitis media (CSOM) among school children in a rural communty in Nigeria. Nigerian Postgraduate Medical Journal 2002;9:63-6" if row_id==80 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==81 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==82 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==83 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==84 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Abolfotouh MA et al. Hearing loss and other ear problems among schoolboys in Abha, Saudi Arabia. Annals of Saudi Medicine. 1995;15:323-6" if row_id==93 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Muhaimeid H et al. Epidemiology of chronic suppurative otitis media in Saudi children. Int J of Pediatric Otorhinolarygology 1993;26:101-108" if row_id==94 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==95 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==96 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==97 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Eason RJ et al. Chronic suppurative otitis media in the Solomon Islands: a prospective, microbiological, audiometric and therapeutic survey. New Zealand Medical Journal. 1986;99:812-5" if row_id==98 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==99 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==100 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==101 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Chayarpham S et al. A study of the prevalence of and risk factor for ear diseases and hearing loss in primary school children in Hat Yai, Thailand. Journal of the Medical Association of Thailand. 1996;79:468-72" if row_id==102 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==103 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==104 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==105 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==106 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Giles M and Asher I. Prevalence and natural history of otitis media with perforation in Maori school children. Journal of Laryngology and Otology. 1991;105:257-60" if row_id==85 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==86 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==87 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==88 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Giles M and P O'Brien. The prevalence of hearing impairment amongst Maori schoolchildren. Clin Otolaryngol. 1991;16:174-178" if row_id==89 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==90 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==91 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==92 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Todd NW and CA Bowman. Otitis media at Canyon Day, Ariz. Archives of otolaryngology. 1985;111:606-8" if row_id==148 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==149 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==150 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==151 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==152 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Balle VH et al. Prevalence of chronic otitis media in a randomly selected population from two communes in southern Vietnam. Acta Otolaryngol 2000; Suppl 543:51-53" if row_id==153 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Prescott CA and MA Kibel. Ear and hearing disorders in rural grade 2 (Sub B) schoolchildren in the western Cape. SAMJ 1991;79:90-3" if row_id==154 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Monasta L et al. Burden of disease caused by otitis media: systematic review and global estimates. PLoS One. 2012;7(4):e36226" if parameter_type=="Duration" 

replace citation="Julien G et al. Chronis otitis media and hearing deficit among native children of Kuujjuaraapik (Northern Quebec): A pilot project. Canadian Journal of Public Health. 1987;78:57-61" if row_id==12 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==13 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==14 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==15 & grouping=="chronic" & description=="GBD 2010 All uncertfix" 
 
replace citation="S Bruneau et al. Longitudinal observatoins (1987-1997) on the prevalence of middle ear disease and associated risk factors among inuit children of Inukjuak, Nunavik, Quebec, Canada. Infections and zoonotic diseases. 2001;60:632-9" if row_id==19 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==20 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Bastos et al. Middle ear disease and hearing impairment in northern Tanzania. A prevalence study of schoolchildren in the Moshi and Monduli districts. Int. J. Pediatr. 1995;32:1-12" if row_id==107 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==108 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

replace citation="Minja BM and A Machemba. Prevalence of otitis media, hearing impairment and cerumen impaction among school children in rural and urban Dar es Salaam, Tanzania" if row_id==109 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==110 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

//Transformations/Corrections

// nid=103410 - replace numerator, denominator, sample_size, mean & standard_error with correct/updated values.
	// mark data_status=exclude & is_raw="excluded_review"
	replace data_status="excluded" if nid==103410
	replace is_raw="excluded_review" if nid==103410
	
	// create new row with flag new=1 for girls 
	expand 2 in 112 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// Based on the information provided in table III, pg.140 of nid=103410, incidence rate per person-month and incidence rate per person-year were calculated. Because incidence/person year was 1.28, point prevalence and corresponding numerator and denominator were calculated based on incidence and average duration of AOM (assumed to be 3 wks). 
		// calculate point prevalence (mean) for new row 
		replace numerator = 61 if new == 1 
		replace denominator = 824 if new == 1
		replace sample_size= 824 if new==1
		replace mean = numerator/denominator if new==1
		replace data_status=" " if new==1
	// replace is_raw=adjusted for new row
	replace is_raw = "adjusted" if new == 1
	// estimate standard errors and CIs from sample size
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
    replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
    replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
    
	replace age_start=0 if new==1
	replace data_status=" " if new==1
	replace is_raw="adjusted" if new==1
	drop new
	
	// create new row with flag new=1 for boys 
	expand 2 in 113 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// Based on the information provided in table III, pg.140 of nid=103410, incidence rate per person-month and incidence rate per person-year were calculated. Because incidence/person year was 1.28, point prevalence and corresponding numerator and denominator were calculated based on incidence and average duration of AOM (assumed to be 3 wks). 
		// calculate point prevalence (mean) for new row 
		replace numerator = 70 if new == 1 
		replace denominator = 818 if new == 1
		replace sample_size= 818 if new==1 
		replace mean = numerator/denominator if new==1
		replace data_status=" " if new==1
	// replace is_raw=adjusted for new row
	replace is_raw = "adjusted" if new == 1
	// estimate standard errors and CIs from sample size
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
    replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
    replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
    
	replace age_start=0 if new==1
	replace data_status=" " if new==1
	replace is_raw="adjusted" if new==1
	drop new
		
// nid=103411 - convert cumulative incidence to annual incidence 
	// mark data_status=exclude & is_raw="excluded_review"
	replace data_status="excluded" if nid==103411 & grouping=="acute"
	replace is_raw="excluded_review" if nid==103411 & grouping=="acute"
	// create new row with flag new=1 
	expand 2 in 134 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// replace sample_size with correct values as reported in the original study
	replace sample_size=denominator if nid==103411 & grouping=="acute"
    // Replace 'mean' with the annual incidence rate value (28.9017%) which was calculated from the 2-year cumulative incidence (43.9%) by applying the following formula: [-ln(1 - S)]/t; where S is the proportion of new cases over t years and t equals the time of follow-up 
	replace mean=0.289017 if new==1
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
    replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
    replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
    
	replace parameter_type="incidence" if new==1
	replace data_status=" " if new==1
	replace is_raw="adjusted" if new==1
	drop new
	
// nid=103459 - convert cumulative incidence to annual incidence 
	// mark data_status=excluded & is_raw="excluded_review"
	replace data_status="excluded" if nid==103459 & grouping=="acute"
	replace is_raw="excluded_review" if nid==103459 & grouping=="acute"
	// create new row with flag new=1 
	expand 2 in 265 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// Replace 'mean' with the annual incidence rate value (38.83%) which was calculated from the 7-year cumulative incidence (93.4%) by applying the following formula: [-ln(1 - S)]/t; where S is the proportion of new cases over t years and t equals the time of follow-up 
	replace mean=0.3883 if new==1
	replace sample_size=498
	replace denominator=498
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
    replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
    replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
    
	replace parameter_type="incidence" if new==1
	replace data_status=" " if new==1
	replace is_raw="adjusted" if new==1
	replace year_start=1975 if new==1
	replace year_end=1984 if new==1
	replace age_start=0 if new==1
	replace age_end=7 if new==1
	drop new
	
// nid=103460 - convert cumulative incidence to annual incidence 
	// mark data_status=excluded & is_raw="excluded_review"
	replace data_status="excluded" if nid==103460 & grouping=="acute"
	replace is_raw="excluded_review" if nid==103460 & grouping=="acute"
	// create new row with flag new=1 
	expand 2 in 277 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// Replace 'mean' with the annual incidence rate value (41.26%) which was calculated from the 3-year cumulative incidence (71%) by applying the following formula: [-ln(1 - S)]/t; where S is the proportion of new cases over t years and t equals the time of follow-up 
	replace mean=0.412624785 if new==1
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
    replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
    replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
    replace parameter_type="incidence" if new==1
	replace data_status=" " if new==1
	replace is_raw="adjusted" if new==1
	replace age_start=0 if new==1
	replace age_end=3 if new==1
	drop new
	
// nid=103406 - Exclude data points that were mistakenly extracted (cumulative incidence is not useful, keep only the annual incidence rate)
	replace data_status="excluded" if nid==103406
	replace data_status=" " if nid==103406 & mean==0.93
	
// nid=103418 - cumulative incidence and period prevalence data (i.e., % children with at least one episode of AOM in 1980, Table 1, page 20) were extracted, drop the former and covert the latter into point prevaence
	// exclude cumulative incidence data
	replace data_status="excluded" if nid==103418 & year_start==1977
	replace issues="cumulative incidence" if nid==103418 & year_start==1977
	// Convert period to point prevalence for each of the 16 data points
		// mark data_status="excluded" & is_raw="excluded_review" for the 16 data points
			replace data_status="excluded" if nid==103418 & grouping=="acute" & year_start==1980
			replace is_raw="excluded_review" if nid==103418 & grouping=="acute" & year_start==1980
				// data point #1
					// create new row with flag new=1 
						expand 2 in 154 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #2
					// create new row with flag new=1 
						expand 2 in 155 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #3
					// create new row with flag new=1 
						expand 2 in 156 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #4
					// create new row with flag new=1 
						expand 2 in 157 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #5
					// create new row with flag new=1 
						expand 2 in 158 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #6
					// create new row with flag new=1 
						expand 2 in 159 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #7
					// create new row with flag new=1 
						expand 2 in 160 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #8
					// create new row with flag new=1 
						expand 2 in 161 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #9
					// create new row with flag new=1 
						expand 2 in 162 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #10
					// create new row with flag new=1 
						expand 2 in 163 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #11
					// create new row with flag new=1 
						expand 2 in 164 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #12
					// create new row with flag new=1 
						expand 2 in 165 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #13
					// create new row with flag new=1 
						expand 2 in 166 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #14
					// create new row with flag new=1 
						expand 2 in 167 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #15
					// create new row with flag new=1 
						expand 2 in 168 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
					// data point #16
					// create new row with flag new=1 
						expand 2 in 169 , gen(new)
					// null out row_id for new row
						replace row_id = . if new == 1
					// Calculate point prevalence based on period prevalence, duration and recall period  
						gen period_prev=mean if new==1
						gen point_prev=period_prev*(21/(21-1+365)) if new==1
						replace mean=point_prev if new==1
						replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
						replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
						replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
   
						replace data_status=" " if new==1
						replace is_raw="adjusted" if new==1
						drop new period_prev point_prev
						
						
// Resolve ESS split issues 
	//Replace sample_size with correct values for each age/sex group as reported in the original study (nid unavailable, study citation: Hechavarria IF et al. Pesquisaje de otitis media cronica secretoria. Rev Cubana Cir 1989;28:287-293). Recalculate se, lower and upper based on the correct sample size.

	replace sample_size=68 if row_id==21 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace data_status=" " if row_id==21 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if row_id==21 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==21 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==21 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

	
	replace sample_size=44 if row_id==22 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace data_status=" " if row_id==22 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if row_id==22 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==22 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==22 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

	
	replace sample_size=152 if row_id==23 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace data_status=" " if row_id==23 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if row_id==23 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==23 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==23 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

	replace sample_size=113 if row_id==24 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace data_status=" " if row_id==24 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if row_id==24 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==24 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==24 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

	replace sample_size=34 if row_id==25 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace data_status=" " if row_id==25 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if row_id==25 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==25 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==25 & grouping=="chronic" & description=="GBD 2010 All uncertfix"


	replace sample_size=65 if row_id==26 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace data_status=" " if row_id==26 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if row_id==26 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==26 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if row_id==26 & grouping=="chronic" & description=="GBD 2010 All uncertfix"

	replace denominator=sample_size if row_id==21 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==22 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==23 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==24 & grouping=="chronic" & description=="GBD 2010 All uncertfix"| row_id==25 & grouping=="chronic" & description=="GBD 2010 All uncertfix" | row_id==26 & grouping=="chronic" & description=="GBD 2010 All uncertfix"
	
	//nid=103411 - Replace sample_size with correct values as reported in the original study. Recalculate se, lower and upper using the correct sample size.
	replace sample_size=denominator if nid==103411 & grouping=="chronic"
	replace data_status=" " if nid==103411 & grouping=="chronic"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if nid==103411 & grouping=="chronic"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if nid==103411 & grouping=="chronic"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if nid==103411 & grouping=="chronic"
	
	
	//Replace sample_size with correct values as reported in the original study (nid unavailable, study citation: Chayarpham S et al. A study of the prevalence of and risk factor for ear diseases and hearing loss in primary school children in Hat Yai, Thailand. Journal of the Medical Association of Thailand. 1996;79:468-72)
	replace sample_size=denominator if citation=="Chayarpham S et al. A study of the prevalence of and risk factor for ear diseases and hearing loss in primary school children in Hat Yai, Thailand. Journal of the Medical Association of Thailand. 1996;79:468-72" & grouping=="chronic"
	replace data_status=" " if citation=="Chayarpham S et al. A study of the prevalence of and risk factor for ear diseases and hearing loss in primary school children in Hat Yai, Thailand. Journal of the Medical Association of Thailand. 1996;79:468-72" & grouping=="chronic"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if citation=="Chayarpham S et al. A study of the prevalence of and risk factor for ear diseases and hearing loss in primary school children in Hat Yai, Thailand. Journal of the Medical Association of Thailand. 1996;79:468-72" & grouping=="chronic"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if citation=="Chayarpham S et al. A study of the prevalence of and risk factor for ear diseases and hearing loss in primary school children in Hat Yai, Thailand. Journal of the Medical Association of Thailand. 1996;79:468-72" & grouping=="chronic"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if citation=="Chayarpham S et al. A study of the prevalence of and risk factor for ear diseases and hearing loss in primary school children in Hat Yai, Thailand. Journal of the Medical Association of Thailand. 1996;79:468-72" & grouping=="chronic"
	
	//nid=103409 - replace sample_size with correct values as reported in the original study
	replace sample_size=denominator if nid==103409 & grouping=="acute"
	replace data_status=" " if nid==103409 & grouping=="acute"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if nid==103409 & grouping=="acute"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if nid==103409 & grouping=="acute"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if nid==103409 & grouping=="acute"
	
	//nid=103408 - replace sample_size with correct values as reported in the original study
	replace sample_size=denominator if nid==103408 & grouping=="acute"
	replace data_status=" " if nid==103408 & grouping=="acute"
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if nid==103408 & grouping=="acute"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if nid==103408 & grouping=="acute"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if nid==103408 & grouping=="acute"
	
	//nid=103417 - replace sample_size with correct values as reported in the original study. Also needs age-sex splitting which will be done using the agesex_split.ado file
	replace sample_size=denominator if nid==103417 & grouping=="acute"
		
	//nid=103420 - replace sample_size with correct values as reported in the original study
	replace sample_size=denominator if nid==103420 & grouping=="acute"
	replace data_status=" " if nid==103420 & grouping=="acute"
	replace data_status="excluded" if nid==103420 & grouping=="acute" & age_end==13
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if nid==103420 & grouping=="acute"
	replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if nid==103420 & grouping=="acute"
	replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if nid==103420 & grouping=="acute"
	
//Fix data entry errors: replace sample_size and denominator with correct numbers from the original study 
	replace sample_size=3485 if age_start==0 & sex==3 & nid==116106
	replace sample_size=4091 if age_start==1 & sex==3 & nid==116106
	replace sample_size=3951 if age_start==2 & sex==3 & nid==116106
	replace sample_size=3733 if age_start==3 & sex==3 & nid==116106
	replace sample_size=3886 if age_start==4 & sex==3 & nid==116106
	replace sample_size=9416 if age_start==0 & sex==2 & nid==116106
	replace sample_size=9730 if age_start==0 & sex==1 & nid==116106

	replace numerator=1740 if age_start==0 & sex==3 & nid==116106
	replace numerator=1377 if age_start==1 & sex==3 & nid==116106
	replace numerator=836 if age_start==2 & sex==3 & nid==116106
	replace numerator=688 if age_start==3 & sex==3 & nid==116106
	replace numerator=584 if age_start==4 & sex==3 & nid==116106
	replace numerator=2496 if age_start==0 & sex==2 & nid==116106
	replace numerator=2729 if age_start==0 & sex==1 & nid==116106
	
//Fix data entry errors: replace correct values for numerator and denominator for "Eason RJ et al. Chronic suppurative otitis media in the Solomon Islands: a prospective, microbiological, audiometric and therapeutic survey. New Zealand Medical Journal. 1986;99:812-5" 

	// mark data_status=excluded & is_raw="excluded_review"
	replace data_status="excluded" if citation=="Eason RJ et al. Chronic suppurative otitis media in the Solomon Islands: a prospective, microbiological, audiometric and therapeutic survey. New Zealand Medical Journal. 1986;99:812-5" 
	replace is_raw = "excluded_review" if citation=="Eason RJ et al. Chronic suppurative otitis media in the Solomon Islands: a prospective, microbiological, audiometric and therapeutic survey. New Zealand Medical Journal. 1986;99:812-5" 
 
	// Create 3 new rows for the 3 age groups (total sample size=3500 with N=1225 in each age group)
	
	// create new row with flag new=1 for 0-4 yr olds
	expand 2 in 1013 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// Replace 'mean' with the annual incidence rate value (41.26%) which was calculated from the 3-year cumulative incidence (71%) by applying the following formula: [-ln(1 - S)]/t; where S is the proportion of new cases over t years and t equals the time of follow-up 
	replace numerator=72 if new==1
	replace denominator=1225 if new==1
	replace sample_size=1225 if new==1
	replace mean=numerator/denominator if new==1
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
    replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
    replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
	replace data_status=" " if new==1
	replace is_raw="adjusted" if new==1
	replace age_start=0 if new==1
	replace age_end=4 if new==1
	drop new
	
	// create new row with flag new=1 for 5-9 yr olds
	expand 2 in 1014 , gen(new)
	// null out row_id for new row
	replace row_id = . if new == 1
	// Replace 'mean' with the annual incidence rate value (41.26%) which was calculated from the 3-year cumulative incidence (71%) by applying the following formula: [-ln(1 - S)]/t; where S is the proportion of new cases over t years and t equals the time of follow-up 
	replace numerator=44 if new==1
	replace denominator=1225 if new==1
	replace sample_size=1225 if new==1
	replace mean=numerator/denominator if new==1
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
    replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
    replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
	replace data_status=" " if new==1
	replace is_raw="adjusted" if new==1
	replace age_start=5 if new==1
	replace age_end=9 if new==1
	drop new
	
	// create new row with flag new=1 for 10-14 yr olds
	expand 2 in 1015 , gen(new)
	// null out row_id for new row
	replace row_id =. if new == 1
	// Replace 'mean' with the annual incidence rate value (41.26%) which was calculated from the 3-year cumulative incidence (71%) by applying the following formula: [-ln(1 - S)]/t; where S is the proportion of new cases over t years and t equals the time of follow-up 
	replace numerator=16 if new==1
	replace denominator=1225 if new==1
	replace sample_size=1225 if new==1
	replace mean=numerator/denominator if new==1
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if new == 1  
    replace lower = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 - invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1 
    replace upper = 1/(1 + 1/sample_size * invnormal(0.975)^2) * (mean + 1/(2*sample_size) * invnormal(0.975)^2 + invnormal(0.975) * sqrt(1/sample_size * mean * (1 - mean) + 1/(4*sample_size^2) * invnormal(0.975)^2)) if new == 1
	replace data_status=" " if new==1
	replace is_raw="adjusted" if new==1
	replace age_start=0 if new==1
	replace age_end=4 if new==1
	drop new
 
// nid=116116 - convert period to point prevalence before age-sex splitting
gen period_prev=numerator/denominator if nid==116116 & grouping=="acute"
gen point_prev=period_prev*(21/(21-1+365)) if nid==116116 & grouping=="acute"
replace mean= point_prev if nid == 116116 & grouping=="acute"
drop period_prev period_prev
	
// run transformations, organized by logical groupings
	
	// split denominators by age/sex distribution observed in the general population for each given country/year (example given below)
		// mark each group of datapoints to split with a unique group number: 1, 2, 3, etc.
		gen ess_split_group = 1 if nid==103407 & year_start==1978
		replace ess_split_group = 2 if nid==103407 & year_start==1994
		replace ess_split_group = 3 if nid==103414 & grouping=="chronic"
		esssplit ${functional}, date(${date}) tmpdir("ADDRESS")

	// age-sex split data points given by age and by sex separately (example given below)
		// mark each group of datapoints to split with a unique group number: 1, 2, 3, etc.
		gen agesex_split_group = 1 if nid == 116106 
		replace agesex_split_group = 2 if nid==116107
		replace agesex_split_group = 3 if nid==103417 & grouping=="acute"
		replace agesex_split_group = 4 if nid==116116 & grouping=="acute"
		agesexsplit ${functional}, date(${date}) tmpdir("ADDRESS")

// run checks and save in 05_upload; will break if error such as range check or missing required variable (correct and rerun), otherwise will save for upload by data analyst
	// this script will eventually upload directly to the database but we want to run several iterations of uploads centrally so we can build the proper checks; there are a ton of things that can go wrong during upload
	do "FILEPATH"
	cap log close
