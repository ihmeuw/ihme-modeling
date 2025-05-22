# Introduction ------------------------------------------------------------
# Purpose: Apply necessary changes to data utilized in the tobacco pipeline
#             - Usually this is overlapping or duplicated age or sex sets due to over-extractions
# Date Modified: 2024-11-14

message(paste0("Cleaning ", me_name, " data from ", tobdf, ". When a new best dataset or new_data is utilized, these codes can be archived and created again to avoid duplicates, etc."))

if (tobdf=="old_bundle"){
  # Data fixes needed in GBD 2023 bundle 
  old_bundle <- old_bundle[!nid %in% c(398205, 20663, 23289),] # Bad source, not sure why removed
  old_bundle <- old_bundle[!(nid==228155 & location_id==4940),] # Sweden data that needs fixed before GBD 2024
  old_bundle <- old_bundle[!(nid==136037),] # WHO Infobase, not meant to be a source
  old_bundle <- old_bundle[!(nid %in% c(112185, 265260, 433076)),] # Old tabbed data; extract microdata for GBD 2024
  old_bundle[nid==264914, standard_error := mean*(standard_error/100)]
  old_bundle[is.na(location_id) & ihme_loc_id=="IND_44538\r", location_id := 44538]
  old_bundle <- old_bundle[age_end>=age_start,] # Filter out impossible data
  old_bundle <- old_bundle[!(nid %in% c(268805, 341722, 341718, 341714, 341719, 341725, 341721, 341710, 341720, 341724, 341712, 341713)),] # Eurobarometer data; none of these NIDs exist anymore
  old_bundle <- old_bundle %>% filter(!nid==398191) # Eurobarometer; NID doesn't exist anymore
  old_bundle[nid==20663 & year_id==2010, nid := 270469]
  old_bundle[nid==264914, variance:=NA]
  old_bundle <- old_bundle[!(nid %in% c(126170, 233332))] # Duplicative
  old_bundle[nid==257515 & age_end==99, age_end := 24] # Filling in guess at year (shouldn't be 99)
  old_bundle <- old_bundle[!(nid==349185)] # Drop West Bank data
  old_bundle <- old_bundle[nid==373323, nid := 148099]
  
  # Data fixes by outcome
  if (me_name == "smoking_prev_current_any"){
    old_bundle <- old_bundle[!(nid==112217 & age_start==10 & age_end==99),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==12 & age_end==15),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==529869 & age_start==12 & age_end==75),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==230382 & age_start==13 & age_end==13),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==300930 & age_start==13 & age_end==17),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==14 & age_end==19),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==264914 & age_start==14 & age_end==99),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==480992 & age_start==15 & age_end==24),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(308316, 322237) & age_start==15 & age_end==49),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(111895, 135662) & age_start==15 & age_end==64),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(150993, 352850) & age_start==15 & age_end==69),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(130569, 11046, 13226, 65273, 155628, 220585, 329984, 112141, 317302, 420659) & age_start==15 & age_end==99),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==11468 & age_start==15 & age_end==124),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==16 & age_end==17),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(38640) & age_start==16 & age_end==19 & location_id==102),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(112166, 112167) & age_start==16 & age_end==84),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(230382) & age_start==17 & age_end==17),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(111809) & age_start==14 & age_end==19),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==18 & age_end==19),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(303300, 352860) & age_start==18 & age_end==69),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(38640) & age_start==18 & age_end==84 & location_id==71),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914, 200160, 139214) & age_start==18 & age_end==99),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==20 & age_end==24),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==20 & age_end==29),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==25 & age_end==29),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(126409) & age_start==25 & age_end==44),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==30057 & age_start==25 & age_end==64),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==30 & age_end==39),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==40 & age_end==49),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(230494, 230510, 230514, 230516, 230517) & age_start==40 & age_end==54),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(126409) & age_start==45 & age_end==65),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==40 & age_end==69),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==50 & age_end==59),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==60 & age_end==69),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==627 & age_start==60 & age_end==99),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914) & age_start==70 & age_end==99),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==270469 & var=="cig_current_any_var"),] # Added 2024/6/17 because this NID/var combo was causing errors in dcast below
    old_bundle <- old_bundle[!(nid==38640 & location_id==86 & age_start==25 & sample_size==50000),] # Duplicate
    old_bundle <- old_bundle[!(nid==480992 & var=="smoked_current_ltd_var"),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==11468 & var=="cig_current_any_var"),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==20798 & location_id==196 & var=="cig_current_daily_var"),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==20798 & location_id %in% c(482:490) & age_start==15 & age_end==19),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==30177 & age_start==15 & age_end==65 & sex_id==2 & var=="smoked_current_daily_var"),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==38640 & location_id==86 & year_id==2000 & age_start==15 & age_end==17),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==38640 & location_id==86 & year_id %in% c(2001:2005) & age_start==15 & age_end==24),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==38640 & location_id==86 & year_id %in% c(2001:2005) & age_start==25 & age_end==44),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==38640 & location_id==86 & year_id %in% c(2001:2005) & age_start==45 & age_end==64),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==38640 & location_id==86 & year_id %in% c(2001:2005) & age_start==65 & age_end==84),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(38640, 111818, 112042) & age_end==17),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==38640 & location_id==101 & year_id==1985 & age_start==15 & age_end==84),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==38640 & location_id==80 & year_id==1986 & sample_size<10000),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==20798 & location_id==196 & age_end==19),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==112214 & age_start==15 & age_end==99 & location_id==72),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==129770 & location_id==163 & age_start==18 & age_end==99),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid==129905 & location_id==163 & age_start==20 & age_end==99),] # Duplicate age sets
    old_bundle <- old_bundle[!(nid %in% c(161781, 264914, 303300, 13226, 122665) & sex_id==3),] # Duplicate age sets
    old_bundle[nid==11046 & sex_id==2 & age_start==15 & age_end==99 & var=="cig_current_any_var", c("val", "sample_size","variance") := list(NA,NA,NA)]
    old_bundle <- old_bundle[!nid==264914,]
    
    
  } else if(me_name == "smoking_prev_former_any"){
    old_bundle <- old_bundle[!(survey_name %like% "Latinobarometer")]
    old_bundle <- old_bundle[!(nid==218613)]
    old_bundle <- old_bundle[!(nid==265055 & mean < 0.002)] # Bad NID; very low means
    old_bundle[nid==126442, c("mean", "sample_size", "variance") := list(NA, NA, NA)]
    old_bundle <- old_bundle[!(nid %in% c(126187) & age_start==14 & age_end==99)]
    old_bundle <- old_bundle[!(nid %in% c(11468) & age_start==15 & age_end==124)]
    old_bundle <- old_bundle[!(nid %in% c(218613, 11468) & age_start==15 & age_end==199)]
    old_bundle <- old_bundle[!(nid==111818 & age_end==17)]
    old_bundle <- old_bundle[!(nid %in% c(112214, 155628) & age_start==15 & age_end==99)]
    old_bundle <- old_bundle[!(nid==135662 & age_start==15 & age_end==64)]
    old_bundle <- old_bundle[!(nid==352850 & age_start==15 & age_end==69)]
    old_bundle <- old_bundle[!(nid==329984 & age_start==15 & age_end==99)]
    old_bundle <- old_bundle[!(nid==126397 & age_start==18 & age_end==69)]
    old_bundle <- old_bundle[!(nid==200160 & age_start==18 & age_end==99)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==20 & age_end==29)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==25 & age_end==34)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==30 & age_end==39)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==40 & age_end %in% c(49, 69))]
    old_bundle <- old_bundle[!(nid==161781 & age_start==50 & age_end==59)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==70 & age_end==99)]
    
  } else if (me_name == "smoking_prev"){
    old_bundle <- old_bundle[!(nid %in% c( 22388, 22403, 22433, 22449, 22463, 22476, 95628, 95629, 95630, 95631, 119359, 119598, 119609, 161371, 162036, 233304, 341359) & age_start > 65)]
    old_bundle <- old_bundle[!nid==264914,]
    old_bundle <- old_bundle[!(nid==529869 & age_start==12 & age_end==75)]
    old_bundle <- old_bundle[!(nid==126187 & age_start==14 & age_end==99)]
    old_bundle <- old_bundle[!(nid==20798 & age_start==15 & age_end==19)]
    old_bundle <- old_bundle[!(nid==480992 & age_start==15 & age_end==24)]
    old_bundle <- old_bundle[!(nid==30177 & age_start==15 & age_end==65)]
    old_bundle <- old_bundle[!(nid %in% c(150993, 352850) & age_start==15 & age_end==69)]
    old_bundle <- old_bundle[!(nid %in% c(130569, 155628, 420659) & age_start==15 & age_end==99),]
    old_bundle <- old_bundle[!(nid==30057 & age_start==25 & age_end==64)]
    old_bundle <- old_bundle[!(nid==111818 & age_start==14 & age_end==17)]
    old_bundle <- old_bundle[!(nid==38640 & age_start==15 & age_end==17 & location_id==102)]
    old_bundle <- old_bundle[!(nid==38640 & age_start==15 & age_end==19 & location_id==80)]
    old_bundle <- old_bundle[!(nid==38640 & age_start==19 & age_end==24 & location_id==80)]
    old_bundle <- old_bundle[!(nid==38640 & age_start==35 & age_end==49 & location_id==80)]
    old_bundle <- old_bundle[!(nid==38640 & age_start==50 & age_end==64 & location_id==80)]
    old_bundle <- old_bundle[!(nid==38640 & age_start==65 & age_end==84 & location_id==80)]
    old_bundle <- old_bundle[!(nid %in% c(112166, 112167, 124411) & age_start==16 & age_end==84),]
    old_bundle <- old_bundle[!(nid %in% c(112166, 112167, 112186, 126397) & age_start==18 & age_end==69),]
    old_bundle <- old_bundle[!(nid %in% c(129770) & age_start==18 & age_end==99),]
    old_bundle <- old_bundle[!(nid %in% c(129905) & age_start==20 & age_end==99),]
    old_bundle <- old_bundle[!(nid %in% c(161781, 200160) & age_start==18 & age_end==99)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==20 & age_end==29)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==25 & age_end==34)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==30 & age_end==39)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==40 & age_end %in% c(49, 69))]
    old_bundle <- old_bundle[!(nid==161781 & age_start==50 & age_end==59)]
    old_bundle <- old_bundle[!(nid==161781 & age_start==70 & age_end==99)]
    old_bundle <- old_bundle[!(nid==300930 & age_start==13 & age_end==17)]
    old_bundle <- old_bundle[!(nid==111809 & age_start==14 & age_end==19)]
    old_bundle <- old_bundle[!(nid==126409 & age_start==45 & age_end==65)]
    
  }
  # Check duplicates (fix and run until no duplicates found)
  old_bundle <- old_bundle %>% dplyr::select(-any_of(c("seq"))) %>% distinct() # removes rows that are fully duplicated (i.e. no investigation necessary)
  message("Done cleaning old_bundle!")
}

if (tobdf=="new_data"){
  # General data fixes for newly-imported data
  # Check this each round to see what still applies and what can be cleaned up
  new_data <- new_data[!nid %in% c(398205, 20663, 23289),] # Bad source, not sure why removed
  new_data <- new_data[!(nid==228155 & location_id==4940),] # Sweden data that needs fixed before GBD 2024
  new_data <- new_data[!(nid==136037),] # WHO Infobase, not meant to be a source
  new_data <- new_data[!(nid %in% c(112185, 265260, 433076)),] # Old tabbed data; extract microdata for GBD 2024
  new_data[nid==264914, standard_error := val*(standard_error/100)]
  new_data[is.na(location_id) & ihme_loc_id=="IND_44538\r", location_id := 44538]
  new_data <- new_data[age_end>=age_start,] # Filter out impossible data
  new_data <- new_data[!(nid %in% c(268805, 341722, 341718, 341714, 341719, 341725, 341721, 341710, 341720, 341724, 341712, 341713)),] # Eurobarometer data; none of these NIDs exist anymore
  new_data <- new_data %>% filter(!nid==398191) # Removes duplicate EM data, keeps non-duplicate data
  new_data[nid==20663 & year_id==2010, nid := 270469]
  new_data[nid==264914, variance:=NA]
  new_data <- new_data[!(nid %in% c(126170, 233332))] # Duplicative
  new_data[nid==257515 & age_end==99, age_end := 24] # Filling in guess at year (shouldn't be 99)
  new_data <- new_data[!(nid==349185)] # Drop West Bank data
  new_data <- new_data[nid==373323, nid := 148099]
  new_data <- new_data[location_id==44533, location_id := 6]
  
  
  # Data fixes by outcome
  if (me_name == "smoking_prev_current_any"){
    new_data <- new_data[!(nid==112217 & age_start==10 & age_end==99),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==12 & age_end==15),] # Duplicate age sets
    new_data <- new_data[!(nid==529869 & age_start==12 & age_end==75),] # Duplicate age sets
    new_data <- new_data[!(nid==230382 & age_start==13 & age_end==13),] # Duplicate age sets
    new_data <- new_data[!(nid==300930 & age_start==13 & age_end==17),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==14 & age_end==19),] # Duplicate age sets
    new_data <- new_data[!(nid==264914 & age_start==14 & age_end==99),] # Duplicate age sets
    new_data <- new_data[!(nid==480992 & age_start==15 & age_end==24),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(308316, 322237) & age_start==15 & age_end==49),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(111895, 135662) & age_start==15 & age_end==64),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(150993, 352850) & age_start==15 & age_end==69),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(130569, 11046, 13226, 65273, 155628, 220585, 329984, 112141, 317302) & age_start==15 & age_end==99),] # Duplicate age sets
    new_data <- new_data[!(nid==11468 & age_start==15 & age_end==199),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==16 & age_end==17),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(38640) & age_start==16 & age_end==19 & location_id==102),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(112166, 112167) & age_start==16 & age_end==84),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(230382) & age_start==17 & age_end==17),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(111809) & age_start==14 & age_end==19),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==18 & age_end==19),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(303300, 352860) & age_start==18 & age_end==69),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(38640) & age_start==18 & age_end==84 & location_id==71),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914, 200160, 139214) & age_start==18 & age_end==99),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==20 & age_end==24),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==20 & age_end==29),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==25 & age_end==29),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(126409) & age_start==25 & age_end==44),] # Duplicate age sets
    new_data <- new_data[!(nid==30057 & age_start==25 & age_end==64),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==30 & age_end==39),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==40 & age_end==49),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(230494, 230510, 230514, 230516, 230517) & age_start==40 & age_end==54),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(126409) & age_start==45 & age_end==65),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==40 & age_end==69),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==50 & age_end==59),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==60 & age_end==69),] # Duplicate age sets
    new_data <- new_data[!(nid==627 & age_start==60 & age_end==99),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914) & age_start==70 & age_end==99),] # Duplicate age sets
    new_data <- new_data[!(nid==270469 & var=="cig_current_any_var"),] # Added 2024/6/17 because this NID/var combo was causing errors in dcast below
    new_data <- new_data[!(nid==38640 & location_id==86 & age_start==25 & sample_size==50000),] # Duplicate
    new_data <- new_data[!(nid==480992 & var=="smoked_current_ltd_var"),] # Duplicate age sets
    new_data <- new_data[!(nid==11468 & var=="cig_current_any_var"),] # Duplicate age sets
    new_data <- new_data[!(nid==20798 & location_id==196 & var=="cig_current_daily_var"),] # Duplicate age sets
    new_data <- new_data[!(nid==20798 & location_id %in% c(482:490) & age_start==15 & age_end==19),] # Duplicate age sets
    new_data <- new_data[!(nid==30177 & age_start==15 & age_end==65 & sex_id==2 & var=="smoked_current_daily_var"),] # Duplicate age sets
    new_data <- new_data[!(nid==38640 & location_id==86 & year_id==2000 & age_start==15 & age_end==17),] # Duplicate age sets
    new_data <- new_data[!(nid==38640 & location_id==86 & year_id %in% c(2001:2005) & age_start==15 & age_end==24),] # Duplicate age sets
    new_data <- new_data[!(nid==38640 & location_id==86 & year_id %in% c(2001:2005) & age_start==25 & age_end==44),] # Duplicate age sets
    new_data <- new_data[!(nid==38640 & location_id==86 & year_id %in% c(2001:2005) & age_start==45 & age_end==64),] # Duplicate age sets
    new_data <- new_data[!(nid==38640 & location_id==86 & year_id %in% c(2001:2005) & age_start==65 & age_end==84),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(38640, 111818, 112042) & age_end==17),] # Duplicate age sets
    new_data <- new_data[!(nid==38640 & location_id==101 & year_id==1985 & age_start==15 & age_end==84),] # Duplicate age sets
    new_data <- new_data[!(nid==38640 & location_id==80 & year_id==1986 & sample_size<10000),] # Duplicate age sets
    new_data <- new_data[!(nid==20798 & location_id==196 & age_end==19),] # Duplicate age sets
    new_data <- new_data[!(nid==112214 & age_start==15 & age_end==99 & location_id==72),] # Duplicate age sets
    new_data <- new_data[!(nid==129770 & location_id==163 & age_start==18 & age_end==99),] # Duplicate age sets
    new_data <- new_data[!(nid==129905 & location_id==163 & age_start==20 & age_end==99),] # Duplicate age sets
    new_data <- new_data[!(nid %in% c(161781, 264914, 303300, 13226, 122665) & sex_id==3),] # Duplicate age sets
    new_data[nid==11046 & sex_id==2 & age_start==15 & age_end==99 & var=="cig_current_any_var", c("val", "sample_size","variance") := list(NA,NA,NA)]
    new_data <- new_data[!nid==264914,]
    new_data <- new_data[!(nid==38640 & var %like% "daily"),]
    new_data <- new_data[!(nid==38640 & location_id==95 & age_start==16)]
    new_data <- new_data[!(nid==38640 & location_id==102 & age_start==15 & year_id==1986)]
    new_data <- new_data[!(nid==126205 & age_start==18)]
    new_data <- new_data[!(nid==240631 & sex_id %in% c(1,2))]
    new_data <- new_data[!(nid %in% c(356835, 448338, 480879, 480880) & sex_id %in% c(3))]
    new_data <- new_data[!(nid %in% c(365438, 421665) & source=="tabulations_2018")]
    new_data <- new_data[!(nid==111887 & var %like% "daily"),]
    new_data <- new_data[!(nid==119495 & var=="smoked_current_any_var"),]
    
    # Case of nid 103981 having daily & less than daily but no "any"; sum those two vals and sample sizes
        new_data <- new_data[!(nid==103981 & sex_id==3),]
        daily_ltd <- new_data[nid==103981 & var %in% c("smoked_current_daily_var", "smoked_current_ltd_var"),]
        new_data <- setdiff(new_data, daily_ltd)
        daily_ltd_sum <- sum_rows <- daily_ltd[, lapply(.SD, sum), 
                                                 by=c("year_id", "location_id", "sex_id", "age_start", "age_end", "nid"), 
                                                 .SDcols=c("sample_size", "val")]
        daily_ltd <- daily_ltd %>% distinct(year_id, location_id, sex_id, age_start, age_end, nid, .keep_all = T) %>% dplyr::select(-c(val, sample_size)) %>% 
                                   merge(daily_ltd_sum)
        new_data <- rbind(new_data, daily_ltd, fill=T)
    
    
    
  } else if(me_name == "smoking_prev_former_any"){
    new_data <- new_data[!(survey_name %like% "Latinobarometer")]
    new_data <- new_data[!(nid==218613)]
    new_data <- new_data[!(nid==265055 & val < 0.002)] # Bad NID; very low vals
    new_data[nid==126442, c("val", "sample_size", "variance") := list(NA, NA, NA)]
    new_data <- new_data[!(nid %in% c(218613, 11468) & age_start==15 & age_end==199)]
    new_data <- new_data[!(nid==111818 & age_end==17)]
    new_data <- new_data[!(nid %in% c(112214, 155628) & age_start==15 & age_end==99)]
    new_data <- new_data[!(nid==135662 & age_start==15 & age_end==64)]
    new_data <- new_data[!(nid==352850 & age_start==15 & age_end==69)]
    new_data <- new_data[!(nid==329984 & age_start==15 & age_end==99)]
    new_data <- new_data[!(nid==126397 & age_start==18 & age_end==69)]
    new_data <- new_data[!(nid==200160 & age_start==18 & age_end==99)]
    new_data <- new_data[!(nid %in% c(161781) & age_start==18 & age_end==99)]
    new_data <- new_data[!(nid==161781 & age_start==20 & age_end==29)]
    new_data <- new_data[!(nid==161781 & age_start==25 & age_end==34)]
    new_data <- new_data[!(nid==161781 & age_start==30 & age_end==39)]
    new_data <- new_data[!(nid==161781 & age_start==40 & age_end %in% c(49, 69))]
    new_data <- new_data[!(nid==161781 & age_start==50 & age_end==59)]
    new_data <- new_data[!(nid==161781 & age_start==70 & age_end==99)]
    new_data <- new_data[!(nid==126187 & age_start==14 & age_end==99)]
    new_data <- new_data[!(nid==103981 & sex_id==3)]
    
  } else if (me_name == "smoking_prev"){
    new_data <- new_data[!(nid %in% c(22388, 22403, 22433, 22449, 22463, 22476, 95628, 95629, 95630, 95631, 119359, 119598, 119609, 161371, 162036, 233304, 341359) & age_start > 65)]
    new_data <- new_data[!nid==264914]
    new_data <- new_data[!(nid==529869 & age_start==12 & age_end==75)]
    new_data <- new_data[!(nid==300930 & age_start==13 & age_end==17)]
    new_data <- new_data[!(nid==111818 & age_start==14 & age_end==17)]
    new_data <- new_data[!(nid %in% c(20798, 480992) & age_start==15 & age_end==24)]
    new_data <- new_data[!(nid==30177 & age_start==15 & age_end==65)]
    new_data <- new_data[!(nid %in% c(150993, 352850) & age_start==15 & age_end==69)]
    new_data <- new_data[!(nid %in% c(20798, 130569, 155628) & age_start==15 & age_end==99)]
    new_data <- new_data[!(nid %in% c(112166, 112167) & age_start==16 & age_end==84)]
    new_data <- new_data[!(nid %in% c(129770, 200160, 161781) & age_start==18 & age_end==99)]
    new_data <- new_data[!(nid==129905 & age_start==20 & age_end==99)]
    new_data <- new_data[!(nid==30057 & age_start==25 & age_end==64)]
    new_data <- new_data[!(nid==38640 & age_start==15 & age_end==17 & location_id==102)]
    new_data <- new_data[!(nid==38640 & age_start==15 & age_end==19 & location_id==80)]
    new_data <- new_data[!(nid==38640 & age_start==19 & age_end==24 & location_id==80)]
    new_data <- new_data[!(nid==38640 & age_start==35 & age_end==49 & location_id==80)]
    new_data <- new_data[!(nid==38640 & age_start==50 & age_end==64 & location_id==80)]
    new_data <- new_data[!(nid==38640 & age_start==65 & age_end==84 & location_id==80)]
    new_data <- new_data[!(nid==161781 & age_start==20 & age_end==29)]
    new_data <- new_data[!(nid==161781 & age_start==25 & age_end==34)]
    new_data <- new_data[!(nid==161781 & age_start==30 & age_end==39)]
    new_data <- new_data[!(nid==161781 & age_start==40 & age_end %in% c(49, 69))]
    new_data <- new_data[!(nid==161781 & age_start==50 & age_end==59)]
    new_data <- new_data[!(nid==161781 & age_start==70 & age_end==99)]
    new_data <- new_data[!(nid==111809 & age_start==14 & age_end==19)]
    new_data <- new_data[!(nid==126409 & age_start==45 & age_end==65)]
    new_data <- new_data[!(nid %in% c(103981, 161781) & sex_id==3)]
    new_data <- new_data[!(nid %in% c(161781) & age_start==14)]
    new_data <- new_data[!(nid %in% c(38640) & age_start==15)]
    
  }
new_data <- new_data %>% distinct() # removes rows that are fully duplicated (i.e. no investigation necessary)
message("Done cleaning new_data!")
}

