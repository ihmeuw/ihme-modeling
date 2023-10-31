## Purpose: Manually outlier data points
## --------------------------------------

# Load libraries
library(dplyr)
library(ggplot2)
library(data.table)
library('ihme.covid', lib.loc = 'FILEPATH')
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Set permissions
Sys.umask('0002')

# Define args if interactive. Parse args otherwise.
if (interactive()){
  code_dir <- "FILEPATH"
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--code_dir", type = "character", help = "Root of repository.")

  args <- parser$parse_args(get_args())
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  print("Arguments passed were:")
  for (arg in names(args)){
    print(paste0(arg, ": ", get(arg)))
  }
  
}

# Define additional args
plot_bool <- TRUE # if you want to plot along the way

# Define dirs
output_dir <-  "FILEPATH" 
today <- (Sys.Date()) %>% gsub('-', '_', .)
run_index <- ihme.covid::get_latest_output_date_index(output_dir, today)
output_subdir <- file.path(output_dir, sprintf("%s.%02i", today, run_index))
metadata_path <- file.path(output_subdir, "02_impute_data.metadata.yaml")
message(paste0("Saving outputs here: ", output_subdir))

# Load location sets
locs <- fread(file.path(output_subdir, "locs.csv"))
locs_for_pub <- fread(file.path(output_subdir, "locs_for_pub.csv"))

source(paste0(code_dir,"FILEPATH/custom_functions.R"))


## PART 1: load & prep data -----------------------------------------------------------------

# read in mobility data
all_mobility_decrease <- fread(file.path(output_subdir, "time_series_raw_with_rolling_mean.csv"))
all_mobility_decrease[, date:= as.Date(date)]

# offset 0 value data:
all_mobility_decrease[change_from_normal == 0, change_from_normal := 1]

# check for dups
all_mobility_decrease[, count := .N, by = c("ihme_loc_id","date")]
dups <- nrow(all_mobility_decrease[count > 1])
if(dups>0){
  stop(paste0("Dataset has ", dups, " duplicates"))
}

# Make a dataset of all possible location-days.
all_data_permutations <- as.data.table(expand.grid(ihme_loc_id = unique(all_mobility_decrease$ihme_loc_id),
                                                   date = seq(min(all_mobility_decrease$date),
                                                              max(all_mobility_decrease$date),by=1)))

# reshape from long to wide
all_mobility_decrease_wide <- dcast.data.table(all_mobility_decrease, ihme_loc_id + date ~ source,
                                               value.var = c("change_from_normal","change_from_normal_avg"))

# create a version with all location/dates
df_compiled <- merge(all_mobility_decrease_wide, all_data_permutations, by=c("ihme_loc_id","date"), all=T)


## PART 2: Outlier -----------------------------------------------------------------

# take the rolling avg (for plotting)
df_compiled <- df_compiled[, change_from_normal := change_from_normal_GOOGLE]
df_compiled <- rolling_fun_simple(df_compiled,5)
setnames(df_compiled,"change_from_normal_avg","all_rolling_mean")

# Set an upper and lower cap of 100 and -100
df_compiled[change_from_normal_GOOGLE > 100, change_from_normal_GOOGLE := 100]
df_compiled[change_from_normal_GOOGLE < -100, change_from_normal_GOOGLE := -100]

# If there are any locations where the first or last day are very different, drop those data
df_compiled_drop <- unique(df_compiled[is.finite(change_from_normal),.(ihme_loc_id,date,change_from_normal)])
df_compiled_drop <- df_compiled_drop[order(ihme_loc_id, date)]
df_compiled_drop[, order:= 1:.N, by=c("ihme_loc_id")]
df_compiled_drop[, rev_order:=.N:1, by=c("ihme_loc_id")]

drop_head <- df_compiled_drop[order %in% c(1,2,3)]
drop_head[, diff_1 := shift(change_from_normal,-1)-change_from_normal,by="ihme_loc_id"]
drop_head[, diff_2 := shift(change_from_normal,-2)-change_from_normal,by="ihme_loc_id"]
drop_head <- drop_head[abs(diff_1)>10 & abs(diff_2)>10 & abs(diff_1-diff_2)<10, .(ihme_loc_id, date, drop_endpoints = 1)]

drop_tail <- df_compiled_drop[rev_order %in% c(1,2,3)]
drop_tail[, diff_1 := shift(change_from_normal,1)-change_from_normal,by="ihme_loc_id"]
drop_tail[, diff_2 := shift(change_from_normal,2)-change_from_normal,by="ihme_loc_id"]
drop_tail <- drop_tail[abs(diff_1)>2 & abs(diff_2)>2 & abs(diff_1-diff_2)<2,.(ihme_loc_id, date, drop_endpoints = 1)]

drop <- rbind(drop_head, drop_tail)

df_compiled <- merge(df_compiled, drop, by=c("ihme_loc_id", "date"), all.x=T)
df_compiled[drop_endpoints == 1, out_flag_google := 1]

# Manual outliering here:

# Reported values
df_compiled[ihme_loc_id=="KOR" & date >= '2020-09-30' & date <= '2020-10-04', out_flag_google := 1] #Korea, dip in early october 2020
df_compiled[ihme_loc_id=="BLR" & date >= '2020-08-09' & date <= '2020-08-11', out_flag_google := 1] #Belarus, big drop off in August 2020
df_compiled[ihme_loc_id=="TZA" & date >= '2020-10-27' & date <= '2020-11-05', out_flag_google := 1] #Tanzania, outlier massive drop-off due to election
df_compiled[ihme_loc_id=="CIV" & date >= '2020-10-29' & date <= '2020-11-09', out_flag_google := 1] #Cote d'Ivoire, massive drop-off due to election
df_compiled[ihme_loc_id=="AFG" & date == '2020-10-29', out_flag_google := 1] #Afghanistan, one unexplained low day
df_compiled[ihme_loc_id=="AFG" & date >= '2021-03-21' & date <= '2021-03-22',  out_flag_google := 1] #Afghanistan, 2 low days
df_compiled[ihme_loc_id=="AFG" & date == '2021-04-13',  out_flag_google := 1] #Afghanistan, 1 low day
df_compiled[ihme_loc_id=="ZAF" & date == '2021-03-22',  out_flag_google := 1] #South Africa, one low day
df_compiled[ihme_loc_id=='ESP_60358' & (date=='2021-03-28'|date=='2021-04-01'|date=='2021-04-02'|date=='2021-04-03'|date=='2021-04-04'),
            out_flag_google := 1] #Aragon, 5 unexplained high days
df_compiled[grepl("BRA", ihme_loc_id) & date=='2021-06-03', out_flag_google := 1] # Many Brazil subnats: 6/3 low google
df_compiled[ihme_loc_id=="BRB" & date=='2021-06-30', out_flag_google:=1] #Barbados, one low day
df_compiled[ihme_loc_id=="BRB" & date >= '2021-07-02' & date <= '2021-07-03', out_flag_google:=1] #Barbados, 2 low days
df_compiled[ihme_loc_id=="EST" & date >= '2021-06-23' & date <= '2021-06-25', out_flag_google:=1] #Estonia, 3 low days 
df_compiled[ihme_loc_id=="LTU" & date >= '2021-06-24' & date <= '2021-06-25', out_flag_google:=1] #Lithuania, 2 low days
df_compiled[ihme_loc_id=="LTU" & date == '2021-07-06', out_flag_google:=1] #Lithuania, 1 low day
df_compiled[ihme_loc_id=="LVA" & date >= '2021-06-23' & date <= '2021-06-25', out_flag_google:=1] #Latvia, 3 low days
df_compiled[ihme_loc_id=="SVN" & date == '2021-06-25', out_flag_google:=1] #Slovenia, 1 low day
df_compiled[ihme_loc_id=="UKR" & (date == '2021-06-21' | date=='2021-06-28'), out_flag_google:=1] #Ukraine, 2 low days
df_compiled[ihme_loc_id=="FIN" & date >= '2021-06-25' & date <= '2021-06-26', out_flag_google:=1] #Finland, 2 low days
df_compiled[ihme_loc_id=="JAM" & date == '2021-07-04', out_flag_google:=1] #Jamaica, 1 low day
df_compiled[grepl("USA", ihme_loc_id) & date >= '2021-07-02' & date <= '2021-07-04', out_flag_google:=1] #US states, drop for 4th of july

# Christmas & New Year for all sources
df_compiled[date>='2020-12-24' & date <= '2020-12-26', out_flag_google := 1]
df_compiled[date>='2020-12-31' & date <= '2021-01-02', out_flag_google := 1]

df_compiled[ihme_loc_id=="SVK" & date == '2021-07-05', out_flag_google := 1] #Slovakia, one low day
df_compiled[ihme_loc_id=="CZE" & date >= '2021-07-05' & date <= '2021-07-06', out_flag_google := 1] #Czechia
df_compiled[ihme_loc_id=="HTI" & date >= '2021-07-07' & date <= '2021-07-09', out_flag_google:=1] #Haiti, 3 low days
df_compiled[ihme_loc_id=="HTI" & date >= '2021-07-22' & date <= '2021-07-23', out_flag_google:=1] #Haiti, 2 low days
df_compiled[ihme_loc_id=="BHS" & date == '2021-07-12', out_flag_google := 1] #Bahamas, one low day
df_compiled[ihme_loc_id=="ISR" & date >= '2021-07-17' & date <= '2021-07-18', out_flag_google:=1] #Israel, 2 low days
df_compiled[ihme_loc_id=="AFG" & date >= '2021-07-20' & date <= '2021-07-22', out_flag_google := 1] #Afghanistan, 3 low days
df_compiled[ihme_loc_id=="IRQ" & date >= '2021-07-20' & date <= '2021-07-22', out_flag_google := 1] #Iraq, one low day
df_compiled[ihme_loc_id=="GAB" & date == '2021-07-20', out_flag_google := 1] #Gabon, one low day
df_compiled[ihme_loc_id=="NGA" & date >= '2021-07-20' & date <= '2021-07-21', out_flag_google := 1] #Nigeria, 2 low days
df_compiled[ihme_loc_id=="PAK_53618" & date == '2021-07-20', out_flag_google := 1] #Islamabad capital territory, one low day
df_compiled[ihme_loc_id=="PHL" & date == '2021-07-20', out_flag_google := 1] #Philippines, one low day
df_compiled[ihme_loc_id=="TGO" & date == '2021-07-20', out_flag_google := 1] #Togo, one low day
df_compiled[ihme_loc_id=="COL" & date == '2021-07-20', out_flag_google := 1] #Colombia, one low day
df_compiled[ihme_loc_id=="NIC" & date == '2021-07-19', out_flag_google := 1] #Nicaragua, one low day
df_compiled[ihme_loc_id=="URY" & date == '2021-07-18', out_flag_google := 1] #Uruguay, one low day
df_compiled[ihme_loc_id=="URY" & date == '2021-07-25', out_flag_google := 1] #Uruguay, one low day
df_compiled[ihme_loc_id %in% c("BEN", "BIH", "GHA", "IDN") & date == '2021-07-20',
            out_flag_google := 1] # Benin, Bosnia, Ghana, Indonesia
df_compiled[ihme_loc_id=="BHR" & date >= '2021-07-20' & date <= '2021-07-22', out_flag_google := 1] #Bahrain, 3 low days
df_compiled[ihme_loc_id=="NER" & date >= '2021-07-20' & date <= '2021-07-21', out_flag_google := 1] #Niger, 2 low days
df_compiled[ihme_loc_id=="BFA" & date >= '2021-07-20' & date <= '2021-07-21', out_flag_google := 1] #Burkina Faso, 2 low days
df_compiled[ihme_loc_id=="TJK" & date >= '2021-07-20' & date <= '2021-07-21', out_flag_google := 1] #Tajikistan, 2 low days
df_compiled[ihme_loc_id=="MAR" & date >= '2021-07-19' & date <= '2021-07-26', out_flag_google := 1] #Morocco, low days
df_compiled[ihme_loc_id=="YEM" & date >= '2021-07-19' & date <= '2021-07-26', out_flag_google := 1] #Yemen, 2 low days
df_compiled[ihme_loc_id=="ARE" & date >= '2021-07-19' & date <= '2021-07-23', out_flag_google := 1] #UAE, 5 low days
df_compiled[ihme_loc_id=="BWA" & date >= '2021-07-19' & date <= '2021-07-20', out_flag_google := 1] #Botswana, 2 low days
df_compiled[ihme_loc_id=="CMR" & date >= '2021-07-19' & date <= '2021-07-20', out_flag_google := 1] #Cameroon, 2 low days
df_compiled[ihme_loc_id=="EGY" & date >= '2021-07-19' & date <= '2021-07-22', out_flag_google := 1] #Egypt, 4 low days
df_compiled[ihme_loc_id=="JOR" & date >= '2021-07-19' & date <= '2021-07-22', out_flag_google := 1] #Jordan, 4 low days
df_compiled[ihme_loc_id=="KGZ" & date >= '2021-07-19' & date <= '2021-07-20', out_flag_google := 1] #Kyrgyzstan, 2 low days
df_compiled[ihme_loc_id=="KWT" & date >= '2021-07-19' & date <= '2021-07-22', out_flag_google := 1] #Kuwait, 4 low days
df_compiled[ihme_loc_id=="LBY" & date >= '2021-07-19' & date <= '2021-07-26', out_flag_google := 1] #Libya, 2 low days
df_compiled[ihme_loc_id=="MLI" & date >= '2021-07-19' & date <= '2021-07-25', out_flag_google := 1] #Mali, 2 low days
df_compiled[ihme_loc_id=="QAT" & date >= '2021-07-19' & date <= '2021-07-25', out_flag_google := 1] #Qatar, 2 low days
df_compiled[ihme_loc_id=="SAU" & date >= '2021-07-19' & date <= '2021-07-22', out_flag_google := 1] #Saudi Arabia, 2 low days
df_compiled[ihme_loc_id=="SGP" & date >= '2021-07-19' & date <= '2021-07-26', out_flag_google := 1] #Singapore, 2 low days
df_compiled[ihme_loc_id=="TUR" & date >= '2021-07-19' & date <= '2021-07-24', out_flag_google := 1] #Turkey, 2 low days
df_compiled[ihme_loc_id %in% c("IND_4861","IND_4863") & date >= '2021-07-18' & date <= '2021-07-20', out_flag_google := 1] #Manipur, Mizoram 3 low days
df_compiled[ihme_loc_id=="IND_4874" & date >= '2021-07-18' & date <= '2021-07-19', out_flag_google := 1] #Uttarakhand, 2 low days
df_compiled[ihme_loc_id=="RWA" & date >= '2021-07-17' & date <= '2021-07-26', out_flag_google := 1] #Rwanda, 4 low days
df_compiled[ihme_loc_id=="KAZ" & date >= '2021-07-05' & date <= '2021-07-06', out_flag_google := 1] #Kazakhstan
df_compiled[ihme_loc_id=="KAZ" & date >= '2021-07-17' & date <= '2021-07-18', out_flag_google := 1] #Kazakhstan
df_compiled[ihme_loc_id=="KAZ" & date == '2021-07-20', out_flag_google := 1] #Kazakhstan
df_compiled[ihme_loc_id=="KAZ" & date >= '2021-07-24' & date <= '2021-07-25', out_flag_google := 1] #Kazakhstan
df_compiled[ihme_loc_id=="CIV" & date=='2021-07-20', out_flag_google := 1] #Cote d'Ivoire, 1 low day
df_compiled[ihme_loc_id=="ESP_60373" & date >= '2021-07-21' & date <= '2021-07-23', out_flag_google := 1] #Melilla, 3 low days
df_compiled[ihme_loc_id=="PNG" & date >= '2021-07-23' & date <= '2021-07-25', out_flag_google := 1] #Papua New Guinea, 3 low days
df_compiled[ihme_loc_id=="JPN" & date >= '2021-07-22' & date <= '2021-07-23', out_flag_google := 1] #Japan, 2 low days
df_compiled[ihme_loc_id=="LBN" & date >= '2021-07-20' & date <= '2021-07-21', out_flag_google := 1] #Lebanon, 2 low days
df_compiled[ihme_loc_id=="OMN" & date >= '2021-07-20' & date <= '2021-07-23', out_flag_google := 1] #Oman, 4 low days
df_compiled[ihme_loc_id=="SEN" & date >= '2021-07-19' & date <= '2021-07-26', out_flag_google := 1] #Senegal, low days
df_compiled[grepl("PAK", ihme_loc_id) & date >= '2021-07-20' & date <= '2021-07-25', out_flag_google := 1] #Pakistan subnationals
df_compiled[ihme_loc_id=="ZMB" & date >= '2021-08-12' & date <= '2021-08-16', out_flag_google := 1] #Zambia, big drop off
df_compiled[grepl("PAK", ihme_loc_id) & date >= '2021-08-14' & date <= '2021-08-20', out_flag_google := 1] #Pakistan
df_compiled[ihme_loc_id=="AFG" & date >= '2021-08-16' & date <= '2021-08-19', out_flag_google := 1] #Afghanistan
df_compiled[ihme_loc_id=="JAM" & (date=='2021-07-04' | date=='2021-08-02' | date=='2021-08-06' | 
                                  date=='2021-08-17' | date=='2021-08-22'), out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="NIC" & (date=='2021-08-08' | date=='2021-08-10' | date=='2021-08-22'), out_flag_google := 1] #Nicaragua
df_compiled[ihme_loc_id=="BHR" & date == '2021-08-09', out_flag_google := 1] #Bahrain
df_compiled[ihme_loc_id=="BHR" & date >= '2021-08-18' & date <= '2021-08-19', out_flag_google := 1] #Bahrain
df_compiled[ihme_loc_id=="IRQ" & date == '2021-08-19', out_flag_google := 1] #Iraq
df_compiled[ihme_loc_id=="USA_529" & date == '2021-08-22', out_flag_google := 1] #Connecticut
df_compiled[ihme_loc_id=="USA_562" & date == '2021-08-22', out_flag_google := 1] #Rhode Island
df_compiled[ihme_loc_id=="ZAF" & date >= '2021-07-12' & date <= '2021-07-14', out_flag_google := 1] #South Africa
df_compiled[ihme_loc_id=="ZAF" & date == '2021-08-09', out_flag_google := 1] #South Africa
df_compiled[ihme_loc_id=="ZWE" & date >= '2021-08-09' & date <= '2021-08-10', out_flag_google := 1] #Zimbabwe
df_compiled[ihme_loc_id=="BEN" & date == '2021-07-20', out_flag_google := 1] #Benin
df_compiled[ihme_loc_id=="BEN" & date == '2021-08-21', out_flag_google := 1] #Benin
df_compiled[ihme_loc_id=="BFA" & date >= '2021-07-20' & date <= '2021-07-21', out_flag_google := 1] #Burkina Faso
df_compiled[ihme_loc_id=="BFA" & date == '2021-08-05', out_flag_google := 1] #Burkina Faso
df_compiled[ihme_loc_id=="BFA" & (date == '2021-08-14' | date == '2021-08-16'), out_flag_google := 1] #Burkina Faso
df_compiled[ihme_loc_id=="DOM" & date >= '2021-08-11' & date <= '2021-08-12', out_flag_google := 1] #Domincan Republic
df_compiled[ihme_loc_id=="DOM" & date == '2021-08-16', out_flag_google := 1] #Domincan Republic
df_compiled[ihme_loc_id=="GAB" & date == '2021-07-20', out_flag_google := 1] #Gabon
df_compiled[ihme_loc_id=="GAB" & date >= '2021-08-16' & date <= '2021-08-17', out_flag_google := 1] #Gabon
df_compiled[ihme_loc_id=="MEX_4665" & date >= '2021-08-18' & date <= '2021-08-19', out_flag_google := 1] #Quintana Roo
df_compiled[ihme_loc_id=="MEX_4672" & date >= '2021-08-20' & date <= '2021-08-21', out_flag_google := 1] #Veracruz de Ignacio de la Llave
df_compiled[ihme_loc_id=="MEX_4673" & date >= '2021-08-19' & date <= '2021-08-20', out_flag_google := 1] #Yucatan
df_compiled[ihme_loc_id=="USA_541" & date >= '2021-08-29' & date <= '2021-09-06', out_flag_google := 1] # Louisiana, hurricane ida
df_compiled[ihme_loc_id=="USA_547" & date >= '2021-08-29' & date <= '2021-08-30', out_flag_google := 1] # Mississippi
df_compiled[ihme_loc_id=="SVK" & date=='2021-09-01', out_flag_google := 1] #Slovakia
df_compiled[ihme_loc_id=="EST" & date=='2021-08-20', out_flag_google := 1] #Estonia
df_compiled[ihme_loc_id=="NIC" & date>='2021-09-04' & date<='2021-09-05', out_flag_google := 1] #Nicaragua
df_compiled[ihme_loc_id=="BEN" & date=='2021-09-03', out_flag_google := 1] #Benin
df_compiled[ihme_loc_id=="TGO" & date>='2021-09-04' & date<='2021-09-05', out_flag_google := 1] #Togo
df_compiled[grepl('USA', ihme_loc_id) & date == '2021-09-06', out_flag_google := 1] #Labor day, all US locations
df_compiled[ihme_loc_id=="FJI" & date=='2021-09-07', out_flag_google := 1] #Fiji
df_compiled[ihme_loc_id=="SVK" & date=='2021-09-01', out_flag_google := 1] #Slovakia
df_compiled[ihme_loc_id=="SVK" & date=='2021-09-15', out_flag_google := 1] #Slovakia
df_compiled[ihme_loc_id=="ISR" & date>='2021-09-06' & date<='2021-09-08', out_flag_google := 1] #Israel
df_compiled[ihme_loc_id=="ISR" & date>='2021-09-15' & date<='2021-09-16', out_flag_google := 1] #Israel
df_compiled[ihme_loc_id=="SLV" & date=='2021-08-06', out_flag_google := 1] #El Salvador
df_compiled[ihme_loc_id=="SLV" & date=='2021-09-15', out_flag_google := 1] #El Salvador
df_compiled[ihme_loc_id=="GTM" & date=='2021-09-15', out_flag_google := 1] #Guatemala
df_compiled[ihme_loc_id=="JAM" & date>='2021-08-23' & date<='2021-08-24', out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="JAM" & date>='2021-08-29' & date<='2021-08-31', out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="JAM" & date>='2021-09-05' & date<='2021-09-07', out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="JAM" & date>='2021-09-12' & date<='2021-09-14', out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="JAM" & date=='2021-09-19', out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="JAM" & date=='2021-09-26', out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="NIC" & date>='2021-09-12' & date<='2021-09-19', out_flag_google := 1] #Nicaragua
df_compiled[ihme_loc_id=="ESP_60359" & date=='2021-09-15', out_flag_google := 1] #Cantabria
df_compiled[ihme_loc_id=="PNG" & date >= '2021-09-16' & date <= '2021-09-19', out_flag_google := 1] #Papua New Guinea, 4 low days
df_compiled[ihme_loc_id=="BGR" & date=='2021-09-06', out_flag_google := 1] #Bulgaria
df_compiled[ihme_loc_id=="BLR" & date=='2021-09-18', out_flag_google := 1] #Belarus
df_compiled[ihme_loc_id=="BLR" & date=='2021-05-15', out_flag_google := 1] #Belarus, historic high day
df_compiled[ihme_loc_id=="CHL" & date >= '2021-09-17' & date <= '2021-09-19', out_flag_google := 1] #Chile  
df_compiled[ihme_loc_id=="HND" & date=='2021-09-15', out_flag_google := 1] #Honduras
df_compiled[ihme_loc_id=="AGO" & (date=='2021-09-17' | date=='2021-09-19'), out_flag_google := 1] #Angola
df_compiled[ihme_loc_id=="TGO" & (date=='2021-09-12' | date=='2021-09-14' | date=='2021-09-19'), out_flag_google := 1] #Togo
df_compiled[grepl('MEX', ihme_loc_id) & date == '2021-09-16', out_flag_google := 1] #Many mexican subnats are very low on this day
df_compiled[grepl('BRA', ihme_loc_id) & date>='2021-09-06' & date<='2021-09-08', out_flag_google := 1] #Many Brazilian subnats are very low on these days
df_compiled[ihme_loc_id=='IND_4862' & (date=='2021-09-12' | date=='2021-09-19' | date=='2021-09-26'), out_flag_google := 1] #Meghalaya
df_compiled[ihme_loc_id=="PNG" & date=='2021-09-26', out_flag_google := 1] #Papua New Guinea
df_compiled[ihme_loc_id=="BGR" & date=='2021-09-22', out_flag_google := 1] #Bulgaria
df_compiled[ihme_loc_id=="BLZ" & (date=='2021-09-10' | date=='2021-09-21' | date=='2021-09-26'), out_flag_google := 1] #Belize
df_compiled[ihme_loc_id=="DOM" & date == '2021-09-24', out_flag_google := 1] #Domincan Republic
df_compiled[ihme_loc_id=="IND_4868" & date=='2021-09-26', out_flag_google := 1] #Rajasthan
df_compiled[ihme_loc_id=="SEN" & date >= '2021-09-25' & date <= '2021-09-27', out_flag_google := 1] #Senegal 
df_compiled[grepl('DEU', ihme_loc_id) & date == '2021-09-26', out_flag_google := 1] #Many German subnats are very high on this day
df_compiled[ihme_loc_id=="KOR" & date >= '2021-09-20' & date <= '2021-09-22', out_flag_google := 1] #Korea
df_compiled[ihme_loc_id=="ZAF_489" & date=='2021-09-24', out_flag_google := 1] #Northern Cape
df_compiled[ihme_loc_id=="ISR" & date>='2021-09-20' & date<='2021-09-21', out_flag_google := 1] #Israel
df_compiled[ihme_loc_id=="DEU_60392" & date=='2021-09-20', out_flag_google := 1] #Thuringia
df_compiled[ihme_loc_id=="BRA_4750" & date=='2021-09-26', out_flag_google := 1] #Acre
df_compiled[ihme_loc_id=="BEN" & date >= '2021-09-25' & date <= '2021-09-26', out_flag_google := 1] #Benin
df_compiled[ihme_loc_id=="KHM" & date=='2021-09-24', out_flag_google := 1] #Cambodia
df_compiled[ihme_loc_id=="ESP_60376" & date >= '2021-09-20' & date <= '2021-09-246', out_flag_google := 1] #La Rioja	
df_compiled[ihme_loc_id=="IND_4864" & date=='2021-09-16', out_flag_google := 1] #Nagaland 
df_compiled[ihme_loc_id=="IND_4865" & date=='2021-09-13', out_flag_google := 1] #Odisha
df_compiled[ihme_loc_id=="IND_4872" & date=='2021-09-17', out_flag_google := 1] #Tripura 
df_compiled[ihme_loc_id=="GHA" & date=='2021-09-21', out_flag_google := 1] #Ghana 
df_compiled[ihme_loc_id=="KHM" & date >= '2021-10-05' & date <= '2021-10-08', out_flag_google := 1] #Cambodia
df_compiled[ihme_loc_id=="PNG" & (date == '2021-10-03' | date == '2021-10-10'), out_flag_google := 1] #Papua New Guinea
df_compiled[ihme_loc_id=="BIH" & date >= '2021-10-09' & date <= '2021-10-10', out_flag_google := 1] #Bosnia & Herz
df_compiled[ihme_loc_id=="CZE" & date >= '2021-09-27' & date <= '2021-09-28', out_flag_google := 1] #Czechia
df_compiled[ihme_loc_id=="SVN" & date == '2021-10-10', out_flag_google := 1] #Slovenia 
df_compiled[ihme_loc_id=="ISR" & date >= '2021-09-27' & date <= '2021-09-28', out_flag_google := 1] #Israel
df_compiled[ihme_loc_id=="URY" & date >= '2021-10-09' & date <= '2021-10-11', out_flag_google := 1] #Uruguay
df_compiled[ihme_loc_id=="JAM" & (date == '2021-10-03' | date == '2021-10-10'), out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="HND" & date >= '2021-10-07' & date <= '2021-10-10', out_flag_google := 1] #Honduras
df_compiled[ihme_loc_id=="PRY" & date == '2021-10-10', out_flag_google := 1] #Paraguay 
df_compiled[ihme_loc_id=="IRQ" & date >= '2021-10-09' & date <= '2021-10-10', out_flag_google := 1] #Iraq
df_compiled[ihme_loc_id=="OMN" & date >= '2021-10-03' & date <= '2021-10-04', out_flag_google := 1] #Oman
df_compiled[ihme_loc_id=="IND_4872" & date == '2021-10-10', out_flag_google := 1] #Tripura
df_compiled[ihme_loc_id=="IND_4875" & date == '2021-10-10', out_flag_google := 1] #West Bengal
df_compiled[ihme_loc_id=="HUN" & (date == '2021-10-23' | date == '2021-10-25'), out_flag_google := 1] #Hungary
df_compiled[ihme_loc_id=="NZL" & date == '2021-10-25', out_flag_google := 1] #New Zealand
df_compiled[ihme_loc_id=="URY" & date == '2021-10-17', out_flag_google := 1] #Uruguay
df_compiled[ihme_loc_id=="URY" & date >= '2021-10-23' & date <= '2021-10-24', out_flag_google := 1] #Uruguay
df_compiled[ihme_loc_id=="JAM" & date >= '2021-10-17' & date <= '2021-10-18', out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="JAM" & date == '2021-10-24', out_flag_google := 1] #Jamaica
df_compiled[ihme_loc_id=="BGD" & (date == '2021-10-15' | date == '2021-10-20'), out_flag_google := 1] #Bangladesh
df_compiled[ihme_loc_id=="NPL" & date >= '2021-10-12' & date <= '2021-10-23', out_flag_google := 1] #Nepal
df_compiled[ihme_loc_id=="ZMB" & (date == '2021-10-18' | date == '2021-10-25'), out_flag_google := 1] #Zambia
df_compiled[ihme_loc_id=="SEN" & date >= '2021-10-18' & date <= '2021-10-19', out_flag_google := 1] #Senegal 
df_compiled[ihme_loc_id=="MEX_4654" & date == '2021-10-25', out_flag_google := 1] #Guerrero
df_compiled[ihme_loc_id=="IDN_4740" & date == '2021-10-20', out_flag_google := 1] #North Maluku, Indonesia
df_compiled[ihme_loc_id=="IDN_4742" & date == '2021-10-20', out_flag_google := 1] #Papua, Indonesia
df_compiled[grepl('BRA', ihme_loc_id) & date == '2021-10-12', out_flag_google := 1] #Many Brazilian subnats are very low on this day
df_compiled[ihme_loc_id=="BRA_4767" & date == '2021-10-25', out_flag_google := 1] #Piaui
df_compiled[ihme_loc_id=="IND_4872" & date >= '2021-10-15' & date <= '2021-10-25', out_flag_google := 1] #Tripura, violence/unrest 
df_compiled[ihme_loc_id=="BOL" & date=='2021-10-11', out_flag_google := 1] #Bolivia
df_compiled[ihme_loc_id=="BOL" & date=='2021-11-02', out_flag_google := 1] #Bolivia
df_compiled[ihme_loc_id=="BOL" & date>='2021-11-08' & date<='2021-11-09', out_flag_google := 1] #Bolivia
df_compiled[ihme_loc_id=="ZAF_489" & date=='2021-11-01', out_flag_google := 1] #Northern Cape
df_compiled[ihme_loc_id=="ESP_60363" & date=='2021-11-01', out_flag_google := 1] #Balearic Islands
df_compiled[ihme_loc_id=="BFA" & date>='2021-11-20' & date<='2021-11-28', out_flag_google := 1] #Burkina Faso
df_compiled[ihme_loc_id=="EGY" & date>='2021-11-20' & date<='2021-11-21', out_flag_google := 1] #Egypt
df_compiled[ihme_loc_id=="URY" & date=='2021-11-21', out_flag_google := 1] #Uruguay
df_compiled[grepl('BRA', ihme_loc_id) & (date=='2021-11-02' | date=='2021-11-15'), out_flag_google := 1] #Many Brazilian subats
df_compiled[ihme_loc_id=="BRA_4771" & date=='2021-11-21', out_flag_google := 1] #Roraima
df_compiled[ihme_loc_id=="BRA_4768" & date=='2021-11-20', out_flag_google := 1] #Rio
df_compiled[ihme_loc_id=="MEX_4659" & date=='2021-11-14', out_flag_google := 1] #Morelos
df_compiled[grepl('USA', ihme_loc_id) & date>='2021-11-25' & date<='2021-11-27', out_flag_google := 1] # Thanksgiving - USA
df_compiled[grepl('IND', ihme_loc_id) & date>='2021-11-04' & date<='2021-11-06', out_flag_google := 1] #many indian subnats low on these days
df_compiled[ihme_loc_id=="OMN" & date >= '2021-11-28' & date <= '2021-11-29', out_flag_google := 1] #Oman
df_compiled[ihme_loc_id=="BHR" & date=='2021-11-26', out_flag_google := 1] #Bahrain
df_compiled[ihme_loc_id=="HND" & date >= '2021-11-28' & date <= '2021-11-29', out_flag_google := 1] #Honduras
df_compiled[ihme_loc_id=="CRI" & date=='2021-11-29', out_flag_google := 1] #Costa Rica
df_compiled[ihme_loc_id=="PRI" & date>='2021-11-25' & date<='2021-11-27', out_flag_google := 1] # Puerto Rico
df_compiled[ihme_loc_id=="ARG" & date=='2021-11-22', out_flag_google := 1] #Argentina
df_compiled[ihme_loc_id=="ARG" & date=='2021-11-28', out_flag_google := 1] #Argentina
df_compiled[ihme_loc_id=="MNG" & date=='2021-11-05', out_flag_google := 1] #Mongolia
df_compiled[ihme_loc_id=="MNG" & date>='2021-11-26' & date<='2021-11-28', out_flag_google := 1] #Mongolia

# Christmas & New Year for all sources
df_compiled[date>='2021-12-24' & date <= '2021-12-26', out_flag_google := 1]
df_compiled[date>='2021-12-31' & date <= '2022-01-02', out_flag_google := 1]

# outliers 1/3
df_compiled[ihme_loc_id=="CAN_43868" & date >= '2021-12-20' & date <= '2021-12-23', out_flag_google := 1] #Canada - Quebec
df_compiled[ihme_loc_id=="CAN_43863" & date=='2021-12-20', out_flag_google := 1] #Canada - NW Territories
df_compiled[ihme_loc_id=="CAN_43862" & date=='2021-12-23', out_flag_google := 1] #Canada - Newfoundland & Labrador
df_compiled[ihme_loc_id=="CZE" & date=='2021-12-23', out_flag_google := 1] #Czechia 
df_compiled[ihme_loc_id=="HUN" & date>= '2021-12-22' & date <= '2021-12-23', out_flag_google := 1] #Hungary 
df_compiled[ihme_loc_id=="DNK" & date=='2021-12-23', out_flag_google := 1] #Denmark 
df_compiled[ihme_loc_id=="FIN" & date=='2021-12-23', out_flag_google := 1] #Finland 
df_compiled[ihme_loc_id=="LUX" & date>='2021-12-20'& date <= '2021-12-23', out_flag_google := 1] #Luxembourg 
df_compiled[ihme_loc_id=="NLD" & date>= '2021-12-19' & date <= '2021-12-23', out_flag_google := 1] #Netherlands 
df_compiled[ihme_loc_id=="NOR" & date=='2021-12-23', out_flag_google := 1] #Norway 
df_compiled[ihme_loc_id=="SWE" & date=='2021-12-23', out_flag_google := 1] #Sweden 
df_compiled[ihme_loc_id=="PRY" & date=='2021-12-23', out_flag_google := 1] #Paraguay
df_compiled[ihme_loc_id=="LBN" & date >= '2021-12-22' & date <= '2021-12-23', out_flag_google := 1] #Lebanon
df_compiled[ihme_loc_id=="BFA" & date=='2021-12-23', out_flag_google := 1] #Burkina Faso
df_compiled[ihme_loc_id=="USA_531" & date>='2021-12-20' & date <= '2021-12-23', out_flag_google := 1] #USA - DC
df_compiled[ihme_loc_id=="ITA_35507" & date=='2021-12-23', out_flag_google := 1] #Italy - Abruzzo
df_compiled[grepl('DEU', ihme_loc_id) & date=='2021-12-23', out_flag_google := 1] #Germany subnats

# outliers 1/24
df_compiled[ihme_loc_id=="GEO" & date=='2022-01-07', out_flag_google := 1] # Georgia
#df_compiled[ihme_loc_id=="KAZ" & date >= '2022-01-06' & date <= '2022-01-09', out_flag_google := 1] #Kazahkstan
df_compiled[ihme_loc_id=="BIH" & date >= '2022-01-02' & date <= '2022-01-03', out_flag_google := 1] # Bosnia
df_compiled[ihme_loc_id=="BIH" & date=='2022-01-07', out_flag_google := 1] # Bosnia
df_compiled[ihme_loc_id=="BGA" & date=='2022-01-03', out_flag_google := 1] # Bulgaria
df_compiled[ihme_loc_id=="BGA" & date >= '2021-12-27' & date <= '2021-12-28', out_flag_google := 1] # Bulgaria
df_compiled[ihme_loc_id=="HRV" & date=='2022-01-02', out_flag_google := 1] #Croatia
df_compiled[ihme_loc_id=="HRV" & date=='2022-01-06', out_flag_google := 1] #Croatia
df_compiled[ihme_loc_id=="MKD" & date >= '2022-01-06' & date <= '2022-01-07', out_flag_google := 1] # North Macedonia
df_compiled[ihme_loc_id=="POL" & date=='2021-12-27', out_flag_google := 1] # Poland
df_compiled[ihme_loc_id=="POL" & date >= '2022-01-06' & date <= '2022-01-07', out_flag_google := 1] # Poland
df_compiled[ihme_loc_id=="SRB" & date=='2022-01-03', out_flag_google := 1] # SErbia
df_compiled[ihme_loc_id=="SRB" & date >= '2022-01-06' & date <= '2022-01-08', out_flag_google := 1]# SErbia
df_compiled[ihme_loc_id=="SVK" & date=='2022-01-06', out_flag_google := 1] # Slovakia
df_compiled[ihme_loc_id=="BLR" & date >= '2022-01-07' & date <= '2022-01-08', out_flag_google := 1]# BElarus
df_compiled[ihme_loc_id=="MDA" & date >= '2022-01-07' & date <= '2022-01-09', out_flag_google := 1] # Moldova
df_compiled[ihme_loc_id=="MDA" & date == '2022-01-03', out_flag_google := 1]# Moldova
df_compiled[ihme_loc_id=="UKR" & date=='2022-01-07', out_flag_google := 1] # Ukraine
df_compiled[ihme_loc_id=="CAN_43866" & date=='2022-01-17', out_flag_google := 1] #CAnada- Ontario
df_compiled[ihme_loc_id=="DOM" & date=='2022-01-09', out_flag_google := 1] # DOminican REpublic
df_compiled[ihme_loc_id=="PRI" & date=='2022-01-06', out_flag_google := 1] # puerto rico
df_compiled[ihme_loc_id=="COL" & date=='2022-01-10', out_flag_google := 1] # Columbia
df_compiled[ihme_loc_id=="NGA" & date >= '2021-12-27' & date <= '2021-12-28', out_flag_google := 1]# NIgeriav
df_compiled[ihme_loc_id=="NGA" & date == '2022-01-03', out_flag_google := 1]# NIgeria
df_compiled[ihme_loc_id=="IND_4870" & date == '2022-01-09', out_flag_google := 1]# Tamil Nadu
df_compiled[ihme_loc_id=="IND_4870" & date == '2022-01-16', out_flag_google := 1]# Tamil Nadu
df_compiled[ihme_loc_id=="IND_4856" & date >= '2022-01-08' & date <= '2022-01-09', out_flag_google := 1]# karnataka
df_compiled[ihme_loc_id=="USA_556" & date == '2022-01-16', out_flag_google := 1]# North CArolina
df_compiled[ihme_loc_id=="USA_556" & date == '2022-01-17', out_flag_google := 1]# North CArolina
df_compiled[ihme_loc_id=="USA_563" & date == '2022-01-16', out_flag_google := 1]# South Carolina
df_compiled[ihme_loc_id=="USA_563" & date == '2022-01-17', out_flag_google := 1]# South Carolina
df_compiled[ihme_loc_id=="USA_571" & date == '2022-01-16', out_flag_google := 1]# West Virginia
df_compiled[ihme_loc_id=="USA_571" & date == '2022-01-17', out_flag_google := 1]# West Virginia
df_compiled[ihme_loc_id=="KEN" & date == '2021-12-27', out_flag_google := 1]# Kenya

# outliered 1/28
#df_compiled[ihme_loc_id=="" & date=='', out_flag_google := 1] # 
#df_compiled[ihme_loc_id=="" & date >= '' & date <= '', out_flag_google := 1] #

df_compiled[ihme_loc_id=="KAZ" & date >= '2022-01-05' & date <= '2022-01-10', out_flag_google := 1] #Kazakstan
df_compiled[ihme_loc_id=="BGR" & date >= '2021-12-27' & date <= '2021-12-28', out_flag_google := 1] #Bulgaria
df_compiled[ihme_loc_id=="BGR" & date == '2022-01-03', out_flag_google := 1] #Bulgaria
df_compiled[ihme_loc_id=="ROU" & date == '2022-01-24', out_flag_google := 1] #Romania
df_compiled[ihme_loc_id=="DOM" & date == '2022-01-21', out_flag_google := 1] #Dominican Republic
df_compiled[ihme_loc_id=="DOM" & date == '2022-01-24', out_flag_google := 1] #Dominican Republic
df_compiled[ihme_loc_id=="DOM" & date == '2022-01-10', out_flag_google := 1] #Dominican Republic
df_compiled[ihme_loc_id=="MKD" & date == '2022-01-19', out_flag_google := 1] #Macedonia
df_compiled[ihme_loc_id=="MKD" & date == '2022-01-23', out_flag_google := 1] #Macedonia
df_compiled[ihme_loc_id=="YEM" & date >= '2022-01-21' & date <= '2022-01-24', out_flag_google := 1] #Yemen
df_compiled[ihme_loc_id=="BFA" & date >= '2022-01-23' & date <= '2022-01-24', out_flag_google := 1] #burkina faso
df_compiled[ihme_loc_id=="GRC" & date >= '2022-01-22' & date <= '2022-01-24', out_flag_google := 1] #Greece
df_compiled[ihme_loc_id=="PRY" & date == '2021-12-08', out_flag_google := 1]# Paraguay
df_compiled[ihme_loc_id=="BHS" & date == '2021-12-27', out_flag_google := 1]# Bahamas
df_compiled[ihme_loc_id=="BHS" & date == '2022-01-03', out_flag_google := 1]# Bahamas
df_compiled[ihme_loc_id=="BHS" & date == '2022-01-10', out_flag_google := 1]# Bahamas
df_compiled[ihme_loc_id=="BRB" & date == '2021-12-01', out_flag_google := 1]# Barbados
df_compiled[ihme_loc_id=="BRB" & date == '2021-12-27', out_flag_google := 1]# Barbados
df_compiled[ihme_loc_id=="BRB" & date == '2022-01-21', out_flag_google := 1]# Barbados
df_compiled[ihme_loc_id=="JAM" & date == '2021-12-27', out_flag_google := 1]# Jamaica
df_compiled[ihme_loc_id=="TTO" & date == '2021-12-27', out_flag_google := 1]# Trinidad
df_compiled[ihme_loc_id=="PAN" & date == '2022-01-10', out_flag_google := 1]# Panama
df_compiled[ihme_loc_id=="TUR" & date == '2022-01-23', out_flag_google := 1]# Turkey
df_compiled[ihme_loc_id=="ARE" & date == '2022-01-14', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="ARE" & date == '2022-01-21', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="ARE" & date == '2022-01-07', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="PNG" & date == '2021-12-27', out_flag_google := 1]# papua new guinea
df_compiled[ihme_loc_id=="LKA" & date == '2022-01-14', out_flag_google := 1]# Sri Lanka
df_compiled[ihme_loc_id=="LKA" & date == '2022-01-17', out_flag_google := 1]# Sri Lanka
df_compiled[ihme_loc_id=="THA" & date == '2022-01-03', out_flag_google := 1]# Thailand
df_compiled[ihme_loc_id=="ZWE" & date == '2021-12-27', out_flag_google := 1]# zimbabwe
df_compiled[ihme_loc_id=="BEN" & date == '2022-01-10', out_flag_google := 1]# Benin
df_compiled[ihme_loc_id=="ZAF" & date == '2021-12-27', out_flag_google := 1]# south africa
df_compiled[ihme_loc_id=="SEN" & date == '2022-01-23', out_flag_google := 1]# senegal
df_compiled[ihme_loc_id=="TGO" & date == '2022-01-03', out_flag_google := 1]# Togo
df_compiled[ihme_loc_id=="SWE" & date >='2022-01-06' & date <= '2022-01-07', out_flag_google := 1]# Sweden

df_compiled[ihme_loc_id=="USA_556" & date == '2022-01-21', out_flag_google := 1]# North CArolina
df_compiled[ihme_loc_id=="USA_556" & date == '2022-01-22', out_flag_google := 1]# North CArolina
df_compiled[ihme_loc_id=="USA_563" & date == '2022-01-21', out_flag_google := 1]# South Carolina
df_compiled[ihme_loc_id=="USA_563" & date == '2022-01-22', out_flag_google := 1]# South Carolina
df_compiled[ihme_loc_id=="BRA_4750" & date == '2022-01-16', out_flag_google := 1]# Acre
df_compiled[ihme_loc_id=="BRA_4750" & date == '2022-01-23', out_flag_google := 1]# Acre
df_compiled[ihme_loc_id=="BRA_4770" & date == '2022-01-03', out_flag_google := 1]# Rondonia
df_compiled[ihme_loc_id=="BRA_4770" & date == '2022-01-23', out_flag_google := 1]# Rondonia
df_compiled[ihme_loc_id=="BRA_4770" & date == '2022-01-24', out_flag_google := 1]# Rondonia
df_compiled[ihme_loc_id=="BRA_4771" & date == '2022-01-20', out_flag_google := 1]# Roraima
df_compiled[ihme_loc_id=="BRA_4768" & date == '2022-01-20', out_flag_google := 1]# Rio de Janiero
df_compiled[ihme_loc_id=="BRA_4769" & date == '2022-01-06', out_flag_google := 1]# Rio grande do norte
df_compiled[ihme_loc_id=="IND_4853" & date == '2022-01-23', out_flag_google := 1]# himachal pradesh
df_compiled[ihme_loc_id=="IND_4857" & date == '2022-01-23', out_flag_google := 1]# kerala
df_compiled[ihme_loc_id=="IND_4869" & date == '2022-01-23', out_flag_google := 1]# sikkim
df_compiled[ihme_loc_id=="IND_4870" & date == '2022-01-23', out_flag_google := 1]# tamil nadu
df_compiled[ihme_loc_id=="IND_4870" & date >= '2022-01-14' & date <= '2022-01-16', out_flag_google := 1]# tamil nadu
df_compiled[ihme_loc_id=="IND_4874" & date == '2022-01-23', out_flag_google := 1]# uttarkhand
df_compiled[ihme_loc_id=="PAK_53618" & date >= '2022-01-22' & date <= '2022-01-23', out_flag_google := 1]# ICT
df_compiled[ihme_loc_id=="DEU_60389" & date == '2022-01-06', out_flag_google := 1]# saxony-anhalt
df_compiled[ihme_loc_id=="DEU_60378" & date == '2022-01-06', out_flag_google := 1]# bavaria
df_compiled[ihme_loc_id=="DEU_60377" & date == '2022-01-06', out_flag_google := 1]# baden-wurt
df_compiled[grepl('ESP', ihme_loc_id) & date=='2022-01-06', out_flag_google := 1] #spain subnats

# drop all of the holidays

df_compiled[date >= '2021-12-20' & date <= '2022-01-04',out_flag_google := 1]

# 2/7
df_compiled[ihme_loc_id=="KHM" & date=='2022-01-07', out_flag_google := 1] # Cambodia
df_compiled[ihme_loc_id=="KHM" & date=='2022-01-31', out_flag_google := 1] # Cambodia
df_compiled[ihme_loc_id=="MYS" & date=='2022-01-18', out_flag_google := 1] # Malaysia
df_compiled[ihme_loc_id=="FJI" & date=='2022-01-10', out_flag_google := 1] # Fiji
df_compiled[ihme_loc_id=="FJI" & date=='2022-01-09', out_flag_google := 1] # Fiji
df_compiled[ihme_loc_id=="UKR" & date=='2022-01-06', out_flag_google := 1] # Ukraine
df_compiled[ihme_loc_id=="UKR" & date=='2022-01-08', out_flag_google := 1] # Ukraine
df_compiled[ihme_loc_id=="JPN" & date=='2022-01-03', out_flag_google := 1] # Japan
df_compiled[ihme_loc_id=="KOR" & date=='2022-01-31', out_flag_google := 1] # Korea
df_compiled[ihme_loc_id=="KOR" & date=='2022-01-29', out_flag_google := 1] # Korea
df_compiled[ihme_loc_id=="EGY" & date=='2022-01-27', out_flag_google := 1] # Egypt
df_compiled[ihme_loc_id=="JOR" & date=='2022-01-27', out_flag_google := 1] # Jordan
df_compiled[ihme_loc_id=="JOR" & date=='2022-01-28', out_flag_google := 1] # Jordan
df_compiled[ihme_loc_id=="ARE" & date=='2022-01-28', out_flag_google := 1] # UAE
df_compiled[ihme_loc_id=="HTI" & date=='2022-01-31', out_flag_google := 1] # Haiti
df_compiled[ihme_loc_id=="TWN" & date=='2022-01-31', out_flag_google := 1] # Taiwan

df_compiled[ihme_loc_id=="USA_529" & date=='2022-01-29', out_flag_google := 1] # Connecticut
df_compiled[ihme_loc_id=="USA_529" & date=='2022-01-17', out_flag_google := 1] # Connecticut
df_compiled[ihme_loc_id=="USA_529" & date=='2022-01-07', out_flag_google := 1] # Connecticut
df_compiled[ihme_loc_id=="USA_530" & date=='2022-01-29', out_flag_google := 1] # Delaware
df_compiled[ihme_loc_id=="USA_530" & date=='2022-01-17', out_flag_google := 1] # Delaware
df_compiled[ihme_loc_id=="USA_530" & date=='2022-01-07', out_flag_google := 1] # Delaware
df_compiled[ihme_loc_id=="USA_542" & date=='2022-01-29', out_flag_google := 1] # Maine
df_compiled[ihme_loc_id=="USA_542" & date=='2022-01-17', out_flag_google := 1] # Maine
df_compiled[ihme_loc_id=="USA_542" & date=='2022-01-07', out_flag_google := 1] # Maine
df_compiled[ihme_loc_id=="USA_544" & date=='2022-01-29', out_flag_google := 1] # Massachusettes
df_compiled[ihme_loc_id=="USA_544" & date=='2022-01-17', out_flag_google := 1] # Massachusettes
df_compiled[ihme_loc_id=="USA_544" & date=='2022-01-07', out_flag_google := 1] # Massachusettes
df_compiled[ihme_loc_id=="USA_552" & date=='2022-01-29', out_flag_google := 1] # New Hampshire
df_compiled[ihme_loc_id=="USA_552" & date=='2022-01-17', out_flag_google := 1] # New Hampshire
df_compiled[ihme_loc_id=="USA_552" & date=='2022-01-07', out_flag_google := 1] # New Hampshire
df_compiled[ihme_loc_id=="USA_553" & date=='2022-01-29', out_flag_google := 1] #  New Jersey
df_compiled[ihme_loc_id=="USA_553" & date=='2022-01-17', out_flag_google := 1] #  New Jersey
df_compiled[ihme_loc_id=="USA_553" & date=='2022-01-07', out_flag_google := 1] #  New Jersey
df_compiled[ihme_loc_id=="USA_555" & date=='2022-01-29', out_flag_google := 1] # New york
df_compiled[ihme_loc_id=="USA_555" & date=='2022-01-17', out_flag_google := 1] # New york
df_compiled[ihme_loc_id=="USA_555" & date=='2022-01-07', out_flag_google := 1] # New york
df_compiled[ihme_loc_id=="USA_558" & date=='2022-01-17', out_flag_google := 1] #  Ohio
df_compiled[ihme_loc_id=="USA_562" & date=='2022-01-29', out_flag_google := 1] #  Rhode Island
df_compiled[ihme_loc_id=="USA_562" & date=='2022-01-17', out_flag_google := 1] #  Rhode Island
df_compiled[ihme_loc_id=="USA_562" & date=='2022-01-07', out_flag_google := 1] #  Rhode Island

df_compiled[ihme_loc_id=="IND_4849" & date >= '2022-01-22' & date <= '2022-01-23', out_flag_google := 1] # Delhi
df_compiled[ihme_loc_id=="IND_4849" & date >= '2022-01-15' & date <= '2022-01-16', out_flag_google := 1] # Delhi
df_compiled[ihme_loc_id=="IND_4849" & date >= '2022-01-08' & date <= '2022-01-09', out_flag_google := 1] # Delhi
df_compiled[ihme_loc_id=="IND_4849" & date == '2022-01-26', out_flag_google := 1] # Delhi
df_compiled[ihme_loc_id=="IND_4857" & date=='2022-01-23', out_flag_google := 1] # Kerala
df_compiled[ihme_loc_id=="IND_4857" & date=='2022-01-30', out_flag_google := 1] # Kerala

# 2/14
df_compiled[ihme_loc_id=="TWN" & date >= '2022-02-01' & date <= '2022-02-04', out_flag_google := 1] #Taiwan
df_compiled[ihme_loc_id=="THA" & date >= '2022-02-05' & date <= '2022-02-06', out_flag_google := 1] #thailand
df_compiled[ihme_loc_id=="MNG" & date >= '2022-02-02' & date <= '2022-02-04', out_flag_google := 1] #Mongolia
df_compiled[ihme_loc_id=="KOR" & date >= '2022-02-01' & date <= '2022-02-02', out_flag_google := 1] #Korea
df_compiled[ihme_loc_id=="SGP" & date >= '2022-02-01' & date <= '2022-02-02', out_flag_google := 1] #Singapore
df_compiled[ihme_loc_id=="NZL" & date=='2022-02-07', out_flag_google := 1] # new zealand
df_compiled[ihme_loc_id=="HTI" & date=='2022-02-07', out_flag_google := 1] # haiti
df_compiled[ihme_loc_id=="ARE" & date=='2022-02-04', out_flag_google := 1] # UAE
df_compiled[ihme_loc_id=="NPL" & date=='2022-02-04', out_flag_google := 1] # nepal
df_compiled[ihme_loc_id=="MUS" & date >= '2022-02-01' & date <= '2022-02-03', out_flag_google := 1] #mauritius
df_compiled[ihme_loc_id=="ZWE" & date >= '2022-02-05' & date <= '2022-02-06', out_flag_google := 1] #zimbabwe

df_compiled[ihme_loc_id=="USA_526" & date >= '2022-02-03' & date <= '2022-02-04', out_flag_google := 1] # Arkanas
df_compiled[ihme_loc_id=="USA_537" & date >= '2022-02-02' & date <= '2022-02-04', out_flag_google := 1] # Indiana
df_compiled[ihme_loc_id=="USA_540" & date >= '2022-02-03' & date <= '2022-02-04', out_flag_google := 1] # Kentucky
df_compiled[ihme_loc_id=="USA_548" & date >= '2022-02-02' & date <= '2022-02-04', out_flag_google := 1] # Missouri
df_compiled[ihme_loc_id=="USA_558" & date >= '2022-02-03' & date <= '2022-02-04', out_flag_google := 1] # Ohio
df_compiled[ihme_loc_id=="USA_559" & date >= '2022-02-02' & date <= '2022-02-04', out_flag_google := 1] # Oklahoma
df_compiled[ihme_loc_id=="USA_566" & date >= '2022-02-03' & date <= '2022-02-04', out_flag_google := 1] # Texas


df_compiled[grepl('MEX', ihme_loc_id) & date >= '2021-12-18' & date <= '2021-12-19', out_flag_google := 1] #mexico subnats
df_compiled[ihme_loc_id=="MEX_4648" & date=='2022-02-07', out_flag_google := 1]# Colima
df_compiled[ihme_loc_id=="MEX_4661" & date=='2022-02-07', out_flag_google := 1]# nuevo leon
df_compiled[ihme_loc_id=="MEX_4669" & date=='2022-02-07', out_flag_google := 1]# tabasco

df_compiled[grepl('IND', ihme_loc_id) & date == '2022-01-26', out_flag_google := 1] # indian subnats - india republic day
df_compiled[ihme_loc_id=="IND_4856" & date>='2022-01-15' & date <= '2022-01-16', out_flag_google := 1]# karnataka
df_compiled[ihme_loc_id=="IND_4857" & date=='2022-02-06', out_flag_google := 1]# kerala
df_compiled[ihme_loc_id=="IND_4868" & date=='2022-01-16', out_flag_google := 1]# rajasthan
df_compiled[ihme_loc_id=="IND_4868" & date=='2022-01-23', out_flag_google := 1]# rajasthan
df_compiled[ihme_loc_id=="IND_4871" & date=='2022-01-15', out_flag_google := 1]# telangana
df_compiled[ihme_loc_id=="IND_4872" & date=='2022-01-14', out_flag_google := 1]# tripura

df_compiled[ihme_loc_id=="CAN_43861" & date=='2022-02-06', out_flag_google := 1]# new brunswick
df_compiled[ihme_loc_id=="CAN_43861" & date=='2022-02-04', out_flag_google := 1]# new brunswick
df_compiled[ihme_loc_id=="CAN_43861" & date=='2022-01-29', out_flag_google := 1]# new brunswick


# 3/3
df_compiled[ihme_loc_id=="EST" & date >= '2022-02-24' & date <= '2022-02-25', out_flag_google := 1] # Estonia
df_compiled[ihme_loc_id=="LTU" & date=='2022-02-16', out_flag_google := 1]# Lithuania
df_compiled[ihme_loc_id=="RUS" & date=='2022-02-23', out_flag_google := 1]# Russia
df_compiled[ihme_loc_id=="JPN" & date=='2022-02-23', out_flag_google := 1]# Japan
df_compiled[ihme_loc_id=="JPN" & date=='2022-02-11', out_flag_google := 1]# Japan
df_compiled[ihme_loc_id=="NLD" & date=='2022-02-18', out_flag_google := 1]# netherlands
df_compiled[ihme_loc_id=="NLD" & date >= '2022-02-26' & date <= '2022-02-27', out_flag_google := 1] # netherlands
df_compiled[ihme_loc_id=="GBR_4749" & date=='2022-02-18', out_flag_google := 1]# england
df_compiled[ihme_loc_id=="ARE" & date=='2022-02-11', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="ARE" & date=='2022-02-18', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="ARE" & date=='2022-02-25', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="IDN" & date >= '2022-02-26' & date <= '2022-02-27', out_flag_google := 1] # indonesia
df_compiled[ihme_loc_id=="LAO" & date=='2022-02-20', out_flag_google := 1]# Lao
df_compiled[ihme_loc_id=="THA" & date=='2022-02-16', out_flag_google := 1]# thailand
df_compiled[ihme_loc_id=="AGO" & date=='2022-02-04', out_flag_google := 1]# angola
df_compiled[ihme_loc_id=="ZMB" & date >= '2022-02-25' & date <= '2022-02-27', out_flag_google := 1] #zambia
df_compiled[ihme_loc_id=="BWA" & date >= '2022-02-26' & date <= '2022-02-27', out_flag_google := 1] #botswana
df_compiled[ihme_loc_id=="NAM" & date >= '2022-02-25' & date <= '2022-02-26', out_flag_google := 1] # namibia
df_compiled[ihme_loc_id=="ZAF" & date >= '2022-02-26' & date <= '2022-02-27', out_flag_google := 1] # south africa
df_compiled[ihme_loc_id=="ZWE" & date=='2022-02-21', out_flag_google := 1]# zimbabwe
df_compiled[ihme_loc_id=="ZWE" & date=='2022-02-26', out_flag_google := 1]# zimbabwe
df_compiled[ihme_loc_id=="ZWE" & date=='2022-02-27', out_flag_google := 1]# zimbabwe

df_compiled[grepl('CAN', ihme_loc_id) & date == '2022-02-21', out_flag_google := 1]# Canada subnats - family day
df_compiled[ihme_loc_id=="USA_526" & date=='2022-02-04', out_flag_google := 1]# Arkanas
df_compiled[ihme_loc_id=="USA_526" & date=='2022-02-24', out_flag_google := 1]# Arkanas
df_compiled[ihme_loc_id=="USA_529" & date=='2022-02-25', out_flag_google := 1]# Connecticut
df_compiled[ihme_loc_id=="USA_539" & date=='2022-02-02', out_flag_google := 1]# Kansas
df_compiled[ihme_loc_id=="USA_539" & date=='2022-02-17', out_flag_google := 1]# Kansas
df_compiled[ihme_loc_id=="USA_542" & date=='2022-02-04', out_flag_google := 1]# Maine
df_compiled[ihme_loc_id=="USA_542" & date=='2022-02-25', out_flag_google := 1]# Maine
df_compiled[ihme_loc_id=="USA_544" & date=='2022-02-04', out_flag_google := 1]# Mass
df_compiled[ihme_loc_id=="USA_544" & date=='2022-02-25', out_flag_google := 1]# Mass
df_compiled[ihme_loc_id=="USA_548" & date=='2022-02-17', out_flag_google := 1]# Missouri
df_compiled[ihme_loc_id=="USA_548" & date=='2022-02-24', out_flag_google := 1]# Mssouri
df_compiled[ihme_loc_id=="USA_549" & date=='2022-02-21', out_flag_google := 1]# Montana
df_compiled[ihme_loc_id=="USA_552" & date=='2022-02-04', out_flag_google := 1]# New Hampshire
df_compiled[ihme_loc_id=="USA_552" & date=='2022-02-25', out_flag_google := 1]# New Hampshire
df_compiled[ihme_loc_id=="USA_559" & date=='2022-02-23', out_flag_google := 1]# Oklahoma
df_compiled[ihme_loc_id=="USA_559" & date=='2022-02-24', out_flag_google := 1]# Oklahoma
df_compiled[ihme_loc_id=="USA_559" & date=='2022-02-25', out_flag_google := 1]# Oklahoma
df_compiled[ihme_loc_id=="USA_562" & date=='2022-02-25', out_flag_google := 1]# Rhode Island
df_compiled[ihme_loc_id=="USA_564" & date=='2022-02-21', out_flag_google := 1]# South Dakota
df_compiled[ihme_loc_id=="USA_564" & date=='2022-02-22', out_flag_google := 1]# South Dakota
df_compiled[ihme_loc_id=="USA_566" & date=='2022-02-24', out_flag_google := 1]# Texas
df_compiled[ihme_loc_id=="USA_3539" & date=='2022-02-21', out_flag_google := 1]# spokane county
df_compiled[ihme_loc_id=="USA_572" & date=='2022-02-22', out_flag_google := 1]# wisconsin
df_compiled[grepl('MEX', ihme_loc_id) & date == '2022-02-07', out_flag_google := 1]# mexico subnats - constitution day

#3/7
df_compiled[ihme_loc_id=="TWN" & date=='2022-02-05', out_flag_google := 1] # name Taiwan
df_compiled[ihme_loc_id=="MNG" & date=='2022-02-01', out_flag_google := 1] # name Mongolia
df_compiled[ihme_loc_id=="AUS" & date=='2022-01-26', out_flag_google := 1] # name Australia
df_compiled[ihme_loc_id=="USA_540" & date=='2022-01-17', out_flag_google := 1] # name Kentucky
df_compiled[ihme_loc_id=="IND_4873" & date=='2022-01-26', out_flag_google := 1] # name Uttar Pradesh

df_compiled[ihme_loc_id=="MNG" & date >= '2022-02-05' & date <= '2022-02-06', out_flag_google := 1] # Taiwan
df_compiled[ihme_loc_id=="NLD" & date >= '2022-01-10' & date <= '2022-01-14', out_flag_google := 1] # Netherlands
df_compiled[ihme_loc_id=="NLD" & date >= '2022-01-03' & date <= '2022-01-08', out_flag_google := 1] # Netherlands
df_compiled[ihme_loc_id=="USA_523" & date >= '2022-01-16' & date <= '2022-01-17', out_flag_google := 1] # Alabama
df_compiled[ihme_loc_id=="USA_540" & date >= '2022-01-06' & date <= '2022-01-07', out_flag_google := 1] # Kentucky


#4/1/2022
# Days that show up weird on the plots bc they go over 100%
df_compiled[ihme_loc_id=="MNG" & date >= '2022-03-14' & date <= '2022-03-18', out_flag_google := 1] #Mongolia
df_compiled[ihme_loc_id=="MNG" & date >= '2022-03-21' & date <= '2022-03-23', out_flag_google := 1] #Mongolia
df_compiled[ihme_loc_id=="LBY" & date=='2022-02-25', out_flag_google := 1]#Libya
df_compiled[ihme_loc_id=="LBY" & date >= '2022-02-22' & date <= '2022-02-25', out_flag_google := 1] # Libya
df_compiled[ihme_loc_id=="LBY" & date >= '2022-03-21' & date <= '2022-03-23', out_flag_google := 1] # Libya
df_compiled[ihme_loc_id=="LBY" & date >= '2022-03-18' & date <= '2022-03-19', out_flag_google := 1] # Libya
df_compiled[ihme_loc_id=="ZWE" & date=='2022-03-06', out_flag_google := 1]#Zimbabwe
df_compiled[ihme_loc_id=="ZWE" & date=='2022-03-13', out_flag_google := 1]#Zimbabwe
df_compiled[ihme_loc_id=="ZWE" & date=='2022-03-20', out_flag_google := 1]#Zimbabwe
df_compiled[ihme_loc_id=="ZWE" & date=='2022-03-27', out_flag_google := 1]#Zimbabwe
df_compiled[ihme_loc_id=="BFA" & date=='2022-03-08', out_flag_google := 1]# Burkina Faso
df_compiled[ihme_loc_id=="BFA" & date=='2022-03-20', out_flag_google := 1]# Burkina Faso
df_compiled[ihme_loc_id=="BFA" & date=='2022-03-27', out_flag_google := 1]# Burkina Faso
df_compiled[ihme_loc_id=="NER" & date=='2022-03-27', out_flag_google := 1]# Niger

df_compiled[ihme_loc_id=="IDN" & date >= '2022-03-26' & date <= '2022-03-27', out_flag_google := 1] # Indonesia
df_compiled[ihme_loc_id=="IDN" & date=='2022-02-28', out_flag_google := 1]# Indonesia
df_compiled[ihme_loc_id=="IDN" & date=='2022-03-03', out_flag_google := 1]# Indonesia
df_compiled[ihme_loc_id=="LAO" & date >= '2022-03-23' & date <= '2022-03-24', out_flag_google := 1]# Lao
df_compiled[ihme_loc_id=="MYS" & date >= '2022-03-26' & date <= '2022-03-27', out_flag_google := 1]# Malaysia
df_compiled[ihme_loc_id=="MYS" & date >= '2022-01-28' & date <= '2022-01-30', out_flag_google := 1]# Malaysia
df_compiled[ihme_loc_id=="MYS" & date >= '2022-02-01' & date <= '2022-02-02', out_flag_google := 1]# Malaysia
df_compiled[ihme_loc_id=="KAZ" & date >= '2022-03-21' & date <= '2022-03-23', out_flag_google := 1]# Kazakhstan
df_compiled[ihme_loc_id=="KAZ" & date >= '2022-03-07' & date <= '2022-03-08', out_flag_google := 1]# Kazakhstan
df_compiled[ihme_loc_id=="KAZ" & date=='2022-03-05', out_flag_google := 1]# Kazakhstan
df_compiled[ihme_loc_id=="TJK" & date >= '2022-03-21' & date <= '2022-03-26', out_flag_google := 1]# Tajikistan
df_compiled[ihme_loc_id=="HUN" & date >= '2022-03-14' & date <= '2022-03-15', out_flag_google := 1]# Hungary
df_compiled[ihme_loc_id=="HUN" & date=='2022-03-26', out_flag_google := 1]# Hungary
df_compiled[ihme_loc_id=="NLD" & date=='2022-03-27', out_flag_google := 1]# Netherlands
df_compiled[ihme_loc_id=="HTI" & date=='2022-03-27', out_flag_google := 1]# Haiti
df_compiled[ihme_loc_id=="HTI" & date>='2022-02-28' & date <= '2022-03-02', out_flag_google := 1]# Haiti
df_compiled[ihme_loc_id=="JAM" & date=='2022-03-02', out_flag_google := 1]# Jamaica
df_compiled[ihme_loc_id=="ARE" & date=='2022-03-04', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="ARE" & date=='2022-03-11', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="ARE" & date=='2022-03-18', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="ARE" & date=='2022-03-25', out_flag_google := 1]# UAE
df_compiled[ihme_loc_id=="ZMB" & date>='2022-02-25' & date <= '2022-02-26', out_flag_google := 1]# Zambia
df_compiled[ihme_loc_id=="ZMB" & date=='2022-03-18', out_flag_google := 1]# Zambia
df_compiled[ihme_loc_id=="BWA" & date>='2022-03-26' & date <= '2022-03-27', out_flag_google := 1]# Botswana
df_compiled[ihme_loc_id=="NAM" & date=='2022-03-21', out_flag_google := 1]# Namibia
df_compiled[ihme_loc_id=="NAM" & date=='2022-03-26', out_flag_google := 1]# Namibia
df_compiled[ihme_loc_id=="ZAF" & date=='2022-03-26', out_flag_google := 1]# South Africa
df_compiled[ihme_loc_id=="ZAF" & date>='2022-03-26' & date <= '2022-03-27', out_flag_google := 1]# South Africa
df_compiled[ihme_loc_id=="BEN" & date=='2022-03-27', out_flag_google := 1]# Benin
df_compiled[ihme_loc_id=="BEN" & date=='2022-03-20', out_flag_google := 1]# Benin
df_compiled[ihme_loc_id=="BEN" & date>='2022-03-13' & date <= '2022-03-14', out_flag_google := 1]# Benin
df_compiled[ihme_loc_id=="NGA" & date=='2022-03-13', out_flag_google := 1]# Nigeria
df_compiled[ihme_loc_id=="NGA" & date=='2022-03-20', out_flag_google := 1]# Nigeria
df_compiled[ihme_loc_id=="PAN" & date=='2022-03-27', out_flag_google := 1]# Panama
df_compiled[ihme_loc_id=="PAN" & date=='2022-02-28', out_flag_google := 1]# Panama
df_compiled[ihme_loc_id=="PAN" & date=='2022-03-01', out_flag_google := 1]# Panama
df_compiled[ihme_loc_id=="VEN" & date=='2022-02-28', out_flag_google := 1]# Venezuela
df_compiled[ihme_loc_id=="VEN" & date=='2022-03-01', out_flag_google := 1]# Venezuela
df_compiled[ihme_loc_id=="ARG" & date=='2022-02-28', out_flag_google := 1]# Argentina
df_compiled[ihme_loc_id=="ARG" & date=='2022-03-01', out_flag_google := 1]# Argentina
df_compiled[ihme_loc_id=="ARG" & date=='2022-03-24', out_flag_google := 1]# Argentina

df_compiled[ihme_loc_id=="CHN_354" & date>='2022-02-01' & date <= '2022-02-03', out_flag_google := 1]# Hong Kong
df_compiled[ihme_loc_id=="PAK_53618" & date>='2022-03-21' & date <= '2022-03-25', out_flag_google := 1]#ICT

#USA 
df_compiled[ihme_loc_id=="USA_531" & date=='2022-03-27', out_flag_google := 1] # DC
df_compiled[ihme_loc_id=="USA_541" & date=='2022-03-22', out_flag_google := 1] # LOUISIANA
df_compiled[ihme_loc_id=="USA_547" & date=='2022-03-22', out_flag_google := 1] # MISSISSIPPI

# Brazil subnats 
df_compiled[grepl('BRA', ihme_loc_id) & date >= '2022-02-28' & date <= '2022-03-01', out_flag_google := 1] # ash wednesday 

# Mexico Subnats
df_compiled[grepl('MEX', ihme_loc_id) & date == '2022-03-21', out_flag_google := 1] # benito juarez bday

# India subnats
df_compiled[grepl('IND', ihme_loc_id) & date >= '2022-03-18' & date <= '2022-03-19', out_flag_google := 1] #holi

# 5/2

# drop easter/passover/ramadan

orthodox_countries <- c('ARM', 'BGR', 'GEO', 'GRC', 'ROU', 'RUS', 'SRB', 'MDA', 'BLR', 'EGY','MKD')

df_compiled[!(ihme_loc_id %in% orthodox_countries) & date >='2022-04-14' & date <= '2022-04-18', out_flag_google := 1]
df_compiled[(ihme_loc_id %in% orthodox_countries) & date >='2022-04-22' & date <= '2022-04-25', out_flag_google := 1]

df_compiled[ihme_loc_id=="CZE" & date=='2022-04-23', out_flag_google := 1] # CZE
df_compiled[ihme_loc_id=="EGY" & date=='2022-04-02', out_flag_google := 1] # egypt
df_compiled[ihme_loc_id=="ISR" & date>='2022-04-21' & date<='2022-04-22', out_flag_google := 1] # israel
df_compiled[ihme_loc_id=="NOR" & date>='2022-04-23' & date<='2022-04-24', out_flag_google := 1] # norway
df_compiled[ihme_loc_id=="CHL" & date>='2022-04-23' & date<='2022-04-24', out_flag_google := 1] # chile
df_compiled[ihme_loc_id=="CRI" & date>='2022-04-11' & date<='2022-04-13', out_flag_google := 1] # costa rica
df_compiled[ihme_loc_id=="VEN" & date=='2022-04-19', out_flag_google := 1] # venezuela
df_compiled[ihme_loc_id=="BHR" & date=='2022-04-25', out_flag_google := 1] # bahrain
df_compiled[ihme_loc_id=="MOZ" & date=='2022-04-07', out_flag_google := 1] # mozambique

df_compiled[grepl('BRA', ihme_loc_id) & date == '2022-04-21', out_flag_google := 1] 
df_compiled[grepl('BRA', ihme_loc_id) & date == '2022-04-25', out_flag_google := 1] 
df_compiled[grepl('ITA', ihme_loc_id) & date == '2022-04-25', out_flag_google := 1] 
df_compiled[grepl('CAN', ihme_loc_id) & date == '2022-04-24', out_flag_google := 1] 
df_compiled[grepl('DEU', ihme_loc_id) & date == '2022-04-23', out_flag_google := 1] 
df_compiled[grepl('DEU', ihme_loc_id) & date == '2022-04-24', out_flag_google := 1] 

# 6/6
df_compiled[ihme_loc_id=="LKA" & date=='2022-05-06', out_flag_google := 1] # sri lanka
df_compiled[ihme_loc_id=="LKA" & date>='2022-05-10' & date<='2022-05-13', out_flag_google := 1] # sri lanka
df_compiled[ihme_loc_id=="LKA" & date>='2022-05-15' & date<='2022-05-16', out_flag_google := 1] # sri lanka
df_compiled[ihme_loc_id=="SVN" & date>='2022-04-27' & date<='2022-04-29', out_flag_google := 1] # slovenia
df_compiled[ihme_loc_id=="SVN" & date>='2022-05-01' & date<='2022-05-02', out_flag_google := 1] # slovenia
df_compiled[ihme_loc_id=="SVN" & date=='2022-05-29', out_flag_google := 1] # slovenia
df_compiled[ihme_loc_id=="NPL" & date>='2022-05-12' & date<='2022-05-16', out_flag_google := 1] # nepal

# Acension day
ascension_locs <- c('AUT', 'BEL', 'FIN', 'FRA', 'NOR', 'LUX', 'CHE')

df_compiled[ihme_loc_id %in% ascension_locs & date>='2022-05-26' & date<='2022-05-27', out_flag_google := 1] # drop ascension day
df_compiled[grepl('DEU', ihme_loc_id) & date>='2022-05-26' & date<='2022-05-27', out_flag_google := 1] #drop ascension day for germany subnats

# Eid 
eid_locs <- c('EGY', 'LBY', 'YEM', 'AFG', 'IRQ', 'JOR', 'KWT', 'OMN', 'QAT', 'SAU', 'TUR')
df_compiled[ihme_loc_id %in% eid_locs & date>='2022-05-01' & date<='2022-05-05', out_flag_google := 1] # drop eid
df_compiled[grepl('PAK', ihme_loc_id) & date>='2022-05-01' & date<='2022-05-05', out_flag_google := 1] #drop eid for pakistan subnats

# victoria day (canada)
df_compiled[grepl('CAN', ihme_loc_id) & date=='2022-05-23', out_flag_google := 1] #drop victoria day for canadian subnats

#subnats
df_compiled[ihme_loc_id=="BRA_4766" & date=='2022-05-25', out_flag_google := 1] #pernambuco
df_compiled[ihme_loc_id=="BRA_4766" & date=='2022-05-28', out_flag_google := 1] #pernambuco
df_compiled[ihme_loc_id=="BRA_4766" & date=='2022-05-29', out_flag_google := 1] #pernambuco
df_compiled[ihme_loc_id=="USA_524" & date=='2022-05-28', out_flag_google := 1] #alaska
df_compiled[ihme_loc_id=="USA_524" & date=='2022-05-29', out_flag_google := 1] #alaska
df_compiled[ihme_loc_id=="USA_557" & date=='2022-04-14', out_flag_google := 1] #north dakota
df_compiled[ihme_loc_id=="USA_557" & date=='2022-04-17', out_flag_google := 1] #north dakota

# 7/11
df_compiled[ihme_loc_id=="" & date=='', out_flag_google := 1]
df_compiled[ihme_loc_id=="" & date>='' & date<='', out_flag_google := 1] 

df_compiled[ihme_loc_id=="IDN" & date>='2022-07-02' & date<='2022-07-03', out_flag_google := 1] # Indonesia
df_compiled[ihme_loc_id=="SWE" & date=='2022-07-04', out_flag_google := 1] # Sweden
df_compiled[ihme_loc_id=="LUX" & date>='2022-07-02' & date<='2022-07-03', out_flag_google := 1] # Luxembourg
df_compiled[ihme_loc_id=="LUX" & date=='2022-06-06', out_flag_google := 1]# Luxembourg
df_compiled[ihme_loc_id=="LUX" & date=='2022-05-26', out_flag_google := 1]# Luxembourg
df_compiled[ihme_loc_id=="LUX" & date=='2022-05-09', out_flag_google := 1]# Luxembourg
df_compiled[ihme_loc_id=="TTO" & date>='2022-06-28' & date<='2022-06-29', out_flag_google := 1]  # Trinidad and Tobago
df_compiled[ihme_loc_id=="TTO" & date>='2022-07-02' & date<='2022-07-03', out_flag_google := 1]  # Trinidad and Tobago
df_compiled[ihme_loc_id=="TTO" & date=='2022-06-20', out_flag_google := 1] # Trinidad and Tobago
df_compiled[ihme_loc_id=="TTO" & date=='2022-06-16', out_flag_google := 1] # Trinidad and Tobago
df_compiled[ihme_loc_id=="TTO" & date=='2022-05-30', out_flag_google := 1] # Trinidad and Tobago
df_compiled[ihme_loc_id=="ECU" & date>='2022-06-21' & date<='2022-06-24', out_flag_google := 1] # Ecuador
df_compiled[ihme_loc_id=="ECU" & date>='2022-07-01' & date<='2022-07-04', out_flag_google := 1] # Ecuador
df_compiled[ihme_loc_id=="AGO" & date=='2022-06-25', out_flag_google := 1]# Angola
df_compiled[ihme_loc_id=="AGO" & date=='2022-07-02', out_flag_google := 1]# Angola
df_compiled[ihme_loc_id=="AGO" & date=='2022-06-30', out_flag_google := 1]# Angola
df_compiled[ihme_loc_id=="AGO" & date=='2022-06-18', out_flag_google := 1]# Angola
df_compiled[ihme_loc_id=="AGO" & date=='2022-06-11', out_flag_google := 1]# Angola
df_compiled[ihme_loc_id=="AGO" & date=='2022-06-04', out_flag_google := 1]# Angola
df_compiled[ihme_loc_id=="GAB" & date>='2022-07-02' & date<='2022-07-03', out_flag_google := 1]  # Gabon
df_compiled[ihme_loc_id=="GAB" & date=='2022-06-06', out_flag_google := 1] # Gabon
df_compiled[ihme_loc_id=="GAB" & date=='2022-05-26', out_flag_google := 1] # Gabon
df_compiled[ihme_loc_id=="GAB" & date=='2022-04-18', out_flag_google := 1] # Gabon
df_compiled[ihme_loc_id=="SEN" & date>='2022-07-03' & date<='2022-07-04', out_flag_google := 1] # Senegal
df_compiled[ihme_loc_id=="SEN" & date=='2022-06-06', out_flag_google := 1] # Senegal
df_compiled[ihme_loc_id=="SEN" & date=='2022-05-26', out_flag_google := 1]# Senegal
df_compiled[ihme_loc_id=="SEN" & date>='2022-05-01' & date<='2022-05-04', out_flag_google := 1] # Senegal

# midsummer
df_compiled[ihme_loc_id=="EST" & date>='2022-06-23' & date<='2022-06-24', out_flag_google := 1]# Estonia
df_compiled[ihme_loc_id=="LVA" & date>='2022-06-23' & date<='2022-06-24', out_flag_google := 1]# Latvia
df_compiled[ihme_loc_id=="LTU" & date=='2022-06-24', out_flag_google := 1]# Lithuania
df_compiled[ihme_loc_id=="FIN" & date>='2022-06-24' & date <='2022-06-25', out_flag_google := 1] # Finland
df_compiled[ihme_loc_id=="SWE" & date>='2022-06-24' & date <='2022-06-25', out_flag_google := 1]# Sweden
df_compiled[ihme_loc_id=="LUX" & date>='2022-06-23' & date <='2022-06-24', out_flag_google := 1]# Luxembourg

# subnats

df_compiled[grepl('USA', ihme_loc_id) & date=='2022-07-04', out_flag_google := 1]# US subnats - independence day
df_compiled[ihme_loc_id=="USA_557" & date=='2022-05-30', out_flag_google := 1]# North Dakota
df_compiled[grepl('CAN', ihme_loc_id) & date=='2022-07-01', out_flag_google := 1]# Canada subnats - independence day
df_compiled[ihme_loc_id=="BRA_4769" & date=='2022-07-03', out_flag_google := 1]# Rio Grande do Norte (brazil)
df_compiled[ihme_loc_id=="BRA_4769" & date=='2022-06-16', out_flag_google := 1]# Rio Grande do Norte (brazil)
df_compiled[ihme_loc_id=="IND_4844" & date>='2022-06-18' & date<='2022-06-20', out_flag_google := 1]# Bihar
df_compiled[ihme_loc_id=="IND_4844" & date>='2022-06-28' & date<='2022-06-29', out_flag_google := 1]# Bihar
df_compiled[ihme_loc_id=="IND_4846" & date=='2022-07-02', out_flag_google := 1]# Chattisgarh
df_compiled[ihme_loc_id=="IND_4861" & date=='2022-06-18', out_flag_google := 1] # Manipur
df_compiled[ihme_loc_id=="IND_4868" & date>='2022-06-28' & date<='2022-07-03', out_flag_google := 1]# Rajasthan
#df_compiled[grepl('DEU', ihme_loc_id) & date=='2022-07-02', out_flag_google := 1] # germany subnats
#df_compiled[grepl('DEU', ihme_loc_id) & date=='2022-07-03', out_flag_google := 1] # germany subnats


# 8/1
df_compiled[ihme_loc_id=="LKA" & date>='2022-07-09' & date<='2022-07-10', out_flag_google := 1] # sri lanka
df_compiled[ihme_loc_id=="LKA" & date>='2022-07-13' & date<='2022-07-15', out_flag_google := 1] # sri lanka
df_compiled[ihme_loc_id=="PNG" & date>='2022-07-23' & date<='2022-07-25', out_flag_google := 1]# papua new guinea
df_compiled[ihme_loc_id=="MNG" & date>='2022-07-11' & date<='2022-07-16', out_flag_google := 1]# mongolia
df_compiled[ihme_loc_id=="HRV" & date=='2022-07-24', out_flag_google := 1]# Croatia
df_compiled[ihme_loc_id=="HRV" & date=='2022-07-23', out_flag_google := 1]# Croatia
df_compiled[ihme_loc_id=="HRV" & date=='2022-07-17', out_flag_google := 1]# Croatia
df_compiled[ihme_loc_id=="CZE" & date>='2022-07-04' & date<='2022-07-08', out_flag_google := 1]# Czechia
df_compiled[ihme_loc_id=="CZE" & date=='2022-07-01', out_flag_google := 1]#Czechia
df_compiled[ihme_loc_id=="KOR" & date>='2022-07-23' & date<='2022-07-24', out_flag_google := 1]# south korea
df_compiled[ihme_loc_id=="BEL" & date>='2022-07-21' & date<='2022-07-22', out_flag_google := 1]# Belgium
df_compiled[ihme_loc_id=="BEL" & date=='2022-07-19', out_flag_google := 1]#Belgium
df_compiled[ihme_loc_id=="BEL" & date=='2022-07-25', out_flag_google := 1]#Belgium
df_compiled[ihme_loc_id=="DNK" & date=='2022-07-25', out_flag_google := 1]#Denmark
df_compiled[ihme_loc_id=="NLD" & date=='2022-07-25', out_flag_google := 1]# Netherlands
df_compiled[ihme_loc_id=="SWE" & date=='2022-07-25', out_flag_google := 1]# Sweden
df_compiled[ihme_loc_id=="NOR" & date=='2022-07-25', out_flag_google := 1]# norway
df_compiled[ihme_loc_id=="CHE" & date=='2022-07-25', out_flag_google := 1]# switzerland
df_compiled[ihme_loc_id=="CRI" & date=='2022-07-25', out_flag_google := 1]# costa rica
df_compiled[ihme_loc_id=="BHR" & date>='2022-07-09' & date<='2022-07-11', out_flag_google := 1]# Bahrain
df_compiled[ihme_loc_id=="EGY" & date>='2022-07-09' & date<='2022-07-14', out_flag_google := 1]## egypt
df_compiled[ihme_loc_id=="EGY" & date=='2022-06-30', out_flag_google := 1]#egypt
df_compiled[ihme_loc_id=="IRQ" & date>='2022-07-09' & date<='2022-07-14', out_flag_google := 1]## iraq
df_compiled[ihme_loc_id=="JOR" & date>='2022-07-09' & date<='2022-07-14', out_flag_google := 1]## jordan
df_compiled[ihme_loc_id=="LBY" & date>='2022-07-09' & date<='2022-07-14', out_flag_google := 1]## libyra
df_compiled[ihme_loc_id=="MAR" & date>='2022-07-09' & date<='2022-07-13', out_flag_google := 1]## morocco
df_compiled[ihme_loc_id=="OMN" & date>='2022-07-09' & date<='2022-07-12', out_flag_google := 1]## oman
df_compiled[ihme_loc_id=="QAT" & date>='2022-07-09' & date<='2022-07-15', out_flag_google := 1]## qatar
df_compiled[ihme_loc_id=="TUR" & date>='2022-07-09' & date<='2022-07-15', out_flag_google := 1]## turkey
df_compiled[ihme_loc_id=="YEM" & date>='2022-07-09' & date<='2022-07-15', out_flag_google := 1]## yemen
df_compiled[ihme_loc_id=="YEM" & date=='2022-07-24', out_flag_google := 1]## yemen
df_compiled[ihme_loc_id=="AFG" & date>='2022-07-09' & date<='2022-07-12', out_flag_google := 1]## afghanistan
df_compiled[ihme_loc_id=="BGD" & date>='2022-07-08' & date<='2022-07-14', out_flag_google := 1]## bangladesh
df_compiled[ihme_loc_id=="MOZ" & date=='2022-06-25', out_flag_google := 1]## mozambique
df_compiled[ihme_loc_id=="MOZ" & date=='2022-07-14', out_flag_google := 1]## mozambique
df_compiled[ihme_loc_id=="MOZ" & date=='2022-07-24', out_flag_google := 1]## mozambique
df_compiled[ihme_loc_id=="TZA" & date=='2022-07-07', out_flag_google := 1]## tanzania
df_compiled[ihme_loc_id=="TZA" & date=='2022-07-24', out_flag_google := 1]## tanzania
df_compiled[ihme_loc_id=="CMR" & date>='2022-07-24' & date<='2022-07-25', out_flag_google := 1]## cameroon
df_compiled[ihme_loc_id=="MLI" & date=='2022-07-09', out_flag_google := 1]## mali
df_compiled[ihme_loc_id=="MLI" & date=='2022-07-10', out_flag_google := 1]## mali
df_compiled[ihme_loc_id=="MLI" & date=='2022-07-11', out_flag_google := 1]## mali
df_compiled[ihme_loc_id=="MLI" & date=='2022-07-24', out_flag_google := 1]## mali
df_compiled[ihme_loc_id=="SEN" & date>='2022-07-10' & date<='2022-07-16', out_flag_google := 1]## senegal
df_compiled[ihme_loc_id=="SEN" & date>='2022-07-22' & date<='2022-07-24', out_flag_google := 1]## senegal
df_compiled[ihme_loc_id=="SEN" & date=='2022-07-20', out_flag_google := 1]## senegal

df_compiled[ihme_loc_id=="CHN_518" & date>='2022-07-27' & date<='2022-07-28', out_flag_google := 1]## tibet
df_compiled[ihme_loc_id=="IND_4869" & date>='2022-07-23' & date<='2022-07-25', out_flag_google := 1]## sikkim
df_compiled[ihme_loc_id=="IND_4871" & date>='2022-07-09' & date<='2022-07-14', out_flag_google := 1]## telangana
df_compiled[ihme_loc_id=="IND_4871" & date=='2022-07-22', out_flag_google := 1]## telangana
df_compiled[ihme_loc_id=="IND_4871" & date=='2022-07-25', out_flag_google := 1]## telangana
df_compiled[ihme_loc_id=="CHN_354" & date>='2022-07-01' & date<='2022-07-02', out_flag_google := 1]# hong kong
df_compiled[ihme_loc_id=="ESP_60361" & date=='2022-07-25', out_flag_google := 1]## madrid
df_compiled[ihme_loc_id=="ESP_60372" & date=='2022-07-25', out_flag_google := 1]## galicia
df_compiled[ihme_loc_id=="ESP_60374" & date=='2022-07-25', out_flag_google := 1]## basque country
df_compiled[ihme_loc_id=="ESP_60359" & date=='2022-07-24', out_flag_google := 1]## cantabria
df_compiled[ihme_loc_id=="ESP_60370" & date=='2022-07-09', out_flag_google := 1]## navarre
df_compiled[ihme_loc_id=="ESP_60370" & date=='2022-07-10', out_flag_google := 1]## navarre
df_compiled[ihme_loc_id=="ESP_60370" & date=='2022-07-15', out_flag_google := 1]## navarre
df_compiled[ihme_loc_id=="ESP_60370" & date=='2022-07-16', out_flag_google := 1]## navarre
df_compiled[ihme_loc_id=="ESP_60370" & date=='2022-07-25', out_flag_google := 1]## navarre
df_compiled[ihme_loc_id=="DEU_60379" & date=='2022-07-25', out_flag_google := 1]## berlin
df_compiled[ihme_loc_id=="DEU_60385" & date>='2022-07-23' & date<='2022-07-25', out_flag_google := 1]# mecklenburg-V

# pakistan subnats
df_compiled[ihme_loc_id=="PAK_53615" & date>='2022-07-10' & date<='2022-07-13', out_flag_google := 1]# azad
df_compiled[ihme_loc_id=="PAK_53616" & date>='2022-07-10' & date<='2022-07-13', out_flag_google := 1]# balo
df_compiled[ihme_loc_id=="PAK_53616" & date=='2022-07-24', out_flag_google := 1]## balo
df_compiled[ihme_loc_id=="PAK_53618" & date>='2022-07-08' & date<='2022-07-13', out_flag_google := 1]# ICT
df_compiled[ihme_loc_id=="PAK_53619" & date>='2022-07-10' & date<='2022-07-12', out_flag_google := 1]# khyber
df_compiled[ihme_loc_id=="PAK_53619" & date=='2022-07-24', out_flag_google := 1]## khyber
df_compiled[ihme_loc_id=="PAK_53620" & date>='2022-07-10' & date<='2022-07-12', out_flag_google := 1]# punjab
df_compiled[ihme_loc_id=="PAK_53620" & date=='2022-07-24', out_flag_google := 1]## punjab
df_compiled[ihme_loc_id=="PAK_53621" & date>='2022-07-08' & date<='2022-07-12', out_flag_google := 1]# sindh
df_compiled[ihme_loc_id=="PAK_53621" & date>='2022-07-24' & date<='2022-07-25', out_flag_google := 1]# sindh

# 8/29
df_compiled[ihme_loc_id=="PHL" & date>='2022-08-20' & date<='2022-08-21', out_flag_google := 1]## phillippines
df_compiled[ihme_loc_id=="BIH" & date=='2022-07-31', out_flag_google := 1]## bosnia
df_compiled[ihme_loc_id=="BIH" & date=='2022-08-07', out_flag_google := 1]## bosnia
df_compiled[ihme_loc_id=="BIH" & date=='2022-08-14', out_flag_google := 1]## bosnia
df_compiled[ihme_loc_id=="HRV" & date>='2022-08-20' & date<='2022-08-21', out_flag_google := 1]## croatia
df_compiled[ihme_loc_id=="SVN" & date=='2022-08-15', out_flag_google := 1]## slovenia
df_compiled[ihme_loc_id=="SVN" & date>='2022-08-20' & date<='2022-08-21', out_flag_google := 1]## slovenia
df_compiled[ihme_loc_id=="LTU" & date=='2022-08-15', out_flag_google := 1]## lithuania
df_compiled[ihme_loc_id=="LTU" & date>='2022-08-20' & date<='2022-08-21', out_flag_google := 1]## lithuania
df_compiled[ihme_loc_id=="LUX" & date=='2022-08-15', out_flag_google := 1]## luxembourg
df_compiled[ihme_loc_id=="LUX" & date>='2022-08-20' & date<='2022-08-21', out_flag_google := 1]## luxembourg
df_compiled[ihme_loc_id=="NLD" & date>='2022-08-20' & date<='2022-08-21', out_flag_google := 1]## netherlands
df_compiled[ihme_loc_id=="NOR" & date=='2022-08-14', out_flag_google := 1]## norway
df_compiled[ihme_loc_id=="NOR" & date>='2022-08-20' & date<='2022-08-22', out_flag_google := 1]## norway
df_compiled[ihme_loc_id=="SWE" & date>='2022-08-20' & date<='2022-08-22', out_flag_google := 1]## sweden
df_compiled[ihme_loc_id=="CHE" & date=='2022-08-01', out_flag_google := 1]## switzerland
df_compiled[ihme_loc_id=="CHE" & date=='2022-08-21', out_flag_google := 1]## switzerland
df_compiled[ihme_loc_id=="BHR" & date>='2022-08-08' & date<='2022-08-09', out_flag_google := 1]## bahrain
df_compiled[ihme_loc_id=="AFG" & date=='2022-08-08', out_flag_google := 1]## afghanistan
df_compiled[ihme_loc_id=="AFG" & date=='2022-08-16', out_flag_google := 1]## afghanistan
df_compiled[ihme_loc_id=="AFG" & date>='2022-08-20' & date<='2022-08-22', out_flag_google := 1]## afghanistan
df_compiled[ihme_loc_id=="KEN" & date>='2022-08-09' & date<='2022-08-12', out_flag_google := 1]## kenya
df_compiled[ihme_loc_id=="KEN" & date>='2022-08-15' & date<='2022-08-16', out_flag_google := 1]## kenya
df_compiled[ihme_loc_id=="KEN" & date>='2022-08-21' & date<='2022-08-22', out_flag_google := 1]## kenya

df_compiled[ihme_loc_id=="USA_564" & date=='2022-08-07', out_flag_google := 1]## south dakota
df_compiled[ihme_loc_id=="USA_564" & date=='2022-08-14', out_flag_google := 1]## south dakota
df_compiled[ihme_loc_id=="USA_564" & date=='2022-08-21', out_flag_google := 1]## south dakota
df_compiled[grepl('ITA', ihme_loc_id) & date=='2022-08-15', out_flag_google := 1]# Italy subnats - saint mary holiday
df_compiled[grepl('ITA', ihme_loc_id) &  date>='2022-08-20' & date <= '2022-08-21', out_flag_google := 1]# Italy subnats
df_compiled[grepl('DEU', ihme_loc_id) & date>='2022-08-20' & date <= '2022-08-21', out_flag_google := 1]# germany subnats


#10/17

df_compiled[ihme_loc_id=="KHM" & date>='2022-09-23' & date<='2022-09-28', out_flag_google := 1] #cambodia
df_compiled[ihme_loc_id=="LAO" & date=='2022-10-08', out_flag_google := 1] #laos
df_compiled[ihme_loc_id=="FJI" & date=='2022-10-06', out_flag_google := 1] #fiji
df_compiled[ihme_loc_id=="FJI" & date=='2022-10-07', out_flag_google := 1] #fiji
df_compiled[ihme_loc_id=="FJI" & date=='2022-10-08', out_flag_google := 1] #fiji
df_compiled[ihme_loc_id=="FJI" & date=='2022-09-07', out_flag_google := 1] #fiji
df_compiled[ihme_loc_id=="KOR" & date>='2022-09-09' & date<='2022-09-12', out_flag_google := 1] #korea
df_compiled[ihme_loc_id=="KOR" & date=='2022-10-03', out_flag_google := 1] #korea
df_compiled[ihme_loc_id=="ARG" & date>='2022-10-07' & date<='2022-10-08', out_flag_google := 1] #argentina
df_compiled[ihme_loc_id=="CHL" & date>='2022-09-16' & date<='2022-09-19', out_flag_google := 1] #chile
df_compiled[ihme_loc_id=="CHL" & date=='2022-10-08', out_flag_google := 1] #chile
df_compiled[ihme_loc_id=="HND" & date>='2022-10-05' & date<='2022-10-08', out_flag_google := 1] #honduras
df_compiled[ihme_loc_id=="NIC" & date>='2022-09-14' & date<='2022-09-15', out_flag_google := 1] #nicaragua
df_compiled[ihme_loc_id=="EGY" & date=='2022-10-08', out_flag_google := 1] #egypt
df_compiled[ihme_loc_id=="EGY" & date=='2022-10-06', out_flag_google := 1] #egypt
df_compiled[ihme_loc_id=="ARE" & date=='2022-09-18', out_flag_google := 1] #uae
df_compiled[ihme_loc_id=="ARE" & date=='2022-09-25', out_flag_google := 1] #uae
df_compiled[ihme_loc_id=="ARE" & date=='2022-10-02', out_flag_google := 1] #uae
df_compiled[ihme_loc_id=="ARE" & date=='2022-10-07', out_flag_google := 1] #uae
df_compiled[ihme_loc_id=="NPL" & date>='2022-10-03' & date<='2022-10-08', out_flag_google := 1]#nepal
df_compiled[ihme_loc_id=="BEN" & date=='2022-09-25', out_flag_google := 1] #benin
df_compiled[ihme_loc_id=="BEN" & date=='2022-10-07', out_flag_google := 1] #benin
df_compiled[ihme_loc_id=="HTI" & date>='2022-09-01', out_flag_google := 1] #ahiti

df_compiled[grepl('CHN', ihme_loc_id) &  date>='2022-10-01' & date <= '2022-10-07', out_flag_google := 1] #china public holiday
df_compiled[ihme_loc_id=="USA_563" & date=='2022-09-05', out_flag_google := 1] #south carolina
df_compiled[ihme_loc_id=="USA_563" & date=='2022-09-30', out_flag_google := 1] #south carolina
df_compiled[grepl('MEX', ihme_loc_id) &  date=='2022-09-16', out_flag_google := 1] #mexico independence day
df_compiled[ihme_loc_id=="BRA_4750" & date=='2022-10-07', out_flag_google := 1] #acre
df_compiled[ihme_loc_id=="BRA_4751" & date=='2022-09-07', out_flag_google := 1] #alagos
df_compiled[ihme_loc_id=="BRA_4751" & date=='2022-09-16', out_flag_google := 1] #alagos
df_compiled[ihme_loc_id=="BRA_4751" & date=='2022-10-08', out_flag_google := 1] #alagos
df_compiled[ihme_loc_id=="IND_4850" & date>='2022-08-31' & date<='2022-09-03', out_flag_google := 1] # goa
df_compiled[ihme_loc_id=="IND_4850" & date>='2022-10-05' & date<='2022-10-05', out_flag_google := 1] #goa
df_compiled[ihme_loc_id=="IND_4859" & date>='2022-10-05' & date<='2022-10-05', out_flag_google := 1] #madhya pradesh
df_compiled[ihme_loc_id=="IND_4869" & date=='2022-10-01', out_flag_google := 1] #mafya
df_compiled[ihme_loc_id=="IND_4869" & date>='2022-10-04' & date<='2022-10-07', out_flag_google := 1] # madhya
df_compiled[ihme_loc_id=="IND_4871" & date>='2022-10-05' & date<='2022-10-08', out_flag_google := 1] # telangana
df_compiled[ihme_loc_id=="IND_4872" & date>='2022-09-28' & date<='2022-10-01', out_flag_google := 1] # tripura
df_compiled[ihme_loc_id=="IND_4875" & date=='2022-10-02', out_flag_google := 1] #west bengal
df_compiled[ihme_loc_id=="PAK_53616" & date>='2022-09-25' & date<='2022-10-30', out_flag_google := 1] #balochistan
df_compiled[grepl('PAK', ihme_loc_id) &  date>='2022-08-08' & date <= '2022-08-09', out_flag_google := 1] #pakistan subnats
df_compiled[ihme_loc_id=="DEU_60385" & date=='2022-10-03', out_flag_google := 1] #mecklenberg-v

# plot to see what this will take out:
if(plot_bool){
  post_out_path <- paste0(output_subdir,"/post_outliering.pdf")
  post_outliering_plot(post_out_path,df_compiled, loc_list = unique(locs_for_pub$ihme_loc_id))
}


initial_data_path <- paste0(output_subdir,"/post_outliering_indicated_data.csv")
fwrite(df_compiled, initial_data_path)


yaml::write_yaml(
  list(
    script = "02_inpute_data.R",
    output_dir = output_subdir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)
