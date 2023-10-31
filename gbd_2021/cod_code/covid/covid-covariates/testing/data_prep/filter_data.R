#################################################
## Outliers and modifications for testing data
#################################################
### Remove issues with the data. Do not change data. Just manually drop.
##   -Anything that happens in this function should be flagged for David and team to fix in the actual data
##   -Called in prep_data.R

filter_data <- function(dt) {
  
  # SCOPING - keep for cleaning work -----

    # dt <- copy(raw_testing_dt)
    # # ensure chronologic order (necessary for spike backdistribute)
    # dt <- dt[order(dt$location_id, dt$date)]
    # dt[, filtered_cumul := combined_cumul]
    # dt[ , filtered_daily := c(0, diff(filtered_cumul)), by = .(location_id)]
    # # Find me an outlier
    # loc_id <- 211
    # location_df <- copy(dt)
    # location_df <- location_df[location_id == loc_id]
    # location_df <- merge(location_df, dt_pop, by = "location_id")
    # location_df[, filtered_daily := c(0, diff(filtered_cumul))]
    # location_df[, filtered_daily_pc := filtered_daily / population * 100000]
    # # plot positive outliers (since negatives get removed other ways)
    # plot(location_df$date ,location_df$filtered_cumul)
    # plot(location_df[filtered_daily_pc > 0, date] ,location_df[filtered_daily_pc > 0, filtered_daily_pc])
    # # top 20 outliers
    # location_df[
    #   order(location_df$filtered_daily_pc, decreasing = T),
    #   .(location_id, location_name, date, filtered_daily_pc)
    # ][
    #   1:20
    # ]

  # SCOPING END ----
  
  message("Filtering out data...")
  # ensure chronologic order (necessary for spike backdistribute)
  dt <- dt[order(dt$location_id, dt$date)]
  dt[, filtered_cumul := combined_cumul]
  dt[ , filtered_daily := c(0, diff(filtered_cumul)), by = .(location_id)]
  
  # Crop-back ----
  
  message("Cropping back the following locations:")
  # Bolzano (35498) - needs a spike backdistribute https://github.com/pcm-dpc/COVID-19/blob/master/note/dpc-covid19-ita-note.csv
  message("Bolzano (35498)")
  dt <- dt[!(location_id == 35498 & date >= "2022-04-04")]
  
  # 2022-08-01
  message("Spain (92)") 
  dt <- dt[!(location_id == 92 & date >= "2022-06-13")]
  
  
  # Filtering -----
  
  message("Removing South Africa testing data, shift to national level values")
  #dt <- dt[!(location_id %in% hierarchy[parent_id==196]$location_id)]
  
  message("Removing spike in Russia")
  dt <- dt[!(location_id == 62 & date == as.Date("2020-05-16"))]
  
  message("Removing last few days of DC data")
  dt[(location_id == 531 & date >= as.Date("2020-05-23") & date <= as.Date("2020-05-25")), filtered_cumul := NA]
  
  message("Fixing extraction in Guyana")
  dt[location_id == 113 & date == "2020-09-08", filtered_cumul := NA]
  dt[location_id == 113 & date == "2020-09-21", filtered_cumul := NA]
  
  message("Outliers in Connecticut")
  # dt[location_id == 529 & date == "2021-02-04", filtered_cumul := NA]
  # dt[location_id == 529 & date > "2021-02-04", filtered_cumul := NA]
  
  # message("Remove two recent dates of data in King/Snohomish, Alaska, and Vermont")
  message("Remove last date of data in WA counties")
  dt[, max_date := max(date), by=location_id]
  # dt[location_id %in% c(60886, 60887, 3539, 524, 568) & date >= max_date - 2, filtered_cumul := NA]
  #dt[location_id %in% c(60886, 60887, 3539) & date %in% c(max_date - 0:4), filtered_cumul := NA]
  #dt$max_date <- NULL
  
  #message("Removing negative in Spain")
  #dt[(location_name == "Spain" & date == "2020-08-27"), filtered_cumul := NA]
  #dt[(location_name == "Spain" & date >= "2020-09-24"), filtered_cumul := NA]
  #message("Removing data error across Spanish states")
  #dt[location_id %in% c(60360, 60369, 60361, 60362, 60373, 60371, 60368) & date >= "2020-09-24", filtered_cumul := NA]
  
  message("Drop zeros in Acre")
  dt[(location_name == "Acre" & date >= "2020-07-24"), filtered_cumul := NA]
  message("Drop recent very low days in Ceara, Rondonia")
  dt[location_id == 4770 & date < "2020-01-31" & raw_cumul > 125000, date := date + 365]
  message("Update Santa Catarina")
  # dt[location_id == 4773 & date %in% as.Date(c("2021-03-10","2021-03-11","2021-03-12","2021-04-03")),
  #    filtered_cumul := NA]
  
  
  message("Drop recent very low days in Mizoram")
  dt[(location_id == 4863 & date >= "2020-09-06" & date <= "2020-09-11"), filtered_cumul := NA]
  
  message("Drop recent very high days in Assam")
  dt[(location_id == 4843 & date >= "2020-09-28" & date <= "2020-09-30"), filtered_cumul := NA]
  
  message("Removing spike in Central Kalimantan")
  dt[(location_id == 4730 & date == "2020-09-08"), filtered_cumul := NA]
  dt[(location_id == 4729 & date == "2020-09-08"), filtered_cumul := NA]
  
  message("Removing spike in El Salvador")
  dt[(location_id == 127 & date >= "2020-09-20" & date < "2020-09-25"), filtered_cumul := NA]
  
  # message("Fixing extraction error in Malawi")
  # dt[(location_id == 182 & date == "2020-09-15"), filtered_cumul := 48816]
  
  message("Fixing extraction error in Iraq")
  #dt[(location_id == 143 & date == "2020-09-25"), filtered_cumul := NA]
  
  # message("Fixing extraction error in Palestine")
  # dt[(location_id == 149 & date == "2020-10-13"), filtered_cumul := NA]
  # 
  message("Palestine testing data changed, perhaps")
  dt[location_id == 149 & date >= "2021-03-24", filtered_cumul := filtered_cumul - 100000]
  
  message("Removing negative in Mozambique")
  dt[(location_id == 184 & date %in% as.Date(c("2020-09-12","2020-09-17"))), filtered_cumul := NA]
  
  message("Removing low/mistake in Namibia")
  dt[(location_id == 195 & date %in% as.Date(c("2020-09-11","2020-09-12"))), filtered_cumul := NA]
  
  # message("Removing error in Bosnia and Herzegovina")
  # dt[location_id == 44 & date == "2020-10-21", filtered_cumul := NA]
  
  message("Removing data in China national")
  dt <- dt[location_id != 6]
  
  message("Removing Basque Country")
  dt <- dt[location_id != 60374]
  
  message("Removing something odd in Alberta")
  dt[location_id == 43858 & date == "2020-10-22", filtered_cumul := NA]
  
  message("Removing something odd in Hungary")
  dt[location_id == 43858 & date == "2020-12-02", filtered_cumul := NA]
  
  message("Removing something odd in Austria")
  dt[location_id == 75 & date == "2020-04-01", filtered_cumul := NA]
  dt[location_id == 75 & date == "2021-01-03", filtered_cumul := NA]
  #dt[location_id == 75 & date > "2021-01-11", filtered_cumul := (filtered_cumul - 2885871) / 4.25]
  #dt[location_id == 75 & date > "2021-01-11", filtered_cumul := NA]
  
  message("Removing something odd in Punjab")
  dt[location_id == 4867 & date == "2020-02-20", filtered_cumul := NA]
  dt[location_id == 4867 & date == "2020-06-02", filtered_cumul := NA]
  
  message("Removing something odd in Northern Mariana Islands")
  dt[location_id == 376 & date == "2020-11-18", filtered_cumul := NA]
  
  message("Removing huge negative in Jammu & Kashmir")
  dt[location_id == 4854 & date == "2020-12-12", filtered_cumul := NA]
  dt[location_id == 4854 & date == "2021-04-23", filtered_cumul := NA]
  dt[location_id == 4854 & date == "2021-05-07", filtered_cumul := NA]
  dt[location_id == 4854 & date == "2021-04-24", filtered_cumul := NA]
  dt[location_id == 4854 & date %in% as.Date(c("2021-04-28","2021-04-29")), filtered_cumul := NA]
  
  message("Removing huge negative in Nagaland")
  dt[location_id == 4864 & date == "2020-11-17", filtered_cumul := NA]
  
  message("Remove increase in Nagaland")
  dt[location_id == 4864 & date == "2021-05-21", filtered_cumul := NA]
  
  # message("Fixing error in Moldova")
  # dt[location_id == 61 & date == "2020-12-17", filtered_cumul := 518954]
  
  # message("Fixing error in Parana")
  # dt[location_id == 4765 & date == "2020-12-23", filtered_cumul := NA]
  
  message("Fixing error in Malta")
  dt[location_id == 88 & date == "2021-05-10", filtered_cumul := NA]
  
  message("Dropping data in Galicia")
  dt <- dt[!(location_id == 60372 & date < "2020-05-11")]
  
  message("Trinidad and Tobago")
  dt[location_id == 119 & date >= "2020-12-21", filtered_cumul := filtered_cumul - 30800]
  
  dt[location_id == 150 & date >= "2020-07-06", filtered_cumul := filtered_cumul - 107693]
  
  message("Fix Cyprus")
  dt[location_id == 77 & date == "2021-01-21", filtered_cumul := NA]
  
  # message('Adjust Benin for high value')
  # dt[...]
  
  #message("Fix Djibouti")
  #dt[location_id == 177 & date == "2021-01-22", filtered_cumul := NA]
  
  # message("Remove negatives in Sierra Leone")
  # dt[location_id == 217 & date %in% as.Date(c("2020-07-06","2020-11-10","2020-12-12")), filtered_cumul := NA]
  # 
  # message("Decrease Bhutan")
  # dt[location_id == 162 & date > "2021-01-05", filtered_cumul := filtered_cumul - 144847]
  # 
  message("Decrease Sardegna")
  dt[location_id == 162 & date >= "2021-02-15", filtered_cumul := filtered_cumul - 43000]
  
  message("Removing data change across Mexican states")
  dt[location_id %in% hierarchy[parent_id == 130, location_id] & date %in% as.Date(c("2020-10-23","2020-10-24","2020-10-25")), filtered_cumul := NA]
  
  message("- Malaysia")
  dt[(location_name == "Malaysia" & date %in% c(as.Date("2020-05-16"), as.Date("2020-05-17"), as.Date("2020-05-20"))), filtered_cumul := NA]
  
  message("Adjusting rogue Jamaica datapoint")
  dt[location_id == 115 & date == "2021-03-03", filtered_cumul := 220382]
  
  message("Adjust Poland")
  dt[location_id == 51 & date %in% as.Date(c("2021-01-29","2021-03-29")), filtered_cumul := NA]
  
  message("Adjust Peru for no negative")
  dt[location_id == 123 & date == "2020-12-16", filtered_cumul := NA]
  
  # message("Adjust Bosnia & Herzegovina for no negative")
  # dt[location_id == 44 & date == "2021-03-02", filtered_cumul := NA]
  
  #message("Adjust Bahamas")
  #dt[location_id == 106 & date == "2021-03-21", filtered_cumul := NA]
  
  message("Adjust Albania for negative")
  dt[location_id == 43 & date %in% as.Date(c("2021-02-14","2021-02-20","2021-01-15","2021-01-20", "2021-03-29", "2021-04-02",
                                             "2021-04-05","2021-04-06")), filtered_cumul := NA]
  
  message("Distribute February 1 across a week in Canada")
  dt[location_id %in% hierarchy[parent_id == 101, location_id] & date > "2021-01-22" & date < "2021-02-01", filtered_cumul := NA]
  
  message("Adjust Timor-Leste")
  dt[location_id == 19 & date > "2021-03-15", filtered_cumul := filtered_cumul - 8000]
  
  message("Drop Sao Tome and Principe (huge gap in data)")
  dt[location_id == 215 & date == "2021-03-26", filtered_cumul := NA]
  
  # message("Issue in Togo")
  # dt[location_id == 218 & date %in% as.Date(c("2021-04-15","2021-04-18")), filtered_cumul := NA]
  # 
  message("Resolve massive 
          spike in Cuba")
  dt[location_id == 109 & date == "2021-04-28", filtered_cumul := NA]
  
  # message("Georgia (country) extraction error?")
  # dt[location_id == 35 & date >= "2021-05-06", filtered_cumul := filtered_cumul - 1300000]
  # 
  dt[location_id == 121 & date == "2021-05-03", filtered_cumul := NA]
  
  message("Add missing days (average) for Sierra Leone")
  dt[location_id == 217 & date == "2021-05-03", filtered_cumul := filtered_cumul + 350 * 12]
  
  message("remove outliers in Mauritius (183)") # added 2022-06-07"
  dt[location_id == 183 & date %in% as.Date(c("2022-03-07", "2022-05-29")), filtered_cumul := NA]
  
  
  # Added 2022-08-01
  # First 3 not totally effective - really need spike-backdistribute function
  message("Oregon outliers")
  dt[location_id == 560 & date == "2022-06-02", filtered_cumul := NA]
  
  message("Ecuador outliers")
  dt[location_id == 122 & date == "2022-05-31", filtered_cumul := NA]
  
  message("Chad outliers")
  dt[location_id == 204 & date == "2022-01-14", filtered_cumul := NA]
  
  message("Assam outliers")
  # Assam reported 401,098 new tests on 7/24, but then on 7/26 they report
  # -392,501. So that huge point is an error, and they have corrected it by
  # reporting negative daily tests two days later
  dt[location_id == 4843 & date %in% as.Date(c("2022-07-24", "2022-07-25")), filtered_cumul := NA]
  
  # New as of 2022-11-10
  message ("Uttar Pradesh outliers")
  # two days of high then negative reporting to correct
  dt[location_id == 4873 & date %in% as.Date(c("2022-08-14", "2022-08-16")), filtered_cumul := NA] # OK

  message("Backdistributing outliers over whole time series: ")
  # Mongolia, the one that started it all...
  dt <- backdistribute_spike(dt, 38L, "2021-12-03")
  # mong <- dt[location_id == 38L]
  # plot(mong$date, mong$filtered_cumul)
  # plot(mong$date, mong$filtered_daily)
  dt <- backdistribute_spike(dt, 53617, "2020-04-18") # Gilgit-Baltistan OK 
  dt <- backdistribute_spike(dt, 4773, "2021-04-03") # Santa Catarina OK
  dt <- backdistribute_spike(dt, 4765, "2021-06-10") # Paraná OK
  dt <- backdistribute_spike(dt, 4759, "2021-10-14") # Maranhão OK
  dt <- backdistribute_spike(dt, 211, "2022-03-12") # Mali OK
  
  message("  Burkina Faso outlier")
  dt[location_id == 201 & date %in% as.Date(c("2022-01-14")), filtered_cumul := NA] # back
  # dt <- backdistribute_spike(dt, 201, "2022-01-14") # didn't work as expected - tune up function when time allows
  message("  Balearic Islands outlier")
  dt[location_id == 60363 & date %in% as.Date(c("2020-07-10")), filtered_cumul := NA] # back
  # dt <- backdistribute_spike(dt, 60363, "2020-07-10") # didn't work as expected - tune up function when time allows
  
  # Try these when time allows as well
  # positive outlier 2022-11-10
  dt[location_id == 4854 & date %in% as.Date(c("2021-12-01")), filtered_cumul := NA]
  
  
  # Clean up ----
  
  # Cleanup for return to data_prep
  dt <- dt[order(location_id, date)]
  dt$rownum <- 1:nrow(dt)
  dt <- dt[, .(location_id, location_name, date, raw_cumul, raw_daily, combined_cumul, filtered_cumul, positive)]
  
  return(dt)
}



