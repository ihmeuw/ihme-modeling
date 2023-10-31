
# compare aggregated shocks files between run ids

library(data.table)
library(ggplot2)
library(haven)
library(stringr)

# set run ids
new_run_id <- 340
old_run_id <- 337
new_GBD_round <- ""
old_GBD_round <- ""
gbd_year <- 2020

# location map
locs <- demInternal::get_locations(gbd_year = gbd_year)

# aggregated shocks files saved as intermediates
check_new <- setDT(read_dta(paste0("")))
check_old <- setDT(read_dta(paste0("")))

# reset names
setnames(check_new, "numkilled", "numkilled_new")
setnames(check_old, "numkilled", "numkilled_old")

# merge together and with location information
check <- merge(
  check_new,
  check_old,
  by = c("location_id","year","age_group_id","sex"),
  all = T
)

check <- merge(
  locs[,c("location_id","ihme_loc_id","location_name")],
  check,
  by = "location_id",
  all.y = T
)

check[, diff := numkilled_old - numkilled_new]
check[, perc_diff := ((numkilled_old - numkilled_new) / numkilled_old) * 100]

# save dataset
readr::write_csv(check, paste0(""))
readr::write_csv(check[!is.na(perc_diff) & !perc_diff == 0], paste0(""))

# make and save plot
pdf(paste0(""),
    height = 10, width = 16)

# by location
for(ll in unique(check$location_id)) {

  temp <- check[location_id == ll]
  # plot all ages and all sexes
  temp <- temp[, .(numkilled_new = sum(numkilled_new, na.rm = T), numkilled_old = sum(numkilled_old, na.rm = T)),
               by = c("location_id","ihme_loc_id","location_name","year") ]
  temp <- melt(
    temp,
    id.vars = c("location_id","ihme_loc_id","location_name","year"),
    variable.name = "GBD_round",
    value.name = "Shocks"
  )
  temp[GBD_round == "numkilled_new", GBD_round := paste0("GBD_", new_GBD_round)]
  temp[GBD_round == "numkilled_old", GBD_round := paste0("GBD_", old_GBD_round)]

  gg <- ggplot(data = temp, aes(x = year)) +
    geom_line(aes(y = Shocks, color = GBD_round)) +
    geom_point(aes(y = Shocks, color = GBD_round)) +
    labs(y = "Shocks", x = "Year",
         title = paste0("Shocks (all ages, all sexes) for: ",temp[1,location_name]," (",temp[1,ihme_loc_id],")")) +
    theme_bw()

  print(gg)

}

dev.off()

