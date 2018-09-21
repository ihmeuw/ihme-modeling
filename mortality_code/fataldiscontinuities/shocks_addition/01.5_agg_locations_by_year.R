# run agg_results for just one year

if (Sys.info()[1] == 'Windows') {
    username <- "USER"
    root <- ""
    source("PATH")
} else {
    username <- Sys.getenv("USER")
    root <- ""
    source("PATH")
    year <- commandArgs()[3]
}

library(data.table)

# read in the shock data for the year we're interested in
shocks <- fread(paste0("PATH", year, ".csv"))
shocks[, V1:= NULL]
draws <- grep("draw", names(shocks), value=T)
# for debugging in output file
print(year)
print(draws)

# run agg_results
compiled_shock_numbers <- agg_results(data=shocks, id_vars=c("location_id", "year_id", "sex_id", "age_group_id"), value_vars=draws, loc_scalars = F, agg_sdi=T)

# save the output in the same dir
write.csv(compiled_shock_numbers, paste0("PATH", year, "_aggregates.csv"))
