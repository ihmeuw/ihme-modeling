## Setup
Sys.umask("0002")
Sys.setenv(MKL_VERBOSE = 0)
user <- Sys.info()["user"]
library(data.table)
library(ggplot2)
setDTthreads(1)

tmpinstall <- system(SYSTEM_COMMAND)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T)

code_dir <- file.path("FILEPATH", user, "covid-beta-inputs/")
source(file.path(code_dir, "utils.R"))

## Arguments
N_comp <- 5
lsvid <- 680

## Paths
in_dir <- "FILEPATH"
file_list <- list.files(in_dir)
out_path <- "FILEPATH/data_comp.pdf"

## Tables
source(file.path("FILEPATH/get_location_metadata.R"))
hierarchy <- get_location_metadata(location_set_id = 111, location_set_version_id = lsvid)
pop <- get_populations(hierarchy)

## Read data
files <- rev(file_list)[seq(N_comp)]
dt <- rbindlist(lapply(files, function(f) {
    version_dt <- fread(file.path(in_dir, f, "testing/all_locations_tests.csv"))
    version_dt[, version := f]
}))
dt[, date := as.Date(date, format = "%d.%m.%y")]
dt <- dt[!is.na(total_tests)]
dt[, daily_tests := total_tests - shift(total_tests), by = c("location_id", "version")]
dt <- merge(dt, pop)
dt[, test_pc := daily_tests / pop * 1e5]

## Plot
sorted <- ihme.covid::sort_hierarchy(hierarchy)[location_id %in% unique(dt$location_id)]
sorted[, ord := .I]
dt <- merge(dt, sorted[, .(location_id, ord)], by = "location_id")
dt <- dt[order(ord)]
dt[, plot_name := factor(ord, levels = unique(ord), labels = unique(dt[, .(ord, location)])$location)]
dt[, plot_page := (ord - 1) - (ord - 1) %% 9 + 1]
dt[, version := factor(version, levels = unique(dt$version))]
pdf(out_path, width = 12, height = 9)
for(page in sort(unique(dt$plot_page))) {
    plot_dt <- dt[plot_page == page]
    plot_dt <- plot_dt[rev(order(version))]
    gg <- ggplot(plot_dt, aes(x = date, y = test_pc, color = version)) + 
        geom_line(size = 1.1) + 
        scale_color_brewer(palette = "Set2") + 
        facet_wrap(~plot_name, scales = "free_y") + theme_bw() +
        theme(legend.position="bottom")
    print(gg)
}
dev.off()