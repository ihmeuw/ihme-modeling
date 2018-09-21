# script to attach geographies from the geography codebooks, and to grab administrative
# polygons for clusters with gps coordinates
# clear workspace
rm(list = ls())

# load packages
require(raster)
require(readstata13)

# read in CBH-SBH dataset
cbh_sbh <- read.csv('<<<< FILEPATH REDACTED >>>>>/CBH_with_SBH.csv',
                    stringsAsFactors = FALSE)

# read in geography codebook, combined
geographies <- read.csv("<<<< FILEPATH REDACTED >>>>>/combined_geography_codebook_Y2017M03D27.csv",
                        stringsAsFactors = FALSE)

# read in admin 1 shapefile
country_a1 <- shapefile('<<<< FILEPATH REDACTED >>>>>/admin2013_1.shp')

# read in admin 2 shapefile
country_a2 <- shapefile('<<<< FILEPATH REDACTED >>>>>/admin2013_2.shp')

# match geographies (for point records)
# create a match id, which is identical across the two datasets
cbh_sbh$match_id <- paste(cbh_sbh$nid, cbh_sbh$cluster_number, sep = "_")
geographies$match_id <- paste(geographies$nid, geographies$cluster_number, sep = "_")

# match based on this id
matched_svy <- match(cbh_sbh$match_id, geographies$match_id)

# attach fields based on this match index
cbh_sbh$point <- geographies$point[matched_svy]
cbh_sbh$lat <- geographies$latnum[matched_svy]
cbh_sbh$lon <- geographies$longnum[matched_svy]
cbh_sbh$location_name <- geographies$location_name[matched_svy]
cbh_sbh$admin_level <- geographies$admin_level[matched_svy]

# create subsets based on point/polygon
cbh_sbh_na <- cbh_sbh[is.na(cbh_sbh$point),]
cbh_sbh_nonna <- cbh_sbh[!is.na(cbh_sbh$point),]

cbh_sbh_points <- cbh_sbh_nonna[cbh_sbh_nonna$point == 1, ]
cbh_sbh_non_point <- cbh_sbh_nonna[cbh_sbh_nonna$point == 0, ]

# sample points to grab admin 1 location names
# create matrix of point locations
lat <- cbh_sbh_points$lat
long <- cbh_sbh_points$lon

pts <- cbind(long,
             lat)

# using the sp package, where country is the admin 1 shapefile and pts
# is a two-column (long, lat) matrix of coordinates:
pts_sp <- SpatialPoints(pts,
                        proj4string = CRS(projection(country_a1)))

# get ISO codes for all points
admin_1 <- over(pts_sp, country_a1)$NAME

# merge back with background dataset
cbh_sbh_points$admin_1 <- admin_1

# create matrix of point locations
lat <- cbh_sbh_points$lat
long <- cbh_sbh_points$lon

pts <- cbind(long,
             lat)

pts_sp <- SpatialPoints(pts,
                        proj4string = CRS(projection(country_a2)))

# get admin2 names for all points
admin_2 <- over(pts_sp, country_a2)$NAME

# merge back with background dataset
cbh_sbh_points$admin_2 <- admin_2

# merge back macro dhs data
cbh_sbh_matched <- rbind(cbh_sbh_points,
                         cbh_sbh_non_point,
                         cbh_sbh_na)

# remove unnecessary cols
cbh_sbh_matched$lat <- NULL
cbh_sbh_matched$lon <- NULL
cbh_sbh_matched$point <- NULL
cbh_sbh_matched$location_name <- NULL
cbh_sbh_matched$admin_level <- NULL
cbh_sbh_matched$match_id <- NULL

# correct some positional errors for Egypt surveys ('Administrative unit not available')
cbh_sbh_matched$admin_2[cbh_sbh_matched$admin_2 == "Administrative unit not available"] <- NA
cbh_sbh_matched$admin_2[cbh_sbh_matched$admin_2 == ""] <- NA

post_98 <- cbh_sbh_matched[cbh_sbh_matched$year >= 1998, ]

# remove unnecessary dataframes
rm(cbh_sbh_na,
   cbh_sbh_non_point,
   cbh_sbh_points,
   cbh_sbh_nonna,
   pts,
   admin_1,
   admin_2,
   lat,
   long,
   pts_sp,
   matched_svy)

# get nid lists
post_98_nids <- unique(post_98[c('nid', 'country', 'year', 'survey')])
post_88_nids <- unique(cbh_sbh_matched[c('nid', 'country', 'year', 'survey')])

# write to disk
write.csv(post_98,
          '<<<< FILEPATH REDACTED >>>>>/CBH_with_SBH_1998_2016.csv',
          row.names = FALSE)

write.csv(cbh_sbh_matched,
          '<<<< FILEPATH REDACTED >>>>>/CBH_with_SBH_1988_2016.csv',
          row.names = FALSE)

write.csv(post_98_nids,
          '<<<< FILEPATH REDACTED >>>>>/post_1998_nids.csv',
          row.names = FALSE)

write.csv(post_88_nids,
          '<<<< FILEPATH REDACTED >>>>>/post_1988_nids.csv',
          row.names = FALSE)

# rm(list = setdiff(ls(), "post_98"))
