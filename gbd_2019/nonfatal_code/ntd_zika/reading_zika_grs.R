
#reading in Zika georestrictions 

zika_grs<- read.csv("FILEPATH")        ## Zika georestrictions

zika_grs_endemic<- subset(zika_grs, zika_grs$value_endemicity==1)

zika_grs_endemic_locs<- unique(zika_grs_endemic$location_id)

zika_grs_endemic_locs<- as.data.frame(zika_grs_endemic_locs)

write.csv(zika_grs_endemic_locs, 'FILEPATH')        ## Zika endemic locations
