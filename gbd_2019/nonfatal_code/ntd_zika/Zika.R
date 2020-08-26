########################################################
########################################################

## Prep R
require(lme4)

os <- .Platform$OS.type
if (os == "windows") {
    prefix <- "FILEPATH"
}  else {
    prefix <- "FILEPATH"
}

#source central functions

source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))

## Filepaths
locdat <- get_location_metadata(location_set_id=ADDRESS)



### Zika 
MEList <- c(ADDRESS1, ADDRESS2, ADDRESS3, ADDRESS4)

for (me in MEList){
    tmp_data <- me_data <- get_draws("modelable_entity_id", me, source = 'epi',gbd_round_id=5, status='best', num_workers = 30)
    unique_locations<-unique(me_data$location_id)
    
    me_2016<-subset(me_data, me_data$year_id==2016)
    me_2017<-subset(me_data, me_data$year_id==2017)
    ULocs <- unique(me_2016$location_id)
    
    pb <- txtProgressBar(min=1,max=length(ULocs),initial=1,style=3)
    for (i in 1:length(ULocs)){
        setTxtProgressBar(pb,i)
        tmploc <- which(locdat$location_id == ULocs[i])
        if (locdat$region_name[i] == "Latin America and Caribbean"){
            Rat <- 1/runif(min=4,max=10,1)
        } else {
            Rat <- 1/runif(min=2,max=5,1)
            }
        tmplocs <- which(me_2016$location_id == ULocs[i])
        me_2017[tmplocs,2:1001] <- me_2016[tmplocs,2:1001] * Rat
        }
    close(pb)
    
    Toss <- which(me_data$year_id == 2017)
    me_data <- rbind(me_data[-Toss,], me_2017)

    unique_locations<-unique(me_data$location_id)
    for (i in 1:length(unique_locations)){
        subset<-subset(me_data, me_data$location_id==unique_locations[i])
        write.csv("FILEPATH/",me,unique_locations[i]))
        print(paste0("Currently at ", i))
        }
    Measure <- '5 6'
    if (length(unique(me_data$measure_id)) == 1){
        Measure <- me_data$measure_id[1]
        }
    }



### Save Results
  save_results_epi(input_dir="FILEPATH/",me),
                  input_file_pattern = "{location_id}.csv",
                  modelable_entity_id = me,
                  measure_id = Measure,
                  description = "New GBD2017 upload",
                  year_id=sort(unique(me_data$year_id)),
                  mark_best=TRUE)
