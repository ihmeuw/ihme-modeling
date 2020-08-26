## this script makes some qsub strings and launches jobs

make_qsub <- function(p.t.s , ## path to script you want to launch
                      p.t.sh, ## path to shell script to launch R
                      p.t.e , ## path to directory to write errors
                      p.t.o , ## path to directory to write stdout
                      proj  , ## project name to use in launch
                      slots , ## slots to request
                      mem   , ## memory in integer GB to request
                      name, ## name of qsub
                      arg1){ ## main argument to pass - assumed to be integer

  qsub <- sprintf('qsub -e %s -o %s -P %s -pe multi_slot %s -l mem_free=%iG -N %s %s %s %i',
                  p.t.e,
                  p.t.o,
                  proj,
                  slots,
                  mem,
                  name,
                  p.t.sh,
                  p.t.s,
                  arg1)

  message(qsub)

  return(qsub)
}



os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

load(file = paste0(prefix,'FILEPATH'))

#only need cl endemic locations that are in the full_loc_set

full_loc_set<-subset(full_loc_set, full_loc_set %in% unique_cl_locations)

for(ss in c('FILEPATH')){ ## can add more paths/to/scripts in this vector to loop over them
  for(ii in full_loc_set){

    qsub <- make_qsub(p.t.s = ss,
                      p.t.sh = 'FILEPATH',
                      p.t.e = 'FILEPATH',
                      p.t.o = 'FILEPATH',
                      proj = 'ADDRESS',
                      slots = 1,
                      mem = 2,
                      name = paste0("loc_",ii),
                      arg1 = ii
                      )

    system(qsub) ## submits the job to the scheduler via bash

  }
}
