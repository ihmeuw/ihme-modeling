#qsub_epi_uploader

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


#for Ebola ADDRESS NF

for(ss in c('FILEPATH')){ ## can add more paths/to/scripts in this vector to loop over them


    qsub <- make_qsub(p.t.s = ss,
                      p.t.sh = 'FILEPATH/',
                      p.t.e = '/FILEPATH',
                      p.t.o = '/FILEPATH',
                      proj = 'ADDRESS',
                      slots = 20,
                      mem = 2,
                      name = "evd_nf_ADDRESS",
                      arg1 = 1
                      )

    system(qsub) ## submits the job to the scheduler via bash

}



for(ss in c('FILEPATH',
            'FILEPATH')){ ## can add more paths/to/scripts in this vector to loop over them


    qsub <- make_qsub(p.t.s = ss,
                      p.t.sh = 'FILEPATH',
                      p.t.e = '/FILEPATH',
                      p.t.o = '/FILEPATH',
                      proj = 'ADDRESS',
                      slots = 20,
                      mem = 2,
                      name = if(ss == 'FILEPATH'){"vl_ADDRESS_f"}else{"vl_ADDRESS_f"},
                      arg1 = 1
                      )

    system(qsub) ## submits the job to the scheduler via bash

}

#for CL Non-fatal

for(ss in c('FILEPATH')){ ## can add more paths/to/scripts in this vector to loop over them


    qsub <- make_qsub(p.t.s = ss,
                      p.t.sh = 'FILEPATH/0',
                      p.t.e = '/FILEPATH',
                      p.t.o = '/FILEPATH',
                      proj = 'ADDRESS',
                      slots = 20,
                      mem = 2,
                      name = "cl_nf_ADDRESS",
                      arg1 = 1
                      )

    system(qsub) ## submits the job to the scheduler via bash

}
