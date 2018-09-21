/*
Fast_read.ado is a wrapper over read_hdf.py. It allows stata to read hdf files
Arguments:
    input_files = space separated list of hdf file paths
    where = string containing conditions that need to be true for a row to be returned.
            This is passed directly to pandas, so the format needs to match. 
            See http://pandas.pydata.org/pandas-docs/stable/io.html#io-hdf5'
            An example: "sex_id in [1,2] and age_group_id != 27"
    num_slots = how many cores/slots do you want to use in reading files? Make sure your cluster job
            has reserved enough! For example, if you are running a 10 slot job, 4 are devoted to stata,
            and 1 to invoking fast_read.py. So you could use 5 slots to read files.
How does it work?
    It calls a python process to read hdf files. The results are streamed through a named pipe that 
    stata then infiles. This is about 10x faster than import delimited, and ~10x slower than read_dta
*/

cap program drop fast_read
program define fast_read
    syntax, input_files(string) [where(string)] num_slots(int) code_dir(string) clear

    // Use the job_id as a pipe name for qsub instances
    local job_id : env JOB_ID
    local pipe_name = "`job_id'.dct"

    // Use a combination of time and SSH params for seeding qlogin instances
    if strlen("`job_id'") == 0 {
        local t = clock("`c(current_date)' `c(current_time)'", "DMYhms")
        local tstr = substr("`t'", -6, 3)

        local ssh : env SSH_CLIENT
        qui di regexm("`ssh'", "(.*) (.*) (.*)")
        local ssh1 = substr(regexs(2), 1, 3)
        local ssh2 = substr(regexs(3), 1, 3)

        local pipe_name = "`ssh1'`ssh2'`tstr'.dct"
    }

    local reader = "`code_dir'/core/readers/read_hdf.py"
    local python = "/usr/local/anaconda-current/bin/python"
    
    if !mi("`where'") {
        local maybe_where = `"--where "`where'""'
    }

    !mkfifo `pipe_name' 
    !`python' `reader' `input_files' `maybe_where' --num_slots `num_slots' --file_format dct > `pipe_name' &
    cap infile using `pipe_name', clear

     if _rc {
         noisily di as error "Error occurred trying to get draws."
         !rm `pipe_name'
         error 1
     }
     else {
         noisily di "Successfully read `c(N)' rows and `c(k)' columns"
         !rm `pipe_name'
         // ID columns are long instead of int (because of location_id), so we
         // can save some space
         compress *_id
     }

end

