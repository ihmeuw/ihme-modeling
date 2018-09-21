/*
Launch one job for each risk.
There's a preprocessing step involving transforming PAF files.
*/
set more off
set odbcmgr unixodbc
set rmsg on

local paf_version_id = "`1'"
local convert_pafs = 0 // if 1, launch qsub jobs to convert pafs to a better format
local run_calc = 1 // if 1, launch qsub jobs to run sev_calc.do

local code_dir = "FILEPATH"

// launch parallel jobs (1 for each loc) to take paf dtas and make hdf
if `convert_pafs' == 1 {

    local base_dir = "FILEPATH/pafs/`paf_version_id'"
    local hdf_dir = "`base_dir'/tmp_sev"

    // if hdf dir doesn't exists, make it (and one for YLDs).
    mata: st_local("dir_exists", strofreal(direxists(st_local("hdf_dir"))))
    if !`dir_exists' {
        mkdir "`hdf_dir'"
        mkdir "`hdf_dir'/yld"
    }

    // Launch one job per country. Get all valid country ids by parsing
    // PAF dta file names in base_dir
    mata:
        file_list = dir(st_local("base_dir"), "files", "*.dta")
        location_ids = J(length(file_list), 1, "") // initialize list for results
        t = tokeninit("_") // mata's token parsing object. Means parse on underscore
        for (i=1; i<=length(file_list); i++) {
            file_name = file_list[i]
            tokenset(t, file_name)
            split_tokens = tokengetall(t)
            location_id = split_tokens[1] // location_id is first element of each file name
            location_ids[i] = location_id
        }

        // since files are stored by country_year, deduplicate list of location_ids
        location_ids = uniqrows(location_ids)

        // now turn list of location ids into string to feed back to stata
        result = ""
        for (i=1; i<=length(location_ids); i++) {
            result = location_ids[i] + " " + result
        }

        st_local("location_ids", result)
    end

    // submit jobs
    local py_shell = "FILEPATH/python_shell.sh"
    local slots = 10
    local mem = `=2*`slots''
    foreach loc_id of local location_ids {
      capture confirm file "FILEPATH/pafs/`paf_version_id'/tmp_sev/`loc_id'.h5"
      if _rc {
       !qsub -P proj_rfprep -N "hdf_`loc_id'" ///
       -o FILEPATH -e FILEPATH ///
       -l mem_free=`mem'G -pe multi_slot `slots' ///
       "`py_shell'" ///
       "`code_dir'/core/pafs_to_hdf.py" ///
       "`loc_id'"
     }
   }
}

// Find all risks, their risk id and cont/categ type
// Launch sev calculation for each job
import delimited "`code_dir'/core/data/risk_cont.csv", clear
keep rei_id continuous
levelsof rei_id, local(rei_ids)
if `run_calc' == 1 {
  // submit jobs
  local stata_shell = "FILEPATH/stata_shell.sh"
  local slots = 46
  local mem = `=2*`slots''
  foreach rei_id of local rei_ids {
    preserve
      keep if rei_id == `rei_id'
      levelsof continuous, local(continuous)
      capture confirm file "FILEPATH/sev/`paf_version_id'/summary/`rei_id'.dta"
      if _rc {
        !qsub -P proj_rfprep -N "sev_`rei_id'" ///
          -o FILEAPTH -e FILEPATH ///
          -l mem_free=`mem'G -pe multi_slot `slots' -l hosttype=intel ///
          "`stata_shell'" "`code_dir'/sev_calc.do" ///
          "`code_dir' `rei_id' `paf_version_id' `continuous'"
      }
    restore
  }
}

//END
