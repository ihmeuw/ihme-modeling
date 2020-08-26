tmp folders should return -- 31,274
	823(most detailed locs) * 38 (years 1980-2017)
use 00_check_filenames.R to generate counts of files present in each folder (by cause/sex)

Still an issue with loc_id = 44662 (an UTLA, Bolton)
	returns missing ages (i think just maternal ages? 7-15? need to doublecheck next run)
use 01_overwrite44662.R to copy-over data from 44661 (Cumbria)

Still an issue with 2017 data for 618 (Other's)
	2017 csv's are empty for all locations
use 02_fix_618_2017.R to copy-over data from 2016 (per location)

AS OF 6/23 (i think) these issues were resolved