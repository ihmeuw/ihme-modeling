from glob import glob
from db_queries import get_location_metadata

measure_ids = [5,6]
years = [1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019]
locs = get_location_metadata(location_set_id=35, gbd_round_id=6)
locs = locs[locs['most_detailed']==1]
locs = locs.location_id.unique().tolist()
sexs = [1,2]
indir = 'ADDRESS'

def incomplete_jobs(meid):
	files = glob("FILEPATH/*.csv" % (indir, meid))
	rms = measure_ids
	rls = locs
	rys = years
	rsx = sexs
	reqd = ["%s/%s/01_draws/%s_%s_%s_%s.csv" % (indir, meid, m, l, y, s) for m in rms for l in rls for y in rys for s in rsx]
	missing = set(reqd) - set(files)
	print("Missing %s files..." % len(missing))
	return missing

