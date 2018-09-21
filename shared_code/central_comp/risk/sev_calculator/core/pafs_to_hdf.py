# simple preprocessing step -- take all paf dtas for a given location
# and save them as hdfs for the sev calculator to use.
# backpain and hearing yld pafs are saved separately from the other yll pafs
import pandas as pd
import sys
import glob
import os

# parse loc_id and risk_version
risk_version = 0
loc_id = int(sys.argv[1])

# get all files for a given location_id
paf_dir = 'FILEPATH/pafs/{}'.format(risk_version)
files = glob.glob(paf_dir + '/{}_*.dta'.format(loc_id))
print len(files)

if not files:
    print "input arguments didn't match any dta files"
    print str(risk_version) + ", " + str(loc_id)
    sys.exit()

yll_h5 = pd.HDFStore(paf_dir + '/tmp_sev/{}.h5'.format(loc_id), mode='w',
                     complib='zlib', complevel=1)

yld_h5 = pd.HDFStore(paf_dir + '/tmp_sev/yld/{}.h5'.format(loc_id), mode='w',
                     complib='zlib', complevel=1)

print files
for f in files:
    print f
    tmp = pd.read_stata(f)

    # we want to export yld pafs to a separate directory
    # for backpain and hearing to use
    ylds = tmp.query('rei_id in [130, 132]')
    # get rid of superflous columns
    ylds = ylds[[col for col in ylds.columns if ('yll' not in col) and
                 ('1000' not in col) and ('draw' not in col)]]
    ylds.columns = ylds.columns.str.replace('paf_yld', 'draw')
    # append to open store
    yld_h5.append('draws', ylds, data_columns=['rei_id'])

    # for yll pafs:
    # we don't want ylds
    tmp = tmp[[col for col in tmp.columns if
               ('yld' not in col) and ('1000' not in col) and
               ('draw' not in col)]]
    # get rid of superflous columns
    tmp.columns = tmp.columns.str.replace('paf_yll', 'draw')
    # append to open store
    yll_h5.append('draws', tmp, data_columns=['rei_id'])
    print "read"

# close the files, and delete the empty ones
for store in [yll_h5, yld_h5]:
    if not store.groups():
        # h5 file is empty, delete it
        store.close()
        os.remove(store.filename)
    else:
        store.close()
