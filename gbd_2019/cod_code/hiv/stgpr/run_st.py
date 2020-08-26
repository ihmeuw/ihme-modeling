import os 
import sys
sys.path.append('FILEPATH)
from st_gpr import spacetime as st
import pandas as pd
import getpass

reload(st)



# Filepath settings
user = getpass.getuser()
lindir = "FILEPATH"
stdir = "FILEPATH"

def run_spacetime(location_id, lambdaa=1.0, omega=2, zeta=0.95, location_set_version_id = 420):

    # Results dir
    try:
        os.makedirs(stdir)
    except:
        pass

    # Read in some data...
    data = pd.read_csv("%s/linear_predictions.csv" % lindir)
    results = []
    for sex in [1, 2]:

        sdata = data[data.sex_id == sex]

        ################################
        # All country example
        ################################

        # Initialize the smoother
        s = st.Smoother(sdata, location_set_version_id,datavar='ln_dr',modelvar='ln_dr_predicted',pred_age_group_ids=data.age_group_id.unique(),pred_start_year = data.year_id.min())

        # Set parameters (can additionally specify omega (age weight, positive
        # real number) and zeta (space weight, between 0 and 1))
        s.lambdaa = lambdaa
        s.omega = omega
        s.zeta = zeta
        # s.sn_weight = 0.2

        # Tell the smoother to calculate both time weights and age weights
        s.time_weights()
        s.age_weights()

        # Run the smoother and write the results to a file
        s.smooth(location_id)

        # Using the "include_mad" will calculate the global / regional /
        # national MAD estimates of the ST residuals, in case you need them
        # for the GPR step... results = s.format_output(include_mad=True)
        r = s.results
        r['sex_id'] = sex
        results.append(r)

    results = pd.concat(results)
    results.to_csv('%s/%s.csv' % (stdir, location_id), index=False)

if __name__ == "__main__":

    import sys
    location_id = int(sys.argv[1])
    lambdaa = float(sys.argv[2])
    omega = float(sys.argv[3])
    zeta = float(sys.argv[4])
    location_set_version_id = 420
    run_spacetime(location_id, lambdaa, omega, zeta)
