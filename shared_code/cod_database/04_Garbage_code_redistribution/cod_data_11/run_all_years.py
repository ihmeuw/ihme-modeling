# Purpose: Create pafs and calculate proportions for each year that CoD needs

import os
# need to be in this directory for create_pafs to work (imports from this
# directory)
os.chdir("/home/j/WORK/03_cod/01_database/02_programs/maternal_custom")


def _submitpython(script, project='proj_codprep', log=False,
                  slots=2, *params):
    """Launch a python script, given the fullpath and a list of parameters."""
    import subprocess
    if log == True:
        cmd = ['qsub', '-cwd', 'P', project, '-pe', 'multi_slot',
               slots, '-l', 'mem_free={mem}g'.format(mem=slots * 2), '-N',
               jobname, '-o', logo, '-e', loge, shell_script, worker]
    else:
        cmd = ['qsub', '-cwd', 'P', project, '-pe', 'multi_slot',
               slots, '-l', 'mem_free={mem}g'.format(mem=slots * 2), '-N',
               jobname, shell_script, worker]

    for param in params:
        cmd.append(param)
    return subprocess.call(cmd)


def run_all_years(source, source_list, write_to_h5):
    """Submit a job for all split groups in a source."""

    shell_script = ("/homes/strUser/cod-data/02_programs/"
                    "usable/run_on_cluster.sh")
    worker = ("/home/j/WORK/03_cod/01_database/02_programs/"
              "maternal_custom/cod_hiv_correction/"
              "calc_percent_to_hiv_for_cod.py")
    log_err_dir = "/share/temp/sgeoutput/strUser/errors/maternal_props"
    log_out_dir = "/share/temp/sgeoutput/strUser/output/maternal_props"
    if not os.path.exists(log_err_dir):
        os.mkdir(log_err_dir)
    if not os.path.exists(log_out_dir):
        os.mkdir(log_out_dir)
    print 'submitting jobs...'
    slots = 5
    for year in range(2005, 2016):
        loge = "{ld}/mp_{y}".format(ld=log_err_dir, y=year)
        logo = "{ld}/mp_{y}".format(ld=log_out_dir, y=year)
        jobname = 'mp_{}'.format(year)
        call = ('qsub -cwd -P proj_codprep -pe multi_slot {slts} '
                '-l mem_free={mem}g -N {n} -o {logo} -e {loge} {ss} '
                '{w} {src} {list} {sg} {h5}'.format(
                    slts=slots,
                    mem=slots * 2,
                    n=jobname,
                    ss=shell_script,
                    logo=logo,
                    loge=loge,
                    w=worker,
                    src=source,
                    list=source_list,
                    sg=sg,
                    h5=write_to_h5
                )
                )
        subprocess.call(call, shell=True)

if __name__ == "__main__":
    for year in range(1980, 2016):
        # create the pafs
        print "Starting `year'..."
        _submitpython("cod_hiv_correction/create_pafs.py", year)
        # calc props
        print "...pafs done"
        s = "cod_hiv_correction/calc_percent_to_hiv_for_cod.py"
        _submitpython(s, year)
        print "...maternal_hiv props done"
        print "`year' Done!"
