

import argparse
import os
from gbd_inj.inj_helpers import paths, versions, inj_info


def main():
    error_list = []
    full_ecode_list = inj_info.DETAILED_ECODES  

    ecodes = args.ecodes
    if ecodes == ['all']:
        ecodes = full_ecode_list
    else:
        ecodes = ['inj_' + e for e in ecodes]

    # error check for repeated elements
    if len(ecodes) != len(set(ecodes)):
        print('WARNING: Removing repeated elements from e-code list')
        ecodes = set(ecodes)

    for ecode in ecodes:
        try:
            if ecode not in full_ecode_list:
                error_list.append(ecode[4:])
                raise EcodeError('ERROR: \'{}\' isn\'t a valid e-code. Are you sure you entered it right?'.format(ecode[4:]))
            parent = inj_info.ECODE_PARENT[ecode]
            version = versions.get_best_version(parent)
            most_recent = versions.get_most_recent(parent, version) 
            run(ecode, most_recent, args.best, version)

        except EcodeError as ecode_error:
            print(ecode_error)
        except StepError as step_error:
            print(step_error)
    if len(error_list) > 0:
        print('\n\n##### ERROR:\nThe following e-codes weren\'t recognized: {}\nRemember that parent causes aren\'t run'
              ' for upload. Also remember not to include the \'inj_\' part of '
              'the e-code name. All other e-codes you specified (if any) were submitted successfully.'.format(error_list))


def submit_array_job(ecode,version,tasks,mark_best,task_start=1, extra_name=''):
    if ecode in inj_info.SHOCK_ECODES:
        extra = ' -l mem_free=150g'
    else:
        extra = ''
    base = "qsub -cwd -P proj_injuries -N {ecode}_UPLOAD{X} -t {T}-{N} -j y -o {logs} -pe multi_slot {slots}{mem_free} {shell} {script}".format(
        logs=paths.LOG_DIR,
        N=int(tasks),
        T=int(task_start),
        slots=40,
        mem_free=extra,
        script=os.path.join(paths.CODE_DIR, 'pipeline_infra', 'step_masters', 'upload_master.py'),
        shell=paths.SHELL_PATH,
        ecode=ecode,
        X=extra_name
    )
    args = " {ecode} {version} {best}".format(
        ecode=ecode,
        version=version,
        best=mark_best
    )
    print(base + args)
    os.popen(base+args)


def run(ecode, most_recent, mark_best, version):
    # check if you've completed enough steps to start with 'first'
    if str(most_recent) <> '6':
        raise StepError('You can\'t upload {} until it finishes the pipeline'.format(ecode))
    
    tasks = len(inj_info.NCODES)  #+8  # for the spinal splits (4 levels for two ncodes)  NO LONGER UPLOADING SPLITS
    if mark_best:
        best = 1
    else:
        best = 0
    
    print("submitting array for {}".format(ecode))
    submit_array_job(ecode, version, tasks, best)


class StepError(Exception):
    pass


class EcodeError(Exception):
    pass


if __name__ == '__main__':
    
    last_step = '6'
    
    parser = argparse.ArgumentParser()
    parser.add_argument('ecodes', nargs='+',
                        help='The list of e-codes to run the process for. Don\'t include \'inj_\'. '
                             'Accepts keyword \'all\' to run all injuries.')
    parser.add_argument('-b', '--best', action='store_true', help='Whether to mark the models best.')
    
    args = parser.parse_args()
    
    print("ecodes:")
    print(args.ecodes)
    print("mark best: {}".format(args.best))
    
    main()