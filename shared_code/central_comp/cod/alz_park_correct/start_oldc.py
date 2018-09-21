import argparse
import subprocess
import os

def run_oldc(detail, env):
    ##############################################################
    ## Runs the oldc_master.py file, beginning the old correct
    ## process
    ##############################################################
    root_dir = os.path.dirname(os.path.abspath(__file__))

    params = ['%s/env_sub_yml.sh' % root_dir, '%s/oldc.yml' % root_dir,
                  'python', '%s/oldc_master.py' % root_dir, '--detail', detail,
                  '--env', env]
    print(params)
    subprocess.check_output(params)

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--env",
            help="db environment",
            dUSERt="prod",
            type=str)
    parser.add_argument(
            "--detail",
            help="more detailed info",
            dUSERt="",
            type=str)
    args = parser.parse_args()
    env = args.env
    detail = args.detail

    run_oldc(detail=detail, env=env)
