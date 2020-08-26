import versioneer
from setuptools import setup
try:
    from autowrap import wrapper
    HAS_AUTOWRAP = True
except:
    HAS_AUTOWRAP = False
import subprocess
import re
import os
import sys


here = os.path.abspath(os.path.dirname(__file__))
package_dir = os.path.join(here, 'dalynator)')

# Make package importable so wrappers can be generated before true installation
sys.path.insert(0, package_dir)

# this could potentially be looked up using an environment variable...
wrapper_build_dir = os.path.join(here, 'build')
wrapper_cfg = os.path.join(here, 'autowrap.cfg')

# Extend the build_py command to create wrappers
vcmds = versioneer.get_cmdclass()
_build_py = vcmds['build_py']


class WrapperInstall(_build_py):
    def run(self):

        # get name of current environment
        if HAS_AUTOWRAP:
            stdout = subprocess.check_output("conda info --envs", shell=True)
            env_name, env_path = re.findall('\n(\S*)\s*\*\s*(\S*)\n',
                                            stdout,
                                            re.MULTILINE)[0]

            # make wrapper directory
            if not os.path.exists(wrapper_build_dir):
                os.makedirs(wrapper_build_dir)

            # gen wrappers
            wrappers = wrapper.generate(
                wrapper_cfg,
                wrapper_build_dir,
                env_path)

            # TODO: move wrappers
            for wf in wrappers:
                pass

        # run normal install
        _build_py.run(self)


cmds = {}
cmds['sdist'] = vcmds['sdist']
cmds['version'] = vcmds['version']
cmds['build_py'] = WrapperInstall
setup(
    version=versioneer.get_version(),
    cmdclass=cmds,
    name='dalynator',
    description='DALY and attributable burden computation in Python',
    url='ADDRESS',
    author='AUTHOR',
    author_email='EMAIL',
    install_requires=['adding_machine',
                      'aggregator',
                      'cluster_utils',
                      'draw_sources',
                      'jobmon',
                      'hierarchies',
                      'pandas',
                      'tables==3.4.4',
                      'gbd',
                      'db_queries',
                      'gbd_outputs_versions>=1.0.3',
                      'dataframe_io',
                      'db_tools',
                      'core_maths',
                      'ihme_dimensions',
                      'mysql-connector-python',
                      ],
    packages=['dalynator', 'dalynator.tasks', 'dalynator.columnstore'],
    include_package_data=True,
    entry_points={'console_scripts': [
        'run_all_burdenator=dalynator.tasks.run_all_burdenator:main',
        'run_all_dalynator=dalynator.tasks.run_all_dalynator:main',
        'run_dalynator_most_detailed=dalynator.tasks.run_pipeline_dalynator_most_detailed:main',
        'run_burdenator_most_detailed=dalynator.tasks.run_pipeline_burdenator_most_detailed:main',
        'run_loc_agg=dalynator.tasks.run_pipeline_burdenator_loc_agg:main',
        'run_cleanup=dalynator.tasks.run_pipeline_burdenator_cleanup:main',
        'run_pct_change=dalynator.tasks.run_pipeline_pct_change:main',
        'run_cs_sort=dalynator.tasks.run_pipeline_cs_sort:main',
        'run_upload=dalynator.tasks.run_pipeline_upload:main',
        'sync_db_metadata=dalynator.columnstore.mysql2cs:main',
    ]}
)
