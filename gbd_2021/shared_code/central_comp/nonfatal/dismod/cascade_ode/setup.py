import versioneer
from setuptools import setup
import os
import sys
try:
    from autowrap.build import get_WrapperInstall
    HAS_AUTOWRAP = True
except:
    HAS_AUTOWRAP = False


here = os.path.abspath(os.path.dirname(__file__))
package_dir = os.path.join(here, 'cascade_ode)')

# Make package importable so wrappers can be generated before true installation
sys.path.insert(0, package_dir)

wrapper_build_dir = os.path.join(here, 'build')
wrapper_cfg = os.path.join(here, 'autowrap.cfg')

# Extend the build_py command to create wrappers, if autowrap is installed
vcmds = versioneer.get_cmdclass()

cmds = {}
cmds['sdist'] = vcmds['sdist']
cmds['version'] = vcmds['version']
if HAS_AUTOWRAP:
    cmds['build_py'] = get_WrapperInstall(vcmds['build_py'], wrapper_build_dir,
                                          wrapper_cfg)
else:
    cmds['version'] = vcmds['build_py']

setup(
    version=versioneer.get_version(),
    cmdclass=cmds,
    name='cascade_ode',
    description=('The cascading application of DisMod ODE from global to '
                 'subnational levels'),
    url='URL',
    author='AUTHOR',
    author_email='AUTHOR@uw.edu',
    install_requires=[
        'pandas==0.25.3',
        'sqlalchemy==1.1.14',
        'hierarchies>=2.1.*',
        'jobmon==1.1.5',
        'tables>=3.6.*',
        'db_tools>=1.1.1',
        'db_queries==23.2.1',
        'save_results>=4.3.4',
        'retrying==1.3.3',
        'drmaa==0.7.9',
        'elmo==4.7.4',
        'gbd==4.2.0',
        'click',
        'ihme-rules>=3.7.0'
    ],
    include_package_data=True,
    packages=[
        'cascade_ode',
        'cascade_ode.emr',
        'cascade_ode.developer_command_line_utilities'
    ],
    entry_points={'console_scripts': [
        'monitor_callable=cascade_ode.monitor_callable:main',
        'run_all=cascade_ode.run_all:main',
        'run_children=cascade_ode.run_children:main',
        'run_global=cascade_ode.run_global:main',
        'varnish=cascade_ode.varnish:main',
        'hint_cascade=cascade_ode.developer_command_line_utilities.hint_cascade_resources:main',
        'duplicate_model_versions=cascade_ode.developer_command_line_utilities.duplicate_model_versions:cli'
    ]})
