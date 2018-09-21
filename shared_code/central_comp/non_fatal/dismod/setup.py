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

# this could potentially be looked up using an environment variable...
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
    url='strInternalURL',
    author='strUserName',
    author_email='strUserEmail',
    install_requires=[
        'pandas',
        'sqlalchemy',
        'hierarchies',
        'jobmon',
        'adding_machine',
        'tables'
    ],
    packages=['cascade_ode'],
    entry_points={'console_scripts': []})
