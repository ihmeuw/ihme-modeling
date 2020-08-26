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
package_dir = os.path.join(here, 'le_decomp)')

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

dependencies = ['cluster_utils', 'db_queries>=22.0.0', 'db_tools>=0.9.0',
                'fbd_research', 'gbd>=2.0.0', 'gbd_outputs_versions>=1.0.3'
                'hierarchies>=1.0.0']
setup(
    version=versioneer.get_version(),
    cmdclass=cmds,
    name='le_decomp',
    description='a package that is deployable to pypi',
    url='URL',
    author='USER',
    author_email='EMAIL',
    install_requires=dependencies,
    packages=['le_decomp'],
    entry_points={'console_scripts': []})
