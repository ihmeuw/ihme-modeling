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
package_dir = os.path.join(here, 'split_models)')

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
    name='split_models',
    description='package for splitting cod and epi models',
    url='FILEPATH',
    author='AUTHOR',
    author_email='AUTHOR',
    install_requires=[
        'chronos>=2.0.0',
        'cluster_utils',
        'core_maths',
        'db_queries>=22.0.0',
        'gbd>=2.0.1',
        'get_draws>=2.0.0',
        'hierarchies>=2.0.0',
        'ihme_rules>=3.0.0',
        'jobmon==1.1.4',
        'pandas!=0.24.0',
        'test_support'
    ],
    packages=['split_models'],
    python_requires='>=3.6',
    include_package_data=True)
