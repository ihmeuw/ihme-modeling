import versioneer
from setuptools import (
    setup,
    find_packages,
)
import os
import sys
try:
    from autowrap.build import get_WrapperInstall
    HAS_AUTOWRAP = True
except:
    HAS_AUTOWRAP = False


here = os.path.abspath(os.path.dirname(__file__))
package_dir = os.path.join(here, 'winnower)')

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
    name='winnower',
    description='Package to extract and transform survey data for general use',
    url='<ADDRESS>',
    author='<REDACTED>',
    author_email='<USERNAME>@uw.edu',
    python_requires='>=3.7',
    install_requires=[
        'pandas==1.1.4',
        'boltons==20.2.1',
        'attrs==19.3.0',
        'xlrd==2.0.1',
        'openpyxl',
        'simpledbf==0.2.6',
        'pyreadstat==1.1.2',
        'PyYAML',
        'configargparse',
        'ihmeutils',
        'nepali-datetime==1.0.6',
        'ethiopian-date==1.0',
        'persiantools==2.1.1',
    ],
    extras_require={
        'dev': [  # install with `pip install -e .[dev]`
            'flake8',
            'pytest',
            'pytest-cov',
            'pytest-watch',
        ],
    },
    packages=find_packages('src'),
    package_data={
        # Include default logging configuration
        'winnower': ['log_config.ini'],
    },
    package_dir={'': 'src'},
    entry_points={'console_scripts': [
        'run_extract = winnower.commands.run_extract:run_extract',
        'generate-extraction-hook-template = winnower.commands.generate_extraction_hook_template:entry_point', # noqa
        'list_extraction_ids = winnower.commands.list_extraction_ids:list_extraction_ids',  # noqa
        'winnower-query = winnower.commands.iquery:interactive_query',  # noqa
    ]})
