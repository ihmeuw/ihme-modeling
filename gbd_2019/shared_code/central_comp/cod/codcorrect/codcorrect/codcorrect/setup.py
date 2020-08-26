from setuptools import setup
import versioneer

dependencies = [
    'aggregator>=0.12.4',
    'cluster_utils>=0.1.0',
    'db_queries>=18.0.0',
    'db_tools>=0.7.0',
    'draw_sources>=0.0.11',
    'gbd>=0.5.0',
    'gbd_outputs_versions>=1.0.0',
    'get_draws>=1.0.7',
    'hierarchies>=1.0.1',
    'jobmon',
    'pandas>=0.25.0'
    'pytest',
    'sqlalchemy'
]

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    name='fauxcorrect',
    description='Apply GBD 2017 scalars to GBD 2019 CoD model results',
    url='URL',
    author='NAME',
    author_email='EMAIL',
    install_requires=dependencies,
    packages=['fauxcorrect'],
    entry_points={'console_scripts': []}
)
