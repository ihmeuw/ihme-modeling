import setuptools
import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

dependencies = [
    "aggregator @ ADDRESS",
    "db_queries>=23.2.0",
    "db_tools>=0.9.0",
    "dill",
    "draw_sources",
    "gbd>=2.1.0",
    "hierarchies",
    "codem_hybridizer>=2.1.0",
    "jobmon==1.1.5",
    "matplotlib",
    "numpy<1.20",
    "pandas",
    "pymc",
    "pymysql",
    "scipy",
    "seaborn",
    "sqlalchemy",
    "statsmodels",
    "tqdm"
]

setuptools.setup(
    name="codem",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="USERNAMES",
    author_email="USERNAME",
    description="Cause of Death Ensemble Model",
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    install_requires=dependencies,
    url="ADDRESS",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    entry_points={'console_scripts': [
        'run=codem.scripts.run:main',
        'single_model=codem.scripts.single_model:main',
        'model_launch=codem.scripts.model_launch:main',
        'central_run=codem.scripts.central_run:main',
        'InputData=codem.work_exec.InputData:main',
        'GenerateKnockouts=codem.work_exec.GenerateKnockouts:main',
        'CovariateSelection=codem.work_exec.CovariateSelection:main',
        'LinearModelBuilds=codem.work_exec.LinearModelBuilds:main',
        'ReadSpacetimeModels=codem.work_exec.ReadSpacetimeModels:main',
        'ApplySpacetimeSmoothing=codem.work_exec.ApplySpacetimeSmoothing:main',
        'ApplyGPSmoothing=codem.work_exec.ApplyGPSmoothing:main',
        'ReadLinearModels=codem.work_exec.ReadLinearModels:main',
        'SpacetimePV=codem.work_exec.SpacetimePV:main',
        'LinearPV=codem.work_exec.LinearPV:main',
        'OptimalPSI=codem.work_exec.OptimalPSI:main',
        'LinearDraws=codem.work_exec.LinearDraws:main',
        'GPRDraws=codem.work_exec.GPRDraws:main',
        'EnsemblePredictions=codem.work_exec.EnsemblePredictions:main',
        'WriteResults=codem.work_exec.WriteResults:main',
        'Diagnostics=codem.work_exec.Diagnostics:main',
        'Email=codem.work_exec.Email:main'
    ]}
)
