import setuptools

import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

dependencies = [
    "codem",
    "db_queries>=23.0.17",
    "db_tools",
    "gbd",
    "get_draws",
    "jobmon==1.1.5",
    "numpy",
    "pandas",
]

setuptools.setup(
    name="codem_hybridizer",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="USERNAME",
    author_email="USERNAME",
    description="CODEm Hybridizer",
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    install_requires=dependencies,
    url="URL",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    entry_points={'console_scripts': [
        'hybridize=hybridizer.scripts.hybridize:main',
        'hybrid_launch=hybridizer.scripts.hybrid_launch:main'
    ]}
)
