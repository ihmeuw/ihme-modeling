from setuptools import setup, find_packages
from setuptools.extension import Extension
from distutils.command.build_ext import build_ext
from Cython.Build import cythonize
import numpy
import cython_gsl

ext_modules = [
    Extension(
        "como.cython_modules.fast_random",
        ["FILEPATH/fast_random.pyx"],
        libraries=cython_gsl.get_libraries(),
        library_dirs=[cython_gsl.get_library_dir()],
        include_dirs=[numpy.get_include(), cython_gsl.get_include()])
]
ext_modules = cythonize(ext_modules)

url = "URLPATH"

setup(
    name='como',
    description="Comorbidity simulator",
    url=url,
    author='Tom Fleming, Logan Sandar',
    author_email='USER@uw.edu, USER@uw.edu',
    install_requires=[
        "pandas",
        "sqlalchemy",
        "numpy",
        "pymysql",
        "transmogrifier",
        "hierarchies",
        "db_tools"],
    package_data={
        'como': ['config/*', 'dws/combine/*', '__version__.txt']},
    include_package_data=True,
    packages=find_packages(),
    scripts=[
        'bin/run_como',
        'bin/compute_nonfatal',
        'bin/aggregate_nonfatal',
        'bin/summarize_nonfatal',
        'bin/upload_nonfatal',
        'bin/gini_leaf',
        'bin/gini_agg',
        'bin/run_gini'],
    ext_modules=ext_modules,
    cmdclass={'build_ext': build_ext},
)
