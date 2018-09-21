from setuptools import setup
from setuptools.extension import Extension
from distutils.command.build_ext import build_ext
from Cython.Build import cythonize
import numpy
import cython_gsl

ext_modules = [
    Extension(
        "fast_random",
        ["fast_random.pyx"],
        libraries=cython_gsl.get_libraries(),
        library_dirs=[cython_gsl.get_library_dir()],
        include_dirs=[numpy.get_include(), cython_gsl.get_include()]),
    Extension(
        "flatten",
        ["flatten.pyx"])
]
ext_modules = cythonize(ext_modules)

setup(
    name='fast_random',
    ext_modules=ext_modules,
    cmdclass={'build_ext': build_ext})
