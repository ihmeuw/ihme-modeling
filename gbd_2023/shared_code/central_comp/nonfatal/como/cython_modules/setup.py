from distutils.command.build_ext import build_ext
from setuptools import setup
from setuptools.extension import Extension
from Cython.Build import cythonize
import numpy
import cython_gsl

EXT_MODULES = [
    Extension(
        "fast_random",
        ["fast_random.pyx"],
        libraries=cython_gsl.get_libraries(),
        library_dirs=[cython_gsl.get_library_dir()],
        include_dirs=[numpy.get_include(), cython_gsl.get_include()],
    ),
]

# compile .pyx -> .c file
EXT_MODULES = cythonize(EXT_MODULES)

# compile .c -> .so file
setup(
    name="fast_random",
    ext_modules=EXT_MODULES,
    cmdclass={"build_ext": build_ext},
)
