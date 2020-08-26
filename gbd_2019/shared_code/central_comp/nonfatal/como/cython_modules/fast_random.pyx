# %load_ext cythonmagic
# %%cython -l gsl -l gslcblas

import cython
cimport cython
from cython_gsl cimport *

import numpy as np
cimport numpy as np
from numpy cimport *

# Allocate memory
cdef gsl_rng *r = gsl_rng_alloc(gsl_rng_mt19937)

# Use the GSL random uniform function to generate draws from a bernoulli distribution
def bernoulli(int N, ndarray[double, ndim=1] p):
    cdef:
        Py_ssize_t i, j
        np.ndarray[uint32_t, ndim=2] n = np.empty((N,len(p)), dtype='uint32')

    for i in range(N):
        for j in range(len(p)):
            n[i,j] = gsl_rng_uniform(r) <= p[j]

    return n
