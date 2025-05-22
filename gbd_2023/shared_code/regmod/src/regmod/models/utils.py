import numpy as np
from msca.linalg.matrix import asmatrix
from scipy.sparse import csc_matrix
from regmod._typing import NDArray, Matrix


def model_post_init(
    mat: NDArray,
    uvec: NDArray,
    linear_umat: NDArray,
    linear_uvec: NDArray,
) -> tuple[Matrix, Matrix, NDArray]:
    # design matrix
    issparse = mat.size == 0 or ((mat == 0).sum() / mat.size) > 0.95
    if issparse:
        mat = csc_matrix(mat).astype(np.float64)
    mat = asmatrix(mat)

    # constraints
    cmat = np.vstack([np.identity(mat.shape[1]), linear_umat])
    cvec = np.hstack([uvec, linear_uvec])

    index = ~np.isclose(cmat, 0.0).all(axis=1)
    cmat = cmat[index]
    cvec = cvec[:, index]

    if cmat.size > 0:
        scale = np.abs(cmat).max(axis=1)
        cmat = cmat / scale[:, np.newaxis]
        cvec = cvec / scale

    cmat = np.vstack([-cmat[~np.isneginf(cvec[0])], cmat[~np.isposinf(cvec[1])]])
    cvec = np.hstack([-cvec[0][~np.isneginf(cvec[0])], cvec[1][~np.isposinf(cvec[1])]])
    if issparse:
        cmat = csc_matrix(cmat).astype(np.float64)
    cmat = asmatrix(cmat)

    return mat, cmat, cvec
