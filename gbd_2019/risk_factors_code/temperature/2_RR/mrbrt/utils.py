# utility functions for mrbrt class
import numpy as np
import cdd
import ipopt
from xspline import xspline


# different function forms and its jacobians
def linear(mat):

    def F(beta, mat=mat):
        return mat.dot(beta)

    def JF(beta, mat=mat):
        return mat

    return F, JF

def ratioLinear(mat1, mat2):

    def F(beta, mat1=mat1, mat2=mat2):
        return mat1.dot(beta)/mat2.dot(beta)

    def JF(beta, mat1=mat1, mat2=mat2):
        vec1 = mat1.dot(beta)
        vec2 = mat2.dot(beta)
        vec1 = vec1.reshape(vec1.size, 1)
        vec2 = vec2.reshape(vec2.size, 1)
        return mat1/vec2 - mat2*(vec1/vec2**2)

    return F, JF

def logRatioLinear(mat1, mat2):

    def F(beta, mat1=mat1, mat2=mat2):
        return np.log(mat1.dot(beta)/mat2.dot(beta))

    def JF(beta, mat1=mat1, mat2=mat2):
        vec1 = mat1.dot(beta)
        vec2 = mat2.dot(beta)
        vec1 = vec1.reshape(vec1.size, 1)
        vec2 = vec2.reshape(vec2.size, 1)
        return mat1/vec1 - mat2/vec2

    return F, JF

def funcXCov(x_cov, spline_list=[]):
    if 'linear' in x_cov['cov_type']:
        if isinstance(x_cov['mat'], tuple):
            mat1, mat2 = x_cov['mat']
            mat1 = mat1.T
            mat2 = mat2.T
            assert mat1.shape[1] == mat2.shape[1]
        if isinstance(x_cov['mat'], np.ndarray):
            mat = x_cov['mat']
            if mat.ndim == 1:
                mat = mat.reshape(mat.size, 1)
            else:
                mat = mat.T

    if 'spline' in x_cov['cov_type']:
        mat = x_cov['mat']
        spline_id = x_cov['spline_id']
        spline = spline_list[spline_id]

    if x_cov['cov_type'] == 'linear':
        F, JF = linear(mat)

    if x_cov['cov_type'] == 'ratio_linear':
        F, JF = ratioLinear(mat1, mat2)

    if x_cov['cov_type'] == 'log_ratio_linear':
        F, JF = logRatioLinear(mat1, mat2)

    if x_cov['cov_type'] == 'spline':
        mat = spline.designMat(mat)
        F, JF = linear(mat)

    if x_cov['cov_type'] == 'ndspline':
        mat = spline.designMat(mat, grid_off=True)
        F, JF = linear(mat)

    if x_cov['cov_type'] == 'spline_integral':
        mat = normalizedDesignIMat(mat[0], mat[1], spline)
        F, JF = linear(mat)

    if x_cov['cov_type'] == 'diff_spline':
        mat1 = spline.designMat(mat[0])
        mat2 = spline.designMat(mat[1])
        F, JF = linear(mat1 - mat2)

    if x_cov['cov_type'] == 'ratio_spline':
        mat1 = spline.designMat(mat[0])
        mat2 = spline.designMat(mat[1])
        F, JF = ratioLinear(mat1, mat2)

    if x_cov['cov_type'] == 'ratio_spline_integral':
        mat1 = normalizedDesignIMat(mat[0], mat[1], spline)
        mat2 = normalizedDesignIMat(mat[2], mat[3], spline)
        F, JF = ratioLinear(mat1, mat2)

    if x_cov['cov_type'] == 'log_ratio_spline':
        mat1 = spline.designMat(mat[0])
        mat2 = spline.designMat(mat[1])
        F, JF = logRatioLinear(mat1, mat2)

    if x_cov['cov_type'] == 'log_ratio_spline_integral':
        mat1 = normalizedDesignIMat(mat[0], mat[1], spline)
        mat2 = normalizedDesignIMat(mat[2], mat[3], spline)
        F, JF = logRatioLinear(mat1, mat2)

    return F, JF

def funcZCov(z_cov, spline_list=[]):
    if 'linear' in z_cov['cov_type']:   
        mat = z_cov['mat']
        if mat.ndim == 1:
            mat = mat.reshape(mat.size, 1)
        else:
            mat = mat.T

    if 'spline' in z_cov['cov_type']:
        mat = z_cov['mat']
        spline_id = z_cov['spline_id']
        spline = spline_list[spline_id]

    if z_cov['cov_type'] == 'spline':
        mat = spline.designMat(mat)

    if z_cov['cov_type'] == 'ndspline':
        mat = spline.designMat(mat, grid_off=True)

    if z_cov['cov_type'] == 'spline_integral':
        mat = spline.designIMat(mat[0], mat[1], 1)

    return mat

def normalizedDesignIMat(x0, x1, spline):
    dx = x1 - x0

    val_idx = (dx == 0.0)
    int_idx = (dx != 0.0)

    dx = dx.reshape(dx.size, 1)
    mat = np.zeros((dx.size, spline.num_spline_bases))

    if np.any(val_idx):
        mat[val_idx, :] = spline.designMat(x0[val_idx])

    mat[int_idx, :] = spline.designIMat(x0[int_idx], x1[int_idx], 1)/\
        dx[int_idx, :]

    return mat


# priors
def positiveUPrior(n):
    return np.array([[0.0]*n, [np.inf]*n])

def negativeUPrior(n):
    return np.array([[-np.inf]*n, [0.0]*n])

def constantUPrior(c, n):
    return np.array([[c]*n, [c]*n])

def rangeUPrior(a, n):
    return np.array([[a[0]]*n, [a[1]]*n])

def rangeGPrior(a, n):
    return np.array([[a[0]]*n, [a[1]]*n])

def defaultUPrior(n):
    return np.array([[-np.inf]*n, [np.inf]*n])

def defaultGPrior(n):
    return np.array([[0.0]*n, [np.inf]*n])

def defaultLPrior(n):
    return np.array([[0.0]*n, [np.inf]*n])


# random knots
def sampleKnots(t0, tk, k, b=None, d=None, N=1):
    """sample knots given a set of rules"""
    # check input
    assert t0 <= tk
    assert k >= 2

    if d is not None:
        assert d.shape == (k, 2) and sum(d[:, 0]) <= 1.0 and\
            np.all(d >= 0.0) and np.all(d <= 1.0)
    else:
        d = np.repeat(np.array([[0.0, 1.0]]), k, axis=0)

    if b is not None:
        assert b.shape == (k - 1, 2) and\
            np.all(b[:, 0] <= b[:, 1]) and\
            np.all(b[:-1, 1] <= b[1:, 1]) and\
            np.all(b >= 0.0) and np.all(b <= 1.0)
    else:
        b = np.repeat(np.array([[0.0, 1.0]]), k - 1, axis=0)

    d = d*(tk - t0)
    b = b*(tk - t0) + t0
    d[0] += t0
    d[-1] -= tk

    # find vertices of the polyhedron
    D = -colDiffMat(k - 1)
    I = np.identity(k - 1)

    A1 = np.vstack((-D, D))
    A2 = np.vstack((-I, I))

    b1 = np.hstack((-d[:, 0], d[:, 1]))
    b2 = np.hstack((-b[:, 0], b[:, 1]))

    A = np.vstack((A1, A2))
    b = np.hstack((b1, b2))

    mat = np.insert(-A, 0, b, axis=1)
    mat = cdd.Matrix(mat)
    mat.rep_type = cdd.RepType.INEQUALITY
    poly = cdd.Polyhedron(mat)
    ext = poly.get_generators()
    vertices_and_rays = np.array(ext)

    if vertices_and_rays.size == 0:
        print('there is no feasible knots')
        return None

    if np.any(vertices_and_rays[:, 0] == 0.0):
        print('polyhedron is not closed, something is wrong.')
        return None
    else:
        vertices = vertices_and_rays[:, 1:]

    # sample from the convex combination of the vertices
    n = vertices.shape[0]
    s_simplex = sampleSimplex(n, N=N)
    s = s_simplex.dot(vertices)

    s = np.insert(s, 0, t0, axis=1)
    s = np.insert(s, k, tk, axis=1)

    return s


def sampleSimplex(n, N=1):
    """sample from n dimensional simplex"""
    assert n >= 1

    # special case when n == 1
    if n == 1:
        return np.ones((N, n))

    # other cases
    s = np.random.rand(N, n - 1)
    s.sort(axis=1)
    s = np.insert(s, 0, 0.0, axis=1)
    s = np.insert(s, n, 1.0, axis=1)

    w = np.zeros((n + 1, n))
    id_d0 = np.diag_indices(n)
    id_d1 = (id_d0[0] + 1, id_d0[1])
    w[id_d0] = -1.0
    w[id_d1] = 1.0

    return s.dot(w)


def colDiffMat(n):
    """column difference matrix"""
    D = np.zeros((n + 1, n))
    id_d0 = np.diag_indices(n)
    id_d1 = (id_d0[0] + 1, id_d0[1])
    D[id_d0] = -1.0
    D[id_d1] = 1.0

    return D

def scoreMR_datafit(mr):
    """score the result of mrbert"""
    if mr.lt.soln is None:
        print('must optimize MR first!')
        return None

    return -mr.lt.objective(mr.lt.soln)

def scoreMR_TV(mr, n=1):
    """score the result of mrbert"""
    if mr.lt.soln is None:
        print('must optimize MR first!')
        return None

    spline = mr.spline_list[0]
    x = np.linspace(spline.knots[0], spline.knots[-1], 201)
    dmat = spline.designDMat(x, n)
    d = dmat.dot(mr.beta_soln[mr.id_spline_beta_list[0]])
    return -np.mean(np.abs(d))

# def scoreMR_D2LogRR(mr):
#     """score the result of mrbrt"""
#     if mr.lt.soln is None:
#         print('must optimize MR first!')
#         return None

#     spline = mr.spline_list[0]

#     x = np.linspace(spline.knots[0], spline.knots[-1], 201)
#     y = spline.designMat(x).dot(mr.beta_soln)
#     dy = spline.designDMat(x, 1).dot(mr.beta_soln)
#     ddy = spline.designDMat(x, 2).dot(mr.beta_soln)

#     d = (ddy*y - dy**2)/y**2
#     return -np.mean(abs(d))

def scores2weights(score_set):
    """convert the score_set to weights"""
    offset = 1.0
    score_min = np.min(score_set) - offset
    score_max = np.max(score_set)
    weights = (score_set - score_min)/(score_max - score_min)

    return weights/np.sum(weights)

def nonLinearTrans(score, slope=6.0, quantile=0.7):
    score_min = np.min(score)
    score_max = np.max(score)
    weight = (score - score_min)/(score_max - score_min)


    sorted_weight = np.sort(weight)
    x = sorted_weight[int(0.8*weight.size)]
    y = 1.0 - x

    # calculate the transformation coefficient
    c = np.zeros(4)
    c[1] = slope*x**2/quantile
    c[0] = quantile*np.exp(c[1]/x)
    c[3] = slope*y**2/(1.0 - quantile)
    c[2] = (1.0 - quantile)*np.exp(c[3]/y)

    weight_trans = np.zeros(weight.size)

    for i in range(weight.size):
        w = weight[i]
        if w == 0.0:
            weight_trans[i] = 0.0
        elif w < x:
            weight_trans[i] = c[0]*np.exp(-c[1]/w)
        elif w < 1.0:
            weight_trans[i] = 1.0 - c[2]*np.exp(-c[3]/(1.0 - w))
        else:
            weight_trans[i] = 1.0

    weight_trans = (weight_trans - np.min(weight_trans))/\
        (np.max(weight_trans) - np.min(weight_trans))

    return weight_trans

# initialization
def ratioInit(mr, x_cov_id):
    x_cov = mr.x_cov_list[x_cov_id]

    Y = mr.obs_mean

    if x_cov['cov_type'] == 'ratio_spline':
        x = x_cov['mat']
        spline = mr.spline_list[x_cov['spline_id']]
        mat1 = spline.designMat(x[0])
        mat2 = spline.designMat(x[1])
    elif x_cov['cov_type'] == 'ratio_spline_integral':
        x = x_cov['mat']
        spline = mr.spline_list[x_cov['spline_id']]
        mat1 = normalizedDesignIMat(x[0], x[1], spline)
        mat2 = normalizedDesignIMat(x[2], x[3], spline)
    elif x_cov['cov_type'] == 'log_ratio_spline':
        Y = np.exp(Y)
        x = x_cov['mat']
        spline = mr.spline_list[x_cov['spline_id']]
        mat1 = spline.designMat(x[0])
        mat2 = spline.designMat(x[1])
    elif x_cov['cov_type'] == 'log_ratio_spline_integral':
        Y = np.exp(Y)
        x = x_cov['mat']
        spline = mr.spline_list[x_cov['spline_id']]
        mat1 = normalizedDesignIMat(x[0], x[1], spline)
        mat2 = normalizedDesignIMat(x[2], x[3], spline)
    elif x_cov['cov_type'] == 'ratio_linear':
        mat1 = x_cov['mat'][0].T
        mat2 = x_cov['mat'][1].T
    elif x_cov['cov_type'] == 'log_ratio_linear':
        Y = np.exp(Y)
        mat1 = x_cov['mat'][0].T
        mat2 = x_cov['mat'][1].T
    else:
        print('unsupported covariance type')
        return None

    ipopt_obj = ratioInitIP(Y, mat1, mat2)
    beta0_sub = ipopt_obj.initialize()
    beta0_sub /= np.linalg.norm(beta0_sub)

    beta0 = np.zeros(mr.k_beta)
    beta0[mr.id_beta_list[x_cov_id]] = beta0_sub

    gamma0 = np.repeat(0.1, mr.k_gamma)

    return np.hstack((beta0, gamma0))

class ratioInitIP:
    def __init__(self, Y, X1, X2, lam=0.5):
        self.Y = Y
        self.X1 = X1
        self.X2 = X2
        self.lam = lam

        self.k = X1.shape[1]
        self.N = Y.size

        self.M = X2*Y.reshape(self.N, 1) - X1
        self.C = np.vstack((X1, X2))
        self.c = np.array([[1.0]*(2*self.N), [np.inf]*(2*self.N)])

    def objective(self, beta):
        R = self.M.dot(beta)
        return 0.5*np.sum(R**2) + 0.5*self.lam*np.sum(beta**2)

    def gradient(self, beta):
        return self.M.T.dot(self.M.dot(beta)) + self.lam*beta

    def constraints(self, beta):
        return self.C.dot(beta)

    def jacobian(self, beta):
        return self.C

    def initialize(self, beta0=None, print_level=0, max_iter=20):
        if beta0 is None:
            beta0 = np.repeat(0.1, self.k)

        assert beta0.size == self.k

        opt_problem = ipopt.problem(
            n=self.k,
            m=2*self.N,
            problem_obj=self,
            cl=self.c[0],
            cu=self.c[1]
            )

        opt_problem.addOption('print_level', print_level)
        opt_problem.addOption('max_iter', max_iter)

        beta_soln, info = opt_problem.solve(beta0)
        self.beta_soln = beta_soln
        self.info = info

        return beta_soln

# construct x cov, z cov and prior
def constructXCov(x_cov_list, spline_list=[]):
    """
    Construct the x cov F and JF
    """
    F_list = []
    JF_list = []
    id_beta_list = []
    id_spline_beta_list = [None for spline in spline_list]
    k_beta_list = []

    k_beta = 0

    # extract number of data point
    mat = x_cov_list[0]['mat']
    if isinstance(mat, tuple):
        mat = mat[0]
    if mat.ndim == 1:
        N = mat.size
    else:
        N = mat.shape[1]

    for x_cov in x_cov_list:
        # update beta index
        if 'linear' in x_cov['cov_type']:
            if isinstance(x_cov['mat'], tuple):
                curr_k_beta = x_cov['mat'][0].shape[0]
            if isinstance(x_cov['mat'], np.ndarray):
                if x_cov['mat'].ndim == 1:
                    curr_k_beta = 1
                else:
                    curr_k_beta = x_cov['mat'].shape[0]

            id_beta_list.append(slice(k_beta, k_beta + curr_k_beta))
            k_beta += curr_k_beta

        if 'spline' in x_cov['cov_type']:
            spline_id = x_cov['spline_id']
            spline = spline_list[spline_id]
            curr_k_beta = spline.num_spline_bases
            if id_spline_beta_list[spline_id] is None:
                id_beta_list.append(slice(k_beta, k_beta + curr_k_beta))
                k_beta += curr_k_beta
                id_spline_beta_list[spline_id] = id_beta_list[-1]
            else:
                id_beta_list.append(id_spline_beta_list[spline_id])

        k_beta_list.append(curr_k_beta)

        # create F and JF
        F_sub, JF_sub = funcXCov(x_cov, spline_list=spline_list)

        F_list.append(F_sub)
        JF_list.append(JF_sub)

    def F(beta):
        f = np.zeros(N, dtype=beta.dtype)
        for i in range(len(x_cov_list)):
            f += F_list[i](beta[id_beta_list[i]])

        return f

    def JF(beta):
        jf = np.zeros((N, k_beta), dtype=beta.dtype)
        for i in range(len(x_cov_list)):
            jf[:, id_beta_list[i]] += JF_list[i](beta[id_beta_list[i]])

        return jf

    return F, JF, F_list, JF_list,\
        id_beta_list, id_spline_beta_list, k_beta_list

def constructZCov(z_cov_list, spline_list=[]):
    """
    Construct the z cov Z
    """
    Z_list = []
    id_gamma_list = []
    id_spline_gamma_list = [None for spline in spline_list]
    k_gamma_list = []

    k_gamma = 0

    # extract number of data point
    mat = z_cov_list[0]['mat']
    if isinstance(mat, tuple):
        mat = mat[0]
    if mat.ndim == 1:
        N = mat.size
    else:
        N = mat.shape[1]

    for z_cov in z_cov_list:
        # update gamma index
        if 'linear' in z_cov['cov_type']:
            if z_cov['mat'].ndim == 1:
                curr_k_gamma = 1
            else:
                curr_k_gamma = z_cov['mat'].shape[0]
            id_gamma_list.append(slice(k_gamma, k_gamma + curr_k_gamma))
            k_gamma += curr_k_gamma

        if 'spline' in z_cov['cov_type']:
            spline_id = z_cov['spline_id']
            spline = spline_list[spline_id]
            curr_k_gamma = spline.num_spline_bases
            if id_spline_gamma_list[spline_id] is None:
                curr_k_gamma = spline.num_spline_bases
                id_gamma_list.append(slice(k_gamma, k_gamma + curr_k_gamma))
                k_gamma += curr_k_gamma
                id_spline_gamma_list[spline_id] = id_gamma_list[-1]
            else:
                id_gamma_list.append(id_spline_gamma_list[spline_id])

        k_gamma_list.append(curr_k_gamma)
        # create Z
        Z_sub = funcZCov(z_cov, spline_list=spline_list)
        
        Z_list.append(Z_sub)

    Z = np.hstack(Z_list)

    return Z, Z_list,\
        id_gamma_list, id_spline_gamma_list, k_gamma_list

def constructPrior(prior_list, mr):
    if not prior_list:
        return None, None, None, None, None, None, None, None, None,\
            [], [], [], [], [], [], [], [], [], [], [], []

    C_list, JC_list, c_list = [], [], []
    H_list, JH_list, h_list = [], [], []
    uprior, gprior, lprior = None, None, None

    num_constraints = 0
    num_regularizers = 0
    num_constraints_list = []
    num_regularizers_list = []

    id_C_list = []
    id_H_list = []
    id_C_var_list = []
    id_H_var_list = []

    for prior in mr.prior_list:
        C_sub, JC_sub, c_sub = None, None, None
        H_sub, JH_sub, h_sub = None, None, None

        if 'spline' in prior['prior_type']:
            spline_id = mr.x_cov_list[prior['x_cov_id']]['spline_id']
            spline = mr.spline_list[spline_id]

        if prior['prior_type'] == 'spline_shape_function_uprior' or\
            prior['prior_type'] == 'spline_shape_function_gprior' or\
            prior['prior_type'] == 'spline_shape_derivative_uprior' or\
            prior['prior_type'] == 'spline_shape_derivative_gprior' or\
            prior['prior_type'] == 'spline_shape_monotonicity' or\
            prior['prior_type'] == 'spline_shape_convexity':

            x = np.linspace(prior['interval'][0],
                            prior['interval'][1],
                            prior['num_points'])


        if prior['prior_type'] == 'spline_shape_function_uprior' or\
            prior['prior_type'] == 'spline_shape_derivative_uprior' or\
            prior['prior_type'] == 'spline_shape_monotonicity' or\
            prior['prior_type'] == 'spline_shape_convexity':

            id_C_list.append(slice(num_constraints,
                                   num_constraints + prior['num_points']))
            num_constraints += prior['num_points']
            num_constraints_list.append(prior['num_points'])
            id_C_var_list.append(mr.id_spline_beta_list[spline_id])

        if prior['prior_type'] == 'spline_shape_function_gprior' or\
            prior['prior_type'] == 'spline_shape_derivative_gprior':

            id_H_list.append(slice(num_regularizers,
                                   num_regularizers + prior['num_points']))
            num_regularizers += prior['num_points']
            num_regularizers_list.append(prior['num_points'])
            id_H_var_list.append(mr.id_spline_beta_list[spline_id])

        if prior['prior_type'] == 'ndspline_shape_function_uprior':
            interval = prior['interval']
            num_points = prior['num_points']
            x = [np.linspace(interval[i][0], interval[i][1], num_points[i])
                 for i in range(spline.ndim)]
            id_C_list.append(slice(num_constraints,
                                   num_constraints + np.prod(num_points)))
            num_constraints += np.prod(num_points)
            num_constraints_list.append(np.prod(num_points))
            id_C_var_list.append(mr.id_spline_beta_list[spline_id])

            mat = spline.designMat(x)
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            c_sub = rangeUPrior(prior['indicator'], mat.shape[0])

        if prior['prior_type'] == 'ndspline_shape_function_gprior':
            interval = prior['interval']
            num_points = prior['num_points']
            x = [np.linspace(interval[i][0], interval[i][1], num_points[i])
                 for i in range(spline.ndim)]
            id_H_list.append(slice(num_regularizers,
                                   num_regularizers + np.prod(num_points)))
            num_regularizers += np.prod(num_points)
            num_regularizers_list.append(np.prod(num_points))
            id_H_var_list.append(mr.id_spline_beta_list[spline_id])

            mat = spline.designMat(x)
            H_sub = lambda beta, mat=mat: mat.dot(beta)
            JH_sub = lambda beta, mat=mat: mat
            h_sub = rangeGPrior(prior['indicator'], mat.shape[0])

        if prior['prior_type'] == 'ndspline_shape_derivative_uprior':
            interval = prior['interval']
            num_points = prior['num_points']
            x = [np.linspace(interval[i][0], interval[i][1], num_points[i])
                 for i in range(spline.ndim)]
            id_C_list.append(slice(num_constraints,
                                   num_constraints + np.prod(num_points)))
            num_constraints += np.prod(num_points)
            num_constraints_list.append(np.prod(num_points))
            id_C_var_list.append(mr.id_spline_beta_list[spline_id])

            n = [0]*spline.ndim
            n[prior['dim_id']] = 1
            mat = spline.designDMat(x, n)
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            c_sub = rangeUPrior(prior['indicator'], mat.shape[0])

        if prior['prior_type'] == 'ndspline_shape_derivative_gprior':
            interval = prior['interval']
            num_points = prior['num_points']
            x = [np.linspace(interval[i][0], interval[i][1], num_points[i])
                 for i in range(spline.ndim)]
            id_H_list.append(slice(num_regularizers,
                                   num_regularizers + np.prod(num_points)))
            num_regularizers += np.prod(num_points)
            num_regularizers_list.append(np.prod(num_points))
            id_H_var_list.append(mr.id_spline_beta_list[spline_id])

            n = [0]*spline.ndim
            n[prior['dim_id']] = 1
            mat = spline.designDMat(x, n)
            H_sub = lambda beta, mat=mat: mat.dot(beta)
            JH_sub = lambda beta, mat=mat: mat
            h_sub = rangeGPrior(prior['indicator'], mat.shape[0])

        if prior['prior_type'] == 'ndspline_shape_monotonicity':
            interval = prior['interval']
            num_points = prior['num_points']
            x = [np.linspace(interval[i][0], interval[i][1], num_points[i])
                 for i in range(spline.ndim)]
            id_C_list.append(slice(num_constraints,
                                   num_constraints + np.prod(num_points)))
            num_constraints += np.prod(num_points)
            num_constraints_list.append(np.prod(num_points))
            id_C_var_list.append(mr.id_spline_beta_list[spline_id])

            n = [0]*spline.ndim
            n[prior['dim_id']] = 1
            mat = spline.designDMat(x, n)
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            if prior['indicator'] == 'increasing':
                c_sub = positiveUPrior(mat.shape[0])
            if prior['indicator'] == 'decreasing':
                c_sub = negativeUPrior(mat.shape[0])

        if prior['prior_type'] == 'ndspline_shape_convexity':
            interval = prior['interval']
            num_points = prior['num_points']
            x = [np.linspace(interval[i][0], interval[i][1], num_points[i])
                 for i in range(spline.ndim)]
            id_C_list.append(slice(num_constraints,
                                   num_constraints + np.prod(num_points)))
            num_constraints += np.prod(num_points)
            num_constraints_list.append(np.prod(num_points))
            id_C_var_list.append(mr.id_spline_beta_list[spline_id])

            n = [0]*spline.ndim
            n[prior['dim_id']] = 2
            mat = spline.designDMat(x, n)
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            if prior['indicator'] == 'convex':
                c_sub = positiveUPrior(mat.shape[0])
            if prior['indicator'] == 'concave':
                c_sub = negativeUPrior(mat.shape[0])

        if prior['prior_type'] == 'spline_shape_function_uprior':
            mat = spline.designMat(x)
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            c_sub = rangeUPrior(prior['indicator'], mat.shape[0])

        if prior['prior_type'] == 'spline_shape_function_gprior':
            mat = spline.designMat(x)
            H_sub = lambda beta, mat=mat: mat.dot(beta)
            JH_sub = lambda beta, mat=mat: mat
            h_sub = rangeGPrior(prior['indicator'], mat.shape[0])

        if prior['prior_type'] == 'spline_shape_derivative_uprior':
            mat = spline.designDMat(x, 1)
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            c_sub = rangeUPrior(prior['indicator'], mat.shape[0])

        if prior['prior_type'] == 'spline_shape_derivative_gprior':
            mat = spline.designDMat(x, 1)
            H_sub = lambda beta, mat=mat: mat.dot(beta)
            JH_sub = lambda beta, mat=mat: mat
            h_sub = rangeGPrior(prior['indicator'], mat.shape[0])

        if prior['prior_type'] == 'spline_shape_monotonicity':
            mat = spline.designDMat(x, 1)
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            if prior['indicator'] == 'increasing':
                c_sub = positiveUPrior(mat.shape[0])
            if prior['indicator'] == 'decreasing':
                c_sub = negativeUPrior(mat.shape[0])

        if prior['prior_type'] == 'spline_shape_convexity':
            mat = spline.designDMat(x, 2)
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            if prior['indicator'] == 'convex':
                c_sub = positiveUPrior(mat.shape[0])
            if prior['indicator'] == 'concave':
                c_sub = negativeUPrior(mat.shape[0])

        if prior['prior_type'] == 'spline_shape_max_derivative_uprior':
            id_C_list.append(slice(num_constraints,
                                   num_constraints + spline.num_invls))
            num_constraints += spline.num_invls
            num_constraints_list.append(spline.num_invls)
            id_C_var_list.append(mr.id_beta_list[prior['x_cov_id']])

            mat = spline.lastDMat()
            C_sub = lambda beta, mat=mat: mat.dot(beta)
            JC_sub = lambda beta, mat=mat: mat
            c_sub = prior['prior']

        if prior['prior_type'] == 'spline_shape_max_derivative_gprior':
            id_H_list.append(slice(num_regularizers,
                                   num_regularizers + spline.num_invls))
            num_regularizers += spline.num_invls
            num_regularizers_list.append(spline.num_invls)
            id_H_var_list.append(mr.id_beta_list[prior['x_cov_id']])

            mat = spline.lastDMat()
            def H_sub(beta, mat=mat):
                return mat.dot(beta)
            JH_sub = lambda beta, mat=mat: mat
            h_sub = prior['prior']

        if prior['prior_type'] == 'spline_normalize_beta':
            id_C_list.append(slice(num_constraints, num_constraints + 1))
            num_constraints += 1
            num_constraints_list.append(1)
            id_C_var_list.append(mr.id_spline_beta_list[spline_id])

            C_sub = lambda beta: np.sum(beta**2)
            JC_sub = lambda beta: 2.0*beta
            c_sub = constantUPrior(1.0, 1)

        if prior['prior_type'] == 'x_cov_constraint':
            id_C_list.append(slice(num_constraints,
                                   num_constraints + prior['num_constraints']))
            num_constraints += prior['num_constraints']
            num_constraints_list.append(prior['num_constraints'])
            id_C_var_list.append(mr.id_beta_list[prior['x_cov_id']])

            C_sub = prior['C']
            JC_sub = prior['JC']
            c_sub = prior['c']

        if prior['prior_type'] == 'x_cov_regularizer':
            id_H_list.append(
                slice(num_constraints,
                      num_constraints + prior['num_regularizers']))
            num_regularizers += prior['num_regularizers']
            num_regularizers_list.append(prior['num_regularizers'])
            id_H_var_list.append(mr.id_beta_list[prior['x_cov_id']])

            H_sub = prior['H']
            JH_sub = prior['JH']
            h_sub = prior['h']

        if prior['prior_type'] == 'z_cov_constraint':
            id_C_list.append(slice(num_constraints,
                                   num_constraints + prior['num_constraints']))
            num_constraints += prior['num_constraints']
            num_constraints_list.append(prior['num_constraints'])
            gamma_id = mr.id_gamma_list[prior['z_cov_id']]
            gamma_id = slice(mr.k_beta + gamma_id.start,
                             mr.k_beta + gamma_id.stop)

            id_C_var_list.append(gamma_id)

            C_sub = prior['C']
            JC_sub = prior['JC']
            c_sub = prior['c']

        if prior['prior_type'] == 'z_cov_regularizer':
            id_H_list.append(
                slice(num_constraints,
                      num_constraints + prior['num_regularizers']))
            num_regularizers += prior['num_regularizers']
            num_regularizers_list.append(prior['num_regularizers'])
            gamma_id = mr.id_gamma_list[prior['z_cov_id']]
            gamma_id = slice(mr.k_beta + gamma_id.start,
                             mr.k_beta + gamma_id.stop)

            id_H_var_list.append(gamma_id)

            H_sub = prior['H']
            JH_sub = prior['JH']
            h_sub = prior['h']

        if prior['prior_type'] == 'general_constraint':
            id_C_list.append(slice(num_constraints,
                                   num_constraints + prior['num_constraints']))
            num_constraints += prior['num_constraints']
            num_constraints_list.append(prior['num_constraints'])
            id_C_var_list.append(slice(0, mr.k))

            C_sub = prior['C']
            JC_sub = prior['JC']
            c_sub = prior['c']

        if prior['prior_type'] == 'general_regularizer':
            id_H_list.append(
                slice(num_constraints,
                      num_constraints + prior['num_regularizers']))
            num_regularizers += prior['num_regularizers']
            num_regularizers_list.append(prior['num_regularizers'])
            id_H_var_list.append(slice(0, mr.k))

            H_sub = prior['H']
            JH_sub = prior['JH']
            h_sub = prior['h']

        if C_sub is not None:
            C_list.append(C_sub)
            JC_list.append(JC_sub)
            c_list.append(c_sub)

        if H_sub is not None:
            H_list.append(H_sub)
            JH_list.append(JH_sub)
            h_list.append(h_sub)

        if prior['prior_type'] == 'x_cov_uprior':
            if uprior is None:
                uprior = defaultUPrior(mr.k)

            uprior[:, mr.id_beta_list[prior['x_cov_id']]] = prior['prior']

        if prior['prior_type'] == 'x_cov_gprior':
            if gprior is None:
                gprior = defaultGPrior(mr.k)

            gprior[:, mr.id_beta_list[prior['x_cov_id']]] = prior['prior']

        if prior['prior_type'] == 'x_cov_lprior':
            if lprior is None:
                lprior = defaultLPrior(mr.k)

            lprior[:, mr.id_beta_list[prior['x_cov_id']]] = prior['prior']

        if prior['prior_type'] == 'z_cov_uprior':
            if uprior is None:
                uprior = defaultUPrior(mr.k)

            gamma_id = mr.id_gamma_list[prior['z_cov_id']]
            gamma_id = slice(mr.k_beta + gamma_id.start,
                             mr.k_beta + gamma_id.stop)

            uprior[:, gamma_id] = prior['prior']

        if prior['prior_type'] == 'z_cov_gprior':
            if gprior is None:
                gprior = defaultGPrior(mr.k)

            gamma_id = mr.id_gamma_list[prior['z_cov_id']]
            gamma_id = slice(mr.k_beta + gamma_id.start,
                             mr.k_beta + gamma_id.stop)

            gprior[:, gamma_id] = prior['prior']

        if prior['prior_type'] == 'z_cov_lprior':
            if lprior is None:
                lprior = defaultLPrior(mr.k)

            gamma_id = mr.id_gamma_list[prior['z_cov_id']]
            gamma_id = slice(mr.k_beta + gamma_id.start,
                             mr.k_beta + gamma_id.stop)

            lprior[:, gamma_id] = prior['prior']

    if not C_list:
        C, JC, c = None, None, None
    else:
        def C(x):
            vec = np.zeros(num_constraints)
            for i in range(len(C_list)):
                vec[id_C_list[i]] = C_list[i](x[id_C_var_list[i]])

            return vec

        def JC(x):
            mat = np.zeros((num_constraints, mr.k))
            for i in range(len(C_list)):
                mat[id_C_list[i], id_C_var_list[i]] =\
                    JC_list[i](x[id_C_var_list[i]])

            return mat

        c = np.hstack(c_list)

    if not H_list:
        H, JH, h = None, None, None
    else:
        def H(x):
            vec = np.zeros(num_regularizers)
            for i in range(len(H_list)):
                vec[id_H_list[i]] = H_list[i](x[id_H_var_list[i]])

            return vec

        def JH(x):
            mat = np.zeros((num_regularizers, mr.k))
            for i in range(len(H_list)):
                mat[id_H_list[i], id_H_var_list[i]] =\
                    JH_list[i](x[id_H_var_list[i]])

            return mat

        h = np.hstack(h_list)

    return C, JC, c, H, JH, h, uprior, gprior, lprior,\
        C_list, JC_list, c_list,\
        H_list, JH_list, h_list,\
        id_C_list, id_C_var_list,\
        id_H_list, id_H_var_list,\
        num_constraints_list, num_regularizers_list


# construct predict x cov and z cov
def constructPredXCov(x_cov_list, mr):
    """
    Construct the pred x cov F and JF
    """
    F_list = []
    JF_list = []
    id_beta_list = []

    # extract number of data point
    mat = x_cov_list[0]['mat']
    if isinstance(mat, tuple):
        mat = mat[0]
    if mat.ndim == 1:
        N = mat.size
    else:
        N = mat.shape[1]

    for x_cov in x_cov_list:
        # update beta index
        id_beta_list.append(mr.id_beta_list[x_cov['x_cov_id']])
        # create F and JF
        F_sub, JF_sub = funcXCov(x_cov, spline_list=mr.spline_list)

        F_list.append(F_sub)
        JF_list.append(JF_sub)

    def F(beta):
        f = np.zeros(N, dtype=beta.dtype)
        for i in range(len(x_cov_list)):
            f += F_list[i](beta[id_beta_list[i]])

        return f

    def JF(beta):
        jf = np.zeros((N, k_beta), dtype=beta.dtype)
        for i in range(len(x_cov_list)):
            jf[:, id_beta_list[i]] += JF_list[i](beta[id_beta_list[i]])

        return jf

    return F, JF, F_list, JF_list, id_beta_list


def constructPredZCov(z_cov_list, mr):
    """
    Construct the z cov Z
    """
    Z_list = []
    id_gamma_list = []

    # extract number of data point
    mat = z_cov_list[0]['mat']
    if isinstance(mat, tuple):
        mat = mat[0]
    if mat.ndim == 1:
        N = mat.size
    else:
        N = mat.shape[1]

    for z_cov in z_cov_list:
        # update gamma index
        id_gamma_list.append(mr.id_gamma_list[z_cov['z_cov_id']])
        # create Z
        Z_sub = funcZCov(z_cov, spline_list=mr.spline_list)
        
        Z_list.append(Z_sub)

    Z = np.hstack(Z_list)

    return Z, Z_list, id_gamma_list
