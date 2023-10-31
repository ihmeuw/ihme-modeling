import pytest
from datetime import datetime

from codem.joblaunch.profiling import get_all_parameters


def test_nonexistent_cause():
    with(pytest.raises(RuntimeError)):
        get_all_parameters(cause_ids=[100000])


def test_codem_jobs():
    params = get_all_parameters(cause_ids=686, hybridizer=False)
    assert 686 in params
    assert all([i in params[686] for i in [1, 2]])
    assert all([i in params[686][j] for i, j in zip([3, 3], [1, 2])])
    assert all([4 in params[686][j][i] for i, j in zip([3, 3], [1, 2])])
    assert all([i in params[686][1][3][4] for i in
                ['cores_requested', 'ram_gb', 'ram_gb_requested', 'runtime_min']])


def test_hybrid_jobs():
    params = get_all_parameters(cause_ids=686, hybridizer=True)
    assert 686 in params
    assert 4 in params[686][3][3]
    assert all([i in params[686][3][3][4] for i in
                ['cores_requested', 'ram_gb', 'ram_gb_requested', 'runtime_min']])
