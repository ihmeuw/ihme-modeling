"""
Test NamedList and ChainNamedList
"""
import pytest

from regmod.composite_models.collections import NamedList, ChainNamedList


# pylint:disable=redefined-outer-name


@pytest.fixture
def nlist():
    return NamedList({"a": 1, "b": 2, "c": 3})


@pytest.fixture
def other_nlist():
    return NamedList({"d": 4, "e": 5, "f": 6})


@pytest.fixture
def chain_nlist(nlist, other_nlist):
    return ChainNamedList(nlist, other_nlist)


def test_default_init():
    nlist = NamedList()
    assert len(nlist) == 0


@pytest.mark.parametrize("key", [-1, 2, "c"])
def test_pop(nlist, key):
    value = nlist.pop(key)
    assert value == 3
    assert len(nlist.keys()) == 2
    assert len(nlist.values()) == 2


@pytest.mark.parametrize("key", [-1, 2, "c"])
def test_getitem(nlist, key):
    assert nlist[key] == 3


@pytest.mark.parametrize("key", [-1, 2, "c"])
def test_setitem(nlist, key):
    nlist[key] = 4
    assert nlist[key] == 4


def test_len(nlist):
    assert len(nlist) == 3


def test_iter(nlist):
    assert list(nlist.values()) == list(i for i in nlist)


def test_chain(chain_nlist):
    assert len(chain_nlist.named_lists) == 2


def test_chain_keys(chain_nlist):
    assert list(chain_nlist.keys()) == ["a", "b", "c", "d", "e", "f"]


def test_chain_values(chain_nlist):
    assert list(chain_nlist.values()) == [1, 2, 3, 4, 5, 6]


def test_chain_pop(chain_nlist):
    assert chain_nlist.pop() == 6
    assert chain_nlist.pop("a") == 1
    assert len(chain_nlist) == 4


def test_chain_contains(chain_nlist):
    assert "a" in chain_nlist
    assert "d" in chain_nlist
    assert "bakka" not in chain_nlist


def test_chain_getitem(chain_nlist):
    assert chain_nlist["a"] == 1
    assert chain_nlist[0] == 1
    assert chain_nlist[-1] == 6
    assert chain_nlist[4] == 5


def test_chain_setitem(chain_nlist):
    chain_nlist["a"] = 100
    chain_nlist[4] = 500
    assert chain_nlist[0] == 100
    assert chain_nlist["e"] == 500


def test_chain_iter(chain_nlist):
    assert list(chain_nlist) == list(chain_nlist.values())
