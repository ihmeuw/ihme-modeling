"""
Test Tree Node
"""
import pytest
from pandas import DataFrame

from regmod.composite_models import Node


# pylint: disable=redefined-outer-name


@pytest.fixture
def simple_node():
    node = Node("0")
    node.extend(["1", "2"])
    node["1"].extend(["3", "4"])

    return node


@pytest.mark.parametrize("name", ["dummy"])
def test_init_attr(name):
    node = Node(name)
    assert node.name == name
    assert node.parent is None
    assert len(node.children) == 0


def test_attr():
    node = Node("0")
    sub_node = Node("1")
    node.append(sub_node)

    assert node.children[0] is sub_node
    assert sub_node.parent is node


def test_isroot():
    node = Node("0")
    sub_node = Node("1")
    node.append(sub_node)

    assert node.isroot
    assert not sub_node.isroot


def test_isleaf():
    node = Node("0")
    sub_node = Node("1")
    node.append(sub_node)

    assert not node.isleaf
    assert sub_node.isleaf


def test_full_name():
    node = Node("0")
    sub_node = Node("1")
    node.append(sub_node)

    assert node.full_name == "0"
    assert sub_node.full_name == "0/1"


def test_root():
    node = Node("0")
    sub_node = Node("1")
    node.append(sub_node)

    assert sub_node.root is node


def test_leafs():
    node = Node("0")
    node.extend(["1", "2"])

    assert len(node.leafs) == 2


def test_branch(simple_node):
    branch = simple_node.branch
    lower_node_names = set(n.name for n in branch)
    assert lower_node_names == set(["0", "1", "2", "3", "4"])


def test_tree(simple_node):
    tree = simple_node["1"]["3"].tree
    all_node_names = set(n.name for n in tree)
    assert all_node_names == set(["0", "1", "2", "3", "4"])


def test_level(simple_node):
    root_node = simple_node
    leaf_node = simple_node.leafs[0]

    assert root_node.level == 0
    assert leaf_node.level == 2


def test_append(simple_node):
    node = Node("1")
    node.append("a")
    assert node.children[0].name == "a"

    simple_node.append(node)
    assert simple_node["1"].children.named_lists[0][-1].name == "a"


def test_extend(simple_node):
    simple_node["2"].extend(["5", "6"])
    assert set(n.name for n in simple_node["2"].children.values()) == set(["5", "6"])


def test_merge(simple_node):
    node = Node("0")
    node.append("a")

    simple_node.merge(node)
    assert simple_node.children.named_lists[0][-1].name == "a"


def test_pop(simple_node):
    node = simple_node.pop()
    assert node.isroot
    assert len(simple_node.children) == 1


def test_detach(simple_node):
    node = simple_node.children[0]
    node.detach()
    assert node.isroot
    assert len(simple_node.children) == 1


def test_get_name(simple_node):
    node = simple_node["1"]["3"]
    assert node.get_name(0) == "0/1/3"
    assert node.get_name(1) == "1/3"
    assert node.get_name(2) == "3"


def test_getitem(simple_node):
    assert simple_node["1"].name == "1"


def test_len(simple_node):
    assert len(simple_node) == 5


def test_or(simple_node):
    node = Node("0")
    node.append("a")

    result_node = simple_node | node
    assert result_node is simple_node
    assert result_node.children["a"].name == "a"


def test_truediv(simple_node):
    node = simple_node / "a"

    assert node.root is simple_node
    assert simple_node.children["a"].name == "a"


def test_contains(simple_node):
    node = Node("1")
    node.extend(["3", "4"])
    assert node in simple_node


def test_eq(simple_node):
    node = Node("1")
    node.extend(["3", "4"])
    assert node == simple_node["1"]


def test_lt(simple_node):
    node = Node("1")
    node.extend(["3", "4"])
    assert node < simple_node


def test_copy(simple_node):
    node = simple_node.copy()
    assert node is not simple_node
    assert node == simple_node


@pytest.mark.parametrize("obj", [0, "0", Node("0")])
def test_as_node(obj):
    node = Node.as_node(obj)
    assert isinstance(node, Node)
    assert node.name == "0"


def test_from_names():
    node = Node.from_names(["0", "1", "3"])
    leaf = node.leafs[0]
    assert leaf.full_name == "0/1/3"


def test_from_full_names():
    node = Node.from_full_name("0/1/3")
    leaf = node.leafs[0]
    assert leaf.full_name == "0/1/3"


def test_get_leafs(simple_node):
    assert (set(node.name for node in simple_node.leafs) ==
            set(node.name for node in simple_node.get_leafs()))

    assert (set(node.name for node in simple_node.get_leafs(1)) ==
            set(["0", "1", "2", "3", "4"]))
