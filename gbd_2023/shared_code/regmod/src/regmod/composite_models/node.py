"""
Tree Node
"""
from functools import reduce
from itertools import chain
from operator import attrgetter, truediv
from typing import Any, Iterable, List, Optional, Union

from regmod.composite_models.collections import ChainNamedList


class Node:
    """Node class contains the hierarchy information.

    Parameters
    ----------
    name : str
        The name of the node.

    Attributes
    ----------
    name : str
        The name of the node.
    parent : Optional[Node]
        The parent node. If `None`, the node is the root node.
    children : ChainNamedList
        The children nodes. There are two compartments, one for primary children
        and one for secondary children.
    isroot : bool
    isleaf : bool
    full_name : str
    root : Node
    leafs : List[Node]
    branch : List[Node]
    tree : List[Node]
    level : int

    Methods
    -------
    append(node, rank=0)
        Append node to the current node.
    extend(nodes, rank=0)
        Append nodes to the current node.
    merge(node)
        Merge current node with other node.
    pop(name=-1, rank=None)
        Pop the children node by name of index.
    detach()
        Detach from the parent.
    get_name(level)
        Get the name up to the given level.
    get_leafs(*ranks)
        Get the leafs with given rank.
    copy()
        Copy the current node.
    """

    def __init__(self, name: str):
        self.name = name
        self.parent = None
        self.children = ChainNamedList({}, {})

    name = property(attrgetter("_name"))

    @name.setter
    def name(self, name: str):
        name = str(name)
        if "/" in name:
            raise ValueError(f"name={name} cannot contain character '/'")
        self._name = name

    @property
    def isroot(self) -> bool:
        """Indicator if the node is the root node."""
        return self.parent is None

    @property
    def isleaf(self) -> bool:
        """Indicator if the node is the leaf node."""
        return len(self.children) == 0

    @property
    def full_name(self) -> str:
        """Full name include all ancestors."""
        return self.get_name(0)

    @property
    def root(self) -> "Node":
        """The root node of current node."""
        if self.isroot:
            return self
        return self.parent.root

    @property
    def leafs(self) -> List["Node"]:
        """The leaf nodes of current node."""
        if self.isleaf:
            return [self]
        return list(chain.from_iterable(
            node.leafs for node in self.children
        ))

    @property
    def branch(self) -> List["Node"]:
        """The branch of current node."""
        if self.isleaf:
            return [self]
        return [self] + list(chain.from_iterable(
            node.branch for node in self.children
        ))

    @property
    def tree(self) -> List["Node"]:
        """The entire tree of current node."""
        return self.root.branch

    @property
    def level(self) -> int:
        """The level of current node, level of a root node is 0."""
        if self.isroot:
            return 0
        return self.parent.level + 1

    def append(self, node: Union[str, "Node"], rank: int = 0):
        """Append node to the current node.

        Parameters
        ----------
        node : Union[str, Node]
            The node need to be appended.
        rank : int, optional
            Indicate which compartment to append to, by default 0.

        Raises
        ------
        ValueError
            Rasied when the node to be appended already have parent.
        """
        node = self.as_node(node)
        if not node.isroot:
            raise ValueError(f"Cannot append {node}, "
                             f"already have parent {node.parent}.")
        if node.name in self.children:
            self.children[node.name].merge(node)
        else:
            node.parent = self
            self.children.named_lists[rank][node.name] = node

    def extend(self, nodes: Iterable[Union[str, "Node"]], rank: int = 0):
        """Append nodes to the current node.

        Parameters
        ----------
        nodes : Iterable[Union[str, Node]]
            The node need to be appended.
        rank : int, optional
            Indicate which compartment to append to, by default 0.
        """
        for node in nodes:
            self.append(node, rank=rank)

    def merge(self, node: Union[str, "Node"]):
        """Merge current node with other node.

        Parameters
        ----------
        node : Union[str, Node]
            Node to be merged with.

        Raises
        ------
        ValueError
            Raised when the other nodes have a different name.
        """
        if node.name != self.name:
            raise ValueError("Cannot merge with node with different name.")
        for rank, children in enumerate(node.children.named_lists):
            while len(children) > 0:
                self.append(node.pop(rank=rank), rank=rank)

    def pop(self,
            name: Union[int, str] = -1,
            rank: Optional[int] = None) -> "Node":
        """Pop the children node by name of index.

        Parameters
        ----------
        name : Union[int, str], optional
            The name or index of the children node. Default to -1 which is the
            last added children node.
        rank : Optional[int], optional
            Indicate which compartment to append to, by default None. When it
            is None, it will automatically find which compartment the name
            belongs to and then pop.

        Returns
        -------
        Node
            Poped children node.
        """
        children = self.children
        if rank is not None:
            children = self.children.named_lists[rank]
        node = children.pop(name)
        node.parent = None
        return node

    def detach(self):
        """Detach from the parent.
        """
        if not self.isroot:
            self.parent.pop(self.name)

    def get_name(self, level: int) -> str:
        """Get the name up to the given level.

        Parameters
        ----------
        level : int
            Given level.

        Returns
        -------
        str
            Name up to the given level.
        """
        if self.level <= level or self.isroot:
            return self.name
        return "/".join([self.parent.get_name(level), self.name])

    def get_leafs(self, *ranks) -> List["Node"]:
        """Get the leafs with given rank.

        Returns
        -------
        List[Node]
            A list of leaf nodes.
        """
        if len(ranks) == 0:
            ranks = range(len(self.children.named_lists))
        leafs = list(chain.from_iterable(
            node.get_leafs(*ranks) for node in self.children
        ))
        if all(len(self.children.named_lists[rank]) == 0 for rank in ranks):
            leafs.insert(0, self)
        return leafs

    def copy(self) -> "Node":
        """Copy the current node.

        Returns
        -------
        Node
            A copy of the current node.
        """
        return self.__copy__()

    def __getitem__(self, name: str) -> "Node":
        names = name.split("/", 1)
        node = self.children[names[0]]
        if len(names) == 1:
            return node
        return node[names[1]]

    def __len__(self) -> int:
        if self.isleaf:
            return 1
        return 1 + sum(len(node) for node in self.children)

    def __or__(self, node: Union[str, "Node"]) -> "Node":
        self.merge(node)
        return self

    def __truediv__(self, node: Union[str, "Node"]) -> "Node":
        rank = 0
        self.append(node, rank=rank)
        return self.children.named_lists[rank][-1]

    def __floordiv__(self, node: Union[str, "Node"]) -> "Node":
        rank = 1
        self.append(node, rank=rank)
        return self.children.named_lists[rank][-1]

    def __contains__(self, node: "Node") -> bool:
        if not isinstance(node, Node):
            raise TypeError("Can only contain Node.")
        if node == self:
            return True
        return any(node in _node for _node in self.children)

    def __eq__(self, node: "Node") -> bool:
        if not isinstance(node, Node):
            raise TypeError("Can only compare to Node.")
        self_names = set(_node.get_name(self.level) for _node in self.branch)
        node_names = set(_node.get_name(node.level) for _node in node.branch)
        return self_names == node_names

    def __lt__(self, node: "Node") -> bool:
        if not isinstance(node, Node):
            raise TypeError("Can only compare to Node.")
        self_names = (_node.get_name(self.level) for _node in self.branch)
        node_names = (_node.get_name(node.level) for _node in node.branch)
        return all(any(self_name in node_name for node_name in node_names)
                   for self_name in self_names)

    def __gt__(self, node: "Node") -> bool:
        return node < self

    def __le__(self, node: "Node") -> bool:
        return node == self or self < node

    def __ge__(self, node: "Node") -> bool:
        return node == self or self > node

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name})"

    def __copy__(self) -> "Node":
        self_node = type(self)(self.name)
        for node in self.children:
            self_node.append(node.__copy__())
        return self_node

    @classmethod
    def as_node(cls, obj: Any) -> "Node":
        """Convert object into instance of Node.

        Parameters
        ----------
        obj : Any
            Object to be converted.

        Returns
        -------
        Node
            Converted node.
        """
        if isinstance(obj, Node):
            return obj
        return cls(str(obj))

    @classmethod
    def from_names(cls, names: Iterable[str]) -> "Node":
        """Create chain of nodes by their names.

        Parameters
        ----------
        names : Iterable[str]
            A list of names.

        Returns
        -------
        Node
            Root node of the chain.
        """
        return reduce(truediv, map(cls.as_node, names)).root

    @classmethod
    def from_full_name(cls, full_name: str) -> "Node":
        """Create chain of nodes by the full name of the leaf node.

        Parameters
        ----------
        full_name : str
            Full name of the leaf node.

        Returns
        -------
        Node
            Root node of the chain.
        """
        names = full_name.split("/")
        return cls.from_names(names)
