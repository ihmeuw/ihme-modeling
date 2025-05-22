"""
Customize Collections Class
"""
from collections import OrderedDict
from itertools import islice, chain
from typing import Any, Iterator, Union, Tuple


class NamedList(OrderedDict):
    """Named list, a simple inherent class from OrderedDict.
    """

    def _as_key(self, key: Union[int, str]) -> str:
        """Private function that process the key, if the ``key`` is integer, it
        will be treated as position of the inquired value, and function will
        find and return the corresponding key in the dictionary.

        Parameters
        ----------
        key : Union[int, str]
            Integer index or string key of the inquried value.

        Returns
        -------
        str
            Corresponding key in the dictionary.
        """
        if isinstance(key, int):
            index = key if key >= 0 else len(self) + key
            key = next(islice(self.keys(), index, None))
        return key

    def pop(self, key: Union[int, str] = -1) -> Any:
        """Pop value out of list/dictionary, overwrite the pop function from the
        OrderedDict to accept the integer index.

        Parameters
        ----------
        key : Union[int, str]
            Integer index or string key of the inquried value.

        Returns
        -------
        Any
            Value in the dictionary corresponding to the key.
        """
        return super().pop(self._as_key(key))

    def __iter__(self) -> Iterator:
        """Overwrite __iter__ function from OrderedDict, iterate over values
        instead of keys.

        Returns
        -------
        Iterator
            Iterator of the values of the dictionary.
        """
        return iter(self.values())

    def __getitem__(self, key: Union[int, str]) -> Any:
        """Overwrite __getitem__ function from OrderedDict, allow integer index.

        Parameters
        ----------
        key : Union[int, str]
            Integer index or string key of the inquried value.

        Returns
        -------
        Any
            Value in the dictionary corresponding to the key.
        """
        return super().__getitem__(self._as_key(key))

    def __setitem__(self, key: Union[int, str], value: Any):
        """Overwrite __setitem__ function from OrderedDict, allow integer index.

        Parameters
        ----------
        key : Union[int, str]
            Integer index or string key of the inquried value.
        """
        super().__setitem__(self._as_key(key), value)


class ChainNamedList:
    """A unified view of a set of NamedList

    Attributes
    ----------
    named_lists : List[NamedList]
        A list of NamedList instances.
    """

    def __init__(self, *args):
        """Constructor of ChainNamedList

        Parameters
        ----------
        args : Tuple[NamedList]
            A list of NamedList instances.
        """
        self.named_lists = [
            arg if isinstance(arg, NamedList) else NamedList(arg)
            for arg in args
        ]

    def keys(self) -> Iterator:
        """Get view of keys in named_lists.

        Returns
        -------
        Iterator:
            An iterator of all keys in named_lists.
        """
        return chain.from_iterable(nlst.keys() for nlst in self.named_lists)

    def values(self) -> Iterator:
        """Get view of values in name_lists.

        Returns
        -------
        Iterator:
            An iterator of all values in named_lists.
        """
        return chain.from_iterable(nlst.values() for nlst in self.named_lists)

    def _as_key(self,
                key: Union[int, str],
                raise_keyerror: bool = True) -> Tuple[int, str]:
        """Private function that process the keys. Similar to the one in
        ``NamedList`` transform index to key. Return additional information of
        which list contains the key, allow function to raise ``KeyError`` when
        key is not found.

        Parameters
        ----------
        key : Union[int, str]
            Integer index or string key of the inquried value.
        raise_keyerror: bool
            If ``True``, a ``KeyError`` will be raise when key is not found.

        Raises
        ------
        KeyError
            When ``raise_keyerror`` is ``True`` and key is not found.

        Returns
        -------
        Tuple[int, str]
            Returns the id of the list which contains of the key and the key. If
            key is not found among all lists, return id as -1.
        """
        if isinstance(key, int):
            index = key if key >= 0 else len(self) + key
            key = next(islice(self.keys(), index, None))
        i = next(
            (i for i, nlst in enumerate(self.named_lists) if key in nlst), -1
        )
        if raise_keyerror and i == -1:
            raise KeyError(f"'{key}'")
        return i, key

    def pop(self, key: Union[int, str] = -1) -> Any:
        """Pop value out of list/dictionary.

        Parameters
        ----------
        key : Union[int, str]
            Integer index or string key of the inquried value.

        Returns
        -------
        Any
            Value in the dictionary corresponding to the key.
        """
        i, key = self._as_key(key)
        return self.named_lists[i].pop(key)

    def __contains__(self, key: str) -> bool:
        """Lists contain the key.

        Parameters
        ----------
        key : str
            Key that need to be checked.

        Returns
        -------
        bool
            If lists contain they key return ``True`` else ``False``.
        """
        return any(key in nlst for nlst in self.named_lists)

    def __iter__(self) -> Iterator:
        """Iterate over values of the lists.

        Returns
        -------
        Iterator
            Iterator of the values of the lists.
        """
        return iter(self.values())

    def __getitem__(self, key: Union[int, str]) -> Any:
        """Value inquring that allow integer index.

        Parameters
        ----------
        key : Union[int, str]
            Integer index or string key of the inquried value.

        Returns
        -------
        Any
            Value in the dictionary corresponding to the key.
        """
        i, key = self._as_key(key)
        return self.named_lists[i][key]

    def __setitem__(self, key: Union[int, str], value: Any):
        """Set values, allow integer index.

        Parameters
        ----------
        key : Union[int, str]
            Integer index or string key of the inquried value.
        """
        i, key = self._as_key(key, raise_keyerror=False)
        i = i if i >= 0 else len(self.named_lists) + i
        self.named_lists[i][key] = value

    def __len__(self) -> int:
        """Return the total number of elements in the lists

        Returns
        -------
        int
            Total number of elements in the lists.
        """
        return sum(len(nlst) for nlst in self.named_lists)

    def __repr__(self) -> str:
        head = type(self).__name__
        contents = ", ".join(map(str, self.named_lists))
        return f"{head}({contents})"
