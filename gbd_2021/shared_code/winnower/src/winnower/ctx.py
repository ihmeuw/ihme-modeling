
import sys
from attr import (
    attrs,
    attrib,
)

from .globals import _session_ctx_stack, _extraction_ctx_stack
# TODO: The following is probably not needed on python3
# from flask._compat import BROKEN_PYPY_CTXMGR_EXIT, reraise

# Certain versions of pypy have a bug where clearing the exception stack
# breaks the __exit__ function in a very peculiar way.  The second level of
# exception blocks is necessary because pypy seems to forget to check if an
# exception happened until the next bytecode instruction?
#
# Relevant PyPy bugfix commit:
# https://bitbucket.org/pypy/pypy/commits/77ecf91c635a287e88e60d8ddb0f4e9df4003301
# Expected to be released in PyPy2 2.3 and later versions.
#
# Ubuntu 14.04 has PyPy 2.2.1, which does exhibit this bug.
# BROKEN_PYPY_CTXMGR_EXIT = False
# if hasattr(sys, 'pypy_version_info'):
#    class _Mgr(object):
#        def __enter__(self):
#            return self
#        def __exit__(self, *args):
#            if hasattr(sys, 'exc_clear'):
#                # Python 3 (PyPy3) doesn't have exc_clear
#                sys.exc_clear()
#    try:
#        try:
#            with _Mgr():
#                raise AssertionError()
#        except:
#            raise
#    except TypeError:
#        BROKEN_PYPY_CTXMGR_EXIT = True
#    except AssertionError:
#        pass


# a singleton sentinel value for parameter defaults
_sentinel = object()


class WinnowerExtractionContext(object):
    """The extraction context binds an extractor object implicitly
    to the current thread or greenlet.
    """

    def __init__(self, winnower_extraction=None, extraction_globals={}):
        self.winnower_extraction = winnower_extraction
        self.g = _WinnowerExtractionCtxGlobals(**extraction_globals)

        # winnower extraction contexts can be pushed multiple times
        # but there a basic "refcount" is enough to track them.
        self._refcnt = 0

    def has_extractor(self):
        """
        Checks to see if this is a dummy Extraction Context.

        A dummy Extraction Context is created when winnower is used without
        first creating an Extractor Context. This can happen when run_extract
        is called for example and is meant to be used in situations where
        multiple extraction contexts are unlikely to be needed. Extraction
        Globals are created with default values.
        """
        if self.winnower_extraction is None:
            return True
        return False

    def push(self):
        """Binds the winnower extraction context to the current context."""
        self._refcnt += 1
        if hasattr(sys, 'exc_clear'):
            sys.exc_clear()
        _extraction_ctx_stack.push(self)

    def pop(self, exc=_sentinel):
        """Pops the extraction context."""
        try:
            self._refcnt -= 1
            if self._refcnt <= 0:
                if exc is _sentinel:
                    exc = sys.exc_info()[1]
                # self.app.do_teardown_appcontext(exc)
        finally:
            rv = _extraction_ctx_stack.pop()

        fail_msg = ("Popped wrong winnower extraction context. "
                    f"({rv!r} instead of {self!r}")
        assert rv is self, fail_msg

    def __enter__(self):
        self.push()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.pop(exc_value)

        # if BROKEN_PYPY_CTXMGR_EXIT and exc_type is not None:
        #     reraise(exc_type, exc_value, tb)


class WinnowerSessionContext(object):
    """The session context binds an session object implicitly
    to the current thread or greenlet.
    """

    def __init__(self, winnower_session, session_globals={}):
        self.winnower_session = winnower_session
        self.g = _WinnowerSessionCtxGlobals(**session_globals)

        # winnower session contexts can be pushed multiple times
        # but there a basic "refcount" is enough to track them.
        self._refcnt = 0

    def push(self):
        """Binds the winnower session context to the current context."""
        self._refcnt += 1
        if hasattr(sys, 'exc_clear'):
            sys.exc_clear()
        _session_ctx_stack.push(self)

    def pop(self, exc=_sentinel):
        """Pops the app context."""
        try:
            self._refcnt -= 1
            if self._refcnt <= 0:
                if exc is _sentinel:
                    exc = sys.exc_info()[1]
                # self.app.do_teardown_appcontext(exc)
        finally:
            rv = _session_ctx_stack.pop()

        fail_msg = ("Popped wrong winnower session context. "
                    f"({rv!r} instead of {self!r}")
        assert rv is self, fail_msg

    def __enter__(self):
        self.push()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.pop(exc_value)

        # if BROKEN_PYPY_CTXMGR_EXIT and exc_type is not None:
        #     reraise(exc_type, exc_value, tb)


class _WinnowerCtxGlobalsBase(object):
    """A plain object. Used as a namespace for storing data during a
    winnower session or extraction context.
    Creating an session context automatically creates this object.
    .. describe:: 'key' in g
        Check whether an attribute is present.
        .. versionadded:: 0.10
    .. describe:: iter(g)
        Return an iterator over the attribute names.
        .. versionadded:: 0.10
    """

    def get(self, name, default=None):
        """Get an attribute by name, or a default value. Like
        :meth:`dict.get`.
        :param name: Name of attribute to get.
        :param default: Value to return if the attribute is not present.
        .. versionadded:: 0.10
        """
        return self.__dict__.get(name, default)

    def _pop(self, name, default=_sentinel):
        """Get and remove an attribute by name. Like :meth:`dict.pop`.
        :param name: Name of attribute to pop.
        :param default: Value to return if the attribute is not present,
            instead of raise a ``KeyError``.
        .. versionadded:: 0.11
        """
        if default is _sentinel:
            return self.__dict__.pop(name)
        else:
            return self.__dict__.pop(name, default)

    def _setdefault(self, name, default=None):
        """Get the value of an attribute if it is present, otherwise
        set and return a default value. Like :meth:`dict.setdefault`.
        :param name: Name of attribute to get.
        :param: default: Value to set and return if the attribute is not
            present.
        .. versionadded:: 0.11
        """
        return self.__dict__.setdefault(name, default)

    def __contains__(self, item):
        return item in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __repr__(self):
        top = _extraction_ctx_stack.top
        if top is not None:
            return '<winnower.g of %r>' % top.app.name
        return object.__repr__(self)


@attrs(frozen=True)
class _WinnowerSessionCtxGlobals(_WinnowerCtxGlobalsBase):
    strict = attrib(default=False)


@attrs(frozen=True)
class _WinnowerExtractionCtxGlobals(_WinnowerCtxGlobalsBase):
    strict = attrib(default=False)
    more_debug = attrib(default=False)
