# -*- coding: utf-8 -*-
"""
    winnower.globals
    ~~~~~~~~~~~~~

    Defines all the thread safe global objects that are
    accessible from any context.
"""

from winnower.contrib.werkzeug.local import LocalStack, LocalProxy


# TODO: Not sure if session context is actually useful.
_session_ctx_err_msg = '''\
Working outside of winnower config loader context.

This typically means that you attempted to use functionality that needed
to interface with the current application object in some way. To solve
this, set up a session context using ... TODO:.\
'''

_extraction_ctx_err_msg = '''\
Working outside of winnower extraction context.

This typically means that you attempted to use functionality that needed
to interface with the current extractor object in some way. To solve
this, set up an extraction context using
UbcovConfigLoader.wrap_extractor_with_context().

Use this method to obtain an extractor contextmanager
that automatically cleans up the extraction context after the extraction
is complete. For example:

    # First, create a config_loader ... then
    extractor = config_loader.get_extractor(...)
    extractor_globals = {"strict": True}
    with config_loader.wrap_extractor_with_context(
            extractor, extractor_globals) as e:
        extraction = e.get_extraction()
        df = extraction.execute()
'''


def _find_session_ctx():
    top = _session_ctx_stack.top
    if top is None:
        raise RuntimeError(_session_ctx_err_msg)
    return top


def _find_session():
    top = _session_ctx_stack.top
    if top is None:
        raise RuntimeError(_session_ctx_err_msg)
    return top.winnower_session


def _find_globals():
    top = _session_ctx_stack.top
    if top is None:
        raise RuntimeError(_session_ctx_err_msg)
    return top.g


def _find_extraction_ctx():
    top = _extraction_ctx_stack.top
    if top is None:
        # Generate a dummy extraction context with
        # the extraction globals set to their defaults.

        # Import here to avoid circular import.
        from winnower.ctx import WinnowerExtractionContext
        # Create a dummy Extraction Context i.e. winnower_extraction
        # property will be None but extraction globals will have default
        # values.
        top = WinnowerExtractionContext()
        top.push()
    return top


def _find_extraction():
    top = _find_extraction_ctx()
    if top is None or top.has_extractor():
        raise RuntimeError(_extraction_ctx_err_msg)
    return top.winnower_extraction


def _find_extraction_globals():
    top = _find_extraction_ctx()
    if top is None:
        raise RuntimeError(_extraction_ctx_err_msg)
    return top.g


# session context locals
_session_ctx_stack = LocalStack()
current_session = LocalProxy(_find_session)
current_session_ctx = LocalProxy(_find_session_ctx)
g = LocalProxy(_find_globals)

# extraction context locals
_extraction_ctx_stack = LocalStack()
current_extraction = LocalProxy(_find_extraction)
current_extraction_ctx = LocalProxy(_find_extraction_ctx)
eg = LocalProxy(_find_extraction_globals)
