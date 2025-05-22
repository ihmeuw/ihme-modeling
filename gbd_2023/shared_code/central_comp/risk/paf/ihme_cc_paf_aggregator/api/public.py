"""Top-level public ihme_cc_paf_aggregator functions.

These functions are a facade over library code. Typically, they would start a session
with the appropriate database, initialize HTTP clients to external services, set up logging
and metrics, etc.

This structure allows us to change library code as needed while maintaining the same top-level
interface. It also ensures a clean separation of interface from implementation details.
"""
