"""Vendored copy of dlt-hub/verified-sources/sources/pg_replication.

Pinned upstream SHA: 75b3ec17eab99d0079d9f61b7f47fc8b899a5738
Upstream URL: https://github.com/dlt-hub/verified-sources/tree/75b3ec17eab99d0079d9f61b7f47fc8b899a5738/sources/pg_replication
License: Apache-2.0 (see ./LICENSE).

The verified source is not published to PyPI. Vendoring keeps installs
offline-safe, lets us pin a known-good revision, and gives a patch surface for
upstream caveats (DDL drift, slot teardown). See ./NOTICE.md for upgrade
instructions.

This shim re-exports the two public entry points the project uses
(``init_replication`` from ``helpers``, ``replication_resource`` from the
upstream ``__init__``, renamed here to ``resource.py``). Everything else stays
private to the vendor directory.
"""

from __future__ import annotations

from .helpers import init_replication
from .resource import replication_resource

__all__ = ["init_replication", "replication_resource"]
