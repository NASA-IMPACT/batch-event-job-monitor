"""Shared Lambda asset for constructs bundling their own handler code.

Every construct in this package that bundles a handler from
``batch_event_job_monitor.handlers`` zips the same source tree: the parent
directory of the installed ``batch_event_job_monitor`` package, restricted to
just that subpackage. Computing the entry path from the installed package
(rather than a relative string like ``"src"``) keeps it correct regardless of
the CDK app's working directory -- this is a distributed construct library,
so a literal ``"src"`` would resolve against whichever downstream consumer's
cwd happens to be, not this repository's.

Excluding ``batch_event_job_monitor_cdk`` keeps the CDK library itself (and
its ``aws_cdk``/``constructs`` imports) out of the Lambda runtime zip.
Excluding ``__pycache__``/``*.pyc`` keeps the asset hash independent of the
machine and Python version that ran synth -- a ``.pyc`` byte-for-byte differs
across interpreters, so leaving them in makes every synth redeploy the
function even with no source change.
"""

from __future__ import annotations

from pathlib import Path

import batch_event_job_monitor

HANDLER_ENTRY = str(Path(batch_event_job_monitor.__file__).parent.parent)
HANDLER_EXCLUDE = [
    "*",
    "!batch_event_job_monitor",
    "!batch_event_job_monitor/**",
    "**/__pycache__",
    "**/*.pyc",
]
