"""Benchmark scenarios — one module per KPI.

Each scenario is a stand-alone CLI: ``python -m scenarios.<name>
--library {ca,asynch,cc} [--runs N] [--warmup N]``. Each emits one
JSON line per measured run on stdout; ``benchmarks/report.py``
aggregates them into the final report.
"""
