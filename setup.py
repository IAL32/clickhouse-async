"""Setup script for clickhouse-async."""

from setuptools import setup

# This file is provided for compatibility with tools that don't support pyproject.toml yet.
# The actual package configuration is in pyproject.toml.

setup(
    name="clickhouse-async",
    version="0.1.0",
    description="Async Python client for ClickHouse with full TCP support",
    author="IAL32",
    author_email="562969+IAL32@users.noreply.github.com",
    packages=["clickhouse_async"],
    python_requires=">=3.11",
    install_requires=[],
)
