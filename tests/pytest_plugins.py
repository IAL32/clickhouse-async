"""Pytest plugins for test configuration."""

import logging

# Suppress testcontainers logs
logging.getLogger("testcontainers.core.container").setLevel(logging.WARNING)
logging.getLogger("testcontainers.core.waiting_utils").setLevel(logging.WARNING)
logging.getLogger("testcontainers.core.docker_client").setLevel(logging.WARNING)
