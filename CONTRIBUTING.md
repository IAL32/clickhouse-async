# Contributing to clickhouse-async

Thank you for considering contributing to clickhouse-async! This document provides guidelines and instructions for contributing.

## Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/your-username/clickhouse-async.git`
3. Change into the project directory: `cd clickhouse-async`
4. Install dependencies with uv: `uv sync` (add `--extra compression` for the LZ4 / ZSTD / cityhash extras)
5. Run commands via `uv run <cmd>` — `uv` is project-runner-based, no separate shell activation needed.

## Development Workflow

1. Create a new branch for your feature or bugfix: `git checkout -b feature-name`
2. Make your changes
3. Run the tests to ensure everything is working: `uv run pytest`
4. Format your code: `uv run ruff format` and lint with `uv run ruff check`
5. Check type annotations with `uv run ty check` — **note**: the project uses [`ty`](https://github.com/astral-sh/ty), not mypy; do not add a parallel mypy config.
6. Commit your changes using the Conventional Commits format (see below)
7. Push your branch: `git push origin feature-name`
8. Open a pull request

### Iterating on CI changes locally with `act`

GitHub CI takes ~25–60 s per round-trip. To iterate on workflow changes
without burning CI minutes, the project ships a wrapper around
[`act`](https://nektosact.com/) that runs the GitHub Actions workflows
inside a local Docker container with the same surface as the real
runner.

```bash
brew install act                      # one-time, macOS

./scripts/act.sh full                 # run unit + integration job
                                      # (Python 3.12 by default)
./scripts/act.sh full 3.13            # pin a different Python
./scripts/act.sh full all             # full matrix — see caveat below
./scripts/act.sh unit                 # bare-install unit job only
./scripts/act.sh lint                 # lint + types job only
./scripts/act.sh prek                 # prek workflow
```

Configuration lives in `.actrc` at the repo root (gitignored). Two
flags are load-bearing:

- `--container-architecture linux/amd64`: the upstream act runner
  images aren't published for arm64.
- `--container-options=--network=host`: real GitHub CI runs the
  workflow on a bare Linux host, so a published Docker port lands on
  the same loopback the test process reaches via `127.0.0.1`. With
  `act`'s default bridge networking the runner sees a *different*
  loopback and tests get ECONNREFUSED. `--network=host` puts the
  runner on the host's network namespace and matches real CI.

**Matrix caveat**: `act push -j full` (without a `--matrix python:...`
filter) runs every Python version in parallel, all sharing the host's
Docker daemon. The parallel jobs race over the
`clickhouse-async-dev` container name and the first to finish kills
the container the others are still using. Real CI doesn't have this
problem because each matrix job has its own runner. For local
iteration, prefer `./scripts/act.sh full <python>` for a single
matrix entry.

## Commit Message Format

This project uses [Conventional Commits](https://www.conventionalcommits.org/) for commit messages, which are enforced by [`prek`](https://prek.j178.dev/) hooks using Commitizen. This format is used for automatic versioning and changelog generation.

`prek` is a drop-in replacement for `pre-commit` (same `.pre-commit-config.yaml`); we use it for the speed-up. It is declared in the project's dev dependencies, so `uv sync` installs it. After cloning, install the hooks once with:

```bash
uv run prek install
```

Do not install or invoke `pre-commit` directly — stick to `prek`.

The commit message should be structured as follows:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Types:
- `feat`: A new feature (triggers MINOR version bump)
- `fix`: A bug fix (triggers PATCH version bump)
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `build`: Changes that affect the build system or external dependencies
- `ci`: Changes to our CI configuration files and scripts
- `chore`: Other changes that don't modify src or test files

Breaking Changes:
- Add `BREAKING CHANGE:` in the footer or append a `!` after the type/scope to trigger a MAJOR version bump

Examples:
```
feat: add user authentication

fix(database): resolve connection timeout issue

feat!: redesign API
```

You can use Commitizen to help format your commit messages:
```bash
uv run cz commit
```

## Code Style

This project uses Ruff for code formatting and linting, which enforces PEP 8 style guidelines and other best practices. We use [`ty`](https://github.com/astral-sh/ty) for static type checking — **not** mypy; do not add mypy or a parallel `mypy.ini` / `[tool.mypy]` config. The configuration for both tools lives in `pyproject.toml`. Please ensure your code passes both `ruff check` / `ruff format` and `ty check` before submitting a pull request.

## Running Tests

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=clickhouse_async

# Run specific tests
uv run pytest tests/test_specific_file.py
```

## Building Documentation

Documentation is built using Sphinx. To build the documentation:

```bash
cd docs
uv run make html
```

## Releasing

This project uses automatic semantic versioning based on Conventional Commits. When commits are pushed to the main branch, a GitHub Action will:

1. Analyze commit messages since the last release
2. Determine the appropriate version bump (MAJOR, MINOR, or PATCH)
3. Update version numbers in the codebase
4. Generate/update the changelog
5. Create a GitHub release
6. Publish the package to PyPI

The version is determined as follows:
- `fix:` commits trigger a PATCH version bump (e.g., 1.0.0 → 1.0.1)
- `feat:` commits trigger a MINOR version bump (e.g., 1.0.0 → 1.1.0)
- `BREAKING CHANGE:` or `feat!:` commits trigger a MAJOR version bump (e.g., 1.0.0 → 2.0.0)

Maintainers do not need to manually create releases. The process is fully automated based on the commit messages.

## Code of Conduct

Please be respectful and considerate of others when contributing to this project. We strive to maintain a welcoming and inclusive environment for everyone.
