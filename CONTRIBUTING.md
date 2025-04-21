# Contributing to clickhouse-async

Thank you for considering contributing to clickhouse-async! This document provides guidelines and instructions for contributing.

## Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/your-username/clickhouse-async.git`
3. Change into the project directory: `cd clickhouse-async`
4. Install dependencies with Poetry: `poetry install`
5. Activate the virtual environment: `poetry shell`

## Development Workflow

1. Create a new branch for your feature or bugfix: `git checkout -b feature-name`
2. Make your changes
3. Run the tests to ensure everything is working: `pytest`
4. Format your code: `ruff format .` and lint with `ruff check .`
5. Check type annotations with mypy: `mypy clickhouse_async tests`
6. Commit your changes using the Conventional Commits format (see below)
7. Push your branch: `git push origin feature-name`
8. Open a pull request

## Commit Message Format

This project uses [Conventional Commits](https://www.conventionalcommits.org/) for commit messages, which are enforced by pre-commit hooks using Commitizen. This format is used for automatic versioning and changelog generation.

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
poetry run cz commit
```

## Code Style

This project uses Ruff for code formatting and linting, which enforces PEP 8 style guidelines and other best practices. We also use mypy for static type checking with strict settings. The configuration for both tools is defined in `pyproject.toml`. Please ensure your code passes both Ruff and mypy checks before submitting a pull request.

## Running Tests

```bash
# Run all tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=clickhouse_async

# Run specific tests
poetry run pytest tests/test_specific_file.py
```

## Building Documentation

Documentation is built using Sphinx. To build the documentation:

```bash
cd docs
poetry run make html
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
