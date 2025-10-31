# Repository Guidelines

## Project Structure & Module Organization
Application code lives in `src/`, split by responsibility: `src/backend` hosts the Flask API and CLI, `src/analysis` contains anomaly-detection pipelines, `src/db` handles data ingestion, and `src/frontend` serves static assets and Jinja templates. Shared utilities sit under `src/common`. Automated tests mirror this layout in `tests/backend`, `tests/analysis`, and `tests/db`. Reference designs, API specs, and QA artefacts are stored in `docs/` with subfolders that match each module. Infrastructure manifests (Docker, compose, deployment notes) reside in `infra/`.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate` to provision the local environment, followed by `pip install -r requirements.txt`.
- `python -m src.backend.app` starts the API on `http://127.0.0.1:8000/`.
- `python -m src.backend.cli summary` (or `anomalies --limit 5`, `snapshot --graphml out.graphml`) provides quick data inspections.
- `docker compose up` boots the containerised stack using the provided `docker-compose.yml`.
- `pytest` runs the unit suite; add `--cov=src --cov-report=term` to track the ≥90% coverage target.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation and explicit imports. Use `snake_case` for modules, packages, functions, and variables; reserve `CamelCase` for classes and exceptions. Keep functions cohesive and document complex routines with concise docstrings. Store configuration constants in a dedicated module under `src/common` rather than scattering literals. Type hints are encouraged in new code paths.

## Testing Guidelines
Place new tests in the mirrored package under `tests/` using the `test_<feature>.py` naming scheme and arrange fixtures in module-level helpers. Prefer `pytest` parametrisation to cover edge cases. Run `pytest --cov=src --cov-report=term` before opening a pull request to confirm coverage stays above the agreed threshold. Update datasets inside `tests/fixtures/` if realistic samples are required, and keep generated artefacts out of version control.

## Commit & Pull Request Guidelines
Commit messages typically use a Portuguese imperative prefix (`Add:`, `Att:`); continue that pattern while keeping subjects under 72 characters. Group related changes into single commits with meaningful bodies when rationale is non-obvious. Pull requests should link relevant issues, describe the impact on data pipelines and API endpoints, and include screenshots or CLI snippets for UI/CLI changes. Highlight any configuration toggles touched (.env defaults, Docker services) so reviewers can verify environments confidently.

## Environment & Configuration Tips
Copy `.env.example` to `.env` and adjust API toggles (`ENABLE_LIVE_FETCH`, `GRAPH_DATA_SOURCE`, rate limits) before running the backend or CLI. Avoid committing secrets; instead, document required keys in `docs/infra/`. When experimenting with live TCE-RJ calls, cap `TCE_API_MAX_PAGES` and `TCE_API_MAX_RECORDS` to protect remote services and keep debugging noise manageable.
