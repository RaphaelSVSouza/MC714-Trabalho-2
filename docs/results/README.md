# Results

Generated at (UTC): 2026-07-04T22:49:12.375641+00:00

Environment:
- Three nodes plus the `resource` observer
- Python 3.12
- Docker Compose local stack
- Timeout configuration matches `docker-compose.yml`

Commands used:
- `python scripts/run_experiments.py`
- `docker compose up --build -d`
- `docker compose stop node3` / `docker compose start node3` for election recovery

Metric notes:
- `violations`: count of observed causality or safety violations in the repetition
- `min` / `max` / `mean` / `median`: computed from the numeric metric column in `experiment-summary.csv`
- `observations`: short human-readable note about what the metric represents

The JSON and CSV files are generated from the live stack and should be treated as the source of truth for the report.
