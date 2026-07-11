# Publication Checklist

This repo is intended to be safe for a public GitHub portfolio.

## Public-Safe Scope

- [x] Personal learning project.
- [x] Synthetic manufacturing-style data only.
- [x] No company code.
- [x] No customer data.
- [x] No private business logic.
- [x] No credentials or secrets are required to run tests.
- [x] Runtime MongoDB and production Airflow deployment gaps are documented as blockers/backlog.

## Checked Before Initial Publication

Commands:

```bash
git ls-files | rg -v "^(PUBLICATION_CHECKLIST.md|VERIFICATION_LOG.md)$" | xargs rg -n -i "(api[_-]?key|access[_-]?key|secret|token|password|passwd|private[_-]?key|mongodb\\+srv|Bearer |AKIA|BEGIN RSA|BEGIN OPENSSH|client_secret|refresh_token)"
git ls-files | rg -v "^(PUBLICATION_CHECKLIST.md|VERIFICATION_LOG.md)$" | xargs rg -n -i "(personal path|private email|private company name|customer name|internal path)"
pytest
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run --catalog-backend json --output-dir /tmp/manufacturing-mini-publication-cli
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.operator_report --output-dir /tmp/manufacturing-mini-publication-cli --business-date 2026-06-29
PYTHONPATH=src python -m manufacturing_data_platform.pipeline.run_eav --catalog-backend json --output-dir /tmp/manufacturing-mini-publication-eav-cli
```

Expected:

- Secret scan returns no sensitive repo content.
- Personal path/name scan returns no public-facing leakage.
- Tests pass.
- JSON CLI path succeeds.
- Operator evidence report CLI path succeeds.
- EAV JSON CLI path succeeds.

## Not Public

The following should not be published as part of this repo:

- Personal mission documents.
- Resume/application materials.
- Job tracking databases or scraping tools.
- Company reference code or internal paths.
- Generated lakehouse outputs under `data/lakehouse/`.
