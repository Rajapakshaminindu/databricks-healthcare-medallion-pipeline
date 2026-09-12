---
name: Data Quality Incident
about: Report data discrepancies, schema drifts, null spikes, or SLA breaches
title: "[DATA INCIDENT]: "
labels: ["data-quality"]
assignees: ""
---

### Incident Summary
Brief summary of the data anomaly or constraint violation.

### Dataset / Table Details
- **Catalog / Database:**
- **Table Name:** (e.g. `healthcare_silver.patients`)
- **Metric impacted:** (e.g. Null value spike, Duplicate primary keys)

### Impact
- [ ] Upstream ingestion failure
- [ ] Downstream Gold mart corruption
- [ ] Power BI / Dashboard reporting error
