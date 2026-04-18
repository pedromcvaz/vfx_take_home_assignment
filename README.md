# vfx_take_home_assignment
Repo for the take home assignment from VFX Financial’s interview process


## Schema Contracts

Master models use `contract: enforced: true` to ensure:
- Type safety across dbt runs
- Breaking changes are caught in CI/CD
- All columns are documented (no undocumented fields)

This prevents downstream breakage when analysts/BI tools consume these tables.


## Data Quality Approach

**Dual-layer validation strategy:**
- **Staging WHERE clause**: Filters critical nulls (user_id, product_id, purchase_date, final_price) 
  to prevent corrupt data from reaching master tables
- **dbt tests**: Monitor data quality and alert when source degradation occurs

**Rationale**: WHERE clauses are defensive barriers; tests are monitoring tools. 
For non-critical fields (e.g., discount, payment_method), we allow nulls through 
and handle them with COALESCE/defaults in master models.