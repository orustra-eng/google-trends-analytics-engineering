# Google Trends Analytics Engineering (dbt)

## 📌 Overview
This project implements an end-to-end analytics engineering pipeline on Google Trends data using **dbt**.

The goal is to transform raw search trend data into a clean, scalable, and analyst-friendly data model that supports trend monitoring, regional comparison, and emerging topic detection.

---

## ⚠️ Data Characteristics & Challenges

The Google Trends dataset introduces several important constraints:

- **Top-N truncation**: Only the top 25 terms per week and DMA are available  
- **Relative scoring**: Scores are normalized (0–100), not absolute search volume  
- **Snapshot behavior**: Multiple refreshes exist for the same logical period  
- **Sparse signals**: Most scores are near zero with occasional spikes  

These characteristics significantly influence modeling decisions.

---

## 🏗️ Architecture

The project follows a layered dbt architecture:

### 1. Staging (`stg_`)
- Standardizes raw source data
- Renames columns and enforces types
- No business logic

### 2. Intermediate (`int_`)
- Applies business logic
- Handles snapshot selection (latest per week + DMA)
- Computes derived fields (e.g., WoW changes)

### 3. Marts
- **Fact table**: `fct_search_trends` (grain: week, DMA, term)
- **Dimensions**: `dim_week`, `dim_dma`, `dim_term`
- **BI mart**: `mart_search_trends` (denormalized for analytics)

---

## 📊 Data Model

### Fact Table
- Grain: `(week_id, dma_id, term_id)`
- Measures:
  - `score` (normalized popularity)
  - `rank` (relative position within week + DMA)

### Dimensions
- `dim_week`: time attributes
- `dim_dma`: geographic dimension
- `dim_term`: unique search terms

### Mart
- Denormalized table optimized for BI and ad hoc analysis

---

## 🧪 Testing Strategy

The project includes both **schema tests** and **custom data tests**:

### Schema tests
- Uniqueness constraints
- Not-null checks
- Referential integrity
- Accepted value ranges (rank, score)

### Custom tests
- Max 25 terms per (week, DMA)
- No duplicate ranks within a group
- Consistency of active terms

---

## ⚖️ Key Design Decisions

- Use **latest snapshot per (week, DMA)** for consistency
- Favor **term presence and score trends** over rank-based analysis
- Separate **business logic (intermediate)** from **serving layer (mart)**
- Provide both:
  - dimensional model (flexibility)
  - denormalized mart (usability)

---

## 🚀 Scalability Considerations

- Incremental models for large datasets
- Partitioning by `week`
- Clustering by `dma_id` and `term_id`
- Modular design supports adding:
  - new regions
  - enriched term metadata
  - additional marts

---

## 🔄 CI / Workflow

- GitHub Actions pipeline defined (`dbt-ci.yml`)
- Runs dbt build on pull requests
- Ensures models and tests validate before merge

*(Note: CI requires warehouse credentials via GitHub Secrets in production setups)*

---

## ▶️ How to Run

```bash
dbt deps
dbt seed   # if applicable
dbt run
dbt test