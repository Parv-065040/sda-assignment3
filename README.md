# Streaming Data Analytics — Production-Grade E-commerce Growth & Retention Command Center

## Executive Overview

This repository contains the production-grade extension of the **Streaming Data Analytics Assignment 3** project.

The project transforms a basic Kafka producer/consumer and Grafana dashboard into a **production-shaped real-time analytics platform** for an e-commerce business. The platform combines:

- Real-time event streaming
- Kafka + Zookeeper
- MySQL persistence
- 15-second tumbling-window KPI computation
- Dead-letter queue handling
- Retry and exponential backoff
- Safe Kafka offset management
- Statistical anomaly detection
- Grafana alerting and email notifications
- RFM customer segmentation
- FastAPI REST services
- Executive/CMO-level Grafana dashboard
- Customer value, campaign, funnel and search analytics

> **Important:** All business data used in the demonstration is synthetic/Faker-generated. The dashboard validates the streaming architecture and analytical mechanics; the displayed revenue, customer counts and other business values are not real company results.

---

## 1. Business Objective

The platform is designed to answer five executive questions continuously:

1. **What is happening right now?**
2. **Where is the customer funnel leaking?**
3. **Which customers require retention attention?**
4. **Which campaigns and categories are generating meaningful engagement?**
5. **When does behaviour become abnormal enough to require action?**

The dashboard is therefore designed as a **Growth & Retention Command Center**, not merely a technical monitoring screen.

---

## 2. Architecture

```text
                    ┌───────────────────────────┐
                    │   Synthetic Event Data    │
                    │       Python Producers    │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │     Kafka + Zookeeper      │
                    │       Event Backbone       │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │    Streaming Consumers     │
                    │  Validation + Aggregation  │
                    │  KPI + Anomaly Detection  │
                    └───────┬───────────┬───────┘
                            │           │
                    success │           │ bad/unprocessable
                            ▼           ▼
                    ┌─────────────┐  ┌───────────┐
                    │    MySQL    │  │    DLQ    │
                    │   Storage   │  │   Topic   │
                    └──────┬──────┘  └───────────┘
                           │
             ┌─────────────┼───────────────┐
             │             │               │
             ▼             ▼               ▼
       ┌──────────┐  ┌────────────┐  ┌────────────┐
       │ RFM      │  │  FastAPI   │  │  Grafana   │
       │ Service  │  │ REST Layer │  │ Dashboard  │
       └──────────┘  └────────────┘  └─────┬──────┘
                                           │
                                           ▼
                              ┌──────────────────────┐
                              │ Executive Decisions  │
                              │ + Alerts + Actions   │
                              └──────────────────────┘
```

---

## 3. Core Technology Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka |
| Coordination | Zookeeper |
| Processing | Python |
| Database | MySQL |
| Visualization | Grafana |
| API | FastAPI |
| Containerization | Docker / Docker Compose |
| Customer analytics | RFM segmentation |
| Alerting | Grafana Alerting + SMTP |
| Configuration | Environment variables / YAML / Docker Compose |

---

## 4. Production Features

### 4.1 Foundation Hardening

The original manually executed scripts were converted into Docker services.

Implemented:

- Docker Compose orchestration
- Environment-based configuration
- Structured logging
- Kafka retry/backoff
- MySQL retry/backoff
- Named persistent volumes
- Automatic database initialization

The complete stack can therefore be brought up consistently instead of relying on a sequence of manual commands.

### 4.2 Reliability & Failure Handling

The consumer contains several production-oriented controls:

- **Dead-letter queue:** malformed/unprocessable messages are isolated instead of crashing the consumer.
- **Consumer identity:** `consumer_id` distinguishes parallel consumer instances.
- **Manual offset commits:** offsets are committed only after successful downstream database writes.
- **Retry/backoff:** transient infrastructure failures do not immediately terminate processing.

A real offset-commit ordering issue was found during testing and fixed before deployment.

### 4.3 Statistical Anomaly Detection

The system monitors:

- Conversion Rate
- Cart Abandonment Rate
- Revenue
- Ad Spend

Rather than relying only on static thresholds, the consumer compares the current metric with its recent historical behaviour and flags statistically unusual movement.

Detected anomalies are:

1. Stored in the alerts table
2. Exposed through the API
3. Displayed in Grafana
4. Used for alert annotations
5. Capable of triggering email notifications for critical events

A controlled 9-window simulation was used for validation: stable windows generated no false alarms, followed by a deliberately broken window that triggered the expected alert.

---

## 5. RFM Customer Segmentation

A customer-level transaction table was introduced because aggregate windows alone cannot answer retention questions.

Each customer is scored on:

- **Recency** — how recently the customer purchased
- **Frequency** — how often the customer purchased
- **Monetary** — how much the customer spent

The implementation uses quintile-based scoring and classifies customers into eight segments:

1. Champions
2. Loyal Customers
3. Big Spenders
4. Can't Lose Them
5. At Risk
6. New Customers
7. Lost
8. Needs Attention

The RFM rules were exhaustively tested across all **125 possible R/F/M score combinations**. A rule-ordering issue was identified and corrected.

Because the demonstration compresses long-term behaviour into minutes, recency is measured at minute-level granularity rather than day-level granularity.

---

## 6. FastAPI Layer

FastAPI establishes the project as a reusable analytics service rather than making Grafana the only consumer of the data.

The API exposes metrics, alerts and segmentation information through **8 REST endpoints**, with interactive documentation.

The client-supplied endpoint was also tested against a deliberate SQL-injection attempt and rejected cleanly with HTTP 400.

This architecture allows future consumers such as:

- React / Next.js
- Internal operations applications
- CRM workflows
- Other BI tools
- Automated decision systems

to consume the same analytical layer.

---

## 7. Grafana Command Center

The final dashboard is organized into business sections:

### Executive Summary
Provides:

- Total Revenue
- Cart Abandonment Rate
- Total Orders
- Total Customers Scored
- Overall Conversion Rate
- Total Ad Spend
- Active Critical Alerts

### Funnel & Customer Segments

Shows:

- Views
- Cart Adds
- Purchases
- RFM segment distribution

### Segment Trend & Campaign → Segment

Shows:

- Segment movement over time
- Which customer segments campaigns reach

### Trends

Shows:

- Conversion Rate
- Cart Abandonment Rate
- Revenue
- Five-window moving averages
- Critical/warning anomaly annotations

### Category & Campaign

Shows:

- Revenue by product category
- Order value distribution
- Campaign clicks
- Impressions
- Spend
- CTR

### Operational Pulse

Shows:

- Live activity
- At-risk customer list
- Recent alerts

### Segment Value / Search / Heatmap

Shows:

- Lifetime value by segment
- Top search queries
- Traffic intensity over time

---

## 8. Chart Validation Note

The charts in the captured dashboard are **rendering the stored data correctly**.

Some values can look unusual because the demonstration data is synthetic.

For example, the captured campaign table contains CTR values above 100%. This is not a Grafana visualization error. The original event generator allowed clicks and impressions to be sampled independently, which could create more clicks than impressions at aggregate level.

The correct interpretation is:

> **The visualization is correct; the original synthetic data-generation relationship was unrealistic.**

A producer-side fix has been implemented so future regenerated data makes clicks a structural subset of impressions.

Similarly, the flat-to-active pattern in the segment trend and heatmap corresponds to the documented database reset and pipeline restart. It is a real pattern in the captured data, not a chart-rendering defect.

This distinction is important:

**Visualization correctness ≠ upstream data realism.**

---

## 9. Key Business Insights From the Demonstration

### Funnel

The captured run shows:

- 57,694 views
- 21,748 purchases

The gap demonstrates why the platform focuses on the browsing → cart → purchase journey.

### Customer value

The captured RFM snapshot shows strong value concentration.

The Champions segment contains only 6 customers but represents approximately **₹51.9 crore** of combined lifetime value in the demonstration snapshot.

### Retention

Lost and Needs Attention customers also contain substantial historical value.

This creates a clear analytical distinction between:

- acquiring a new customer, and
- recovering a previously valuable customer.

### Campaign intelligence

`RETARGETING_CART` reaches visible Champions and Needs Attention populations.

`NEW_LAUNCH_PROMO` also reaches a visible At Risk population, which creates a useful campaign-targeting investigation.

### Streaming alerts

The system can identify compound business events where multiple metrics change together rather than treating every metric independently.

---

## 10. Repository Structure

A representative structure is:

```text
.
├── api/
├── consumer/
├── db/
├── mnt/
├── producer/
├── segmentation/
├── .env.example
├── docker-compose.yml
├── PHASE1_NOTES.md
├── README.md
└── docs/
    └── screenshots/
```

The exact implementation may contain additional files depending on the current project state.

---

## 11. Running the Project

### Prerequisites

Install:

- Docker Desktop
- Git
- PowerShell / terminal

### 1. Clone the repository

```powershell
git clone https://github.com/Parv-065040/sda-assignment3.git
cd sda-assignment3
```

### 2. Configure environment variables

Copy the example file:

```powershell
Copy-Item .env.example .env
```

Edit `.env` with the required local configuration.

**Never commit real passwords, API keys or SMTP credentials.**

### 3. Start the stack

```powershell
docker compose up -d --build
```

### 4. Check services

```powershell
docker compose ps
```

### 5. View logs

```powershell
docker compose logs -f
```

Or inspect an individual service:

```powershell
docker compose logs -f consumer
```

### 6. Stop the stack

```powershell
docker compose down
```

If persistent volumes also need to be removed:

```powershell
docker compose down -v
```

> Removing volumes deletes the persisted demo database. Use this only when a clean environment is intentionally required.

---

## 12. Validation Strategy

The project was validated at multiple levels:

| Validation | Purpose |
|---|---|
| 9-window anomaly simulation | Validate alert behaviour |
| 125-combination RFM sweep | Validate segment rule ordering |
| Failure/offset reasoning | Validate data-loss protection |
| DLQ handling | Validate malformed-message isolation |
| SQL-injection test | Validate API input handling |
| Dashboard validation | Validate KPI, filter and annotation behaviour |
| Live deployment checks | Validate the integrated Docker stack |

---

## 13. Known Limitations

1. **Synthetic data:** all business events are generated for demonstration.
2. **Small customer sample:** the captured snapshot contains 38 scored customers.
3. **Compressed time:** weeks/months of behaviour are demonstrated in minutes.
4. **Captured CTR anomaly:** the screenshot reflects an older synthetic data-generation method.
5. **Search-query realism:** the captured screenshot contains generic dictionary-style search terms.
6. **Reset event:** some time-series panels show a restart/reset point in the captured run.

These limitations do not invalidate the streaming architecture; they define the boundary between a production-shaped academic prototype and a real production deployment.

---

## 14. Production Roadmap

### Priority 0

- Regenerate sample data using corrected CTR logic.
- Regenerate realistic product search queries.
- Preserve historical data across longer runs.

### Priority 1

- Add event-level schema/data-quality contracts.
- Add authentication and authorization to the API.
- Introduce proper secret management.
- Move to sanitized real event data where permitted.

### Priority 2

- Add campaign ROI and customer-level attribution.
- Add CI/CD and container health checks.
- Add a dedicated React/Next.js executive front end.
- Integrate retention workflows and CRM actions.

---

## 15. Project Outcome

The project demonstrates an end-to-end data-product architecture:

```text
Event
  ↓
Kafka
  ↓
Resilient Streaming Consumer
  ↓
MySQL
  ↓
RFM + Anomaly Intelligence
  ↓
FastAPI
  ↓
Grafana
  ↓
Executive Decision / Alert
```

The key achievement is the shift from a **dashboard that reports data** to a **platform that continuously detects, explains and surfaces business signals**.

---

## 16. Author

**Parv Jhamb**  
Roll No. **065040**  
PGDM, Batch-27  
FORE School of Management, New Delhi  
Specialization: Big Data Analytics and Marketing

Course: **Streaming Data Analytics**

