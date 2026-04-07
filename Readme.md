# Olist E-Commerce Data Warehouse (SQL Server)

End-to-End Data Warehouse auf Basis des öffentlichen [Olist Brazilian E-Commerce Datensatzes](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — rund 100.000 Transaktionen aus dem brasilianischen E-Commerce-Markt.

Ziel des Projekts ist eine produktionsnahe DWH-Pipeline in SQL Server mit Batch-Historisierung, inkrementellem Ladekonzept und vollständigem Audit-Trail. Die Pipeline implementiert gängige Patterns aus der Praxis: Metadata-Driven Orchestrierung über eine zentrale Konfigurationstabelle, Datenqualitätsprüfung, Soft Delete und transaktionssichere Stored Procedures.

---

## Architektur

```
CSV-Dateien
    │
    ▼
┌────────────────────────────────────────────────────────────────────┐
│  RAW                                                               │
│  Append-Only Staging · Batch-Historisierung · keine Transformation │
│  Metafelder: batch_id, load_ts, file_name                          │
└────────────────────────────────────────────────────────────────────┘
    │  batch_id wird an CLEANSED weitergegeben
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  CLEANSED                                                       │
│  Inkrementelles Ladekonzept via MERGE · SHA2-256 Row-Hash       │
│  DQ-Checks (Completeness, Validity, Uniqueness)                 │
│  Soft Delete für quellengetreue Historisierung                  │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  MART                                                           │
│  Star-Schema · Fakten- und Dimensionstabellen                   │
│  (in Entwicklung)                                               │
└─────────────────────────────────────────────────────────────────┘
```

### Querschnittsschemas

| Schema | Inhalt |
|---|---|
| `audit` | `load_log`, `error_log`, `dq_log`, `job_log` — vollständiger Audit-Trail jedes Ladevorgangs |
| `orchestration` | `pipeline_config` (Metadata Framework), `sp_run_layer`, `sp_run_full_load`, `agent_job_full_load` (SQL Server Agent Job) |

---

## Pipeline-Design

### Raw — Append-Only Staging mit Batch-Historisierung

Jeder Load erhält eine eindeutige `batch_id` (GUID), die allen Zeilen des Batches zugewiesen wird. Die raw-Tabellen wachsen mit jedem Load — Historisierung auf Batch-Ebene ist damit vollständig gewährleistet. Non-Clustered Indexes auf `batch_id` stellen sicher, dass der `WHERE batch_id = @batch_id`-Filter in den CLEANSED-SPs als Index Seek ausgeführt wird.

### Cleansed — Inkrementelles Ladekonzept

Der CLEANSED-Layer liest aus RAW über die `batch_id` des letzten erfolgreichen RAW-Loads. Das MERGE-Statement erkennt Änderungen über einen SHA2-256-Hash aller fachlichen Spalten:

```sql
HASHBYTES('SHA2_256', CONCAT(col1, '|', col2, '|', ...)) AS row_hash
```

Zeilen, die im aktuellen Batch nicht mehr vorkommen, werden **soft-deleted** (`is_deleted = 1`, `deleted_at`) statt physisch gelöscht — der Audit-Trail und Mart-FK-Referenzen bleiben intakt. Wiederauftauchende Datensätze werden automatisch reaktiviert.

### Datenqualitätsprüfung

Vor jedem MERGE läuft eine CTE-basierte DQ-Prüfung über drei Dimensionen:

| Dimension | Prüfungen |
|---|---|
| **Completeness** | NULL-Werte, leere Strings nach Bereinigung |
| **Validity** | Länge, Format (Hex-IDs, numerische Felder, Datumsformat), Wertemenge (z.B. `order_status`), logische Konsistenz (z.B. Lieferdatum vor Kaufdatum) |
| **Uniqueness** | Duplikate des Primärschlüssels innerhalb eines Batches |

Ergebnisse werden in `audit.dq_log` geschrieben. Bei Duplikaten wird der MERGE mit einem expliziten `THROW` abgebrochen.

### Transaktionsmanagement

RUNNING-Eintrag und DQ-Log werden **außerhalb** der Transaktion geschrieben — sie überleben einen Rollback und bleiben für die Fehlerdiagnose querybar. MERGE + SUCCESS-Update laufen **innerhalb** einer expliziten Transaktion und committen atomar.

### Metadata-Driven Orchestrierung

Der Kern der Orchestrierung ist die Tabelle `orchestration.pipeline_config` — ein Metadata Framework, das alle ETL-Pipelines zentral konfiguriert und steuert:

```
pipeline_config
├── sp_name            → welche SP wird aufgerufen
├── source_pipeline_id → FK auf die upstream RAW-Pipeline
├── file_path / file_name → Quelldatei
├── load_sequence      → Ausführungsreihenfolge innerhalb eines Layers
├── is_active          → Pipeline ein-/ausschaltbar
└── last_run_status / last_batch_id → Laufzeitstatus, wird nach jedem Load aktualisiert
```

Die Orchestrierungs-SPs lesen ausschließlich aus dieser Tabelle — neue Entities erfordern nur einen neuen `pipeline_config`-Eintrag, keine Änderung an der Orchestrierungslogik.

Das Seeding erfolgt über `dev_pipeline_config.sql` — in einer produktiven Umgebung würde jede Stage (DEV/TEST/PROD) auf einer eigenen SQL Server Instanz laufen und das jeweils passende Seed-Script gegen diese Instanz ausgeführt.

- `orchestration.sp_run_full_load` — startet einen vollständigen Lauf über alle Layer, schreibt in `audit.job_log`
- `orchestration.sp_run_layer` — iteriert über alle aktiven Pipelines eines Layers (Cursor, `load_sequence`-Reihenfolge)

Der SQL Server Agent Job (`agent_job_full_load.sql`) ruft `sp_run_full_load` auf und ermöglicht automatisiertes Scheduling des vollständigen Pipeline-Laufs — täglich, wöchentlich oder nach individueller Konfiguration — ohne manuellen Eingriff.

---

## Datenbasis

**Quelle:** [Olist Brazilian E-Commerce – Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| Datei | Inhalt |
|---|---|
| `olist_customers_dataset.csv` | Kundenstammdaten |
| `olist_orders_dataset.csv` | Bestellkopfdaten |
| `olist_order_items_dataset.csv` | Bestellpositionen |
| `olist_order_payments_dataset.csv` | Zahlungsinformationen |
| `olist_order_reviews_dataset.csv` | Kundenbewertungen |
| `olist_products_dataset.csv` | Produktstammdaten |
| `olist_sellers_dataset.csv` | Verkäuferstammdaten |
| `olist_geolocation_dataset.csv` | PLZ-Geodaten |
| `product_category_name_translation.csv` | Kategorie-Übersetzungen (PT → EN) |

---

## Projektstruktur

```
olist-ecommerce-dwh/
├── sql/
│   ├── create_schemas.sql
│   ├── migrations/
│   │   ├── V001__disable_non_customers_pipelines.sql
│   │   └── ...
│   ├── audit/
│   │   └── schema/
│   │       └── create_audit_tables.sql
│   ├── raw/
│   │   ├── schema/
│   │   │   └── create_raw_tables.sql
│   │   ├── procedures/
│   │   │   ├── raw_sp_load_customers.sql
│   │   │   ├── raw_sp_load_orders.sql
│   │   │   └── ...
│   │   └── eda/
│   │       ├── eda_customers.sql
│   │       ├── eda_orders.sql
│   │       └── ...
│   ├── cleansed/
│   │   ├── schema/
│   │   │   └── create_cleansed_tables.sql
│   │   └── procedures/
│   │       ├── cleansed_sp_load_customers.sql
│   │       ├── cleansed_sp_load_orders.sql
│   │       └── ...
│   ├── mart/    # in Entwicklung
│   └── orchestration/
│       ├── schema/
│       │   ├── create_orchestration_tables.sql
│       │   └── create_orchestration_triggers.sql
│       ├── procedures/
│       │   ├── orchestration_sp_run_full_load.sql
│       │   └── orchestration_sp_run_layer.sql
│       ├── config/
│       │   └── dev_pipeline_config.sql
│       └── jobs/
│           └── agent_job_full_load.sql
└── python/
    └── generate_create_tables.py
```

---

## Technologien

| Tool | Verwendung |
|---|---|
| **MS SQL Server** | Datenbank, gesamte Pipeline-Logik |
| **SSMS** | Entwicklung, Testing, lokale Ausführung |
| **SQL Server Agent** | Job-Scheduling (produktive Ausführung) |
| **Python** | DDL-Generierung |
| **Git / GitHub** | Versionierung |

---

## Status

| Komponente | Status |
|---|---|
| Schemas & Audit-Tabellen | Abgeschlossen |
| Orchestrierung (pipeline_config, Agent Job) | Abgeschlossen |
| RAW-Layer: customers, orders, order_items, order_payments, geolocation | Abgeschlossen |
| RAW-Layer: order_reviews, products, sellers, product_category_name_translation | In Entwicklung |
| CLEANSED-Layer: customers, orders, order_items | Abgeschlossen |
| CLEANSED-Layer: verbleibende 6 Entities | In Entwicklung |
| MART-Layer | Geplant |
| Power BI Reporting | Geplant |

---

## Setup

Siehe [SETUP.md](SETUP.md) für Schritt-für-Schritt-Anleitung zur lokalen Reproduzierbarkeit.
