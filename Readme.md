# Olist E-Commerce Data Warehouse (MSSQL)

End-to-End Data Warehouse auf Basis eines produktiven, anonymisierten E-Commerce Datensatzes (~100.000 Transaktionen). Grundlage der Pipeline-Konzeption bildete eine explorative Datenanalyse (EDA) je Rohdatentabelle.

Die Pipeline umfasst drei Schichten:
- **Raw** – unveränderte Rohdaten-Landezone mit Append-Only-Historisierung
- **Cleansed** – inkrementelles Ladekonzept mit Row-Hash-basierter Änderungserkennung, Datenqualitätsprüfungen und Error Logging
- **Mart** – Star-Schema mit Nonclustered Columnstore-Indizierung als Grundlage für ein performantes Power BI Reporting

---

## Datenbasis

**Quelle:** [Olist Brazilian E-Commerce – Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

9 CSV-Dateien mit Informationen zu Bestellungen, Kunden, Produkten, Verkäufern, Zahlungen, Bewertungen und Geodaten.

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
| `product_category_name_translation.csv` | Kategorie-Übersetzungen |

---

## Architektur

```
CSV-Dateien
    │
    ▼
┌──────────────────────────────────────────────────────┐
│  RAW                                                 │
│  Append-Only · Historisierung · keine Transformation │
│  Metafelder: row_id, batch_id, load_ts, file_name    │
└──────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────┐
│  CLEANSED                                        │
│  Incremental Load (MERGE) · Row-Hash · DQ-Checks │
│  Normalisierung, Typbereinigung, Error Logging   │
└──────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────┐
│  MART                                           │
│  Star-Schema · Dimensionen & Faktentabellen     │
│  (in Entwicklung)                               │
└─────────────────────────────────────────────────┘
```

---

## Technologien

| Tool | Verwendung |
|---|---|
| **MS SQL Server** | Datenbank, Pipeline-Logik |
| **SSMS** | Entwicklung, Testing, Pipeline-Ausführung |
| **Visual Studio Code** | Code-Verwaltung, Projektstruktur |
| **GitHub** | Versionierung |
| **Python** | DDL-Generierung |

---

## Projektstruktur

```
olist-ecommerce-dwh/
├── python/
│   └── generate_create_tables.py
└── sql/
    ├── create_schemas.sql
    ├── exec_full_load.sql
    ├── raw/
    │   ├── create_raw_tables.sql
    │   ├── sps/
    │   │   ├── raw_sp_load_customers.sql
    │   │   ├── raw_sp_load_orders.sql
    │   │   └── ...
    │   └── eda/
    │       ├── eda_customers.sql
    │       ├── eda_orders.sql
    │       └── ...
    ├── cleansed/
    │   ├── create_cleansed_tables.sql
    │   └── sps/
    │       ├── cleansed_sp_load_customers.sql
    │       ├── cleansed_sp_load_orders.sql
    │       └── ...
    └── mart/
        └── (in Entwicklung)
```

---

## Pipeline-Design

### Raw – Append-Only mit Historisierung

Jeder Load-Vorgang erhält eine eindeutige `batch_id` (GUID), die allen Zeilen eines Loads zugewiesen wird. Dadurch ist jeder Ladevorgang vollständig nachvollziehbar und isolierbar.

### Cleansed – Incremental Load mit Row-Hash

Der Cleansed-Layer verwendet `MERGE` mit einem SHA2-256-Hash über alle fachlichen Spalten. Eine Zeile wird nur aktualisiert wenn sich der Hash geändert hat – unnötige Updates werden vermieden.

```sql
HASHBYTES('SHA2_256', CONCAT(col1, '|', col2, '|', ...)) AS row_hash
```

Zusätzlich werden Datenfehler (NULL-Werte, ungültige Längen, leere Strings) vor dem MERGE in `cleansed.error_log` protokolliert. Fehlerhafte Zeilen werden gefiltert und nicht in Cleansed übernommen.

### Orchestrierung

Die Orchestrierung erfolgt über Master-Skripte in SSMS, die `batch_id` wird per `OUTPUT`-Parameter von Raw an Cleansed übergeben. In einer produktiven Umgebung würde dies über SQL Server Agent Jobs oder Azure Data Factory abgebildet – die SP-Logik bleibt dabei identisch.

```
exec_full_load.sql     → Standardlauf: Raw + Cleansed + Mart
```

---

## Status

| Komponente | Status |
|---|---|
| DWH: Raw-Layer | In Entwicklung |
| DWH: Cleansed-Layer |  In Entwicklung |
| DWH: Mart-Layer |  Geplant |
| Power BI Reporting |  Geplant |

---

**Voraussetzungen:**
- MS SQL Server (Developer Edition oder höher)
- SSMS
- Olist-Datensatz lokal verfügbar ([Kaggle Download](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce))

**Setup:**

1. Schemas anlegen → `sql/create_schemas.sql`
2. Raw-Tabellen erstellen → `sql/raw/create_raw_tables.sql`
3. Cleansed-Tabellen erstellen → `sql/cleansed/create_cleansed_tables.sql`
4. Mart-Tabellen erstellen → `sql/mart/create_mart_tables.sql`
5. Stored Procedures deployen → alle `sps/` Ordner
6. Basispfad im Master-Skript anpassen → in `sql/exec_full_load.sql`
7. Pipeline ausführen → `sql/exec_full_load.sql`
