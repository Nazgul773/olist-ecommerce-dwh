# Setup — Lokale Reproduzierbarkeit

Schritt-für-Schritt-Anleitung zur vollständigen Einrichtung des Data Warehouse auf einer lokalen SQL Server Instanz.

---

## Voraussetzungen

- MS SQL Server (Developer Edition oder höher)
- SSMS 19+
- PowerShell 5.1+
- Olist-Datensatz lokal verfügbar -> [Kaggle Download](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Ausführungsreihenfolge

Alle SQL-Skripte werden in SSMS ausgeführt. Die Reihenfolge ist zwingend — spätere Skripte referenzieren Objekte aus früheren.

### 1. Datenbank anlegen

In SSMS eine neue Datenbank `OlistDWH` erstellen (per UI oder manuell):

```sql
CREATE DATABASE OlistDWH;
```

### 2. Schemas anlegen

```
sql/setup/create_schemas.sql
```

Legt die Schemas `raw`, `cleansed`, `mart`, `audit`, `orchestration` an.

### 3. Audit-Tabellen

```
sql/audit/schema/create_audit_tables.sql
```

Muss vor den anderen Schemas ausgeführt werden, da `audit.job_log` von den FK-Constraints in `orchestration.pipeline_config` referenziert wird.

### 4. Orchestrierung — Tabellen & Trigger

```
sql/orchestration/schema/create_orchestration_tables.sql
sql/orchestration/schema/create_orchestration_triggers.sql
```

`create_orchestration_triggers.sql` legt den AFTER UPDATE Trigger an, der `modified_ts` in `pipeline_config` automatisch aktualisiert.

### 5. Raw-Tabellen

```
sql/raw/schema/create_raw_tables.sql
```

Enthält alle `CREATE TABLE`-Definitionen der RAW-Layer.

### 6. Cleansed-Tabellen

```
sql/cleansed/schema/create_cleansed_tables.sql
```

Enthält alle `CREATE TABLE`-Definitionen der CLEANSED-Layer.

### 7. Mart-Tabellen

```
sql/mart/schema/create_mart_tables.sql
```

Legt das Star-Schema an: `dim_date`, `dim_customer`, `dim_seller`, `dim_product`, `dim_payment_type`, `dim_order_status`, `fact_sales`, `fact_payments`.

### 8. Stored Procedures deployen

Alle Dateien sind idempotent (`CREATE OR ALTER`) und können in beliebiger Reihenfolge innerhalb der Gruppe ausgeführt werden.

**RAW:**

```
sql/raw/procedures/raw_sp_load_customers.sql
sql/raw/procedures/raw_sp_load_orders.sql
sql/raw/procedures/raw_sp_load_order_items.sql
sql/raw/procedures/raw_sp_load_order_payments.sql
sql/raw/procedures/raw_sp_load_order_reviews.sql
sql/raw/procedures/raw_sp_load_products.sql
sql/raw/procedures/raw_sp_load_sellers.sql
sql/raw/procedures/raw_sp_load_geolocation.sql
sql/raw/procedures/raw_sp_load_product_category_name_translation.sql
```

**CLEANSED:**

```
sql/cleansed/procedures/cleansed_sp_load_customers.sql
sql/cleansed/procedures/cleansed_sp_load_orders.sql
sql/cleansed/procedures/cleansed_sp_load_order_items.sql
sql/cleansed/procedures/cleansed_sp_load_order_payments.sql
sql/cleansed/procedures/cleansed_sp_load_order_reviews.sql
sql/cleansed/procedures/cleansed_sp_load_products.sql
sql/cleansed/procedures/cleansed_sp_load_sellers.sql
sql/cleansed/procedures/cleansed_sp_load_geolocation.sql
sql/cleansed/procedures/cleansed_sp_load_product_category_name_translation.sql
```

**MART:**

```
sql/mart/procedures/mart_sp_load_dim_date.sql
sql/mart/procedures/mart_sp_load_dim_customer.sql
sql/mart/procedures/mart_sp_load_dim_seller.sql
sql/mart/procedures/mart_sp_load_dim_product.sql
sql/mart/procedures/mart_sp_load_dim_payment_type.sql
sql/mart/procedures/mart_sp_load_dim_order_status.sql
sql/mart/procedures/mart_sp_load_fact_sales.sql
sql/mart/procedures/mart_sp_load_fact_payments.sql
```

**Orchestrierung:**

```
sql/orchestration/procedures/orchestration_sp_run_layer.sql
sql/orchestration/procedures/orchestration_sp_run_full_load.sql
```

### 9. Pipeline-Konfiguration befüllen

```
sql/orchestration/config/dev_pipeline_config.sql
```

**Vor der Ausführung:** `@DatasetRoot` auf den lokalen Ordner mit den Olist-CSV-Dateien setzen.

Das Skript ist idempotent — bereits vorhandene Einträge werden übersprungen.

### 10. SQL Server Agent Job registrieren (optional)

```
sql/orchestration/jobs/agent_job_full_load.sql
```

**Vor der Ausführung:** `@ScriptRoot` in Zeile 24 auf den lokalen Pfad zum `scripts/ps`-Ordner setzen.

Registriert den Agent Job `OlistDWH_Orchestration_FullLoad_Daily` mit zwei Steps:

1. **Preprocess CSVs** (CmdExec) — ruft `preprocess_all.ps1` auf, konvertiert Quelldateien mit `needs_preprocessing = 1` von comma- nach pipe-delimited
2. **Execute Full Load Pipeline** (T-SQL) — ruft `orchestration.sp_run_full_load` auf

Nur relevant wenn der Job über den SQL Server Agent geplant werden soll.

---

## Pipeline manuell ausführen

Nach dem Setup kann der vollständige Lauf direkt in SSMS ausgelöst werden:

```sql
EXEC orchestration.sp_run_full_load @triggered_by = 'MANUAL';
```

Oder nur ein einzelner Layer:

```sql
EXEC orchestration.sp_run_layer @layer = 'RAW';
EXEC orchestration.sp_run_layer @layer = 'CLEANSED';
EXEC orchestration.sp_run_layer @layer = 'MART';
```

---

## Audit-Abfragen

Laufstatus aller Pipelines:

```sql
SELECT pipeline_id, layer, table_name, load_sequence, last_run_status, last_run_ts, last_batch_id
FROM orchestration.pipeline_config;
```

Letzter Job-Lauf:

```sql
SELECT TOP 1 * FROM audit.job_log
ORDER BY start_ts DESC;
```

Alle Loads des letzten Job-Laufs:

```sql
WITH last_job_run AS (
	SELECT TOP 1 job_run_id
	FROM audit.job_log
	ORDER BY start_ts DESC
)
SELECT * FROM audit.load_log
WHERE job_run_id = (SELECT job_run_id FROM last_job_run);
```

Fehler des letzten Job-Laufs:

```sql
WITH last_job_run AS (
	SELECT TOP 1 job_run_id
	FROM audit.job_log
	ORDER BY start_ts DESC
)
SELECT * FROM audit.error_log
WHERE job_run_id = (SELECT job_run_id FROM last_job_run);
```

Fehler eines Batches:

```sql
SELECT * FROM audit.error_log
WHERE batch_id = '<batch_id>';
```

DQ-Probleme des letzten Job-Laufs:

```sql
WITH last_job_run AS (
	SELECT TOP 1 job_run_id
	FROM audit.job_log
	ORDER BY start_ts DESC
)
SELECT * FROM audit.dq_log
WHERE job_run_id = (SELECT job_run_id FROM last_job_run);
```

DQ-Probleme eines Batches:

```sql
SELECT table_name, column_name, issue, affected_row_count
FROM audit.dq_log
WHERE batch_id = '<batch_id>'
ORDER BY table_name, affected_row_count DESC;
```
