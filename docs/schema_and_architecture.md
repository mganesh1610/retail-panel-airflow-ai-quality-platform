# Retail Consumer Shopping Panel — Schema, Airflow Operators & Model Gating Architecture

## 1. Source Dataset & Provenance
- **Primary Verified Public Dataset:** UCI Online Retail Consumer Shopping Basket Dataset (Daqing Chen, 2015, DOI: `10.24432/C5BW33`, CC BY 4.0, https://archive.ics.uci.edu/dataset/352/online+retail).
- **Archive SHA-256:** `f5385cbb54bbebf7196389109c6b0621faab0c304e3702548165e71c84aede8b`.
- **Observed Volume:** `541,909` source shopping basket lines across `25,900` invoices, `4,070` distinct `StockCode` identifiers, `4,372` known consumer accounts, and `38` countries (`2010-12-01` to `2011-12-09`).
- **Backup Public Dataset:** UCI Online Retail II (`1,067,371` invoice lines across `2009–2011`, DOI: `10.24432/C5CG6D`, https://archive.ics.uci.edu/dataset/502/online+retail+ii).

## 2. Bronze-Silver-Gold Schema & Quarantine Mapping

| Source Column | Target Warehouse Field | Type | Validation & Transformation Rule |
| :--- | :--- | :--- | :--- |
| `InvoiceNo` | `INVOICE_NO` | `VARCHAR(32)` | Trimmed string; prefix `'C'` flags `9,288` cancellation lines. |
| `StockCode` | `STOCK_CODE` | `VARCHAR(32)` | `4,070` distinct codes; non-merchandise codes (`POST`, `DOT`, `M`, `BANK CHARGES`, `B`, `CRUK`, `D`, `S`, `PADS`) tagged by `InternalAIQualityAgentOperator`. |
| `Description` | `DESCRIPTION` | `VARCHAR(512)` | Nullable item description preserved without synthetic imputation. |
| `Quantity` | `QUANTITY` | `INTEGER` | Signed unit quantity. |
| `InvoiceDate` | `INVOICE_DATE`, `YEAR_MONTH` | `TIMESTAMP_NTZ` | Verified within `2010-12-01` to `2011-12-09` (`0` out-of-range rows). |
| `UnitPrice` | `UNIT_PRICE_MILLS` | `BIGINT` | Converted via exact `Decimal` half-up rounding to integer mills (`GBP * 1000`); `2` negative-price rows quarantined. |
| `CustomerID` | `GOVERNED_CUSTOMER_ID` | `VARCHAR(32)` | `4,372` known consumer accounts normalized; `135,078` missing-ID lines governed in `UNKNOWN_GUEST` bucket. |
| `Country` | `COUNTRY` | `VARCHAR(128)` | `38` countries aggregated into `314` `YEAR_MONTH x COUNTRY` Gold cells. |
