# SQL Transfer — dbt to Athena

## Hướng dẫn nhanh

### Mở ứng dụng

Mở **PowerShell**, chạy:

```powershell
Set-Location -LiteralPath 'C:\nguyen\Projects\VIB2\transfer tool'
.\start.ps1
```

Giữ cửa sổ PowerShell mở và truy cập **[http://127.0.0.1:8001](http://127.0.0.1:8001)**
trong trình duyệt. Lần đầu cần Python 3.12 trở lên và Internet để cài thư viện.

Nếu PowerShell chặn file `.ps1` và môi trường `.venv` đã được cài đặt, chạy:

```powershell
Set-Location -LiteralPath 'C:\nguyen\Projects\VIB2\transfer tool'
.\.venv\Scripts\python.exe server.py
```

### Sử dụng giao diện

1. Nhập **ETL date** dạng `yyyymmdd`, ví dụ `20260701`. Bỏ trống mặc định dùng
   **20260701**, kể cả project khai báo ngày khác. Ngày trong SQL được chuẩn hóa
   thành `2026-07-01`. Biến `ds_dt` dùng cùng ngày này nếu bạn không truyền riêng
   trong **Additional variables**; các ngày nghiệp vụ khác giữ quy tắc hiện có.
2. Dán toàn bộ câu dbt SQL vào ô **dbt SQL**. Có thể giữ nguyên `config()`,
   `ref()`, `source()`, `var()` và macro.
   Bạn cũng có thể chỉ nhập tên model, ví dụ `brz_tdh_sds_account_snp`, rồi nhấn
   **Convert SQL** để nhận `dev_bronze.tdh_sds_1d_account_snp`. Tra tên dùng cùng
   alias/schema resolver của `ref()`; kết quả là tên bảng để copy, không phải
   câu SELECT. Cách xử lý SQL có `FROM {{ ref(...) }}` giữ nguyên.
3. Nếu SQL có `is_incremental()`, chọn **Full refresh** để dùng nhánh `false`
   hoặc **Incremental** để dùng nhánh `true`.
4. Nếu cần bổ sung biến, mở **Additional variables** và nhập JSON, ví dụ:

   ```json
   {"t24_dm_dt": "2026-09-30"}
   ```

5. Nhấn **Convert SQL**, rồi xem **Table mapping**, **Conversion notes** và
   **Warnings & errors**.
6. Khi trạng thái là **Converted**, nhấn **Copy** hoặc **Download SQL**, sau đó
   dán SQL vào Athena để chạy. Nút **Clear** xóa SQL và kết quả đang hiển thị.

Nếu hiện **DRAFT · conversion blocked**, đọc lỗi và bổ sung thông tin còn thiếu
trước khi chuyển lại. Công cụ chỉ chuyển đổi SQL, không tự chạy query trên Athena.
Repo `km-etl-newcore` chỉ được đọc để tra metadata.

### Tắt ứng dụng và xử lý lỗi mở trang

- Nhấn **Ctrl+C** trong cửa sổ PowerShell đang chạy server để tắt ứng dụng.
- Nếu trình duyệt không kết nối được, kiểm tra server còn chạy và terminal có
  hiển thị `http://127.0.0.1:8001` hay không.
- Nếu cổng `8001` đang bận, ứng dụng có thể đã chạy; thử mở URL trên trước.
  Hoặc chạy cổng khác rồi mở URL tương ứng:

  ```powershell
  .\.venv\Scripts\python.exe server.py --port 8002
  ```

  Truy cập **[http://127.0.0.1:8002](http://127.0.0.1:8002)**.

Local Python + HTML/JavaScript application for converting pasted dbt Spark SQL
to a single SELECT query for Athena Engine 3 (Trino). No AI, API key, AWS access,
SQL execution, or dbt invocation is involved.

## Start on Windows

Requires Python 3.12 or later. In PowerShell:

```powershell
Set-Location -LiteralPath 'C:\nguyen\Projects\VIB2\transfer tool'
.\start.ps1
```

Open **http://127.0.0.1:8001**. The first start creates `.venv` and installs
the pinned dependencies. Dependency installation needs access to PyPI; normal
conversion is offline. To start directly after installation:

```powershell
.\.venv\Scripts\python.exe server.py
```

The server binds only to `127.0.0.1`. If port 8001 is occupied, it exits with a
clear message. To use another port: `.\start.ps1 -Port 8002`. Stop with Ctrl+C.
If PowerShell blocks local scripts, use the direct Python commands rather than
changing your machine's execution policy:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe server.py
```

## Use

1. Paste one `SELECT` or `WITH ... SELECT` query, including dbt Jinja if present.
2. Optionally enter the ETL date as **yyyymmdd** (for example `20260701`)
   and supply additional variables as a JSON object. The UI converts the date
   to ISO `2026-07-01` for the API and SQL; impossible calendar dates are rejected.
3. If the SQL calls `is_incremental()`, choose Full refresh or Incremental.
4. Click **Convert SQL**. Review relation mappings, resolved variables, notes,
   and warnings. Copy or download `athena_query.sql` when conversion succeeds.

An explicit ETL date overrides `etl_date` in the JSON variables. If neither is
supplied, `etl_date` defaults to **20260701** (`2026-07-01` in SQL), overriding
both project declarations and defaults in `var('etl_date', ...)`. `ds_dt` uses
the selected/default ETL date unless explicitly supplied in JSON. These two
date variables accept `YYYYMMDD` or ISO `YYYY-MM-DD` in the API/JSON and are
normalized to ISO. For other variables the precedence remains user JSON,
the default argument to `var()`, then the project declaration. Other business
dates, such as `t24_dm_dt`, are not changed. Resolved variables show whether
the date came from user input or the application default.

If a variable/macro/reference or SQL construct cannot be resolved reliably,
the output is marked **DRAFT · conversion blocked**. Copy/download buttons
stay disabled. For a successfully parsed query, type names are normalized even
when another conversion step is blocked. Unsupported Spark functions or
unresolved Jinja can still remain, so the panel is labelled **SQL draft · not
executable**, not Athena SQL. Do not run this draft in Athena.

## Read-only metadata

The default metadata repo is the adjacent `../km-etl-newcore` directory.
For another checkout:

```powershell
.\.venv\Scripts\python.exe server.py --repo 'C:\path\to\repo'
```

Current model SQL, project configurations, model YAML, source YAML and macro
definitions are indexed. `dbt/target/manifest.json`, when available, is read as
supplemental metadata; an entry cannot override current source or revive a
deleted model. Changes to metadata files are picked up on the next conversion.
The metadata repo is never written to. No dbt compile, profile connection,
database operation, or S3 write occurs.

Relation labels:

- **Verified (dbt)**: resolved from current dbt configuration or source YAML.
  This is not verification against Athena/Glue.
- **Rule-based**: derived from the supplied layer naming convention.
- **Unknown**: insufficient or ambiguous metadata; conversion is blocked.

Default schemas: bronze → `dev_bronze`, landing → `dev_lnd`, staging →
`dev_km_stg`. Explicit model aliases take priority. The `_tdh_sds` →
`_tdh_sds_1d` convention applies only to bronze fallback names without aliases;
an existing `_1d` suffix is not repeated. Current explicit schema expressions
are evaluated for `target.name = 'dev'`. Database-qualified JDBC schemas are
not guessed to be Athena relations.

## Conversion policy and limits

- Query type expressions (including `CAST`, `TRY_CAST`, typed nulls and nested
  arrays/maps) use these Athena Engine 3 / Trino names:

  | Spark query type | Athena query type |
  | --- | --- |
  | `STRING` | `VARCHAR` |
  | `INT` | `INTEGER` |
  | `LONG` | `BIGINT` |
  | `FLOAT` (32-bit) | `REAL` (32-bit) |

  `DOUBLE` and `DECIMAL(p,s)` retain their original precision. Type conversion
  uses the SQL syntax tree, so string literals, identifiers, cast operands and
  null values are not replaced as text. CAST/TRY_CAST behavior continues to
  follow the Spark parser's existing null/ANSI policy, with the runtime warning
  described below. Hive-compatible DDL declarations have different naming
  rules and are not rewritten: this application accepts SELECT queries only.
  See [AWS Athena data types](https://docs.aws.amazon.com/athena/latest/ug/data-types.html).
- The converter uses a Spark syntax tree and emits Trino SQL without running
  a SQL optimizer. It does not intentionally change joins, predicates, grouping,
  unions, output ordering, precision or window definitions.
- Spark execution hints such as `BROADCAST` are removed and reported because
  Athena cannot use them.
- `REGEXP_EXTRACT` preserves Spark's empty-string result on no match.
  `LEAST`/`GREATEST` preserve Spark's handling of null operands.
- Safe `TO_DATE` expressions use `TRY` to preserve null on invalid input.
  Integer casts of known numeric expressions truncate toward zero. Casts of
  columns with unknown source types still require checking against real types.
- Pure SQL-producing Jinja macros are rendered from the actual repo definition
  in a strict sandbox. Unknown macros and runtime/database-dependent macros
  block conversion. Unsupported macros are not replaced with invented SQL.
- Hash functions such as Spark `xxhash64` are blocked rather than substituted
  with a Trino hash that produces different keys.
- Parse success is not proof of equivalent runtime behavior. Source column
  types, implicit coercion, ANSI casting settings, timestamp/timezone settings,
  table existence and data correctness are not verified without a database.
  `CURRENT_TIMESTAMP` and recognized unsupported constructs block conversion;
  check cast warnings before running a converted query.
- Raw relation names already written in SQL are retained. Only `ref()` and
  `source()` are automatically resolved; raw names are not catalog-verified.
- Version/package-specific refs, ephemeral model inlining and DDL/write
  statements are outside this version's supported input.
- SQL and results remain in browser/server memory and are not persisted.
  Clear removes the displayed query/results. Metadata and SQL never leave the
  computer during conversion.

## API and checks

`GET /api/health` returns the read-only repo path and index counts.

`POST /api/convert` takes:

The `sql` field also accepts a single bare model name. This returns
`output_kind: "relation"` and the resolved name in `sql`; it does not synthesize
a SELECT. SQL statements containing raw physical names keep their existing
behavior; name-only lookup does not rewrite those references.

```json
{
  "sql": "SELECT * FROM {{ ref('km_stg_tdh_sds_cl_repay_mapping_key') }}",
  "etl_date": "2026-07-01",
  "variables": {},
  "mode": "full_refresh"
}
```

The response contains `status` (`converted`/`blocked`), `sql`, `mappings`,
`variables`, `notes`, `warnings`, `errors`, and metadata counts. Input errors
return HTTP 400. Maximum request body: 2 MB.

Run tests:

```powershell
.\.venv\Scripts\python.exe -m unittest -v
```

Tests cover metadata freshness, aliases/sources, variable precedence,
incremental branches, macros, unsupported hashes, dialect conversion, decimal
precision, relational structure, and HTTP behavior. When the adjacent repo
exists, its current CL_REPAY_MAPPING_KEY and FX_REV_DEAL_MAPPING_KEY models
are also checked without executing either model.
