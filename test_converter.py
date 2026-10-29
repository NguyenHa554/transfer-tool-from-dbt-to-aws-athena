import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from http.server import ThreadingHTTPServer

import sqlglot
from sqlglot import exp

from converter import Converter
from server import make_handler, LocalHTTPServer


class ConverterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp.name)
        self.write('dbt/dbt_project.yml', "name: sample\nvars:\n  env_bronze: \"{{ target.name }}_bronze\"\n  env_landing: \"{{ target.name }}_lnd\"\n  etl_date: '2026-07-01'\n  cutoff: '2026-06-01'\n")
        self.write('dbt/models/bronze/brz_customer.sql', "{{ config(alias='customer_snapshot', schema=var('env_bronze'), post_hook='OPTIMIZE {{ this }}') }} SELECT 1")
        self.write('dbt/models/landing/lnd_key.sql', "{{ config(schema=var('env_landing'), alias='ods_keys', pre_hook=[\"{{ cleanup() }}\"]) }} SELECT 1")
        self.write('dbt/models/km_staging/stg_customer.sql', "{{ config(schema='dev_km_stg', alias='customer') }} SELECT 1")
        self.write('dbt/models/source.yml', 'version: 2\nsources:\n- name: landing\n  schema: dev_lnd\n  tables:\n  - name: customer\n    identifier: customer_raw\n')
        self.write('dbt/macros/pure.sql', "{% macro twice(x) %}({{ x }} + {{ x }}){% endmacro %}")
        self.write('dbt/macros/runtime.sql', "{% macro runtime() %}{{ run_query('select 1') }}{% endmacro %}")
        self.converter = Converter(self.repo)

    def tearDown(self):
        self.temp.cleanup()

    def write(self, name, text):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8')

    def convert(self, sql, **kwargs):
        return self.converter.convert({'sql': sql, **kwargs})

    def test_refs_use_aliases_and_ignore_hooks(self):
        result = self.convert("SELECT * FROM {{ ref('brz_customer') }} b LEFT JOIN {{ ref('lnd_key') }} k ON b.id=k.id LEFT JOIN {{ ref('stg_customer') }} s ON s.id=b.id")
        self.assertEqual(result['status'], 'converted', result['errors'])
        for name in ('dev_bronze.customer_snapshot', 'dev_lnd.ods_keys', 'dev_km_stg.customer'):
            self.assertIn(name, result['sql'])
        self.assertEqual(len(result['mappings']), 3)

    def test_inline_config_ignored_without_evaluating_runtime_arguments(self):
        result = self.convert("{{ config(pre_hook=adapter.run_query('x'), alias='unused') }} SELECT 1")
        self.assertEqual(result['status'], 'converted', result['errors'])

    def test_bare_model_names_use_existing_ref_resolution(self):
        for name in ('brz_customer', 'lnd_key', 'stg_customer'):
            with self.subTest(name=name):
                lookup = self.convert(f'  {name};  ')
                query = self.convert("SELECT * FROM {{ ref('" + name + "') }}")
                self.assertEqual(lookup['status'], 'converted', lookup['errors'])
                self.assertEqual(lookup['output_kind'], 'relation')
                self.assertEqual(lookup['sql'], query['mappings'][0]['table'])
                self.assertEqual(lookup['mappings'][0]['reference'], name)
                self.assertNotIn('SELECT', lookup['sql'])
                self.assertIn('SELECT', query['sql'])

    def test_unknown_bare_model_does_not_guess_relation(self):
        result = self.convert('unknown_model')
        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['mappings'][0]['status'], 'Unknown')

    def test_query_with_raw_table_name_unchanged(self):
        result = self.convert('SELECT * FROM brz_customer')
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertIn('FROM brz_customer', result['sql'])
        self.assertEqual(result['mappings'], [])

    def test_bronze_suffix_once(self):
        result = self.convert("SELECT * FROM {{ ref('brz_invoice_tdh_sds') }} UNION ALL SELECT * FROM {{ ref('brz_invoice_tdh_sds_1d') }}")
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertNotIn('_1d_1d', result['sql'])
        self.assertTrue(all(m['status'] == 'Rule-based' for m in result['mappings']))

    def test_unknown_ref_and_source_blocked(self):
        for sql in ("SELECT * FROM {{ ref('mystery') }}", "SELECT * FROM {{ source('mystery', 'table') }}"):
            result = self.convert(sql)
            self.assertEqual(result['status'], 'blocked')
            self.assertEqual(result['mappings'][0]['status'], 'Unknown')

    def test_yaml_source_identifier(self):
        result = self.convert("SELECT * FROM {{ source('landing', 'customer') }}")
        self.assertIn('dev_lnd.customer_raw', result['sql'])

    def test_variable_precedence_and_missing(self):
        result = self.convert("SELECT '{{ var('etl_date', '2025-01-01') }}', '{{ var('cutoff') }}'", etl_date='2026-08-01')
        self.assertIn('2026-08-01', result['sql'])
        self.assertIn('2026-06-01', result['sql'])
        result = self.convert("SELECT '{{ var('etl_date', '2025-01-01') }}'")
        self.assertIn('2026-07-01', result['sql'])
        self.assertEqual(self.convert("SELECT '{{ var('missing') }}'")['status'], 'blocked')

    def test_default_business_dates_override_project_values(self):
        self.write('dbt/dbt_project.yml', "name: sample\nvars:\n  etl_date: '2026-07-31'\n  ds_dt: '2026-07-31'\n  t24_dm_dt: '2026-09-30'\n")
        result = self.convert("SELECT DATE '{{ var('etl_date') }}', DATE '{{ var('ds_dt') }}', DATE '{{ var('t24_dm_dt') }}'", etl_date='')
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertEqual(result['sql'].count('2026-07-01'), 2)
        self.assertNotIn('2026-07-31', result['sql'])
        self.assertIn('2026-09-30', result['sql'])
        self.assertTrue(all(v['origin'] == 'Application default (20260701)' for v in result['variables'] if v['name'] in ('etl_date', 'ds_dt')))

    def test_supplied_dates_and_independent_ds_dt(self):
        sql = "SELECT '{{ var('etl_date') }}', '{{ var('ds_dt') }}'"
        result = self.convert(sql, etl_date='20260815')
        self.assertEqual(result['sql'].count('2026-08-15'), 2)
        result = self.convert(sql, variables={'etl_date':'20260815', 'ds_dt':'20260901'})
        self.assertIn('2026-08-15', result['sql'])
        self.assertIn('2026-09-01', result['sql'])
        result = self.convert(sql, etl_date='20260702', variables={'etl_date':'20260815', 'ds_dt':'20260901'})
        self.assertIn('2026-07-02', result['sql'])
        self.assertIn('2026-09-01', result['sql'])

    def test_incremental_explicit_choice(self):
        sql = "SELECT * FROM {{ ref('stg_customer') }} {% if is_incremental() %} WHERE id>10 {% else %} WHERE id>0 {% endif %}"
        self.assertEqual(self.convert(sql)['status'], 'blocked')
        self.assertIn('id > 10', self.convert(sql, mode='incremental')['sql'])
        self.assertIn('id > 0', self.convert(sql, mode='full_refresh')['sql'])

    def test_pure_macro_and_runtime_block(self):
        result = self.convert("SELECT {{ twice('x') }} AS total FROM {{ ref('stg_customer') }}")
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertIn('x + x', result['sql'])
        self.assertEqual(self.convert('SELECT {{ runtime() }}')['status'], 'blocked')
        self.assertEqual(self.convert('SELECT {{ unknown_macro() }}')['status'], 'blocked')

    def test_hash_not_substituted(self):
        result = self.convert('SELECT xxhash64(id) AS id FROM t')
        self.assertEqual(result['status'], 'blocked')
        self.assertTrue(any('xxhash64' in error.lower() for error in result['errors']))

    def test_blocked_function_does_not_skip_type_normalization(self):
        result = self.convert('SELECT pmod(conv(code,16,10), 100) AS key, CAST(NULL AS STRING) AS a, TRY_CAST(x AS FLOAT) AS b FROM t')
        self.assertEqual(result['status'], 'blocked')
        self.assertIn('AS VARCHAR', result['sql'])
        self.assertIn('AS REAL', result['sql'])
        self.assertNotRegex(result['sql'], r'(?i)\bAS\s+(STRING|FLOAT)\b')
        self.assertIn('PMOD', result['sql'])
        self.assertIn('CONV', result['sql'])
        self.assertTrue(any('SQL draft only' in warning for warning in result['warnings']))

    def test_dialect_and_precision(self):
        result = self.convert("SELECT CAST(amount AS DECIMAL(38,10)), CAST(id AS STRING), datediff(b,a), date_add(a,3), a RLIKE '^x' FROM t")
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertIn('DECIMAL(38, 10)', result['sql'])
        self.assertIn('VARCHAR', result['sql'])
        self.assertIn('DATE_DIFF', result['sql'])
        self.assertIn('REGEXP_LIKE', result['sql'])

    def test_regex_empty_match_and_least_nulls(self):
        result = self.convert("SELECT regexp_extract(x, '(x)', 1), least(x,y), greatest(x,y) FROM t")
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertIn('COALESCE(REGEXP_EXTRACT', result['sql'])
        self.assertIn('ARRAY_MIN', result['sql'])
        self.assertIn('ARRAY_MAX', result['sql'])
        self.assertIn('IS NULL', result['sql'])

    def test_athena_query_cast_and_try_cast_types(self):
        for source_type, target_type in [('STRING', 'VARCHAR'), ('INT', 'INTEGER'), ('LONG', 'BIGINT'), ('FLOAT', 'REAL')]:
            with self.subTest(source_type=source_type):
                result = self.convert(f'SELECT CAST(value AS {source_type}) AS a, TRY_CAST(value AS {source_type}) AS b FROM t')
                self.assertEqual(result['status'], 'converted', result['errors'])
                tree = sqlglot.parse_one(result['sql'], read='trino')
                casts = list(tree.find_all(exp.Cast))
                self.assertEqual(len(casts), 2)
                self.assertTrue(all(cast.to.sql(dialect='trino') == target_type for cast in casts))
                if source_type != 'LONG':
                    self.assertTrue(any('Athena query data types:' in note for note in result['notes']))
                self.assertNotRegex(result['sql'], r'(?i)\bAS\s+(STRING|INT|LONG|FLOAT)\b')

    def test_nested_query_types_and_typed_null_preserve_precision(self):
        result = self.convert('SELECT CAST(NULL AS MAP<STRING, ARRAY<FLOAT>>) AS a, TRY_CAST(NULL AS ARRAY<LONG>) AS b, CAST(NULL AS DECIMAL(38,10)) AS amount, CAST(NULL AS DOUBLE) AS rate')
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertIn('MAP(VARCHAR, ARRAY(REAL))', result['sql'])
        self.assertIn('ARRAY(BIGINT)', result['sql'])
        self.assertIn('DECIMAL(38, 10)', result['sql'])
        self.assertIn('DOUBLE', result['sql'])
        casts = list(sqlglot.parse_one(result['sql'], read='trino').find_all(exp.Cast))
        self.assertTrue(all(isinstance(cast.this, exp.Null) for cast in casts))

    def test_type_conversion_does_not_replace_literals_or_identifiers(self):
        result = self.convert("SELECT 'CAST(x AS STRING)' AS example, STRING AS string_column, CAST(id AS STRING) AS converted FROM t")
        self.assertEqual(result['status'], 'converted', result['errors'])
        tree = sqlglot.parse_one(result['sql'], read='trino')
        self.assertEqual(tree.expressions[0].this.this, 'CAST(x AS STRING)')
        self.assertEqual(tree.expressions[1].this.name, 'STRING')
        self.assertEqual(tree.expressions[2].this.to.sql(dialect='trino'), 'VARCHAR')

    def test_hive_ddl_not_processed_as_query_type_conversion(self):
        result = self.convert('CREATE TABLE t (value STRING, amount FLOAT)')
        self.assertEqual(result['status'], 'blocked')
        self.assertTrue(any('DDL' in error for error in result['errors']))
        self.assertFalse(any('Athena query data types:' in note for note in result['notes']))

    def test_safe_date_and_nested_null_rewrites(self):
        result = self.convert("SELECT to_date(x, 'yyyyMMdd'), least(regexp_extract(x, '(x)', 1), y) FROM t")
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertIn('TRY(', result['sql'])
        self.assertIn('ARRAY_MIN', result['sql'])
        self.assertIn('COALESCE(REGEXP_EXTRACT', result['sql'])

    def test_integer_rounding_and_division(self):
        result = self.convert('SELECT CAST(1.9 AS INT), CAST(-1.9 AS INT), 1 / 2')
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertIn('TRUNCATE(1.9)', result['sql'])
        self.assertIn('TRUNCATE(-1.9)', result['sql'])
        self.assertIn('DOUBLE', result['sql'])

    def test_add_months_converts_to_calendar_month_date_add(self):
        cases = [
            "add_months(DATE '2026-01-31', 1)",
            "add_months(DATE '2024-01-31', 1)",
            "add_months(DATE '2026-03-31', -1)",
            "add_months(DATE '2026-07-01', 0)",
            'add_months(start_date, months_count)',
            'add_months(NULL, 1)',
            'add_months(start_date, NULL)',
            "add_months(TIMESTAMP '2026-01-31 12:34:56', 1)",
        ]
        for expression in cases:
            with self.subTest(expression=expression):
                result = self.convert(f'SELECT {expression} AS next_date FROM t')
                self.assertEqual(result['status'], 'converted', result['errors'])
                self.assertNotIn('ADD_MONTHS(', result['sql'].upper())
                converted = sqlglot.parse_one(result['sql'], read='trino').expressions[0].this
                self.assertIsInstance(converted, exp.DateAdd)
                self.assertEqual(converted.args['unit'].name.upper(), 'MONTH')
                self.assertIsInstance(converted.this, exp.Cast)
                self.assertEqual(converted.this.to.this, exp.DataType.Type.DATE)
                original = sqlglot.parse_one(f'SELECT {expression}', read='spark').expressions[0]
                offset = converted.expression
                # Trino's DATE_ADD generator coerces dynamic offsets to BIGINT.
                if isinstance(offset, exp.Cast) and offset.to.this == exp.DataType.Type.BIGINT:
                    offset = offset.this
                self.assertEqual(offset.sql(), original.expression.sql())
                self.assertTrue(any('ADD_MONTHS' in note for note in result['notes']))

    def test_nested_add_months_no_unsupported_function_remains(self):
        result = self.convert("SELECT add_months(add_months(d, 1), -2) AS d2 FROM t WHERE add_months(d, n) > DATE '2026-07-01'")
        self.assertEqual(result['status'], 'converted', result['errors'])
        self.assertNotIn('ADD_MONTHS(', result['sql'].upper())
        self.assertEqual(len(list(sqlglot.parse_one(result['sql'], read='trino').find_all(exp.DateAdd))), 3)

    def test_structural_preservation(self):
        sql = "WITH a AS (SELECT id, CASE WHEN x>0 THEN x ELSE NULL END v, ROW_NUMBER() OVER (PARTITION BY id ORDER BY d DESC) rn FROM t WHERE active='Y') SELECT a.id, v FROM a LEFT JOIN u ON a.id=u.id WHERE rn=1 UNION ALL SELECT id, v FROM old"
        result = self.convert(sql)
        self.assertEqual(result['status'], 'converted', result['errors'])
        before, after = sqlglot.parse_one(sql, read='spark'), sqlglot.parse_one(result['sql'], read='trino')
        for cls in (exp.Join, exp.Where, exp.Case, exp.Union, exp.Window, exp.Select):
            self.assertEqual(len(list(before.find_all(cls))), len(list(after.find_all(cls))), cls.__name__)
        self.assertEqual(next(after.find_all(exp.Join)).side, 'LEFT')
        self.assertFalse(next(after.find_all(exp.Union)).args['distinct'])

    def test_current_source_beats_stale_manifest_and_refreshes(self):
        self.write('dbt/target/manifest.json', json.dumps({'nodes': {'model.sample.stg_customer': {'resource_type':'model','name':'stg_customer','alias':'old_alias','schema':'old_schema'}}}))
        self.write('dbt/models/km_staging/stg_customer.sql', "{{ config(schema='dev_km_stg', alias='new_customer') }} SELECT 1")
        self.assertIn('dev_km_stg.new_customer', self.convert("SELECT * FROM {{ ref('stg_customer') }}")['sql'])

    def test_no_ddl_or_multiple_statements(self):
        for sql in ('DROP TABLE x', 'SELECT 1; SELECT 2;', 'INSERT INTO x SELECT 1'):
            self.assertEqual(self.convert(sql)['status'], 'blocked')

    def test_ephemeral_ref_blocked(self):
        self.write('dbt/models/km_staging/temp.sql', "{{ config(materialized='ephemeral') }} SELECT 1")
        self.assertEqual(self.convert("SELECT * FROM {{ ref('temp') }}")['status'], 'blocked')

    def test_invalid_payload(self):
        for payload in ({'sql':''}, {'sql':'SELECT 1','variables':[]}, {'sql':'SELECT 1','etl_date':'not-a-date'}):
            with self.assertRaises(ValueError):
                self.converter.convert(payload)

    def test_http_api_and_cross_origin(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(self.converter))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f'http://127.0.0.1:{server.server_port}'
            self.assertEqual(json.load(urlopen(base+'/api/health'))['status'], 'ok')
            payload = json.dumps({'sql':'SELECT 1'}).encode()
            request = Request(base+'/api/convert', data=payload, headers={'Content-Type':'application/json'})
            self.assertEqual(json.load(urlopen(request))['status'], 'converted')
            request.add_header('Origin', 'https://example.com')
            with self.assertRaises(HTTPError) as error:
                urlopen(request)
            self.assertEqual(error.exception.code, 403)
        finally:
            server.shutdown()
            server.server_close()

    def test_occupied_port_is_rejected(self):
        server = LocalHTTPServer(('127.0.0.1', 0), make_handler(self.converter))
        try:
            with self.assertRaises(OSError):
                LocalHTTPServer(('127.0.0.1', server.server_port), make_handler(self.converter))
        finally:
            server.server_close()


class RealRepoTests(unittest.TestCase):
    def test_bare_account_model_name_matches_ref(self):
        repo = Path(__file__).resolve().parent.parent / 'km-etl-newcore'
        if not repo.exists():
            self.skipTest('Read-only reference repo unavailable')
        converter = Converter(repo)
        lookup = converter.convert({'sql': 'brz_tdh_sds_account_snp'})
        query = converter.convert({'sql': "SELECT * FROM {{ ref('brz_tdh_sds_account_snp') }}"})
        self.assertEqual(lookup['status'], 'converted', lookup['errors'])
        self.assertEqual(lookup['sql'], 'dev_bronze.tdh_sds_1d_account_snp')
        self.assertEqual(lookup['sql'], query['mappings'][0]['table'])

    def test_trs_invoice_blocked_macro_retains_normalized_types(self):
        repo = Path(__file__).resolve().parent.parent / 'km-etl-newcore'
        model = repo / 'dbt/models/km_staging/trs/km_stg_trs_cl_invoice.sql'
        if not model.exists():
            self.skipTest('Read-only reference model unavailable')
        result = Converter(repo).convert({'sql': model.read_text(encoding='utf-8-sig'), 'etl_date': '2026-07-01'})
        self.assertEqual(result['status'], 'blocked')
        self.assertNotRegex(result['sql'], r'(?i)\bAS\s+STRING\b')
        self.assertRegex(result['sql'], r'(?i)CAST\(NULL AS VARCHAR\) AS FEE_KEY')
        self.assertRegex(result['sql'], r'(?i)CAST\(NULL AS VARCHAR\) AS FEE_NO')
        self.assertTrue(any('conv' in error.lower() for error in result['errors']))

    def test_lending_and_forex_current_sources(self):
        repo = Path(__file__).resolve().parent.parent / 'km-etl-newcore'
        if not repo.exists():
            self.skipTest('Read-only reference repo unavailable')
        converter = Converter(repo)
        base = repo / 'dbt/models/km_staging/tdh_sds'
        for filename in ('lending/km_stg_tdh_sds_cl_repay_mapping_key.sql', 'forex/km_stg_tdh_sds_fx_rev_deal_mapping_key.sql'):
            result = converter.convert({'sql':(base/filename).read_text(encoding='utf-8-sig'), 'etl_date':'2026-07-01'})
            self.assertEqual(result['status'], 'converted', result['errors'])
            self.assertNotIn('{{', result['sql'])
            self.assertTrue(all(row['status'] != 'Unknown' for row in result['mappings']))
            if 'repay' in filename:
                self.assertIn('DD.DD_KEY = MK.DD_KEY', result['sql'])
                self.assertNotIn('ADD_MONTHS(', result['sql'].upper())


if __name__ == '__main__':
    unittest.main(verbosity=2)
