'use strict';
const $ = id => document.getElementById(id);
let result = null;
let busy = false;
function list(id, values) {
  $(id).replaceChildren(...values.map(value => { const li = document.createElement('li'); li.textContent = value; return li; }));
}
function invalidate() {
  result = null;
  $('copy').disabled = $('download').disabled = true;
  $('result-status').textContent = 'Input changed · convert again';
}
['input', 'etl-date', 'variables', 'mode'].forEach(id => $(id).addEventListener('input', () => {
  invalidate();
  $('input-size').textContent = `${$('input').value.length.toLocaleString()} characters`;
}));
$('convert-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (busy) return;
  busy = true;
  ['input','etl-date','variables','mode','clear'].forEach(id => $(id).disabled = true);
  $('convert').disabled = true;
  $('convert').textContent = 'Converting…';
  $('feedback').textContent = 'Reading local metadata and converting SQL…';
  try {
    const variables = JSON.parse($('variables').value || '{}');
    const dateInput = $('etl-date').value;
    if (dateInput && !/^[0-9]{8}$/.test(dateInput)) throw new Error('ETL date must use yyyymmdd, for example 20260701.');
    const etlDate = dateInput ? `${dateInput.slice(0,4)}-${dateInput.slice(4,6)}-${dateInput.slice(6,8)}` : '';
    const response = await fetch('/api/convert', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({sql:$('input').value, etl_date:etlDate, variables, mode:$('mode').value})});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Conversion request failed.');
    result = data;
    $('output').value = data.sql;
    const relationOnly = data.output_kind === 'relation';
    $('output-title').textContent = data.status === 'converted' ? (relationOnly ? 'Athena table' : 'Athena SQL') : 'SQL draft · not executable';
    $('output-engine').textContent = data.status === 'converted' ? (relationOnly ? 'Athena relation' : 'Athena Engine 3') : 'Incomplete conversion';
    $('report').hidden = false;
    $('result-status').textContent = data.status === 'converted' ? (relationOnly ? 'Table resolved · catalog unverified' : 'Converted · execution unverified') : 'DRAFT · conversion blocked';
    $('copy').disabled = $('download').disabled = data.status !== 'converted';
    $('mapping-count').textContent = `${data.mappings.length} relation${data.mappings.length === 1 ? '' : 's'}`;
    $('mapping-body').replaceChildren(...data.mappings.map(mapping => {
      const tr = document.createElement('tr');
      ['reference','table','status','evidence'].forEach(key => { const td = document.createElement('td'); td.textContent = mapping[key]; tr.append(td); });
      return tr;
    }));
    list('notes', data.notes.length ? data.notes : ['No completed conversion steps to report.']);
    list('errors', data.errors);
    list('warnings', data.warnings);
    $('resolved-vars').replaceChildren(...data.variables.map(variable => {
      const div = document.createElement('div'); div.className = 'variable'; div.textContent = `${variable.name} = ${JSON.stringify(variable.value)}`;
      const origin = document.createElement('span'); origin.textContent = variable.origin; div.append(origin); return div;
    }));
    $('feedback').textContent = data.status === 'converted' ? (relationOnly ? 'Table name resolved. Copy it to use in your Athena query.' : 'Conversion complete. Review mappings before running in Athena.') : 'Resolve the listed errors before using this SQL.';
  } catch (error) {
    invalidate();
    $('feedback').textContent = error.message;
  } finally {
    busy = false;
    ['input','etl-date','variables','mode','clear'].forEach(id => $(id).disabled = false);
    $('convert').disabled = false;
    $('convert').textContent = 'Convert SQL →';
  }
});
$('copy').addEventListener('click', async () => {
  if (result?.status !== 'converted') return;
  try { await navigator.clipboard.writeText(result.sql); $('feedback').textContent = result.output_kind === 'relation' ? 'Athena table name copied.' : 'Athena SQL copied.'; }
  catch { $('output').focus(); $('output').select(); $('feedback').textContent = 'Press Ctrl+C to copy the selected SQL.'; }
});
$('download').addEventListener('click', () => {
  if (result?.status !== 'converted') return;
  const url = URL.createObjectURL(new Blob([result.sql + '\n'], {type:'text/plain;charset=utf-8'}));
  const link = document.createElement('a'); link.href = url; link.download = result.output_kind === 'relation' ? 'athena_table.txt' : 'athena_query.sql'; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});
$('clear').addEventListener('click', () => {
  $('input').value = ''; $('output').value = ''; $('report').hidden = true;
  $('output-title').textContent = 'Athena SQL'; $('output-engine').textContent = 'Athena Engine 3';
  invalidate(); $('input-size').textContent = '0 characters'; $('result-status').textContent = 'Waiting for input';
  $('feedback').textContent = 'SQL stays on this computer.'; $('input').focus();
});
fetch('/api/health').then(response => response.json()).then(data => {
  $('repo-status').textContent = `Read-only repo: ${data.repo} · ${data.metadata.models.toLocaleString()} models`;
}).catch(() => { $('repo-status').textContent = 'Local server unavailable. Start with start.ps1.'; });
