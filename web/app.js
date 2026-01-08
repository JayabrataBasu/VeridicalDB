document.addEventListener('DOMContentLoaded', () => {
  const runBtn = document.getElementById('run')
  const sqlEl = document.getElementById('sql')
  const statusEl = document.getElementById('status')
  const resultsEl = document.getElementById('results')

  runBtn.addEventListener('click', async () => {
    statusEl.textContent = 'Status: running...'
    resultsEl.textContent = ''
    try {
      const resp = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: sqlEl.value })
      })
      if (!resp.ok) throw new Error('server error: ' + resp.status)
      const data = await resp.json()
      renderResults(data)
      statusEl.textContent = 'Status: done'
    } catch (err) {
      statusEl.textContent = 'Status: error'
      resultsEl.textContent = 'Error: ' + err.message
    }
  })

  function renderResults(data) {
    const cols = data.columns || []
    const rows = data.rows || []
    if (rows.length === 0) {
      resultsEl.textContent = 'No rows returned.'
      return
    }
    const table = document.createElement('table')
    table.style.width = '100%'
    table.style.borderCollapse = 'collapse'
    const thead = document.createElement('thead')
    const tr = document.createElement('tr')
    cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; th.style.textAlign='left'; th.style.padding='6px'; tr.appendChild(th) })
    thead.appendChild(tr)
    table.appendChild(thead)
    const tbody = document.createElement('tbody')
    rows.forEach(r => {
      const tr = document.createElement('tr')
      r.forEach(cell => { const td = document.createElement('td'); td.textContent = String(cell); td.style.padding='6px'; tr.appendChild(td) })
      tbody.appendChild(tr)
    })
    table.appendChild(tbody)
    resultsEl.innerHTML = ''
    resultsEl.appendChild(table)
  }
})