// PSE Web UI — JavaScript glue for WASM engine
import init, { PseWasm } from './pkg/pse_wasm.js';

let engine = null;
let csvData = null;

async function initialize() {
    await init();
    engine = new PseWasm();
    updateStatus();
    document.getElementById('loading').style.display = 'none';
    document.getElementById('app').style.display = 'block';
}

// ─── CSV Upload ─────────────────────────────────────────────────────────────

function setupUpload() {
    const area = document.getElementById('upload-area');
    const input = document.getElementById('csv-input');

    area.addEventListener('click', () => input.click());
    area.addEventListener('dragover', (e) => {
        e.preventDefault();
        area.classList.add('drag-over');
    });
    area.addEventListener('dragleave', () => area.classList.remove('drag-over'));
    area.addEventListener('drop', (e) => {
        e.preventDefault();
        area.classList.remove('drag-over');
        if (e.dataTransfer.files.length > 0) handleFile(e.dataTransfer.files[0]);
    });
    input.addEventListener('change', (e) => {
        if (e.target.files.length > 0) handleFile(e.target.files[0]);
    });
}

function handleFile(file) {
    const reader = new FileReader();
    reader.onload = (e) => {
        csvData = e.target.result;
        processCSV(csvData);
    };
    reader.readAsText(file);
}

function loadSampleData() {
    csvData = PseWasm.sample_csv();
    processCSV(csvData);
}

function processCSV(csv) {
    const result = JSON.parse(engine.ingest_csv(csv));
    if (result.error) {
        alert('CSV parse error: ' + result.error);
        return;
    }
    document.getElementById('upload-status').textContent =
        `Ingested ${result.rows_ingested} rows, ${result.columns} columns, ${result.entities} entities`;
    showQualityReport(csv);
    updateStatus();
    switchTab('quality');
}

// ─── Engine Controls ────────────────────────────────────────────────────────

function runEngine() {
    const ticks = parseInt(document.getElementById('tick-slider').value);
    const result = JSON.parse(engine.run(ticks));
    document.getElementById('run-result').textContent =
        `Ran ${result.ticks_run} ticks → ${result.new_crystals} new crystals, ${result.memory_hits} memory hits`;
    showCrystals();
    showAccumulationCurve();
    updateStatus();
}

function resetObservations() {
    engine.reset_observations();
    csvData = null;
    document.getElementById('upload-status').textContent = '';
    document.getElementById('run-result').textContent = '';
    updateStatus();
    clearResults();
}

function resetAll() {
    engine.reset_all();
    csvData = null;
    document.getElementById('upload-status').textContent = '';
    document.getElementById('run-result').textContent = '';
    updateStatus();
    clearResults();
}

function clearResults() {
    document.getElementById('quality-content').innerHTML = '<div class="empty-state">Upload CSV data to see quality report</div>';
    document.getElementById('crystals-content').innerHTML = '<div class="empty-state">Run the engine to discover crystals</div>';
    document.getElementById('curve-content').innerHTML = '<div class="empty-state">Run the engine to see accumulation curve</div>';
}

// ─── Display Functions ──────────────────────────────────────────────────────

function updateStatus() {
    const s = JSON.parse(engine.status());
    document.getElementById('stat-ticks').textContent = s.total_ticks;
    document.getElementById('stat-crystals').textContent = s.crystals;
    document.getElementById('stat-memory').textContent = s.memory_size;
    document.getElementById('stat-hitrate').textContent =
        s.memory_size > 0 ? (s.hit_rate * 100).toFixed(1) + '%' : '—';
    document.getElementById('stat-vertices').textContent = s.graph_vertices;
}

function showQualityReport(csv) {
    const report = JSON.parse(engine.quality_report(csv));
    if (report.error) {
        document.getElementById('quality-content').innerHTML =
            `<div class="empty-state">${report.error}</div>`;
        return;
    }

    let html = '<h3 style="margin-bottom:0.75rem;font-size:0.95rem;">Column Statistics</h3>';
    html += '<table><thead><tr><th>Column</th><th>Type</th><th>Null%</th><th>Min</th><th>Max</th><th>Mean</th><th>Std</th></tr></thead><tbody>';
    for (const col of report.column_stats) {
        html += `<tr>
            <td>${col.name}</td>
            <td>${col.dtype}</td>
            <td>${col.null_pct.toFixed(1)}%</td>
            <td>${col.min != null ? col.min.toFixed(2) : '—'}</td>
            <td>${col.max != null ? col.max.toFixed(2) : '—'}</td>
            <td>${col.mean != null ? col.mean.toFixed(2) : '—'}</td>
            <td>${col.std != null ? col.std.toFixed(2) : '—'}</td>
        </tr>`;
    }
    html += '</tbody></table>';

    if (report.anomalies.length > 0) {
        html += '<h3 style="margin:1rem 0 0.5rem;font-size:0.95rem;">Anomalies (' + report.anomaly_count + ')</h3>';
        html += '<table><thead><tr><th>Type</th><th>Column</th><th>Description</th><th>Confidence</th></tr></thead><tbody>';
        for (const a of report.anomalies) {
            const badge = a.anomaly_type === 'OutlierValue' ? 'outlier' :
                          a.anomaly_type === 'MissingValueCluster' ? 'missing' : 'drift';
            html += `<tr>
                <td><span class="badge ${badge}">${a.anomaly_type}</span></td>
                <td>${a.column}</td>
                <td>${a.description}</td>
                <td>${(a.confidence * 100).toFixed(0)}%</td>
            </tr>`;
        }
        html += '</tbody></table>';
    }

    if (report.drift_events.length > 0) {
        html += '<h3 style="margin:1rem 0 0.5rem;font-size:0.95rem;">Drift Events (' + report.drift_count + ')</h3>';
        html += '<table><thead><tr><th>Column</th><th>Start Row</th><th>Magnitude</th><th>Description</th></tr></thead><tbody>';
        for (const d of report.drift_events) {
            html += `<tr><td>${d.column}</td><td>${d.drift_start_row}</td><td>${d.magnitude.toFixed(3)}</td><td>${d.description}</td></tr>`;
        }
        html += '</tbody></table>';
    }

    document.getElementById('quality-content').innerHTML = html;
}

function showCrystals() {
    const crystals = JSON.parse(engine.crystals());
    if (crystals.length === 0) {
        document.getElementById('crystals-content').innerHTML =
            '<div class="empty-state">No crystals discovered yet. Run more ticks.</div>';
        return;
    }

    let html = '';
    for (const c of crystals) {
        html += `<div class="crystal-card">
            <div class="id">${c.crystal_id}</div>
            <div class="meta">
                <div>Stability: <span>${c.stability_score.toFixed(4)}</span></div>
                <div>Region: <span>${c.region_size}</span></div>
                <div>Constraints: <span>${c.constraint_count}</span></div>
                <div>Tick: <span>${c.created_at}</span></div>
                <div>MCI: <span>${c.consensus.mci.toFixed(3)}</span></div>
            </div>
        </div>`;
    }
    document.getElementById('crystals-content').innerHTML = html;
}

function showAccumulationCurve() {
    const data = JSON.parse(engine.accumulation_curve());
    if (data.length < 2) {
        document.getElementById('curve-content').innerHTML =
            '<div class="empty-state">Need more ticks for accumulation curve</div>';
        return;
    }

    const w = 600, h = 200, pad = 40;
    const maxTick = Math.max(...data.map(d => d.tick));
    const maxCrystals = Math.max(...data.map(d => d.total_crystals), 1);
    const maxHits = Math.max(...data.map(d => d.memory_hits), 1);

    const sx = (t) => pad + (t / maxTick) * (w - 2 * pad);
    const sy = (v, max) => h - pad - (v / max) * (h - 2 * pad);

    let crystalPath = data.map((d, i) =>
        `${i === 0 ? 'M' : 'L'}${sx(d.tick).toFixed(1)},${sy(d.total_crystals, maxCrystals).toFixed(1)}`
    ).join(' ');

    let hitsPath = data.map((d, i) =>
        `${i === 0 ? 'M' : 'L'}${sx(d.tick).toFixed(1)},${sy(d.memory_hits, maxHits).toFixed(1)}`
    ).join(' ');

    const svg = `<svg viewBox="0 0 ${w} ${h}" style="width:100%;max-width:${w}px;">
        <line x1="${pad}" y1="${h - pad}" x2="${w - pad}" y2="${h - pad}" stroke="#30363d" />
        <line x1="${pad}" y1="${pad}" x2="${pad}" y2="${h - pad}" stroke="#30363d" />
        <text x="${w / 2}" y="${h - 5}" fill="#8b949e" font-size="10" text-anchor="middle">Ticks</text>
        <text x="10" y="${h / 2}" fill="#8b949e" font-size="10" text-anchor="middle" transform="rotate(-90,10,${h / 2})">Count</text>
        <path d="${crystalPath}" fill="none" stroke="#58a6ff" stroke-width="2" />
        <path d="${hitsPath}" fill="none" stroke="#3fb950" stroke-width="2" stroke-dasharray="4,2" />
        <rect x="${w - pad - 120}" y="5" width="115" height="35" fill="#161b22" rx="3" />
        <line x1="${w - pad - 115}" y1="15" x2="${w - pad - 95}" y2="15" stroke="#58a6ff" stroke-width="2" />
        <text x="${w - pad - 90}" y="18" fill="#c9d1d9" font-size="9">Crystals</text>
        <line x1="${w - pad - 115}" y1="30" x2="${w - pad - 95}" y2="30" stroke="#3fb950" stroke-width="2" stroke-dasharray="4,2" />
        <text x="${w - pad - 90}" y="33" fill="#c9d1d9" font-size="9">Memory hits</text>
    </svg>`;
    document.getElementById('curve-content').innerHTML = '<div class="chart-container">' + svg + '</div>';
}

// ─── Tab Switching ──────────────────────────────────────────────────────────

function switchTab(name) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelector(`[data-tab="${name}"]`).classList.add('active');
    document.getElementById(name + '-content').classList.add('active');
}

// ─── Tick Slider ────────────────────────────────────────────────────────────

function updateTickLabel() {
    const val = document.getElementById('tick-slider').value;
    document.getElementById('tick-value').textContent = val;
}

// ─── Init ───────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    setupUpload();
    initialize().catch(e => {
        document.getElementById('loading').textContent = 'Failed to load WASM: ' + e;
    });

    // Tab click handlers
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => switchTab(tab.dataset.tab));
    });

    // Button handlers
    document.getElementById('btn-run').addEventListener('click', runEngine);
    document.getElementById('btn-reset').addEventListener('click', resetObservations);
    document.getElementById('btn-reset-all').addEventListener('click', resetAll);
    document.getElementById('btn-sample').addEventListener('click', loadSampleData);
    document.getElementById('tick-slider').addEventListener('input', updateTickLabel);
});
