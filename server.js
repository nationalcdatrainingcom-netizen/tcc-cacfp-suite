const express = require('express');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { promisify } = require('util');
const gzipAsync = promisify(zlib.gzip);
const gunzipAsync = promisify(zlib.gunzip);
const { Pool } = require('pg');
const { Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
        AlignmentType, BorderStyle, WidthType, ShadingType, HeadingLevel,
        ImageRun, PageBreak } = require('docx');

const app = express();
const PORT = process.env.PORT || 3000;
const ACCESS_PIN = process.env.ACCESS_PIN || 'tcc2026';

// ── DATABASE ──────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
  // Tuning for stability under large payload inserts (archive saves can be 20+ MB)
  max: 10,                         // modest pool size — Standard DB allows ~97 connections
  idleTimeoutMillis: 30000,        // close idle connections after 30s (default 10s is too aggressive)
  connectionTimeoutMillis: 10000,  // wait up to 10s for a connection before giving up
  statement_timeout: 60000,        // kill queries that run longer than 60s (prevents stuck archive inserts)
  query_timeout: 60000,
  keepAlive: true,                 // TCP keepalives prevent idle connection drops by Render network
  keepAliveInitialDelayMillis: 10000
});

// Swallow pool-level errors so an idle-connection drop doesn't crash the process
pool.on('error', (err) => {
  console.error('⚠️ Unexpected Postgres pool error (non-fatal):', err.message);
});

app.use(express.json({ limit: '200mb' }));
app.use(express.urlencoded({ limit: '200mb', extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// File uploads — store in memory for DB storage
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 200 * 1024 * 1024 } });

// ── SCHEMA INIT ───────────────────────────────────────────
async function initDB() {
  await pool.query(`
    -- Fiscal years
    CREATE TABLE IF NOT EXISTS fiscal_years (
      id SERIAL PRIMARY KEY,
      label VARCHAR(20) NOT NULL UNIQUE,
      start_month INTEGER NOT NULL DEFAULT 10,
      start_year INTEGER NOT NULL,
      end_month INTEGER NOT NULL DEFAULT 9,
      end_year INTEGER NOT NULL,
      is_active BOOLEAN DEFAULT false,
      created_at TIMESTAMP DEFAULT NOW()
    );

    -- Staff roster (persists across months, carries forward to new years)
    CREATE TABLE IF NOT EXISTS staff (
      id SERIAL PRIMARY KEY,
      name VARCHAR(150) NOT NULL,
      center VARCHAR(50) NOT NULL,
      hourly_rate NUMERIC(8,2) NOT NULL DEFAULT 0,
      is_active BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );

    -- Monthly staff time entries (from Time Distribution forms)
    CREATE TABLE IF NOT EXISTS staff_time_entries (
      id SERIAL PRIMARY KEY,
      staff_id INTEGER REFERENCES staff(id) ON DELETE CASCADE,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      month_key VARCHAR(10) NOT NULL,
      food_service_hours NUMERIC(8,2) DEFAULT 0,
      admin_hours NUMERIC(8,2) DEFAULT 0,
      hourly_rate_used NUMERIC(8,2) DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(staff_id, fiscal_year_id, month_key)
    );

    -- Uploaded documents (PDFs stored as bytea for audit)
    CREATE TABLE IF NOT EXISTS documents (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      month_key VARCHAR(10),
      doc_type VARCHAR(80) NOT NULL,
      filename VARCHAR(255) NOT NULL,
      mime_type VARCHAR(100),
      file_data BYTEA,
      staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
      metadata JSONB DEFAULT '{}',
      uploaded_at TIMESTAMP DEFAULT NOW()
    );

    -- Monthly financial data (claims, food costs, attendance, etc.)
    CREATE TABLE IF NOT EXISTS monthly_data (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      month_key VARCHAR(10) NOT NULL,
      data_type VARCHAR(80) NOT NULL,
      data JSONB DEFAULT '{}',
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(fiscal_year_id, month_key, data_type)
    );

    -- NFSA Revenue tracking
    CREATE TABLE IF NOT EXISTS revenue_entries (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      month_key VARCHAR(10) NOT NULL,
      revenue_type VARCHAR(80) NOT NULL,
      description TEXT,
      amount NUMERIC(12,2) DEFAULT 0,
      source VARCHAR(120),
      created_at TIMESTAMP DEFAULT NOW()
    );

    -- Program documents (application, approval, etc.)
    CREATE TABLE IF NOT EXISTS program_documents (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      doc_type VARCHAR(80) NOT NULL,
      label VARCHAR(255),
      filename VARCHAR(255) NOT NULL,
      mime_type VARCHAR(100),
      file_data BYTEA,
      uploaded_at TIMESTAMP DEFAULT NOW()
    );

    -- Year-end report data
    CREATE TABLE IF NOT EXISTS yer_data (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id) UNIQUE,
      food_cost NUMERIC(12,2) DEFAULT 0,
      cacfp_reimbursement NUMERIC(12,2) DEFAULT 0,
      total_salaries NUMERIC(12,2) DEFAULT 0,
      total_benefits NUMERIC(12,2) DEFAULT 0,
      total_admin NUMERIC(12,2) DEFAULT 0,
      total_revenue NUMERIC(12,2) DEFAULT 0,
      fund_balance NUMERIC(12,2) DEFAULT 0,
      notes TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    );

    -- Child attendance times (daily sign-in/sign-out from Playground + CDC)
    CREATE TABLE IF NOT EXISTS child_attendance_times (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      month_key VARCHAR(10) NOT NULL,
      center VARCHAR(50) NOT NULL,
      child_last VARCHAR(120) NOT NULL,
      child_first VARCHAR(120) NOT NULL,
      attend_date DATE NOT NULL,
      check_in VARCHAR(20),
      check_out VARCHAR(20),
      status VARCHAR(10) DEFAULT 'present',
      hours_decimal NUMERIC(5,2) DEFAULT 0,
      source VARCHAR(30) DEFAULT 'playground',
      signer_in VARCHAR(120),
      signer_out VARCHAR(120),
      imported_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(fiscal_year_id, month_key, center, child_last, child_first, attend_date, check_in, source)
    );

    -- Cross-check resolutions (monitor decisions on flagged meal-window discrepancies)
    CREATE TABLE IF NOT EXISTS crosscheck_resolutions (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      month_key VARCHAR(10) NOT NULL,
      center VARCHAR(50) NOT NULL,
      flag_key VARCHAR(400) NOT NULL,
      child_name VARCHAR(200),
      flag_date DATE,
      meal_type VARCHAR(20),
      check_in VARCHAR(20),
      window_end VARCHAR(20),
      status VARCHAR(30) DEFAULT 'pending',
      resolution_notes TEXT,
      attached_review_id INTEGER,
      resolved_by VARCHAR(150),
      resolved_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(fiscal_year_id, month_key, center, flag_key)
    );

    -- Seed first fiscal year if none exists
    INSERT INTO fiscal_years (label, start_year, end_year, is_active)
    VALUES ('2025-2026', 2025, 2026, true)
    ON CONFLICT (label) DO NOTHING;
  `);
  try { await pool.query('ALTER TABLE daily_cacfp_entries ADD COLUMN IF NOT EXISTS adult_meal BOOLEAN DEFAULT false'); } catch(e) {}
  try { await pool.query('CREATE INDEX IF NOT EXISTS idx_cat_center_month ON child_attendance_times(fiscal_year_id, month_key, center)'); } catch(e) {}
  try { await pool.query('CREATE INDEX IF NOT EXISTS idx_ccr_status ON crosscheck_resolutions(fiscal_year_id, month_key, center, status)'); } catch(e) {}
  console.log('✅ Database tables ready');
}

// ── AUTH MIDDLEWARE ────────────────────────────────────────
function authCheck(req, res, next) {
  const pin = req.headers['x-access-pin'] || req.query.pin;
  if (pin === ACCESS_PIN) return next();
  res.status(401).json({ error: 'Invalid PIN' });
}

// ── PUBLIC: PIN check ─────────────────────────────────────
app.post('/api/auth', (req, res) => {
  if (req.body.pin === ACCESS_PIN) return res.json({ ok: true });
  res.status(401).json({ error: 'Invalid PIN' });
});

// ── FISCAL YEARS ──────────────────────────────────────────
app.get('/api/fiscal-years', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM fiscal_years ORDER BY start_year DESC');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/fiscal-years', authCheck, async (req, res) => {
  try {
    const { label, start_year, end_year } = req.body;
    await pool.query('UPDATE fiscal_years SET is_active = false');
    const { rows } = await pool.query(
      `INSERT INTO fiscal_years (label, start_year, end_year, is_active)
       VALUES ($1, $2, $3, true)
       ON CONFLICT (label) DO UPDATE SET is_active = true
       RETURNING *`,
      [label, start_year, end_year]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/fiscal-years/:id/activate', authCheck, async (req, res) => {
  try {
    await pool.query('UPDATE fiscal_years SET is_active = false');
    const { rows } = await pool.query(
      'UPDATE fiscal_years SET is_active = true WHERE id = $1 RETURNING *',
      [req.params.id]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── STAFF ROSTER ──────────────────────────────────────────
app.get('/api/staff', authCheck, async (req, res) => {
  try {
    const center = req.query.center;
    let q = 'SELECT * FROM staff WHERE is_active = true';
    const params = [];
    if (center) { q += ' AND center = $1'; params.push(center); }
    q += ' ORDER BY name';
    const { rows } = await pool.query(q, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/staff', authCheck, async (req, res) => {
  try {
    const { name, center, hourly_rate } = req.body;
    const { rows } = await pool.query(
      'INSERT INTO staff (name, center, hourly_rate) VALUES ($1, $2, $3) RETURNING *',
      [name, center, hourly_rate || 0]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/staff/:id', authCheck, async (req, res) => {
  try {
    const { name, hourly_rate, is_active } = req.body;
    const { rows } = await pool.query(
      `UPDATE staff SET
        name = COALESCE($1, name),
        hourly_rate = COALESCE($2, hourly_rate),
        is_active = COALESCE($3, is_active),
        updated_at = NOW()
       WHERE id = $4 RETURNING *`,
      [name, hourly_rate, is_active, req.params.id]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── STAFF TIME ENTRIES ────────────────────────────────────
app.get('/api/staff-time', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.query;
    let q = `SELECT ste.*, s.name, s.center, s.hourly_rate as current_rate
             FROM staff_time_entries ste
             JOIN staff s ON s.id = ste.staff_id
             WHERE 1=1`;
    const params = [];
    if (fiscal_year_id) { params.push(fiscal_year_id); q += ` AND ste.fiscal_year_id = $${params.length}`; }
    if (month_key) { params.push(month_key); q += ` AND ste.month_key = $${params.length}`; }
    q += ' ORDER BY s.center, s.name';
    const { rows } = await pool.query(q, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/staff-time', authCheck, async (req, res) => {
  try {
    const { staff_id, fiscal_year_id, month_key, food_service_hours, admin_hours, hourly_rate_used } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO staff_time_entries (staff_id, fiscal_year_id, month_key, food_service_hours, admin_hours, hourly_rate_used)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (staff_id, fiscal_year_id, month_key)
       DO UPDATE SET food_service_hours = $4, admin_hours = $5, hourly_rate_used = $6, updated_at = NOW()
       RETURNING *`,
      [staff_id, fiscal_year_id, month_key, food_service_hours || 0, admin_hours || 0, hourly_rate_used || 0]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/staff-time/bulk', authCheck, async (req, res) => {
  try {
    const { entries, fiscal_year_id, month_key } = req.body;
    const results = [];
    for (const e of entries) {
      const { rows } = await pool.query(
        `INSERT INTO staff_time_entries (staff_id, fiscal_year_id, month_key, food_service_hours, admin_hours, hourly_rate_used)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (staff_id, fiscal_year_id, month_key)
         DO UPDATE SET food_service_hours = $4, admin_hours = $5, hourly_rate_used = $6, updated_at = NOW()
         RETURNING *`,
        [e.staff_id, fiscal_year_id, month_key, e.food_service_hours || 0, e.admin_hours || 0, e.hourly_rate_used || 0]
      );
      results.push(rows[0]);
    }
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/staff-time/totals', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id } = req.query;
    const { rows } = await pool.query(`
      SELECT month_key,
        SUM(food_service_hours * hourly_rate_used) as food_service_cost,
        SUM(admin_hours * hourly_rate_used) as admin_cost,
        SUM(food_service_hours) as total_fs_hours,
        SUM(admin_hours) as total_admin_hours,
        COUNT(DISTINCT staff_id) as staff_count
      FROM staff_time_entries
      WHERE fiscal_year_id = $1
      GROUP BY month_key
      ORDER BY month_key
    `, [fiscal_year_id]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── DOCUMENT UPLOAD/DOWNLOAD ──────────────────────────────
app.post('/api/documents/upload', authCheck, upload.single('file'), async (req, res) => {
  try {
    const { fiscal_year_id, month_key, doc_type, staff_id, metadata } = req.body;
    const file = req.file;
    if (!file) return res.status(400).json({ error: 'No file uploaded' });
    const { rows } = await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, staff_id, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, filename, doc_type, uploaded_at`,
      [fiscal_year_id, month_key || null, doc_type, file.originalname, file.mimetype, file.buffer,
       staff_id || null, metadata ? JSON.parse(metadata) : {}]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/documents', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, doc_type, staff_id } = req.query;
    let q = 'SELECT id, fiscal_year_id, month_key, doc_type, filename, mime_type, staff_id, metadata, uploaded_at FROM documents WHERE 1=1';
    const params = [];
    if (fiscal_year_id) { params.push(fiscal_year_id); q += ` AND fiscal_year_id = $${params.length}`; }
    if (month_key) { params.push(month_key); q += ` AND month_key = $${params.length}`; }
    if (doc_type) { params.push(doc_type); q += ` AND doc_type = $${params.length}`; }
    if (staff_id) { params.push(staff_id); q += ` AND staff_id = $${params.length}`; }
    q += ' ORDER BY uploaded_at DESC';
    const { rows } = await pool.query(q, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/documents/:id/download', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT filename, mime_type, file_data FROM documents WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    res.setHeader('Content-Disposition', `attachment; filename="${rows[0].filename}"`);
    res.setHeader('Content-Type', rows[0].mime_type || 'application/octet-stream');
    res.send(rows[0].file_data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── PDF RASTERIZATION ─────────────────────────────────────
// Rasterize an uploaded PDF to PNG page images for inline embedding in print packages.
// Returns { pages: [base64PNG, ...], pageCount, filename }.
// Non-PDF images pass through as a single-page array.
// Cached in memory per document ID so repeated prints are instant.
const rasterizeCache = new Map(); // id → { pages, pageCount, filename, cachedAt, byteSize }
const RASTERIZE_CACHE_MAX = 10; // number of docs cached
const RASTERIZE_CACHE_MAX_BYTES = 200 * 1024 * 1024; // 200MB total ceiling
let rasterizeCacheBytes = 0;

// Serialization lock: only one PDF is being rasterized at any moment.
// This prevents pdfjs from loading two PDFs simultaneously (each can take ~100MB),
// which would almost certainly OOM the server on a 512MB tier.
let rasterizeInProgress = null;
async function acquireRasterizeLock() {
  while (rasterizeInProgress) {
    await rasterizeInProgress;
  }
  let release;
  rasterizeInProgress = new Promise(r => { release = r; });
  return () => {
    const wasInProgress = rasterizeInProgress;
    rasterizeInProgress = null;
    release();
  };
}

function evictRasterizeOldest() {
  const oldestKey = rasterizeCache.keys().next().value;
  if (oldestKey !== undefined) {
    const entry = rasterizeCache.get(oldestKey);
    rasterizeCacheBytes -= (entry?.byteSize || 0);
    if (rasterizeCacheBytes < 0) rasterizeCacheBytes = 0;
    rasterizeCache.delete(oldestKey);
  }
}

function sumRasterizeSize(pages) {
  // Approximate byte cost: data URLs are base64-encoded, ~1.33x the raw byte size.
  // Sum length of each page string.
  let total = 0;
  for (const p of pages) total += p.length;
  return total;
}

app.get('/api/documents/:id/rasterize', authCheck, async (req, res) => {
  const id = req.params.id;

  // Cache hit? Handle BEFORE acquiring lock (no heavy work)
  if (rasterizeCache.has(id)) {
    const hit = rasterizeCache.get(id);
    rasterizeCache.delete(id);
    rasterizeCache.set(id, hit);
    return res.json({ pages: hit.pages, pageCount: hit.pageCount, filename: hit.filename, cached: true });
  }

  // Serialize rasterize work — only one PDF parsed at a time to avoid OOM
  const releaseLock = await acquireRasterizeLock();
  try {

    const { rows } = await pool.query(
      'SELECT filename, mime_type, file_data FROM documents WHERE id = $1', [id]);
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    const { filename, mime_type, file_data } = rows[0];
    const mt = (mime_type || '').toLowerCase();

    let pages = [];

    if (mt === 'application/pdf' || filename.toLowerCase().endsWith('.pdf')) {
      // Dynamic import — pdf-to-img is ESM-only
      const { pdf } = await import('pdf-to-img');
      try {
        const doc = await pdf(file_data, { scale: 2 }); // 2x = ~144 DPI, good print quality
        for await (const pageBuf of doc) {
          pages.push('data:image/png;base64,' + pageBuf.toString('base64'));
        }
      } catch (pdfErr) {
        console.error(`PDF rasterize failed for doc ${id} (${filename}):`, pdfErr.message);
        return res.status(422).json({
          error: 'Could not rasterize PDF',
          detail: pdfErr.message,
          filename
        });
      }
    } else if (mt.startsWith('image/')) {
      // Images pass through as a single-page data URL
      pages.push(`data:${mt};base64,` + Buffer.from(file_data).toString('base64'));
    } else {
      return res.status(415).json({
        error: 'Unsupported file type for inline embedding',
        mime_type: mt,
        filename
      });
    }

    const byteSize = sumRasterizeSize(pages);
    const result = { pages, pageCount: pages.length, filename, cachedAt: Date.now(), byteSize };

    // Decide whether to cache at all — skip caching of huge docs to preserve headroom
    const LARGE_DOC_THRESHOLD = 15 * 1024 * 1024; // 15MB
    const shouldCache = byteSize < LARGE_DOC_THRESHOLD;

    if (shouldCache) {
      // Evict by count
      while (rasterizeCache.size >= RASTERIZE_CACHE_MAX) evictRasterizeOldest();
      // Evict by bytes
      while (rasterizeCacheBytes + byteSize > RASTERIZE_CACHE_MAX_BYTES && rasterizeCache.size > 0) {
        evictRasterizeOldest();
      }
      rasterizeCache.set(id, result);
      rasterizeCacheBytes += byteSize;
    }

    res.json({ pages: result.pages, pageCount: result.pageCount, filename: result.filename, cached: false });

    // Hint to GC after large rasterization (only effective with --expose-gc flag, but harmless otherwise)
    if (global.gc && byteSize > 5 * 1024 * 1024) {
      setImmediate(() => { try { global.gc(); } catch(e){} });
    }
  } catch (e) {
    console.error('rasterize error:', e);
    res.status(500).json({ error: e.message });
  } finally {
    releaseLock();
  }
});

// Clear the rasterize cache (call when a document is re-uploaded/replaced)
function invalidateRasterizeCache(docId) {
  const k = String(docId);
  if (k && rasterizeCache.has(k)) {
    const e = rasterizeCache.get(k);
    rasterizeCacheBytes -= (e?.byteSize || 0);
    if (rasterizeCacheBytes < 0) rasterizeCacheBytes = 0;
    rasterizeCache.delete(k);
  }
}

app.delete('/api/documents/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM documents WHERE id = $1', [req.params.id]);
    invalidateRasterizeCache(req.params.id);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── MONTHLY PACKAGE ARCHIVE ───────────────────────────────
// ARCHITECTURE NOTE (April 2026 rewrite):
// Archives used to be stored as `bytea` blobs in the `documents` table, but 30-40MB INSERTs
// were crashing the Postgres connection ("terminated unexpectedly", ECONNREFUSED) on Render's
// shared DB. The fix: keep only metadata in Postgres, store the actual compressed HTML file on
// the persistent disk mounted at ARCHIVES_DIR. Tiny rows, fast saves, no connection drops.
//
// DISK LAYOUT: /opt/render/project/src/archives/{fiscal_year_id}/{month_key}/{filename}.gz
//   - Each file is gzip-compressed HTML (same as what the browser sends)
//   - Decompressed on the fly by the view endpoint

const ARCHIVES_DIR = process.env.ARCHIVES_DIR || '/opt/render/project/src/archives';

// Ensure archives dir exists at startup (fail loudly if not — it means the disk isn't mounted)
function ensureArchivesDir() {
  try {
    fs.mkdirSync(ARCHIVES_DIR, { recursive: true });
    // Test write to confirm the disk is actually mounted and writable
    const probe = path.join(ARCHIVES_DIR, '.write-probe');
    fs.writeFileSync(probe, 'ok');
    fs.unlinkSync(probe);
    console.log(`✅ Archives directory ready: ${ARCHIVES_DIR}`);
  } catch (e) {
    console.error(`❌ CRITICAL: Archives directory not writable at ${ARCHIVES_DIR}`);
    console.error('   Most likely the persistent disk is not attached to this service.');
    console.error('   Attach a disk in Render: Service → Disk → Add Disk');
    console.error('     Name: archives, Mount Path: /opt/render/project/src/archives, Size: 5GB');
    console.error('   Error:', e.message);
  }
}
ensureArchivesDir();

// Build the on-disk path for an archive based on its metadata
function archivePathFor(fyId, monthKey, fileName) {
  // Sanitize filename hard — no path traversal, no funky chars
  const safe = String(fileName).replace(/[^a-zA-Z0-9._-]/g, '_');
  const dir = path.join(ARCHIVES_DIR, String(fyId), String(monthKey));
  return { dir, full: path.join(dir, safe + '.gz') };
}

// One-time migration: move any existing archive blobs from Postgres `bytea` to disk.
// Called at startup. Idempotent — skips rows that have already been migrated.
async function migrateArchivesToDisk() {
  try {
    // Find legacy archive rows that still have file_data bytes
    const { rows } = await pool.query(
      `SELECT id, fiscal_year_id, month_key, filename, mime_type, octet_length(file_data) as sz
       FROM documents
       WHERE doc_type = 'archived_monthly_package'
         AND file_data IS NOT NULL
         AND octet_length(file_data) > 0`
    );
    if (!rows.length) {
      console.log('archive migration: nothing to migrate (or already migrated)');
      return;
    }
    console.log(`archive migration: ${rows.length} legacy archive(s) to move to disk...`);
    for (const row of rows) {
      try {
        // Pull the blob separately so the listing query stays light
        const { rows: blobRows } = await pool.query(
          `SELECT file_data, mime_type FROM documents WHERE id = $1`,
          [row.id]
        );
        if (!blobRows[0] || !blobRows[0].file_data) continue;
        let bytes = blobRows[0].file_data;
        // If it was stored uncompressed, gzip it now for consistency on disk
        if (blobRows[0].mime_type !== 'text/html+gzip') {
          bytes = await gzipAsync(bytes, { level: 6 });
        }
        const { dir, full } = archivePathFor(row.fiscal_year_id, row.month_key, row.filename);
        fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(full, bytes);
        // Clear the bytea to free the DB space, but KEEP the row so Archives page still lists it.
        // Update mime_type to 'text/html+gzip' since the on-disk copy is always gzipped now.
        await pool.query(
          `UPDATE documents SET file_data = NULL, mime_type = 'text/html+gzip' WHERE id = $1`,
          [row.id]
        );
        console.log(`archive migration: moved id=${row.id} (${(row.sz/1024/1024).toFixed(1)}MB) → ${full}`);
      } catch (rowErr) {
        console.error(`archive migration: failed id=${row.id}:`, rowErr.message);
      }
    }
    console.log('archive migration: done');
  } catch (e) {
    console.error('archive migration: skipped due to error:', e.message);
  }
}
// Run migration after a short delay to let DB pool stabilize after startup
setTimeout(migrateArchivesToDisk, 5000);

// Save a rendered HTML version of the Complete CACFP Documentation for a month.
// Receives the ALREADY-gzipped bytes as the raw body (application/octet-stream).
// Writes them to the persistent disk and stores only metadata in Postgres.
app.post(
  '/api/archive-package',
  authCheck,
  express.raw({ type: '*/*', limit: '200mb' }),
  async (req, res) => {
    const startMs = Date.now();
    try {
      const { fiscal_year_id, month_key, filename, raw_bytes, encoding } = req.query;
      if (!fiscal_year_id || !month_key) {
        return res.status(400).json({ error: 'Missing fiscal_year_id or month_key query params' });
      }
      if (!Buffer.isBuffer(req.body) || req.body.length === 0) {
        return res.status(400).json({ error: 'Missing or empty request body' });
      }

      // If client sent uncompressed, compress it now — on disk we always store gzipped
      let bytesToStore = req.body;
      if (encoding !== 'gzip') {
        bytesToStore = await gzipAsync(bytesToStore, { level: 6 });
      }

      const originalSize = raw_bytes ? parseInt(raw_bytes, 10) : req.body.length;
      const safeName = filename || `CACFP_Package_${month_key}_${Date.now()}.html`;

      // Write to disk FIRST (the expensive part). If disk write fails, don't pollute DB with orphan rows.
      const { dir, full } = archivePathFor(fiscal_year_id, month_key, safeName);
      try {
        fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(full, bytesToStore);
      } catch (diskErr) {
        console.error('archive-package: disk write failed:', diskErr);
        return res.status(500).json({
          error: 'Disk write failed: ' + diskErr.message +
                 '. Is the persistent disk attached at ' + ARCHIVES_DIR + '?'
        });
      }
      const diskMs = Date.now() - startMs;

      const meta = {
        generated_at: new Date().toISOString(),
        byte_size: originalSize,
        stored_bytes: bytesToStore.length,
        compressed: true,
        storage: 'disk',
        disk_path: full
      };

      // Now insert a tiny metadata-only row (no file_data). This is ~1KB and never chokes Postgres.
      // UPSERT: if an archive with the same (fiscal_year, month, filename) already exists,
      // update it instead of creating a duplicate row. This matches natural user expectation —
      // re-saving the same month/part REPLACES the previous copy rather than piling up duplicates.
      try {
        // Check for existing archive with the same filename (same fy + month + part)
        const existing = await pool.query(
          `SELECT id FROM documents
           WHERE doc_type = 'archived_monthly_package'
             AND fiscal_year_id = $1 AND month_key = $2 AND filename = $3
           LIMIT 1`,
          [fiscal_year_id, month_key, safeName]
        );

        let rows;
        if (existing.rows.length > 0) {
          // Replace the existing archive (disk file was already overwritten above)
          const r = await pool.query(
            `UPDATE documents
             SET mime_type = 'text/html+gzip', file_data = NULL, metadata = $2, uploaded_at = NOW()
             WHERE id = $1
             RETURNING id, filename, doc_type, month_key, uploaded_at, metadata`,
            [existing.rows[0].id, JSON.stringify(meta)]
          );
          rows = r.rows;
          console.log(`archive-package: REPLACED id=${rows[0].id} (same fy/month/filename)`);
        } else {
          const r = await pool.query(
            `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
             VALUES ($1, $2, 'archived_monthly_package', $3, 'text/html+gzip', NULL, $4)
             RETURNING id, filename, doc_type, month_key, uploaded_at, metadata`,
            [fiscal_year_id, month_key, safeName, JSON.stringify(meta)]
          );
          rows = r.rows;
        }
        const totalMs = Date.now() - startMs;
        console.log(
          `archive-package: saved id=${rows[0].id} ` +
          `(${(bytesToStore.length/1024/1024).toFixed(1)}MB gzipped, ` +
          `${(originalSize/1024/1024).toFixed(1)}MB original) ` +
          `disk=${diskMs}ms total=${totalMs}ms`
        );
        return res.json(rows[0]);
      } catch (dbErr) {
        // DB insert failed after disk write succeeded. Try to clean up the orphan file.
        try { fs.unlinkSync(full); } catch(_){}
        console.error('archive-package: metadata insert failed:', dbErr);
        return res.status(500).json({ error: 'Metadata insert failed: ' + dbErr.message });
      }
    } catch (e) {
      console.error('archive-package error:', e);
      res.status(500).json({ error: e.message });
    }
  }
);

// List archived packages — metadata only, never loads blobs
app.get('/api/archived-packages', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.query;
    let q = `SELECT id, fiscal_year_id, month_key, filename, metadata, uploaded_at
             FROM documents WHERE doc_type = 'archived_monthly_package'`;
    const params = [];
    if (fiscal_year_id) { params.push(fiscal_year_id); q += ` AND fiscal_year_id = $${params.length}`; }
    if (month_key) { params.push(month_key); q += ` AND month_key = $${params.length}`; }
    q += ' ORDER BY uploaded_at DESC';
    const { rows } = await pool.query(q, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// View an archived package in-browser. Reads the gzip file from disk, decompresses, streams.
// Back-compat: if (by some chance) an old row still has file_data bytes, falls back to those.
app.get('/api/archived-packages/:id/view', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT filename, mime_type, file_data, metadata, fiscal_year_id, month_key
       FROM documents
       WHERE id = $1 AND doc_type = 'archived_monthly_package'`,
      [req.params.id]
    );
    if (!rows[0]) return res.status(404).send('Archive not found');
    const row = rows[0];

    let gzipBytes;
    // Path 1: on-disk (the new way)
    const { full } = archivePathFor(row.fiscal_year_id, row.month_key, row.filename);
    if (fs.existsSync(full)) {
      gzipBytes = fs.readFileSync(full);
    }
    // Path 2: metadata has an explicit disk_path (for custom locations)
    else if (row.metadata && row.metadata.disk_path && fs.existsSync(row.metadata.disk_path)) {
      gzipBytes = fs.readFileSync(row.metadata.disk_path);
    }
    // Path 3: legacy — bytes still in Postgres (migration hasn't run or failed for this row)
    else if (row.file_data) {
      gzipBytes = row.file_data;
    }
    else {
      return res.status(404).send(
        'Archive file missing on disk. ' +
        'This can happen if the persistent disk was detached or the file was deleted.'
      );
    }

    // If it was stored uncompressed (legacy only), send as-is
    let body;
    if (row.mime_type === 'text/html+gzip') {
      try { body = await gunzipAsync(gzipBytes); }
      catch (gzErr) {
        console.error('archive view: gunzip failed:', gzErr);
        return res.status(500).send('Error decompressing archive: ' + gzErr.message);
      }
    } else {
      body = gzipBytes;
    }
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Content-Disposition', `inline; filename="${row.filename}"`);
    res.send(body);
  } catch (e) {
    console.error('archive view error:', e);
    res.status(500).send('Error: ' + e.message);
  }
});

// Delete archive — removes both the disk file and the Postgres metadata row.
// (The generic /api/documents/:id DELETE endpoint also works but doesn't clean up the file,
//  so archives have their own delete endpoint. Falls through to that for legacy disk-less rows.)
app.delete('/api/archived-packages/:id', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT filename, fiscal_year_id, month_key, metadata FROM documents
       WHERE id = $1 AND doc_type = 'archived_monthly_package'`,
      [req.params.id]
    );
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    const row = rows[0];
    // Remove on-disk file (best-effort)
    const { full } = archivePathFor(row.fiscal_year_id, row.month_key, row.filename);
    try { if (fs.existsSync(full)) fs.unlinkSync(full); } catch (e) {
      console.warn(`archive delete: could not unlink ${full}: ${e.message}`);
    }
    if (row.metadata && row.metadata.disk_path && row.metadata.disk_path !== full) {
      try { if (fs.existsSync(row.metadata.disk_path)) fs.unlinkSync(row.metadata.disk_path); } catch(_){}
    }
    await pool.query(`DELETE FROM documents WHERE id = $1`, [req.params.id]);
    res.json({ success: true });
  } catch (e) {
    console.error('archive delete error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── MONTHLY DATA ──────────────────────────────────────────
app.get('/api/monthly-data', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, data_type } = req.query;
    let q = 'SELECT * FROM monthly_data WHERE 1=1';
    const params = [];
    if (fiscal_year_id) { params.push(fiscal_year_id); q += ` AND fiscal_year_id = $${params.length}`; }
    if (month_key) { params.push(month_key); q += ` AND month_key = $${params.length}`; }
    if (data_type) { params.push(data_type); q += ` AND data_type = $${params.length}`; }
    const { rows } = await pool.query(q, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/monthly-data', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, data_type, data } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO monthly_data (fiscal_year_id, month_key, data_type, data)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (fiscal_year_id, month_key, data_type)
       DO UPDATE SET data = $4, updated_at = NOW()
       RETURNING *`,
      [fiscal_year_id, month_key, data_type, JSON.stringify(data)]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── REVENUE ENTRIES ───────────────────────────────────────
app.get('/api/revenue', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.query;
    let q = 'SELECT * FROM revenue_entries WHERE 1=1';
    const params = [];
    if (fiscal_year_id) { params.push(fiscal_year_id); q += ` AND fiscal_year_id = $${params.length}`; }
    if (month_key) { params.push(month_key); q += ` AND month_key = $${params.length}`; }
    q += ' ORDER BY month_key, revenue_type';
    const { rows } = await pool.query(q, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/revenue', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, revenue_type, description, amount, source } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO revenue_entries (fiscal_year_id, month_key, revenue_type, description, amount, source)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
      [fiscal_year_id, month_key, revenue_type, description, amount, source]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/revenue/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM revenue_entries WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/revenue/summary', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id } = req.query;
    const { rows } = await pool.query(`
      SELECT month_key, revenue_type, SUM(amount) as total
      FROM revenue_entries WHERE fiscal_year_id = $1
      GROUP BY month_key, revenue_type ORDER BY month_key
    `, [fiscal_year_id]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── PROGRAM DOCUMENTS ─────────────────────────────────────
app.post('/api/program-documents/upload', authCheck, upload.single('file'), async (req, res) => {
  try {
    const { fiscal_year_id, doc_type, label } = req.body;
    const file = req.file;
    if (!file) return res.status(400).json({ error: 'No file' });
    const { rows } = await pool.query(
      `INSERT INTO program_documents (fiscal_year_id, doc_type, label, filename, mime_type, file_data)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, doc_type, label, filename, uploaded_at`,
      [fiscal_year_id, doc_type, label || file.originalname, file.originalname, file.mimetype, file.buffer]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/program-documents', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, doc_type } = req.query;
    let q = 'SELECT id, fiscal_year_id, doc_type, label, filename, mime_type, uploaded_at FROM program_documents WHERE 1=1';
    const params = [];
    if (fiscal_year_id) { params.push(fiscal_year_id); q += ` AND fiscal_year_id = $${params.length}`; }
    if (doc_type) { params.push(doc_type); q += ` AND doc_type = $${params.length}`; }
    q += ' ORDER BY uploaded_at DESC';
    const { rows } = await pool.query(q, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/program-documents/:id/download', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT filename, mime_type, file_data FROM program_documents WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    res.setHeader('Content-Disposition', `attachment; filename="${rows[0].filename}"`);
    res.setHeader('Content-Type', rows[0].mime_type || 'application/octet-stream');
    res.send(rows[0].file_data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/program-documents/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM program_documents WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── YER DATA ──────────────────────────────────────────────
app.get('/api/yer', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id } = req.query;
    const { rows } = await pool.query(
      'SELECT * FROM yer_data WHERE fiscal_year_id = $1', [fiscal_year_id]
    );
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/yer', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, food_cost, cacfp_reimbursement, notes } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO yer_data (fiscal_year_id, food_cost, cacfp_reimbursement, notes)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (fiscal_year_id)
       DO UPDATE SET food_cost = COALESCE($2, yer_data.food_cost),
         cacfp_reimbursement = COALESCE($3, yer_data.cacfp_reimbursement),
         notes = COALESCE($4, yer_data.notes),
         updated_at = NOW()
       RETURNING *`,
      [fiscal_year_id, food_cost, cacfp_reimbursement, notes]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── PLAYGROUND DAILY STAFF HOURS IMPORT ──────────────────
app.post('/api/playground-import', authCheck, upload.single('file'), async (req, res) => {
  try {
    const { fiscal_year_id, month_key, center } = req.body;
    const file = req.file;
    if (!file) return res.status(400).json({ error: 'No file' });

    await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,$2,'playground_staff_hours',$3,$4,$5,$6)`,
      [fiscal_year_id, month_key, file.originalname, file.mimetype, file.buffer,
       JSON.stringify({ center: center || 'unknown' })]
    );

    const csv = file.buffer.toString('utf8').replace(/^\uFEFF/, '');
    const lines = csv.split('\n').map(l => l.trim()).filter(Boolean);
    if (lines.length < 2) return res.status(400).json({ error: 'Empty CSV' });

    const hdr = lines[0].split(',').map(h => h.replace(/"/g, '').trim().toLowerCase());
    const idxLast = hdr.indexOf('last name');
    const idxFirst = hdr.indexOf('first name');
    const idxDate = hdr.indexOf('date');
    const idxTimes = hdr.indexOf('times');
    const idxBreaks = hdr.indexOf('breaks');
    const idxBillable = hdr.indexOf('billable');
    if (idxLast < 0 || idxFirst < 0 || idxDate < 0) return res.status(400).json({ error: 'Missing required columns' });

    function parseCSVRows(text) {
      const rows = []; let row = []; let field = ''; let inQ = false;
      for (let i = 0; i < text.length; i++) {
        const c = text[i];
        if (inQ) { if (c === '"' && text[i+1] === '"') { field += '"'; i++; } else if (c === '"') inQ = false; else field += c; }
        else { if (c === '"') inQ = true; else if (c === ',') { row.push(field.trim()); field = ''; }
          else if (c === '\n' || c === '\r') { if (c === '\r' && text[i+1] === '\n') i++; row.push(field.trim()); if (row.length > 1 || row[0]) rows.push(row); row = []; field = ''; }
          else field += c; }
      }
      if (field || row.length) { row.push(field.trim()); rows.push(row); }
      return rows;
    }

    const dataRows = parseCSVRows(csv);
    dataRows.shift();

    const staffRes = await pool.query('SELECT id, name, center FROM staff WHERE is_active = true');
    const staffMap = {};
    for (const s of staffRes.rows) { staffMap[s.name.toLowerCase()] = s; }

    let imported = 0, unmatched = [], added = [];
    const ML_NUM = {jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11};
    const targetMonth = ML_NUM[month_key];

    for (const cols of dataRows) {
      if (cols.length < Math.max(idxLast, idxFirst, idxDate) + 1) continue;
      const lastName = cols[idxLast] || '';
      const firstName = cols[idxFirst] || '';
      const fullName = `${firstName} ${lastName}`.trim();
      const dateStr = cols[idxDate] || '';

      const dm = dateStr.match(/(\d+)\/(\d+)\/(\d+)/);
      if (!dm) continue;
      const rowMonth = parseInt(dm[1]) - 1;
      const rowDay = parseInt(dm[2]);
      if (rowMonth !== targetMonth) continue;

      const key = fullName.toLowerCase();
      let staff = staffMap[key];
      if (!staff) {
        const fnLow = firstName.toLowerCase();
        const lnLow = lastName.toLowerCase();
        const NICKNAMES = {abby:'abigail',abigail:'abby',mike:'michael',michael:'mike',
          liz:'elizabeth',elizabeth:'liz',beth:'elizabeth',bill:'william',william:'bill',
          bob:'robert',robert:'bob',rob:'robert',jim:'james',james:'jim',jimmy:'james',
          joe:'joseph',joseph:'joe',jen:'jennifer',jennifer:'jen',jenny:'jennifer',
          kate:'katherine',katherine:'kate',kathy:'katherine',cathy:'catherine',catherine:'cathy',
          dan:'daniel',daniel:'dan',dave:'david',david:'dave',tom:'thomas',thomas:'tom',
          tony:'anthony',anthony:'tony',chris:'christopher',christopher:'chris',
          matt:'matthew',matthew:'matt',nick:'nicholas',nicholas:'nick',
          sam:'samuel',samuel:'sam',samantha:'sam',steve:'steven',steven:'steve',
          pat:'patricia',patricia:'pat',ed:'edward',edward:'ed',alex:'alexander',
          rick:'richard',richard:'rick',dick:'richard',will:'william',josh:'joshua',joshua:'josh',
          meg:'megan',megan:'meg',maddie:'madison',madison:'maddie',mandy:'amanda',amanda:'mandy'};
        const nickVariants = [fnLow];
        if (NICKNAMES[fnLow]) nickVariants.push(NICKNAMES[fnLow]);
        for (const [nick, full] of Object.entries(NICKNAMES)) {
          if (full === fnLow && !nickVariants.includes(nick)) nickVariants.push(nick);
        }

        for (const s of staffRes.rows) {
          const parts = s.name.toLowerCase().split(' ');
          if (parts.length < 2) continue;
          const sFirst = parts[0];
          const sLast = parts[parts.length - 1];
          if (sLast !== lnLow) continue;

          for (const variant of nickVariants) {
            if (variant === sFirst) { staff = s; break; }
          }
          if (staff) break;

          if (fnLow.length >= 3 && sFirst.startsWith(fnLow.substring(0, 3))) { staff = s; break; }
          if (sFirst.length >= 3 && fnLow.startsWith(sFirst.substring(0, 3))) { staff = s; break; }
        }
      }
      if (!staff) {
        const staffCenter = center || 'niles';
        try {
          const newStaff = await pool.query(
            'INSERT INTO staff (name, center, hourly_rate) VALUES ($1, $2, 0) RETURNING *',
            [fullName, staffCenter]
          );
          staff = newStaff.rows[0];
          staffMap[key] = staff;
          const defaultPin = String(1000 + Math.floor(Math.random() * 9000));
          await pool.query(
            'INSERT INTO staff_pins (staff_id, pin, role) VALUES ($1, $2, $3) ON CONFLICT (staff_id) DO NOTHING',
            [staff.id, defaultPin, 'staff']
          );
          if (!added.includes(fullName)) added.push(fullName + ' (PIN: ' + defaultPin + ')');
        } catch (addErr) {
          if (!unmatched.includes(fullName)) unmatched.push(fullName);
          continue;
        }
      }

      const timesRaw = cols[idxTimes] || '';
      const timeSegments = timesRaw.split(/\n/).map(t => t.trim()).filter(Boolean);
      let startTime = '', endTime = '';
      for (const seg of timeSegments) {
        const tm = seg.match(/(\d+:\d+[ap]m)\s*-\s*(\d+:\d+[ap]m)/i);
        if (tm) { if (!startTime) startTime = tm[1]; endTime = tm[2]; }
      }

      const breaksRaw = cols[idxBreaks] || '0 hrs 0 min';
      const bm = breaksRaw.match(/(\d+)\s*hrs?\s*(\d+)\s*min/);
      const breakHrs = bm ? parseInt(bm[1]) + parseInt(bm[2]) / 60 : 0;

      const billRaw = cols[idxBillable] || '0 hrs 0 min';
      const blm = billRaw.match(/(\d+)\s*hrs?\s*(\d+)\s*min/);
      const billableHrs = blm ? parseInt(blm[1]) + parseInt(blm[2]) / 60 : 0;

      await pool.query(
        `INSERT INTO playground_staff_hours (staff_id, fiscal_year_id, month_key, day_of_month, start_time, end_time, total_worked, total_absent)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (staff_id, fiscal_year_id, month_key, day_of_month)
         DO UPDATE SET start_time=$5, end_time=$6, total_worked=$7, total_absent=$8, imported_at=NOW()`,
        [staff.id, fiscal_year_id, month_key, rowDay, startTime, endTime,
         Math.round(billableHrs * 100) / 100, Math.round(breakHrs * 100) / 100]
      );
      imported++;
    }

    res.json({ ok: true, imported, unmatched, added, total_rows: dataRows.length });
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// ── GET MERGED DATA (Playground + Phone CACFP entries) ───
app.get('/api/merged-time/:staffId', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.query;
    const sid = req.params.staffId;

    const pgRes = await pool.query(
      'SELECT * FROM playground_staff_hours WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3 ORDER BY day_of_month',
      [sid, fiscal_year_id, month_key]
    );
    const ceRes = await pool.query(
      'SELECT * FROM daily_cacfp_entries WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3 ORDER BY day_of_month',
      [sid, fiscal_year_id, month_key]
    );
    const sigRes = await pool.query(
      'SELECT * FROM monthly_signatures WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3',
      [sid, fiscal_year_id, month_key]
    );
    const sRes = await pool.query('SELECT * FROM staff WHERE id=$1', [sid]);

    const days = {};
    for (const p of pgRes.rows) {
      days[p.day_of_month] = {
        day: p.day_of_month, start_time: p.start_time, end_time: p.end_time,
        total_worked: parseFloat(p.total_worked) || 0, total_absent: parseFloat(p.total_absent) || 0,
        food_service_hours: 0, admin_hours: 0
      };
    }
    for (const c of ceRes.rows) {
      if (!days[c.day_of_month]) days[c.day_of_month] = { day: c.day_of_month, start_time: '', end_time: '', total_worked: 0, total_absent: 0 };
      days[c.day_of_month].food_service_hours = parseFloat(c.food_service_hours) || 0;
      days[c.day_of_month].admin_hours = parseFloat(c.admin_hours) || 0;
    }

    for (const d of Object.values(days)) {
      d.non_cacfp = Math.max(0, d.total_worked - d.food_service_hours - d.admin_hours);
    }

    res.json({
      staff: sRes.rows[0],
      signature: sigRes.rows[0] || null,
      days: Object.values(days).sort((a, b) => a.day - b.day)
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/merged-time-all', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, show_all } = req.query;
    const staffRes = await pool.query(
      `SELECT s.id, s.name, s.center, s.hourly_rate FROM staff s
       JOIN staff_pins sp ON sp.staff_id = s.id WHERE s.is_active = true ORDER BY s.center, s.name`
    );
    const result = [];
    for (const s of staffRes.rows) {
      const pgRes = await pool.query(
        'SELECT * FROM playground_staff_hours WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3',
        [s.id, fiscal_year_id, month_key]
      );
      const ceRes = await pool.query(
        'SELECT * FROM daily_cacfp_entries WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3',
        [s.id, fiscal_year_id, month_key]
      );
      const sigRes = await pool.query(
        'SELECT * FROM monthly_signatures WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3',
        [s.id, fiscal_year_id, month_key]
      );
      let totalFS = 0, totalAdm = 0, totalWorked = 0, totalAbsent = 0, daysWorked = 0;
      const hasPlayground = pgRes.rows.length > 0;
      const hasCACFP = ceRes.rows.length > 0;
      for (const c of ceRes.rows) { totalFS += parseFloat(c.food_service_hours) || 0; totalAdm += parseFloat(c.admin_hours) || 0; }
      for (const p of pgRes.rows) { totalWorked += parseFloat(p.total_worked) || 0; totalAbsent += parseFloat(p.total_absent) || 0; daysWorked++; }

      if (show_all === 'true' || hasPlayground || hasCACFP || totalFS > 0 || totalAdm > 0 || totalWorked > 0) {
        result.push({
          ...s, totalFS, totalAdm, totalWorked, totalAbsent, daysWorked, hasPlayground, hasCACFP,
          signature: sigRes.rows[0] || null
        });
      }
    }
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/approve-month', authCheck, async (req, res) => {
  try {
    const { staff_id, fiscal_year_id, month_key, supervisor_signature } = req.body;
    await pool.query(
      `UPDATE monthly_signatures SET supervisor_signature=$1, supervisor_signed_at=NOW(), status='approved'
       WHERE staff_id=$2 AND fiscal_year_id=$3 AND month_key=$4`,
      [supervisor_signature, staff_id, fiscal_year_id, month_key]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/approve-month-bulk', authCheck, async (req, res) => {
  try {
    const { staff_ids, fiscal_year_id, month_key, supervisor_signature } = req.body;
    let approved = 0;
    for (const sid of staff_ids) {
      const r = await pool.query(
        `UPDATE monthly_signatures SET supervisor_signature=$1, supervisor_signed_at=NOW(), status='approved'
         WHERE staff_id=$2 AND fiscal_year_id=$3 AND month_key=$4 AND status='submitted'`,
        [supervisor_signature, sid, fiscal_year_id, month_key]
      );
      if (r.rowCount > 0) approved++;
    }
    res.json({ ok: true, approved });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GENERATE INDIVIDUAL MDE T&A FORM (.docx) ─────────────
app.post('/api/generate-ta-form', authCheck, async (req, res) => {
  try {
    const { staff_id, fiscal_year_id, month_key, store_in_docs } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
    const fy = fyRes.rows[0]; if (!fy) return res.status(404).json({ error: 'FY not found' });
    const sRes = await pool.query('SELECT * FROM staff WHERE id=$1', [staff_id]);
    const staff = sRes.rows[0]; if (!staff) return res.status(404).json({ error: 'Staff not found' });

    const ML = {oct:'October',nov:'November',dec:'December',jan:'January',feb:'February',mar:'March',apr:'April',may:'May',jun:'June',jul:'July',aug:'August',sep:'September'};
    const fyYear = mk => ['oct','nov','dec'].includes(mk) ? fy.start_year : fy.end_year;
    const year = fyYear(month_key);
    const monthLabel = ML[month_key] + ' ' + year;
    const MN = {jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11};
    const numDays = new Date(year, MN[month_key] + 1, 0).getDate();

    const pgRes = await pool.query('SELECT * FROM playground_staff_hours WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3', [staff_id, fiscal_year_id, month_key]);
    const ceRes = await pool.query('SELECT * FROM daily_cacfp_entries WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3', [staff_id, fiscal_year_id, month_key]);
    const sigRes = await pool.query('SELECT * FROM monthly_signatures WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3', [staff_id, fiscal_year_id, month_key]);
    const sig = sigRes.rows[0] || {};

    const pgMap = {}; pgRes.rows.forEach(r => { pgMap[r.day_of_month] = r; });
    const ceMap = {}; ceRes.rows.forEach(r => { ceMap[r.day_of_month] = r; });

    const fmt = n => n > 0 ? n.toFixed(2) : '';
    const navy = '1B2A4A';
    const thinB = { top:{style:BorderStyle.SINGLE,size:1,color:'999999'}, bottom:{style:BorderStyle.SINGLE,size:1,color:'999999'}, left:{style:BorderStyle.SINGLE,size:1,color:'999999'}, right:{style:BorderStyle.SINGLE,size:1,color:'999999'} };

    function cell(text, opts = {}) {
      return new TableCell({
        width: opts.w ? { size: opts.w, type: WidthType.PERCENTAGE } : undefined,
        borders: thinB,
        shading: opts.bg ? { type: ShadingType.SOLID, color: opts.bg } : undefined,
        children: [new Paragraph({
          alignment: opts.align || AlignmentType.CENTER,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 16, font: 'Calibri', color: opts.color || '333333' })]
        })]
      });
    }

    const hdrRow = new TableRow({ children: [
      cell('Date', { bold: true, bg: navy, color: 'FFFFFF', w: 6 }),
      cell('Starting\nTime', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
      cell('Ending\nTime', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
      cell('Total Hrs\nWorked', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
      cell('Total Hrs\nAbsent', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
      cell('Non-CACFP\nHours', { bold: true, bg: navy, color: 'FFFFFF', w: 14 }),
      cell('CACFP Hrs\n(Food Svc)', { bold: true, bg: navy, color: 'FFFFFF', w: 14 }),
      cell('CACFP Hrs\n(Admin)', { bold: true, bg: navy, color: 'FFFFFF', w: 14 }),
    ]});

    let totWorked = 0, totAbsent = 0, totNonCACFP = 0, totFS = 0, totAdm = 0;
    const dayRows = [];
    for (let d = 1; d <= numDays; d++) {
      const pg = pgMap[d]; const ce = ceMap[d];
      const worked = parseFloat(pg?.total_worked) || 0;
      const absent = parseFloat(pg?.total_absent) || 0;
      const fsH = parseFloat(ce?.food_service_hours) || 0;
      const admH = parseFloat(ce?.admin_hours) || 0;
      const nonCACFP = Math.max(0, worked - fsH - admH);
      totWorked += worked; totAbsent += absent; totNonCACFP += nonCACFP; totFS += fsH; totAdm += admH;
      const bg = (pg || ce) ? undefined : 'F8F8F8';
      dayRows.push(new TableRow({ children: [
        cell(String(d), { bold: true, bg }),
        cell(pg?.start_time || '', { bg }),
        cell(pg?.end_time || '', { bg }),
        cell(fmt(worked), { bg }),
        cell(fmt(absent), { bg }),
        cell(fmt(nonCACFP), { bg }),
        cell(fmt(fsH), { bg: fsH > 0 ? 'E6F1FB' : bg }),
        cell(fmt(admH), { bg: admH > 0 ? 'E6F1FB' : bg }),
      ]}));
    }

    const totRow = new TableRow({ children: [
      cell('', { bg: 'E0E0E0' }),
      cell('', { bg: 'E0E0E0' }), cell('Totals', { bold: true, bg: 'E0E0E0' }),
      cell(totWorked.toFixed(2), { bold: true, bg: 'E0E0E0' }),
      cell(totAbsent.toFixed(2), { bold: true, bg: 'E0E0E0' }),
      cell(totNonCACFP.toFixed(2), { bold: true, bg: 'E0E0E0' }),
      cell(totFS.toFixed(2), { bold: true, bg: 'E0E0E0' }),
      cell(totAdm.toFixed(2), { bold: true, bg: 'E0E0E0' }),
    ]});

    const rate = parseFloat(staff.hourly_rate) || 0;

    const doc = new Document({
      sections: [{
        properties: { page: { margin: { top: 500, bottom: 500, left: 600, right: 600 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [
            new TextRun({ text: 'Michigan Department of Education', size: 18, font: 'Calibri', color: '666666' }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [
            new TextRun({ text: 'Child and Adult Care Food Program', size: 18, font: 'Calibri', color: '666666' }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 }, children: [
            new TextRun({ text: 'Time and Attendance / Time Distribution', bold: true, size: 24, font: 'Calibri', color: navy }) ]}),
          new Paragraph({ spacing: { after: 60 }, children: [
            new TextRun({ text: 'Name: ', bold: true, size: 20, font: 'Calibri' }),
            new TextRun({ text: staff.name, size: 20, font: 'Calibri', underline: {} }),
            new TextRun({ text: '          Month/Year: ', bold: true, size: 20, font: 'Calibri' }),
            new TextRun({ text: monthLabel, size: 20, font: 'Calibri', underline: {} }),
          ]}),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [hdrRow, ...dayRows, totRow] }),
          new Paragraph({ spacing: { before: 200, after: 80 }, children: [
            new TextRun({ text: `Total CACFP Administrative Time: ${totAdm.toFixed(2)} hrs × Hourly Rate $${rate.toFixed(2)} = Administrative Costs $${(totAdm * rate).toFixed(2)}`, size: 18, font: 'Calibri' })
          ]}),
          new Paragraph({ spacing: { after: 80 }, children: [
            new TextRun({ text: `Total CACFP Food Service Labor Time: ${totFS.toFixed(2)} hrs × Hourly Rate $${rate.toFixed(2)} = Food Service Costs $${(totFS * rate).toFixed(2)}`, size: 18, font: 'Calibri' })
          ]}),
          new Paragraph({ spacing: { before: 200, after: 60 }, children: [
            new TextRun({ text: 'Employee Signature: ', bold: true, size: 18, font: 'Calibri' }),
            new TextRun({ text: sig.employee_signature || '________________', italics: !!sig.employee_signature, size: 18, font: 'Calibri', underline: {} }),
            new TextRun({ text: '    Date: ', bold: true, size: 18, font: 'Calibri' }),
            new TextRun({ text: sig.employee_signed_at ? new Date(sig.employee_signed_at).toLocaleDateString() : '________', size: 18, font: 'Calibri', underline: {} }),
          ]}),
          new Paragraph({ spacing: { after: 60 }, children: [
            new TextRun({ text: 'Supervisor Signature (certification): ', bold: true, size: 18, font: 'Calibri' }),
            new TextRun({ text: sig.supervisor_signature || '________________', italics: !!sig.supervisor_signature, size: 18, font: 'Calibri', underline: {} }),
            new TextRun({ text: '    Date: ', bold: true, size: 18, font: 'Calibri' }),
            new TextRun({ text: sig.supervisor_signed_at ? new Date(sig.supervisor_signed_at).toLocaleDateString() : '________', size: 18, font: 'Calibri', underline: {} }),
          ]}),
          new Paragraph({ spacing: { before: 200 }, alignment: AlignmentType.RIGHT, children: [
            new TextRun({ text: 'Rev. 3/08', size: 14, font: 'Calibri', color: '999999' })
          ]}),
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    const filename = `TA_${staff.name.replace(/\s/g,'_')}_${month_key}_${fy.label}.docx`;

    if (store_in_docs) {
      await pool.query(
        `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, staff_id, metadata)
         VALUES ($1,$2,'ta_form',$3,'application/vnd.openxmlformats-officedocument.wordprocessingml.document',$4,$5,$6)`,
        [fiscal_year_id, month_key, filename, buffer, staff_id, JSON.stringify({ generated: true, totFS, totAdm })]
      );
    }

    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.post('/api/generate-ta-forms-all', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, supervisor_signature } = req.body;
    const staffRes = await pool.query(
      `SELECT s.id FROM staff s
       JOIN staff_pins sp ON sp.staff_id = s.id
       JOIN monthly_signatures ms ON ms.staff_id = s.id AND ms.fiscal_year_id = $1 AND ms.month_key = $2
       WHERE s.is_active = true AND ms.status IN ('submitted','approved')`,
      [fiscal_year_id, month_key]
    );

    let generated = 0;
    for (const s of staffRes.rows) {
      await pool.query(
        `UPDATE monthly_signatures SET supervisor_signature=$1, supervisor_signed_at=NOW(), status='approved'
         WHERE staff_id=$2 AND fiscal_year_id=$3 AND month_key=$4`,
        [supervisor_signature, s.id, fiscal_year_id, month_key]
      );
      generated++;
    }

    res.json({ ok: true, generated, message: `${generated} forms approved. Generate individual forms from the review panel.` });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GENERATE COMPLETE STAFF DOCUMENTATION PACKAGE (.docx) ─
app.post('/api/generate-staff-package', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, supervisor_signature } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
    const fy = fyRes.rows[0]; if (!fy) return res.status(404).json({ error: 'FY not found' });

    const ML = {oct:'October',nov:'November',dec:'December',jan:'January',feb:'February',mar:'March',apr:'April',may:'May',jun:'June',jul:'July',aug:'August',sep:'September'};
    const fyYear = mk => ['oct','nov','dec'].includes(mk) ? fy.start_year : fy.end_year;
    const year = fyYear(month_key);
    const monthLabel = ML[month_key] + ' ' + year;
    const MN = {jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11};
    const numDays = new Date(year, MN[month_key] + 1, 0).getDate();
    const navy = '1B2A4A'; const gold = 'C5972C';
    const fmtD = n => '$' + Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const fmtN = n => n > 0 ? n.toFixed(2) : '';

    const thinB = { top:{style:BorderStyle.SINGLE,size:1,color:'999999'}, bottom:{style:BorderStyle.SINGLE,size:1,color:'999999'}, left:{style:BorderStyle.SINGLE,size:1,color:'999999'}, right:{style:BorderStyle.SINGLE,size:1,color:'999999'} };
    function cell(text, opts = {}) {
      return new TableCell({
        width: opts.w ? { size: opts.w, type: WidthType.PERCENTAGE } : undefined,
        borders: thinB,
        shading: opts.bg ? { type: ShadingType.SOLID, color: opts.bg } : undefined,
        children: [new Paragraph({ alignment: opts.align || AlignmentType.CENTER,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 16, font: 'Calibri', color: opts.color || '333333' })] })]
      });
    }

    const staffRes = await pool.query(
      `SELECT DISTINCT s.id, s.name, s.center, s.hourly_rate FROM staff s
       JOIN staff_pins sp ON sp.staff_id = s.id
       LEFT JOIN daily_cacfp_entries d ON d.staff_id = s.id AND d.fiscal_year_id = $1 AND d.month_key = $2
       LEFT JOIN playground_staff_hours p ON p.staff_id = s.id AND p.fiscal_year_id = $1 AND p.month_key = $2
       WHERE s.is_active = true AND (d.id IS NOT NULL OR p.id IS NOT NULL)
       ORDER BY s.center, s.name`,
      [fiscal_year_id, month_key]
    );

    const sections = [];

    const summaryRows = [new TableRow({ children: [
      cell('Staff Name', { bold: true, bg: navy, color: 'FFFFFF', w: 25 }),
      cell('Center', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
      cell('Rate/Hr', { bold: true, bg: navy, color: 'FFFFFF', w: 10, align: AlignmentType.RIGHT }),
      cell('FS Hours', { bold: true, bg: navy, color: 'FFFFFF', w: 10, align: AlignmentType.RIGHT }),
      cell('FS Cost', { bold: true, bg: navy, color: 'FFFFFF', w: 13, align: AlignmentType.RIGHT }),
      cell('Admin Hrs', { bold: true, bg: navy, color: 'FFFFFF', w: 10, align: AlignmentType.RIGHT }),
      cell('Admin Cost', { bold: true, bg: navy, color: 'FFFFFF', w: 13, align: AlignmentType.RIGHT }),
    ]})];

    let grandFS = 0, grandAdm = 0, grandFSHrs = 0, grandAdmHrs = 0;

    for (let si = 0; si < staffRes.rows.length; si++) {
      const s = staffRes.rows[si];
      const ceRes = await pool.query(
        'SELECT COALESCE(SUM(food_service_hours),0) as tfs, COALESCE(SUM(admin_hours),0) as tadm FROM daily_cacfp_entries WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3',
        [s.id, fiscal_year_id, month_key]
      );
      const rate = parseFloat(s.hourly_rate) || 0;
      const fsH = parseFloat(ceRes.rows[0].tfs) || 0;
      const admH = parseFloat(ceRes.rows[0].tadm) || 0;
      grandFS += fsH * rate; grandAdm += admH * rate; grandFSHrs += fsH; grandAdmHrs += admH;
      const bg = si % 2 === 0 ? undefined : 'F5F5F5';
      summaryRows.push(new TableRow({ children: [
        cell(s.name, { bg, align: AlignmentType.LEFT }),
        cell(s.center === 'niles' ? 'Niles' : 'Peace Blvd', { bg }),
        cell(fmtD(rate), { bg, align: AlignmentType.RIGHT }),
        cell(fmtN(fsH), { bg, align: AlignmentType.RIGHT }),
        cell(fsH > 0 ? fmtD(fsH * rate) : '—', { bg, align: AlignmentType.RIGHT }),
        cell(fmtN(admH), { bg, align: AlignmentType.RIGHT }),
        cell(admH > 0 ? fmtD(admH * rate) : '—', { bg, align: AlignmentType.RIGHT }),
      ]}));
    }

    const benefits = grandFS * 0.0765;
    summaryRows.push(new TableRow({ children: [
      cell('TOTALS', { bold: true, bg: 'E0E0E0' }), cell('', { bg: 'E0E0E0' }),
      cell('', { bg: 'E0E0E0' }),
      cell(grandFSHrs.toFixed(2), { bold: true, bg: 'E0E0E0', align: AlignmentType.RIGHT }),
      cell(fmtD(grandFS), { bold: true, bg: 'E0E0E0', align: AlignmentType.RIGHT }),
      cell(grandAdmHrs.toFixed(2), { bold: true, bg: 'E0E0E0', align: AlignmentType.RIGHT }),
      cell(fmtD(grandAdm), { bold: true, bg: 'E0E0E0', align: AlignmentType.RIGHT }),
    ]}));

    sections.push({
      properties: { page: { margin: { top: 600, bottom: 600, left: 600, right: 600 },
        size: { orientation: 'landscape', width: 15840, height: 12240 } } },
      children: [
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
          new TextRun({ text: "The Children's Center, Inc.", bold: true, size: 28, font: 'Calibri', color: navy }) ]}),
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
          new TextRun({ text: `CACFP Staff Cost Summary — ${monthLabel}`, size: 22, font: 'Calibri', color: '666666' }) ]}),
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 }, children: [
          new TextRun({ text: `FY ${fy.label} | Sponsor #990004457`, size: 18, font: 'Calibri', color: '999999' }) ]}),
        new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: summaryRows }),
        new Paragraph({ spacing: { before: 200 }, children: [
          new TextRun({ text: `Food Service Salaries: ${fmtD(grandFS)} | Benefits (7.65%): ${fmtD(benefits)} | Admin Costs: ${fmtD(grandAdm)} | Total NFSA Cost: ${fmtD(grandFS + benefits + grandAdm)}`, size: 18, font: 'Calibri', color: '555555' }) ]}),
        new Paragraph({ spacing: { before: 100 }, children: [
          new TextRun({ text: `Staff count: ${staffRes.rows.length} | Generated: ${new Date().toLocaleDateString('en-US')}`, size: 16, font: 'Calibri', color: '999999' }) ]}),
      ]
    });

    for (const s of staffRes.rows) {
      const pgRes = await pool.query('SELECT * FROM playground_staff_hours WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3', [s.id, fiscal_year_id, month_key]);
      const ceRes = await pool.query('SELECT * FROM daily_cacfp_entries WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3', [s.id, fiscal_year_id, month_key]);
      const sigRes = await pool.query('SELECT * FROM monthly_signatures WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3', [s.id, fiscal_year_id, month_key]);
      const sig = sigRes.rows[0] || {};
      const pgMap = {}; pgRes.rows.forEach(r => { pgMap[r.day_of_month] = r; });
      const ceMap = {}; ceRes.rows.forEach(r => { ceMap[r.day_of_month] = r; });
      const rate = parseFloat(s.hourly_rate) || 0;

      const hdrRow = new TableRow({ children: [
        cell('Date', { bold: true, bg: navy, color: 'FFFFFF', w: 6, sz: 14 }),
        cell('Start', { bold: true, bg: navy, color: 'FFFFFF', w: 11, sz: 14 }),
        cell('End', { bold: true, bg: navy, color: 'FFFFFF', w: 11, sz: 14 }),
        cell('Worked', { bold: true, bg: navy, color: 'FFFFFF', w: 11, sz: 14 }),
        cell('Absent', { bold: true, bg: navy, color: 'FFFFFF', w: 11, sz: 14 }),
        cell('Non-CACFP', { bold: true, bg: navy, color: 'FFFFFF', w: 13, sz: 14 }),
        cell('CACFP FS', { bold: true, bg: '1a4a7a', color: 'FFFFFF', w: 13, sz: 14 }),
        cell('CACFP Adm', { bold: true, bg: navy, color: 'FFFFFF', w: 13, sz: 14 }),
      ]});

      let tW = 0, tA = 0, tNC = 0, tFS = 0, tAd = 0;
      const dayRows = [];
      for (let d = 1; d <= numDays; d++) {
        const pg = pgMap[d]; const ce = ceMap[d];
        const w = parseFloat(pg?.total_worked) || 0;
        const a = parseFloat(pg?.total_absent) || 0;
        const fs = parseFloat(ce?.food_service_hours) || 0;
        const ad = parseFloat(ce?.admin_hours) || 0;
        const nc = Math.max(0, w - fs - ad);
        tW += w; tA += a; tNC += nc; tFS += fs; tAd += ad;
        const bg = (pg || ce) ? undefined : 'F8F8F8';
        dayRows.push(new TableRow({ children: [
          cell(String(d), { bold: true, bg, sz: 14 }),
          cell(pg?.start_time || '', { bg, sz: 14 }),
          cell(pg?.end_time || '', { bg, sz: 14 }),
          cell(fmtN(w), { bg, sz: 14 }),
          cell(fmtN(a), { bg, sz: 14 }),
          cell(fmtN(nc), { bg, sz: 14 }),
          cell(fmtN(fs), { bg: fs > 0 ? 'E6F1FB' : bg, sz: 14 }),
          cell(fmtN(ad), { bg: ad > 0 ? 'E6F1FB' : bg, sz: 14 }),
        ]}));
      }
      const totRow = new TableRow({ children: [
        cell('', { bg: 'E0E0E0', sz: 14 }), cell('', { bg: 'E0E0E0', sz: 14 }),
        cell('Totals', { bold: true, bg: 'E0E0E0', sz: 14 }), cell('', { bg: 'E0E0E0', sz: 14 }),
        cell(tW.toFixed(2), { bold: true, bg: 'E0E0E0', sz: 14 }),
        cell(tNC.toFixed(2), { bold: true, bg: 'E0E0E0', sz: 14 }),
        cell(tFS.toFixed(2), { bold: true, bg: 'D4E8FB', sz: 14 }),
        cell(tAd.toFixed(2), { bold: true, bg: 'E0E0E0', sz: 14 }),
      ]});

      sections.push({
        properties: { page: { margin: { top: 500, bottom: 500, left: 600, right: 600 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 40 }, children: [
            new TextRun({ text: 'Michigan Department of Education', size: 16, font: 'Calibri', color: '666666' }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 40 }, children: [
            new TextRun({ text: 'Child and Adult Care Food Program', size: 16, font: 'Calibri', color: '666666' }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 100 }, children: [
            new TextRun({ text: 'Time and Attendance / Time Distribution', bold: true, size: 22, font: 'Calibri', color: navy }) ]}),
          new Paragraph({ spacing: { after: 60 }, children: [
            new TextRun({ text: 'Name: ', bold: true, size: 18, font: 'Calibri' }),
            new TextRun({ text: s.name, size: 18, font: 'Calibri', underline: {} }),
            new TextRun({ text: '     Month/Year: ', bold: true, size: 18, font: 'Calibri' }),
            new TextRun({ text: monthLabel, size: 18, font: 'Calibri', underline: {} }),
          ]}),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [hdrRow, ...dayRows, totRow] }),
          new Paragraph({ spacing: { before: 120, after: 60 }, children: [
            new TextRun({ text: `Total CACFP Admin Time: ${tAd.toFixed(2)} hrs × $${rate.toFixed(2)} = ${fmtD(tAd * rate)}`, size: 16, font: 'Calibri' }) ]}),
          new Paragraph({ spacing: { after: 60 }, children: [
            new TextRun({ text: `Total CACFP FS Labor Time: ${tFS.toFixed(2)} hrs × $${rate.toFixed(2)} = ${fmtD(tFS * rate)}`, size: 16, font: 'Calibri' }) ]}),
          new Paragraph({ spacing: { before: 120, after: 40 }, children: [
            new TextRun({ text: 'Employee Signature: ', bold: true, size: 16, font: 'Calibri' }),
            new TextRun({ text: sig.employee_signature || '________________', italics: !!sig.employee_signature, size: 16, font: 'Calibri', underline: {} }),
            new TextRun({ text: '    Date: ', bold: true, size: 16, font: 'Calibri' }),
            new TextRun({ text: sig.employee_signed_at ? new Date(sig.employee_signed_at).toLocaleDateString() : '________', size: 16, font: 'Calibri', underline: {} }),
          ]}),
          new Paragraph({ spacing: { after: 40 }, children: [
            new TextRun({ text: 'Supervisor Signature: ', bold: true, size: 16, font: 'Calibri' }),
            new TextRun({ text: supervisor_signature || sig.supervisor_signature || '________________', italics: true, size: 16, font: 'Calibri', underline: {} }),
            new TextRun({ text: '    Date: ', bold: true, size: 16, font: 'Calibri' }),
            new TextRun({ text: sig.supervisor_signed_at ? new Date(sig.supervisor_signed_at).toLocaleDateString() : new Date().toLocaleDateString(), size: 16, font: 'Calibri', underline: {} }),
          ]}),
          new Paragraph({ spacing: { before: 60 }, alignment: AlignmentType.RIGHT, children: [
            new TextRun({ text: 'Rev. 3/08', size: 12, font: 'Calibri', color: '999999' }) ]}),
        ]
      });
    }

    const doc = new Document({ sections });
    const buffer = await Packer.toBuffer(doc);
    const filename = `CACFP_Staff_Package_${month_key}_${fy.label}.docx`;

    await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,$2,'staff_package',$3,'application/vnd.openxmlformats-officedocument.wordprocessingml.document',$4,$5)`,
      [fiscal_year_id, month_key, filename, buffer, JSON.stringify({ generated: true, staff_count: staffRes.rows.length, grandFS, grandAdm })]
    );

    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// ── IMPORT PLAYGROUND DAILY CHILD ATTENDANCE CSV ─────────
app.post('/api/child-attendance-import', authCheck, upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file' });
    const { fiscal_year_id, month_key, center } = req.body;
    if (!fiscal_year_id || !month_key || !center) return res.status(400).json({ error: 'Missing fiscal_year_id, month_key, or center' });

    const text = req.file.buffer.toString('utf8').replace(/^\uFEFF/, '');
    const lines = text.split('\n').filter(l => l.trim());
    
    const header = lines[0];
    if (!header.toLowerCase().includes('last name') && !header.toLowerCase().includes('last')) {
      return res.status(400).json({ error: 'Invalid format — expected CSV with Last name, First name, Date, Check-in, Check-out columns' });
    }
    if (!header.toLowerCase().includes('check-in') && !header.toLowerCase().includes('check in')) {
      return res.status(400).json({ error: 'Invalid format — expected CSV with Check-in and Check-out columns' });
    }

    await pool.query(
      'DELETE FROM child_attendance_times WHERE fiscal_year_id=$1 AND month_key=$2 AND center=$3 AND source=$4',
      [fiscal_year_id, month_key, center, 'playground']
    );

    await pool.query(
      'DELETE FROM documents WHERE fiscal_year_id=$1 AND month_key=$2 AND doc_type=$3',
      [fiscal_year_id, month_key, 'child_attendance_daily_'+center]
    );
    await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,$2,$3,$4,'text/csv',$5,$6)`,
      [fiscal_year_id, month_key, 'child_attendance_daily_'+center, req.file.originalname, req.file.buffer, JSON.stringify({center, source:'playground'})]
    );

    let imported = 0, skipped = 0, absent = 0;
    const children = new Set();

    const mkMap = {oct:10,nov:11,dec:12,jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9};
    const targetMonth = mkMap[month_key];
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
    const fy = fyRes.rows[0];
    const targetYear = targetMonth >= 10 ? fy.start_year : fy.end_year;

    const headerLower = header.toLowerCase();
    const hasSplitCols = headerLower.includes('check in 2') || headerLower.includes('check-in 2');
    
    const hParts = header.split(',').map(h => h.replace(/"/g, '').trim().toLowerCase());
    let colCheckIn = hParts.findIndex(h => h === 'check-in' || h === 'check in');
    let colCheckOut = hParts.findIndex(h => h === 'check-out' || h === 'check out');
    let colSignerIn = -1, colSignerOut = -1;
    if (colCheckIn >= 0) {
      for (let ci = colCheckIn + 1; ci < hParts.length; ci++) {
        if (hParts[ci] === 'signer' || hParts[ci] === 'by') { colSignerIn = ci; break; }
        if (hParts[ci] === 'signature' || hParts[ci] === 'check-out' || hParts[ci] === 'check out') break;
      }
    }
    if (colCheckOut >= 0) {
      for (let ci = colCheckOut + 1; ci < hParts.length; ci++) {
        if (hParts[ci] === 'signer' || hParts[ci] === 'by') { colSignerOut = ci; break; }
        if (hParts[ci] === 'signature' || hParts[ci] === 'check in 2' || hParts[ci] === 'check-in 2') break;
      }
    }
    if (colCheckIn < 0) colCheckIn = 3;
    if (colCheckOut < 0) colCheckOut = hParts.length >= 9 ? 6 : 5;
    if (colSignerIn < 0) colSignerIn = colCheckIn + 1;
    if (colSignerOut < 0) colSignerOut = colCheckOut + 1;

    const parseTime = (t) => {
      if (!t) return null;
      const m = t.match(/(\d{1,2}):(\d{2})\s*(AM|PM)/i);
      if (!m) return null;
      let h = parseInt(m[1]);
      const min = parseInt(m[2]);
      const ampm = m[3].toUpperCase();
      if (ampm === 'PM' && h < 12) h += 12;
      if (ampm === 'AM' && h === 12) h = 0;
      return h + min / 60;
    };

    const calcHours = (cin, cout) => {
      const inT = parseTime(cin), outT = parseTime(cout);
      if (inT !== null && outT !== null && outT > inT) return Math.round((outT - inT) * 100) / 100;
      return 0;
    };

    const isTimeVal = (v) => v && v !== '-' && v !== '––' && !v.includes('http') && v.match(/\d{1,2}:\d{2}\s*(AM|PM)/i);
    const isDash = (v) => !v || v === '-' || v === '––' || v.includes('––');

    for (let i = 1; i < lines.length; i++) {
      const parts = lines[i].split(',');
      if (parts.length < 7) continue;
      
      const lastName = (parts[0] || '').trim();
      const firstName = (parts[1] || '').trim();
      const dateStr = (parts[2] || '').trim();
      if (!lastName || !dateStr) continue;

      let checkIn = (parts[colCheckIn] || '').trim();
      let checkOut = (parts[colCheckOut] || '').trim();
      let signerIn = (parts[colSignerIn] || '').trim();
      let signerOut = (parts[colSignerOut] || '').trim();
      if (signerIn.includes('http')) signerIn = '';
      if (signerOut.includes('http')) signerOut = '';

      let checkIn2 = '', checkOut2 = '', signerIn2 = '', signerOut2 = '';
      if (hasSplitCols && parts.length >= 13) {
        checkIn2 = (parts[9] || '').trim();
        checkOut2 = (parts[12] || '').trim();
        signerIn2 = (parts[10] || '').trim();
        signerOut2 = (parts[13] || '').trim();
        if (signerIn2.includes('http')) signerIn2 = '';
        if (signerOut2.includes('http')) signerOut2 = '';
        checkIn2 = checkIn2.replace(/[^\x20-\x7E]/g, '').trim();
        checkOut2 = checkOut2.replace(/[^\x20-\x7E]/g, '').trim();
        if (isDash(checkIn2)) checkIn2 = '';
        if (isDash(checkOut2)) checkOut2 = '';
      }

      let month, day, year;
      if (dateStr.includes('-') && dateStr.match(/^\d{4}-/)) {
        const dp = dateStr.split('-');
        if (dp.length !== 3) continue;
        year = parseInt(dp[0]); month = parseInt(dp[1]); day = parseInt(dp[2]);
      } else {
        const dateParts = dateStr.split('/');
        if (dateParts.length !== 3) continue;
        month = parseInt(dateParts[0]); day = parseInt(dateParts[1]); year = parseInt(dateParts[2]);
      }
      if (month !== targetMonth || year !== targetYear) continue;
      
      const attendDate = `${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
      
      const attendDt = new Date(attendDate + 'T12:00:00');
      const dow = attendDt.getDay();
      if (dow === 0 || dow === 6) continue;
      
      children.add(`${lastName}|${firstName}`);

      let status = 'present';
      let hoursDecimal = 0;
      let actualCheckIn = checkIn, actualCheckOut = checkOut;
      
      if (checkIn === 'Absent' || checkIn === '-' || !checkIn) {
        status = 'absent';
        actualCheckIn = 'Absent';
        actualCheckOut = '';
      } else {
        hoursDecimal = calcHours(checkIn, checkOut);
      }

      if (status === 'absent') absent++;

      try {
        await pool.query(
          `INSERT INTO child_attendance_times 
           (fiscal_year_id, month_key, center, child_last, child_first, attend_date, check_in, check_out, status, hours_decimal, source, signer_in, signer_out)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
           ON CONFLICT (fiscal_year_id, month_key, center, child_last, child_first, attend_date, check_in, source)
           DO UPDATE SET check_out=$8, status=$9, hours_decimal=$10, signer_in=$12, signer_out=$13, imported_at=NOW()`,
          [fiscal_year_id, month_key, center, lastName, firstName, attendDate,
           actualCheckIn, actualCheckOut || '', status, hoursDecimal, 'playground', signerIn, signerOut]
        );
        imported++;
      } catch(e) { skipped++; }

      if (checkIn2 && isTimeVal(checkIn2)) {
        const hours2 = calcHours(checkIn2, checkOut2);
        try {
          await pool.query(
            `INSERT INTO child_attendance_times 
             (fiscal_year_id, month_key, center, child_last, child_first, attend_date, check_in, check_out, status, hours_decimal, source, signer_in, signer_out)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'present',$9,'playground',$10,$11)
             ON CONFLICT (fiscal_year_id, month_key, center, child_last, child_first, attend_date, check_in, source)
             DO UPDATE SET check_out=$8, hours_decimal=$9, signer_in=$10, signer_out=$11, imported_at=NOW()`,
            [fiscal_year_id, month_key, center, lastName, firstName, attendDate,
             checkIn2, checkOut2 || '', hours2, signerIn2, signerOut2]
          );
          imported++;
        } catch(e) { skipped++; }
      }
    }

    const catRes = await pool.query(
      'SELECT child_last, child_first, attend_date, status FROM child_attendance_times WHERE fiscal_year_id=$1 AND month_key=$2 AND center=$3 AND source=$4 ORDER BY child_last, child_first, attend_date',
      [fiscal_year_id, month_key, center, 'playground']
    );
    
    const childMap = {};
    const allDates = new Set();
    for (const r of catRes.rows) {
      const dateStr = r.attend_date.toISOString().split('T')[0];
      const dt = new Date(dateStr + 'T12:00:00');
      const dow = dt.getDay();
      if (dow === 0 || dow === 6) continue;

      const key = `${r.child_last}, ${r.child_first}`;
      if (!childMap[key]) childMap[key] = { name: `${r.child_last} ${r.child_first}`, present: 0, absent: 0, dailyStatus: {}, classroom: '', presentDates: new Set() };
      
      allDates.add(dateStr);
      if (r.status === 'present') {
        if (!childMap[key].presentDates.has(dateStr)) {
          childMap[key].presentDates.add(dateStr);
          childMap[key].present++;
        }
        childMap[key].dailyStatus[dateStr] = 'P';
      } else if (!childMap[key].dailyStatus[dateStr]) {
        childMap[key].dailyStatus[dateStr] = 'A';
        childMap[key].absent++;
      }
    }
    
    const sortedDates = [...allDates].sort();
    const dayHeaders = sortedDates.map(d => {
      const dt = new Date(d + 'T12:00:00');
      const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      return `${days[dt.getDay()]} ${dt.getMonth()+1}/${dt.getDate()}`;
    });
    
    const activeChildren = Object.values(childMap).filter(ch => ch.present > 0);
    const childData = activeChildren.map(ch => ({
      name: ch.name, classroom: ch.classroom, present: ch.present, absent: ch.absent,
      dailyStatus: sortedDates.map(d => ch.dailyStatus[d] || '')
    }));
    childData.sort((a, b) => a.name.localeCompare(b.name));
    
    const opDays = sortedDates.filter(d => activeChildren.some(ch => ch.dailyStatus[d] === 'P')).length;
    const totalPresent = childData.reduce((s, c) => s + c.present, 0);
    const enrolled = childData.length;
    const ada = opDays > 0 ? Math.round((totalPresent / opDays) * 10) / 10 : 0;

    const existingAtt = await pool.query(
      `SELECT * FROM monthly_data WHERE fiscal_year_id=$1 AND month_key=$2 AND data_type='attendance'`,
      [fiscal_year_id, month_key]
    );
    const attData = existingAtt.rows[0]?.data || {};
    attData[center] = {
      enrolled, ada, days: opDays, totalPresent,
      capacity: center === 'niles' ? 105 : 164,
      childData, dayHeaders, hasTimesData: true,
      _filename: req.file.originalname
    };
    await pool.query(
      `INSERT INTO monthly_data (fiscal_year_id, month_key, data_type, data)
       VALUES ($1,$2,'attendance',$3) ON CONFLICT (fiscal_year_id, month_key, data_type)
       DO UPDATE SET data=$3, updated_at=NOW()`,
      [fiscal_year_id, month_key, JSON.stringify(attData)]
    );

    res.json({ ok: true, imported, skipped, absent, children: children.size, days: sortedDates.length });
  } catch (e) { console.error('Child attendance import error:', e); res.status(500).json({ error: e.message }); }
});

app.get('/api/child-attendance-times', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, center } = req.query;
    let q = 'SELECT * FROM child_attendance_times WHERE fiscal_year_id=$1 AND month_key=$2';
    const params = [fiscal_year_id, month_key];
    if (center) { params.push(center); q += ` AND center=$${params.length}`; }
    q += ' ORDER BY child_last, child_first, attend_date, check_in';
    const { rows } = await pool.query(q, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GENERATE ATTENDANCE TIME LOG REPORT (.docx) ──────────
app.post('/api/generate-attendance-time-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, center } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
    const fy = fyRes.rows[0]; if (!fy) return res.status(404).json({ error: 'FY not found' });

    const ML = {oct:'October',nov:'November',dec:'December',jan:'January',feb:'February',mar:'March',apr:'April',may:'May',jun:'June',jul:'July',aug:'August',sep:'September'};
    const fyYear = mk => ['oct','nov','dec'].includes(mk) ? fy.start_year : fy.end_year;
    const monthLabel = ML[month_key] + ' ' + fyYear(month_key);
    const navy = '1B2A4A';
    const thinB = { top:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'}, bottom:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'}, left:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'}, right:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'} };
    function cell(text, opts = {}) {
      return new TableCell({
        width: opts.w ? { size: opts.w, type: WidthType.PERCENTAGE } : undefined,
        borders: thinB,
        shading: opts.bg ? { type: ShadingType.SOLID, color: opts.bg } : undefined,
        children: [new Paragraph({ alignment: opts.align || AlignmentType.CENTER,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 16, font: 'Calibri', color: opts.color || '333333' })] })]
      });
    }

    const sections = [];
    for (const c of center ? [center] : ['niles', 'peace']) {
      const centerLabel = c === 'niles' ? 'The Children\'s Center — Niles' : 'The Children\'s Center — Peace Boulevard';
      
      const timesRes = await pool.query(
        'SELECT * FROM child_attendance_times WHERE fiscal_year_id=$1 AND month_key=$2 AND center=$3 ORDER BY child_last, child_first, attend_date, check_in',
        [fiscal_year_id, month_key, c]
      );
      if (!timesRes.rows.length) continue;

      const byChild = {};
      const allDates = new Set();
      for (const r of timesRes.rows) {
        const dateStr = r.attend_date.toISOString().split('T')[0];
        const dt = new Date(dateStr + 'T12:00:00');
        if (dt.getDay() === 0 || dt.getDay() === 6) continue;
        
        const key = `${r.child_last}|${r.child_first}`;
        if (!byChild[key]) byChild[key] = { last: r.child_last, first: r.child_first, days: [], presentDates: new Set() };
        byChild[key].days.push(r);
        allDates.add(dateStr);
      }
      
      for (const k of Object.keys(byChild)) {
        const presentDays = new Set(byChild[k].days.filter(d => d.status === 'present').map(d => d.attend_date.toISOString().split('T')[0]));
        byChild[k].presentDates = presentDays;
        if (presentDays.size === 0) delete byChild[k];
      }
      
      const childKeys = Object.keys(byChild).sort();
      const sortedDates = [...allDates].sort();

      let totalPresent = 0;
      for (const k of childKeys) {
        totalPresent += byChild[k].presentDates.size;
      }
      const opDays = sortedDates.filter(d => {
        return childKeys.some(k => byChild[k].presentDates.has(d));
      }).length;
      const enrolled = childKeys.length;
      const ada = opDays > 0 ? Math.round((totalPresent / opDays) * 10) / 10 : 0;
      const adaPct = enrolled > 0 ? ((ada / enrolled) * 100).toFixed(1) : '0';

      const dayAbbrs = sortedDates.map(d => {
        const dt = new Date(d + 'T12:00:00');
        return `${dt.getMonth()+1}/${dt.getDate()}`;
      });
      const gridHdr = [
        cell('Child Name', { bold: true, bg: navy, color: 'FFFFFF', sz: 12, align: AlignmentType.LEFT, w: 20 }),
      ];
      for (const da of dayAbbrs) gridHdr.push(cell(da, { bold: true, bg: navy, color: 'FFFFFF', sz: 7 }));
      gridHdr.push(cell('Days', { bold: true, bg: navy, color: 'FFFFFF', sz: 10 }));
      gridHdr.push(cell('Hrs', { bold: true, bg: navy, color: 'FFFFFF', sz: 10 }));

      const gridRows = [];
      for (const k of childKeys) {
        const ch = byChild[k];
        const rowCells = [cell(`${ch.last}, ${ch.first}`, { sz: 10, align: AlignmentType.LEFT })];
        let daysPresent = 0, totalHrs = 0;
        for (const d of sortedDates) {
          const dayRecs = ch.days.filter(dd => dd.attend_date.toISOString().split('T')[0] === d);
          if (dayRecs.length && dayRecs[0].status === 'present') {
            daysPresent++;
            const hrs = dayRecs.reduce((s, r) => s + parseFloat(r.hours_decimal || 0), 0);
            totalHrs += hrs;
            rowCells.push(cell('✓', { sz: 8, bg: 'E8F5E9' }));
          } else if (dayRecs.length && dayRecs[0].status === 'absent') {
            rowCells.push(cell('A', { sz: 8, bg: 'FFF3E0', color: 'E65100' }));
          } else {
            rowCells.push(cell('', { sz: 8 }));
          }
        }
        rowCells.push(cell(String(daysPresent), { bold: true, sz: 10 }));
        rowCells.push(cell(totalHrs > 0 ? totalHrs.toFixed(1) : '', { sz: 9 }));
        gridRows.push(new TableRow({ children: rowCells }));
      }

      const timeHdr = [
        cell('Child Name', { bold: true, bg: navy, color: 'FFFFFF', sz: 14, align: AlignmentType.LEFT, w: 22 }),
        cell('Date', { bold: true, bg: navy, color: 'FFFFFF', sz: 14, w: 14 }),
        cell('Check-In', { bold: true, bg: navy, color: 'FFFFFF', sz: 14, w: 14 }),
        cell('Check-Out', { bold: true, bg: navy, color: 'FFFFFF', sz: 14, w: 14 }),
        cell('Hours', { bold: true, bg: navy, color: 'FFFFFF', sz: 14, w: 10 }),
        cell('Status', { bold: true, bg: navy, color: 'FFFFFF', sz: 14, w: 10 }),
        cell('Source', { bold: true, bg: navy, color: 'FFFFFF', sz: 12, w: 10 }),
      ];
      const timeRows = [];
      for (const k of childKeys) {
        const ch = byChild[k];
        let first = true;
        for (const d of ch.days) {
          const dateStr = new Date(d.attend_date).toLocaleDateString('en-US', { weekday: 'short', month: 'numeric', day: 'numeric' });
          const bg = d.status === 'absent' ? 'FFF8E1' : undefined;
          timeRows.push(new TableRow({ children: [
            cell(first ? `${ch.last}, ${ch.first}` : '', { sz: 12, align: AlignmentType.LEFT, bg }),
            cell(dateStr, { sz: 12, bg }),
            cell(d.status === 'absent' ? '—' : (d.check_in || ''), { sz: 12, bg }),
            cell(d.status === 'absent' ? '—' : (d.check_out || ''), { sz: 12, bg }),
            cell(d.status === 'present' && d.hours_decimal > 0 ? parseFloat(d.hours_decimal).toFixed(1) : '', { sz: 12, bg }),
            cell(d.status === 'present' ? 'Present' : 'Absent', { sz: 11, bg, color: d.status === 'present' ? '2E7D32' : 'E65100' }),
            cell(d.source || 'playground', { sz: 10, bg, color: '999999' }),
          ]}));
          first = false;
        }
      }

      sections.push({
        properties: { page: { margin: { top: 500, bottom: 500, left: 400, right: 400 },
          size: { orientation: 'landscape', width: 15840, height: 12240 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [
            new TextRun({ text: centerLabel, bold: true, size: 26, font: 'Calibri', color: navy }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [
            new TextRun({ text: `Child Attendance Time Log — ${monthLabel}`, size: 20, font: 'Calibri', color: '666666' }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 100 }, children: [
            new TextRun({ text: `FY ${fy.label} | Sponsor #990004457`, size: 14, font: 'Calibri', color: '999999' }) ]}),
          new Paragraph({ spacing: { after: 60 }, children: [
            new TextRun({ text: 'ADA Calculation: ', bold: true, size: 16, font: 'Calibri', color: navy }),
            new TextRun({ text: `${totalPresent} total child-days present ÷ ${opDays} operating days = ${ada} average daily attendance (${adaPct}% of ${enrolled} enrolled)`, size: 16, font: 'Calibri' }) ]}),
          new Paragraph({ spacing: { after: 20 }, children: [
            new TextRun({ text: `Enrolled: ${enrolled}  |  Operating Days: ${opDays}  |  Total Child-Days: ${totalPresent}  |  ADA: ${ada}`, size: 14, font: 'Calibri', color: '555555' }) ]}),
          new Paragraph({ spacing: { after: 100 }, children: [
            new TextRun({ text: '— Daily Attendance Summary Grid —', bold: true, size: 16, font: 'Calibri', color: navy }) ]}),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [new TableRow({ children: gridHdr }), ...gridRows] }),
        ]
      });

      sections.push({
        properties: { page: { margin: { top: 500, bottom: 500, left: 600, right: 600 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [
            new TextRun({ text: centerLabel, bold: true, size: 24, font: 'Calibri', color: navy }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 100 }, children: [
            new TextRun({ text: `Detailed Sign-In / Sign-Out Time Log — ${monthLabel}`, size: 20, font: 'Calibri', color: '666666' }) ]}),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [new TableRow({ children: timeHdr }), ...timeRows] }),
          new Paragraph({ spacing: { before: 100 }, children: [
            new TextRun({ text: `Generated: ${new Date().toLocaleDateString('en-US')} | Records: ${timesRes.rows.length}`, size: 12, font: 'Calibri', color: '999999' }) ]}),
        ]
      });
    }

    if (!sections.length) return res.status(400).json({ error: 'No child attendance time data found. Upload a Playground Daily Attendance CSV first.' });

    const doc = new Document({ sections });
    const buffer = await Packer.toBuffer(doc);
    const filename = `Child_Attendance_TimeLog_${center || 'All'}_${month_key}_${fy.label}.docx`;

    await pool.query(
      'DELETE FROM documents WHERE fiscal_year_id=$1 AND month_key=$2 AND doc_type=$3',
      [fiscal_year_id, month_key, 'attendance_time_report']
    );
    await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,$2,'attendance_time_report',$3,'application/vnd.openxmlformats-officedocument.wordprocessingml.document',$4,$5)`,
      [fiscal_year_id, month_key, filename, buffer, JSON.stringify({ generated: true, center: center || 'all' })]
    );

    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.post('/api/generate-attendance-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, center } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
    const fy = fyRes.rows[0]; if (!fy) return res.status(404).json({ error: 'FY not found' });

    const ML = {oct:'October',nov:'November',dec:'December',jan:'January',feb:'February',mar:'March',apr:'April',may:'May',jun:'June',jul:'July',aug:'August',sep:'September'};
    const fyYear = mk => ['oct','nov','dec'].includes(mk) ? fy.start_year : fy.end_year;
    const monthLabel = ML[month_key] + ' ' + fyYear(month_key);
    const navy = '1B2A4A';
    const thinB = { top:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'}, bottom:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'}, left:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'}, right:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'} };
    function cell(text, opts = {}) {
      return new TableCell({
        width: opts.w ? { size: opts.w, type: WidthType.PERCENTAGE } : undefined,
        borders: thinB,
        shading: opts.bg ? { type: ShadingType.SOLID, color: opts.bg } : undefined,
        children: [new Paragraph({ alignment: opts.align || AlignmentType.CENTER,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 14, font: 'Calibri', color: opts.color || '333333' })] })]
      });
    }

    const mdRes = await pool.query(
      `SELECT * FROM monthly_data WHERE fiscal_year_id=$1 AND month_key=$2 AND data_type='attendance'`,
      [fiscal_year_id, month_key]
    );
    const attData = mdRes.rows[0]?.data || {};
    const sections = [];

    for (const c of center ? [center] : ['niles', 'peace']) {
      const d = attData[c];
      if (!d || !d.childData) continue;
      const centerLabel = c === 'niles' ? 'Niles' : 'Peace Boulevard';
      const dayHeaders = d.dayHeaders || [];

      const hdrCells = [
        cell('Name', { bold: true, bg: navy, color: 'FFFFFF', sz: 12, align: AlignmentType.LEFT }),
        cell('Class', { bold: true, bg: navy, color: 'FFFFFF', sz: 10 }),
      ];
      for (const dh of dayHeaders) hdrCells.push(cell(dh, { bold: true, bg: navy, color: 'FFFFFF', sz: 9 }));
      hdrCells.push(cell('Days', { bold: true, bg: navy, color: 'FFFFFF', sz: 11 }));

      const dataRows = [];
      for (const ch of d.childData) {
        const rowCells = [
          cell(ch.name, { sz: 11, align: AlignmentType.LEFT }),
          cell((ch.classroom || '').substring(0, 10), { sz: 9 }),
        ];
        const daily = ch.dailyStatus || [];
        for (let i = 0; i < dayHeaders.length; i++) {
          const st = daily[i] || '';
          rowCells.push(cell(st === 'P' ? '✓' : '', { sz: 10, bg: st === 'P' ? 'E8F5E9' : undefined }));
        }
        rowCells.push(cell(String(ch.present || 0), { bold: true, sz: 11 }));
        dataRows.push(new TableRow({ children: rowCells }));
      }

      sections.push({
        properties: { page: { margin: { top: 500, bottom: 500, left: 400, right: 400 },
          size: { orientation: 'landscape', width: 15840, height: 12240 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [
            new TextRun({ text: "The Children's Center, Inc.", bold: true, size: 24, font: 'Calibri', color: navy }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [
            new TextRun({ text: `Child Attendance Report — ${centerLabel}`, size: 20, font: 'Calibri', color: '666666' }) ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 }, children: [
            new TextRun({ text: `${monthLabel} | FY ${fy.label}`, size: 16, font: 'Calibri', color: '999999' }) ]}),
          new Paragraph({ spacing: { after: 80 }, children: [
            new TextRun({ text: `Enrolled: ${d.enrolled}  |  Operating Days: ${d.days}  |  Total Child-Days Present: ${d.totalPresent}  |  ADA: ${d.ada}  (${d.enrolled > 0 ? ((d.ada/d.enrolled)*100).toFixed(1) : '0'}%)`, size: 16, font: 'Calibri', color: '555555' }) ]}),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [new TableRow({ children: hdrCells }), ...dataRows] }),
          new Paragraph({ spacing: { before: 100 }, children: [
            new TextRun({ text: `Generated: ${new Date().toLocaleDateString('en-US')}`, size: 14, font: 'Calibri', color: '999999' }) ]}),
        ]
      });
    }

    if (sections.length === 0) return res.status(400).json({ error: 'No attendance data found for this month' });

    const doc = new Document({ sections });
    const buffer = await Packer.toBuffer(doc);
    const filename = `Child_Attendance_${center || 'All'}_${month_key}_${fy.label}.docx`;

    await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,$2,'attendance_report',$3,'application/vnd.openxmlformats-officedocument.wordprocessingml.document',$4,$5)`,
      [fiscal_year_id, month_key, filename, buffer, JSON.stringify({ generated: true, center: center || 'all' })]
    );

    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.get('/api/child-attendance-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, center } = req.query;
    const mdRes = await pool.query(
      `SELECT * FROM monthly_data WHERE fiscal_year_id=$1 AND month_key=$2 AND data_type='attendance'`,
      [fiscal_year_id, month_key]
    );
    if (!mdRes.rows.length) return res.json({ children: [], summary: {} });
    const data = mdRes.rows[0].data;
    const allChildren = [];
    for (const c of ['niles', 'peace']) {
      if (center && c !== center) continue;
      const cd = data[c];
      if (!cd || !cd.children) continue;
      for (const child of cd.children) {
        allChildren.push({ ...child, center: c });
      }
    }
    res.json({ children: allChildren, data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════
// ATTENDANCE vs MEAL COUNT CROSS-CHECK (interval-overlap based)
// ═══════════════════════════════════════════════════════════
// CACFP meal service windows at TCC:
//   Breakfast:  6:30 AM – 9:30 AM
//   AM Snack:   9:30 AM – 11:15 AM
//   Lunch:      11:15 AM – 2:00 PM
//   PM Snack:   2:00 PM – end of day (any time after 2:00 PM)
//
// A meal claim is flagged ONLY IF the child was not present at any point during
// that meal's service window. Split-day children (e.g. morning care → school →
// afterschool care) commonly have two or more in/out pairs per day, so the check
// walks ALL intervals and tests whether any interval overlaps the window.
//
// An interval is [check_in, check_out]. A null check_out means the child was
// still checked in at end of day, so the interval is treated as [check_in, ∞].
//
// Infants (under 1 year) eat on demand — exempt from window validation.
// Infants are identified by classroom name: Tiny Treasures, Caterpillars, Butterflies.
const INFANT_CLASSROOMS = new Set([
  'tiny treasures',
  'caterpillars',
  'butterflies'
]);
const MEAL_WINDOWS = {
  breakfast: { start: 6 * 60 + 30, end: 9 * 60 + 30,  label: 'Breakfast', display: '6:30 AM – 9:30 AM' },
  amSnack:   { start: 9 * 60 + 30, end: 11 * 60 + 15, label: 'AM Snack',  display: '9:30 AM – 11:15 AM' },
  lunch:     { start: 11 * 60 + 15, end: 14 * 60,     label: 'Lunch',     display: '11:15 AM – 2:00 PM' },
  pmSnack:   { start: 14 * 60, end: 24 * 60,          label: 'PM Snack',  display: 'after 2:00 PM' }
};

// Parse "9:15 AM" / "2:30 PM" to minutes since midnight
function parseTimeToMinutes(t) {
  if (!t || typeof t !== 'string') return null;
  const m = t.trim().match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (!m) return null;
  let h = parseInt(m[1]);
  const min = parseInt(m[2]);
  const ampm = m[3].toUpperCase();
  if (ampm === 'PM' && h !== 12) h += 12;
  if (ampm === 'AM' && h === 12) h = 0;
  return h * 60 + min;
}

function minutesToTimeStr(min) {
  if (min === null || min === undefined) return '';
  let h = Math.floor(min / 60);
  const m = min % 60;
  const ampm = h >= 12 ? 'PM' : 'AM';
  if (h === 0) h = 12;
  else if (h > 12) h -= 12;
  return `${h}:${String(m).padStart(2,'0')} ${ampm}`;
}

// Does any interval in `intervals` overlap [winStart, winEnd)?
// Each interval is {inMin, outMin}; outMin null means open-ended (still on site).
function intervalsOverlapWindow(intervals, winStart, winEnd) {
  if (!intervals || !intervals.length) return false;
  for (const iv of intervals) {
    if (iv.inMin === null) continue;
    const effectiveOut = iv.outMin === null ? 24 * 60 : iv.outMin;
    // Overlap: interval start < window end AND interval end > window start
    if (iv.inMin < winEnd && effectiveOut > winStart) return true;
  }
  return false;
}

// Format all intervals for display: "6:45 AM–8:25 AM, 3:30 PM–6:00 PM"
function formatIntervals(intervals) {
  if (!intervals || !intervals.length) return '';
  return intervals.map(iv => {
    const inStr = iv.inMin !== null ? minutesToTimeStr(iv.inMin) : '?';
    const outStr = iv.outMin !== null ? minutesToTimeStr(iv.outMin) : 'still in';
    return `${inStr}–${outStr}`;
  }).join(', ');
}

app.get('/api/audit-crosscheck', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.query;

    const attRes = await pool.query(
      `SELECT * FROM monthly_data WHERE fiscal_year_id=$1 AND month_key=$2 AND data_type='attendance'`,
      [fiscal_year_id, month_key]
    );
    const mealRes = await pool.query(
      `SELECT * FROM monthly_data WHERE fiscal_year_id=$1 AND month_key=$2 AND data_type='meals'`,
      [fiscal_year_id, month_key]
    );
    const timesRes = await pool.query(
      `SELECT * FROM child_attendance_times WHERE fiscal_year_id=$1 AND month_key=$2`,
      [fiscal_year_id, month_key]
    );
    const attData = attRes.rows[0]?.data || {};
    const mealData = mealRes.rows[0]?.data || {};

    const resRes = await pool.query(
      `SELECT * FROM crosscheck_resolutions WHERE fiscal_year_id=$1 AND month_key=$2`,
      [fiscal_year_id, month_key]
    );
    const resMap = {};
    for (const r of resRes.rows) {
      resMap[`${r.center}::${r.flag_key}`] = r;
    }

    // Build lookup: ALL in/out intervals per child per date
    // Keyed by `${center}::${normName}::${dateISO}` → [{inMin, outMin}, ...]
    const timeLookup = {};
    const normName = n => (n || '').toString().trim().toLowerCase().replace(/\s+/g, ' ');
    for (const r of timesRes.rows) {
      if (r.status !== 'present') continue;
      const dateISO = r.attend_date instanceof Date
        ? r.attend_date.toISOString().split('T')[0]
        : String(r.attend_date).split('T')[0];
      const nameKey = normName(`${r.child_first} ${r.child_last}`);
      const key = `${r.center}::${nameKey}::${dateISO}`;
      const inMin = parseTimeToMinutes(r.check_in);
      const outMin = parseTimeToMinutes(r.check_out);
      if (inMin === null) continue; // no usable data
      if (!timeLookup[key]) timeLookup[key] = [];
      timeLookup[key].push({ inMin, outMin });
    }
    // Sort each child's intervals by check-in time for display consistency
    for (const k of Object.keys(timeLookup)) {
      timeLookup[k].sort((a, b) => a.inMin - b.inMin);
    }

    const flags = [];
    let totalMealsClaimed = 0;
    let childrenChecked = 0;

    for (const center of ['niles', 'peace']) {
      const meals = mealData[center] || {};
      if (!meals.children || !meals.dayLabels) continue;

      const fyRes2 = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
      const fy = fyRes2.rows[0];
      const MN = {jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11};
      const monthIdx = MN[month_key];
      const year = ['oct','nov','dec'].includes(month_key) ? fy.start_year : fy.end_year;

      // Extract day-of-month from each meal dayLabel (e.g. "Mon 3/15/2025" → 15)
      const dayOfMonth = meals.dayLabels.map(dl => {
        const m = String(dl).match(/\/(\d{1,2})(?:\/|\s|$)/) || String(dl).match(/(\d{1,2})\s*$/);
        return m ? parseInt(m[1]) : null;
      });

      for (const child of meals.children) {
        if (!child.dailyDetail) continue;
        childrenChecked++;

        // Infant check — skip entirely
        const classroomLower = (child.classroom || '').toLowerCase().trim();
        const isInfant = INFANT_CLASSROOMS.has(classroomLower);
        if (isInfant) continue;

        const childNameKey = normName(child.name);

        for (let dayIdx = 0; dayIdx < child.dailyDetail.length; dayIdx++) {
          const dd = child.dailyDetail[dayIdx];
          if (!dd) continue;
          const dom = dayOfMonth[dayIdx];
          if (!dom) continue;

          // Meals CLAIMED for this day (pmX = excluded by 3-meal rule, so not claimed)
          const claimed = {
            breakfast: !!dd.bk,
            amSnack: !!dd.am,
            lunch: !!dd.ln,
            pmSnack: !!dd.pm && !dd.pmX
          };
          const anyClaimed = claimed.breakfast || claimed.amSnack || claimed.lunch || claimed.pmSnack;
          if (!anyClaimed) continue;

          const dateISO = `${year}-${String(monthIdx + 1).padStart(2,'0')}-${String(dom).padStart(2,'0')}`;
          const timeKey = `${center}::${childNameKey}::${dateISO}`;
          const intervals = timeLookup[timeKey];

          totalMealsClaimed += (claimed.breakfast?1:0) + (claimed.amSnack?1:0) + (claimed.lunch?1:0) + (claimed.pmSnack?1:0);

          // If no attendance time record, can't validate — skip silently
          if (!intervals || !intervals.length) continue;

          const intervalsDisplay = formatIntervals(intervals);
          // Derived display fields (kept for UI back-compat)
          const firstIn = intervals[0].inMin;
          const lastOutVal = intervals.reduce((lo, iv) => {
            if (iv.outMin === null) return null; // still-in wins
            if (lo === null) return lo;          // already still-in
            return Math.max(lo, iv.outMin);
          }, intervals[0].outMin);

          const emit = (mealType, reason) => {
            const w = MEAL_WINDOWS[mealType];
            const flagKey = `${childNameKey}::${dateISO}::${mealType}`;
            const resKey = `${center}::${flagKey}`;
            const existing = resMap[resKey] || {};
            flags.push({
              flag_key: flagKey,
              center,
              child: child.name,
              classroom: child.classroom || '',
              date: dateISO,
              dayDisplay: String(meals.dayLabels[dayIdx] || dom),
              meal_type: mealType,
              meal_label: w.label,
              check_in: minutesToTimeStr(firstIn),
              check_out: lastOutVal !== null ? minutesToTimeStr(lastOutVal) : null,
              intervals_display: intervalsDisplay,
              window_display: w.display,
              window_end: mealType === 'pmSnack' ? '2:00 PM' : minutesToTimeStr(w.end),
              reason,
              category: child.cat || 'C',
              status: existing.status || 'pending',
              resolution_notes: existing.resolution_notes || '',
              attached_review_id: existing.attached_review_id || null,
              resolved_by: existing.resolved_by || null,
              resolved_at: existing.resolved_at || null
            });
          };

          // Check each claimed meal: does ANY interval overlap the window?
          for (const mk of ['breakfast', 'amSnack', 'lunch', 'pmSnack']) {
            if (!claimed[mk]) continue;
            const w = MEAL_WINDOWS[mk];
            if (!intervalsOverlapWindow(intervals, w.start, w.end)) {
              emit(mk,
                `${w.label} claimed, but child was not on site during the ${w.display} window. Attendance: ${intervalsDisplay}.`);
            }
          }
        }
      }
    }

    const counts = { total: flags.length, pending: 0, resolved: 0, report_as_discrepancy: 0 };
    for (const f of flags) counts[f.status] = (counts[f.status] || 0) + 1;

    res.json({
      flags,
      counts,
      summary: {
        totalMealsClaimed,
        childrenChecked,
        infantClassrooms: [...INFANT_CLASSROOMS]
      }
    });
  } catch (e) { console.error('crosscheck error:', e); res.status(500).json({ error: e.message }); }
});

// ── CROSS-CHECK RESOLUTIONS ──────────────────────────────
app.post('/api/crosscheck-resolutions', authCheck, async (req, res) => {
  try {
    const {
      fiscal_year_id, month_key, center, flag_key,
      child_name, flag_date, meal_type, check_in, window_end,
      status, resolution_notes, resolved_by
    } = req.body;

    let attached_review_id = null;
    if (status === 'report_as_discrepancy') {
      const revRes = await pool.query(
        `SELECT id FROM monitoring_reviews
         WHERE fiscal_year_id=$1 AND center=$2 AND status='in_progress'
         ORDER BY review_date DESC, created_at DESC LIMIT 1`,
        [fiscal_year_id, center]
      );
      if (revRes.rows[0]) attached_review_id = revRes.rows[0].id;
    }

    const { rows } = await pool.query(
      `INSERT INTO crosscheck_resolutions
         (fiscal_year_id, month_key, center, flag_key, child_name, flag_date, meal_type,
          check_in, window_end, status, resolution_notes, resolved_by, resolved_at, attached_review_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW(),$13)
       ON CONFLICT (fiscal_year_id, month_key, center, flag_key)
       DO UPDATE SET
         status=$10,
         resolution_notes=$11,
         resolved_by=$12,
         resolved_at=NOW(),
         attached_review_id=$13,
         updated_at=NOW()
       RETURNING *`,
      [fiscal_year_id, month_key, center, flag_key, child_name, flag_date, meal_type,
       check_in, window_end, status, resolution_notes || '', resolved_by || '', attached_review_id]
    );
    res.json(rows[0]);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.get('/api/crosscheck-resolutions', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, center, status, attached_review_id } = req.query;
    let q = 'SELECT * FROM crosscheck_resolutions WHERE 1=1';
    const p = [];
    if (fiscal_year_id) { p.push(fiscal_year_id); q += ` AND fiscal_year_id=$${p.length}`; }
    if (month_key)      { p.push(month_key);      q += ` AND month_key=$${p.length}`; }
    if (center)         { p.push(center);         q += ` AND center=$${p.length}`; }
    if (status)         { p.push(status);         q += ` AND status=$${p.length}`; }
    if (attached_review_id) { p.push(attached_review_id); q += ` AND attached_review_id=$${p.length}`; }
    q += ' ORDER BY flag_date, child_name';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/crosscheck-resolutions/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM crosscheck_resolutions WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GENERATE MONTHLY STAFF COST REPORT (.docx) ───────────
app.post('/api/generate-staff-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id = $1', [fiscal_year_id]);
    const fy = fyRes.rows[0];
    if (!fy) return res.status(404).json({ error: 'Fiscal year not found' });

    const ML = { oct:'October',nov:'November',dec:'December',jan:'January',feb:'February',
      mar:'March',apr:'April',may:'May',jun:'June',jul:'July',aug:'August',sep:'September'};
    const fyYear = mk => {
      const first = ['oct','nov','dec'];
      return first.includes(mk) ? fy.start_year : fy.end_year;
    };
    const monthLabel = `${ML[month_key]} ${fyYear(month_key)}`;

    const { rows: entries } = await pool.query(`
      SELECT ste.*, s.name, s.center
      FROM staff_time_entries ste
      JOIN staff s ON s.id = ste.staff_id
      WHERE ste.fiscal_year_id = $1 AND ste.month_key = $2 AND s.is_active = true
      ORDER BY s.center, s.name
    `, [fiscal_year_id, month_key]);

    const fmt = n => '$' + Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const navy = '1B2A4A';
    const noBorder = { top: { style: BorderStyle.NONE }, bottom: { style: BorderStyle.NONE },
      left: { style: BorderStyle.NONE }, right: { style: BorderStyle.NONE } };
    const thinBorder = {
      top: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' },
      bottom: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' },
      left: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' },
      right: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' }
    };

    function makeCell(text, opts = {}) {
      return new TableCell({
        width: opts.width ? { size: opts.width, type: WidthType.PERCENTAGE } : undefined,
        borders: opts.noBorder ? noBorder : thinBorder,
        shading: opts.shading ? { type: ShadingType.SOLID, color: opts.shading } : undefined,
        children: [new Paragraph({
          alignment: opts.align || AlignmentType.LEFT,
          children: [new TextRun({
            text: text || '',
            bold: opts.bold || false,
            size: opts.size || 20,
            font: 'Calibri',
            color: opts.color || '333333'
          })]
        })]
      });
    }

    const headerRow = new TableRow({
      children: [
        makeCell('Staff Name', { bold: true, shading: navy, color: 'FFFFFF', width: 28 }),
        makeCell('Center', { bold: true, shading: navy, color: 'FFFFFF', width: 14 }),
        makeCell('Rate/Hr', { bold: true, shading: navy, color: 'FFFFFF', width: 12, align: AlignmentType.RIGHT }),
        makeCell('FS Hours', { bold: true, shading: navy, color: 'FFFFFF', width: 12, align: AlignmentType.RIGHT }),
        makeCell('FS Cost', { bold: true, shading: navy, color: 'FFFFFF', width: 17, align: AlignmentType.RIGHT }),
        makeCell('Admin Hrs', { bold: true, shading: navy, color: 'FFFFFF', width: 12, align: AlignmentType.RIGHT }),
        makeCell('Admin Cost', { bold: true, shading: navy, color: 'FFFFFF', width: 17, align: AlignmentType.RIGHT }),
      ]
    });

    let grandFS = 0, grandAdmin = 0, grandFSHrs = 0, grandAdmHrs = 0;
    const dataRows = entries.map((e, i) => {
      const rate = parseFloat(e.hourly_rate_used) || 0;
      const fsH = parseFloat(e.food_service_hours) || 0;
      const admH = parseFloat(e.admin_hours) || 0;
      const fsCost = fsH * rate;
      const admCost = admH * rate;
      grandFS += fsCost; grandAdmin += admCost; grandFSHrs += fsH; grandAdmHrs += admH;
      const bg = i % 2 === 0 ? undefined : 'F5F5F5';
      const centerLabel = e.center === 'niles' ? 'Niles' : 'Peace Blvd';
      return new TableRow({
        children: [
          makeCell(e.name, { shading: bg }),
          makeCell(centerLabel, { shading: bg }),
          makeCell(fmt(rate), { align: AlignmentType.RIGHT, shading: bg }),
          makeCell(fsH > 0 ? fsH.toFixed(2) : '—', { align: AlignmentType.RIGHT, shading: bg }),
          makeCell(fsCost > 0 ? fmt(fsCost) : '—', { align: AlignmentType.RIGHT, shading: bg }),
          makeCell(admH > 0 ? admH.toFixed(2) : '—', { align: AlignmentType.RIGHT, shading: bg }),
          makeCell(admCost > 0 ? fmt(admCost) : '—', { align: AlignmentType.RIGHT, shading: bg }),
        ]
      });
    });

    const benefits = grandFS * 0.0765;

    const totalsRow = new TableRow({
      children: [
        makeCell('TOTALS', { bold: true, shading: 'E8E8E8' }),
        makeCell('', { shading: 'E8E8E8' }),
        makeCell('', { shading: 'E8E8E8' }),
        makeCell(grandFSHrs.toFixed(2), { bold: true, align: AlignmentType.RIGHT, shading: 'E8E8E8' }),
        makeCell(fmt(grandFS), { bold: true, align: AlignmentType.RIGHT, shading: 'E8E8E8' }),
        makeCell(grandAdmHrs.toFixed(2), { bold: true, align: AlignmentType.RIGHT, shading: 'E8E8E8' }),
        makeCell(fmt(grandAdmin), { bold: true, align: AlignmentType.RIGHT, shading: 'E8E8E8' }),
      ]
    });

    const doc = new Document({
      sections: [{
        properties: { page: { margin: { top: 720, bottom: 720, left: 720, right: 720 },
          size: { orientation: 'landscape', width: 15840, height: 12240 } } },
        children: [
          new Paragraph({
            alignment: AlignmentType.CENTER, spacing: { after: 80 },
            children: [new TextRun({ text: "The Children's Center, Inc.", bold: true, size: 28, font: 'Calibri', color: navy })]
          }),
          new Paragraph({
            alignment: AlignmentType.CENTER, spacing: { after: 80 },
            children: [new TextRun({ text: `CACFP Monthly Staff Cost Report`, size: 24, font: 'Calibri', color: '666666' })]
          }),
          new Paragraph({
            alignment: AlignmentType.CENTER, spacing: { after: 200 },
            children: [new TextRun({ text: `${monthLabel} | FY ${fy.label} | Sponsor #990004457`, size: 20, font: 'Calibri', color: '999999' })]
          }),
          new Table({
            width: { size: 100, type: WidthType.PERCENTAGE },
            rows: [headerRow, ...dataRows, totalsRow]
          }),
          new Paragraph({ spacing: { before: 300 }, children: [] }),
          new Paragraph({
            children: [
              new TextRun({ text: 'Summary: ', bold: true, size: 20, font: 'Calibri' }),
              new TextRun({ text: `Food Service Salaries: ${fmt(grandFS)} | Benefits (7.65%): ${fmt(benefits)} | Admin Costs: ${fmt(grandAdmin)} | Total NFSA Cost: ${fmt(grandFS + benefits + grandAdmin)}`, size: 20, font: 'Calibri', color: '555555' })
            ]
          }),
          new Paragraph({ spacing: { before: 200 }, children: [
            new TextRun({ text: `Report generated: ${new Date().toLocaleDateString('en-US')} | Staff count: ${entries.length}`, size: 18, font: 'Calibri', color: '999999' })
          ]}),
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    res.setHeader('Content-Disposition', `attachment; filename="Staff_Cost_Report_${month_key}_${fy.label}.docx"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// ── GENERATE NFSA GENERAL LEDGER (.docx) ──────────────────
app.post('/api/generate-gl', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id } = req.body;

    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id = $1', [fiscal_year_id]);
    const fy = fyRes.rows[0];
    if (!fy) return res.status(404).json({ error: 'Fiscal year not found' });

    const salRes = await pool.query(`
      SELECT month_key,
        SUM(food_service_hours * hourly_rate_used) as fs_cost,
        SUM(admin_hours * hourly_rate_used) as admin_cost
      FROM staff_time_entries WHERE fiscal_year_id = $1
      GROUP BY month_key ORDER BY month_key
    `, [fiscal_year_id]);

    const yerRes = await pool.query('SELECT * FROM yer_data WHERE fiscal_year_id = $1', [fiscal_year_id]);
    const yer = yerRes.rows[0] || {};

    const revRes = await pool.query(`
      SELECT month_key, revenue_type, SUM(amount) as total
      FROM revenue_entries WHERE fiscal_year_id = $1
      GROUP BY month_key, revenue_type ORDER BY month_key
    `, [fiscal_year_id]);

    let totalFSCost = 0, totalAdminCost = 0;
    for (const r of salRes.rows) {
      totalFSCost += parseFloat(r.fs_cost) || 0;
      totalAdminCost += parseFloat(r.admin_cost) || 0;
    }
    const benefits = totalFSCost * 0.0765;
    const totalSalaries = totalFSCost;
    const foodCost = parseFloat(yer.food_cost) || 0;
    const cacfpReimb = parseFloat(yer.cacfp_reimbursement) || 0;
    const totalExpenses = foodCost + totalSalaries + benefits + totalAdminCost;
    let totalRevenue = cacfpReimb;
    for (const r of revRes.rows) { totalRevenue += parseFloat(r.total) || 0; }
    const fundBalance = totalRevenue - totalExpenses;

    const fmt = n => '$' + Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    const noBorder = { top: { style: BorderStyle.NONE }, bottom: { style: BorderStyle.NONE },
      left: { style: BorderStyle.NONE }, right: { style: BorderStyle.NONE } };
    const navy = '1B2A4A';

    function makeRow(label, amount, bold = false, shading = null) {
      return new TableRow({
        children: [
          new TableCell({
            width: { size: 70, type: WidthType.PERCENTAGE },
            borders: noBorder,
            shading: shading ? { type: ShadingType.SOLID, color: shading } : undefined,
            children: [new Paragraph({
              children: [new TextRun({ text: label, bold, size: 22, font: 'Calibri',
                color: shading === navy ? 'FFFFFF' : '333333' })]
            })]
          }),
          new TableCell({
            width: { size: 30, type: WidthType.PERCENTAGE },
            borders: noBorder,
            shading: shading ? { type: ShadingType.SOLID, color: shading } : undefined,
            children: [new Paragraph({
              alignment: AlignmentType.RIGHT,
              children: [new TextRun({ text: amount, bold, size: 22, font: 'Calibri',
                color: shading === navy ? 'FFFFFF' : '333333' })]
            })]
          })
        ]
      });
    }

    const doc = new Document({
      sections: [{
        properties: { page: { margin: { top: 720, bottom: 720, left: 1080, right: 1080 } } },
        children: [
          new Paragraph({
            alignment: AlignmentType.CENTER,
            spacing: { after: 100 },
            children: [new TextRun({ text: "The Children's Center, Inc.", bold: true, size: 32, font: 'Calibri', color: navy })]
          }),
          new Paragraph({
            alignment: AlignmentType.CENTER,
            spacing: { after: 100 },
            children: [new TextRun({ text: 'Non-profit Food Service Account (NFSA) — General Ledger', size: 24, font: 'Calibri', color: '666666' })]
          }),
          new Paragraph({
            alignment: AlignmentType.CENTER,
            spacing: { after: 300 },
            children: [new TextRun({ text: `Fiscal Year: ${fy.label} | Sponsor #990004457`, size: 20, font: 'Calibri', color: '999999' })]
          }),
          new Table({
            width: { size: 100, type: WidthType.PERCENTAGE },
            rows: [
              makeRow('REVENUE', '', true, navy),
              makeRow('  CACFP Reimbursement (MIND Line 3a)', fmt(cacfpReimb)),
              makeRow('  Program Meal Revenue (B & C Categories)', fmt(totalRevenue - cacfpReimb)),
              makeRow('  Total Revenue', fmt(totalRevenue), true, 'F0F0F0'),
              makeRow('', ''),
              makeRow('EXPENSES', '', true, navy),
              makeRow('  Food & Supplies (Account 64100)', fmt(foodCost)),
              makeRow('  Food Service Salaries', fmt(totalSalaries)),
              makeRow('  Employee Benefits (7.65%)', fmt(benefits)),
              makeRow('  Administrative Costs', fmt(totalAdminCost)),
              makeRow('  Total Expenses', fmt(totalExpenses), true, 'F0F0F0'),
              makeRow('', ''),
              makeRow('FUND BALANCE', fmt(fundBalance), true, fundBalance >= 0 ? '2D7D46' : 'C0392B'),
            ]
          }),
          new Paragraph({ spacing: { before: 400 }, children: [] }),
          new Paragraph({
            children: [new TextRun({ text: 'Notes: ', bold: true, size: 20, font: 'Calibri' }),
              new TextRun({ text: yer.notes || 'Revenue includes CACFP reimbursement and tuition-funded meal revenue for Category B and C meals, as required by MDE NFSA revenue policy.',
                size: 20, font: 'Calibri', color: '666666' })]
          }),
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    res.setHeader('Content-Disposition', `attachment; filename="NFSA_General_Ledger_${fy.label}.docx"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── MONITORING TABLE INIT ─────────────────────────────────
async function initMonitoringTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS monitoring_reviews (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      center VARCHAR(50) NOT NULL,
      review_date DATE,
      announced BOOLEAN DEFAULT false,
      meal_observed VARCHAR(50),
      monitor_name VARCHAR(150),
      arrival_time VARCHAR(20),
      departure_time VARCHAR(20),
      status VARCHAR(20) DEFAULT 'in_progress',
      form_data JSONB DEFAULT '{}',
      findings JSONB DEFAULT '[]',
      five_day_data JSONB DEFAULT '{}',
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS monitoring_schedule (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      center VARCHAR(50) NOT NULL,
      planned_date DATE,
      announced BOOLEAN DEFAULT false,
      includes_meal_obs BOOLEAN DEFAULT false,
      review_id INTEGER REFERENCES monitoring_reviews(id) ON DELETE SET NULL,
      notes TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS training_records (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      training_date DATE NOT NULL,
      training_type VARCHAR(50) NOT NULL,
      topic VARCHAR(300) NOT NULL,
      location VARCHAR(200),
      center VARCHAR(50),
      trainer VARCHAR(150),
      attendees JSONB DEFAULT '[]',
      notes TEXT,
      doc_id INTEGER REFERENCES documents(id) ON DELETE SET NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS corrective_actions (
      id SERIAL PRIMARY KEY,
      fiscal_year_id INTEGER REFERENCES fiscal_years(id),
      review_id INTEGER REFERENCES monitoring_reviews(id) ON DELETE SET NULL,
      center VARCHAR(50) NOT NULL,
      finding_item VARCHAR(20),
      finding_description TEXT NOT NULL,
      corrective_action TEXT,
      assigned_to VARCHAR(150),
      due_date DATE,
      status VARCHAR(30) DEFAULT 'open',
      resolved_date DATE,
      resolved_notes TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
    -- ── CHILD ROSTER ───────────────────────────────────────
    -- Auto-populated from attendance/meal uploads, one row per unique child per center.
    -- normalized_key = lowercase, trimmed, single-space, used for dedup.
    CREATE TABLE IF NOT EXISTS children (
      id SERIAL PRIMARY KEY,
      center VARCHAR(50) NOT NULL,
      child_first VARCHAR(120) NOT NULL,
      child_last VARCHAR(120) NOT NULL,
      normalized_key VARCHAR(300) NOT NULL,
      classroom VARCHAR(120),
      category VARCHAR(5),
      first_seen_month VARCHAR(10),
      last_seen_month VARCHAR(10),
      last_seen_date DATE,
      is_active BOOLEAN DEFAULT true,
      notes TEXT,
      metadata JSONB DEFAULT '{}',
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(center, normalized_key)
    );
    CREATE INDEX IF NOT EXISTS idx_children_last_seen ON children(last_seen_date);
    CREATE INDEX IF NOT EXISTS idx_children_active ON children(center, is_active);

    -- Per-child documents: HIES, medical exception, infant food sign-off.
    -- Multiple files can belong to the same logical document (child + doc_type + cacfp_year_label).
    -- Group them in queries by (child_id, doc_type, cacfp_year_label).
    CREATE TABLE IF NOT EXISTS child_documents (
      id SERIAL PRIMARY KEY,
      child_id INTEGER REFERENCES children(id) ON DELETE CASCADE,
      doc_type VARCHAR(50) NOT NULL,
      cacfp_year_label VARCHAR(20),
      signing_date DATE,
      approval_date DATE,
      annual_review_reminder_date DATE,
      filename VARCHAR(255) NOT NULL,
      mime_type VARCHAR(100),
      file_data BYTEA NOT NULL,
      page_count INTEGER DEFAULT 1,
      file_sort_order INTEGER DEFAULT 0,
      notes TEXT,
      metadata JSONB DEFAULT '{}',
      uploaded_at TIMESTAMP DEFAULT NOW(),
      uploaded_by VARCHAR(120)
    );
    CREATE INDEX IF NOT EXISTS idx_child_docs_child ON child_documents(child_id, doc_type);
    CREATE INDEX IF NOT EXISTS idx_child_docs_year ON child_documents(cacfp_year_label);

    -- Roster merge-request queue: holds near-duplicate child pairs awaiting manual review
    CREATE TABLE IF NOT EXISTS roster_merge_requests (
      id SERIAL PRIMARY KEY,
      proposed_child_id INTEGER REFERENCES children(id) ON DELETE CASCADE,
      existing_child_id INTEGER REFERENCES children(id) ON DELETE CASCADE,
      similarity_score NUMERIC(4,2),
      reason TEXT,
      status VARCHAR(20) DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT NOW(),
      resolved_at TIMESTAMP,
      resolved_by VARCHAR(120)
    );
    CREATE INDEX IF NOT EXISTS idx_merge_requests_status ON roster_merge_requests(status);
  `);
  console.log('✅ Monitoring tables ready');
}

// ── MONITORING API ───────────────────────────────────────
const CLASSROOMS = {
  niles: [
    {name:'Tiny Treasures',ages:'Infants'},{name:'Koalas',ages:'Toddlers'},{name:'Jellyfish',ages:'Toddlers'},
    {name:'Fireflies',ages:'3s'},{name:'Flamingos',ages:'Multi-age/School-age'},
    {name:'Honey Bees',ages:'4s and 5s'},{name:'Otters',ages:'4s and 5s'}
  ],
  peace: [
    {name:'Caterpillars',ages:'Infants'},{name:'Butterflies',ages:'Infants/Toddlers'},
    {name:'Dolphins',ages:'Toddlers'},{name:'Kangas',ages:'Toddlers'},{name:'Lions',ages:'Toddlers'},
    {name:'Montessori',ages:'Infants/Toddlers'},{name:'Bears',ages:'2½'},
    {name:'Flamingos',ages:'Multi-age 2½-4'},{name:'Penguins',ages:'4s and 5s'},
    {name:'Dinos',ages:'4s and 5s'},{name:'Tigers',ages:'2s and 3s'}
  ]
};

app.get('/api/classrooms/:center', authCheck, (req, res) => {
  res.json(CLASSROOMS[req.params.center] || []);
});

app.get('/api/monitoring', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, center } = req.query;
    let q = 'SELECT * FROM monitoring_reviews WHERE 1=1';
    const p = [];
    if (fiscal_year_id) { p.push(fiscal_year_id); q += ` AND fiscal_year_id=$${p.length}`; }
    if (center) { p.push(center); q += ` AND center=$${p.length}`; }
    q += ' ORDER BY review_date DESC';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/monitoring/:id', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM monitoring_reviews WHERE id=$1', [req.params.id]);
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/monitoring', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, center, review_date, announced, meal_observed, monitor_name } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO monitoring_reviews (fiscal_year_id, center, review_date, announced, meal_observed, monitor_name, form_data)
       VALUES ($1,$2,$3,$4,$5,$6,'{}') RETURNING *`,
      [fiscal_year_id, center, review_date, announced || false, meal_observed || '', monitor_name || '']
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/monitoring/:id', authCheck, async (req, res) => {
  try {
    const { form_data, findings, five_day_data, status, arrival_time, departure_time, announced, meal_observed, monitor_name, review_date } = req.body;
    const sets = []; const vals = []; let n = 0;
    if (form_data !== undefined) { n++; sets.push(`form_data=$${n}`); vals.push(JSON.stringify(form_data)); }
    if (findings !== undefined) { n++; sets.push(`findings=$${n}`); vals.push(JSON.stringify(findings)); }
    if (five_day_data !== undefined) { n++; sets.push(`five_day_data=$${n}`); vals.push(JSON.stringify(five_day_data)); }
    if (status) { n++; sets.push(`status=$${n}`); vals.push(status); }
    if (arrival_time) { n++; sets.push(`arrival_time=$${n}`); vals.push(arrival_time); }
    if (departure_time) { n++; sets.push(`departure_time=$${n}`); vals.push(departure_time); }
    if (announced !== undefined) { n++; sets.push(`announced=$${n}`); vals.push(announced); }
    if (meal_observed) { n++; sets.push(`meal_observed=$${n}`); vals.push(meal_observed); }
    if (monitor_name) { n++; sets.push(`monitor_name=$${n}`); vals.push(monitor_name); }
    if (review_date) { n++; sets.push(`review_date=$${n}`); vals.push(review_date); }
    sets.push('updated_at=NOW()');
    n++; vals.push(req.params.id);
    const { rows } = await pool.query(`UPDATE monitoring_reviews SET ${sets.join(',')} WHERE id=$${n} RETURNING *`, vals);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/monitoring/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM monitoring_reviews WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GENERATE MONITORING REVIEW DOCX ──────────────────────
app.post('/api/monitoring/:id/generate-docx', authCheck, async (req, res) => {
  try {
    const revRes = await pool.query('SELECT * FROM monitoring_reviews WHERE id=$1', [req.params.id]);
    const rev = revRes.rows[0];
    if (!rev) return res.status(404).json({ error: 'Review not found' });

    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [rev.fiscal_year_id]);
    const fy = fyRes.rows[0];

    // Pull cross-check flags attached to this review
    const ccRes = await pool.query(
      'SELECT * FROM crosscheck_resolutions WHERE attached_review_id=$1 ORDER BY flag_date, child_name',
      [rev.id]
    );

    const fd = rev.form_data || {};
    const findings = rev.findings || [];
    const fiveDay = rev.five_day_data || {};
    const centerLabel = rev.center === 'niles' ? 'Niles' : 'Peace Boulevard';
    const navy = '1B2A4A';

    const thinB = {
      top:{style:BorderStyle.SINGLE,size:1,color:'999999'},
      bottom:{style:BorderStyle.SINGLE,size:1,color:'999999'},
      left:{style:BorderStyle.SINGLE,size:1,color:'999999'},
      right:{style:BorderStyle.SINGLE,size:1,color:'999999'}
    };
    function cell(text, opts = {}) {
      return new TableCell({
        width: opts.w ? { size: opts.w, type: WidthType.PERCENTAGE } : undefined,
        borders: thinB,
        shading: opts.bg ? { type: ShadingType.SOLID, color: opts.bg } : undefined,
        children: [new Paragraph({
          alignment: opts.align || AlignmentType.LEFT,
          children: [new TextRun({
            text: text || '', bold: opts.bold || false, size: opts.sz || 18,
            font: 'Calibri', color: opts.color || '333333'
          })]
        })]
      });
    }
    function para(text, opts = {}) {
      return new Paragraph({
        spacing: { after: opts.after || 60, before: opts.before || 0 },
        alignment: opts.align || AlignmentType.LEFT,
        children: [new TextRun({
          text: text || '', bold: opts.bold || false, size: opts.sz || 20,
          font: 'Calibri', color: opts.color || '333333'
        })]
      });
    }
    function heading(text) {
      return new Paragraph({
        spacing: { before: 200, after: 100 },
        children: [new TextRun({ text, bold: true, size: 22, font: 'Calibri', color: navy })]
      });
    }

    // Build sections 100-1000 (MDE monitoring form sections)
    const sectionBlocks = [];
    const sectionTitles = {
      '100': '100 - Administrative Review',
      '200': '200 - Meal Pattern Review',
      '300': '300 - Menu & Production Records',
      '400': '400 - Meal Service Observation',
      '500': '500 - Enrollment & Income Eligibility',
      '600': '600 - Civil Rights',
      '700': '700 - Staff Training',
      '800': '800 - Food Safety & Sanitation',
      '900': '900 - Record Keeping',
      '1000': '1000 - Financial Review'
    };
    for (const secKey of Object.keys(sectionTitles)) {
      const secData = fd[secKey] || {};
      const items = Object.keys(secData).sort();
      if (!items.length) continue;

      const rows = [new TableRow({ children: [
        cell('Item', { bold: true, bg: navy, color: 'FFFFFF', w: 10, align: AlignmentType.CENTER }),
        cell('Response', { bold: true, bg: navy, color: 'FFFFFF', w: 15, align: AlignmentType.CENTER }),
        cell('Notes / Comments', { bold: true, bg: navy, color: 'FFFFFF', w: 75 })
      ]})];
      for (const itemKey of items) {
        const v = secData[itemKey] || {};
        const resp = v.response || '';
        const notes = v.notes || '';
        let respColor = '333333';
        if (resp === 'Y') respColor = '2E7D32';
        else if (resp === 'N') respColor = 'C0392B';
        else if (resp === 'N/A') respColor = '888888';
        rows.push(new TableRow({ children: [
          cell(itemKey, { bold: true, align: AlignmentType.CENTER, sz: 16 }),
          cell(resp, { bold: true, align: AlignmentType.CENTER, color: respColor, sz: 18 }),
          cell(notes, { sz: 16 })
        ]}));
      }
      sectionBlocks.push(heading(sectionTitles[secKey]));
      sectionBlocks.push(new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows }));
    }

    // Meal observation table (if provided)
    const mealObsBlocks = [];
    if (fd.mealObservation && Array.isArray(fd.mealObservation.classrooms) && fd.mealObservation.classrooms.length) {
      mealObsBlocks.push(heading('Meal Service Observation'));
      mealObsBlocks.push(para(`Meal observed: ${rev.meal_observed || 'Not specified'}`, { sz: 18, after: 100 }));
      const mo = fd.mealObservation;
      const obsRows = [new TableRow({ children: [
        cell('Classroom', { bold: true, bg: navy, color: 'FFFFFF', w: 20 }),
        cell('Ages', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
        cell('# Children', { bold: true, bg: navy, color: 'FFFFFF', w: 10, align: AlignmentType.CENTER }),
        cell('Pattern Met', { bold: true, bg: navy, color: 'FFFFFF', w: 12, align: AlignmentType.CENTER }),
        cell('Portions OK', { bold: true, bg: navy, color: 'FFFFFF', w: 12, align: AlignmentType.CENTER }),
        cell('Observations', { bold: true, bg: navy, color: 'FFFFFF', w: 34 })
      ]})];
      for (const c of mo.classrooms) {
        obsRows.push(new TableRow({ children: [
          cell(c.name || '', { sz: 16 }),
          cell(c.ages || '', { sz: 14 }),
          cell(String(c.count || ''), { align: AlignmentType.CENTER, sz: 16 }),
          cell(c.patternMet || '', { align: AlignmentType.CENTER, sz: 16, color: c.patternMet === 'Y' ? '2E7D32' : c.patternMet === 'N' ? 'C0392B' : '333333' }),
          cell(c.portionsOK || '', { align: AlignmentType.CENTER, sz: 16, color: c.portionsOK === 'Y' ? '2E7D32' : c.portionsOK === 'N' ? 'C0392B' : '333333' }),
          cell(c.notes || '', { sz: 14 })
        ]}));
      }
      mealObsBlocks.push(new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: obsRows }));
    }

    // Five-day reconciliation table
    const fiveDayBlocks = [];
    if (fiveDay && Array.isArray(fiveDay.days) && fiveDay.days.length) {
      fiveDayBlocks.push(heading('Five-Day Meal Count Reconciliation'));
      const fdRows = [new TableRow({ children: [
        cell('Date', { bold: true, bg: navy, color: 'FFFFFF', w: 15 }),
        cell('Meal', { bold: true, bg: navy, color: 'FFFFFF', w: 13 }),
        cell('Count POS', { bold: true, bg: navy, color: 'FFFFFF', w: 12, align: AlignmentType.CENTER }),
        cell('Count Claimed', { bold: true, bg: navy, color: 'FFFFFF', w: 12, align: AlignmentType.CENTER }),
        cell('Attendance', { bold: true, bg: navy, color: 'FFFFFF', w: 12, align: AlignmentType.CENTER }),
        cell('Discrepancy', { bold: true, bg: navy, color: 'FFFFFF', w: 13, align: AlignmentType.CENTER }),
        cell('Notes', { bold: true, bg: navy, color: 'FFFFFF', w: 23 })
      ]})];
      for (const d of fiveDay.days) {
        const meals = d.meals || {};
        for (const mk of ['breakfast','amSnack','lunch','pmSnack']) {
          const m = meals[mk];
          if (!m) continue;
          const discrepancy = (m.pos || 0) - (m.claimed || 0);
          fdRows.push(new TableRow({ children: [
            cell(d.date || '', { sz: 14 }),
            cell(mk === 'amSnack' ? 'AM Snack' : mk === 'pmSnack' ? 'PM Snack' : mk.charAt(0).toUpperCase()+mk.slice(1), { sz: 14 }),
            cell(String(m.pos || 0), { align: AlignmentType.CENTER, sz: 14 }),
            cell(String(m.claimed || 0), { align: AlignmentType.CENTER, sz: 14 }),
            cell(String(m.attendance || 0), { align: AlignmentType.CENTER, sz: 14 }),
            cell(discrepancy === 0 ? '—' : String(discrepancy), { align: AlignmentType.CENTER, sz: 14, bold: discrepancy !== 0, color: discrepancy !== 0 ? 'C0392B' : '333333' }),
            cell(m.notes || '', { sz: 12 })
          ]}));
        }
      }
      fiveDayBlocks.push(new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: fdRows }));
    }

    // Cross-check flag findings (attached to this review)
    const ccBlocks = [];
    if (ccRes.rows.length) {
      ccBlocks.push(heading('Cross-Check Discrepancies (Meal Window Validation)'));
      ccBlocks.push(para('The following discrepancies were identified during monthly meal-window cross-checks and added to this review:', { sz: 16, after: 100 }));
      const ccRows = [new TableRow({ children: [
        cell('Date', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
        cell('Child', { bold: true, bg: navy, color: 'FFFFFF', w: 22 }),
        cell('Meal', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
        cell('Check-In', { bold: true, bg: navy, color: 'FFFFFF', w: 12, align: AlignmentType.CENTER }),
        cell('Window', { bold: true, bg: navy, color: 'FFFFFF', w: 12, align: AlignmentType.CENTER }),
        cell('Monitor Notes', { bold: true, bg: navy, color: 'FFFFFF', w: 30 })
      ]})];
      for (const r of ccRes.rows) {
        const d = r.flag_date instanceof Date ? r.flag_date.toLocaleDateString('en-US') : String(r.flag_date);
        ccRows.push(new TableRow({ children: [
          cell(d, { sz: 14 }),
          cell(r.child_name || '', { sz: 14 }),
          cell(r.meal_type || '', { sz: 14 }),
          cell(r.check_in || '', { align: AlignmentType.CENTER, sz: 14 }),
          cell(`by ${r.window_end || ''}`, { align: AlignmentType.CENTER, sz: 14 }),
          cell(r.resolution_notes || '', { sz: 12 })
        ]}));
      }
      ccBlocks.push(new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: ccRows }));
    }

    // Findings table (general)
    const findingsBlocks = [];
    if (findings.length) {
      findingsBlocks.push(heading('Findings Summary'));
      const fRows = [new TableRow({ children: [
        cell('Item', { bold: true, bg: navy, color: 'FFFFFF', w: 10 }),
        cell('Description', { bold: true, bg: navy, color: 'FFFFFF', w: 50 }),
        cell('Corrective Action', { bold: true, bg: navy, color: 'FFFFFF', w: 25 }),
        cell('Due Date', { bold: true, bg: navy, color: 'FFFFFF', w: 15, align: AlignmentType.CENTER })
      ]})];
      for (const f of findings) {
        fRows.push(new TableRow({ children: [
          cell(f.item || '', { sz: 14 }),
          cell(f.description || '', { sz: 14 }),
          cell(f.correctiveAction || '', { sz: 14 }),
          cell(f.dueDate || '', { align: AlignmentType.CENTER, sz: 14 })
        ]}));
      }
      findingsBlocks.push(new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: fRows }));
    }

    // ═══════════════════════════════════════════════════════════
    // FAMILY RECORDS APPENDIX
    // Children who attended this center in the 3 months prior to
    // review_date, with their HIES / medical / infant sign-off docs.
    // ═══════════════════════════════════════════════════════════
    const appendixBlocks = [];
    try {
      if (rev.review_date) {
        const reviewDate = new Date(rev.review_date);
        const windowStart = new Date(reviewDate);
        windowStart.setMonth(windowStart.getMonth() - 3);

        // Collect month_keys within the 3-month window.
        // Include the review_date month AND the two preceding months.
        // E.g. review 2026-06-15 → window May/Apr/Mar; we pull mar/apr/may/jun attendance.
        const windowMonthKeys = [];
        for (let i = 0; i < 4; i++) {
          const d = new Date(reviewDate);
          d.setMonth(d.getMonth() - i);
          const mIdx = d.getMonth(); // 0=Jan
          const mkArr = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
          windowMonthKeys.push(mkArr[mIdx]);
        }

        // Query monthly_data attendance within the review's fiscal year
        const attRes = await pool.query(
          `SELECT month_key, data FROM monthly_data
           WHERE fiscal_year_id = $1 AND data_type = 'attendance' AND month_key = ANY($2)`,
          [rev.fiscal_year_id, windowMonthKeys]
        );

        // Collect attended child names for this center
        const attendedNames = new Set(); // normalized_key
        function normForMatch(first, last) {
          const clean = s => (s || '').toString().toLowerCase().trim().replace(/\s+/g,' ').replace(/[^a-z0-9' -]/g,'');
          return `${clean(last)}|${clean(first)}`;
        }

        for (const row of attRes.rows) {
          const centerData = (row.data || {})[rev.center];
          if (!centerData || !centerData.childData) continue;
          for (const ch of centerData.childData) {
            const raw = (ch.name || '').trim();
            if (!raw) continue;
            let first, last;
            if (raw.includes(',')) {
              const p = raw.split(',').map(s => s.trim());
              last = p[0]; first = p[1];
            } else {
              const parts = raw.split(/\s+/);
              first = parts[0]; last = parts.slice(1).join(' ');
            }
            if (!first || !last) continue;
            attendedNames.add(normForMatch(first, last));
          }
        }

        if (attendedNames.size > 0) {
          // Load matching children from roster
          const childRes = await pool.query(
            `SELECT * FROM children WHERE center = $1 AND normalized_key = ANY($2) ORDER BY child_last, child_first`,
            [rev.center, [...attendedNames]]
          );
          const attendedChildren = childRes.rows;

          // Load all their documents at once
          const childIds = attendedChildren.map(c => c.id);
          let allDocs = [];
          if (childIds.length) {
            const { rows } = await pool.query(
              `SELECT * FROM child_documents WHERE child_id = ANY($1) ORDER BY child_id, doc_type, cacfp_year_label DESC NULLS LAST, file_sort_order`,
              [childIds]
            );
            allDocs = rows;
          }

          // Group docs by (child_id, doc_type)
          const docsByChild = new Map(); // child_id → {hies:[], medical_exception:[], infant_food_signoff:[]}
          for (const d of allDocs) {
            if (!docsByChild.has(d.child_id)) docsByChild.set(d.child_id, { hies: [], medical_exception: [], infant_food_signoff: [] });
            const bucket = docsByChild.get(d.child_id);
            if (bucket[d.doc_type]) bucket[d.doc_type].push(d);
          }

          // Determine current CACFP year label from review date
          const rd = new Date(rev.review_date);
          const rdY = rd.getFullYear(), rdM = rd.getMonth();
          const currentCacfpYear = rdM >= 9 ? `${rdY}-${rdY+1}` : `${rdY-1}-${rdY}`;

          const INFANT_CLASSROOMS_SET = new Set(['Tiny Treasures','Koalas','Montessori Infants','Butterflies','Caterpillars']);

          // Build three groups of (child, docs-to-embed)
          const hiesGroup = []; // [{child, files:[docs]}]
          const medicalGroup = [];
          const infantGroup = [];

          for (const child of attendedChildren) {
            const bucket = docsByChild.get(child.id) || { hies: [], medical_exception: [], infant_food_signoff: [] };

            // HIES: only for Cat A or B, latest CACFP year group
            if (child.category === 'A' || child.category === 'B') {
              const yearDocs = bucket.hies.filter(d => d.cacfp_year_label === currentCacfpYear);
              if (yearDocs.length) hiesGroup.push({ child, files: yearDocs });
            }

            // Medical: any exists → include (latest group)
            if (bucket.medical_exception.length) {
              medicalGroup.push({ child, files: bucket.medical_exception });
            }

            // Infant sign-off: child currently in an infant classroom
            if (child.classroom && INFANT_CLASSROOMS_SET.has(child.classroom) && bucket.infant_food_signoff.length) {
              infantGroup.push({ child, files: bucket.infant_food_signoff });
            }
          }

          // Helper: rasterize a doc's PDF/image into PNG buffers for ImageRun
          async function docToImageBuffers(doc) {
            const mt = (doc.mime_type || '').toLowerCase();
            const fn = (doc.filename || '').toLowerCase();
            const buffers = [];
            if (mt === 'application/pdf' || fn.endsWith('.pdf')) {
              try {
                const { pdf } = await import('pdf-to-img');
                const pages = await pdf(doc.file_data, { scale: 2 });
                for await (const pageBuf of pages) buffers.push(pageBuf);
              } catch (e) {
                console.warn(`rasterize failed for ${doc.filename}: ${e.message}`);
              }
            } else if (mt.startsWith('image/')) {
              buffers.push(Buffer.from(doc.file_data));
            }
            return buffers;
          }

          // Build the appendix only if any group has entries
          if (hiesGroup.length || medicalGroup.length || infantGroup.length) {
            // Appendix cover
            appendixBlocks.push(new Paragraph({
              children: [new PageBreak()]
            }));
            appendixBlocks.push(new Paragraph({
              alignment: AlignmentType.CENTER, spacing: { before: 200, after: 120 },
              children: [new TextRun({ text: 'APPENDIX — Family Records', bold: true, size: 32, font: 'Calibri', color: navy })]
            }));
            appendixBlocks.push(new Paragraph({
              alignment: AlignmentType.CENTER, spacing: { after: 80 },
              children: [new TextRun({ text: `${centerLabel} | Review Date ${dateStr}`, size: 20, font: 'Calibri', color: '666666' })]
            }));
            appendixBlocks.push(new Paragraph({
              alignment: AlignmentType.CENTER, spacing: { after: 300 },
              children: [new TextRun({ text: `Includes children who attended in the 3 months prior to the review.`, size: 18, font: 'Calibri', color: '999999', italics: true })]
            }));
            // Appendix TOC
            appendixBlocks.push(new Paragraph({
              spacing: { before: 100, after: 60 },
              children: [new TextRun({ text: 'Contents', bold: true, size: 22, font: 'Calibri', color: navy })]
            }));
            appendixBlocks.push(new Paragraph({
              spacing: { after: 40 },
              children: [new TextRun({ text: `A. Household Income Eligibility Statements — ${hiesGroup.length} child${hiesGroup.length===1?'':'ren'} (Cat A/B only)`, size: 18, font: 'Calibri' })]
            }));
            appendixBlocks.push(new Paragraph({
              spacing: { after: 40 },
              children: [new TextRun({ text: `B. Medical Exception / Physician Statements — ${medicalGroup.length} child${medicalGroup.length===1?'':'ren'}`, size: 18, font: 'Calibri' })]
            }));
            appendixBlocks.push(new Paragraph({
              spacing: { after: 200 },
              children: [new TextRun({ text: `C. Infant Food Sign-Off Forms — ${infantGroup.length} child${infantGroup.length===1?'':'ren'} (currently in infant classrooms)`, size: 18, font: 'Calibri' })]
            }));

            // Helper: render one section (title + entries)
            async function renderSection(sectionLetter, sectionTitle, group) {
              appendixBlocks.push(new Paragraph({ children: [new PageBreak()] }));
              appendixBlocks.push(new Paragraph({
                spacing: { before: 100, after: 80 },
                children: [new TextRun({ text: `${sectionLetter}. ${sectionTitle}`, bold: true, size: 26, font: 'Calibri', color: navy })]
              }));
              if (!group.length) {
                appendixBlocks.push(new Paragraph({
                  spacing: { after: 120 },
                  children: [new TextRun({ text: 'No matching records for this section.', size: 18, font: 'Calibri', color: '999999', italics: true })]
                }));
                return;
              }
              for (const entry of group) {
                const { child, files } = entry;
                appendixBlocks.push(new Paragraph({
                  spacing: { before: 200, after: 40 },
                  children: [new TextRun({
                    text: `${child.child_last}, ${child.child_first}`,
                    bold: true, size: 22, font: 'Calibri', color: navy
                  })]
                }));
                const metaParts = [];
                if (child.classroom) metaParts.push(child.classroom);
                if (child.category) metaParts.push(`Category ${child.category}`);
                if (metaParts.length) {
                  appendixBlocks.push(new Paragraph({
                    spacing: { after: 40 },
                    children: [new TextRun({ text: metaParts.join(' · '), size: 16, font: 'Calibri', color: '666666' })]
                  }));
                }
                // Render each file's pages
                for (const f of files) {
                  const imgBuffers = await docToImageBuffers(f);
                  if (imgBuffers.length === 0) {
                    appendixBlocks.push(new Paragraph({
                      spacing: { after: 40 },
                      children: [new TextRun({ text: `⚠️ ${f.filename} could not be embedded (unsupported format).`, size: 16, font: 'Calibri', color: 'C0392B', italics: true })]
                    }));
                    continue;
                  }
                  for (let pi = 0; pi < imgBuffers.length; pi++) {
                    appendixBlocks.push(new Paragraph({
                      alignment: AlignmentType.CENTER,
                      spacing: { before: 80, after: 40 },
                      children: [new ImageRun({
                        data: imgBuffers[pi],
                        transformation: { width: 540, height: 700 }
                      })]
                    }));
                    if (imgBuffers.length > 1) {
                      appendixBlocks.push(new Paragraph({
                        alignment: AlignmentType.CENTER, spacing: { after: 80 },
                        children: [new TextRun({ text: `${f.filename} — page ${pi+1} of ${imgBuffers.length}`, size: 12, font: 'Calibri', color: '999999' })]
                      }));
                    }
                  }
                }
              }
            }

            await renderSection('A', 'Household Income Eligibility Statements (HIES)', hiesGroup);
            await renderSection('B', 'Medical Exception / Physician Statements', medicalGroup);
            await renderSection('C', 'Infant Food Sign-Off Forms', infantGroup);
          }
        }
      }
    } catch (appendixErr) {
      console.warn('Family records appendix error:', appendixErr);
      // Don't fail the whole doc — appendix is best-effort
    }

    // Assemble document
    const dateStr = rev.review_date
      ? new Date(rev.review_date).toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })
      : '[Not set]';

    const doc = new Document({
      sections: [{
        properties: { page: { margin: { top: 720, bottom: 720, left: 720, right: 720 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
            new TextRun({ text: "The Children's Center, Inc.", bold: true, size: 28, font: 'Calibri', color: navy })
          ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
            new TextRun({ text: `CACFP Sponsor Self-Monitoring Review`, size: 24, font: 'Calibri', color: '666666' })
          ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 }, children: [
            new TextRun({ text: `${centerLabel} | ${dateStr}`, size: 20, font: 'Calibri', color: '999999' })
          ]}),
          para(`Sponsor: The Children's Center, Inc. (#990004457)`, { sz: 18 }),
          para(`Center: ${centerLabel}`, { sz: 18 }),
          para(`Review Type: ${rev.announced ? 'Announced' : 'Unannounced'}`, { sz: 18 }),
          para(`Monitor: ${rev.monitor_name || '[Not specified]'}`, { sz: 18 }),
          para(`Meal Observed: ${rev.meal_observed || '[Not specified]'}`, { sz: 18 }),
          para(`Arrival: ${rev.arrival_time || '[N/A]'} | Departure: ${rev.departure_time || '[N/A]'}`, { sz: 18 }),
          para(`Fiscal Year: ${fy?.label || ''}`, { sz: 18, after: 200 }),

          ...mealObsBlocks,
          ...sectionBlocks,
          ...fiveDayBlocks,
          ...ccBlocks,
          ...findingsBlocks,

          new Paragraph({ spacing: { before: 400, after: 100 }, children: [
            new TextRun({ text: 'Signatures', bold: true, size: 22, font: 'Calibri', color: navy })
          ]}),
          para('Monitor Signature: _________________________________    Date: ____________', { sz: 18, after: 200 }),
          para('Center Director Signature: _________________________________    Date: ____________', { sz: 18, after: 200 }),
          para('Sponsor Executive Director Signature: _________________________________    Date: ____________', { sz: 18, after: 200 }),

          new Paragraph({ spacing: { before: 300 }, alignment: AlignmentType.RIGHT, children: [
            new TextRun({ text: `Generated ${new Date().toLocaleDateString('en-US')} | Review ID: ${rev.id}`, size: 14, font: 'Calibri', color: '999999' })
          ]}),

          ...appendixBlocks
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    const filename = `Monitoring_${rev.center}_${(rev.review_date || 'draft').toString().slice(0,10)}.docx`;

    await pool.query(
      `INSERT INTO documents (fiscal_year_id, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,'monitoring_review',$2,'application/vnd.openxmlformats-officedocument.wordprocessingml.document',$3,$4)`,
      [rev.fiscal_year_id, filename, buffer, JSON.stringify({ review_id: rev.id, center: rev.center })]
    );

    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// ── MONITORING PREFILL (pull data from DB to pre-populate form) ──
app.get('/api/monitoring/:id/prefill', authCheck, async (req, res) => {
  try {
    const revRes = await pool.query('SELECT * FROM monitoring_reviews WHERE id=$1', [req.params.id]);
    const rev = revRes.rows[0];
    if (!rev) return res.status(404).json({ error: 'Review not found' });

    // Pull classroom roster for this center
    const classrooms = CLASSROOMS[rev.center] || [];

    // Pull most recent attendance for center (last available month)
    const attRes = await pool.query(
      `SELECT month_key, data FROM monthly_data
       WHERE fiscal_year_id=$1 AND data_type='attendance'
       ORDER BY updated_at DESC LIMIT 1`,
      [rev.fiscal_year_id]
    );
    const latestAtt = attRes.rows[0] || null;
    const centerAtt = latestAtt?.data?.[rev.center] || null;

    // Pull any open corrective actions
    const caRes = await pool.query(
      `SELECT * FROM corrective_actions
       WHERE fiscal_year_id=$1 AND center=$2 AND status='open'
       ORDER BY due_date ASC NULLS LAST`,
      [rev.fiscal_year_id, rev.center]
    );

    // Pull training records from this fiscal year
    const trRes = await pool.query(
      `SELECT * FROM training_records WHERE fiscal_year_id=$1 ORDER BY training_date DESC`,
      [rev.fiscal_year_id]
    );

    // Pull cross-check flags already attached to this review
    const ccRes = await pool.query(
      'SELECT * FROM crosscheck_resolutions WHERE attached_review_id=$1 ORDER BY flag_date',
      [rev.id]
    );

    res.json({
      review: rev,
      classrooms,
      latestAttendance: centerAtt ? { month: latestAtt.month_key, enrolled: centerAtt.enrolled, ada: centerAtt.ada } : null,
      openCorrectiveActions: caRes.rows,
      trainings: trRes.rows,
      crossCheckFlags: ccRes.rows
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── ADULT MEAL ENTRIES ────────────────────────────────────
app.get('/api/adult-meals', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.query;
    let q = `SELECT d.*, s.name FROM daily_cacfp_entries d
             LEFT JOIN staff s ON s.id = d.staff_id
             WHERE d.adult_meal = true`;
    const p = [];
    if (fiscal_year_id) { p.push(fiscal_year_id); q += ` AND d.fiscal_year_id=$${p.length}`; }
    if (month_key) { p.push(month_key); q += ` AND d.month_key=$${p.length}`; }
    q += ' ORDER BY d.day_of_month, s.name';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── CORRECTIVE ACTIONS ────────────────────────────────────
app.get('/api/corrective-actions', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, center, status, review_id } = req.query;
    let q = 'SELECT * FROM corrective_actions WHERE 1=1';
    const p = [];
    if (fiscal_year_id) { p.push(fiscal_year_id); q += ` AND fiscal_year_id=$${p.length}`; }
    if (center) { p.push(center); q += ` AND center=$${p.length}`; }
    if (status) { p.push(status); q += ` AND status=$${p.length}`; }
    if (review_id) { p.push(review_id); q += ` AND review_id=$${p.length}`; }
    q += ' ORDER BY due_date ASC NULLS LAST, created_at DESC';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/corrective-actions', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, review_id, center, finding_item, finding_description,
            corrective_action, assigned_to, due_date } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO corrective_actions (fiscal_year_id, review_id, center, finding_item,
          finding_description, corrective_action, assigned_to, due_date)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [fiscal_year_id, review_id || null, center, finding_item || '',
       finding_description, corrective_action || '', assigned_to || '', due_date || null]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/corrective-actions/:id', authCheck, async (req, res) => {
  try {
    const { corrective_action, assigned_to, due_date, status, resolved_date, resolved_notes } = req.body;
    const sets = []; const vals = []; let n = 0;
    if (corrective_action !== undefined) { n++; sets.push(`corrective_action=$${n}`); vals.push(corrective_action); }
    if (assigned_to !== undefined) { n++; sets.push(`assigned_to=$${n}`); vals.push(assigned_to); }
    if (due_date !== undefined) { n++; sets.push(`due_date=$${n}`); vals.push(due_date); }
    if (status !== undefined) { n++; sets.push(`status=$${n}`); vals.push(status); }
    if (resolved_date !== undefined) { n++; sets.push(`resolved_date=$${n}`); vals.push(resolved_date); }
    if (resolved_notes !== undefined) { n++; sets.push(`resolved_notes=$${n}`); vals.push(resolved_notes); }
    sets.push('updated_at=NOW()');
    n++; vals.push(req.params.id);
    const { rows } = await pool.query(
      `UPDATE corrective_actions SET ${sets.join(',')} WHERE id=$${n} RETURNING *`, vals);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/corrective-actions/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM corrective_actions WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── MONITORING SCHEDULE ───────────────────────────────────
app.get('/api/monitoring-schedule', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, center } = req.query;
    let q = 'SELECT * FROM monitoring_schedule WHERE 1=1';
    const p = [];
    if (fiscal_year_id) { p.push(fiscal_year_id); q += ` AND fiscal_year_id=$${p.length}`; }
    if (center) { p.push(center); q += ` AND center=$${p.length}`; }
    q += ' ORDER BY planned_date';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/monitoring-schedule', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, center, planned_date, announced, includes_meal_obs, notes } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO monitoring_schedule (fiscal_year_id, center, planned_date, announced, includes_meal_obs, notes)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [fiscal_year_id, center, planned_date, !!announced, !!includes_meal_obs, notes || '']
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/monitoring-schedule/:id', authCheck, async (req, res) => {
  try {
    const { planned_date, announced, includes_meal_obs, notes, review_id } = req.body;
    const sets = []; const vals = []; let n = 0;
    if (planned_date !== undefined) { n++; sets.push(`planned_date=$${n}`); vals.push(planned_date); }
    if (announced !== undefined) { n++; sets.push(`announced=$${n}`); vals.push(announced); }
    if (includes_meal_obs !== undefined) { n++; sets.push(`includes_meal_obs=$${n}`); vals.push(includes_meal_obs); }
    if (notes !== undefined) { n++; sets.push(`notes=$${n}`); vals.push(notes); }
    if (review_id !== undefined) { n++; sets.push(`review_id=$${n}`); vals.push(review_id); }
    n++; vals.push(req.params.id);
    const { rows } = await pool.query(
      `UPDATE monitoring_schedule SET ${sets.join(',')} WHERE id=$${n} RETURNING *`, vals);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/monitoring-schedule/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM monitoring_schedule WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── TRAINING RECORDS ──────────────────────────────────────
app.get('/api/training', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, training_type } = req.query;
    let q = 'SELECT * FROM training_records WHERE 1=1';
    const p = [];
    if (fiscal_year_id) { p.push(fiscal_year_id); q += ` AND fiscal_year_id=$${p.length}`; }
    if (training_type) { p.push(training_type); q += ` AND training_type=$${p.length}`; }
    q += ' ORDER BY training_date DESC';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/training', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, training_date, training_type, topic, location,
            center, trainer, attendees, notes, doc_id } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO training_records (fiscal_year_id, training_date, training_type, topic,
          location, center, trainer, attendees, notes, doc_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [fiscal_year_id, training_date, training_type, topic, location || '',
       center || '', trainer || '', JSON.stringify(attendees || []), notes || '', doc_id || null]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/training/:id', authCheck, async (req, res) => {
  try {
    const { training_date, training_type, topic, location, center, trainer, attendees, notes } = req.body;
    const sets = []; const vals = []; let n = 0;
    if (training_date !== undefined) { n++; sets.push(`training_date=$${n}`); vals.push(training_date); }
    if (training_type !== undefined) { n++; sets.push(`training_type=$${n}`); vals.push(training_type); }
    if (topic !== undefined) { n++; sets.push(`topic=$${n}`); vals.push(topic); }
    if (location !== undefined) { n++; sets.push(`location=$${n}`); vals.push(location); }
    if (center !== undefined) { n++; sets.push(`center=$${n}`); vals.push(center); }
    if (trainer !== undefined) { n++; sets.push(`trainer=$${n}`); vals.push(trainer); }
    if (attendees !== undefined) { n++; sets.push(`attendees=$${n}`); vals.push(JSON.stringify(attendees)); }
    if (notes !== undefined) { n++; sets.push(`notes=$${n}`); vals.push(notes); }
    n++; vals.push(req.params.id);
    const { rows } = await pool.query(
      `UPDATE training_records SET ${sets.join(',')} WHERE id=$${n} RETURNING *`, vals);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/training/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM training_records WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GENERATE TRAINING SUMMARY REPORT (.docx) ─────────────
app.post('/api/generate-training-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
    const fy = fyRes.rows[0];
    if (!fy) return res.status(404).json({ error: 'FY not found' });

    const { rows } = await pool.query(
      `SELECT * FROM training_records WHERE fiscal_year_id=$1 ORDER BY training_date DESC`,
      [fiscal_year_id]
    );

    const navy = '1B2A4A';
    const thinB = {
      top:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'},
      bottom:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'},
      left:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'},
      right:{style:BorderStyle.SINGLE,size:1,color:'AAAAAA'}
    };
    function cell(text, opts = {}) {
      return new TableCell({
        width: opts.w ? { size: opts.w, type: WidthType.PERCENTAGE } : undefined,
        borders: thinB,
        shading: opts.bg ? { type: ShadingType.SOLID, color: opts.bg } : undefined,
        children: [new Paragraph({
          alignment: opts.align || AlignmentType.LEFT,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 16, font: 'Calibri', color: opts.color || '333333' })]
        })]
      });
    }

    const hdr = new TableRow({ children: [
      cell('Date', { bold: true, bg: navy, color: 'FFFFFF', w: 12 }),
      cell('Type', { bold: true, bg: navy, color: 'FFFFFF', w: 13 }),
      cell('Topic', { bold: true, bg: navy, color: 'FFFFFF', w: 30 }),
      cell('Trainer', { bold: true, bg: navy, color: 'FFFFFF', w: 15 }),
      cell('Attendees', { bold: true, bg: navy, color: 'FFFFFF', w: 20, align: AlignmentType.CENTER }),
      cell('Location', { bold: true, bg: navy, color: 'FFFFFF', w: 10 })
    ]});
    const dataRows = rows.map(r => {
      const att = Array.isArray(r.attendees) ? r.attendees : [];
      const attStr = att.length ? `${att.length} staff` : '—';
      const d = r.training_date ? new Date(r.training_date).toLocaleDateString('en-US') : '';
      return new TableRow({ children: [
        cell(d, { sz: 14 }),
        cell(r.training_type || '', { sz: 14 }),
        cell(r.topic || '', { sz: 14 }),
        cell(r.trainer || '', { sz: 14 }),
        cell(attStr, { align: AlignmentType.CENTER, sz: 14 }),
        cell(r.location || '', { sz: 12 })
      ]});
    });

    const doc = new Document({
      sections: [{
        properties: { page: { margin: { top: 720, bottom: 720, left: 720, right: 720 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
            new TextRun({ text: "The Children's Center, Inc.", bold: true, size: 28, font: 'Calibri', color: navy })
          ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
            new TextRun({ text: `CACFP Training Records — FY ${fy.label}`, size: 22, font: 'Calibri', color: '666666' })
          ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 }, children: [
            new TextRun({ text: `Sponsor #990004457 | Total Training Events: ${rows.length}`, size: 16, font: 'Calibri', color: '999999' })
          ]}),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows: [hdr, ...dataRows] }),
          new Paragraph({ spacing: { before: 200 }, alignment: AlignmentType.RIGHT, children: [
            new TextRun({ text: `Generated ${new Date().toLocaleDateString('en-US')}`, size: 12, font: 'Calibri', color: '999999' })
          ]})
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    res.setHeader('Content-Disposition', `attachment; filename="Training_Report_${fy.label}.docx"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// ── CACFP MEAL PATTERNS ──────────────────────────────────
const MEAL_PATTERNS = {
  '1-2': {
    breakfast: { milk: '4 fl oz', grain: '½ oz eq', fruit: '¼ cup', vegetable: '¼ cup' },
    amSnack:   { milk: '4 fl oz', grain: '½ oz eq', fruit: '½ cup', vegetable: '½ cup', meat: '½ oz eq' },
    lunch:     { milk: '4 fl oz', grain: '½ oz eq', fruit: '⅛ cup', vegetable: '⅛ cup', meat: '1 oz eq' },
    pmSnack:   { milk: '4 fl oz', grain: '½ oz eq', fruit: '½ cup', vegetable: '½ cup', meat: '½ oz eq' }
  },
  '3-5': {
    breakfast: { milk: '6 fl oz', grain: '½ oz eq', fruit: '½ cup', vegetable: '½ cup' },
    amSnack:   { milk: '4 fl oz', grain: '½ oz eq', fruit: '½ cup', vegetable: '½ cup', meat: '½ oz eq' },
    lunch:     { milk: '6 fl oz', grain: '½ oz eq', fruit: '¼ cup', vegetable: '¼ cup', meat: '1½ oz eq' },
    pmSnack:   { milk: '4 fl oz', grain: '½ oz eq', fruit: '½ cup', vegetable: '½ cup', meat: '½ oz eq' }
  },
  '6-12': {
    breakfast: { milk: '8 fl oz', grain: '1 oz eq', fruit: '½ cup', vegetable: '½ cup' },
    amSnack:   { milk: '8 fl oz', grain: '1 oz eq', fruit: '¾ cup', vegetable: '¾ cup', meat: '1 oz eq' },
    lunch:     { milk: '8 fl oz', grain: '1 oz eq', fruit: '¼ cup', vegetable: '½ cup', meat: '2 oz eq' },
    pmSnack:   { milk: '8 fl oz', grain: '1 oz eq', fruit: '¾ cup', vegetable: '¾ cup', meat: '1 oz eq' }
  },
  'Infants-6-11mo': {
    breakfast: { formula: '6-8 fl oz', grain: '0-½ oz eq', fruit: '0-¼ cup', vegetable: '0-¼ cup' },
    amSnack:   { formula: '2-4 fl oz', grain: '0-½ oz eq serving' },
    lunch:     { formula: '6-8 fl oz', grain: '0-½ oz eq', fruit: '0-¼ cup', vegetable: '0-¼ cup', meat: '0-4 oz' },
    pmSnack:   { formula: '2-4 fl oz', grain: '0-½ oz eq serving' }
  }
};
const CLASSROOM_AGE_MAP = {
  'Tiny Treasures': 'Infants-6-11mo', 'Caterpillars': 'Infants-6-11mo',
  'Butterflies': 'Infants-6-11mo', 'Koalas': '1-2', 'Jellyfish': '1-2',
  'Dolphins': '1-2', 'Kangas': '1-2', 'Lions': '1-2', 'Montessori': '1-2',
  'Tigers': '1-2', 'Bears': '3-5', 'Fireflies': '3-5', 'Flamingos': '3-5',
  'Honey Bees': '3-5', 'Otters': '3-5', 'Penguins': '3-5', 'Dinos': '3-5'
};

app.get('/api/meal-patterns/:ages', authCheck, (req, res) => {
  const p = MEAL_PATTERNS[req.params.ages];
  if (!p) return res.status(404).json({ error: 'Unknown age group' });
  res.json(p);
});

app.get('/api/meal-patterns', authCheck, (req, res) => {
  res.json({ patterns: MEAL_PATTERNS, classroomAgeMap: CLASSROOM_AGE_MAP });
});

// ── GENERATE PORTION POSTER FOR A CLASSROOM (.docx) ──────
app.post('/api/generate-portion-poster', authCheck, async (req, res) => {
  try {
    const { classroom, ages } = req.body;
    const ageKey = ages || CLASSROOM_AGE_MAP[classroom] || '3-5';
    const pat = MEAL_PATTERNS[ageKey];
    if (!pat) return res.status(400).json({ error: 'Unknown age group' });

    const navy = '1B2A4A';
    const thinB = {
      top:{style:BorderStyle.SINGLE,size:2,color:'1B2A4A'},
      bottom:{style:BorderStyle.SINGLE,size:2,color:'1B2A4A'},
      left:{style:BorderStyle.SINGLE,size:2,color:'1B2A4A'},
      right:{style:BorderStyle.SINGLE,size:2,color:'1B2A4A'}
    };
    function cell(text, opts = {}) {
      return new TableCell({
        borders: thinB,
        shading: opts.bg ? { type: ShadingType.SOLID, color: opts.bg } : undefined,
        children: [new Paragraph({
          alignment: opts.align || AlignmentType.CENTER,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 28, font: 'Calibri', color: opts.color || '333333' })]
        })]
      });
    }

    const mealLabels = { breakfast: 'Breakfast', amSnack: 'AM Snack', lunch: 'Lunch', pmSnack: 'PM Snack' };
    const components = ['milk','formula','grain','fruit','vegetable','meat'];

    const headerRow = new TableRow({ children: [
      cell('Component', { bold: true, bg: navy, color: 'FFFFFF', sz: 24 }),
      ...Object.keys(mealLabels).map(mk =>
        cell(mealLabels[mk], { bold: true, bg: navy, color: 'FFFFFF', sz: 24 })
      )
    ]});

    const rows = [headerRow];
    for (const comp of components) {
      const hasAny = Object.keys(mealLabels).some(mk => pat[mk]?.[comp]);
      if (!hasAny) continue;
      rows.push(new TableRow({ children: [
        cell(comp.charAt(0).toUpperCase() + comp.slice(1), { bold: true, bg: 'F0F4F8', sz: 22 }),
        ...Object.keys(mealLabels).map(mk => cell(pat[mk]?.[comp] || '—', { sz: 24 }))
      ]}));
    }

    const doc = new Document({
      sections: [{
        properties: { page: {
          margin: { top: 720, bottom: 720, left: 720, right: 720 },
          size: { orientation: 'landscape', width: 15840, height: 12240 }
        } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
            new TextRun({ text: "The Children's Center — CACFP Portion Guide", bold: true, size: 36, font: 'Calibri', color: navy })
          ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
            new TextRun({ text: classroom || '', bold: true, size: 44, font: 'Calibri', color: '333333' })
          ]}),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 300 }, children: [
            new TextRun({ text: `Age Group: ${ageKey}`, size: 28, font: 'Calibri', color: '666666' })
          ]}),
          new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows }),
          new Paragraph({ spacing: { before: 400 }, alignment: AlignmentType.CENTER, children: [
            new TextRun({ text: 'Per USDA CACFP Meal Pattern Requirements', size: 18, font: 'Calibri', color: '999999', italics: true })
          ]})
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    res.setHeader('Content-Disposition', `attachment; filename="Portion_Poster_${(classroom || 'Classroom').replace(/\s/g,'_')}.docx"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// ── GENERATE ALL PORTION POSTERS (for both centers) ──────
app.post('/api/generate-all-posters', authCheck, async (req, res) => {
  try {
    const navy = '1B2A4A';
    const thinB = {
      top:{style:BorderStyle.SINGLE,size:2,color:'1B2A4A'},
      bottom:{style:BorderStyle.SINGLE,size:2,color:'1B2A4A'},
      left:{style:BorderStyle.SINGLE,size:2,color:'1B2A4A'},
      right:{style:BorderStyle.SINGLE,size:2,color:'1B2A4A'}
    };
    function cell(text, opts = {}) {
      return new TableCell({
        borders: thinB,
        shading: opts.bg ? { type: ShadingType.SOLID, color: opts.bg } : undefined,
        children: [new Paragraph({
          alignment: opts.align || AlignmentType.CENTER,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 22, font: 'Calibri', color: opts.color || '333333' })]
        })]
      });
    }

    const mealLabels = { breakfast: 'Breakfast', amSnack: 'AM Snack', lunch: 'Lunch', pmSnack: 'PM Snack' };
    const components = ['milk','formula','grain','fruit','vegetable','meat'];
    const sections = [];

    for (const center of ['niles','peace']) {
      for (const cls of CLASSROOMS[center]) {
        const ageKey = CLASSROOM_AGE_MAP[cls.name] || '3-5';
        const pat = MEAL_PATTERNS[ageKey];
        if (!pat) continue;

        const headerRow = new TableRow({ children: [
          cell('Component', { bold: true, bg: navy, color: 'FFFFFF', sz: 22 }),
          ...Object.keys(mealLabels).map(mk => cell(mealLabels[mk], { bold: true, bg: navy, color: 'FFFFFF', sz: 22 }))
        ]});
        const rows = [headerRow];
        for (const comp of components) {
          const hasAny = Object.keys(mealLabels).some(mk => pat[mk]?.[comp]);
          if (!hasAny) continue;
          rows.push(new TableRow({ children: [
            cell(comp.charAt(0).toUpperCase() + comp.slice(1), { bold: true, bg: 'F0F4F8', sz: 20 }),
            ...Object.keys(mealLabels).map(mk => cell(pat[mk]?.[comp] || '—', { sz: 22 }))
          ]}));
        }

        sections.push({
          properties: { page: {
            margin: { top: 720, bottom: 720, left: 720, right: 720 },
            size: { orientation: 'landscape', width: 15840, height: 12240 }
          } },
          children: [
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [
              new TextRun({ text: `${center === 'niles' ? 'Niles' : 'Peace Blvd'}`, size: 22, font: 'Calibri', color: '999999' })
            ]}),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
              new TextRun({ text: cls.name, bold: true, size: 40, font: 'Calibri', color: navy })
            ]}),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 }, children: [
              new TextRun({ text: `${cls.ages} | Age Group ${ageKey}`, size: 24, font: 'Calibri', color: '666666' })
            ]}),
            new Table({ width: { size: 100, type: WidthType.PERCENTAGE }, rows }),
            new Paragraph({ spacing: { before: 200 }, alignment: AlignmentType.CENTER, children: [
              new TextRun({ text: 'Per USDA CACFP Meal Pattern Requirements', size: 16, font: 'Calibri', color: '999999', italics: true })
            ]})
          ]
        });
      }
    }

    const doc = new Document({ sections });
    const buffer = await Packer.toBuffer(doc);
    res.setHeader('Content-Disposition', `attachment; filename="All_Portion_Posters.docx"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// ── STAFF MERGE (merge duplicate staff entries) ───────────
app.post('/api/staff/merge', authCheck, async (req, res) => {
  try {
    const { keep_id, merge_ids } = req.body;
    if (!keep_id || !Array.isArray(merge_ids) || !merge_ids.length) {
      return res.status(400).json({ error: 'Must provide keep_id and merge_ids array' });
    }
    for (const mid of merge_ids) {
      if (mid === keep_id) continue;
      await pool.query('UPDATE staff_time_entries SET staff_id=$1 WHERE staff_id=$2', [keep_id, mid]);
      await pool.query('UPDATE documents SET staff_id=$1 WHERE staff_id=$2', [keep_id, mid]);
      try { await pool.query('UPDATE daily_cacfp_entries SET staff_id=$1 WHERE staff_id=$2', [keep_id, mid]); } catch(e) {}
      try { await pool.query('UPDATE playground_staff_hours SET staff_id=$1 WHERE staff_id=$2', [keep_id, mid]); } catch(e) {}
      try { await pool.query('UPDATE monthly_signatures SET staff_id=$1 WHERE staff_id=$2', [keep_id, mid]); } catch(e) {}
      await pool.query('UPDATE staff SET is_active=false WHERE id=$1', [mid]);
    }
    res.json({ ok: true, merged: merge_ids.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════
// CHILD ROSTER API
// ═══════════════════════════════════════════════════════════

// Normalize a child name for dedup: lowercase, trim, collapse whitespace
function normalizeChildName(first, last) {
  const clean = s => (s || '').toString().toLowerCase().trim().replace(/\s+/g, ' ').replace(/[^a-z0-9' -]/g, '');
  return `${clean(last)}|${clean(first)}`;
}

// Levenshtein distance — used for fuzzy name matching
function levenshtein(a, b) {
  if (!a || !b) return Math.max((a||'').length, (b||'').length);
  const m = a.length, n = b.length;
  const dp = Array.from({length: m+1}, () => new Array(n+1).fill(0));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      const cost = a[i-1] === b[j-1] ? 0 : 1;
      dp[i][j] = Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+cost);
    }
  }
  return dp[m][n];
}

// Is child A in center X "active" right now? Rolling 3-month rule.
// Compares last_seen_date against (today - 90 days).
function computeActiveFlag(lastSeenDate) {
  if (!lastSeenDate) return false;
  const d = lastSeenDate instanceof Date ? lastSeenDate : new Date(lastSeenDate);
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 90);
  return d >= cutoff;
}

// ── CHILDREN CRUD ─────────────────────────────────────────
app.get('/api/children', authCheck, async (req, res) => {
  try {
    const { center, classroom, category, active, search, missing_doc } = req.query;
    let q = 'SELECT * FROM children WHERE 1=1';
    const p = [];
    if (center) { p.push(center); q += ` AND center = $${p.length}`; }
    if (classroom) { p.push(classroom); q += ` AND classroom = $${p.length}`; }
    if (category) { p.push(category); q += ` AND category = $${p.length}`; }
    if (search) {
      p.push('%' + search.toLowerCase() + '%');
      q += ` AND (LOWER(child_first) LIKE $${p.length} OR LOWER(child_last) LIKE $${p.length})`;
    }
    q += ' ORDER BY center, child_last, child_first';
    const { rows } = await pool.query(q, p);

    // Compute "is_active" on read using last_seen_date + 90 days
    let out = rows.map(r => ({ ...r, is_active: computeActiveFlag(r.last_seen_date) }));
    if (active === 'true')  out = out.filter(r => r.is_active);
    if (active === 'false') out = out.filter(r => !r.is_active);

    // Optional "missing doc" filter
    if (missing_doc) {
      const ids = out.map(r => r.id);
      if (ids.length) {
        const { rows: docRows } = await pool.query(
          `SELECT child_id, doc_type FROM child_documents WHERE child_id = ANY($1)`,
          [ids]
        );
        const haveDoc = new Set(docRows.filter(d => d.doc_type === missing_doc).map(d => d.child_id));
        out = out.filter(r => !haveDoc.has(r.id));
      }
    }
    res.json(out);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// Search similar children by name (for manual-add fuzzy warnings)
// MUST come before /api/children/:id to avoid being shadowed
app.get('/api/children/search-similar', authCheck, async (req, res) => {
  try {
    const { first, last, center } = req.query;
    if (!first && !last) return res.json([]);
    let q = 'SELECT id, center, child_first, child_last, classroom, category, last_seen_date FROM children WHERE 1=1';
    const p = [];
    if (center) { p.push(center); q += ` AND center = $${p.length}`; }
    const { rows } = await pool.query(q, p);
    const target = normalizeChildName(first || '', last || '');
    const results = rows.map(r => {
      const nk = normalizeChildName(r.child_first, r.child_last);
      const distance = levenshtein(target, nk);
      const maxLen = Math.max(target.length, nk.length) || 1;
      const similarity = 1 - distance / maxLen;
      return { ...r, similarity };
    }).filter(r => r.similarity >= 0.7).sort((a, b) => b.similarity - a.similarity).slice(0, 10);
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/children/:id', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM children WHERE id = $1', [req.params.id]);
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    const child = rows[0];
    child.is_active = computeActiveFlag(child.last_seen_date);
    // Attach their documents (minus binary data)
    const { rows: docRows } = await pool.query(
      `SELECT id, child_id, doc_type, cacfp_year_label, signing_date, approval_date,
              annual_review_reminder_date, filename, mime_type, page_count, file_sort_order,
              notes, metadata, uploaded_at, uploaded_by
       FROM child_documents WHERE child_id = $1
       ORDER BY doc_type, cacfp_year_label DESC NULLS LAST, file_sort_order, uploaded_at`,
      [child.id]
    );
    child.documents = docRows;
    res.json(child);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/children', authCheck, async (req, res) => {
  try {
    const { center, child_first, child_last, classroom, category, notes } = req.body;
    if (!center || !child_first || !child_last) {
      return res.status(400).json({ error: 'center, child_first, and child_last required' });
    }
    const nk = normalizeChildName(child_first, child_last);
    const { rows } = await pool.query(
      `INSERT INTO children (center, child_first, child_last, normalized_key, classroom, category, notes, last_seen_date)
       VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_DATE)
       ON CONFLICT (center, normalized_key) DO UPDATE SET
         classroom = COALESCE($5, children.classroom),
         category = COALESCE($6, children.category),
         notes = COALESCE($7, children.notes),
         updated_at = NOW()
       RETURNING *`,
      [center, child_first, child_last, nk, classroom, category, notes || null]
    );
    const child = rows[0];
    child.is_active = computeActiveFlag(child.last_seen_date);
    res.json(child);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.put('/api/children/:id', authCheck, async (req, res) => {
  try {
    const { child_first, child_last, classroom, category, notes, is_active } = req.body;
    const sets = []; const vals = []; let n = 0;
    if (child_first !== undefined) { n++; sets.push(`child_first=$${n}`); vals.push(child_first); }
    if (child_last !== undefined)  { n++; sets.push(`child_last=$${n}`);  vals.push(child_last); }
    // If either name changed, also recompute normalized_key
    if (child_first !== undefined || child_last !== undefined) {
      const cur = await pool.query('SELECT child_first, child_last FROM children WHERE id=$1', [req.params.id]);
      if (cur.rows[0]) {
        const newFirst = child_first !== undefined ? child_first : cur.rows[0].child_first;
        const newLast  = child_last  !== undefined ? child_last  : cur.rows[0].child_last;
        n++; sets.push(`normalized_key=$${n}`); vals.push(normalizeChildName(newFirst, newLast));
      }
    }
    if (classroom !== undefined) { n++; sets.push(`classroom=$${n}`); vals.push(classroom); }
    if (category !== undefined)  { n++; sets.push(`category=$${n}`);  vals.push(category); }
    if (notes !== undefined)     { n++; sets.push(`notes=$${n}`);     vals.push(notes); }
    // Manual is_active override — stored as a flag; `computeActiveFlag` still wins on read
    // unless we explicitly set last_seen_date. For manual deactivation, set last_seen_date far in past.
    if (is_active === false) { sets.push('last_seen_date = CURRENT_DATE - INTERVAL \'1 year\''); }
    sets.push('updated_at = NOW()');
    n++; vals.push(req.params.id);
    const { rows } = await pool.query(
      `UPDATE children SET ${sets.join(', ')} WHERE id=$${n} RETURNING *`, vals
    );
    const child = rows[0];
    if (child) child.is_active = computeActiveFlag(child.last_seen_date);
    res.json(child);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.delete('/api/children/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM children WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


// ── SYNC CHILDREN FROM UPLOADS ────────────────────────────
// Scans meal uploads, attendance summaries, and attendance-time rows across
// all months; creates/updates children rows. Meal uploads are authoritative
// for classroom + category. Fuzzy-matching similar names queues merge requests.
app.post('/api/children/sync-from-uploads', authCheck, async (req, res) => {
  try {
    // Preload current roster
    const { rows: rosterRows } = await pool.query('SELECT id, center, child_first, child_last, normalized_key, last_seen_date, classroom FROM children');
    const rosterByKey = new Map();
    for (const r of rosterRows) rosterByKey.set(`${r.center}::${r.normalized_key}`, r);

    // Collect every child reference from all sources
    // seen[`${center}::${normKey}`] = { first, last, center, classroom, category, lastMonth, lastDate }
    const seen = new Map();
    const bumpSeen = (center, first, last, classroom, category, monthKey, date) => {
      if (!first || !last || !center) return;
      const nk = normalizeChildName(first, last);
      const key = `${center}::${nk}`;
      const existing = seen.get(key);
      if (!existing) {
        seen.set(key, {
          first: first.trim(), last: last.trim(), center, nk,
          classroom: classroom || null,
          category: category || null,
          lastMonth: monthKey || null,
          lastDate: date || null,
          firstMonth: monthKey || null,
          mealSourced: !!classroom
        });
      } else {
        // Meal uploads are authoritative for classroom/category — latest meal wins
        if (classroom) {
          existing.classroom = classroom;
          existing.mealSourced = true;
        }
        if (category) existing.category = category;
        // Track newest occurrence
        if (monthKey && (!existing.lastMonth || monthKey > existing.lastMonth)) existing.lastMonth = monthKey;
        if (date && (!existing.lastDate || date > existing.lastDate)) existing.lastDate = date;
        // Track oldest
        if (monthKey && (!existing.firstMonth || monthKey < existing.firstMonth)) existing.firstMonth = monthKey;
      }
    };

    // 1) Scan meal uploads
    const { rows: mealRows } = await pool.query(
      `SELECT md.month_key, md.data, fy.start_year, fy.end_year
       FROM monthly_data md JOIN fiscal_years fy ON fy.id = md.fiscal_year_id
       WHERE md.data_type = 'meals'`
    );
    for (const r of mealRows) {
      const d = r.data || {};
      for (const center of ['niles', 'peace']) {
        const cd = d[center];
        if (!cd || !cd.children) continue;
        for (const ch of cd.children) {
          // Meal CSV names are "First Last"; split on last space
          const name = (ch.name || '').trim();
          if (!name) continue;
          const parts = name.split(/\s+/);
          const first = parts[0];
          const last = parts.slice(1).join(' ');
          if (!last) continue;
          bumpSeen(center, first, last, ch.classroom, ch.cat, r.month_key, null);
        }
      }
    }

    // 2) Scan monthly_data attendance (classroom info sometimes present; names "Last, First" style)
    const { rows: attRows } = await pool.query(
      `SELECT md.month_key, md.data FROM monthly_data md WHERE md.data_type = 'attendance'`
    );
    for (const r of attRows) {
      const d = r.data || {};
      for (const center of ['niles', 'peace']) {
        const cd = d[center];
        if (!cd || !cd.childData) continue;
        for (const ch of cd.childData) {
          // Attendance names can be "Last First", "First Last", or "Last, First"
          const raw = (ch.name || '').trim();
          if (!raw) continue;
          let first, last;
          if (raw.includes(',')) {
            const p = raw.split(',').map(s => s.trim());
            last = p[0]; first = p[1];
          } else {
            const parts = raw.split(/\s+/);
            // Guess: if "Last First" style — heuristically treat single 2-part name with meal-upload data already present.
            // Default to "First Last"
            first = parts[0]; last = parts.slice(1).join(' ');
          }
          if (!first || !last) continue;
          bumpSeen(center, first, last, ch.classroom || null, null, r.month_key, null);
        }
      }
    }

    // 3) Scan child_attendance_times (last_seen_date source)
    const { rows: catRows } = await pool.query(
      `SELECT center, child_first, child_last, month_key, MAX(attend_date) as last_date
       FROM child_attendance_times WHERE status = 'present'
       GROUP BY center, child_first, child_last, month_key`
    );
    for (const r of catRows) {
      const dateISO = r.last_date instanceof Date ? r.last_date.toISOString().slice(0,10) : r.last_date;
      bumpSeen(r.center, r.child_first, r.child_last, null, null, r.month_key, dateISO);
    }

    // 4) Reconcile into children table
    let created = 0, updated = 0;
    const mergeCandidates = []; // fuzzy matches → queue manually
    const existingByCenter = new Map(); // center → list of {id, nk} for fuzzy lookup
    for (const [k, v] of rosterByKey) {
      if (!existingByCenter.has(v.center)) existingByCenter.set(v.center, []);
      existingByCenter.get(v.center).push({ id: v.id, nk: v.normalized_key });
    }

    for (const [key, s] of seen) {
      const rosterKey = `${s.center}::${s.nk}`;
      const existing = rosterByKey.get(rosterKey);
      if (existing) {
        // Update — meal uploads always win for classroom
        const setParts = ['updated_at = NOW()'];
        const vals = [existing.id];
        let n = 1;
        if (s.mealSourced && s.classroom) {
          n++; setParts.push(`classroom = $${n}`); vals.push(s.classroom);
        }
        if (s.category) {
          n++; setParts.push(`category = $${n}`); vals.push(s.category);
        }
        if (s.lastMonth) {
          n++; setParts.push(`last_seen_month = CASE WHEN last_seen_month IS NULL OR $${n} > last_seen_month THEN $${n} ELSE last_seen_month END`); vals.push(s.lastMonth);
        }
        if (s.lastDate) {
          n++; setParts.push(`last_seen_date = CASE WHEN last_seen_date IS NULL OR $${n}::date > last_seen_date THEN $${n}::date ELSE last_seen_date END`); vals.push(s.lastDate);
        }
        if (s.firstMonth) {
          n++; setParts.push(`first_seen_month = CASE WHEN first_seen_month IS NULL OR $${n} < first_seen_month THEN $${n} ELSE first_seen_month END`); vals.push(s.firstMonth);
        }
        await pool.query(`UPDATE children SET ${setParts.join(', ')} WHERE id = $1`, vals);
        updated++;
      } else {
        // Fuzzy check — do we have a near-duplicate in the same center?
        let fuzzyMatchId = null, fuzzyScore = 0;
        const candidates = existingByCenter.get(s.center) || [];
        for (const cand of candidates) {
          const distance = levenshtein(s.nk, cand.nk);
          const maxLen = Math.max(s.nk.length, cand.nk.length) || 1;
          const sim = 1 - distance / maxLen;
          if (sim >= 0.85 && sim > fuzzyScore) {
            fuzzyMatchId = cand.id;
            fuzzyScore = sim;
          }
        }
        // Create the new child row (even if fuzzy match — queue merge request for review)
        const { rows: inserted } = await pool.query(
          `INSERT INTO children (center, child_first, child_last, normalized_key, classroom, category, first_seen_month, last_seen_month, last_seen_date)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id`,
          [s.center, s.first, s.last, s.nk, s.classroom, s.category, s.firstMonth, s.lastMonth, s.lastDate]
        );
        const newId = inserted[0].id;
        created++;
        if (fuzzyMatchId) {
          mergeCandidates.push({ proposed: newId, existing: fuzzyMatchId, score: fuzzyScore });
        }
        // Add to existing-by-center for subsequent fuzzy checks
        if (!existingByCenter.has(s.center)) existingByCenter.set(s.center, []);
        existingByCenter.get(s.center).push({ id: newId, nk: s.nk });
      }
    }

    // 5) Create merge requests for fuzzy matches (skip duplicates)
    let mergeRequests = 0;
    for (const m of mergeCandidates) {
      const exists = await pool.query(
        `SELECT id FROM roster_merge_requests
         WHERE proposed_child_id = $1 AND existing_child_id = $2 AND status = 'pending'`,
        [m.proposed, m.existing]
      );
      if (exists.rows.length === 0) {
        await pool.query(
          `INSERT INTO roster_merge_requests (proposed_child_id, existing_child_id, similarity_score, reason)
           VALUES ($1, $2, $3, $4)`,
          [m.proposed, m.existing, m.score.toFixed(2), `Auto-detected during sync (${(m.score*100).toFixed(0)}% name similarity)`]
        );
        mergeRequests++;
      }
    }

    res.json({
      ok: true,
      scanned_children: seen.size,
      created,
      updated,
      merge_requests_created: mergeRequests
    });
  } catch (e) {
    console.error('children sync error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── ROSTER MERGE REQUESTS ─────────────────────────────────
app.get('/api/roster-merge-requests', authCheck, async (req, res) => {
  try {
    const { status } = req.query;
    let q = `SELECT rmr.*,
               c1.child_first AS p_first, c1.child_last AS p_last, c1.center AS p_center,
               c1.classroom AS p_classroom, c1.category AS p_category, c1.last_seen_date AS p_last_seen,
               c2.child_first AS e_first, c2.child_last AS e_last, c2.center AS e_center,
               c2.classroom AS e_classroom, c2.category AS e_category, c2.last_seen_date AS e_last_seen
             FROM roster_merge_requests rmr
             JOIN children c1 ON c1.id = rmr.proposed_child_id
             JOIN children c2 ON c2.id = rmr.existing_child_id
             WHERE 1=1`;
    const p = [];
    if (status) { p.push(status); q += ` AND rmr.status = $${p.length}`; }
    q += ' ORDER BY rmr.created_at DESC';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Approve a merge: keep existing_child_id, move documents from proposed to existing, delete proposed
app.post('/api/roster-merge-requests/:id/approve', authCheck, async (req, res) => {
  try {
    const { rows: mrRows } = await pool.query(
      `SELECT * FROM roster_merge_requests WHERE id = $1 AND status = 'pending'`,
      [req.params.id]
    );
    const mr = mrRows[0];
    if (!mr) return res.status(404).json({ error: 'Request not found or already resolved' });

    // Transfer documents, then delete proposed child
    await pool.query(
      `UPDATE child_documents SET child_id = $1 WHERE child_id = $2`,
      [mr.existing_child_id, mr.proposed_child_id]
    );
    // Also re-point any other pending merge requests
    await pool.query(
      `UPDATE roster_merge_requests SET existing_child_id = $1 WHERE existing_child_id = $2 AND id != $3`,
      [mr.existing_child_id, mr.proposed_child_id, mr.id]
    );
    await pool.query('DELETE FROM children WHERE id = $1', [mr.proposed_child_id]);
    await pool.query(
      `UPDATE roster_merge_requests SET status = 'approved', resolved_at = NOW(), resolved_by = $1 WHERE id = $2`,
      [req.body?.resolved_by || 'Mary Wardlaw', mr.id]
    );
    res.json({ ok: true, kept_child_id: mr.existing_child_id });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Reject a merge: keep both children separate
app.post('/api/roster-merge-requests/:id/reject', authCheck, async (req, res) => {
  try {
    await pool.query(
      `UPDATE roster_merge_requests SET status = 'rejected', resolved_at = NOW(), resolved_by = $1 WHERE id = $2`,
      [req.body?.resolved_by || 'Mary Wardlaw', req.params.id]
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════
// CHILD DOCUMENTS API (HIES, medical exception, infant sign-off)
// ═══════════════════════════════════════════════════════════
const VALID_CHILD_DOC_TYPES = new Set(['hies', 'medical_exception', 'infant_food_signoff']);

// Rough PDF page count (counts "/Type /Page" entries; good enough for the `page_count` field)
function countPdfPages(buffer) {
  try {
    const s = buffer.toString('latin1');
    const matches = s.match(/\/Type\s*\/Page(?![a-zA-Z])/g);
    return matches ? matches.length : 1;
  } catch { return 1; }
}

// Upload a file to a child's document set
// Multiple files with same (child_id, doc_type, cacfp_year_label) group into one logical document
app.post('/api/children/:id/documents', authCheck, upload.single('file'), async (req, res) => {
  try {
    const childId = req.params.id;
    const { doc_type, cacfp_year_label, signing_date, approval_date, notes, file_sort_order } = req.body;
    if (!doc_type || !VALID_CHILD_DOC_TYPES.has(doc_type)) {
      return res.status(400).json({ error: 'doc_type must be one of: ' + [...VALID_CHILD_DOC_TYPES].join(', ') });
    }
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    // Verify child exists
    const childRes = await pool.query('SELECT id FROM children WHERE id = $1', [childId]);
    if (!childRes.rows[0]) return res.status(404).json({ error: 'Child not found' });

    // Compute annual_review_reminder_date for medical (1 year from today)
    let reviewDate = null;
    if (doc_type === 'medical_exception') {
      const d = new Date();
      d.setFullYear(d.getFullYear() + 1);
      reviewDate = d.toISOString().slice(0, 10);
    }

    // Page count estimate
    const pageCount = (req.file.mimetype === 'application/pdf') ? countPdfPages(req.file.buffer) : 1;

    const { rows } = await pool.query(
      `INSERT INTO child_documents (child_id, doc_type, cacfp_year_label, signing_date, approval_date,
          annual_review_reminder_date, filename, mime_type, file_data, page_count, file_sort_order, notes, uploaded_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       RETURNING id, child_id, doc_type, cacfp_year_label, signing_date, approval_date,
                 annual_review_reminder_date, filename, mime_type, page_count, file_sort_order, notes, uploaded_at, uploaded_by`,
      [childId, doc_type, cacfp_year_label || null,
       signing_date || null, approval_date || null, reviewDate,
       req.file.originalname, req.file.mimetype, req.file.buffer, pageCount,
       parseInt(file_sort_order) || 0, notes || null, req.body.uploaded_by || 'Mary Wardlaw']
    );
    res.json(rows[0]);
  } catch (e) {
    console.error('child doc upload error:', e);
    res.status(500).json({ error: e.message });
  }
});

// List all documents for a child
app.get('/api/children/:id/documents', authCheck, async (req, res) => {
  try {
    const { doc_type } = req.query;
    let q = `SELECT id, child_id, doc_type, cacfp_year_label, signing_date, approval_date,
                    annual_review_reminder_date, filename, mime_type, page_count, file_sort_order,
                    notes, uploaded_at, uploaded_by
             FROM child_documents WHERE child_id = $1`;
    const p = [req.params.id];
    if (doc_type) { p.push(doc_type); q += ` AND doc_type = $${p.length}`; }
    q += ' ORDER BY doc_type, cacfp_year_label DESC NULLS LAST, file_sort_order, uploaded_at';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Download one child document
app.get('/api/children/:childId/documents/:docId/download', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT filename, mime_type, file_data FROM child_documents WHERE id = $1 AND child_id = $2`,
      [req.params.docId, req.params.childId]
    );
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    res.setHeader('Content-Disposition', `attachment; filename="${rows[0].filename}"`);
    res.setHeader('Content-Type', rows[0].mime_type || 'application/octet-stream');
    res.send(rows[0].file_data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Rasterize one child document (same behavior as /api/documents/:id/rasterize)
app.get('/api/children/:childId/documents/:docId/rasterize', authCheck, async (req, res) => {
  const cacheKey = 'child:' + req.params.docId;
  // Cache hit? Handle before acquiring lock
  if (rasterizeCache.has(cacheKey)) {
    const hit = rasterizeCache.get(cacheKey);
    rasterizeCache.delete(cacheKey);
    rasterizeCache.set(cacheKey, hit);
    return res.json({ pages: hit.pages, pageCount: hit.pageCount, filename: hit.filename, cached: true });
  }
  const releaseLock = await acquireRasterizeLock();
  try {
    const { rows } = await pool.query(
      `SELECT filename, mime_type, file_data FROM child_documents WHERE id = $1 AND child_id = $2`,
      [req.params.docId, req.params.childId]
    );
    if (!rows[0]) return res.status(404).json({ error: 'Not found' });
    const { filename, mime_type, file_data } = rows[0];
    const mt = (mime_type || '').toLowerCase();
    let pages = [];
    if (mt === 'application/pdf' || filename.toLowerCase().endsWith('.pdf')) {
      const { pdf } = await import('pdf-to-img');
      try {
        const doc = await pdf(file_data, { scale: 2 });
        for await (const pageBuf of doc) pages.push('data:image/png;base64,' + pageBuf.toString('base64'));
      } catch (pdfErr) {
        return res.status(422).json({ error: 'Could not rasterize PDF', detail: pdfErr.message, filename });
      }
    } else if (mt.startsWith('image/')) {
      pages.push(`data:${mt};base64,` + Buffer.from(file_data).toString('base64'));
    } else {
      return res.status(415).json({ error: 'Unsupported file type', mime_type: mt, filename });
    }
    const byteSize = sumRasterizeSize(pages);
    const result = { pages, pageCount: pages.length, filename, cachedAt: Date.now(), byteSize };
    const LARGE_DOC_THRESHOLD = 15 * 1024 * 1024;
    if (byteSize < LARGE_DOC_THRESHOLD) {
      while (rasterizeCache.size >= RASTERIZE_CACHE_MAX) evictRasterizeOldest();
      while (rasterizeCacheBytes + byteSize > RASTERIZE_CACHE_MAX_BYTES && rasterizeCache.size > 0) {
        evictRasterizeOldest();
      }
      rasterizeCache.set(cacheKey, result);
      rasterizeCacheBytes += byteSize;
    }
    res.json({ pages: result.pages, pageCount: result.pageCount, filename: result.filename, cached: false });
    if (global.gc && byteSize > 5 * 1024 * 1024) {
      setImmediate(() => { try { global.gc(); } catch(e){} });
    }
  } catch (e) {
    console.error('child rasterize error:', e);
    res.status(500).json({ error: e.message });
  } finally {
    releaseLock();
  }
});

// Update a child document's metadata (dates, notes, sort order)
app.put('/api/children/:childId/documents/:docId', authCheck, async (req, res) => {
  try {
    const { signing_date, approval_date, cacfp_year_label, notes, file_sort_order, annual_review_reminder_date } = req.body;
    const sets = []; const vals = []; let n = 0;
    if (signing_date !== undefined)   { n++; sets.push(`signing_date=$${n}`); vals.push(signing_date || null); }
    if (approval_date !== undefined)  { n++; sets.push(`approval_date=$${n}`); vals.push(approval_date || null); }
    if (cacfp_year_label !== undefined) { n++; sets.push(`cacfp_year_label=$${n}`); vals.push(cacfp_year_label || null); }
    if (notes !== undefined)          { n++; sets.push(`notes=$${n}`); vals.push(notes || null); }
    if (file_sort_order !== undefined){ n++; sets.push(`file_sort_order=$${n}`); vals.push(parseInt(file_sort_order) || 0); }
    if (annual_review_reminder_date !== undefined) { n++; sets.push(`annual_review_reminder_date=$${n}`); vals.push(annual_review_reminder_date || null); }
    if (!sets.length) return res.json({ ok: true, noChange: true });
    n++; vals.push(req.params.docId);
    n++; vals.push(req.params.childId);
    const { rows } = await pool.query(
      `UPDATE child_documents SET ${sets.join(', ')} WHERE id = $${n-1} AND child_id = $${n} RETURNING *`,
      vals
    );
    // Invalidate rasterize cache for this doc
    invalidateRasterizeCache('child:' + req.params.docId);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Delete one child document
app.delete('/api/children/:childId/documents/:docId', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM child_documents WHERE id = $1 AND child_id = $2', [req.params.docId, req.params.childId]);
    invalidateRasterizeCache('child:' + req.params.docId);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// List ALL child documents (for the documents library view)
app.get('/api/child-documents/all', authCheck, async (req, res) => {
  try {
    const { doc_type, cacfp_year_label } = req.query;
    let q = `SELECT cd.id, cd.child_id, cd.doc_type, cd.cacfp_year_label, cd.signing_date, cd.approval_date,
                    cd.annual_review_reminder_date, cd.filename, cd.mime_type, cd.page_count,
                    cd.notes, cd.uploaded_at, cd.uploaded_by,
                    c.center, c.child_first, c.child_last, c.classroom, c.category
             FROM child_documents cd JOIN children c ON c.id = cd.child_id WHERE 1=1`;
    const p = [];
    if (doc_type) { p.push(doc_type); q += ` AND cd.doc_type = $${p.length}`; }
    if (cacfp_year_label) { p.push(cacfp_year_label); q += ` AND cd.cacfp_year_label = $${p.length}`; }
    q += ' ORDER BY c.center, c.child_last, c.child_first, cd.doc_type, cd.uploaded_at DESC';
    const { rows } = await pool.query(q, p);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── STARTUP ───────────────────────────────────────────────
initDB()
  .then(initMonitoringTables)
  .then(() => {
    app.listen(PORT, () => {
      console.log(`✅ CACFP Suite server listening on port ${PORT}`);
    });
  })
  .catch(err => {
    console.error('❌ Startup failed:', err);
    process.exit(1);
  });
