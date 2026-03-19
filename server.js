const express = require('express');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
        AlignmentType, BorderStyle, WidthType, ShadingType, HeadingLevel } = require('docx');

const app = express();
const PORT = process.env.PORT || 3000;
const ACCESS_PIN = process.env.ACCESS_PIN || '2024tcc';

// ── DATABASE ──────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// File uploads — store in memory for DB storage
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

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

    -- Seed first fiscal year if none exists
    INSERT INTO fiscal_years (label, start_year, end_year, is_active)
    VALUES ('2025-2026', 2025, 2026, true)
    ON CONFLICT (label) DO NOTHING;
  `);
  // Add adult_meal column if not exists
  try { await pool.query('ALTER TABLE daily_cacfp_entries ADD COLUMN IF NOT EXISTS adult_meal BOOLEAN DEFAULT false'); } catch(e) {}
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
    // Deactivate all, then insert new as active
    await pool.query('UPDATE fiscal_years SET is_active = false');
    const { rows } = await pool.query(
      `INSERT INTO fiscal_years (label, start_year, end_year, is_active)
       VALUES ($1, $2, $3, true)
       ON CONFLICT (label) DO UPDATE SET is_active = true
       RETURNING *`,
      [label, start_year, end_year]
    );
    // Copy active staff roster to new year (they'll need new time entries)
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

// Bulk upsert time entries for a month
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

// Get monthly salary/admin totals for a fiscal year
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

app.delete('/api/documents/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM documents WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── MONTHLY DATA (generic key-value per month) ───────────
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

// Revenue summary for fiscal year
app.get('/api/revenue/summary', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id } = req.query;
    const { rows } = await pool.query(`
      SELECT month_key, revenue_type,
        SUM(amount) as total
      FROM revenue_entries
      WHERE fiscal_year_id = $1
      GROUP BY month_key, revenue_type
      ORDER BY month_key
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

    // Store the CSV as a document for audit
    await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,$2,'playground_staff_hours',$3,$4,$5,$6)`,
      [fiscal_year_id, month_key, file.originalname, file.mimetype, file.buffer,
       JSON.stringify({ center: center || 'unknown' })]
    );

    const csv = file.buffer.toString('utf8').replace(/^\uFEFF/, '');
    const lines = csv.split('\n').map(l => l.trim()).filter(Boolean);
    if (lines.length < 2) return res.status(400).json({ error: 'Empty CSV' });

    // Parse header
    const hdr = lines[0].split(',').map(h => h.replace(/"/g, '').trim().toLowerCase());
    const idxLast = hdr.indexOf('last name');
    const idxFirst = hdr.indexOf('first name');
    const idxDate = hdr.indexOf('date');
    const idxTimes = hdr.indexOf('times');
    const idxBreaks = hdr.indexOf('breaks');
    const idxBillable = hdr.indexOf('billable');
    if (idxLast < 0 || idxFirst < 0 || idxDate < 0) return res.status(400).json({ error: 'Missing required columns' });

    // Parse CSV rows (handle quoted fields with newlines)
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
    dataRows.shift(); // remove header

    // Get staff roster
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

      // Parse date
      const dm = dateStr.match(/(\d+)\/(\d+)\/(\d+)/);
      if (!dm) continue;
      const rowMonth = parseInt(dm[1]) - 1;
      const rowDay = parseInt(dm[2]);
      if (rowMonth !== targetMonth) continue;

      // Match staff
      const key = fullName.toLowerCase();
      let staff = staffMap[key];
      if (!staff) {
        const fnLow = firstName.toLowerCase();
        const lnLow = lastName.toLowerCase();
        // Common nickname mappings
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
        // Also check if any nickname maps TO this name
        for (const [nick, full] of Object.entries(NICKNAMES)) {
          if (full === fnLow && !nickVariants.includes(nick)) nickVariants.push(nick);
        }

        for (const s of staffRes.rows) {
          const parts = s.name.toLowerCase().split(' ');
          if (parts.length < 2) continue;
          const sFirst = parts[0];
          const sLast = parts[parts.length - 1];
          if (sLast !== lnLow) continue; // Last name must match exactly

          // Check all nickname variants
          for (const variant of nickVariants) {
            if (variant === sFirst) { staff = s; break; }
          }
          if (staff) break;

          // Prefix match (3+ chars)
          if (fnLow.length >= 3 && sFirst.startsWith(fnLow.substring(0, 3))) { staff = s; break; }
          if (sFirst.length >= 3 && fnLow.startsWith(sFirst.substring(0, 3))) { staff = s; break; }
        }
      }
      if (!staff) {
        // Auto-add new staff member from CSV
        const staffCenter = center || 'niles';
        try {
          const newStaff = await pool.query(
            'INSERT INTO staff (name, center, hourly_rate) VALUES ($1, $2, 0) RETURNING *',
            [fullName, staffCenter]
          );
          staff = newStaff.rows[0];
          staffMap[key] = staff;
          // Also create a default PIN so they can log in to the phone app
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

      // Parse times
      const timesRaw = cols[idxTimes] || '';
      const timeSegments = timesRaw.split(/\n/).map(t => t.trim()).filter(Boolean);
      let startTime = '', endTime = '';
      for (const seg of timeSegments) {
        const tm = seg.match(/(\d+:\d+[ap]m)\s*-\s*(\d+:\d+[ap]m)/i);
        if (tm) { if (!startTime) startTime = tm[1]; endTime = tm[2]; }
      }

      // Parse breaks
      const breaksRaw = cols[idxBreaks] || '0 hrs 0 min';
      const bm = breaksRaw.match(/(\d+)\s*hrs?\s*(\d+)\s*min/);
      const breakHrs = bm ? parseInt(bm[1]) + parseInt(bm[2]) / 60 : 0;

      // Parse billable
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

    // Playground hours (start, end, worked, absent)
    const pgRes = await pool.query(
      'SELECT * FROM playground_staff_hours WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3 ORDER BY day_of_month',
      [sid, fiscal_year_id, month_key]
    );

    // Phone CACFP entries (food service + admin hours)
    const ceRes = await pool.query(
      'SELECT * FROM daily_cacfp_entries WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3 ORDER BY day_of_month',
      [sid, fiscal_year_id, month_key]
    );

    // Signature
    const sigRes = await pool.query(
      'SELECT * FROM monthly_signatures WHERE staff_id=$1 AND fiscal_year_id=$2 AND month_key=$3',
      [sid, fiscal_year_id, month_key]
    );

    // Staff info
    const sRes = await pool.query('SELECT * FROM staff WHERE id=$1', [sid]);

    // Merge by day
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

    // Calculate non-CACFP for each day
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

// ── GET ALL MERGED DATA FOR A MONTH ──────────────────────
app.get('/api/merged-time-all', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.query;
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
      result.push({
        ...s, totalFS, totalAdm, totalWorked, totalAbsent, daysWorked, hasPlayground, hasCACFP,
        signature: sigRes.rows[0] || null
      });
    }
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── APPROVE MONTH (supervisor signature) ─────────────────
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

    // Get merged data
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

    // Header row
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

    // Totals row
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

    // Optionally store in documents table
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

// ── GENERATE ALL T&A FORMS + STORE IN DOCS ───────────────
app.post('/api/generate-ta-forms-all', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, supervisor_signature } = req.body;
    // Get all staff with submitted signatures
    const staffRes = await pool.query(
      `SELECT s.id FROM staff s
       JOIN staff_pins sp ON sp.staff_id = s.id
       JOIN monthly_signatures ms ON ms.staff_id = s.id AND ms.fiscal_year_id = $1 AND ms.month_key = $2
       WHERE s.is_active = true AND ms.status IN ('submitted','approved')`,
      [fiscal_year_id, month_key]
    );

    // Approve all and generate forms by calling the single endpoint logic
    let generated = 0;
    for (const s of staffRes.rows) {
      // Apply supervisor signature
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

    // Get all staff with entries
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

    // ── SECTION 1: SUMMARY PAGE ──
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

    // ── SECTION 2+: INDIVIDUAL T&A FORMS ──
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

    // Store in documents
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

// ── GENERATE CHILD ATTENDANCE DETAIL REPORT (.docx) ──────
app.post('/api/generate-attendance-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, center } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
    const fy = fyRes.rows[0]; if (!fy) return res.status(404).json({ error: 'FY not found' });

    const ML = {oct:'October',nov:'November',dec:'December',jan:'January',feb:'February',mar:'March',apr:'April',may:'May',jun:'June',jul:'July',aug:'August',sep:'September'};
    const fyYear = mk => ['oct','nov','dec'].includes(mk) ? fy.start_year : fy.end_year;
    const monthLabel = ML[month_key] + ' ' + fyYear(month_key);
    const navy = '1B2A4A';
    const fmtN = n => n > 0 ? n.toFixed(1) : '';
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

      // Summary header row + day columns
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

// ── CHILD ATTENDANCE DETAIL REPORT ────────────────────────
app.get('/api/child-attendance-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key, center } = req.query;
    const mdRes = await pool.query(
      `SELECT * FROM monthly_data WHERE fiscal_year_id=$1 AND month_key=$2 AND data_type='attendance'`,
      [fiscal_year_id, month_key]
    );
    if (!mdRes.rows.length) return res.json({ children: [], summary: {} });
    const data = mdRes.rows[0].data;
    const centerData = center ? (data[center] || {}) : data;

    // Get the raw children attendance if stored
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

// ── ATTENDANCE vs MEAL COUNT CROSS-CHECK ─────────────────
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
    const attData = attRes.rows[0]?.data || {};
    const mealData = mealRes.rows[0]?.data || {};
    const flags = [];

    for (const center of ['niles', 'peace']) {
      const att = attData[center] || {};
      const meals = mealData[center] || {};
      const attChildren = att.children || [];
      const mealChildren = meals.children || [];

      // Build attendance day map per child
      const attMap = {};
      for (const child of attChildren) {
        const name = child.name || child.childName || '';
        if (!attMap[name]) attMap[name] = { days: 0, name };
        attMap[name].days = child.daysPresent || child.days || 0;
      }

      // Check meal children against attendance
      for (const child of mealChildren) {
        const name = child.name || child.childName || '';
        const mealDays = child.totalMealDays || child.daysWithMeals || 0;
        const attChild = attMap[name];
        if (!attChild) {
          flags.push({ type: 'no_attendance', center, child: name, detail: `${mealDays} meal days claimed but no attendance record found` });
        } else if (mealDays > attChild.days) {
          flags.push({ type: 'meal_exceeds_attendance', center, child: name, detail: `${mealDays} meal days but only ${attChild.days} attendance days` });
        }
      }

      // Check for attendance without meals
      for (const name in attMap) {
        const hasMeals = mealChildren.some(c => (c.name || c.childName) === name);
        if (!hasMeals && attMap[name].days > 0) {
          flags.push({ type: 'no_meals', center, child: name, detail: `${attMap[name].days} attendance days but no meals claimed` });
        }
      }
    }

    res.json({ flags, attData, mealData });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GENERATE MONTHLY STAFF COST REPORT (.docx) ───────────
app.post('/api/generate-staff-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id = $1', [fiscal_year_id]);
    const fy = fyRes.rows[0];
    if (!fy) return res.status(404).json({ error: 'Fiscal year not found' });

    // Month labels
    const ML = { oct:'October',nov:'November',dec:'December',jan:'January',feb:'February',
      mar:'March',apr:'April',may:'May',jun:'June',jul:'July',aug:'August',sep:'September'};
    const fyYear = mk => {
      const first = ['oct','nov','dec'];
      return first.includes(mk) ? fy.start_year : fy.end_year;
    };
    const monthLabel = `${ML[month_key]} ${fyYear(month_key)}`;

    // Get staff time entries for this month with staff details
    const { rows: entries } = await pool.query(`
      SELECT ste.*, s.name, s.center
      FROM staff_time_entries ste
      JOIN staff s ON s.id = ste.staff_id
      WHERE ste.fiscal_year_id = $1 AND ste.month_key = $2 AND s.is_active = true
      ORDER BY s.center, s.name
    `, [fiscal_year_id, month_key]);

    const fmt = n => '$' + Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const navy = '1B2A4A';
    const gold = 'C5972C';
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

    // Header row
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

    // Totals row
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

    // Get fiscal year info
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id = $1', [fiscal_year_id]);
    const fy = fyRes.rows[0];
    if (!fy) return res.status(404).json({ error: 'Fiscal year not found' });

    // Get salary totals
    const salRes = await pool.query(`
      SELECT month_key,
        SUM(food_service_hours * hourly_rate_used) as fs_cost,
        SUM(admin_hours * hourly_rate_used) as admin_cost
      FROM staff_time_entries WHERE fiscal_year_id = $1
      GROUP BY month_key ORDER BY month_key
    `, [fiscal_year_id]);

    // Get YER data
    const yerRes = await pool.query('SELECT * FROM yer_data WHERE fiscal_year_id = $1', [fiscal_year_id]);
    const yer = yerRes.rows[0] || {};

    // Get revenue
    const revRes = await pool.query(`
      SELECT month_key, revenue_type, SUM(amount) as total
      FROM revenue_entries WHERE fiscal_year_id = $1
      GROUP BY month_key, revenue_type ORDER BY month_key
    `, [fiscal_year_id]);

    // Calculate totals
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

    // Build document
    const noBorder = { top: { style: BorderStyle.NONE }, bottom: { style: BorderStyle.NONE },
      left: { style: BorderStyle.NONE }, right: { style: BorderStyle.NONE } };
    const navy = '1B2A4A';
    const gold = 'C5972C';

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

// ── START ─────────────────────────────────────────────────
// First add monitoring tables
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
  `);
  console.log('✅ Monitoring tables ready');
}

// ── MONITORING API ───────────────────────────────────────

// Classroom definitions
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

// List all monitoring reviews
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

// Get single review
app.get('/api/monitoring/:id', authCheck, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM monitoring_reviews WHERE id=$1', [req.params.id]);
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Create new review
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

// Save form data (auto-save as monitor fills it out)
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

// Delete review
app.delete('/api/monitoring/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM monitoring_reviews WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GENERATE MONITORING REVIEW .docx ─────────────────────
app.post('/api/monitoring/:id/generate-docx', authCheck, async (req, res) => {
  try {
    const review = (await pool.query('SELECT * FROM monitoring_reviews WHERE id=$1', [req.params.id])).rows[0];
    if (!review) return res.status(404).json({ error: 'Review not found' });

    const fd = review.form_data || {};
    const findings = review.findings || [];
    const center = review.center;
    const cLabel = center === 'niles' ? 'Niles' : 'Peace Boulevard';
    const rooms = CLASSROOMS[center] || [];
    const navy = '1B2A4A';
    const revDate = review.review_date ? new Date(review.review_date).toLocaleDateString() : '';

    const thinB = {top:{style:BorderStyle.SINGLE,size:1,color:'999999'},bottom:{style:BorderStyle.SINGLE,size:1,color:'999999'},left:{style:BorderStyle.SINGLE,size:1,color:'999999'},right:{style:BorderStyle.SINGLE,size:1,color:'999999'}};
    function c(text,opts={}){
      return new TableCell({
        width:opts.w?{size:opts.w,type:WidthType.PERCENTAGE}:undefined,borders:thinB,
        shading:opts.bg?{type:ShadingType.SOLID,color:opts.bg}:undefined,
        children:[new Paragraph({alignment:opts.align||AlignmentType.LEFT,
          children:[new TextRun({text:text||'',bold:opts.bold||false,size:opts.sz||16,font:'Calibri',color:opts.color||'333333'})]})]
      });
    }
    function ynCell(key){
      const v=fd[key];
      // Reverse items: 701,1001,1003 and fiveday discrepancies
      const REV=new Set(['701','1001','1003']);
      const isRev=REV.has(key)||key.includes('_disc');
      const yMark=v==='Y'?'☑':'☐';const nMark=v==='N'?'☑':'☐';const naMark=v==='NA'?'☑':'☐';
      return [c(yMark,{align:AlignmentType.CENTER,sz:14,w:6,bg:v==='Y'?(isRev?'FCE4EC':'E8F5E9'):undefined}),
              c(nMark,{align:AlignmentType.CENTER,sz:14,w:6,bg:v==='N'?(isRev?'E8F5E9':'FCE4EC'):undefined}),
              c(naMark,{align:AlignmentType.CENTER,sz:14,w:6})];
    }
    function secRow(num,text,key){
      return new TableRow({children:[
        c(num,{bold:true,sz:14,w:6}),c(text,{sz:14,w:52}),
        ...ynCell(key),c(fd[key+'_cmt']||'',{sz:12,w:24})
      ]});
    }
    function secHdr(title){
      return new TableRow({children:[
        c(title,{bold:true,bg:navy,color:'FFFFFF',sz:16,w:58}),
        c('Yes',{bold:true,bg:navy,color:'FFFFFF',sz:14,w:6,align:AlignmentType.CENTER}),
        c('No',{bold:true,bg:navy,color:'FFFFFF',sz:14,w:6,align:AlignmentType.CENTER}),
        c('N/A',{bold:true,bg:navy,color:'FFFFFF',sz:14,w:6,align:AlignmentType.CENTER}),
        c('Comments',{bold:true,bg:navy,color:'FFFFFF',sz:14,w:24})
      ]});
    }

    const allRows = [];

    // Page 1: Header + Sections 100-300
    allRows.push(secHdr('Section 100. General Information'));
    allRows.push(secRow('101',"The facility's license is current.",'101'));
    allRows.push(secRow('102','The facility is within its licensed capacity.','102'));
    allRows.push(secRow('103','The facility offers drinking water to participants throughout the day.','103'));
    allRows.push(secHdr('Section 200. Training'));
    allRows.push(secRow('201','NEW FACILITIES/NEW STAFF: Staff have received training prior to CACFP operations.','201'));
    allRows.push(secRow('202','The facility conducted annual CACFP training for all key staff.','202'));
    allRows.push(secRow('203','Sponsor training documentation includes: date(s), location(s), topics, names/signatures.','203'));
    allRows.push(secHdr('Section 300. Civil Rights'));
    allRows.push(secRow('301','No separation by race, color, sex, age, disability or national origin.','301'));
    allRows.push(secRow('302','Potentially eligible persons have equal opportunity to participate in CACFP.','302'));
    allRows.push(secRow('303','Current USDA "And Justice for All" poster is displayed.','303'));
    allRows.push(secRow('304','USDA nondiscrimination statement is on all materials and websites.','304'));
    allRows.push(secRow('305','Front-line staff trained on civil rights and complaint procedures.','305'));
    allRows.push(secHdr('Section 400. Records and Recordkeeping'));
    allRows.push(secRow('401','A daily count is maintained for all meals served to adults.','401'));
    allRows.push(secRow('402','No more than 2 meals/1 snack or 1 meal/2 snacks per participant per day.','402'));
    allRows.push(secRow('405','Meals only claimed for participants within CACFP age requirements.','405'));
    allRows.push(secRow('406','Facility daily attendance records are maintained.','406'));
    allRows.push(secRow('407','Meal attendance is taken at the point of service.','407'));
    allRows.push(secRow('408','Meal attendance records are available and up to date.','408'));
    allRows.push(secHdr('Section 500. Menus'));
    allRows.push(secRow('501','Menu(s) meet program requirements (month, date, components).','501'));
    allRows.push(secRow('502','Menu(s) are available for meals claimed.','502'));
    allRows.push(secRow('503','Nutritional labels/PFS verified for meal pattern requirements.','503'));
    allRows.push(secRow('504','Procedure in place for recording menu substitutions.','504'));
    allRows.push(secRow('505','100% juice limited to one meal/snack per day.','505'));
    allRows.push(secRow('506','At least one serving of grains per day is whole grain-rich.','506'));
    allRows.push(secRow('507','Grain based desserts not served as creditable components.','507'));
    allRows.push(secRow('508','Meat/meat alternate not served more than 3x weekly replacing grain at breakfast.','508'));
    allRows.push(secRow('509','Yogurt ≤ 23g sugar per 6oz.','509'));
    allRows.push(secRow('510','Breakfast cereal ≤ 6g sugar per dry ounce.','510'));
    allRows.push(secRow('511','Lunch/supper: at least 1 vegetable and 1 fruit or 2 vegetables.','511'));
    allRows.push(secRow('512','Unflavored whole milk for children ages 1-2.','512'));
    allRows.push(secRow('513','Unflavored low-fat milk for children ages 2-5.','513'));
    allRows.push(secRow('514','Special Dietary Needs Accommodations forms available.','514'));
    allRows.push(secRow('516','Facility offers formula and developmentally appropriate foods to infants.','516'));
    allRows.push(secRow('517','Infant Formula/Food Sign-off form on file when parent provides formula.','517'));

    // Meal observation questions
    allRows.push(secHdr('Section 600. Meal Observation'));
    allRows.push(secRow('603','Minimum portion served met requirements for age groups.','603'));
    allRows.push(secRow('604','Procedures in place to ensure minimum portions are served.','604'));
    allRows.push(secRow('605','Meal/snack met appropriate meal pattern for components and age.','605'));
    allRows.push(secRow('606','Meal/snack same as posted menu for the day.','606'));
    allRows.push(secRow('607','Meal/snack within approved meal service times.','607'));
    allRows.push(secRow('608','Meal attendance taken at point of service during observation.','608'));
    allRows.push(secRow('609','Appropriate variety of milk served to each age group.','609'));
    allRows.push(secHdr('Section 700. Health and Safety'));
    allRows.push(secRow('701','Were imminent threats to health/safety observed? (Yes=threat found)','701'));
    allRows.push(secHdr('Section 800. Enrollment'));
    allRows.push(secRow('801','Current enrollment documentation on file for each participant.','801'));
    allRows.push(secRow('802','Enrollment forms updated annually.','802'));
    allRows.push(secRow('803','Forms contain: name, dated signature, normal days/hours, meals received.','803'));
    allRows.push(secRow('804','Enrolled participants informed of WIC benefits.','804'));
    allRows.push(secRow('805','Parent Information Sheet distributed to enrolled participants.','805'));
    allRows.push(secHdr('Section 900. Meal Count Reconciliation'));
    allRows.push(secRow('901','Enrollment, attendance, and meal attendance reconcile.','901'));
    allRows.push(secRow('902','Participants present during observation match claimed numbers.','902'));
    allRows.push(secHdr('Section 1000. Previous Reviews'));
    allRows.push(secRow('1001','There were findings from previous review. (Yes=findings exist)','1001'));
    allRows.push(secRow('1002','Findings from previous review were corrected.','1002'));
    allRows.push(secRow('1003','Change to facility administrative staff. (Yes=change occurred)','1003'));

    // Classroom observation table
    const classroomRows = [new TableRow({children:[
      c('Room',{bold:true,bg:'E0E0E0',sz:14}),c('Participants',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
      c('Adults',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),c('POS',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
      c('Milk %',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),c('Comments',{bold:true,bg:'E0E0E0',sz:14})
    ]})];
    for(const rm of rooms){
      const rk='room_'+rm.name.replace(/\s/g,'_');
      classroomRows.push(new TableRow({children:[
        c(rm.name+' ('+rm.ages+')',{sz:13}),
        c(fd[rk+'_parts']||'',{sz:13,align:AlignmentType.CENTER}),
        c(fd[rk+'_adults']||'',{sz:13,align:AlignmentType.CENTER}),
        c(fd[rk+'_pos']||'',{sz:13,align:AlignmentType.CENTER}),
        c(fd[rk+'_milk']||'',{sz:13,align:AlignmentType.CENTER}),
        c(fd[rk+'_cmt']||'',{sz:12})
      ]}));
    }

    // Five-Day Reconciliation table
    const fdayHdr = new TableRow({children:[
      c('Day',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
      c('Date',{bold:true,bg:'E0E0E0',sz:14}),
      c('# Attend',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
      c('Bkfst MC',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
      c('AM Snk',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
      c('Lunch MC',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
      c('PM Snk',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
      c('Discrep?',{bold:true,bg:'E0E0E0',sz:14,align:AlignmentType.CENTER}),
    ]});
    const fdayRows=[fdayHdr];
    for(let d=1;d<=5;d++){
      const dk='fiveday_'+d;
      const disc=fd[dk+'_disc'];
      fdayRows.push(new TableRow({children:[
        c(String(d),{bold:true,sz:14,align:AlignmentType.CENTER}),
        c(fd[dk+'_date']||'',{sz:13}),
        c(fd[dk+'_att']||'',{sz:13,align:AlignmentType.CENTER}),
        c(fd[dk+'_bkfst']||'',{sz:13,align:AlignmentType.CENTER}),
        c(fd[dk+'_amsnk']||'',{sz:13,align:AlignmentType.CENTER}),
        c(fd[dk+'_lunch']||'',{sz:13,align:AlignmentType.CENTER}),
        c(fd[dk+'_pmsnk']||'',{sz:13,align:AlignmentType.CENTER}),
        c(disc==='Y'?'Yes':disc==='N'?'No':'',{sz:13,align:AlignmentType.CENTER,bg:disc==='Y'?'FCE4EC':undefined}),
      ]}));
    }
    if(fd.fiveday_verified){
      fdayRows.push(new TableRow({children:[
        c(`Verified by: ${fd.fiveday_verified_initials||''}`,{bold:true,bg:'E8F5E9',sz:13}),
        c('',{}),c('',{}),c('',{}),c('',{}),c('',{}),c('',{}),c('',{})
      ]}));
    }

    // Findings summary
    const findingsParas = [];
    if(findings.length===0){
      findingsParas.push(new Paragraph({spacing:{after:60},children:[
        new TextRun({text:'☑ No Finding(s)',bold:true,size:18,font:'Calibri',color:'2E7D32'})]}));
    }else{
      findingsParas.push(new Paragraph({spacing:{after:60},children:[
        new TextRun({text:'☑ Corrective action by site is required',bold:true,size:18,font:'Calibri',color:'C62828'})]}));
      for(const f of findings){
        findingsParas.push(new Paragraph({spacing:{after:40},children:[
          new TextRun({text:`• Item ${f.item}: `,bold:true,size:16,font:'Calibri'}),
          new TextRun({text:f.comment||'No comment',size:16,font:'Calibri'})]}));
      }
    }
    if(fd.findings_text){
      findingsParas.push(new Paragraph({spacing:{before:100,after:60},children:[
        new TextRun({text:'Additional Findings & Recommendations:',bold:true,size:16,font:'Calibri'})]}));
      findingsParas.push(new Paragraph({spacing:{after:60},children:[
        new TextRun({text:fd.findings_text,size:16,font:'Calibri'})]}));
    }

    const doc = new Document({
      sections: [{
        properties:{page:{margin:{top:500,bottom:500,left:600,right:600}}},
        children:[
          // Header
          new Paragraph({alignment:AlignmentType.CENTER,spacing:{after:40},children:[
            new TextRun({text:'Child and Adult Care Food Program',bold:true,size:22,font:'Calibri',color:navy})]}),
          new Paragraph({alignment:AlignmentType.CENTER,spacing:{after:80},children:[
            new TextRun({text:'Monitoring Review for Sponsored Facilities',size:18,font:'Calibri',color:'666666'})]}),
          new Paragraph({spacing:{after:40},children:[
            new TextRun({text:`${review.announced?'☑':'☐'} Announced   ${!review.announced?'☑':'☐'} Unannounced`,size:16,font:'Calibri'}),
            new TextRun({text:`          Meal Observed: ${review.meal_observed||''}`,size:16,font:'Calibri'})]}),
          new Paragraph({spacing:{after:40},children:[
            new TextRun({text:`Sponsor: The Children's Center, Inc.  #990004457`,size:16,font:'Calibri'}),
            new TextRun({text:`     Date: ${revDate}`,size:16,font:'Calibri'}),
            new TextRun({text:`     Arrival: ${fd.arrival_time||''}`,size:16,font:'Calibri'})]}),
          new Paragraph({spacing:{after:80},children:[
            new TextRun({text:`Facility: ${cLabel}`,size:16,font:'Calibri'})]}),

          // Main review table
          new Paragraph({spacing:{after:40},children:[
            new TextRun({text:'REVIEW AREAS',bold:true,size:20,font:'Calibri',color:navy})]}),
          new Table({width:{size:100,type:WidthType.PERCENTAGE},rows:allRows}),

          // Classroom observation
          new Paragraph({spacing:{before:200,after:60},children:[
            new TextRun({text:'Meal Observation — Classroom Detail',bold:true,size:18,font:'Calibri',color:navy})]}),
          new Table({width:{size:100,type:WidthType.PERCENTAGE},rows:classroomRows}),

          // Five-Day Reconciliation
          new Paragraph({spacing:{before:200,after:60},children:[
            new TextRun({text:'Five-Day Aggregate Meal Count Reconciliation',bold:true,size:18,font:'Calibri',color:navy})]}),
          new Table({width:{size:100,type:WidthType.PERCENTAGE},rows:fdayRows}),

          // Findings
          new Paragraph({spacing:{before:200,after:60},children:[
            new TextRun({text:'Findings and Recommendations for Corrective Action',bold:true,size:18,font:'Calibri',color:navy})]}),
          ...findingsParas,

          // Signatures
          new Paragraph({spacing:{before:200,after:40},children:[
            new TextRun({text:'Monitor Signature: ',bold:true,size:16,font:'Calibri'}),
            new TextRun({text:fd.monitor_sig||'________________',italics:!!fd.monitor_sig,size:16,font:'Calibri',underline:{}}),
            new TextRun({text:`     Date: ${fd.sig_date||revDate}`,size:16,font:'Calibri'}),
            new TextRun({text:`     Departure: ${fd.departure_time||''}`,size:16,font:'Calibri'})]}),
          new Paragraph({spacing:{after:40},children:[
            new TextRun({text:'Site Representative: ',bold:true,size:16,font:'Calibri'}),
            new TextRun({text:fd.site_rep_sig||'________________',italics:!!fd.site_rep_sig,size:16,font:'Calibri',underline:{}}),
            new TextRun({text:`     Date: ${fd.sig_date||revDate}`,size:16,font:'Calibri'})]}),
          new Paragraph({spacing:{before:60},alignment:AlignmentType.RIGHT,children:[
            new TextRun({text:'Rev. 3/2023',size:12,font:'Calibri',color:'999999'})]}),
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    const filename = `Monitoring_Review_${center}_${revDate.replace(/\//g,'-')}.docx`;

    // Store in documents
    const fyId = review.fiscal_year_id;
    const mk = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'][new Date(review.review_date).getMonth()];
    await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,$2,'monitoring_review',$3,'application/vnd.openxmlformats-officedocument.wordprocessingml.document',$4,$5)`,
      [fyId, mk, filename, buffer, JSON.stringify({review_id:review.id,center,date:revDate})]
    );

    res.setHeader('Content-Disposition',`attachment; filename="${filename}"`);
    res.setHeader('Content-Type','application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch(e){ console.error(e); res.status(500).json({error:e.message}); }
});

// Get pre-populated data for a monitoring review (pulls from CACFP suite data)
app.get('/api/monitoring/:id/prefill', authCheck, async (req, res) => {
  try {
    const review = (await pool.query('SELECT * FROM monitoring_reviews WHERE id=$1', [req.params.id])).rows[0];
    if (!review) return res.status(404).json({ error: 'Not found' });

    const center = review.center;
    const fyId = review.fiscal_year_id;

    // Get previous month's data for five-day reconciliation
    const reviewDate = new Date(review.review_date);
    const prevMonth = reviewDate.getMonth() === 0 ? 11 : reviewDate.getMonth() - 1;
    const prevMonthKeys = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
    const prevMK = prevMonthKeys[prevMonth];

    // Also try the SAME month as the review (monitor might be reviewing current month data)
    const sameMK = prevMonthKeys[reviewDate.getMonth()];

    // Try previous month first, fall back to same month
    let attData = {};
    let mealData = {};
    let usedMonth = prevMK;

    for (const mk of [prevMK, sameMK]) {
      const attRes = await pool.query(
        `SELECT * FROM monthly_data WHERE fiscal_year_id=$1 AND month_key=$2 AND data_type='attendance'`,
        [fyId, mk]
      );
      if (attRes.rows.length > 0 && attRes.rows[0].data?.[center]) {
        attData = attRes.rows[0].data[center];
        usedMonth = mk;
        break;
      }
    }

    for (const mk of [prevMK, sameMK]) {
      const mealRes = await pool.query(
        `SELECT * FROM monthly_data WHERE fiscal_year_id=$1 AND month_key=$2 AND data_type='meals'`,
        [fyId, mk]
      );
      if (mealRes.rows.length > 0 && mealRes.rows[0].data?.[center]) {
        mealData = mealRes.rows[0].data[center];
        break;
      }
    }

    // Check what monthly_data exists for debugging
    const allMD = await pool.query(
      `SELECT month_key, data_type, 
       CASE WHEN data->$2 IS NOT NULL THEN true ELSE false END as has_center_data
       FROM monthly_data WHERE fiscal_year_id=$1 ORDER BY month_key`,
      [fyId, center]
    );

    const enrolled = attData.enrolled || 0;
    const hasDailyTotals = !!(attData.dailyTotals && attData.dailyTotals.length > 0);
    const hasDayHeaders = !!(attData.dayHeaders && attData.dayHeaders.length > 0);

    res.json({
      center,
      classrooms: CLASSROOMS[center] || [],
      enrollment: enrolled,
      attendance: attData,
      meals: mealData,
      prevMonth: usedMonth,
      sponsorName: "The Children's Center, Inc.",
      agreementNum: '990004457',
      _debug: {
        reviewDate: review.review_date,
        triedMonths: [prevMK, sameMK],
        usedMonth,
        hasDailyTotals,
        hasDayHeaders,
        dayHeadersSample: (attData.dayHeaders || []).slice(0, 5),
        dailyTotalsSample: (attData.dailyTotals || []).slice(0, 5),
        mealDataKeys: Object.keys(mealData),
        availableData: allMD.rows
      }
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── ADULT MEAL TOTALS ────────────────────────────────────
app.get('/api/adult-meals', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, month_key } = req.query;
    // Get daily adult meal counts aggregated by day
    const { rows } = await pool.query(
      `SELECT d.day_of_month, COUNT(*) as adult_count, 
       ARRAY_AGG(s.name) as staff_names
       FROM daily_cacfp_entries d
       JOIN staff s ON s.id = d.staff_id
       WHERE d.fiscal_year_id = $1 AND d.month_key = $2 AND d.adult_meal = true
       GROUP BY d.day_of_month ORDER BY d.day_of_month`,
      [fiscal_year_id, month_key]
    );
    // Also get total for the month
    const totalRes = await pool.query(
      `SELECT COUNT(*) as total FROM daily_cacfp_entries
       WHERE fiscal_year_id = $1 AND month_key = $2 AND adult_meal = true`,
      [fiscal_year_id, month_key]
    );
    res.json({ daily: rows, total: parseInt(totalRes.rows[0].total) || 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── TRAINING RECORDS ─────────────────────────────────────
app.get('/api/training', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id } = req.query;
    const q = fiscal_year_id
      ? 'SELECT * FROM training_records WHERE fiscal_year_id=$1 ORDER BY training_date DESC'
      : 'SELECT * FROM training_records ORDER BY training_date DESC';
    const { rows } = await pool.query(q, fiscal_year_id ? [fiscal_year_id] : []);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/training', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id, training_date, training_type, topic, location, center, trainer, attendees, notes } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO training_records (fiscal_year_id, training_date, training_type, topic, location, center, trainer, attendees, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [fiscal_year_id, training_date, training_type, topic, location || '', center || 'both', trainer || '', JSON.stringify(attendees || []), notes || '']
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/training/:id', authCheck, async (req, res) => {
  try {
    const { training_date, training_type, topic, location, center, trainer, attendees, notes } = req.body;
    const { rows } = await pool.query(
      `UPDATE training_records SET training_date=$1, training_type=$2, topic=$3, location=$4, center=$5, trainer=$6, attendees=$7, notes=$8
       WHERE id=$9 RETURNING *`,
      [training_date, training_type, topic, location || '', center || 'both', trainer || '', JSON.stringify(attendees || []), notes || '', req.params.id]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/training/:id', authCheck, async (req, res) => {
  try {
    await pool.query('DELETE FROM training_records WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Generate training documentation report
app.post('/api/generate-training-report', authCheck, async (req, res) => {
  try {
    const { fiscal_year_id } = req.body;
    const fyRes = await pool.query('SELECT * FROM fiscal_years WHERE id=$1', [fiscal_year_id]);
    const fy = fyRes.rows[0]; if (!fy) return res.status(404).json({ error: 'FY not found' });
    const { rows } = await pool.query('SELECT * FROM training_records WHERE fiscal_year_id=$1 ORDER BY training_date', [fiscal_year_id]);
    const navy = '1B2A4A';

    const border = { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' };
    const borders = { top: border, bottom: border, left: border, right: border };
    const cm = { top: 80, bottom: 80, left: 120, right: 120 };
    function cell(text, opts = {}) {
      return new TableCell({ borders, margins: cm,
        width: opts.w ? { size: opts.w, type: WidthType.DXA } : undefined,
        shading: opts.bg ? { type: ShadingType.CLEAR, fill: opts.bg } : undefined,
        children: [new Paragraph({ alignment: opts.align || AlignmentType.LEFT,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 18, font: 'Arial', color: opts.color || '333333' })] })] });
    }

    const tableWidth = 13680; // landscape content width
    const colWidths = [1400, 1600, 3000, 1800, 2000, 3880];
    const hdr = new TableRow({ children: [
      cell('Date', { bold: true, bg: navy, color: 'FFFFFF', w: colWidths[0] }),
      cell('Type', { bold: true, bg: navy, color: 'FFFFFF', w: colWidths[1] }),
      cell('Topic', { bold: true, bg: navy, color: 'FFFFFF', w: colWidths[2] }),
      cell('Location', { bold: true, bg: navy, color: 'FFFFFF', w: colWidths[3] }),
      cell('Trainer', { bold: true, bg: navy, color: 'FFFFFF', w: colWidths[4] }),
      cell('Attendees', { bold: true, bg: navy, color: 'FFFFFF', w: colWidths[5] }),
    ] });
    const dataRows = rows.map((r, i) => new TableRow({ children: [
      cell(new Date(r.training_date).toLocaleDateString(), { w: colWidths[0], bg: i % 2 ? 'F5F5F5' : undefined }),
      cell(r.training_type, { w: colWidths[1], bg: i % 2 ? 'F5F5F5' : undefined }),
      cell(r.topic, { w: colWidths[2], bg: i % 2 ? 'F5F5F5' : undefined }),
      cell(r.location, { w: colWidths[3], bg: i % 2 ? 'F5F5F5' : undefined }),
      cell(r.trainer, { w: colWidths[4], bg: i % 2 ? 'F5F5F5' : undefined }),
      cell((r.attendees || []).join(', '), { w: colWidths[5], sz: 14, bg: i % 2 ? 'F5F5F5' : undefined }),
    ] }));

    const cacfpCount = rows.filter(r => r.training_type === 'CACFP').length;
    const crCount = rows.filter(r => r.training_type === 'Civil Rights').length;

    const doc = new Document({
      sections: [{
        properties: { page: { size: { width: 12240, height: 15840, orientation: 'landscape' }, margin: { top: 720, bottom: 720, left: 1080, right: 1080 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
            new TextRun({ text: "The Children's Center, Inc.", bold: true, size: 28, font: 'Arial', color: navy }) ] }),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 }, children: [
            new TextRun({ text: `CACFP Training Documentation — FY ${fy.label}`, size: 22, font: 'Arial', color: '666666' }) ] }),
          new Paragraph({ spacing: { after: 100 }, children: [
            new TextRun({ text: `Total sessions: ${rows.length} | CACFP: ${cacfpCount} | Civil Rights: ${crCount} | Generated: ${new Date().toLocaleDateString()}`, size: 16, font: 'Arial', color: '999999' }) ] }),
          new Table({ width: { size: tableWidth, type: WidthType.DXA }, columnWidths: colWidths, rows: [hdr, ...dataRows] }),
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    const filename = `Training_Documentation_${fy.label}.docx`;
    await pool.query(
      `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
       VALUES ($1,'annual','training_report',$2,'application/vnd.openxmlformats-officedocument.wordprocessingml.document',$3,$4)`,
      [fiscal_year_id, filename, buffer, JSON.stringify({ sessions: rows.length })]
    );
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// ── CACFP MEAL PATTERN REQUIREMENTS ──────────────────────
const MEAL_PATTERNS = {
  'Infants 0-5mo': {
    breakfast: { components: ['Iron-fortified formula or breast milk: 4-6 fl oz'] },
    lunch: { components: ['Iron-fortified formula or breast milk: 4-6 fl oz'] },
    snack: { components: ['Iron-fortified formula or breast milk: 4-6 fl oz'] }
  },
  'Infants 6-11mo': {
    breakfast: { components: ['Iron-fortified formula or breast milk: 6-8 fl oz', 'Iron-fortified infant cereal: 0-4 tbsp (as developmentally ready)'] },
    lunch: { components: ['Iron-fortified formula or breast milk: 6-8 fl oz', 'Iron-fortified infant cereal and/or meat/meat alt: 0-4 tbsp', 'Vegetable, fruit, or both: 0-4 tbsp'] },
    snack: { components: ['Iron-fortified formula or breast milk: 2-4 fl oz', 'Crackers/bread or infant cereal: 0-½ slice or 0-4 tbsp'] }
  },
  'Children 1-2yr': {
    breakfast: { components: ['Milk (unflavored whole): ½ cup', 'Grains: ½ slice bread or ¼ cup cereal', 'Fruit, vegetable, or both: ¼ cup'] },
    lunch: { components: ['Milk (unflavored whole): ½ cup', 'Meat/meat alternate: 1 oz', 'Vegetable: ⅛ cup', 'Fruit: ⅛ cup', 'Grains: ½ slice bread'] },
    snack: { components: ['Select 2 of 5 components:', 'Milk: ½ cup', 'Meat/meat alternate: ½ oz', 'Vegetable, fruit, or both: ½ cup', 'Grains: ½ slice bread'] }
  },
  'Children 3-5yr': {
    breakfast: { components: ['Milk (unflavored low-fat or fat-free): ¾ cup', 'Grains: ½ slice bread or ⅓ cup cereal', 'Fruit, vegetable, or both: ½ cup'] },
    lunch: { components: ['Milk (unflavored low-fat or fat-free): ¾ cup', 'Meat/meat alternate: 1½ oz', 'Vegetable: ¼ cup', 'Fruit: ¼ cup', 'Grains: ½ slice bread'] },
    snack: { components: ['Select 2 of 5 components:', 'Milk: ½ cup', 'Meat/meat alternate: ½ oz', 'Vegetable, fruit, or both: ½ cup', 'Grains: ½ slice bread'] }
  },
  'Children 6-12yr': {
    breakfast: { components: ['Milk (unflavored or flavored low-fat/fat-free): 1 cup', 'Grains: 1 slice bread or ¾ cup cereal', 'Fruit, vegetable, or both: ½ cup'] },
    lunch: { components: ['Milk (unflavored or flavored low-fat/fat-free): 1 cup', 'Meat/meat alternate: 2 oz', 'Vegetable: ½ cup', 'Fruit: ¼ cup', 'Grains: 1 slice bread'] },
    snack: { components: ['Select 2 of 5 components:', 'Milk: 1 cup', 'Meat/meat alternate: 1 oz', 'Vegetable, fruit, or both: ¾ cup', 'Grains: 1 slice bread'] }
  }
};

// Map classroom age groups to meal pattern categories
const CLASSROOM_AGE_MAP = {
  'Infants': ['Infants 0-5mo', 'Infants 6-11mo'],
  'Infants/Toddlers': ['Infants 6-11mo', 'Children 1-2yr'],
  'Toddlers': ['Children 1-2yr'],
  '2s': ['Children 1-2yr'],
  '2½': ['Children 1-2yr', 'Children 3-5yr'],
  '2s and 3s': ['Children 1-2yr', 'Children 3-5yr'],
  '3s': ['Children 3-5yr'],
  'Multi-age 2½-4': ['Children 1-2yr', 'Children 3-5yr'],
  'Multi-age/School-age': ['Children 3-5yr', 'Children 6-12yr'],
  '4s and 5s': ['Children 3-5yr'],
  'School-age': ['Children 6-12yr']
};

// Generate classroom portion poster .docx
app.post('/api/generate-portion-poster', authCheck, async (req, res) => {
  try {
    const { center, classroom_name, classroom_ages } = req.body;
    const ageGroups = CLASSROOM_AGE_MAP[classroom_ages] || ['Children 3-5yr'];
    const navy = '1B2A4A'; const gold = 'C5972C';

    const border = { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' };
    const borders = { top: border, bottom: border, left: border, right: border };
    const cm = { top: 80, bottom: 80, left: 120, right: 120 };

    function cell(text, opts = {}) {
      return new TableCell({
        borders, margins: cm,
        width: opts.w ? { size: opts.w, type: WidthType.DXA } : undefined,
        shading: opts.bg ? { type: ShadingType.CLEAR, fill: opts.bg } : undefined,
        children: [new Paragraph({ alignment: opts.align || AlignmentType.LEFT,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 22, font: 'Arial', color: opts.color || '333333' })] })]
      });
    }

    const sections = [];
    for (const ageGroup of ageGroups) {
      const pattern = MEAL_PATTERNS[ageGroup];
      if (!pattern) continue;

      const tableWidth = 9360;
      const rows = [];

      // Header
      rows.push(new TableRow({ children: [
        cell('Meal', { bold: true, bg: navy, color: 'FFFFFF', w: 1800, sz: 24 }),
        cell('Required Components & Minimum Portions', { bold: true, bg: navy, color: 'FFFFFF', w: 7560, sz: 24 }),
      ] }));

      for (const [meal, data] of Object.entries(pattern)) {
        const mealLabel = meal.charAt(0).toUpperCase() + meal.slice(1);
        const bg = meal === 'breakfast' ? 'FFF8E1' : meal === 'lunch' ? 'E8F5E9' : 'E3F2FD';
        for (let i = 0; i < data.components.length; i++) {
          rows.push(new TableRow({ children: [
            cell(i === 0 ? mealLabel : '', { bold: i === 0, bg: i === 0 ? bg : undefined, w: 1800, sz: 22 }),
            cell(data.components[i], { w: 7560, sz: 22, bg: i === 0 ? bg : undefined }),
          ] }));
        }
      }

      sections.push({
        properties: { page: { size: { width: 12240, height: 15840 }, margin: { top: 720, bottom: 720, left: 1440, right: 1440 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 }, children: [
            new TextRun({ text: "The Children's Center", bold: true, size: 32, font: 'Arial', color: navy }) ] }),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
            new TextRun({ text: 'CACFP Meal Pattern Requirements', size: 28, font: 'Arial', color: gold }) ] }),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 }, children: [
            new TextRun({ text: `${classroom_name} — ${ageGroup}`, bold: true, size: 36, font: 'Arial', color: navy }) ] }),
          new Table({ width: { size: tableWidth, type: WidthType.DXA }, columnWidths: [1800, 7560], rows }),
          new Paragraph({ spacing: { before: 200 }, alignment: AlignmentType.CENTER, children: [
            new TextRun({ text: 'Ensure all components are served in the minimum portions listed above.', size: 20, font: 'Arial', color: '666666', italics: true }) ] }),
          new Paragraph({ spacing: { before: 60 }, alignment: AlignmentType.CENTER, children: [
            new TextRun({ text: 'Questions? Contact your CACFP coordinator.', size: 18, font: 'Arial', color: '999999' }) ] }),
        ]
      });
    }

    if (sections.length === 0) return res.status(400).json({ error: 'No meal patterns found for this age group' });

    const doc = new Document({ sections });
    const buffer = await Packer.toBuffer(doc);
    const filename = `Portion_Poster_${classroom_name.replace(/\s/g, '_')}.docx`;

    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// Generate all classroom posters for a center
app.post('/api/generate-all-posters', authCheck, async (req, res) => {
  try {
    const { center } = req.body;
    const rooms = CLASSROOMS[center] || [];
    const navy = '1B2A4A'; const gold = 'C5972C';

    const border = { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' };
    const borders = { top: border, bottom: border, left: border, right: border };
    const cm = { top: 80, bottom: 80, left: 120, right: 120 };
    function cell(text, opts = {}) {
      return new TableCell({
        borders, margins: cm,
        width: opts.w ? { size: opts.w, type: WidthType.DXA } : undefined,
        shading: opts.bg ? { type: ShadingType.CLEAR, fill: opts.bg } : undefined,
        children: [new Paragraph({ alignment: opts.align || AlignmentType.LEFT,
          children: [new TextRun({ text: text || '', bold: opts.bold || false, size: opts.sz || 22, font: 'Arial', color: opts.color || '333333' })] })]
      });
    }

    const sections = [];
    for (const room of rooms) {
      const ageGroups = CLASSROOM_AGE_MAP[room.ages] || ['Children 3-5yr'];
      for (const ageGroup of ageGroups) {
        const pattern = MEAL_PATTERNS[ageGroup];
        if (!pattern) continue;
        const tableWidth = 9360;
        const rows = [new TableRow({ children: [
          cell('Meal', { bold: true, bg: navy, color: 'FFFFFF', w: 1800, sz: 24 }),
          cell('Required Components & Minimum Portions', { bold: true, bg: navy, color: 'FFFFFF', w: 7560, sz: 24 }),
        ] })];
        for (const [meal, data] of Object.entries(pattern)) {
          const mealLabel = meal.charAt(0).toUpperCase() + meal.slice(1);
          const bg = meal === 'breakfast' ? 'FFF8E1' : meal === 'lunch' ? 'E8F5E9' : 'E3F2FD';
          for (let i = 0; i < data.components.length; i++) {
            rows.push(new TableRow({ children: [
              cell(i === 0 ? mealLabel : '', { bold: i === 0, bg: i === 0 ? bg : undefined, w: 1800, sz: 22 }),
              cell(data.components[i], { w: 7560, sz: 22, bg: i === 0 ? bg : undefined }),
            ] }));
          }
        }
        sections.push({
          properties: { page: { size: { width: 12240, height: 15840 }, margin: { top: 720, bottom: 720, left: 1440, right: 1440 } } },
          children: [
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 }, children: [
              new TextRun({ text: "The Children's Center", bold: true, size: 32, font: 'Arial', color: navy }) ] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 }, children: [
              new TextRun({ text: 'CACFP Meal Pattern Requirements', size: 28, font: 'Arial', color: gold }) ] }),
            new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 }, children: [
              new TextRun({ text: `${room.name} — ${ageGroup}`, bold: true, size: 36, font: 'Arial', color: navy }) ] }),
            new Table({ width: { size: tableWidth, type: WidthType.DXA }, columnWidths: [1800, 7560], rows }),
            new Paragraph({ spacing: { before: 200 }, alignment: AlignmentType.CENTER, children: [
              new TextRun({ text: 'Ensure all components are served in the minimum portions listed above.', size: 20, font: 'Arial', color: '666666', italics: true }) ] }),
          ]
        });
      }
    }

    const doc = new Document({ sections });
    const buffer = await Packer.toBuffer(doc);
    const cLabel = center === 'niles' ? 'Niles' : 'Peace';
    const filename = `All_Portion_Posters_${cLabel}.docx`;
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.send(buffer);
  } catch (e) { console.error(e); res.status(500).json({ error: e.message }); }
});

// Get meal pattern requirements for a classroom (for monitoring worksheet)
app.get('/api/meal-patterns/:ages', authCheck, (req, res) => {
  const ageGroups = CLASSROOM_AGE_MAP[req.params.ages] || ['Children 3-5yr'];
  const patterns = {};
  for (const ag of ageGroups) {
    if (MEAL_PATTERNS[ag]) patterns[ag] = MEAL_PATTERNS[ag];
  }
  res.json(patterns);
});

// ── MERGE DUPLICATE STAFF ─────────────────────────────────
app.post('/api/staff/merge', authCheck, async (req, res) => {
  try {
    const { keep_id, remove_id } = req.body;
    if (!keep_id || !remove_id) return res.status(400).json({ error: 'Need keep_id and remove_id' });
    if (keep_id === remove_id) return res.status(400).json({ error: 'Cannot merge with self' });

    // Move all data from remove_id to keep_id
    await pool.query('UPDATE daily_cacfp_entries SET staff_id=$1 WHERE staff_id=$2', [keep_id, remove_id]);
    await pool.query('UPDATE playground_staff_hours SET staff_id=$1 WHERE staff_id=$2 AND NOT EXISTS (SELECT 1 FROM playground_staff_hours p2 WHERE p2.staff_id=$1 AND p2.fiscal_year_id=playground_staff_hours.fiscal_year_id AND p2.month_key=playground_staff_hours.month_key AND p2.day_of_month=playground_staff_hours.day_of_month)', [keep_id, remove_id]);
    await pool.query('UPDATE monthly_signatures SET staff_id=$1 WHERE staff_id=$2 AND NOT EXISTS (SELECT 1 FROM monthly_signatures m2 WHERE m2.staff_id=$1 AND m2.fiscal_year_id=monthly_signatures.fiscal_year_id AND m2.month_key=monthly_signatures.month_key)', [keep_id, remove_id]);
    await pool.query('UPDATE staff_time_entries SET staff_id=$1 WHERE staff_id=$2 AND NOT EXISTS (SELECT 1 FROM staff_time_entries s2 WHERE s2.staff_id=$1 AND s2.fiscal_year_id=staff_time_entries.fiscal_year_id AND s2.month_key=staff_time_entries.month_key)', [keep_id, remove_id]);

    // Delete remaining duplicates that couldn't be moved
    await pool.query('DELETE FROM playground_staff_hours WHERE staff_id=$1', [remove_id]);
    await pool.query('DELETE FROM monthly_signatures WHERE staff_id=$1', [remove_id]);
    await pool.query('DELETE FROM staff_time_entries WHERE staff_id=$1', [remove_id]);
    await pool.query('DELETE FROM daily_cacfp_entries WHERE staff_id=$1', [remove_id]);
    await pool.query('DELETE FROM staff_pins WHERE staff_id=$1', [remove_id]);
    await pool.query('DELETE FROM staff WHERE id=$1', [remove_id]);

    const kept = (await pool.query('SELECT * FROM staff WHERE id=$1', [keep_id])).rows[0];
    res.json({ ok: true, kept });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

initDB().then(() => {
  return initMonitoringTables();
}).then(() => {
  app.listen(PORT, () => console.log(`🍽️ TCC CACFP Suite v4 running on port ${PORT}`));
}).catch(err => {
  console.error('DB init error:', err);
  app.listen(PORT, () => console.log(`🍽️ TCC CACFP Suite v4 running on port ${PORT} (DB init failed)`));
});
