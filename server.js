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
initDB().then(() => {
  app.listen(PORT, () => console.log(`🍽️ TCC CACFP Suite v4 running on port ${PORT}`));
}).catch(err => {
  console.error('DB init error:', err);
  app.listen(PORT, () => console.log(`🍽️ TCC CACFP Suite v4 running on port ${PORT} (DB init failed)`));
});
