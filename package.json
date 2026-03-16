const express = require('express');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
        AlignmentType, BorderStyle, WidthType, ShadingType } = require('docx');

const app = express();
const PORT = process.env.PORT || 3000;
const ACCESS_PIN = process.env.ACCESS_PIN || '2024tcc';

// ── DATABASE ──────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS cacfp_data (
        id SERIAL PRIMARY KEY,
        data_key VARCHAR(255) UNIQUE NOT NULL,
        data_value JSONB NOT NULL,
        updated_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS cacfp_uploads (
        id SERIAL PRIMARY KEY,
        upload_key VARCHAR(255) NOT NULL,
        original_name VARCHAR(500),
        stored_path VARCHAR(500),
        upload_type VARCHAR(100),
        month_key VARCHAR(50),
        center VARCHAR(50),
        uploaded_at TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('Database ready');
  } catch (err) { console.error('DB init error:', err.message); }
}

async function dbGet(key) {
  try {
    const res = await pool.query('SELECT data_value FROM cacfp_data WHERE data_key=$1', [key]);
    return res.rows[0] ? res.rows[0].data_value : null;
  } catch (e) { return null; }
}

async function dbSet(key, value) {
  try {
    await pool.query(
      `INSERT INTO cacfp_data (data_key, data_value, updated_at) VALUES ($1,$2,NOW())
       ON CONFLICT (data_key) DO UPDATE SET data_value=$2, updated_at=NOW()`,
      [key, JSON.stringify(value)]
    );
    return true;
  } catch (e) { console.error('dbSet:', e.message); return false; }
}

// ── MIDDLEWARE ────────────────────────────────────────────
app.use(express.json({ limit: '20mb' }));
app.use(express.static('public'));

function requirePIN(req, res, next) {
  const pin = req.headers['x-pin'] || req.body?.pin || req.query?.pin;
  if (pin === ACCESS_PIN) return next();
  res.status(401).json({ error: 'Invalid PIN' });
}

// ── FILE UPLOAD ───────────────────────────────────────────
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const dir = 'uploads/';
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    cb(null, dir);
  },
  filename: (req, file, cb) => { cb(null, Date.now() + '-' + file.originalname); }
});
const upload = multer({ storage, limits: { fileSize: 20 * 1024 * 1024 } });

// ── AUTH ──────────────────────────────────────────────────
app.post('/api/auth', (req, res) => {
  if (req.body.pin === ACCESS_PIN) res.json({ success: true });
  else res.status(401).json({ error: 'Incorrect PIN. Please try again.' });
});

// ── CORE DATA ─────────────────────────────────────────────
app.get('/api/data', requirePIN, async (req, res) => {
  const data = await dbGet('app_state');
  res.json(data || {});
});

app.post('/api/data', requirePIN, async (req, res) => {
  const ok = await dbSet('app_state', req.body);
  res.json({ success: ok });
});

// ── PARSE CACFP CSV ───────────────────────────────────────
app.post('/api/parse-cacfp-csv', requirePIN, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  try {
    const content = fs.readFileSync(req.file.path, 'utf-8').replace(/^\uFEFF/, '');
    const lines = content.split('\n');

    // Parse header for dates
    const headerLine = lines[2] || lines[1];
    const headers = headerLine.split(',');
    const dateLabels = headers.slice(4).map(h => h.trim().replace(/\r/g,''));
    const nDates = dateLabels.length;

    const MEAL_ORDER = ['Breakfast','AM snack','Lunch','PM snack','Dinner','Not specified'];

    // Parse students
    const students = [];
    let i = 3;
    while (i < lines.length) {
      const line = lines[i].trim();
      if (!line) { i++; continue; }
      const row = parseCSVLine(lines[i]);
      if (row.length === 4 && row[0].trim() && !row[0].includes('School Name') &&
          !MEAL_ORDER.includes(row[3].trim())) {
        const first = row[0].trim(), last = row[1]?.trim() || '', classroom = row[3]?.trim() || '';
        let eligibility = '', meals = {};
        let j = i + 1, mFound = 0;
        while (j < lines.length && mFound < MEAL_ORDER.length) {
          const mline = lines[j].trim();
          if (!mline) { j++; continue; }
          const mrow = parseCSVLine(lines[j]);
          if (mrow.length > 3 && MEAL_ORDER.includes(mrow[3].trim())) {
            const mt = mrow[3].trim();
            if (mFound === 0 && ['Paid','Free','Reduced'].includes(mrow[0].trim()))
              eligibility = mrow[0].trim();
            const days = mrow.slice(4, 4 + nDates).map(d => d.trim() === 'X');
            while (days.length < nDates) days.push(false);
            meals[mt] = days;
            mFound++; j++;
          } else break;
        }
        students.push({ first, last, classroom, eligibility, meals });
        i = j;
      } else i++;
    }

    // Apply 3-meal rule & calculate totals
    const catMap = { 'Free': 'A', 'Reduced': 'B', 'Paid': 'C', '': 'C' };
    const mealTypes = ['Breakfast','AM snack','Lunch','PM snack'];
    const daily = {};
    mealTypes.forEach(mt => { daily[mt] = { A:[...Array(nDates)].map(()=>0), B:[...Array(nDates)].map(()=>0), C:[...Array(nDates)].map(()=>0) }; });

    // Track exclusions for annotated report
    const exclusions = {};
    students.forEach(s => {
      exclusions[s.first+'_'+s.last] = {};
      for (let d = 0; d < nDates; d++) {
        const served = mealTypes.filter(mt => s.meals[mt]?.[d]);
        if (served.length > 3) {
          const excl = new Set();
          let toExcl = served.slice();
          while (toExcl.filter(m => !excl.has(m)).length > 3) {
            for (const c of ['PM snack','AM snack','Breakfast']) {
              if (toExcl.includes(c) && !excl.has(c)) { excl.add(c); break; }
            }
          }
          exclusions[s.first+'_'+s.last][d] = [...excl];
        }
      }
    });

    students.forEach(s => {
      const cat = catMap[s.eligibility] || 'C';
      const excl = exclusions[s.first+'_'+s.last];
      for (let d = 0; d < nDates; d++) {
        const dayExcl = new Set(excl[d] || []);
        mealTypes.forEach(mt => {
          if (s.meals[mt]?.[d] && !dayExcl.has(mt)) daily[mt][cat][d]++;
        });
      }
    });

    // Monthly totals
    const monthly = {};
    mealTypes.forEach(mt => {
      monthly[mt] = {
        A: daily[mt].A.reduce((a,b)=>a+b,0),
        B: daily[mt].B.reduce((a,b)=>a+b,0),
        C: daily[mt].C.reduce((a,b)=>a+b,0),
      };
      monthly[mt].Total = monthly[mt].A + monthly[mt].B + monthly[mt].C;
    });

    // Snacks combined
    monthly['Snacks'] = {
      A: monthly['AM snack'].A + monthly['PM snack'].A,
      B: monthly['AM snack'].B + monthly['PM snack'].B,
      C: monthly['AM snack'].C + monthly['PM snack'].C,
      Total: monthly['AM snack'].Total + monthly['PM snack'].Total,
    };

    // Days food service provided = days where any meal was served to anyone
    const daysWithService = dateLabels.filter((_, d) =>
      mealTypes.some(mt => students.some(s => s.meals[mt]?.[d]))
    ).length;

    // Total enrollment from this report
    const totalEnrolled = students.length;

    res.json({
      success: true,
      filename: req.file.originalname,
      students: totalEnrolled,
      daysWithService,
      monthly,
      dateLabels,
      exclusionCount: Object.values(exclusions).reduce((sum, days) =>
        sum + Object.values(days).reduce((s,e) => s + e.length, 0), 0),
    });
  } catch (err) {
    console.error('CSV parse error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── PARSE ATTENDANCE CSV ──────────────────────────────────
app.post('/api/parse-attendance-csv', requirePIN, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  try {
    const content = fs.readFileSync(req.file.path, 'utf-8').replace(/^\uFEFF/, '');
    const lines = content.split('\n');

    // Find summary stats line
    let totalEnrolled = 0, avgDailyAttendance = 0, totalPresent = 0;
    let operatingDays = 0;

    for (const line of lines) {
      if (line.includes('Number students')) {
        const match = line.match(/(\d+)/);
        if (match) totalEnrolled = parseInt(match[1]);
      }
      if (line.includes('Average daily attendance') && !line.includes('%')) {
        const match = line.match(/(\d+\.?\d*)/g);
        if (match && match.length >= 2) avgDailyAttendance = parseFloat(match[1]);
      }
    }

    // Parse header for dates, count operating days (Mon-Fri with P entries)
    const headerRow = parseCSVLine(lines[2] || lines[1]);
    const dateCols = headerRow.slice(3, -2); // skip classroom, first, last, total present, total absent

    // Count days that had any attendance (P entries)
    const dataLines = lines.slice(3).filter(l => l.trim() && !l.includes('AGGREGATE') && !l.includes('Summary'));
    if (dateCols.length > 0 && dataLines.length > 0) {
      for (let d = 0; d < dateCols.length; d++) {
        const hasPresent = dataLines.some(l => {
          const row = parseCSVLine(l);
          return row[3 + d]?.trim() === 'P';
        });
        if (hasPresent) operatingDays++;
      }
    }

    // Get aggregate totals row
    const aggLine = lines.find(l => l.includes('AGGREGATE'));
    if (aggLine) {
      const aggRow = parseCSVLine(aggLine);
      const dailyCounts = aggRow.slice(3, -1).map(v => parseInt(v) || 0).filter(v => v > 0);
      if (dailyCounts.length > 0 && !avgDailyAttendance) {
        avgDailyAttendance = dailyCounts.reduce((a,b)=>a+b,0) / dailyCounts.length;
      }
    }

    res.json({
      success: true,
      filename: req.file.originalname,
      totalEnrolled,
      avgDailyAttendance: Math.round(avgDailyAttendance * 10) / 10,
      operatingDays,
    });
  } catch (err) {
    console.error('Attendance parse error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── PARSE CDC PDF ─────────────────────────────────────────
app.post('/api/parse-cdc-pdf', requirePIN, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  try {
    // Use pdf-parse to extract text
    let pdfParse;
    try { pdfParse = require('pdf-parse'); } catch(e) {
      return res.json({ success: false, error: 'pdf-parse not available', manualEntry: true });
    }

    const dataBuffer = fs.readFileSync(req.file.path);
    const pdfData = await pdfParse(dataBuffer);
    const text = pdfData.text;

    // Extract children using regex pattern
    // Pattern: "Child's Name : FirstName LastName" followed by payment info
    const childPattern = /Child's Name\s*:\s*([^\n]+)\n.*?Total for Child:\s*\$\s*([\d,.]+)/gs;
    const amountPattern = /\$\s*([\d,]+\.\d{2})\s*$/m;

    const children = [];
    const seen = new Set();

    // Split by child entries
    const sections = text.split(/Child's Name\s*:/);
    for (let i = 1; i < sections.length; i++) {
      const section = sections[i];
      const nameMatch = section.match(/^\s*([A-Za-z][^\n]+)/);
      if (!nameMatch) continue;
      const name = nameMatch[1].trim();

      // Get amount paid
      const totalMatch = section.match(/Total for Child:\s*\$\s*([\d,]+\.\d{2})/);
      if (!totalMatch) continue;
      const amountPaid = parseFloat(totalMatch[1].replace(',',''));

      // Check for error descriptions that indicate $0
      const noAuth = section.includes('No Authorization');
      const dupBill = section.includes('Duplicate Bill');

      // Only count if payment > 0
      if (amountPaid > 0 && !seen.has(name.toLowerCase())) {
        seen.add(name.toLowerCase());
        children.push({ name, amountPaid, status: 'qualifying' });
      }
    }

    res.json({
      success: true,
      filename: req.file.originalname,
      qualifyingChildren: children.length,
      children,
    });
  } catch (err) {
    console.error('CDC PDF parse error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── PARSE FOOD RECEIPT (AI extraction) ───────────────────
app.post('/api/parse-receipt', requirePIN, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  try {
    let extractedTotal = null;
    const filename = req.file.originalname.toLowerCase();

    // Try pdf-parse for PDFs
    if (filename.endsWith('.pdf')) {
      try {
        const pdfParse = require('pdf-parse');
        const dataBuffer = fs.readFileSync(req.file.path);
        const pdfData = await pdfParse(dataBuffer);
        const text = pdfData.text;

        // Look for total patterns
        const patterns = [
          /(?:total|amount due|grand total|subtotal)[:\s]*\$?\s*([\d,]+\.\d{2})/gi,
          /\$\s*([\d,]+\.\d{2})\s*(?:total|due)/gi,
        ];
        for (const pattern of patterns) {
          const match = pattern.exec(text);
          if (match) {
            extractedTotal = parseFloat(match[1].replace(',',''));
            break;
          }
        }
      } catch(e) { /* fall through to manual */ }
    }

    res.json({
      success: true,
      filename: req.file.originalname,
      extractedTotal,
      requiresManualEntry: extractedTotal === null,
      message: extractedTotal
        ? `Extracted total: $${extractedTotal.toFixed(2)} — please verify`
        : 'Could not auto-extract total — please enter manually',
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GENERIC UPLOAD (store filename in state) ──────────────
app.post('/api/upload/:docType', requirePIN, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  const existing = (await dbGet('app_state')) || {};
  if (!existing.docNames) existing.docNames = {};
  existing.docNames[req.params.docType] = req.file.originalname;
  await dbSet('app_state', existing);
  res.json({ success: true, filename: req.file.originalname });
});

// ── GENERATE GENERAL LEDGER ───────────────────────────────
app.post('/api/generate-gl', requirePIN, async (req, res) => {
  try {
    const data = req.body;
    const salaryTotal = parseFloat(data.salaryTotal) || 0;
    const benefitsTotal = salaryTotal * 0.0765;
    const foodCost = parseFloat(data.foodCost) || 0;
    const adminCost = parseFloat(data.adminCost) || 0;
    const cacfpReimb = parseFloat(data.cacfpReimbursement) || 0;
    const totalExp = salaryTotal + benefitsTotal + foodCost + adminCost;
    const fundMod = Math.max(0, totalExp - cacfpReimb);
    const totalRev = cacfpReimb + fundMod;
    const fy = data.fiscalYear || 'FY2026';
    const yearNum = parseInt(fy.replace('FY',''));
    const fyStart = `October 1, ${yearNum - 1}`;
    const fyEnd = `September 30, ${yearNum}`;
    const fmt = n => '$' + parseFloat(n||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});

    const b={style:BorderStyle.SINGLE,size:1,color:"CCCCCC"};
    const borders={top:b,bottom:b,left:b,right:b};
    const tb={style:BorderStyle.SINGLE,size:3,color:"0F2340"};
    const tborders={top:tb,bottom:tb,left:tb,right:tb};
    const hc=(t,w)=>new TableCell({borders:tborders,width:{size:w,type:WidthType.DXA},shading:{fill:"0F2340",type:ShadingType.CLEAR},margins:{top:80,bottom:80,left:120,right:120},children:[new Paragraph({alignment:AlignmentType.CENTER,children:[new TextRun({text:t,bold:true,color:"FFFFFF",font:"Arial",size:20})]})]});
    const dc=(t,w,bold=false,fill="FFFFFF",align=AlignmentType.LEFT)=>new TableCell({borders,width:{size:w,type:WidthType.DXA},shading:{fill,type:ShadingType.CLEAR},margins:{top:80,bottom:80,left:120,right:120},children:[new Paragraph({alignment:align,children:[new TextRun({text:t,bold,font:"Arial",size:20})]})]});
    const sc=(t,w,align=AlignmentType.LEFT)=>new TableCell({borders,width:{size:w,type:WidthType.DXA},shading:{fill:"D6E4F0",type:ShadingType.CLEAR},margins:{top:80,bottom:80,left:120,right:120},children:[new Paragraph({alignment:align,children:[new TextRun({text:t,bold:true,font:"Arial",size:20})]})]});
    const tc=(t,w,align=AlignmentType.LEFT)=>new TableCell({borders:tborders,width:{size:w,type:WidthType.DXA},shading:{fill:"0F2340",type:ShadingType.CLEAR},margins:{top:80,bottom:80,left:120,right:120},children:[new Paragraph({alignment:align,children:[new TextRun({text:t,bold:true,color:"FFFFFF",font:"Arial",size:20})]})]});
    const ar=(d,desc,amt)=>new TableRow({children:[dc(d,1800),dc(desc,5160),dc(amt,2400,false,"FFFFFF",AlignmentType.RIGHT)]});
    const sr=(label,amt)=>new TableRow({children:[sc("",1800),sc(label,5160),sc(amt,2400,AlignmentType.RIGHT)]});
    const tr2=(label,amt)=>new TableRow({children:[tc("",1800),tc(label,5160),tc(amt,2400,AlignmentType.RIGHT)]});

    const MONTHS=['October','November','December','January','February','March','April','May','June','July','August','September'];
    const ms=data.monthlySalaries||{};
    const mc=data.monthlyClaims||{};
    const mf=data.monthlyFoodCosts||{};
    const ma=data.monthlyAdminCosts||{};

    const salRows=MONTHS.map(m=>{const yr=['October','November','December'].includes(m)?yearNum-1:yearNum;const key=`${m}_${yr}`;return ar(`${m} ${yr}`,"Food Service Staff — Niles & Peace Centers",fmt(parseFloat(ms[key]||0)));});
    const claimRows=MONTHS.map(m=>{const yr=['October','November','December'].includes(m)?yearNum-1:yearNum;const key=`${m}_${yr}`;return ar(`${m} ${yr}`,"CACFP Federal Reimbursement — Child Care Meals",fmt(parseFloat(mc[key]||0)));});
    const foodRows=MONTHS.map(m=>{const yr=['October','November','December'].includes(m)?yearNum-1:yearNum;const key=`${m}_${yr}`;return ar(`${m} ${yr}`,"Food Purchases — All CACFP Sites",fmt(parseFloat(mf[key]||0)));});
    const adminRows=MONTHS.map(m=>{const yr=['October','November','December'].includes(m)?yearNum-1:yearNum;const key=`${m}_${yr}`;return ar(`${m} ${yr}`,"Administrative Costs — CACFP Coordinator",fmt(parseFloat(ma[key]||0)));});

    const doc=new Document({sections:[{
      properties:{page:{size:{width:12240,height:15840},margin:{top:1080,right:1080,bottom:1080,left:1080}}},
      children:[
        new Paragraph({alignment:AlignmentType.CENTER,spacing:{after:60},children:[new TextRun({text:"The Children's Center",bold:true,font:"Arial",size:32,color:"0F2340"})]}),
        new Paragraph({alignment:AlignmentType.CENTER,spacing:{after:60},children:[new TextRun({text:"Non-profit Food Service Account (NFSA) — Detailed General Ledger",bold:true,font:"Arial",size:26})]}),
        new Paragraph({alignment:AlignmentType.CENTER,spacing:{after:60},children:[new TextRun({text:`${fy}: ${fyStart} – ${fyEnd}`,font:"Arial",size:24})]}),
        new Paragraph({alignment:AlignmentType.CENTER,spacing:{after:300},border:{bottom:{style:BorderStyle.SINGLE,size:6,color:"0F2340",space:1}},children:[new TextRun({text:`CACFP Sponsor ID: ${data.sponsorId||'990004457'} | Niles & Peace Boulevard Centers | Mary Wardlaw, Owner`,font:"Arial",size:20,italics:true})]}),

        new Paragraph({spacing:{before:200,after:100},children:[new TextRun({text:"NFSA SUMMARY",bold:true,font:"Arial",size:24,color:"0F2340"})]}),
        new Table({width:{size:9360,type:WidthType.DXA},columnWidths:[6960,2400],rows:[
          new TableRow({children:[hc("Description",6960),hc("Amount",2400)]}),
          new TableRow({children:[dc("CACFP Federal Reimbursement (Line 3a)",6960),dc(fmt(cacfpReimb),2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("Fund Modification — Tuition/Subsidy/GSRP (Line 10)",6960),dc(fmt(fundMod),2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("TOTAL REVENUE",6960,true,"E8EEF5"),dc(fmt(totalRev),2400,true,"E8EEF5",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("Food Service Salaries (Line 1)",6960),dc(fmt(salaryTotal),2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("Employee Benefits 7.65% (Line 2)",6960),dc(fmt(benefitsTotal),2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("Administrative Costs (Line 3)",6960),dc(fmt(adminCost),2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("Food Cost (Line 10)",6960),dc(fmt(foodCost),2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("TOTAL EXPENDITURES",6960,true,"E8EEF5"),dc(fmt(totalExp),2400,true,"E8EEF5",AlignmentType.RIGHT)]}),
          new TableRow({children:[tc("ENDING FUND BALANCE",6960),tc("$0.00",2400,AlignmentType.RIGHT)]}),
        ]}),

        new Paragraph({spacing:{before:300,after:80},children:[new TextRun({text:"SECTION 1: REVENUE",bold:true,font:"Arial",size:24,color:"0F2340"})]}),
        new Paragraph({spacing:{before:80,after:80},children:[new TextRun({text:"1A. CACFP Federal Reimbursement (CNP-YER Line 3a)",bold:true,font:"Arial",size:22})]}),
        new Table({width:{size:9360,type:WidthType.DXA},columnWidths:[1800,5160,2400],rows:[new TableRow({children:[hc("Period",1800),hc("Description",5160),hc("Amount",2400)]}),...claimRows,sr("TOTAL CACFP Reimbursement",fmt(cacfpReimb))]}),
        new Paragraph({spacing:{before:200,after:80},children:[new TextRun({text:"1B. Fund Modification (CNP-YER Line 10)",bold:true,font:"Arial",size:22})]}),
        new Table({width:{size:9360,type:WidthType.DXA},columnWidths:[1800,5160,2400],rows:[new TableRow({children:[hc("Period",1800),hc("Description",5160),hc("Amount",2400)]}),ar(`${fyStart} – ${fyEnd}`,"Transfer from general operating funds — Category B & C meal revenue gap",fmt(fundMod)),sr("TOTAL Fund Modification",fmt(fundMod)),tr2("TOTAL REVENUE",fmt(totalRev))]}),

        new Paragraph({spacing:{before:300,after:80},children:[new TextRun({text:"SECTION 2: EXPENDITURES",bold:true,font:"Arial",size:24,color:"0F2340"})]}),
        new Paragraph({spacing:{before:80,after:80},children:[new TextRun({text:"2A. Food Service Staff Salaries (CNP-YER Line 1)",bold:true,font:"Arial",size:22})]}),
        new Table({width:{size:9360,type:WidthType.DXA},columnWidths:[1800,5160,2400],rows:[new TableRow({children:[hc("Period",1800),hc("Description",5160),hc("Amount",2400)]}),...salRows,sr("TOTAL Salaries",fmt(salaryTotal))]}),
        new Paragraph({spacing:{before:200,after:80},children:[new TextRun({text:"2B. Employee Benefits (CNP-YER Line 2)",bold:true,font:"Arial",size:22})]}),
        new Table({width:{size:9360,type:WidthType.DXA},columnWidths:[1800,5160,2400],rows:[new TableRow({children:[hc("Period",1800),hc("Description",5160),hc("Amount",2400)]}),ar(`${fyStart} – ${fyEnd}`,`Employer payroll taxes: SS 6.2% + Medicare 1.45% × ${fmt(salaryTotal)}`,fmt(benefitsTotal)),sr("TOTAL Benefits",fmt(benefitsTotal))]}),
        new Paragraph({spacing:{before:200,after:80},children:[new TextRun({text:"2C. Administrative Costs (CNP-YER Line 3)",bold:true,font:"Arial",size:22})]}),
        new Table({width:{size:9360,type:WidthType.DXA},columnWidths:[1800,5160,2400],rows:[new TableRow({children:[hc("Period",1800),hc("Description",5160),hc("Amount",2400)]}),...adminRows,sr("TOTAL Admin Costs",fmt(adminCost))]}),
        new Paragraph({spacing:{before:200,after:80},children:[new TextRun({text:"2D. Food Cost (CNP-YER Line 10)",bold:true,font:"Arial",size:22})]}),
        new Table({width:{size:9360,type:WidthType.DXA},columnWidths:[1800,5160,2400],rows:[new TableRow({children:[hc("Period",1800),hc("Description",5160),hc("Amount",2400)]}),...foodRows,sr("TOTAL Food Cost",fmt(foodCost)),tr2("TOTAL EXPENDITURES",fmt(totalExp))]}),

        new Paragraph({spacing:{before:300,after:80},children:[new TextRun({text:"SECTION 3: BALANCE SUMMARY",bold:true,font:"Arial",size:24,color:"0F2340"})]}),
        new Table({width:{size:9360,type:WidthType.DXA},columnWidths:[6960,2400],rows:[
          new TableRow({children:[hc("Description",6960),hc("Amount",2400)]}),
          new TableRow({children:[dc(`Beginning Fund Balance (${fyStart})`,6960),dc("$0.00",2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("Add: Total NFSA Revenue",6960),dc(fmt(totalRev),2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[dc("Less: Total NFSA Expenditures",6960),dc(`(${fmt(totalExp)})`,2400,false,"FFFFFF",AlignmentType.RIGHT)]}),
          new TableRow({children:[tc(`Ending Fund Balance (${fyEnd})`,6960),tc("$0.00",2400,AlignmentType.RIGHT)]}),
        ]}),

        new Paragraph({spacing:{before:300,after:80},border:{top:{style:BorderStyle.SINGLE,size:4,color:"0F2340",space:1}},children:[new TextRun({text:"CERTIFICATION",bold:true,font:"Arial",size:24,color:"0F2340"})]}),
        new Paragraph({spacing:{after:80},children:[new TextRun({text:`I certify that the information in this general ledger is accurate and complete for The Children's Center NFSA, ${fy}: ${fyStart} through ${fyEnd}. Centers: Niles (210 E Main St) and Peace Boulevard.`,font:"Arial",size:20})]}),
        new Paragraph({spacing:{after:200},children:[new TextRun({text:"All amounts reconcile with the CNP-YER submitted to the Michigan Department of Education.",font:"Arial",size:20})]}),
        new Paragraph({spacing:{after:80},children:[new TextRun({text:"Authorized Signature: _________________________________     Date: _______________",font:"Arial",size:20})]}),
        new Paragraph({spacing:{after:40},children:[new TextRun({text:"Printed Name: Mary Wardlaw     Title: Owner, The Children's Center",font:"Arial",size:20})]}),
        new Paragraph({spacing:{after:40},children:[new TextRun({text:`CACFP Sponsor ID: ${data.sponsorId||'990004457'}`,font:"Arial",size:20})]}),
      ]
    }]});

    const buffer = await Packer.toBuffer(doc);
    res.setHeader('Content-Type','application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.setHeader('Content-Disposition',`attachment; filename=TCC_NFSA_General_Ledger_${fy}.docx`);
    res.send(buffer);
  } catch(err) { console.error(err); res.status(500).json({error:err.message}); }
});

// ── GENERATE YER SUMMARY ──────────────────────────────────
app.post('/api/generate-yer', requirePIN, (req, res) => {
  const data=req.body;
  const sal=parseFloat(data.salaryTotal)||0;
  const ben=sal*0.0765;
  const food=parseFloat(data.foodCost)||0;
  const admin=parseFloat(data.adminCost)||0;
  const cacfp=parseFloat(data.cacfpReimbursement)||0;
  const totalExp=sal+ben+food+admin;
  const fundMod=Math.max(0,totalExp-cacfp);
  const totalRev=cacfp+fundMod;
  const fmt=n=>'$'+parseFloat(n||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  res.json({
    fiscalYear:data.fiscalYear||'FY2026', sponsorId:data.sponsorId||'990004457',
    revenue:{line3a:fmt(cacfp),line10:fmt(fundMod),line11:fmt(totalRev)},
    expenditures:{line1:fmt(sal),line2:fmt(ben),line3:fmt(admin),line10:fmt(food),line11:fmt(totalExp)},
    balance:{beginning:'$0.00',revenue:fmt(totalRev),expenditures:fmt(totalExp),ending:'$0.00'}
  });
});

// ── HELPERS ───────────────────────────────────────────────
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    if (line[i] === '"') { inQuotes = !inQuotes; }
    else if (line[i] === ',' && !inQuotes) { result.push(current); current = ''; }
    else { current += line[i]; }
  }
  result.push(current);
  return result.map(v => v.replace(/\r/g,''));
}

initDB().then(() => app.listen(PORT, () => console.log(`TCC CACFP Suite v2 on port ${PORT}`)));
