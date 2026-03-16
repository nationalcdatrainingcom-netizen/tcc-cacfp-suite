const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
        AlignmentType, BorderStyle, WidthType, ShadingType } = require('docx');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static('public'));

// In-memory data store
let store = {};

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const dir = 'uploads/';
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    cb(null, dir);
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + '-' + file.originalname);
  }
});
const upload = multer({ storage });

app.get('/api/data', (req, res) => res.json(store));
app.post('/api/data', (req, res) => {
  store = { ...store, ...req.body };
  res.json({ success: true });
});

app.post('/api/upload/:docType', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  if (!store.docNames) store.docNames = {};
  store.docNames[req.params.docType] = req.file.originalname;
  res.json({ success: true, filename: req.file.originalname });
});

// Generate NFSA General Ledger
app.post('/api/generate-gl', async (req, res) => {
  try {
    const data = { ...store, ...req.body };
    const salaryTotal = parseFloat(data.salaryTotal) || 0;
    const benefitsTotal = salaryTotal * 0.0765;
    const foodCost = parseFloat(data.foodCost) || 0;
    const cacfpReimb = parseFloat(data.cacfpReimbursement) || 0;
    const totalExp = salaryTotal + benefitsTotal + foodCost;
    const fundMod = Math.max(0, totalExp - cacfpReimb);
    const totalRev = cacfpReimb + fundMod;
    const fy = data.fiscalYear || 'FY2026';
    const yearNum = parseInt(fy.replace('FY', ''));
    const fyStart = `October 1, ${yearNum - 1}`;
    const fyEnd = `September 30, ${yearNum}`;
    const fmt = n => '$' + parseFloat(n || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    const b = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
    const borders = { top: b, bottom: b, left: b, right: b };
    const tb = { style: BorderStyle.SINGLE, size: 3, color: "1a2e4a" };
    const tborders = { top: tb, bottom: tb, left: tb, right: tb };

    const hc = (text, w) => new TableCell({ borders: tborders, width: { size: w, type: WidthType.DXA }, shading: { fill: "1a2e4a", type: ShadingType.CLEAR }, margins: { top: 80, bottom: 80, left: 120, right: 120 }, children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ text, bold: true, color: "FFFFFF", font: "Arial", size: 20 })] })] });
    const dc = (text, w, bold = false, fill = "FFFFFF", align = AlignmentType.LEFT) => new TableCell({ borders, width: { size: w, type: WidthType.DXA }, shading: { fill, type: ShadingType.CLEAR }, margins: { top: 80, bottom: 80, left: 120, right: 120 }, children: [new Paragraph({ alignment: align, children: [new TextRun({ text, bold, font: "Arial", size: 20 })] })] });
    const sc = (text, w, align = AlignmentType.LEFT) => new TableCell({ borders, width: { size: w, type: WidthType.DXA }, shading: { fill: "D6E4F0", type: ShadingType.CLEAR }, margins: { top: 80, bottom: 80, left: 120, right: 120 }, children: [new Paragraph({ alignment: align, children: [new TextRun({ text, bold: true, font: "Arial", size: 20 })] })] });
    const tc = (text, w, align = AlignmentType.LEFT) => new TableCell({ borders: tborders, width: { size: w, type: WidthType.DXA }, shading: { fill: "1a2e4a", type: ShadingType.CLEAR }, margins: { top: 80, bottom: 80, left: 120, right: 120 }, children: [new Paragraph({ alignment: align, children: [new TextRun({ text, bold: true, color: "FFFFFF", font: "Arial", size: 20 })] })] });

    const ar = (date, desc, amt, fill = "FFFFFF") => new TableRow({ children: [dc(date, 1800, false, fill), dc(desc, 5160, false, fill), dc(amt, 2400, false, fill, AlignmentType.RIGHT)] });
    const sr = (label, amt) => new TableRow({ children: [sc("", 1800), sc(label, 5160), sc(amt, 2400, AlignmentType.RIGHT)] });
    const tr2 = (label, amt) => new TableRow({ children: [tc("", 1800), tc(label, 5160), tc(amt, 2400, AlignmentType.RIGHT)] });

    const MONTHS = ['October','November','December','January','February','March','April','May','June','July','August','September'];
    const monthlySalaries = data.monthlySalaries || {};
    const monthlyClaims = data.monthlyClaims || {};

    const salRows = MONTHS.map(m => {
      const yr = ['October','November','December'].includes(m) ? yearNum - 1 : yearNum;
      const key = `${m}_${yr}`;
      return ar(`${m} ${yr}`, "Food Service Staff — Niles & St. Joseph Centers", fmt(parseFloat(monthlySalaries[key] || 0)));
    });

    const claimRows = MONTHS.map(m => {
      const yr = ['October','November','December'].includes(m) ? yearNum - 1 : yearNum;
      const key = `${m}_${yr}`;
      return ar(`${m} ${yr}`, "CACFP Federal Reimbursement — Child Care Meals", fmt(parseFloat(monthlyClaims[key] || 0)));
    });

    const doc = new Document({
      sections: [{
        properties: { page: { size: { width: 12240, height: 15840 }, margin: { top: 1080, right: 1080, bottom: 1080, left: 1080 } } },
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [new TextRun({ text: "The Children's Center", bold: true, font: "Arial", size: 32, color: "1a2e4a" })] }),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [new TextRun({ text: "Non-profit Food Service Account (NFSA) — Detailed General Ledger", bold: true, font: "Arial", size: 26 })] }),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 }, children: [new TextRun({ text: `${fy}: ${fyStart} – ${fyEnd}`, font: "Arial", size: 24 })] }),
          new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 300 }, border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "1a2e4a", space: 1 } }, children: [new TextRun({ text: `CACFP Sponsor ID: ${data.sponsorId || '990004457'} | 210 East Main Street, Niles, MI 49120 | Mary Wardlaw, Owner`, font: "Arial", size: 20, italics: true })] }),

          new Paragraph({ spacing: { before: 200, after: 100 }, children: [new TextRun({ text: "NFSA SUMMARY", bold: true, font: "Arial", size: 24, color: "1a2e4a" })] }),
          new Table({ width: { size: 9360, type: WidthType.DXA }, columnWidths: [6960, 2400], rows: [
            new TableRow({ children: [hc("Description", 6960), hc("Amount", 2400)] }),
            new TableRow({ children: [dc("CACFP Federal Reimbursement (Line 3a)", 6960), dc(fmt(cacfpReimb), 2400, false, "FFFFFF", AlignmentType.RIGHT)] }),
            new TableRow({ children: [dc("Fund Modification — Tuition/Subsidy/GSRP (Line 10)", 6960), dc(fmt(fundMod), 2400, false, "FFFFFF", AlignmentType.RIGHT)] }),
            new TableRow({ children: [dc("TOTAL REVENUE", 6960, true, "E8EEF5"), dc(fmt(totalRev), 2400, true, "E8EEF5", AlignmentType.RIGHT)] }),
            new TableRow({ children: [dc("Food Service Salaries (Line 1)", 6960), dc(fmt(salaryTotal), 2400, false, "FFFFFF", AlignmentType.RIGHT)] }),
            new TableRow({ children: [dc("Employee Benefits 7.65% (Line 2)", 6960), dc(fmt(benefitsTotal), 2400, false, "FFFFFF", AlignmentType.RIGHT)] }),
            new TableRow({ children: [dc("Food Cost (Line 10)", 6960), dc(fmt(foodCost), 2400, false, "FFFFFF", AlignmentType.RIGHT)] }),
            new TableRow({ children: [dc("TOTAL EXPENDITURES", 6960, true, "E8EEF5"), dc(fmt(totalExp), 2400, true, "E8EEF5", AlignmentType.RIGHT)] }),
            new TableRow({ children: [tc("ENDING FUND BALANCE", 6960), tc("$0.00", 2400, AlignmentType.RIGHT)] }),
          ]}),

          new Paragraph({ spacing: { before: 300, after: 80 }, children: [new TextRun({ text: "SECTION 1: REVENUE", bold: true, font: "Arial", size: 24, color: "1a2e4a" })] }),
          new Paragraph({ spacing: { before: 80, after: 80 }, children: [new TextRun({ text: "1A. CACFP Federal Reimbursement (CNP-YER Line 3a)", bold: true, font: "Arial", size: 22 })] }),
          new Table({ width: { size: 9360, type: WidthType.DXA }, columnWidths: [1800, 5160, 2400], rows: [
            new TableRow({ children: [hc("Period", 1800), hc("Description", 5160), hc("Amount", 2400)] }),
            ...claimRows,
            sr("TOTAL CACFP Reimbursement", fmt(cacfpReimb)),
          ]}),

          new Paragraph({ spacing: { before: 200, after: 80 }, children: [new TextRun({ text: "1B. Fund Modification (CNP-YER Line 10)", bold: true, font: "Arial", size: 22 })] }),
          new Paragraph({ spacing: { before: 0, after: 80 }, children: [new TextRun({ text: "Transfer from tuition income, DHHS childcare subsidy, GSRP reimbursements, and general operating funds to cover NFSA deficit for Category B and C meal participants.", font: "Arial", size: 18, italics: true })] }),
          new Table({ width: { size: 9360, type: WidthType.DXA }, columnWidths: [1800, 5160, 2400], rows: [
            new TableRow({ children: [hc("Period", 1800), hc("Description", 5160), hc("Amount", 2400)] }),
            ar(`${fyStart} – ${fyEnd}`, "Transfer from general operating funds — Category B & C meal revenue gap", fmt(fundMod)),
            sr("TOTAL Fund Modification", fmt(fundMod)),
            tr2("TOTAL REVENUE", fmt(totalRev)),
          ]}),

          new Paragraph({ spacing: { before: 300, after: 80 }, children: [new TextRun({ text: "SECTION 2: EXPENDITURES", bold: true, font: "Arial", size: 24, color: "1a2e4a" })] }),
          new Paragraph({ spacing: { before: 80, after: 80 }, children: [new TextRun({ text: "2A. Food Service Staff Salaries (CNP-YER Line 1)", bold: true, font: "Arial", size: 22 })] }),
          new Paragraph({ spacing: { before: 0, after: 80 }, children: [new TextRun({ text: "Direct labor costs for food service staff at Niles and St. Joseph centers, documented via monthly time and attendance records.", font: "Arial", size: 18, italics: true })] }),
          new Table({ width: { size: 9360, type: WidthType.DXA }, columnWidths: [1800, 5160, 2400], rows: [
            new TableRow({ children: [hc("Period", 1800), hc("Description", 5160), hc("Amount", 2400)] }),
            ...salRows,
            sr("TOTAL Food Service Salaries", fmt(salaryTotal)),
          ]}),

          new Paragraph({ spacing: { before: 200, after: 80 }, children: [new TextRun({ text: "2B. Employee Benefits (CNP-YER Line 2)", bold: true, font: "Arial", size: 22 })] }),
          new Table({ width: { size: 9360, type: WidthType.DXA }, columnWidths: [1800, 5160, 2400], rows: [
            new TableRow({ children: [hc("Period", 1800), hc("Description", 5160), hc("Amount", 2400)] }),
            ar(`${fyStart} – ${fyEnd}`, `Employer payroll taxes: SS 6.2% + Medicare 1.45% × ${fmt(salaryTotal)}`, fmt(benefitsTotal)),
            sr("TOTAL Benefits", fmt(benefitsTotal)),
          ]}),

          new Paragraph({ spacing: { before: 200, after: 80 }, children: [new TextRun({ text: "2C. Food Cost (CNP-YER Line 10)", bold: true, font: "Arial", size: 22 })] }),
          new Table({ width: { size: 9360, type: WidthType.DXA }, columnWidths: [1800, 5160, 2400], rows: [
            new TableRow({ children: [hc("Period", 1800), hc("Description", 5160), hc("Amount", 2400)] }),
            ar(`${fyStart} – ${fyEnd}`, "Food Purchases — All CACFP Sites (QuickBooks Account 64100)", fmt(foodCost)),
            sr("TOTAL Food Cost", fmt(foodCost)),
            tr2("TOTAL EXPENDITURES", fmt(totalExp)),
          ]}),

          new Paragraph({ spacing: { before: 300, after: 80 }, children: [new TextRun({ text: "SECTION 3: BALANCE SUMMARY", bold: true, font: "Arial", size: 24, color: "1a2e4a" })] }),
          new Table({ width: { size: 9360, type: WidthType.DXA }, columnWidths: [6960, 2400], rows: [
            new TableRow({ children: [hc("Description", 6960), hc("Amount", 2400)] }),
            new TableRow({ children: [dc(`Beginning Fund Balance (${fyStart})`, 6960), dc("$0.00", 2400, false, "FFFFFF", AlignmentType.RIGHT)] }),
            new TableRow({ children: [dc("Add: Total NFSA Revenue", 6960), dc(fmt(totalRev), 2400, false, "FFFFFF", AlignmentType.RIGHT)] }),
            new TableRow({ children: [dc("Less: Total NFSA Expenditures", 6960), dc(`(${fmt(totalExp)})`, 2400, false, "FFFFFF", AlignmentType.RIGHT)] }),
            new TableRow({ children: [tc(`Ending Fund Balance (${fyEnd})`, 6960), tc("$0.00", 2400, AlignmentType.RIGHT)] }),
          ]}),

          new Paragraph({ spacing: { before: 300, after: 80 }, border: { top: { style: BorderStyle.SINGLE, size: 4, color: "1a2e4a", space: 1 } }, children: [new TextRun({ text: "CERTIFICATION", bold: true, font: "Arial", size: 24, color: "1a2e4a" })] }),
          new Paragraph({ spacing: { after: 80 }, children: [new TextRun({ text: `I certify that the information in this general ledger is accurate and complete for The Children's Center NFSA, ${fy}: ${fyStart} through ${fyEnd}.`, font: "Arial", size: 20 })] }),
          new Paragraph({ spacing: { after: 200 }, children: [new TextRun({ text: "All amounts reconcile with the CNP-YER submitted to the Michigan Department of Education.", font: "Arial", size: 20 })] }),
          new Paragraph({ spacing: { after: 80 }, children: [new TextRun({ text: "Authorized Signature: _________________________________     Date: _______________", font: "Arial", size: 20 })] }),
          new Paragraph({ spacing: { after: 40 }, children: [new TextRun({ text: "Printed Name: Mary Wardlaw     Title: Owner, The Children's Center", font: "Arial", size: 20 })] }),
          new Paragraph({ spacing: { after: 40 }, children: [new TextRun({ text: `CACFP Sponsor ID: ${data.sponsorId || '990004457'}`, font: "Arial", size: 20 })] }),
        ]
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.setHeader('Content-Disposition', `attachment; filename=TCC_NFSA_General_Ledger_${fy}.docx`);
    res.send(buffer);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Generate YER Summary JSON
app.post('/api/generate-yer', (req, res) => {
  const data = { ...store, ...req.body };
  const sal = parseFloat(data.salaryTotal) || 0;
  const ben = sal * 0.0765;
  const food = parseFloat(data.foodCost) || 0;
  const cacfp = parseFloat(data.cacfpReimbursement) || 0;
  const totalExp = sal + ben + food;
  const fundMod = Math.max(0, totalExp - cacfp);
  const totalRev = cacfp + fundMod;
  const fmt = n => '$' + parseFloat(n || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  res.json({
    fiscalYear: data.fiscalYear || 'FY2026',
    sponsorId: data.sponsorId || '990004457',
    revenue: { line3a: fmt(cacfp), line10: fmt(fundMod), line11: fmt(totalRev) },
    expenditures: { line1: fmt(sal), line2: fmt(ben), line10: fmt(food), line11: fmt(totalExp) },
    balance: { beginning: '$0.00', revenue: fmt(totalRev), expenditures: fmt(totalExp), ending: '$0.00' }
  });
});

app.listen(PORT, () => console.log(`TCC CACFP Suite running on port ${PORT}`));
