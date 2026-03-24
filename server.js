// ══════════════════════════════════════════════════════════
// SERVER.JS PATCH — Strip Signature Columns from Stored CSVs
// Two changes needed:
// ══════════════════════════════════════════════════════════

// ─────────────────────────────────────────────────────────
// CHANGE 1: Add this function BEFORE the line:
//   app.post('/api/child-attendance-import', ...
// ─────────────────────────────────────────────────────────

// ── Clean CSV: remove Signature URL columns and fully blank columns ──
function cleanCSVForStorage(rawText) {
  const lines = rawText.split('\n').filter(l => l.trim());
  if (lines.length < 2) return rawText;

  const headerParts = lines[0].split(',');

  // Identify columns to remove:
  // 1. "Signature" columns (contain URLs to signature images)
  // 2. Completely blank/dash-only columns beyond the name+date cols
  const sigCols = new Set();
  const colHasData = {};

  for (let c = 0; c < headerParts.length; c++) {
    const hdr = (headerParts[c] || '').replace(/"/g, '').trim().toLowerCase();
    // Mark signature URL columns (NOT "Signer" name columns — those are fine)
    if (hdr === 'signature' || hdr === 'signature 2') {
      sigCols.add(c);
    }
    colHasData[c] = false;
  }

  // Scan data rows to find truly blank columns
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',');
    for (let c = 0; c < parts.length; c++) {
      const val = (parts[c] || '').replace(/"/g, '').trim();
      // URLs, dashes, and empty = not real data
      if (val && val !== '-' && val !== '––' && val !== '\u2013\u2013' && !val.startsWith('http')) {
        colHasData[c] = true;
      }
    }
  }

  // Build list of columns to keep
  const keepCols = [];
  for (let c = 0; c < headerParts.length; c++) {
    if (sigCols.has(c)) continue;                    // always strip signature URL cols
    if (!colHasData[c] && c >= 3) continue;           // strip blank data cols (keep 0-2: Last, First, Date)
    keepCols.push(c);
  }

  // Rebuild CSV with only kept columns
  const cleanLines = lines.map(line => {
    const parts = line.split(',');
    return keepCols.map(c => (parts[c] || '')).join(',');
  });

  return cleanLines.join('\n');
}


// ─────────────────────────────────────────────────────────
// CHANGE 2: Inside /api/child-attendance-import, find:
//
//     // Also store original file
//     await pool.query(
//       `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
//        VALUES ($1,$2,$3,$4,'text/csv',$5,$6)`,
//       [fiscal_year_id, month_key, 'child_attendance_daily_'+center, req.file.originalname, req.file.buffer, JSON.stringify({center, source:'playground'})]
//     );
//
// REPLACE WITH:
//
//     // Store cleaned CSV (signature URLs and blank columns stripped)
//     const cleanedCSV = cleanCSVForStorage(text);
//     const cleanedBuffer = Buffer.from(cleanedCSV, 'utf8');
//     await pool.query(
//       `INSERT INTO documents (fiscal_year_id, month_key, doc_type, filename, mime_type, file_data, metadata)
//        VALUES ($1,$2,$3,$4,'text/csv',$5,$6)`,
//       [fiscal_year_id, month_key, 'child_attendance_daily_'+center, req.file.originalname, cleanedBuffer, JSON.stringify({center, source:'playground', cleaned: true})]
//     );
// ─────────────────────────────────────────────────────────
