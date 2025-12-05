/**
 * === 1. Status formatting mapping ===
 */
function getStatusFormat(status) {
  const norm = String(status || "").trim().toUpperCase();

  if (norm === "LIVE") return { font: "#d4edbc", bg: "#11734b" };
  if (norm.includes("END CONTRACT")) return { font: "#f6c8aa", bg: "#753800" };
  if (norm.includes("RENT TO OWN")) return { font: "#473821", bg: "#ffe5a0" };
  if (norm.includes("TERMINATED")) return { font: "#f2cfc9", bg: "#b10202" };
  if (norm.includes("INACTIVE-A")) return { font: "#ffc8aa", bg: "#753800" };
  if (norm.includes("INACTIVE-B")) return { font: "#ffcfc9", bg: "#b10202" };

  return { font: "#000000", bg: "#ffffff" };
}

/**
 * === 2. Normalize a single row for compilation ===
 */
function normalizeRowForOutput(r, f, b, startCol, sourceLabel, headers, MONTH_COUNT) {
  const totalIssuedIndex = 7 + MONTH_COUNT;
  const totalPaidIndex = totalIssuedIndex + 1;
  const totalExpectedIndex = totalIssuedIndex + 2;
  const paymentStatusIndex = totalIssuedIndex + 3;
  const remarksIndex = totalIssuedIndex + 4;
  const sourceIndex = totalIssuedIndex + 5;

  const outLen = headers.length;
  const outV = new Array(outLen).fill("");
  const outF = new Array(outLen).fill("#000000");
  const outB = new Array(outLen).fill("#ffffff");

  const invoiceStartIndexInR = 9 - startCol;
  const invoiceEndIndexInR = invoiceStartIndexInR + MONTH_COUNT - 1;

  // Copy values, font colors, and backgrounds
  for (let i = 0; i <= invoiceEndIndexInR && i < r.length; i++) {
    outV[i] = r[i] || "";
    outF[i] = f[i] || "#000000";
    outB[i] = b[i] || "#ffffff";
  }

  // Detect if this sheet style has totals or not
  const hasTotals = r.length > invoiceEndIndexInR + 4;
  if (hasTotals) {
    outV[totalIssuedIndex] = r[invoiceEndIndexInR + 1] || "";
    outV[totalPaidIndex] = r[invoiceEndIndexInR + 2] || "";
    outV[totalExpectedIndex] = r[invoiceEndIndexInR + 3] || "";
    outV[paymentStatusIndex] = r[invoiceEndIndexInR + 4] || "";
    outV[remarksIndex] = r[invoiceEndIndexInR + 5] || "";

    outF[remarksIndex] = f[invoiceEndIndexInR + 5] || "#000000";
    outB[remarksIndex] = b[invoiceEndIndexInR + 5] || "#ffffff";
  } else {
    outV[remarksIndex] = r[invoiceEndIndexInR + 1] || "";
    outF[remarksIndex] = f[invoiceEndIndexInR + 1] || "#000000";
    outB[remarksIndex] = b[invoiceEndIndexInR + 1] || "#ffffff";
  }

  // Source tag
  outV[sourceIndex] = sourceLabel;

  return { values: outV, fonts: outF, bgs: outB };
}

/**
 * === 3. Pull data + normalize entire sheet ===
 */
function pullAndNormalizeSheet(sheet, startCol, sourceLabel, headers, MONTH_COUNT) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < 3 || lastCol < startCol) {
    Logger.log(`Skipping ${sheet.getName()} (too few rows/cols)`);
    return [];
  }
  const numRows = lastRow - 2;
  const numCols = lastCol - startCol + 1;
  const range = sheet.getRange(3, startCol, numRows, numCols);

  const vals = range.getDisplayValues();
  const fonts = range.getFontColors();
  const bgs = range.getBackgrounds();

  const results = vals.map((r, i) =>
    normalizeRowForOutput(r, fonts[i], bgs[i], startCol, sourceLabel, headers, MONTH_COUNT)
  );
  Logger.log(`Pulled ${results.length} rows from "${sheet.getName()}"`);
  return results;
}

/**
 * === 4. Apply all formatting to output sheet ===
 */
function applyFormatting(outSheet, allValues, allFonts, allBgs, headers, MONTH_COUNT) {
  const dataRange = outSheet.getRange(2, 1, allValues.length, headers.length);
  const fontColors = dataRange.getFontColors();
  const bgColors = dataRange.getBackgrounds();

  // Apply Contract ID font colors (col C = 2)
  for (let r = 0; r < allValues.length; r++) {
    fontColors[r][2] = allFonts[r][2];
  }

  // Apply invoice backgrounds (cols 7..7+MONTH_COUNT-1)
  for (let r = 0; r < allValues.length; r++) {
    for (let c = 7; c < 7 + MONTH_COUNT; c++) {
      bgColors[r][c] = allBgs[r][c];
    }
  }

  // Apply remarks font + bg (last major column)
  const remarksCol = headers.length - 2; // before "Source Sheet"
  for (let r = 0; r < allValues.length; r++) {
    fontColors[r][remarksCol] = allFonts[r][remarksCol];
    bgColors[r][remarksCol] = allBgs[r][remarksCol];
  }

  // Apply status colors (col index 6)
  for (let r = 0; r < allValues.length; r++) {
    const fmt = getStatusFormat(allValues[r][6]);
    fontColors[r][6] = fmt.font;
    bgColors[r][6] = fmt.bg;
  }

  dataRange.setFontColors(fontColors);
  dataRange.setBackgrounds(bgColors);
}

/**
 * === 5. Style header row ===
 */
function styleHeaders(outSheet, headers) {
  const headerRange = outSheet.getRange(1, 1, 1, headers.length);
  headerRange
    .setBackground("#4B0082")
    .setFontColor("#ffffff")
    .setFontWeight("bold")
    .setHorizontalAlignment("center");
}

/**
 * === 6. Center align A–G ===
 */
function centerAlignCols(outSheet, allValues) {
  if (allValues.length > 0) {
    outSheet.getRange(2, 1, allValues.length, 7).setHorizontalAlignment("center");
  }
}

/**
 * === MAIN ===
 */
function compileAllCustomers() {
  const RESULT_SS = SpreadsheetApp.getActiveSpreadsheet();
  const SHEET_NAME = "Compiled_Raw(Sheets)";
  const headers = [
    "Customer Name",
    "Customer Type",
    "Contract IDs",
    "Start Date",
    "Package",
    "Qty",
    "Status",
  ];
  for (let m = 1; m <= 36; m++) headers.push("Month " + m);
  headers.push("Total Issued", "Total Paid", "Total Expected", "Payment Status", "Remarks", "Source Sheet");

  const MONTH_COUNT = 36;
  const allValues = [];
  const allFonts = [];
  const allBgs = [];

  // === Source 1: A-customers ===
  const A_SOURCE_ID = "1lLm5phswPd1xNnwNV84pEfrYL02qEz4Q1x5lw0jwl0c";
  const A_SOURCE_SS = SpreadsheetApp.openById(A_SOURCE_ID);
  const A_SHEETS = ["SME (ALL)", "IND (ALL)"];

  // === Source 2: B–Z customers ===
  const OTHER_SOURCE_ID = "177xFK3HChEvHWzWfsDm-8m2G2EUn4Y0RTleoqxRbdVM";
  const OTHER_SOURCE_SS = SpreadsheetApp.openById(OTHER_SOURCE_ID);
  const OTHER_SHEETS = OTHER_SOURCE_SS.getSheets()
    .map(s => s.getName())
    .filter(name => /^[B-Z]$/i.test(name));

  // === Prepare output ===
  let outSheet = RESULT_SS.getSheetByName(SHEET_NAME);
  if (!outSheet) outSheet = RESULT_SS.insertSheet(SHEET_NAME);
  else outSheet.clear();

  // === Pull all sources ===
  A_SHEETS.forEach(name => {
    const sheet = A_SOURCE_SS.getSheetByName(name);
    if (!sheet) return;
    const rows = pullAndNormalizeSheet(sheet, 2, "A_" + name, headers, MONTH_COUNT);
    rows.forEach(o => {
      allValues.push(o.values);
      allFonts.push(o.fonts);
      allBgs.push(o.bgs);
    });
  });

  OTHER_SHEETS.forEach(name => {
    const sheet = OTHER_SOURCE_SS.getSheetByName(name);
    if (!sheet) return;
    const startCol = name === "D" ? 1 : 2;
    const rows = pullAndNormalizeSheet(sheet, startCol, "Other_" + name, headers, MONTH_COUNT);
    rows.forEach(o => {
      allValues.push(o.values);
      allFonts.push(o.fonts);
      allBgs.push(o.bgs);
    });
  });

  // === Write & format ===
  outSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  if (allValues.length > 0) {
    outSheet.getRange(2, 1, allValues.length, headers.length).setValues(allValues);
  }

  applyFormatting(outSheet, allValues, allFonts, allBgs, headers, MONTH_COUNT);
  styleHeaders(outSheet, headers);
  centerAlignCols(outSheet, allValues);
  outSheet.setFrozenRows(1);

  Logger.log(`✅ Compiled ${allValues.length} rows into "${SHEET_NAME}" with modular formatting`);
}
