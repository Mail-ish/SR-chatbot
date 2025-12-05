function processSiteContractsFrom2022() {
  const ss = SpreadsheetApp.getActive();
  const srcSheet = ss.getSheetByName("All Contract(Site)");
  if (!srcSheet) throw new Error("Source sheet 'All Contract(Site)' not found!");

  const outputSheetName = "2-Contract(site)";
  let outSheet = ss.getSheetByName(outputSheetName);
  if (outSheet) outSheet.clear();
  else outSheet = ss.insertSheet(outputSheetName);

  const data = srcSheet.getDataRange().getValues();
  const formatting = srcSheet.getDataRange().getFontColors();

  const headers = data.shift();
  formatting.shift();

  const COL_CONTRACT = 0;
  const COL_START = 2;
  const COL_END = 3;
  const COL_COMPANY = 9;
  const COL_PIC = 10;
  const COL_SKU = 22;
  const COL_QTY = 23;

  // Filter rows: start date from 2022 onwards & valid contract
  const filteredData = [];
  const filteredFmt = [];
  data.forEach((row, i) => {
    const contract = row[COL_CONTRACT];
    const startValue = row[COL_START];
    let startDate = null;

    if (startValue) {
      if (startValue instanceof Date) {
        startDate = startValue;
      } else if (typeof startValue === "string") {
        startDate = new Date(startValue);
      }
    }

    if (startDate && startDate.getFullYear() >= 2022 && contract && contract !== "N/A") {
      filteredData.push(row);
      filteredFmt.push(formatting[i]);
    }
  });

  if (filteredData.length === 0) {
    Logger.log("⚠️ No contracts from 2022 onwards found.");
  }

  // Group by company or PIC
  const groups = new Map();
  filteredData.forEach((row, i) => {
    const key = (row[COL_COMPANY] && row[COL_COMPANY] !== "N/A") ? row[COL_COMPANY] : row[COL_PIC];
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push({ row, fmt: filteredFmt[i] });
  });

  // Sort each group by start date (most recent first)
  groups.forEach(rows =>
    rows.sort((a, b) => new Date(b.row[COL_START]) - new Date(a.row[COL_START]))
  );

  // Flatten and remove duplicates by (contract, sku, start, end, qty)
  const uniqueCheck = new Set();
  const outputRows = [];
  const outputFmt = [];
  groups.forEach(rows => {
    rows.forEach(item => {
      const r = item.row;
      const key = [
        r[COL_CONTRACT],
        r[COL_SKU],
        formatKeyDate(r[COL_START]),
        formatKeyDate(r[COL_END]),
        r[COL_QTY]
      ].join("|");

      if (!uniqueCheck.has(key)) {
        uniqueCheck.add(key);
        outputRows.push(r);
        outputFmt.push(item.fmt);
      }
    });
  });

  // Highlight duplicate contract numbers
  const contractCounts = {};
  outputRows.forEach(r =>
    contractCounts[r[COL_CONTRACT]] = (contractCounts[r[COL_CONTRACT]] || 0) + 1
  );

  outputFmt.forEach((fmt, i) => {
    if (contractCounts[outputRows[i][COL_CONTRACT]] > 1) {
      fmt[COL_CONTRACT] = "#ff0000";
    }
  });

  // Write to output sheet safely
  outSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  if (outputRows.length > 0) {
    outSheet.getRange(2, 1, outputRows.length, headers.length)
      .setValues(outputRows)
      .setFontColors(outputFmt);
  }

  outSheet.setFrozenRows(1);

  Logger.log(`✅ Written ${outputRows.length} cleaned rows (from 2022 onwards) to '${outputSheetName}'`);
}

/** Ensures date keys compare correctly */
function formatKeyDate(v) {
  if (v instanceof Date) return v.toISOString().substring(0, 10);
  if (typeof v === "string") return v; // keep string as-is for key comparison
  return "";
}
