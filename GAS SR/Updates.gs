/**
 * Incremental updater for Compiled_Raw(Sheets)
 * - UniqueKey appended as last column: CustomerName|ContractIDs|SourceSheet
 * - Option B: always overwrite row data from sources (no preservation of manual edits)
 * - Hides UniqueKey column after update
 */
function compileAllCustomersIncremental() {
  const RESULT_SS = SpreadsheetApp.getActiveSpreadsheet();
  const SHEET_NAME = "Compiled_Raw(Sheets)";
  const MONTH_COUNT = 36;

  // === build headers (same as existing, with UniqueKey appended at end) ===
  const headers = [
    "Customer Name",
    "Customer Type",
    "Contract IDs",
    "Start Date",
    "Package",
    "Qty",
    "Status",
  ];
  for (let m = 1; m <= MONTH_COUNT; m++) headers.push("Month " + m);
  headers.push("Total Issued", "Total Paid", "Total Expected", "Payment Status", "Remarks", "Source Sheet");
  headers.push("UniqueKey"); // appended last

  // --- prepare output sheet (do not clear) ---
  let outSheet = RESULT_SS.getSheetByName(SHEET_NAME);
  if (!outSheet) {
    outSheet = RESULT_SS.insertSheet(SHEET_NAME);
    outSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  } else {
    // ensure header exists and has the UniqueKey column
    const existingHdr = outSheet.getRange(1, 1, 1, Math.max(outSheet.getLastColumn(), headers.length)).getValues()[0];
    if (existingHdr.length < headers.length || existingHdr.slice(0, headers.length).join("|") !== headers.join("|")) {
      outSheet.insertColumnsAfter(existingHdr.length || 1, Math.max(0, headers.length - (existingHdr.length || 1)));
      outSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    } else {
      outSheet.getRange(1, 1, 1, headers.length).setValues([headers]); // ensure consistent header text/order
    }
  }

  // --- load existing rows into map: key -> rowNum and also keep full oldRows for quick compare ---
  const lastRow = Math.max(outSheet.getLastRow(), 1);
  const lastCol = headers.length;
  const existingMap = Object.create(null);
  let oldRows = [];
  if (lastRow > 1) {
    // read existing data but only columns equal to headers length
    oldRows = outSheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
    for (let i = 0; i < oldRows.length; i++) {
      const rowArr = oldRows[i];
      const key = String(rowArr[rowArr.length - 1] || "").trim(); // UniqueKey is last column
      if (key) existingMap[key] = i + 2; // store actual sheet row number
    }
  }

  // --- helper small functions ---
  function buildUniqueKey(values) {
    // values array matches your normalized row except the UniqueKey (which we add later)
    const customer = String(values[0] || "").trim();           // Customer Name
    const contract = String(values[2] || "").trim();           // Contract IDs
    const pack = String(values[4] || "").trim();               // Package
    const qty = String(values[5] || "").trim();                // Qty
    const source = String(values[values.length - 1] || "").trim(); // Source Sheet

    return `${customer}|${contract}|${pack}|${qty}|${source}`;
  }
  function rowsEqual(a, b) {
    if (!a || !b) return false;
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      // normalize empty vs "" vs null
      const sa = a[i] === null || a[i] === undefined ? "" : String(a[i]);
      const sb = b[i] === null || b[i] === undefined ? "" : String(b[i]);
      if (sa !== sb) return false;
    }
    return true;
  }

  // --- Pull from sources (reuse your existing pullAndNormalizeSheet) ---
  const newRows = []; // will hold objects: { values: [...], fonts: [...], bgs: [...] , uniqueKey }
  // Source A
  const A_SOURCE_ID = "1lLm5phswPd1xNnwNV84pEfrYL02qEz4Q1x5lw0jwl0c";
  const A_SOURCE_SS = SpreadsheetApp.openById(A_SOURCE_ID);
  const A_SHEETS = ["SME (ALL)", "IND (ALL)"];
  for (const name of A_SHEETS) {
    const sheet = A_SOURCE_SS.getSheetByName(name);
    if (!sheet) continue;
    const pulled = pullAndNormalizeSheet(sheet, 2, "A_" + name, headers.slice(0, -1), MONTH_COUNT);
    // pullAndNormalizeSheet returns arrays of { values, fonts, bgs } where values length matches headers without UniqueKey
    for (const o of pulled) {
      const valuesWithSource = o.values.slice(); // make a copy
      // ensure Source Sheet is in the last expected source position (before UniqueKey)
      // Note: pullAndNormalizeSheet used your headers (without UniqueKey). Last column should be "Source Sheet"
      const uniqueKey = buildUniqueKey(valuesWithSource);
      valuesWithSource.push(uniqueKey); // append UniqueKey at end
      newRows.push({ values: valuesWithSource, fonts: o.fonts, bgs: o.bgs, key: uniqueKey });
    }
  }

  // Source OTHER (B-Z)
  const OTHER_SOURCE_ID = "177xFK3HChEvHWzWfsDm-8m2G2EUn4Y0RTleoqxRbdVM";
  const OTHER_SOURCE_SS = SpreadsheetApp.openById(OTHER_SOURCE_ID);
  const OTHER_SHEETS = OTHER_SOURCE_SS.getSheets().map(s => s.getName()).filter(name => /^[B-Z]$/i.test(name));
  for (const name of OTHER_SHEETS) {
    const sheet = OTHER_SOURCE_SS.getSheetByName(name);
    if (!sheet) continue;
    const startCol = name === "D" ? 1 : 2;
    const pulled = pullAndNormalizeSheet(sheet, startCol, "Other_" + name, headers.slice(0, -1), MONTH_COUNT);
    for (const o of pulled) {
      const valuesWithSource = o.values.slice();
      const uniqueKey = buildUniqueKey(valuesWithSource);
      valuesWithSource.push(uniqueKey);
      newRows.push({ values: valuesWithSource, fonts: o.fonts, bgs: o.bgs, key: uniqueKey });
    }
  }

  // --- Build maps of new keys for fast lookup ---
  const newMap = Object.create(null);
  for (let i = 0; i < newRows.length; i++) newMap[newRows[i].key] = newRows[i];

  // --- Determine inserts, updates, deletes ---
  const toInsert = [];
  const toUpdate = []; // { rowNum, values, fonts, bgs }
  const seenKeys = new Set();

  for (const nr of newRows) {
    const key = nr.key;
    seenKeys.add(key);
    if (!existingMap[key]) {
      toInsert.push(nr);
    } else {
      // compare with existing row values read earlier (oldRows)
      const rowNum = existingMap[key];
      const oldRow = oldRows[rowNum - 2] || []; // array of values
      // Build the new values array shaped to full headers length
      const newVals = nr.values.slice(); // should already match headers length
      if (!rowsEqual(oldRow, newVals)) {
        toUpdate.push({ rowNum: rowNum, values: newVals, fonts: nr.fonts, bgs: nr.bgs });
      }
    }
  }

  // Collect deletes (existing keys not in new)
  const toDeleteRowNums = [];
  for (const key in existingMap) {
    if (!seenKeys.has(key)) toDeleteRowNums.push(existingMap[key]);
  }
  toDeleteRowNums.sort((a, b) => b - a); // delete from bottom-up

  // --- Perform deletes (bottom up) ---
  const startTimeDeletes = Date.now();
  for (const rowNum of toDeleteRowNums) {
    outSheet.deleteRow(rowNum);
  }
  if (toDeleteRowNums.length) Logger.log(`Deleted ${toDeleteRowNums.length} rows (t=${Date.now()-startTimeDeletes}ms)`);

  // After deletes, the sheet row numbers changed. We must recompute existingMap for updates.
  // Rebuild existingMap if we have updates to do (cheap)
  if (toUpdate.length > 0) {
    const newLastRow = Math.max(outSheet.getLastRow(), 1);
    const newOldRows = outSheet.getRange(2, 1, Math.max(0, newLastRow - 1), lastCol).getValues();
    const newExistingMap = Object.create(null);
    for (let i = 0; i < newOldRows.length; i++) {
      const key = String(newOldRows[i][newOldRows[i].length - 1] || "").trim();
      if (key) newExistingMap[key] = i + 2;
    }
    // update rowNums in toUpdate according to newExistingMap
    for (let i = 0; i < toUpdate.length; i++) {
      const key = String(toUpdate[i].values[toUpdate[i].values.length - 1] || "").trim();
      if (newExistingMap[key]) {
        toUpdate[i].rowNum = newExistingMap[key];
      } else {
        // sometimes a row we intended to update got pushed down; fallback to treat it as insert
        toInsert.push({ values: toUpdate[i].values, fonts: toUpdate[i].fonts, bgs: toUpdate[i].bgs, key: key });
        toUpdate[i] = null;
      }
    }
    // filter out nulls
    for (let i = toUpdate.length - 1; i >= 0; i--) if (!toUpdate[i]) toUpdate.splice(i, 1);
  }

  // --- Perform updates in contiguous batches to minimize API calls ---
  toUpdate.sort((a, b) => a.rowNum - b.rowNum);
  let batchUpdates = [];
  let batchFonts = [];
  let batchBgs = [];
  let batchStartRow = null;

  function flushBatchUpdate() {
    if (!batchUpdates.length) return;
    outSheet.getRange(batchStartRow, 1, batchUpdates.length, lastCol).setValues(batchUpdates);
    // apply formatting fonts and bgs for each row in batch
    if (batchFonts.length) {
      outSheet.getRange(batchStartRow, 1, batchFonts.length, lastCol).setFontColors(batchFonts);
      outSheet.getRange(batchStartRow, 1, batchBgs.length, lastCol).setBackgrounds(batchBgs);
    }
    batchUpdates = [];
    batchFonts = [];
    batchBgs = [];
    batchStartRow = null;
  }

  for (const u of toUpdate) {
    if (batchStartRow === null) {
      batchStartRow = u.rowNum;
      batchUpdates.push(u.values);
      batchFonts.push(u.fonts);
      batchBgs.push(u.bgs);
    } else if (u.rowNum === batchStartRow + batchUpdates.length) {
      // contiguous row -> append
      batchUpdates.push(u.values);
      batchFonts.push(u.fonts);
      batchBgs.push(u.bgs);
    } else {
      // flush then start new batch
      flushBatchUpdate();
      batchStartRow = u.rowNum;
      batchUpdates.push(u.values);
      batchFonts.push(u.fonts);
      batchBgs.push(u.bgs);
    }
  }
  flushBatchUpdate();
  if (toUpdate.length) Logger.log(`Updated ${toUpdate.length} rows.`);

  // --- Perform inserts in a single batch appended to bottom (if any) ---
  if (toInsert.length > 0) {
    const appendVals = toInsert.map(t => t.values);
    const appendFonts = toInsert.map(t => t.fonts);
    const appendBgs = toInsert.map(t => t.bgs);

    const writeStartRow = outSheet.getLastRow() + 1;
    outSheet.getRange(writeStartRow, 1, appendVals.length, lastCol).setValues(appendVals);

    // apply fonts/backgrounds for appended rows
    outSheet.getRange(writeStartRow, 1, appendFonts.length, lastCol).setFontColors(appendFonts);
    outSheet.getRange(writeStartRow, 1, appendBgs.length, lastCol).setBackgrounds(appendBgs);

    Logger.log(`Inserted ${toInsert.length} rows at row ${writeStartRow}.`);
  }

  // --- Hide UniqueKey column (last column) ---
  const uniqueKeyColIndex = headers.length;
  try {
    outSheet.hideColumns(uniqueKeyColIndex);
  } catch (e) {
    // if already hidden or index invalid, ignore
  }

  // --- Final cosmetics: freeze header, center A-G (col 1..7) ---
  outSheet.setFrozenRows(1);
  if (outSheet.getLastRow() > 1) {
    const rowsForAlign = Math.max(1, outSheet.getLastRow() - 1);
    outSheet.getRange(2, 1, rowsForAlign, 7).setHorizontalAlignment("center");
  }

  Logger.log(`Incremental compile complete. Inserts: ${toInsert.length}, Updates: ${toUpdate.length}, Deletes: ${toDeleteRowNums.length}`);
}
