function buildContractView() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const srcSheet = ss.getSheetByName("Integrated");
  if (!srcSheet) throw new Error("Source sheet 'Integrated' not found.");
  
  const outSheetName = "Contract View";
  let outSheet = ss.getSheetByName(outSheetName);
  if (!outSheet) outSheet = ss.insertSheet(outSheetName);
  outSheet.clearContents();

  const rows = srcSheet.getDataRange().getValues();
  if (rows.length <= 1) {
    Logger.log("No data found.");
    return;
  }

  const headers = rows[0];
  const data = rows.slice(1);
  
  const idx = {
    siteId: headers.indexOf("Con. ID (site)"),
    sheetId: headers.indexOf("Con. ID (sheet)"),
    cust: headers.indexOf("Customer Name"),
    pic: headers.indexOf("PIC Name"),
    seg: headers.indexOf("Customer Segment"),
    status: headers.indexOf("Status"),
    start: headers.indexOf("Start Date"),
    end: headers.indexOf("End Date"),
    period: headers.indexOf("Period"),
    sku: headers.indexOf("SKU"),
    val: headers.indexOf("Contract Value"),
    flags: headers.indexOf("Flags")
  };

  // --- Group by Con. ID (site), fallback to Con. ID (sheet) ---
  const groups = {};
  for (const r of data) {
    const siteId = (r[idx.siteId] || "").toString().trim();
    const sheetId = (r[idx.sheetId] || "").toString().trim();
    const key = siteId || `SHEETONLY:${sheetId}`;
    if (!key) continue;
    if (!groups[key]) groups[key] = [];
    groups[key].push(r);
  }

  const merged = [];

  for (const key in groups) {
    const rows = groups[key];
    const combineUnique = (colIdx) =>
      [...new Set(rows.map(r => (r[colIdx] || "").toString().trim()).filter(v => v))].join(" || ");

    const siteId = key.startsWith("SHEETONLY:") ? "" : key;
    const sheetIds = combineUnique(idx.sheetId);
    const skuCombined = combineUnique(idx.sku);
    const siteIdFinal = siteId || `SHEET_${sheetIds.split(" || ")[0] || ""}`;

    // --- Sum contract values ---
    let totalVal = 0;
    for (const r of rows) {
      const val = parseFloat((r[idx.val] || "").toString().replace(/[^0-9.]/g, ""));
      if (!isNaN(val)) totalVal += val;
    }
    const totalValStr = totalVal > 0 ? totalVal.toFixed(2) : "N/A";

    const fieldsToCheck = [
      { key: "Customer Name", idx: idx.cust },
      { key: "PIC Name", idx: idx.pic },
      { key: "Customer Segment", idx: idx.seg },
      { key: "Status", idx: idx.status },
      { key: "Start Date", idx: idx.start },
      { key: "End Date", idx: idx.end },
      { key: "Period", idx: idx.period }
    ];

    const conflicts = [];
    const values = {};
    for (const f of fieldsToCheck) {
      const uniqueVals = [...new Set(rows.map(r => (r[f.idx] || "").toString().trim()))].filter(Boolean);
      let val = uniqueVals[0] || "";
      
      // --- Handle date formatting ---
      if (f.key.includes("Date") && val) {
        if (val instanceof Date) {
          val = Utilities.formatDate(val, Session.getScriptTimeZone(), "yyyy-MM-dd");
        } else if (/GMT|UTC/.test(val)) {
          const d = new Date(val);
          if (!isNaN(d)) val = Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
        }
      }

      // --- Handle status priority rules ---
      if (f.key === "Status" && uniqueVals.length > 0) {
        const joined = uniqueVals.join(" || ").toUpperCase();
        if (joined.includes("LIVE")) {
          val = "LIVE";
        } else if (joined.includes("END CONTRACT")) {
          val = "INACTIVE";
        }
      }

      // --- Fallback UNKNOWN status ---
      if (f.key === "Status" && (uniqueVals.length === 0 || val === "" || val === "N/A")) {
        val = "UNKNOWN";
      }

      values[f.key] = val;
      if (uniqueVals.length > 1) conflicts.push(f.key);
    }

    const contractType = key.startsWith("SHEETONLY:") ? "Sheet only" : "Integrated";
    let flagMsg = conflicts.length ? `Conflict: ${conflicts.join(", ")}` : "";
    if (contractType === "Sheet only") {
      flagMsg = flagMsg ? `${flagMsg} | Sheet only` : "Sheet only";
    }

    merged.push([
      siteIdFinal, sheetIds,
      values["Customer Name"], values["PIC Name"],
      values["Customer Segment"], values["Status"],
      values["Start Date"], values["End Date"],
      values["Period"], skuCombined, totalValStr,
      contractType, flagMsg
    ]);
  }

  // --- Separate UNKNOWN rows ---
  const unknownRows = merged.filter(r => (r[5] || "").toUpperCase() === "UNKNOWN");
  const knownRows = merged.filter(r => (r[5] || "").toUpperCase() !== "UNKNOWN");

  // --- Sort known rows by latest start date ---
  knownRows.sort((a, b) => new Date(b[6] || 0) - new Date(a[6] || 0));

  const outHeaders = [
    "Con. ID (site)", "Con. ID (sheet)", "Customer Name", "PIC Name",
    "Customer Segment", "Status", "Start Date", "End Date",
    "Period", "SKU", "Total Contract Value", "Contract Type", "Flags"
  ];

  // --- Write Contract View (without UNKNOWN) ---
  outSheet.getRange(1, 1, 1, outHeaders.length).setValues([outHeaders]);

  const CHUNK_SIZE = 3000;
  for (let i = 0; i < knownRows.length; i += CHUNK_SIZE) {
    const chunk = knownRows.slice(i, i + CHUNK_SIZE);
    outSheet.getRange(i + 2, 1, chunk.length, outHeaders.length).setValues(chunk);
    Utilities.sleep(200);
  }

  // --- Write Unknown Sheet ---
  let unknownSheet = ss.getSheetByName("Unknown");
  if (!unknownSheet) unknownSheet = ss.insertSheet("Unknown");
  unknownSheet.clearContents();

  if (unknownRows.length > 0) {
    unknownSheet.getRange(1, 1, 1, outHeaders.length).setValues([outHeaders]);
    for (let i = 0; i < unknownRows.length; i += CHUNK_SIZE) {
      const chunk = unknownRows.slice(i, i + CHUNK_SIZE);
      unknownSheet.getRange(i + 2, 1, chunk.length, outHeaders.length).setValues(chunk);
      Utilities.sleep(200);
    }
    Logger.log(`Unknown contracts exported: ${unknownRows.length} rows.`);
  } else {
    unknownSheet.getRange(1, 1).setValue("No UNKNOWN status contracts found.");
  }

  Logger.log(`Contract View generated: ${knownRows.length} rows (excluding UNKNOWN).`);
}
