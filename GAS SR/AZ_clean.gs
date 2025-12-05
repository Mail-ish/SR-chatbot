// ======================
// Compiled_Raw -> 1-Contract(sheet)
// Group by customer + package + qty + start date
// Preserve contract fonts, invoice backgrounds/fonts, remarks fonts/bg
// Flag renewals as "Renewal from <baseContract>" and do NOT merge them with originals
// Flag conflicts: "Conflict – Invoice Data", "Conflict – Start Date", "Conflict – Status", etc.
// True Merge: only final merged rows are output
// Logs merge mapping & conflicts to Execution log
// ======================

function cleanSheets() {
  ProcessSheetContracts();  // your existing function
  generate2Contract();  // this new stage
}

/*
// --- FIX: Robust start date normalization ---
function normalizeDateForGrouping(dateStr) {
  if (!dateStr) return "";
  const months = {
    "JANUARY":1,"FEBRUARY":2,"MARCH":3,"APRIL":4,"MAY":5,"JUNE":6,
    "JULY":7,"AUGUST":8,"SEPTEMBER":9,"OCTOBER":10,"NOVEMBER":11,"DECEMBER":12,
    "JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"OCT":10,"NOV":11,"DEC":12
  };
  const parts = dateStr.toString().trim().split(/[-\/\s]+/); // split by -, / or space
  if (parts.length < 3) return dateStr.trim(); // fallback
  let day = parseInt(parts[0],10);
  let month = parts[1].toUpperCase();
  let year = parseInt(parts[2],10);
  if (isNaN(day) || isNaN(year)) return dateStr.trim();
  month = months[month] || parseInt(month,10);
  if (!month || month < 1 || month > 12) return dateStr.trim();
  return `${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
}
*/

// === Helper: Normalize and stabilize date format ===
function normalizeDate(value) {
  if (!value) return "";
  try {
    // Convert "5-November-2023" → "2023-11-05"
    const tryParse = new Date(value);
    if (!isNaN(tryParse)) {
      const local = new Date(tryParse.getFullYear(), tryParse.getMonth(), tryParse.getDate());
      return local.toISOString().split("T")[0]; // yyyy-mm-dd
    }

    // If parsing failed (e.g., "5-November-2023" text), fix long month names
    const fixed = value.toString().replace(
      /(\d{1,2})-([A-Za-z]+)-(\d{4})/,
      (_, d, m, y) => {
        const short = m.slice(0, 3); // Nov, Dec, etc.
        return `${d}-${short}-${y}`;
      }
    );
    const parsed = new Date(fixed);
    if (!isNaN(parsed)) {
      const local = new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
      return local.toISOString().split("T")[0];
    }

    return value.toString().trim();
  } catch {
    return value.toString().trim();
  }
}


/* === Status formatting mapping === */
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

/* === Summarize invoices (keeps your original logic) === */
function summarizeInvoices(values, bgs, fonts) {
  let seenEndEarly = false;
  let missingGap = false;
  let confirmed = 0, pending = 0, missing = 0;
  let invoiceCells = 0;

  let firstIdx = -1, lastIdx = -1;
  for (let i = 0; i < values.length; i++) {
    const bg = (bgs[i] || "").toLowerCase();
    if (values[i] || bg.includes("000000") || bg.includes("ff0000") || bg.includes("00ff00") || bg.includes("ffff00")) {
      if (firstIdx === -1) firstIdx = i;
      lastIdx = i;
    }
  }

  if (firstIdx !== -1) {
    for (let i = firstIdx; i <= lastIdx; i++) {
      const bg = (bgs[i] || "").toLowerCase();
      const val = values[i];
      if (!val && (bg === "" || bg === "#ffffff" || bg.includes("ffffff"))) {
        missingGap = true;
        break;
      }
    }
  }

  for (let i = 0; i < values.length; i++) {
    const bg = (bgs[i] || "").toLowerCase();
    if (!values[i] && (bg === "" || bg.includes("ffffff"))) continue;

    if (bg.includes("00ff00")) { // Green
      confirmed++;
      invoiceCells++;
    } else if (bg.includes("ffff00")) { // Yellow
      pending++;
      invoiceCells++;
    } else if (bg.includes("ff0000")) { // Red
      missing++;
      invoiceCells++;
    } else if (bg.includes("000000")) { // Black
      seenEndEarly = true; // don’t count as invoice
    } else {
      pending++;
      invoiceCells++;
    }
  }

  let remarks = [];

  if (seenEndEarly) {
    if (confirmed > 0 && confirmed === invoiceCells) {
      remarks.push("Ended early; Paid in full");
    } else {
      remarks.push("Ended early");
    }
  }

  if (missingGap) remarks.push("Missing invoice(s)");
  if (confirmed > 0 && confirmed === invoiceCells && !seenEndEarly) remarks.push("Paid in full");
  if (pending > 0) remarks.push(`Outstanding payment(s) (${pending})`);
  if (missing > 0) remarks.push(`Missing/unexpected invoice(s) (${missing})`);

  return remarks.length === 0 ? "No invoices" : remarks.join("; ");
}

/* === Helper: clean package text like in Cleaned_New === */
function cleanPackage(pkg) {
  if (pkg === null || pkg === undefined) return "";
  return String(pkg).trim().replace(/\s+/g, " ");
}

/* === Extract trailing block strictly:
   - takes entire suffix after the last '-'
   - returns the suffix only if it's ALL digits and length >= 3
   - otherwise returns null
*/
function getTrailingBlock(contract) {
  if (!contract) return null;
  const m = String(contract).trim().match(/-([^-]+)$/);
  if (!m) return null;
  const suffix = m[1];
  if (/^\d{3,}$/.test(suffix)) return suffix;
  return null;
}

/* === Check strict renewal: only suffix "-1" counts === */
function isStrictRenewal(contract) {
  return /-1$/.test(String(contract || ""));
}

/* === Process one row into grouped structure ===
   Group key: customer + "||" + pkg + "||" + qty + "||" + startDate
   Renewals (only ending with -1) are flagged and never merged with originals.
   Trailing block merging uses full trailing block (digits >=3).
*/
function processRow(row, idx, fonts, bgs, grouped, config, contractCount) {
  const sheetRow = idx + 2;
  const {
    CUSTOMER_COL, CUSTOMER_TYPE_COL, CONTRACT_COL, STARTDATE_COL,
    PACKAGE_COL, QTY_COL, STATUS_COL, INVOICE_START, INVOICE_END,
    TOTAL_ISSUED_COL, TOTAL_PAID_COL, TOTAL_EXPECTED_COL,
    PAYMENT_STATUS_COL, REMARKS_COL, SOURCE_COL, MONTH_COUNT
  } = config;

  const customer = (row[CUSTOMER_COL] || "").toString().trim();
  let customerType = (row[CUSTOMER_TYPE_COL] || "").toString().trim();
  let pkg = cleanPackage(row[PACKAGE_COL] || "");
  const qty = (row[QTY_COL] || "").toString().trim();
  const contract = row[CONTRACT_COL] ? String(row[CONTRACT_COL]).trim() : "";
  let status = row[STATUS_COL] ? String(row[STATUS_COL]).trim() : "";
  // take raw start value then normalize
  const rawStart = row[STARTDATE_COL] || "";

  const totalIssued = row[TOTAL_ISSUED_COL] || "";
  const totalPaid = row[TOTAL_PAID_COL] || "";
  const totalExpected = row[TOTAL_EXPECTED_COL] || "";
  const paymentStatus = row[PAYMENT_STATUS_COL] || "";
  const remarks = row[REMARKS_COL] || "";
  const sourceSheet = row[SOURCE_COL] || "";

  // 1) Standardize customer type
  if (!customerType) {
    if (/SDN BHD|BERHAD|ENTERPRISE/i.test(customer)) customerType = "SME";
    else customerType = "Individual";
  }

  // 2) Standardize status variants
  if (/rent to own|end contract|terminated/i.test(status)) {
    status = "INACTIVE-A (RTO/EOC/TER)";
    row[STATUS_COL] = status;
  }

  // 3) Normalize start date to ISO yyyy-mm-dd (helper below)
  const normalizedDate = normalizeDate(rawStart); // returns "" or "YYYY-MM-DD"

  // 4) Guard: skip rows that lack customer or contract (make unique row key)
  if (!customer || !contract) {
    const key = `ROW${sheetRow}`;
    grouped[key] = {
      customer: customer || "MISSING",
      customerType,
      customerNames: [customer || "MISSING"],
      contracts: contract ? [contract] : [],
      contractFonts: [(fonts[idx]?.[CONTRACT_COL]) || "#000000"],
      startDate: normalizedDate,
      pkg,
      qty,
      status,
      totalIssued, totalPaid, totalExpected,
      paymentStatus, remarks, sourceSheet,
      invoiceStatus: "Skipped – missing customer or contract",
      statusFont: "#000000", statusBg: "#ffffff",
      invoices: row.slice(INVOICE_START, INVOICE_END + 1),
      invoiceFonts: (fonts[idx]?.slice(INVOICE_START, INVOICE_END + 1)) || Array(MONTH_COUNT).fill(""),
      invoiceBgs: (bgs[idx]?.slice(INVOICE_START, INVOICE_END + 1)) || Array(MONTH_COUNT).fill(""),
      remarkFont: (fonts[idx]?.[REMARKS_COL]) || "#000000",
      remarkBg: (bgs[idx]?.[REMARKS_COL]) || "#ffffff",
      notes: ["Skipped – missing customer or contract"],
      _sources: contract ? [{ contract, rowIndex: sheetRow }] : []
    };
    return;
  }

  // 5) Renewals/trailing block
  const isRenewal = !!isStrictRenewal(contract);
  const baseContract = isRenewal ? contract.replace(/-1$/, "") : null;
  const trailingBlock = getTrailingBlock(contract);

  const invoiceVals = row.slice(INVOICE_START, INVOICE_END + 1);
  const invoiceFonts = (fonts[idx]?.slice(INVOICE_START, INVOICE_END + 1)) || Array(MONTH_COUNT).fill("");
  const invoiceBgs = (bgs[idx]?.slice(INVOICE_START, INVOICE_END + 1)) || Array(MONTH_COUNT).fill("");
  const rawContractFont = (fonts[idx]?.[CONTRACT_COL]) || "#000000";
  const statusFmt = getStatusFormat(status);

  // 6) Primary groupKey includes pkg — but if no match, we'll try a fallback that ignores pkg
  const safePkg = pkg || "(no_pkg)";
  let groupKey = `${customer}||${safePkg}||${qty || "(no_qty)"}||${normalizedDate}`;

  // 6a) Only perform fallback merge if *pkg is blank*
  if (!grouped[groupKey] && !pkg) {
    const simpleMatchKey = Object.keys(grouped).find(k => {
      const parts = k.split("||");
      if (parts.length < 4) return false;
      const kcust = parts[0].trim();
      const kqty = parts[2].trim();
      const kdate = parts[3].trim();
      return kcust === customer && kqty === (qty || "") && kdate === normalizedDate;
    });
    if (simpleMatchKey) {
      groupKey = simpleMatchKey;
      if (grouped[groupKey].pkg) pkg = grouped[groupKey].pkg;
    }
  }


  // 7) If group still not exist, create it
  if (!grouped[groupKey]) {
    grouped[groupKey] = {
      customer, customerType,
      customerNames: [customer],
      contracts: [contract],
      contractFonts: [rawContractFont],
      startDate: normalizedDate,
      pkg, qty, status,
      totalIssued, totalPaid, totalExpected,
      paymentStatus, remarks, sourceSheet,
      invoiceStatus: summarizeInvoices(invoiceVals, invoiceBgs, invoiceFonts),
      statusFont: statusFmt.font, statusBg: statusFmt.bg,
      invoices: invoiceVals.slice(),
      invoiceFonts: invoiceFonts.slice(),
      invoiceBgs: invoiceBgs.slice(),
      remarkFont: (fonts[idx]?.[REMARKS_COL]) || "#000000",
      remarkBg: (bgs[idx]?.[REMARKS_COL]) || "#ffffff",
      notes: isRenewal ? [`Renewal from ${baseContract}`] : [],
      isRenewal, trailingBlock,
      _sources: [{ contract, rowIndex: sheetRow }]
    };
    return;
  }

  // 8) Merge into existing group
  const g = grouped[groupKey];

  // propagate pkg/status back into row and group
  if (!pkg && g.pkg) { pkg = g.pkg; row[PACKAGE_COL] = pkg; }
  else if (!g.pkg && pkg) g.pkg = pkg;
  if (!status && g.status) { status = g.status; row[STATUS_COL] = status; }
  else if (!g.status && status) g.status = status;

  // SME(ALL) priority rules unchanged
  const thisSheet = sourceSheet === "A_SME (ALL)" ? "SME (ALL)" : sourceSheet;
  const existingSheet = g.sourceSheet === "A_SME (ALL)" ? "SME (ALL)" : g.sourceSheet;
  if (thisSheet === "SME (ALL)" && existingSheet !== "SME (ALL)") {
    // replace with SME(ALL) authoritative row
    grouped[groupKey] = { ...g, ...{
      contracts: [contract],
      contractFonts: [(fonts[idx]?.[CONTRACT_COL]) || "#000000"],
      status, pkg, startDate: normalizedDate, sourceSheet: thisSheet,
      totalIssued, totalPaid, totalExpected,
      paymentStatus, remarks,
      invoices: invoiceVals.slice(),
      invoiceFonts: invoiceFonts.slice(),
      invoiceBgs: invoiceBgs.slice(),
      invoiceStatus: summarizeInvoices(invoiceVals, invoiceBgs, invoiceFonts),
      notes: (g.notes || []).concat(["Replaced non-SME with SME (ALL) data"])
    }};
    return;
  }
  if (existingSheet === "SME (ALL)" && thisSheet !== "SME (ALL)") return;

  // merge invoice cells
  for (let i = 0; i < MONTH_COUNT; i++) {
    if ((!g.invoices[i] || g.invoices[i] === "") && invoiceVals[i]) {
      g.invoices[i] = invoiceVals[i];
      g.invoiceFonts[i] = invoiceFonts[i];
      g.invoiceBgs[i] = invoiceBgs[i];
    }
  }

  // merge metadata
  g._sources.push({ contract, rowIndex: sheetRow });
  if (!g.contracts.includes(contract)) g.contracts.push(contract);
  if (isRenewal && !g.notes.some(n => n.includes("Renewal"))) {
    g.notes.push(`Renewal from ${baseContract}`);
    g.isRenewal = true;
  }
  g.totalIssued = g.totalIssued || totalIssued;
  g.totalPaid = g.totalPaid || totalPaid;
  g.totalExpected = g.totalExpected || totalExpected;
  g.paymentStatus = g.paymentStatus || paymentStatus;
  g.remarks = g.remarks || remarks;
  g.invoiceStatus = summarizeInvoices(g.invoices, g.invoiceBgs, g.invoiceFonts);
}


/* === Write grouped results to sheet === */
function writeResultsToSheet(ss, grouped, MONTH_COUNT, sheetName, contractCount) {
  let outSheet = ss.getSheetByName(sheetName);
  if (!outSheet) outSheet = ss.insertSheet(sheetName);
  else outSheet.clear();

  // Headers
  const headers = [
    "Customer Name", "Customer Type", "Contract IDs", "Package", "Qty", "Status", "Start Date"
  ];
  for (let m = 1; m <= MONTH_COUNT; m++) headers.push("Month " + m);
  headers.push("Total Issued", "Total Paid", "Total Expected", "Payment Status", "Remarks", "Flags", "Invoice Status", "Source Sheet");

  const results = [];
  const statusFonts = [];
  const statusBgs = [];
  const invoiceFonts = [];
  const invoiceBgs = [];
  const contractFonts = [];
  const remarkFonts = [];
  const remarkBgs = [];

  const groupedVals = Object.values(grouped);

  /** Fill missing statuses ONLY at final output stage */
  function fillStatusForOutput(entry) {
    let status = entry.status ? entry.status.trim() : "";
    if (status) return; // already has status

    const pkg = (entry.pkg || "").toUpperCase();
    const start = entry.startDate ? new Date(entry.startDate) : null;
    if (!start || isNaN(start)) return;

    let months = 0;
    if (/^(SRLP|SRDT|SRIPAD87)/i.test(pkg)) months = 24;
    else if (/^(SAPLP|SAPDT)/i.test(pkg)) months = 36;
    else return;

    const endDate = new Date(start);
    endDate.setMonth(start.getMonth() + months);

    const today = new Date();
    entry.status = today > endDate ? "END CONTRACT" : "LIVE";
  }

  // For merge-logging (map: sourceContract -> masterContract)
  const mergeLog = [];

  // Determine master selection for groups that have multiple contracts:
  // We will choose the contract with earliest startDate among that group's contracts as the master id.
  // However, grouped entry has single startDate (groups are created by startDate), so choose alphabetically earliest contract as the anchor.
  // (This is consistent because groups are per start date already.)
  groupedVals.forEach(g => {

    // FINAL-STAGE STATUS FILLER — runs ONLY before output
    fillStatusForOutput(g);

    const customerTypeClean = (g.customerType || "").replace(/\s*\|\|\s*$/g, "");
    const custName = g.customerNames ? g.customerNames.join(" || ") : (g.customer || "");

    // Determine final flag (Merged / Merged by trailing numbers (XXX) / blank)
    let flag = "";
    if (g._sources && g._sources.length > 1) {
      // There were merges. Determine if any merge used a trailing block:
      // If the merged-in sources include contracts that differ only by trailing block we can indicate trailing used.
      // We will check if group had trailingBlock on any item and use that in message.
      const trailingUsed = g.trailingBlock ? g.trailingBlock : null;
      if (trailingUsed) {
        flag = `Merged by trailing numbers (${trailingUsed})`;
      } else {
        flag = "Merged";
      }

      // Build merge log entries: each source contract -> masterContract (we choose the first contract in g.contracts list as master)
      const masterContract = (g.contracts && g.contracts.length > 0) ? g.contracts[0] : "(unknown)";
      for (const s of (g._sources || [])) {
        // skip the master if it equals masterContract
        if (s.contract === masterContract) continue;
        mergeLog.push(`${s.contract} -> ${masterContract}`);
      }
    }

    // Compose output row
    const row = [
      custName,
      customerTypeClean,
      (g.contracts || []).join(" || "),
      g.pkg || "",
      g.qty || "",
      g.status || "",
      g.startDate || "",
      ...(g.invoices || Array(MONTH_COUNT).fill("")),
      g.totalIssued || "",
      g.totalPaid || "",
      g.totalExpected || "",
      g.paymentStatus || "",
      g.remarks || "",
      (g.notes || []).join("; "),
      g.invoiceStatus || "",
      g.sourceSheet || ""
    ];
    results.push(row);

    statusFonts.push([g.statusFont || "#000000"]);
    statusBgs.push([g.statusBg || "#ffffff"]);

    const invF = (g.invoiceFonts || []).slice();
    const invB = (g.invoiceBgs || []).slice();
    for (let i = 0; i < MONTH_COUNT; i++) {
      if (!invF[i]) invF[i] = "#000000";
      if (!invB[i]) invB[i] = "#ffffff";
    }
    invoiceFonts.push(invF);
    invoiceBgs.push(invB);

    // Decide contract font to apply to Contract IDs column.
    // Preference: preserve first contract font if available.
    // Additionally, if any contract in this group appears >1 in original dataset, mark the contract font red.
    let cFont = (g.contractFonts && g.contractFonts.length > 0) ? g.contractFonts[0] : "#000000";
    const anyDuplicateContract = (g.contracts || []).some(c => contractCount[c] && contractCount[c] > 1);
    if (anyDuplicateContract) {
      // mark red to surface duplicates as requested
      cFont = "#FF0000";
    }
    contractFonts.push([cFont]);

    remarkFonts.push([ g.remarkFont || "#000000" ]);
    remarkBgs.push([ g.remarkBg || "#ffffff" ]);
  });

  // Sort by customer name (col 0)
  const combined = results.map((r, i) => ({
    row: r,
    statusFont: statusFonts[i],
    statusBg: statusBgs[i],
    invFonts: invoiceFonts[i],
    invBgs: invoiceBgs[i],
    contractFont: contractFonts[i],
    remarkFont: remarkFonts[i],
    remarkBg: remarkBgs[i]
  }));

  combined.sort((a, b) => (a.row[0] || "").toLowerCase().localeCompare((b.row[0] || "").toLowerCase()));

  const sortedResults = combined.map(c => c.row);
  const sortedStatusFonts = combined.map(c => c.statusFont);
  const sortedStatusBgs = combined.map(c => c.statusBg);
  const sortedInvoiceFonts = combined.map(c => c.invFonts);
  const sortedInvoiceBgs = combined.map(c => c.invBgs);
  const sortedContractFonts = combined.map(c => c.contractFont);
  const sortedRemarkFonts = combined.map(c => c.remarkFont);
  const sortedRemarkBgs = combined.map(c => c.remarkBg);

  // Write headers + data
  outSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  if (sortedResults.length > 0) {
    outSheet.getRange(2, 1, sortedResults.length, headers.length).setValues(sortedResults);
  }

  // Apply invoice fonts + backgrounds (Month columns)
  const invoiceStartCol = headers.indexOf("Month 1") + 1;
  if (sortedInvoiceFonts.length > 0) {
    outSheet.getRange(2, invoiceStartCol, sortedInvoiceFonts.length, MONTH_COUNT).setFontColors(sortedInvoiceFonts);
    outSheet.getRange(2, invoiceStartCol, sortedInvoiceBgs.length, MONTH_COUNT).setBackgrounds(sortedInvoiceBgs);
  }

  // Apply status formatting (single column)
  const statusColIndex = headers.indexOf("Status") + 1;
  if (sortedStatusFonts.length > 0) {
    outSheet.getRange(2, statusColIndex, sortedStatusFonts.length, 1).setFontColors(sortedStatusFonts);
    outSheet.getRange(2, statusColIndex, sortedStatusBgs.length, 1).setBackgrounds(sortedStatusBgs);
  }

  // Apply contract ID font colors
  const contractColIndex = headers.indexOf("Contract IDs") + 1;
  if (sortedContractFonts.length > 0) {
    outSheet.getRange(2, contractColIndex, sortedContractFonts.length, 1).setFontColors(sortedContractFonts);
  }

  // Apply Remarks font/bg
  const remarksColIndex = headers.indexOf("Remarks") + 1;
  if (sortedRemarkFonts.length > 0) {
    outSheet.getRange(2, remarksColIndex, sortedRemarkFonts.length, 1).setFontColors(sortedRemarkFonts);
    outSheet.getRange(2, remarksColIndex, sortedRemarkBgs.length, 1).setBackgrounds(sortedRemarkBgs);
  }

  // Insert Flags (AW) and Invoice Summary (AX) and Source Sheet (AY)
  // Flags col index:
  const flagsColIndex = headers.indexOf("Flags") + 1;
  const invoiceSummaryColIndex = headers.indexOf("Invoice Status") + 1;
  const sourceColIndex = headers.indexOf("Source Sheet") + 1;

  // Build flags array and also build mapping log (we used grouped._sources earlier)
  const flags = [];
  const invoiceSummary = []; // will mirror Invoice Status already set in row but we can set explicitly
  const sources = [];

  // Because we sorted results we need to match the sorted combined structure to groupedVals order.
  // We'll iterate combined and create flags by repro calculation: find the group matching row's contract list (first contract)
  for (let i = 0; i < combined.length; i++) {
    const row = combined[i].row;
    const contractIDs = row[2] || ""; // "A || B || C"
    const contractsList = contractIDs.split(" || ").map(s => s.trim()).filter(Boolean);
    // find the grouped entry whose contracts string matches (simple approach)
    const gEntry = groupedVals.find(g => ((g.contracts || []).join(" || ")) === contractsList.join(" || "));
    let flagText = "";
    if (gEntry) {
      if (gEntry._sources && gEntry._sources.length > 1) {
        const trailingUsed = gEntry.trailingBlock ? gEntry.trailingBlock : null;
        flagText = trailingUsed ? `Merged by trailing numbers (${trailingUsed})` : `Merged`;
        // log the mapping
        const master = (gEntry.contracts && gEntry.contracts[0]) ? gEntry.contracts[0] : "(master)";
        for (const s of gEntry._sources) {
          if (s.contract === master) continue;
          //Logger.log(`Merged: ${s.contract} -> ${master}`);
        }
      } else {
        // Not merged (single source)
        flagText = (gEntry.notes && gEntry.notes.length > 0) ? (gEntry.notes.join("; ")) : "";
      }
    } else {
      flagText = "";
    }
    flags.push([flagText]);
    invoiceSummary.push([row[invoiceSummaryColIndex - 1]]); // invoice status already in row
    sources.push([row[sourceColIndex - 1] || ""]);
  }

  // Write Flags, Invoice Summary, Source columns
  if (flags.length > 0) {
    outSheet.getRange(2, flagsColIndex, flags.length, 1).setValues(flags);
  }
  if (invoiceSummary.length > 0) {
    // Invoice Status is already in data at column Invoice Status, but ensure it's present in AX (we keep as same col)
    // The header "Invoice Status" exists; nothing to overwrite except ensure cell types match.
  }
  if (sources.length > 0) {
    outSheet.getRange(2, sourceColIndex, sources.length, 1).setValues(sources);
  }

  // Center columns A-G and apply header formatting
  outSheet.getRange(2, 1, outSheet.getLastRow() - 1, Math.min(7, headers.length)).setHorizontalAlignment("center").setVerticalAlignment("middle");
  applyFinalFormatting(outSheet, headers.length);

  Logger.log(`Wrote ${sortedResults.length} rows to "${sheetName}"`);
}

/* === Main orchestrator === */
function ProcessSheetContracts() {
  const ss = SpreadsheetApp.getActive();
  const srcSheet = ss.getSheetByName("Compiled_Raw(Sheets)");
  if (!srcSheet) {
    throw new Error("Source sheet 'Compiled_Raw(Sheets)' not found!");
  }

  const config = {
    CUSTOMER_COL: 0,
    CUSTOMER_TYPE_COL: 1,
    CONTRACT_COL: 2,
    STARTDATE_COL: 3,
    PACKAGE_COL: 4,
    QTY_COL: 5,
    STATUS_COL: 6,
    INVOICE_START: 7,
    INVOICE_END: 42,
    TOTAL_ISSUED_COL: 43,
    TOTAL_PAID_COL: 44,
    TOTAL_EXPECTED_COL: 45,
    PAYMENT_STATUS_COL: 46,
    REMARKS_COL: 47,
    SOURCE_COL: 48,
    MONTH_COUNT: 36
  };

  const numRows = srcSheet.getLastRow() - 1;
  if (numRows <= 0) {
    Logger.log("No data to process in source sheet.");
    return;
  }
  const numCols = srcSheet.getLastColumn();
  const range = srcSheet.getRange(2, 1, numRows, numCols);
  const data = range.getDisplayValues();
  const fonts = range.getFontColors();
  const bgs = range.getBackgrounds();

  Logger.log(`Pulled ${data.length} rows × ${numCols} cols from "${srcSheet.getName()}"`);

  // build contract occurrence count (for duplicate highlighting)
  const contractCount = {};
  for (let i = 0; i < data.length; i++) {
    const c = (data[i][config.CONTRACT_COL] || "").toString().trim();
    if (!c) continue;
    contractCount[c] = (contractCount[c] || 0) + 1;
  }

  const grouped = {};
  for (let i = 0; i < data.length; i++) {
    processRow(data[i], i, fonts, bgs, grouped, config, contractCount);
  }

  // Finally write grouped results
  writeResultsToSheet(ss, grouped, config.MONTH_COUNT, "1-Contract(sheet)", contractCount);
  Logger.log("Cleaning complete. Check '1-Contract(sheet)'.");
}

/* === Final formatting helper === */
function applyFinalFormatting(outSheet, lastCol) {
  const lastRow = outSheet.getLastRow();
  if (lastRow < 1) return;

  const headerRange = outSheet.getRange(1, 1, 1, lastCol);
  headerRange
    .setBackground("#8B008B")
    .setFontColor("#ffffff")
    .setFontWeight("bold")
    .setHorizontalAlignment("center")
    .setVerticalAlignment("middle")
    .setWrap(true);

  outSheet.getRange(1, 1, lastRow, Math.min(7, lastCol))
    .setHorizontalAlignment("center")
    .setVerticalAlignment("middle");

  outSheet.setFrozenRows(1);
  outSheet.setRowHeight(1, 28);
}



// ===========================================================
// 2-CONTRACT STAGE: Merge multi-customer contracts
// ===========================================================

/* ============================================================
   generate2Contract(): build 2-Contract(sheet)
   ============================================================ */
function generate2Contract() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sourceSheet = ss.getSheetByName("1-Contract(sheet)");
  if (!sourceSheet) throw new Error("1-Contract(sheet) not found.");
  const targetSheetName = "2-Contract(sheet)";
  const targetSheet = ss.getSheetByName(targetSheetName) || ss.insertSheet(targetSheetName);

  const range = sourceSheet.getDataRange();
  const values = range.getValues();
  const fonts = range.getFontColors();
  const bgs = range.getBackgrounds();

  if (values.length < 2) {
    Logger.log("No data found in 1-Contract.");
    return;
  }

  const header = values[0];
  const data = values.slice(1);
  const fontData = fonts.slice(1);
  const bgData = bgs.slice(1);

  // column references
  const CUSTOMER_COL = header.indexOf("Customer Name");
  const CUSTOMERTYPE_COL = header.indexOf("Customer Type");
  const CONTRACT_COL = header.indexOf("Contract IDs");
  const STARTDATE_COL = header.indexOf("Start Date");
  const PACKAGE_COL = header.indexOf("Package");
  const QTY_COL = header.indexOf("Qty");
  const STATUS_COL = header.indexOf("Status");
  const REMARKS_COL = header.indexOf("Remarks");

  const FLAG_COL = header.indexOf("Flags"); // optional flag col if exists

  // Detect invoice columns dynamically
  const INVOICE_START = header.findIndex(h => h.toString().toLowerCase().includes("month 1"));
  const INVOICE_END = header.findLastIndex(h => h.toString().toLowerCase().includes("month"));
  const MONTH_COUNT = INVOICE_END - INVOICE_START + 1;

  const grouped = {};

  // === Loop through rows and merge
  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const f = fontData[i];
    const b = bgData[i];
    processRowStage2(row, i, f, b, grouped, {
      CUSTOMER_COL, CUSTOMERTYPE_COL, CONTRACT_COL, STARTDATE_COL,
      PACKAGE_COL, QTY_COL, STATUS_COL, REMARKS_COL,
      INVOICE_START, INVOICE_END, MONTH_COUNT, FLAG_COL
    });
  }

  // === Write output ===
  const output = [header];
  const outFonts = [fonts[0]];
  const outBgs = [bgs[0]];

  for (const key in grouped) {
    const g = grouped[key];
    const row = [...Array(header.length).fill("")];

    row[CUSTOMER_COL] = g.customer;
    row[CUSTOMERTYPE_COL] = g.customerType;
    row[CONTRACT_COL] = g.contracts.join(" || ");
    row[PACKAGE_COL] = g.pkgFull || g.pkg;
    row[QTY_COL] = g.qty;
    row[STATUS_COL] = g.status;
    row[STARTDATE_COL] = g.startDate;
    row[REMARKS_COL] = g.remarks;
    if (FLAG_COL !== -1) row[FLAG_COL] = g.flag;

    for (let j = 0; j < MONTH_COUNT; j++) {
      row[INVOICE_START + j] = g.invoices[j] || "";
    }

    // Copy any untouched columns (Totals, Payment Status, etc.)
    for (let c = 0; c < header.length; c++) {
      if (!row[c] && g.extra && g.extra[c] !== undefined) {
        row[c] = g.extra[c];
      }
    }

    output.push(row);

    // === fonts + backgrounds ===
    const fontRow = Array(header.length).fill("#000000");
    const bgRow = Array(header.length).fill("#ffffff");

    // Contract font (red)
    fontRow[CONTRACT_COL] = g.contractFont || "#000000";
    bgRow[CONTRACT_COL] = g.contractBg || "#ffffff";

    // Invoices
    for (let j = 0; j < MONTH_COUNT; j++) {
      fontRow[INVOICE_START + j] = g.invoiceFonts[j] || "#000000";
      bgRow[INVOICE_START + j] = g.invoiceBgs[j] || "#ffffff";
    }

    // Status + Remarks
    fontRow[STATUS_COL] = g.statusFont || "#000000";
    bgRow[STATUS_COL] = g.statusBg || "#ffffff";
    fontRow[REMARKS_COL] = g.remarkFont || "#000000";
    bgRow[REMARKS_COL] = g.remarkBg || "#ffffff";

    outFonts.push(fontRow);
    outBgs.push(bgRow);
  }

  targetSheet.clear();
  const dest = targetSheet.getRange(1, 1, output.length, output[0].length);
  dest.setValues(output);
  dest.setFontColors(outFonts);
  dest.setBackgrounds(outBgs);

  Logger.log(`✅ 2-Contract generated successfully with ${output.length - 1} rows.`);
}

/* ============================================================
   processRowStage2(): merge multi-customer contracts
   ============================================================ */
function processRowStage2(row, idx, fonts, bgs, grouped, config) {
  const {
    CUSTOMER_COL, CUSTOMERTYPE_COL, CONTRACT_COL,
    STARTDATE_COL, PACKAGE_COL, QTY_COL, STATUS_COL, REMARKS_COL,
    INVOICE_START, INVOICE_END, MONTH_COUNT, FLAG_COL
  } = config;

  // === Safe conversions ===
  const customer = String(row[CUSTOMER_COL] || "").trim();
  let customerType = String(row[CUSTOMERTYPE_COL] || "").trim() || "Individual";
  const contract = String(row[CONTRACT_COL] || "").trim();

  const startDateVal = row[STARTDATE_COL];
  const startDate =
    startDateVal instanceof Date
      ? Utilities.formatDate(startDateVal, Session.getScriptTimeZone(), "yyyy-MM-dd")
      : String(startDateVal || "").trim();

  const pkgFull = cleanPackage(row[PACKAGE_COL] || "");
  const pkgBase = pkgFull.split("/")[0].trim(); // part before slash
  const qty = String(row[QTY_COL] || "").trim();
  const status = String(row[STATUS_COL] || "").trim();
  const remarks = String(row[REMARKS_COL] || "").trim();
  const flagVal = FLAG_COL !== -1 ? String(row[FLAG_COL] || "").trim() : "";

  const invoiceVals = row.slice(INVOICE_START, INVOICE_END + 1);
  const invoiceFonts = fonts.slice(INVOICE_START, INVOICE_END + 1);
  const invoiceBgs = bgs.slice(INVOICE_START, INVOICE_END + 1);

  const trailingMatch = contract.match(/(\d{3,})$/);
  const trailingBlock = trailingMatch ? trailingMatch[1] : contract;
  const mergeKey = `${trailingBlock}||${pkgBase}||${qty}`;

  // === Create new entry ===
  if (!grouped[mergeKey]) {
    grouped[mergeKey] = {
      customer,
      customerNames: [customer],
      customerType,
      contracts: [contract],
      startDate,
      pkg: pkgBase,
      pkgFull,
      qty,
      status,
      remarks,
      invoices: invoiceVals.slice(),
      invoiceFonts: invoiceFonts.slice(),
      invoiceBgs: invoiceBgs.slice(),
      remarkFont: fonts[REMARKS_COL] || "#000000",
      remarkBg: bgs[REMARKS_COL] || "#ffffff",
      statusFont: fonts[STATUS_COL] || "#000000",
      statusBg: bgs[STATUS_COL] || "#ffffff",
      contractFont: fonts[CONTRACT_COL] || "#000000",
      contractBg: bgs[CONTRACT_COL] || "#ffffff",
      flag: flagVal,
      extra: row.slice()
    };
    return;
  }

  // === Merge existing group ===
  const g = grouped[mergeKey];

  // Merge customer type
  g.customerType = (g.customerType === "SME" || customerType === "SME") ? "SME" : "Individual";

  // Merge customers
  const allCustomers = new Set([...g.customerNames, customer]);
  g.customerNames = [...allCustomers];
  g.customer = g.customerNames.join(" || ");

  // Merge contracts
  if (!g.contracts.includes(contract)) g.contracts.push(contract);

  // Merge invoices
  let numericConflict = false;
  for (let i = 0; i < MONTH_COUNT; i++) {
    const ex = g.invoices[i];
    const nx = invoiceVals[i];
    if (!nx) continue;

    const exNum = parseFloat(ex);
    const nxNum = parseFloat(nx);
    const exIsNum = !isNaN(exNum);
    const nxIsNum = !isNaN(nxNum);

    if (exIsNum && nxIsNum && exNum !== nxNum) {
      numericConflict = true;
      break;
    }

    if (!ex) {
      g.invoices[i] = nx;
      g.invoiceFonts[i] = invoiceFonts[i];
      g.invoiceBgs[i] = invoiceBgs[i];
    } else if (!exIsNum && nxIsNum) {
      g.invoices[i] = nx;
      g.invoiceFonts[i] = invoiceFonts[i];
      g.invoiceBgs[i] = invoiceBgs[i];
    }
  }

  if (numericConflict) {
    const sepKey = `${mergeKey}||conflict_${idx + 2}`;
    grouped[sepKey] = {
      ...grouped[mergeKey],
      customer,
      customerNames: [customer],
      contracts: [contract],
      invoices: invoiceVals.slice(),
      invoiceFonts: invoiceFonts.slice(),
      invoiceBgs: invoiceBgs.slice(),
      remarks,
      flag: flagVal,
      extra: row.slice()
    };
    //Logger.log(`Conflict – Invoice Data for contract ${contract} (row ${idx + 2})`);
    return;
  }

  // Merge remarks
  if (remarks && !g.remarks.includes(remarks)) {
    g.remarks += g.remarks ? ` || ${remarks}` : remarks;
  }

  // Merge statuses
  if (status && !g.status.includes(status)) {
    g.status += g.status ? ` || ${status}` : status;
  }

  // Merge start dates
  if (startDate && !g.startDate.includes(startDate)) {
    g.startDate += g.startDate ? ` || ${startDate}` : startDate;
  }

  // Merge flagging — mark only merged groups as "Merged"
  g.flag = "Merged";
}

/* Utility */
function cleanPackage(pkg) {
  return String(pkg).trim().replace(/\s+/g, " ");
}


/* === onOpen menu === */
function onOpen() {
  SpreadsheetApp.getUi().createMenu("Contract Cleaner")
    .addItem("Clean Compiled_Raw", "main")
    .addToUi();
}
