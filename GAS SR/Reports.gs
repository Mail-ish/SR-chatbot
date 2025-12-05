// ===== Global Helper Functions =====
function resetSheet(ss, sheetName) {
  const existing = ss.getSheetByName(sheetName);
  if (existing) ss.deleteSheet(existing);
  return ss.insertSheet(sheetName);
}
function toNum(v) {
  if (v === null || v === undefined || v === "") return NaN;
  if (typeof v === "number") return v;
  const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ""));
  return isNaN(n) ? NaN : n;
}
function addMonths(d, m) {
  const nd = new Date(d);
  nd.setMonth(nd.getMonth() + m);
  return nd;
}
function monthsBetween(start, end) {
  if (!start || !end) return [];
  const res = [];
  let cur = new Date(start.getFullYear(), start.getMonth(), 1);
  const last = new Date(end.getFullYear(), end.getMonth(), 1);
  while (cur <= last) {
    res.push(`${cur.getFullYear()}-${String(cur.getMonth() + 1).padStart(2, "0")}`);
    cur = addMonths(cur, 1);
  }
  return res;
}
function ymFromDate(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
}
function toDate(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  const d = new Date(String(v).trim());
  return isNaN(d) ? null : d;
}

function buildSMEContractListing() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const integrated = ss.getSheetByName("Integrated");
  const contractView = ss.getSheetByName("Contract View");
  if (!integrated || !contractView) throw new Error("Missing required sheets.");

  const outName = "SME Contract Listing";
  let outSheet = ss.getSheetByName(outName);
  if (!outSheet) outSheet = ss.insertSheet(outName);
  outSheet.clearContents();

  function formatDate(val) {
    if (!val) return "N/A";
    if (Object.prototype.toString.call(val) === "[object Date]" && !isNaN(val)) {
      return Utilities.formatDate(val, Session.getScriptTimeZone(), "yyyy-MM-dd");
    }
    const str = val.toString().trim();
    const parsed = new Date(str);
    if (!isNaN(parsed)) {
      return Utilities.formatDate(parsed, Session.getScriptTimeZone(), "yyyy-MM-dd");
    }
    return str || "N/A";
  }

  // ===== Load Integrated Data =====
  const intRows = integrated.getDataRange().getValues();
  const intHeaders = intRows[0];
  const intData = intRows.slice(1);

  const idxInt = {
    siteId: intHeaders.indexOf("Con. ID (site)"),
    sheetId: intHeaders.indexOf("Con. ID (sheet)"),
    seg: intHeaders.indexOf("Customer Segment"),
    sku: intHeaders.indexOf("SKU"),
    qty: intHeaders.indexOf("Qty"),
  };

  // ===== Map SKU+Qty per contract (only SME) =====
  const siteMap = {};
  const sheetMap = {};
  for (const r of intData) {
    const seg = (r[idxInt.seg] || "").toString().toUpperCase();
    if (seg !== "SME") continue;

    const sku = (r[idxInt.sku] || "").toString().trim();
    const qty = (r[idxInt.qty] || "").toString().trim();
    const pair = sku ? `${sku}${qty ? " (" + qty + ")" : ""}` : "";

    const siteId = (r[idxInt.siteId] || "").toString().trim();
    const sheetId = (r[idxInt.sheetId] || "").toString().trim();

    if (siteId) {
      if (!siteMap[siteId]) siteMap[siteId] = [];
      if (pair) siteMap[siteId].push(pair);
    } else if (sheetId) {
      if (!sheetMap[sheetId]) sheetMap[sheetId] = [];
      if (pair) sheetMap[sheetId].push(pair);
    }
  }

  // ===== Load Contract View =====
  const cvRows = contractView.getDataRange().getValues();
  const cvHeaders = cvRows[0];
  const cvData = cvRows.slice(1);

  const idxCv = {
    siteId: cvHeaders.indexOf("Con. ID (site)"),
    sheetId: cvHeaders.indexOf("Con. ID (sheet)"),
    cust: cvHeaders.indexOf("Customer Name"),
    seg: cvHeaders.indexOf("Customer Segment"),
    start: cvHeaders.indexOf("Start Date"),
    end: cvHeaders.indexOf("End Date"),
    val: cvHeaders.indexOf("Total Contract Value"),
    flags: cvHeaders.indexOf("Flags"),
  };

  const results = [];

  for (const r of cvData) {
    const seg = (r[idxCv.seg] || "").toString().toUpperCase();
    if (seg !== "SME") continue;

    const siteId = (r[idxCv.siteId] || "").toString().trim();
    const sheetId = (r[idxCv.sheetId] || "").toString().trim();

    const skuList = siteMap[siteId] || sheetMap[sheetId] || [];
    const skuCombined = skuList.length ? skuList.join(" || ") : "N/A";

    let val = (r[idxCv.val] || "").toString().trim();
    if (val && val !== "N/A") {
      const num = parseFloat(val.replace(/[^0-9.]/g, ""));
      if (!isNaN(num)) val = "RM " + num.toLocaleString("en-MY", { minimumFractionDigits: 2 });
    }

    results.push([
      siteId,
      sheetId,
      (r[idxCv.cust] || "").toString().trim(),
      skuCombined,
      formatDate(r[idxCv.start]),
      formatDate(r[idxCv.end]),

      val || "RM 0.00",
      (r[idxCv.flags] || "").toString().trim()
    ]);
  }

  // ===== Sort by latest start date =====
  results.sort((a, b) => new Date(b[4] || 0) - new Date(a[4] || 0));

  const headers = [
    "Con. ID (site)",
    "Con. ID (sheet)",
    "Customer Name",
    "SKU (with Qty)",
    "Start Date",
    "End Date",
    "Total Contract Value",
    "Flags"
  ];

  outSheet.getRange(1, 1, 1, headers.length).setValues([headers]);

  const CHUNK = 3000;
  for (let i = 0; i < results.length; i += CHUNK) {
    const chunk = results.slice(i, i + CHUNK);
    outSheet.getRange(i + 2, 1, chunk.length, headers.length).setValues(chunk);
    Utilities.sleep(200);
  }

  Logger.log(`SME Contract Listing generated: ${results.length} rows.`);
}

function buildExpectedInvoiceSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const cv = ss.getSheetByName("Contract View");
  const intg = ss.getSheetByName("Integrated");
  const inv = ss.getSheetByName("Invoices");
  if (!cv || !inv) throw new Error("Missing required sheets (Contract View / Invoices).");

  const targetSpreadsheetId = "1dk-iP5a0iSbXzdNN0ZF_9uCHfSFVUMVVONX0w1xN_yw";
  let targetSS;
  try {
    targetSS = SpreadsheetApp.openById(targetSpreadsheetId);
  } catch (err) {
    Logger.log("⚠️ Warning: Unable to open target spreadsheet. Will only write local summary. " + err);
    targetSS = null;
  }

  const tz = Session.getScriptTimeZone();

  // === indexes and data ===
  const cvData = cv.getDataRange().getValues();
  const cvHdr = cvData[0];
  const cvRows = cvData.slice(1);
  const IDX_CV = {
    siteId: cvHdr.indexOf("Con. ID (site)"),
    sheetId: cvHdr.indexOf("Con. ID (sheet)"),
    start: cvHdr.indexOf("Start Date"),
    end: cvHdr.indexOf("End Date"),
    period: cvHdr.indexOf("Period"),
    cust: cvHdr.indexOf("Customer Name"),
    seg: cvHdr.indexOf("Customer Segment"),
  };

  const intgData = intg ? intg.getDataRange().getValues() : [];
  const intgHdr = intgData.length ? intgData[0] : [];
  const intgRows = intgData.length ? intgData.slice(1) : [];
  const IDX_INTG = {
    siteId: intgHdr.indexOf("Con. ID (site)"),
    sheetId: intgHdr.indexOf("Con. ID (sheet)"),
    leading: intgHdr.indexOf("Leading Months Paid"),
    tailing: intgHdr.indexOf("Tailing Months Paid"),
    unitPrice: intgHdr.indexOf("Unit Price"),
    period: intgHdr.indexOf("Period"),
  };

  const invData = inv.getDataRange().getValues();
  const invHdr = invData[0];
  const invRows = invData.slice(1);
  const IDX_INV = {
    month: invHdr.indexOf("period_month"),
    contractNum: invHdr.indexOf("contract_number"),
    legacy: invHdr.indexOf("legacy_order_id"),
    amount: invHdr.indexOf("amount"),
    invoiceNo: invHdr.indexOf("number"),
    payStatus: invHdr.indexOf("status"),
  };

  const today = new Date();
  const currentFullMonthDate = new Date(today.getFullYear(), today.getMonth() , 1);
  const currentFullMonthYM = ymFromDate(currentFullMonthDate);

  // === build intg map (monthly price etc) ===
  const intgMap = {};
  for (const r of intgRows) {
    const key = (r[IDX_INTG.siteId] || r[IDX_INTG.sheetId] || "").toString().trim().toUpperCase();
    if (!key) continue;
    const qty = toNum(r[intgHdr.indexOf("Qty")]) || 0;
    const unit = toNum(r[IDX_INTG.unitPrice]) || 0;
    const leading = parseInt(r[IDX_INTG.leading] || 0, 10) || 0;
    const tailing = parseInt(r[IDX_INTG.tailing] || 0, 10) || 0;
    const period = parseInt((r[IDX_INTG.period] || "").toString().replace(/[^\d]/g, ""), 10) || 0;
    const monthly = qty * unit;

    if (monthly <= 0) {
      Logger.log(`⚠️ Skipping contract ${key}: monthly=${monthly}, qty=${qty}, unit=${unit}`);
      continue;
    }

    intgMap[key] = { leading, tailing, period, monthly };
  }

  // === actual invoices map ===
  const actualLookup = {};
  const actualByMonth = {};
  const actualByContract = {};
  for (const r of invRows) {
    const m = (r[IDX_INV.month] || "").toString().trim();
    if (!m) continue;
    const keyC = (r[IDX_INV.contractNum] || r[IDX_INV.legacy] || "").toString().trim().toUpperCase();
    if (!keyC) continue;

    const amt = toNum(r[IDX_INV.amount]) || 0;
    actualLookup[`${keyC}|${m}`] = r;
    if (!actualByMonth[m]) actualByMonth[m] = { count: 0, total: 0 };
    actualByMonth[m].count++;
    actualByMonth[m].total += amt;

    actualByContract[keyC] = (actualByContract[keyC] || 0) + amt;
  }

  const expectedByMonth = {};
  const expectedByContract = {};
  const expectedDetail = [];
  const missingRows = [];
  const detailRowsWithStatus = [];
  const earliestYM = "2022-01";

  // === build expected ===
  for (const r of cvRows) {
    const siteId = (r[IDX_CV.siteId] || "").toString().trim().toUpperCase();
    const sheetId = (r[IDX_CV.sheetId] || "").toString().trim().toUpperCase();
    const contractId = siteId || sheetId;
    if (!contractId) continue;

    const ii = intgMap[contractId];
    if (!ii) {
      Logger.log(`⚠️ No intgMap for contract ${contractId}, skipping`);
      continue;
    }

    const cust = (r[IDX_CV.cust] || "").toString().trim();
    const seg = (r[IDX_CV.seg] || "").toString().trim();
    const start = toDate(r[IDX_CV.start]);
    if (!start) continue;

    let end = toDate(r[IDX_CV.end]);
    const { leading, tailing, period, monthly } = ii;
    if (!end && period) end = addMonths(start, period - 1);
    if (!end) continue;

    const allMonths = monthsBetween(start, end);
    if (!allMonths.length) continue;

    const upfrontCount = Math.max(0, leading + tailing);
    const totalMonths = allMonths.length;
    const startYM = allMonths[0];

    // upfront
    if (upfrontCount > 0 && startYM >= earliestYM && startYM <= currentFullMonthYM) {
      const amt = parseFloat((monthly * upfrontCount).toFixed(2));
      expectedDetail.push([contractId, cust, seg, startYM, amt, "UPFRONT"]);
      expectedByMonth[startYM] = expectedByMonth[startYM] || { count: 0, total: 0 };
      expectedByMonth[startYM].count++;
      expectedByMonth[startYM].total += amt;
      expectedByContract[contractId] = (expectedByContract[contractId] || 0) + amt;
    }

    // monthly
    for (let i = leading; i < totalMonths - tailing; i++) {
      const m = allMonths[i];
      if (m < earliestYM || m > currentFullMonthYM) continue;
      expectedDetail.push([contractId, cust, seg, m, monthly, "NORMAL"]);
      expectedByMonth[m] = expectedByMonth[m] || { count: 0, total: 0 };
      expectedByMonth[m].count++;
      expectedByMonth[m].total += monthly;
      expectedByContract[contractId] = (expectedByContract[contractId] || 0) + monthly;
    }
  }

  // === mark missing vs ok (MATCH ACCOUNT STATEMENT LOGIC) ===
  for (const ed of expectedDetail) {
    const [contractId, cust, seg, m, expectedAmt] = ed;

    const totalExpected = expectedByContract[contractId] || 0;         // total contract expected
    const totalActual = actualByContract[contractId] || 0;             // total paid so far

    // determine if contract is fully paid already
    const fullyPaid = totalActual >= totalExpected - 1;                // small tolerance

    const invRow = actualLookup[`${contractId}|${m}`];

    if (invRow) {
      // has invoice → evaluate real payment status
      const actualAmt = toNum(invRow[IDX_INV.amount]) || 0;
      const invNo = invRow[IDX_INV.invoiceNo] || "";
      const payStatus = (invRow[IDX_INV.payStatus] || "").toString().toLowerCase();

      let status = "OK";

      if (payStatus.includes("paid")) {
        status = "PAID";
      } else if (payStatus.includes("partial")) {
        status = "PARTIAL";
      } else if (payStatus.includes("unpaid") || payStatus.includes("pending")) {
        status = "UNPAID";
      }

      detailRowsWithStatus.push([
        contractId, cust, seg, m,
        expectedAmt,
        actualAmt,
        invNo,
        payStatus,
        status
      ]);

    } else {

      // NO INVOICE FOUND
      if (fullyPaid) {
        // If contract fully settled → DO NOT mark missing
        detailRowsWithStatus.push([
          contractId, cust, seg, m,
          expectedAmt,
          "",
          "Fully Paid",
          "Fully Paid",
          "OK"
        ]);
      } else {
        // Missing invoice and still outstanding
        missingRows.push([contractId, cust, seg, m, expectedAmt]);

        detailRowsWithStatus.push([
          contractId, cust, seg, m,
          expectedAmt,
          "",
          "Missing Invoice",
          "Missing Invoice",
          "MISSING"
        ]);
      }
    }
  }

  // === summary by month ===
  const allMonths = Array.from(new Set([...Object.keys(expectedByMonth), ...Object.keys(actualByMonth)]))
    .filter(m => m >= "2022-01" && m <= currentFullMonthYM)
    .sort((a, b) => b.localeCompare(a));

  const sHeaders = ["Month", "Expected Invoices", "Expected Total (RM)", "Actual Invoices", "Actual Total (RM)", "Missing Invoices"];
  const summaryRows = allMonths.map(m => {
    const e = expectedByMonth[m] || {};
    const a = actualByMonth[m] || {};
    return [m, e.count || 0, "RM " + (e.total || 0).toFixed(2), a.count || 0, "RM " + (a.total || 0).toFixed(2), Math.max((e.count || 0) - (a.count || 0), 0)];
  });

  // === write local summary ===
  let summary = ss.getSheetByName("Expected Invoice Summary") || ss.insertSheet("Expected Invoice Summary");
  summary.clearContents();
  summary.getRange(1, 1, 1, sHeaders.length).setValues([sHeaders]);
  if (summaryRows.length) summary.getRange(2, 1, summaryRows.length, sHeaders.length).setValues(summaryRows);

  // === footer ===
  if (summaryRows.length) {
    const totalContracts = cvRows.length;
    const totalExpected = summaryRows.reduce((sum, r) => sum + parseFloat((r[2] || "").toString().replace(/[^\d.-]/g, "")) || 0, 0);
    const totalActual = summaryRows.reduce((sum, r) => sum + parseFloat((r[4] || "").toString().replace(/[^\d.-]/g, "")) || 0, 0);
    const totalOutstanding = totalExpected - totalActual;
    const footerRow = summaryRows.length + 3;
    summary.getRange(footerRow, 1, 1, 1).setValue(
      `Total Contracts: ${totalContracts} | Total Expected: RM ${totalExpected.toFixed(2)} | Total Actual: RM ${totalActual.toFixed(2)} | Total Outstanding: RM ${totalOutstanding.toFixed(2)}`
    );
    const footerRange = summary.getRange(footerRow, 1);
    footerRange.setFontWeight("bold").setFontColor("#1565C0");
    summary.setRowHeights(footerRow, 1, 25);
  }

  Logger.log(`✅ Local summary written: ${summaryRows.length} months`);

    // === write to external target ===
    if (targetSS) {

      // ---- Helper: delete any existing sheets in targetSS that start with any of the prefixes ----
      function deleteTargetSheetsWithPrefixes(prefixes) {
        try {
          const allSheets = targetSS.getSheets();
          // iterate from end to start when deleting
          for (let i = allSheets.length - 1; i >= 0; i--) {
            const sh = allSheets[i];
            const name = sh.getName();
            for (const p of prefixes) {
              if (name.startsWith(p)) {
                try {
                  targetSS.deleteSheet(sh);
                  Logger.log(`🗑️ Deleted existing target sheet: ${name}`);
                } catch (e) {
                  Logger.log(`⚠️ Failed to delete ${name}: ${e}`);
                }
                break; // already deleted, move to next sheet
              }
            }
          }
        } catch (e) {
          Logger.log(`⚠️ deleteTargetSheetsWithPrefixes failed: ${e}`);
        }
      }

      // Remove previous output sheets that might have been created earlier
      deleteTargetSheetsWithPrefixes(["Missing Invoices", "Expected Invoice Detail"]);

      function safeWriteMissingToBoth(baseName, headers, rows) {
        if (!rows || !rows.length) return;
        const byYear = {};
        for (const r of rows) {
          const ym = (r[3] || "").toString();
          const y = ym.split("-")[0] || "unknown";
          if (!byYear[y]) byYear[y] = [];
          byYear[y].push(r);
        }
        for (const [y, yearRows] of Object.entries(byYear)) {
          const chunkSize = 50000;
          const chunks = Math.ceil(yearRows.length / chunkSize);
          for (let c = 0; c < chunks; c++) {
            const chunkRows = yearRows.slice(c * chunkSize, (c + 1) * chunkSize);
            const sheetName = chunks > 1 ? `${baseName} ${y} (${c + 1})` : `${baseName} ${y}`;
            // create fresh sheet (guaranteed removed above if existed)
            let sh = targetSS.getSheetByName(sheetName);
            if (sh) {
              // if somehow exists (race), clear and reuse
              sh.clearContents();
            } else {
              sh = targetSS.insertSheet(sheetName);
            }
            sh.getRange(1, 1, 1, headers.length).setValues([headers]);
            if (chunkRows.length) {
              sh.getRange(2, 1, chunkRows.length, headers.length).setValues(chunkRows);
            }
            sh.setFrozenRows(1);
            Logger.log(`📄 ${sheetName}: wrote ${chunkRows.length} rows`);
          }
        }
      }

      safeWriteMissingToBoth(
        "Missing Invoices",
        ["Contract ID", "Customer Name", "Customer Type", "Missing Month", "Expected Amount (RM)"],
        missingRows.map(r => [r[0], r[1], r[2], r[3], "RM " + parseFloat(r[4] || 0).toFixed(2)])
      );

      /*safeWriteMissingToBoth(
        "Expected Invoice Detail",
        [
          "Contract ID",
          "Customer Name",
          "Customer Type",
          "Month",
          "Expected Amount (RM)",
          "Actual Amount (RM)",
          "Invoice No.",
          "Payment Status",
          "Status",
        ],
        detailRowsWithStatus.map(r => [
          r[0],
          r[1],
          r[2],
          r[3],
          "RM " + parseFloat(r[4] || 0).toFixed(2),
          r[5] ? "RM " + parseFloat(r[5]).toFixed(2) : "",
          r[6] || "Missing Invoice",
          r[7] || "Missing Invoice",
          r[8],
        ])
      );*/

      Logger.log(`✅ External sheets written successfully`);
      addExternalReportLink();
    }
}



function buildOutstandingByContract() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const cv = ss.getSheetByName("Contract View");
  const inv = ss.getSheetByName("Invoices");
  const intg = ss.getSheetByName("Integrated");
  if (!cv || !inv || !intg)
    throw new Error("Missing required sheets: Contract View, Invoices, or Integrated.");

  // === Load Data ===
  const cvData = cv.getDataRange().getValues();
  const cvHdr = cvData[0];
  const cvRows = cvData.slice(1);
  const IDX_CV = {
    siteId: cvHdr.indexOf("Con. ID (site)"),
    sheetId: cvHdr.indexOf("Con. ID (sheet)"),
    cust: cvHdr.indexOf("Customer Name"),
    seg: cvHdr.indexOf("Customer Segment"),
    start: cvHdr.indexOf("Start Date"),
    end: cvHdr.indexOf("End Date"),
    total: cvHdr.indexOf("Total Contract Value"),
  };

  const intgData = intg.getDataRange().getValues();
  const intgHdr = intgData[0];
  const intgRows = intgData.slice(1);
  const IDX_INTG = {
    siteId: intgHdr.indexOf("Con. ID (site)"),
    qty: intgHdr.indexOf("Qty"),
    unit: intgHdr.indexOf("Unit Price"),
    leading: intgHdr.indexOf("Leading Months Paid"),
    tailing: intgHdr.indexOf("Tailing Months Paid"),
  };

  const invData = inv.getDataRange().getValues();
  const invHdr = invData[0];
  const invRows = invData.slice(1);
  const IDX_INV = {
    number: invHdr.indexOf("number"),
    period: invHdr.indexOf("period_month"),
    contractNum: invHdr.indexOf("contract_number"),
    legacyNum: invHdr.indexOf("legacy_order_id"),
    status: invHdr.indexOf("status"),
    amount: invHdr.indexOf("amount"),
  };

  const today = new Date();

  // === Build Integrated Monthly Rates ===
  const intgMap = {};
  for (const r of intgRows) {
    const cid = (r[IDX_INTG.siteId] || "").toString().trim().toUpperCase();
    if (!cid) continue;
    const qty = toNum(r[IDX_INTG.qty]);
    const unit = toNum(r[IDX_INTG.unit]);
    const leading = parseInt(r[IDX_INTG.leading]) || 0;
    const tailing = parseInt(r[IDX_INTG.tailing]) || 0;

    if (!intgMap[cid]) intgMap[cid] = { monthly: 0, leading: 0, tailing: 0 };
    intgMap[cid].monthly += qty * unit;
    intgMap[cid].leading = Math.max(intgMap[cid].leading, leading);
    intgMap[cid].tailing = Math.max(intgMap[cid].tailing, tailing);
  }

  // === Build Invoice Lookup ===
  const actualLookup = {};
  for (const r of invRows) {
    const month = (r[IDX_INV.period] || "").toString().trim();
    if (!month) continue;
    const id1 = (r[IDX_INV.contractNum] || "").toString().trim().toUpperCase();
    const id2 = (r[IDX_INV.legacyNum] || "").toString().trim().toUpperCase();
    const entry = {
      number: r[IDX_INV.number],
      status: (r[IDX_INV.status] || "").toString().toLowerCase(),
      amount: toNum(r[IDX_INV.amount]),
    };
    if (id1) actualLookup[`${id1}|${month}`] = entry;
    if (id2) actualLookup[`${id2}|${month}`] = entry;
  }

  // === Process Each Contract ===
  const contracts = [];
  for (const r of cvRows) {
    const site = (r[IDX_CV.siteId] || "").toString().trim().toUpperCase();
    const sheet = (r[IDX_CV.sheetId] || "").toString().trim().toUpperCase();
    const cust = (r[IDX_CV.cust] || "").toString().trim();
    const seg = (r[IDX_CV.seg] || "").toString().trim().toUpperCase();
    const start = toDate(r[IDX_CV.start]);
    let end = toDate(r[IDX_CV.end]);
    const totalVal = toNum(r[IDX_CV.total]) || 0;

    const cid = site || sheet;
    if (!cid || !start) continue;
    if (!end || isNaN(end)) {
      // Fallback: 12 months from start
      end = addMonths(start, 11);
    }

    const ii = intgMap[cid] || {};
    const allMonths = monthsBetween(start, end).filter(m => new Date(m + "-01") <= today);
    const totalMonths = allMonths.length;
    if (totalMonths === 0) continue;

    let monthly = ii.monthly > 0 ? ii.monthly : totalVal / totalMonths;
    monthly = parseFloat(monthly.toFixed(2));

    let paidTotal = 0, unpaidTotal = 0, missingTotal = 0, partialTotal = 0;
    let unpaidCount = 0, missingCount = 0, partialCount = 0;
    const unpaidList = [];

    for (const ym of allMonths) {
      const inv = actualLookup[`${cid}|${ym}`];
      if (!inv) {
        missingCount++;
        missingTotal += monthly;
        unpaidList.push(`${ym} (Missing)`);
        continue;
      }

      const st = inv.status;
      const amt = toNum(inv.amount);

      if (st.includes("paid")) {
        paidTotal += amt;
      } else if (st.includes("partial")) {
        partialCount++;
        partialTotal += amt;
        unpaidList.push(`${ym} (${inv.number || "Partial"})`);
      } else if (st.includes("unpaid") || st.includes("pending")) {
        unpaidCount++;
        unpaidTotal += amt || monthly;
        unpaidList.push(`${ym} (${inv.number || "Unpaid"})`);
      }
    }

    // Expected cumulative value up to now (based on months elapsed)
    const expectedSoFar = Math.min(allMonths.length, totalMonths) * monthly;

    // Outstanding is how much we should’ve received so far minus what’s paid
    let outstanding = Math.max(0, expectedSoFar - paidTotal);

    // Never exceed total contract value
    if (outstanding + paidTotal > totalVal)
      outstanding = totalVal - paidTotal;


    outstanding = parseFloat(outstanding.toFixed(2));


    const summary = `unpaid(${unpaidCount}); missing(${missingCount}); partial(${partialCount})`;

    contracts.push({
      seg, cust, cid,
      totalValue: totalVal,
      paidTotal,
      outstanding,
      unpaidMonths: unpaidList.join(" || "),
      summary
    });
  }

  // === Sort & Output ===
  const segOrder = { SME: 1, IND: 2 };
  contracts.sort((a, b) => {
    const sa = segOrder[a.seg] || 99;
    const sb = segOrder[b.seg] || 99;
    if (sa !== sb) return sa - sb;
    return a.cust.localeCompare(b.cust);
  });

  const outName = "Outstanding Overview";
  let out = ss.getSheetByName(outName);
  if (!out) out = ss.insertSheet(outName);
  out.clearContents();

  const headers = [
    "Customer Segment",
    "Customer Name",
    "Contract ID",
    "Total Contract Value (RM)",
    "Total Paid (RM)",
    "Total Outstanding (RM)",
    "Unpaid Months (with Invoice Nos.)",
    "Summary (unpaid/missing/partial)"
  ];

  const rows = contracts.map(c => [
    c.seg,
    c.cust,
    c.cid,
    "RM " + c.totalValue.toLocaleString("en-MY", { minimumFractionDigits: 2 }),
    "RM " + c.paidTotal.toLocaleString("en-MY", { minimumFractionDigits: 2 }),
    "RM " + c.outstanding.toLocaleString("en-MY", { minimumFractionDigits: 2 }),
    c.unpaidMonths,
    c.summary
  ]);

  out.getRange(1, 1, 1, headers.length).setValues([headers]);
  if (rows.length)
    out.getRange(2, 1, rows.length, headers.length).setValues(rows);

  Logger.log(`✅ Outstanding Overview written for ${rows.length} contracts.`);
}


function addExternalReportLink() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const summary = ss.getSheetByName("Expected Invoice Summary");
  if (!summary) {
    Logger.log("⚠️ No 'Expected Invoice Summary' sheet found.");
    return;
  }

  const linkUrl = "https://docs.google.com/spreadsheets/d/1dk-iP5a0iSbXzdNN0ZF_9uCHfSFVUMVVONX0w1xN_yw";
  const linkCell = summary.getRange("H1");

  // Add formula
  linkCell.setFormula(`=HYPERLINK("${linkUrl}", "📂 View Full Invoice Report")`);

  // Apply formatting
  const text = linkCell.getDisplayValue();
  const richText = SpreadsheetApp.newRichTextValue()
    .setText(text)
    .setLinkUrl(linkUrl)
    .setTextStyle(
      SpreadsheetApp.newTextStyle()
        .setForegroundColor("#1155CC")
        .setBold(true)
        .setUnderline(true)
        .setFontSize(11)
        .build()
    )
    .build();

  linkCell.setRichTextValue(richText);
  Logger.log("✅ External report link added successfully.");
}


function buildReportItems() {
  buildSMEContractListing();
  buildExpectedInvoiceSummary();
  buildOutstandingByContract();
}
