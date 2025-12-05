/**
 * Consolidated Account Statement builder (Option A: Direct Consolidation)
 * - Preserves original business logic and ordering
 * - Replaces repeated invoice/receipt/paid-date/ledger blocks with helpers
 * - Columns: Contract ID, Invoice No., Customer Name, Month, Debit, Payment Status, Paid At, Credit, Total Paid, Balance, Receipt No.
 *
 * Performance notes:
 * - Uses pre-built lookup objects for invoices/receipts
 * - Caches commonly used utilities
 * - Keeps logic identical to your base
 */
function buildAccountStatement() {
  const START_TS = Date.now();
  const LOG_PREFIX = "buildAccountStatement:";
  const TARGET_SHEET_ID = "1dk-iP5a0iSbXzdNN0ZF_9uCHfSFVUMVVONX0w1xN_yw";
  const TARGET_BASE_NAME = "Account Statement";
  const MAX_ROWS_PER_SHEET = 50000;
  const BATCH_WRITE_CHUNK = 5000;
  const MAX_MONTHS = 36;
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const TZ = Session.getScriptTimeZone ? Session.getScriptTimeZone() : "GMT+8";

  Logger.log(`${LOG_PREFIX} starting...`);

  // --- 1) load sheets ---
  const cv = ss.getSheetByName("Contract View");
  const intg = ss.getSheetByName("Integrated");
  const inv = ss.getSheetByName("Invoices");
  const receipts = ss.getSheetByName("Receipts");
  if (!cv || !intg || !inv || !receipts) throw new Error("Missing required sheets.");

  const cvData = cv.getDataRange().getValues();
  const intgData = intg.getDataRange().getValues();
  const invData = inv.getDataRange().getValues();
  const receiptsData = receipts.getDataRange().getValues();

  if (cvData.length <= 1) Logger.log(`${LOG_PREFIX} ⚠ Contract View has no data rows.`);
  if (intgData.length <= 1) Logger.log(`${LOG_PREFIX} ⚠ Integrated has no data rows.`);
  if (invData.length <= 1) Logger.log(`${LOG_PREFIX} ⚠ Invoices has no data rows.`);

  const cvHdr = cvData[0], intgHdr = intgData[0], invHdr = invData[0];
  const cvRows = cvData.slice(1), intgRows = intgData.slice(1), invRows = invData.slice(1);
  const receiptsHdr = receiptsData[0];
  const receiptsRows = receiptsData.slice(1);

  // --- helper index finders ---
  const idx = hdr => name => hdr.indexOf(name);
  const cvIdx = idx(cvHdr), intgIdx = idx(intgHdr), invIdx = idx(invHdr);
  const recIdx = name => receiptsHdr.indexOf(name);

  const IDX_CV = {
    siteId: cvIdx("Con. ID (site)"),
    sheetId: cvIdx("Con. ID (sheet)"),
    cust: cvIdx("Customer Name"),
    start: cvIdx("Start Date"),
    end: cvIdx("End Date"),
    period: cvIdx("Period")
  };

  const IDX_INTG = {
    siteId: intgIdx("Con. ID (site)"),
    sheetId: intgIdx("Con. ID (sheet)"),
    qty: intgIdx("Qty"),
    unit: intgIdx("Unit Price"),
    leading: intgIdx("Leading Months Paid"),
    tailing: intgIdx("Tailing Months Paid"),
    period: intgIdx("Period"),
    contractValue: intgIdx("Contract Value")
  };

  const IDX_INV = {
    month: invIdx("period_month"),
    contractId: invIdx("contract_number"),
    legacy: invIdx("legacy_order_id"),
    amount: invIdx("amount"),
    invoiceNo: invIdx("number"),
    status: invIdx("status"),
    paidAt: invIdx("paid_at")
  };

  const IDX_REC = {
    number: recIdx("number"),
    contractId: recIdx("contract_number"),
    paymentDate: recIdx("payment_date"),
    payerName: recIdx("payer_name"),
    invoiceNumber: recIdx("invoice_number"),
    additionalInv: recIdx("additional_invoice_numbers"),
    amount: recIdx("amount"),
    paymentMethod: recIdx("payment_method"),
    paymentReference: recIdx("payment_reference"),
    status: recIdx("status"),
    latestAction: recIdx("latest_action_status")
  };

  // --- small utility helpers (kept logically identical to original) ---
  const toNum = v => {
    if (v === null || v === undefined || v === "") return 0;
    if (typeof v === "number") return v;
    const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ""));
    return isNaN(n) ? 0 : n;
  };
  const toDate = v => {
    if (v instanceof Date) return v;
    if (!v && v !== 0) return null;
    const s = String(v).trim();
    // dd/mm/yyyy
    const dmy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (dmy) return new Date(+dmy[3], +dmy[2] - 1, +dmy[1]);
    // yyyy-mm-dd
    const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (ymd) return new Date(+ymd[1], +ymd[2] - 1, +ymd[3]);
    const d = new Date(s);
    return isNaN(d) ? null : d;
  };
  const isDateObj = d => d instanceof Date && !isNaN(d.getTime());
  const formatPaidAt = v => {
    const d = toDate(v);
    return isDateObj(d) ? Utilities.formatDate(d, TZ, "dd/MM/yyyy") : "-";
  };
  const ym = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  const addMonths = (d, n) => new Date(d.getFullYear(), d.getMonth() + n, 1);
  const monthsBetween = (s, e) => {
    const out = [];
    let cur = new Date(s.getFullYear(), s.getMonth(), 1);
    const end = new Date(e.getFullYear(), e.getMonth(), 1);
    while (cur <= end && out.length < MAX_MONTHS) {
      out.push(ym(cur));
      cur = addMonths(cur, 1);
    }
    return out;
  };
  const safeParsePeriod = v => {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    if (!/^\d{1,2}$/.test(s)) return null;
    const p = parseInt(s, 10);
    if (p <= 0 || p > 99) return null;
    return p;
  };
  const norm = v => v ? String(v).trim().toUpperCase() : "";

  // --- PROFILE ---
  const timers = {};
  const tstart = l => timers[l] = Date.now();
  const tend = l => { if (timers[l]) { Logger.log(`${LOG_PREFIX} timer ${l}: ${Date.now()-timers[l]} ms`); delete timers[l]; } };

  // --- 1) BUILD INTG MAP ---
  tstart("intg_map");
  const intgMap = Object.create(null);
  for (let i = 0, L = intgRows.length; i < L; i++) {
    const r = intgRows[i];
    const key = ((r[IDX_INTG.siteId] || r[IDX_INTG.sheetId] || "") + "").trim().toUpperCase();
    if (!key) continue;
    const qty = toNum(r[IDX_INTG.qty]), unit = toNum(r[IDX_INTG.unit]);
    const monthly = qty * unit, contractValue = toNum(r[IDX_INTG.contractValue]);
    const leading = parseInt(r[IDX_INTG.leading] || 0, 10) || 0;
    const tailing = parseInt(r[IDX_INTG.tailing] || 0, 10) || 0;
    const periodParsed = safeParsePeriod(r[IDX_INTG.period]);
    if (!intgMap[key]) intgMap[key] = { monthly, contractValue, leading, tailing, period: periodParsed || 0 };
    else {
      intgMap[key].monthly += monthly;
      intgMap[key].contractValue += contractValue;
      intgMap[key].leading += leading;
      intgMap[key].tailing += tailing;
      if (!intgMap[key].period && periodParsed) intgMap[key].period = periodParsed;
    }
  }
  tend("intg_map");

  // --- 2) BUILD INVOICE LOOKUPS (by contract|month) ---
  tstart("inv_map");
  const actualByKey = Object.create(null); // contract|YYYY-MM -> array of invoice rows
  const invoiceLookup = Object.create(null); // INVNO -> { amount, paidFromReceipts, receiptNos:[], receiptDates:[], invRows:[] }
  for (let i = 0, L = invRows.length; i < L; i++) {
    const r = invRows[i];
    const contractId = ((r[IDX_INV.contractId] || r[IDX_INV.legacy] || "") + "").trim().toUpperCase();
    const m = (r[IDX_INV.month] || "") + "";
    if (contractId && m) {
      const key = `${contractId}|${m}`;
      if (!actualByKey[key]) actualByKey[key] = [];
      actualByKey[key].push(r);
    }

    const invNoRaw = r[IDX_INV.invoiceNo];
    const invNo = (invNoRaw || "") + "";
    const invKey = invNo.trim().toUpperCase();
    if (invKey) {
      if (!invoiceLookup[invKey]) {
        invoiceLookup[invKey] = {
          amount: toNum(r[IDX_INV.amount]),
          paidFromReceipts: 0,
          receiptNos: [],
          receiptDates: [],
          invRows: [r]
        };
      } else {
        invoiceLookup[invKey].invRows.push(r);
      }
    }
  }
  tend("inv_map");

  // --- 2b) BUILD RECEIPTS LOOKUP (for reference) AND ALLOCATE RECEIPTS (FCFS) ---
  tstart("receipt_map_and_alloc");
  const receiptByInvoice = Object.create(null);
  function pushRecRef(invNo, row) {
    if (!invNo && invNo !== 0) return;
    const key = String(invNo).trim().toUpperCase();
    if (!key) return;
    if (!receiptByInvoice[key]) receiptByInvoice[key] = [];
    receiptByInvoice[key].push(row);
  }

  function parseAdditionalInvs(raw) {
    if (!raw && raw !== 0) return [];
    const s = String(raw).trim();
    if (!s) return [];
    return s.split(',').map(x => x.trim()).filter(Boolean);
  }

  for (let i = 0, L = receiptsRows.length; i < L; i++) {
    const r = receiptsRows[i];
    const inv1 = r[IDX_REC.invoiceNumber];
    pushRecRef(inv1, r);
    const extra = r[IDX_REC.additionalInv];
    if (extra) parseAdditionalInvs(extra).forEach(v => pushRecRef(v, r));
    const invRef = r[IDX_REC.paymentReference];
    pushRecRef(invRef, r);
  }

  for (let i = 0, L = receiptsRows.length; i < L; i++) {
    const r = receiptsRows[i];
    const receiptNo = r[IDX_REC.number] || "";
    const invPrimary = r[IDX_REC.invoiceNumber] || "";
    const extras = parseAdditionalInvs(r[IDX_REC.additionalInv]);
    const ordered = [];
    if (invPrimary) ordered.push(norm(invPrimary));
    for (let j = 0; j < extras.length; j++) ordered.push(norm(extras[j]));
    if (ordered.length === 0 && r[IDX_REC.paymentReference]) {
      ordered.push(norm(r[IDX_REC.paymentReference]));
    }

    let remaining = toNum(r[IDX_REC.amount]);
    if (remaining <= 0) continue;

    for (let k = 0; k < ordered.length && remaining > 0; k++) {
      const invRaw = ordered[k];
      const invKey = norm(invRaw);
      if (!invKey) continue;
      const invObj = invoiceLookup[invKey];
      if (!invObj) continue;
      const invoiceAmount = Math.max(invObj.amount || 0, 0);
      const alreadyAllocated = invObj.paidFromReceipts || 0;
      const invoiceRemaining = Math.max(invoiceAmount - alreadyAllocated, 0);
      if (invoiceRemaining <= 0) continue;
      const allocate = Math.min(invoiceRemaining, remaining);
      if (allocate <= 0) continue;
      invObj.paidFromReceipts = (invObj.paidFromReceipts || 0) + allocate;
      invObj.receiptNos = invObj.receiptNos || [];
      invObj.receiptDates = invObj.receiptDates || [];
      invObj.receiptNos.push(receiptNo);
      const payDate = r[IDX_REC.paymentDate];
      if (payDate) invObj.receiptDates.push(payDate);
      remaining -= allocate;
    }
  }
  tend("receipt_map_and_alloc");

  // -------------------------
  // === Helper functions ===
  // -------------------------
  function resolvePaidAtRaw(paidAtRaw, recDates) {
    let paidAtFinal = paidAtRaw || "";
    if (!paidAtFinal || String(paidAtFinal).trim() === "") {
      if (recDates && recDates.length) {
        // earliest valid receipt date
        const validDates = recDates.map(d => toDate(d)).filter(isDateObj).sort((a, b) => a - b);
        if (validDates.length) paidAtFinal = validDates[0];
      }
    }
    // convert to Date if string
    if (typeof paidAtFinal === "string") {
      const tmpDate = toDate(paidAtFinal);
      paidAtFinal = isDateObj(tmpDate) ? tmpDate : null;
    }
    return formatPaidAt(paidAtFinal);
  }

  function buildReceiptNosStr(invKey, invLookup, receiptByInvoiceLocal) {
    if (invLookup && invLookup.receiptNos && invLookup.receiptNos.length) return invLookup.receiptNos.join(', ');
    if (receiptByInvoiceLocal[invKey] && receiptByInvoiceLocal[invKey].length) return receiptByInvoiceLocal[invKey].map(rr => rr[IDX_REC.number]).filter(Boolean).join(', ');
    return "-";
  }

  function updateLedgerAndCap(ledger, amtInvoicedDelta, amtPaidDelta, contractValue) {
    ledger.invoiced += amtInvoicedDelta;
    ledger.paid += amtPaidDelta;
    ledger.outstanding = ledger.invoiced - ledger.paid;
    if (ledger.invoiced > contractValue) ledger.invoiced = contractValue;
    if (ledger.outstanding > contractValue) ledger.outstanding = contractValue;
  }

  function tidyOutputRow(rawRow) {
    const out = [];
    out.push(rawRow[0] ? String(rawRow[0]) : "-"); // Contract ID
    out.push(rawRow[1] ? String(rawRow[1]) : "-"); // Invoice No.
    out.push(rawRow[2] ? String(rawRow[2]) : "-"); // Customer Name
    out.push(rawRow[3] ? String(rawRow[3]) : "-"); // Month
    const amt = rawRow[4]; // Debit
    out.push((amt === null || amt === undefined || amt === "") ? "-" : (typeof amt === "number" ? amt : (isNaN(Number(amt)) ? "-" : Number(amt))));
    out.push(rawRow[5] ? String(rawRow[5]) : "-"); // Payment Status
    out.push(rawRow[6] ? String(rawRow[6]) : "-"); // Paid At
    const credit = rawRow[7]; // Credit
    out.push((credit === null || credit === undefined || credit === "") ? "-" : (typeof credit === "number" ? credit : (isNaN(Number(credit)) ? "-" : Number(credit))));
    const tp = rawRow[8]; // Total Paid
    out.push((tp === null || tp === undefined || tp === "") ? "-" : (typeof tp === "number" ? tp : (isNaN(Number(tp)) ? "-" : Number(tp))));
    const outn = rawRow[9]; // Balance
    out.push((outn === null || outn === undefined || outn === "") ? "-" : (typeof outn === "number" ? outn : (isNaN(Number(outn)) ? "-" : Number(outn))));
    out.push(rawRow[10] ? String(rawRow[10]) : "-"); // Receipt No.
    return out;
  }

  function processInvoiceRow(contractId, cust, month, invr, invoiceLookupLocal, receiptByInvoiceLocal, ledger, contractValue) {
    const invNoRaw = invr[IDX_INV.invoiceNo];
    const invNo = invNoRaw ? String(invNoRaw) : null;
    const amt = toNum(invr[IDX_INV.amount]);
    const st = ((invr[IDX_INV.status] || "") + "").trim().toUpperCase();
    const paidAtRaw = invr[IDX_INV.paidAt] || "";

    const invKey = norm(invNo);
    const invLookup = invoiceLookupLocal[invKey] || null;
    const paidFromReceipts = invLookup ? (invLookup.paidFromReceipts || 0) : 0;
    const recDates = invLookup && invLookup.receiptDates ? invLookup.receiptDates : [];
    const recNosAllocated = invLookup && invLookup.receiptNos && invLookup.receiptNos.length ? invLookup.receiptNos : [];

    const paidAtFormatted = resolvePaidAtRaw(paidAtRaw, recDates);

    // Determine credit (paid amount)
    let paidAmt = 0;
    if (st.includes("PARTIAL") || st.includes("PARTIALLY")) {
      paidAmt = paidFromReceipts;
    } else {
      const isPaid = st.includes("PAID") && !st.includes("PARTIAL") && !st.includes("UNPAID") && !st.includes("PENDING");
      paidAmt = isPaid ? amt : 0;
    }
    paidAmt = toNum(paidAmt);

    const receiptNosStr = recNosAllocated.length ? recNosAllocated.join(', ') : buildReceiptNosStr(invKey, invLookup, receiptByInvoiceLocal);

    // Update ledger: invoiced (Debit) and paid (Credit)
    updateLedgerAndCap(ledger, amt, paidAmt, contractValue);

    const debit = amt;
    const credit = paidAmt;

    const outRow = tidyOutputRow([
      contractId,
      invNo || "Missing Invoice",
      cust || "-",
      month,
      debit,
      st || "-",
      paidAtFormatted,
      credit,
      ledger.paid,
      ledger.outstanding,
      receiptNosStr
    ]);

    return { outRow, amt, paidAmt };
  }

  function processMissingInvoice(contractId, cust, month, expectedAmt, ledger, contractValue) {
    if (expectedAmt <= 0) return null;
    updateLedgerAndCap(ledger, expectedAmt, 0, contractValue);
    const debit = expectedAmt;
    const credit = 0;
    const outRow = tidyOutputRow([
      contractId,
      "Missing Invoice",
      cust || "-",
      month,
      debit,
      "Missing",
      "-",
      credit,
      ledger.paid,
      ledger.outstanding,
      "-"
    ]);
    return outRow;
  }

  // -------------------------
  // === Streaming & Sheets ==
  // -------------------------
  tstart("build_rows");
  let targetSS;
  try { targetSS = SpreadsheetApp.openById(TARGET_SHEET_ID); }
  catch (err) { throw new Error(`${LOG_PREFIX} Cannot open target spreadsheet: ${err}`); }

  const existingTargetSheets = targetSS.getSheets().filter(s => s.getName().startsWith(TARGET_BASE_NAME));
  for (let i = existingTargetSheets.length - 1; i >= 1; i--) targetSS.deleteSheet(existingTargetSheets[i]);

  const sheetCache = Object.create(null);
  function getOrCreateSheetByIndex(index) {
    if (sheetCache[index]) return sheetCache[index];
    const name = index === 1 ? TARGET_BASE_NAME : `${TARGET_BASE_NAME} (${index})`;
    let s = targetSS.getSheetByName(name);
    if (!s) s = targetSS.insertSheet(name);
    sheetCache[index] = s;
    return s;
  }

  let outSheetIndex = 1;
  let outSheet = getOrCreateSheetByIndex(outSheetIndex);
  outSheet.clearContents();

  const headers = ["Contract ID","Invoice No.","Customer Name","Month","Debit","Payment Status","Paid At","Credit","Total Paid","Balance","Receipt No."];
  outSheet.getRange(1,1,1,headers.length).setValues([headers]);
  let outRowPos = 2;

  function openNextSheet() {
    outSheetIndex++;
    outSheet = getOrCreateSheetByIndex(outSheetIndex);
    outSheet.clearContents();
    outSheet.getRange(1,1,1,headers.length).setValues([headers]);
    outRowPos = 2;
    Logger.log(`${LOG_PREFIX} created new output sheet: ${outSheet.getName()}`);
  }

  let streamBuffer = [];
  const flushStream = () => {
    if (!streamBuffer.length) return;
    if (outRowPos + streamBuffer.length - 1 > MAX_ROWS_PER_SHEET) openNextSheet();
    outSheet.getRange(outRowPos,1,streamBuffer.length,headers.length).setValues(streamBuffer);
    outRowPos += streamBuffer.length;
    streamBuffer = [];
  };
  const streamPush = row => { streamBuffer.push(row); if (streamBuffer.length >= BATCH_WRITE_CHUNK) flushStream(); };

  // --- SORT CV ROWS: Customer Name → Contract ID → Start Date ---
  cvRows.sort((a, b) => {
    const custA = (a[IDX_CV.cust] || "").toString().toUpperCase();
    const custB = (b[IDX_CV.cust] || "").toString().toUpperCase();
    if (custA < custB) return -1;
    if (custA > custB) return 1;
    const idA = ((a[IDX_CV.siteId] || a[IDX_CV.sheetId] || "") + "").toUpperCase();
    const idB = ((b[IDX_CV.siteId] || b[IDX_CV.sheetId] || "") + "").toUpperCase();
    const blankA = idA.trim() === "";
    const blankB = idB.trim() === "";
    if (blankA && !blankB) return 1;
    if (!blankA && blankB) return -1;
    if (idA < idB) return -1;
    if (idA > idB) return 1;
    const dA = new Date(a[IDX_CV.start]);
    const dB = new Date(b[IDX_CV.start]);
    if (isNaN(dA) && !isNaN(dB)) return 1;
    if (!isNaN(dA) && isNaN(dB)) return -1;
    if (dA < dB) return -1;
    if (dA > dB) return 1;
    return 0;
  });

  Logger.log(`${LOG_PREFIX} processing ${cvRows.length} contract rows...`);
  const startBuild = Date.now();

  for (let i = 0, CVL = cvRows.length; i < CVL; i++) {
    const r = cvRows[i];
    const site = norm(r[IDX_CV.siteId]);
    const sheetId = norm(r[IDX_CV.sheetId]);
    const contractId = site || sheetId;
    if (!contractId) continue;

    const map = intgMap[contractId];
    if (!map) continue; // no integrated info -> skip

    const cust = (r[IDX_CV.cust] || "") + "";
    const start = toDate(r[IDX_CV.start]);
    let end = toDate(r[IDX_CV.end]);
    if (!isDateObj(start)) {
      Logger.log(`${LOG_PREFIX} ⚠ Skipping ${contractId} due to invalid start`);
      continue;
    }

    // determine months sequence (respect MAX_MONTHS)
    let months = [];
    if (!isDateObj(end)) {
      if (map.period && map.period > 0) {
        end = addMonths(start, map.period - 1);
        if (isDateObj(end)) months = monthsBetween(start, end);
      } else {
        months = [];
      }
    } else {
      if (end >= start) months = monthsBetween(start, end);
      else months = [];
    }
    if (months.length > MAX_MONTHS) months = months.slice(0, MAX_MONTHS);

    // ledger state
    const ledger = { invoiced: 0, paid: 0, outstanding: 0 };
    const contractValue = map.contractValue || 0;

    // upfront handling (leading + tailing)
    const upfrontCount = (map.leading || 0) + (map.tailing || 0);

    // 3a) Upfront combined (if months known)
    if (upfrontCount > 0 && months.length > 0) {
      const firstMonth = months[0];
      const key = `${contractId}|${firstMonth}`;
      const invRowsForKey = actualByKey[key] || [];
      if (invRowsForKey.length > 0) {
        for (let j = 0; j < invRowsForKey.length; j++) {
          const invr = invRowsForKey[j];
          const processed = processInvoiceRow(contractId, cust, firstMonth, invr, invoiceLookup, receiptByInvoice, ledger, contractValue);
          streamPush(processed.outRow);
        }
      } else {
        const remainingInvoiceable = Math.max(0, contractValue - ledger.invoiced);
        const expectedUpfront = Math.min((map.monthly || 0) * upfrontCount, remainingInvoiceable);
        if (expectedUpfront > 0) {
          const outRow = processMissingInvoice(contractId, cust, firstMonth, expectedUpfront, ledger, contractValue);
          if (outRow) streamPush(outRow);
        }
      }
    } else if (upfrontCount > 0 && months.length === 0) {
      const startYM = ym(start);
      const key = `${contractId}|${startYM}`;
      const invRowsForKey = actualByKey[key] || [];
      if (invRowsForKey.length > 0) {
        for (let j = 0; j < invRowsForKey.length; j++) {
          const invr = invRowsForKey[j];
          const processed = processInvoiceRow(contractId, cust, startYM, invr, invoiceLookup, receiptByInvoice, ledger, contractValue);
          streamPush(processed.outRow);
        }
      } else {
        // nothing to output when months unknown and no invoice
      }
    }

    // 3b) normal monthly iteration (after upfront months)
    for (let mIndex = upfrontCount; mIndex < months.length; mIndex++) {
      const m = months[mIndex];
      const key = `${contractId}|${m}`;
      const invRowsForKey = actualByKey[key] || [];
      if (invRowsForKey.length > 0) {
        for (let j = 0; j < invRowsForKey.length; j++) {
          const invr = invRowsForKey[j];
          const processed = processInvoiceRow(contractId, cust, m, invr, invoiceLookup, receiptByInvoice, ledger, contractValue);
          streamPush(processed.outRow);
        }
      } else {
        if (ledger.invoiced >= contractValue) {
          continue;
        }
        const remainingInvoiceable = Math.max(0, contractValue - ledger.invoiced);
        const expectedAmt = Math.min(map.monthly || 0, remainingInvoiceable);
        if (expectedAmt > 0) {
          const outRow = processMissingInvoice(contractId, cust, m, expectedAmt, ledger, contractValue);
          if (outRow) streamPush(outRow);
        }
      }

      if (streamBuffer.length >= BATCH_WRITE_CHUNK) flushStream();
    }
  }

  // flush remaining buffer
  flushStream();
  tend("build_rows");

  // finalize: freeze headers
  for (let idx = 1; idx <= outSheetIndex; idx++) {
    const nm = idx === 1 ? TARGET_BASE_NAME : `${TARGET_BASE_NAME} (${idx})`;
    const s = targetSS.getSheetByName(nm);
    if (s) s.setFrozenRows(1);
  }

  Logger.log(`${LOG_PREFIX} done. Sheets written: ${outSheetIndex}. Time: ${Date.now() - START_TS} ms`);
}
