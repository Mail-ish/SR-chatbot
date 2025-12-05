function buildLiveContractSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const src = ss.getSheetByName("Contract View");
  const inv = ss.getSheetByName("Invoices");
  if (!src) throw new Error("❌ Missing sheet: Contract View");
  if (!inv) throw new Error("❌ Missing sheet: Invoices");

  const cvData = src.getDataRange().getValues();
  const cvHdr = cvData[0].map(h => (h || "").toString().trim().toLowerCase());
  const cvRows = cvData.slice(1);

  const invData = inv.getDataRange().getValues();
  const invHdr = invData[0].map(h => (h || "").toString().trim().toLowerCase());
  const invRows = invData.slice(1);

  // === column indexes ===
  const findIdx = (hdr, hint) => hdr.findIndex(h => h.includes(hint.toLowerCase()));

  const IDX_CV = {
    siteId: findIdx(cvHdr, "con. id"),
    name: findIdx(cvHdr, "customer name"),
    seg: findIdx(cvHdr, "segment"),
    status: findIdx(cvHdr, "status"),
    start: findIdx(cvHdr, "start date"),
    end: findIdx(cvHdr, "end date"),
    period: findIdx(cvHdr, "period"),
    total: findIdx(cvHdr, "total contract value"),
  };

  const IDX_INV = {
    contractNum: findIdx(invHdr, "contract_number"),
    legacy: findIdx(invHdr, "legacy_order_id"),
    amount: findIdx(invHdr, "amount"),
    status: findIdx(invHdr, "status"),
  };

  const today = new Date();

  // === Helper functions ===
  function toDate(v) {
    if (!v || v === "") return null;
    if (v instanceof Date) return v;
    if (typeof v === "number") return new Date(1899, 11, 30 + v); // Excel serial date fix
    const d = new Date(v);
    return isNaN(d) ? null : d;
  }

  function formatDate(d) {
    if (!d || !(d instanceof Date) || isNaN(d)) return "MISSING";
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  }

  function addMonths(date, n) {
    const d = new Date(date);
    d.setMonth(d.getMonth() + n);
    return d;
  }

  function monthsBetween(start, end) {
    if (!start || !end) return 0;
    return (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
  }

  function safeNumber(v) {
    if (v == null || v === "") return 0;
    if (typeof v === "number") return v;
    const n = parseFloat(String(v).replace(/[^\d.-]/g, ""));
    return isNaN(n) ? 0 : n;
  }

  // === Build invoice total lookup ===
  const paidByContract = {};
  for (const r of invRows) {
    const cnum = (r[IDX_INV.contractNum] || r[IDX_INV.legacy] || "").toString().trim().toUpperCase();
    if (!cnum) continue;
    const amt = safeNumber(r[IDX_INV.amount]);
    const stat = (r[IDX_INV.status] || "").toString().toLowerCase();
    if (["paid", "partially", "completed"].includes(stat)) {
      paidByContract[cnum] = (paidByContract[cnum] || 0) + amt;
    }
  }

  // === Main loop: build summary per contract ===
  const results = [];

  for (const r of cvRows) {
    const status = (r[IDX_CV.status] || "").toString().trim().toUpperCase();
    if (status !== "LIVE") continue;

    const cid = (r[IDX_CV.siteId] || "").toString().trim().toUpperCase();
    const cust = (r[IDX_CV.name] || "").toString().trim();
    const seg = (r[IDX_CV.seg] || "").toString().trim();
    const start = toDate(r[IDX_CV.start]);
    let end = toDate(r[IDX_CV.end]);
    let period = parseInt(r[IDX_CV.period]) || 0;
    const totalVal = safeNumber(r[IDX_CV.total]);
    let note = "";

    // Infer end date if missing
    if (!end && start && period > 0) {
      end = addMonths(start, period - 1);
      note = "End date inferred";
    } else if (!end && !period) {
      note = "⚠️ Missing end date and period";
    }

    const monthsTotal = start && end ? monthsBetween(start, end) : period || 0;
    const monthsElapsed = start ? monthsBetween(start, today) + 1 : 0;
    const monthsRemain = monthsTotal > 0 ? Math.max(monthsTotal - monthsElapsed, 0) : 0;

    const totalPaid = paidByContract[cid] || 0;
    const totalOutstanding = totalVal - totalPaid;
    const progress = totalVal > 0 ? (totalPaid / totalVal) * 100 : 0;
    const monthly = monthsTotal > 0 ? totalVal / monthsTotal : 0;
    const projectedRemaining = monthly * monthsRemain;

    results.push([
      cid,
      cust,
      seg,
      formatDate(start),
      formatDate(end),
      "LIVE",
      monthsTotal || "UNKNOWN",
      monthsElapsed,
      monthsRemain,
      `RM ${totalVal.toFixed(2)}`,
      `RM ${totalPaid.toFixed(2)}`,
      `RM ${totalOutstanding.toFixed(2)}`,
      `${progress.toFixed(2)}%`,
      `RM ${monthly.toFixed(2)}`,
      `RM ${projectedRemaining.toFixed(2)}`,
      note || "OK",
    ]);
  }

  // === Write output ===
  const out = ss.getSheetByName("Live Summary") || ss.insertSheet("Live Summary");
  out.clearContents();

  const headers = [
    "Contract ID",
    "Customer Name",
    "Customer Type",
    "Start Date",
    "End Date",
    "Status",
    "Contract Period (Months)",
    "Months Elapsed",
    "Months Remaining",
    "Total Contract Value (RM)",
    "Total Paid (RM)",
    "Total Outstanding (RM)",
    "Progress %",
    "Expected Monthly (RM)",
    "Projected Remaining (RM)",
    "Notes",
  ];

  out.getRange(1, 1, 1, headers.length).setValues([headers]);

  if (results.length) {
    out.getRange(2, 1, results.length, headers.length).setValues(results);

    // ✅ Force Start Date & End Date columns to TEXT (keep YYYY-MM-DD format)
    out.getRange(2, 4, results.length, 2).setNumberFormat('@');
  }

  Logger.log(`✅ Live Contract Summary written: ${results.length} LIVE contracts.`);
}
