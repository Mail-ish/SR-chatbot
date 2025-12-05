function buildAccountStatementSummarised() {
  const TARGET_SHEET_ID = "1dk-iP5a0iSbXzdNN0ZF_9uCHfSFVUMVVONX0w1xN_yw";
  const TARGET_SHEET_NAME = "Account Statement - summarised";
  const CHUNK = 3000;

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const targetSS = SpreadsheetApp.openById(TARGET_SHEET_ID);

  // --- 1) Load Contract View ---
  const cv = ss.getSheetByName("Contract View");
  if (!cv) throw new Error("Contract View sheet not found in active spreadsheet.");
  const cvData = cv.getDataRange().getValues();
  if (cvData.length <= 1) throw new Error("Contract View has no data rows.");
  const cvHdr = cvData[0].map(h => (h || "").toString().trim());
  const cvRows = cvData.slice(1);

  const idxCV = name => cvHdr.indexOf(name);
  const I_SITE = idxCV("Con. ID (site)");
  const I_SHEET = idxCV("Con. ID (sheet)");
  const I_CUST = idxCV("Customer Name");
  const I_STATUS = idxCV("Status");
  const I_PERIOD = idxCV("Period");
  const I_TOTALCV = idxCV("Total Contract Value");

  const contractViewMap = Object.create(null);
  const contractViewOrder = [];

  for (const r of cvRows) {
    const site = (r[I_SITE] || "").toString().trim();
    const sheetId = (r[I_SHEET] || "").toString().trim();
    let finalId = site && String(site).startsWith("SHEET_") ? (sheetId || site) : (site || sheetId);
    if (!finalId) continue;

    if (!contractViewMap[finalId]) {
      contractViewMap[finalId] = {
        contractId: finalId,
        customerName: (r[I_CUST] || "").toString().trim() || "-",
        contractStatus: (r[I_STATUS] || "").toString().trim() || "-",
        period: (r[I_PERIOD] || "").toString().trim() || "",
        totalContractValueRaw: r[I_TOTALCV] || ""
      };
      contractViewOrder.push(finalId);
    }
  }

  // --- 2) Build intgMap ---
  const intg = ss.getSheetByName("Integrated") ||
               ss.getSheetByName("integrated") ||
               ss.getSheetByName("1-Integrated");

  const intgMap = Object.create(null);
  if (intg) {
    const intgData = intg.getDataRange().getValues();
    const intgHdr = intgData[0].map(x => (x || "").toString().trim());
    const intgRows = intgData.slice(1);

    const i_site = intgHdr.indexOf("Con. ID (site)");
    const i_sheet = intgHdr.indexOf("Con. ID (sheet)");
    const i_qty = intgHdr.indexOf("Qty");
    const i_unit = intgHdr.indexOf("Unit Price");
    const i_contractValue = intgHdr.indexOf("Contract Value");

    const parseNum = v => {
      if (v === null || v === undefined || v === "") return 0;
      if (typeof v === "number") return v;
      const n = parseFloat(String(v).toString().replace(/[^\d.\-]/g, ""));
      return isNaN(n) ? 0 : n;
    };

    for (const r of intgRows) {
      const key = ((r[i_site] || r[i_sheet] || "") + "").toString().trim();
      if (!key) continue;
      const qty = parseNum(r[i_qty]);
      const unit = parseNum(r[i_unit]);
      const cv = parseNum(r[i_contractValue]);
      if (!intgMap[key]) intgMap[key] = { monthly: 0, contractValue: 0 };
      intgMap[key].monthly += (qty * unit);
      intgMap[key].contractValue += cv;
    }
  }

  // --- 3) Read Account Statement sheets ---
  const statementSheets = targetSS
    .getSheets()
    .filter(s => s.getName() === "Account Statement" || s.getName().startsWith("Account Statement "));

  const agg = Object.create(null);
  let occurrenceCounter = 0;

  const safeNum = v => {
    if (v === null || v === undefined || v === "") return 0;
    if (typeof v === "number") return v;
    const n = parseFloat(String(v).replace(/[^\d.\-]/g, ""));
    return isNaN(n) ? 0 : n;
  };

  const parseDateSafe = v => {
    if (!v) return null;
    const d = (v instanceof Date) ? v : new Date(v);
    return (d instanceof Date && !isNaN(d.getTime())) ? d : null;
  };

  // --- Loop Each Ledger Sheet ---
  for (const sh of statementSheets) {
    const data = sh.getDataRange().getValues();
    if (!data || data.length <= 1) continue;

    const hdr = data[0].map(h => (h || "").toString().trim());
    const rows = data.slice(1);

    const idxOf = name => {
      const i = hdr.indexOf(name);
      if (i >= 0) return i;
      const lower = name.toLowerCase();
      for (let j = 0; j < hdr.length; j++) {
        if ((hdr[j] || "").toString().toLowerCase().includes(lower)) return j;
      }
      return -1;
    };

    const I_CONTRACT = idxOf("Contract ID");
    const I_INVOICENO = idxOf("Invoice No.") >= 0 ? idxOf("Invoice No.") :
                        idxOf("Invoice") >= 0     ? idxOf("Invoice") :
                                                    idxOf("number");
    const I_DEBIT = idxOf("Debit");
    const I_PAID_AT = idxOf("Paid At") >= 0 ? idxOf("Paid At") :
                      idxOf("Paid Date") >= 0 ? idxOf("Paid Date") :
                                                idxOf("paid_at");
    const I_OUTSTANDING = idxOf("Balance");
    const I_CUSTOMER = idxOf("Customer Name") >= 0 ? idxOf("Customer Name") :
                       idxOf("Customer") >= 0      ? idxOf("Customer") : -1;

    const creditIdx = hdr.findIndex(h => h.toLowerCase().includes("credit"));

    // --- Process Each Row ---
    for (let r = 0; r < rows.length; r++) {
      occurrenceCounter++;
      const row = rows[r];

      const contractId = ((row[I_CONTRACT] || "") + "").trim();
      if (!contractId) continue;

      if (!agg[contractId]) {
        agg[contractId] = {
          contractId,
          customerName: (I_CUSTOMER >= 0 ? (row[I_CUSTOMER] || "") : "") || "",
          totalInvoiced: 0,
          totalPaid: 0,
          lastPaidDate: null,
          lastBalance: null,
          invoiceCount: 0
        };
      }
      const entry = agg[contractId];

      // --- Detect "Missing Invoice" and SKIP it entirely ---
      const invNoRaw = (I_INVOICENO >= 0 ? (row[I_INVOICENO] || "") : "").toString().trim();
      if (invNoRaw.toLowerCase().includes("missing")) continue;

      const isRealInvoice = invNoRaw !== "";

      // --- Debit (Invoice Amount) ---
      const debit = I_DEBIT >= 0 ? safeNum(row[I_DEBIT]) : 0;
      if (isRealInvoice && debit > 0) {
        entry.totalInvoiced += debit;
        entry.invoiceCount++;
      }

      // --- Credit (Paid Amount) ---
      if (creditIdx >= 0) {
        const credit = safeNum(row[creditIdx]);
        if (credit > 0) {
          entry.totalPaid += credit;

          const pd = parseDateSafe(row[I_PAID_AT]);
          if (pd && (!entry.lastPaidDate || pd > entry.lastPaidDate)) {
            entry.lastPaidDate = pd;
          }
        }
      }

      // --- Balance ---
      if (I_OUTSTANDING >= 0) {
        const bal = safeNum(row[I_OUTSTANDING]);
        if (!isNaN(bal)) entry.lastBalance = bal;
      }
    }
  }

  // --- 4) Build Output ---
  const outHeaders = [
    "Contract ID",
    "Customer Name",
    "Contract Status",
    "Total Contract Value",
    "Calculated Monthly Amount",
    "Total Invoiced",
    "Total Paid",
    "Outstanding",
    "Invoice Count",
    "Last Paid Date"
  ];

  const outRows = [];

  const moneyFmt = n => {
    if (n === null || n === undefined || isNaN(n)) return "N/A";
    return "RM " + Number(n).toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  };

  const dateFmt = d => {
    if (!d) return "-";
    const dd = String(d.getDate()).padStart(2, "0");
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const yyyy = d.getFullYear();
    return `${dd}/${mm}/${yyyy}`;
  };

  const parseRawNum = v => {
    if (v === null || v === undefined || v === "") return NaN;
    if (typeof v === "number") return v;
    const n = parseFloat(String(v).replace(/[^\d.\-]/g, ""));
    return isNaN(n) ? NaN : n;
  };

  for (const contractId of contractViewOrder) {
    const cvEntry = contractViewMap[contractId];
    if (!cvEntry) continue;

    const customerName = cvEntry.customerName || "-";
    const contractStatus = cvEntry.contractStatus || "-";

    const totalContractValueParsed = parseRawNum(cvEntry.totalContractValueRaw);
    const totalContractValue = isNaN(totalContractValueParsed)
      ? NaN
      : totalContractValueParsed;

    let calcMonthly = null;
    if (intgMap[contractId] && intgMap[contractId].monthly > 0) {
      calcMonthly = intgMap[contractId].monthly;
    } else {
      const period = parseInt(cvEntry.period || "", 10);
      if (!isNaN(period) && period > 0 && !isNaN(totalContractValueParsed)) {
        calcMonthly = totalContractValueParsed / period;
      } else {
        calcMonthly = NaN;
      }
    }

    const a = agg[contractId] || {
      totalInvoiced: 0,
      totalPaid: 0,
      invoiceCount: 0,
      lastBalance: null,
      lastPaidDate: null
    };

    let outstandingVal;
    if (a.lastBalance !== null && !isNaN(a.lastBalance)) {
      outstandingVal = a.lastBalance;
    } else {
      outstandingVal = a.totalInvoiced - a.totalPaid;
    }

    outRows.push([
      contractId,
      customerName,
      contractStatus,
      isNaN(totalContractValue) ? "N/A" : moneyFmt(totalContractValue),
      isNaN(calcMonthly) ? "N/A" : moneyFmt(calcMonthly),
      a.totalInvoiced ? moneyFmt(a.totalInvoiced) : "N/A",
      a.totalPaid ? moneyFmt(a.totalPaid) : "N/A",
      !isNaN(outstandingVal) ? moneyFmt(Math.max(0, outstandingVal)) : "N/A",
      a.invoiceCount,
      a.lastPaidDate ? dateFmt(a.lastPaidDate) : "-"
    ]);
  }

  // --- 5) Write Output ---
  let outSh = targetSS.getSheetByName(TARGET_SHEET_NAME);
  if (!outSh) outSh = targetSS.insertSheet(TARGET_SHEET_NAME);
  outSh.clearContents();

  outSh.getRange(1, 1, 1, outHeaders.length).setValues([outHeaders]);

  for (let i = 0; i < outRows.length; i += CHUNK) {
    const chunk = outRows.slice(i, i + CHUNK);
    outSh.getRange(i + 2, 1, chunk.length, outHeaders.length).setValues(chunk);
  }

  outSh.setFrozenRows(1);

  Logger.log(`Account Statement summarised written to '${TARGET_SHEET_NAME}' — ${outRows.length} contracts.`);
}
