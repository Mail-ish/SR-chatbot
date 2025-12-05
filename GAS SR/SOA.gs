/**
 * Ultra-Optimized SOA Detailed Ledger → External Spreadsheet
 * Uses Contract View instead of Contract Report
 */
function buildSOAtoExternal() {
  const START = Date.now();
  const LOG = msg => Logger.log("SOA: " + msg);

  // CONFIG
  const LOCAL = SpreadsheetApp.getActiveSpreadsheet();
  const TARGET_ID = "1xxHdcl8TJ4Sy_EIuVQgNdiyWE1fSWGtyrhp04dT724g";

  const SH_INV = "Invoices";
  const SH_RCPT = "Receipts";
  const SH_CV = "Contract View";

  const OUTPUT_BASE = "SOA";
  const BATCH_SIZE = 5000;
  const MAX_ROWS_PER_SHEET = 50000;

  LOG("Loading sheets…");

  const shInv = LOCAL.getSheetByName(SH_INV);
  const shRcpt = LOCAL.getSheetByName(SH_RCPT);
  const shCV = LOCAL.getSheetByName(SH_CV);

  if (!shInv || !shRcpt || !shCV) throw new Error("Required sheet missing: Invoices / Receipts / Contract View");

  const invData = shInv.getDataRange().getValues();
  const rcptData = shRcpt.getDataRange().getValues();
  const cvData = shCV.getDataRange().getValues();

  const invHdr = invData[0], invRows = invData.slice(1);
  const rcptHdr = rcptData[0], rcptRows = rcptData.slice(1);
  const cvHdr = cvData[0], cvRows = cvData.slice(1);

  const idx = (hdr, name) => hdr.indexOf(name);

  // INVOICE COLUMN INDEXES
  const I = {
    number: idx(invHdr, "number"),
    period: idx(invHdr, "period_month"),
    contract: idx(invHdr, "contract_number"),
    amount: idx(invHdr, "amount"),
    status: idx(invHdr, "status"),
    company: idx(invHdr, "company")
  };

  // RECEIPT COLUMN INDEXES
  const R = {
    number: idx(rcptHdr, "number"),
    contract: idx(rcptHdr, "contract_number"),
    payDate: idx(rcptHdr, "payment_date"),
    payer: idx(rcptHdr, "payer_name"),
    invLink: idx(rcptHdr, "invoice_number"),
    amount: idx(rcptHdr, "amount"),
    status: idx(rcptHdr, "status")
  };

  // CONTRACT VIEW COLUMN INDEXES
  const CV_CON_ID = idx(cvHdr, "Con. ID (site)"); // use site ID as primary key
  const CV_CUSTOMER = idx(cvHdr, "Customer Name");
  if (CV_CON_ID === -1 || CV_CUSTOMER === -1) throw new Error("Required columns missing in Contract View");

  // HELPER FUNCTIONS
  const toNum = v => (typeof v === "number" ? v : parseFloat(String(v).replace(/[^0-9.\-]/g, "")) || 0);
  const parseYMD = s => (s instanceof Date ? s : s ? new Date(s) : new Date(1970,0,1));
  const parsePeriod = s => {
    if (!s) return new Date(1970,0,1);
    if (s instanceof Date) return s;
    const m = String(s).match(/^(\d{4})[\/-](\d{1,2})$/);
    return m ? new Date(+m[1], +m[2]-1, 1) : new Date(s);
  };
  const fmtYM = d => d ? `${d.getFullYear()}/${String(d.getMonth()+1).padStart(2,"0")}` : "-";
  const fmtYMD = d => d ? `${d.getFullYear()}/${String(d.getMonth()+1).padStart(2,"0")}/${String(d.getDate()).padStart(2,"0")}` : "-";

  LOG("Indexing contracts…");
  const contractToCustomer = new Map();
  for (const row of cvRows) {
    const cid = String(row[CV_CON_ID] || "").trim().toUpperCase();
    if (!cid) continue;
    const customer = String(row[CV_CUSTOMER] || "").trim().toUpperCase();
    if (customer) contractToCustomer.set(cid, customer);
  }

  LOG("Loading invoice & receipt entries…");
  const group = new Map();
  const pushEntry = (cid, customer, dateObj, inv, rec, debit, credit, status) => {
    if (!group.has(cid)) group.set(cid, []);
    group.get(cid).push({
      contract: cid,
      customer,
      dateObj,
      dateText: fmtYMD(dateObj),
      inv,
      rec,
      debit,
      credit,
      status: status.toUpperCase(),
      balance: 0
    });
  };

  // ADD INVOICES
  for (const r of invRows) {
    const cidRaw = r[I.contract];
    if (!cidRaw) continue;
    const cid = String(cidRaw).trim().toUpperCase();
    if (!contractToCustomer.has(cid)) continue;
    pushEntry(cid, contractToCustomer.get(cid), parsePeriod(r[I.period]), r[I.number]||"", "", toNum(r[I.amount]), 0, r[I.status]||"");
  }

  // ADD RECEIPTS
  for (const r of rcptRows) {
    const cidRaw = r[R.contract];
    if (!cidRaw) continue;
    const cid = String(cidRaw).trim().toUpperCase();
    if (!contractToCustomer.has(cid)) continue;
    pushEntry(cid, contractToCustomer.get(cid), parseYMD(r[R.payDate]), r[R.invLink]||"", r[R.number]||"", 0, toNum(r[R.amount]), r[R.status]||"");
  }

  LOG("Sorting & computing balances…");
  for (const [cid, arr] of group) {
    arr.sort((a,b) => a.dateObj - b.dateObj || (a.debit && b.credit ? -1 : b.debit && a.credit ? 1 : 0));
    let bal = 0;
    for (const e of arr) bal = e.balance = +((bal + e.debit - e.credit).toFixed(2));
  }

  LOG("Opening target spreadsheet…");
  const target = SpreadsheetApp.openById(TARGET_ID);

  // DELETE old SOA sheets
  for (const sh of target.getSheets()) {
    const nm = sh.getName();
    if (nm === OUTPUT_BASE || nm.startsWith(OUTPUT_BASE + " (")) {
      try { target.deleteSheet(sh); } catch(_){}
    }
  }

  LOG("Creating new output sheet…");
  let outIdx = 1;
  let shOut = target.insertSheet(OUTPUT_BASE);
  const HEAD = ["Contract ID","Customer Name","Date","Invoice No.","Receipt No.","Debit","Credit","Payment Status","Balance"];
  shOut.getRange(1,1,1,HEAD.length).setValues([HEAD]);
  shOut.setFrozenRows(1);

  let rowPos = 2;
  let buffer = [];
  const flush = () => {
    if (!buffer.length) return;
    if (rowPos + buffer.length - 1 > MAX_ROWS_PER_SHEET) {
      outIdx++;
      shOut = target.insertSheet(`${OUTPUT_BASE} (${outIdx})`);
      shOut.getRange(1,1,1,HEAD.length).setValues([HEAD]);
      shOut.setFrozenRows(1);
      rowPos = 2;
    }
    shOut.getRange(rowPos,1,buffer.length,HEAD.length).setValues(buffer);
    rowPos += buffer.length;
    buffer = [];
  };

  // WRITE rows contract by contract
  Array.from(group.keys()).sort().forEach(cid => {
    group.get(cid).forEach(e => {
      buffer.push([e.contract, e.customer, fmtYMD(e.dateObj), e.inv, e.rec, e.debit||"", e.credit||"", e.status, e.balance]);
      if (buffer.length >= BATCH_SIZE) flush();
    });
  });

  flush();
  LOG("DONE in " + (Date.now() - START) + " ms");
}

