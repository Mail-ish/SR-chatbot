function integrateContractsFast() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const siteSheet = ss.getSheetByName("2-Contract(site)");
  const sheetSheet = ss.getSheetByName("2-Contract(sheet)");
  const salesSheet = ss.getSheetByName("Sales");
  const partnerSheet = ss.getSheetByName("Partners");
  const outSheet = ss.getSheetByName("Integrated");
  if (!siteSheet || !sheetSheet || !outSheet) throw new Error("Missing required sheets.");
    // === Load external Product Map (only for OTHER SKUs) ===
  const externalMap = loadExternalProductMap();
  const externalAtoZMap = loadExternalAtoZMap();


  const siteRowsRaw = siteSheet.getDataRange().getValues();
  const sheetRowsRaw = sheetSheet.getDataRange().getValues();
  const salesRows = (salesSheet && salesSheet.getLastRow() > 1) ? salesSheet.getDataRange().getValues().slice(1) : [];
  const partnerRows = (partnerSheet && partnerSheet.getLastRow() > 1) ? partnerSheet.getDataRange().getValues().slice(1) : [];

  if (siteRowsRaw.length <= 1 && sheetRowsRaw.length <= 1) {
    Logger.log("No data found.");
    return;
  }

  const siteRows = siteRowsRaw.slice(1);
  const sheetRows = sheetRowsRaw.slice(1);

  // Partner map
  const partnerMap = {};
  for (const r of partnerRows) {
    const id = (r[0] || "").toString().trim();
    const name = (r[1] || "").toString().trim();
    if (name) partnerMap[name.toUpperCase()] = id;
  }

  // Sales map
  const salesMap = {};
  for (const r of salesRows) {
    const srcRaw = (r[1] || "").toString().trim().toUpperCase();
    const agency = (r[2] || "").toString().trim();
    const salesPerson = (r[2] || "").toString().trim();
    const conId = (r[5] || "").toString().trim();
    if (!conId) continue;
    let salesSrc = "online";
    if (srcRaw === "AFFILIATE") salesSrc = "internal";
    else if (srcRaw === "AGENCY") salesSrc = "agency";
    else if (srcRaw === "REFERRAL") salesSrc = "referral";
    salesMap[conId.toUpperCase()] = { salesSrc, agency, salesPerson };
  }

  // ===== Sheet meta precomputation =====
  const sheetMeta = [];
  const legacyTokenMap = {};

  for (let i = 0; i < sheetRows.length; i++) {
    const r = sheetRows[i];
    const conId = (r[2] || "").toString().trim();
    const conIdNorm = conId.toUpperCase();

    const custMulti = (r[0] || "").toString().split("||").map(c => c.trim()).filter(Boolean);
    const custNormMulti = custMulti.map(c => normalizeName(c));

    const pkg = cleanPackage(((r[3] || "").toString().split("/")[0] || "").trim());
    const qty = (r[4] || "").toString().trim();
    const startsMulti = (r[6] || "").toString().split("||").map(s => normalizeDate(s)).filter(Boolean);
    const seg = (r[1] || "").toString().trim();

    sheetMeta.push({ index: i, conId, conIdNorm, custMulti, custNormMulti, pkg, qty, startsMulti, seg });

    for (const token of conIdNorm.split(/[\/\-]/)) {
      if (token.length > 4) {
        if (!legacyTokenMap[token]) legacyTokenMap[token] = [];
        legacyTokenMap[token].push(i);
      }
    }
  }

  // ===== Site meta =====
  const siteMeta = siteRows.map((r) => {
    const siteConId = (r[0] || "").toString().trim();
    const legacyOrder = (r[15] || "").toString().trim();
    const siteCust = (r[9] || "").toString().trim();
    const sitePic = (r[10] || "").toString().trim();
    const nameKey = siteCust && siteCust !== "N/A" ? siteCust : sitePic;
    const nameKeyNorm = normalizeName(nameKey);
    const siteSku = cleanPackage(((r[22] || "").toString().split("/")[0] || "").trim());
    const siteQty = (r[23] || "").toString().trim();
    const siteStart = normalizeDate(r[2], r[4]);
    const siteEnd = normalizeDate(r[3], r[4]);
    return { row: r, siteConId, legacyOrder, nameKey, nameKeyNorm, siteSku, siteQty, siteStart, siteEnd };
  });

  // ===== Smart legacy lookup =====
  function smartLegacyLookup(legacyOrder, siteStart, siteSku, nameKeyNorm) {
    if (!legacyOrder) return null;
    const keyU = legacyOrder.toUpperCase();
    const siteStartTime = new Date(siteStart || "1970-01-01").getTime();

    for (const m of sheetMeta) {
      if (m.conIdNorm === keyU && m.custNormMulti.includes(nameKeyNorm)) return sheetRows[m.index];
    }

    const tokens = keyU.split(/[\/\-]/);
    let candidates = [];
    for (const token of tokens) {
      if (legacyTokenMap[token]) {
        candidates.push(...legacyTokenMap[token].filter(idx =>
          sheetMeta[idx].custNormMulti.includes(nameKeyNorm)
        ));
      }
    }
    candidates = [...new Set(candidates)];
    if (candidates.length === 0) return null;

    const scored = candidates.map(i => {
      const meta = sheetMeta[i];
      const diffs = meta.startsMulti.map(s => Math.abs(new Date(s || "1970-01-01").getTime() - siteStartTime));
      const diff = diffs.length ? Math.min(...diffs) : Infinity;
      const sameSku = (meta.pkg === siteSku) ? -100000 : 0;
      return { i, score: diff + sameSku };
    });
    scored.sort((a, b) => a.score - b.score);
    return sheetRows[scored[0].i];
  }

  // ===== Integration core =====
  const integrated = [];
  const matchedSheetIdx = new Set();

  for (const sMeta of siteMeta) {
    const r = sMeta.row;
    const { siteConId, legacyOrder, nameKey, nameKeyNorm, siteSku, siteQty, siteStart, siteEnd } = sMeta;
    const sitePeriod = (r[16] || "").toString().trim();
    const siteStatusRaw = (r[4] || "").toString().trim();
    const sitePeriodMonths = parseInt((r[16] || "").toString().trim(), 10);
    const statusMapped = mapStatus(siteStatusRaw, sMeta.siteStart, sMeta.siteEnd, sitePeriodMonths);

    let match = smartLegacyLookup(legacyOrder, siteStart, siteSku, nameKeyNorm);
    if (!match) {
      for (const meta of sheetMeta) {
        const nameMatch = meta.custNormMulti.some(c => nameKeyNorm.includes(c) || c.includes(nameKeyNorm));
        const skuMatch = meta.pkg === siteSku;
        const qtyMatch = meta.qty === siteQty;
        const dateMatch = meta.startsMulti.some(d => d && siteStart && d.split("||").some(x => x.trim() === siteStart));

        if (nameMatch && skuMatch && qtyMatch && dateMatch) {
          match = sheetRows[meta.index];
          break;
        }
      }
    }

    if (!match) {
      const compKeys = [];
      const startCandidates = siteStart ? [siteStart] : [];
      const nameCandidates = [nameKeyNorm];

      for (const n of nameCandidates) {
        for (const s of startCandidates) {
          compKeys.push([n, siteQty, s].join("|"));
        }
      }

      for (const key of compKeys) {
        for (const m of sheetMeta) {
          if (
            m.custNormMulti.includes(nameKeyNorm) &&
            m.startsMulti.some(st => Math.abs(new Date(st) - new Date(siteStart)) < 86400000 * 90)
          ) {
            match = sheetRows[m.index];
            break;
          }
        }
        if (match) break;
      }
    }

    if (match) matchedSheetIdx.add(sheetRows.indexOf(match));

    const sheetStart = match ? normalizeDate(match[6], r[4]) : "";
    const sheetSku = match ? cleanPackage(((match[3] || "").toString().split("/")[0] || "").trim()) : "";
    const sheetStatus = match ? ((match[5] || "").toString().trim().toUpperCase() || "") : "";

    let startFinal = (siteStart && siteStart !== "N/A") ? siteStart : (sheetStart || "N/A");
    let skuFinal = (siteSku && siteSku !== "N/A") ? siteSku : (sheetSku || "N/A");
    let statusFinal = (statusMapped && statusMapped !== "N/A") ? statusMapped : (sheetStatus || "N/A");

    const contractVal = match ? (match[45] || r[1] || "") : (r[1] || "");
    let sheetSeg = match ? (match[1] || "").toString().trim().toUpperCase() : "";
    let custSeg = (sheetSeg && sheetSeg !== "N/A") ? sheetSeg : deriveSegmentFromSite(r);
    if (custSeg.toUpperCase() === "INDIVIDUAL") custSeg = "IND";

    const salesRef = salesMap[(siteConId || "").toUpperCase()] || {};
    const salesSrc = salesRef.salesSrc || "online";
    const agency = salesRef.agency || "";
    const salesPerson = salesRef.salesPerson || "";
    const salesPersonId = partnerMap[(salesPerson || "").toUpperCase()] || "";

    // --- FILTERING RULES ---
    const nameRaw = (nameKey || "").toString().trim().toUpperCase();
    const skuRaw = (skuFinal || "").toString().trim().toUpperCase();
    const excludedNames = [
      "GANESH",
      "GNESH88",
      "GANESH DEGARAJ",
      "ALEXANDER AND CORTEZ TRADING",
      "JENKINS AND ROSA ASSOCIATES",
      "XYZ ENTERPRISE",
      "GANESHA SDN BHD"
    ];

    if (
      (excludedNames.includes(nameRaw)) ||
      (nameRaw.includes("TEST") && !nameRaw.includes("TESTBITS")) ||
      (!nameRaw.includes("TESTBITS") && (skuRaw.includes("TEST") || skuRaw.includes("TESTING")))
    ) {
      continue; // Skip unwanted rows
    }

    // Determine final product category
    let productCategory = classifyCategory(skuFinal);

    // --- Apply external Product Map if available ---
    if (skuFinal) {
      const skuKey = cleanText(skuFinal);           // Normalize SKU for map
      const externalCat = externalMap[skuKey];      // Lookup external map

      if (externalCat === "EXCLUDE") {
        Logger.log(`⛔ Skipping SKU "${skuFinal}" as category is EXCLUDE.`);
        continue; // skip this row entirely
      } else if (externalCat && productCategory === "OTHER") {
        productCategory = externalCat; // replace OTHER with mapped category
      }
    }

    // Extra safeguard for any manual EXCLUDE classification
    if (productCategory.includes("EXCLUDE")) continue;

    // --- SECOND External Mapping (A-Z) only if still OTHER ---
    if (productCategory === "OTHER") {
      const key1 = `${(match ? (match[2] || "") : siteConId).toString().trim().toUpperCase()}|${nameRaw}|${skuRaw}|${(siteQty || "").toString().trim()}`;
      const key2 = `${siteConId.toString().trim().toUpperCase()}|${nameRaw}|${skuRaw}|${(siteQty || "").toString().trim()}`;

      const mappedCat = externalAtoZMap[key1] || externalAtoZMap[key2];
      if (mappedCat) {
        if (mappedCat.includes("EXCLUDE")) {
          continue;  // skip row entirely
        }
        productCategory = mappedCat; // override OTHER
      }
    }

    // --- Only push after filters pass ---
    integrated.push([
      salesSrc, agency, siteConId || "", match ? (match[2] || "") : "",
      nameKey || "", (r[10] || "") || nameKey || "", custSeg || "", statusFinal,
      startFinal || "", siteEnd || "", sitePeriod || "", skuFinal || "",
      productCategory, siteQty || "", r[13] || "", (r[20] || "") || "",
      (r[21] || "") || "", contractVal || "", (r[24] || "") || "",
      salesPerson || "", salesPersonId || "", (r[17] || "") || "",
      match ? "Matched" : "Unmatched", match ? "Both" : "Site"
    ]);
  }

  // ===== Add unmatched sheet-only =====
  for (let i = 0; i < sheetRows.length; i++) {
    if (matchedSheetIdx.has(i)) continue;
    const s = sheetRows[i];
    const conIdSheetOnly = (s[2] || "").toString().trim();
    if (!conIdSheetOnly) continue;

    const sheetCust = (s[0] || "").toString().trim();
    const pkg = (s[3] || "").toString().trim();
    const qty = (s[4] || "").toString().trim();
    const rawStatus = (s[5] || "").toString().trim();
    const startDate = normalizeDate(s[6], s[5]);
    const endDate = normalizeDate(s[7], s[5]);
    const periodMonths = parseInt((s[16] || "").toString().trim(), 10); // adjust index if period column exists
    const statusMapped = mapStatus(rawStatus, startDate, endDate, periodMonths);

    const finalStatus = (statusMapped && statusMapped !== "N/A") ? statusMapped : (rawStatus || "UNKNOWN");

    // Contract value from column AT (index 45)
    let contractValSheet = (s[45] || "").toString().trim();
    if (!contractValSheet || contractValSheet === "N/A" || contractValSheet === "-") {
      contractValSheet = "N/A";
    }

    let custSegSheet = (s[1] || "").toString().trim();
    if (custSegSheet.toUpperCase() === "INDIVIDUAL") custSegSheet = "IND";

    let productCategory = classifyCategory(pkg);

    // --- SECOND External Mapping for Sheet-only rows ---
    if (productCategory === "OTHER") {
      const key1 = `${conIdSheetOnly.toString().trim().toUpperCase()}|${sheetCust.toString().trim().toUpperCase()}|${pkg.toString().trim().toUpperCase()}|${qty}`;
      const mappedCat = externalAtoZMap[key1];

      if (mappedCat) {
        if (mappedCat.includes("EXCLUDE")) continue; // skip
        productCategory = mappedCat;
      }
    }

    if (pkg) {
      const skuKey = cleanText(pkg);
      const externalCat = externalMap[skuKey];

      if (externalCat === "EXCLUDE") {
        Logger.log(`⛔ Skipping sheet-only SKU "${pkg}" as category is EXCLUDE.`);
        continue;
      } else if (externalCat && productCategory === "OTHER") {
        productCategory = externalCat;
      }
    }

    if (productCategory.includes("EXCLUDE")) continue;


    integrated.push([
      "", "", "", conIdSheetOnly,
      sheetCust || "", sheetCust || "",
      custSegSheet || "", finalStatus,
      startDate || "", "", "", pkg || "",
      classifyCategory(pkg), qty || "",
      "", "", "", contractValSheet, "", "",
      "", "", "Sheet only", "Sheet"
    ]);
  }

  // ===== Group + Sort by latest start date =====
  const grouped = {};
  for (const row of integrated) {
    const custRaw = row[4] || "UNKNOWN";
    const custKey = normalizeName(custRaw) || "UNKNOWN";
    if (!grouped[custKey]) grouped[custKey] = { custRaw, rows: [] };
    grouped[custKey].rows.push(row);
  }

  const groupMeta = [];
  for (const k in grouped) {
    const meta = grouped[k];
    let latest = 0;
    for (const r of meta.rows) {
      const d = r[8] || "";
      const t = d ? new Date(d).getTime() : 0;
      if (t > latest) latest = t;
    }
    meta.rows.sort((a, b) => new Date(b[8] || 0) - new Date(a[8] || 0));
    groupMeta.push({ custKey: k, latest, rows: meta.rows });
  }
  groupMeta.sort((A, B) => B.latest - A.latest);

  const sortedIntegrated = groupMeta.flatMap(g => g.rows);

  // ===== Filter rows: keep only start date from 2022 onwards OR LIVE status =====
  const filteredIntegrated = sortedIntegrated.filter(row => {
    const start = row[8]; // Start Date column
    const status = (row[7] || "").toString().trim().toUpperCase();

    if (!start || start === "N/A") return false;
    const startDate = new Date(start);
    if (isNaN(startDate.getTime())) return false;

    const year = startDate.getFullYear();
    return (year >= 2022) || (status === "LIVE");
  });

  // ===== Write Output =====
  outSheet.clearContents();
  const headers = [
    "Sales Source", "PIC/Agency", "Con. ID (site)", "Con. ID (sheet)",
    "Customer Name", "PIC Name", "Customer Segment", "Status",
    "Start Date", "End Date", "Period", "SKU", "Product Category",
    "Qty", "Delivery Address", "Leading Months Paid", "Tailing Months Paid", 
    "Contract Value", "Unit Price", "Sales Person", "Sales Person ID", "Contract Category",
    "Flags", "Source Sheet" 
  ];
  outSheet.getRange(1, 1, 1, headers.length).setValues([headers]);

  const CHUNK_SIZE = 3000;
  for (let i = 0; i < filteredIntegrated.length; i += CHUNK_SIZE) {
    const chunk = filteredIntegrated.slice(i, i + CHUNK_SIZE);
    outSheet.getRange(i + 2, 1, chunk.length, headers.length).setValues(chunk);
    SpreadsheetApp.flush();
    Utilities.sleep(100);
  }

  // ===== Create "Integrated (OTHER)" sheet with only OTHER category =====
  const otherSheetName = "Integrated (OTHER)";
  let otherSheet = ss.getSheetByName(otherSheetName);
  if (!otherSheet) otherSheet = ss.insertSheet(otherSheetName);
  otherSheet.clearContents();

  // Filter rows where Product Category === "OTHER"
  const productCategoryIdx = headers.indexOf("Product Category");
  const otherRows = filteredIntegrated.filter(r => 
    (r[productCategoryIdx] || "").toString().trim().toUpperCase() === "OTHER"
  );

  // Write output if any rows found
  if (otherRows.length > 0) {
    otherSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    const CHUNK_SIZE = 3000;
    for (let i = 0; i < otherRows.length; i += CHUNK_SIZE) {
      const chunk = otherRows.slice(i, i + CHUNK_SIZE);
      otherSheet.getRange(i + 2, 1, chunk.length, headers.length).setValues(chunk);
      SpreadsheetApp.flush();
      Utilities.sleep(100);
    }
    Logger.log(`🟡 Created "${otherSheetName}" with ${otherRows.length} rows of Product Category = OTHER.`);
  } else {
    otherSheet.getRange(1, 1).setValue("No rows found with Product Category = OTHER.");
    Logger.log(`🟡 No "OTHER" category rows found.`);
  }


  Logger.log(`✅ Integrated ${filteredIntegrated.length} rows successfully (filtered by start date and status).`);

}

/** === Helper functions === */
function normalizeName(n) {
  return (n || "").toString().trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
}
function normalizeDate(d, rawStatus = "") {
  if (!d || d === "N/A") return "";
  try {
    // --- Handle multiple date entries like "2023-05-01 || 2024-04-29"
    const parts = d.toString().split("||").map(p => p.trim()).filter(Boolean);
    const parsedDates = parts
      .map(p => {
        if (p instanceof Date) return p;
        const tryDate = new Date(p);
        return isNaN(tryDate.getTime()) ? null : tryDate;
      })
      .filter(Boolean);

    if (parsedDates.length === 0) return "";

    // --- Special handling for mixed status (END CONTRACT || LIVE)
    const status = (rawStatus || "").toUpperCase();
    if (status.includes("END CONTRACT") && status.includes("LIVE")) {
      const latest = new Date(Math.max(...parsedDates.map(dt => dt.getTime())));
      return Utilities.formatDate(latest, Session.getScriptTimeZone(), "yyyy-MM-dd");
    }

    // Otherwise: return first normalized date
    const first = parsedDates[0];
    return Utilities.formatDate(first, Session.getScriptTimeZone(), "yyyy-MM-dd");
  } catch (e) {}
  return "";
}
function cleanPackage(p) {
  return (p || "").toString().trim().toUpperCase();
}
function cleanText(str) {
  return (str || "").toString().replace(/\s+/g, " ").trim().toUpperCase();
}
function classifyCategory(sku) {
  if (!sku) return "OTHER";
  if (sku.includes("SRLP") || sku.includes("SRDT") || sku.includes("SRIPAD87")) return "SSS";
  if (sku.includes("SAPLP") || sku.includes("SAPDT") ) return "SAP";
  if (sku.includes("GWLP") || sku.includes("GWDT") ) return "GW";
  if (sku.includes("SAMV") || sku.includes("SRPX") ) return "SMARTBOARD";
  if (sku.includes("RTO") ) return "SSS/RTO";
  return "OTHER";
}
function deriveSegmentFromSite(r) {
  const siteJ = (r[9] || "").toString().trim().toUpperCase(); // Column J
  const siteK = (r[10] || "").toString().trim().toUpperCase(); // Column K
  
  if (siteJ && siteJ !== "N/A") return "SME";
  if (siteK && siteK !== "N/A") return "IND";
  return "N/A";
}
/**
 * Maps raw site status text into standardized statuses:
 * LIVE / INACTIVE / PENDING / UNKNOWN
 * Includes logic for OVERDUE contracts based on end date or calculated period.
 */
function mapStatus(rawStatus, startDate, endDate, periodMonths) {
  if (!rawStatus) return "UNKNOWN";

  // NEW BEHAVIOR:
  // ---------------------------------------------------------
  // Return LIVE if raw contains LIVE
  if (rawStatus.toUpperCase().includes("LIVE")) {
    return "LIVE";
  }

  // Return INACTIVE if raw contains INACTIVE-A or INACTIVE-B
  if (rawStatus.toUpperCase().includes("INACTIVE-A") ||
      rawStatus.toUpperCase().includes("INACTIVE-B") ||
      rawStatus.toUpperCase().includes("END CONTRACT")){ 
    return "INACTIVE";
  }

  // Otherwise leave status AS IS (preserve original)
  return rawStatus.toString().trim();

  /* ============================================================
     ORIGINAL STANDARDISATION LOGIC (NOW COMMENTED OUT)
     ============================================================

  const statuses = rawStatus.split("||").map(s => s.trim().toUpperCase()).filter(Boolean);
  const now = new Date();

  const normalized = statuses.map(st => {
    if (st.includes("INACTIVE-A") || st.includes("INACTIVE-B")) return "INACTIVE";
    if (["TERMINATED", "END CONTRACT", "CANCELLED", "EXPIRED"].includes(st)) return "INACTIVE";
    if (["ACTIVE", "LIVE"].includes(st)) return "LIVE";

    if (st === "PENDING") {
      let start = null, end = null;

      if (startDate && startDate !== "N/A") {
        const s = new Date(startDate);
        if (!isNaN(s.getTime())) start = s;
      }

      if (endDate && endDate !== "N/A") {
        const e = new Date(endDate);
        if (!isNaN(e.getTime())) end = e;
      }

      if (!end && start && periodMonths && !isNaN(periodMonths)) {
        end = new Date(start);
        end.setMonth(end.getMonth() + Number(periodMonths));
      }

      if (!start || !end) return "UNKNOWN";
      if (now < start || now >= end) return "INACTIVE";
      if (now >= start && now < end) return "LIVE";
      return "UNKNOWN";
    }

    if (st === "OVERDUE") {
      let end = null;
      if (endDate && endDate !== "N/A") {
        const d = new Date(endDate);
        if (!isNaN(d.getTime())) end = d;
      }
      if (!end && startDate && startDate !== "N/A" && periodMonths && !isNaN(periodMonths)) {
        const s = new Date(startDate);
        if (!isNaN(s.getTime())) {
          end = new Date(s);
          end.setMonth(end.getMonth() + Number(periodMonths));
        }
      }
      if (!end) return "UNKNOWN";
      return now < end ? "LIVE" : "INACTIVE";
    }

    return "UNKNOWN";
  });

  const uniqueNorm = [...new Set(normalized)];

  if (uniqueNorm.includes("LIVE")) return "LIVE";
  if (uniqueNorm.includes("ACTIVE")) return "LIVE";
  if (uniqueNorm.includes("INACTIVE")) return "INACTIVE";
  return uniqueNorm[0] || "UNKNOWN";

  ============================================================ */
}
/**
 * Loads the external "Product Map" from another spreadsheet.
 * Only used for SKUs that were originally classified as "OTHER".
 */
function loadExternalProductMap() {
  const externalId = "1gvXkc1lwIUTW_sU4BOqolCqc2NY-ccShLwskw-A1QBY";
  const sheetName = "Product Map";
  const map = {};

  try {
    const extSS = SpreadsheetApp.openById(externalId);
    const mapSheet = extSS.getSheetByName(sheetName);
    if (!mapSheet) {
      Logger.log(`⚠️ Missing sheet "${sheetName}" in external mapping file.`);
      return map;
    }

    const data = mapSheet.getRange(1, 1, mapSheet.getLastRow(), 2).getValues();

    for (const [skuRaw, catRaw] of data) {
      const sku = cleanText(skuRaw);        // SKU cleaned
      const cat = cleanText(catRaw);        // Category cleaned
      if (!sku) continue;
      map[sku] = cat || "OTHER";            // Default to OTHER if blank
    }

    Logger.log(`✅ Loaded ${Object.keys(map).length} SKU mappings from external "Product Map".`);

  } catch (e) {
    Logger.log(`⚠️ Failed to load external Product Map: ${e}`);
  }

  return map;
}
function loadExternalAtoZMap() {
  const externalId = "1SJdoDLzDo8x4LtsSlM3rlSprFaVOKF-8eiTWj49qBHQ";
  const sheetName = "Copy of A-Z Contract Results - Filtered_SKU_Blanks";

  const map = {};  // key → productCategory

  try {
    const ss = SpreadsheetApp.openById(externalId);
    const sh = ss.getSheetByName(sheetName);
    if (!sh) {
      Logger.log(`⚠️ Missing sheet "${sheetName}" in external file.`);
      return map;
    }

    const data = sh.getRange(1, 1, sh.getLastRow(), sh.getLastColumn()).getValues();
    const headers = data[0];
    const rows = data.slice(1);

    const idxSheet = headers.indexOf("Con. ID (sheet)");
    const idxSite = headers.indexOf("Con. ID (site)");
    const idxName = headers.indexOf("Customer Name");
    const idxSku  = headers.indexOf("SKU");
    const idxQty  = headers.indexOf("Qty");
    const idxCat  = headers.indexOf("Product Category");

    if ([idxSheet, idxSite, idxName, idxSku, idxQty, idxCat].includes(-1)) {
      Logger.log("⚠️ External A-Z sheet missing required columns.");
      return map;
    }

    for (const r of rows) {
      const conSheet = (r[idxSheet] || "").toString().trim().toUpperCase();
      const conSite  = (r[idxSite]  || "").toString().trim().toUpperCase();
      const name     = (r[idxName]  || "").toString().trim().toUpperCase();
      const sku      = (r[idxSku]   || "").toString().trim().toUpperCase();
      const qty      = (r[idxQty]   || "").toString().trim();
      const cat      = (r[idxCat]   || "").toString().trim().toUpperCase();

      if (!name || !sku) continue;

      // Build matching keys
      const keys = [];

      if (conSheet) keys.push(`${conSheet}|${name}|${sku}|${qty}`);
      if (conSite)  keys.push(`${conSite}|${name}|${sku}|${qty}`);

      for (const key of keys) {
        map[key] = cat;
      }
    }

    Logger.log(`📘 Loaded ${Object.keys(map).length} rows from external A-Z mapping.`);
  } catch (e) {
    Logger.log(`⚠️ Failed to load external A-Z map: ${e}`);
  }

  return map;
}

