function generateContractReport() {
  const ss = SpreadsheetApp.getActive();
  const integratedSheet = ss.getSheetByName("Integrated");
  if (!integratedSheet) {
    Logger.log("❌ Integrated sheet not found. Aborting Contract Report generation.");
    return;
  }

  const data = integratedSheet.getDataRange().getValues();
  const headers = data.shift(); // remove header row

  const reportSheetName = "Contract Report";
  let reportSheet = ss.getSheetByName(reportSheetName);
  if (!reportSheet) reportSheet = ss.insertSheet(reportSheetName);
  reportSheet.getRange("A:K").clearContent();

  const reportHeaders = [
    "Contract Category",
    "Contract ID",
    "Company Name",
    "Customer Name",
    "Status",
    "Package",
    "Qty",
    "Start Date",
    "End Date",
    "Contract Period",
    "Delivery Address"
  ];
  reportSheet.getRange(1, 1, 1, reportHeaders.length).setValues([reportHeaders]);

  // --- date formatting helper ---
  function formatDate(val) {
    if (!val || val === "N/A") return "-";

    let d;

    if (val instanceof Date) {
      d = val;
    } else {
      d = new Date(val);
      if (isNaN(d.getTime())) return "-";
    }

    const dd = String(d.getDate()).padStart(2, "0");
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const yyyy = d.getFullYear();

    return `${dd}/${mm}/${yyyy}`;
  }

  const rows = data.map(row => {
    const get = (i) => {
      const val = (row[i] || "").toString().trim();
      return (!val || val === "N/A" || val === "N") ? "-" : val;
    };

    // Contract ID: C → fallback D
    let contractId = get(2);
    if (contractId === "-") contractId = get(3);

    // Uppercase customer name
    let custName = get(4);
    if (custName !== "-") custName = custName.toUpperCase();

    // SME → Company Name (uppercase)
    const segment = get(6).toUpperCase();
    let companyName = segment === "SME" ? get(4) : "-";
    if (companyName !== "-") companyName = companyName.toUpperCase();

    return [
      get(21),                    // Contract Category
      contractId,                // Contract ID
      companyName,               // Company Name
      custName,                  // Customer Name
      get(7),                    // Status
      get(11),                   // Package
      get(13),                   // Qty
      formatDate(row[8]),        // Start Date (I)
      formatDate(row[9]),        // End Date (J)
      get(10),                   // Contract Period
      get(14)                    // Delivery Address
    ];
  });

  if (rows.length > 0) {
    reportSheet.getRange(2, 1, rows.length, 11).setValues(rows);
  }

  Logger.log(`📄 Contract Report generated: ${rows.length} rows`);
}

