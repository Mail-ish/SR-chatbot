function filterBlankAndNA_SKUs() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const src = ss.getSheetByName("Integrated (OTHER)");
  if (!src) throw new Error('Sheet "Integrated (OTHER)" not found.');

  const destName = "Filtered_SKU_Blanks";
  let dest = ss.getSheetByName(destName);
  if (!dest) dest = ss.insertSheet(destName);
  else dest.clear();

  const range = src.getDataRange();
  const data = range.getValues();
  const headers = data.shift();

  // Use getDisplayValues to preserve visible date formats
  const displayData = range.getDisplayValues();
  displayData.shift(); // remove header row

  // Filter for blank, "N", or "N/A" in column L (index 11)
  const filtered = data
    .map((r, i) => ({ raw: r, disp: displayData[i] }))
    .filter(obj => {
      const sku = String(obj.raw[11] || "").trim().toUpperCase();
      return sku === "" || sku === "N" || sku === "N/A";
    });

  // Sort order: blank first, then "N", then "N/A"
  const order = { "": 0, "N": 1, "N/A": 2 };
  filtered.sort((a, b) => {
    const sa = String(a.raw[11] || "").trim().toUpperCase();
    const sb = String(b.raw[11] || "").trim().toUpperCase();
    return order[sa] - order[sb];
  });

  // Extract display rows for output
  const output = filtered.map(f => f.disp);

  if (output.length === 0) {
    dest.getRange(1, 1).setValue("No blank/N/N.A SKUs found.");
    Logger.log("✅ No rows matched the filter.");
    return;
  }

  dest.getRange(1, 1, 1, headers.length).setValues([headers]);
  dest.getRange(2, 1, output.length, headers.length).setValues(output);
  dest.autoResizeColumns(1, headers.length);

  Logger.log(`✅ Filtered ${output.length} rows into "${destName}".`);
}
