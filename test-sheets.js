require("dotenv").config();
const { google } = require("googleapis");

(async () => {
  try {
    // Diagnóstico rápido
    console.log("EMAIL:", process.env.GOOGLE_SA_CLIENT_EMAIL);
    console.log("KEY length:", (process.env.GOOGLE_SA_PRIVATE_KEY || "").length);

    const auth = new google.auth.JWT({
      email: process.env.GOOGLE_SA_CLIENT_EMAIL,
      key: (process.env.GOOGLE_SA_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    });

    const sheets = google.sheets({ version: "v4", auth });
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.SHEETS_SPREADSHEET_ID,
      range: (process.env.SHEETS_STOCK_TAB || "View_Stock_API") + "!A1:O10",
    });

    console.log("OK, primeras celdas:", res.data.values);
  } catch (e) {
    console.error("FALLÓ:", e.response?.data || e.message || e);
  }
})();
