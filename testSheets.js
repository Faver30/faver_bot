require('dotenv').config();
const SheetsDB = require('./googleSheets');

(async () => {
  try {
    const counts = await SheetsDB.getStockCounts();
    console.log('Stock por plataforma:', counts);
  } catch (e) {
    console.error('Fallo test:', e);
  }
})();
