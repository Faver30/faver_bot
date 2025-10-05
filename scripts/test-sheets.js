(async () => {
  try {
    const SheetsDB = require('../googleSheets'); // ajusta la ruta si tu test está en otra carpeta
    const title = await SheetsDB.pingSheetsTitle();
    console.log('Ping OK. Título del Sheet:', title);

    // Smoke test de precios
    const prices = await SheetsDB.getAllPrices();
    console.log('Precios cargados:', Object.keys(prices).length, 'claves');

    // Smoke test de funciones clave
    if (typeof SheetsDB.upsertAccounts !== 'function') throw new Error('upsertAccounts no existe');
    if (typeof SheetsDB.appendAccounts !== 'function') throw new Error('appendAccounts no existe');

    console.log('Funciones presentes: appendAccounts y upsertAccounts ✅');
  } catch (e) {
    console.error('Fallo en test-sheets:', e);
    process.exit(1);
  }
})();
