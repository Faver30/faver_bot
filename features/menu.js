"use strict";

/** -------- LISTA (sections + rows) -------- */
async function sendMainMenuList(sock, jid) {
  const payload = {
    text: "¿En qué te ayudo hoy? Elige una opción:",
    footer: "Faver Bot",
    title: "Menú principal",
    buttonText: "Abrir menú",
    sections: [
      {
        title: "Opciones",
        rows: [
          { title: "Ver precios", rowId: "PRICES", description: "Planes y tarifas" },
          { title: "Comprar",     rowId: "BUY",    description: "Iniciar compra" },
          { title: "Soporte",     rowId: "SUPPORT",description: "Hablar con soporte" },
        ],
      },
    ],
  };

  await sock.sendMessage(jid, payload);
}

/** -------- BOTONES (reply buttons) -------- */
async function sendMainMenuButtons(sock, jid) {
  await sock.sendMessage(jid, {
    text: "Elige una opción:",
    buttons: [
      { buttonId: "BUY",     buttonText: { displayText: "Comprar" },     type: 1 },
      { buttonId: "PRICES",  buttonText: { displayText: "Ver precios" }, type: 1 },
      { buttonId: "SUPPORT", buttonText: { displayText: "Soporte" },     type: 1 },
    ],
    headerType: 1,
  });
}

/** -------- Validador opcional para LIST -------- */
function validateListPayload(payload) {
  const errs = [];
  if (!payload?.text) errs.push("text requerido");
  if (!payload?.buttonText) errs.push("buttonText requerido");
  if (!Array.isArray(payload?.sections) || payload.sections.length === 0)
    errs.push("sections no puede estar vacío");

  if (Array.isArray(payload?.sections)) {
    if (payload.sections.length > 10) errs.push("máximo 10 sections");
    payload.sections.forEach((sec, i) => {
      if (!sec.title) errs.push(`sections[${i}].title requerido`);
      if (!Array.isArray(sec.rows) || sec.rows.length === 0)
        errs.push(`sections[${i}].rows no puede estar vacío`);
      if (Array.isArray(sec.rows) && sec.rows.length > 24)
        errs.push(`sections[${i}] supera 24 rows`);
      const ids = new Set();
      sec.rows?.forEach((r, j) => {
        if (!r.title) errs.push(`row[${i}:${j}].title requerido`);
        if (!r.rowId) errs.push(`row[${i}:${j}].rowId requerido`);
        if (r.rowId && ids.has(r.rowId)) errs.push(`rowId duplicado: ${r.rowId}`);
        ids.add(r.rowId);
      });
    });
  }
  return errs;
}

module.exports = {
  sendMainMenuList,
  sendMainMenuButtons,
  validateListPayload,
};