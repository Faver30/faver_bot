// googleSheets.js (compat + fallback a googleapis)
// ------------------------------------------------
let GoogleSpreadsheet;
try {
  ({ GoogleSpreadsheet } = require('google-spreadsheet'));
  if (!GoogleSpreadsheet) {
    const gsMod = require('google-spreadsheet');
    GoogleSpreadsheet = gsMod.GoogleSpreadsheet || gsMod;
  }
} catch (_) {
  // si no está instalado, seguimos con googleapis directamente
}

const { google } = require('googleapis');
const { nanoid } = require('nanoid'); // si ya lo tienes arriba, no lo dupliques

// --- Normaliza la private key (Windows-friendly) ---
function normalizePrivateKey(k) {
  if (!k) return '';
  let v = String(k).trim();
  if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
    v = v.slice(1, -1);
  }
  v = v.replace(/\\r/g, '\r').replace(/\\n/g, '\n');
  return v;
}

// --- Credenciales desde .env (ÚNICA versión) ---
function getCreds() {
  const client_email    = process.env.GOOGLE_SA_CLIENT_EMAIL || '';
  const private_key_raw = process.env.GOOGLE_SA_PRIVATE_KEY || '';
  const private_key     = normalizePrivateKey(private_key_raw);

  if (!client_email)  throw new Error('Falta GOOGLE_SA_CLIENT_EMAIL en .env');
  if (!private_key)   throw new Error('Falta GOOGLE_SA_PRIVATE_KEY en .env (o llegó vacío tras normalizar)');
  if (!private_key.includes('BEGIN PRIVATE KEY'))
    throw new Error('GOOGLE_SA_PRIVATE_KEY mal formateada (esperaba "BEGIN PRIVATE KEY")');

  return { client_email, private_key };
}

// --- IDs de pestañas ---
function getSheetIds() {
  return {
    spreadsheetId: process.env.SHEETS_SPREADSHEET_ID,
    stockTab:      process.env.SHEETS_STOCK_TAB     || 'View_Stock_API',
    accountsTab:   process.env.SHEETS_ACCOUNTS_TAB  || 'Cuentas',
    pricesTab:     process.env.SHEETS_PRICES_TAB    || 'Precios',
    lotsTab:       process.env.SHEETS_LOTS_TAB      || 'Precios_Lotes',
    salesTab:      process.env.SHEETS_SALES_TAB     || 'Ventas_Log',
    configTab:     process.env.SHEETS_CONFIG_TAB    || 'Config',
  };
}

// --- Cliente de Google Sheets (ÚNICA versión) ---
async function getSheetsClient() {
  const { client_email, private_key } = getCreds();

  const auth = new google.auth.JWT({
    email:  client_email,
    key:    private_key, // OJO: propiedad 'key'
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  // fuerza obtener token (evita llamada anónima)
  const tokens = await auth.authorize();
  if (!tokens?.access_token) {
    throw new Error('No se pudo obtener access_token (revisa credenciales/API habilitada)');
  }

  return google.sheets({ version: 'v4', auth });
}

// ========== Debug: ping al título del Sheet ==========
async function pingSheetsTitle() {
  const { spreadsheetId } = getSheetIds();
  const sheets = await getSheetsClient();
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  return meta.data.properties?.title || '(sin título)';
}
function keyify(s){
  return String(s || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '')
    .trim();
}


function norm(v) { return String(v ?? '').trim(); }
function lower(v){ return norm(v).toLowerCase(); }


/* ========== Mapeo flexible de encabezados ========== */
const HDR = {
  // Cuentas
  plataforma:  ['plataforma','platform'],
  type:        ['type','tipo'],
  perfil:      ['perfil','profile','profile_id','p'],
  username:    ['username','user','correo','email','usuario'],
  password:    ['password','pass','contra','contraseña'],
  pin:         ['pin'],
  duration:    ['duration','term_months','meses','tiempo','duracion'],
  expires_at:  ['expires_at','vence','fecha_vencimiento'],
  buyer_phone: ['buyer_phone','telefono','teléfono','phone','celular','numero','número'],
  seller:      ['seller','vendedor'],
  extra:       ['extra','notes','nota','perfil_extra','observaciones'],
  estado:      ['estado','status'],
  sold_to:     ['sold_to','vendido_a','cliente','buyer'],
  sold_at:     ['sold_at','fecha_venta','sold date','fecha'],
  order_id:    ['order_id','orden','order','folio'],
  price:       ['price','precio'],
  currency:    ['currency','moneda'],
  code:        ['code','código','codigo'],

  // Ventas_Log
  platform:    ['platform','plataforma'],
  plan:        ['plan'],
};

function indexHeaders(headerRow, keys) {
  const low = (headerRow || []).map(h => lower(h));
  const idx = {};
  for (const k of keys) {
    const aliases = HDR[k] || [k];
    idx[k] = -1;
    for (const a of aliases) {
      const i = low.indexOf(lower(a));
      if (i !== -1) { idx[k] = i; break; }
    }
  }
  return idx;
}

// --- Utilidad: mapa de encabezados en minúsculas -> índice
function mapHeaderIndexes(headerRow) {
  const m = {};
  (headerRow || []).forEach((h, i) => {
    const key = String(h || "").trim().toLowerCase();
    if (key) m[key] = i;
  });
  return m;
}

// --- Helpers para duración/fechas ---
function monthsFromDuration(dur) {
  const m = String(dur || '').trim().toLowerCase().match(/^(\d+)\s*m$/);
  return m ? Math.max(1, parseInt(m[1], 10)) : 1; // por defecto 1 mes si no matchea
}
function addMonthsUTC(d, months) {
  const dt = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  dt.setUTCMonth(dt.getUTCMonth() + months);
  return dt;
}
function formatDateYYYYMMDD(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}
function parseYYYYMMDD(s) {
  const m = String(s || '').trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const y = +m[1], mo = +m[2] - 1, d = +m[3];
  const dt = new Date(Date.UTC(y, mo, d));
  return isNaN(dt.getTime()) ? null : dt;
}
function isExpired(expires_at) {
  const d = parseYYYYMMDD(expires_at);
  if (!d) return false;
  const today = new Date();
  const t = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const e = new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  return e.getTime() <= t.getTime();
}
// username + perfil (perfil vacío => "completa")
function profileKey(username, perfil) {
  const u = lower(username || '');
  const p = lower(perfil || '');
  return `${u}|${p || 'completa'}`;
}

/* ========== googleapis (v4) auth/cliente ========== */
function getJwtAuthRW() {
  const { client_email, private_key } = getCreds();
  if (!client_email || !private_key) throw new Error('Faltan GOOGLE_SA_CLIENT_EMAIL o GOOGLE_SA_PRIVATE_KEY');
  return new google.auth.JWT({
    email: client_email,
    key: private_key,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'], // RW
  });
}
function sheetsV4(auth = getJwtAuthRW()) {
  return google.sheets({ version: 'v4', auth });
}

/* ===================================================================
   LECTURA DE STOCK  (accounts / view / merge controlado por STOCK_SOURCE)
   =================================================================== */
async function countFromAccounts_v4() {
  const { spreadsheetId, accountsTab } = getSheetIds();
  const sc = sheetsV4();
  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${accountsTab}!A1:Z100000`,
  });
  const values = res.data.values || [];
  if (!values.length) return {};

  const head = values[0] || [];
  const idx = indexHeaders(head, ['plataforma','estado','perfil']);
  if (idx.plataforma < 0 || idx.estado < 0) return {};

  const counts = {};
  for (let i = 1; i < values.length; i++) {
    const r = values[i] || [];
    const plat = (r[idx.plataforma] || '').toString().trim().toLowerCase();
    const est  = (r[idx.estado]     || '').toString().trim().toLowerCase();
    if (!plat) continue;
    if (est === '' || est === 'free' || est === 'libre') {
      counts[plat] = (counts[plat] || 0) + 1;
    }
  }
  return counts;
}

async function countFromView_v4() {
  const { spreadsheetId, stockTab } = getSheetIds();
  const sc = sheetsV4();
  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${stockTab}!A1:Z100000`,
  });
  const values = res.data.values || [];
  if (!values.length) return {};

  const head = values[0] || [];
  const lowHead = head.map(h => (h || '').toString().trim().toLowerCase());

  const idxCuenta = indexHeaders(head, ['plataforma','estado']);
  const haveCuenta = idxCuenta.plataforma >= 0 && idxCuenta.estado >= 0;

  const platIdx =
    lowHead.indexOf('plataforma') !== -1 ? lowHead.indexOf('plataforma')
      : (lowHead.indexOf('platform') !== -1 ? lowHead.indexOf('platform') : -1);

  const countCandidates = ['disponibles','libres','free','count','stock','cantidad','available','free_count','libres_count'];
  let countIdx = -1;
  for (const c of countCandidates) {
    const i = lowHead.indexOf(c);
    if (i !== -1) { countIdx = i; break; }
  }
  const haveVista = platIdx >= 0 && countIdx >= 0;

  const counts = {};

  if (haveCuenta) {
    for (let i = 1; i < values.length; i++) {
      const r = values[i] || [];
      const plat = (r[idxCuenta.plataforma] || '').toString().trim().toLowerCase();
      const est  = (r[idxCuenta.estado]     || '').toString().trim().toLowerCase();
      if (!plat) continue;
      if (est === '' || est === 'free' || est === 'libre') {
        counts[plat] = (counts[plat] || 0) + 1;
      }
    }
    return counts;
  }

  if (haveVista) {
    for (let i = 1; i < values.length; i++) {
      const r = values[i] || [];
      const plat = (r[platIdx] || '').toString().trim().toLowerCase();
      if (!plat) continue;
      const numRaw = (r[countIdx] || '').toString().trim();
      const n = Number(numRaw.replace(/[^0-9.-]/g, '')) || 0;
      counts[plat] = n;
    }
    return counts;
  }

  return {};
}

function mergeCounts(a = {}, b = {}) {
  const out = { ...a };
  for (const [k, v] of Object.entries(b)) out[k] = (out[k] || 0) + v;
  return out;
}

// API pública para .stock
async function getStockCounts() {
  const source = (process.env.STOCK_SOURCE || '').toLowerCase(); // accounts|view|merge
  try {
    if (source === 'view') {
      return await countFromView_v4();
    }
    if (source === 'merge') {
      const [acc, view] = await Promise.all([countFromAccounts_v4(), countFromView_v4()]);
      return mergeCounts(acc, view);
    }
    return await countFromAccounts_v4(); // default: accounts
  } catch (e) {
    try { return await countFromView_v4(); }
    catch { return {}; }
  }
}
/* ===================================================================
   STOCK DETALLADO: plataforma -> (completa|perfil) -> duración
   =================================================================== */

// normaliza la duración a "Xm" (si no hay, asume "1m")
function normDuration(dur) {
  const m = String(dur || "").trim().toLowerCase().match(/^(\d+)\s*m$/);
  return m ? `${Math.max(1, parseInt(m[1], 10))}m` : "1m";
}
function rowIsPerfil(typeVal, perfilVal) {
  const t = String(typeVal || "").toLowerCase().trim();
  const p = String(perfilVal || "").trim();
  return t === "perfil" || !!p;
}
function incDetailed(det, plat, kind, dur, n = 1) {
  if (!plat) return;
  det[plat] = det[plat] || {};
  det[plat][kind] = det[plat][kind] || { total: 0, byDuration: {} };
  det[plat][kind].total += n;
  det[plat][kind].byDuration[dur] = (det[plat][kind].byDuration[dur] || 0) + n;
}
function mergeDetailed(a = {}, b = {}) {
  const out = JSON.parse(JSON.stringify(a));
  for (const [plat, kinds] of Object.entries(b || {})) {
    out[plat] = out[plat] || {};
    for (const [kind, obj] of Object.entries(kinds || {})) {
      out[plat][kind] = out[plat][kind] || { total: 0, byDuration: {} };
      out[plat][kind].total += obj.total || 0;
      for (const [dur, cnt] of Object.entries(obj.byDuration || {})) {
        out[plat][kind].byDuration[dur] =
          (out[plat][kind].byDuration[dur] || 0) + (cnt || 0);
      }
    }
  }
  return out;
}

// ----- Cuentas -> detallado
async function detailedFromAccounts_v4() {
  const { spreadsheetId, accountsTab } = getSheetIds();
  const sc = sheetsV4();
  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${accountsTab}!A1:Z100000`,
  });
  const values = res.data.values || [];
  if (!values.length) return {};
  const head = values[0] || [];
  const idx = indexHeaders(head, [
    "plataforma", "type", "perfil", "duration", "estado"
  ]);
  if (idx.plataforma < 0) return {};
  const det = {};
  for (let i = 1; i < values.length; i++) {
    const r = values[i] || [];
    const plat = (r[idx.plataforma] || "").toString().trim().toLowerCase();
    if (!plat) continue;
    const st = idx.estado >= 0 ? (r[idx.estado] || "").toString().trim().toLowerCase() : "";
    if (!(st === "" || st === "free" || st === "libre")) continue;
    const isPerfil = rowIsPerfil(idx.type >= 0 ? r[idx.type] : "", idx.perfil >= 0 ? r[idx.perfil] : "");
    const dur = normDuration(idx.duration >= 0 ? r[idx.duration] : "");
    incDetailed(det, plat, isPerfil ? "perfil" : "completa", dur, 1);
  }
  return det;
}

// ----- Vista -> detallado (si la vista está en filas; si es agregada sin duración, se omite)
async function detailedFromView_v4() {
  const { spreadsheetId, stockTab } = getSheetIds();
  const sc = sheetsV4();
  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${stockTab}!A1:Z100000`,
  });
  const values = res.data.values || [];
  if (!values.length) return {};
  const header = values[0] || [];
  const idxMap = mapHeaderIndexes(header);

  // Necesitamos al menos plataforma + status; si no hay duration, contaremos como 1m.
  const iPlatform = (idxMap["plataforma"] ?? idxMap["platform"] ?? -1);
  const iType     = (idxMap["type"] ?? idxMap["tipo"] ?? -1);
  const iPerfil   = (idxMap["perfil"] ?? idxMap["profile"] ?? -1);
  const iStatus   = (idxMap["estado"] ?? idxMap["status"] ?? -1);
  const iDuration = (idxMap["duration"] ?? -1);

  if (iPlatform < 0) return {};
  const det = {};

  // Heurística: si hay columna de "count" agregada (no por filas), no podemos
  // sacar duración -> lo ignoramos para el detallado.
  const low = header.map(h => String(h || "").trim().toLowerCase());
  const aggregateCols = ["disponibles", "libres", "free", "count", "stock", "cantidad", "available"];
  const hasAggregateCount = aggregateCols.some(c => low.includes(c));
  if (hasAggregateCount && iDuration < 0) {
    // No hay forma de saber duraciones en vista agregada
    return {};
  }

  for (let r = 1; r < values.length; r++) {
    const row = values[r] || [];
    const plat = (row[iPlatform] || "").toString().trim().toLowerCase();
    if (!plat) continue;
    const st = (iStatus >= 0 ? row[iStatus] : "").toString().trim().toLowerCase();
    if (!(st === "" || st === "free" || st === "libre")) continue;

    const isPerfil = rowIsPerfil(iType >= 0 ? row[iType] : "", iPerfil >= 0 ? row[iPerfil] : "");
    const dur = normDuration(iDuration >= 0 ? row[iDuration] : "");
    incDetailed(det, plat, isPerfil ? "perfil" : "completa", dur, 1);
  }
  return det;
}

async function getStockCountsDetailed() {
  const src = (process.env.STOCK_SOURCE || "").toLowerCase(); // accounts|view|merge
  if (src === "view") {
    return await detailedFromView_v4();
  }
  if (src === "merge") {
    const [a, v] = await Promise.all([detailedFromAccounts_v4(), detailedFromView_v4()]);
    return mergeDetailed(a, v);
  }
  // default: accounts
  return await detailedFromAccounts_v4();
}


/* ===================================================================
   DETALLE DE STOCK (Cuentas): total, por tipo y duración
   =================================================================== */
async function getDetailedStock() {
  const { spreadsheetId, accountsTab } = getSheetIds();
  const sc = sheetsV4();

  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${accountsTab}!A1:Z100000`,
  });
  const rows = res.data.values || [];
  if (!rows.length) return {};

  const head = rows[0] || [];
  // índices flexibles
  const idx = indexHeaders(head, [
    'plataforma','type','perfil','duration','estado'
  ]);
  const iPlat = idx.plataforma;
  const iType = idx.type;     // puede ser 'perfil'/'completa' o vacío
  const iPerf = idx.perfil;   // si hay texto => perfil
  const iDur  = idx.duration; // "1m","2m","3m"
  const iEst  = idx.estado;

  const out = {}; // { plat: { total, completa:{'1m':n}, perfil:{'3m':n} } }

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const plat = (row[iPlat] || '').toString().trim().toLowerCase();
    if (!plat) continue;

    const est  = (row[iEst]  || '').toString().trim().toLowerCase();
    if (!(est === '' || est === 'free' || est === 'libre')) continue;

    // tipo: si hay perfil o type == 'perfil' => perfil; si no => completa
    const rowIsPerfil =
      (!!(row[iPerf] || '')) ||
      (String(row[iType] || '').trim().toLowerCase() === 'perfil');
    const tipo = rowIsPerfil ? 'perfil' : 'completa';

    const dur = (row[iDur] || '1m').toString().trim().toLowerCase();

    out[plat] = out[plat] || { total: 0, completa: {}, perfil: {} };
    out[plat].total += 1;
    out[plat][tipo][dur] = (out[plat][tipo][dur] || 0) + 1;
  }

  return out;
}


/* =======================================================================
   VENDER N CUENTAS / MARCAR SOLD (pestaña Cuentas)
   (AHORA con wantPerfil para filtrar perfil/completa)
   ======================================================================= */
// === vender N cuentas desde Cuentas (google-spreadsheet)
async function takeNFreeAndMark_viaGS(
  key, qty, soldToTag, orderIdPrefix, buyerPhone = '', sellerName = 'bot', wantPerfil = false
) {
  const { spreadsheetId, accountsTab } = getSheetIds();
  const doc = new GoogleSpreadsheet(spreadsheetId);
  if (typeof doc.useServiceAccountAuth !== 'function') {
    throw new Error('doc.useServiceAccountAuth no está disponible');
  }
  await doc.useServiceAccountAuth(getCreds());
  await doc.loadInfo();
  const sheet = doc.sheetsByTitle[accountsTab];
  if (!sheet) throw new Error(`No encuentro la pestaña "${accountsTab}"`);

  await sheet.loadHeaderRow();
  const rows = await sheet.getRows();

  const picked = [];
  for (const r of rows) {
    if (picked.length >= qty) break;
    const plat   = lower(r.plataforma || r.platform || '');
    const status = lower(r.estado || r.status || 'free');
    if (plat !== lower(key)) continue;
    if (!(status === 'free' || status === 'libre' || status === '')) continue;

    const rowIsPerfil = !!(r.perfil || r.profile) || lower(r.type || r.tipo || '') === 'perfil';
    if (wantPerfil && !rowIsPerfil) continue;
    if (!wantPerfil && rowIsPerfil) continue;

    const duration = r.duration || '1m';
    const months   = monthsFromDuration(duration);
    const expires  = formatDateYYYYMMDD(addMonthsUTC(new Date(), months));

    picked.push({
      platform: key,
      type: r.type || r.tipo || (rowIsPerfil ? 'perfil' : 'completa'),
      perfil: r.perfil || r.profile || '',
      username: r.username || '',
      password: r.password || '',
      pin: (/^\d{3,8}$/.test(String(r.pin||'')) ? String(r.pin) : ''),
      duration,
      expires_at: r.expires_at || expires,
      extra: r.extra || '',
      price: Number(r.price || 0) || null,
      currency: r.currency || 'MXN',
      code: r.code || '',
    });

    r.estado = 'sold';
    if ('sold_to' in r)     r.sold_to = soldToTag || '';
    if ('sold_at' in r)     r.sold_at = new Date().toLocaleString('es-MX');
    if ('order_id' in r)    r.order_id = `${orderIdPrefix}-${picked.length}`;
    if ('buyer_phone' in r) r.buyer_phone = buyerPhone || '';
    if ('seller' in r)      r.seller = sellerName || 'bot';
    if ('expires_at' in r && !r.expires_at) r.expires_at = expires;
    if ('duration' in r && !r.duration)     r.duration = duration;

    await r.save();
  }

  return picked;
}

// === vender N cuentas desde Cuentas (googleapis v4)
async function takeNFreeAndMark_viaV4(
  key, qty, soldToTag, orderIdPrefix, buyerPhone = '', sellerName = 'bot', wantPerfil = false
) {
  const { spreadsheetId, accountsTab } = getSheetIds();
  const sc = sheetsV4();

  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${accountsTab}!A1:Z100000`,
  });
  const values = res.data.values || [];
  if (!values.length) return [];

  const head = values[0];
  const idx = indexHeaders(head, [
    'plataforma','type','perfil','username','password','pin','duration','expires_at',
    'buyer_phone','seller','extra','estado','sold_to','sold_at','order_id',
    'price','currency','code'
  ]);
  if (idx.plataforma < 0 || idx.username < 0 || idx.password < 0 || idx.estado < 0) {
    throw new Error('Encabezados mínimos faltantes en Cuentas (plataforma, username, password, estado)');
  }

  const updates = [];
  const picked  = [];
  let remaining = qty;

  for (let i = 1; i < values.length && remaining > 0; i++) {
    const r = values[i] || [];
    const plat   = lower(r[idx.plataforma] || '');
    const status = lower(r[idx.estado] || 'free');
    if (plat !== lower(key)) continue;
    if (!(status === 'free' || status === 'libre' || status === '')) continue;

    const rowIsPerfil =
      !!(idx.perfil >= 0 && (r[idx.perfil] || '')) ||
      lower(idx.type >= 0 ? (r[idx.type] || '') : '') === 'perfil';
    if (wantPerfil && !rowIsPerfil) continue;
    if (!wantPerfil && rowIsPerfil) continue;

    const duration = idx.duration >= 0 ? (r[idx.duration] || '1m') : '1m';
    const months   = monthsFromDuration(duration);
    const expires  = formatDateYYYYMMDD(addMonthsUTC(new Date(), months));

    picked.push({
      platform: key,
      type:      rowIsPerfil ? 'perfil' : 'completa',
      perfil:    idx.perfil    >= 0 ? (r[idx.perfil] || '') : '',
      username:  r[idx.username] || '',
      password:  r[idx.password] || '',
      pin:       (idx.pin >= 0 && /^\d{3,8}$/.test(String(r[idx.pin]||''))) ? String(r[idx.pin]) : '',
      duration,
      expires_at: idx.expires_at >= 0 ? (r[idx.expires_at] || expires) : expires,
      extra:     idx.extra     >= 0 ? (r[idx.extra] || '') : '',
      price:     idx.price     >= 0 ? Number(r[idx.price] || 0) || null : null,
      currency:  idx.currency  >= 0 ? (r[idx.currency] || 'MXN') : 'MXN',
      code:      idx.code      >= 0 ? (r[idx.code] || '') : '',
    });

    const rowNum = i + 1;
    const col = c => String.fromCharCode(65 + c);

    updates.push({ range: `${accountsTab}!${col(idx.estado)}${rowNum}`, values: [['sold']] });
    if (idx.sold_to     >= 0) updates.push({ range: `${accountsTab}!${col(idx.sold_to)}${rowNum}`,     values: [[soldToTag || '']] });
    if (idx.sold_at     >= 0) updates.push({ range: `${accountsTab}!${col(idx.sold_at)}${rowNum}`,     values: [[new Date().toLocaleString('es-MX')]] });
    if (idx.order_id    >= 0) updates.push({ range: `${accountsTab}!${col(idx.order_id)}${rowNum}`,    values: [[`${orderIdPrefix}-${picked.length}`]] });
    if (idx.buyer_phone >= 0) updates.push({ range: `${accountsTab}!${col(idx.buyer_phone)}${rowNum}`, values: [[buyerPhone || '']] });
    if (idx.seller      >= 0) updates.push({ range: `${accountsTab}!${col(idx.seller)}${rowNum}`,      values: [[sellerName || 'bot']] });
    if (idx.expires_at  >= 0) updates.push({ range: `${accountsTab}!${col(idx.expires_at)}${rowNum}`,  values: [[values[i][idx.expires_at] || expires]] });
    if (idx.duration    >= 0 && !values[i][idx.duration]) {
      updates.push({ range: `${accountsTab}!${col(idx.duration)}${rowNum}`, values: [[duration]] });
    }

    remaining--;
  }

  if (!picked.length) return [];
  await sc.spreadsheets.values.batchUpdate({
    spreadsheetId,
    requestBody: { valueInputOption: 'RAW', data: updates },
  });
  return picked;
}

// compat que decide GS/V4 + bandera wantPerfil
async function takeNFreeAndMark(
  key, qty, soldToTag, orderIdPrefix, buyerPhone = '', sellerName = 'bot', wantPerfil = false
) {
  try {
    return await takeNFreeAndMark_viaGS(key, qty, soldToTag, orderIdPrefix, buyerPhone, sellerName, wantPerfil);
  } catch (_) {
    return await takeNFreeAndMark_viaV4(key, qty, soldToTag, orderIdPrefix, buyerPhone, sellerName, wantPerfil);
  }
}

// === vender 1 (wrapper de la anterior) — ahora acepta opts.wantPerfil === true/false
async function takeOneFreeAndMark(key, soldToTag, orderId, opts = {}) {
  const orderPrefix = String(orderId || nanoid(6));
  const picked = await takeNFreeAndMark(
    key,
    1,
    soldToTag,
    orderPrefix,
    opts.buyerPhone || '',
    opts.sellerName || 'bot',
    opts.wantPerfil === true // por defecto completa; si true, toma perfil
  );
  return picked[0] || null;
}

/* =======================================================================
   NUEVO: Tomar N desde la VISTA (View_Stock_API) y marcar en la vista
   ======================================================================= */
async function takeNFreeFromViewAndMark(
  key,
  qty,
  buyerTag,
  orderIdPrefix,
  buyerPhone = "",
  sellerName = "bot",
  durationLabel // "1m","2m","3m"... (opcional)
) {
  const { spreadsheetId, stockTab } = getSheetIds();
  const sc = sheetsV4();
  qty = Math.max(1, Number(qty || 1));

  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${stockTab}!A1:Z100000`,
  });
  const rows = res.data.values || [];
  if (!rows.length) return [];

  const header = rows[0];
  const idx = mapHeaderIndexes(header);

  const iPlatform   = (idx["plataforma"] ?? idx["platform"] ?? -1);
  const iType       = (idx["type"] ?? idx["tipo"] ?? -1);
  const iProfile    = (idx["perfil"] ?? idx["profile"] ?? -1);
  const iUsername   = (idx["username"] ?? -1);
  const iPassword   = (idx["password"] ?? -1);
  const iPin        = (idx["pin"] ?? -1);
  const iDuration   = (idx["duration"] ?? -1);
  const iExpires    = (idx["expires_at"] ?? idx["vence"] ?? -1);
  const iExtra      = (idx["extra"] ?? idx["notes"] ?? -1);
  const iPrice      = (idx["price"] ?? idx["precio"] ?? -1);
  const iCurrency   = (idx["currency"] ?? idx["moneda"] ?? -1);
  const iCode       = (idx["code"] ?? idx["código"] ?? idx["codigo"] ?? -1);
  const iStatus     = (idx["status"] ?? idx["estado"] ?? -1);

  const iSoldAt     = (idx["sold_at"] ?? -1);
  const iBuyerPhone = (idx["buyer_phone"] ?? -1);
  const iSeller     = (idx["seller"] ?? -1);
  const iOrder      = (idx["order"] ?? idx["order_id"] ?? -1);
  const iSoldTo     = (idx["buyer"] ?? idx["sold_to"] ?? -1);

  // === FILTRADO POR PLATAFORMA Y ESTADO FREE/LIBRE/'' ===
  let free = [];
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const plat = (iPlatform >= 0 ? row[iPlatform] : '').toString().trim().toLowerCase();
    const st   = (iStatus   >= 0 ? row[iStatus]   : '').toString().trim().toLowerCase();
    if (!plat) continue;
    if (plat !== String(key || "").toLowerCase()) continue;
    if (!(st === "" || st === "free" || st === "libre")) continue;
    free.push({ row, r });
  }

  // === NUEVO: si se pidió duration, filtrar EXACTO por esa duración ===
  const wantDur = (durationLabel || '').toString().trim().toLowerCase();
  if (wantDur && iDuration >= 0) {
    const onlyWanted = free.filter(({ row }) =>
      (row[iDuration] || '').toString().trim().toLowerCase() === wantDur
    );
    free = onlyWanted;
  }

  // (opcional) aquí podrías ordenar por otro criterio si quieres
  if (!free.length) return [];

  const chosen = free.slice(0, qty);
  if (!chosen.length) return [];

  const picked = [];
  const updates = [];

  for (let i = 0; i < chosen.length; i++) {
    const { row, r } = chosen[i];
    const duration = iDuration >= 0 ? (row[iDuration] || "1m") : "1m";
    const months   = monthsFromDuration(duration);
    const expires  = formatDateYYYYMMDD(addMonthsUTC(new Date(), months));

    const out = {
      platform: key,
      type:      iType     >= 0 ? (row[iType] || "") : "",
      perfil:    iProfile  >= 0 ? (row[iProfile] || "") : "",
      username:  iUsername >= 0 ? (row[iUsername] || "") : "",
      password:  iPassword >= 0 ? (row[iPassword] || "") : "",
      pin:       iPin      >= 0 ? (row[iPin] || "") : "",
      duration,
      expires_at: iExpires >= 0 ? (row[iExpires] || expires) : expires,
      extra:     iExtra    >= 0 ? (row[iExtra] || "") : "",
      price:     iPrice    >= 0 ? Number(row[iPrice] || 0) || null : null,
      currency:  iCurrency >= 0 ? (row[iCurrency] || "MXN") : "MXN",
      code:      iCode     >= 0 ? (row[iCode] || "") : "",
    };
    picked.push(out);

    const col = c => String.fromCharCode(65 + c);
    const rowNum = r + 1;

    if (iStatus >= 0) updates.push({ range: `${stockTab}!${col(iStatus)}${rowNum}`, values: [["sold"]] });
    if (iSoldTo >= 0) updates.push({ range: `${stockTab}!${col(iSoldTo)}${rowNum}`, values: [[buyerTag || ""]] });
    if (iSoldAt >= 0) updates.push({ range: `${stockTab}!${col(iSoldAt)}${rowNum}`, values: [[new Date().toLocaleString("es-MX")]] });
    if (iOrder >= 0)  updates.push({ range: `${stockTab}!${col(iOrder)}${rowNum}`, values: [[`${orderIdPrefix}-${i+1}`]] });
    if (iBuyerPhone >= 0) updates.push({ range: `${stockTab}!${col(iBuyerPhone)}${rowNum}`, values: [[buyerPhone || ""]] });
    if (iSeller >= 0) updates.push({ range: `${stockTab}!${col(iSeller)}${rowNum}`, values: [[sellerName || "bot"]] });
    if (iExpires >= 0 && !rows[r][iExpires]) updates.push({ range: `${stockTab}!${col(iExpires)}${rowNum}`, values: [[out.expires_at]] });
    if (iDuration >= 0 && !rows[r][iDuration]) updates.push({ range: `${stockTab}!${col(iDuration)}${rowNum}`, values: [[duration]] });
  }

  if (updates.length) {
    await sc.spreadsheets.values.batchUpdate({
      spreadsheetId,
      requestBody: { valueInputOption: "RAW", data: updates },
    });
  }

  return picked;
}

/* =======================================================================
   LOG DE VENTA (append en pestaña Ventas_Log)
   ======================================================================= */
async function appendSaleLog_viaGS(row) {
  const { spreadsheetId, salesTab } = getSheetIds();
  const doc = new GoogleSpreadsheet(spreadsheetId);
  if (typeof doc.useServiceAccountAuth !== 'function') {
    throw new Error('doc.useServiceAccountAuth no está disponible');
  }
  await doc.useServiceAccountAuth(getCreds());
  await doc.loadInfo();
  const sheet = doc.sheetsByTitle[salesTab];
  if (!sheet) throw new Error(`No encuentro la pestaña "${salesTab}"`);

  await sheet.loadHeaderRow();
  const needed = ['platform','plan','username','password','extra','price','currency','code','sold_to','sold_at','order_id'];
  const have = (sheet.headerValues || []).map(h => lower(h));
  const missing = needed.filter(h => !have.includes(lower(h)));
  if (missing.length) await sheet.setHeaderRow(needed);

  await sheet.addRow({
    platform: row.platform || '',
    plan: row.plan || '',
    username: row.username || '',
    password: row.password || '',
    extra: row.extra || '',
    price: row.price ?? '',
    currency: row.currency || 'MXN',
    code: row.code || '',
    sold_to: row.sold_to || '',
    sold_at: row.sold_at || new Date().toLocaleDateString('es-MX'),
    order_id: row.order_id || '',
  });
}

async function appendSaleLog_viaV4(row) {
  const { spreadsheetId, salesTab } = getSheetIds();
  const sc = sheetsV4();

  const needed = ['platform','plan','username','password','extra','price','currency','code','sold_to','sold_at','order_id'];
  const get = await sc.spreadsheets.values.get({ spreadsheetId, range: `${salesTab}!A1:Z1` });
  let header = (get.data.values && get.data.values[0]) || [];
  if ((header || []).length === 0) {
    await sc.spreadsheets.values.update({
      spreadsheetId,
      range: `${salesTab}!A1:${String.fromCharCode(65 + needed.length - 1)}1`,
      valueInputOption: 'RAW',
      requestBody: { values: [needed] },
    });
  }

  const line = [
    row.platform || '',
    row.plan || '',
    row.username || '',
    row.password || '',
    row.extra || '',
    row.price ?? '',
    row.currency || 'MXN',
    row.code || '',
    row.sold_to || '',
    row.sold_at || new Date().toLocaleDateString('es-MX'),
    row.order_id || '',
  ];

  await sc.spreadsheets.values.append({
    spreadsheetId,
    range: `${salesTab}!A1`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [line] },
  });
}

async function appendSaleLog(row) {
  const { spreadsheetId, salesTab } = getSheetIds();
  const sc = sheetsV4();
  const doc = new GoogleSpreadsheet(spreadsheetId);
  try {
    if (typeof doc.useServiceAccountAuth === 'function') {
      await doc.useServiceAccountAuth(getCreds());
      await doc.loadInfo();
      const sheet = doc.sheetsByTitle[salesTab];
      if (!sheet) throw new Error(`No encuentro la pestaña "${salesTab}"`);

      await sheet.loadHeaderRow();
      const needed = ['platform','plan','username','password','extra','price','currency','code','sold_to','sold_at','order_id'];
      const have = (sheet.headerValues || []).map(h => lower(h));
      const missing = needed.filter(h => !have.includes(lower(h)));
      if (missing.length) await sheet.setHeaderRow(needed);

      await sheet.addRow({
        platform: row.platform || '',
        plan: row.plan || '',
        username: row.username || '',
        password: row.password || '',
        extra: row.extra || '',
        price: row.price ?? '',
        currency: row.currency || 'MXN',
        code: row.code || '',
        sold_to: row.sold_to || '',
        sold_at: row.sold_at || new Date().toLocaleDateString('es-MX'),
        order_id: row.order_id || '',
      });
    } else {
      const needed = ['platform','plan','username','password','extra','price','currency','code','sold_to','sold_at','order_id'];
      const get = await sc.spreadsheets.values.get({ spreadsheetId, range: `${salesTab}!A1:Z1` });
      let header = (get.data.values && get.data.values[0]) || [];
      if ((header || []).length === 0) {
        await sc.spreadsheets.values.update({
          spreadsheetId,
          range: `${salesTab}!A1:${String.fromCharCode(65 + needed.length - 1)}1`,
          valueInputOption: 'RAW',
          requestBody: { values: [needed] },
        });
      }

      const line = [
        row.platform || '',
        row.plan || '',
        row.username || '',
        row.password || '',
        row.extra || '',
        row.price ?? '',
        row.currency || 'MXN',
        row.code || '',
        row.sold_to || '',
        row.sold_at || new Date().toLocaleDateString('es-MX'),
        row.order_id || '',
      ];

      await sc.spreadsheets.values.append({
        spreadsheetId,
        range: `${salesTab}!A1`,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values: [line] },
      });
    }
  } catch (error) {
    console.error('Error al agregar el registro de venta:', error);
  }
}

/* ===================== helpers de desglose (NUEVOS) ===================== */
function normalizeDur(s){
  const m = String(s || "1m").trim().toLowerCase().match(/^(\d+)\s*m$/);
  return m ? `${Math.max(1, parseInt(m[1],10))}m` : "1m";
}
function addBreak(target, plat, kind, dur){
  target[plat] = target[plat] || { total:0, completa:{}, perfil:{} };
  target[plat].total++;
  const bucket = target[plat][kind];
  bucket[dur] = (bucket[dur] || 0) + 1;
}

// === detalle desde Cuentas
async function detailFromAccounts_v4(){
  const { spreadsheetId, accountsTab } = getSheetIds();
  const sc = sheetsV4();
  const res = await sc.spreadsheets.values.get({
    spreadsheetId, range: `${accountsTab}!A1:Z100000`,
  });
  const values = res.data.values || [];
  if(!values.length) return {};
  const head = values[0];
  const idx = indexHeaders(head, ['plataforma','estado','type','perfil','duration']);
  const out = {};
  for(let i=1;i<values.length;i++){
    const r = values[i] || [];
    const plat = (r[idx.plataforma]||"").toString().trim().toLowerCase();
    if(!plat) continue;
    const est = (r[idx.estado]||"").toString().trim().toLowerCase();
    if(!(est===""||est==="free"||est==="libre")) continue;

    const rowIsPerfil = !!(idx.perfil>=0 && (r[idx.perfil]||"")) ||
                        (idx.type>=0 && String(r[idx.type]||"").toLowerCase()==="perfil");
    const kind = rowIsPerfil ? "perfil" : "completa";
    const dur  = normalizeDur(idx.duration>=0 ? (r[idx.duration]||"1m") : "1m");
    addBreak(out, plat, kind, dur);
  }
  return out;
}

// === detalle desde View_Stock_API
async function detailFromView_v4(){
  const { spreadsheetId, stockTab } = getSheetIds();
  const sc = sheetsV4();
  const res = await sc.spreadsheets.values.get({
    spreadsheetId, range: `${stockTab}!A1:Z100000`,
  });
  const rows = res.data.values || [];
  if(!rows.length) return {};
  const head = rows[0];
  const idx = mapHeaderIndexes(head);

  const iPlatform = (idx["plataforma"] ?? idx["platform"] ?? -1);
  const iStatus   = (idx["estado"] ?? idx["status"] ?? -1);
  const iType     = (idx["type"] ?? idx["tipo"] ?? -1);
  const iPerfil   = (idx["perfil"] ?? idx["profile"] ?? -1);
  const iDur      = (idx["duration"] ?? -1);

  const out = {};
  for(let r=1;r<rows.length;r++){
    const row = rows[r] || [];
    const plat = (iPlatform>=0 ? row[iPlatform] : "").toString().trim().toLowerCase();
    if(!plat) continue;
    const st   = (iStatus>=0 ? row[iStatus] : "").toString().trim().toLowerCase();
    if(!(st===""||st==="free"||st==="libre")) continue;

    const rowIsPerfil = (iPerfil>=0 && (row[iPerfil]||"")) ||
                        (iType>=0 && String(row[iType]||"").toLowerCase()==="perfil");
    const kind = rowIsPerfil ? "perfil" : "completa";
    const dur  = normalizeDur(iDur>=0 ? (row[iDur]||"1m") : "1m");
    addBreak(out, plat, kind, dur);
  }
  return out;
}

// Combina detalles según STOCK_SOURCE
function mergeDetail(a={}, b={}){
  const out = JSON.parse(JSON.stringify(a));
  for(const [plat,info] of Object.entries(b)){
    if(!out[plat]) out[plat] = { total:0, completa:{}, perfil:{} };
    out[plat].total += info.total||0;
    for(const [d,c] of Object.entries(info.completa||{})){
      out[plat].completa[d] = (out[plat].completa[d]||0)+c;
    }
    for(const [d,c] of Object.entries(info.perfil||{})){
      out[plat].perfil[d] = (out[plat].perfil[d]||0)+c;
    }
  }
  return out;
}

async function getStockDetail(){
  const src = (process.env.STOCK_SOURCE || "").toLowerCase();
  if(src==="view")  return await detailFromView_v4();
  if(src==="merge"){
    const [a,b] = await Promise.all([detailFromAccounts_v4(), detailFromView_v4()]);
    return mergeDetail(a,b);
  }
  // default: accounts
  return await detailFromAccounts_v4();
}
// ================= PRECIOS EN SHEETS =================
function _normKey(s) {
  return String(s || '').trim().toLowerCase();
}

// Normaliza duración: "base" (o vacío) => "1m"
function _normDur(d) {
  const v = _normKey(d);
  if (!v || v === 'base') return '1m';
  return v;
}

async function ensurePricesHeader(sc, spreadsheetId, tab){
  const need = ['plataforma','tipo','duration','price','currency'];
  const get = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${tab}!A1:Z1`
  }).catch(()=>({ data:{ values:[] }}));
  const header = (get.data.values && get.data.values[0]) || [];
  if (!header.length){
    await sc.spreadsheets.values.update({
      spreadsheetId,
      range: `${tab}!A1:E1`,
      valueInputOption: 'RAW',
      requestBody: { values: [need] }
    });
  }
}

/* ======================= CONFIG (pestaña 'Config') ======================= */
// Estructura recomendada de encabezados en 'Config':
// Key | Value | UpdatedAt | UpdatedBy (opcional)

async function ensureConfigHeader(sc, spreadsheetId, tab) {
  const need = ['Key','Value','UpdatedAt','UpdatedBy'];
  const get = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${tab}!A1:D1`
  }).catch(()=>({ data:{ values:[] }}));
  const header = (get.data.values && get.data.values[0]) || [];
  if (!header.length){
    await sc.spreadsheets.values.update({
      spreadsheetId,
      range: `${tab}!A1:D1`,
      valueInputOption: 'RAW',
      requestBody: { values: [need] }
    });
  }
}

async function sheetsGetConfig(key) {
  const { spreadsheetId, configTab } = getSheetIds();
  const sc = sheetsV4();
  await ensureConfigHeader(sc, spreadsheetId, configTab);

  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${configTab}!A2:B100000`, // Key | Value
  }).catch(()=>({ data:{ values: [] }}));

  const rows = res.data.values || [];
  const K = String(key || '').trim().toLowerCase();
  for (const r of rows) {
    const k = (r[0] || '').toString().trim().toLowerCase();
    if (k === K) return (r[1] || '').toString();
  }
  return null;
}

async function sheetsSetConfig(key, value, opts = {}) {
  const { spreadsheetId, configTab } = getSheetIds();
  const sc = sheetsV4();
  await ensureConfigHeader(sc, spreadsheetId, configTab);

  // lee todo para ver si existe
  const get = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${configTab}!A1:D100000`,
  }).catch(()=>({ data:{ values: [] }}));

  const all = get.data.values || [];
  const header = all[0] || ['Key','Value','UpdatedAt','UpdatedBy'];
  const data = all.slice(1);

  // buscar fila
  const K = String(key || '').trim();
  let foundRow = -1;
  for (let i=0;i<data.length;i++){
    const k = (data[i][0] || '').toString().trim();
    if (k && k.toLowerCase() === K.toLowerCase()) {
      foundRow = i + 2; // 1 header + 1-based
      break;
    }
  }

  const now = new Date().toISOString();
  const by  = opts.updatedBy || (process.env.BOT_NAME || 'bot');

  if (foundRow > 0) {
    await sc.spreadsheets.values.update({
      spreadsheetId,
      range: `${configTab}!A${foundRow}:D${foundRow}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[K, String(value ?? ''), now, by]] }
    });
  } else {
    await sc.spreadsheets.values.append({
      spreadsheetId,
      range: `${configTab}!A1`,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[K, String(value ?? ''), now, by]] }
    });
  }
  return true;
}

/* ======================= Flag: venta_lotes ======================= */
const VENTA_LOTES_KEY = 'venta_lotes';
let _ventaLotesCache = { value: null, ts: 0 };
const _VENTA_TTL_MS = 60_000; // 1 minuto

function _toBool(v) {
  const s = String(v || '').trim().toLowerCase();
  return ['true','1','on','sí','si','encender','activar'].includes(s);
}

async function isVentaLotesOn({ forceRefresh = false } = {}) {
  const now = Date.now();
  if (!forceRefresh && _ventaLotesCache.value !== null && (now - _ventaLotesCache.ts) < _VENTA_TTL_MS) {
    return !!_ventaLotesCache.value;
  }
  const raw = await sheetsGetConfig(VENTA_LOTES_KEY);
  const flag = _toBool(raw);
  _ventaLotesCache = { value: flag, ts: now };
  return flag;
}

async function setVentaLotes(flag, opts = {}) {
  const val = !!flag;
  await sheetsSetConfig(VENTA_LOTES_KEY, String(val), opts);
  _ventaLotesCache = { value: val, ts: Date.now() };
  return val;
}

/**
 * Lee TODOS los precios y devuelve un objeto plano:
 *   key => precio
 * Claves posibles:
 *   "max", "max:1m", "max:2m", "max:perfil", "max:perfil:1m", ...
 * Regla: cualquier "base" en la hoja se interpreta como "1m".
 */
async function getAllPrices(){
  const { spreadsheetId, pricesTab } = getSheetIds();
  const sc  = sheetsV4();
  const tab = pricesTab || (process.env.SHEETS_PRICES_TAB || 'Precios');

  await ensurePricesHeader(sc, spreadsheetId, tab);

  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${tab}!A2:E100000`,
  }).catch(()=>({ data:{ values: [] }}));

  const rows = res.data.values || [];
  const out  = {};

  for (const r of rows){
    const plataformaRaw = (r[0] || '').trim();
    const plataforma    = keyify(plataformaRaw);  // <<--- clave canónica sin espacios
    const tipo          = String(r[1] || 'completa').trim().toLowerCase(); // completa|perfil
    const duration      = _normDur(r[2]);                                    // "1m","2m",...
    const price         = Number(r[3]);

    if (!plataforma || !isFinite(price)) continue;

    if (tipo === 'perfil'){
      if (duration === '1m') out[`${plataforma}:perfil`] = price;  // alias a 1m
      out[`${plataforma}:perfil:${duration}`] = price;
    } else {
      if (duration === '1m') out[`${plataforma}`] = price;         // alias a 1m
      out[`${plataforma}:${duration}`] = price;
    }
  }
  return out;
}

/** Upsert (plataforma+tipo+duration) en la hoja de precios */
async function upsertPrice({ plataforma, tipo='completa', duration='1m', price=0, currency='MXN' }){
  const { spreadsheetId, pricesTab } = getSheetIds();
  const sc  = sheetsV4();
  const tab = pricesTab || (process.env.SHEETS_PRICES_TAB || 'Precios');

  await ensurePricesHeader(sc, spreadsheetId, tab);

  const res = await sc.spreadsheets.values.get({
    spreadsheetId,
    range: `${tab}!A1:E100000`,
  });
  const values = res.data.values || [];
  const header = values[0] || ['plataforma','tipo','duration','price','currency'];

  const wantPlatK = keyify(plataforma);
  const wantTipo  = String(tipo||'completa').trim().toLowerCase();
  const wantDur   = _normDur(duration);

  let foundRow = -1;
  for (let i=1; i<values.length; i++){
    const r = values[i] || [];
    const platK = keyify(r[0] || '');
    const tip   = String(r[1] || 'completa').trim().toLowerCase();
    const dur   = _normDur(r[2]);
    if (platK === wantPlatK && tip === wantTipo && dur === wantDur){
      foundRow = i + 1; // 1-based
      break;
    }
  }

  if (foundRow > 0){
    await sc.spreadsheets.values.update({
      spreadsheetId,
      range: `${tab}!A${foundRow}:E${foundRow}`,
      valueInputOption: 'RAW',
      requestBody: { values: [[plataforma, wantTipo, wantDur, Number(price), currency||'MXN']] }
    });
  } else {
    await sc.spreadsheets.values.append({
      spreadsheetId,
      range: `${tab}!A1`,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [[plataforma, wantTipo, wantDur, Number(price), currency||'MXN']] }
    });
  }
  return true;
}

// ========== FIND PRICE ROW (Precios) — via googleapis ==========
function _san(t){ return String(t ?? '').toLowerCase().trim().replace(/\s+/g,' '); }
function _num(x){
  const s = String(x ?? '').replace(/[^\d.,-]/g,'');
  if (!s) return null;
  const n = (s.includes(',') && !s.includes('.'))
    ? Number(s.replace(/\./g,'').replace(',','.'))
    : Number(s.replace(/,/g,''));
  return Number.isFinite(n) ? n : null;
}

async function findPriceRow({ platform, type, duration }) {
  const { spreadsheetId, pricesTab } = getSheetIds();
  const sheets = await getSheetsClient();

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${pricesTab}!A:E`,  // plataforma | tipo | duracion | precio | moneda
  });

  const rows = res.data.values || [];
  if (!rows.length) return null;

  const [head, ...data] = rows;
  const idx = {
    plataforma: head.findIndex(h => _san(h) === 'plataforma'),
    tipo:       head.findIndex(h => _san(h) === 'tipo'),
    duracion:   head.findIndex(h => _san(h) === 'duracion'),
    precio:     head.findIndex(h => _san(h) === 'precio'),
    moneda:     head.findIndex(h => _san(h) === 'moneda'),
  };
  if (idx.plataforma < 0 || idx.tipo < 0 || idx.duracion < 0) return null;

  const P = _san(platform), T = _san(type), D = _san(duration);
  const hit = data.find(r =>
    _san(r[idx.plataforma]) === P &&
    _san(r[idx.tipo])       === T &&
    _san(r[idx.duracion])   === D
  );
  if (!hit) return null;

  const precio = idx.precio >= 0 ? (_num(hit[idx.precio]) ?? hit[idx.precio]) : '';
  const moneda = idx.moneda >= 0 ? (hit[idx.moneda] || 'MXN') : 'MXN';

  return { platform, type, duration, precio, moneda };
}

// ================== APPEND ACCOUNTS (dinámico por encabezados) ==================
function _sanHead(h){ return String(h||'').toLowerCase().trim(); }
function _pick(obj, ...keys){
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null) return obj[k];
  }
  return '';
}

async function appendAccounts(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return { inserted: 0 };

  const { spreadsheetId, accountsTab } = getSheetIds();
  const sheets = await getSheetsClient();

  // Lee la fila de encabezados (A1)
  const headRes = await sheets.spreadsheets.values.get({
    spreadsheetId, range: `${accountsTab}!1:1`,
  });
  const headers = (headRes.data.values && headRes.data.values[0]) || [];
  if (!headers.length) throw new Error(`La pestaña "${accountsTab}" no tiene encabezados en la fila 1.`);

  const normHeads = headers.map(_sanHead);

  // Para cada objeto, construye la fila respetando el orden de headers
  const values = rows.map(r => {
    return normHeads.map(h => {
      switch (h) {
        case 'plataforma':  return _pick(r, 'platform', 'plataforma');
        case 'type':        return _pick(r, 'type', 'tipo');
        case 'perfil':      return _pick(r, 'perfil', 'profile');
        case 'username':    return _pick(r, 'username', 'user');
        case 'password':    return _pick(r, 'password', 'pass');
        case 'pin':         return _pick(r, 'pin');
        case 'duration':    return _pick(r, 'duration', 'duracion');
        case 'expires_at':  return _pick(r, 'expires_at');
        case 'buyer_phone': return _pick(r, 'buyer_phone');
        case 'seller':      return _pick(r, 'seller', 'sellerName') || 'bot';
        case 'extra':       return _pick(r, 'extra', 'notes');
        case 'estado':      return _pick(r, 'estado', 'status') || 'free';
        case 'sold_to':     return _pick(r, 'sold_to');
        case 'sold_at':     return _pick(r, 'sold_at');
        case 'order_id':    return _pick(r, 'order_id', 'code_id');
        case 'price':       return _pick(r, 'price');
        case 'currency':    return _pick(r, 'currency') || 'MXN';
        case 'code':        return _pick(r, 'code');
        default:            return '';
      }
    });
  });

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${accountsTab}!A:Z`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values },
  });

  return { inserted: values.length };
}

// =============== UPSERT ACCOUNTS (no duplica) ===============
function _sanKey(x){ return String(x ?? '').toLowerCase().trim().replace(/\s+/g,' '); }
function _headIdx(headers) {
  const nh = headers.map(h => String(h||'').toLowerCase().trim());
  const idx = (name) => nh.indexOf(name);
  return {
    plataforma: idx('plataforma'),
    type:       idx('type'),
    perfil:     idx('perfil'),
    username:   idx('username'),
    password:   idx('password'),
    pin:        idx('pin'),
    duration:   idx('duration'),
    expires_at: idx('expires_at'),
    buyer_phone:idx('buyer_phone'),
    seller:     idx('seller'),
    extra:      idx('extra'),
    estado:     idx('estado'),
    sold_to:    idx('sold_to'),
    sold_at:    idx('sold_at'),
    order_id:   idx('order_id'),
    price:      idx('price'),
    currency:   idx('currency'),
    code:       idx('code'),
  };
}

function _buildRowFromHeaders(headers, payload) {
  const h = headers.map(x => String(x||'').toLowerCase().trim());
  const pick = (...k)=> {
    for (const key of k) if (payload[key] !== undefined && payload[key] !== null) return payload[key];
    return '';
  };
  return h.map(col => {
    switch (col) {
      case 'plataforma':  return pick('platform','plataforma');
      case 'type':        return pick('type','tipo');
      case 'perfil':      return pick('perfil','profile');
      case 'username':    return pick('username','user');
      case 'password':    return pick('password','pass');
      case 'pin':         return pick('pin');
      case 'duration':    return pick('duration','duracion');
      case 'expires_at':  return pick('expires_at');
      case 'buyer_phone': return pick('buyer_phone');
      case 'seller':      return pick('seller','sellerName') || 'bot';
      case 'extra':       return pick('extra','notes');
      case 'estado':      return pick('estado','status') || 'free';
      case 'sold_to':     return pick('sold_to');
      case 'sold_at':     return pick('sold_at');
      case 'order_id':    return pick('order_id','code_id');
      case 'price':       return pick('price');
      case 'currency':    return pick('currency') || 'MXN';
      case 'code':        return pick('code');
      default:            return '';
    }
  });
}

/**
 * upsertAccounts(rows):
 * - Clave única: plataforma + type + duration + username (normalizados)
 * - Si existe → UPDATE en esa fila
 * - Si no existe → APPEND al final
 * Devuelve { added, updated, skipped }
 */
async function upsertAccounts(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return { added: 0, updated: 0, skipped: 0 };

  const { spreadsheetId, accountsTab } = getSheetIds();
  const sheets = await getSheetsClient();

  // 1) Leer encabezados + datos existentes
  const getRes = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${accountsTab}!A:Z`,
  });
  const all = getRes.data.values || [];
  const headers = all[0] || [];
  const data = all.slice(1);
  if (!headers.length) throw new Error(`La pestaña "${accountsTab}" no tiene encabezados`);

  const idx = _headIdx(headers);
  if (idx.plataforma < 0 || idx.type < 0 || idx.duration < 0 || idx.username < 0) {
    throw new Error('Faltan columnas clave: plataforma/type/duration/username');
  }

  // 2) Mapear filas existentes por clave
  const map = new Map(); // key -> rowNumber (1-based, incluyendo encabezado)
  data.forEach((r, i) => {
    const key = [
      _sanKey(r[idx.plataforma]),
      _sanKey(r[idx.type]),
      _sanKey(r[idx.duration]),
      _sanKey(r[idx.username]),
    ].join('|');
    const rowNumber = i + 2; // +1 por 0-index y +1 por encabezado
    if (key.trim()) map.set(key, rowNumber);
  });

  // 3) Preparar updates y appends
  const updates = [];
  const appends = [];
  let added = 0, updated = 0;

  for (const p of rows) {
    const key = [
      _sanKey(p.platform ?? p.plataforma),
      _sanKey(p.type ?? p.tipo),
      _sanKey(p.duration ?? p.duracion),
      _sanKey(p.username ?? p.user),
    ].join('|');

    if (map.has(key)) {
      // UPDATE
      const rowNumber = map.get(key);
      const rowValues = _buildRowFromHeaders(headers, p);
      updates.push({
        range: `${accountsTab}!A${rowNumber}:${String.fromCharCode(64 + headers.length)}${rowNumber}`,
        values: [rowValues],
      });
      updated++;
    } else {
      // APPEND
      const rowValues = _buildRowFromHeaders(headers, p);
      appends.push(rowValues);
      added++;
    }
  }

  // 4) Ejecutar en Sheets
  if (updates.length) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId,
      requestBody: {
        valueInputOption: 'USER_ENTERED',
        data: updates,
      },
    });
  }
  if (appends.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: `${accountsTab}!A:Z`,
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: appends },
    });
  }

  return { added, updated, skipped: 0 };
}

// ========== LISTAR PRECIOS POR PLATAFORMA (A:E) ==========
function _sanPlain(s) {
  return String(s ?? '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // quita acentos
    .toLowerCase()
    .replace(/\s+/g, ''); // quita espacios
}


// ===== Helpers de normalización (coinciden con lo ya usado) =====
function _sanWord(s){ return String(s??'').toLowerCase().trim().replace(/\s+/g,' '); }
function _plain(s){
  return String(s ?? '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\s+/g,''); // sin espacios
}

/**
 * Lee la pestaña de Precios y devuelve:
 * {
 *   items: [{ plataforma, tipo, duracion, precio?, moneda? }, ...],
 *   platformsSet: Set([...plataformas únicas]),
 *   mapByPlain: Map(plain(plataforma) -> { name, tipos:Set, duraciones:Set })
 * }
 */
async function getPriceCatalog() {
  const { spreadsheetId, pricesTab } = getSheetIds();
  const sheets = await getSheetsClient();

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${pricesTab}!A:E`,
  });

  const rows = res.data.values || [];
  if (!rows.length) throw new Error('Precios vacío');
  const [head, ...data] = rows;

  const idx = {
    plataforma: head.findIndex(h => _sanWord(h) === 'plataforma'),
    tipo:       head.findIndex(h => _sanWord(h) === 'tipo'),
    duracion:   head.findIndex(h => _sanWord(h) === 'duracion'),
    precio:     head.findIndex(h => _sanWord(h) === 'precio'),
    moneda:     head.findIndex(h => _sanWord(h) === 'moneda'),
  };
  if (idx.plataforma < 0 || idx.tipo < 0 || idx.duracion < 0) {
    throw new Error('Encabezados de Precios inválidos (faltan plataforma/tipo/duracion)');
  }

  const items = [];
  const mapByPlain = new Map();
  for (const r of data) {
    const plataforma = (r[idx.plataforma] ?? '').trim();
    const tipo       = (r[idx.tipo] ?? '').toLowerCase().trim();
    const duracion   = String(r[idx.duracion] ?? '').toLowerCase().trim();
    if (!plataforma || !tipo || !duracion) continue;

    const precio     = idx.precio >= 0 ? (r[idx.precio] ?? '') : '';
    const moneda     = idx.moneda >= 0 ? (r[idx.moneda] ?? 'MXN') : 'MXN';

    items.push({ plataforma, tipo, duracion, precio, moneda });

    const key = _plain(plataforma);
    if (!mapByPlain.has(key)) {
      mapByPlain.set(key, { name: plataforma, tipos: new Set(), duraciones: new Set() });
    }
    const v = mapByPlain.get(key);
    v.tipos.add(tipo);
    v.duraciones.add(duracion);
  }

  const platformsSet = new Set([...mapByPlain.values()].map(v => v.name));
  return { items, platformsSet, mapByPlain };
}

// ¿Existe exactamente (plataforma, tipo, duracion) en Precios?
async function hasPriceCombo({ plataforma, tipo, duracion }) {
  const { items } = await getPriceCatalog();
  const P = _plain(plataforma), T = _sanWord(tipo), D = _sanWord(duracion);
  return items.some(it => _plain(it.plataforma)===P && _sanWord(it.tipo)===T && _sanWord(it.duracion)===D);
}

// Lista todas las filas de una plataforma (para .precio /.stock)
async function listPricesByPlatform(platformInput) {
  const { items, mapByPlain } = await getPriceCatalog();
  const key = _plain(platformInput);
  const match = mapByPlain.get(key);
  if (!match) return [];
  return items.filter(it => _plain(it.plataforma) === key);
}

// Helpers de “MAX”: separa base y variante (p.ej. "MAX ESTANDAR" → base=MAX, variante=ESTANDAR)
// Para otras marcas que no son MAX, base = plataforma, variante=''
function splitMaxVariant(plataforma) {
  const p = _sanWord(plataforma).toUpperCase();
  if (p.startsWith('MAX ')) {
    const rest = plataforma.toUpperCase().slice(4).trim(); // lo que viene después de "MAX "
    return { base: 'MAX', variant: rest || 'GEN' };
  }
  return { base: plataforma, variant: '' };
}

// Precio de lotes: pestaña "Precios_Lotes"
// Encabezados requeridos (fila 1): plataforma | tipo | duracion | lote | precio | moneda
async function findLotPriceRow({ plataforma, tipo, duracion, lote = 10 }) {
  const { spreadsheetId, lotsTab } = getSheetIds();
  const sheets = await getSheetsClient();

  const norm = (s) => String(s ?? '').toLowerCase().trim().replace(/\s+/g, ' ');
  const plain = (s) => String(s ?? '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase().replace(/\s+/g,'');

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${lotsTab}!A:F`,
  }).catch(() => ({ data: { values: [] }}));

  const rows = res.data.values || [];
  if (!rows.length) return null;
  const [head, ...data] = rows;
  const h = head.map(x => String(x||'').toLowerCase().trim());
  const idx = {
    plataforma: h.indexOf('plataforma'),
    tipo:       h.indexOf('tipo'),
    duracion:   h.indexOf('duracion'),
    lote:       h.indexOf('lote'),
    precio:     h.indexOf('precio'),
    moneda:     h.indexOf('moneda'),
  };
  if (idx.plataforma < 0 || idx.tipo < 0 || idx.duracion < 0 || idx.lote < 0 || idx.precio < 0) return null;

  const P = plain(plataforma);
  const T = norm(tipo);
  const D = norm(duracion);
  const L = Number(lote) || 10;

  for (const r of data) {
    const rp = plain(r[idx.plataforma] || '');
    const rt = norm(r[idx.tipo] || '');
    const rd = norm(r[idx.duracion] || '');
    const rl = Number(r[idx.lote] || 0);
    if (rp === P && rt === T && rd === D && rl === L) {
      return {
        plataforma: r[idx.plataforma],
        tipo: r[idx.tipo],
        duracion: r[idx.duracion],
        lote: rl,
        precio: Number(r[idx.precio] || 0) || 0,
        moneda: idx.moneda >= 0 ? (r[idx.moneda] || 'MXN') : 'MXN',
      };
    }
  }
  return null;
}


/* ======================================================================= */
module.exports = {
  getStockCounts,
  getStockCountsDetailed,
  getDetailedStock,
  getStockDetail,
  appendAccounts,
  takeOneFreeAndMark,
  takeNFreeAndMark,
  takeNFreeFromViewAndMark,
  appendSaleLog,
  getAllPrices,   
  upsertPrice,    
  pingSheetsTitle,
  findPriceRow,
  upsertAccounts,
  getPriceCatalog,
  listPricesByPlatform,
  hasPriceCombo,
  splitMaxVariant,
  findLotPriceRow,
  sheetsGetConfig,
  sheetsSetConfig,
  isVentaLotesOn,
  setVentaLotes,
};
