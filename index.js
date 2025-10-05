// ========== B-Bot Secure (Baileys) – SOLO Google Sheets (stock/ventas) ==========
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  jidNormalizedUser,
  downloadContentFromMessage,
  areJidsSameUser,
  
} = require("@whiskeysockets/baileys");
const Pino = require("pino");
const qrcode = require("qrcode-terminal");
const { Boom } = require("@hapi/boom");
const mime = require("mime-types");
const math = require("mathjs");
const fs = require("fs");
const path = require("path");
const { nanoid } = require("nanoid");

require("dotenv").config();

/* === Google Sheets (helper) === */
const SheetsDB = require("./googleSheets");
/* === Menú (listas/botones) === */
const {
  sendMainMenuList,
  sendMainMenuButtons,
  validateListPayload,
} = require('./features/menu');
const { proto, generateWAMessageFromContent } = require("@whiskeysockets/baileys");

/* ========= CONFIG ========= */
const OWNERS = (process.env.OWNERS || "").split(",").map(s => s.trim()).filter(Boolean);
const ANTILINK_DEFAULT = (process.env.ANTILINK_DEFAULT || "false") === "true";
const WELCOME_DEFAULT  = (process.env.WELCOME_DEFAULT  || "true") === "true";
const GOODBYE_DEFAULT  = (process.env.GOODBYE_DEFAULT  || "true") === "true";
const RATE_LIMIT_PER_MINUTE = Number(process.env.RATE_LIMIT_PER_MINUTE || 12);
const TAGALL_MAX = Number(process.env.TAGALL_MAX || 80);
const DEFAULT_CC = process.env.DEFAULT_CC || "57";
const FANTASMA_DAYS = Number(process.env.FANTASMA_DAYS || 7);

/* === Grupos especiales === */
const CONTROL_GROUP_NAME = (process.env.CONTROL_GROUP_NAME || "").trim();
const CONTROL_GROUP_IDS  = (process.env.CONTROL_GROUP_IDS  || "").split(",").map(s => s.trim()).filter(Boolean);

const SUPPORT_GROUP_NAME = (process.env.SUPPORT_GROUP_NAME || "").trim();
const SUPPORT_GROUP_IDS  = (process.env.SUPPORT_GROUP_IDS  || "").split(",").map(s => s.trim()).filter(Boolean);
const CONTROL_ROOM_JID = process.env.CONTROL_ROOM_JID || '';

function isInControlRoom(info) {
  if (!CONTROL_ROOM_JID) return true;
  const chatId = info?.chat || info?.remoteJid || info?.jid || '';
  return chatId === CONTROL_ROOM_JID;
}
function parseOnOff(argRaw) {
  const v = String(argRaw || '').trim().toLowerCase();
  const ON  = ['on','encender','activar','true','1','sí','si'];
  const OFF = ['off','apagar','desactivar','false','0','no'];
  if (ON.includes(v))  return true;
  if (OFF.includes(v)) return false;
  return null;
}
/* === Dirs === */
const DATA_DIR  = path.join(__dirname, "data");
const QR_DIR    = path.join(__dirname, "qr_code");
const MEDIA_DIR = path.join(DATA_DIR, "media");
if (!fs.existsSync(DATA_DIR))  fs.mkdirSync(DATA_DIR,  { recursive: true });
if (!fs.existsSync(QR_DIR))    fs.mkdirSync(QR_DIR,    { recursive: true });
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

/* === Resellers store === */
const RESELLERS_FILE = path.join(DATA_DIR, 'resellers.json');

// Asegura el archivo en el primer arranque
(function ensureResellersFile() {
  try {
    if (!fs.existsSync(RESELLERS_FILE)) {
      fs.writeFileSync(RESELLERS_FILE, JSON.stringify({ resellers: [] }, null, 2));
    }
  } catch (e) {
    console.error('Error creando data/resellers.json:', e);
  }
})();

function loadResellers() {
  try {
    const raw = fs.readFileSync(RESELLERS_FILE, 'utf8');
    const json = JSON.parse(raw || '{}');
    return Array.isArray(json.resellers) ? json.resellers : [];
  } catch (e) {
    console.error('Error cargando resellers.json:', e);
    return [];
  }
}

function saveResellers(list) {
  try {
    fs.writeFileSync(RESELLERS_FILE, JSON.stringify({ resellers: list }, null, 2));
    return true;
  } catch (e) {
    console.error('Error guardando resellers.json:', e);
    return false;
  }
}


function isControlChat(jid, meta) {
  // por ID explícito
  const idList = new Set((CONTROL_GROUP_IDS || []).map(String));
  if (idList.has(jid)) return true;

  // por nombre (si definiste CONTROL_GROUP_NAME)
  const name = (meta?.subject || '').trim();
  if (CONTROL_GROUP_NAME && name && name.toLowerCase() === CONTROL_GROUP_NAME.toLowerCase()) {
    return true;
  }
  return false;
}

function isBotAdminInMeta(meta, botJid) {
  const me = jidNormalizedUser(botJid || '');
  const ps = meta?.participants || [];
  return ps.some(p => {
    const pid = jidNormalizedUser(p?.id || '');
    const admin = (p?.admin === 'admin' || p?.admin === 'superadmin');
    return pid === me && admin;
  });
}

// ——— Trae SOLO grupos donde el bot es admin (excluye Sala de Control)
async function fetchAllAdminGroups(sock) {
  const all = await sock.groupFetchAllParticipating(); // { jid: metaParcial }
  const out = [];

  for (const [jid, metaParcial] of Object.entries(all)) {
    if (isControlChat(jid, metaParcial)) continue; // no Sala de Control

    let meta;
    try {
      meta = await sock.groupMetadata(jid); // metadata completa
    } catch {
      meta = metaParcial;
    }

    if (isBotAdminInGroup(meta, sock)) {
      out.push({ jid, meta });
    }
  }

  return out;
}

// 2) Nombre de grupo (para logs bonitos)
async function groupNameOrJid(sock, jid) {
  if (!jid.endsWith('@g.us')) return jid;
  try {
    const meta = await sock.groupMetadata(jid);
    return meta?.subject || jid;
  } catch { return jid; }
}

// 3) Primer JID de la sala de control (útil para mandar confirmaciones)
function getPrimaryControlJid() {
  return (CONTROL_GROUP_IDS && CONTROL_GROUP_IDS[0]) || '';
}

// ——— Normaliza y detecta si soy admin en un meta COMPLETO
function isBotAdminInGroup(meta, sock){
  const me = jidNormalizedUser(sock?.user?.id || sock?.user?.jid || '');
  const parts = meta?.participants || [];
  return parts.some(p => {
    const pid   = jidNormalizedUser(p?.id || p?.jid || '');
    const admin = (p?.admin === 'admin' || p?.admin === 'superadmin' || p?.admin === true);
    return admin && pid === me;
  });
}
async function getAdminGroups(sock){
  try {
    const freshMs = 10 * 60 * 1000;
    if (fs.existsSync(ADMIN_GROUPS_CACHE)) {
      const cached = JSON.parse(fs.readFileSync(ADMIN_GROUPS_CACHE, 'utf8'));
      if (cached?.at && Date.now() - cached.at < freshMs && Array.isArray(cached.jids)) return cached.jids;
    }

    const all = await sock.groupFetchAllParticipating(); // { jid: metaParcial }
    const mine = [];

    for (const [jid, metaParcial] of Object.entries(all || {})) {
      // excluir sala de control
      const subj = (metaParcial?.subject || '').trim().toLowerCase();
      if ((CONTROL_GROUP_IDS || []).includes(jid)) continue;
      if (CONTROL_GROUP_NAME && subj === CONTROL_GROUP_NAME.toLowerCase()) continue;

      let meta;
      try { meta = await sock.groupMetadata(jid); } catch { meta = metaParcial; }
      if (isBotAdminInGroup(meta, sock)) mine.push(jid);
    }

    fs.writeFileSync(ADMIN_GROUPS_CACHE, JSON.stringify({ at: Date.now(), jids: mine }, null, 2));
    return mine;
  } catch (e) {
    console.error('getAdminGroups error:', e?.message || e);
    return [];
  }
}
// 6) Envío masivo con pausa (para .notify desde control)
async function broadcastNotify(sock, jids, payload, quotedInfo) {
  let ok = 0, fail = 0;
  for (const jid of jids) {
    try {
      await sock.sendMessage(jid, payload, quotedInfo ? { quoted: quotedInfo } : undefined);
      ok++;
      await new Promise(r => setTimeout(r, 800)); // anti-rate limit
    } catch (e) {
      fail++;
      console.error('notify->', jid, e?.message || e);
    }
  }
  return { ok, fail, total: jids.length };
}

async function getMentionableParticipants(sock, gid) {
  try {
    const meta = await sock.groupMetadata(gid);
    const me   = jidNormalizedUser(sock.user?.id || sock.user?.jid || "");
    return (meta?.participants || [])
      .map(p => jidNormalizedUser(p.id))
      .filter(j => j && j !== me); // excluye al bot
  } catch {
    return [];
  }
}


/* Descargar media de mensaje o del citado (para .notify) */
async function maybeDownloadQuotedMedia(info) {
  try {
    // 1) media citada
    const q = info?.message?.extendedTextMessage?.contextInfo?.quotedMessage;
    if (q?.imageMessage) {
      const stream = await downloadContentFromMessage(q.imageMessage, 'image');
      const chunks = []; for await (const c of stream) chunks.push(c);
      return { type: 'image', data: Buffer.concat(chunks), mimetype: q.imageMessage.mimetype || 'image/jpeg' };
    }
    if (q?.videoMessage) {
      const stream = await downloadContentFromMessage(q.videoMessage, 'video');
      const chunks = []; for await (const c of stream) chunks.push(c);
      return { type: 'video', data: Buffer.concat(chunks), mimetype: q.videoMessage.mimetype || 'video/mp4' };
    }
  } catch {}

  try {
    // 2) media en el propio mensaje
    const m = info?.message;
    if (m?.imageMessage) {
      const stream = await downloadContentFromMessage(m.imageMessage, 'image');
      const chunks = []; for await (const c of stream) chunks.push(c);
      return { type: 'image', data: Buffer.concat(chunks), mimetype: m.imageMessage.mimetype || 'image/jpeg' };
    }
    if (m?.videoMessage) {
      const stream = await downloadContentFromMessage(m.videoMessage, 'video');
      const chunks = []; for await (const c of stream) chunks.push(c);
      return { type: 'video', data: Buffer.concat(chunks), mimetype: m.videoMessage.mimetype || 'video/mp4' };
    }
  } catch {}

  return null;
}

function bareNum(j) {
  return String(j || '')
    .split('@')[0]   // antes de @
    .split(':')[0]   // sin sufijo de device
    .replace(/\D/g, ''); // solo dígitos
}


function isReseller(jid) {
  try {
    const id = jidNormalizedUser(jid);
    const rs = loadResellers();
    return rs.map(j => jidNormalizedUser(j)).includes(id);
  } catch {
    return false;
  }
}

// === Helpers de grupos ===
async function getAllGroupsExceptControl(sock){
  try{
    const all = await sock.groupFetchAllParticipating(); // { jid: metaParcial }
    const out = [];
    for (const [jid, meta] of Object.entries(all || {})){
      const subj = (meta?.subject || '').trim().toLowerCase();

      // Excluir sala de control por ID o por nombre
      let isCtrl = false;
      if ((CONTROL_GROUP_IDS || []).includes(jid)) isCtrl = true;
      else if (CONTROL_GROUP_NAME && subj === CONTROL_GROUP_NAME.toLowerCase()) isCtrl = true;

      if (!isCtrl) out.push(jid);
    }
    return out;
  }catch(e){
    console.error('getAllGroupsExceptControl:', e?.message || e);
    return [];
  }
}


function onlyDigits(s=''){ return String(s).replace(/\D/g,''); }
function isMePidLoose(pid, sock){
  const raw = String(sock?.user?.id || '');
  const phoneDigits = onlyDigits(raw.split('@')[0]);   // 57315...
  const pidDigits   = onlyDigits(String(pid||'').split('@')[0]); // para @lid será larguísimo

  // 1) si coincide exacto el JID (raro, pero puede pasar)
  if (String(pid) === raw) return true;

  // 2) si el @lid contiene el número del bot (a veces viene incrustado)
  if (phoneDigits && pidDigits && pidDigits.includes(phoneDigits)) return true;

  // 3) si algún “device suffix” coincide: 573...:10@s.whatsapp.net vs 573...@s.whatsapp.net
  const base = phoneDigits.replace(/:\d+$/,'');
  if (base && pidDigits && (pidDigits.includes(base) || base.includes(pidDigits))) return true;

  return false;
}
function myIds(sock){
  const raw = sock?.user?.id || sock?.user?.jid || '';
  const num = onlyDigits(raw);           // 5731538155510
  return {
    raw,                                 // 5731538155510:10@s.whatsapp.net (ejemplo)
    num,                                 // 5731538155510
    lid: `${num}@lid`,                   // 5731538155510@lid
    wa:  `${num}@s.whatsapp.net`,        // 5731538155510@s.whatsapp.net
  };
}

function isMePid(pid='', sock){
  const me = myIds(sock);
  const d  = onlyDigits(pid);
  return pid === me.lid || pid === me.wa || d === me.num;
}


// Permiso combinado para .cargarsaldo (OWNER o RESELLER)
function canUseSaldo(jid) {
  return isOwner(jid) || isReseller(jid);
}

// Un reseller solo puede tocar saldo de usuarios "normales" (ni owners ni resellers)
function canTouchSaldo(actorJid, targetJid) {
  // Owners pueden tocar a cualquiera
  if (isOwner(actorJid)) return true;

  // Si el actor NO es reseller, no puede (ya filtra canUseSaldo antes)
  if (!isReseller(actorJid)) return false;

  // Si el destino es owner o reseller → NO permitido
  if (isOwner(targetJid) || isReseller(targetJid)) return false;

  // Caso normal: reseller → usuario común
  return true;
}

/* ========= Rutas ========= */
const antilinkPath   = path.join(DATA_DIR, "antilink.json");
const welcomePath    = path.join(DATA_DIR, "welcome.json");
const aliasesPath    = path.join(DATA_DIR, "aliases.json");
const productsPath   = path.join(DATA_DIR, "products.json");     // catálogos por GRUPO (texto/imagen) + global
const helpPath       = path.join(DATA_DIR, "help.json");         // ayudas por GRUPO (texto)
const mutedPath      = path.join(DATA_DIR, "muted.json");
const activityPath   = path.join(DATA_DIR, "activity.json");
const disabledPath   = path.join(DATA_DIR, "disabled_chats.json"); // { chatId: true }
const goodbyePath    = path.join(DATA_DIR, "goodbye.json");        // on/off por grupo para despedida

/* === GLOBAL STORE (precios/ventas/saldo) === */
const balancesPath   = path.join(DATA_DIR, "balances.json");     // { userJid: number }
const pricesPath     = path.join(DATA_DIR, "prices.json");       // { storageKey: { key: number } }
const salesPath      = path.join(DATA_DIR, "sales.json");        // [ {...} ]

/* === SOPORTE === */
const ticketsPath    = path.join(DATA_DIR, "support_tickets.json"); // { [supportMsgId]: { ... , answered, remindAt, reminded } }

/* === PAGOS === */
const paymentsPath   = path.join(DATA_DIR, "payments.json");        // { streaming:{text?,image?}, cine:{...} }
const payAssignPath  = path.join(DATA_DIR, "payment_assign.json");  // { groupJid: "streaming"|"cine" }

/* ========= Helpers JSON ========= */
const readJSON  = (p, fb = {}) => { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return fb; } };
const writeJSON = (p, o) => fs.writeFileSync(p, JSON.stringify(o, null, 2));

const antilinkDB = readJSON(antilinkPath, {});
const ADMIN_GROUPS_CACHE = path.join(DATA_DIR, "admin_groups_cache.json");
const welcomeDB  = readJSON(welcomePath, {});
const aliasesConf= readJSON(aliasesPath, {});
const productsDB = readJSON(productsPath, {}); 
const helpDB     = readJSON(helpPath,     {}); 
const mutedDB    = readJSON(mutedPath,   {});
const activityDB = readJSON(activityPath,  {});
const balancesDB = readJSON(balancesPath, {});
const pricesDB   = readJSON(pricesPath,  {});
const salesDB    = readJSON(salesPath,   []);
const ticketsDB  = readJSON(ticketsPath, {});
const disabledDB = readJSON(disabledPath, {});
const paymentsDB = readJSON(paymentsPath, { streaming:null, cine:null });
const payAssign  = readJSON(payAssignPath, {});
const goodbyeDB  = readJSON(goodbyePath, {}); 

// === FEATURES / FLAGS (panel) ===
const featuresPath = path.join(DATA_DIR, "features.json");
const featuresDB = readJSON(featuresPath, { ventalotes: false }); // por defecto OFF
function saveFeatures() { writeJSON(featuresPath, featuresDB); }
function isVentaLotesOn() { return !!featuresDB.ventalotes; }
function setVentaLotes(on) { featuresDB.ventalotes = !!on; saveFeatures(); }


/* ========= Imagen bienvenida / menú / despedida ========= */
function resolveImagePath(val, fallback) {
  const v = val || fallback || "";
  if (!v) return "";
  if (/^https?:\/\//i.test(v)) return v;
  const abs = path.isAbsolute(v) ? v : path.join(__dirname, v);
  return fs.existsSync(abs) ? abs : "";
}
const DEFAULT_WELCOME_IMAGE = "C:\\bot faver\\b-bot-secure\\media\\bienvenida.jpeg";
const DEFAULT_MENU_IMAGE    = "C:\\bot faver\\b-bot-secure\\media\\menu.jpeg";

const WELCOME_IMAGE   = resolveImagePath(process.env.WELCOME_IMAGE, DEFAULT_WELCOME_IMAGE);
const GOODBYE_IMAGE   = resolveImagePath(process.env.GOODBYE_IMAGE, "");
const MENU_IMAGE      = resolveImagePath(process.env.MENU_IMAGE, DEFAULT_MENU_IMAGE);
const MENU_TITLE      = process.env.MENU_TITLE || "📜 MENÚ DEL BOT";
const BOT_IMAGE       = resolveImagePath(process.env.BOT_IMAGE, "");
const BOT_VIDEO       = resolveImagePath(process.env.BOT_VIDEO, "");
const CATALOGO_NOTAS  = process.env.CATALOGO_NOTAS || "";


/* ========= Aliases ========= */
const ALIASES = Object.entries(aliasesConf).reduce((acc, [canon, list]) => {
  acc[canon] = canon;
  (list || []).forEach(a => acc[a.toLowerCase()] = canon);
  return acc;
}, {});

/* ========= Claves de catálogos ========= */
const TEMP_KEYS = {
  // ===================== Notas =====================
  horario:       ["horario"],
  cuentas:       ["cuentas"],

  // ======= Otros catálogos (sin stock) =======
  codigos:       ["codigos","códigos","codigo","código"],
  seguros:       ["seguros","seguro"],
  forma:         ["forma","formas"],
  justificantes: ["justificantes","justificante","similar","justificante similar"],
  certificados:  ["certificados","certificado"],
  reportes:      ["reportes","reporte"],
  cita:          ["cita","citas"],
  pedido:        ["pedido","pedidos"],
  vuelos:        ["vuelos","vuelo"],
  libros:        ["libros","libro"],
  ado:           ["ado"],
  comisionista:  ["comisionista","comisionistas"],
  peliculas:     ["peliculas","películas"],
  cinemex:       ["cinemex"],
  juegos:        ["juegos"],
  recargas:      ["recargas","recarga"],
  actas:         ["actas","acta"],
  estafa:        ["estafa"],
  imss:          ["imss"],
  vigencia:      ["vigencia"],
  insentivos:    ["insentivos","incentivos"], // se mantienen ambas grafías
  cinepolis:     ["cinepolis","cinépolis","link cine","linkcine"],
  rfc:           ["rfc"],
  rebote:        ["rebote"],
  saldoafavor:   ["saldoafavor","saldo a favor"],
  predial:       ["predial"],
  prestamo:      ["prestamo","préstamo"],
  tramites:      ["tramites","trámites"],
  garantia:      ["garantia","garantía"],
  boletos:       ["boletos","boleto"],
  prohibido:     ["prohibido"],
  tenencia:      ["tenencia"],
  packpromo:     ["packpromo","pack promo"],
  tarjeta:       ["tarjeta"],

  // ======= Streaming / Digital (con stock) =======
  // Generales
  netflix:             ["netflix"],
  max:                 ["max","hbomax","hbo max"],
  disney:              ["disney"],            // genérico (si no distingues plan)
  spotify:             ["spotify"],
  primevideo:          ["prime video","amazon prime","prime","amazon"],
  paramount:           ["paramount","paramount+","p+"],
  vixplus:             ["vix plus","vix+","vix"],
  crunchyroll:         ["crunchyroll","crunchy","crunchyrol","crunchy rol"],
  claro_video:         ["claro video","clarovideo"],
  deezer_plus:         ["deezer plus","deezer"],
  youtube_premium:     ["youtube premium","yt premium","youtube"],
  youtube_familiar_mx: ["youtube premium familiar ip mx","youtube familiar mx","yt familiar mx"],
  canva:               ["canva","canva pro","canva+","canva plus"],
  canva_equipos:       ["canva pro equipos","canva teams"],
  canva_edu:           ["canva edu","canva education","canva escuela"],
  duolingo_super:      ["duolingo super"],
  duolingo_super_familiar: ["duolingo super familiar"],
  pornhub_premium:     ["pornhub premium","pornhub"],
  iptv:                ["iptv"],
  flujo_tv:            ["flujo tv","flujotv"],
  magis_tv:            ["magis tv","magistv"],
  mubi:                ["mubi"],
  viki:                ["viki"],
  chatgpt:             ["chat gpt","chatgpt","gpt"],
  capcut:              ["capcut","cap cut"],

  // Variantes Disney (si manejas planes separados)
  disney_premium_espn:    ["disney premium con espn","disney con espn"],
  disney_premium_sinespn: ["disney premium sin espn","disney sin espn"],
  disney_estandar_espn:   ["disney estandar con espn","disney estándar con espn"],

  // Variantes MAX (si te sirven)
  max_basico:            ["max basico","max básico"],
  max_estandar:          ["max estandar","max estándar"],
  max_platino:           ["max platino","max premium"],

  // Perfiles / extras
  extra:                 ["extra","extranetflix","netflixextra","perfilprivado","perfil privado"],

  // Utilitarios / links
  linkvix:               ["linkvix","link vix"],
  linkdeezer:            ["linkdeezer","link deezer"],

  // ======= Licencias / Software =======
  windows11_home_oem:        ["windows 11 home oem key"],
  windows11_home_retail:     ["windows 11 home retail key"],
  windows11_pro_retail:      ["windows 11 pro retail key"],
  windows11_pro_oem:         ["windows 11 pro oem key"],
  windows10_home_retail:     ["microsoft windows 10 home retail key"],
  windows10_home_oem:        ["microsoft windows 10 home oem key"],
  windows10_pro_oem:         ["microsoft windows 10 pro oem key"],
  windows10_pro_retail:      ["microsoft windows 10 pro retail key"],
  windows_server_2022_std:   ["windows server 2022 standard"],
  windows_server_2022_dc:    ["windows server datacenter 2022"],
  office365_proplus_lifetime:["microsoft office 365 pro plus - lifetime account - 5 devices"],
};

const CANON_KEYS = Object.keys(TEMP_KEYS);

// Streaming con stock (orden A–Z) – SOLO para mostrar listas/menú
const STREAMING_KEYS = [
  "canvaedu","canvapro","capcut","chatgpt","clarovideo","crunchy","deezer","disney",
  "duolingo","extra","flujotv","iptv","linkdeezer","linkvix","max","mubi","netflix",
  "office","paramount","pornhub","prime","spotify","viki","vix","windows","youtube"
].sort();

const NOTE_ONLY_KEYS = ["cuentas", "horario"];

/* ========= Utils ========= */
const PREFIXES = (process.env.PREFIXES || process.env.PREFIX || ".").split(",").map(s => s.trim()).filter(Boolean);
const whichPrefix = (t) => PREFIXES.find(p => t && t.startsWith(p));
const isCmd   = (t) => Boolean(whichPrefix(t));
const cmdName = (t) => { const p = whichPrefix(t) || ""; return (t.slice(p.length).trim().split(/\s+/)[0] || "").toLowerCase(); };
const cmdArgs = (t) => { const p = whichPrefix(t) || ""; return t.slice(p.length).trim().split(/\s+/).slice(1); };

// Devuelve { duration: '1m' | '2m' | ..., rest: argsSinEsaDuracion }
function extractDurationFromArgs(args = []) {
  let duration = "";
  const out = [];
  for (const a of args) {
    const tok = String(a).trim().toLowerCase();
    if (!duration && /^\d+\s*m$/.test(tok)) {
      duration = tok.replace(/\s+/g, ''); // "1 m" => "1m"
    } else {
      out.push(a);
    }
  }
  return { duration, rest: out };
}
// ===== Conversational UI (memoria simple por usuario) =====
const convo = new Map(); // { [userJid]: { mode, step, data: {...} } }

const BTN = {
  BUY:        'FLOW_BUY',
  PRICES:     'FLOW_PRICES',
  SUPPORT:    'FLOW_SUPPORT',
  AGENT:      'FLOW_AGENT',

  TYPE_COMPLETA: 'TYPE_COMPLETA',
  TYPE_PERFIL:   'TYPE_PERFIL',
  CONFIRM_BUY:   'CONFIRM_BUY',
  CANCEL:        'FLOW_CANCEL',
};
// ===== Helpers de botones / menú =====
function getBtnId(info) {
  const m = info?.message || {};
  // soporta respuestas de buttons, templateButtons y listas
  return (
    m?.buttonsResponseMessage?.selectedButtonId ||
    m?.templateButtonReplyMessage?.selectedId ||
    m?.listResponseMessage?.singleSelectReply?.selectedRowId ||
    ""
  );
}

function resetConvo(userJid) {
  convo.delete(userJid);
}

// Menú de texto (sin botones/listas)
async function showHomeTextMenu(sock, chatId, userJid) {
  const at = `@${String(userJid).split("@")[0]}`;
  const txt =
`Hola ${at} 👋
¿En qué te ayudo hoy? Escribe una opción:

• *comprar*
• *precios*
• *soporte*`;

  await sock.sendMessage(chatId, { text: txt, mentions: [userJid] });
  // estado base del flujo
  convo.set(userJid, { mode: "home", step: 0, data: {} });
}

// Envía botones (quick replies) usando templateMessage (forzado)
async function sendButtonsForce(sock, jid) {
  const msg = proto.Message.fromObject({
    templateMessage: {
      hydratedTemplate: {
        hydratedContentText: 'Elige una opción:',
        hydratedButtons: [
          { quickReplyButton: { displayText: 'Comprar',     id: BTN.BUY } },
          { quickReplyButton: { displayText: 'Ver precios', id: BTN.PRICES } },
          { quickReplyButton: { displayText: 'Soporte',     id: BTN.SUPPORT } },
        ],
      },
    },
  });
  const wm = generateWAMessageFromContent(jid, msg, { userJid: sock.user?.id });
  await sock.relayMessage(jid, wm.message, { messageId: wm.key.id });
}

const buckets = new Map();
function allow(jid){ const now=Date.now(); const r=buckets.get(jid)||{count:0,ts:now}; if(now-r.ts>60_000){r.count=0;r.ts=now;} if(r.count>=RATE_LIMIT_PER_MINUTE)return false; r.count++; buckets.set(jid,r); return true; }
function extractPlainText(msg){
  return (msg?.message?.conversation)
      || (msg?.message?.extendedTextMessage?.text)
      || (msg?.message?.imageMessage?.caption)
      || (msg?.message?.videoMessage?.caption)
      || "";
}
function getQuotedText(info){
  const ctx = info.message?.extendedTextMessage?.contextInfo;
  if (!ctx || !ctx.quotedMessage) return "";
  return extractPlainText({ message: ctx.quotedMessage });
}
function getQuotedObj(info){ return info.message?.extendedTextMessage?.contextInfo?.quotedMessage || null; }
function commandTailPreserving(text){ const p=whichPrefix(text)||""; const after=text.slice(p.length); const first=after.split(/\s+/)[0]||""; return after.slice(first.length).replace(/^[ \t]/,""); }
function getMentionedOrQuoted(info){ const ctx=info.message?.extendedTextMessage?.contextInfo||{}; const mentioned=ctx.mentionedJid||[]; const quoted=ctx.participant?[ctx.participant]:[]; return mentioned.length?mentioned:quoted; }
function stripAccents(s){ return (s||"").normalize("NFD").replace(/[\u0300-\u036f]/g,""); }
function keyify(s){ return stripAccents(String(s||"").toLowerCase()).replace(/[^a-z0-9]+/g,""); }
function matchKey(raw){
  const r=keyify(raw);
  for (const canon of CANON_KEYS){
    if (r===keyify(canon)) return canon;
    for (const a of TEMP_KEYS[canon]){
      if (r===keyify(a)) return canon;
    }
  }
  return null;
}

/* === OWNER helper: soporta @s.whatsapp.net, @lid, etc. === */
function isOwner(jid) {
  const justDigits = String(jid || "").split("@")[0].replace(/\D/g, "");
  return OWNERS.some(o =>
    String(o || "").split("@")[0].replace(/\D/g, "") === justDigits
  );
}

/* === Detectar producto/cuenta desde texto libre (para soporte) === */
function pickFirstCanonKeyFromText(text){
  const t = keyify(text || "");
  for (const canon of CANON_KEYS){
    if (t.includes(keyify(canon))) return canon;
    for (const a of (TEMP_KEYS[canon]||[])){
      if (t.includes(keyify(a))) return canon;
    }
  }
  return null;
}
function findAccountToken(text){
  const m1 = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (m1) return m1[0];
  const m2 = text.match(/\S+:\S+/);
  if (m2) return m2[0];
  return "";
}
function parseSupportInput(tail, quoted){
  const joined = [tail, quoted].filter(Boolean).join("\n").trim();
  if (!joined) return { product:"", account:"", desc:"" };

  let product="", account="", desc="";
  const firstLine = (tail||"").trim();

  if (firstLine){
    const parts = firstLine.split(/\s+/);
    if (parts.length){
      const mKey = matchKey(parts[0]);
      if (mKey){ product = mKey; parts.shift(); }
      if (parts.length && /@|:/.test(parts[0])) { account = parts.shift(); }
      desc = [parts.join(" "), quoted].filter(Boolean).join("\n").trim();
    }
  }

  if (!product){
    const guess = pickFirstCanonKeyFromText(joined);
    if (guess) product = guess;
  }
  if (!account){
    const guessAcc = findAccountToken(joined);
    if (guessAcc) account = guessAcc;
  }
  if (!desc){
    desc = joined;
  }

  return { product, account, desc: desc.trim() };
}
// ---- Normalización y resolución de plataforma contra "Precios"
function plainKey(s='') {
  return String(s)
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // sin acentos
    .toLowerCase().replace(/\s+/g, '');               // sin espacios
}

/**
 * Devuelve el nombre de plataforma tal como aparece en Precios
 * a partir de un texto libre del usuario (soporta "max", "max estandar", etc.)
 *
 * Usa SheetsDB.getPriceCatalog() que expone { mapByPlain, list }
 *  - mapByPlain: Map(plainName -> { name, ... })
 */
async function resolvePlatformNameOrNull(queryRaw) {
  const q = String(queryRaw || '').trim();
  if (!q) return null;

  const { mapByPlain, list } = await SheetsDB.getPriceCatalog(); // ya lo usas en .stock
  const entries = Array.from(mapByPlain.entries()); // [ [plain, obj], ... ]
  const p = plainKey(q);

  // match exacto por clave "plain"
  let hit = entries.find(([k]) => k === p);
  if (hit) return hit[1].name || hit[1];

  // empieza por...
  hit = entries.find(([k]) => k.startsWith(p));
  if (hit) return hit[1].name || hit[1];

  // contiene...
  hit = entries.find(([k]) => k.includes(p));
  if (hit) return hit[1].name || hit[1];

  // última chance: incluye por nombre visible
  hit = entries.find(([, v]) =>
    String(v?.name || v).toLowerCase().includes(q.toLowerCase())
  );
  return hit ? (hit[1].name || hit[1]) : null;
}

/* === GLOBAL STORAGE KEY (para precios/ventas) === */
function globalStorageKey(){
  if (CONTROL_GROUP_IDS.length) return CONTROL_GROUP_IDS[0];
  if (CONTROL_GROUP_NAME) return CONTROL_GROUP_NAME.toLowerCase();
  return "__global_store__";
}
function isControlContext(chatId, groupMeta, sender){
  if (CONTROL_GROUP_IDS.length && CONTROL_GROUP_IDS.includes(chatId)) return true;
  const subject = groupMeta?.subject || "";
  if (CONTROL_GROUP_NAME && subject.toLowerCase() === CONTROL_GROUP_NAME.toLowerCase()) return true;
  const isDM = !chatId.endsWith("@g.us");
  if (isDM && isOwner(sender)) return true;
  return false;
}
function isSupportContext(chatId, groupMeta){
  if (SUPPORT_GROUP_IDS.length && SUPPORT_GROUP_IDS.includes(chatId)) return true;
  const subject = groupMeta?.subject || "";
  if (SUPPORT_GROUP_NAME && subject.toLowerCase() === SUPPORT_GROUP_NAME.toLowerCase()) return true;
  return false;
}
async function resolveSupportTarget(sock){ // id de grupo soporte
  if (SUPPORT_GROUP_IDS.length) return SUPPORT_GROUP_IDS[0];
  if (SUPPORT_GROUP_NAME) {
    try {
      const all = await sock.groupFetchAllParticipating();
      const hit = Object.values(all || {}).find(
        g => (g.subject || "").toLowerCase() === SUPPORT_GROUP_NAME.toLowerCase()
      );
      if (hit?.id) return hit.id;
    } catch {}
  }
  if (CONTROL_GROUP_IDS.length) return CONTROL_GROUP_IDS[0]; // respaldo
  return null;
}

/* === Descargar imagen === */
async function downloadImageMsg(imageMessage){
  const stream = await downloadContentFromMessage(imageMessage, "image");
  const chunks=[]; for await(const c of stream) chunks.push(c);
  return Buffer.concat(chunks);
}
async function getImageFromMsgOrQuoted(info){
  const ownImg = info.message?.imageMessage;
  if (ownImg?.mimetype){ const buf=await downloadImageMsg(ownImg); return { buffer:buf, mimetype:ownImg.mimetype }; }
  const q = getQuotedObj(info); const qImg = q?.imageMessage;
  if (qImg?.mimetype){ const buf=await downloadImageMsg(qImg); return { buffer:buf, mimetype:qImg.mimetype }; }
  return null;
}
function ensureGroupMediaDir(){
  const gdir = path.join(MEDIA_DIR, "global");
  if (!fs.existsSync(gdir)) fs.mkdirSync(gdir, { recursive: true });
  return gdir;
}

/* ========= Saldo global ========= */
function getBalance(userJid){ return Number(balancesDB[userJid] || 0); }
function addBalance(userJid, delta){
  const next = (Number(balancesDB[userJid] || 0) + Number(delta));
  balancesDB[userJid] = Math.max(0, Math.round((next + Number.EPSILON)*100)/100);
  writeJSON(balancesPath, balancesDB);
  return balancesDB[userJid];
}

function setBalance(jid, amount) {
  try {
    const id = jidNormalizedUser(jid);
    balancesDB[id] = Number(amount) || 0;
    writeJSON(balancesPath, balancesDB);
    return balancesDB[id];
  } catch { return 0; }
}

async function safeGetPriceCatalog() {
  try {
    const it = await SheetsDB.getPriceCatalog();
    // normaliza un Map vacío si no viene
    const mapByPlain = it?.mapByPlain instanceof Map ? it.mapByPlain : new Map();
    const list = Array.isArray(it?.list) ? it.list : [];
    return { mapByPlain, list };
  } catch {
    return { mapByPlain: new Map(), list: [] };
  }
}

// Busca un nombre canónico de plataforma a partir de texto libre y el catálogo
function resolvePlatFromCatalog(query, mapByPlain) {
  const q = String(query || '').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/\s+/g,'');
  if (!q) return null;
  const entries = Array.from(mapByPlain.entries()); // [ [plain, obj], ... ]
  let hit = entries.find(([plain]) => plain === q);
  if (hit) return hit[1].name || hit[1];

  hit = entries.find(([plain]) => plain.startsWith(q));
  if (hit) return hit[1].name || hit[1];

  hit = entries.find(([plain]) => plain.includes(q));
  if (hit) return hit[1].name || hit[1];

  hit = entries.find(([, v]) => String(v?.name || v).toLowerCase().includes(q));
  return hit ? (hit[1].name || hit[1]) : null;
}

/* ========= Precios / Ventas (local) ========= */
function setPriceGlobal(key, price){
  const sk = globalStorageKey();
  pricesDB[sk] = pricesDB[sk] || {};
  pricesDB[sk][key] = Number(price);
  writeJSON(pricesPath, pricesDB);
}
function getPriceGlobal(key){
  const sk = globalStorageKey();
  return Number((pricesDB[sk]||{})[key] ?? 0);
}
function logSaleGlobal({ groupJid, userJid, key, qty, unitPrice, first }){
  const storageKey = globalStorageKey();
  const total = qty * unitPrice;
  salesDB.push({ ts: Date.now(), storageKey, groupJid, userJid, key, qty, unitPrice, total, first });
  writeJSON(salesPath, salesDB);
}

/* ========= Fechas ========= */
function tryParseDate(s){
  if(!s) return null; s=s.trim();
  let m=s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/); if(m) return new Date(+m[1],+m[2]-1,+m[3]);
  m=s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/); if(m){ let y=+m[3]; if(y<100) y+=2000; return new Date(y,+m[2]-1,+m[1]); }
  const d=new Date(s); return isNaN(d.getTime())?null:d;
}

/* ======== Catálogo GLOBAL ======== */
const GLOBAL_CATALOG_KEY = "__global_catalog__";
function getCatalogForChat(chatId){
  // Mezcla: lo del grupo pisa lo global. Así puedes sobreescribir algo solo en X grupo si quieres.
return {
  ...(productsDB[GLOBAL_CATALOG_KEY] || {}),
  ...(productsDB[chatId] || {})
};
}

/* ========= Conexión ========= */
async function startSock(){
  const { state, saveCreds } = await useMultiFileAuthState(QR_DIR);
  const sock = makeWASocket({
    auth: state,
    logger: Pino({ level: "info" }),
    browser: ["B-Bot Secure", "Chrome", "1.2"],
    generateHighQualityLinkPreview: true
  });

  sock.ev.on("connection.update", ({ connection, lastDisconnect, qr })=>{
    if (qr) {
      console.log("\n================= QR =================");
      console.log("Escanea desde WhatsApp > Dispositivos vinculados");
      try { qrcode.generate(qr, { small:false }); } catch { console.log(qr); }
      console.log("======================================\n");
    }
    if (connection === "close"){
      const code = new Boom(lastDisconnect?.error)?.output?.statusCode;
      const loggedOut = code === DisconnectReason.loggedOut;
      console.log("⚠️  Conexión cerrada. Código:", code, " | loggedOut:", loggedOut);
      if (!loggedOut) { console.log("🔁 Reintentando..."); startSock(); } else console.log("🔒 Sesión cerrada.");
    } else if (connection === "open") console.log("✅ Conectado");
  });

  (async () => {
        try {
          const title = await SheetsDB.pingSheetsTitle();
          console.log('✅ Google Sheets conectado. Título:', title);
          // si quieres, puedes enviar un mensaje a tu grupo de control:
          // await sock.sendMessage('<jid-del-grupo>@g.us', { text: `Sheets OK: ${title}` });
        } catch (e) {
          console.error('❌ Error pingSheetsTitle:', e?.message || e);
          // opcional: notificar en un grupo de soporte
          // await sock.sendMessage('<jid-del-grupo>@g.us', { text: `Sheets ERROR: ${e?.message || e}` });
        }
      })();

  sock.ev.on("creds.update", saveCreds);

  /* ========= Recordatorios soporte (cada 60 s) ========= */
  setInterval(async () => {
    try{
      const now = Date.now();
      for (const [id, t] of Object.entries(ticketsDB)){
        if (!t.answered && t.remindAt && now >= t.remindAt && !t.reminded){
          const supportId = await resolveSupportTarget(sock);
          if (!supportId) continue;
          const atOwners = OWNERS.length ? " " + OWNERS.map(j=>`@${j.split("@")[0]}`).join(" ") : "";
          const txt = `⏰ *Recordatorio*: Ticket sin respuesta${atOwners}\n• Producto: *${t.product || "—"}*\n• Cuenta: *${t.account || "—"}*\n• Cliente: @${(t.userJid||"").split("@")[0]}\n\nResponde citando el mensaje original para notificar al cliente.`;
          await sock.sendMessage(supportId, { text: txt, mentions: [...OWNERS, t.userJid].filter(Boolean) });
          t.reminded = true;
          writeJSON(ticketsPath, ticketsDB);
        }
      }
    }catch(e){ /* silencio */ }
  }, 60_000);

  /* ========= Mensajes entrantes ========= */
  sock.ev.on("messages.upsert", async ({ messages, type })=>{
    if (type !== "notify") return;
    for (const info of messages){
      try{
        const from = info.key.remoteJid; if (!from) continue;
        const isGroup = from.endsWith("@g.us");
        const sender  = jidNormalizedUser(info.key.participant || info.key.remoteJid);
        const body    = extractPlainText(info);

        // PASO 2.A — MENÚ AUTOMÁTICO EN PRIVADO + LECTURA DE BOTONES
const btnId = getBtnId(info);

// 1) Si es privado y el usuario saluda -> mostrar menú
const looksLikeGreeting = (t) =>
  /^(hola|holi|buenas|buenos dias|buenos días|menu|menú|hi|hello|hey)$/i
    .test(String(t || '').trim());
if (!isGroup && !btnId && !isCmd(body) && looksLikeGreeting(body)) {
await showHomeTextMenu(sock, from, sender);
  continue;
}

// 2) Si viene de botones -> responder (placeholders)
if (btnId) {
  if (btnId === BTN.BUY) {
    convo.set(sender, { mode: 'buy', step: 1, data: {} });
    await sock.sendMessage(from, { text: '🛒 Perfecto. ¿Qué plataforma quieres? (ej: Netflix, Max, Spotify, etc.)' }, { quoted: info });
  } else if (btnId === BTN.PRICES) {
    await sock.sendMessage(from, { text: '💸 Pídeme: `.precio <plataforma>` (ej: `.precio netflix`). Pronto lo haré automático aquí.' }, { quoted: info });
  } else if (btnId === BTN.SUPPORT) {
    convo.set(sender, { mode: 'support', step: 1, data: {} });
    await sock.sendMessage(from, { text: '🆘 Cuéntame el problema o responde tu mensaje con `.soporte` y lo envío al equipo.' }, { quoted: info });
  } else if (btnId === BTN.CANCEL) {
    resetConvo(sender);
    await sock.sendMessage(from, { text: '❌ Cancelado. Escribe *menu* para empezar de nuevo.' }, { quoted: info });
  }
  continue; // importante: ya atendimos el evento
}


// === parseo de comando (debe ir ANTES de usar 'name' en cualquier lado)
const prefixUsed = whichPrefix(body);
const isCommand  = Boolean(prefixUsed);
let nameRaw = "";
let name    = "";
let args    = [];

if (isCommand) {
  nameRaw = cmdName(body);
  name    = ALIASES[nameRaw] || nameRaw;
  args    = cmdArgs(body);

  // normalizaciones / alias
  if (name === "link") name = "linkgp";
  if (name === "cargasaldo" || name === "cargasaldos") name = "cargarsaldo";
  if (name === "cargarcuentas") name = "cargacuentas";
}
        if (isGroup){
          activityDB[from] = activityDB[from] || {};
          activityDB[from][sender] = Date.now();
          writeJSON(activityPath, activityDB);
        }

// === GUARD: en grupos, validar admin SOLO para comandos sensibles ===
let iAmAdmin = false;

if (isGroup) {
  try {
    const meta = await sock.groupMetadata(from);
    const mePart = (meta.participants || []).find(p => isMePid(p?.id || p?.jid || '', sock));
    iAmAdmin = !!(mePart && (mePart.admin === 'admin' || mePart.admin === 'superadmin' || mePart.admin === true));
  } catch (e) {
    iAmAdmin = false;
  }

  const adminOnlyCommands = new Set([
    'kick','daradmin','quitaradmin','grupo','linkgp','resetlink',
    'tagall','todos','cerrar','abrir','add','addnum'
  ]);

  if (name && adminOnlyCommands.has(name) && !iAmAdmin && !isOwner(sender)) {
    await sock.sendMessage(from, { text: '⛔ Este comando requiere que el bot sea admin o que seas OWNER.' }, { quoted: info });
    continue;
  }
}

// No procesar nada si el chat está desactivado, excepto OWNER
if (disabledDB[from] && !isOwner(sender)) continue;

        /* ===== Procesar respuesta de SOPORTE (debe citar) – SIEMPRE, aun sin comandos ===== */
        try {
          if (isGroup) {
            const meta = await sock.groupMetadata(from);
            if (isSupportContext(from, meta)) {
              const ctx = info.message?.extendedTextMessage?.contextInfo;
              const stanzaId = ctx?.stanzaId || ctx?.stanzaID;
              if (stanzaId && ticketsDB[stanzaId]) {
                const t = ticketsDB[stanzaId];
                const replyText = extractPlainText(info) || "";
                const low = replyText.toLowerCase().trim();

                // 1) Mensaje "activa/viva/funciona/buena/ok"
                if (/(^|\b)(activa|viva|funciona|buena|ok)(\b|!|\.)/i.test(low)) {
                  const msg =
`✅ *Cuenta verificada (OK)*
• Producto: *${t.product || "—"}*
• Cuenta: *${t.account || "—"}*

${replyText}`;
                  await sock.sendMessage(t.userJid, { text: msg });
                  await sock.sendMessage(from, { react: { text: "✅", key: info.key } });
                  t.answered = true; t.reminded = true;
                  writeJSON(ticketsPath, ticketsDB);
                  continue; // ya atendimos
                }

                // 2) Mensaje de garantía "reemplazo ..." o solo user:pass
                let extracted = "";
                let m;
                if ((m = /reemplaz[oa]?\s+(.+)$/i.exec(replyText))) extracted = m[1].trim();
                if (!extracted) extracted = replyText.trim();
                extracted = extracted.replace(/\s+/g, " ").trim();

                let newUser = "", newPass = "";
                if (/^.+:.+$/.test(extracted)) {
                  const [u,p] = extracted.split(":", 2);
                  newUser = (u || "").trim();
                  newPass = (p || "").trim();
                } else {
                  const parts = extracted.split(" ");
                  if (parts.length === 2) { newUser = parts[0].trim(); newPass = parts[1].trim(); }
                }

                const seemsUser = /@|\./.test(newUser) || /\w+/.test(newUser);
                const valid = newUser && newPass && seemsUser && newPass.length >= 3;

                if (valid) {
                  try {
                    if (typeof SheetsDB.replaceCredentials === "function") {
                      const findBy = (t.account || "").trim();
                      await SheetsDB.replaceCredentials({ findBy, newUser, newPass });
                    }
                  } catch {}

                  const msg =
`🛡️ *Garantía aplicada*
Tus nuevas credenciales:
\`${newUser}:${newPass}\`

Si necesitas ayuda extra, responde a este chat.`;
                  await sock.sendMessage(t.userJid, { text: msg });
                  await sock.sendMessage(from, { react: { text: "✅", key: info.key } });
                  t.answered = true; t.reminded = true;
                  writeJSON(ticketsPath, ticketsDB);
                  continue; // ya atendimos
                }
              }
            }
          }
        } catch (_) { /* silencio */ }

if (!isGroup && !isCommand) {
  const state = convo.get(sender);

  if (state) {
    const msg = String(body || '').trim().toLowerCase();

    // HOME: espera "comprar", "precios" o "soporte"
    if (state.mode === 'home') {
      if (/^soporte$/.test(msg)) {
        convo.set(sender, { mode: 'support', step: 1, data: {} });
        await sock.sendMessage(from, { text:
`🆘 Cuéntame el problema. 
Puedes escribir: 
*soporte <producto?> <cuenta?> <detalle>* 
(o responde al mensaje y luego escribe *soporte*).` });
        return;
      }

      if (/^precios?$/.test(msg)) {
        try {
          const { list } = await SheetsDB.getPriceCatalog();
          const plats = [...new Set((list || []).map(r => r.plataforma))].slice(0, 30);
          await sock.sendMessage(from, { text:
`💸 *Precios*
Pídeme: *precio <plataforma>*

Ejemplos:
• precio netflix
• precio max estandar

Disponibles (muestra parcial):
- ${plats.join('\n- ')}` });
        } catch {
          await sock.sendMessage(from, { text: 'Pídeme: *precio <plataforma>* (ej: *precio netflix*)' });
        }
        return;
      }

      if (/^comprar$/.test(msg)) {
        convo.set(sender, { mode: 'buy', step: 1, data: {} });
        await sock.sendMessage(from, { text:
`🛒 OK. ¿Qué *plataforma* quieres?
Ejemplos: *netflix*, *max estandar*, *spotify*, *disney*, *canva pro* ...` });
        return;
      }

await showHomeTextMenu(sock, from, sender);
      return;
    }

    // BUY: paso 1 → plataforma
    if (state.mode === 'buy' && state.step === 1) {
      state.data.platformQuery = body.trim();
      state.step = 2;
      convo.set(sender, state);
      await sock.sendMessage(from, { text:
`¿Tipo? Escribe *completa* o *perfil*.
Si no estás seguro, escribe *completa*.` });
      return;
    }

    // BUY: paso 2 → tipo
    if (state.mode === 'buy' && state.step === 2) {
      const t = String(body || '').trim().toLowerCase();
      const type = /^(perfil|completa)$/.test(t) ? t : 'completa';
      state.data.type = type;
      state.step = 3;
      convo.set(sender, state);
      await sock.sendMessage(from, { text:
`¿Duración? (ej: *1m*, *3m*, *12m*).` });
      return;
    }

// BUY: paso 3 → duración y ejecutar compra
if (state.mode === 'buy' && state.step === 3) {
  const rawDur = String(body || '').trim().toLowerCase();
  const normDur = (d) => {
    if (/^\d+$/.test(d)) return `${d}m`;
    if (/^\d+m$/.test(d)) return d;
    if (/^\d+\s*m$/.test(d)) return d.replace(/\s+/g,'');
    const m = d.match(/^(\d+)\s*m(?:es(?:es)?)?$/);
    return m ? `${m[1]}m` : d;
  };
  const duration = normDur(rawDur);

  const platformQuery = state.data.platformQuery;
  const type = state.data.type;

  try {
    // 🔹 NUEVO: resolver nombre canónico tal como está en *Precios*
    const canonPlatform = await resolvePlatformNameOrNull(platformQuery);
    if (!canonPlatform) {
      try {
        const { list } = await SheetsDB.getPriceCatalog();
        const plats = [...new Set((list || []).map(r => r.plataforma))].slice(0, 20);
        await sock.sendMessage(from, { text:
`❌ Plataforma no encontrada en *Precios*.

Ejemplos:
- ${plats.join('\n- ')}

Vuelve a escribir la plataforma (p.ej. *max estandar*, *spotify*, *vix*).` });
      } catch {
        await sock.sendMessage(from, { text: '❌ Plataforma no encontrada en *Precios*.' });
      }
      convo.set(sender, { mode: 'home', step: 0, data: {} });
      return;
    }

    // 🔹 y usar SIEMPRE el canónico en las consultas
    const priceRows = await SheetsDB.listPricesByPlatform(canonPlatform);
    if (!priceRows.length) {
      await sock.sendMessage(from, { text: '❌ Plataforma no encontrada en *Precios*.' });
      convo.set(sender, { mode: 'home', step: 0, data: {} });
      return;
    }

    const rowsDur = priceRows.filter(r => String(r.duracion || '').toLowerCase() === duration);
    if (!rowsDur.length) {
      const durs = [...new Set(priceRows.map(r => r.duracion))].join(', ');
      await sock.sendMessage(from, { text:
`❌ Duración no disponible para *${priceRows[0].plataforma}*.
Disponibles: ${durs}` });
      convo.set(sender, { mode: 'home', step: 0, data: {} });
      return;
    }

    const chosenRow =
      rowsDur.find(r => String(r.tipo || '').toLowerCase() === type) || rowsDur[0];

    // 🔹 usa el canónico para todo lo demás
    const platformName = canonPlatform;
    const realType     = String(chosenRow.tipo || type || 'completa').toLowerCase();
    const price        = Number(chosenRow.precio || 0) || 0;
    const currency     = chosenRow.moneda || 'MXN';

    const bal = getBalance(sender);
    if (price > 0 && bal < price) {
      await sock.sendMessage(from, { text:
`💳 Saldo insuficiente. Necesitas *${price} ${currency}*, tienes *${bal.toFixed(2)}*.` });
      convo.set(sender, { mode: 'home', step: 0, data: {} });
      return;
    }

    const asMeses = (d) => {
      const n = parseInt(String(d).replace(/\D/g,''), 10) || 0;
      return n ? (n === 1 ? '1 mes' : `${n} meses`) : '';
    };
    const wantDurations = [duration];
    const alt = asMeses(duration);
    if (alt) wantDurations.push(alt);

    const soldToTag = `wa:${sender.split('@')[0]}`;
    const orderIdPrefix = `ORD-${Date.now().toString(36).toUpperCase()}`;
    const buyerPhone = sender.split('@')[0];
    const sellerName = isOwner(sender) ? 'owner' : (isReseller(sender) ? 'reseller' : 'bot');

    let picked = [];
    for (const d of wantDurations) {
      try {
        // 🔹 aquí también, usar platformName (canónico)
        picked = await SheetsDB.takeNFreeFromViewAndMark(
          platformName, realType, d, 1, soldToTag, orderIdPrefix, buyerPhone, sellerName
        );
        if (picked && picked.length) break;
      } catch {}
    }

    if (!picked || !picked.length) {
      await sock.sendMessage(from, { text: '😕 Sin stock para esa combinación.' });
      convo.set(sender, { mode: 'home', step: 0, data: {} });
      return;
    }

    const acc = picked[0];
    const orderId = acc.order_id || `${orderIdPrefix}-1`;

    if (price > 0) addBalance(sender, -price);
    try {
      await SheetsDB.appendSaleLog({
        platform: platformName,  // 🔹 canónico
        plan: realType,
        username: acc.username || '',
        password: acc.password || '',
        extra: acc.perfil ? `perfil:${acc.perfil}${acc.pin ? ' pin:'+acc.pin : ''}` : '',
        price,
        currency,
        code: orderId,
      });
    } catch {}

    await sock.sendMessage(sender, {
      text:
`🎫 *Tu compra está lista*
• Plataforma: *${platformName}*
• Tipo: *${realType}*
• Duración: *${duration}*
• Usuario: \`${acc.username || '-'}\`
• Clave: \`${acc.password || '-'}\`
${acc.perfil ? `• Perfil: ${acc.perfil}\n` : ''}${acc.pin ? `• PIN: ${acc.pin}\n` : ''}• Pedido: \`${orderId}\`
${price>0 ? `• Cargo: ${price} ${currency}\n• Saldo restante: ${getBalance(sender).toFixed(2)}` : ''}`
    });

    await sock.sendMessage(from, { text:
`🛒 *Pedido completado* 
• ${platformName} • ${realType} • ${duration}
Revisé tu DM y te envié las credenciales ✅` });

  } catch (e) {
    console.error('flow comprar error:', e);
    await sock.sendMessage(from, { text: '⚠️ Error procesando la compra.' });
  }

  convo.set(sender, { mode: 'home', step: 0, data: {} });
  return;
}

  // si no hay estado, puedes ignorar o mostrar menú:
  // await showHomeMenu(sock, from, sender);
}
        /* === A partir de aquí sí filtramos no-comandos === */
        if (!isCommand) continue;
        
        // SOLO rate-limit para comandos
        if (!allow(sender)){
          await sock.sendMessage(from,{text:"⏳ Demasiados comandos. Intenta en un minuto."},{quoted:info});
          continue;
        }

        // Antilink (permite invitaciones de WhatsApp)
        const antiOn = antilinkDB[from] ?? ANTILINK_DEFAULT;
        if (isGroup && antiOn){
          const txt=(body||"").toLowerCase();
          const hasLink=/(https?:\/\/|wa\.me\/|chat\.whatsapp\.com)/i.test(txt);
          if (hasLink && !/chat\.whatsapp\.com\/[A-Za-z0-9]+/i.test(txt)){
            try{ await sock.sendMessage(from,{ delete:info.key }); } catch {}
            await sock.sendMessage(from,{ text:"🚫 Enlaces no permitidos." });
            continue;
          }
        }

        // aliases / typos comunes (DEBE estar aquí, donde 'name' ya existe)
        if (name === "cargasaldo")    name = "cargarsaldo";
          if (name === "cargarcuentas") name = "cargacuentas";

let metadata = null;
try {
  metadata = isGroup ? await sock.groupMetadata(from) : null;
} catch (_) {
  metadata = null; // no rompas el flujo
}

const participants = metadata?.participants || [];

const admins = participants
  .filter(p => Boolean(p?.admin)) // 'admin' | 'superadmin' | undefined
  .map(p => jidNormalizedUser(p.id));

const isAdmin = admins.includes(jidNormalizedUser(sender)) || isOwner(sender);

        const isControl = isControlContext(from, metadata, sender);

        async function reply(text, extra={}){ return sock.sendMessage(from,{text, ...extra},{quoted:info}); }

        // Lista básica de comandos abiertos
        const openCommands = new Set([
          "menu","saldo","stock","resetlink","grupo","listaadmin",
          "op","bot","fantasma","grupoinfo","welcome",
          "precio","ayuda","miscompras","soporte","idgrupo","id","numero","misgrupos",
          "tarjeta","add","catalogo","productos","bothijueputa","hijueputa"
        ]);
        const isUserAllowed = openCommands.has(name) || !!matchKey(name) || name.startsWith("comprar");
        if (isGroup){
          const isSaldoOp = (name === "cargarsaldo");
          const isAddDelete = name.startsWith("add") || name.startsWith("delete") || name === "addayuda";
        // Permitir también al OWNER fuera de Control
         if (isAddDelete && name !== "add" && !(isControl || isOwner(sender))) {
            return reply("`.add...` y `.delete...` solo en *Sala de Control* o para *OWNER*.");
        }
          if (!isAdmin && !isUserAllowed && !isSaldoOp){
            return reply("Solo los *administradores* pueden usar este comando.");
          }
          if (isSaldoOp && !isAdmin) return reply("Solo los *administradores* pueden usar .cargarsaldo.");
        } else {
          if (!(isUserAllowed || isOwner(sender))) return reply("Por privado puedes usar: *saldo*, *stock*, *precio*, *ayuda*, *miscompras*, *comprar* y ver catálogos.");
        }

        /* ===== OFFBOT / ONBOT ===== */
        if (name === "offbot" || name === "onbot"){
          if (!(isControl || isOwner(sender))) return reply("Solo *OWNER* o *Sala de Control*.");
          let target = args[0] || from;
          if (!/@(g\.us|s\.whatsapp\.net)$/.test(target)) target = target.trim();
          if (!target || !/(@g\.us|@s\.whatsapp\.net)$/.test(target)) return reply(`Uso: .${name} <id_chat?>\nSi omites el ID aplica al chat actual.`);
          if (name === "offbot"){ disabledDB[target] = true; writeJSON(disabledPath, disabledDB); return reply(`🛑 Bot desactivado para: ${target}`); }
          else { delete disabledDB[target]; writeJSON(disabledPath, disabledDB); return reply(`✅ Bot activado para: ${target}`); }
        }

        /* ===== Mostrar catálogos .<clave> (CONSERVANDO *negritas*) ===== */
        const showMatch = matchKey(name);
        if (showMatch && args.length === 0){
          // ⬇⬇  CAMBIO: obtenemos mezcla GLOBAL+GRUPO
          const catalog = getCatalogForChat(from);
          const record = catalog[showMatch];

          let header = "";
          if (STREAMING_KEYS.includes(showMatch)) {
            try {
              const counts = await SheetsDB.getStockCounts();
              const count = counts[showMatch] || 0;
              header = `*Disponibles: ${count}*\n\n`;
            } catch {}
          }

          if (!record) { await reply((header || "").trim() || ""); continue; }

          const safeText = typeof record === "object" ? (record.text || "") : (record || "");
          const caption = (header + (safeText || "")).trim();

          if (record && typeof record === "object" && record.image?.path && fs.existsSync(record.image.path)){
            try { 
              await sock.sendMessage(from, { image: fs.readFileSync(record.image.path), caption }, { quoted: info }); 
            }
            catch { await sock.sendMessage(from, { text: caption || "(sin contenido)" }, { quoted: info }); }
          } else {
            await sock.sendMessage(from, { text: caption || header || "(sin contenido)" }, { quoted: info });
          }
          return;
        }


        function normType(t=''){
  t = String(t).trim().toLowerCase();
  if (t.startsWith('comp')) return 'completa';
  if (t.startsWith('perf') || t === 'p') return 'perfil';
  return t || 'completa';
}

function normDuration(s=''){
  const t = String(s).toLowerCase().trim();
  if (/^\d+$/.test(t)) return `${t}m`;              // "1" -> "1m"
  if (/^\d+\s*m$/.test(t)) return t.replace(/\s+/g,''); // "1 m" -> "1m"
  if (/^\d+m$/.test(t)) return t;                   // "1m"
  const m = t.match(/^(\d+)\s*m(?:es(?:es)?)?$/);   // "1 mes"/"3 meses"
  return m ? `${m[1]}m` : t || '1m';
}

function mask(s=''){
  s = String(s);
  if (s.length <= 4) return '*'.repeat(Math.max(3, s.length));
  return s.slice(0,2) + '*'.repeat(Math.max(3, s.length-4)) + s.slice(-2);
}


        /* ===== ADD/DELETE catálogos ===== */
        const addMatch = name.startsWith("add")    ? matchKey(name.slice(3))  : null;
        const delMatch = name.startsWith("delete") ? matchKey(name.slice(6))  : null;

        if (addMatch){
          const quotedText = getQuotedText(info);
          const tailText   = commandTailPreserving(body);
          const textToSave = (quotedText || tailText || "").replace(/\s+$/,"");

          let savedImage = null;
          try{
            const img = await getImageFromMsgOrQuoted(info);
            if (img){
              const ext = mime.extension(img.mimetype) || "jpg";
              const gdir = ensureGroupMediaDir();
              const fname = `${addMatch}_${Date.now()}.${ext}`;
              const fpath = path.join(gdir, fname);
              fs.writeFileSync(fpath, img.buffer);
              savedImage = { path: fpath, mime: img.mimetype };
            }
          }catch{}

          if (!textToSave && !savedImage) return reply(`Uso: .add${addMatch} <texto> (puedes responder/usar imagen)`);

          // ⬇⬇ CAMBIO: si estoy en CONTROL, guardo global; si no, por grupo
          const targetKey = isControl ? GLOBAL_CATALOG_KEY : from;
          productsDB[targetKey] = productsDB[targetKey] || {};
          const prev = productsDB[targetKey][addMatch];
          let next;
          if (savedImage){
            next = { text: textToSave || (typeof prev === "string" ? prev : prev?.text || ""), image: savedImage };
          } else {
            next = (prev && typeof prev === "object") ? { ...prev, text: textToSave } : textToSave;
          }
          productsDB[targetKey][addMatch] = next;
          writeJSON(productsPath, productsDB);
          await reply(`✅ *${addMatch}* guardado${savedImage ? " con imagen" : ""}${isControl ? " (GLOBAL)" : ""}.`);
          return;
        }

        if (delMatch){
          // ⬇⬇ CAMBIO: borrar global si estoy en CONTROL; si no, borrar del grupo
          const targetKey = isControl ? GLOBAL_CATALOG_KEY : from;
          productsDB[targetKey] = productsDB[targetKey] || {};
          delete productsDB[targetKey][delMatch];
          writeJSON(productsPath, productsDB);
          await reply(`🗑️ *${delMatch}* eliminado${isControl ? " (GLOBAL)" : ""}.`);
          return;
        }

        /* ===== AYUDA (solo CONTROL) ===== */
        if (name === "addayuda"){
          if (!isControl) return reply("`.addayuda` solo en *Sala de Control*.");
          const key = matchKey(args[0]||"");
          if (!key) return reply("Uso: .addayuda <clave> <texto>  (o responde con el texto)");
          const textToSave = (getQuotedText(info) || commandTailPreserving(body).replace(/^\s*\S+\s*/,"")).trim();
          if (!textToSave) return reply("Escribe el texto de ayuda (o respóndelo).");
          helpDB[from] = helpDB[from] || {};
          helpDB[from][key] = textToSave;
          writeJSON(helpPath, helpDB);
          return reply(`✅ Ayuda para *${key}* actualizada.`);
        }

        // Solo admin u OWNER pueden promover / degradar
          if ((name === "daradmin" || name === "quitaradmin") && !isAdmin) {
           return reply("Solo los administradores o el OWNER pueden usar este comando.");
        }

        // === helpers locales (puedes ponerlos una sola vez arriba del switch) ===
function resolveTargetFromMsg() {
  let t = (getMentionedOrQuoted(info) || [])[0] || null;
  if (!t) {
    const textAll = commandTailPreserving(body) || '';
    const m = textAll.match(/(?:@|\+)?(\d{6,})/);
    if (m && m[1]) {
      let phone = m[1].replace(/\D/g, '');
      if (phone.length <= 10 && !phone.startsWith(DEFAULT_CC)) phone = DEFAULT_CC + phone;
      t = `${phone}@s.whatsapp.net`;
    }
  }
  return t;
}

/* ========= SWITCH ========= */
switch (name) {
  case "menu": {
    const caption =
      `📜 MENÚ DEL BOT

💢 ┈┈┈LISTA - COMANDOS┈┈ 💢
❇️ .menu | .bot
❇️ .stock [clave]
❇️ .precio <clave>
❇️ .ayuda <clave>
❇️ .miscompras (privado)
❇️ .soporte <producto?> <cuenta?> <detalle>
❇️ .tarjeta
❇️ .saldo | .cargarsaldo @user <monto> (admins)
❇️ .grupo abrir|cerrar
❇️ .listaadmin | .daradmin | .quitaradmin | .kick
❇️ .notify <texto> | .todos
❇️ .mute @user | .unmute @user
❇️ .antilink on|off | .welcome on|off
❇️ .grupoinfo | .linkgp | .resetlink
❇️ .id / .idgrupo / .numero / .misgrupos
❇️ .offbot [id] | .onbot [id]
❇️ .op <expr>
❇️ .fantasma
💢 ┈┈┈CATÁLOGOS┈┈ 💢
❇️ .catalogo / .productos
❇️ .<clave> / .add<clave> / .delete<clave>
*Ejemplos:* .netflix, .addnetflix, .deletenetflix
💢 ┈┈┈CONTROL┈┈ 💢
❇️ .cargacuentas | .addlote
❇️ .setprecio <clave> <precio>
❇️ .ventas [24|48|rango]
❇️ .infovendidas [<clave>] [24|48|rango]
❇️ .add<clave> / .delete<clave> (solo Control)
❇️ .addpagostreaming / .addpagocine
❇️ .deletepagostreaming / .deletepagocine
❇️ .settarjeta <streaming|cine> <idGrupo>
❇️ .listresellers | .addreseller | .delreseller
❇️ .ventalotes on|off`;
    try {
      if (MENU_IMAGE && !/^https?:\/\//i.test(MENU_IMAGE))
        await sock.sendMessage(from, { image: fs.readFileSync(MENU_IMAGE), caption }, { quoted: info });
      else if (MENU_IMAGE && /^https?:\/\//i.test(MENU_IMAGE))
        await sock.sendMessage(from, { image: { url: MENU_IMAGE }, caption }, { quoted: info });
      else
        await sock.sendMessage(from, { text: caption }, { quoted: info });
    } catch {
      await sock.sendMessage(from, { text: caption }, { quoted: info });
    }
    break;
  }
  /* ===================== CARGA/UPSERT DE CUENTAS ===================== */
  case 'cargacuentas':
  case 'cargacuenta':
  case 'addcuentas':
  case 'addcuenta': {
    if (!isOwner(sender)) return reply('🔒 Solo owners.');

    const textAll = (commandTailPreserving(body) || '').trim();
    if (!textAll) {
      await reply(
        'Uso:\n' +
        '• .addcuenta PLATAFORMA|TIPO|DURACIÓN|USUARIO|PASSWORD|perfil?|pin?|price?|currency?\n' +
        '   Ej: .addcuenta CANVA PRO|completa|1 mes|user@mail.com|Pass123!|Perfil 1|1234|40|MXN\n' +
        '\n' +
        '• También con espacios:\n' +
        '   .addcuenta CANVA PRO completa 1 mes user@mail.com Pass123! [Perfil_1] [1234] [40] [MXN]'
      );
      break;
    }

    // =============== 1) Parseo flexible ===============
    let platform = '',
      type = '',
      duration = '',
      username = '',
      password = '',
      perfil = '',
      pin = '',
      price = '',
      currency = '';
    const TYPE_SET = new Set(['completa', 'perfil']);

    if (textAll.includes('|')) {
      // Formato con pipes
      const parts = textAll.split('|').map(s => s.trim());
      if (parts.length < 5) {
        await reply('Formato con | inválido. Mínimo: plataforma|tipo|duración|usuario|password');
        break;
      }
      [platform, type, duration, username, password, perfil = '', pin = '', price = '', currency = ''] = parts;
    } else {
      // Formato con espacios
      const toks = textAll.split(/\s+/);
      const idxType = toks.findIndex(t => TYPE_SET.has(String(t).toLowerCase()));
      if (idxType <= 0 || toks.length < idxType + 5) {
        await reply('Formato con espacios inválido.\nEj: .addcuenta CANVA PRO completa 1 mes user@mail.com Pass123!');
        break;
      }
      platform = toks.slice(0, idxType).join(' ');
      type = toks[idxType].toLowerCase();

      // duración puede venir como "1", "1m", "1 mes", "3 meses"...
      duration = toks[idxType + 1] || '';
      if (toks[idxType + 2]) duration += ' ' + toks[idxType + 2];

      username = toks[idxType + 3];
      password = toks[idxType + 4];

      // opcionales
      perfil = toks[idxType + 5] ? toks[idxType + 5].replace(/_/g, ' ') : '';
      pin = toks[idxType + 6] || '';
      price = toks[idxType + 7] || '';
      currency = toks[idxType + 8] || '';
    }

    // =============== 2) Normalizaciones mínimas ===============
    // (usa los helpers de PASO 1 que agregamos antes en el archivo)
    type = normType(type); // → 'completa' | 'perfil'
    duration = normDuration(duration); // → '1m', '3m', '12m', ...

    // también arreglamos cositas estéticas
    platform = platform.replace(/\s+/g, ' ').trim();
    currency = (currency || 'MXN').toUpperCase();

    // Validación mínima
    if (!platform || !type || !duration || !username || !password) {
      await reply('Faltan campos. Mínimo: plataforma, tipo, duración, usuario, password.');
      break;
    }

    // =============== 3) Upsert en Google Sheets ===============
    try {
      const payload = {
        platform, // columna: plataforma (respeta mayúsculas como lo escribiste)
        type, // 'completa' | 'perfil'
        duration, // '1m', '3m', ...
        username,
        password,
        perfil,
        pin,
        price: price ? Number(price) : '',
        currency,
      };

      // 3.1 Verificar contra hoja Precios (para avisarte si NO existe)
      let okCombo = false;
      try {
        okCombo = await SheetsDB.hasPriceCombo({
          plataforma: platform,
          tipo: type,
          duracion: duration
        });
      } catch { /* no rompemos si falla la consulta */ }

      // 3.2 Upsert real
      const res = await SheetsDB.upsertAccounts([payload]);
      const added = (res.added ?? res.inserted ?? 0);
      const updated = (res.updated ?? 0);
      const skipped = (res.skipped ?? 0);

      // 3.3 Respuesta al chat
      let msg =
        `✅ Cuentas procesadas en *${platform.toUpperCase()}*:\n` +
        `• Agregadas: *${added}*\n` +
        `• Reemplazadas: *${updated}*\n` +
        `• Omitidas: *${skipped}*`;

      if (!okCombo) {
        msg += `\n\n⚠️ *Recordatorio:* Esta combinación (*${platform} • ${type} • ${duration}*) no está en la hoja *Precios*. Actualiza la lista.`;

        // Aviso a Sala de Control (si está configurada)
        try {
          for (const gid of (CONTROL_GROUP_IDS || [])) {
            if (!gid) continue;
            await sock.sendMessage(gid, {
              text: `🔔 Nuevo item fuera de *Precios*:\n${platform} • ${type} • ${duration}\nSubido por: @${sender.split('@')[0]}`
            }, { quoted: info });
          }
        } catch {}
      }

      await reply(msg);
    } catch (e) {
      console.error('addcuenta error:', e);
      await reply('⚠️ Error al insertar/actualizar en *Cuentas*. Revisa encabezados y permisos.');
    }

    break;
  }
  case 'addlote': {
    if (!(isOwner(sender) || isReseller(sender))) {
      await reply('🔒 Solo owners o resellers.');
      break;
    }

    const raw = (commandTailPreserving(body) || '').trim();
    if (!raw) {
      await reply(
        'Uso:\n' +
        '.addlote <PLATAFORMA> <completa|perfil> <duración>\n' +
        'usuario@mail.com|Clave\nusuario2@mail.com Clave2\n...\n\n' +
        'Ej:\n.addlote MAX ESTANDAR completa 1m\nmail1@mail.com|Pass#1\nmail2@mail.com Pass#2'
      );
      break;
    }

    // 1) Separar encabezado y líneas de cuentas
    const lines = raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    if (lines.length < 2) {
      await reply('Debes incluir al menos 1 línea de cuentas después del encabezado.');
      break;
    }

    // Encabezado puede venir "PLAT TIPO DUR" o "PLAT|TIPO|DUR"
    let head = lines[0];
    let platform = '',
      type = '',
      duration = '';
    if (head.includes('|')) {
      const [p, t, d] = head.split('|').map(s => s.trim());
      platform = p || '';
      type = (t || '').toLowerCase();
      duration = (d || '').toLowerCase();
    } else {
      const toks = head.split(/\s+/);
      // Buscar el token del tipo
      const TYPE_SET = new Set(['completa', 'perfil']);
      const idxType = toks.findIndex(t => TYPE_SET.has(t.toLowerCase()));
      if (idxType <= 0 || toks.length < idxType + 2) {
        await reply('Encabezado inválido. Ej: "MAX ESTANDAR completa 1m"');
        break;
      }
      platform = toks.slice(0, idxType).join(' ');
      type = toks[idxType].toLowerCase();
      duration = (toks[idxType + 1] || '').toLowerCase();
    }

    // Normalizar duración "1" -> "1m", "1 mes" -> "1m"
    const normDur = (tRaw) => {
      const t = String(tRaw || '').toLowerCase().trim();
      if (/^\d+$/.test(t)) return `${t}m`;
      if (/^\d+m$/.test(t)) return t;
      if (/^\d+\s*m$/.test(t)) return t.replace(/\s+/g, '');
      const m = t.match(/^(\d+)\s*m(?:es(?:es)?)?$/); // 1 mes / 3 meses
      if (m) return `${m[1]}m`;
      return t;
    };
    duration = normDur(duration);

    if (!platform || !type || !duration) {
      await reply('Encabezado incompleto. Requiere: plataforma, tipo y duración.');
      break;
    }

    // 2) Parsear cada línea de cuenta: "user|pass" o "user pass" (opcionales: perfil|pin|price|currency)
    const rowsToInsert = [];
    for (let i = 1; i < lines.length; i++) {
      const ln = lines[i];
      if (!ln) continue;

      // Soporta "a|b|perfil|pin|price|currency"  o  "a b"
      let user = '',
        pass = '',
        perfil = '',
        pin = '',
        price = '',
        currency = '';
      if (ln.includes('|')) {
        const parts = ln.split('|').map(s => s.trim());
        [user, pass, perfil = '', pin = '', price = '', currency = ''] = parts;
      } else {
        const p = ln.split(/\s+/);
        if (p.length < 2) {
          continue;
        }
        [user, pass] = p;
      }

      if (!user || !pass) continue;

      rowsToInsert.push({
        platform,
        type,
        duration,
        username: user,
        password: pass,
        perfil,
        pin,
        price: price ? Number(price) : '',
        currency: currency || 'MXN',
      });
    }

    if (!rowsToInsert.length) {
      await reply('No detecté cuentas válidas en el cuerpo del mensaje.');
      break;
    }

    // 3) Comprobar si la combinación existe en Precios (una vez por lote)
    let okCombo = true;
    try {
      okCombo = await SheetsDB.hasPriceCombo({
        plataforma: platform,
        tipo: type,
        duracion: duration
      });
    } catch {}

    // 4) Insertar con upsert (agrega o reemplaza si ya existe la misma clave)
    try {
      // si tu upsert admite lotes grandes, perfecto; si no, haz chunks de 100
      const res = await SheetsDB.upsertAccounts(rowsToInsert);

      const added = (res.added ?? res.inserted ?? 0);
      const updated = (res.updated ?? 0);
      const skipped = (res.skipped ?? 0);

      let msg =
        `✅ Lote procesado en *${platform.toUpperCase()}* ${type} ${duration}:\n` +
        `• Agregadas: *${added}*\n` +
        `• Reemplazadas: *${updated}*\n` +
        `• Omitidas: *${skipped}*`;

      if (!okCombo) {
        msg += `\n\n⚠️ *Recordatorio:* Esta combinación (*${platform} • ${type} • ${duration}*) no está en *Precios*. Actualiza la lista.`;
        try {
          for (const gid of (CONTROL_GROUP_IDS || [])) {
            await sock.sendMessage(gid, {
              text: `🔔 Lote fuera de *Precios*:\n${platform} • ${type} • ${duration}\nSubido por: @${sender.split('@')[0]}`
            }, { quoted: info });
          }
        } catch {}
      }

      await reply(msg);
    } catch (e) {
      console.error('addlote error:', e);
      await reply('⚠️ Error al insertar/actualizar el lote en *Cuentas*. Revisa encabezados y permisos.');
    }

    break;
  }
  case 'listresellers':
  case 'resellerlist':
  case 'resellers': {
    if (!isOwner(sender)) return reply('🔒 Solo owners.');
    const list = loadResellers();
    if (!list.length) return reply('📭 No hay *resellers* configurados.');
    const mentions = list.map(j => `@${String(j).split('@')[0]}`).join('\n• ');
    await reply(`🧾 *Resellers (${list.length})*\n• ${mentions}`, { mentions: list });
    break;
  }
  case 'addreseller':
  case 'reselleradd': {
    if (!isOwner(sender)) return reply('🔒 Solo owners.');
    const target = resolveTargetFromMsg();
    if (!target) return reply('Uso: `.addreseller @usuario` (o responde a un mensaje del usuario).');

    const botNum = String(sock.user?.id || '').split('@')[0].replace(/\D/g, '');
    const tgtNum = String(target).split('@')[0].replace(/\D/g, '');
    if (botNum && tgtNum && botNum === tgtNum) return reply('🤖 No puedo asignarme a mí mismo.');
    if (isOwner(target)) return reply('👑 Ese número ya es *OWNER*.');

    const rs = loadResellers();
    if (rs.some(j => jidNormalizedUser(j) === jidNormalizedUser(target))) {
      return reply(`ℹ️ Ya es *RESELLER*: @${tgtNum}`, { mentions: [target] });
    }
    rs.push(jidNormalizedUser(target));
    if (!saveResellers(rs)) return reply('⚠️ Error guardando *resellers.json*');
    await reply(`✅ Asignado *RESELLER* a @${tgtNum}`, { mentions: [target] });
    break;
  }
  case 'delreseller':
  case 'resellerdel':
  case 'rmreseller': {
    if (!isOwner(sender)) return reply('🔒 Solo owners.');
    const target = resolveTargetFromMsg();
    if (!target) return reply('Uso: `.delreseller @usuario` (o responde a un mensaje del usuario).');

    const rs = loadResellers();
    const before = rs.length;
    const keep = rs.filter(j => jidNormalizedUser(j) !== jidNormalizedUser(target));
    if (keep.length === before) return reply('❌ Ese número no está como *RESELLER*.');
    if (!saveResellers(keep)) return reply('⚠️ Error guardando *resellers.json*');

    const tgtNum = String(target).split('@')[0].replace(/\D/g, '');
    await reply(`🗑️ Quitado *RESELLER* a @${tgtNum}`, { mentions: [target] });
    break;
  }
  case 'reseller': {
    if (!isOwner(sender)) return reply('🔒 Solo owners.');
    const sub = (args[0] || '').toLowerCase();
    if (sub === 'list') {
      const list = loadResellers();
      if (!list.length) return reply('📭 No hay *resellers* configurados.');
      const mentions = list.map(j => `@${String(j).split('@')[0]}`).join('\n• ');
      await reply(`🧾 *Resellers (${list.length})*\n• ${mentions}`, { mentions: list });
      break;
    }
    if (sub === 'add' || sub === 'del') {
      await reply('Usa:\n• `.addreseller @usuario`\n• `.delreseller @usuario`\n• `.listresellers`');
      break;
    }
    await reply('Uso de *reseller*:\n• `.reseller add @usuario`\n• `.reseller del @usuario`\n• `.reseller list`\n(ó usa los alias: `.addreseller`, `.delreseller`, `.listresellers`)');
    break;
  }
  case "op": {
    const expr = args.join(" ");
    if (!expr) return reply("Uso: .op <expresión>. Ej: .op (2+3)*4");
    try {
      const res = math.evaluate(expr);
      await reply(`🧮 Resultado: ${res}`);
    } catch {
      await reply("⚠️ Expresión inválida.");
    }
    break;
  }
  case 'ping': {
    await reply('✅ pong');
    break;
  }
  case 'pingsheets': {
    if (!isOwner(sender)) {
      await reply('🔒 Solo owners.');
      break;
    }
    try {
      const title = await SheetsDB.pingSheetsTitle();
      await reply(`✅ Sheets OK: ${title}`);
    } catch (e) {
      await reply(`❌ Sheets ERROR: ${e?.message || e}`);
    }
    break;
  }
  case 'n':
  case 'notify': {
    if (!isGroup) {
      await reply('Usa `.notify` dentro de un *grupo* (puedes responder a una imagen/video).');
      break;
    }

    const caption = (commandTailPreserving(body) || '').trim();
    const media = await maybeDownloadQuotedMedia(info);

    if (!caption && !media) {
      await reply('Escribe el mensaje después de `.notify`, o responde con `.notify` a una *imagen/video* para reenviarla con/sin caption.');
      break;
    }

    const allMembers = (participants || []).map(p => jidNormalizedUser(p.id));
    const CHUNK = TAGALL_MAX;

    if (media?.type === 'image') {
      await sock.sendMessage(
        from, {
          image: media.data,
          mimetype: media.mimetype || 'image/jpeg',
          caption,
          mentions: allMembers.slice(0, CHUNK)
        }, {
          quoted: info
        }
      );
    } else if (media?.type === 'video') {
      await sock.sendMessage(
        from, {
          video: media.data,
          mimetype: media.mimetype || 'video/mp4',
          caption,
          mentions: allMembers.slice(0, CHUNK)
        }, {
          quoted: info
        }
      );
    } else {
      await sock.sendMessage(
        from, {
          text: caption,
          mentions: allMembers.slice(0, CHUNK)
        }, {
          quoted: info
        }
      );
    }

    for (let i = CHUNK; i < allMembers.length; i += CHUNK) {
      const slice = allMembers.slice(i, i + CHUNK);
      await sock.sendMessage(from, { text: '📣', mentions: slice });
      await new Promise(r => setTimeout(r, 250));
    }
    break;
  }
  case 'notifyall': {
    if (!isControlContext(from, metadata, sender) && !isOwner(sender)) {
      await reply('Este comando se usa desde la *Sala de Control*.');
      break;
    }

    const rawTail = (commandTailPreserving(body) || '').trim();
    const media = await maybeDownloadQuotedMedia(info);

    let caption = rawTail;
    if (!caption && media) caption = '';
    if (!caption && !media) {
      await reply('Uso:\n.notifyall <texto>\nO responde con .notifyall a una imagen/video.');
      break;
    }

    const targets = (await getAllGroupsExceptControl(sock)).map(jid => ({ jid }));
    if (!targets.length) {
      await reply('No encontré otros grupos (aparte de la Sala de Control).');
      break;
    }

    const sleep = ms => new Promise(r => setTimeout(r, ms));
    let ok = 0,
      fail = 0;

    await reply(`Enviando a ${targets.length} grupos... (esto puede tardar un poco)`);

    for (const { jid } of targets) {
      try {
        const mentions = await getMentionableParticipants(sock, jid);
        if (media?.type === 'image') {
          await sock.sendMessage(jid, {
            image: media.data,
            mimetype: media.mimetype || 'image/jpeg',
            caption,
            mentions
          });
        } else if (media?.type === 'video') {
          await sock.sendMessage(jid, {
            video: media.data,
            mimetype: media.mimetype || 'video/mp4',
            caption,
            mentions
          });
        } else {
          await sock.sendMessage(jid, { text: caption, mentions });
        }
        ok++;
      } catch (e) {
        fail++;
        console.error('notifyall->', jid, e?.message || e);
      }
      await sleep(400);
    }
    await reply(`✅ Listo. Envíos OK: *${ok}* / Fallidos: *${fail}*.`);
    break;
  }
  case 'refreshadmingroups':
  case 'reloadadmingroups': {
    if (!isControlContext(from, metadata, sender) && !isOwner(sender)) {
      await reply('Este comando se usa desde la *Sala de Control* o por *OWNER*.');
      break;
    }

    try {
      if (fs.existsSync(ADMIN_GROUPS_CACHE)) {
        fs.unlinkSync(ADMIN_GROUPS_CACHE);
      }
    } catch (e) {}

    await reply('♻️ Recalculando grupos donde el bot es admin...');

    let jids = [];
    try {
      jids = await getAdminGroups(sock);
    } catch (e) {
      await reply('⚠️ Error recalculando grupos. Revisa logs.');
      break;
    }

    if (!jids.length) {
      await reply('🔍 No se encontraron grupos (aparte de la Sala de Control) donde el bot sea admin.');
      break;
    }

    const lines = [];
    for (const jid of jids) {
      const name = await groupNameOrJid(sock, jid).catch(() => jid);
      lines.push(`• ${name}\n  ${jid}`);
    }

    await reply(`✅ Caché reconstruido.\nGrupos detectados: *${jids.length}*\n\n${lines.join('\n')}`);
    break;
  }
  case 'notifydebug': {
    const metaHere = from.endsWith('@g.us') ? await sock.groupMetadata(from) : null;
    if (!isControlContext(from, metaHere, sender) && !isOwner(sender)) {
      await reply('Este debug se usa en la Sala de Control.');
      break;
    }
    const all = await sock.groupFetchAllParticipating();
    const botJid = sock.user?.id;
    const lines = [];
    let sendables = 0;

    for (const [jid, meta] of Object.entries(all)) {
      const name = meta?.subject || jid;
      const isCtrl = isControlChat(jid, meta);
      const isAdm = isBotAdminInGroup(meta, botJid);
      const willSend = !isCtrl && isAdm;
      if (willSend) sendables++;
      lines.push(
        `• ${name}\n  jid: ${jid}\n  admin:${isAdm ? '✅' : '❌'}  control:${isCtrl ? '✅' : '❌'}  enviar:${willSend ? '✅' : '❌'}`
      );
    }
    await reply(`Grupos detectados: ${Object.keys(all).length}\nListos para .notify: ${sendables}\n\n${lines.join('\n\n')}`);
    break;
  }
  case 'apagados': {
    if (!isOwner(sender)) {
      await reply('🔒 Solo owners.');
      break;
    }
    const ids = Object.keys(disabledDB || {}).filter(j => disabledDB[j]);
    const total = ids.length;

    const lines = [];
    for (const jid of ids) {
      const name = await groupNameOrJid(sock, jid).catch(() => jid);
      lines.push(`• ${name}\n  ${jid}`);
    }
    const text =
      total === 0 ?
      '✅ No hay grupos apagados.' :
      `🚫 *Grupos con el bot APAGADO (${total})*\n\n${lines.join('\n')}`;

    const inCtrl = isControlContext(
      from,
      await sock.groupMetadata(from).catch(() => null),
      sender
    );
    if (!inCtrl) {
      const target = getPrimaryControlJid();
      if (target) {
        await sock.sendMessage(
          target, {
            text: `📋 Reporte solicitado por @${sender.split('@')[0]}:\n\n${text}`,
            mentions: [sender]
          }
        );
        await reply('📨 Envié el listado a la *Sala de Control*.');
      } else {
        await reply(text);
      }
    } else {
      await reply(text);
    }
    break;
  }
  case 'statusbot':
  case 'wherebot': {
    if (!isOwner(sender)) return reply('🔒 Solo owners.');
    const state = disabledDB[from] ? 'APAGADO' : 'ENCENDIDO';
    const msg = `🤖 En este chat el bot está: *${state}*`;

    const inCtrl = isControlContext(from, await sock.groupMetadata(from).catch(() => null), sender);
    if (!inCtrl) {
      const target = getPrimaryControlJid();
      if (target) {
        const name = await groupNameOrJid(sock, from);
        await sock.sendMessage(target, { text: `📍 Estado en *${name}*:\n${msg}` });
        await reply(`${msg}\n\n📨 También se notificó en la *Sala de Control*.`);
      } else {
        await reply(msg);
      }
    } else {
      await reply(msg);
    }
    break;
  }
  case "grupo": {
    const action = (args[0] || "").toLowerCase();
    if (["abrir", "open", "ᴀʙʀɪʀ"].includes(action)) {
      await sock.groupSettingUpdate(from, "not_announcement");
      await reply("🔓 Grupo *abierto* para todos.");
    } else if (["cerrar", "close", "ᴄᴇʀʀᴀʀ"].includes(action)) {
      await sock.groupSettingUpdate(from, "announcement");
      await reply("🔐 Grupo *cerrado* (solo admins).");
    } else await reply("Uso: .grupo abrir | .grupo cerrar");
    break;
  }
  case "daradmin": {
    const targets = getMentionedOrQuoted(info);
    if (!targets.length) return reply("Menciona a quien dar admin.");
    await sock.groupParticipantsUpdate(from, targets, "promote");
    await reply("✅ Admin otorgado.");
    break;
  }
  case "quitaradmin": {
    const targets = getMentionedOrQuoted(info);
    if (!targets.length) return reply("Menciona a quien quitar admin.");
    await sock.groupParticipantsUpdate(from, targets, "demote");
    await reply("✅ Admin retirado.");
    break;
  }
  case "listaadmin": {
    const meta = await sock.groupMetadata(from);
    const adminIds = (meta.participants || []).filter(p => p.admin).map(p => p.id);
    const adminsList = adminIds.map(j => `@${j.split("@")[0]}`);
    await sock.sendMessage(from, { text: `👮 *Admins de ${meta.subject}:*\n\n${adminsList.join("\n")||"—"}`, mentions: adminIds }, { quoted: info });
    break;
  }
  case "ban":
  case "kick": {
    const targets = getMentionedOrQuoted(info);
    if (!targets.length) return reply("Menciona a quien expulsar.");
    await sock.groupParticipantsUpdate(from, targets, "remove");
    await reply("🚪 Usuario(s) expulsado(s).");
    break;
  }
  case "grupoinfo": {
    const meta = await sock.groupMetadata(from);
    const name = meta.subject || "—";
    const size = (meta.participants || []).length;
    const adminIds = (meta.participants || []).filter(p => p.admin).map(p => p.id);
    const adminsList = adminIds.map(j => `@${j.split("@")[0]}`);
    const desc = (meta.desc && (meta.desc.description || meta.desc.toString?.())) || "—";
    await sock.sendMessage(from, { text: `🛈 *Información del grupo*\n\n• Nombre: ${name}\n• Miembros: ${size}\n• Admins:\n${adminsList.map(a => "  - " + a).join("\n")||"  —"}\n\n• Descripción:\n${desc}`, mentions: adminIds }, { quoted: info });
    break;
  }
  case "idgrupo": {
    if (!isGroup) return reply("Este comando solo funciona dentro de un *grupo*.");
    const meta = await sock.groupMetadata(from);
    await reply(`🆔 *ID del grupo*\n${from}\n\n📛 *Nombre*: ${meta.subject || "—"}`);
    break;
  }
  case "id": {
    const targets = getMentionedOrQuoted(info);
    const meta = isGroup ? await sock.groupMetadata(from) : null;
    let lines = [];
    lines.push(`👤 *Tu JID*: ${sender}`);
    lines.push(`💬 *Chat ID*: ${from}${isGroup ? `  (grupo: ${meta?.subject || "—"})` : "  (DM)"}`);
    if (targets.length) {
      lines.push("\n👥 *JIDs mencionados/citados:*");
      for (const j of targets) lines.push(`• ${j}  (@${j.split("@")[0]})`);
    } else {
      lines.push("\nℹ️ Puedes mencionar o responder un mensaje para ver su JID.");
    }
    await sock.sendMessage(from, { text: lines.join("\n") }, { quoted: info });
    break;
  }
  case "numero": {
    const targets = getMentionedOrQuoted(info);
    const list = (targets.length ? targets : [sender]).map(j => j.split("@")[0]);
    await sock.sendMessage(from, { text: `📱 ${list.join(", ")}` }, { quoted: info });
    break;
  }
  case "misgrupos": {
    const all = await sock.groupFetchAllParticipating();
    const groups = Object.values(all || {});
    if (!groups.length) return reply("No veo grupos.");
    const lines = groups
      .sort((a, b) => (a.subject || "").localeCompare(b.subject || ""))
      .map(g => `• ${g.subject || "—"}\n  ${g.id}`);
    await sock.sendMessage(from, { text: `🗂️ *Grupos donde está el bot:*\n\n${lines.join("\n")}` }, { quoted: info });
    break;
  }
  case "linkgp": {
    try {
      const code = await sock.groupInviteCode(from);
      await reply(`🔗 Link del grupo:\nhttps://chat.whatsapp.com/${code}`);
    } catch (e) {
      await reply("Necesito ser admin para generar el link.");
    }
    break;
  }
  case "resetlink": {
    try {
      await sock.groupRevokeInvite(from);
      const code = await sock.groupInviteCode(from);
      await reply(`♻️ Link restablecido:\nhttps://chat.whatsapp.com/${code}`);
    } catch (e) {
      await reply("Necesito ser admin para restablecer el link.");
    }
    break;
  }
  case 'admindebug': {
    if (!isGroup) {
      await reply('Solo en grupos.');
      break;
    }
    const meta = await sock.groupMetadata(from).catch(() => null);
    if (!meta) {
      await reply('No pude leer metadata.');
      break;
    }
    const me = myIds(sock);
    let iAmAdmin = false;
    const lines = (meta.participants || []).map(p => {
      const pid = p?.id || p?.jid || '';
      const role = p?.admin || '-';
      const itsMe = isMePid(pid, sock);
      if (itsMe && (role === 'admin' || role === 'superadmin' || role === true)) iAmAdmin = true;
      return `• ${pid} | admin:${role}${itsMe ? ' 👈 (yo)' : ''}`;
    });
    await reply(
      `Grupo: ${meta.subject || from}
Soy admin: ${iAmAdmin ? '✅' : '❌'}
Mi id(raw): ${me.raw}
Mi num: ${me.num}
Mi @lid: ${me.lid}

${lines.join('\n')}`
    );
    break;
  }
  case "mute": {
    const targets = getMentionedOrQuoted(info);
    if (!targets.length) return reply("Menciona o responde a quien mutear.");
    mutedDB[from] = Array.from(new Set([...(mutedDB[from] || []), ...targets]));
    writeJSON(mutedPath, mutedDB);
    await reply(`🔇 Mute para: ${targets.map(j => `@${j.split("@")[0]}`).join(" ")}`);
    break;
  }
  case "unmute": {
    const targets = getMentionedOrQuoted(info);
    if (!targets.length) return reply("Menciona o responde a quien desmutear.");
    mutedDB[from] = (mutedDB[from] || []).filter(j => !targets.includes(j));
    writeJSON(mutedPath, mutedDB);
    await reply(`🔊 Unmute para: ${targets.map(j => `@${j.split("@")[0]}`).join(" ")}`);
    break;
  }
  case "bobo":
  case "bothijueputa":
  case "perro":
  case "maricon":
  case "botmaricon":
  case "marica":
  case "botmarica":
  case "marika":
  case "botmarika":
  case "hijueputa": {
    await reply("🤬malparido cacorro hijueputa🤬");
    break;
  }
  case "antilink": {
    const on = /^(on|encender|activar|true|1)$/i.test(args[0] || "");
    const off = /^(off|apagar|desactivar|false|0)$/i.test(args[0] || "");
    if (!on && !off) return reply("Uso: .antilink on|off");
    antilinkDB[from] = on;
    writeJSON(antilinkPath, antilinkDB);
    await reply(`Antilink: ${on?"✅ activado":"⛔ desactivado"}.`);
    break;
  }
  case "welcome": {
    const on = /^(on|encender|activar|true|1)$/i.test(args[0] || "");
    const off = /^(off|apagar|desactivar|false|0)$/i.test(args[0] || "");
    if (!on && !off) return reply("Uso: .welcome on|off");
    welcomeDB[from] = on;
    writeJSON(welcomePath, welcomeDB);
    await reply(`Welcome: ${on?"✅ activado":"⛔ desactivado"}.`);
    break;
  }
  case "offwelcome": {
    welcomeDB[from] = false;
    writeJSON(welcomePath, welcomeDB);
    await reply("⛔ Bienvenida desactivada.");
    break;
  }
  case "bye": {
    const on = /^(on|encender|activar|true|1)$/i.test(args[0] || "");
    const off = /^(off|apagar|desactivar|false|0)$/i.test(args[0] || "");
    if (!on && !off) return reply("Uso: .bye on|off");
    goodbyeDB[from] = on;
    writeJSON(goodbyePath, goodbyeDB);
    await reply(`Bye/Despedida: ${on ? "✅ activada" : "⛔ desactivada"}.`);
    break;
  }
  case "offbye": {
    goodbyeDB[from] = false;
    writeJSON(goodbyePath, goodbyeDB);
    await reply("⛔ Despedida desactivada.");
    break;
  }
  case "todos":
  case "tagall": {
    const list = participants.map(p => p.id).slice(0, TAGALL_MAX);
    const textT = "📣 " + (commandTailPreserving(body) || "Llamando a todos:") + "\n\n" + list.map(j => `@${j.split("@")[0]}`).join(" ");
    await sock.sendMessage(from, { text: textT, mentions: list });
    break;
  }
  case "catalogo":
  case "productos": {
    const notas = (CATALOGO_NOTAS || "").trim();
    if (!notas) {
      return reply("📒 (sin notas de catálogo configuradas)\n\nDefine *CATALOGO_NOTAS* en el `.env` para usar este comando.");
    }
    await sock.sendMessage(from, { text: notas }, { quoted: info });
    break;
  }
  case 'menubtn': {
    await sock.sendMessage(from, {
      text: 'Elige una opción:',
      footer: ' ',
      templateButtons: [
        { index: 1, quickReplyButton: { displayText: 'Comprar', id: BTN.BUY } },
        { index: 2, quickReplyButton: { displayText: 'Ver precios', id: BTN.PRICES } },
        { index: 3, quickReplyButton: { displayText: 'Soporte', id: BTN.SUPPORT } },
      ],
    }, { quoted: info });
    break;
  }
case "stock": {
  const icon = "📦";
  const tail = (commandTailPreserving(body) || '').trim().toLowerCase();
  try {
    const { mapByPlain, list } = await safeGetPriceCatalog();

    // Si el catálogo está vacío, sal con mensaje útil
    if (mapByPlain.size === 0 && (!list || !list.length)) {
      await reply('⚠️ No pude leer el *catálogo de Precios*. Verifica permisos/encabezados en Sheets.');
      break;
    }

    // Stock detallado del helper (espera { [plataforma]: { completa:{total, byDuration}, perfil:{...} } })
    const detAll = await SheetsDB.getStockCountsDetailed() || {};
    const allPlats = Object.keys(detAll || {});
    if (!allPlats.length) {
      await reply('No hay stock disponible.');
      break;
    }

    // Mantén solo plataformas que estén en Precios
    const platsInPrices = allPlats.filter(p => {
      const plain = String(p || '')
        .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
        .toLowerCase().replace(/\s+/g,'');
      return mapByPlain.has(plain) ||
             !!resolvePlatFromCatalog(p, mapByPlain); // por si el plain difiere
    });

    // Si el usuario filtró algo (ej: ".stock max" o ".stock netflix")
    let onlyPlats = platsInPrices.slice();
    if (tail) {
      if (tail === 'max') {
        // filtro directo a agrupación MAX
        onlyPlats = platsInPrices.filter(p => String(p).toUpperCase().startsWith('MAX '));
      } else {
        // intenta resolver por canónico desde tail
        const canon = resolvePlatFromCatalog(tail, mapByPlain);
        if (canon) {
          onlyPlats = platsInPrices.filter(p => String(p).toLowerCase() === String(canon).toLowerCase());
          if (!onlyPlats.length) onlyPlats = [canon]; // por si detAll usa canónico exacto
        } else {
          // fallback: substring
          onlyPlats = platsInPrices.filter(p => p.toLowerCase().includes(tail));
        }
      }
    }

    const fmtDurations = (byDuration = {}) => {
      const entries = Object.entries(byDuration)
        .map(([k, v]) => [String(k).toLowerCase(), Number(v) || 0])
        .filter(([, v]) => v > 0)
        .sort((a, b) => {
          // ordena por número de meses si es "Xm"
          const am = parseInt(a[0].replace(/\D/g,''), 10) || 0;
          const bm = parseInt(b[0].replace(/\D/g,''), 10) || 0;
          return am - bm;
        });
      return entries.map(([k, v]) => `  ${k} ${v} und`).join('\n');
    };

    const blocks = [];

    // ---- Bloque agregado MAX (si hay cualquiera que empiece con "MAX ")
    const maxLike = onlyPlats.filter(p => p.toUpperCase().startsWith('MAX '));
    if (maxLike.length) {
      let totalMax = 0;
      const variants = {}; // { variant: { completa:{dur:cnt}, perfil:{...} } }

      for (const p of maxLike) {
        const d = detAll[p] || {};
        let variant = 'default';
        try {
          if (typeof SheetsDB.splitMaxVariant === 'function') {
            variant = SheetsDB.splitMaxVariant(p)?.variant || 'default';
          } else {
            // intenta extraer lo que sigue a "MAX "
            const m = String(p).toUpperCase().slice(4).trim(); // después de MAX
            variant = m || 'default';
          }
        } catch { /* noop */ }

        variants[variant] = variants[variant] || { completa: {}, perfil: {} };

        for (const T of ['completa', 'perfil']) {
          const src = d[T]?.byDuration || {};
          for (const [dur, cnt] of Object.entries(src)) {
            variants[variant][T][dur] = (variants[variant][T][dur] || 0) + (Number(cnt) || 0);
            totalMax += Number(cnt) || 0;
          }
        }
      }

      if (totalMax > 0) {
        let out = `max ${icon} ${totalMax} und\n`;
        for (const T of ['completa', 'perfil']) {
          const any = Object.values(variants).some(v => Object.keys(v[T] || {}).length);
          if (!any) continue;
          out += `* ${T}:\n`;
          for (const [vName, obj] of Object.entries(variants)) {
            const lines = fmtDurations(obj[T]);
            if (lines) out += `  ${String(vName).toLowerCase()}:\n${lines}\n`;
          }
        }
        blocks.push(out.trim());

        // Si el usuario pidió específicamente “max”, termina aquí
        if (tail === 'max') {
          await reply(blocks.join('\n\n'));
          break;
        }
      }
    }

    // ---- Resto de plataformas “normales”
    const rest = onlyPlats.filter(p => !p.toUpperCase().startsWith('MAX '));
    for (const p of rest) {
      const d = detAll[p] || {};
      const total = (d.completa?.total || 0) + (d.perfil?.total || 0);
      if (!total) continue;

      const lines = [];
      lines.push(`${p.toLowerCase()} ${icon} ${total} und`);
      if (d.completa?.total) {
        lines.push(`* completa:`);
        const s = fmtDurations(d.completa.byDuration);
        if (s) lines.push(s);
      }
      if (d.perfil?.total) {
        lines.push(`* perfil:`);
        const s = fmtDurations(d.perfil.byDuration);
        if (s) lines.push(s);
      }
      blocks.push(lines.join('\n'));
    }

    if (!blocks.length) {
      // Mensaje más útil cuando el filtro no encontró nada
      if (tail) {
        await reply(`No hay stock para *${tail}* o no existe en *Precios*.`);
      } else {
        await reply('No hay stock disponible en *Precios*.');
      }
      break;
    }

    await reply(blocks.join('\n\n'));
  } catch (e) {
    console.error('stock error:', e);
    await reply('⚠️ Error obteniendo *stock*. Verifica Precios y tus permisos.');
  }
  break;
}


  case "ayuda": {
    const key = matchKey(args[0] || "");
    if (!key) {
      await reply("Uso: .ayuda <clave>");
      break;
    }
    const txt = (helpDB[from] || {})[key];
    if (!txt) {
      await reply(`(sin ayuda para *${key}*)\n👉 Pide a un admin que use: .addayuda ${key} <texto> (en Control)`);
      break;
    }
    await reply(txt);
    break;
  }
  case "miscompras": {
    if (isGroup) {
      await reply("ℹ️ *Mis compras* solo funciona por *privado* con el bot.");
      break;
    }
    const sameJid = j => jidNormalizedUser(j) === jidNormalizedUser(sender);
    const rows = (salesDB || [])
      .filter(s => sameJid(s.userJid))
      .sort((a, b) => (b.ts || 0) - (a.ts || 0))
      .slice(0, 50);

    if (!rows.length) {
      await reply("Aún no tienes compras registradas.");
      break;
    }
    let total = 0;
    const lines = rows.map(r => {
      const when = new Date(r.ts || Date.now()).toLocaleString('es-MX');
      const qty = Number(r.qty || 0);
      const lineTotal = Number(r.total || 0);
      total += lineTotal;
      return `• ${when} | ${r.key} x${qty} | $${lineTotal.toFixed(2)}`;
    });
    const totalTxt = total.toLocaleString('es-MX', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    await reply(`🧾 *Tus compras*\n\n${lines.join("\n")}\n\n💰 Total gastado: *$${totalTxt}*`);
    break;
  }
  case "soporte": {
    const supportId = await resolveSupportTarget(sock);
    if (!supportId) {
      return reply(
        "⚠️ No encuentro el *Grupo de Soporte*. " +
        `Verifica que el bot esté dentro de "${(SUPPORT_GROUP_NAME || "Soporte")}" ` +
        "o define SUPPORT_GROUP_IDS en .env"
      );
    }
    const tail = (commandTailPreserving(body) || "").trim();
    const quoted = (getQuotedText(info) || "").trim();
    const base = [tail, quoted].filter(Boolean).join("\n").trim();
    const { product, account, desc } = parseSupportInput(base, quoted || tail);
    if (!desc) {
      return reply(
        "Uso: `.soporte <producto?> <cuenta?> <detalle>`\n" +
        "También puedes *responder tu mensaje con el reporte* y escribir `.soporte`."
      );
    }
    const qCtx = info.message?.extendedTextMessage?.contextInfo;
    const originStanza = qCtx?.stanzaId || qCtx?.stanzaID || null;
    if (originStanza && ticketsDB[originStanza]) {
      return reply("📌 Ya existe un ticket para ese mensaje citado. Nuestro equipo lo está atendiendo.");
    }
    const atUser = `@${sender.split("@")[0]}`;
    const ticketText =
      `🆘 *Ticket de soporte*
• Cliente: ${atUser}
• Producto: *${product || "—"}*
• Cuenta: *${account || "—"}*
• Detalle:
${desc}

Responde *citando este mensaje* con:
- "reemplazo ..." para enviar nuevas credenciales
- o "activa/viva/funciona/buena/ok" para informar que está operativa.`;

    let sent;
    try {
      const media = await maybeDownloadQuotedMedia(info);
      if (media?.type === 'image') {
        sent = await sock.sendMessage(
          supportId, { image: media.data, mimetype: media.mimetype || 'image/jpeg', caption: ticketText, mentions: [sender] }
        );
      } else if (media?.type === 'video') {
        sent = await sock.sendMessage(
          supportId, { video: media.data, mimetype: media.mimetype || 'video/mp4', caption: ticketText, mentions: [sender] }
        );
      } else {
        sent = await sock.sendMessage(supportId, { text: ticketText, mentions: [sender] });
      }
    } catch (e) {
      console.error("send support ticket error:", e);
      return reply("⚠️ No pude enviar el ticket al grupo de soporte. Inténtalo de nuevo en un momento.");
    }
    const supportMsgId = sent?.key?.id || null;
    const now = Date.now();
    const ticketKey = supportMsgId || originStanza || `tk_${now}`;

    ticketsDB[ticketKey] = {
      userJid: sender,
      groupJid: from,
      product: product || "",
      account: account || "",
      desc,
      ts: now,
      answered: false,
      reminded: false,
      supportMsgId,
      originStanza: originStanza || null,
      remindAt: now + 2 * 60 * 60 * 1000
    };
    writeJSON(ticketsPath, ticketsDB);

    if (originStanza && !ticketsDB[originStanza]) {
      ticketsDB[originStanza] = { aliasOf: ticketKey, ts: now };
      writeJSON(ticketsPath, ticketsDB);
    }
    await sock.sendMessage(
      from, { text: "✅ *Reporte recibido*. Nuestro equipo ya está trabajando en ello. Te avisaremos por este chat en cuanto tengamos respuesta." }, { quoted: info }
    );
    break;
  }
  case "addpagostreaming":
  case "addpagocine": {
    if (!isControl) return reply("Este comando solo en *Sala de Control*.");

    const kind = name === "addpagostreaming" ? "streaming" : "cine";
    const txt = (getQuotedText(info) || commandTailPreserving(body) || "").trim();
    let savedImage = null;
    try {
      const media = await getImageFromMsgOrQuoted(info);
      if (media && media.buffer) {
        const allowed = /^image\//i.test(media.mimetype || "");
        if (!allowed) {
          await reply("Sólo se permiten *imágenes* para el método de pago.");
        } else {
          const ext = (mime.extension(media.mimetype) || "jpg").toLowerCase();
          const dir = ensureGroupMediaDir();
          const fOut = path.join(dir, `${kind}_${Date.now()}.${ext}`);
          if (media.buffer.length > 5 * 1024 * 1024) {
            return reply("La imagen supera el límite de *5MB*.");
          }
          try {
            fs.writeFileSync(fOut, media.buffer);
            savedImage = { path: fOut, mime: media.mimetype };
          } catch (e) {
            console.error("write pago image err:", e);
            await reply("⚠️ No pude guardar la imagen, guardo solo el texto.");
          }
        }
      }
    } catch (e) {
      console.error("getImageFromMsgOrQuoted err:", e);
    }
    if (!txt && !savedImage) {
      return reply(`Uso: .${name} <texto>  (o responde/adjunta una *imagen*)`);
    }
    paymentsDB[kind] = {
      text: txt || "",
      image: savedImage || null,
      updatedAt: Date.now(),
      updatedBy: sender
    };
    writeJSON(paymentsPath, paymentsDB);
    await reply(`💳 Método de pago (${kind}) actualizado.${savedImage ? " (con imagen)" : ""}`);
    break;
  }
  case "deletepagostreaming":
  case "deletepagocine": {
    if (!isControl) return reply("Este comando solo en *Sala de Control*.");
    const kind = name === "deletepagostreaming" ? "streaming" : "cine";
    try {
      const rec = paymentsDB[kind];
      if (rec?.image?.path) {
        try {
          if (fs.existsSync(rec.image.path)) fs.unlinkSync(rec.image.path);
        } catch (e) {
          console.warn("No pude borrar la imagen del método de pago:", e?.message || e);
        }
      }
      delete paymentsDB[kind];
      writeJSON(paymentsPath, paymentsDB);
      return reply(`🗑️ Método de pago (${kind}) eliminado.`);
    } catch (e) {
      console.error("deletepago error:", e);
      return reply("⚠️ No pude eliminar el método de pago. Revisa logs.");
    }
  }
  case "settarjeta": {
    if (!isControl) return reply("Este comando solo en *Sala de Control*.");
    const kind = /^(streaming|cine)$/i.test(args[0] || "") ? args[0].toLowerCase() : "";
    let gid = (args[1] || "").trim();
    if (!kind || !gid) {
      return reply("Uso: .settarjeta <streaming|cine> <idGrupo>\nEj: .settarjeta streaming 120363012345678901@g.us");
    }
    if (!/@g\.us$/i.test(gid)) {
      return reply("⚠️ El segundo parámetro debe ser un *ID de grupo* válido que termine en @g.us");
    }
    gid = gid.replace(/\s+/g, "");
    if ((CONTROL_GROUP_IDS || []).includes(gid)) {
      return reply("🚫 No puedes asignar método a la *Sala de Control*.");
    }
    let subject = "";
    try {
      const meta = await sock.groupMetadata(gid);
      subject = (meta?.subject || "").trim();
    } catch (e) {
      return reply("❌ No pude leer metadata de ese grupo. Verifica que el bot esté dentro del grupo y el ID sea correcto.");
    }
    try {
      payAssign[gid] = kind;
      writeJSON(payAssignPath, payAssign);
    } catch (e) {
      console.error("settarjeta write error:", e);
      return reply("⚠️ No pude guardar la asignación. Revisa permisos de escritura.");
    }
    return reply(`✅ Grupo *${subject || gid}* asignado a método: *${kind}*`);
  }
  case "tarjeta": {
    if (!isGroup) return reply("Este comando se usa dentro de un *grupo*.");
    const kind = payAssign[from];
    if (!kind) {
      return reply(
        "⚠️ Este grupo no tiene método de pago asignado.\n" +
        "Pide en Control: `.settarjeta <streaming|cine> <idGrupo>`"
      );
    }
    const rec = paymentsDB[kind];
    if (!rec) return reply(`⚠️ No hay contenido configurado para *${kind}* (usa .addpago${kind}).`);
    const caption = String(rec.text || "");
    const mediaInfo = rec.image;
    if (mediaInfo?.path) {
      try {
        if (!fs.existsSync(mediaInfo.path)) {
          return await reply(caption || "(sin contenido)");
        }
        const buf = fs.readFileSync(mediaInfo.path);
        const mime = String(mediaInfo.mime || "");
        if (mime.startsWith("video/")) {
          await sock.sendMessage(from, { video: buf, mimetype: mime || "video/mp4", caption }, { quoted: info });
        } else {
          await sock.sendMessage(from, { image: buf, mimetype: mime || "image/jpeg", caption }, { quoted: info });
        }
        break;
      } catch (e) {
        console.error("tarjeta media send error:", e);
        await reply(caption || "(sin contenido)");
        break;
      }
    }
    await reply(caption || "(sin contenido)");
    break;
  }
  case "bot": {
    const msg =
      `*¡Activa tu acceso al bot ahora! 🤖✨*
*Respuestas rápidas, ventas, soporte y seguridad 🔒*

*Planes:*

*1 mes: $350 MXN*
*2 meses: $680 MXN*
*3 meses: $960 MXN*
*6 meses: $1,860 MXN*
*12 meses: $3,500 MXN*

_*💬 Escríbenos y te activamos en minutos.*_`;
    try {
      if (BOT_VIDEO && fs.existsSync(BOT_VIDEO)) {
        await sock.sendMessage(
          from, { video: fs.readFileSync(BOT_VIDEO), caption: msg, mimetype: "video/mp4" }, { quoted: info }
        );
      } else if (BOT_IMAGE && fs.existsSync(BOT_IMAGE)) {
        await sock.sendMessage(
          from, { image: fs.readFileSync(BOT_IMAGE), caption: msg }, { quoted: info }
        );
      } else {
        await sock.sendMessage(from, { text: msg }, { quoted: info });
      }
    } catch {
      await sock.sendMessage(from, { text: msg }, { quoted: info });
    }
    break;
  }
  case "fantasma": {
    if (!isGroup) {
      await reply("Este comando solo funciona en *grupos*.");
      break;
    }
    const days = Number(FANTASMA_DAYS) > 0 ? Number(FANTASMA_DAYS) : 30;
    const threshold = Date.now() - days * 24 * 60 * 60 * 1000;
    let meta;
    try {
      meta = await sock.groupMetadata(from);
    } catch {
      await reply("No pude leer la lista de miembros.");
      break;
    }
    const meId = String(sock.user?.id || "");
    const participants = Array.isArray(meta?.participants) ? meta.participants : [];
    const members = participants
      .map(p => (p?.id || p?.jid || "").trim())
      .filter(j => j.endsWith("@s.whatsapp.net") || j.endsWith("@g.us"))
      .filter(j => j && j !== meId);
    const last = activityDB[from] || {};
    let ghosts = members.filter(jid => !last[jid] || last[jid] < threshold);
    ghosts.sort((a, b) => (last[a] || 0) - (last[b] || 0));
    if (!ghosts.length) {
      await sock.sendMessage(from, {
        text: `👻 *Usuarios inactivos* (≥ ${days} días)\n\n— Ninguno —`
      }, { quoted: info });
      break;
    }
    const tagLine = jid => `• @${String(jid).split("@")[0]}`;
    const lines = ghosts.map(tagLine);
    const CHUNK = 25;
    for (let i = 0; i < ghosts.length; i += CHUNK) {
      const sliceJids = ghosts.slice(i, i + CHUNK);
      const sliceLines = lines.slice(i, i + CHUNK).join("\n");
      const txt =
        `👻 *Usuarios inactivos* (≥ ${days} días) — ${i + 1}-${Math.min(i + CHUNK, ghosts.length)}/${ghosts.length}

${sliceLines}`;
      await sock.sendMessage(from, { text: txt, mentions: sliceJids }, { quoted: info });
      await new Promise(r => setTimeout(r, 300));
    }
    break;
  }
  case 'saldo':
  case '.saldo':
  case 'misaldo': {
    const CURRENCY = process.env.CURRENCY || 'MXN';
    const CURSYM = process.env.CURRENCY_SYMBOL || '$';
    let target = sender;
    const m = (getMentionedOrQuoted(info) || [])[0] || null;
    if (m && (isOwner(sender) || isReseller(sender))) target = m;
    const balRaw = getBalance(target);
    const bal = Number.isFinite(balRaw) ? balRaw : 0;
    const who = (target === sender) ? 'Tu saldo' : `Saldo de @${String(target).split('@')[0]}`;
    await sock.sendMessage(
      from, { text: `💰 ${who}: *${CURSYM}${bal.toFixed(2)}* ${CURRENCY}`, mentions: target === sender ? [] : [target] }, { quoted: info }
    );
    break;
  }
  case 'saldos': {
    const controls = Array.isArray(CONTROL_GROUP_IDS) ? CONTROL_GROUP_IDS.filter(Boolean) : [];
    const inControl = controls.includes(from);
    const hasPrivs = inControl;
    if (!hasPrivs) {
      return reply('🔒 Solo *Sala de Control* o *OWNER/RESELLER*.');
    }
    const CURRENCY = process.env.CURRENCY || 'MXN';
    const CURSYM = process.env.CURRENCY_SYMBOL || '$';
    const rows = Object.entries(balancesDB || {})
      .map(([jid, val]) => [jid, Number(val) || 0])
      .filter(([, v]) => v > 0)
      .sort((a, b) => b[1] - a[1]);
    if (!rows.length) {
      await reply('No hay saldos positivos.');
      break;
    }
    const makeLines = (slice) =>
      slice.map(([jid, v]) => `• @${jid.split('@')[0]}: ${CURSYM}${v.toFixed(2)} ${CURRENCY}`).join('\n');
    const CHUNK_SIZE = 30;
    const header = '📊 *Saldos (positivos)*';
    const chunks = [];
    for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
      chunks.push(rows.slice(i, i + CHUNK_SIZE));
    }
    const targets = inControl || !controls.length ? [from] : controls;
    for (const gid of targets) {
      for (let idx = 0; idx < chunks.length; idx++) {
        const chunk = chunks[idx];
        const mentions = chunk.map(([jid]) => jid);
        const body = `${header}${chunks.length > 1 ? ` (parte ${idx + 1}/${chunks.length})` : ''}\n\n${makeLines(chunk)}`;
        await sock.sendMessage(gid, { text: body, mentions }, { quoted: info });
        await new Promise(r => setTimeout(r, 250));
      }
    }
    if (!inControl && controls.length) {
      await reply('📤 Envié el reporte de saldos a la *sala de control*.');
    }
    break;
  }
  case "addsaldo":
  case "cargarsaldo": {
    if (!canUseSaldo(sender)) {
      return reply("🔒 Solo *OWNERS* o *RESELLERS* pueden usar *cargarsaldo*.");
    }
    const CURRENCY = process.env.CURRENCY || 'MXN';
    const CURSYM = process.env.CURRENCY_SYMBOL || '$';
    const MAX_TOPUP = Number(process.env.MAX_TOPUP || 50000);
    let target = (getMentionedOrQuoted(info) || [])[0] || null;
    if (!target) {
      const textAll = commandTailPreserving(body) || "";
      const m = textAll.match(/(?:@|\+)?(\d{6,})/);
      if (m && m[1]) {
        let phone = m[1].replace(/\D/g, "");
        if (phone.length <= 10 && !phone.startsWith(DEFAULT_CC)) phone = DEFAULT_CC + phone;
        target = `${phone}@s.whatsapp.net`;
      }
    }
    if (target) {
      const tgtNum = String(target).split("@")[0].replace(/\D/g, "");
      const botNum = String(sock.user?.id || "").split("@")[0].replace(/\D/g, "");
      if (tgtNum && botNum && tgtNum === botNum) {
        return reply("🤖 No puedo recargar saldo al *bot*.");
      }
    }
    const tail = commandTailPreserving(body) || "";
    const mAmt = tail.match(/(-?\d[\d.,]*)\s*$/);
    const parseAmount = (s) => {
      if (!s) return NaN;
      let x = s.trim();
      if (/,/.test(x) && /\./.test(x)) {
        x = x.replace(/\./g, '').replace(',', '.');
      } else if (/,/.test(x) && !/\./.test(x)) {
        x = x.replace(',', '.');
      } else {
        x = x.replace(/(?<=\d)\,(?=\d{3}\b)/g, '');
      }
      return Number(x);
    };
    const amount = parseAmount(mAmt ? mAmt[1] : "");
    if (!target || !isFinite(amount) || amount <= 0) {
      return reply(
        "Uso: .cargarsaldo @usuario <monto_positivo>\n" +
        "También puedes responder a un mensaje del usuario o escribir su número.\n" +
        "Ej: .cargarsaldo @Fulanito 500"
      );
    }
    if (amount > MAX_TOPUP) {
      return reply(`⚠️ Monto demasiado alto. Límite por operación: ${CURSYM}${MAX_TOPUP.toFixed(2)} ${CURRENCY}.`);
    }
    if (isReseller(sender) && (isOwner(target) || isReseller(target))) {
      return reply("🚫 Un *RESELLER* no puede ajustar saldo de *OWNERS* ni de otros *RESELLERS*.");
    }
    const safeAmount = Math.round(amount * 100) / 100;
    const next = addBalance(target, safeAmount);
    await sock.sendMessage(
      from, { text: `💳 Saldo para @${target.split("@")[0]}: *${CURSYM}${next.toFixed(2)}* ${CURRENCY} (global)\n➕ Carga: *${CURSYM}${safeAmount.toFixed(2)}*`, mentions: [target] }, { quoted: info }
    );
    try {
      await sock.sendMessage(target, {
        text: `✅ Se te cargaron *${CURSYM}${safeAmount.toFixed(2)}* ${CURRENCY}.\nNuevo saldo: *${CURSYM}${next.toFixed(2)}* ${CURRENCY}.`
      });
    } catch {}
    break;
  }
  case "quitarsaldo":
  case "quitasaldo":
  case "delsaldo": {
    if (!canUseSaldo(sender)) {
      return reply("🔒 Solo *OWNERS* o *RESELLERS* pueden usar *quitarsaldo*.");
    }
    const CURRENCY = process.env.CURRENCY || 'MXN';
    const CURSYM = process.env.CURRENCY_SYMBOL || '$';
    const MAX_DEDUCT = Number(process.env.MAX_DEDUCT || 50000);
    let target = (getMentionedOrQuoted(info) || [])[0] || null;
    if (!target) {
      const textAll = commandTailPreserving(body) || "";
      const m = textAll.match(/(?:@|\+)?(\d{6,})/);
      if (m && m[1]) {
        let phone = m[1].replace(/\D/g, "");
        if (phone.length <= 10 && !phone.startsWith(DEFAULT_CC)) phone = DEFAULT_CC + phone;
        target = `${phone}@s.whatsapp.net`;
      }
    }
    if (target) {
      const tgtNum = String(target).split("@")[0].replace(/\D/g, "");
      const botNum = String(sock.user?.id || "").split("@")[0].replace(/\D/g, "");
      if (tgtNum && botNum && tgtNum === botNum) {
        return reply("🤖 No puedo ajustar saldo del *bot*.");
      }
    }
    const tail = commandTailPreserving(body) || "";
    const mAmt = tail.match(/(-?\d[\d.,]*)\s*$/);
    const parseAmount = (s) => {
      if (!s) return NaN;
      let x = s.trim();
      if (/,/.test(x) && /\./.test(x)) {
        x = x.replace(/\./g, '').replace(',', '.');
      } else if (/,/.test(x) && !/\./.test(x)) {
        x = x.replace(',', '.');
      } else {
        x = x.replace(/(?<=\d)\,(?=\d{3}\b)/g, '');
      }
      return Number(x);
    };
    const amount = parseAmount(mAmt ? mAmt[1] : "");
    if (!target || !isFinite(amount) || amount <= 0) {
      return reply(
        "Uso: .quitarsaldo @usuario <monto_positivo>\n" +
        "También puedes responder a un mensaje del usuario o escribir su número.\n" +
        "Ej: .quitarsaldo @Fulanito 300"
      );
    }
    if (amount > MAX_DEDUCT) {
      return reply(`⚠️ Monto demasiado alto. Límite por operación: ${CURSYM}${MAX_DEDUCT.toFixed(2)} ${CURRENCY}.`);
    }
    if (isReseller(sender) && (isOwner(target) || isReseller(target))) {
      return reply("🚫 Un *RESELLER* no puede ajustar saldo de *OWNERS* ni de otros *RESELLERS*.");
    }
    const safeAmount = Math.round(amount * 100) / 100;
    const next = addBalance(target, -safeAmount);
    await sock.sendMessage(
      from, {
        text: `💳 Saldo para @${target.split("@")[0]}: *${CURSYM}${next.toFixed(2)}* ${CURRENCY} (global)\n` +
          `🧾 Movimiento: -*${CURSYM}${safeAmount.toFixed(2)}*`,
        mentions: [target]
      }, { quoted: info }
    );
    try {
      await sock.sendMessage(target, {
        text: `⚠️ Se te descontaron *${CURSYM}${safeAmount.toFixed(2)}* ${CURRENCY}.\nNuevo saldo: *${CURSYM}${next.toFixed(2)}* ${CURRENCY}.`
      });
    } catch {}
    break;
  }
  case 'ventalotes':
  case 'lotes':
  case 'venta_lotes': {
    try {
      if (!isOwner(sender)) {
        return reply('🔒 Solo *OWNERS* pueden cambiar este ajuste.');
      }
      if (!isInControlRoom(info)) {
        return reply('🏷️ Este ajuste global solo puede hacerse desde la *Sala de Control*.');
      }
      const arg = (args[0] || '').toLowerCase();
      const val = parseOnOff(arg);
      if (val === true) {
        await setVentaLotes(true);
        return reply('✅ *Venta por lotes* ACTIVADA.\nAhora puedes usar comandos como `.addlote` o `.comprarlote`.');
      }
      if (val === false) {
        await setVentaLotes(false);
        return reply('🟡 *Venta por lotes* DESACTIVADA.\nLas compras vuelven al modo *individual*.');
      }
      const flag = await isVentaLotesOn();
      return reply(
        `⚙️ *Venta por lotes:* ${flag ? 'ON ✅' : 'OFF 🟡'}\n` +
        `Usa: *.ventalotes on* | *.ventalotes off*\n` +
        `Sinónimos: on/encender/activar | off/apagar/desactivar`
      );
    } catch (err) {
      console.error('ventalotes error:', err);
      return reply('❌ Ocurrió un error al ajustar *Venta por lotes*. Intenta de nuevo.');
    }
    break;
  }
case 'comprar': {
  const text = (commandTailPreserving(body) || '').trim();
  if (!text) {
    await reply(
      'Uso: .comprar <plataforma> [completa|perfil] <duración>\n' +
      'Ej: .comprar max estandar 1m\n' +
      'Ej: .comprar canva pro completa 3m'
    );
    break;
  }

  // ---- helpers locales para parsear ----
  const pickType = (words) => {
    const idx = words.findIndex(w => /^(completa|perfil)$/i.test(w));
    return idx >= 0 ? { idx, type: words[idx].toLowerCase() } : { idx: -1, type: '' };
  };
  const normalizeDur = (s='') => {
    const t = String(s).trim().toLowerCase();
    if (/^\d+$/.test(t)) return `${t}m`;
    if (/^\d+\s*m$/.test(t)) return t.replace(/\s+/g,'');
    if (/^\d+m$/.test(t)) return t;
    const m = t.match(/^(\d+)\s*m(?:es(?:es)?)?$/); // "1 mes"/"3 meses"
    return m ? `${m[1]}m` : t;
  };

  try {
    // 1) Separar en tokens y extraer plataforma / tipo / duración
    const toks = text.split(/\s+/).filter(Boolean);
    if (!toks.length) {
      await reply('Indica la plataforma. Ej: `.comprar netflix 1m` o `.comprar max estandar completa 3m`');
      break;
    }

    // Detectar duración (último token que parezca duración)
    let duration = '';
    if (toks.length) {
      const last = toks[toks.length - 1];
      const cand = normalizeDur(last);
      if (/^\d+m$/.test(cand)) {
        duration = cand;
        toks.pop();
      }
    }
    if (!duration) duration = '1m'; // por defecto

    // Detectar tipo (completa|perfil) en el resto
    let { idx: idxType, type } = pickType(toks);
    if (idxType >= 0) toks.splice(idxType, 1);
    type = type || 'completa';

    // Lo que queda es la plataforma (pueden ser 1+ palabras)
    const platformQuery = toks.join(' ').trim();
    if (!platformQuery) {
      await reply('Especifica la *plataforma*. Ej: `.comprar spotify 1m`');
      break;
    }

    // 2) Resolver nombre canónico tal como está en *Precios*
    const canonPlatform = await resolvePlatformNameOrNull(platformQuery);
    if (!canonPlatform) {
      try {
        const { list } = await SheetsDB.getPriceCatalog();
        const plats = [...new Set((list || []).map(r => r.plataforma))].slice(0, 20);
        await reply(
          '❌ Plataforma no encontrada en *Precios*.\n\nEjemplos:\n- ' +
          plats.join('\n- ')
        );
      } catch {
        await reply('❌ Plataforma no encontrada en *Precios*.');
      }
      break;
    }

    // 3) Ver precios disponibles para esa plataforma
    const priceRows = await SheetsDB.listPricesByPlatform(canonPlatform);
    if (!priceRows.length) {
      await reply('❌ Plataforma no encontrada en *Precios*.');
      break;
    }

    // Filtrar por duración solicitada
    const rowsDur = priceRows.filter(r => String(r.duracion || '').toLowerCase() === duration);
    if (!rowsDur.length) {
      const durs = [...new Set(priceRows.map(r => r.duracion))].join(', ');
      await reply(`❌ Duración no disponible para *${priceRows[0].plataforma}*.\nDisponibles: ${durs}`);
      break;
    }

    // Elegir fila por tipo (si no hay, toma la primera de esa duración)
    const chosenRow = rowsDur.find(r => String(r.tipo || '').toLowerCase() === type) || rowsDur[0];

    const platformName = canonPlatform; // usar canónico en todo
    const realType     = String(chosenRow.tipo || type || 'completa').toLowerCase();
    const price        = Number(chosenRow.precio || 0) || 0;
    const currency     = chosenRow.moneda || 'MXN';

    // 4) Validar saldo
    const bal = getBalance(sender);
    if (price > 0 && bal < price) {
      await reply(`💳 Saldo insuficiente. Necesitas *${price} ${currency}*, tienes *${bal.toFixed(2)}*.`);
      break;
    }

    // 5) Intentar tomar 1 cuenta del stock
    const wantDurations = [duration];
    const asMeses = (() => {
      const n = parseInt(String(duration).replace(/\D/g,''), 10) || 0;
      return n ? (n === 1 ? '1 mes' : `${n} meses`) : '';
    })();
    if (asMeses) wantDurations.push(asMeses);

    const soldToTag   = `wa:${sender.split('@')[0]}`;
    const orderPrefix = `ORD-${Date.now().toString(36).toUpperCase()}`;
    const buyerPhone  = sender.split('@')[0];
    const sellerName  = isOwner(sender) ? 'owner' : (isReseller(sender) ? 'reseller' : 'bot');

    let picked = [];
    for (const d of wantDurations) {
      try {
        picked = await SheetsDB.takeNFreeFromViewAndMark(
          platformName, realType, d, 1, soldToTag, orderPrefix, buyerPhone, sellerName
        );
        if (picked && picked.length) break;
      } catch {}
    }

    if (!picked || !picked.length) {
      await reply('😕 Sin stock para esa combinación.');
      break;
    }

    const acc     = picked[0];
    const orderId = acc.order_id || `${orderPrefix}-1`;

    // 6) Descontar saldo y registrar venta
    if (price > 0) addBalance(sender, -price);
    try {
      await SheetsDB.appendSaleLog({
        platform: platformName,
        plan: realType,
        username: acc.username || '',
        password: acc.password || '',
        extra: acc.perfil ? `perfil:${acc.perfil}${acc.pin ? ' pin:'+acc.pin : ''}` : '',
        price,
        currency,
        code: orderId,
      });
    } catch {}

    // 7) Entregar por DM y avisar en el chat
    await sock.sendMessage(sender, {
      text:
`🎫 *Tu compra está lista*
• Plataforma: *${platformName}*
• Tipo: *${realType}*
• Duración: *${duration}*
• Usuario: \`${acc.username || '-'}\`
• Clave: \`${acc.password || '-'}\`
${acc.perfil ? `• Perfil: ${acc.perfil}\n` : ''}${acc.pin ? `• PIN: ${acc.pin}\n` : ''}• Pedido: \`${orderId}\`
${price>0 ? `• Cargo: ${price} ${currency}\n• Saldo restante: ${getBalance(sender).toFixed(2)}` : ''}`
    });

    await reply(
      `🛒 *Pedido completado*\n• ${platformName} • ${realType} • ${duration}\nRevisé tu DM y te envié las credenciales ✅`
    );
  } catch (e) {
    console.error('cmd .comprar error:', e);
    await reply('⚠️ Error procesando la compra.');
  }
  break;
}

case 'preciolote': {
  const text = (commandTailPreserving(body) || '').trim();
  if (!text) {
    await reply('Uso:\n• .preciolote <plataforma>\n• .preciolote <plataforma> <duración> [completa|perfil]\nEj: .preciolote max estandar 1m');
    break;
  }

  const toks = text.split(/\s+/);
  const isDur = t => /^\d+m$/.test(t) || /^\d+$/.test(t) || /^(\d+)\s*m(?:es(?:es)?)?$/i.test(t);
  const normDur = t => {
    t = String(t || '').toLowerCase().trim();
    if (/^\d+$/.test(t)) return `${t}m`;
    if (/^\d+m$/.test(t)) return t;
    const m = t.match(/^(\d+)\s*m(?:es(?:es)?)?$/);
    return m ? `${m[1]}m` : t;
  };

  let idxDur = toks.findIndex(isDur);
  let duration = idxDur >= 0 ? normDur(toks[idxDur]) : '';
  let before = idxDur >= 0 ? toks.slice(0, idxDur) : toks.slice();
  let type = null;

  if (before.length && /^(completa|perfil)$/i.test(before[before.length - 1])) {
    type = before.pop().toLowerCase();
  }

  const platQuery = before.join(' ').trim();
  if (!platQuery) {
    await reply('❗ Indica la plataforma. Ej: .preciolote max estandar 1m');
    break;
  }

  try {
    // Busca filas de precios para la plataforma
    const items = await SheetsDB.listPricesByPlatform(platQuery);
    if (!items.length) {
      await reply('❌ Plataforma no encontrada en *Precios*.');
      break;
    }

    const platformName = items[0].plataforma;

    // Si no dieron duración, muestra disponibles
    if (!duration) {
      const durDisponibles = new Set(items.map(r => String(r.duracion || '').toLowerCase()).filter(Boolean));
      const lista = [...durDisponibles].sort().join(', ');
      await reply(`🗂️ *${platformName}* — consulta por lotes:\nDuraciones disponibles (según Precios): ${lista}\n\nPide una en específico:\n.preciolote ${platQuery} 1m`);
      break;
    }

    const rowsDur = items.filter(r => String(r.duracion || '').toLowerCase() === duration);
    if (!rowsDur.length) {
      const durs = [...new Set(items.map(r => r.duracion))].join(', ');
      await reply(`❌ Duración no disponible para *${platformName}*.\nDisponibles: ${durs}`);
      break;
    }

    if (!type) {
      const tipos = [...new Set(rowsDur.map(r => (r.tipo || '').toLowerCase()).filter(Boolean))];
      if (tipos.length > 1) {
        await reply(`⚠️ Hay varios tipos (${tipos.join(', ')}).\nUsa: .preciolote ${platQuery} ${duration} completa`);
        break;
      }
      type = (rowsDur[0].tipo || 'completa').toLowerCase();
    }

    const lot = 10; // tamaño de lote por defecto
    const p = await SheetsDB.findLotPriceRow({
      plataforma: platformName,
      tipo: type,
      duracion: duration,
      lote: lot
    });

    if (!p) {
      await reply('⚠️ No hay precio configurado en *Precios_Lotes* para esa combinación.');
      break;
    }

    // Campos posibles en la fila (ponemos fallbacks por si tu hoja usa otros nombres)
    const lotPrice = Number(p.precio_lote ?? p.precio ?? p.total ?? 0);
    const currency = p.moneda || p.currency || 'MXN';
    const unit = lotPrice && lot ? (lotPrice / lot) : 0;

    await reply(
      `📦 *${platformName}* • ${type} • ${duration}\n` +
      `Lote de ${lot}: *${lotPrice} ${currency}*\n` +
      `Precio unitario aprox.: *${unit.toFixed(2)} ${currency}*`
    );
  } catch (e) {
    console.error('preciolote error:', e);
    await reply('⚠️ Error consultando precios por lote.');
  }
  break;
}
  case "precios": {
    const text = (commandTailPreserving(body) || '').trim();
    if (!text) {
      await reply('Uso:\n• .precio <plataforma>\n• .precio <plataforma> <duración>\nEj: .precio max estandar\nEj: .precio max estandar 1m');
      break;
    }
    const toks = text.split(/\s+/);
    const isDur = (t) => {
      const s = String(t || '').toLowerCase();
      return /^\d+m$/.test(s) || /^\d+$/.test(s) || /^(\d+)\s*m(?:es(?:es)?)?$/.test(s);
    };
    const normDur = (t) => {
      const s = String(t || '').toLowerCase().trim();
      if (/^\d+$/.test(s)) return `${s}m`;
      if (/^\d+m$/.test(s)) return s;
      const m = s.match(/^(\d+)\s*m(?:es(?:es)?)?$/);
      return m ? `${m[1]}m` : s;
    };
    let duration = '';
    if (toks.length >= 1 && isDur(toks[toks.length - 1])) {
      duration = normDur(toks.pop());
    }
    const platQuery = toks.join(' ');
    if (!platQuery) {
      await reply('❗ Indica la plataforma. Ej: .precio max estandar 1m');
      break;
    }
    try {
      const items = await SheetsDB.listPricesByPlatform(platQuery);
      if (!items.length) {
        await reply('❌ No encontré esa plataforma en *Precios*. Verifica el nombre tal como está en la hoja.');
        break;
      }
      const platformName = items[0].plataforma;
      if (duration) {
        const rowsDur = items.filter(r => String(r.duracion || '').toLowerCase() === duration);
        if (!rowsDur.length) {
          const durs = [...new Set(items.map(r => r.duracion))].join(', ');
          await reply(`❌ Duración no disponible para *${platformName}*.\nDisponibles: ${durs}`);
          break;
        }
        const lines = rowsDur.map(r => {
          const tipo = (r.tipo || '').toLowerCase() || 'completa';
          const price = Number(r.precio || 0) || 0;
          const cur = r.moneda || 'MXN';
          return `• ${tipo}: *${price} ${cur}*`;
        });
        await reply(
          `💲 *${platformName}* • ${duration}\n` +
          (lines.join('\n') || '—')
        );
        break;
      }
      const map = new Map();
      for (const r of items) {
        const dur = String(r.duracion || '').toLowerCase();
        const tipo = String(r.tipo || 'completa').toLowerCase();
        const price = Number(r.precio || 0) || 0;
        const cur = r.moneda || 'MXN';
        if (!map.has(dur)) map.set(dur, new Map());
        map.get(dur).set(tipo, { price, cur });
      }
      const order = (a, b) => (parseInt(a) || 0) - (parseInt(b) || 0);
      const durKeys = [...map.keys()].sort(order);
      const blocks = durKeys.map(dur => {
        const tipos = map.get(dur);
        const parts = [];
        for (const [tipo, obj] of tipos.entries()) {
          parts.push(`  • ${tipo}: *${obj.price} ${obj.cur}*`);
        }
        return `${dur}\n${parts.join('\n')}`;
      });
      await reply(`💲 *${platformName}*\n\n${blocks.join('\n\n')}`);
    } catch (e) {
      console.error('precio error:', e);
      await reply('⚠️ Error leyendo *Precios*. Revisa que la hoja esté accesible y con encabezados correctos.');
    }
    break;
  }
  case "ventas24":
  case "ventas48":
  case "ventas": {
    if (!isControl) return reply(`Este comando solo en *Sala de Control* (${CONTROL_GROUP_NAME || "defínela en .env"}) o DM de *OWNER*.`);
    let start = 0,
      end = Date.now();
    if (name === "ventas24" || name === "ventas48") {
      const hours = name === "ventas24" ? 24 : 48;
      start = Date.now() - hours * 60 * 60 * 1000;
    } else if (args.length) {
      const joined = args.join(" ").trim();
      const m = joined.match(/^([^:]+):(.+)$/);
      if (m) {
        const d1 = tryParseDate(m[1]);
        const d2 = tryParseDate(m[2]);
        if (d1 && d2) {
          start = d1.getTime();
          end = d2.getTime() + 24 * 60 * 60 * 1000 - 1;
        }
      }
    }
    const rows = salesDB.filter(s => s.ts >= start && s.ts <= end).slice(-2000);
    if (!rows.length) return reply("No hay ventas en el periodo solicitado.");
    let total = 0;
    const lines = rows.map(r => {
      total += r.total;
      const when = new Date(r.ts).toLocaleString();
      const user = `@${r.userJid.split("@")[0]}`;
      const tag = r.first ? ` | ${r.first}` : "";
      return `${when} | ${user} | ${r.key} x${r.qty}${tag} | $${(r.total).toFixed(2)}`;
    });
    const mentions = rows.map(r => r.userJid);
    const header = (start ? `🧾 Ventas (todas las sucursales) desde ${new Date(start).toLocaleDateString()} hasta ${new Date(end).toLocaleDateString()}` : "🧾 Ventas recientes (todas las sucursales)");
    await sock.sendMessage(from, { text: `${header}\n\n${lines.join("\n")}\n\n💰 Total: $${total.toFixed(2)}`, mentions }, { quoted: info });
    break;
  }
  case "infovendidas": {
    if (!isControl) return reply(`Este comando solo en *Sala de Control* (${CONTROL_GROUP_NAME || "defínela en .env"}) o DM de *OWNER*.`);
    let start = 0,
      end = Date.now(),
      keyFilter = null;
    function parseRangeToken(tok) {
      tok = String(tok || "").trim();
      if (!tok) return false;
      if (/^\d{2,3}$/.test(tok)) {
        start = Date.now() - Number(tok) * 60 * 60 * 1000;
        end = Date.now();
        return true;
      }
      const m = tok.match(/^(\d{4}-\d{2}-\d{2})[:](\d{4}-\d{2}-\d{2})$/);
      if (m) {
        const d1 = tryParseDate(m[1]);
        const d2 = tryParseDate(m[2]);
        if (d1 && d2) {
          start = d1.getTime();
          end = d2.getTime() + 24 * 60 * 60 * 1000 - 1;
          return true;
        }
      }
      return false;
    }
    if (args.length === 1) {
      if (!parseRangeToken(args[0])) {
        const mk = matchKey(args[0]);
        if (mk) keyFilter = mk;
      }
    } else if (args.length >= 2) {
      const mk = matchKey(args[0]);
      if (mk) keyFilter = mk;
      const tail = args.slice(mk ? 1 : 0).join("").trim();
      if (tail) parseRangeToken(tail);
    }
    const rows = salesDB.filter(s => s.ts >= start && s.ts <= end && (!keyFilter || s.key === keyFilter))
      .sort((a, b) => (a.key || "").localeCompare(b.key || "") || a.ts - b.ts);
    if (!rows.length) return reply("No hay ventas en el periodo/criterio solicitado.");
    const byKey = {};
    const mentions = new Set();
    for (const r of rows) {
      const k = r.key || "—";
      byKey[k] = byKey[k] || [];
      const buyerTag = `@${(r.userJid||"").split("@")[0]}`;
      mentions.add(r.userJid);
      const when = new Date(r.ts).toLocaleString();
      const email = r.first || "(sin correo)";
      byKey[k].push(`• ${email} — ${when} — ${buyerTag}`);
    }
    const orderKeys = Object.keys(byKey).sort((a, b) => a.localeCompare(b));
    let header = "🧾 *Informe de cuentas vendidas*";
    if (start) header += `\nPeriodo: ${new Date(start).toLocaleDateString()} a ${new Date(end).toLocaleDateString()}`;
    if (keyFilter) header += `\nProducto: *${keyFilter.toUpperCase()}*`;
    const blocks = [header, ""];
    for (const k of orderKeys) {
      blocks.push(`*${k.toUpperCase()}*`);
      blocks.push(byKey[k].join("\n"));
      blocks.push("");
    }
    await sock.sendMessage(from, { text: blocks.join("\n"), mentions: Array.from(mentions) }, { quoted: info });
    break;
  }
  case "add": {
    if (!isGroup) return reply("Este comando solo funciona dentro de un *grupo*.");
    if (!isAdmin) return reply("Solo los *administradores* pueden usar .add.");
    const nums = (args.join(" ") || getQuotedText(info) || "").match(/\d{6,}/g) || [];
    if (!nums.length)
      return reply(`Uso: .add <número(s)>\nEj: .add 3115284260 5522334455\n(Si el número no tiene LADA, se usa ${DEFAULT_CC})`);
    const jids = Array.from(new Set(nums.map(n => {
      let phone = n.replace(/\D/g, "");
      if (/^\d{6,}$/.test(phone) && !/^(\d{8,})@s\.whatsapp\.net$/.test(phone)) {
        if (phone.length <= 10 && !phone.startsWith(DEFAULT_CC)) phone = DEFAULT_CC + phone;
      }
      return phone + "@s.whatsapp.net";
    })));
    try {
      await sock.groupParticipantsUpdate(from, jids, "add");
      await reply(`➕ Intentando agregar:\n${jids.map(j => `• ${j.split("@")[0]}`).join("\n")}`);
    } catch (e) {
      await reply("No pude agregar alguno(s) de los números. Verifica formato o privacidad.");
    }
    break;
  }

  default: {
    await reply("Comando no reconocido. Usa .menu");
    break;
  }
  }

const WELCOME_CAPTION = `
┏🌸 𝙷𝙾𝙻𝙰 {name}......
┣💋 𝙱𝙸𝙴𝙽𝚅𝙴𝙽𝙸𝙳𝙰/𝙾 💋
┣━━━━━━💞━━━━━━━┓
┣ 💖 PORTATE BIEN 💖
┗━━━━━━💞━━━━━━━┛
@ʙʏ Faver bot ❤‍🔥`;

const GOODBYE_CAPTION = `
┏🌸 𝙰𝙳𝙸𝙾𝚂 {name} 💓
┣━━━━━━💞━━━━━━━┓
┣  ✨ 𝙷𝙰𝚂𝚃𝙰 𝙽𝚄𝙽𝙲𝙰  ✨ 
┃                      𝚈
┣🎀 𝙽𝙾 𝚁𝙴𝙶𝚁𝙴𝚂𝙴𝚂 😌👌 🎀
┗━━━━━━🎀━━━━━━━┛
@ʙʏ Faver bot 💕`;


// ========= Funciones para configurar el mensaje de bienvenida y despedida con imágenes =========

sock.ev.on("group-participants.update", async (u) => {
  try {
    const { id: groupJid, participants, action } = u;

    // Lee metadatos del grupo (sin romper si falla)
    const groupMeta = await sock.groupMetadata(groupJid).catch(() => null);
    if (!groupMeta) return;

    // ✅ Detectar SI soy admin usando JID normalizado (robusto con multi-device)
    const admins = (groupMeta.participants || [])
      .filter(p => p?.admin)
      .map(p => jidNormalizedUser(p.id));

    const iAmAdmin = admins.includes(jidNormalizedUser(sock.user.id));
    if (!iAmAdmin) return; // Para bienvenida/despedida quieres que el bot sea admin

    // Respeta switches por chat
    const welcomeOn = (welcomeDB[groupJid] ?? WELCOME_DEFAULT);
    const byeOn     = (goodbyeDB[groupJid] ?? GOODBYE_DEFAULT);
    const groupName = groupMeta?.subject || "este grupo";

    // participants puede venir como ['xxx@s.whatsapp.net', ...] o [{id:'...'}, ...]
    for (const p of (participants || [])) {
      const jid = (typeof p === "string") ? p : p?.id;
      if (!jid) continue;

      const at = `@${jid.split("@")[0]}`;

      if (action === "add" && welcomeOn) {
        const caption = WELCOME_CAPTION.replace("{name}", at).replace("{group}", groupName);
        try {
          if (WELCOME_IMAGE) {
            if (/^https?:\/\//i.test(WELCOME_IMAGE)) {
              await sock.sendMessage(groupJid, { image: { url: WELCOME_IMAGE }, caption, mentions: [jid] });
            } else {
              await sock.sendMessage(groupJid, { image: fs.readFileSync(WELCOME_IMAGE), caption, mentions: [jid] });
            }
          } else {
            await sock.sendMessage(groupJid, { text: caption, mentions: [jid] });
          }
        } catch {}
      }

      if (action === "remove" && byeOn) {
        const captionBye = GOODBYE_CAPTION.replace("{name}", at).replace("{group}", groupName);
        try {
          if (GOODBYE_IMAGE) {
            if (/^https?:\/\//i.test(GOODBYE_IMAGE)) {
              await sock.sendMessage(groupJid, { image: { url: GOODBYE_IMAGE }, caption: captionBye, mentions: [jid] });
            } else {
              await sock.sendMessage(groupJid, { image: fs.readFileSync(GOODBYE_IMAGE), caption: captionBye, mentions: [jid] });
            }
          } else {
            await sock.sendMessage(groupJid, { text: captionBye, mentions: [jid] });
          }
        } catch {}
      }
    }
  } catch (e) {
    console.error("Error en bienvenida/despedida: ", e);
  }
  }
}); 

}
startSock().catch((e) => {
  console.error('Fatal start error:', e?.message || e);
});
