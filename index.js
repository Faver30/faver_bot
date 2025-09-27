// ========== B-Bot Secure (Baileys) – Global Store (precios/cuentas), Ventas globales, Stock global, Soporte ==========
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  jidNormalizedUser,
  downloadContentFromMessage
} = require("@whiskeysockets/baileys");
const Pino = require("pino");
const qrcode = require("qrcode-terminal");
const { Boom } = require("@hapi/boom");
const mime = require("mime-types");
const math = require("mathjs");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

/* ========= CONFIG ========= */
const OWNERS = (process.env.OWNERS || "").split(",").map(s => s.trim()).filter(Boolean);
const ANTILINK_DEFAULT = (process.env.ANTILINK_DEFAULT || "false") === "true";
const WELCOME_DEFAULT  = (process.env.WELCOME_DEFAULT  || "true") === "true";
const RATE_LIMIT_PER_MINUTE = Number(process.env.RATE_LIMIT_PER_MINUTE || 12);
const TAGALL_MAX = Number(process.env.TAGALL_MAX || 80);
const DEFAULT_CC = process.env.DEFAULT_CC || "57";
const FANTASMA_DAYS = Number(process.env.FANTASMA_DAYS || 7);

/* === Grupos especiales === */
const CONTROL_GROUP_NAME = (process.env.CONTROL_GROUP_NAME || "").trim();
const CONTROL_GROUP_IDS  = (process.env.CONTROL_GROUP_IDS  || "").split(",").map(s => s.trim()).filter(Boolean);

const SUPPORT_GROUP_NAME = (process.env.SUPPORT_GROUP_NAME || "").trim();
const SUPPORT_GROUP_IDS  = (process.env.SUPPORT_GROUP_IDS  || "").split(",").map(s => s.trim()).filter(Boolean);

/* === Dirs === */
const DATA_DIR  = path.join(__dirname, "data");
const QR_DIR    = path.join(__dirname, "qr_code");
const MEDIA_DIR = path.join(DATA_DIR, "media");
if (!fs.existsSync(DATA_DIR))  fs.mkdirSync(DATA_DIR,  { recursive: true });
if (!fs.existsSync(QR_DIR))    fs.mkdirSync(QR_DIR,    { recursive: true });
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

/* ========= Rutas ========= */
const antilinkPath  = path.join(DATA_DIR, "antilink.json");
const welcomePath   = path.join(DATA_DIR, "welcome.json");
const aliasesPath   = path.join(DATA_DIR, "aliases.json");
const productsPath  = path.join(DATA_DIR, "products.json");   // catálogos por GRUPO (texto/imagen)
const helpPath      = path.join(DATA_DIR, "help.json");       // ayudas por GRUPO (texto)
const mutedPath     = path.join(DATA_DIR, "muted.json");
const activityPath  = path.join(DATA_DIR, "activity.json");

/* === GLOBAL STORE (precios/cuentas/sales/saldo) === */
const balancesPath  = path.join(DATA_DIR, "balances.json");   // { userJid: number }
const accountsPath  = path.join(DATA_DIR, "accounts.json");   // { storageKey: { key: [ "correo:pass", ... ] } }
const pricesPath    = path.join(DATA_DIR, "prices.json");     // { storageKey: { key: number } }
const salesPath     = path.join(DATA_DIR, "sales.json");      // [ { ts, storageKey, groupJid, userJid, key, qty, unitPrice, total, first } ]

/* === SOPORTE === */
const ticketsPath   = path.join(DATA_DIR, "support_tickets.json"); // { [supportMsgId]: { userJid, groupJid, product, account, desc, ts } }

/* ========= Helpers JSON ========= */
const readJSON  = (p, fb = {}) => { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return fb; } };
const writeJSON = (p, o) => fs.writeFileSync(p, JSON.stringify(o, null, 2));

const antilinkDB = readJSON(antilinkPath, {});
const welcomeDB  = readJSON(welcomePath, {});
const aliasesConf= readJSON(aliasesPath, {});
const productsDB = readJSON(productsPath, {}); // por grupo
const helpDB     = readJSON(helpPath,     {}); // por grupo
const mutedDB    = readJSON(mutedPath,   {});
const activityDB = readJSON(activityPath,  {});
const balancesDB = readJSON(balancesPath, {});
const accountsDB = readJSON(accountsPath, {});
const pricesDB   = readJSON(pricesPath,  {});
const salesDB    = readJSON(salesPath,   []);
const ticketsDB  = readJSON(ticketsPath, {});

/* ========= Imagen bienvenida / menú ========= */
const DEFAULT_WELCOME_IMAGE = "C:\\bot faver\\b-bot-secure\\media\\bienvenida.jpeg";
const DEFAULT_MENU_IMAGE    = "C:\\bot faver\\b-bot-secure\\media\\menu.jpeg";
function resolveImagePath(val, fallback) {
  const v = val || fallback || "";
  if (!v) return "";
  if (/^https?:\/\//i.test(v)) return v;
  const abs = path.isAbsolute(v) ? v : path.join(__dirname, v);
  return fs.existsSync(abs) ? abs : "";
}
const WELCOME_IMAGE   = resolveImagePath(process.env.WELCOME_IMAGE, DEFAULT_WELCOME_IMAGE);
const WELCOME_CAPTION = process.env.WELCOME_CAPTION || "👋 ¡Bienvenid@ {name} a {group}!";
const MENU_IMAGE      = resolveImagePath(process.env.MENU_IMAGE, DEFAULT_MENU_IMAGE);
const MENU_TITLE      = process.env.MENU_TITLE || "📜 MENÚ DEL BOT";

/* ========= Aliases ========= */
const ALIASES = Object.entries(aliasesConf).reduce((acc, [canon, list]) => {
  acc[canon] = canon;
  (list || []).forEach(a => acc[a.toLowerCase()] = canon);
  return acc;
}, {});

/* ========= Claves de catálogos ========= */
const TEMP_KEYS = {
  // Notas
  horario:["horario"],
  cuentas:["cuentas"],

  // Otros catálogos (sin stock)
  reglas2:["reglas2"], reembolsos:["reembolsos","reembolso"],
  codigos:["codigos","códigos","codigo","código"],
  seguros:["seguros","seguro"],
  forma:["forma","formas"],
  justificantes:["justificantes","justificante","similar","justificante similar"],
  certificados:["certificados","certificado"],
  reportes:["reportes","reporte"],
  cita:["cita","citas"],
  pedido:["pedido","pedidos"],
  vuelos:["vuelos","vuelo"],
  libros:["libros","libro"],
  ado:["ado"],
  comisionista:["comisionista","comisionistas"],
  peliculas:["peliculas","películas"],
  cinemex:["cinemex"],
  juegos:["juegos"],
  recargas:["recargas","recarga"],
  pago:["pago","pagos",".tarjeta","tarjeta"],
  actas:["actas","acta"],
  estafa:["estafa"],
  imss:["imss"],
  vigencia:["vigencia"],
  insentivos:["insentivos","incentivos"],
  cinepolis:["cinepolis","cinépolis","link cine","linkcine"],
  rfc:["rfc"],
  rebote:["rebote"],
  saldoafavor:["saldoafavor","saldo a favor"],
  predial:["predial"],
  prestamo:["prestamo","préstamo"],
  tramites:["tramites","trámites"],
  garantia:["garantia","garantía"],
  boletos:["boletos","boleto"],
  prohibido:["prohibido"],
  tenencia:["tenencia"],
  packpromo:["packpromo","pack promo"],

  // Streaming (con stock global)
  extra:["extra","extranetflix","netflixextra","perfilprivado"],
  netflix:["netflix"],
  disney:["disney"],
  vix:["vix"],
  canvapro:["canvapro","canva pro","canva+","canva plus"],
  canvaedu:["canvaedu","canva edu","canva education","canva escuela"],
  spotify:["spotify"],
  paramount:["paramount","paramount+"],
  deezer:["deezer"],
  prime:["prime","amazon prime","amazon"],
  max:["max","hbomax","hbo max"],
  crunchy:["crunchy","crunchyroll","crunchyrol"],
  mubi:["mubi"],
  linkvix:["linkvix","link vix"],
  linkdeezer:["linkdeezer","link deezer"],
  duolingo:["duolingo"],
  clarovideo:["clarovideo","claro video"],
  youtube:["youtube","YouTube"],
  pornhub:["pornhub"],
  iptv:["iptv"],
  flujotv:["flujotv","flujo tv"],
  windows:["windows"],
  office:["office"],
  chatgpt:["chatgpt","gpt"],
  capcut:["capcut"],
  viki:["viki"]
};
const CANON_KEYS = Object.keys(TEMP_KEYS);

// Streaming con stock (orden A–Z)
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
function matchKey(raw){ const r=keyify(raw); for(const canon of CANON_KEYS){ if(r===keyify(canon))return canon; for(const a of TEMP_KEYS[canon]){ if(r===keyify(a)) return canon; } } return null; }

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

/* === GLOBAL STORAGE KEY (único para precios/cuentas/ventas) === */
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
  if (isDM && OWNERS.includes(sender)) return true;
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

/* ========= Bienvenida ========= */
function getCountryFromJid(jid){
  const num = (jid.split("@")[0] || "");
  const codes = {
    '1':'EE.UU. 🇺🇸','20':'Egipto 🇪🇬','27':'Sudáfrica 🇿🇦','30':'Grecia 🇬🇷','31':'Países Bajos 🇳🇱','33':'Francia 🇫🇷',
    '34':'España 🇪🇸','39':'Italia 🇮🇹','41':'Suiza 🇨🇭','43':'Austria 🇦🇹','44':'Reino Unido 🇬🇧','45':'Dinamarca 🇩🇰',
    '46':'Suecia 🇸🇪','47':'Noruega 🇳🇴','49':'Alemania 🇩🇪','51':'Perú 🇵🇪','52':'México 🇲🇽','54':'Argentina 🇦🇷',
    '55':'Brasil 🇧🇷','57':'Colombia 🇨🇴','58':'Venezuela 🇻🇪','60':'Malasia 🇲🇾','61':'Australia 🇦🇺','62':'Indonesia 🇮🇩',
    '63':'Filipinas 🇵🇭','64':'Nueva Zelanda 🇳🇿','65':'Singapur 🇸🇬','81':'Japón 🇯🇵','82':'Corea del Sur 🇰🇷',
    '86':'China 🇨🇳','90':'Turquía 🇹🇷','93':'Afganistán 🇦🇫','94':'Sri Lanka 🇱🇰','95':'Myanmar 🇲🇲',
    '212':'Marruecos 🇲🇦','216':'Túnez 🇹🇳','225':'Costa de Marfil 🇨🇮','226':'Burkina Faso 🇧🇫','228':'Togo 🇹🇬',
    '229':'Benín 🇧🇯','231':'Liberia 🇱🇷','233':'Ghana 🇬🇭','235':'Chad 🇹🇩','240':'Guinea Ecuatorial 🇬🇶',
    '241':'Gabón 🇬🇦','242':'Congo 🇨🇬','243':'Rep. Dem. del Congo 🇨🇩','245':'Guinea-Bisáu 🇬🇼',
    '246':'Isla de Navidad 🇨🇽','250':'Ruanda 🇷🇼','251':'Etiopía 🇪🇹','252':'Somalia 🇸🇴','254':'Kenia 🇰🇪',
    '256':'Uganda 🇺🇬','257':'Burundi 🇧🇮','258':'Mozambique 🇲🇿','260':'Zambia 🇿🇲','263':'Zimbabue 🇿🇼',
    '264':'Namibia 🇳🇦','265':'Malawi 🇲🇼','352':'Luxemburgo 🇱🇺','353':'Irlanda 🇮🇪','354':'Islandia 🇮🇸',
    '355':'Albania 🇦🇱','356':'Malta 🇲🇹','358':'Finlandia 🇫🇮','370':'Lituania 🇱🇹','371':'Letonia 🇱🇻',
    '372':'Estonia 🇪🇪','374':'Armenia 🇦🇲','378':'San Marino 🇸🇲','381':'Serbia 🇷🇸','389':'Macedonia del Norte 🇲🇰',
    '420':'República Checa 🇨🇿','421':'Eslovaquia 🇸🇰','502':'Guatemala 🇬🇹','507':'Panamá 🇵🇦','591':'Bolivia 🇧🇴',
    '592':'Guyana 🇬🇾','598':'Uruguay 🇺🇾','672':'Islas de Navidad 🇨🇽','673':'Brunéi 🇧🇳','674':'Nauru 🇳🇷',
    '675':'Papúa Nueva Guinea 🇵🇬','676':'Tonga 🇹🇴','678':'Vanuatu 🇻🇺','681':'Wallis y Futuna 🇼🇫',
    '682':'Islas Cook 🇨🇰','683':'Islas del Pacífico 🇵🇬','685':'Samoa 🇼🇸','686':'Islas Cook 🇨🇰',
    '689':'Polinesia Francesa 🇵🇫','691':'Micronesia 🇫🇲','770':'Comoras 🇰🇲','850':'Corea del Norte 🇰🇵',
    '963':'Siria 🇸🇾','975':'Bhután 🇧🇹','976':'Mongolia 🇲🇳','994':'Azerbaiyán 🇦🇿',
    '995':'Georgia 🇬🇪','998':'Uzbekistán 🇺🇿','1868':'Trinidad y Tobago 🇹🇹','1869':'San Cristóbal y Nieves 🇰🇳',
    '1876':'Jamaica 🇯🇲'
  };
  for (let len=4; len>=1; len--){
    const pref = num.slice(0,len);
    if (codes[pref]) return codes[pref];
  }
  return "Desconocido 🌍";
}
function buildWelcomeCard({ atName, groupName }) {
  return `𝙷𝙾𝙻𝙰  ${atName} ¿𝙲𝙾𝙈𝙊 𝙴𝚂𝚃𝙰𝚂?
${groupName}, 𝚃𝙴 𝙳𝙰 𝙻𝙰 𝙱𝙸𝙴𝙽𝚅𝙸𝙳𝙰.♉

╔══════⚛️══════╗
╠♉ 𝑫𝒂𝒕𝒐𝒔 𝑫𝒆𝒍 𝑼𝒔𝒖𝒂𝒓𝒊𝒐 ♉
╠══════⚛️══════╝
╠➪ 𝙽𝙾𝙼𝙱𝚁𝙴: ${atName}
╠➪ 𝙿𝙰𝙸𝚂: ${getCountryFromJid(atName.replace('@',''))}
╚══════════════╝`;
}

/* ========= Saldo global ========= */
function getBalance(userJid){ return Number(balancesDB[userJid] || 0); }
function addBalance(userJid, delta){
  const next = (Number(balancesDB[userJid] || 0) + Number(delta));
  balancesDB[userJid] = Math.max(0, Math.round((next + Number.EPSILON)*100)/100);
  writeJSON(balancesPath, balancesDB);
  return balancesDB[userJid];
}

/* ========= Global precios/cuentas ========= */
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
function pushAccountsGlobal(key, lines){
  const sk = globalStorageKey();
  accountsDB[sk] = accountsDB[sk] || {};
  accountsDB[sk][key] = accountsDB[sk][key] || [];
  const norm = lines.map(s=>String(s).trim()).filter(Boolean).map(s=>s.replace(/\s+/g,"")).map(s=>s.replace(/[|,;]/g,":"));
  accountsDB[sk][key].push(...norm);
  writeJSON(accountsPath, accountsDB);
  return accountsDB[sk][key].length;
}
function popAccountsGlobal(key, qty){
  const sk = globalStorageKey();
  accountsDB[sk] = accountsDB[sk] || {};
  accountsDB[sk][key] = accountsDB[sk][key] || [];
  if (accountsDB[sk][key].length < qty) return null;
  const out = accountsDB[sk][key].splice(0, qty);
  writeJSON(accountsPath, accountsDB);
  return out;
}
function getAccountsCountGlobal(key){
  const sk = globalStorageKey();
  return (accountsDB[sk] && accountsDB[sk][key] ? accountsDB[sk][key].length : 0);
}
function logSaleGlobal({ groupJid, userJid, key, qty, unitPrice, first }){
  const storageKey = globalStorageKey();
  const total = qty * unitPrice;
  salesDB.push({ ts: Date.now(), storageKey, groupJid, userJid, key, qty, unitPrice, total, first });
  writeJSON(salesPath, salesDB);
}

/* ========= Fechas para .ventas ========= */
function tryParseDate(s){
  if(!s) return null; s=s.trim();
  let m=s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/); if(m) return new Date(+m[1],+m[2]-1,+m[3]);
  m=s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/); if(m){ let y=+m[3]; if(y<100) y+=2000; return new Date(y,+m[2]-1,+m[1]); }
  const d=new Date(s); return isNaN(d.getTime())?null:d;
}

/* ========= Conexión ========= */
async function startSock(){
  const { state, saveCreds } = await useMultiFileAuthState(QR_DIR);
  const sock = makeWASocket({
    auth: state,
    logger: Pino({ level: "info" }),
    browser: ["B-Bot Secure", "Chrome", "1.1"],
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
  sock.ev.on("creds.update", saveCreds);

  /* ========= Mensajes entrantes ========= */
  sock.ev.on("messages.upsert", async ({ messages, type })=>{
    if (type !== "notify") return;
    for (const info of messages){
      try{
        const from = info.key.remoteJid; if (!from) continue;
        const isGroup = from.endsWith("@g.us");
        const sender  = jidNormalizedUser(info.key.participant || info.key.remoteJid);
        const body    = extractPlainText(info);

        if (isGroup){
          activityDB[from] = activityDB[from] || {};
          activityDB[from][sender] = Date.now();
          writeJSON(activityPath, activityDB);
        }

        if (!allow(sender)){ await sock.sendMessage(from,{text:"⏳ Demasiados comandos. Intenta en un minuto."},{quoted:info}); continue; }

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

        /* ===== Procesar respuesta de SOPORTE en el grupo de soporte (debe citar) ===== */
        if (isGroup) {
          const meta = await sock.groupMetadata(from);
          if (isSupportContext(from, meta)) {
            const ctx = info.message?.extendedTextMessage?.contextInfo;
            const stanzaId = ctx?.stanzaId || ctx?.stanzaID;
            const replyText = extractPlainText(info);
            if (stanzaId && ticketsDB[stanzaId]) {
              const t = ticketsDB[stanzaId];
              let header = "📩 *Respuesta de soporte*";
              const low = (replyText||"").toLowerCase();
              if (/(reemplaz|reemplazo)/.test(low)) header = "🔁 *Reemplazo aprobado*";
              else if (/(activa|viva|funciona|buena|ok)/.test(low)) header = "✅ *Cuenta verificada*";

              const user = t.userJid;
              const msg =
`${header}

• Producto: *${t.product || "—"}*
• Cuenta: *${t.account || "—"}*

${replyText || "(sin detalle)"}`
              try { await sock.sendMessage(user, { text: msg }); } catch {}
            }
          }
        }

        if (!isCmd(body)) continue;

        let nameRaw = cmdName(body);
        let name = ALIASES[nameRaw] || nameRaw;
        const args = cmdArgs(body);

        const metadata = isGroup ? await sock.groupMetadata(from) : null;
        const participants = metadata?.participants || [];
        const admins = participants.filter(p=>p.admin).map(p=>p.id);
        const isAdmin = admins.includes(sender) || OWNERS.includes(sender);

        const isControl = isControlContext(from, metadata, sender);

        async function reply(text){ return sock.sendMessage(from,{text},{quoted:info}); }

        if (name === "link") name = "linkgp";

        // Permisos base
        const openCommands = new Set([
          "saldo","stock","menu","linkgp","resetlink","kick","grupo","daradmin","quitaradmin","listaadmin",
          "notify","todos","mute","unmute","op","bot","fantasma","grupoinfo","antilink","welcome",
          "precio","ayuda","miscompras","soporte","idgrupo","id","numero","misgrupos"
        ]);
        const isUserAllowed = openCommands.has(name) || !!matchKey(name) || name.startsWith("comprar");
        if (isGroup){
          const isSaldoOp = (name === "cargarsaldo");
          const isAddDelete = name.startsWith("add") || name.startsWith("delete") || name === "addayuda";
          if (!isAdmin && isAddDelete) return reply("Solo los *administradores* pueden usar `.add...` y `.delete...`");
          if (!isAdmin && !isUserAllowed && !isSaldoOp){
            return reply("Solo los *administradores* pueden usar este comando.");
          }
          if (isSaldoOp && !isAdmin) return reply("Solo los *administradores* pueden usar .cargarsaldo.");
        } else {
          if (!(isUserAllowed || OWNERS.includes(sender))) return reply("Por privado puedes usar: *saldo*, *stock*, *precio*, *ayuda*, *miscompras*, *comprar* y ver catálogos.");
        }

        /* ===== Mostrar catálogos .<clave> (texto/imagen) ===== */
        const showMatch = matchKey(name);
        if (showMatch && args.length === 0){
          const record = (productsDB[from] || {})[showMatch];

          // Encabezado: streaming muestra "Disponibles: N". Notas (.horario/.cuentas) sin encabezado.
          let header = "";
          if (STREAMING_KEYS.includes(showMatch)) {
            const count = getAccountsCountGlobal(showMatch);
            header = `*Disponibles: ${count}*\n\n`;
          }

          if (!record) { await reply((header || "").trim() || ""); continue; }

          if (record && typeof record === "object" && record.image?.path && fs.existsSync(record.image.path)){
            const safeText = (record.text || "").replace(/```/g, "` ` `");
            const caption = header + (safeText ? ("```" + safeText + "```") : "");
            try { await sock.sendMessage(from, { image: fs.readFileSync(record.image.path), caption }, { quoted: info }); }
            catch { await sock.sendMessage(from, { text: caption || "(sin contenido)" }, { quoted: info }); }
          } else {
            const safe = (record || "").toString().replace(/```/g, "` ` `");
            const textOut = header + (record ? ("```" + safe + "```") : "");
            await sock.sendMessage(from, { text: textOut || header || "(sin contenido)" }, { quoted: info });
          }
          continue;
        }

        /* ===== ADD/DELETE catálogos ===== */
        const addMatch = name.startsWith("add")    ? matchKey(name.slice(3))  : null;
        const delMatch = name.startsWith("delete") ? matchKey(name.slice(6))  : null;

        if (addMatch){
          if (addMatch === "stock") return reply("`.addstock` ya no existe. Usa `.cargacuentas <clave>` para cargar stock global.");
          if (isGroup && !isAdmin) return reply(`.add${addMatch} solo para *admins* del grupo.`);
          if (!isGroup && !OWNERS.includes(sender)) return reply("Por privado, solo *OWNER* puede usar `.add...`.");

          const quotedText = getQuotedText(info);
          const tailText   = commandTailPreserving(body);
          const textToSave = (quotedText || tailText || "").trimEnd();

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

          productsDB[from] = productsDB[from] || {};
          const prev = productsDB[from][addMatch];
          let next;
          if (savedImage){
            next = { text: textToSave || (typeof prev === "string" ? prev : prev?.text || ""), image: savedImage };
          } else {
            next = (prev && typeof prev === "object") ? { ...prev, text: textToSave } : textToSave;
          }
          productsDB[from][addMatch] = next;
          writeJSON(productsPath, productsDB);
          await reply(`✅ *${addMatch}* guardado${savedImage ? " con imagen" : ""}.`);
          continue;
        }

        if (delMatch){
          if (delMatch === "stock") return reply("`.deletestock` ya no existe.");
          if (isGroup && !isAdmin) return reply("`.delete...` solo para *admins* del grupo.");
          if (!isGroup && !OWNERS.includes(sender)) return reply("Por privado, solo *OWNER* puede usar `.delete...`.");

          productsDB[from] = productsDB[from] || {};
          delete productsDB[from][delMatch];
          writeJSON(productsPath, productsDB);
          await reply(`🗑️ *${delMatch}* eliminado.`);
          continue;
        }

        /* ===== AYUDA ===== */
        if (name === "addayuda"){
          if (isGroup && !isAdmin) return reply("`.addayuda` solo para *admins* del grupo.");
          if (!isGroup && !OWNERS.includes(sender)) return reply("Por privado, solo *OWNER* puede usar `.addayuda`.");
          const key = matchKey(args[0]||"");
          if (!key) return reply("Uso: .addayuda <clave> <texto>  (o responde con el texto)");
          const textToSave = (getQuotedText(info) || commandTailPreserving(body).replace(/^\s*\S+\s*/,"")).trim();
          if (!textToSave) return reply("Escribe el texto de ayuda (o respóndelo).");
          helpDB[from] = helpDB[from] || {};
          helpDB[from][key] = textToSave;
          writeJSON(helpPath, helpDB);
          return reply(`✅ Ayuda para *${key}* actualizada.`);
        }

        /* ========= SWITCH ========= */
        switch (name){
          case "menu": {
            const caption =
`📜 MENÚ DEL BOT

╭───────────────💸°•°• 
│  「 𝘐𝘯𝘧𝘰 𝘋𝘦𝘭 𝘉𝘰𝘵𝘴𝘪𝘵𝘰 」
│   👑 𝘊𝘳𝘦𝘢𝘥𝘰𝘳 𝘉𝘺 Faver 🇨🇴  
╰───────────────💸•°•°

💢 ┈┈┈LISTA - COMANDOS┈┈ 💢
❇️ .menu
❇️ .link
❇️ .resetlink
❇️ .kick
❇️ .grupo abrir|cerrar
❇️ .daradmin
❇️ .quitaradmin
❇️ .listaadmin
❇️ .notify <texto>
❇️ .todos
❇️ .mute @user
❇️ .unmute @user
❇️ .add <número>
❇️ .op <expr>
❇️ .bot
❇️ .fantasma
❇️ .stock [clave]
❇️ .precio <clave>
❇️ .ayuda <clave> / .addayuda <clave> <texto> (admins)
❇️ .miscompras (privado)
❇️ .soporte <producto?> <cuenta?> <detalle>  (o responde un mensaje y escribe .soporte)
❇️ .saldo
❇️ .cargarsaldo @user <monto> (admins)
❇️ .cargacuentas <clave> (control)
❇️ .setprecio <clave> <precio> (control)
❇️ .ventas [rango|24|48] (control)
❇️ .comprar<clave> <n>
❇️ .id / .idgrupo / .numero / .misgrupos
💢 ┈┈┈STREAMING┈┈ 💢 
${STREAMING_KEYS.map(k=>`❇️ .${k} / .add${k} / .delete${k}`).join("\n")}
💢 ┈┈┈OTROS CATÁLOGOS┈┈ 💢 
❇️ .seguros / .addseguros / .deleteseguros
❇️ .forma / .addforma / .deleteforma
❇️ .justificantes / .addjustificantes / .deletejustificantes
❇️ .certificados / .addcertificados / .deletecertificados
❇️ .cita / .addcita / .deletecita
❇️ .vuelos / .addvuelos / .deletevuelos
❇️ .libros / .addlibros / .deletelibros
❇️ .ado / .addado / .deleteado
❇️ .peliculas / .addpeliculas / .deletepeliculas
❇️ .cinemex / .addcinemex / .deletecinemex
❇️ .juegos / .addjuegos / .deletejuegos
❇️ .recargas / .addrecargas / .deleterecargas
❇️ .pago / .addpago / .deletepago
❇️ .actas / .addactas / .deleteactas
❇️ .imss / .addimss / .deleteimss
❇️ .cinepolis / .addcinepolis / .deletecinepolis
❇️ .rfc / .addrfc / .deleterfc
❇️ .predial / .addpredial / .deletepredial
❇️ .prestamo / .addprestamo / .deleteprestamo
❇️ .tramites / .addtramites / .deletetramites
❇️ .boletos / .addboletos / .deleteboletos
❇️ .tenencia / .addtenencia / .deletetenencia
❇️ .packpromo / .addpackpromo / .deletepackpromo`;
            try{
              if (MENU_IMAGE && !/^https?:\/\//i.test(MENU_IMAGE))
                await sock.sendMessage(from,{ image: fs.readFileSync(MENU_IMAGE), caption },{ quoted:info });
              else if (MENU_IMAGE && /^https?:\/\//i.test(MENU_IMAGE))
                await sock.sendMessage(from,{ image: { url: MENU_IMAGE }, caption },{ quoted:info });
              else
                await sock.sendMessage(from,{ text: caption },{ quoted:info });
            }catch{ await sock.sendMessage(from,{ text: caption },{ quoted:info }); }
            break;
          }

          case "op": {
            const expr = args.join(" ");
            if (!expr) return reply("Uso: .op <expresión>. Ej: .op (2+3)*4");
            try { const res = math.evaluate(expr); await reply(`🧮 Resultado: ${res}`); }
            catch { await reply("⚠️ Expresión inválida."); }
            break;
          }

case "infovendidas": {
  if (!isControl) return reply(`Este comando solo en *Sala de Control* (${CONTROL_GROUP_NAME || "defínela en .env"}) o DM de *OWNER*.`);

  // Parseo flexible de argumentos:
  // Formatos soportados:
  //   .infovendidas
  //   .infovendidas 24 | 48
  //   .infovendidas YYYY-MM-DD:YYYY-MM-DD
  //   .infovendidas <clave> [24|48|rango]
  let start = 0, end = Date.now();
  let keyFilter = null;

  function parseRangeToken(tok){
    tok = String(tok||"").trim();
    if (!tok) return false;
    if (/^\d{2,3}$/.test(tok)) { // 24, 48, 72...
      start = Date.now() - Number(tok)*60*60*1000;
      end = Date.now();
      return true;
    }
    const m = tok.match(/^(\d{4}-\d{2}-\d{2})[:](\d{4}-\d{2}-\d{2})$/);
    if (m) {
      const d1 = tryParseDate(m[1]);
      const d2 = tryParseDate(m[2]);
      if (d1 && d2) {
        start = d1.getTime();
        end = d2.getTime() + 24*60*60*1000 - 1;
        return true;
      }
    }
    return false;
  }

  if (args.length === 1) {
    // Puede ser solo rango/horas O solo clave
    if (!parseRangeToken(args[0])) {
      const mk = matchKey(args[0]);
      if (mk) keyFilter = mk;
    }
  } else if (args.length >= 2) {
    // <clave> <rango>
    const mk = matchKey(args[0]);
    if (mk) keyFilter = mk;
    // Lo demás que quede lo tratamos como rango
    const tail = args.slice(mk ? 1 : 0).join("").trim();
    if (tail) parseRangeToken(tail);
  }

  // Filtramos ventas
  const rows = salesDB
    .filter(s => s.ts >= start && s.ts <= end && (!keyFilter || s.key === keyFilter))
    .sort((a,b) => (a.key||"").localeCompare(b.key||"") || a.ts - b.ts);

  if (!rows.length) return reply("No hay ventas en el periodo/criterio solicitado.");

  // Agrupado por producto
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

  // Construimos salida
  const orderKeys = Object.keys(byKey).sort((a,b)=>a.localeCompare(b));
  let header = "🧾 *Informe de cuentas vendidas*";
  if (start) {
    const d1 = new Date(start).toLocaleDateString();
    const d2 = new Date(end).toLocaleDateString();
    header += `\nPeriodo: ${d1} a ${d2}`;
  }
  if (keyFilter) header += `\nProducto: *${keyFilter.toUpperCase()}*`;

  const blocks = [header, ""];
  for (const k of orderKeys) {
    blocks.push(`*${k.toUpperCase()}*`);
    blocks.push(byKey[k].join("\n"));
    blocks.push(""); // línea en blanco
  }

  const textOut = blocks.join("\n");
  await sock.sendMessage(from, { text: textOut, mentions: Array.from(mentions) }, { quoted: info });
  break;
}

          case "notify": {
            const meta = await sock.groupMetadata(from);
            const members = (meta.participants || []).map(p=>p.id);
            const textMsg = commandTailPreserving(body) || "";
            await sock.sendMessage(from,{ text:textMsg, mentions:members },{ quoted:info });
            break;
          }

          case "grupo": {
            const action=(args[0]||"").toLowerCase();
            if (["abrir","open","ᴀʙʀɪʀ"].includes(action)){ await sock.groupSettingUpdate(from,"not_announcement"); await reply("🔓 Grupo *abierto* para todos."); }
            else if (["cerrar","close","ᴄᴇʀʀᴀʀ"].includes(action)){ await sock.groupSettingUpdate(from,"announcement"); await reply("🔐 Grupo *cerrado* (solo admins)."); }
            else await reply("Uso: .grupo abrir | .grupo cerrar");
            break;
          }

          case "daradmin": {
            const targets=getMentionedOrQuoted(info); if(!targets.length) return reply("Menciona a quien dar admin.");
            await sock.groupParticipantsUpdate(from, targets, "promote"); await reply("✅ Admin otorgado."); break;
          }
          case "quitaradmin": {
            const targets=getMentionedOrQuoted(info); if(!targets.length) return reply("Menciona a quien quitar admin.");
            await sock.groupParticipantsUpdate(from, targets, "demote"); await reply("✅ Admin retirado."); break;
          }
          case "listaadmin": {
            const meta=await sock.groupMetadata(from);
            const adminIds=(meta.participants||[]).filter(p=>p.admin).map(p=>p.id);
            const adminsList=adminIds.map(j=>`@${j.split("@")[0]}`);
            await sock.sendMessage(from,{ text:`👮 *Admins de ${meta.subject}:*\n\n${adminsList.join("\n")||"—"}`, mentions:adminIds },{ quoted:info });
            break;
          }
          case "kick": {
            const targets=getMentionedOrQuoted(info); if(!targets.length) return reply("Menciona a quien expulsar.");
            await sock.groupParticipantsUpdate(from, targets, "remove"); await reply("🚪 Usuario(s) expulsado(s)."); break;
          }

          case "grupoinfo": {
            const meta=await sock.groupMetadata(from);
            const name=meta.subject||"—"; const size=(meta.participants||[]).length;
            const adminIds=(meta.participants||[]).filter(p=>p.admin).map(p=>p.id);
            const adminsList=adminIds.map(j=>`@${j.split("@")[0]}`);
            const desc=(meta.desc && (meta.desc.description||meta.desc.toString?.()))||"—";
            await sock.sendMessage(from,{ text:`🛈 *Información del grupo*\n\n• Nombre: ${name}\n• Miembros: ${size}\n• Admins:\n${adminsList.map(a=>"  - "+a).join("\n")||"  —"}\n\n• Descripción:\n${desc}`, mentions:adminIds },{ quoted:info });
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
              .sort((a,b)=> (a.subject||"").localeCompare(b.subject||""))
              .map(g => `• ${g.subject || "—"}\n  ${g.id}`);
            await sock.sendMessage(from, { text: `🗂️ *Grupos donde está el bot:*\n\n${lines.join("\n")}` }, { quoted: info });
            break;
          }

          case "linkgp": {
            const meta=await sock.groupMetadata(from);
            const botIsAdmin=(meta.participants||[]).some(p=>p.id===sock.user.id && p.admin);
            if(!botIsAdmin) return reply("Necesito ser admin para generar el link.");
            try{ const code=await sock.groupInviteCode(from); await reply(`🔗 Link del grupo:\nhttps://chat.whatsapp.com/${code}`); }
            catch{ await reply("No pude obtener el link."); }
            break;
          }

          case "resetlink": {
            const meta=await sock.groupMetadata(from);
            const botIsAdmin=(meta.participants||[]).some(p=>p.id===sock.user.id && p.admin);
            if(!botIsAdmin) return reply("Necesito ser admin para restablecer el link.");
            try{ await sock.groupRevokeInvite(from); const code=await sock.groupInviteCode(from); await reply(`♻️ Link restablecido:\nhttps://chat.whatsapp.com/${code}`); }
            catch{ await reply("No pude restablecer el link."); }
            break;
          }

          case "add": {
            const raw=args[0] || (body.match(/(\+?\d[\d\s-]{6,}\d)/)?.[1] ?? "");
            let s=String(raw).replace(/[^\d+]/g,"");
            if(s.startsWith("+")) s=s.slice(1);
            if(s.length===10 && s.startsWith("3")) s=DEFAULT_CC + s;
            if(!/^\d{7,15}$/.test(s)) return reply("Formato inválido. Ej: .add 573001234567");
            const targetJid=s+"@s.whatsapp.net";
            try{
              await sock.groupParticipantsUpdate(from,[targetJid],"add");
              await sock.sendMessage(from,{ text:`✅ Agregado: @${s}`, mentions:[targetJid] },{ quoted:info });
            }catch{
              try{ const code=await sock.groupInviteCode(from); const link=`https://chat.whatsapp.com/${code}`;
                await reply(`⚠️ No pude agregarlo. Comparte este link:\n${link}`);
                try{ await sock.sendMessage(targetJid,{ text:`👋 Te invitan al grupo: *${metadata.subject}*\nÚnete: ${link}` }); }catch{}
              }catch{ await reply("No pude agregar ni generar link."); }
            }
            break;
          }

          case "mute": {
            const targets=getMentionedOrQuoted(info); if(!targets.length) return reply("Menciona o responde a quien mutear.");
            mutedDB[from]=Array.from(new Set([...(mutedDB[from]||[]), ...targets]));
            writeJSON(mutedPath, mutedDB);
            await reply(`🔇 Mute para: ${targets.map(j=>`@${j.split("@")[0]}`).join(" ")}`); break;
          }
          case "unmute": {
            const targets=getMentionedOrQuoted(info); if(!targets.length) return reply("Menciona o responde a quien desmutear.");
            mutedDB[from]=(mutedDB[from]||[]).filter(j=>!targets.includes(j));
            writeJSON(mutedPath, mutedDB);
            await reply(`🔊 Unmute para: ${targets.map(j=>`@${j.split("@")[0]}`).join(" ")}`); break;
          }

          case "antilink": {
            const on=/^(on|encender|activar|true|1)$/i.test(args[0]||"");
            const off=/^(off|apagar|desactivar|false|0)$/i.test(args[0]||"");
            if(!on && !off) return reply("Uso: .antilink on|off");
            antilinkDB[from]=on; writeJSON(antilinkPath, antilinkDB);
            await reply(`Antilink: ${on?"✅ activado":"⛔ desactivado"}.`); break;
          }
          case "welcome": {
            const on=/^(on|encender|activar|true|1)$/i.test(args[0]||"");
            const off=/^(off|apagar|desactivar|false|0)$/i.test(args[0]||"");
            if(!on && !off) return reply("Uso: .welcome on|off");
            welcomeDB[from]=on; writeJSON(welcomePath, welcomeDB);
            await reply(`Welcome: ${on?"✅ activado":"⛔ desactivado"}.`); break;
          }

          case "todos":
          case "tagall": {
            const list=participants.map(p=>p.id).slice(0, TAGALL_MAX);
            const textT="📣 "+(commandTailPreserving(body)||"Llamando a todos:")+"\n\n"+list.map(j=>`@${j.split("@")[0]}`).join(" ");
            await sock.sendMessage(from,{ text:textT, mentions:list });
            break;
          }

          /* ===== STOCK GLOBAL ===== */
          case "stock": {
            const icon = "📦";
            if (!args.length){
              const lines = STREAMING_KEYS.map(k => {
                const n = getAccountsCountGlobal(k);
                return `${k} ${n>0 ? `${icon} ${n}` : "Agotado ❌"}`;
              });
              await reply(lines.join("\n"));
              break;
            } else {
              const k = matchKey(args[0] || "");
              if (!k) return reply("Uso: .stock  ó  .stock <clave>");
              if (!STREAMING_KEYS.includes(k)) return reply(`La clave *${k}* no maneja stock.`);
              const n = getAccountsCountGlobal(k);
              await reply(`${icon} *${k.toUpperCase()}* → ${n}`);
              break;
            }
          }

          /* ===== INFO DE PRECIOS / AYUDAS / HISTORIAL ===== */
          case "precio": {
            const key = matchKey(args[0]||"");
            if (!key) return reply("Uso: .precio <clave>");
            const val = getPriceGlobal(key);
            if (!val) return reply(`Aún no hay precio configurado para *${key}*.`);
            return reply(`💲 Precio de *${key}*: *$${val.toFixed(2)}*`);
          }

          case "ayuda": {
            const key = matchKey(args[0]||"");
            if (!key) return reply("Uso: .ayuda <clave>");
            const txt = (helpDB[from]||{})[key];
            if (!txt) return reply(`(sin ayuda para *${key}*)\n👉 Pide a un admin que use: .addayuda ${key} <texto>`);
            return reply(txt);
          }

          case "miscompras": {
            if (isGroup) return reply("ℹ️ *Mis compras* solo funciona por *privado* con el bot.");
            const rows = salesDB.filter(s => s.userJid === sender).slice(-50);
            if (!rows.length) return reply("Aún no tienes compras registradas.");
            let total=0;
            const lines=rows.map(r=>{
              total += r.total;
              const when=new Date(r.ts).toLocaleString();
              return `• ${when} | ${r.key} x${r.qty} | $${(r.total).toFixed(2)}`;
            });
            return reply(`🧾 *Tus compras*\n\n${lines.join("\n")}\n\n💰 Total gastado: *$${total.toFixed(2)}*`);
          }

          /* ===== SOPORTE (flexible) ===== */
          case "soporte": {
            const supportId = await resolveSupportTarget(sock);
            if (!supportId)
              return reply("⚠️ No encuentro el *Grupo de Soporte*. Verifica que el bot esté dentro de \""+(SUPPORT_GROUP_NAME||"Soporte")+"\" o define SUPPORT_GROUP_IDS en .env");

            const tail   = commandTailPreserving(body) || "";
            const quoted = getQuotedText(info) || "";

            const { product, account, desc } = parseSupportInput(tail, quoted);
            if (!desc)
              return reply("Uso: `.soporte <producto?> <cuenta?> <detalle>`\nTambién puedes *responder tu mensaje con el reporte* y escribir `.soporte`.");

            const atUser = `@${sender.split("@")[0]}`;

            const textToSupport =
`🆘 *Ticket de soporte*
• Cliente: ${atUser}
• Producto: *${product || "—"}*
• Cuenta: *${account || "—"}*
• Detalle:
${desc}

Responde *citando este mensaje* con:
- "reemplazo ..." para enviar nuevas credenciales
- o "activa/viva/funciona/buena/ok" para informar que está operativa.`;

            const sent = await sock.sendMessage(supportId, { text: textToSupport, mentions: [sender] });
            const msgId = sent?.key?.id;
            if (msgId) {
              ticketsDB[msgId] = { userJid: sender, groupJid: from, product, account, desc, ts: Date.now() };
              writeJSON(ticketsPath, ticketsDB);
            }

            await sock.sendMessage(from, {
              text: "✅ *Reporte recibido*. Nuestro equipo ya está trabajando en ello. Te avisaremos por este chat en cuanto tengamos respuesta.",
            }, { quoted: info });

            break;
          }

          /* ===== BOT info ===== */
          case "bot": {
            const INFO_ENABLED = (process.env.BOT_INFO_ENABLED || "false").toLowerCase() === "true";
            const INFO_TEXT    = process.env.BOT_INFO_TEXT || "";
            const msg = INFO_ENABLED && INFO_TEXT
              ? INFO_TEXT
              : "🤖 *Bot activo*.
Usa `.menu` para ver comandos.";
            await sock.sendMessage(from, { text: msg }, { quoted: info });
            break;
          }

          case "fantasma": {
            const meta=await sock.groupMetadata(from);
            const members=(meta.participants||[]).map(p=>p.id);
            const last=activityDB[from]||{};
            const threshold=Date.now()-FANTASMA_DAYS*24*60*60*1000;
            const ghosts=members.filter(j=>!(j in last)||last[j]<threshold);
            const lines=ghosts.map(j=>`• @${j.split("@")[0]}`);
            const txt=`👻 *Usuarios inactivos* (≥ ${FANTASMA_DAYS} días)\n\n${lines.join("\n")||"— Ninguno —"}`;
            await sock.sendMessage(from,{ text:txt, mentions:ghosts },{ quoted:info });
            break;
          }

          /* ===== SALDO ===== */
          case "saldo": {
            let target=sender;
            if (isAdmin || OWNERS.includes(sender)){ const ment=getMentionedOrQuoted(info); if(ment && ment.length) target=ment[0]; }
            const bal=getBalance(target);
            const who=target===sender?"Tu saldo":`Saldo de @${target.split("@")[0]}`;
            await sock.sendMessage(from,{ text:`💳 ${who}: *${bal.toFixed(2)}*`, mentions: target===sender?[]:[target] },{ quoted:info });
            break;
          }

          case "cargarsaldo": {
            if (isGroup){ if(!isAdmin) return reply("Solo los *administradores* pueden usar .cargarsaldo."); }
            else if (!OWNERS.includes(sender)) return reply("Solo los *OWNER* pueden recargar saldo por privado.");
            const ment=getMentionedOrQuoted(info);
            const amount=Number(args.find(a=>/^-?\d+(\.\d+)?$/.test(a))||0);
            if(!ment.length || !amount) return reply("Uso: .cargarsaldo @user <monto>");
            const next=addBalance(ment[0], amount);
            await sock.sendMessage(from,{ text:`💳 Saldo para @${ment[0].split("@")[0]}: *${next.toFixed(2)}* (global)`, mentions:ment },{ quoted:info });
            break;
          }

          /* ===== CONTROL GLOBAL ===== */
          case "setprecio": {
            if(!isControl) return reply(`Solo en *Sala de Control* (${CONTROL_GROUP_NAME || "defínela en .env"}) o DM de *OWNER*.`);
            const key=matchKey(args[0]||""); const price=Number(args[1]||"");
            if(!key || !price) return reply("Uso: .setprecio <clave> <precio>\nEj: .setprecio netflix 180");
            setPriceGlobal(key, price);
            await reply(`💲 Precio de *${key}* fijado en *${price}* (global).`);
            break;
          }

          case "cargacuentas": {
            if(!isControl) return reply(`Solo en *Sala de Control* (${CONTROL_GROUP_NAME || "defínela en .env"}) o DM de *OWNER*.`);
            const key=matchKey(args[0]||"");
            if(!key) return reply("Uso: .cargacuentas <clave> (pega o cita líneas: correo:pass)");
            const txtBlock=getQuotedText(info) || commandTailPreserving(body);
            if(!txtBlock.trim()) return reply("Pega o cita las cuentas (una por línea) en formato correo:contraseña.");
            const lines=txtBlock.split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
            const total=pushAccountsGlobal(key, lines);
            await reply(`✅ Cuentas cargadas para *${key}*. Total GLOBAL en stock: *${total}*`);
            break;
          }

          case "ventas24":
          case "ventas48":
          case "ventas": {
            if(!isControl) return reply(`Este comando solo en *Sala de Control* (${CONTROL_GROUP_NAME || "defínela en .env"}) o DM de *OWNER*.`);
            let start=0, end=Date.now();
            if (name==="ventas24" || name==="ventas48"){
              const hours = name==="ventas24"?24:48;
              start = Date.now()-hours*60*60*1000;
            } else if (args.length){
              const joined=args.join(" ").trim();
              const m=joined.match(/^([^:]+):(.+)$/);
              if(m){ const d1=tryParseDate(m[1]); const d2=tryParseDate(m[2]); if(d1&&d2){ start=d1.getTime(); end=d2.getTime()+24*60*60*1000-1; } }
            }
            const rows = salesDB.filter(s => s.ts >= start && s.ts <= end).slice(-2000);
            if (!rows.length) return reply("No hay ventas en el periodo solicitado.");
            let total=0;
            const lines=rows.map(r=>{
              total += r.total;
              const when=new Date(r.ts).toLocaleString();
              const user=`@${r.userJid.split("@")[0]}`;
              const tag=r.first?` | ${r.first}`:"";
              return `${when} | ${user} | ${r.key} x${r.qty}${tag} | $${(r.total).toFixed(2)}`;
            });
            const mentions=rows.map(r=>r.userJid);
            const header=(start?`🧾 Ventas (todas las sucursales) desde ${new Date(start).toLocaleDateString()} hasta ${new Date(end).toLocaleDateString()}`:"🧾 Ventas recientes (todas las sucursales)");
            await sock.sendMessage(from,{ text:`${header}\n\n${lines.join("\n")}\n\n💰 Total: $${total.toFixed(2)}`, mentions },{ quoted:info });
            break;
          }

          /* ======== COMPRAR ======== */
          default: {
            if (name.startsWith("comprar")){
              const rawKey=name.replace(/^comprar/,"");
              const key=matchKey(rawKey);
              const qty=Number(args[0]||1);
              if(!key) return reply("Uso: .comprar<clave> <cantidad>\nEj: .comprarnetflix 1");
              if(!Number.isInteger(qty) || qty<=0) return reply("La cantidad debe ser un entero positivo.");

              const unit=getPriceGlobal(key);
              if(!unit || unit<=0) return reply(`No hay precio configurado para *${key}*. Contacta a un admin del grupo *${CONTROL_GROUP_NAME||"Admin"}*.`);
              const total=unit*qty;

              const bal=getBalance(sender);
              if(bal<total){ await reply(`❌ Saldo insuficiente. Necesitas *$${total.toFixed(2)}*. Saldo actual: *$${bal.toFixed(2)}*.\n👉 Comunícate con un administrador para recargar saldo.`); return; }

              const available=getAccountsCountGlobal(key);
              if(available<qty) return reply(`❌ Stock insuficiente de *${key}*. Disponible: *${available}*.`);

              const items=popAccountsGlobal(key, qty);
              if(!items) return reply("Error al extraer cuentas. Intenta de nuevo.");

              addBalance(sender, -total);
              const first=(items[0]||"").split(":")[0]||"";
              logSaleGlobal({ groupJid: from, userJid: sender, key, qty, unitPrice: unit, first });

              const dmText =
`🎉 *Compra exitosa*
• Producto: *${key}*
• Cantidad: *${qty}*
• Precio unitario: *$${unit.toFixed(2)}*
• Total: *$${total.toFixed(2)}*
• Saldo restante: *$${getBalance(sender).toFixed(2)}*

*Datos de acceso:*
${items.map((it,i)=>` ${i+1}. ${it}`).join("\n")}

💢NO CAMBIAR CORREO, NO MODIFICAR PLAN NI CONTRASEÑA, PARA NO PERDER GARANTIA💢`;

              try { await sock.sendMessage(sender,{ text: dmText }); await reply(`✅ Te envié *${qty} ${key}* por privado.`); }
              catch { await reply("✅ Compra exitosa, pero no pude enviarte DM. Escríbeme al privado y te lo reenvío."); try{ await sock.sendMessage(sender,{ text:"👋 Escríbeme aquí para poder entregarte tus datos comprados." }); }catch{} }
              return;
            }

            await reply("Comando no reconocido. Usa .menu");
          }
        }
      }catch(e){ console.error("handle error:", e); }
    }
  });

  /* ========= Bienvenida/Despedida ========= */
  sock.ev.on("group-participants.update", async (u)=>{
    try{
      const { id: groupJid, participants, action } = u;
      const on = welcomeDB[groupJid] ?? WELCOME_DEFAULT; if (!on) return;

      let groupName="este grupo";
      try{ const meta=await sock.groupMetadata(groupJid); groupName=meta?.subject||groupName; }catch{}

      for (const jid of participants){
        const at=`@${jid.split("@")[0]}`;
        if (action==="add"){
          const caption=(WELCOME_CAPTION||"").replace("{name}", at).replace("{group}", groupName);
          try{
            if (WELCOME_IMAGE){
              if (/^https?:\/\//i.test(WELCOME_IMAGE)) await sock.sendMessage(groupJid,{ image:{url:WELCOME_IMAGE}, caption, mentions:[jid] });
              else await sock.sendMessage(groupJid,{ image: fs.readFileSync(WELCOME_IMAGE), caption, mentions:[jid] });
            }
          }catch{}
          await sock.sendMessage(groupJid,{ text: buildWelcomeCard({ atName: at, groupName }), mentions:[jid] });
        } else if (action==="remove"){
          await sock.sendMessage(groupJid,{ text:`👋 Adiós ${at}.`, mentions:[jid] });
        }
      }
    }catch(e){ console.error("welcome error:", e); }
  });
}

startSock().catch(e=>console.error("fatal", e));
