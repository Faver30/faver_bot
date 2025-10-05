const fs = require("fs");
const path = require("path");

const paths = [
  "C:\\bot faver\\bot-bels\\data\\media\\bienvenida.jpeg",
  "C:\\bot faver\\bot-bels\\data\\media\\menu.jpeg"
];

function check(p) {
  const abs = path.isAbsolute(p) ? p : path.resolve(__dirname, p);
  const ok = fs.existsSync(abs);
  console.log(`${ok ? "✅ OK" : "❌ NO ENCONTRADA"}  ->  ${abs}`);
}

console.log("Directorio actual (__dirname):", __dirname);
paths.forEach(check);
