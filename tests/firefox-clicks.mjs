// Real clicks in a real Firefox, over WebDriver BiDi (built into Firefox; node 22's WebSocket talks to it).
//
//     node tests/firefox-clicks.mjs site        # run by tests/check_site.py
//
// Headless Chromium once passed while Firefox sent every click on the landing page to the hero's 3D-transformed
// floor grid, so the language tabs did nothing. This test moves the pointer to the middle of each control and
// presses it, then checks that the element under the pointer is the control itself and that its state changed:
//   index.html, docs/clients.html  every language tab in every tab group: selected, its panel shown
//   docs/benchmarks.html           the first section link in the sidebar: the page moved to that section
// The pages are served from a small HTTP server on a free port; Firefox gets a free BiDi port
// (--remote-debugging-port=0, read back from its output) and a fresh profile, under ~/snap/firefox/common when
// Firefox is a snap (a snap cannot use a profile elsewhere). Exit 0: all passed; 1: a failure; 3: skipped (no
// Firefox, or it did not start).
import { spawn, spawnSync } from "node:child_process";
import { createServer } from "node:http";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir, homedir } from "node:os";
import { extname, join, normalize, resolve } from "node:path";

const site = resolve(process.argv[2] || "site");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const skip = (why) => { console.log("SKIP " + why); process.exit(3); };

const which = spawnSync("sh", ["-c", "command -v firefox"], { encoding: "utf8" });
const firefox = process.env.FIREFOX || which.stdout.trim();
if (!firefox) skip("no firefox");

/* the site over HTTP on a free port */
const types = { ".html": "text/html", ".css": "text/css", ".js": "text/javascript", ".svg": "image/svg+xml",
  ".png": "image/png" };
const server = createServer((req, res) => {
  const p = normalize(decodeURIComponent(new URL(req.url, "http://x").pathname)).replace(/^(\.\.[/\\])+/, "");
  let f = join(site, p);
  if (existsSync(f) && statSync(f).isDirectory()) f = join(f, "index.html");
  if (!f.startsWith(site) || !existsSync(f)) { res.writeHead(404); res.end(); return; }
  res.writeHead(200, { "content-type": types[extname(f)] || "application/octet-stream" });
  res.end(readFileSync(f));
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const base = "http://127.0.0.1:" + server.address().port + "/";

const snap = join(homedir(), "snap", "firefox", "common");
const prof = mkdtempSync(existsSync(snap) ? join(snap, "tsdb-check-") : join(tmpdir(), "tsdb-check-"));
const ff = spawn(firefox, ["--headless", "--remote-debugging-port=0", "--profile", prof, "--width=1280",
  "--height=900", "--no-remote"], { stdio: ["ignore", "pipe", "pipe"] });
function stop(code) {
  try { ff.kill(); } catch {}
  server.close();
  setTimeout(() => { try { rmSync(prof, { recursive: true, force: true }); } catch {} process.exit(code); }, 500);
}

/* Firefox prints "WebDriver BiDi listening on ws://127.0.0.1:PORT" once it serves */
const wsUrl = await new Promise((r) => {
  let out = "";
  const t = setTimeout(() => r(null), 45000);
  const look = (d) => {
    out += d;
    const m = out.match(/WebDriver BiDi listening on (ws:\/\/[\d.:]+)/);
    if (m) { clearTimeout(t); r(m[1]); }
  };
  ff.stdout.on("data", look);
  ff.stderr.on("data", look);
  ff.on("exit", () => { clearTimeout(t); r(null); });
});
if (!wsUrl) { console.log("SKIP firefox did not start its BiDi server"); stop(3); await sleep(1e6); }

const ws = new WebSocket(wsUrl + "/session");
await new Promise((r, j) => { ws.onopen = r; ws.onerror = j; }).catch(() => { console.log("SKIP no BiDi"); stop(3); });
let id = 0;
const pending = new Map();
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); }
};
const send = (method, params = {}) => new Promise((r) => {
  const i = ++id;
  pending.set(i, r);
  ws.send(JSON.stringify({ id: i, method, params }));
});
await send("session.new", { capabilities: {} });
const ctx = (await send("browsingContext.getTree", {})).result.contexts[0].context;
const ev = async (expr) => (await send("script.evaluate", { expression: expr, target: { context: ctx },
  awaitPromise: false, resultOwnership: "none" })).result?.result?.value;

/* Put the control in view, find its middle, say what element is at that point, then press it there. */
async function click(find) {
  const got = await ev(`(function () { var t = ${find}; if (!t) return null;
    t.scrollIntoView({ block: "center" }); var r = t.getBoundingClientRect();
    var x = r.x + r.width / 2, y = r.y + r.height / 2, at = document.elementFromPoint(x, y);
    window.__target = t;
    return JSON.stringify({ x: Math.round(x), y: Math.round(y), hit: !!at && (at === t || t.contains(at)),
      at: at ? at.tagName.toLowerCase() + (at.className && at.className.baseVal === undefined ? "." + at.className : "") : "nothing" }); })()`);
  if (!got) return null;
  const b = JSON.parse(got);
  await send("input.performActions", { context: ctx, actions: [{ type: "pointer", id: "mouse",
    parameters: { pointerType: "mouse" }, actions: [{ type: "pointerMove", x: b.x, y: b.y },
      { type: "pointerDown", button: 0 }, { type: "pointerUp", button: 0 }] }] });
  await send("input.releaseActions", { context: ctx });
  await sleep(250);
  return b;
}

let failed = 0;
function report(ok, what) {
  console.log((ok ? "PASS " : "FAIL ") + what);
  if (!ok) failed++;
}

async function open(page) {
  await send("browsingContext.navigate", { context: ctx, url: base + page, wait: "complete" });
  await sleep(700);
}

for (const page of ["index.html", "docs/clients.html"]) {
  await open(page);
  const groups = await ev(`document.querySelectorAll(".tabs").length`);
  if (!groups) { report(false, `${page}: no tab groups`); continue; }
  for (let g = 0; g < groups; g++) {
    for (const key of ["rust", "python", "java", "c"]) {
      const find = `document.querySelectorAll(".tabs")[${g}].querySelector('[role="tab"][data-key="${key}"]')`;
      const b = await click(find);
      if (!b) continue; /* this group has no such language */
      const state = await ev(`(function () { var t = window.__target, p = document.getElementById(t.getAttribute("aria-controls"));
        return t.getAttribute("aria-selected") === "true" && p && !p.hidden ? "selected, panel shown" : "not selected"; })()`);
      report(b.hit && state === "selected, panel shown",
        `${page}: tab group ${g + 1}, ${key}: under the pointer ${b.hit ? "the tab" : b.at}; ${state}`);
    }
  }
}

await open("docs/benchmarks.html");
const b = await click(`document.querySelector(".sidebar .toc a")`);
if (!b) report(false, "docs/benchmarks.html: no section link in the sidebar");
else {
  const where = await ev(`(function () { var a = window.__target; return location.hash === a.getAttribute("href") ?
    "moved to " + location.hash : "still at '" + location.hash + "'"; })()`);
  report(b.hit && where.startsWith("moved"),
    `docs/benchmarks.html: sidebar section link: under the pointer ${b.hit ? "the link" : b.at}; ${where}`);
}

console.log(failed ? `RESULT ${failed} failed` : "RESULT ok");
stop(failed ? 1 : 0);
await sleep(1e6);
