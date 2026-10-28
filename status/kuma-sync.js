// kuma-sync: keeps one Uptime Kuma monitor per internal address of a replication-manager
// cluster (its apps, database servers and proxies), read from the replication-manager API.
// It runs next to Kuma (start.sh) and talks to it on its socket.io API, with Kuma's own
// socket.io-client. Only the monitors it created are changed or removed: they carry the
// description "managed by kuma-sync (<cluster>)"; monitors added by hand are left alone.
//
// Environment:
//   REPMAN_API_URL, REPMAN_API_TOKEN  replication-manager API and a read-only API token
//   REPMAN_API_TLS_VERIFY             "false" accepts the API's self-signed certificate (the
//                                     internal address replication-manager gives its apps)
//   REPMAN_CLUSTER                    the cluster (namespace) whose addresses are monitored
//   KUMA_ADMIN_USER, KUMA_ADMIN_PASSWORD  Kuma's admin, created at the first start
//   SYNC_INTERVAL (60 s), MONITOR_INTERVAL (60 s), KUMA_URL (http://localhost:3001)
//   SELF_HOST                         this app's own host, not monitored
//   SSO_ENABLED, SSO_EMAILS_FILE      set by start.sh when oauth2-proxy guards the page: the
//                                     file gets the emails of the cluster's users holding
//                                     cluster-show-routes, and Kuma's own login is turned off;
//                                     without SSO, Kuma's login is kept on
//   STATUS_PAGE_SLUG, STATUS_PAGE_TITLE  a Kuma status page (letters, digits, dashes), read-only;
//                                     empty, none. Kuma owns its groups (made, renamed, ordered
//                                     and filled in Kuma's editor): the sync only places a
//                                     managed monitor that is in no group yet, in the default
//                                     group of its kind (Applications, Databases, Proxies,
//                                     Partner consoles), created when missing. Behind the SSO
//                                     only its data stays open (start.sh), for a page elsewhere
//                                     (the Cloud18 members zone) to show it
//   EXTERNAL_MONITORS                 addresses outside the cluster, "name|https://url;name|url":
//                                     one http check each (certificate verified), monitor
//                                     "<name>", default group "Partner consoles"; empty, none
"use strict";
const { io } = require("/app/node_modules/socket.io-client");
const fs = require("fs");
const https = require("https");
const http = require("http");

const env = process.env;
const KUMA_URL = env.KUMA_URL || "http://localhost:3001";
const API = (env.REPMAN_API_URL || "").replace(/\/+$/, "");
const TOKEN = env.REPMAN_API_TOKEN || "";
const CLUSTER = env.REPMAN_CLUSTER || "";
const USER = env.KUMA_ADMIN_USER || "admin";
const PASSWORD = env.KUMA_ADMIN_PASSWORD || "";
const SYNC_INTERVAL = Math.max(20, Number(env.SYNC_INTERVAL) || 60);
const CHECK_INTERVAL = Math.max(20, Number(env.MONITOR_INTERVAL) || 60);
const SELF_HOST = env.SELF_HOST || "";
const TLS_VERIFY = String(env.REPMAN_API_TLS_VERIFY || "true").toLowerCase() !== "false";
const MARK = `managed by kuma-sync (${CLUSTER})`;
const SSO = env.SSO_ENABLED === "true";
const SSO_EMAILS_FILE = env.SSO_EMAILS_FILE || "";
const STATUS_PAGE_SLUG = /^[a-z0-9-]+$/.test(env.STATUS_PAGE_SLUG || "") ? env.STATUS_PAGE_SLUG : "";
const STATUS_PAGE_TITLE = env.STATUS_PAGE_TITLE || STATUS_PAGE_SLUG;
// "name|url" pairs; an entry without a name or an http(s) url is skipped
const EXTERNAL = (env.EXTERNAL_MONITORS || "").split(";")
    .map((e) => e.split("|").map((x) => x.trim()))
    .filter(([name, url]) => name && /^https?:\/\/[^\s]+$/.test(url || ""));

const log = (...a) => console.log(new Date().toISOString(), "kuma-sync:", ...a);
const sleep = (s) => new Promise((r) => setTimeout(r, s * 1000));

function call(sock, event, ...args) {
    // a number as the last argument is the time to wait for the answer (30 s by default)
    const wait = typeof args[args.length - 1] === "number" && event === "needSetup" ? args.pop() : 30000;
    return new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error(`${event}: no answer from Kuma`)), wait);
        sock.emit(event, ...args, (res) => {
            clearTimeout(t);
            resolve(res);
        });
    });
}

// api reads one replication-manager endpoint. node's https (not fetch) so a self-signed
// certificate can be accepted for this API only (REPMAN_API_TLS_VERIFY=false).
function api(path) {
    const url = new URL(API + path);
    const lib = url.protocol === "https:" ? https : http;
    return new Promise((resolve, reject) => {
        const req = lib.get(url, {
            headers: { Authorization: "Bearer " + TOKEN },
            rejectUnauthorized: TLS_VERIFY,
            timeout: 20000,
        }, (res) => {
            let body = "";
            res.setEncoding("utf8");
            res.on("data", (c) => {
                body += c;
                if (body.length > 32 * 1024 * 1024) {
                    req.destroy(new Error("answer over 32 MB"));
                }
            });
            res.on("end", () => {
                if (res.statusCode < 200 || res.statusCode > 299) {
                    return reject(new Error(`${path}: HTTP ${res.statusCode}`));
                }
                try {
                    resolve(JSON.parse(body));
                } catch (e) {
                    reject(new Error(`${path}: ${e.message}`));
                }
            });
        });
        req.on("timeout", () => req.destroy(new Error("timeout")));
        req.on("error", (e) => reject(new Error(`${path}: ${e.code || e.message}`)));
    });
}

// statusCodes turns a replication-manager expect-status ("200", "200,204") into Kuma's list.
function statusCodes(expect) {
    const codes = String(expect || "200").split(",").map((c) => c.trim()).filter((c) => /^[1-5][0-9]{2}$/.test(c));
    return codes.length ? codes : ["200"];
}

// noRouteCheck: the check of an app without route, the way replication-manager runs it
// (app-monitor-mode): http/https/https-cert a GET on the app host (a full name or an IP as is,
// a short name on the cluster address) and port (left out when it is the scheme's default)
// with its path and expected status, https-cert with the certificate checked; ping an ICMP
// echo; port (default) a TCP connect on the cluster address.
function noRouteCheck(app) {
    const cfg = app.config || {};
    const mode = String(cfg.appMonitorMode || "").trim().toLowerCase();
    if (mode === "ping") {
        return { type: "ping", hostname: app.host };
    }
    if (mode !== "http" && mode !== "https" && mode !== "https-cert") {
        return app.port ? { type: "port", hostname: app.host, port: Number(app.port) } : null;
    }
    const scheme = mode === "http" ? "http" : "https";
    const host = /[.:]/.test(cfg.appHost || "") ? cfg.appHost : app.host;
    const port = String(cfg.appPort || app.port || "");
    const defaultPort = (scheme === "https" && port === "443") || (scheme === "http" && port === "80");
    return {
        type: "http",
        url: `${scheme}://${host}${defaultPort || !port ? "" : ":" + port}${cfg.appMonitorPath || "/"}`,
        accepted_statuscodes: statusCodes(cfg.appMonitorExpectStatus),
        ignoreTls: mode !== "https-cert",
        expiryNotification: mode === "https-cert",
    };
}

// desired reads the cluster from replication-manager: name -> monitor fields. An app is checked
// on each http route the way replication-manager checks it (its monitor path and expected
// status, on the internal host and destination port), a tcp route on its port, an app without
// route by its monitor mode (noRouteCheck); a database server on its port; a proxy on its write
// and read ports.
async function desired() {
    const want = new Map();
    // the kind of each monitor (app, db, proxy, console): its default group on the status page
    const kinds = new Map();
    const set = (kind, name, f) => {
        // monitors carry no kind prefix (the group says it); two kinds with one name stay apart
        const key = want.has(name) ? `${name} (${kind})` : name;
        want.set(key, f);
        kinds.set(key, kind);
    };
    const c = encodeURIComponent(CLUSTER);
    const port = (kind, name, hostname, p) => set(kind, name, { type: "port", hostname, port: Number(p) });

    for (const app of await api(`/api/clusters/${c}/topology/apps`)) {
        if (!app.host || app.host === SELF_HOST) {
            continue;
        }
        // the app's routes come with the topology (replication-manager #2158): no deployment read
        const routes = app.routes || [];
        // one check per internal address: two public names on the same port and path share it
        const checks = new Map();
        const monitoredPorts = new Set(routes.filter((r) => r.monitor && r.monitor.path).map((r) => r.destPort || r.port || app.port));
        for (const r of routes) {
            const p = r.destPort || r.port || app.port;
            if (!p || (!(r.monitor && r.monitor.path) && monitoredPorts.has(p))) {
                continue; // a route without monitor on a port another route checks: that check covers it
            }
            if (r.protocol === "http" || r.protocol === "https") {
                const path = (r.monitor && r.monitor.path) || "/";
                checks.set(`:${p}${path === "/" ? "" : path}`, {
                    type: "http",
                    url: `http://${app.host}:${p}${path}`,
                    accepted_statuscodes: statusCodes(r.monitor && r.monitor.expectStatus),
                });
            } else {
                checks.set(`:${p}`, { type: "port", hostname: app.host, port: Number(p) });
            }
        }
        if (!checks.size) {
            const f = noRouteCheck(app);
            if (f) {
                checks.set(`:${app.port}`, f);
            }
        }
        for (const [where, f] of checks) {
            set("app", checks.size > 1 ? `${app.name} ${where}` : app.name, f);
        }
    }
    for (const s of await api(`/api/clusters/${c}/topology/servers`)) {
        if (s.host && s.port) {
            port("db", s.host.split(".")[0], s.host, s.port);
        }
    }
    for (const p of await api(`/api/clusters/${c}/topology/proxies`)) {
        if (!p.host) {
            continue;
        }
        const name = p.host.split(".")[0];
        if (p.writePort) {
            port("proxy", `${name} write`, p.host, p.writePort);
        }
        if (p.readPort && p.readPort !== p.writePort) {
            port("proxy", `${name} read`, p.host, p.readPort);
        }
    }
    // outside the cluster (EXTERNAL_MONITORS): checked as a visitor reaches them, certificate included
    for (const [name, url] of EXTERNAL) {
        set("console", name, { type: "http", url, accepted_statuscodes: ["200-299"], ignoreTls: false, expiryNotification: false });
    }
    return { want, kinds };
}

// writeViewers writes the emails allowed through oauth2-proxy (one per line), replacing the file
// in one rename so oauth2-proxy never reads half of it. A failed read keeps the current list.
async function writeViewers() {
    const emails = await api(`/api/clusters/${encodeURIComponent(CLUSTER)}/users/route-viewers`);
    if (!Array.isArray(emails)) {
        throw new Error("route-viewers: not a list");
    }
    const content = emails.filter((e) => typeof e === "string" && /^[^\s@]+@[^\s@]+$/.test(e)).join("\n") + "\n";
    let current = "";
    try {
        current = fs.readFileSync(SSO_EMAILS_FILE, "utf8");
    } catch (e) {
        // first write
    }
    if (current === content) {
        return;
    }
    fs.writeFileSync(SSO_EMAILS_FILE + ".tmp", content, { mode: 0o600 });
    fs.renameSync(SSO_EMAILS_FILE + ".tmp", SSO_EMAILS_FILE);
    log(`SSO: ${emails.length} user(s) with cluster-show-routes on ${CLUSTER} may open the page`);
}

// kumaLogin keeps Kuma's own login off behind the SSO and on without it: with the SSO, Kuma
// listens on 127.0.0.1 only and oauth2-proxy has checked the person.
async function kumaLogin(sock) {
    const s = await call(sock, "getSettings");
    if (!s || !s.ok) {
        throw new Error(`getSettings: ${s && s.msg}`);
    }
    const data = s.data || {};
    if (Boolean(data.disableAuth) === SSO) {
        return;
    }
    const r = await call(sock, "setSettings", { ...data, disableAuth: SSO }, PASSWORD);
    if (!r || !r.ok) {
        throw new Error(`setSettings: ${r && r.msg}`);
    }
    log(`Kuma's own login ${SSO ? "off (SSO in front)" : "on (no SSO)"}`);
}

// the default status page group of each kind of monitor
const DEFAULT_GROUP = { app: "Applications", db: "Databases", proxy: "Proxies", console: "Partner consoles" };

// kumaGet reads one of Kuma's own http endpoints (its public status page data).
function kumaGet(path) {
    return new Promise((resolve, reject) => {
        http.get(KUMA_URL + path, (res) => {
            let body = "";
            res.on("data", (d) => (body += d));
            res.on("end", () => {
                try {
                    res.statusCode === 200 ? resolve(JSON.parse(body)) : reject(new Error(`${path}: ${res.statusCode}`));
                } catch (e) {
                    reject(e);
                }
            });
        }).on("error", reject);
    });
}

// statusPage keeps the STATUS_PAGE_SLUG page. Kuma owns its groups: the sync reads them and only
// places each managed monitor that is in no group yet, in the default group of its kind (created
// when missing); groups, their names, order and members set in Kuma are kept. Saved only when a
// monitor was placed. Hand-made status pages are left alone.
async function statusPage(sock, managed, kinds, existing) {
    if (!STATUS_PAGE_SLUG) {
        return;
    }
    let page = await call(sock, "getStatusPage", STATUS_PAGE_SLUG);
    if (!page || !page.ok) {
        const r = await call(sock, "addStatusPage", STATUS_PAGE_TITLE, STATUS_PAGE_SLUG);
        if (!r || !r.ok) {
            throw new Error(`addStatusPage: ${r && r.msg}`);
        }
        page = await call(sock, "getStatusPage", STATUS_PAGE_SLUG);
        if (!page || !page.ok) {
            throw new Error(`getStatusPage: ${page && page.msg}`);
        }
        log(`status page ${STATUS_PAGE_SLUG} created`);
    }
    const data = await kumaGet(`/api/status-page/${STATUS_PAGE_SLUG}`);
    // the data may be cached by Kuma a few minutes: a monitor deleted since is dropped here
    const groups = (data.publicGroupList || []).map((g) => ({
        id: g.id,
        name: g.name,
        monitorList: (g.monitorList || []).filter((m) => existing.has(m.id)).map((m) => ({ id: m.id, sendUrl: m.sendUrl })),
    }));
    const placed = new Set(groups.flatMap((g) => g.monitorList.map((m) => m.id)));
    let added = 0;
    for (const m of [...managed].sort((a, b) => a.name.localeCompare(b.name))) {
        const name = DEFAULT_GROUP[kinds.get(m.name)];
        if (placed.has(m.id) || !name) {
            continue;
        }
        let g = groups.find((x) => x.name === name);
        if (!g) {
            g = { name, monitorList: [] };
            groups.push(g);
        }
        g.monitorList.push({ id: m.id });
        added++;
    }
    if (!added) {
        return;
    }
    const cfg = page.config || {};
    const r = await call(sock, "saveStatusPage", STATUS_PAGE_SLUG, { ...cfg, analyticsType: cfg.analyticsType || null }, cfg.icon || "", groups);
    if (!r || !r.ok) {
        throw new Error(`saveStatusPage: ${r && r.msg}`);
    }
    log(`status page ${STATUS_PAGE_SLUG}: ${added} monitor(s) placed in their default group`);
}

function newMonitor(name, fields) {
    return {
        name,
        description: MARK,
        interval: CHECK_INTERVAL,
        retryInterval: CHECK_INTERVAL,
        resendInterval: 0,
        maxretries: 2,
        timeout: Math.round(CHECK_INTERVAL * 0.8),
        method: "GET",
        maxredirects: 10,
        ignoreTls: true,
        upsideDown: false,
        expiryNotification: false,
        httpBodyEncoding: "json",
        accepted_statuscodes: ["200-299"],
        notificationIDList: {},
        conditions: [],
        kafkaProducerBrokers: [],
        kafkaProducerSaslOptions: {},
        rabbitmqNodes: [],
        active: true,
        ...fields,
    };
}

// changed tells whether a managed monitor no longer matches what the cluster wants.
function changed(m, f) {
    if (m.type !== f.type) {
        return true;
    }
    if (f.type === "http") {
        return m.url !== f.url || JSON.stringify(m.accepted_statuscodes) !== JSON.stringify(f.accepted_statuscodes) ||
            (f.ignoreTls !== undefined && Boolean(m.ignoreTls) !== f.ignoreTls) ||
            (f.expiryNotification !== undefined && Boolean(m.expiryNotification) !== f.expiryNotification);
    }
    if (f.type === "ping") {
        return m.hostname !== f.hostname;
    }
    return m.hostname !== f.hostname || Number(m.port) !== f.port;
}

async function session() {
    const sock = io(KUMA_URL, { transports: ["websocket"], reconnection: false });
    let list = null;
    sock.on("monitorList", (l) => {
        list = l;
    });
    try {
        await new Promise((resolve, reject) => {
            sock.once("connect", resolve);
            sock.once("connect_error", reject);
        });
        // Kuma wires its handlers a moment after the connection: ask again before giving up
        let need;
        for (let i = 0; i < 3 && need === undefined; i++) {
            need = await call(sock, "needSetup", 10000).catch(() => undefined);
        }
        if (need === undefined) {
            throw new Error("Kuma not ready (needSetup unanswered)");
        }
        if (need) {
            const r = await call(sock, "setup", USER, PASSWORD);
            if (!r.ok) {
                throw new Error(`setup: ${r.msg}`);
            }
            log(`Kuma admin ${USER} created`);
        }
        const r = await call(sock, "login", { username: USER, password: PASSWORD, token: "" });
        if (!r.ok) {
            throw new Error(`login: ${r.msg}`);
        }
        for (let i = 0; i < 30 && list === null; i++) {
            await sleep(1);
        }
        if (list === null) {
            throw new Error("no monitor list from Kuma");
        }

        await kumaLogin(sock);
        if (SSO) {
            await writeViewers();
        }

        // a failed read of the cluster changes nothing: never remove monitors on an API error
        const { want, kinds } = await desired();
        const managed = Object.values(list).filter((m) => m.description === MARK);
        const byName = new Map(managed.map((m) => [m.name, m]));
        let added = 0, edited = 0, removed = 0, renamed = 0;
        // monitors named by an older sync with their kind ("app x", "db x"...) are renamed in place:
        // same monitor, its history kept
        for (const m of managed) {
            const name = m.name.replace(/^(app|db|proxy|console) /, "");
            if (name === m.name || !want.has(name) || byName.has(name)) {
                continue;
            }
            const res = await call(sock, "editMonitor", { ...m, name });
            if (!res.ok) {
                log(`rename ${m.name}: ${res.msg}`);
                continue;
            }
            byName.delete(m.name);
            m.name = name;
            byName.set(name, m);
            renamed++;
        }
        for (const [name, f] of want) {
            const m = byName.get(name);
            if (!m) {
                const res = await call(sock, "add", newMonitor(name, f));
                res.ok ? added++ : log(`add ${name}: ${res.msg}`);
            } else if (changed(m, f)) {
                const res = await call(sock, "editMonitor", { ...m, ...f });
                res.ok ? edited++ : log(`edit ${name}: ${res.msg}`);
            }
        }
        if (want.size) {
            for (const m of managed) {
                if (!want.has(m.name)) {
                    const res = await call(sock, "deleteMonitor", m.id, false);
                    res.ok ? removed++ : log(`delete ${m.name}: ${res.msg}`);
                }
            }
        }
        if (added || edited || removed || renamed) {
            log(`${CLUSTER}: ${want.size} addresses, ${added} added, ${edited} changed, ${removed} removed, ${renamed} renamed`);
        }
        // the monitors as Kuma listed them at the start of this pass: one added now joins at the next
        await statusPage(sock, managed.filter((m) => want.has(m.name)), kinds, new Set(Object.values(list).map((m) => m.id)));
    } finally {
        sock.close();
    }
}

async function main() {
    for (const k of ["REPMAN_API_URL", "REPMAN_API_TOKEN", "REPMAN_CLUSTER", "KUMA_ADMIN_PASSWORD"]) {
        if (!env[k]) {
            log(`${k} is not set: nothing to sync`);
            for (;;) {
                await sleep(3600);
            }
        }
    }
    await sleep(15); // Kuma's start
    log(`syncing ${CLUSTER} from ${API} every ${SYNC_INTERVAL}s`);
    for (;;) {
        try {
            await session();
        } catch (e) {
            log(`sync failed: ${e.message}`);
        }
        await sleep(SYNC_INTERVAL);
    }
}

main();
