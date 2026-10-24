// kuma-sync: keeps one Uptime Kuma monitor per internal address of a replication-manager
// cluster (its apps, database servers and proxies), read from the replication-manager API.
// It runs next to Kuma (start.sh) and talks to it on its socket.io API, with Kuma's own
// socket.io-client. Only the monitors it created are changed or removed: they carry the
// description "managed by kuma-sync (<cluster>)"; monitors added by hand are left alone.
//
// Environment:
//   REPMAN_API_URL, REPMAN_API_TOKEN  replication-manager API and a read-only API token
//   REPMAN_CLUSTER                    the cluster (namespace) whose addresses are monitored
//   KUMA_ADMIN_USER, KUMA_ADMIN_PASSWORD  Kuma's admin, created at the first start
//   SYNC_INTERVAL (60 s), MONITOR_INTERVAL (60 s), KUMA_URL (http://localhost:3001)
//   SELF_HOST                         this app's own host, not monitored
"use strict";
const { io } = require("/app/node_modules/socket.io-client");

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
const MARK = `managed by kuma-sync (${CLUSTER})`;

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

async function api(path) {
    let res;
    try {
        res = await fetch(API + path, {
            headers: { Authorization: "Bearer " + TOKEN },
            signal: AbortSignal.timeout(20000),
        });
    } catch (e) {
        // fetch only says "fetch failed": the cause (DNS, refused, TLS) is underneath
        throw new Error(`${path}: ${(e.cause && (e.cause.code || e.cause.message)) || e.message}`);
    }
    if (!res.ok) {
        throw new Error(`${path}: HTTP ${res.status}`);
    }
    return res.json();
}

// statusCodes turns a replication-manager expect-status ("200", "200,204") into Kuma's list.
function statusCodes(expect) {
    const codes = String(expect || "200").split(",").map((c) => c.trim()).filter((c) => /^[1-5][0-9]{2}$/.test(c));
    return codes.length ? codes : ["200"];
}

// desired reads the cluster from replication-manager: name -> monitor fields. An app is checked
// on each http route the way replication-manager checks it (its monitor path and expected
// status, on the internal host and destination port), a tcp route or an app without route on
// its port; a database server on its port; a proxy on its write and read ports.
async function desired() {
    const want = new Map();
    const c = encodeURIComponent(CLUSTER);
    const port = (name, hostname, p) => want.set(name, { type: "port", hostname, port: Number(p) });

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
        if (!checks.size && app.port) {
            checks.set(`:${app.port}`, { type: "port", hostname: app.host, port: Number(app.port) });
        }
        for (const [where, f] of checks) {
            want.set(checks.size > 1 ? `app ${app.name} ${where}` : `app ${app.name}`, f);
        }
    }
    for (const s of await api(`/api/clusters/${c}/topology/servers`)) {
        if (s.host && s.port) {
            port(`db ${s.host.split(".")[0]}`, s.host, s.port);
        }
    }
    for (const p of await api(`/api/clusters/${c}/topology/proxies`)) {
        if (!p.host) {
            continue;
        }
        const name = p.host.split(".")[0];
        if (p.writePort) {
            port(`proxy ${name} write`, p.host, p.writePort);
        }
        if (p.readPort && p.readPort !== p.writePort) {
            port(`proxy ${name} read`, p.host, p.readPort);
        }
    }
    return want;
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
        return m.url !== f.url || JSON.stringify(m.accepted_statuscodes) !== JSON.stringify(f.accepted_statuscodes);
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

        // a failed read of the cluster changes nothing: never remove monitors on an API error
        const want = await desired();
        const managed = Object.values(list).filter((m) => m.description === MARK);
        const byName = new Map(managed.map((m) => [m.name, m]));
        let added = 0, edited = 0, removed = 0;
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
        if (added || edited || removed) {
            log(`${CLUSTER}: ${want.size} addresses, ${added} added, ${edited} changed, ${removed} removed`);
        }
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
