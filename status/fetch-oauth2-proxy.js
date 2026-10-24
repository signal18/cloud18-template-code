// fetch-oauth2-proxy VERSION SHA256 DIR: downloads the oauth2-proxy release tarball (linux amd64),
// refuses it unless its sha256 is the pinned one, and extracts the binary to DIR/oauth2-proxy.
"use strict";
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");

const [version, sha256, dir] = process.argv.slice(2);
const name = `oauth2-proxy-${version}.linux-amd64`;
const url = `https://github.com/oauth2-proxy/oauth2-proxy/releases/download/${version}/${name}.tar.gz`;
const MAX = 64 * 1024 * 1024;

(async () => {
    const res = await fetch(url, { signal: AbortSignal.timeout(120000) });
    if (!res.ok) {
        throw new Error(`${url}: HTTP ${res.status}`);
    }
    if (Number(res.headers.get("content-length")) > MAX) {
        throw new Error(`${url}: over ${MAX} bytes`);
    }
    const body = Buffer.from(await res.arrayBuffer());
    if (body.length > MAX) {
        throw new Error(`${url}: over ${MAX} bytes`);
    }
    const got = crypto.createHash("sha256").update(body).digest("hex");
    if (got !== sha256) {
        throw new Error(`${name}.tar.gz: sha256 ${got}, expected ${sha256}`);
    }
    const tgz = path.join(dir, `${name}.tar.gz`);
    fs.writeFileSync(tgz, body);
    execFileSync("tar", ["-xzf", tgz, "-C", dir, "--strip-components=1", `${name}/oauth2-proxy`]);
    fs.unlinkSync(tgz);
    fs.chmodSync(path.join(dir, "oauth2-proxy"), 0o755);
    console.log(`oauth2-proxy ${version} ready (sha256 checked)`);
})().catch((e) => {
    console.error("fetch-oauth2-proxy:", e.message);
    process.exit(1);
});
