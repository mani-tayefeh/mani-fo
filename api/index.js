import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

const STRIP_HEADERS = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

function buildHeaders(req) {
  const headers = {};
  let clientIp;

  for (const [key, value] of Object.entries(req.headers)) {
    const k = key.toLowerCase();

    if (STRIP_HEADERS.has(k)) continue;
    if (k.startsWith("x-vercel-")) continue;

    if (k === "x-real-ip") {
      clientIp = value;
      continue;
    }

    if (k === "x-forwarded-for") {
      clientIp ||= value;
      continue;
    }

    headers[k] = Array.isArray(value) ? value.join(", ") : value;
  }

  if (clientIp) headers["x-forwarded-for"] = clientIp;

  return headers;
}

function buildFetchOptions(req, headers) {
  const method = req.method;
  const hasBody = method !== "GET" && method !== "HEAD";

  const options = {
    method,
    headers,
    redirect: "manual",
  };

  if (hasBody) {
    options.body = Readable.toWeb(req);
    options.duplex = "half";
  }

  return options;
}

function copyUpstreamHeaders(upstream, res) {
  for (const [key, value] of upstream.headers) {
    if (key.toLowerCase() === "transfer-encoding") continue;
    try {
      res.setHeader(key, value);
    } catch {}
  }
}

export default async function handler(req, res) {
  if (!TARGET_BASE) {
    res.statusCode = 500;
    return res.end("Misconfigured: TARGET_DOMAIN is not set");
  }

  const targetUrl = TARGET_BASE + req.url;

  try {
    const headers = buildHeaders(req);
    const fetchOpts = buildFetchOptions(req, headers);

    const upstream = await fetch(targetUrl, fetchOpts);

    res.statusCode = upstream.status;
    copyUpstreamHeaders(upstream, res);

    if (upstream.body) {
      await pipeline(Readable.fromWeb(upstream.body), res);
    } else {
      res.end();
    }
  } catch (err) {
    console.error("relay error:", err);

    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway: Tunnel Failed");
    }
  }
}
