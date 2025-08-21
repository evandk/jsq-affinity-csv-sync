import axios from "axios";
import { parse } from "csv-parse/sync";
import stringSimilarity from "string-similarity";

const LIST_ID = 300305; // Affinity list id

const V2 = axios.create({
  baseURL: "https://api.affinity.co/v2",
  headers: {
    Authorization: `Bearer ${process.env.AFFINITY_V2_TOKEN}`,
    "Content-Type": "application/json",
  },
  timeout: 60000,
});

// Ordered statuses to support thresholding and non-downgrade (earliest → latest)
const STATUS_ORDER = [
  "Target Identified",
  "Intro/First Meeting",
  "Early Dialogue (Post-Intro)",
  "Deck & PPM Sent",
  "Circle Back after First Close",
  "Invited to Data Room",
  "Data Room Accessed / NDA Executed",
  "Verbal Commit",
  "Ready for Sub Docs",
  "Sub Docs Sent",
  "Sub Docs Signed",
  "Committed"
];
const STATUS_INDEX = new Map(STATUS_ORDER.map((s, i) => [s.toLowerCase(), i]));
const MIN_STATUS_LABEL = process.env.MIN_STATUS_LABEL || "Invited to Data Room";
const MAX_CSV_BYTES = Number(process.env.MAX_CSV_BYTES || 2_000_000); // ~2MB default
const REQUIRE_API_KEY = process.env.CSV_SYNC_API_KEY ? true : false;
const REDACT_RESPONSE = process.env.REDACT_RESPONSE === '1';

function normalizeName(name) {
  const raw = String(name || "").normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  return raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function safeBestMatch(main, arr, minScore = 0.92) {
  try {
    const query = String(main ?? "");
    const candidates = Array.isArray(arr) ? arr.map(x => String(x ?? "")).filter(s => s.length > 0) : [];
    if (!query || candidates.length === 0) return null;
    const { bestMatch } = stringSimilarity.findBestMatch(query, candidates);
    if (bestMatch && bestMatch.rating >= minScore) return bestMatch.target;
    return null;
  } catch {
    return null;
  }
}

function isAuthorized(req) {
  if (!REQUIRE_API_KEY) return true;
  const apiKey = process.env.CSV_SYNC_API_KEY;
  const hdrKey = req.headers['x-api-key'] || req.headers['X-API-Key'] || req.headers['x-api_key'];
  if (hdrKey && String(hdrKey) === apiKey) return true;
  const auth = String(req.headers['authorization'] || '');
  if (auth.startsWith('Basic ')) {
    try {
      const b64 = auth.slice(6);
      const [u, p] = Buffer.from(b64, 'base64').toString('utf8').split(':');
      const userOk = process.env.BASIC_AUTH_USER ? u === process.env.BASIC_AUTH_USER : true;
      const passOk = process.env.BASIC_AUTH_PASS ? p === process.env.BASIC_AUTH_PASS : true;
      return userOk && passOk;
    } catch { /* ignore */ }
  }
  if (auth.startsWith('Bearer ')) {
    const token = auth.slice(7).trim();
    if (token && token === apiKey) return true;
  }
  return false;
}

function isPassedLabel(label) {
  const l = String(label || '').trim().toLowerCase();
  return l === 'passed' || l === 'declined' || l === 'no go' || l === 'no-go' || l === 'nog o';
}

async function fetchEntryStatusLabel(entryId, statusFieldId) {
  try {
    const { data } = await V2.get(`/lists/${LIST_ID}/list-entries/${entryId}/fields`);
    const field = (data?.data || []).find(f => String(f.id) === String(statusFieldId));
    return field?.value?.data?.text || '';
  } catch {
    return '';
  }
}

async function fetchEntriesWithStatus(statusFieldId) {
  const entries = [];
  const currentStatusById = new Map();
  let nextUrl = `/lists/${LIST_ID}/list-entries?fieldIds[]=${encodeURIComponent(statusFieldId)}`;
  while (nextUrl) {
    const { data } = await V2.get(nextUrl);
    const batch = data?.data || [];
    for (const e of batch) {
      entries.push(e);
      const f = (e.fields || []).find(x => String(x.id) === String(statusFieldId));
      const label = f?.value?.data?.text ? String(f.value.data.text) : "";
      if (label) currentStatusById.set(e.id, label);
    }
    nextUrl = data?.pagination?.nextUrl || null;
  }
  return { entries, currentStatusById };
}

async function buildLabelMapFromEntries(statusFieldId) {
  const labelToId = new Map();
  let nextUrl = `/lists/${LIST_ID}/list-entries?fieldIds[]=${encodeURIComponent(statusFieldId)}`;
  while (nextUrl) {
    const { data } = await V2.get(nextUrl);
    const entries = data?.data || [];
    for (const e of entries) {
      const f = (e.fields || []).find(x => String(x.id) === String(statusFieldId));
      const v = f?.value?.data;
      const text = v?.text;
      const optId = v?.dropdownOptionId;
      if (text && optId) {
        labelToId.set(String(text).toLowerCase(), optId);
      }
    }
    nextUrl = data?.pagination?.nextUrl || null;
  }
  return labelToId;
}

function mergeManualOverrides(labelToId) {
  try {
    const raw = process.env.STATUS_LABEL_TO_ID_JSON;
    if (!raw) return;
    const obj = JSON.parse(raw);
    Object.entries(obj).forEach(([k, v]) => {
      const key = String(k).toLowerCase();
      const val = Number(v);
      if (Number.isFinite(val)) labelToId.set(key, val);
    });
  } catch {/* ignore */}
}

function applyAlias(targetLabel) {
  try {
    const raw = process.env.STATUS_LABEL_ALIASES_JSON;
    if (!raw) return targetLabel;
    const obj = JSON.parse(raw);
    const lower = String(targetLabel || '').toLowerCase();
    const alias = obj[lower];
    return alias || targetLabel;
  } catch { return targetLabel; }
}

function mapSubStatusWithOverrides(sub) {
  try {
    const raw = process.env.SUB_STATUS_TO_STAGE_JSON;
    if (!raw) return null;
    const obj = JSON.parse(raw); // keys lowercased
    const key = String(sub || '').toLowerCase();
    return obj[key] || null;
  } catch { return null; }
}

function accessedFromDetail(detailRaw) {
  const raw = String(detailRaw || '').trim();
  if (!raw) return false;
  const parts = raw.split(';');
  for (let part of parts) {
    const p = part.trim().toLowerCase();
    if (!p) continue;
    if (p.includes('not yet accessed') || p.includes('not yet')) {
      continue;
    }
    // any date-like token or digits imply accessed
    if (/[0-9]/.test(p) || /(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i.test(part)) {
      return true;
    }
  }
  return false;
}

async function fetchStatusFieldAndOptions() {
  const { data } = await V2.get(`/lists/${LIST_ID}/fields`);
  const fields = data?.data || [];
  // Prefer explicit env var if provided
  const desiredName = String(process.env.STATUS_FIELD_NAME || "Status").toLowerCase();
  let statusField = fields.find(f => String(f?.name || "").toLowerCase() === desiredName);
  if (!statusField) {
    // Fallback: any dropdown-like field whose name contains 'status'
    statusField = fields.find(f => /status/i.test(String(f?.name || "")));
  }
  if (!statusField) throw new Error("Could not find Status field on this list");

  const options = statusField.dropdown_options || statusField.dropdownOptions || statusField.options || [];
  let labelToId = new Map(options.map(o => [String(o?.name || o?.label || "").toLowerCase(), o?.id]));
  // Fallback: learn map by scanning entries when options are not available via fields API
  if (labelToId.size === 0) {
    labelToId = await buildLabelMapFromEntries(statusField.id);
  }
  // Manual overrides (hard-coded mapping)
  mergeManualOverrides(labelToId);
  return { statusFieldId: statusField.id, labelToId, field: statusField };
}

async function updateStatus(entryId, statusFieldId, optionId, valueType) {
  const type = String(valueType || 'ranked-dropdown');
  const payload = {
    value: {
      type,
      data: { dropdownOptionId: optionId }
    }
  };
  await V2.post(`/lists/${LIST_ID}/list-entries/${entryId}/fields/${statusFieldId}`, payload);
}

// Safely read CSV body without destroying the request stream
async function readCsvBody(req, limitBytes) {
  return await new Promise((resolve) => {
    let data = "";
    let total = 0;
    let done = false;
    function finish(result) { if (done) return; done = true; resolve(result); }
    try { req.setEncoding('utf8'); } catch {}
    req.on('data', (chunk) => {
      if (done) return;
      total += chunk.length;
      if (limitBytes && total > limitBytes) {
        try { req.pause(); } catch {}
        req.removeAllListeners('data');
        req.removeAllListeners('end');
        return finish({ text: "", tooLarge: true });
      }
      data += chunk;
    });
    req.on('end', () => finish({ text: data, tooLarge: false }));
    req.on('error', () => finish({ text: "", tooLarge: false }));
    req.on('aborted', () => finish({ text: "", tooLarge: false }));
  });
}

function deriveStatusLabelFromRow(row) {
  const get = (k) => String(row[k] ?? row[String(k).replace(/^\s+|\s+$/g, "")] ?? "").trim();
  const lower = (s) => String(s || "").toLowerCase();
  const anyVal = (re) => {
    for (const k of Object.keys(row)) {
      if (re.test(k)) return String(row[k] ?? "");
    }
    return "";
  };

  // Prefer subscription and data room signals
  const subRaw = get("Subscription Status") || get("Subscription");
  const subAny = anyVal(/subscription/i);
  const sub = lower(subRaw || subAny);
  const override = mapSubStatusWithOverrides(sub);
  if (override) return override;

  const dataRoomGranted = lower(get("Data room granted"));
  const dataRoomAccessDetailRaw = get("Data room access detail") || get(" Data room access detail") || anyVal(/data\s*room.*access/i);
  const dataRoomLastAccessed = lower(get("Data room last accessed"));

  // High-priority from subscription
  if (/counter\s*-?signed|fully\s*executed|executed|signed/.test(sub)) return "Sub Docs Signed";
  if (/awaiting.*investor.*signature|staff review/.test(sub)) return "Ready for Sub Docs";
  if (/started|draft|invited/.test(sub)) return "Sub Docs Sent";

  // Data room signals (detail first, then explicit last accessed)
  if (accessedFromDetail(dataRoomAccessDetailRaw)) return "Data Room Accessed / NDA Executed";
  if (dataRoomLastAccessed && !/not yet accessed|not\s*yet/.test(dataRoomLastAccessed)) return "Data Room Accessed / NDA Executed";
  if (/granted|yes|y/.test(dataRoomGranted)) return "Invited to Data Room";

  // Fallback to prospect-level hints (ignore soft-circled as authoritative)
  const prospectStatus = lower(get("Prospect Status"));
  const latestUpdate = lower(get("Latest update"));
  if (prospectStatus.includes('soft') && prospectStatus.includes('circled')) return "";
  if (/ppm|pitch\s*deck|deck\s*sent/.test(latestUpdate)) return "Deck & PPM Sent";
  if (/first\s*meeting|meeting\s+scheduled|intro\s*call|intro\b/.test(latestUpdate) || /intro|first\s*meeting/.test(prospectStatus)) return "Intro/First Meeting";
  if (/contacted|engaged|early\s*dialogue/.test(prospectStatus)) return "Early Dialogue (Post-Intro)";
  if (/target\s*identified/.test(prospectStatus) || prospectStatus === "new") return "Target Identified";

  return "";
}

function resolveStatusOptionId(statusLabel, labelToId) {
  if (!statusLabel) return null;
  const effective = applyAlias(statusLabel);
  const norm = String(effective).toLowerCase();
  // Direct
  if (labelToId.has(norm)) return labelToId.get(norm);
  const labels = Array.from(labelToId.keys());
  // Synonym patterns (broadened)
  const hasAll = (l, pats) => pats.every(p => l.includes(p));
  const pickBy = (patternsArr) => labels.find(l => hasAll(l, patternsArr));
  let candidate = null;
  if (!candidate && (norm.includes('data') || norm.includes('nda'))) {
    candidate = pickBy(['nda']) || pickBy(['data','room']) || pickBy(['access']);
  }
  if (!candidate && norm.includes('ready')) {
    candidate = pickBy(['ready','sub']) || pickBy(['ready','doc']);
  }
  if (!candidate && norm.includes('sent')) {
    candidate = pickBy(['sent','sub']) || pickBy(['sent','doc']);
  }
  if (!candidate && norm.includes('signed')) {
    candidate = pickBy(['sign','sub']) || pickBy(['execut']);
  }
  if (!candidate && norm.includes('verbal')) {
    candidate = pickBy(['verbal']);
  }
  if (!candidate && (norm.includes('deck') || norm.includes('ppm'))) {
    candidate = pickBy(['deck']) || pickBy(['ppm']) || pickBy(['material']);
  }
  if (!candidate && norm.includes('intro')) {
    candidate = pickBy(['intro']);
  }
  if (!candidate && norm.includes('early')) {
    candidate = pickBy(['early']);
  }
  if (!candidate && norm.includes('target')) {
    candidate = pickBy(['target']) || pickBy(['new']);
  }
  if (!candidate && norm.includes('commit')) {
    candidate = pickBy(['commit']);
  }
  if (candidate && labelToId.has(candidate)) return labelToId.get(candidate);
  // Fuzzy (safe)
  const target = safeBestMatch(norm, labels, 0.80);
  return target ? labelToId.get(target) : null;
}

function redact(results) {
  if (!REDACT_RESPONSE) return results;
  return results.map(r => ({
    matched: r.matched,
    updated: r.updated,
    wouldUpdate: r.wouldUpdate,
    reason: r.reason
  }));
}

export default async function handler(req, res) {
  try {
    // Security headers
    res.setHeader('Cache-Control', 'no-store');

    if (req.method !== "POST") return res.status(405).json({ ok: false, error: "Use POST" });
    if (!process.env.AFFINITY_V2_TOKEN) return res.status(500).json({ ok: false, error: "Missing AFFINITY_V2_TOKEN" });

    if (!isAuthorized(req)) return res.status(401).json({ ok: false, error: "Unauthorized" });

    const isDryRun = (() => {
      try {
        const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
        return url.searchParams.get('dry') === '1';
      } catch { return false; }
    })();

    // Read body (support raw text CSV or JSON { csv })
    const contentType = String(req.headers["content-type"] || "").toLowerCase();
    const contentLen = Number(req.headers['content-length'] || 0);
    if (contentLen && contentLen > MAX_CSV_BYTES) {
      return res.status(413).json({ ok: false, error: "CSV too large" });
    }

    let csvText = "";
    if (contentType.includes("text/csv") || contentType.includes("application/octet-stream") || contentType.includes("text/plain")) {
      if (typeof req.body === "string") {
        csvText = req.body;
      } else if (Buffer.isBuffer(req.body)) {
        csvText = req.body.toString("utf8");
      } else {
        const { text, tooLarge } = await readCsvBody(req, MAX_CSV_BYTES);
        if (tooLarge) return res.status(413).json({ ok: false, error: "CSV too large" });
        csvText = text;
      }
    } else {
      // Support JSON uploads: { csv: "..." }
      const raw = typeof req.body === "string" ? req.body : (Buffer.isBuffer(req.body) ? req.body.toString("utf8") : "");
      const parsed = raw ? JSON.parse(raw) : (req.body || {});
      csvText = parsed.csv || "";
    }

    if (!csvText) return res.status(400).json({ ok: false, error: "No CSV provided" });
    if (Buffer.byteLength(csvText, 'utf8') > MAX_CSV_BYTES) return res.status(413).json({ ok: false, error: "CSV too large" });

    // Parse CSV
    const records = parse(csvText, {
      columns: true,
      skip_empty_lines: true
    });

    // Expect a name column; status may be direct or derived
    const wantsRaw = records.map(r => ({
      name: String(r.Name || r["Investor Name"] || r["LP Name"] || r["Organization"] || r["Contacts"] || r["Contact"] || "").split(';')[0].trim(),
      raw: r
    })).filter(r => r.name);
    if (!wantsRaw.length) return res.status(400).json({ ok: false, error: "CSV has no Name column" });

    // Discover Status field + options from Affinity
    const { statusFieldId, labelToId, field: statusField } = await fetchStatusFieldAndOptions();

    // Load Affinity entries and current status
    const { entries, currentStatusById } = await fetchEntriesWithStatus(statusFieldId);
    const nameToEntry = new Map();
    for (const e of entries) {
      const nm = normalizeName(e?.entity?.name || (e?.entity?.first_name && `${e.entity.first_name} ${e.entity.last_name}`) || "");
      if (nm) nameToEntry.set(nm, e);
    }

    const knownOptions = Array.from(new Set(Array.from(labelToId.keys())));
    const minIdx = STATUS_INDEX.get(String(MIN_STATUS_LABEL).toLowerCase()) ?? -1;

    const results = [];
    for (const rec of wantsRaw) {
      const targetNorm = normalizeName(rec.name);
      let entry = nameToEntry.get(targetNorm);

      // Fallback fuzzy (safe)
      if (!entry) {
        const candidates = Array.from(nameToEntry.keys());
        const target = safeBestMatch(targetNorm, candidates, 0.88);
        if (target) entry = nameToEntry.get(target);
      }

      const statusLabel = deriveStatusLabelFromRow(rec.raw);
      if (!entry) {
        results.push({ name: rec.name, statusLabel, matched: false });
        continue;
      }

      // Hard lock: do not change if currently Passed (or synonyms). Use bulk map, then per-entry fallback.
      let currentLabel = String(currentStatusById.get(entry.id) || "");
      if (!currentLabel) currentLabel = await fetchEntryStatusLabel(entry.id, statusFieldId);
      if (isPassedLabel(currentLabel)) {
        results.push({ name: rec.name, statusLabel, matched: true, entryId: entry.id, updated: false, reason: `Currently '${currentLabel}'; no change` });
        continue;
      }

      // Enforce minimum threshold and non-downgrade
      const currentIdx = STATUS_INDEX.get(currentLabel.toLowerCase());
      const derivedIdx = STATUS_INDEX.get(String(statusLabel || '').toLowerCase());

      if ((minIdx !== -1 && (derivedIdx ?? -1) < minIdx)) {
        results.push({ name: rec.name, statusLabel, matched: true, entryId: entry.id, updated: false, reason: `Status before minimum threshold (${MIN_STATUS_LABEL})` });
        continue;
      }
      if ((currentIdx ?? -1) !== -1 && (derivedIdx ?? -1) !== -1 && derivedIdx < currentIdx) {
        results.push({ name: rec.name, statusLabel, matched: true, entryId: entry.id, updated: false, reason: `Would downgrade from '${currentLabel}' to '${statusLabel}'` });
        continue;
      }
      if (currentLabel && currentLabel.toLowerCase() === String(statusLabel || '').toLowerCase()) {
        results.push({ name: rec.name, statusLabel, matched: true, entryId: entry.id, updated: false, reason: 'Unchanged' });
        continue;
      }

      const optionId = resolveStatusOptionId(statusLabel, labelToId);
      if (!statusLabel) {
        results.push({ name: rec.name, matched: true, entryId: entry.id, updated: false, reason: "Could not derive status from CSV row" });
        continue;
      }
      if (!optionId) {
        results.push({ name: rec.name, statusLabel, matched: true, entryId: entry.id, updated: false, reason: `Unknown status '${statusLabel}' for field '${statusField.name}'`, knownOptions });
        continue;
      }

      if (isDryRun) {
        results.push({ name: rec.name, statusLabel, matched: true, entryId: entry.id, updated: false, wouldUpdate: true });
        continue;
      }

      try {
        await updateStatus(entry.id, statusFieldId, optionId, statusField.valueType || statusField.value_type);
        results.push({ name: rec.name, statusLabel, matched: true, entryId: entry.id, updated: true });
      } catch (e) {
        results.push({ name: rec.name, statusLabel, matched: true, entryId: entry.id, updated: false, error: e?.response?.data || e.message });
      }
    }

    return res.status(200).json({ ok: true, total: wantsRaw.length, results: redact(results) });
  } catch (e) {
    const status = e?.response?.status || 500;
    const data = e?.response?.data || e.message;
    return res.status(500).json({ ok: false, error: { status, data } });
  }
}


