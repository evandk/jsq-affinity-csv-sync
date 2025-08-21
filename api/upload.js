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
const PREFER_ORGANIZATIONS = process.env.PREFER_ORGANIZATIONS === '1';

function normalizeAscii(s) {
  return String(s || "").normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function normalizeName(name) {
  const raw = normalizeAscii(name);
  return raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function stripLegalSuffixes(orgName) {
  const n = normalizeName(orgName);
  // remove common corporate suffixes at end
  return n
    .replace(/\b(incorporated|inc|ltd|limited|llc|l\.l\.c\.|llp|l\.l\.p\.|lp|l\.p\.|plc|gmbh|sarl|s\.a\.?|ag|bv|b\.v\.)\b\.?$/g, "")
    .replace(/\b(co|co\.|company|partners|holdings|capital)\b$/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeOrgKey(orgName) {
  return stripLegalSuffixes(orgName);
}

function normalizePersonKey(name) {
  const n = normalizeName(name).replace(/\b(jr|sr|ii|iii|iv)\b/g, "").replace(/\s+/g, " ").trim();
  return n;
}

function firstLastFromPersonName(name) {
  const n = normalizePersonKey(name);
  const parts = n.split(' ').filter(Boolean);
  if (parts.length === 0) return { first: "", last: "" };
  if (parts.length === 1) return { first: parts[0], last: "" };
  return { first: parts[0], last: parts[parts.length - 1] };
}

function loadNicknameAliases() {
  const defaults = {
    "alexander": ["alex"],
    "andrew": ["andy"],
    "anthony": ["tony"],
    "benjamin": ["ben"],
    "charles": ["charlie", "chuck"],
    "christopher": ["chris"],
    "daniel": ["dan", "danny"],
    "david": ["dave"],
    "elizabeth": ["liz", "beth", "lizzy", "eliza"],
    "jacob": ["jake"],
    "james": ["jim", "jimmy"],
    "jonathan": ["jon"],
    "joshua": ["josh"],
    "katherine": ["kate", "katie", "kat"],
    "louis": ["lou"],
    "matthew": ["matt"],
    "michael": ["mike"],
    "nicholas": ["nick"],
    "patrick": ["pat"],
    "robert": ["rob", "bob", "bobby"],
    "steven": ["steve"],
    "stephen": ["steve"],
    "thomas": ["tom"],
    "william": ["will", "bill", "billy"]
  };
  try {
    const raw = process.env.NICKNAME_ALIASES_JSON;
    if (!raw) return defaults;
    const custom = JSON.parse(raw);
    Object.entries(custom).forEach(([k, v]) => {
      const key = String(k).toLowerCase();
      const arr = Array.isArray(v) ? v.map(x => String(x).toLowerCase()) : [String(v).toLowerCase()];
      defaults[key] = Array.from(new Set([...(defaults[key] || []), ...arr]));
    });
    return defaults;
  } catch { return defaults; }
}

const NICKNAMES = loadNicknameAliases();

function personKeyVariants(name) {
  const { first, last } = firstLastFromPersonName(name);
  const variants = new Set();
  if (!first && !last) return [];
  const firsts = new Set([first]);
  const nick = NICKNAMES[first];
  if (nick && nick.length) nick.forEach(n => firsts.add(n));
  // Also if first looks like nickname, add possible canonical forms
  Object.entries(NICKNAMES).forEach(([canon, arr]) => {
    if (arr.includes(first)) firsts.add(canon);
  });
  for (const f of firsts) {
    const key = normalizeName(`${f} ${last}`);
    variants.add(key);
  }
  return Array.from(variants);
}

// Heuristics to classify entity person vs organization, even when first/last are absent
function isLikelyPersonName(name) {
  const n = normalizeName(name);
  const parts = n.split(' ').filter(Boolean);
  if (parts.length < 2 || parts.length > 4) return false;
  const orgKeywords = new Set(["inc","llc","ltd","limited","capital","partners","partner","holdings","group","ventures","foundation","family","company","co","plc","bv","gmbh","sarl","ag","llp","lp","org","foundation"]);
  return !parts.some(p => orgKeywords.has(p));
}
function classifyEntityType(entity) {
  const ent = entity || {};
  const typeRaw = String(ent.type || ent.entity_type || "").toLowerCase();
  if (/person|people|contact/.test(typeRaw)) return 'person';
  if (/org|company|organization/.test(typeRaw)) return 'organization';
  const first = ent.first_name ? String(ent.first_name) : "";
  const last = ent.last_name ? String(ent.last_name) : "";
  if (first || last) return 'person';
  const name = String(ent.name || "");
  if (isLikelyPersonName(name)) return 'person';
  return 'organization';
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

function safeBestMatchWithRating(main, arr) {
  try {
    const query = String(main ?? "");
    const candidates = Array.isArray(arr) ? arr.map(x => String(x ?? "")).filter(s => s.length > 0) : [];
    if (!query || candidates.length === 0) return null;
    const { bestMatch } = stringSimilarity.findBestMatch(query, candidates);
    return bestMatch || null;
  } catch { return null; }
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

function extractNamesFromValueData(val) {
  const out = [];
  if (!val) return out;
  const consume = (obj) => {
    if (!obj) return;
    const n = obj.name || (obj.first_name && obj.last_name ? `${obj.first_name} ${obj.last_name}` : (obj.first_name || obj.last_name));
    if (n) out.push(String(n));
    else if (obj.text) out.push(String(obj.text));
  };
  if (Array.isArray(val)) {
    val.forEach(consume);
  } else if (Array.isArray(val.entities)) {
    val.entities.forEach(consume);
  } else if (val.entity) {
    consume(val.entity);
  } else if (val.name || val.first_name || val.last_name || val.text) {
    consume(val);
  }
  return Array.from(new Set(out));
}

function extractAssociatedNamesFromFields(fields, peopleFieldIds, orgFieldIds) {
  const assocPeople = [];
  const assocOrgs = [];
  const peopleSet = new Set((peopleFieldIds || []).map(String));
  const orgSet = new Set((orgFieldIds || []).map(String));

  // If we have explicit ids, only use them; else scan all fields heuristically
  const scanAll = peopleSet.size === 0 && orgSet.size === 0;

  for (const f of fields || []) {
    const id = String(f.id);
    const val = f?.value?.data;
    if (!val) continue;
    if (scanAll || orgSet.has(id)) {
      const maybe = extractNamesFromValueData(val);
      if (maybe.length) assocOrgs.push(...maybe);
    }
    if (scanAll || peopleSet.has(id)) {
      const maybe = extractNamesFromValueData(val);
      if (maybe.length) assocPeople.push(...maybe);
    }
  }
  return { assocPeople: Array.from(new Set(assocPeople)), assocOrgs: Array.from(new Set(assocOrgs)) };
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

  // Detect People and Organization fields (broader patterns)
  const peopleFieldIds = fields
    .filter(f => /(people|contacts?|contact)/i.test(String(f?.name || "")))
    .map(f => f.id);
  const orgFieldIds = fields
    .filter(f => /(organization|organizations|company|firm|employer|org)/i.test(String(f?.name || "")))
    .map(f => f.id);

  const options = statusField.dropdown_options || statusField.dropdownOptions || statusField.options || [];
  let labelToId = new Map(options.map(o => [String(o?.name || o?.label || "").toLowerCase(), o?.id]));
  // Fallback: learn map by scanning entries when options are not available via fields API
  if (labelToId.size === 0) {
    labelToId = await buildLabelMapFromEntries(statusField.id);
  }
  // Manual overrides (hard-coded mapping)
  mergeManualOverrides(labelToId);
  return { statusFieldId: statusField.id, labelToId, field: statusField, peopleFieldIds, orgFieldIds };
}

async function fetchEntriesWithStatus(statusFieldId, peopleFieldIds, orgFieldIds) {
  const entries = [];
  const currentStatusById = new Map();
  const associationsById = new Map();
  const fids = [statusFieldId].filter(Boolean);
  for (const fid of (peopleFieldIds || [])) fids.push(fid);
  for (const fid of (orgFieldIds || [])) fids.push(fid);
  const query = fids.map(fid => `fieldIds[]=${encodeURIComponent(String(fid))}`).join('&');
  let nextUrl = `/lists/${LIST_ID}/list-entries?${query}`;
  while (nextUrl) {
    const { data } = await V2.get(nextUrl);
    const batch = data?.data || [];
    for (const e of batch) {
      entries.push(e);
      const f = (e.fields || []).find(x => String(x.id) === String(statusFieldId));
      const label = f?.value?.data?.text ? String(f.value.data.text) : "";
      if (label) currentStatusById.set(e.id, label);
      const assoc = extractAssociatedNamesFromFields(e.fields || [], peopleFieldIds, orgFieldIds);
      // Include the entity itself as an association to aid pairing
      const ent = e?.entity || {};
      const entType = classifyEntityType(ent);
      if (entType === 'person') {
        const display = (ent.first_name || ent.last_name) ? `${ent.first_name || ''} ${ent.last_name || ''}`.trim() : String(ent.name || '').trim();
        if (display) assoc.assocPeople.push(display);
      } else {
        const nm = String(ent.name || '').trim();
        if (nm) assoc.assocOrgs.push(nm);
      }
      assoc.assocPeople = Array.from(new Set(assoc.assocPeople));
      assoc.assocOrgs = Array.from(new Set(assoc.assocOrgs));
      associationsById.set(e.id, assoc);
    }
    nextUrl = data?.pagination?.nextUrl || null;
  }
  return { entries, currentStatusById, associationsById };
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

async function fetchStatusFieldAndOptionsWrapper() {
  return await fetchStatusFieldAndOptions();
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

function extractOrgCandidates(row) {
  const candidates = [];
  const tryKeys = ["Organization", "Firm", "Company"]; // prioritized org-like columns
  for (const k of tryKeys) {
    const v = row[k];
    if (v && String(v).trim()) candidates.push(String(v).trim());
  }
  return Array.from(new Set(candidates));
}

function extractPersonCandidates(row) {
  const candidates = [];
  const tryKeys = ["Name", "Investor Name", "LP Name", "Contacts", "Contact"]; // person-like columns
  for (const k of tryKeys) {
    const v = row[k];
    if (!v) continue;
    const s = String(v);
    if (k.toLowerCase().includes('contact') || s.includes('∙') || s.includes(';')) {
      s.split(/\s*[;|∙]\s*/).forEach(part => { if (part && part.trim()) candidates.push(part.trim()); });
    } else {
      candidates.push(s.trim());
    }
  }
  // de-dup
  return Array.from(new Set(candidates));
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

    // Expect candidates separated into org vs person for type-safe matching
    const wantsRaw = records.map(r => ({
      orgCandidates: extractOrgCandidates(r),
      personCandidates: extractPersonCandidates(r),
      raw: r
    })).filter(r => r.orgCandidates.length || r.personCandidates.length);
    if (!wantsRaw.length) return res.status(400).json({ ok: false, error: "CSV has neither Organization nor Person names" });

    // Discover Status field + options from Affinity
    const { statusFieldId, labelToId, field: statusField, peopleFieldIds, orgFieldIds } = await fetchStatusFieldAndOptionsWrapper();

    // Load Affinity entries and current status, then build type-specific indexes with associations
    const { entries, currentStatusById, associationsById } = await fetchEntriesWithStatus(statusFieldId, peopleFieldIds, orgFieldIds);
    const orgKeyToEntry = new Map();
    const personKeyToEntry = new Map();
    const orgKeys = new Set();
    const personKeys = new Set();

    // Indices from associations (People/Org fields)
    const personAssocKeyToEntries = new Map(); // key -> Entry[]
    const orgAssocKeyToEntries = new Map();

    for (const e of entries) {
      const ent = e?.entity || {};
      const entType = classifyEntityType(ent);
      if (entType === 'person') {
        const first = ent.first_name ? String(ent.first_name) : "";
        const last = ent.last_name ? String(ent.last_name) : "";
        const display = (first || last) ? `${first} ${last}`.trim() : String(ent.name || '').trim();
        const variants = personKeyVariants(display);
        for (const v of variants) {
          if (!personKeyToEntry.has(v)) personKeyToEntry.set(v, e);
          personKeys.add(v);
        }
      } else {
        const nm = ent.name ? String(ent.name) : "";
        if (nm) {
          const key = normalizeOrgKey(nm);
          if (!orgKeyToEntry.has(key)) orgKeyToEntry.set(key, e);
          orgKeys.add(key);
        }
      }
      // Build assoc indices
      const assoc = associationsById.get(e.id) || { assocPeople: [], assocOrgs: [] };
      for (const pn of assoc.assocPeople || []) {
        const variants = personKeyVariants(pn);
        for (const v of variants) {
          if (!personAssocKeyToEntries.has(v)) personAssocKeyToEntries.set(v, []);
          const arr = personAssocKeyToEntries.get(v);
          if (!arr.find(x => x.id === e.id)) arr.push(e);
        }
      }
      for (const on of assoc.assocOrgs || []) {
        const ok = normalizeOrgKey(on);
        if (!orgAssocKeyToEntries.has(ok)) orgAssocKeyToEntries.set(ok, []);
        const arr2 = orgAssocKeyToEntries.get(ok);
        if (!arr2.find(x => x.id === e.id)) arr2.push(e);
      }
    }

    const knownOptions = Array.from(new Set(Array.from(labelToId.keys())));
    const minIdx = STATUS_INDEX.get(String(MIN_STATUS_LABEL).toLowerCase()) ?? -1;

    const results = [];
    for (const rec of wantsRaw) {
      // Try org+person pair matching when both provided
      let best = { entry: null, type: "", score: 0, name: "" };

      const personVariantSets = rec.personCandidates.map(n => ({ raw: n, variants: personKeyVariants(n) }));
      const normalizedOrgCandidates = rec.orgCandidates.map(normalizeOrgKey);

      // Pair path A: Start from org, validate associated people
      for (let i = 0; i < normalizedOrgCandidates.length; i++) {
        const orgKey = normalizedOrgCandidates[i];
        const orgEntry = orgKeyToEntry.get(orgKey);
        if (orgEntry) {
          const assoc = associationsById.get(orgEntry.id) || { assocPeople: [], assocOrgs: [] };
          // exact person variant hit boosts to 1.0
          for (const pv of personVariantSets) {
            const assocPeopleNorm = assoc.assocPeople.map(normalizePersonKey);
            const exact = pv.variants.find(v => assocPeopleNorm.includes(v));
            if (exact && 1.0 > best.score) { best = { entry: orgEntry, type: "organization", score: 1.0, name: rec.orgCandidates[i] }; }
            if (!exact && assocPeopleNorm.length) {
              const m = safeBestMatchWithRating(normalizePersonKey(pv.raw), assocPeopleNorm);
              if (m && m.rating > best.score && m.rating >= 0.9) {
                best = { entry: orgEntry, type: "organization", score: m.rating, name: rec.orgCandidates[i] };
              }
            }
          }
        } else {
          // fuzzy org then validate person
          const mOrg = safeBestMatchWithRating(orgKey, Array.from(orgKeys));
          if (mOrg && mOrg.rating >= 0.9) {
            const e = orgKeyToEntry.get(mOrg.target);
            const assoc = associationsById.get(e.id) || { assocPeople: [], assocOrgs: [] };
            for (const pv of personVariantSets) {
              const assocPeopleNorm = assoc.assocPeople.map(normalizePersonKey);
              const exact = pv.variants.find(v => assocPeopleNorm.includes(v));
              if (exact && mOrg.rating > best.score) { best = { entry: e, type: "organization", score: mOrg.rating, name: rec.orgCandidates[i] }; }
              if (!exact && assocPeopleNorm.length) {
                const m = safeBestMatchWithRating(normalizePersonKey(pv.raw), assocPeopleNorm);
                if (m && Math.min(m.rating, mOrg.rating) > best.score && m.rating >= 0.9) {
                  best = { entry: e, type: "organization", score: Math.min(m.rating, mOrg.rating), name: rec.orgCandidates[i] };
                }
              }
            }
          }
        }
      }

      // Pair path B: Start from person, validate associated org
      for (const pv of personVariantSets) {
        let foundDirect = false;
        for (const v of pv.variants) {
          const pEntry = personKeyToEntry.get(v);
          if (pEntry) {
            foundDirect = true;
            const assoc = associationsById.get(pEntry.id) || { assocPeople: [], assocOrgs: [] };
            const assocOrgsNorm = assoc.assocOrgs.map(normalizeOrgKey);
            for (let i = 0; i < normalizedOrgCandidates.length; i++) {
              const ok = normalizedOrgCandidates[i];
              if (assocOrgsNorm.includes(ok) && 1.0 > best.score) {
                best = { entry: pEntry, type: "person", score: 1.0, name: pv.raw };
              } else if (assocOrgsNorm.length) {
                const m = safeBestMatchWithRating(ok, assocOrgsNorm);
                if (m && m.rating > best.score && m.rating >= 0.9) {
                  best = { entry: pEntry, type: "person", score: m.rating, name: pv.raw };
                }
              }
            }
          }
        }
        if (!foundDirect) {
          const base = normalizePersonKey(pv.raw);
          const mP = safeBestMatchWithRating(base, Array.from(personKeys));
          if (mP && mP.rating >= 0.9) {
            const e = personKeyToEntry.get(mP.target);
            const assoc = associationsById.get(e.id) || { assocPeople: [], assocOrgs: [] };
            const assocOrgsNorm = assoc.assocOrgs.map(normalizeOrgKey);
            for (let i = 0; i < normalizedOrgCandidates.length; i++) {
              const ok = normalizedOrgCandidates[i];
              if (assocOrgsNorm.includes(ok) && mP.rating > best.score) {
                best = { entry: e, type: "person", score: mP.rating, name: pv.raw };
              } else if (assocOrgsNorm.length) {
                const m = safeBestMatchWithRating(ok, assocOrgsNorm);
                const score = Math.min(m?.rating || 0, mP.rating);
                if (m && score > best.score && m.rating >= 0.9) {
                  best = { entry: e, type: "person", score, name: pv.raw };
                }
              }
            }
          }
        }
      }

      // Fallback: use association indices when there is no direct entity match
      if (!best.entry) {
        // Person-only via associations → choose entry whose org matches CSV org if present
        for (const name of rec.personCandidates) {
          const variants = personKeyVariants(name);
          for (const v of variants) {
            const list = personAssocKeyToEntries.get(v) || [];
            if (list.length === 1 && 0.95 > best.score) {
              best = { entry: list[0], type: classifyEntityType(list[0]?.entity) === 'person' ? 'person' : 'organization', score: 0.95, name };
            } else if (list.length > 1 && normalizedOrgCandidates.length) {
              for (const e of list) {
                const assoc = associationsById.get(e.id) || { assocOrgs: [] };
                const assocOrgsNorm = (assoc.assocOrgs || []).map(normalizeOrgKey);
                if (normalizedOrgCandidates.some(ok => assocOrgsNorm.includes(ok)) && 0.93 > best.score) {
                  best = { entry: e, type: classifyEntityType(e?.entity) === 'person' ? 'person' : 'organization', score: 0.93, name };
                  break;
                }
              }
            }
          }
        }
        // Org-only via associations
        for (const name of rec.orgCandidates) {
          const ok = normalizeOrgKey(name);
          const list = orgAssocKeyToEntries.get(ok) || [];
          if (list.length === 1 && 0.92 > best.score) {
            best = { entry: list[0], type: 'organization', score: 0.92, name };
          }
        }
      }

      // If still no pair found, fall back to type-only matching
      if (!best.entry) {
        // Organizations
        for (const name of rec.orgCandidates) {
          const exactKey = normalizeOrgKey(name);
          const direct = orgKeyToEntry.get(exactKey);
          if (direct && 1.0 > best.score) best = { entry: direct, type: "organization", score: 1.0, name };
          if (!direct) {
            const m = safeBestMatchWithRating(exactKey, Array.from(orgKeys));
            if (m && m.rating > best.score && m.rating >= 0.88) {
              best = { entry: orgKeyToEntry.get(m.target), type: "organization", score: m.rating, name };
            }
          }
        }
        // People
        for (const name of rec.personCandidates) {
          const variants = personKeyVariants(name);
          let matched = false;
          for (const v of variants) {
            const direct = personKeyToEntry.get(v);
            if (direct && 1.0 > best.score) { best = { entry: direct, type: "person", score: 1.0, name }; matched = true; break; }
          }
          if (!matched) {
            const base = normalizePersonKey(name);
            const m = safeBestMatchWithRating(base, Array.from(personKeys));
            if (m && m.rating > best.score && m.rating >= 0.85) {
              best = { entry: personKeyToEntry.get(m.target), type: "person", score: m.rating, name };
            }
          }
        }
      }

      const entry = best.entry;
      const statusLabel = deriveStatusLabelFromRow(rec.raw);
      const displayName = rec.orgCandidates[0] || rec.personCandidates[0] || best.name || "";

      if (!entry) {
        results.push({ name: displayName, statusLabel, matched: false, reason: "No suitable org/person match" });
        continue;
      }

      // Hard lock: do not change if currently Passed (or synonyms). Use bulk map, then per-entry fallback.
      let currentLabel = String(currentStatusById.get(entry.id) || "");
      if (!currentLabel) currentLabel = await fetchEntryStatusLabel(entry.id, statusFieldId);
      if (isPassedLabel(currentLabel)) {
        results.push({ name: displayName, statusLabel, matched: true, entryId: entry.id, updated: false, reason: `Currently '${currentLabel}'; no change`, matchType: best.type, score: Number(best.score.toFixed(3)) });
        continue;
      }

      // Enforce minimum threshold and non-downgrade
      const currentIdx = STATUS_INDEX.get(currentLabel.toLowerCase());
      const derivedIdx = STATUS_INDEX.get(String(statusLabel || '').toLowerCase());

      if ((minIdx !== -1 && (derivedIdx ?? -1) < minIdx)) {
        results.push({ name: displayName, statusLabel, matched: true, entryId: entry.id, updated: false, reason: `Status before minimum threshold (${MIN_STATUS_LABEL})`, matchType: best.type, score: Number(best.score.toFixed(3)) });
        continue;
      }
      if ((currentIdx ?? -1) !== -1 && (derivedIdx ?? -1) !== -1 && derivedIdx < currentIdx) {
        results.push({ name: displayName, statusLabel, matched: true, entryId: entry.id, updated: false, reason: `Would downgrade from '${currentLabel}' to '${statusLabel}'`, matchType: best.type, score: Number(best.score.toFixed(3)) });
        continue;
      }
      if (currentLabel && currentLabel.toLowerCase() === String(statusLabel || '').toLowerCase()) {
        results.push({ name: displayName, statusLabel, matched: true, entryId: entry.id, updated: false, reason: 'Unchanged', matchType: best.type, score: Number(best.score.toFixed(3)) });
        continue;
      }

      const optionId = resolveStatusOptionId(statusLabel, labelToId);
      if (!statusLabel) {
        results.push({ name: displayName, matched: true, entryId: entry.id, updated: false, reason: "Could not derive status from CSV row", matchType: best.type, score: Number(best.score.toFixed(3)) });
        continue;
      }
      if (!optionId) {
        results.push({ name: displayName, statusLabel, matched: true, entryId: entry.id, updated: false, reason: `Unknown status '${statusLabel}' for field '${statusField.name}'`, knownOptions, matchType: best.type, score: Number(best.score.toFixed(3)) });
        continue;
      }

      if (isDryRun) {
        results.push({ name: displayName, statusLabel, matched: true, entryId: entry.id, updated: false, wouldUpdate: true, matchType: best.type, score: Number(best.score.toFixed(3)) });
        continue;
      }

      try {
        await updateStatus(entry.id, statusFieldId, optionId, statusField.valueType || statusField.value_type);
        results.push({ name: displayName, statusLabel, matched: true, entryId: entry.id, updated: true, matchType: best.type, score: Number(best.score.toFixed(3)) });
      } catch (e) {
        results.push({ name: displayName, statusLabel, matched: true, entryId: entry.id, updated: false, error: e?.response?.data || e.message, matchType: best.type, score: Number(best.score.toFixed(3)) });
      }
    }

    return res.status(200).json({ ok: true, total: wantsRaw.length, results: redact(results) });
  } catch (e) {
    const status = e?.response?.status || 500;
    const data = e?.response?.data || e.message;
    return res.status(500).json({ ok: false, error: { status, data } });
  }
}


