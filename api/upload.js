import axios from "axios";
import { parse } from "csv-parse/sync";
import stringSimilarity from "string-similarity";

const LIST_ID = 300305; // Affinity list id
const FIELD_STATUS = "field-REPLACE_STATUS_ID"; // Dropdown field id in Affinity for Status
const STATUS_LABEL_TO_OPTION_ID = {
  // Example mapping: set real option ids from /v2/lists/{id}/fields
  "Sub docs signed": "dropdown-option-REPLACE_OPTION_ID",
};

const V2 = axios.create({
  baseURL: "https://api.affinity.co/v2",
  headers: {
    Authorization: `Bearer ${process.env.AFFINITY_V2_TOKEN}`,
    "Content-Type": "application/json",
  },
  timeout: 60000,
});

function normalizeName(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[,\.]/g, " ")
    .replace(/\s+lp\b|\s+llc\b|\s+ltd\b|\s+inc\b|\s+co\b|\s+corp\b|\s+partners?\b|\s+capital\b|\s+ventures?\b|\s+management\b|\s+holdings?\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchAllEntries() {
  const entries = [];
  let nextUrl = `/lists/${LIST_ID}/list-entries`;
  while (nextUrl) {
    const { data } = await V2.get(nextUrl);
    entries.push(...(data?.data || []));
    nextUrl = data?.pagination?.nextUrl || null;
  }
  return entries;
}

async function updateStatus(entryId, optionId) {
  await V2.post(`/lists/${LIST_ID}/list-entries/${entryId}/fields/${FIELD_STATUS}`,
    { value: { type: "dropdown", data: optionId } }
  );
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).json({ ok: false, error: "Use POST" });
    if (!process.env.AFFINITY_V2_TOKEN) return res.status(500).json({ ok: false, error: "Missing AFFINITY_V2_TOKEN" });

    // Read body (support multipart or raw text); simplest path: expect text/csv in body
    const contentType = String(req.headers["content-type"] || "").toLowerCase();
    let csvText = "";
    if (contentType.includes("text/csv") || contentType.includes("application/octet-stream") || contentType.includes("text/plain")) {
      csvText = typeof req.body === "string" ? req.body : (Buffer.isBuffer(req.body) ? req.body.toString("utf8") : "");
      if (!csvText) {
        // raw stream fallback
        csvText = await new Promise((resolve, reject) => {
          let data = "";
          req.on("data", chunk => data += chunk);
          req.on("end", () => resolve(data));
          req.on("error", reject);
        });
      }
    } else {
      // Support JSON uploads: { csv: "..." }
      const raw = typeof req.body === "string" ? req.body : (Buffer.isBuffer(req.body) ? req.body.toString("utf8") : "");
      const parsed = raw ? JSON.parse(raw) : (req.body || {});
      csvText = parsed.csv || "";
    }

    if (!csvText) return res.status(400).json({ ok: false, error: "No CSV provided" });

    // Parse CSV
    const records = parse(csvText, {
      columns: true,
      skip_empty_lines: true
    });

    // Expect columns: Name, Status (adjust as needed)
    const wants = records.map(r => ({
      name: String(r.Name || r["Investor Name"] || r["LP Name"] || "").trim(),
      status: String(r.Status || r["Sub Docs Status"] || r["Stage"] || "").trim()
    })).filter(r => r.name && r.status);

    if (!wants.length) return res.status(400).json({ ok: false, error: "CSV has no Name/Status rows" });

    // Load Affinity entries and build a normalized name index
    const entries = await fetchAllEntries();
    const nameToEntry = new Map();
    for (const e of entries) {
      const nm = normalizeName(e?.entity?.name || e?.entity?.first_name && `${e.entity.first_name} ${e.entity.last_name}` || "");
      if (nm) nameToEntry.set(nm, e);
    }

    const results = [];
    for (const row of wants) {
      const targetNorm = normalizeName(row.name);
      let entry = nameToEntry.get(targetNorm);

      // Fallback fuzzy
      if (!entry) {
        const candidates = Array.from(nameToEntry.keys());
        const { bestMatch } = stringSimilarity.findBestMatch(targetNorm, candidates);
        if (bestMatch && bestMatch.rating >= 0.92) {
          entry = nameToEntry.get(bestMatch.target);
        }
      }

      if (!entry) {
        results.push({ name: row.name, status: row.status, matched: false });
        continue;
      }

      const optionId = STATUS_LABEL_TO_OPTION_ID[row.status] || null;
      if (!optionId) {
        results.push({ name: row.name, status: row.status, matched: true, entryId: entry.id, updated: false, reason: "Unknown status label; map it in STATUS_LABEL_TO_OPTION_ID" });
        continue;
      }

      try {
        await updateStatus(entry.id, optionId);
        results.push({ name: row.name, status: row.status, matched: true, entryId: entry.id, updated: true });
      } catch (e) {
        results.push({ name: row.name, status: row.status, matched: true, entryId: entry.id, updated: false, error: e?.response?.data || e.message });
      }
    }

    return res.status(200).json({ ok: true, total: wants.length, results });
  } catch (e) {
    const status = e?.response?.status || 500;
    const data = e?.response?.data || e.message;
    return res.status(500).json({ ok: false, error: { status, data } });
  }
}


