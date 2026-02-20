import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Prevent duplicate processing when WebSocket reconnects or Feishu redelivers messages.
const DEDUP_TTL_MS = 30 * 60 * 1000; // 30 minutes
const DEDUP_MAX_SIZE = 1_000;
const DEDUP_CLEANUP_INTERVAL_MS = 5 * 60 * 1000; // cleanup every 5 minutes
const DEDUP_STATE_FILE = join(__dirname, "../../state/dedup.json");

// In-memory cache: messageId -> timestamp
let processedMessageIds = new Map<string, number>();
let lastCleanupTime = Date.now();
let dirty = false;

// Ensure state directory exists
function ensureStateDir() {
  const stateDir = dirname(DEDUP_STATE_FILE);
  if (!existsSync(stateDir)) {
    mkdirSync(stateDir, { recursive: true });
  }
}

// Load persisted state from disk
function loadState() {
  try {
    ensureStateDir();
    if (existsSync(DEDUP_STATE_FILE)) {
      const data = JSON.parse(readFileSync(DEDUP_STATE_FILE, "utf-8"));
      if (data.messages && typeof data.messages === "object") {
        processedMessageIds = new Map(Object.entries(data.messages));
        console.log(`[dedup] Loaded ${processedMessageIds.size} message IDs from disk`);
      }
    }
  } catch (err) {
    console.error("[dedup] Failed to load state:", err);
  }
}

// Persist state to disk (debounced)
function saveState() {
  if (!dirty) return;
  try {
    ensureStateDir();
    const obj = Object.fromEntries(processedMessageIds);
    writeFileSync(DEDUP_STATE_FILE, JSON.stringify({ messages: obj, savedAt: Date.now() }));
    dirty = false;
  } catch (err) {
    console.error("[dedup] Failed to save state:", err);
  }
}

// Initialize: load state on module load
loadState();

// Auto-save periodically
setInterval(saveState, 10_000);

export function tryRecordMessage(messageId: string): boolean {
  const now = Date.now();

  // Throttled cleanup: evict expired entries at most once per interval.
  if (now - lastCleanupTime > DEDUP_CLEANUP_INTERVAL_MS) {
    for (const [id, ts] of processedMessageIds) {
      if (now - ts > DEDUP_TTL_MS) {
        processedMessageIds.delete(id);
        dirty = true;
      }
    }
    lastCleanupTime = now;
  }

  if (processedMessageIds.has(messageId)) {
    return false;
  }

  // Evict oldest entries if cache is full.
  if (processedMessageIds.size >= DEDUP_MAX_SIZE) {
    const first = processedMessageIds.keys().next().value!;
    processedMessageIds.delete(first);
  }

  processedMessageIds.set(messageId, now);
  dirty = true;
  return true;
}
