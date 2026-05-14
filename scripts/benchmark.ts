/**
 * Benchmark script comparing local-first databases: ZerithDB, PouchDB, and RxDB
 * Uses fake-indexeddb as a polyfill for Node.js compatibility
 */

import "fake-indexeddb/auto";
import PouchDB from "pouchdb";
import { createApp, type ZerithDBApp } from "zerithdb-sdk";
import { performance } from "node:perf_hooks";

interface TestDocument {
  id: string;
  name: string;
  score: number;
}

/**
 * Common adapter interfaces for benchmarking
 */
interface InsertAdapter {
  name: string;
  setup(): Promise<void>;
  insertAll(docs: TestDocument[]): Promise<void>;
  teardown(): Promise<void>;
}

interface SyncAdapter {
  name: string;
  setup(): Promise<void>;
  syncAll(docs: TestDocument[]): Promise<void>;
  teardown(): Promise<void>;
}

/**
 * Generate a shared dataset of 1000 documents
 */
function generateDataset(count: number): TestDocument[] {
  const docs: TestDocument[] = [];
  for (let i = 0; i < count; i++) {
    docs.push({
      id: `doc_${i}`,
      name: `Document ${i}`,
      score: Math.floor(Math.random() * 100),
    });
  }
  return docs;
}

/**
 * ZerithDB adapter implementation
 */
class ZerithDBInsertAdapter implements InsertAdapter {
  name = "ZerithDB";
  private app: ZerithDBApp | null = null;
  private collection: ReturnType<ZerithDBApp["db"]> | null = null;

  async setup(): Promise<void> {
    this.app = createApp({ appId: "benchmark-test" });
    this.collection = this.app.db<TestDocument>("test");
  }

  async insertAll(docs: TestDocument[]): Promise<void> {
    // Use bulk insert for better performance
    await this.collection!.insertMany(docs);
  }

  async teardown(): Promise<void> {
    if (this.app) {
      await this.app.dispose();
      this.app = null;
      this.collection = null;
    }
  }
}

/**
 * PouchDB adapter implementation
 */
class PouchDBAdapter implements InsertAdapter {
  name = "PouchDB";
  private db: PouchDB.Database | null = null;

  async setup(): Promise<void> {
    this.db = new PouchDB("benchmark-test");
  }

  async insertAll(docs: TestDocument[]): Promise<void> {
    // PouchDB bulk_docs for batch insertion
    const pouchDocs = docs.map((doc) => ({
      _id: doc.id,
      name: doc.name,
      score: doc.score,
    }));
    await this.db!.bulkDocs(pouchDocs);
  }

  async teardown(): Promise<void> {
    if (this.db) {
      await this.db.destroy();
      this.db = null;
    }
  }
}

/**
 * RxDB adapter implementation
 * Note: RxDB v17+ requires additional setup for storage in Node.js
 * This adapter uses a simple fallback for demonstration
 */
class RxDBAdapter implements InsertAdapter {
  name = "RxDB";
  private db: any = null;
  private skipped = false;

  async setup(): Promise<void> {
    try {
      // Import RxDB properly
      const { createRxDatabase } = await import("rxdb");
      const { getRxStorageMemory } = await import("rxdb/plugins/storage-memory");

      this.db = await createRxDatabase({
        name: "benchmark-test",
        storage: getRxStorageMemory(),
      });

      // Create schema
      const schema = {
        version: 0,
        primaryKey: "id",
        type: "object",
        properties: {
          id: { type: "string", maxLength: 100 },
          name: { type: "string" },
          score: { type: "number" },
        },
        required: ["id", "name", "score"],
      };

      await this.db.addCollections({
        test: { schema },
      });
    } catch (err) {
      // If setup fails, mark as skipped
      this.skipped = true;
      console.log(`  Note: ${this.name} skipped - requires additional setup (${err instanceof Error ? err.message : 'unknown error'})`);
    }
  }

  async insertAll(docs: TestDocument[]): Promise<void> {
    if (this.skipped || !this.db) return;
    const collection = this.db.collections.test;
    await collection.bulkInsert(docs);
  }

  async teardown(): Promise<void> {
    if (this.db) {
      try {
        await this.db.destroy();
      } catch {
        // Ignore cleanup errors
      }
      this.db = null;
    }
  }
}

/**
 * Benchmark result type
 */
interface BenchmarkResult {
  dbName: string;
  recordCount: number;
  totalTimeMs: number;
  opsPerSec: number;
}

interface SyncBenchmarkResult {
  dbName: string;
  recordCount: number;
  totalTimeMs: number;
  opsPerSec: number;
}

/**
 * Run benchmark for a single adapter
 * Returns null if the adapter was skipped
 */
async function runInsertBenchmark(
  adapter: InsertAdapter,
  docs: TestDocument[]
): Promise<BenchmarkResult | null> {
  try {
    await adapter.setup();

    // Check if adapter was skipped during setup (e.g., RxDB)
    if ((adapter as any).skipped) {
      await adapter.teardown();
      return null;
    }

    // Clear any existing data
    try {
      // Reset for fresh start
      await adapter.teardown();
      await adapter.setup();
    } catch {
      // Ignore cleanup errors
    }

    const startTime = performance.now();
    await adapter.insertAll(docs);
    const endTime = performance.now();

    const totalTimeMs = endTime - startTime;
    // Handle case where insert was skipped (0ms = no actual work)
    const opsPerSec = totalTimeMs > 0 ? (docs.length / totalTimeMs) * 1000 : 0;

    await adapter.teardown();

    return {
      dbName: adapter.name,
      recordCount: docs.length,
      totalTimeMs: Math.round(totalTimeMs * 100) / 100,
      opsPerSec: Math.round(opsPerSec),
    };
  } catch (err) {
    // If benchmark fails, log and return null
      console.log(
        `  Warning: ${adapter.name} insert benchmark failed - ${err instanceof Error ? err.message : "unknown error"}`
      );
    try {
      await adapter.teardown();
    } catch {
      // Ignore cleanup errors
    }
    return null;
  }
}

async function runSyncBenchmark(
  adapter: SyncAdapter,
  docs: TestDocument[]
): Promise<SyncBenchmarkResult | null> {
  try {
    await adapter.setup();

    if ((adapter as any).skipped) {
      await adapter.teardown();
      return null;
    }

    const startTime = performance.now();
    await adapter.syncAll(docs);
    const endTime = performance.now();

    const totalTimeMs = endTime - startTime;
    const opsPerSec = totalTimeMs > 0 ? (docs.length / totalTimeMs) * 1000 : 0;

    await adapter.teardown();

    return {
      dbName: adapter.name,
      recordCount: docs.length,
      totalTimeMs: Math.round(totalTimeMs * 100) / 100,
      opsPerSec: Math.round(opsPerSec),
    };
  } catch (err) {
    console.log(
      `  Warning: ${adapter.name} sync benchmark failed - ${err instanceof Error ? err.message : "unknown error"}`
    );
    try {
      await adapter.teardown();
    } catch {
      // Ignore cleanup errors
    }
    return null;
  }
}

/**
 * Format number with thousand separators
 */
function formatNumber(num: number): string {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/**
 * Print results in formatted ASCII table
 */
function printTable(results: BenchmarkResult[], batchSizes: number[]): void {
  // Calculate column widths
  const dbNameWidth = Math.max(10, ...results.map((r) => r.dbName.length));
  const countWidth = 12;
  const timeWidth = 15;
  const opsWidth = 12;

  // Print header
  console.log("\n" + "=".repeat(dbNameWidth + countWidth + timeWidth + opsWidth + 6));
  console.log("| " + "DB".padEnd(dbNameWidth) + " | " + "Records".padEnd(countWidth) + " | " + "Time (ms)".padEnd(timeWidth) + " | " + "Ops/sec".padEnd(opsWidth) + " |");
  console.log("=".repeat(dbNameWidth + countWidth + timeWidth + opsWidth + 6));

  // Group by batch size
  for (const size of batchSizes) {
    const sizeResults = results.filter((r) => r.recordCount === size);
    for (const result of sizeResults) {
      console.log(
        "| " +
        result.dbName.padEnd(dbNameWidth) +
        " | " +
        formatNumber(result.recordCount).padEnd(countWidth) +
        " | " +
        result.totalTimeMs.toString().padEnd(timeWidth) +
        " | " +
        formatNumber(result.opsPerSec).padEnd(opsWidth) +
        " |"
      );
    }
    if (size !== batchSizes[batchSizes.length - 1]) {
      console.log("-".repeat(dbNameWidth + countWidth + timeWidth + opsWidth + 6));
    }
  }

  console.log("=".repeat(dbNameWidth + countWidth + timeWidth + opsWidth + 6));
}

/**
 * Main benchmark execution
 */
class ZerithDBSyncAdapter implements SyncAdapter {
  name = "ZerithDB";
  private appA: ZerithDBApp | null = null;
  private appB: ZerithDBApp | null = null;
  private unsubscribers: Array<() => void> = [];

  async setup(): Promise<void> {
    this.appA = createApp({ appId: `benchmark-sync-a-${Date.now()}` });
    this.appB = createApp({ appId: `benchmark-sync-b-${Date.now()}` });

    this.appA.sync.enable();
    this.appB.sync.enable();

    const handleA = (evt: { collectionName: string; update: Uint8Array }) => {
      this.appB?.sync.applyRemoteUpdate(evt.collectionName, evt.update, "peer-a");
    };
    const handleB = (evt: { collectionName: string; update: Uint8Array }) => {
      this.appA?.sync.applyRemoteUpdate(evt.collectionName, evt.update, "peer-b");
    };

    this.appA.sync.on("update:local", handleA);
    this.appB.sync.on("update:local", handleB);

    this.unsubscribers.push(() => this.appA?.sync.off("update:local", handleA));
    this.unsubscribers.push(() => this.appB?.sync.off("update:local", handleB));
  }

  async syncAll(docs: TestDocument[]): Promise<void> {
    const docA = this.appA?.sync.getDoc("test");
    const docB = this.appB?.sync.getDoc("test");
    if (!docA || !docB) return;

    const mapA = docA.getMap<TestDocument>("records");
    const mapB = docB.getMap<TestDocument>("records");

    for (const doc of docs) {
      mapA.set(doc.id, doc);
    }

    await waitFor(() => mapB.size >= docs.length, 10_000);
  }

  async teardown(): Promise<void> {
    for (const unsub of this.unsubscribers) {
      unsub();
    }
    this.unsubscribers = [];
    if (this.appA) {
      await this.appA.dispose();
      this.appA = null;
    }
    if (this.appB) {
      await this.appB.dispose();
      this.appB = null;
    }
  }
}

class PouchDBSyncAdapter implements SyncAdapter {
  name = "PouchDB";
  private dbA: PouchDB.Database | null = null;
  private dbB: PouchDB.Database | null = null;

  async setup(): Promise<void> {
    const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`;
    this.dbA = new PouchDB(`benchmark-sync-a-${suffix}`);
    this.dbB = new PouchDB(`benchmark-sync-b-${suffix}`);
  }

  async syncAll(docs: TestDocument[]): Promise<void> {
    const pouchDocs = docs.map((doc) => ({
      _id: doc.id,
      name: doc.name,
      score: doc.score,
    }));

    await this.dbA!.bulkDocs(pouchDocs);
    await this.dbA!.replicate.to(this.dbB!);
  }

  async teardown(): Promise<void> {
    if (this.dbA) {
      await this.dbA.destroy();
      this.dbA = null;
    }
    if (this.dbB) {
      await this.dbB.destroy();
      this.dbB = null;
    }
  }
}

class RxDBSyncAdapter implements SyncAdapter {
  name = "RxDB";
  private dbA: any = null;
  private dbB: any = null;
  private skipped = false;

  async setup(): Promise<void> {
    try {
      const { createRxDatabase } = await import("rxdb");
      const { getRxStorageMemory } = await import("rxdb/plugins/storage-memory");

      this.dbA = await createRxDatabase({
        name: `benchmark-sync-a-${Date.now()}`,
        storage: getRxStorageMemory(),
      });
      this.dbB = await createRxDatabase({
        name: `benchmark-sync-b-${Date.now()}`,
        storage: getRxStorageMemory(),
      });

      const schema = {
        version: 0,
        primaryKey: "id",
        type: "object",
        properties: {
          id: { type: "string", maxLength: 100 },
          name: { type: "string" },
          score: { type: "number" },
        },
        required: ["id", "name", "score"],
      };

      await this.dbA.addCollections({ test: { schema } });
      await this.dbB.addCollections({ test: { schema } });
    } catch (err) {
      this.skipped = true;
      console.log(
        `  Note: ${this.name} skipped - requires additional setup (${err instanceof Error ? err.message : "unknown error"})`
      );
    }
  }

  async syncAll(docs: TestDocument[]): Promise<void> {
    if (this.skipped || !this.dbA || !this.dbB) return;
    const source = this.dbA.collections.test;
    const target = this.dbB.collections.test;

    await source.bulkInsert(docs);

    if (typeof source.exportJSON === "function" && typeof target.importJSON === "function") {
      const exported = await source.exportJSON();
      await target.importJSON(exported);
      return;
    }

    const allDocs = await source.find().exec();
    await target.bulkInsert(allDocs.map((doc: any) => doc.toJSON()));
  }

  async teardown(): Promise<void> {
    if (this.dbA) {
      try {
        await this.dbA.destroy();
      } catch {
        // Ignore cleanup errors
      }
      this.dbA = null;
    }
    if (this.dbB) {
      try {
        await this.dbB.destroy();
      } catch {
        // Ignore cleanup errors
      }
      this.dbB = null;
    }
  }
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const start = performance.now();
  while (performance.now() - start < timeoutMs) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("Timed out waiting for sync");
}

async function main() {
  console.log("Local-First Database Benchmark");
  console.log("================================");
  console.log("\nGenerating shared dataset of 1000 documents...");

  // Generate shared dataset
  const fullDataset = generateDataset(1000);

  // Define batch sizes to test
  const batchSizes = [100, 500, 1000];

  // Create adapters
  const insertAdapters: InsertAdapter[] = [
    new ZerithDBInsertAdapter(),
    new PouchDBAdapter(),
    new RxDBAdapter(),
  ];

  const syncAdapters: SyncAdapter[] = [
    new ZerithDBSyncAdapter(),
    new PouchDBSyncAdapter(),
    new RxDBSyncAdapter(),
  ];

  // Run insert benchmarks
  const insertResults: BenchmarkResult[] = [];
  console.log("\nInsert Benchmark");
  console.log("----------------");

  for (const adapter of insertAdapters) {
    console.log(`\nBenchmarking ${adapter.name} inserts...`);

    for (const batchSize of batchSizes) {
      const docs = fullDataset.slice(0, batchSize);

      console.log(`  Inserting ${batchSize} records...`);
      const result = await runInsertBenchmark(adapter, docs);

      if (result !== null) {
        insertResults.push(result);
        console.log(
          `    -> ${result.totalTimeMs}ms (${result.opsPerSec.toLocaleString()} ops/sec)`
        );
      }
    }
  }

  if (insertResults.length > 0) {
    printTable(insertResults, batchSizes);
  } else {
    console.log("\nNo insert results to display.");
  }

  // Run sync benchmarks
  const syncResults: SyncBenchmarkResult[] = [];
  console.log("\nSync Benchmark");
  console.log("--------------");

  for (const adapter of syncAdapters) {
    console.log(`\nBenchmarking ${adapter.name} sync...`);

    for (const batchSize of batchSizes) {
      const docs = fullDataset.slice(0, batchSize);

      console.log(`  Syncing ${batchSize} records...`);
      const result = await runSyncBenchmark(adapter, docs);

      if (result !== null) {
        syncResults.push(result);
        console.log(
          `    -> ${result.totalTimeMs}ms (${result.opsPerSec.toLocaleString()} ops/sec)`
        );
      }
    }
  }

  if (syncResults.length > 0) {
    printTable(syncResults, batchSizes);
  } else {
    console.log("\nNo sync results to display.");
  }

  console.log("\nBenchmark complete!");
}

main().catch(console.error);