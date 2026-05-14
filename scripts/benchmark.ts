/**
 * Benchmark script comparing local-first databases: ZerithDB, PouchDB, and RxDB
 * Uses fake-indexeddb as a polyfill for Node.js compatibility
 */

import "fake-indexeddb/auto";
import PouchDB from "pouchdb";
import RxDB from "rxdb";
import { createApp, type ZerithDBApp } from "zerithdb-sdk";
import { performance } from "node:perf_hooks";

interface TestDocument {
  id: string;
  name: string;
  score: number;
}

/**
 * Common DBAdapter interface for benchmarking
 */
interface DBAdapter {
  name: string;
  setup(): Promise<void>;
  insertAll(docs: TestDocument[]): Promise<void>;
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
class ZerithDBAdapter implements DBAdapter {
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
class PouchDBAdapter implements DBAdapter {
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
class RxDBAdapter implements DBAdapter {
  name = "RxDB";
  private db: any = null;
  private skipped = false;

  async setup(): Promise<void> {
    try {
      // Import RxDB properly
      const { createRxDatabase, addRxPlugin } = await import("rxdb");

      // Try to use memory storage - this may need @rxdb/memory plugin in v17+
      // For now, we'll try using IndexedDB which works with fake-indexeddb
      this.db = await createRxDatabase({
        name: "benchmark-test",
        storage: "idb", // Use IndexedDB which works with fake-indexeddb
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

/**
 * Run benchmark for a single adapter
 * Returns null if the adapter was skipped
 */
async function runBenchmark(
  adapter: DBAdapter,
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
    console.log(`  Warning: ${adapter.name} benchmark failed - ${err instanceof Error ? err.message : 'unknown error'}`);
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
async function main() {
  console.log("Local-First Database Benchmark");
  console.log("================================");
  console.log("\nGenerating shared dataset of 1000 documents...");

  // Generate shared dataset
  const fullDataset = generateDataset(1000);

  // Define batch sizes to test
  const batchSizes = [100, 500, 1000];

  // Create adapters
  const adapters: DBAdapter[] = [
    new ZerithDBAdapter(),
    new PouchDBAdapter(),
    new RxDBAdapter(),
  ];

  // Run benchmarks
  const allResults: BenchmarkResult[] = [];

  for (const adapter of adapters) {
    console.log(`\nBenchmarking ${adapter.name}...`);

    for (const batchSize of batchSizes) {
      // Slice the appropriate number of documents
      const docs = fullDataset.slice(0, batchSize);

      console.log(`  Inserting ${batchSize} records...`);
      const result = await runBenchmark(adapter, docs);

      // Only add valid results (skip null results from failed/skipped adapters)
      if (result !== null) {
        allResults.push(result);
        console.log(
          `    -> ${result.totalTimeMs}ms (${result.opsPerSec.toLocaleString()} ops/sec)`
        );
      }
    }
  }

  // Print results table only if we have results
  if (allResults.length > 0) {
    printTable(allResults, batchSizes);
  } else {
    console.log("\nNo benchmark results to display.");
  }

  console.log("\nBenchmark complete!");
}

main().catch(console.error);