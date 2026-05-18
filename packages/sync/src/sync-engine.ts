import * as Y from "yjs";
import * as awarenessProtocol from "y-protocols/awareness";
import { IndexeddbPersistence } from "y-indexeddb";
import type { ZerithDBConfig, SyncState, SyncPlugin, IncomingPeerDataMessage } from "zerithdb-core";
import { EventEmitter } from "zerithdb-core";
import type { DbClient } from "zerithdb-db";
import type { NetworkManager } from "zerithdb-network";
import type { SyncProtocol } from "zerithdb-core";
import { InboxQueue } from "./queue/InboxQueue.js";
import { OutboxQueue } from "./queue/OutboxQueue.js";
import { EphemeralStateManager } from "./ephemeral-state.js";
import { bytesToBase64, base64ToBytes } from "zerithdb-utils";
import { DefaultSyncProtocol } from "./protocol.js";

type SyncEvents = {
  "state:change": SyncState;
  "update:local": { collectionName: string; update: Uint8Array };
  "update:remote": { collectionName: string; update: Uint8Array; fromPeer: string };
  "conflict:flagged": {
    collectionName: string;
    fromPeer: string;
    localSnapshot: Uint8Array;
    incomingUpdate: Uint8Array;
    suggestion?: string;
  };
};

/**
 * Deterministic sync engine using Vector Clocks and Lamport timestamps.
 * Replaces Yjs with an explicit state-based replication protocol.
 * Integrates Inbox/Outbox queues to handle offline-first mutation logging.
 */
export class SyncEngine extends EventEmitter<SyncEvents> {
  private readonly docs = new Map<string, Y.Doc>();
  private readonly persistences = new Map<string, IndexeddbPersistence>();
  private readonly awarenesses = new Map<string, awarenessProtocol.Awareness>();
  readonly outbox: OutboxQueue<Uint8Array>;
  readonly inbox: InboxQueue<Uint8Array>;
  public readonly ephemeral: EphemeralStateManager;
  private _enabled = false;
  
  private _state: SyncState = { synced: false, pendingUpdates: 0, connectedPeers: 0 };
  private plugins = new Map<string, SyncPlugin>();
  private activePluginVersion = 1;
  private pendingUpdates = new Map<string, Uint8Array[]>();

  private syncTimer: any = null;
  private syncTimerIsRaf: boolean = false;
  private protocol: SyncProtocol = new DefaultSyncProtocol();

  constructor(
    private readonly config: ZerithDBConfig,
    private readonly db: DbClient,
    private readonly network: NetworkManager,
    private readonly auth: AuthManager
  ) {
    super();

    this.outbox = new OutboxQueue(config.appId);
    this.inbox = new InboxQueue(config.appId);
    this.ephemeral = new EphemeralStateManager(config, network);

    this.onPeerUpdate = this.onPeerUpdate.bind(this);
    this.onLocalMutation = this.onLocalMutation.bind(this);
    this.onPeerConnected = this.onPeerConnected.bind(this);
    this.onPeerDisconnected = this.onPeerDisconnected.bind(this);

    this.outbox.onChange(() => {
      void this.refreshPendingCount();
    });

    if (typeof document !== "undefined") {
      document.addEventListener("visibilitychange", this.handleVisibilityChange);
    }

    // [UCAN] Get the local app owner's DID
    const identity = this.auth.identity;
    if (!identity) {
      throw new ZerithDBError(
        ErrorCode.AUTH_KEY_NOT_FOUND,
        "SyncEngine requires a signed‑in identity. Call auth.signIn() before enabling sync."
      );
    }
    this.appOwnerDid = identity.did;
  }

  private handleVisibilityChange = (): void => {
    if (document.visibilityState === "visible") {
      if (this.pendingUpdates.size > 0 && !this.syncTimer) {
        this.flushUpdates();
      }
    } else if (document.visibilityState === "hidden") {
      if (this.syncTimer) {
        if (this.syncTimerIsRaf && typeof window !== "undefined" && window.cancelAnimationFrame) {
          window.cancelAnimationFrame(this.syncTimer);
        } else {
          clearTimeout(this.syncTimer);
        }

        this.syncTimer = null;
        this.syncTimerIsRaf = false;
      }
    }
  };

  enable(): void {
    if (this._enabled) return;

    this._enabled = true;

    this.network.on("message", this.onPeerUpdate);
    this.network.on("peer:connected", this.onPeerConnected);
    this.network.on("peer:disconnected", this.onPeerDisconnected);
    this.updateState({ synced: true, connectedPeers: this.network.connectedPeerCount });
    void this.flushOutbox();

    // Start background anti-entropy sync (every 100ms) to guarantee strong eventual consistency
    this.antiEntropyTimer = setInterval(() => {
      this.triggerAntiEntropy();
    }, 100);
  }

  disable(): void {
    this._enabled = false;

    this.network.off("message", this.onPeerUpdate);
    this.network.off("peer:connected", this.onPeerConnected);
  this.network.off("peer:disconnected", this.onPeerDisconnected);
    this.ephemeral.disable();
    this.updateState({ synced: false, connectedPeers: 0 });

    if (this.antiEntropyTimer) {
      clearInterval(this.antiEntropyTimer);
      this.antiEntropyTimer = null;
    }
  }

  private triggerAntiEntropy(): void {
    if (!this._enabled || this.network.connectedPeerCount === 0) return;
    for (const [collectionName, doc] of this.docs.entries()) {
      const stateVector = Y.encodeStateVector(doc);
      this.network.broadcast({
        type: "sync-request",
        payload: this.encodeMessage(collectionName, stateVector),
      });
    }
  }

  registerPlugin(plugin: SyncPlugin): void {
    this.plugins.set(plugin.id, plugin);

    if (plugin.version > this.activePluginVersion) {
      this.activePluginVersion = plugin.version;
    }
  }

  async loadPlugin(pluginUrl: string): Promise<void> {
    try {
      const module = await import(pluginUrl);
      const plugin = module.default as SyncPlugin;
      this.registerPlugin(plugin);
    } catch (err) {
      console.error(`Failed to load plugin from ${pluginUrl}`, err);
    }
  }

  proposeUpgrade(pluginUrl: string, version: number): void {
    this.network.broadcast({
      type: "sync-upgrade-offer",
      payload: JSON.stringify({ pluginUrl, version }),
    });
  }

  get state(): Readonly<SyncState> {
    return this._state;
  }

  getDoc(collectionName: string): Y.Doc {
    if (this.docs.has(collectionName)) {
      return this.docs.get(collectionName)!;
    }

    const doc = new Y.Doc({ guid: `${this.config.appId}:${collectionName}` });
    const persistence = new IndexeddbPersistence(
      `zerithdb_sync_${this.config.appId}_${collectionName}`,
      doc
    );

    this.persistences.set(collectionName, persistence);
// Broadcast local updates to peers (batched via requestAnimationFrame)
doc.on("update", (update: Uint8Array, origin: unknown) => {
  if (origin === "remote") return; // Don't echo back remote updates

    doc.on("update", (update: Uint8Array, origin: unknown) => {
      if (origin === "remote") return;
      this.queueUpdate(collectionName, update);
    });

    this.docs.set(collectionName, doc);

    // Request initial synchronization from any already connected peers
    if (this._enabled && this.network.connectedPeerCount > 0) {
      const stateVector = Y.encodeStateVector(doc);
      this.network.broadcast({
        type: "sync-request",
        payload: this.encodeMessage(collectionName, stateVector),
      });
    }

    return doc;
  }

  async applyRemoteUpdate(
    collectionName: string,
    update: Uint8Array,
    fromPeer: string
  ): Promise<void> {
    // [UCAN] Check permission before processing any remote update
    if (!(await this.checkRemotePermission(fromPeer, collectionName, "write"))) {
      console.warn(`Permission denied: peer ${fromPeer} cannot write to ${collectionName}`);
      return;
    }

    let finalUpdate: Uint8Array | null = update;

    for (const plugin of this.plugins.values()) {
      if (plugin.onBeforeApplyUpdate) {
        finalUpdate = await plugin.onBeforeApplyUpdate(collectionName, finalUpdate, fromPeer);
        if (!finalUpdate) return;
      }
    }

    const doc = this.getDoc(collectionName);
    const dataMap = doc.getMap(collectionName);
    const changedKeys = new Set<string>();
    let observing = false;

    const observer = (event: Y.YMapEvent<any>) => {
      for (const [key] of event.changes.keys) {
        changedKeys.add(key);
      }
    };

    if (this.validatorRegistry?.has(collectionName)) {
      observing = true;
      dataMap.observe(observer);
    }

    try {
      await this.handleRemoteUpdate(collectionName, finalUpdate, fromPeer);
    } finally {
      if (observing) {
        dataMap.unobserve(observer);
      }
    }

    if (observing && changedKeys.size > 0) {
      for (const key of changedKeys) {
        const value = dataMap.get(key);
        if (value === undefined) continue; // deleted key

        const result = this.validatorRegistry!.validateRemote(collectionName, value);

        if (!result.valid) {
          this.emit("validation:error", {
            collectionName,
            fromPeer,
            issues: result.issues,
          });
        }
      }
    }
  }

  /**
   * Apply a remote awareness update.
   */
  applyRemoteAwarenessUpdate(collectionName: string, update: Uint8Array): void {
    const awareness = this.getAwareness(collectionName);
    awarenessProtocol.applyAwarenessUpdate(awareness, update, "remote");
  }

  async dispose(): Promise<void> {
    if (typeof document !== "undefined") {
      document.removeEventListener("visibilitychange", this.handleVisibilityChange);
    }

    this.disable();
    if (this.syncTimer) {
      if (this.syncTimerIsRaf && typeof window !== "undefined" && window.cancelAnimationFrame) {
        window.cancelAnimationFrame(this.syncTimer);
      } else {
        clearTimeout(this.syncTimer);
      }

      this.syncTimer = null;
      this.syncTimerIsRaf = false;
    }

    for (const [, persistence] of this.persistences) {
      await persistence.destroy();
    }

    for (const [, doc] of this.docs) {
      doc.destroy();
    }
    for (const [, awareness] of this.awarenesses) {
      awareness.destroy();
    }
    this.docs.clear();
    this.persistences.clear();
    this.awarenesses.clear();
    this.pendingUpdates.clear();
  }

  // ─── Private ──────────────────────────────────────────────────────────────

  private queueUpdate(collectionName: string, update: Uint8Array): void {
    let updates = this.pendingUpdates.get(collectionName);

    if (!updates) {
      updates = [];
      this.pendingUpdates.set(collectionName, updates);
    }

    updates.push(update);

    if (
      !this.syncTimer &&
      (typeof document === "undefined" || document.visibilityState !== "hidden")
    ) {
      if (typeof window !== "undefined" && window.requestAnimationFrame) {
        this.syncTimer = window.requestAnimationFrame(() => this.flushUpdates());

        this.syncTimerIsRaf = true;
      } else {
        this.syncTimer = setTimeout(() => this.flushUpdates(), 50);
        this.syncTimerIsRaf = false;
      }
    }
  }

  private flushUpdates(): void {
    this.syncTimer = null;
    for (const [collectionName, updates] of this.pendingUpdates.entries()) {
      const merged = Y.mergeUpdates(updates);
      void this.handleLocalUpdate(collectionName, merged);
    }

    this.pendingUpdates.clear();
  }

  private onPeerUpdate(msg: IncomingPeerDataMessage): void {
    if (msg.type === "sync-upgrade-offer") {
      const payloadStr =
        typeof msg.payload === "string" ? msg.payload : new TextDecoder().decode(msg.payload);
      const offer = JSON.parse(payloadStr) as { pluginUrl: string; version: number };
      this.loadPlugin(offer.pluginUrl)
        .then(() => {
          this.network.sendTo(msg.from, {
            type: "sync-upgrade-accept",
            payload: JSON.stringify({ version: offer.version }),
          });
        })
        .catch(() => {
          console.warn(`Peer ${msg.from} failed to upgrade. Ignoring their updates.`);
        });

      return;
    }

    if (msg.type === "sync-upgrade-accept") {
      return;
    }

    if (msg.type !== "sync-update" && msg.type !== "awareness-update") return;

    let payload: Uint8Array;

    try {
      payload = base64ToBytes(msg.payload);
    } catch {
      return;
    }

    const decoded = this.decodeMessage(payload);
    if (decoded === null) return;

    if (msg.type === "sync-update") {
      void this.applyRemoteUpdate(decoded.collectionName, decoded.update, msg.from);
    } else {
      this.applyRemoteAwarenessUpdate(decoded.collectionName, decoded.update);
    }
  }

  private onPeerConnected(peer: { peerId: string }): void {
  const peerId = peer.peerId;
    this.updateState({ connectedPeers: this.network.connectedPeerCount });
    void this.sendCapability(peerId);
    void this.flushOutbox();

    if (peer?.peerId) {
      for (const [collectionName, doc] of this.docs.entries()) {
        const stateVector = Y.encodeStateVector(doc);
        this.network.sendTo(peer.peerId, {
          type: "sync-request",
          payload: this.encodeMessage(collectionName, stateVector),
        });
      }
    }
  }

  private onPeerDisconnected(peer: { peerId: string }): void {
  const peerId = peer.peerId;
    this.peerCapabilities.delete(peerId);
    this.updateState({ connectedPeers: this.network.connectedPeerCount });
  }

  private async handleLocalUpdate(collectionName: string, update: Uint8Array): Promise<void> {
    try {
      let finalUpdate: Uint8Array | null = update;

      for (const plugin of this.plugins.values()) {
        if (plugin.onBeforeSendUpdate) {
          finalUpdate = await plugin.onBeforeSendUpdate(collectionName, finalUpdate);
          if (!finalUpdate) return;
        }
      }

      const mutation = await this.outbox.enqueue({
        type: "sync-update",
        collection: collectionName,
        payload: finalUpdate,
      });

      if (!this._enabled) return;

      this.emit("update:local", {
        collectionName,
        update: finalUpdate,
      });

      if (this.network.connectedPeerCount === 0) return;

      this.network.broadcast({
        type: "sync-update",
        payload: this.protocol.encode(collectionName, finalUpdate),
      });

      await this.outbox.acknowledge(mutation.id);
    } catch {
      // Swallow queue errors
    }
  }

  private async handleRemoteUpdate(
    collectionName: string,
    update: Uint8Array,
    fromPeer: string
  ): Promise<void> {
    let mutationId: string | null = null;
    try {
      const rawPayload = typeof msg.payload === "string" ? msg.payload : new TextDecoder().decode(msg.payload);
      const { collectionName, doc, peerId } = JSON.parse(rawPayload);
      
      const mutation = await this.inbox.enqueue({
        type: "sync-update",
        collection: collectionName,
        payload: doc,
      });

      mutationId = mutation.id;
    } catch {
      // continue
    }

    try {
      const doc = this.getDoc(collectionName);
      const localSnapshot = Y.encodeStateAsUpdate(doc);

      for (const plugin of this.plugins.values()) {
        if (!plugin.conflictResolver) continue;

        const resolveConflict = plugin.conflictResolver.resolveConflict;
        if (!resolveConflict) continue;

        const resolution = await resolveConflict(
          collectionName,
          localSnapshot,
          update,
          fromPeer
        );

        if (!resolution) {
          this.emit("conflict:flagged", {
            collectionName,
            fromPeer,
            localSnapshot,
            incomingUpdate: update,
          });
          break;
        }

        if (resolution instanceof Uint8Array) {
          update = resolution;
        } else {
          update = resolution.update;
          if (resolution.suggestion) {
            this.emit("conflict:flagged", {
              collectionName,
              fromPeer,
              localSnapshot,
              incomingUpdate: update,
              suggestion: resolution.suggestion,
            });
          }
        }
      }

      Y.applyUpdate(doc, update, "remote");

      if (mutationId) {
        await this.inbox.acknowledge(mutationId);
      }

      this.emit("update:remote", {
        collectionName,
        update,
        fromPeer,
      });
    } catch {
      if (mutationId) {
        await this.inbox.markFailed(mutationId);
      }
    }
  }

  private onPeerConnected(peer: { peerId: string }): void {
  const peerId = peer.peerId;
    this.updateState({ connectedPeers: this.network.connectedPeerCount });
    void this.sendCapability(peerId);
    void this.flushOutbox();

    const pending = await this.outbox.getPending();
    for (const mutation of pending) {
      this.network.broadcast({
        type: "sync-update",
        payload: this.encodeMessage(mutation.collection, mutation.payload),
      });
      await this.outbox.acknowledge(mutation.id);
    }
  }

  private encodeMessage(collectionName: string, update: Uint8Array): string {
    const nameBytes = new TextEncoder().encode(collectionName);
    const header = new Uint8Array(2);
    header[0] = (nameBytes.length >> 8) & 0xff;
    header[1] = nameBytes.length & 0xff;
    const combined = new Uint8Array(2 + nameBytes.length + update.length);
    combined.set(header, 0);
    combined.set(nameBytes, 2);
    combined.set(update, 2 + nameBytes.length);
    return bytesToBase64(combined);
  }

  private decodeMessage(bytes: Uint8Array): {
    collectionName: string;
    update: Uint8Array;
  } | null {
    try {
      if (bytes.length < 2) return null;
      const nameLen = (bytes[0]! << 8) | bytes[1]!;
      if (bytes.length < 2 + nameLen) return null;
      const nameBytes = bytes.slice(2, 2 + nameLen);
      const update = bytes.slice(2 + nameLen);
      return {
        collectionName: new TextDecoder().decode(nameBytes),
        update,
      };
    } catch {
      return null;
    }
  }

  private updateState(partial: Partial<SyncState>): void {
    this._state = { ...this._state, ...partial };
    this.emit("state:change", this._state);
  }

  private async refreshPendingCount(): Promise<void> {
    const pending = await this.outbox.count();

    this.updateState({
      pendingUpdates: pending,
    });
  }
}