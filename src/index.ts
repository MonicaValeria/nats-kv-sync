import {
  connect,
  StringCodec,
  JSONCodec,
  JetStreamClient,
  JetStreamManager,
  ConsumerConfig,
  DeliverPolicy,
  AckPolicy,
  ReplayPolicy, // <--- Importado
  StorageType, // <--- Importado
} from "nats";
import minimist from "minimist";
import { CrdtOperation, KvMetadata } from "./types";

const argv = minimist(process.argv.slice(2));
const NATS_URL = argv.url || "nats://localhost:4222";
const NODE_ID = argv["node-id"] || "unknown-node";
const BUCKET_NAME = "config";
const META_BUCKET_NAME = "config_meta";
const STREAM_NAME = "REPLICATION";
const SUBJECT_NAME = "rep.kv.ops";

const sc = StringCodec();
const jc = JSONCodec();

async function main() {
  console.log(
    `🚀 Iniciando Agente Syncd [${NODE_ID}] conectando a ${NATS_URL}`
  );

  const nc = await connect({ servers: NATS_URL });

  // Instancia de cliente (para publicar/suscribir)
  const js = nc.jetstream();

  // Instancia de Manager (para crear streams y admin)
  const jsm = await nc.jetstreamManager();

  // 1. Inicializar Stream de Replicación
  await ensureStream(jsm);

  // 2. Inicializar Buckets KV (Datos y Metadatos)
  const kv = await js.views.kv(BUCKET_NAME, { history: 1 });
  const kvMeta = await js.views.kv(META_BUCKET_NAME, { history: 1 });

  // 3. Iniciar Suscriptor Remoto (Escucha cambios de otros sitios)
  startRemoteListener(js, kv, kvMeta);

  // 4. Iniciar Watcher Local (Detecta cambios locales y publica)
  startLocalWatcher(js, kv, kvMeta);

  console.log(`✅ Agente corriendo. Esperando cambios...`);
}

// --- Lógica del Watcher Local ---
async function startLocalWatcher(js: JetStreamClient, kv: any, kvMeta: any) {
  const watch = await kv.watch();
  console.log("👀 Vigilando cambios locales en bucket:", BUCKET_NAME);

  for await (const entry of watch) {
    if (!entry) continue;

    const key = entry.key;
    const value = entry.value ? sc.decode(entry.value) : null;
    const opType =
      entry.operation === "DEL" || entry.value === null ? "DEL" : "PUT";

    // LEER METADATOS ACTUALES
    const metaEntry = await kvMeta.get(key);
    let currentMeta: KvMetadata | null = null;

    if (metaEntry?.value) {
      currentMeta = metaEntry.json() as KvMetadata;
    }

    // Evitar eco
    if (currentMeta && currentMeta.node_id !== NODE_ID) {
      continue;
    }

    const ts = Date.now();

    // 1. Actualizamos meta
    const newMeta: KvMetadata = { ts, node_id: NODE_ID };
    await kvMeta.put(key, jc.encode(newMeta));

    // 2. Publicamos la operación CRDT
    const op: CrdtOperation = {
      op: opType,
      bucket: BUCKET_NAME,
      key: key,
      value: value,
      ts: ts,
      node_id: NODE_ID,
    };

    await js.publish(SUBJECT_NAME, jc.encode(op));
    console.log(`📤 [SYNC OUT] Enviando ${op.op} key=${key} ts=${ts}`);
  }
}

// --- Lógica del Suscriptor Remoto ---
async function startRemoteListener(js: JetStreamClient, kv: any, kvMeta: any) {
  const consumerOpts: ConsumerConfig = {
    durable_name: `syncd-${NODE_ID}`,
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
    replay_policy: ReplayPolicy.Instant, // <--- Corrección 1: Agregado replay_policy
  };

  const sub = await js.pullSubscribe(SUBJECT_NAME, { config: consumerOpts });
  console.log("📥 Suscrito a operaciones remotas (Durable)");

  const done = (async () => {
    for await (const m of sub) {
      const op = jc.decode(m.data) as CrdtOperation;

      if (op.node_id === NODE_ID) {
        m.ack();
        continue;
      }

      const key = op.key;
      const metaEntry = await kvMeta.get(key);
      let localMeta: KvMetadata = { ts: 0, node_id: "" };

      if (metaEntry?.value) {
        localMeta = metaEntry.json() as KvMetadata;
      }

      // CRDT LWW
      const remoteWins =
        op.ts > localMeta.ts ||
        (op.ts === localMeta.ts && op.node_id > localMeta.node_id);

      if (remoteWins) {
        console.log(
          `✅ [SYNC IN] Aplicando remoto (${op.node_id}): key=${key}`
        );

        const newMeta: KvMetadata = { ts: op.ts, node_id: op.node_id };
        await kvMeta.put(key, jc.encode(newMeta));

        if (op.op === "PUT" && op.value !== null) {
          await kv.put(key, sc.encode(op.value));
        } else if (op.op === "DEL") {
          await kv.delete(key);
        }
      } else {
        console.log(`🛡️ [SYNC IN] Ignorado (Old TS) de ${op.node_id}`);
      }

      m.ack();
    }
  })();

  while (true) {
    try {
      sub.pull({ batch: 10, expires: 1000 });
      await new Promise((r) => setTimeout(r, 1000));
    } catch (err) {
      // Reintentar loop
    }
  }
}

// --- Helper ---
async function ensureStream(jsm: JetStreamManager) {
  try {
    await jsm.streams.info(STREAM_NAME);
  } catch (e) {
    await jsm.streams.add({
      name: STREAM_NAME,
      subjects: [SUBJECT_NAME],
      storage: StorageType.File, // <--- Corrección 2: Uso de Enum StorageType
    });
    console.log(`stream ${STREAM_NAME} creado.`);
  }
}

main().catch(console.error);
