import {
  connect,
  StringCodec,
  JSONCodec,
  JetStreamClient,
  JetStreamManager,
  ConsumerConfig,
  DeliverPolicy,
  AckPolicy,
  ReplayPolicy,
  StorageType,
  KvEntry,
} from "nats";
import minimist from "minimist";
import { CrdtOperation, KvMetadata } from "./types";

const argv = minimist(process.argv.slice(2));
const NATS_URL = argv.url || "nats://localhost:4222";
const NODE_ID = argv["node-id"] || "unknown-node";
// Frecuencia de Anti-Entropy en ms (Por defecto 1 minuto para probar, en prod serían 5 min)
const ENTROPY_INTERVAL = 60000;

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
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();

  // 1. Infraestructura
  await ensureStream(jsm);

  // 2. Buckets
  const kv = await js.views.kv(BUCKET_NAME, { history: 1 });
  const kvMeta = await js.views.kv(META_BUCKET_NAME, { history: 1 });

  // 3. Listener Remoto (CRDT Merge)
  startRemoteListener(js, kv, kvMeta);

  // 4. Watcher Local (Detectar cambios usuario)
  startLocalWatcher(js, kv, kvMeta);

  // 5. Anti-Entropy (NUEVO: Reconciliación periódica)
  startAntiEntropy(js, kv, kvMeta);

  console.log(
    `✅ Agente corriendo. Anti-Entropy programado cada ${
      ENTROPY_INTERVAL / 1000
    }s`
  );
}

// --- Lógica de Anti-Entropy (NUEVO) ---
function startAntiEntropy(js: JetStreamClient, kv: any, kvMeta: any) {
  setInterval(async () => {
    console.log("🔄 [ANTI-ENTROPY] Iniciando ciclo de reconciliación...");

    try {
      // Iteramos sobre todas las claves del bucket de datos
      const keys = await kv.keys();
      let count = 0;

      for await (const key of keys) {
        // Obtenemos valor y metadatos actuales
        const entry = await kv.get(key);
        const metaEntry = await kvMeta.get(key);

        if (!entry || !metaEntry) continue;

        const value = sc.decode(entry.value);
        const meta = metaEntry.json() as KvMetadata;

        // Construimos la operación con el estado ACTUAL (preservando el timestamp original)
        const op: CrdtOperation = {
          op: "PUT", // Asumimos PUT si existe. Si soportamos soft-delete, habría que revisar tombstone.
          bucket: BUCKET_NAME,
          key: key,
          value: value,
          ts: meta.ts, // IMPORTANTE: Usar el TS original, no Date.now()
          node_id: meta.node_id,
        };

        // Re-publicamos al stream.
        // Si el otro nodo ya lo tiene, lo ignorará por la regla CRDT.
        // Si no lo tiene, lo aplicará.
        await js.publish(SUBJECT_NAME, jc.encode(op));
        count++;
      }
      console.log(
        `🔄 [ANTI-ENTROPY] Ciclo finalizado. ${count} claves verificadas/re-difundidas.`
      );
    } catch (err) {
      console.error("⚠️ Error en ciclo Anti-Entropy:", err);
    }
  }, ENTROPY_INTERVAL);
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

    const metaEntry = await kvMeta.get(key);
    let currentMeta: KvMetadata | null = null;

    if (metaEntry?.value) {
      currentMeta = metaEntry.json() as KvMetadata;
    }

    // Evitar bucle infinito: Si el cambio coincide con un meta remoto reciente, es un eco.
    if (currentMeta && currentMeta.node_id !== NODE_ID) {
      continue;
    }

    const ts = Date.now();

    // Actualizamos meta
    const newMeta: KvMetadata = { ts, node_id: NODE_ID };
    await kvMeta.put(key, jc.encode(newMeta));

    // Publicamos
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
    replay_policy: ReplayPolicy.Instant,
  };

  const sub = await js.pullSubscribe(SUBJECT_NAME, { config: consumerOpts });
  console.log("📥 Suscrito a operaciones remotas (Durable)");

  const done = (async () => {
    for await (const m of sub) {
      const op = jc.decode(m.data) as CrdtOperation;

      // En Anti-Entropy recibiremos nuestros propios mensajes rebotados.
      // Debemos ignorarlos si el node_id coincide.
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
        // Esto ocurrirá frecuentemente durante Anti-Entropy (datos ya sincronizados)
        // No imprimimos log para no ensuciar la consola, o usamos debug
        // console.log(`🛡️ [SYNC IN] Ignorado (Ya actualizado) de ${op.node_id}`);
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

async function ensureStream(jsm: JetStreamManager) {
  try {
    await jsm.streams.info(STREAM_NAME);
  } catch (e) {
    await jsm.streams.add({
      name: STREAM_NAME,
      subjects: [SUBJECT_NAME],
      storage: StorageType.File,
    });
    console.log(`stream ${STREAM_NAME} creado.`);
  }
}

main().catch(console.error);
