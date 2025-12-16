import {
  connect,
  NatsConnection,
  StringCodec,
  JSONCodec,
  JetStreamClient,
  ConsumerConfig,
  DeliverPolicy,
  AckPolicy,
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
  const js = nc.jetstream();

  // 1. Inicializar Stream de Replicación (Durabilidad - Sección 9.1)
  await ensureStream(js);

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
    if (!entry) continue; // Inicialización

    const key = entry.key;
    const value = entry.value ? sc.decode(entry.value) : null;
    const opType =
      entry.operation === "DEL" || entry.value === null ? "DEL" : "PUT";

    // LEER METADATOS ACTUALES PARA EVITAR BUCLES
    // Si el cambio local coincide exactamente con los metadatos de una operación remota reciente,
    // significa que este evento fue disparado por nuestra propia función 'applyRemoteOperation'.
    // Debemos ignorarlo para no re-publicarlo.
    const metaEntry = await kvMeta.get(key);
    let currentMeta: KvMetadata | null = null;

    if (metaEntry?.value) {
      currentMeta = metaEntry.json() as KvMetadata;
    }

    // Si la última escritura registrada en meta vino de OTRO nodo, y el valor coincide, es un eco.
    // Nota: Esta es una simplificación. En prod, podríamos comparar revisiones o usar un flag en memoria.
    if (currentMeta && currentMeta.node_id !== NODE_ID) {
      // Es muy probable que este evento sea consecuencia de una sincronización remota entrante.
      // Lo ignoramos para romper el bucle.
      continue;
    }

    // Si llegamos aquí, es un cambio generado localmente por el usuario.
    const ts = Date.now(); // Reloj físico simple

    // 1. Actualizamos meta para marcarlo como "nuestro"
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

// --- Lógica del Suscriptor Remoto (CRDT Merge) ---
async function startRemoteListener(js: JetStreamClient, kv: any, kvMeta: any) {
  // Consumidor duradero para recuperar mensajes tras desconexión (Sección 9.1)
  const consumerOpts: ConsumerConfig = {
    durable_name: `syncd-${NODE_ID}`,
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
  };

  const sub = await js.pullSubscribe(SUBJECT_NAME, { config: consumerOpts });
  console.log("📥 Suscrito a operaciones remotas (Durable)");

  const done = (async () => {
    for await (const m of sub) {
      const op = jc.decode(m.data) as CrdtOperation;

      // Ignorar nuestros propios mensajes (eco de red)
      if (op.node_id === NODE_ID) {
        m.ack();
        continue;
      }

      // --- CRDT LWW RESOLUTION ---
      const key = op.key;
      const metaEntry = await kvMeta.get(key);
      let localMeta: KvMetadata = { ts: 0, node_id: "" };

      if (metaEntry?.value) {
        localMeta = metaEntry.json() as KvMetadata;
      }

      // Regla: (ts_remoto > ts_local) OR (ts_remoto == ts_local AND node_id_remoto > node_id_local)
      const remoteWins =
        op.ts > localMeta.ts ||
        (op.ts === localMeta.ts && op.node_id > localMeta.node_id);

      if (remoteWins) {
        console.log(
          `✅ [SYNC IN] Aplicando remoto (${op.node_id}): key=${key} val=${op.value}`
        );

        // 1. Actualizar Metadatos PRIMERO (para que el watcher local sepa que no es un cambio de usuario)
        const newMeta: KvMetadata = { ts: op.ts, node_id: op.node_id };
        await kvMeta.put(key, jc.encode(newMeta));

        // 2. Aplicar cambio al KV
        if (op.op === "PUT" && op.value !== null) {
          await kv.put(key, sc.encode(op.value));
        } else if (op.op === "DEL") {
          await kv.delete(key); // O usar soft delete (tombstone) en KV si se prefiere
        }
      } else {
        console.log(
          `🛡️ [SYNC IN] Conflicto: Gana local. Ignorando remoto de ${op.node_id} (Local TS: ${localMeta.ts} >= Remote TS: ${op.ts})`
        );
      }

      m.ack();
    }
  })();

  // Loop continuo de pull para obtener mensajes
  while (true) {
    try {
      sub.pull({ batch: 10, expires: 1000 });
      await new Promise((r) => setTimeout(r, 1000)); // Evitar busy loop excesivo si no hay msgs
    } catch (err) {
      // manejar errores de conexión
    }
  }
}

// --- Helpers ---

async function ensureStream(js: JetStreamClient) {
  try {
    await js.streamInfo(STREAM_NAME);
  } catch (e) {
    await js.addStream({
      name: STREAM_NAME,
      subjects: [SUBJECT_NAME],
      storage: 1, // File storage (durability)
    });
    console.log(`stream ${STREAM_NAME} creado.`);
  }
}

main().catch(console.error);
