# Documentación del Proyecto: Sincronización KV Distribuida (CRDT LWW con NATS JetStream)

## Hecho por

* Steven Jose Silva Gomez
* Monica Valeia Tipe Castro

## 1. Introducción y Objetivo

Este proyecto implementa un sistema de **sincronización distribuida y eventual** de un almacén **Key-Value (KV)** utilizando **NATS JetStream** y el algoritmo **CRDT (Conflict-free Replicated Data Type)** de tipo **Last-Write-Wins (LWW)**, extendido con metadatos.

El objetivo principal es permitir que múltiples sitios (por ejemplo, nodos **A** y **B**), potencialmente separados geográficamente o expuestos a fallos de red (particiones), puedan operar y actualizar su configuración local de manera independiente.

El sistema garantiza la **convergencia de los datos** una vez que se restablece la comunicación, priorizando la versión más reciente según:
- El **timestamp** de la operación.
- El **ID del nodo** como criterio de desempate determinista.

---

## 2. Arquitectura de Componentes

El sistema consta de tres componentes principales:

| Componente | Función | Tecnologías |
|-----------|--------|-------------|
| **NATS Servers** | Servidores de mensajería asíncrona que hospedan la capa de persistencia (JetStream). | NATS |
| **JetStream Stream (REPLICATION)** | Stream central duradero que almacena todos los eventos de replicación (`rep.kv.ops`) para garantizar que ningún nodo pierda actualizaciones. | NATS JetStream |
| **Agente Syncd (TypeScript)** | Núcleo de la lógica: ejecuta el Watcher local, el Replicador remoto y aplica la resolución de conflictos CRDT. | TypeScript, `nats.js` |

---

## 3. Estructura de Almacenamiento (Key-Value)

En cada nodo se utilizan **dos Buckets KV** de JetStream:

| Bucket | Propósito | Contenido |
|-------|----------|----------|
| **config** | Almacena el valor real de configuración (dato de negocio). | `key → value` |
| **config_meta** | Almacena los metadatos CRDT necesarios para la resolución de conflictos. | `key → { ts: number, node_id: string }` |

Esta separación permite mantener la lógica de negocio desacoplada de la lógica de sincronización y resolución de conflictos.

---

## 4. Lógica de Sincronización

El agente **Syncd** maneja dos flujos principales:
- **Flujo de Salida** (Watcher Local)
- **Flujo de Entrada** (Replicador Remoto)

### 4.1. Flujo de Salida (Publicación de Eventos)

1. **Watcher KV**  
   El agente se suscribe al *watcher* del bucket local `config`.

2. **Detección de Cambio Local**  
   Cuando un usuario o aplicación realiza un `PUT` o `DELETE` en el KV local (por ejemplo, vía CLI), el watcher detecta el evento.

3. **Filtro de Eco**  
   El watcher consulta el bucket `config_meta`. Si la metainformación indica que la última escritura provino de un nodo remoto **y** el valor coincide, el evento se ignora para evitar bucles de replicación.

4. **Generación de Operación CRDT**  
   Si el cambio es genuinamente local, se crea un objeto `CrdtOperation` que incluye:
   - El valor actualizado.
   - El timestamp actual (`ts = Date.now()`).
   - El identificador del nodo local (`node_id`).

5. **Publicación**  
   La operación CRDT se publica en el subject `rep.kv.ops` del stream **REPLICATION**.

---

### 4.2. Flujo de Entrada (Resolución de Conflictos LWW)

1. **Suscripción Durable**  
   El agente utiliza un **Consumidor Pull Durable** (por ejemplo, `syncd-<NODE_ID>`) para suscribirse al subject `rep.kv.ops`. Esto garantiza la recepción de mensajes incluso tras desconexiones prolongadas.

2. **Recepción de Operaciones**  
   El agente recibe una `CrdtOperation` proveniente de un nodo remoto.

3. **Filtrado de Auto‑Eco**  
   Si el `node_id` de la operación coincide con el `NODE_ID` local, el mensaje se ignora.

4. **Resolución de Conflictos (LWW)**  
   Se consulta el bucket `config_meta` local para obtener:
   - `ts_local`: timestamp de la última escritura conocida.
   - `id_local`: ID del nodo de la última escritura.

   La operación remota **solo se aplica** si cumple la regla **Last‑Write‑Wins**:

   ```math
   \text{Aplicar si } (ts_{remoto} > ts_{local}) \\
   \text{o } (ts_{remoto} = ts_{local} \; \text{y} \; id_{remoto} > id_{local})
   ```

   La comparación lexicográfica de `node_id` actúa como desempate determinista cuando los timestamps son idénticos.

5. **Aplicación y Actualización de Metadatos**  
   - Si la operación remota gana:
     1. Se actualiza primero `config_meta` con los metadatos remotos.
     2. Se aplica la operación (`PUT` o `DELETE`) sobre el bucket `config`.

6. **Acuse de Recibo (ACK)**  
   El mensaje se reconoce explícitamente a JetStream (`m.ack()`). Si el ACK no ocurre (por ejemplo, por un fallo del agente), JetStream reintentará la entrega.

---

## 5. Manejo de Particiones de Red (Resiliencia)

La arquitectura es **intrínsecamente tolerante a particiones de red**, gracias a las capacidades de JetStream:

1. **Persistencia**  
   Si un nodo (por ejemplo, el Sitio A) se desconecta, los demás nodos continúan publicando cambios. El stream **REPLICATION** retiene todos los eventos no consumidos.

2. **Reconexión y Recuperación**  
   Cuando el nodo caído vuelve a estar en línea, su consumidor durable (por ejemplo, `syncd-site-a`) reanuda la entrega exactamente desde el último mensaje reconocido, asegurando que:
   - Ninguna actualización se pierde.
   - Todos los nodos convergen finalmente al mismo estado.

