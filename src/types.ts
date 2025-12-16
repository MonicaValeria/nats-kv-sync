export interface CrdtOperation {
  op: 'PUT' | 'DEL';
  bucket: string;
  key: string;
  value: string | null; // null si es DEL
  ts: number;           // timestamp
  node_id: string;
}

export interface KvMetadata {
  ts: number;
  node_id: string;
}