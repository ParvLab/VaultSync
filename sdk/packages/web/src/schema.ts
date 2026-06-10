import type { VaultSyncClient } from './index.js';

export interface FieldDef {
  type: "string" | "number" | "boolean" | "array" | "object";
  crdtType: "lww" | "counter" | "orset" | "text" | "array";
  primaryKey?: boolean;
  indexed?: boolean;
  sync?: boolean;
}

export interface SchemaDefinition {
  version: number;
  fields: Record<string, FieldDef>;
}

export async function defineSchema(
  client: VaultSyncClient,
  docId: string,
  schemaDef: SchemaDefinition
): Promise<void> {
  const fields = Object.entries(schemaDef.fields).map(([name, def]) => {
    const valueTypeMap: Record<string, string> = {
      string: "String",
      number: "Number",
      boolean: "Boolean",
      array: "Array",
      object: "Object",
    };
    const crdtTypeMap: Record<string, string> = {
      lww: "LwwRegister",
      counter: "PnCounter",
      orset: "OrSet",
      text: "Text",
      array: "Array",
    };

    const value_type = valueTypeMap[def.type];
    if (!value_type) {
      throw new Error(`Unsupported value type: ${def.type}`);
    }
    const crdt_type = crdtTypeMap[def.crdtType];
    if (!crdt_type) {
      throw new Error(`Unsupported crdt type: ${def.crdtType}`);
    }

    return {
      name,
      value_type,
      crdt_type,
      primary_key: !!def.primaryKey,
      indexed: !!def.indexed,
      sync: def.sync !== false,
    };
  });

  const schema = {
    doc_id: docId,
    fields,
    version: schemaDef.version,
  };

  await client['inner'].define_schema(docId, JSON.stringify(schema));
}
