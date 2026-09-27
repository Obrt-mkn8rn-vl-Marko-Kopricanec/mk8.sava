import assert from "node:assert/strict";
import { BlobServiceClient, StorageSharedKeyCredential } from "@azure/storage-blob";

// Seed a published server, or verify the same content after operator restore.
const mode = process.argv[2];
assert.ok(mode === "seed" || mode === "verify", "Expected seed or verify.");
const required = name => {
  const value = process.env[name];
  assert.ok(value, `Missing ${name}.`);
  return value;
};
const endpoint = new URL(required("MK8_SAVA_BLOB_ENDPOINT"));
assert.ok(endpoint.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname),
  "Published smoke requires a disposable loopback service, not a live account.");
const service = new BlobServiceClient(endpoint.toString(),
  new StorageSharedKeyCredential(required("MK8_SAVA_ACCOUNT_NAME"), required("MK8_SAVA_ACCOUNT_KEY")),
  { retryOptions: { maxTries: 1 } });
const container = service.getContainerClient("published-native-smoke");
const blob = container.getBlockBlobClient("rows.parquet");

// The independently decoded fixture from ParquetQueryTests: required INT32 id,
// one uncompressed PLAIN data page, three values and compact-Thrift metadata.
const page = Buffer.from("1500151815182c15061500150615060000", "hex");
const values = Buffer.alloc(12);
for (let index = 0; index < 3; index++) values.writeInt32LE(index + 1, index * 4);
const footer = Buffer.concat([
  Buffer.from("1502192c4806", "hex"), Buffer.from("schema"),
  Buffer.from("150200150225001802", "hex"), Buffer.from("id"),
  Buffer.from("001606191c191c26081c150219250006191802", "hex"), Buffer.from("id"),
  Buffer.from("15001606163a163a26080000163a16060000", "hex"),
]);
const length = Buffer.alloc(4);
length.writeInt32LE(footer.length);
const fixture = Buffer.concat([Buffer.from("PAR1"), page, values, footer, length, Buffer.from("PAR1")]);

if (mode === "seed") {
  await container.create();
  await blob.uploadData(fixture, { metadata: { qualification: "published-native-parquet" } });
}
assert.deepEqual(await blob.downloadToBuffer(), fixture);
assert.equal((await blob.getProperties()).metadata.qualification, "published-native-parquet");
const errors = [];
const queried = await blob.query("SELECT id AS id FROM BlobStorage;", {
  inputTextConfiguration: { kind: "parquet" },
  outputTextConfiguration: { kind: "json", recordSeparator: "\n" },
  onError: error => errors.push(error),
});
assert.ok(queried.readableStreamBody);
const chunks = [];
for await (const chunk of queried.readableStreamBody) chunks.push(Buffer.from(chunk));
assert.deepEqual(errors, []);
assert.equal(Buffer.concat(chunks).toString("utf8"), '{"id":1}\n{"id":2}\n{"id":3}\n');
console.log(`Published native Parquet ${mode} passed with official JavaScript SDK 12.32.0.`);

// Exact query fixtures/outcomes from Microsoft's SDK recordings, including
// empty optional XML fields produced by the unmodified JavaScript serializer.
const json = '{"_1":"100","_2":"200","_3":"300","_4":"400"}\n' +
  '{"_1":"150","_2":"250","_3":"350","_4":"450"}\n' +
  '{"_1":"180","_2":"280","_3":"380","_4":"480"}\n';
const cases = [
  { name: "recorded.json", content: json, sql: "select * from BlobStorage", expected: json,
    options: { inputTextConfiguration: { kind: "json", recordSeparator: "\n" },
      outputTextConfiguration: { kind: "json", recordSeparator: "\n" } } },
  { name: "recorded.csv", content: "100.200.300.400!150.250.350.450!180.280.380.480!",
    sql: "select _1 from BlobStorage", expected: "150!180!",
    options: { inputTextConfiguration: { kind: "csv", columnSeparator: ".", recordSeparator: "!", hasHeaders: true },
      outputTextConfiguration: { kind: "csv", columnSeparator: ".", recordSeparator: "!", hasHeaders: false } } },
  { name: "recorded-nonfatal.csv", content: "100,hello,300,400\n150,250,350,450\n",
    sql: "select _2 from BlobStorage where _2 > 100", expected: "250\n", options: {},
    error: { isFatal: false, name: "InvalidTypeConversion", position: 0, description: "Invalid type conversion." } },
  { name: "recorded-fatal.csv", content: "100,200,300,400\n150,250,350,450\n",
    sql: "select * from BlobStorage", expected: "\n",
    options: { inputTextConfiguration: { kind: "json", recordSeparator: "\n" } },
    error: { isFatal: true, name: "ParseError", position: 0,
      description: "Unexpected token ',' at [byte: 3]. Expecting tokens '{', or '['. " } },
];
for (const test of cases) {
  const target = container.getBlockBlobClient(test.name);
  if (mode === "seed") await target.uploadData(Buffer.from(test.content));
  assert.equal((await target.downloadToBuffer()).toString("utf8"), test.content);
  const observedErrors = [];
  const result = await target.query(test.sql, { ...test.options, onError: error => observedErrors.push(error) });
  assert.ok(result.readableStreamBody);
  const data = [];
  for await (const chunk of result.readableStreamBody) data.push(Buffer.from(chunk));
  assert.equal(Buffer.concat(data).toString("utf8"), test.expected, test.name);
  assert.deepEqual(observedErrors, test.error ? [test.error] : [], test.name);
}
console.log(`Published recorded query defaults/errors ${mode} passed without live Azure.`);
