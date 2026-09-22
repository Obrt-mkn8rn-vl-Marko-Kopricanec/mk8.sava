import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  BlobClient,
  BlobSASPermissions,
  BlobServiceClient,
  StorageSharedKeyCredential,
  generateBlobSASQueryParameters,
} from "@azure/storage-blob";

const endpoint = requiredEnvironment("MK8_SAVA_BLOB_ENDPOINT");
const accountName = requiredEnvironment("MK8_SAVA_ACCOUNT_NAME");
const accountKey = requiredEnvironment("MK8_SAVA_ACCOUNT_KEY");
const sdkPackageVersion = "12.32.0";
const credential = new StorageSharedKeyCredential(accountName, accountKey);
const options = {
  retryOptions: { maxTries: 1 },
};
const service = new BlobServiceClient(endpoint, credential, options);
const containerName = `javascript-${randomUUID().replaceAll("-", "")}`;
const container = service.getContainerClient(containerName);
let created = false;

try {
  const create = await container.create({ metadata: { runtime: "node" } });
  assert.equal(create._response.status, 201);
  created = true;

  const containers = [];
  for await (const item of service.listContainers({ prefix: containerName, includeMetadata: true }))
    containers.push(item);
  assert.equal(containers.length, 1);
  assert.equal(containers[0].name, containerName);
  assert.equal(containers[0].metadata.runtime, "node");

  const content = Buffer.alloc(256 * 1024 + 37);
  for (let index = 0; index < content.length; index++)
    content[index] = (index * 29 + Math.floor(index / 257)) % 251;

  const block = container.getBlockBlobClient("folder/sdk-roundtrip.bin");
  const upload = await block.uploadData(content, {
    blobHTTPHeaders: {
      blobContentType: "application/x-mk8-javascript",
      blobCacheControl: "private,max-age=17",
    },
    metadata: { owner: "javascript-sdk" },
    tags: { runtime: "node", purpose: "compatibility" },
  });
  assert.equal(upload._response.status, 201);
  assert.deepEqual(await block.downloadToBuffer(), content);
  assert.deepEqual(await block.downloadToBuffer(12_345, 7_777), content.subarray(12_345, 20_122));

  const properties = await block.getProperties();
  assert.equal(properties.contentLength, content.length);
  assert.equal(properties.contentType, "application/x-mk8-javascript");
  assert.equal(properties.cacheControl, "private,max-age=17");
  assert.equal(properties.metadata.owner, "javascript-sdk");
  assert.equal((await block.getTags()).tags.runtime, "node");

  const listed = [];
  for await (const item of container.listBlobsFlat({
    includeMetadata: true,
    includeTags: true,
    prefix: "folder/",
  })) {
    listed.push(item);
  }
  assert.equal(listed.length, 1);
  assert.equal(listed[0].name, block.name);
  assert.equal(listed[0].metadata.owner, "javascript-sdk");
  assert.equal(listed[0].tags.runtime, "node");

  const snapshot = await block.createSnapshot({ metadata: { owner: "snapshot" } });
  const snapshotClient = block.withSnapshot(snapshot.snapshot);
  assert.deepEqual(await snapshotClient.downloadToBuffer(), content);
  assert.equal((await snapshotClient.getProperties()).metadata.owner, "snapshot");

  const staged = container.getBlockBlobClient("staged.bin");
  const blockIds = ["block-0001", "block-0002"].map(value => Buffer.from(value).toString("base64"));
  const first = Buffer.from("first-block|");
  const second = Buffer.from("second-block");
  await staged.stageBlock(blockIds[0], first, first.length);
  await staged.stageBlock(blockIds[1], second, second.length);
  await staged.commitBlockList(blockIds, {
    metadata: { route: "staged" },
    tags: { runtime: "node" },
  });
  assert.deepEqual(await staged.downloadToBuffer(), Buffer.concat([first, second]));
  assert.deepEqual(
    (await staged.getBlockList("committed")).committedBlocks.map(item => item.name),
    blockIds,
  );

  const append = container.getAppendBlobClient("append.log");
  await append.create();
  await append.appendBlock(Buffer.from("alpha"), 5);
  await append.appendBlock(Buffer.from("|beta"), 5);
  assert.equal((await append.downloadToBuffer()).toString("utf8"), "alpha|beta");

  const page = container.getPageBlobClient("page.bin");
  await page.create(1024);
  const pageBytes = Buffer.alloc(512, 0x5a);
  await page.uploadPages(pageBytes, 0, pageBytes.length);
  const pageRanges = await page.getPageRanges();
  assert.deepEqual(pageRanges.pageRange, [{ offset: 0, count: 511 }]);
  assert.deepEqual(await page.downloadToBuffer(0, 512), pageBytes);

  const lease = block.getBlobLeaseClient();
  const acquired = await lease.acquireLease(-1);
  assert.ok(acquired.leaseId);
  await lease.releaseLease();

  const sas = generateBlobSASQueryParameters(
    {
      containerName,
      blobName: block.name,
      permissions: BlobSASPermissions.parse("r"),
      startsOn: new Date(Date.now() - 60_000),
      expiresOn: new Date(Date.now() + 10 * 60_000),
    },
    credential,
  ).toString();
  const sasBlob = new BlobClient(`${block.url}?${sas}`, undefined, options);
  assert.deepEqual(await sasBlob.downloadToBuffer(), content);

  console.log(
    `@azure/storage-blob ${sdkPackageVersion} compatibility passed for block, staged, append, page, snapshot, lease, list, tags, ranges, and SAS operations.`,
  );
} finally {
  if (created)
    await container.delete().catch(() => {});
}

function requiredEnvironment(name) {
  const value = process.env[name];
  if (!value)
    throw new Error(`Missing required environment variable ${name}.`);
  return value;
}
