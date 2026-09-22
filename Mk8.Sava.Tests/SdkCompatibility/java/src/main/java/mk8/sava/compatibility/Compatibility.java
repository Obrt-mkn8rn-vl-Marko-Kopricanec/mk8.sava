package mk8.sava.compatibility;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerListDetails;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockList;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.models.ListBlobContainersOptions;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.PageRange;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.specialized.PageBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class Compatibility {
    private static final String SDK_VERSION = "12.35.1";

    private Compatibility() {
    }

    public static void main(String[] args) {
        String endpoint = requiredEnvironment("MK8_SAVA_BLOB_ENDPOINT");
        String accountName = requiredEnvironment("MK8_SAVA_ACCOUNT_NAME");
        String accountKey = requiredEnvironment("MK8_SAVA_ACCOUNT_KEY");
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobServiceClient service = new BlobServiceClientBuilder()
            .endpoint(endpoint)
            .credential(credential)
            .buildClient();
        String containerName = "java-" + UUID.randomUUID().toString().replace("-", "");
        BlobContainerClient container = service.getBlobContainerClient(containerName);
        boolean created = false;

        try {
            container.createWithResponse(Map.of("runtime", "java"), null, null, Context.NONE);
            created = true;

            List<BlobContainerItem> containers = new ArrayList<>();
            service.listBlobContainers(
                    new ListBlobContainersOptions()
                        .setPrefix(containerName)
                        .setDetails(new BlobContainerListDetails().setRetrieveMetadata(true)),
                    null)
                .forEach(containers::add);
            require(containers.size() == 1, "expected exactly one container");
            require(containerName.equals(containers.getFirst().getName()), "unexpected container name");
            require("java".equals(containers.getFirst().getMetadata().get("runtime")), "container metadata mismatch");

            byte[] content = new byte[256 * 1024 + 47];
            for (int index = 0; index < content.length; index++) {
                content[index] = (byte) ((index * 41 + index / 271) % 251);
            }

            BlobClient blob = container.getBlobClient("folder/sdk-roundtrip.bin");
            BlockBlobClient block = blob.getBlockBlobClient();
            blob.uploadWithResponse(
                new BlobParallelUploadOptions(BinaryData.fromBytes(content))
                    .setHeaders(new BlobHttpHeaders()
                        .setContentType("application/x-mk8-java")
                        .setCacheControl("private,max-age=29"))
                    .setMetadata(Map.of("owner", "java-sdk"))
                    .setTags(Map.of("runtime", "java", "purpose", "compatibility")),
                null,
                Context.NONE);
            require(Arrays.equals(block.downloadContent().toBytes(), content), "block download mismatch");
            byte[] expectedRange = Arrays.copyOfRange(content, 12_345, 20_122);
            byte[] actualRange = downloadRange(block, 12_345, 7_777);
            require(Arrays.equals(actualRange, expectedRange), "range download mismatch");

            var properties = block.getProperties();
            require(properties.getBlobSize() == content.length, "content length mismatch");
            require("application/x-mk8-java".equals(properties.getContentType()), "content type mismatch");
            require("private,max-age=29".equals(properties.getCacheControl()), "cache control mismatch");
            require("java-sdk".equals(properties.getMetadata().get("owner")), "blob metadata mismatch");
            require("java".equals(block.getTags().get("runtime")), "blob tag mismatch");

            List<BlobItem> blobs = new ArrayList<>();
            container.listBlobs(
                    new ListBlobsOptions()
                        .setPrefix("folder/")
                        .setDetails(new BlobListDetails().setRetrieveMetadata(true).setRetrieveTags(true)),
                    null)
                .forEach(blobs::add);
            require(blobs.size() == 1, "expected exactly one listed blob");
            BlobItem listed = blobs.getFirst();
            require("folder/sdk-roundtrip.bin".equals(listed.getName()), "listed blob name mismatch");
            require("java-sdk".equals(listed.getMetadata().get("owner")), "listed metadata mismatch");
            require("java".equals(listed.getTags().get("runtime")), "listed tag mismatch");

            BlobClientBase snapshot = blob.createSnapshotWithResponse(
                Map.of("owner", "snapshot"),
                null,
                null,
                Context.NONE).getValue();
            require(Arrays.equals(snapshot.downloadContent().toBytes(), content), "snapshot download mismatch");
            require("snapshot".equals(snapshot.getProperties().getMetadata().get("owner")), "snapshot metadata mismatch");

            BlockBlobClient staged = container.getBlobClient("staged.bin").getBlockBlobClient();
            List<String> blockIds = List.of(
                Base64.getEncoder().encodeToString("block-0001".getBytes(StandardCharsets.UTF_8)),
                Base64.getEncoder().encodeToString("block-0002".getBytes(StandardCharsets.UTF_8)));
            byte[] first = "first-block|".getBytes(StandardCharsets.UTF_8);
            byte[] second = "second-block".getBytes(StandardCharsets.UTF_8);
            staged.stageBlock(blockIds.get(0), BinaryData.fromBytes(first));
            staged.stageBlock(blockIds.get(1), BinaryData.fromBytes(second));
            staged.commitBlockListWithResponse(
                new BlockBlobCommitBlockListOptions(blockIds)
                    .setMetadata(Map.of("route", "staged"))
                    .setTags(Map.of("runtime", "java")),
                null,
                Context.NONE);
            byte[] expectedStaged = new byte[first.length + second.length];
            System.arraycopy(first, 0, expectedStaged, 0, first.length);
            System.arraycopy(second, 0, expectedStaged, first.length, second.length);
            require(Arrays.equals(staged.downloadContent().toBytes(), expectedStaged), "staged block download mismatch");
            BlockList blockList = staged.listBlocks(BlockListType.COMMITTED);
            require(blockList.getCommittedBlocks().size() == blockIds.size(), "committed block count mismatch");
            for (int index = 0; index < blockIds.size(); index++) {
                Block committed = blockList.getCommittedBlocks().get(index);
                require(blockIds.get(index).equals(committed.getName()), "committed block ID mismatch");
            }
            require(blockList.getUncommittedBlocks().isEmpty(), "unexpected uncommitted blocks");

            AppendBlobClient append = container.getBlobClient("append.log").getAppendBlobClient();
            append.create();
            byte[] alpha = "alpha".getBytes(StandardCharsets.UTF_8);
            byte[] beta = "|beta".getBytes(StandardCharsets.UTF_8);
            append.appendBlock(new ByteArrayInputStream(alpha), alpha.length);
            append.appendBlock(new ByteArrayInputStream(beta), beta.length);
            require(
                Arrays.equals(append.downloadContent().toBytes(), "alpha|beta".getBytes(StandardCharsets.UTF_8)),
                "append blob download mismatch");

            PageBlobClient page = container.getBlobClient("page.bin").getPageBlobClient();
            page.create(1024);
            byte[] pageBytes = new byte[512];
            Arrays.fill(pageBytes, (byte) 0x5a);
            page.uploadPages(new PageRange().setStart(0).setEnd(511), new ByteArrayInputStream(pageBytes));
            require(
                Arrays.equals(downloadRange(page, 0, 512), pageBytes),
                "page blob download mismatch");

            BlobLeaseClient lease = new BlobLeaseClientBuilder().blobClient(blob).buildClient();
            String leaseId = lease.acquireLease(-1);
            require(leaseId != null && !leaseId.isBlank(), "lease ID is missing");
            lease.releaseLease();

            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            String sas = blob.generateSas(new BlobServiceSasSignatureValues(
                    now.plusMinutes(10),
                    new BlobSasPermission().setReadPermission(true))
                .setStartTime(now.minusMinutes(1)));
            BlobClient sasBlob = new BlobClientBuilder()
                .endpoint(blob.getBlobUrl() + "?" + sas)
                .buildClient();
            require(Arrays.equals(sasBlob.downloadContent().toBytes(), content), "SAS download mismatch");

            System.out.printf(
                "azure-storage-blob %s compatibility passed for block, staged, append, page, snapshot, lease, list, tags, ranges, and SAS operations.%n",
                SDK_VERSION);
        } finally {
            if (created) {
                try {
                    container.delete();
                } catch (RuntimeException ignored) {
                    // Preserve the compatibility result if best-effort cleanup fails.
                }
            }
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing required environment variable " + name);
        }

        return value;
    }

    private static byte[] downloadRange(BlobClientBase client, long offset, long count) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        client.downloadStreamWithResponse(
            output,
            new BlobRange(offset, count),
            null,
            null,
            false,
            null,
            Context.NONE);
        return output.toByteArray();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
