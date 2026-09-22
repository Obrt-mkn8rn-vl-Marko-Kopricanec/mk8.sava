package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

const sdkVersion = "v1.8.1"

func main() {
	ctx := context.Background()
	endpoint := requiredEnvironment("MK8_SAVA_BLOB_ENDPOINT")
	accountName := requiredEnvironment("MK8_SAVA_ACCOUNT_NAME")
	accountKey := requiredEnvironment("MK8_SAVA_ACCOUNT_KEY")
	credential := must(azblob.NewSharedKeyCredential(accountName, accountKey))
	clientOptions := &azblob.ClientOptions{ClientOptions: noRetryOptions()}
	client := must(azblob.NewClientWithSharedKeyCredential(endpoint, credential, clientOptions))
	containerName := "go-" + randomHex(16)
	created := false
	defer func() {
		if created {
			_, _ = client.DeleteContainer(ctx, containerName, nil)
		}
	}()

	must(client.CreateContainer(ctx, containerName, &azblob.CreateContainerOptions{
		Metadata: map[string]*string{"runtime": to.Ptr("go")},
	}))
	created = true

	containers := client.NewListContainersPager(&azblob.ListContainersOptions{
		Prefix:  to.Ptr(containerName),
		Include: azblob.ListContainersInclude{Metadata: true},
	})
	containerCount := 0
	for containers.More() {
		page := must(containers.NextPage(ctx))
		for _, item := range page.ContainerItems {
			require(dereference(item.Name) == containerName, "unexpected container name")
			require(findMetadata(item.Metadata, "runtime") == "go", "container metadata mismatch")
			containerCount++
		}
	}
	require(containerCount == 1, "expected exactly one container")

	content := make([]byte, 256*1024+43)
	for index := range content {
		content[index] = byte((index*37 + index/269) % 251)
	}
	containerClient := client.ServiceClient().NewContainerClient(containerName)
	blockClient := containerClient.NewBlockBlobClient("folder/sdk-roundtrip.bin")
	must(blockClient.UploadBuffer(ctx, content, &blockblob.UploadBufferOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType:  to.Ptr("application/x-mk8-go"),
			BlobCacheControl: to.Ptr("private,max-age=23"),
		},
		Metadata: map[string]*string{"owner": to.Ptr("go-sdk")},
		Tags:     map[string]string{"runtime": "go", "purpose": "compatibility"},
	}))
	require(bytes.Equal(download(must(blockClient.DownloadStream(ctx, nil))), content), "block download mismatch")
	rangeResponse := must(blockClient.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: 12_345, Count: 7_777},
	}))
	require(bytes.Equal(download(rangeResponse), content[12_345:20_122]), "range download mismatch")

	properties := must(blockClient.GetProperties(ctx, nil))
	require(dereference(properties.ContentLength) == int64(len(content)), "content length mismatch")
	require(dereference(properties.ContentType) == "application/x-mk8-go", "content type mismatch")
	require(dereference(properties.CacheControl) == "private,max-age=23", "cache control mismatch")
	require(findMetadata(properties.Metadata, "owner") == "go-sdk", "blob metadata mismatch")
	tags := must(blockClient.GetTags(ctx, nil))
	require(findTag(tags.BlobTagSet, "runtime") == "go", "blob tag mismatch")

	blobs := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:  to.Ptr("folder/"),
		Include: container.ListBlobsInclude{Metadata: true, Tags: true},
	})
	blobCount := 0
	for blobs.More() {
		page := must(blobs.NextPage(ctx))
		for _, item := range page.Segment.BlobItems {
			require(dereference(item.Name) == "folder/sdk-roundtrip.bin", "listed blob name mismatch")
			require(findMetadata(item.Metadata, "owner") == "go-sdk", "listed metadata mismatch")
			require(findTag(item.BlobTags.BlobTagSet, "runtime") == "go", "listed tag mismatch")
			blobCount++
		}
	}
	require(blobCount == 1, "expected exactly one listed blob")

	snapshot := must(blockClient.CreateSnapshot(ctx, &blob.CreateSnapshotOptions{
		Metadata: map[string]*string{"owner": to.Ptr("snapshot")},
	}))
	snapshotClient := must(blockClient.WithSnapshot(dereference(snapshot.Snapshot)))
	require(bytes.Equal(download(must(snapshotClient.DownloadStream(ctx, nil))), content), "snapshot download mismatch")
	snapshotProperties := must(snapshotClient.GetProperties(ctx, nil))
	require(findMetadata(snapshotProperties.Metadata, "owner") == "snapshot", "snapshot metadata mismatch")

	stagedClient := containerClient.NewBlockBlobClient("staged.bin")
	blockIDs := []string{
		base64.StdEncoding.EncodeToString([]byte("block-0001")),
		base64.StdEncoding.EncodeToString([]byte("block-0002")),
	}
	first := []byte("first-block|")
	second := []byte("second-block")
	must(stagedClient.StageBlock(ctx, blockIDs[0], streaming.NopCloser(bytes.NewReader(first)), nil))
	must(stagedClient.StageBlock(ctx, blockIDs[1], streaming.NopCloser(bytes.NewReader(second)), nil))
	must(stagedClient.CommitBlockList(ctx, blockIDs, &blockblob.CommitBlockListOptions{
		Metadata: map[string]*string{"route": to.Ptr("staged")},
		Tags:     map[string]string{"runtime": "go"},
	}))
	require(
		bytes.Equal(download(must(stagedClient.DownloadStream(ctx, nil))), append(first, second...)),
		"staged block download mismatch",
	)
	blockList := must(stagedClient.GetBlockList(ctx, blockblob.BlockListTypeCommitted, nil))
	require(len(blockList.CommittedBlocks) == len(blockIDs), "committed block count mismatch")
	for index, item := range blockList.CommittedBlocks {
		require(dereference(item.Name) == blockIDs[index], "committed block ID mismatch")
	}
	require(len(blockList.UncommittedBlocks) == 0, "unexpected uncommitted blocks")

	appendClient := containerClient.NewAppendBlobClient("append.log")
	must(appendClient.Create(ctx, nil))
	must(appendClient.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader([]byte("alpha"))), nil))
	must(appendClient.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader([]byte("|beta"))), nil))
	require(
		bytes.Equal(download(must(appendClient.DownloadStream(ctx, nil))), []byte("alpha|beta")),
		"append blob download mismatch",
	)

	pageClient := containerClient.NewPageBlobClient("page.bin")
	must(pageClient.Create(ctx, 1024, nil))
	pageBytes := bytes.Repeat([]byte{0x5a}, 512)
	must(pageClient.UploadPages(
		ctx,
		streaming.NopCloser(bytes.NewReader(pageBytes)),
		blob.HTTPRange{Offset: 0, Count: int64(len(pageBytes))},
		nil,
	))
	pageRanges := pageClient.NewGetPageRangesPager(nil)
	rangeCount := 0
	for pageRanges.More() {
		page := must(pageRanges.NextPage(ctx))
		for _, allocated := range page.PageRange {
			require(dereference(allocated.Start) == 0, "page range start mismatch")
			require(dereference(allocated.End) == 511, "page range end mismatch")
			rangeCount++
		}
	}
	require(rangeCount == 1, "expected exactly one page range")
	pageDownload := must(pageClient.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: 0, Count: 512},
	}))
	require(bytes.Equal(download(pageDownload), pageBytes), "page blob download mismatch")

	leaseClient := must(lease.NewBlobClient(blockClient, nil))
	leaseResponse := must(leaseClient.AcquireLease(ctx, -1, nil))
	require(leaseResponse.LeaseID != nil, "lease ID is missing")
	must(leaseClient.ReleaseLease(ctx, nil))

	start := time.Now().UTC().Add(-time.Minute)
	sasURL := must(blockClient.GetSASURL(
		sas.BlobPermissions{Read: true},
		time.Now().UTC().Add(10*time.Minute),
		&blob.GetSASURLOptions{StartTime: &start},
	))
	sasClient := must(blob.NewClientWithNoCredential(
		sasURL,
		&blob.ClientOptions{ClientOptions: noRetryOptions()},
	))
	require(bytes.Equal(download(must(sasClient.DownloadStream(ctx, nil))), content), "SAS download mismatch")

	fmt.Printf(
		"azblob %s compatibility passed for block, staged, append, page, snapshot, lease, list, tags, ranges, and SAS operations.\n",
		sdkVersion,
	)
}

func noRetryOptions() azcore.ClientOptions {
	return azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}}
}

func download(response blob.DownloadStreamResponse) []byte {
	defer response.Body.Close()
	return must(io.ReadAll(response.Body))
}

func findTag(tags []*blob.Tags, key string) string {
	for _, tag := range tags {
		if dereference(tag.Key) == key {
			return dereference(tag.Value)
		}
	}
	return ""
}

func findMetadata(metadata map[string]*string, key string) string {
	for name, value := range metadata {
		if strings.EqualFold(name, key) {
			return dereference(value)
		}
	}
	return ""
}

func randomHex(byteCount int) string {
	value := make([]byte, byteCount)
	must(rand.Read(value))
	return hex.EncodeToString(value)
}

func requiredEnvironment(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic("missing required environment variable " + name)
	}
	return value
}

func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func dereference[T any](value *T) T {
	if value == nil {
		panic("unexpected nil SDK response field")
	}
	return *value
}

func require(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
