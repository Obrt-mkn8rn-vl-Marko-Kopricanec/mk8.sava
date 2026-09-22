#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_lease_client.hpp>
#include <azure/storage/common/storage_credential.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace Blobs = Azure::Storage::Blobs;
namespace Models = Azure::Storage::Blobs::Models;

namespace {

constexpr auto SdkVersion = "12.18.0";

void Require(bool condition, const std::string& message)
{
  if (!condition)
  {
    throw std::runtime_error(message);
  }
}

std::string RequiredEnvironment(const char* name)
{
  auto value = std::getenv(name);
  if (value == nullptr || *value == '\0')
  {
    throw std::runtime_error(std::string("missing required environment variable ") + name);
  }
  return value;
}

std::string RandomHex(std::size_t byteCount)
{
  std::random_device source;
  std::ostringstream output;
  output << std::hex << std::setfill('0');
  for (std::size_t index = 0; index < byteCount; ++index)
  {
    output << std::setw(2) << static_cast<unsigned int>(source() & 0xffU);
  }
  return output.str();
}

std::vector<std::uint8_t> Download(const Blobs::BlobClient& client)
{
  auto response = client.Download();
  return response.Value.BodyStream->ReadToEnd();
}

std::vector<std::uint8_t> DownloadRange(
    const Blobs::BlobClient& client,
    std::int64_t offset,
    std::int64_t length)
{
  Blobs::DownloadBlobOptions options;
  options.Range = Azure::Core::Http::HttpRange{offset, length};
  auto response = client.Download(options);
  return response.Value.BodyStream->ReadToEnd();
}

} // namespace

int main()
{
  const auto endpoint = RequiredEnvironment("MK8_SAVA_BLOB_ENDPOINT");
  const auto accountName = RequiredEnvironment("MK8_SAVA_ACCOUNT_NAME");
  const auto accountKey = RequiredEnvironment("MK8_SAVA_ACCOUNT_KEY");
  auto credential
      = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(accountName, accountKey);
  Blobs::BlobServiceClient service(endpoint, credential);
  const auto containerName = "cpp-" + RandomHex(16);
  auto container = service.GetBlobContainerClient(containerName);
  bool created = false;

  try
  {
    Blobs::CreateBlobContainerOptions createOptions;
    createOptions.Metadata = {{"runtime", "cpp"}};
    container.Create(createOptions);
    created = true;

    Blobs::ListBlobContainersOptions containerListOptions;
    containerListOptions.Prefix = containerName;
    containerListOptions.Include = Models::ListBlobContainersIncludeFlags::Metadata;
    std::size_t containerCount = 0;
    for (auto page = service.ListBlobContainers(containerListOptions); page.HasPage();
         page.MoveToNextPage())
    {
      for (const auto& item : page.BlobContainers)
      {
        Require(item.Name == containerName, "unexpected container name");
        Require(item.Details.Metadata.at("runtime") == "cpp", "container metadata mismatch");
        ++containerCount;
      }
    }
    Require(containerCount == 1, "expected exactly one container");

    std::vector<std::uint8_t> content(256 * 1024 + 53);
    for (std::size_t index = 0; index < content.size(); ++index)
    {
      content[index] = static_cast<std::uint8_t>((index * 43 + index / 277) % 251);
    }

    auto block = container.GetBlockBlobClient("folder/sdk-roundtrip.bin");
    Blobs::UploadBlockBlobFromOptions uploadOptions;
    uploadOptions.HttpHeaders.ContentType = "application/x-mk8-cpp";
    uploadOptions.HttpHeaders.CacheControl = "private,max-age=31";
    uploadOptions.Metadata = {{"owner", "cpp-sdk"}};
    uploadOptions.Tags = {{"runtime", "cpp"}, {"purpose", "compatibility"}};
    block.UploadFrom(content.data(), content.size(), uploadOptions);
    Require(Download(block) == content, "block download mismatch");
    const auto expectedRange = std::vector<std::uint8_t>(
        content.begin() + 12'345,
        content.begin() + 20'122);
    Require(DownloadRange(block, 12'345, 7'777) == expectedRange, "range download mismatch");

    const auto properties = block.GetProperties().Value;
    Require(properties.BlobSize == static_cast<std::int64_t>(content.size()), "content length mismatch");
    Require(properties.HttpHeaders.ContentType == "application/x-mk8-cpp", "content type mismatch");
    Require(properties.HttpHeaders.CacheControl == "private,max-age=31", "cache control mismatch");
    Require(properties.Metadata.at("owner") == "cpp-sdk", "blob metadata mismatch");
    Require(block.GetTags().Value.at("runtime") == "cpp", "blob tag mismatch");

    Blobs::ListBlobsOptions blobListOptions;
    blobListOptions.Prefix = "folder/";
    blobListOptions.Include
        = Models::ListBlobsIncludeFlags::Metadata | Models::ListBlobsIncludeFlags::Tags;
    std::size_t blobCount = 0;
    for (auto page = container.ListBlobs(blobListOptions); page.HasPage(); page.MoveToNextPage())
    {
      for (const auto& item : page.Blobs)
      {
        Require(item.Name == "folder/sdk-roundtrip.bin", "listed blob name mismatch");
        Require(item.Details.Metadata.at("owner") == "cpp-sdk", "listed metadata mismatch");
        Require(item.Details.Tags.at("runtime") == "cpp", "listed tag mismatch");
        ++blobCount;
      }
    }
    Require(blobCount == 1, "expected exactly one listed blob");

    Blobs::CreateBlobSnapshotOptions snapshotOptions;
    snapshotOptions.Metadata = {{"owner", "snapshot"}};
    auto snapshotResult = block.CreateSnapshot(snapshotOptions).Value;
    auto snapshot = block.WithSnapshot(snapshotResult.Snapshot);
    Require(Download(snapshot) == content, "snapshot download mismatch");
    Require(
        snapshot.GetProperties().Value.Metadata.at("owner") == "snapshot",
        "snapshot metadata mismatch");

    auto staged = container.GetBlockBlobClient("staged.bin");
    const std::vector<std::string> blockIds = {
        "YmxvY2stMDAwMQ==",
        "YmxvY2stMDAwMg==",
    };
    const std::vector<std::uint8_t> first = {'f', 'i', 'r', 's', 't', '-', 'b', 'l', 'o', 'c', 'k', '|'};
    const std::vector<std::uint8_t> second = {'s', 'e', 'c', 'o', 'n', 'd', '-', 'b', 'l', 'o', 'c', 'k'};
    Azure::Core::IO::MemoryBodyStream firstStream(first.data(), first.size());
    staged.StageBlock(blockIds[0], firstStream);
    Azure::Core::IO::MemoryBodyStream secondStream(second.data(), second.size());
    staged.StageBlock(blockIds[1], secondStream);
    Blobs::CommitBlockListOptions commitOptions;
    commitOptions.Metadata = {{"route", "staged"}};
    commitOptions.Tags = {{"runtime", "cpp"}};
    staged.CommitBlockList(blockIds, commitOptions);
    auto expectedStaged = first;
    expectedStaged.insert(expectedStaged.end(), second.begin(), second.end());
    Require(Download(staged) == expectedStaged, "staged block download mismatch");
    auto blockList = staged.GetBlockList().Value;
    Require(blockList.CommittedBlocks.size() == blockIds.size(), "committed block count mismatch");
    for (std::size_t index = 0; index < blockIds.size(); ++index)
    {
      Require(blockList.CommittedBlocks[index].Name == blockIds[index], "committed block ID mismatch");
    }
    Require(blockList.UncommittedBlocks.empty(), "unexpected uncommitted blocks");

    auto append = container.GetAppendBlobClient("append.log");
    append.Create();
    const std::vector<std::uint8_t> alpha = {'a', 'l', 'p', 'h', 'a'};
    const std::vector<std::uint8_t> beta = {'|', 'b', 'e', 't', 'a'};
    Azure::Core::IO::MemoryBodyStream alphaStream(alpha.data(), alpha.size());
    append.AppendBlock(alphaStream);
    Azure::Core::IO::MemoryBodyStream betaStream(beta.data(), beta.size());
    append.AppendBlock(betaStream);
    auto expectedAppend = alpha;
    expectedAppend.insert(expectedAppend.end(), beta.begin(), beta.end());
    Require(Download(append) == expectedAppend, "append blob download mismatch");

    auto pageBlob = container.GetPageBlobClient("page.bin");
    pageBlob.Create(1024);
    std::vector<std::uint8_t> pageBytes(512, 0x5a);
    Azure::Core::IO::MemoryBodyStream pageStream(pageBytes.data(), pageBytes.size());
    pageBlob.UploadPages(0, pageStream);
    std::size_t rangeCount = 0;
    for (auto page = pageBlob.GetPageRanges(); page.HasPage(); page.MoveToNextPage())
    {
      for (const auto& range : page.PageRanges)
      {
        Require(range.Offset == 0, "page range start mismatch");
        Require(range.Length.HasValue() && range.Length.Value() == 512, "page range length mismatch");
        ++rangeCount;
      }
    }
    Require(rangeCount == 1, "expected exactly one page range");
    Require(DownloadRange(pageBlob, 0, 512) == pageBytes, "page blob download mismatch");

    Blobs::BlobLeaseClient lease(block, Blobs::BlobLeaseClient::CreateUniqueLeaseId());
    auto leaseResult = lease.Acquire(Blobs::BlobLeaseClient::InfiniteLeaseDuration).Value;
    Require(!leaseResult.LeaseId.empty(), "lease ID is missing");
    lease.Release();

    Azure::Storage::Sas::BlobSasBuilder sasBuilder;
    sasBuilder.Protocol = Azure::Storage::Sas::SasProtocol::HttpsAndHttp;
    sasBuilder.StartsOn = std::chrono::system_clock::now() - std::chrono::minutes(1);
    sasBuilder.ExpiresOn = std::chrono::system_clock::now() + std::chrono::minutes(10);
    sasBuilder.BlobContainerName = containerName;
    sasBuilder.BlobName = "folder/sdk-roundtrip.bin";
    sasBuilder.Resource = Azure::Storage::Sas::BlobSasResource::Blob;
    sasBuilder.SetPermissions(Azure::Storage::Sas::BlobSasPermissions::Read);
    const auto sasToken = sasBuilder.GenerateSasToken(
        Azure::Storage::StorageSharedKeyCredential(accountName, accountKey));
    Blobs::BlobClient sasBlob(block.GetUrl() + sasToken);
    Require(Download(sasBlob) == content, "SAS download mismatch");

    std::cout << "azure-storage-blobs-cpp " << SdkVersion
              << " compatibility passed for block, staged, append, page, snapshot, lease, list, "
                 "tags, ranges, and SAS operations.\n";
  }
  catch (...)
  {
    if (created)
    {
      try
      {
        container.Delete();
      }
      catch (...)
      {
      }
    }
    throw;
  }

  if (created)
  {
    container.Delete();
  }
  return 0;
}
