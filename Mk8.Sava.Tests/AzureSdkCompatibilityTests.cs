using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.IdentityModel.Tokens;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Data.Sqlite;
using Mk8.Sava.Protocol;
using Mk8.Sava.Storage;

namespace Mk8.Sava.Tests;

public sealed class AzureSdkCompatibilityTests(SavaWebApplicationFactory factory)
    : IClassFixture<SavaWebApplicationFactory>
{
    private static readonly JsonSerializerOptions IndentedWebJsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };
    private static readonly string[] RestoredTexts = ["second", "third"];
    private static readonly string[] DirectoryNames = ["alpha", "alpha/beta"];
    private static readonly string[] FileNames = ["alpha/beta/file.txt", "alpha/root.txt", "zeta.txt"];
    private static readonly string[] RootFileNames = ["zeta.txt"];
    private static readonly string[] AnalyticsLogTypes = ["read", "write", "delete"];
    private static readonly int[] FirstParquetIds = [1, 2];
    private static readonly string?[] FirstParquetNames = ["one", "two"];
    private static readonly bool[] FirstParquetEnabled = [true, false];
    private static readonly double[] FirstParquetScores = [1.25D, 2.5D];
    private static readonly DateTime[] FirstParquetObserved =
    [
        new DateTime(2026, 9, 20, 10, 15, 0, DateTimeKind.Utc),
        new DateTime(2026, 9, 21, 11, 30, 0, DateTimeKind.Utc)
    ];
    private static readonly int[] SecondParquetIds = [3];
    private static readonly string?[] SecondParquetNames = ["three"];
    private static readonly bool[] SecondParquetEnabled = [true];
    private static readonly double[] SecondParquetScores = [3.75D];
    private static readonly DateTime[] SecondParquetObserved =
        [new DateTime(2026, 9, 22, 12, 45, 0, DateTimeKind.Utc)];

    private static string SignUserDelegationSas(string base64Key, string stringToSign) =>
        Convert.ToBase64String(HMACSHA256.HashData(
            Convert.FromBase64String(base64Key), Encoding.UTF8.GetBytes(stringToSign)));

    [Fact]
    public async Task PathStyleServiceEndpointAcceptsTerminalAccountSeparator()
    {
        var endpoint = new Uri($"http://localhost/{SavaWebApplicationFactory.AccountName}");
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(factory.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        };
        var service = new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey),
            options);
        var containerName = $"path-style-{Guid.NewGuid():N}";
        var container = service.GetBlobContainerClient(containerName);

        await container.CreateAsync();
        var names = new List<string>();
        await foreach (var item in service.GetBlobContainersAsync(prefix: containerName))
            names.Add(item.Name);

        Assert.Equal([containerName], names);
        await container.DeleteAsync();
    }

    [Fact]
    public async Task BlockBlobRoundTripPreservesBytesPropertiesMetadataTagsRangesAndListings()
    {
        var service = CreateClient(factory);
        var containerName = $"sdk-{Guid.NewGuid():N}";
        var container = service.GetBlobContainerClient(containerName);
        var created = await container.CreateAsync(PublicAccessType.None, new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = "tests" });
        Assert.Equal(201, created.GetRawResponse().Status);

        var content = Enumerable.Range(0, 256 * 1024)
            .Select(index => (byte)((index / 1024) % 7))
            .ToArray();
        var blob = container.GetBlobClient("folder/blob.bin");
        var upload = await blob.UploadAsync(BinaryData.FromBytes(content), new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders
            {
                ContentType = "application/x-mk8-test",
                CacheControl = "private,max-age=30"
            },
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["owner"] = "compatibility" },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["kind"] = "fixture" }
        });
        Assert.Equal(201, upload.GetRawResponse().Status);

        var downloaded = await blob.DownloadContentAsync();
        Assert.Equal(content, downloaded.Value.Content.ToArray());
        Assert.Equal("application/x-mk8-test", downloaded.Value.Details.ContentType);
        Assert.Equal("compatibility", downloaded.Value.Details.Metadata["owner"]);

        var range = new HttpRange(12_345, 7_777);
        var ranged = await blob.DownloadStreamingAsync(new BlobDownloadOptions { Range = range });
        using var rangeBuffer = new MemoryStream();
        await ranged.Value.Content.CopyToAsync(rangeBuffer);
        Assert.Equal(content.AsSpan(12_345, 7_777).ToArray(), rangeBuffer.ToArray());

        var properties = await blob.GetPropertiesAsync();
        Assert.Equal(content.LongLength, properties.Value.ContentLength);
        Assert.Equal("fixture", (await blob.GetTagsAsync()).Value.Tags["kind"]);

        var names = new List<string>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Traits = BlobTraits.Metadata | BlobTraits.Tags,
            Prefix = "folder/"
        }))
            names.Add(item.Name);
        Assert.Contains("folder/blob.bin", names, StringComparer.Ordinal);
    }

    [Fact]
    public async Task MetadataNamesValuesDuplicatesAndSizeMatchAzureLimitsAtomically()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"metadata-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("metadata.bin");
        await blob.UploadAsync(BinaryData.FromString("metadata payload"), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["Original"] = "preserved",
                ["_leading"] = "allowed"
            }
        });

        foreach (var invalidName in new[] { "1starts_with_digit", "contains-hyphen", "contains.dot" })
        {
            var invalid = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { [invalidName] = "value" }));
            Assert.Equal(400, invalid.Status);
            Assert.Equal("InvalidMetadata", invalid.ErrorCode);
            var unchanged = (await blob.GetPropertiesAsync()).Value.Metadata;
            Assert.Equal("preserved", unchanged["Original"]);
            Assert.Equal("allowed", unchanged["_leading"]);
        }

        var nonAsciiName = Assert.Throws<AzureStorageException>(() =>
            ProtocolParsing.ReadMetadata(new HeaderDictionary
            {
                ["x-ms-meta-nön_ascii"] = "value"
            }));
        Assert.Equal("InvalidMetadata", nonAsciiName.ErrorCode);

        var maximumValue = new string('m', 8 * 1024 - 1);
        await blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["k"] = maximumValue });
        var maximumProperties = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(maximumValue, maximumProperties.Metadata["k"]);

        var tooLarge = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["k"] = maximumValue + "x" }));
        Assert.Equal(400, tooLarge.Status);
        Assert.Equal("MetadataTooLarge", tooLarge.ErrorCode);
        var afterTooLarge = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(maximumProperties.ETag, afterTooLarge.ETag);
        Assert.Equal(maximumValue, afterTooLarge.Metadata["k"]);

        await VerifyBlobMetadataRawRequestsAsync(blob);
        await VerifyContainerMetadataRawRequestsAsync(container, blob, maximumProperties.ETag);
        await VerifyInvalidMetadataPublicationsAsync(service, container, blob);
    }

    private async Task VerifyBlobMetadataRawRequestsAsync(BlobClient blob)
    {
        var metadataUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=metadata");
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using (var duplicateRequest = new HttpRequestMessage(HttpMethod.Put, metadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            duplicateRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            duplicateRequest.Headers.TryAddWithoutValidation("x-ms-meta-Duplicate", ["one", "two"]);
            using var response = await transport.SendAsync(duplicateRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidMetadata", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var nonAsciiRequest = new HttpRequestMessage(HttpMethod.Put, metadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            nonAsciiRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            nonAsciiRequest.Headers.TryAddWithoutValidation("x-ms-meta-Value", "olé");
            using var response = await transport.SendAsync(nonAsciiRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidMetadata", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var nonemptyMetadata = new HttpRequestMessage(HttpMethod.Put, metadataUri)
        {
            Content = new ByteArrayContent([1])
        })
        {
            nonemptyMetadata.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            nonemptyMetadata.Headers.TryAddWithoutValidation("x-ms-meta-k", "must-not-publish");
            using var response = await transport.SendAsync(nonemptyMetadata).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var propertiesUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=properties");
        using (var nonemptyProperties = new HttpRequestMessage(HttpMethod.Put, propertiesUri)
        {
            Content = new ByteArrayContent([1])
        })
        {
            nonemptyProperties.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(nonemptyProperties).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private async Task VerifyContainerMetadataRawRequestsAsync(
        BlobContainerClient container, BlobClient blob, ETag expectedBlobETag)
    {
        var containerMetadataUri = AppendQuery(
            container.Uri,
            "restype=container&comp=metadata");
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using (var nonemptyContainerMetadata = new HttpRequestMessage(HttpMethod.Put, containerMetadataUri)
        {
            Content = new ByteArrayContent([1])
        })
        {
            nonemptyContainerMetadata.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            nonemptyContainerMetadata.Headers.TryAddWithoutValidation("x-ms-meta-k", "must-not-publish");
            AddSharedKeyLiteAuthorization(nonemptyContainerMetadata);
            using var response = await transport.SendAsync(nonemptyContainerMetadata).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var validContainerMetadata = new HttpRequestMessage(HttpMethod.Put, containerMetadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            validContainerMetadata.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            validContainerMetadata.Headers.TryAddWithoutValidation("x-ms-meta-fromowner", "allowed");
            AddSharedKeyLiteAuthorization(validContainerMetadata);
            using var response = await transport.SendAsync(validContainerMetadata).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        Assert.Equal(expectedBlobETag, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag);
        Assert.False((await container.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata.ContainsKey("k"));
        Assert.Equal("allowed", (await container.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["fromowner"]);
    }

    private static async Task VerifyInvalidMetadataPublicationsAsync(
        BlobServiceClient service, BlobContainerClient container, BlobClient blob)
    {
        var invalidContainer = service.GetBlobContainerClient($"metadata-invalid-{Guid.NewGuid():N}");
        var invalidContainerCreate = await Assert.ThrowsAsync<RequestFailedException>(() =>
            invalidContainer.CreateAsync(
                PublicAccessType.None,
                new Dictionary<string, string>(StringComparer.Ordinal) { ["invalid-name"] = "value" })).ConfigureAwait(false);
        Assert.Equal("InvalidMetadata", invalidContainerCreate.ErrorCode);
        Assert.False((await invalidContainer.ExistsAsync().ConfigureAwait(false)).Value);

        var invalidUpload = container.GetBlobClient("invalid-upload.bin");
        var invalidBlobCreate = await Assert.ThrowsAsync<RequestFailedException>(() =>
            invalidUpload.UploadAsync(BinaryData.FromString("must not publish"), new BlobUploadOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["invalid-name"] = "value" }
            })).ConfigureAwait(false);
        Assert.Equal("InvalidMetadata", invalidBlobCreate.ErrorCode);
        Assert.False((await invalidUpload.ExistsAsync().ConfigureAwait(false)).Value);

        var invalidSnapshot = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.CreateSnapshotAsync(
                new Dictionary<string, string>(StringComparer.Ordinal) { ["invalid-name"] = "value" })).ConfigureAwait(false);
        Assert.Equal("InvalidMetadata", invalidSnapshot.ErrorCode);
        var snapshots = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions { States = BlobStates.Snapshots })
                           .ConfigureAwait(false))
        {
            if (item.Snapshot is not null)
                snapshots.Add(item);
        }
        Assert.Empty(snapshots);
    }

    [Fact]
    public async Task MetadataAndContainerPropertyResponsesExposeOnlyAzureOperationHeaders()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"response-shape-{Guid.NewGuid():N}");
        var created = await container.CreateAsync(
            PublicAccessType.None,
            new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = "container" });
        Assert.False(created.GetRawResponse().Headers.TryGetValue("x-ms-meta-scope", out _));

        var properties = await container.GetPropertiesAsync();
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseStatus.Unlocked, properties.Value.LeaseStatus);
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Available, properties.Value.LeaseState);
        Assert.False(properties.Value.HasImmutabilityPolicy);
        Assert.False(properties.Value.HasLegalHold);
        Assert.Equal("container", properties.Value.Metadata["scope"]);

        var accessPolicy = await container.GetAccessPolicyAsync();
        Assert.False(accessPolicy.GetRawResponse().Headers.TryGetValue("x-ms-meta-scope", out _));
        Assert.False(accessPolicy.GetRawResponse().Headers.TryGetValue("x-ms-lease-state", out _));

        var setContainerMetadata = await container.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = "updated" });
        Assert.False(setContainerMetadata.GetRawResponse().Headers.TryGetValue("x-ms-meta-scope", out _));
        Assert.False(setContainerMetadata.GetRawResponse().Headers.TryGetValue("x-ms-lease-state", out _));

        var containerMetadataUri = CreateReadContainerMetadataUri(container);

        var blob = container.GetBlobClient("metadata.bin");
        await blob.UploadAsync(
            BinaryData.FromString("metadata response payload"),
            new BlobUploadOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["owner"] = "mk8" },
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["class"] = "response" },
                HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-response-test" }
            });
        var setBlobMetadata = await blob.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["owner"] = "sava" });
        var setBlobHeaders = setBlobMetadata.GetRawResponse().Headers;
        Assert.True(setBlobHeaders.TryGetValue("x-ms-request-server-encrypted", out var encrypted));
        Assert.Equal("true", encrypted);
        Assert.False(setBlobHeaders.TryGetValue("x-ms-meta-owner", out _));
        Assert.False(setBlobHeaders.TryGetValue("x-ms-blob-type", out _));
        Assert.False(setBlobHeaders.TryGetValue("x-ms-lease-status", out _));

        var blobMetadataUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=metadata");
        await VerifyMetadataReadHeadersAsync(containerMetadataUri, blobMetadataUri);
    }

    private static Uri CreateReadContainerMetadataUri(BlobContainerClient container)
    {
        var accountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(5),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountSas.SetPermissions(AccountSasPermissions.Read);
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        return AppendQuery(container.Uri,
            $"restype=container&comp=metadata&{accountSas.ToSasQueryParameters(credential)}");
    }

    private async Task VerifyMetadataReadHeadersAsync(Uri containerMetadataUri, Uri blobMetadataUri)
    {
        using var transport = new HttpClient(factory.Server.CreateHandler());
        foreach (var (method, uri, metadataName, metadataValue) in new[]
                 {
                     (HttpMethod.Get, containerMetadataUri, "x-ms-meta-scope", "updated"),
                     (HttpMethod.Head, containerMetadataUri, "x-ms-meta-scope", "updated"),
                     (HttpMethod.Get, blobMetadataUri, "x-ms-meta-owner", "sava"),
                     (HttpMethod.Head, blobMetadataUri, "x-ms-meta-owner", "sava")
                 })
        {
            using var request = new HttpRequestMessage(method, uri);
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(metadataValue, response.Headers.GetValues(metadataName).Single());
            Assert.False(response.Headers.Contains("x-ms-blob-type"));
            Assert.False(response.Headers.Contains("x-ms-lease-status"));
            Assert.False(response.Headers.Contains("x-ms-lease-state"));
            Assert.False(response.Headers.Contains("x-ms-server-encrypted"));
            Assert.False(response.Headers.Contains("x-ms-tag-count"));
            Assert.Empty(await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false));
        }
    }

    [Fact]
    public async Task EscapedSegmentedAndRootBlobNamesRemainExactAndHonorAzureLimits()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"names-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var expected = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["a/b"] = "ordinary separator",
            ["a//b"] = "empty segment",
            ["a!file"] = "exclamation mark",
            ["%41"] = "literal escape",
            ["A"] = "decoded character",
            ["reserved ?#% ü.bin"] = "reserved and unicode"
        };
        Assert.Equal($"/{container.Name}/a%21file", container.GetBlobClient("a!file").Uri.AbsolutePath);

        foreach (var pair in expected)
            await container.GetBlobClient(pair.Key).UploadAsync(BinaryData.FromString(pair.Value));

        foreach (var pair in expected)
        {
            var content = await container.GetBlobClient(pair.Key).DownloadContentAsync();
            Assert.Equal(pair.Value, content.Value.Content.ToString());
        }

        await PutAndReadDirectNamedBlobAsync(service, container, "/a", "/a", "leading separator");
        await PutAndReadDirectNamedBlobAsync(service, container, "tail/", "tail/", "trailing separator");
        expected.Add("/a", "leading separator");
        expected.Add("tail/", "trailing separator");

        var listed = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var item in container.GetBlobsAsync())
            listed.Add(item.Name);
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), listed.Order(StringComparer.Ordinal));

        await VerifyImplicitRootBlobAsync(service);
        await VerifyBlobNameLimitsAsync(container);
    }

    private async Task PutAndReadDirectNamedBlobAsync(
        BlobServiceClient service, BlobContainerClient container,
        string blobName, string escapedBlobPath, string value)
    {
        var sas = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = blobName,
            Resource = "b",
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10)
        };
        sas.SetPermissions(BlobSasPermissions.Create | BlobSasPermissions.Write | BlobSasPermissions.Read);
        var uri = new Uri(
            $"{service.Uri.AbsoluteUri.TrimEnd('/')}/{container.Name}/{escapedBlobPath}?" +
            sas.ToSasQueryParameters(new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey)));

        using var transport = new HttpClient(factory.Server.CreateHandler());
        using var put = new HttpRequestMessage(HttpMethod.Put, uri)
        {
            Content = new ByteArrayContent(Encoding.UTF8.GetBytes(value))
        };
        put.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        put.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        using var putResponse = await transport.SendAsync(put).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Created, putResponse.StatusCode);

        using var getResponse = await transport.GetAsync(uri).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(value, await getResponse.Content.ReadAsStringAsync().ConfigureAwait(false));
    }

    private async Task VerifyImplicitRootBlobAsync(BlobServiceClient service)
    {
        var root = service.GetBlobContainerClient("$root");
        await root.CreateIfNotExistsAsync().ConfigureAwait(false);
        var rootName = $"implicit-root-{Guid.NewGuid():N}.bin";
        var rootUri = new Uri(service.Uri, Uri.EscapeDataString(rootName));
        using (var transport = new HttpClient(factory.Server.CreateHandler()) { BaseAddress = service.Uri })
        {
            var implicitRoot = new BlobClient(
                rootUri,
                new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
                new BlobClientOptions
                {
                    Transport = new HttpClientTransport(transport),
                    Retry = { MaxRetries = 0 }
                });
            await implicitRoot.UploadAsync(BinaryData.FromString("root payload")).ConfigureAwait(false);
        }
        Assert.Equal(
            "root payload",
            (await root.GetBlobClient(rootName).DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static async Task VerifyBlobNameLimitsAsync(BlobContainerClient container)
    {
        var maximumUnicodeName = new string('é', 1024);
        await container.GetBlobClient(maximumUnicodeName).UploadAsync(BinaryData.FromString("maximum name"))
            .ConfigureAwait(false);
        Assert.Equal(
            "maximum name",
            (await container.GetBlobClient(maximumUnicodeName).DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var oversizedName = new string('é', 1025);
        var oversized = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient(oversizedName).UploadAsync(BinaryData.FromString("must not publish")))
            .ConfigureAwait(false);
        Assert.Equal(400, oversized.Status);
        Assert.Equal("InvalidResourceName", oversized.ErrorCode);

        var maximumSegments = string.Join('/', Enumerable.Repeat("s", 254));
        await container.GetBlobClient(maximumSegments).UploadAsync(BinaryData.FromString("maximum segments"))
            .ConfigureAwait(false);
        Assert.Equal(
            "maximum segments",
            (await container.GetBlobClient(maximumSegments).DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var tooManySegments = string.Join('/', Enumerable.Repeat("s", 255));
        var tooMany = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient(tooManySegments).UploadAsync(BinaryData.FromString("must not publish")))
            .ConfigureAwait(false);
        Assert.Equal(400, tooMany.Status);
        Assert.Equal("InvalidResourceName", tooMany.ErrorCode);
    }

    [Fact]
    public async Task IndexedTagQueriesAreBoundedScopedAndTransactionallyVerified()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        var rejectedBackupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-tag-index-{Guid.NewGuid():N}");
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var token = Guid.NewGuid().ToString("N");
            var firstContainer = service.GetBlobContainerClient($"tag-a-{Guid.NewGuid():N}");
            var secondContainer = service.GetBlobContainerClient($"tag-b-{Guid.NewGuid():N}");
            await firstContainer.CreateAsync();
            await secondContainer.CreateAsync();

            await UploadTaggedAsync(firstContainer, "a", token, "010");
            await UploadTaggedAsync(firstContainer, "b", token, "050");
            await UploadTaggedAsync(firstContainer, "c", token, "150");
            await UploadTaggedAsync(secondContainer, "d", token, "075");
            await UploadTaggedAsync(secondContainer, "e", "another-project", "020");

            var expression = $"\"project\" = '{token}' AND rank >= '010' AND rank < '100'";
            await AssertIndexedTagQueryPagesAsync(service, firstContainer, secondContainer, token, expression);
            await AssertTagUpdateAndValidationAsync(service, firstContainer, secondContainer, token, expression);

            var directConnectionString = new SqliteConnectionStringBuilder
            {
                DataSource = Path.Combine(application.DataPath, "metadata.db"),
                ForeignKeys = true
            }.ToString();
            {
                var connection = new SqliteConnection(directConnectionString);
                await using (connection.ConfigureAwait(false))
                {
                    await connection.OpenAsync();
                    await using var corruptIndex = connection.CreateCommand();
                    corruptIndex.CommandText = "DELETE FROM blob_tags WHERE tag_key = 'project';";
                    Assert.True(await corruptIndex.ExecuteNonQueryAsync() > 0);
                }
            }
            var backup = application.Services.GetRequiredService<StorageBackupService>();
            var mismatch = await Assert.ThrowsAsync<InvalidDataException>(() =>
                backup.CreateAsync(rejectedBackupPath, CancellationToken.None));
            Assert.Contains("blob-tag index", mismatch.Message, StringComparison.Ordinal);
        }
        finally
        {
            await application.DisposeAsync();
            if (Directory.Exists(rejectedBackupPath))
                Directory.Delete(rejectedBackupPath, recursive: true);
        }
    }

    private static async Task UploadTaggedAsync(
        BlobContainerClient container, string name, string project, string rank)
    {
        await container.GetBlobClient(name).UploadAsync(
            BinaryData.FromString(name),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["project"] = project,
                    ["rank"] = rank,
                    ["unselected"] = "not returned"
                }
            }).ConfigureAwait(false);
    }

    private static async Task AssertIndexedTagQueryPagesAsync(
        BlobServiceClient service,
        BlobContainerClient firstContainer,
        BlobContainerClient secondContainer,
        string token,
        string expression)
    {
        var matches = new List<TaggedBlobItem>();
        var markers = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var page in service.FindBlobsByTagsAsync(expression).AsPages(pageSizeHint: 1)
                           .ConfigureAwait(false))
        {
            Assert.Single(page.Values);
            matches.Add(page.Values[0]);
            if (!string.IsNullOrEmpty(page.ContinuationToken))
            {
                Assert.StartsWith("mk8t1.", page.ContinuationToken, StringComparison.Ordinal);
                Assert.True(markers.Add(page.ContinuationToken));
            }
        }
        Assert.Equal(
            [
                $"{firstContainer.Name}/a",
                $"{firstContainer.Name}/b",
                $"{secondContainer.Name}/d"
            ],
            matches.Select(item => $"{item.BlobContainerName}/{item.BlobName}"), StringComparer.Ordinal);
        Assert.Equal(2, markers.Count);
        Assert.All(matches, item =>
        {
            Assert.Equal(2, item.Tags.Count);
            Assert.Equal(token, item.Tags["project"]);
            Assert.False(item.Tags.ContainsKey("unselected"));
        });

        var scoped = new List<string>();
        await foreach (var item in firstContainer.FindBlobsByTagsAsync($"\"project\" = '{token}'")
                           .ConfigureAwait(false))
            scoped.Add(item.BlobName);
        Assert.Equal(["a", "b", "c"], scoped);
    }

    private static async Task AssertTagUpdateAndValidationAsync(
        BlobServiceClient service,
        BlobContainerClient firstContainer,
        BlobContainerClient secondContainer,
        string token,
        string expression)
    {
        await firstContainer.GetBlobClient("b").SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["project"] = "changed",
            ["rank"] = "050"
        }).ConfigureAwait(false);
        var afterUpdate = new List<string>();
        await foreach (var item in service.FindBlobsByTagsAsync(expression).ConfigureAwait(false))
            afterUpdate.Add($"{item.BlobContainerName}/{item.BlobName}");
        Assert.Equal([$"{firstContainer.Name}/a", $"{secondContainer.Name}/d"], afterUpdate);

        var invalidExpression = await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await foreach (var _ in service.FindBlobsByTagsAsync(
                               $"\"project\" > '{token}' AND \"project\" >= '{token}'").ConfigureAwait(false))
            {
            }
        }).ConfigureAwait(false);
        Assert.Equal(400, invalidExpression.Status);

        var invalidTag = await Assert.ThrowsAsync<RequestFailedException>(() =>
            firstContainer.GetBlobClient("a").SetTagsAsync(
                new Dictionary<string, string>(StringComparer.Ordinal) { ["bad?"] = "value" })).ConfigureAwait(false);
        Assert.Equal(400, invalidTag.Status);
        Assert.Equal("InvalidTag", invalidTag.ErrorCode);
    }

    [Fact]
    public async Task BlobTagConditionsHonorSqlGrammarPermissionsAndSourceSemantics()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"tag-conditions-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlobClient("source.txt");
        await source.UploadAsync(
            BinaryData.FromString("conditioned"),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["Status"] = "Done",
                    ["Priority"] = "07",
                    ["special key"] = "yes"
                }
            });

        var currentEtag = await AssertTagReadWriteConditionsAsync(source);
        await AssertTagSqlGrammarAsync(source);
        await AssertTagCopySourceConditionsAsync(container, source);
        await AssertTagSasAndHeaderConditionsAsync(factory, source, currentEtag);
    }

    private static async Task<ETag> AssertTagReadWriteConditionsAsync(BlobClient source)
    {
        var successfulRead = await source.DownloadContentAsync(new BlobDownloadOptions
        {
            Conditions = new BlobRequestConditions
            {
                TagConditions = "(Status <> 'Pending' AND Priority >= '05') OR \"special key\" = 'no'"
            }
        }).ConfigureAwait(false);
        Assert.Equal("conditioned", successfulRead.Value.Content.ToString());

        var metadata = await source.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["condition"] = "passed" },
            new BlobRequestConditions
            {
                TagConditions = "Status = 'Pending' AND Priority >= '05' OR \"special key\" = 'yes'"
            }).ConfigureAwait(false);
        Assert.Equal(200, metadata.GetRawResponse().Status);

        var currentEtag = (await source.GetPropertiesAsync().ConfigureAwait(false)).Value.ETag;
        var conditionedTags = await source.GetTagsAsync(
            new BlobRequestConditions { IfMatch = currentEtag }).ConfigureAwait(false);
        Assert.Equal("Done", conditionedTags.Value.Tags["Status"]);
        var rejectedTagRead = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.GetTagsAsync(new BlobRequestConditions
            {
                IfMatch = new ETag("\"not-the-current-etag\"")
            })).ConfigureAwait(false);
        Assert.Equal(412, rejectedTagRead.Status);
        Assert.Equal("ConditionNotMet", rejectedTagRead.ErrorCode);
        await source.SetTagsAsync(
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["Status"] = "Done",
                ["Priority"] = "07",
                ["special key"] = "yes"
            },
            new BlobRequestConditions { IfMatch = currentEtag }).ConfigureAwait(false);
        var rejectedTagWrite = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.SetTagsAsync(
                new Dictionary<string, string>(StringComparer.Ordinal) { ["Status"] = "rejected" },
                new BlobRequestConditions { IfMatch = new ETag("\"not-the-current-etag\"") }))
            .ConfigureAwait(false);
        Assert.Equal(412, rejectedTagWrite.Status);
        Assert.Equal("ConditionNotMet", rejectedTagWrite.ErrorCode);
        return currentEtag;
    }

    private static async Task AssertTagSqlGrammarAsync(BlobClient source)
    {
        var falseCondition = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions
                {
                    TagConditions = "(Status = 'Pending' OR Priority < '05') AND \"special key\" = 'yes'"
                }
            })).ConfigureAwait(false);
        Assert.Equal(412, falseCondition.Status);
        Assert.Equal("ConditionNotMet", falseCondition.ErrorCode);

        var missingTagIsNotUnequal = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions { TagConditions = "Missing <> 'value'" }
            })).ConfigureAwait(false);
        Assert.Equal(412, missingTagIsNotUnequal.Status);

        var invalidCondition = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions { TagConditions = "Status = 'Done' OR OR Priority = '07'" }
            })).ConfigureAwait(false);
        Assert.Equal(400, invalidCondition.Status);
        Assert.Equal("InvalidHeaderValue", invalidCondition.ErrorCode);

        var excessiveCondition = string.Join(
            " OR ",
            Enumerable.Range(0, 12).Select(index => $"Status = 'value-{index}'"));
        var excessiveOperations = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions { TagConditions = excessiveCondition }
            })).ConfigureAwait(false);
        Assert.Equal(400, excessiveOperations.Status);
        Assert.Equal("InvalidHeaderValue", excessiveOperations.ErrorCode);
    }

    private static async Task AssertTagCopySourceConditionsAsync(
        BlobContainerClient container, BlobClient source)
    {
        var destination = container.GetBlobClient("destination.txt");
        var copy = await destination.StartCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions
            {
                SourceConditions = new BlobRequestConditions
                {
                    TagConditions = "Status = 'Done' AND Priority <= '07'"
                }
            }).ConfigureAwait(false);
        Assert.Equal(202, copy.GetRawResponse().Status);

        var rejectedCopy = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("rejected-copy.txt").StartCopyFromUriAsync(
                source.Uri,
                new BlobCopyFromUriOptions
                {
                    SourceConditions = new BlobRequestConditions { TagConditions = "Status = 'Pending'" }
                })).ConfigureAwait(false);
        Assert.Equal(412, rejectedCopy.Status);
        Assert.Equal("SourceConditionNotMet", rejectedCopy.ErrorCode);
    }

    private static async Task AssertTagSasAndHeaderConditionsAsync(
        SavaWebApplicationFactory application, BlobClient source, ETag currentEtag)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        var readOnlyUri = source.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var unauthorizedRequest = new HttpRequestMessage(HttpMethod.Get, readOnlyUri);
        unauthorizedRequest.Headers.Add("x-ms-version", "2023-11-03");
        unauthorizedRequest.Headers.Add("x-ms-if-tags", "Status = 'Done'");
        using var unauthorizedResponse = await transport.SendAsync(unauthorizedRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Forbidden, unauthorizedResponse.StatusCode);
        Assert.Equal(
            "AuthorizationPermissionMismatch",
            unauthorizedResponse.Headers.GetValues("x-ms-error-code").Single());

        var taggedReadUri = source.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Tag,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var authorizedRequest = new HttpRequestMessage(HttpMethod.Get, taggedReadUri);
        authorizedRequest.Headers.Add("x-ms-version", "2023-11-03");
        authorizedRequest.Headers.Add("x-ms-if-tags", "Status = 'Done'");
        using var authorizedResponse = await transport.SendAsync(authorizedRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, authorizedResponse.StatusCode);

        var oldVersionTagsUri = AppendQuery(
            source.GenerateSasUri(
                BlobSasPermissions.Read | BlobSasPermissions.Tag,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=tags");
        using var oldVersionCondition = new HttpRequestMessage(HttpMethod.Get, oldVersionTagsUri);
        oldVersionCondition.Headers.Add("x-ms-version", "2023-11-03");
        oldVersionCondition.Headers.Add("x-ms-blob-if-match", currentEtag.ToString());
        using var oldVersionConditionResponse = await transport.SendAsync(oldVersionCondition).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, oldVersionConditionResponse.StatusCode);
        Assert.Equal(
            "FeatureVersionMismatch",
            oldVersionConditionResponse.Headers.GetValues("x-ms-error-code").Single());

        var badChecksumTagsUri = AppendQuery(
            source.GenerateSasUri(BlobSasPermissions.Tag, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=tags");
        using var badChecksumTags = new HttpRequestMessage(HttpMethod.Put, badChecksumTagsUri)
        {
            Content = new ByteArrayContent(
                "<Tags><TagSet><Tag><Key>Status</Key><Value>corrupt</Value></Tag></TagSet></Tags>"u8.ToArray())
        };
        badChecksumTags.Headers.Add("x-ms-version", "2026-06-06");
        badChecksumTags.Headers.Add("x-ms-content-crc64", Convert.ToBase64String(new byte[8]));
        badChecksumTags.Content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/xml; charset=utf-8");
        using var badChecksumTagsResponse = await transport.SendAsync(badChecksumTags).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, badChecksumTagsResponse.StatusCode);
        Assert.Equal("Crc64Mismatch", badChecksumTagsResponse.Headers.GetValues("x-ms-error-code").Single());
        Assert.Equal("Done", (await source.GetTagsAsync().ConfigureAwait(false)).Value.Tags["Status"]);
    }

    [Fact]
    public async Task PagedFlatVersionSnapshotAndHierarchyListingsNeverSkipOrRepeatEntries()
    {
        var application = new SavaWebApplicationFactory();
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var containerName = $"pages-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            var versioned = await CreateVersionedListingBlobAsync(metadata, container);
            await AssertVersionSnapshotListingPagesAsync(application, container, metadata, versioned);

            await AssertHierarchyListingPagesAsync(container);

            await AssertLiveContinuationPagesAsync(container);

            await AssertContainerListingPagesAsync(service);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task<(BlobContainerClient Container,
        BlobContainerClient DeletedContainer)> CreateHistoricalListingFixtureAsync(
        SavaWebApplicationFactory application)
    {
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient($"listing-{Guid.NewGuid():N}");
        await container.CreateAsync(
            PublicAccessType.Blob,
            new Dictionary<string, string>(StringComparer.Ordinal) { ["purpose"] = "listing" })
            .ConfigureAwait(false);
        await container.GetBlobClient("folder/a b.txt").UploadAsync(
            BinaryData.FromString("listing payload"),
            new BlobUploadOptions
            {
                HttpHeaders = new BlobHttpHeaders
                {
                    ContentType = "text/plain",
                    ContentEncoding = "gzip"
                }
            }).ConfigureAwait(false);
        await container.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);

        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var serviceProperties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None).ConfigureAwait(false);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            serviceProperties with
            {
                ContainerSoftDeleteEnabled = true,
                ContainerSoftDeleteRetentionDays = 7
            },
            CancellationToken.None).ConfigureAwait(false);
        var deletedContainer = service.GetBlobContainerClient($"deleted-{Guid.NewGuid():N}");
        await deletedContainer.CreateAsync().ConfigureAwait(false);
        await deletedContainer.DeleteAsync().ConfigureAwait(false);
        return (container, deletedContainer);
    }

    private static async Task<HttpResponseMessage> SendHistoricalContainerListAsync(
        HttpClient transport,
        Uri serviceSasUri,
        string version,
        string query)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, AppendQuery(serviceSasUri, $"comp=list&{query}"));
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task<HttpResponseMessage> SendHistoricalBlobListAsync(
        HttpClient transport,
        Uri containerSasUri,
        string version,
        string query)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            AppendQuery(containerSasUri, $"restype=container&comp=list&{query}"));
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertModernContainerListingAsync(
        HttpClient transport,
        Uri serviceSasUri,
        string containerName,
        string deletedContainerName)
    {
        using var modernContainers = await SendHistoricalContainerListAsync(
            transport, serviceSasUri, "2019-12-12", "include=metadata,deleted&maxresults=6000")
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, modernContainers.StatusCode);
        var document = System.Xml.Linq.XDocument.Parse(
            await modernContainers.Content.ReadAsStringAsync().ConfigureAwait(false));
        Assert.Equal($"http://{SavaWebApplicationFactory.AccountName}.localhost/",
            document.Root?.Attribute("ServiceEndpoint")?.Value);
        Assert.Null(document.Root?.Attribute("AccountName"));
        Assert.Equal("5000", document.Root?.Element("MaxResults")?.Value);

        var active = document.Descendants("Container")
            .Single(item => string.Equals(item.Element("Name")?.Value, containerName, StringComparison.Ordinal));
        Assert.Null(active.Element("Url"));
        Assert.Null(active.Element("Deleted"));
        Assert.Equal("blob", active.Element("Properties")?.Element("PublicAccess")?.Value);
        Assert.Equal("locked", active.Element("Properties")?.Element("LeaseStatus")?.Value);
        Assert.Equal("leased", active.Element("Properties")?.Element("LeaseState")?.Value);
        Assert.Equal("fixed", active.Element("Properties")?.Element("LeaseDuration")?.Value);
        Assert.Equal("false", active.Element("Properties")?.Element("HasImmutabilityPolicy")?.Value);
        Assert.Equal("false", active.Element("Properties")?.Element("HasLegalHold")?.Value);
        Assert.Equal("listing", active.Element("Metadata")?.Element("purpose")?.Value);

        var deleted = document.Descendants("Container")
            .Single(item => string.Equals(item.Element("Name")?.Value, deletedContainerName, StringComparison.Ordinal));
        Assert.Equal("true", deleted.Element("Deleted")?.Value);
        Assert.NotEmpty(Assert.IsType<string>(deleted.Element("Version")?.Value));
        Assert.Null(deleted.Element("Properties")?.Element("Deleted"));
        Assert.NotNull(deleted.Element("Properties")?.Element("DeletedTime"));
        Assert.NotNull(deleted.Element("Properties")?.Element("RemainingRetentionDays"));
        Assert.Null(deleted.Element("Properties")?.Element("LeaseStatus"));
    }

    private static async Task AssertLegacyContainerListingsAsync(
        HttpClient transport,
        Uri serviceSasUri,
        string containerName)
    {
        using (var legacyContainers = await SendHistoricalContainerListAsync(
            transport, serviceSasUri, "2012-02-12", "prefix=listing-&maxresults=1").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, legacyContainers.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(
                await legacyContainers.Content.ReadAsStringAsync().ConfigureAwait(false));
            Assert.Equal($"http://{SavaWebApplicationFactory.AccountName}.localhost/",
                document.Root?.Attribute("AccountName")?.Value);
            Assert.Null(document.Root?.Attribute("ServiceEndpoint"));
            var listed = Assert.Single(document.Descendants("Container"));
            Assert.Equal($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}",
                listed.Element("Url")?.Value);
            Assert.NotNull(listed.Element("Properties")?.Element("Last-Modified"));
            Assert.NotNull(listed.Element("Properties")?.Element("LeaseState"));
            Assert.Null(listed.Element("Properties")?.Element("PublicAccess"));
        }

        using var oldestContainers = await SendHistoricalContainerListAsync(
            transport, serviceSasUri, "2008-10-27", "prefix=listing-&maxresults=1").ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, oldestContainers.StatusCode);
        var oldestDocument = System.Xml.Linq.XDocument.Parse(
            await oldestContainers.Content.ReadAsStringAsync().ConfigureAwait(false));
        var properties = Assert.Single(oldestDocument.Descendants("Container")).Element("Properties");
        Assert.NotNull(properties?.Element("LastModified"));
        Assert.Null(properties?.Element("Last-Modified"));
        Assert.DoesNotContain('"', Assert.IsType<string>(properties?.Element("Etag")?.Value));
        Assert.Null(properties?.Element("LeaseStatus"));
    }

    private static async Task AssertHistoricalBlobListingsAsync(
        HttpClient transport,
        Uri containerSasUri,
        string containerName)
    {
        using (var oldestBlobs = await SendHistoricalBlobListAsync(
            transport, containerSasUri, "2008-10-27", "prefix=folder/&maxresults=6001").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, oldestBlobs.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(
                await oldestBlobs.Content.ReadAsStringAsync().ConfigureAwait(false));
            Assert.Equal($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}",
                document.Root?.Attribute("ContainerName")?.Value);
            Assert.Null(document.Root?.Attribute("ServiceEndpoint"));
            Assert.Equal("5000", document.Root?.Element("MaxResults")?.Value);
            var listed = Assert.Single(document.Descendants("Blob"));
            Assert.Equal($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/folder/a%20b.txt",
                listed.Element("Url")?.Value);
            Assert.NotNull(listed.Element("LastModified"));
            Assert.Equal("15", listed.Element("Size")?.Value);
            Assert.Equal("text/plain", listed.Element("ContentType")?.Value);
            Assert.Equal("gzip", listed.Element("ContentEncoding")?.Value);
            Assert.Null(listed.Element("Properties"));
            Assert.Null(listed.Element("BlobType"));
        }

        using (var transitionalBlobs = await SendHistoricalBlobListAsync(
            transport, containerSasUri, "2009-09-19", "prefix=folder/").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, transitionalBlobs.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(
                await transitionalBlobs.Content.ReadAsStringAsync().ConfigureAwait(false));
            var listed = Assert.Single(document.Descendants("Blob"));
            Assert.NotNull(listed.Element("Url"));
            Assert.NotNull(listed.Element("Properties")?.Element("Last-Modified"));
            Assert.Equal("BlockBlob", listed.Element("Properties")?.Element("BlobType")?.Value);
            Assert.Equal("unlocked", listed.Element("Properties")?.Element("LeaseStatus")?.Value);
        }

        using var modernBlobs = await SendHistoricalBlobListAsync(
            transport, containerSasUri, "2013-08-15", "prefix=folder/").ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, modernBlobs.StatusCode);
        var modernDocument = System.Xml.Linq.XDocument.Parse(
            await modernBlobs.Content.ReadAsStringAsync().ConfigureAwait(false));
        Assert.Equal($"http://{SavaWebApplicationFactory.AccountName}.localhost/",
            modernDocument.Root?.Attribute("ServiceEndpoint")?.Value);
        Assert.Equal(containerName, modernDocument.Root?.Attribute("ContainerName")?.Value);
        Assert.Null(Assert.Single(modernDocument.Descendants("Blob")).Element("Url"));
    }

    private static async Task AssertInvalidHistoricalListingsAsync(
        HttpClient transport,
        Uri serviceSasUri,
        Uri containerSasUri)
    {
        using (var oldDeletedContainers = await SendHistoricalContainerListAsync(
            transport, serviceSasUri, "2019-07-07", "include=deleted").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldDeletedContainers.StatusCode);
            Assert.Equal("FeatureVersionMismatch",
                oldDeletedContainers.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldCopyListing = await SendHistoricalBlobListAsync(
            transport, containerSasUri, "2011-08-18", "include=copy").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldCopyListing.StatusCode);
            await AssertVersionedErrorAsync(oldCopyListing, "FeatureVersionMismatch").ConfigureAwait(false);
        }
        using var unknownListing = await SendHistoricalBlobListAsync(
            transport, containerSasUri, "2023-11-03", "include=unknown").ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, unknownListing.StatusCode);
        Assert.Equal("InvalidQueryParameterValue", unknownListing.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task<BlobClient> CreateVersionedListingBlobAsync(
        MetadataStore metadata, BlobContainerClient container)
    {
        var properties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None).ConfigureAwait(false);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            properties with { VersioningEnabled = true },
            CancellationToken.None).ConfigureAwait(false);

        var versioned = container.GetBlobClient("paged/same-name.txt");
        await versioned.UploadAsync(BinaryData.FromString("one"), overwrite: true).ConfigureAwait(false);
        await versioned.UploadAsync(BinaryData.FromString("two"), overwrite: true).ConfigureAwait(false);
        await versioned.UploadAsync(BinaryData.FromString("three"), overwrite: true).ConfigureAwait(false);
        await versioned.CreateSnapshotAsync().ConfigureAwait(false);
        await versioned.CreateSnapshotAsync().ConfigureAwait(false);
        return versioned;
    }

    private static async Task AssertVersionSnapshotListingPagesAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        MetadataStore metadata,
        BlobClient versioned)
    {
        var expectedRecords = (await metadata.ListBlobsAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                includeVersions: true,
                includeSnapshots: true,
                includeDeleted: false,
                CancellationToken.None).ConfigureAwait(false))
            .Where(item => string.Equals(item.Name, versioned.Name, StringComparison.Ordinal))
            .ToArray();
        var listed = new List<BlobItem>();
        var flatTokens = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var page in container
                           .GetBlobsAsync(new GetBlobsOptions
                           {
                               States = BlobStates.Version | BlobStates.Snapshots,
                               Prefix = versioned.Name
                           })
                           .AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            Assert.Single(page.Values);
            listed.Add(page.Values[0]);
            if (!string.IsNullOrEmpty(page.ContinuationToken))
            {
                Assert.StartsWith("mk8s2.", page.ContinuationToken, StringComparison.Ordinal);
                Assert.True(flatTokens.Add(page.ContinuationToken));
            }
        }
        Assert.Equal(expectedRecords.Length, listed.Count);
        Assert.Equal(
            expectedRecords.Select(ListIdentity).Order(StringComparer.Ordinal),
            listed.Select(ListIdentity).Order(StringComparer.Ordinal));
        Assert.NotEmpty(flatTokens);
        var reboundMarkerUri = AppendQuery(
            container.GenerateSasUri(
                BlobContainerSasPermissions.List,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=list&include=versions%2Csnapshots&prefix=other" +
            $"&maxresults=1&marker={Uri.EscapeDataString(flatTokens.First())}");
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var response = await transport.GetAsync(reboundMarkerUri).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("InvalidQueryParameterValue", response.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task AssertLiveContinuationPagesAsync(BlobContainerClient container)
    {
        var livePrefix = $"live-{Guid.NewGuid():N}-";
        await container.GetBlobClient(livePrefix + "b").UploadAsync(BinaryData.FromString("b"))
            .ConfigureAwait(false);
        await container.GetBlobClient(livePrefix + "d").UploadAsync(BinaryData.FromString("d"))
            .ConfigureAwait(false);
        Page<BlobItem>? firstLivePage = null;
        await foreach (var page in container
                           .GetBlobsAsync(new GetBlobsOptions { Prefix = livePrefix })
                           .AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            firstLivePage = page;
            break;
        }
        Assert.NotNull(firstLivePage);
        Assert.Equal(livePrefix + "b", Assert.Single(firstLivePage!.Values).Name);
        Assert.NotNull(firstLivePage.ContinuationToken);
        await container.GetBlobClient(livePrefix + "a").UploadAsync(BinaryData.FromString("a"))
            .ConfigureAwait(false);
        await container.GetBlobClient(livePrefix + "c").UploadAsync(BinaryData.FromString("c"))
            .ConfigureAwait(false);
        var resumedLiveNames = new List<string>();
        await foreach (var page in container
                           .GetBlobsAsync(new GetBlobsOptions { Prefix = livePrefix })
                           .AsPages(firstLivePage.ContinuationToken, pageSizeHint: 1).ConfigureAwait(false))
        {
            resumedLiveNames.AddRange(page.Values.Select(item => item.Name));
        }
        Assert.Equal([livePrefix + "c", livePrefix + "d"], resumedLiveNames);
    }

    private static async Task AssertHierarchyListingPagesAsync(BlobContainerClient container)
    {
        await container.GetBlobClient("folders/a/one").UploadAsync(BinaryData.FromString("a1"))
            .ConfigureAwait(false);
        await container.GetBlobClient("folders/a/two").UploadAsync(BinaryData.FromString("a2"))
            .ConfigureAwait(false);
        await container.GetBlobClient("folders/b/one").UploadAsync(BinaryData.FromString("b1"))
            .ConfigureAwait(false);
        await container.GetBlobClient("folders/root").UploadAsync(BinaryData.FromString("root"))
            .ConfigureAwait(false);
        var hierarchy = new List<string>();
        var hierarchyTokens = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var page in container
                           .GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
                           {
                               Delimiter = "/",
                               Prefix = "folders/"
                           })
                           .AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            Assert.Single(page.Values);
            var item = page.Values[0];
            hierarchy.Add(item.IsPrefix ? $"P:{item.Prefix}" : $"B:{item.Blob.Name}");
            if (!string.IsNullOrEmpty(page.ContinuationToken))
            {
                Assert.StartsWith("mk8s2.", page.ContinuationToken, StringComparison.Ordinal);
                Assert.True(hierarchyTokens.Add(page.ContinuationToken));
            }
        }
        Assert.Equal(["P:folders/a/", "P:folders/b/", "B:folders/root"], hierarchy);

        var startedNames = new List<string>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = "folders/",
            StartFrom = "folders/b/one"
        }).ConfigureAwait(false))
        {
            startedNames.Add(item.Name);
        }
        Assert.Equal(["folders/b/one", "folders/root"], startedNames);
        var startedHierarchy = new List<string>();
        await foreach (var item in container.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
        {
            Delimiter = "/",
            Prefix = "folders/",
            StartFrom = "folders/b/"
        }).ConfigureAwait(false))
        {
            startedHierarchy.Add(item.IsPrefix ? $"P:{item.Prefix}" : $"B:{item.Blob.Name}");
        }
        Assert.Equal(["P:folders/b/", "B:folders/root"], startedHierarchy);
    }

    private static async Task AssertContainerListingPagesAsync(BlobServiceClient service)
    {
        var containerPrefix = $"listed-{Guid.NewGuid():N}-";
        var expectedContainers = Enumerable.Range(0, 3)
            .Select(index => containerPrefix + index)
            .ToArray();
        foreach (var name in expectedContainers)
            await service.GetBlobContainerClient(name).CreateAsync().ConfigureAwait(false);
        var listedContainers = new List<string>();
        var containerTokens = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var page in service
                           .GetBlobContainersAsync(prefix: containerPrefix)
                           .AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            Assert.Single(page.Values);
            listedContainers.Add(page.Values[0].Name);
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                Assert.True(containerTokens.Add(page.ContinuationToken));
        }
        Assert.Equal(expectedContainers, listedContainers, StringComparer.Ordinal);
        Assert.Equal(2, containerTokens.Count);
    }

    [Fact]
    public async Task ApacheArrowListingsHonorSchemaRangesHierarchyAndScopedPaging()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"arrow-{Guid.NewGuid():N}");
        await CreateArrowListingFixtureAsync(container);
        var firstContinuation = await AssertArrowSdkListingAsync(container);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        var listSas = container.GenerateSasUri(
            BlobContainerSasPermissions.List,
            DateTimeOffset.UtcNow.AddMinutes(5));
        var arrowUri = AppendQuery(
            listSas,
            "restype=container&comp=list&include=metadata%2Ctags&startfrom=b.txt&endbefore=d.txt&maxresults=2");
        await AssertArrowRestSchemaAsync(transport, arrowUri);
        await AssertArrowRestFailuresAsync(transport, listSas, arrowUri, firstContinuation);
    }

    private static async Task AssertArrowRestFailuresAsync(
        HttpClient transport, Uri listSas, Uri arrowUri, string firstContinuation)
    {
        var reboundUri = AppendQuery(
            listSas,
            "restype=container&comp=list&startfrom=b.txt&endbefore=c%2Ftwo.txt&maxresults=1" +
            $"&marker={Uri.EscapeDataString(firstContinuation)}");
        using (var reboundRequest = new HttpRequestMessage(HttpMethod.Get, reboundUri))
        {
            reboundRequest.Headers.Add("x-ms-version", "2026-06-06");
            reboundRequest.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(AzureResponseWriter.ArrowStreamContentType));
            using var reboundResponse = await transport.SendAsync(reboundRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, reboundResponse.StatusCode);
            Assert.Equal("InvalidQueryParameterValue", reboundResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        await AssertArrowRejectedAsync(transport, arrowUri, "2026-04-06", arrow: true,
            expectedStatus: HttpStatusCode.Conflict).ConfigureAwait(false);
        await AssertArrowRejectedAsync(transport,
            AppendQuery(listSas, "restype=container&comp=list&endbefore=d.txt"),
            "2026-06-06", arrow: false).ConfigureAwait(false);
        await AssertArrowRejectedAsync(transport,
            AppendQuery(listSas, "restype=container&comp=list&startfrom=d.txt&endbefore=b.txt"),
            "2026-06-06", arrow: true).ConfigureAwait(false);
    }

    private static async Task AssertArrowRejectedAsync(
        HttpClient transport, Uri uri, string version, bool arrow,
        HttpStatusCode expectedStatus = HttpStatusCode.BadRequest)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.Add("x-ms-version", version);
        if (arrow)
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(AzureResponseWriter.ArrowStreamContentType));
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(expectedStatus, response.StatusCode);
        Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
    }

    private static async Task AssertArrowRestSchemaAsync(HttpClient transport, Uri arrowUri)
    {
        using var arrowRequest = new HttpRequestMessage(HttpMethod.Get, arrowUri);
        arrowRequest.Headers.Add("x-ms-version", "2026-12-06");
        arrowRequest.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(AzureResponseWriter.ArrowStreamContentType));
        using var arrowResponse = await transport.SendAsync(
            arrowRequest,
            HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, arrowResponse.StatusCode);
        Assert.Equal(AzureResponseWriter.ArrowStreamContentType, arrowResponse.Content.Headers.ContentType?.MediaType);
        var responseStream = await arrowResponse.Content.ReadAsStreamAsync().ConfigureAwait(false);
        await using var responseStreamDisposal = responseStream.ConfigureAwait(false);
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(responseStream);
        Assert.Equal("2", reader.Schema.Metadata["NumberOfRecords"]);
        Assert.False(string.IsNullOrEmpty(reader.Schema.Metadata["NextMarker"]));
        Assert.False(reader.Schema["Name"].IsNullable);
        Assert.False(reader.Schema["ResourceType"].IsNullable);
        Assert.Equal(
            Apache.Arrow.Types.TimeUnit.Second,
            Assert.IsType<Apache.Arrow.Types.TimestampType>(reader.Schema["Creation-Time"].DataType).Unit);
        using var batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false);
        Assert.NotNull(batch);
        Assert.Equal(2, batch.Length);
        var names = Assert.IsType<Apache.Arrow.StringArray>(batch.Column("Name", StringComparer.Ordinal));
        Assert.Equal("b.txt", names.GetString(0));
        Assert.Equal("c/one.txt", names.GetString(1));
        var inferred = Assert.IsType<Apache.Arrow.BooleanArray>(batch.Column("AccessTierInferred", StringComparer.Ordinal));
        Assert.True(inferred.IsNull(0));
        Assert.True(inferred.GetValue(1));
        Assert.NotNull(batch.Column("Metadata", StringComparer.Ordinal));
        Assert.NotNull(batch.Column("Tags", StringComparer.Ordinal));
        Assert.Null(await reader.ReadNextRecordBatchAsync().ConfigureAwait(false));
    }

    private static async Task<string> AssertArrowSdkListingAsync(BlobContainerClient container)
    {
        var listed = new List<BlobItem>();
        var continuationTokens = new List<string>();
        await foreach (var page in container
                           .GetBlobsAsync(new GetBlobsOptions
                           {
                               ResponseFormat = StorageResponseFormat.Arrow,
                               Traits = BlobTraits.Metadata | BlobTraits.Tags,
                               StartFrom = "b.txt",
                               EndBefore = "d.txt"
                           })
                           .AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            Assert.Single(page.Values);
            listed.Add(page.Values[0]);
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                continuationTokens.Add(page.ContinuationToken);
        }
        Assert.Equal(["b.txt", "c/one.txt", "c/two.txt"], listed.Select(item => item.Name), StringComparer.Ordinal);
        Assert.Equal("arrow", listed[0].Metadata["owner"]);
        Assert.Equal("boundary", listed[0].Tags["kind"]);
        Assert.Equal("text/x-arrow-fixture", listed[0].Properties.ContentType);
        Assert.Equal(AccessTier.Cool, listed[0].Properties.AccessTier);
        Assert.False(listed[0].Properties.AccessTierInferred);
        Assert.True((await container.GetBlobClient("a.txt").GetPropertiesAsync().ConfigureAwait(false))
            .Value.AccessTierInferred);
        Assert.Equal(2, continuationTokens.Count);
        Assert.Equal(2, continuationTokens.Distinct(StringComparer.Ordinal).Count());

        var hierarchy = new List<string>();
        await foreach (var item in container.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
        {
            Delimiter = "/",
            ResponseFormat = StorageResponseFormat.Arrow,
            StartFrom = "b.txt",
            EndBefore = "d.txt"
        }).ConfigureAwait(false))
        {
            hierarchy.Add(item.IsPrefix ? $"P:{item.Prefix}" : $"B:{item.Blob.Name}");
        }
        Assert.Equal(["P:c/", "B:b.txt"], hierarchy);

        var empty = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            ResponseFormat = StorageResponseFormat.Arrow,
            StartFrom = "d.txt",
            EndBefore = "d.txt"
        }).ConfigureAwait(false))
        {
            empty.Add(item);
        }
        Assert.Empty(empty);
        return continuationTokens[0];
    }

    private static async Task CreateArrowListingFixtureAsync(BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        await container.GetBlobClient("a.txt").UploadAsync(BinaryData.FromString("a"))
            .ConfigureAwait(false);
        await container.GetBlobClient("b.txt").UploadAsync(
            BinaryData.FromString("b"),
            new BlobUploadOptions
            {
                HttpHeaders = new BlobHttpHeaders { ContentType = "text/x-arrow-fixture" },
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["owner"] = "arrow" },
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["kind"] = "boundary" },
                AccessTier = AccessTier.Cool
            }).ConfigureAwait(false);
        await container.GetBlobClient("c/one.txt").UploadAsync(BinaryData.FromString("c1"))
            .ConfigureAwait(false);
        await container.GetBlobClient("c/two.txt").UploadAsync(BinaryData.FromString("c2"))
            .ConfigureAwait(false);
        await container.GetBlobClient("d.txt").UploadAsync(BinaryData.FromString("d"))
            .ConfigureAwait(false);
    }

    [Fact]
    public async Task UncommittedBlobsAreListedPagedAndDiscardedByReplacementOrDeletion()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"uncommitted-{Guid.NewGuid():N}");
        var (prefix, pendingNames, committed) = await CreateUncommittedListingFixtureAsync(container);
        var listed = await AssertUncommittedFlatListingPagesAsync(container, prefix, pendingNames, committed);
        await AssertUncommittedHierarchyAndArrowAsync(container, prefix, listed);
        await AssertUncommittedRestXmlAsync(factory, container, prefix, pendingNames[0]);

        var replacement = container.GetBlockBlobClient(prefix + "replace.bin");
        await replacement.UploadAsync(new MemoryStream("replacement"u8.ToArray()));
        Assert.Empty((await replacement.GetBlockListAsync(BlockListTypes.Uncommitted)).Value.UncommittedBlocks);

        var deleted = container.GetBlockBlobClient(prefix + "delete.bin");
        Assert.Equal(202, (await deleted.DeleteAsync()).Status);
        var deletedError = await Assert.ThrowsAsync<RequestFailedException>(
            () => deleted.GetBlockListAsync(BlockListTypes.Uncommitted));
        Assert.Equal((int)HttpStatusCode.NotFound, deletedError.Status);

        await committed.DeleteAsync();
        var committedError = await Assert.ThrowsAsync<RequestFailedException>(
            () => committed.GetBlockListAsync(BlockListTypes.Uncommitted));
        Assert.Equal((int)HttpStatusCode.NotFound, committedError.Status);
    }

    private static async Task AssertUncommittedRestXmlAsync(
        SavaWebApplicationFactory application, BlobContainerClient container,
        string prefix, string firstPendingName)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        var rawUri = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddMinutes(5)),
            $"restype=container&comp=list&include=uncommittedblobs&prefix={Uri.EscapeDataString(prefix)}");
        using var rawRequest = new HttpRequestMessage(HttpMethod.Get, rawUri);
        rawRequest.Headers.Add("x-ms-version", "2026-06-06");
        using var rawResponse = await transport.SendAsync(rawRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, rawResponse.StatusCode);
        var document = System.Xml.Linq.XDocument.Parse(
            await rawResponse.Content.ReadAsStringAsync().ConfigureAwait(false));
        var pending = document.Descendants("Blob")
            .Single(element => string.Equals(element.Element("Name")?.Value,
                firstPendingName, StringComparison.Ordinal));
        var properties = Assert.IsType<System.Xml.Linq.XElement>(pending.Element("Properties"));
        Assert.Equal("0", properties.Element("Content-Length")?.Value);
        Assert.Equal("BlockBlob", properties.Element("BlobType")?.Value);
        Assert.Null(properties.Element("Last-Modified"));
        Assert.Null(properties.Element("Etag"));
        Assert.Null(properties.Element("Content-Type"));
        Assert.Null(pending.Element("Metadata"));
    }

    private static async Task AssertUncommittedHierarchyAndArrowAsync(
        BlobContainerClient container, string prefix, IReadOnlyList<BlobItem> listed)
    {
        var hierarchical = new List<string>();
        await foreach (var item in container.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
        {
            Delimiter = "/",
            Prefix = prefix,
            States = BlobStates.Uncommitted
        }).ConfigureAwait(false))
        {
            hierarchical.Add(item.IsPrefix ? $"P:{item.Prefix}" : $"B:{item.Blob.Name}");
        }
        Assert.Contains($"P:{prefix}folder/", hierarchical, StringComparer.Ordinal);
        Assert.DoesNotContain($"B:{prefix}folder/child.bin", hierarchical, StringComparer.Ordinal);

        var arrowNames = new List<string>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = prefix,
            States = BlobStates.Uncommitted,
            ResponseFormat = StorageResponseFormat.Arrow
        }).ConfigureAwait(false))
        {
            arrowNames.Add(item.Name);
        }
        Assert.Equal(listed.Select(item => item.Name), arrowNames, StringComparer.Ordinal);
    }

    private static async Task<List<BlobItem>> AssertUncommittedFlatListingPagesAsync(
        BlobContainerClient container, string prefix, string[] pendingNames, BlockBlobClient committed)
    {
        var ordinary = new List<string>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions { Prefix = prefix })
                           .ConfigureAwait(false))
            ordinary.Add(item.Name);
        Assert.Equal([committed.Name], ordinary);

        var listed = new List<BlobItem>();
        var markers = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var page in container
                           .GetBlobsAsync(new GetBlobsOptions
                           {
                               Prefix = prefix,
                               States = BlobStates.Uncommitted
                           })
                           .AsPages(pageSizeHint: 1).ConfigureAwait(false))
        {
            listed.Add(Assert.Single(page.Values));
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                Assert.True(markers.Add(page.ContinuationToken));
        }
        Assert.Equal(pendingNames.Append(committed.Name).Order(StringComparer.Ordinal),
            listed.Select(item => item.Name), StringComparer.Ordinal);
        Assert.Equal(listed.Count - 1, markers.Count);
        foreach (var pending in listed.Where(item => !string.Equals(item.Name, committed.Name, StringComparison.Ordinal)))
        {
            Assert.Equal(BlobType.Block, pending.Properties.BlobType);
            Assert.Equal(0, pending.Properties.ContentLength);
            Assert.Null(pending.Properties.ContentType);
        }
        Assert.Equal("committed"u8.Length,
            listed.Single(item => string.Equals(item.Name, committed.Name, StringComparison.Ordinal))
                .Properties.ContentLength);
        return listed;
    }

    private static async Task<(string Prefix, string[] PendingNames, BlockBlobClient Committed)>
        CreateUncommittedListingFixtureAsync(BlobContainerClient container)
    {
        await container.CreateAsync().ConfigureAwait(false);
        var prefix = $"pending-{Guid.NewGuid():N}/";
        var blockId = Convert.ToBase64String("uncommitted-0001"u8);
        var pendingNames = new[]
        {
            prefix + "a.bin",
            prefix + "b.bin",
            prefix + "delete.bin",
            prefix + "folder/child.bin",
            prefix + "replace.bin"
        };
        foreach (var name in pendingNames)
        {
            await container.GetBlockBlobClient(name).StageBlockAsync(
                blockId,
                new MemoryStream(Encoding.UTF8.GetBytes(name))).ConfigureAwait(false);
        }

        var committed = container.GetBlockBlobClient(prefix + "committed.bin");
        await committed.UploadAsync(new MemoryStream("committed"u8.ToArray())).ConfigureAwait(false);
        await committed.StageBlockAsync(
            Convert.ToBase64String("uncommitted-0002"u8),
            new MemoryStream("future block"u8.ToArray())).ConfigureAwait(false);
        return (prefix, pendingNames, committed);
    }

    [Fact]
    public async Task StaticWebsiteServesAnonymousIndexesFallbacksErrorsRangesAndHead()
    {
        var service = CreateClient(factory);
        var original = (await service.GetPropertiesAsync()).Value;
        var suffix = Guid.NewGuid().ToString("N");
        var indexName = $"index-{suffix}.html";
        var errorName = $"errors/404-{suffix}.html";
        var assetName = $"asset-{suffix}.txt";
        var defaultName = $"default-{suffix}.html";

        try
        {
            await ConfigureStaticWebsiteFixtureAsync(service, indexName, errorName, assetName, defaultName);

            using var web = new HttpClient(factory.Server.CreateHandler());
            var endpoint = $"http://{SavaWebApplicationFactory.AccountName}.z99.web.local";
            await AssertStaticWebsiteReadsAsync(web, endpoint, assetName);
            await AssertStaticWebsiteFallbackAndDisableAsync(service, web, endpoint, defaultName);
        }
        finally
        {
            await service.SetPropertiesAsync(original);
        }
    }

    private static async Task AssertStaticWebsiteFallbackAndDisableAsync(
        BlobServiceClient service, HttpClient web, string endpoint, string defaultName)
    {
        var configured = (await service.GetPropertiesAsync().ConfigureAwait(false)).Value;
        configured.StaticWebsite.IndexDocument = null;
        configured.StaticWebsite.DefaultIndexDocumentPath = defaultName;
        await service.SetPropertiesAsync(configured).ConfigureAwait(false);
        var roundTrip = (await service.GetPropertiesAsync().ConfigureAwait(false)).Value.StaticWebsite;
        Assert.Null(roundTrip.IndexDocument);
        Assert.Equal(defaultName, roundTrip.DefaultIndexDocumentPath);
        using (var fallback = await web.GetAsync(new Uri(endpoint + "/client/side/route", UriKind.RelativeOrAbsolute))
                   .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, fallback.StatusCode);
            Assert.Equal("single page fallback", await fallback.Content.ReadAsStringAsync().ConfigureAwait(false));
        }

        configured.StaticWebsite.Enabled = false;
        await service.SetPropertiesAsync(configured).ConfigureAwait(false);
        using var disabled = await web.GetAsync(new Uri(endpoint + "/", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.NotFound, disabled.StatusCode);
        Assert.Equal("text/html", disabled.Content.Headers.ContentType?.MediaType);
        Assert.Contains("requested content does not exist",
            await disabled.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
    }

    private static async Task AssertStaticWebsiteReadsAsync(
        HttpClient web, string endpoint, string assetName)
    {
        using (var root = await web.GetAsync(new Uri(endpoint + "/", UriKind.RelativeOrAbsolute)).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, root.StatusCode);
            Assert.Equal("root index", await root.Content.ReadAsStringAsync().ConfigureAwait(false));
            Assert.Equal("text/html", root.Content.Headers.ContentType?.MediaType);
            Assert.Equal("public, max-age=60", root.Headers.CacheControl?.ToString());
        }
        using (var folder = await web.GetAsync(new Uri(endpoint + "/folder/", UriKind.RelativeOrAbsolute))
                   .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, folder.StatusCode);
            Assert.Equal("folder index", await folder.Content.ReadAsStringAsync().ConfigureAwait(false));
        }
        using (var missing = await web.GetAsync(new Uri(endpoint + "/missing", UriKind.RelativeOrAbsolute))
                   .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
            Assert.Equal("custom not found", await missing.Content.ReadAsStringAsync().ConfigureAwait(false));
            Assert.Equal("text/html", missing.Content.Headers.ContentType?.MediaType);
        }
        using (var rangeRequest = new HttpRequestMessage(HttpMethod.Get, endpoint + "/" + assetName))
        {
            rangeRequest.Headers.Range = new RangeHeaderValue(2, 5);
            using var range = await web.SendAsync(rangeRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.PartialContent, range.StatusCode);
            Assert.Equal("2345", await range.Content.ReadAsStringAsync().ConfigureAwait(false));
            Assert.Equal("bytes 2-5/10", range.Content.Headers.ContentRange?.ToString());
        }
        using (var headRequest = new HttpRequestMessage(HttpMethod.Head, endpoint + "/" + assetName))
        using (var head = await web.SendAsync(headRequest).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, head.StatusCode);
            Assert.Equal(10, head.Content.Headers.ContentLength);
            Assert.Empty(await head.Content.ReadAsByteArrayAsync().ConfigureAwait(false));
        }
        using var postContent = new ByteArrayContent([]);
        using var post = await web.PostAsync(new Uri(endpoint + "/" + assetName, UriKind.RelativeOrAbsolute), postContent)
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);
        Assert.True(post.Content.Headers.TryGetValues("Allow", out var allowedMethods));
        Assert.Equal(["GET", "HEAD"], allowedMethods, StringComparer.Ordinal);
        Assert.Equal("text/html", post.Content.Headers.ContentType?.MediaType);
    }

    private static async Task ConfigureStaticWebsiteFixtureAsync(
        BlobServiceClient service,
        string indexName,
        string errorName,
        string assetName,
        string defaultName)
    {
        var configured = (await service.GetPropertiesAsync().ConfigureAwait(false)).Value;
        configured.StaticWebsite.Enabled = true;
        configured.StaticWebsite.IndexDocument = indexName;
        configured.StaticWebsite.DefaultIndexDocumentPath = null;
        configured.StaticWebsite.ErrorDocument404Path = errorName;
        await service.SetPropertiesAsync(configured).ConfigureAwait(false);

        var roundTrip = (await service.GetPropertiesAsync().ConfigureAwait(false)).Value.StaticWebsite;
        Assert.True(roundTrip.Enabled);
        Assert.Equal(indexName, roundTrip.IndexDocument);
        Assert.Equal(errorName, roundTrip.ErrorDocument404Path);
        Assert.Null(roundTrip.DefaultIndexDocumentPath);

        var website = service.GetBlobContainerClient("$web");
        Assert.True((await website.ExistsAsync().ConfigureAwait(false)).Value);
        await website.GetBlobClient(indexName).UploadAsync(
            BinaryData.FromString("root index"),
            new BlobUploadOptions
            {
                HttpHeaders = new BlobHttpHeaders
                {
                    ContentType = "text/html; charset=utf-8",
                    CacheControl = "public,max-age=60"
                }
            }).ConfigureAwait(false);
        await website.GetBlobClient($"folder/{indexName}").UploadAsync(BinaryData.FromString("folder index"))
            .ConfigureAwait(false);
        await website.GetBlobClient(errorName).UploadAsync(
            BinaryData.FromString("custom not found"),
            new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = "text/html" } })
            .ConfigureAwait(false);
        await website.GetBlobClient(assetName).UploadAsync(
            BinaryData.FromString("0123456789"),
            new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = "text/plain" } })
            .ConfigureAwait(false);
        await website.GetBlobClient(defaultName).UploadAsync(
            BinaryData.FromString("single page fallback"),
            new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = "text/html" } })
            .ConfigureAwait(false);
    }

    [Fact]
    public async Task AccountInformationUsesAzureRoutingAcrossResourceClients()
    {
        var service = CreateClient(factory);
        var missingContainer = service.GetBlobContainerClient($"account-info-{Guid.NewGuid():N}");
        var missingBlob = missingContainer.GetBlobClient("does-not-exist.bin");

        static void AssertAccountInfo(AccountInfo info)
        {
            Assert.Equal(SkuName.StandardLrs, info.SkuName);
            Assert.Equal(AccountKind.StorageV2, info.AccountKind);
            Assert.False(info.IsHierarchicalNamespaceEnabled);
        }

        AssertAccountInfo((await service.GetAccountInfoAsync()).Value);
        AssertAccountInfo((await missingContainer.GetAccountInfoAsync()).Value);
        AssertAccountInfo((await missingBlob.GetAccountInfoAsync()).Value);

        var sasBlob = CreateBlobClient(
            factory,
            missingBlob.GenerateSasUri(
                BlobSasPermissions.Read,
                DateTimeOffset.UtcNow.AddMinutes(5)));
        AssertAccountInfo((await sasBlob.GetAccountInfoAsync()).Value);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        using var legacyRequest = new HttpRequestMessage(
            HttpMethod.Head,
            AppendQuery(sasBlob.Uri, "restype=account&comp=properties"));
        legacyRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-03-28");
        using var legacyResponse = await transport.SendAsync(legacyRequest);
        Assert.Equal(HttpStatusCode.OK, legacyResponse.StatusCode);
        Assert.Equal("Standard_LRS", legacyResponse.Headers.GetValues("x-ms-sku-name").Single());
        Assert.Equal("StorageV2", legacyResponse.Headers.GetValues("x-ms-account-kind").Single());
        Assert.False(legacyResponse.Headers.Contains("x-ms-is-hns-enabled"));
        Assert.Equal(0, legacyResponse.Content.Headers.ContentLength);
    }

    [Fact]
    public async Task HierarchicalNamespaceAccountsExposePosixPropertiesAndRejectUnsupportedBlobApis()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal2 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);

        var accountInfo = (await service.GetAccountInfoAsync()).Value;
        Assert.True(accountInfo.IsHierarchicalNamespaceEnabled);

        var container = service.GetBlobContainerClient($"hns-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("folder/file.txt");
        await blob.UploadAsync(BinaryData.FromString("hierarchical namespace"));
        await AssertHnsPosixPropertiesAsync(blob);

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        await AssertHnsPermissionListingAsync(application, container, credential);
        await AssertUnsupportedHnsBlobOperationsAsync(application, container, credential);
    }

    private static async Task AssertUnsupportedHnsBlobOperationsAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        StorageSharedKeyCredential credential)
    {
        var pageFailure = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetPageBlobClient("page.bin").CreateAsync(512)).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, pageFailure.Status);
        Assert.Equal("BlobOperationNotSupported", pageFailure.ErrorCode);

        var append = container.GetAppendBlobClient("append.log");
        await append.CreateAsync().ConfigureAwait(false);
        await append.AppendBlockAsync(BinaryData.FromString("entry").ToStream()).ConfigureAwait(false);
        var sealFailure = await Assert.ThrowsAsync<RequestFailedException>(() => append.SealAsync())
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, sealFailure.Status);
        Assert.Equal("BlobOperationNotSupported", sealFailure.ErrorCode);

        var objectBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Object,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        objectBuilder.SetPermissions(AccountSasPermissions.Create | AccountSasPermissions.Write);
        var incrementalCopyUri = AppendQuery(
            container.GetPageBlobClient("incremental.vhd").Uri,
            $"comp=incrementalcopy&{objectBuilder.ToSasQueryParameters(credential)}");
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var incrementalCopy = new HttpRequestMessage(HttpMethod.Put, incrementalCopyUri)
        {
            Content = new ByteArrayContent([])
        };
        incrementalCopy.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        incrementalCopy.Headers.TryAddWithoutValidation(
            "x-ms-copy-source",
            "/source/source.vhd?snapshot=2026-09-22T00:00:00.0000000Z");
        using var response = await transport.SendAsync(incrementalCopy).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal("BlobOperationNotSupported", response.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task AssertHnsPermissionListingAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        StorageSharedKeyCredential credential)
    {
        var listBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        listBuilder.SetPermissions(AccountSasPermissions.List);
        var listUri = AppendQuery(
            container.Uri,
            $"restype=container&comp=list&include=permissions&{listBuilder.ToSasQueryParameters(credential)}");
        using var transport = new HttpClient(application.Server.CreateHandler());
        using (var list = new HttpRequestMessage(HttpMethod.Get, listUri))
        {
            list.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(list).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var xml = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            Assert.Contains("<Owner>$superuser</Owner>", xml, StringComparison.Ordinal);
            Assert.Contains("<Group>$superuser</Group>", xml, StringComparison.Ordinal);
            Assert.Contains("<Permissions>rw-r-----</Permissions>", xml, StringComparison.Ordinal);
            Assert.Contains("<Acl>user::rw-,group::r--,other::---</Acl>", xml, StringComparison.Ordinal);
            Assert.Contains("<ResourceType>file</ResourceType>", xml, StringComparison.Ordinal);
        }

        using var invalidDelimiter = new HttpRequestMessage(HttpMethod.Get, AppendQuery(listUri, "delimiter=:"));
        invalidDelimiter.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var rejected = await transport.SendAsync(invalidDelimiter).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, rejected.StatusCode);
        Assert.Equal("InvalidQueryParameterValue", rejected.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task AssertHnsPosixPropertiesAsync(BlobClient blob)
    {
        var properties = await blob.GetPropertiesAsync().ConfigureAwait(false);
        var headers = properties.GetRawResponse().Headers;
        Assert.True(headers.TryGetValue("x-ms-owner", out var owner));
        Assert.Equal("$superuser", owner);
        Assert.True(headers.TryGetValue("x-ms-group", out var group));
        Assert.Equal("$superuser", group);
        Assert.True(headers.TryGetValue("x-ms-permissions", out var permissions));
        Assert.Equal("rw-r-----", permissions);
        Assert.True(headers.TryGetValue("x-ms-acl", out var acl));
        Assert.Equal("user::rw-,group::r--,other::---", acl);
        Assert.True(headers.TryGetValue("x-ms-resource-type", out var resourceType));
        Assert.Equal("file", resourceType);
    }

    [Fact]
    public async Task HierarchicalNamespaceSoftDeleteUsesDeletionIdsAndRestoresASelectedGeneration()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal3 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var container = service.GetBlobContainerClient($"hns-delete-{Guid.NewGuid():N}");
        var blob = await CreateHnsSoftDeletedGenerationsAsync(metadata, container);

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var sasBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        sasBuilder.SetPermissions(AccountSasPermissions.List | AccountSasPermissions.Write);
        var sas = sasBuilder.ToSasQueryParameters(credential);
        using var transport = new HttpClient(application.Server.CreateHandler());
        var deletionIds = await AssertHnsDeletedListingAsync(transport, container, blob, sas);
        await AssertHnsSelectedGenerationRestoredAsync(
            transport, container, blob, sas, deletionIds[0], metadata);
    }

    private static async Task AssertHnsSelectedGenerationRestoredAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobClient blob,
        SasQueryParameters sas,
        ulong deletionId,
        MetadataStore metadata)
    {
        var restored = container.GetBlobClient("restored/selected.txt");
        using (var undelete = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(restored.Uri, $"comp=undelete&{sas}"))
        {
            Content = new ByteArrayContent([])
        })
        {
            undelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            undelete.Headers.TryAddWithoutValidation(
                "x-ms-undelete-source",
                $"{Uri.EscapeDataString(blob.Name)}?deletionid={deletionId.ToString(CultureInfo.InvariantCulture)}");
            using var response = await transport.SendAsync(undelete).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        var restoredText = (await restored.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString();
        Assert.Contains(restoredText, RestoredTexts, StringComparer.Ordinal);
        Assert.Equal("active", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var family = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.Single(family, item => item.IsDeleted);
    }

    private static async Task<ulong[]> AssertHnsDeletedListingAsync(
        HttpClient transport, BlobContainerClient container, BlobClient blob, SasQueryParameters sas)
    {
        var listUri = AppendQuery(
            container.Uri,
            $"restype=container&comp=list&showonly=deleted&{sas}");
        using var listRequest = new HttpRequestMessage(HttpMethod.Get, listUri);
        listRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var listResponse = await transport.SendAsync(listRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var document = System.Xml.Linq.XDocument.Parse(
            await listResponse.Content.ReadAsStringAsync().ConfigureAwait(false));
        var deleted = document.Descendants("Blob")
            .Where(element => string.Equals(element.Element("Name")?.Value, blob.Name, StringComparison.Ordinal))
            .ToArray();
        Assert.Equal(2, deleted.Length);
        var deletionIds = deleted
            .Select(element => ulong.Parse(
                Assert.IsType<System.Xml.Linq.XElement>(element.Element("DeletionId")).Value,
                CultureInfo.InvariantCulture))
            .ToArray();
        Assert.Equal(2, deletionIds.Distinct().Count());
        Assert.All(deleted, element => Assert.Equal("true", element.Element("Deleted")?.Value));
        Assert.All(deleted, element => Assert.Null(element.Element("Snapshot")));
        Assert.All(deleted, element => Assert.Null(element.Element("VersionId")));

        using (var invalidMix = new HttpRequestMessage(HttpMethod.Get, AppendQuery(listUri, "include=deleted")))
        {
            invalidMix.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(invalidMix).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidQueryParameterValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using var arrow = new HttpRequestMessage(HttpMethod.Get, AppendQuery(
            container.Uri, $"restype=container&comp=list&{sas}"));
        arrow.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
        arrow.Headers.TryAddWithoutValidation("Accept", AzureResponseWriter.ArrowStreamContentType);
        using var arrowResponse = await transport.SendAsync(arrow).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, arrowResponse.StatusCode);
        Assert.Equal("BlobOperationNotSupported", arrowResponse.Headers.GetValues("x-ms-error-code").Single());
        return deletionIds;
    }

    private static async Task<BlobClient> CreateHnsSoftDeletedGenerationsAsync(
        MetadataStore metadata, BlobContainerClient container)
    {
        var properties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.SecondAccountName,
            CancellationToken.None).ConfigureAwait(false);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.SecondAccountName,
            properties with
            {
                BlobSoftDeleteEnabled = true,
                BlobSoftDeleteRetentionDays = 7,
                VersioningEnabled = true
            },
            CancellationToken.None).ConfigureAwait(false);

        await container.CreateAsync().ConfigureAwait(false);
        var blob = container.GetBlobClient("folder/repeated.txt");
        await blob.UploadAsync(BinaryData.FromString("first")).ConfigureAwait(false);
        await blob.UploadAsync(BinaryData.FromString("second"), overwrite: true).ConfigureAwait(false);

        var family = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        var current = Assert.Single(family);
        Assert.True(current.IsCurrent);
        Assert.Null(current.VersionId);
        Assert.False(current.IsDeleted);

        await blob.DeleteAsync().ConfigureAwait(false);
        await blob.UploadAsync(BinaryData.FromString("third")).ConfigureAwait(false);
        await blob.DeleteAsync().ConfigureAwait(false);
        await blob.UploadAsync(BinaryData.FromString("active")).ConfigureAwait(false);
        return blob;
    }

    [Fact]
    public async Task HierarchicalNamespacePersistsDirectoriesAndListsTheirAzureProperties()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal4 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-directories-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("alpha/beta/file.txt").UploadAsync(BinaryData.FromString("nested"));
        await container.GetBlobClient("alpha/root.txt").UploadAsync(BinaryData.FromString("root"));
        await container.GetBlobClient("zeta.txt").UploadAsync(BinaryData.FromString("top"));

        var listSas = container.GenerateSasUri(
            BlobContainerSasPermissions.List,
            DateTimeOffset.UtcNow.AddMinutes(10));
        using var transport = new HttpClient(application.Server.CreateHandler());

        await AssertHnsRecursiveDirectoryListingAsync(transport, listSas);
        await AssertHnsHierarchicalDirectoryListingAsync(transport, listSas);
        await AssertHnsDirectoryLifecycleAsync(container);
    }

    private static async Task<System.Xml.Linq.XDocument> ListHnsDirectoryAsync(
        HttpClient transport,
        Uri listSas,
        string query)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            AppendQuery(listSas, $"restype=container&comp=list&{query}"));
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
    }

    private static async Task AssertHnsRecursiveDirectoryListingAsync(
        HttpClient transport,
        Uri listSas)
    {
        var recursive = await ListHnsDirectoryAsync(transport, listSas, "include=permissions").ConfigureAwait(false);
        var recursiveEntries = recursive.Descendants("Blob").ToDictionary(
            element => Assert.IsType<System.Xml.Linq.XElement>(element.Element("Name")).Value,
            StringComparer.Ordinal);
        Assert.Equal(5, recursiveEntries.Count);
        Assert.Equal("directory", recursiveEntries["alpha"].Element("Properties")?.Element("ResourceType")?.Value);
        Assert.Equal("rwxr-x---", recursiveEntries["alpha"].Element("Properties")?.Element("Permissions")?.Value);
        Assert.Equal("directory", recursiveEntries["alpha/beta"].Element("Properties")?.Element("ResourceType")?.Value);
        Assert.Equal("file", recursiveEntries["alpha/beta/file.txt"].Element("Properties")?.Element("ResourceType")?.Value);

        var onlyDirectories = await ListHnsDirectoryAsync(
            transport, listSas, "showonly=directories&include=permissions").ConfigureAwait(false);
        Assert.Equal(
            DirectoryNames,
            onlyDirectories.Descendants("Blob")
                .Select(element => element.Element("Name")?.Value)
                .ToArray());
        var onlyFiles = await ListHnsDirectoryAsync(transport, listSas, "showonly=files").ConfigureAwait(false);
        Assert.Equal(
            FileNames,
            onlyFiles.Descendants("Blob")
                .Select(element => element.Element("Name")?.Value)
                .ToArray());

        var pagedNames = new List<string>();
        var marker = string.Empty;
        do
        {
            var page = await ListHnsDirectoryAsync(transport, listSas,
                "maxresults=1" +
                (string.IsNullOrEmpty(marker) ? string.Empty : $"&marker={Uri.EscapeDataString(marker)}"))
                .ConfigureAwait(false);
            pagedNames.Add(Assert.Single(page.Descendants("Blob")).Element("Name")!.Value);
            marker = page.Root?.Element("NextMarker")?.Value ?? string.Empty;
        }
        while (!string.IsNullOrEmpty(marker));
        Assert.Equal(recursiveEntries.Keys, pagedNames, StringComparer.Ordinal);
    }

    private static async Task AssertHnsHierarchicalDirectoryListingAsync(HttpClient transport, Uri listSas)
    {
        var root = await ListHnsDirectoryAsync(transport, listSas, "delimiter=/&include=permissions")
            .ConfigureAwait(false);
        var rootPrefix = Assert.Single(root.Descendants("BlobPrefix"));
        Assert.Equal("alpha/", rootPrefix.Element("Name")?.Value);
        var prefixProperties = Assert.IsType<System.Xml.Linq.XElement>(rootPrefix.Element("Properties"));
        Assert.Equal("directory", prefixProperties.Element("ResourceType")?.Value);
        Assert.Equal("rwxr-x---", prefixProperties.Element("Permissions")?.Value);
        Assert.Equal("alpha/", rootPrefix.Element("Name")?.Value);
        Assert.Equal(
            RootFileNames,
            root.Descendants("Blob").Select(element => element.Element("Name")?.Value).ToArray());

        var alpha = await ListHnsDirectoryAsync(
            transport, listSas, $"delimiter=/&prefix={Uri.EscapeDataString("alpha/")}&include=permissions")
            .ConfigureAwait(false);
        Assert.Equal("alpha/beta/", Assert.Single(alpha.Descendants("BlobPrefix")).Element("Name")?.Value);
        Assert.Equal("alpha/root.txt", Assert.Single(alpha.Descendants("Blob")).Element("Name")?.Value);
    }

    private static async Task AssertHnsDirectoryLifecycleAsync(BlobContainerClient container)
    {
        var directoryProperties = await container.GetBlobClient("alpha").GetPropertiesAsync().ConfigureAwait(false);
        Assert.True(directoryProperties.GetRawResponse().Headers.TryGetValue("x-ms-resource-type", out var resourceType));
        Assert.Equal("directory", resourceType);
        Assert.True(directoryProperties.GetRawResponse().Headers.TryGetValue("x-ms-permissions", out var permissions));
        Assert.Equal("rwxr-x---", permissions);

        var notEmpty = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("alpha").DeleteAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, notEmpty.Status);
        Assert.Equal("DirectoryIsNotEmpty", notEmpty.ErrorCode);

        await container.GetBlobClient("conflict").UploadAsync(BinaryData.FromString("file")).ConfigureAwait(false);
        var fileAncestor = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("conflict/child.txt").UploadAsync(BinaryData.FromString("child")))
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, fileAncestor.Status);
        Assert.Equal("PathAlreadyExists", fileAncestor.ErrorCode);

        await container.GetBlobClient("branch/child.txt").UploadAsync(BinaryData.FromString("child"))
            .ConfigureAwait(false);
        var directoryTarget = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("branch").UploadAsync(BinaryData.FromString("replacement"), overwrite: true))
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, directoryTarget.Status);
        Assert.Equal("PathAlreadyExists", directoryTarget.ErrorCode);

        await container.GetBlobClient("alpha/beta/file.txt").DeleteAsync().ConfigureAwait(false);
        await container.GetBlobClient("alpha/beta").DeleteAsync().ConfigureAwait(false);
        await container.GetBlobClient("alpha/root.txt").DeleteAsync().ConfigureAwait(false);
        var deleteEmptyDirectory = await container.GetBlobClient("alpha").DeleteAsync().ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status202Accepted, deleteEmptyDirectory.Status);
    }

    [Fact]
    public async Task HierarchicalRecursiveListingSortsSlashBeforePunctuationAcrossContinuations()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var disposal = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-sort-{Guid.NewGuid():N}");
        await container.CreateAsync();
        foreach (var name in new[] { "a!file", "a-file", "a.file", "a0file", "a/child" })
            await container.GetBlobClient(name).UploadAsync(BinaryData.FromString(name));

        var listed = new List<string>();
        var continuations = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var page in container.GetBlobsAsync().AsPages(pageSizeHint: 1))
        {
            listed.Add(Assert.Single(page.Values).Name);
            Assert.True(listed.Count <= 6, "Recursive HNS listing did not advance its continuation.");
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                Assert.True(continuations.Add(page.ContinuationToken));
        }
        Assert.Equal(["a", "a/child", "a!file", "a-file", "a.file", "a0file"], listed);
        Assert.Equal(5, continuations.Count);

        var ranged = new List<string>();
        await foreach (var blob in container.GetBlobsAsync(new GetBlobsOptions { StartFrom = "a.file" }))
            ranged.Add(blob.Name);
        Assert.Equal(["a.file", "a0file"], ranged);
    }

    [Fact]
    public async Task FlatRecursiveListingKeepsBinarySlashOrderAcrossContinuations()
    {
        var container = CreateClient(factory)
            .GetBlobContainerClient($"flat-sort-{Guid.NewGuid():N}");
        await container.CreateAsync();
        foreach (var name in new[] { "a!file", "a-file", "a.file", "a0file", "a/child" })
            await container.GetBlobClient(name).UploadAsync(BinaryData.FromString(name));

        var listed = new List<string>();
        await foreach (var page in container.GetBlobsAsync().AsPages(pageSizeHint: 1))
            listed.Add(Assert.Single(page.Values).Name);
        Assert.Equal(["a!file", "a-file", "a.file", "a/child", "a0file"], listed);

        var ranged = new List<string>();
        await foreach (var blob in container.GetBlobsAsync(new GetBlobsOptions { StartFrom = "a.file" }))
            ranged.Add(blob.Name);
        Assert.Equal(["a.file", "a/child", "a0file"], ranged);
    }

    [Fact]
    public async Task HierarchicalNamespacePersistsBearerOwnerAndInheritedGroupAcrossOverwriteAndRestart()
    {
        const string creatorId = "1a26a4bf-1ae9-40de-8834-678601f7f508";
        const string delegatedCreatorId = "08737b1c-dcce-4a21-8278-6b7cd25fe946";
        const string impersonatedCreatorId = "d6d4280e-c209-4c18-9cfa-1df44fd5da08";
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-hns-owner-{Guid.NewGuid():N}");
        var containerName = $"hns-owner-{Guid.NewGuid():N}";
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            [$"Sava:BearerAuthentication:Principals:{creatorId}:Accounts:0"] = SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{creatorId}:Permissions"] = "racwdxltmeop",
            [$"Sava:BearerAuthentication:Principals:{creatorId}:UserPrincipalName"] = "creator@example.test",
            [$"Sava:BearerAuthentication:Principals:{delegatedCreatorId}:Accounts:0"] =
                SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{delegatedCreatorId}:Permissions"] = "cw",
            [$"Sava:BearerAuthentication:Principals:{delegatedCreatorId}:UserPrincipalName"] =
                "delegated@example.test",
            [$"Sava:BearerAuthentication:Principals:{delegatedCreatorId}:CanGenerateUserDelegationKey"] = "true",
            [$"Sava:BearerAuthentication:Principals:{delegatedCreatorId}:CanManageOwnership"] = "true"
        };

        try
        {
            await CreateHnsOwnershipFixtureAsync(
                dataPath, configuration, containerName, creatorId, delegatedCreatorId, impersonatedCreatorId);
            await VerifyHnsOwnershipAfterRestartAsync(
                dataPath, configuration, containerName, creatorId, delegatedCreatorId, impersonatedCreatorId);
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
    }

    private static async Task CreateHnsOwnershipFixtureAsync(
        string dataPath,
        IReadOnlyDictionary<string, string?> configuration,
        string containerName,
        string creatorId,
        string delegatedCreatorId,
        string impersonatedCreatorId)
    {
        var application = new SavaWebApplicationFactory(dataPath, configuration, deleteDataPath: false);
        await using (application.ConfigureAwait(false))
        {
            await application.InitializeAsync().ConfigureAwait(false);
            var bearer = CreateBearerClient(
                application,
                CreateJwt(SavaWebApplicationFactory.AccountKey, creatorId, SavaWebApplicationFactory.TenantId));
            var created = bearer.GetBlobContainerClient(containerName);
            await created.CreateAsync().ConfigureAwait(false);
            await created.GetBlobClient("parent/child.txt")
                .UploadAsync(BinaryData.FromString("original")).ConfigureAwait(false);

            var shared = CreateClient(application).GetBlobContainerClient(containerName);
            await shared.GetBlobClient("parent/child.txt")
                .UploadAsync(BinaryData.FromString("replacement"), overwrite: true).ConfigureAwait(false);
            await UploadDelegatedOwnerBlobsAsync(
                application, containerName, delegatedCreatorId, impersonatedCreatorId).ConfigureAwait(false);
            await AssertHierarchicalOwnershipAsync(
                application, shared, creatorId, delegatedCreatorId, impersonatedCreatorId).ConfigureAwait(false);
        }
    }

    private static async Task UploadDelegatedOwnerBlobsAsync(
        SavaWebApplicationFactory application,
        string containerName,
        string delegatedCreatorId,
        string impersonatedCreatorId)
    {
        var delegator = CreateBearerClient(
            application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, delegatedCreatorId, SavaWebApplicationFactory.TenantId));
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn }).ConfigureAwait(false);
        var sasBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = "parent/delegated.txt",
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        sasBuilder.SetPermissions(BlobSasPermissions.Create | BlobSasPermissions.Write);
        var sas = sasBuilder.ToSasQueryParameters(key.Value, SavaWebApplicationFactory.AccountName);
        var delegated = CreateBlobClient(
            application,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/" +
                $"{containerName}/parent/delegated.txt?{sas}"));
        await delegated.UploadAsync(BinaryData.FromString("delegated")).ConfigureAwait(false);

        sasBuilder.BlobName = "parent/impersonated.txt";
        sasBuilder.PreauthorizedAgentObjectId = impersonatedCreatorId;
        var impersonationSas = sasBuilder.ToSasQueryParameters(key.Value, SavaWebApplicationFactory.AccountName);
        var impersonated = CreateBlobClient(
            application,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/" +
                $"{containerName}/parent/impersonated.txt?{impersonationSas}"));
        await impersonated.UploadAsync(BinaryData.FromString("impersonated")).ConfigureAwait(false);
    }

    private static async Task VerifyHnsOwnershipAfterRestartAsync(
        string dataPath,
        IReadOnlyDictionary<string, string?> configuration,
        string containerName,
        string creatorId,
        string delegatedCreatorId,
        string impersonatedCreatorId)
    {
        var restarted = new SavaWebApplicationFactory(dataPath, configuration, deleteDataPath: false);
        await using (restarted.ConfigureAwait(false))
        {
            await restarted.InitializeAsync().ConfigureAwait(false);
            await AssertHierarchicalOwnershipAsync(
                restarted,
                CreateClient(restarted).GetBlobContainerClient(containerName),
                creatorId,
                delegatedCreatorId,
                impersonatedCreatorId).ConfigureAwait(false);
        }
    }

    private static async Task AssertHierarchicalOwnershipAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        string creatorId,
        string delegatedCreatorId,
        string impersonatedCreatorId)
    {
        foreach (var name in new[] { "parent", "parent/child.txt" })
        {
            var properties = await container.GetBlobClient(name).GetPropertiesAsync().ConfigureAwait(false);
            Assert.True(properties.GetRawResponse().Headers.TryGetValue("x-ms-owner", out var owner));
            Assert.True(properties.GetRawResponse().Headers.TryGetValue("x-ms-group", out var group));
            Assert.Equal(creatorId, owner);
            Assert.Equal(creatorId, group);
        }
        var delegatedProperties = await container.GetBlobClient("parent/delegated.txt")
            .GetPropertiesAsync().ConfigureAwait(false);
        Assert.True(delegatedProperties.GetRawResponse().Headers.TryGetValue("x-ms-owner", out var delegatedOwner));
        Assert.True(delegatedProperties.GetRawResponse().Headers.TryGetValue("x-ms-group", out var delegatedGroup));
        Assert.Equal(delegatedCreatorId, delegatedOwner);
        Assert.Equal(creatorId, delegatedGroup);
        var impersonatedProperties = await container.GetBlobClient("parent/impersonated.txt")
            .GetPropertiesAsync().ConfigureAwait(false);
        Assert.True(impersonatedProperties.GetRawResponse().Headers.TryGetValue("x-ms-owner", out var impersonatedOwner));
        Assert.True(impersonatedProperties.GetRawResponse().Headers.TryGetValue("x-ms-group", out var impersonatedGroup));
        Assert.Equal(impersonatedCreatorId, impersonatedOwner);
        Assert.Equal(creatorId, impersonatedGroup);

        var listUri = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=list&include=permissions&delimiter=/");
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Get, listUri);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var document = System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
        var directory = Assert.Single(document.Descendants("BlobPrefix"));
        Assert.Equal(creatorId, directory.Element("Properties")?.Element("Owner")?.Value);
        Assert.Equal(creatorId, directory.Element("Properties")?.Element("Group")?.Value);

        await AssertProjectedOwnerNamesAsync(container, transport, listUri, creatorId).ConfigureAwait(false);
    }

    private static async Task AssertProjectedOwnerNamesAsync(
        BlobContainerClient container, HttpClient transport, Uri listUri, string creatorId)
    {
        using var head = new HttpRequestMessage(
            HttpMethod.Head,
            container.GetBlobClient("parent/delegated.txt").GenerateSasUri(
                BlobSasPermissions.Read,
                DateTimeOffset.UtcNow.AddMinutes(5)));
        head.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        head.Headers.TryAddWithoutValidation("x-ms-upn", "true");
        using var projectedHead = await transport.SendAsync(head).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, projectedHead.StatusCode);
        Assert.Equal("delegated@example.test", GetResponseHeader(projectedHead, "x-ms-owner"));
        Assert.Equal("creator@example.test", GetResponseHeader(projectedHead, "x-ms-group"));

        using var projectedListRequest = new HttpRequestMessage(HttpMethod.Get, listUri);
        projectedListRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        projectedListRequest.Headers.TryAddWithoutValidation("x-ms-upn", "true");
        using var projectedListResponse = await transport.SendAsync(projectedListRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, projectedListResponse.StatusCode);
        var projectedList = System.Xml.Linq.XDocument.Parse(await projectedListResponse.Content.ReadAsStringAsync().ConfigureAwait(false));
        var projectedDirectory = Assert.Single(projectedList.Descendants("BlobPrefix"));
        Assert.Equal("creator@example.test", projectedDirectory.Element("Properties")?.Element("Owner")?.Value);
        Assert.Equal("creator@example.test", projectedDirectory.Element("Properties")?.Element("Group")?.Value);

        var recursiveUri = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=list&include=permissions");
        using var recursiveRequest = new HttpRequestMessage(HttpMethod.Get, recursiveUri);
        recursiveRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        recursiveRequest.Headers.TryAddWithoutValidation("x-ms-upn", "true");
        using var recursiveResponse = await transport.SendAsync(recursiveRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, recursiveResponse.StatusCode);
        var recursive = System.Xml.Linq.XDocument.Parse(await recursiveResponse.Content.ReadAsStringAsync().ConfigureAwait(false));
        var delegatedEntry = recursive.Descendants("Blob").Single(element => string.Equals(element.Element("Name")?.Value, "parent/delegated.txt", StringComparison.Ordinal));
        Assert.Equal("delegated@example.test", delegatedEntry.Element("Properties")?.Element("Owner")?.Value);
        Assert.Equal("creator@example.test", delegatedEntry.Element("Properties")?.Element("Group")?.Value);
    }

    [Fact]
    public async Task HierarchicalNamespaceUserDelegationSuoidChecksOwnerAndEveryAncestorBeforeRead()
    {
        const string ownerObjectId = "11e1e2fd-3f95-4e15-b855-c83472392325";
        const string foreignObjectId = "68679a90-477a-4584-94d1-c520340668b1";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            [$"Sava:BearerAuthentication:Principals:{ownerObjectId}:Accounts:0"] =
                SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{ownerObjectId}:Permissions"] = "racwdxltmeop",
            [$"Sava:BearerAuthentication:Principals:{ownerObjectId}:CanGenerateUserDelegationKey"] = "true",
            [$"Sava:BearerAuthentication:Principals:{ownerObjectId}:CanManageOwnership"] = "true"
        });
        await using var applicationDisposal5 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var bearer = CreateBearerClient(
            application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, ownerObjectId, SavaWebApplicationFactory.TenantId));
        var container = bearer.GetBlobContainerClient($"suoid-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("parent/child.txt").UploadAsync(BinaryData.FromString("owned-content"));
        await container.GetAppendBlobClient("parent/log.txt").CreateAsync();

        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await bearer.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn })).Value;

        BlobClient WithSuoid(string name, string objectId) => CreateSuoidBlobClient(
            application, key, container.Name, name, objectId, "rw", startsOn, expiresOn);

        var owned = WithSuoid("parent/child.txt", ownerObjectId);
        await AssertOwnedSuoidOperationsAsync(application, container, owned, WithSuoid, ownerObjectId);

        var foreignAgent = WithSuoid("parent/child.txt", foreignObjectId);
        await AssertForeignSuoidDeniedAsync(application, owned, foreignAgent, ownerObjectId, foreignObjectId);
        await AssertForeignOwnedSuoidPathsAsync(
            application, container, key, WithSuoid, startsOn, expiresOn, ownerObjectId, foreignObjectId);
        await AssertSuoidMissingPathsAsync(WithSuoid, ownerObjectId);
        await GrantForeignSuoidReadAsync(application, container, foreignAgent, foreignObjectId);
    }

    private static async Task AssertOwnedSuoidOperationsAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient owned,
        Func<string, string, BlobClient> withSuoid,
        string ownerObjectId)
    {
        Assert.Equal("owned-content", (await owned.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        Assert.Equal(13, (await owned.GetPropertiesAsync().ConfigureAwait(false)).Value.ContentLength);
        using (var transport = new HttpClient(application.Server.CreateHandler()))
        using (var metadataRequest = new HttpRequestMessage(
                   HttpMethod.Get, new Uri(owned.Uri + "&comp=metadata", UriKind.Absolute)))
        using (var metadataResponse = await transport.SendAsync(metadataRequest).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.OK, metadataResponse.StatusCode);
        var signedBlockBlob = new BlockBlobClient(owned.Uri, new BlobClientOptions
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });
        var ownedQuery = await signedBlockBlob.QueryAsync("SELECT _1 FROM BlobStorage;").ConfigureAwait(false);
        using (var reader = new StreamReader(ownedQuery.Value.Content))
            Assert.Contains("owned-content", await reader.ReadToEndAsync().ConfigureAwait(false), StringComparison.Ordinal);
        await owned.UploadAsync(BinaryData.FromString("changed"), overwrite: true).ConfigureAwait(false);
        Assert.Equal("changed", (await owned.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        var signedAppendUri = withSuoid("parent/log.txt", ownerObjectId).Uri;
        var signedAppend = new AppendBlobClient(signedAppendUri, new BlobClientOptions
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });
        using var payload = new MemoryStream("signed-append"u8.ToArray(), writable: false);
        await signedAppend.AppendBlockAsync(payload).ConfigureAwait(false);
        Assert.Equal("signed-append", (await container.GetBlobClient("parent/log.txt")
            .DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static async Task AssertForeignSuoidDeniedAsync(
        SavaWebApplicationFactory application,
        BlobClient owned,
        BlobClient foreignAgent,
        string ownerObjectId,
        string foreignObjectId)
    {
        var deniedTraversal = await Assert.ThrowsAsync<RequestFailedException>(() =>
            foreignAgent.DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal("AuthorizationFailure", deniedTraversal.ErrorCode);
        var deniedMutation = await Assert.ThrowsAsync<RequestFailedException>(() =>
            foreignAgent.UploadAsync(BinaryData.FromString("forbidden"), overwrite: true)).ConfigureAwait(false);
        Assert.Equal("AuthorizationFailure", deniedMutation.ErrorCode);
        var tampered = CreateBlobClient(
            application,
            new Uri(owned.Uri.AbsoluteUri.Replace(
                $"suoid={ownerObjectId}",
                $"suoid={foreignObjectId}",
                StringComparison.Ordinal)));
        var deniedTampering = await Assert.ThrowsAsync<RequestFailedException>(() =>
            tampered.DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal("AuthenticationFailed", deniedTampering.ErrorCode);
    }

    private static async Task AssertForeignOwnedSuoidPathsAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        Func<string, string, BlobClient> withSuoid,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn,
        string ownerObjectId,
        string foreignObjectId)
    {
        var delegatedWrite = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = "parent/foreign.txt",
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp,
            PreauthorizedAgentObjectId = foreignObjectId
        };
        delegatedWrite.SetPermissions(BlobSasPermissions.Create | BlobSasPermissions.Write);
        var writeSas = delegatedWrite.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName);
        var foreignOwned = CreateBlobClient(
            application,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/parent/foreign.txt" +
                $"?{writeSas}"));
        await foreignOwned.UploadAsync(BinaryData.FromString("foreign-content")).ConfigureAwait(false);
        var deniedTarget = await Assert.ThrowsAsync<RequestFailedException>(() =>
            withSuoid("parent/foreign.txt", ownerObjectId).DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal("AuthorizationFailure", deniedTarget.ErrorCode);

        delegatedWrite.BlobName = "foreign-parent/seed.txt";
        var parentWriteSas = delegatedWrite.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName);
        await CreateBlobClient(
                application,
                new Uri(
                    $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/foreign-parent/seed.txt" +
                    $"?{parentWriteSas}"))
            .UploadAsync(BinaryData.FromString("seed")).ConfigureAwait(false);
        await container.GetBlobClient("foreign-parent/owned.txt")
            .UploadAsync(BinaryData.FromString("owned")).ConfigureAwait(false);
        var deniedParent = await Assert.ThrowsAsync<RequestFailedException>(() =>
            withSuoid("foreign-parent/owned.txt", ownerObjectId).DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal("AuthorizationFailure", deniedParent.ErrorCode);
    }

    private static async Task AssertSuoidMissingPathsAsync(
        Func<string, string, BlobClient> withSuoid,
        string ownerObjectId)
    {
        var missing = await Assert.ThrowsAsync<RequestFailedException>(() =>
            withSuoid("parent/missing.txt", ownerObjectId).DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal("BlobNotFound", missing.ErrorCode);
        var missingParent = await Assert.ThrowsAsync<RequestFailedException>(() =>
            withSuoid("missing-parent/missing.txt", ownerObjectId).DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal("BlobNotFound", missingParent.ErrorCode);
    }

    private static async Task GrantForeignSuoidReadAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient foreignAgent,
        string foreignObjectId)
    {
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var root = await metadata.GetContainerAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(root);
        await metadata.PutContainerAsync(
            root with { AccessAcl = $"user::rwx,user:{foreignObjectId}:--x,group::r-x,mask::r-x,other::---" },
            root.Revision,
            CancellationToken.None).ConfigureAwait(false);
        var parent = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            "parent",
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(parent);
        await metadata.PutBlobRecordAsync(
            parent with { AccessAcl = $"user::rwx,user:{foreignObjectId}:--x,group::r-x,mask::r-x,other::---" },
            parent.Revision,
            CancellationToken.None).ConfigureAwait(false);
        var target = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            "parent/child.txt",
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(target);
        await metadata.PutBlobRecordAsync(
            target with { AccessAcl = $"user::rw-,user:{foreignObjectId}:r--,group::r--,mask::r--,other::---" },
            target.Revision,
            CancellationToken.None).ConfigureAwait(false);
        Assert.Equal("changed", (await foreignAgent.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static BlobClient CreateSuoidBlobClient(
        SavaWebApplicationFactory application,
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        string containerName,
        string name,
        string objectId,
        string permissions,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn)
    {
        const string signedVersion = "2023-11-03";
        var signedStart = startsOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var signedExpiry = expiresOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var keyStart = key.SignedStartsOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var keyExpiry = key.SignedExpiresOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var canonicalResource = $"/blob/{SavaWebApplicationFactory.AccountName}/{containerName}/{name}";
        var stringToSign = string.Join('\n',
            permissions, signedStart, signedExpiry, canonicalResource,
            key.SignedObjectId, key.SignedTenantId, keyStart, keyExpiry,
            key.SignedService, key.SignedVersion,
            string.Empty, objectId, string.Empty, string.Empty,
            "https,http", signedVersion, "b",
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty);
        var signature = SignUserDelegationSas(key.Value, stringToSign);
        var query =
            $"sp={permissions}&st={Uri.EscapeDataString(signedStart)}&se={Uri.EscapeDataString(signedExpiry)}" +
            $"&skoid={key.SignedObjectId}&sktid={key.SignedTenantId}" +
            $"&skt={Uri.EscapeDataString(keyStart)}&ske={Uri.EscapeDataString(keyExpiry)}" +
            $"&sks={key.SignedService}&skv={key.SignedVersion}" +
            $"&suoid={objectId}&spr=https%2Chttp&sv={signedVersion}&sr=b" +
            $"&sig={Uri.EscapeDataString(signature)}";
        return CreateBlobClient(
            application,
            new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{name}?{query}"));
    }

    [Fact]
    public async Task HierarchicalNamespaceBearerReadFallsBackToOwnerAclOnlyWithoutAnRbacGrant()
    {
        const string ownerObjectId = "475a2329-f852-4d23-acba-b11dde00ff74";
        const string readerObjectId = "3a90b230-d1cf-4691-9b1e-916d0a5d850c";
        const string strangerObjectId = "45064256-b8fb-45c6-89a7-589030389341";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            [$"Sava:BearerAuthentication:Principals:{ownerObjectId}:Accounts:0"] =
                SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{ownerObjectId}:Permissions"] = "cw",
            [$"Sava:BearerAuthentication:Principals:{readerObjectId}:Accounts:0"] =
                SavaWebApplicationFactory.AccountName,
            [$"Sava:BearerAuthentication:Principals:{readerObjectId}:Permissions"] = "r"
        });
        await using var applicationDisposal6 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var owner = CreateBearerClient(
            application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, ownerObjectId, SavaWebApplicationFactory.TenantId));
        var reader = CreateBearerClient(
            application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, readerObjectId, SavaWebApplicationFactory.TenantId));
        var stranger = CreateBearerClient(
            application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, strangerObjectId, SavaWebApplicationFactory.TenantId));
        var containerName = $"hns-bearer-acl-{Guid.NewGuid():N}";
        var ownerContainer = owner.GetBlobContainerClient(containerName);
        await ownerContainer.CreateAsync();
        var owned = ownerContainer.GetBlobClient("parent/owned.txt");
        await owned.UploadAsync(BinaryData.FromString("owner-bytes"));

        Assert.Equal("owner-bytes", (await owned.DownloadContentAsync()).Value.Content.ToString());
        Assert.Equal(11, (await owned.GetPropertiesAsync()).Value.ContentLength);
        Assert.Equal(
            "owner-bytes",
            (await reader.GetBlobContainerClient(containerName)
                .GetBlobClient("parent/owned.txt")
                .DownloadContentAsync()).Value.Content.ToString());
        var deniedStranger = await Assert.ThrowsAsync<RequestFailedException>(() =>
            stranger.GetBlobContainerClient(containerName)
                .GetBlobClient("parent/owned.txt")
                .DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedStranger.Status);

        await AssertBearerAclNegativeCasesAsync(application, owner, ownerObjectId, containerName);
    }

    private static async Task AssertBearerAclNegativeCasesAsync(
        SavaWebApplicationFactory application, BlobServiceClient owner, string ownerObjectId, string containerName)
    {
        var appOnlyKey = new SymmetricSecurityKey(
            Convert.FromBase64String(SavaWebApplicationFactory.AccountKey))
        {
            KeyId = "test-key"
        };
        var appOnlyToken = new JwtSecurityTokenHandler().WriteToken(new JwtSecurityToken(
            issuer: "https://issuer.mk8.test",
            audience: "https://storage.azure.com/",
            claims: [new Claim("appid", ownerObjectId), new Claim("tid", SavaWebApplicationFactory.TenantId)],
            notBefore: DateTime.UtcNow.AddMinutes(-1),
            expires: DateTime.UtcNow.AddMinutes(10),
            signingCredentials: new SigningCredentials(appOnlyKey, SecurityAlgorithms.HmacSha256)));
        var appOnly = CreateBearerClient(application, appOnlyToken);
        var deniedWithoutOid = await Assert.ThrowsAsync<RequestFailedException>(() =>
            appOnly.GetBlobContainerClient(containerName)
                .GetBlobClient("parent/owned.txt")
                .DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedWithoutOid.Status);

        var sharedContainerName = $"hns-shared-root-{Guid.NewGuid():N}";
        await CreateClient(application).GetBlobContainerClient(sharedContainerName).CreateAsync().ConfigureAwait(false);
        var sharedRootBlob = owner.GetBlobContainerClient(sharedContainerName)
            .GetBlobClient("owner-file.txt");
        await sharedRootBlob.UploadAsync(BinaryData.FromString("owned-file")).ConfigureAwait(false);
        var deniedRoot = await Assert.ThrowsAsync<RequestFailedException>(() =>
            sharedRootBlob.DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedRoot.Status);
    }

    [Fact]
    public async Task HierarchicalNamespaceBearerReadEvaluatesNamedAndGroupAclsWithAncestorTraversal()
    {
        const string namedObjectId = "4d7ec17e-ae95-4bc6-84e0-b95c5ee4c15b";
        const string groupMemberObjectId = "bd107df5-cd2e-437c-b3ca-5c99f4602ed5";
        const string groupId = "a91490fb-9d7f-4c82-abba-114cc59a3de9";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var applicationDisposal7 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-named-acl-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("parent/data.txt");
        await blob.UploadAsync(BinaryData.FromString("acl-content"));

        var readAcl = await ApplyNamedAndGroupAclsAsync(application, container, blob, namedObjectId, groupId);

        var namedToken = CreateJwt(SavaWebApplicationFactory.AccountKey, namedObjectId);
        var named = CreateBearerClient(application, namedToken);
        var groupMember = CreateBearerClient(
            application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, groupMemberObjectId, groups: [groupId]));
        var namedBlob = named.GetBlobContainerClient(container.Name).GetBlobClient(blob.Name);
        var groupBlob = groupMember.GetBlobContainerClient(container.Name).GetBlobClient(blob.Name);
        using var metadataTransport = new HttpClient(application.Server.CreateHandler());
        await AssertNamedAndGroupAclReadsAsync(
            metadataTransport, namedToken, named, namedBlob, groupBlob, blob.Name, container.Name);
        await AssertNamedAndGroupAclRevocationAsync(
            application, metadataTransport, namedToken, named, namedBlob, groupBlob, blob, container.Name, readAcl);
    }

    private static async Task<string> ApplyNamedAndGroupAclsAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient blob,
        string namedObjectId,
        string groupId)
    {
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var root = await metadata.GetContainerAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(root);
        var traverseAcl = $"user::rwx,user:{namedObjectId}:--x,group::r-x," +
                          $"group:{groupId}:--x,mask::r-x,other::---";
        var readAcl = $"user::rw-,user:{namedObjectId}:r--,group::r--," +
                      $"group:{groupId}:r--,mask::r--,other::---";
        Assert.Equal(3, await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = string.Empty,
                AccessAcl = traverseAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "parent",
                AccessAcl = traverseAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = blob.Name,
                AccessAcl = readAcl
            }).ConfigureAwait(false));
        var updatedRoot = await metadata.GetContainerAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(updatedRoot);
        Assert.NotEqual(root.ETag, updatedRoot.ETag, StringComparer.Ordinal);
        return readAcl;
    }

    private static async Task<HttpStatusCode> GetNamedAclMetadataStatusAsync(
        HttpClient metadataTransport,
        string namedToken,
        BlobClient namedBlob)
    {
        using var metadataRequest = new HttpRequestMessage(
            HttpMethod.Get, new Uri(namedBlob.Uri + "?comp=metadata", UriKind.Absolute));
        metadataRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", namedToken);
        metadataRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var metadataResponse = await metadataTransport.SendAsync(metadataRequest).ConfigureAwait(false);
        return metadataResponse.StatusCode;
    }

    private static async Task AssertNamedAndGroupAclReadsAsync(
        HttpClient metadataTransport,
        string namedToken,
        BlobServiceClient named,
        BlobClient namedBlob,
        BlobClient groupBlob,
        string blobName,
        string containerName)
    {
        Assert.Equal("acl-content", (await namedBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        Assert.Equal("acl-content", (await groupBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        Assert.Equal(HttpStatusCode.OK,
            await GetNamedAclMetadataStatusAsync(metadataTransport, namedToken, namedBlob).ConfigureAwait(false));
        var namedQuery = await named.GetBlobContainerClient(containerName)
            .GetBlockBlobClient(blobName).QueryAsync("SELECT _1 FROM BlobStorage;").ConfigureAwait(false);
        using (var queryReader = new StreamReader(namedQuery.Value.Content))
            Assert.Contains("acl-content", await queryReader.ReadToEndAsync().ConfigureAwait(false), StringComparison.Ordinal);
        Assert.True((await namedBlob.GetPropertiesAsync().ConfigureAwait(false)).GetRawResponse().Headers
            .TryGetValue("x-ms-permissions", out var mode));
        Assert.Equal("rw-r-----", mode);
    }

    private static async Task AssertNamedAndGroupAclRevocationAsync(
        SavaWebApplicationFactory application,
        HttpClient metadataTransport,
        string namedToken,
        BlobServiceClient named,
        BlobClient namedBlob,
        BlobClient groupBlob,
        BlobClient blob,
        string containerName,
        string readAcl)
    {
        await blob.UploadAsync(BinaryData.FromString("replaced-content"), overwrite: true).ConfigureAwait(false);
        Assert.Equal("replaced-content", (await namedBlob.DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
        Assert.Equal("replaced-content", (await groupBlob.DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
        Assert.Equal(1, await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = containerName,
                Path = blob.Name,
                AccessAcl = readAcl.Replace("mask::r--", "mask::---", StringComparison.Ordinal)
            }).ConfigureAwait(false));
        var deniedNamed = await Assert.ThrowsAsync<RequestFailedException>(() =>
            namedBlob.DownloadContentAsync()).ConfigureAwait(false);
        var deniedGroup = await Assert.ThrowsAsync<RequestFailedException>(() =>
            groupBlob.DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedNamed.Status);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedGroup.Status);
        Assert.Equal(HttpStatusCode.Forbidden,
            await GetNamedAclMetadataStatusAsync(metadataTransport, namedToken, namedBlob).ConfigureAwait(false));
        var deniedQuery = await Assert.ThrowsAsync<RequestFailedException>(() => named
            .GetBlobContainerClient(containerName)
            .GetBlockBlobClient(blob.Name).QueryAsync("SELECT _1 FROM BlobStorage;")).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedQuery.Status);
        Assert.Equal("replaced-content", (await blob.DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
    }

    [Fact]
    public async Task HierarchicalAclManifestRollsBackEveryTargetWhenOneDoesNotExist()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var applicationDisposal8 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-acl-atomic-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("present.txt").UploadAsync(BinaryData.FromString("present"));
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var before = await metadata.GetContainerAsync(
            SavaWebApplicationFactory.AccountName, container.Name, includeDeleted: false, CancellationToken.None);
        Assert.NotNull(before);
        var entry = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = string.Empty,
            AccessAcl = "user::rwx,group::r-x,other::r-x"
        };
        await Assert.ThrowsAsync<InvalidDataException>(() => ApplyAclManifestAsync(application,
            entry,
            entry with { Path = "missing.txt" }));
        var after = await metadata.GetContainerAsync(
            SavaWebApplicationFactory.AccountName, container.Name, includeDeleted: false, CancellationToken.None);
        Assert.NotNull(after);
        Assert.Equal(before.ETag, after.ETag);
        Assert.Null(after.AccessAcl);

        await AssertInvalidAclManifestEntriesAsync(application, entry);
        Assert.Equal("present", (await container.GetBlobClient("present.txt").DownloadContentAsync())
            .Value.Content.ToString());

        var oversizedPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-acl-oversized-{Guid.NewGuid():N}.json");
        try
        {
            await File.WriteAllTextAsync(oversizedPath, new string('x', 4 * 1024 * 1024 + 1));
            await Assert.ThrowsAsync<InvalidDataException>(() => application.Services
                .GetRequiredService<BlobService>()
                .ApplyHierarchicalAclManifestAsync(oversizedPath, CancellationToken.None));
        }
        finally
        {
            File.Delete(oversizedPath);
        }
    }

    private static async Task AssertInvalidAclManifestEntriesAsync(
        SavaWebApplicationFactory application, HierarchicalAclManifestEntry entry)
    {
        await Assert.ThrowsAsync<InvalidDataException>(() => ApplyAclManifestAsync(application,
            entry with { AccessAcl = "user::rwx,group::r-x,other::---,default:user::rwx" })).ConfigureAwait(false);
        await Assert.ThrowsAsync<InvalidDataException>(() => ApplyAclManifestAsync(application,
            entry,
            entry with
            {
                Path = "present.txt",
                AccessAcl = "user::rw-,group::r--,other::---," +
                            "default:user::rwx,default:group::r-x,default:other::---"
            })).ConfigureAwait(false);
        await Assert.ThrowsAsync<InvalidDataException>(() => ApplyAclManifestAsync(application,
            entry,
            entry with
            {
                Path = "present.txt",
                AccessAcl = "user::rw-,group::r--,other::---",
                StickyBit = true
            })).ConfigureAwait(false);
        await Assert.ThrowsAsync<InvalidDataException>(() => ApplyAclManifestAsync(application,
            entry,
            entry)).ConfigureAwait(false);
    }

    [Fact]
    public async Task HierarchicalDefaultAclPropagatesOnlyToNewDescendants()
    {
        const string readerObjectId = "e4508a61-2d86-42c7-8b51-3da9d71126b6";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var applicationDisposal9 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-default-acl-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("preexisting.txt").UploadAsync(BinaryData.FromString("old"));

        var rootAcl = $"user::rwx,user:{readerObjectId}:--x,group::r-x,mask::r-x,other::---";
        var defaultAcl = $"default:user::rwx,default:user:{readerObjectId}:r-x," +
                         "default:group::r-x,default:mask::r-x,default:other::---";
        var entry = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = string.Empty,
            AccessAcl = rootAcl + "," + defaultAcl
        };
        await ApplyAclManifestAsync(application, entry);
        var reader = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, readerObjectId))
            .GetBlobContainerClient(container.Name);
        var deniedOld = await Assert.ThrowsAsync<RequestFailedException>(() =>
            reader.GetBlobClient("preexisting.txt").DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedOld.Status);
        await container.GetBlobClient("preexisting.txt").UploadAsync(BinaryData.FromString("replaced"), overwrite: true);
        var deniedReplaced = await Assert.ThrowsAsync<RequestFailedException>(() =>
            reader.GetBlobClient("preexisting.txt").DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedReplaced.Status);

        var nested = container.GetBlobClient("one/two/new.txt");
        await nested.UploadAsync(BinaryData.FromString("new"));
        Assert.Equal("new", (await reader.GetBlobClient(nested.Name).DownloadContentAsync())
            .Value.Content.ToString());
        await AssertInheritedDirectoryAclsAsync(application, container, nested, defaultAcl);

        await ApplyAclManifestAsync(application, entry with
        {
            AccessAcl = rootAcl + ",default:user::rwx,default:group::r-x,default:other::---"
        });
        await container.GetBlobClient("later.txt").UploadAsync(BinaryData.FromString("later"));
        Assert.Equal("new", (await reader.GetBlobClient(nested.Name).DownloadContentAsync())
            .Value.Content.ToString());
        var deniedLater = await Assert.ThrowsAsync<RequestFailedException>(() =>
            reader.GetBlobClient("later.txt").DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedLater.Status);
    }

    private static async Task AssertInheritedDirectoryAclsAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient nested,
        string defaultAcl)
    {
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var one = await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName,
            container.Name, "one", null, null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        var two = await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName,
            container.Name, "one/two", null, null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        var file = await metadata.GetBlobAsync(SavaWebApplicationFactory.AccountName,
            container.Name, nested.Name, null, null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(one);
        Assert.NotNull(two);
        Assert.NotNull(file);
        Assert.Equal(defaultAcl, string.Join(',', one.Acl.Split(',').Where(value => value.StartsWith("default:", StringComparison.Ordinal))));
        Assert.Equal(defaultAcl, string.Join(',', two.Acl.Split(',').Where(value => value.StartsWith("default:", StringComparison.Ordinal))));
        Assert.DoesNotContain("default:", file.Acl, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HierarchicalAclListsOnlyAnAuthorizedDirectoryWithSdkPaging()
    {
        const string readerObjectId = "bb709241-d428-4339-8609-635fdf5fd9ac";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var applicationDisposal10 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-list-acl-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("visible/one.txt").UploadAsync(BinaryData.FromString("one"));
        await container.GetBlobClient("visible/two.txt").UploadAsync(BinaryData.FromString("two"));
        await container.GetBlobClient("hidden/secret.txt").UploadAsync(BinaryData.FromString("secret"));

        var rootAcl = $"user::rwx,user:{readerObjectId}:r-x,group::r-x,mask::r-x,other::---";
        var visibleAcl = rootAcl;
        var hiddenAcl = $"user::rwx,user:{readerObjectId}:--x,group::r-x,mask::r-x,other::---";
        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = string.Empty,
                AccessAcl = rootAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "visible",
                AccessAcl = visibleAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "hidden",
                AccessAcl = hiddenAcl
            });

        var reader = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, readerObjectId))
            .GetBlobContainerClient(container.Name);
        await AssertAuthorizedDirectoryListingAsync(reader);
        await ApplyAclManifestAsync(application, new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "hidden",
            AccessAcl = visibleAcl
        });
        await AssertAuthorizedRecursiveListingAsync(reader);
    }

    private static async Task AssertAuthorizedRecursiveListingAsync(BlobContainerClient reader)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var names = new List<string>();
        await foreach (var page in reader.GetBlobsAsync().AsPages(pageSizeHint: 1)
                           .WithCancellation(timeout.Token).ConfigureAwait(false))
        {
            Assert.Single(page.Values);
            names.Add(page.Values[0].Name);
            Assert.True(names.Count <= 6,
                $"Recursive ACL listing did not advance: {string.Join(',', names)}; marker={page.ContinuationToken}");
        }
        Assert.Equal(
            ["hidden", "hidden/secret.txt", "visible", "visible/one.txt", "visible/two.txt"],
            names);
    }

    private static async Task AssertAuthorizedDirectoryListingAsync(BlobContainerClient reader)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var rootNames = new List<string>();
        var rootListing = reader.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions { Delimiter = "/" });
        await foreach (var page in rootListing.AsPages(pageSizeHint: 1).WithCancellation(timeout.Token).ConfigureAwait(false))
        {
            Assert.Single(page.Values);
            rootNames.Add(page.Values[0].Prefix);
            Assert.True(rootNames.Count <= 3,
                $"Root directory listing did not advance: {string.Join(',', rootNames)}; marker={page.ContinuationToken}");
        }
        Assert.Equal(["hidden/", "visible/"], rootNames);

        var visibleNames = new List<string>();
        var visibleListing = reader.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
        {
            Delimiter = "/",
            Prefix = "visible/"
        });
        await foreach (var page in visibleListing.AsPages(pageSizeHint: 1).WithCancellation(timeout.Token).ConfigureAwait(false))
        {
            Assert.Single(page.Values);
            visibleNames.Add(page.Values[0].Blob.Name);
            Assert.True(visibleNames.Count <= 3, "Nested directory listing did not advance its continuation.");
        }
        Assert.Equal(["visible/one.txt", "visible/two.txt"], visibleNames);

        var hidden = await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            var hiddenListing = reader.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
            {
                Delimiter = "/",
                Prefix = "hidden/"
            });
            await foreach (var _ in hiddenListing.ConfigureAwait(false))
            {
            }
        }).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, hidden.Status);

        var recursive = await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await foreach (var _ in reader.GetBlobsAsync().ConfigureAwait(false))
            {
            }
        }).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, recursive.Status);
    }

    [Fact]
    public async Task HierarchicalDirectoryListHonorsSignedSuoidAndListPermission()
    {
        const string readerObjectId = "024e3416-6d43-4310-a404-b2e26705ba06";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            [$"Sava:BearerAuthentication:Principals:{SavaWebApplicationFactory.DelegatorObjectId}:Permissions"] = "rl",
            [$"Sava:BearerAuthentication:Principals:{SavaWebApplicationFactory.DelegatorObjectId}:CanManageOwnership"] = "true"
        });
        await using var applicationDisposal11 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-list-suoid-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("visible/item.txt").UploadAsync(BinaryData.FromString("item"));
        var accessAcl = $"user::rwx,user:{readerObjectId}:r-x,group::r-x,mask::r-x,other::---";
        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = string.Empty,
                AccessAcl = accessAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "visible",
                AccessAcl = accessAcl
            });

        var delegator = CreateBearerClient(application, CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId));
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn })).Value;
        var sas = BuildSignedDirectoryListSas(key, startsOn, expiresOn, container.Name, readerObjectId);

        await AssertSignedDirectoryListingAsync(application, container.Name, sas, readerObjectId);
        await ApplyAclManifestAsync(application, new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "visible",
            AccessAcl = $"user::rwx,user:{readerObjectId}:--x,group::r-x,mask::r-x,other::---"
        });
        await AssertSignedRecursiveListDeniedAsync(application, container.Name, sas);
    }

    private static async Task AssertSignedRecursiveListDeniedAsync(
        SavaWebApplicationFactory application, string containerName, string sas)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var response = await transport.GetAsync(new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}" +
            $"?restype=container&comp=list&{sas}", UriKind.RelativeOrAbsolute)).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
    }

    private static string BuildSignedDirectoryListSas(
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn,
        string containerName,
        string readerObjectId)
    {
        var signedStart = startsOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var signedExpiry = expiresOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var keyStart = key.SignedStartsOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var keyExpiry = key.SignedExpiresOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        const string signedVersion = "2023-11-03";
        var canonicalResource = $"/blob/{SavaWebApplicationFactory.AccountName}/{containerName}";
        var stringToSign = string.Join('\n',
            "l", signedStart, signedExpiry, canonicalResource,
            key.SignedObjectId, key.SignedTenantId, keyStart, keyExpiry,
            key.SignedService, key.SignedVersion,
            string.Empty, readerObjectId, string.Empty, string.Empty,
            "https,http", signedVersion, "c",
            string.Empty, string.Empty, string.Empty, string.Empty,
            string.Empty, string.Empty, string.Empty);
        var signature = SignUserDelegationSas(key.Value, stringToSign);
        var sas =
            $"sp=l&st={Uri.EscapeDataString(signedStart)}&se={Uri.EscapeDataString(signedExpiry)}" +
            $"&skoid={key.SignedObjectId}&sktid={key.SignedTenantId}" +
            $"&skt={Uri.EscapeDataString(keyStart)}&ske={Uri.EscapeDataString(keyExpiry)}" +
            $"&sks={key.SignedService}&skv={key.SignedVersion}" +
            $"&suoid={readerObjectId}&spr=https%2Chttp&sv={signedVersion}&sr=c" +
            $"&sig={Uri.EscapeDataString(signature)}";
        return sas;
    }

    private static async Task AssertSignedDirectoryListingAsync(
        SavaWebApplicationFactory application,
        string containerName,
        string sas,
        string readerObjectId)
    {
        var endpoint = $"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}";
        using var transport = new HttpClient(application.Server.CreateHandler());

        using (var root = await transport.GetAsync(new Uri(
                   $"{endpoint}?restype=container&comp=list&delimiter=%2F&{sas}", UriKind.RelativeOrAbsolute)).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, root.StatusCode);
            Assert.Contains("<Name>visible/</Name>", await root.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
        }
        using (var nested = await transport.GetAsync(new Uri(
                   $"{endpoint}?restype=container&comp=list&delimiter=%2F&prefix=visible%2F&{sas}", UriKind.RelativeOrAbsolute)).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, nested.StatusCode);
            Assert.Contains("<Name>visible/item.txt</Name>", await nested.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
        }
        using (var recursive = await transport.GetAsync(new Uri(
                   $"{endpoint}?restype=container&comp=list&{sas}", UriKind.RelativeOrAbsolute)).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, recursive.StatusCode);
            var body = await recursive.Content.ReadAsStringAsync().ConfigureAwait(false);
            Assert.Contains("<Name>visible</Name>", body, StringComparison.Ordinal);
            Assert.Contains("<Name>visible/item.txt</Name>", body, StringComparison.Ordinal);
        }
        using (var tampered = await transport.GetAsync(new Uri(
                   $"{endpoint}?restype=container&comp=list&delimiter=%2F&" +
                   sas.Replace(readerObjectId, Guid.NewGuid().ToString(), StringComparison.Ordinal), UriKind.RelativeOrAbsolute)).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Forbidden, tampered.StatusCode);
            Assert.Equal("AuthenticationFailed", tampered.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    [Fact]
    public async Task HierarchicalAclAuthorizesMetadataAndPropertiesFromTheParentDirectory()
    {
        const string writerObjectId = "10f03e3f-27ca-4e69-adad-e02c964aa1e2";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-properties-acl-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var nested = container.GetBlobClient("folder/item.txt");
        var root = container.GetBlobClient("root.txt");
        await nested.UploadAsync(BinaryData.FromString("nested"));
        await root.UploadAsync(BinaryData.FromString("root"));
        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = string.Empty,
                AccessAcl = $"user::rwx,user:{writerObjectId}:--x,group::r-x,mask::-wx,other::---"
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "folder",
                AccessAcl = $"user::rwx,user:{writerObjectId}:-wx,group::r-x,mask::rwx,other::---"
            });

        var writer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, writerObjectId))
            .GetBlobContainerClient(container.Name);
        var writable = writer.GetBlobClient(nested.Name);
        await writable.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["status"] = "updated"
        });
        await writable.SetHttpHeadersAsync(new BlobHttpHeaders { ContentType = "text/plain" });
        var properties = (await nested.GetPropertiesAsync()).Value;
        Assert.Equal("updated", properties.Metadata["status"]);
        Assert.Equal("text/plain", properties.ContentType);

        var deniedMetadata = await Assert.ThrowsAsync<RequestFailedException>(() =>
            writer.GetBlobClient(root.Name).SetMetadataAsync(
                new Dictionary<string, string>(StringComparer.Ordinal) { ["status"] = "forbidden" }));
        Assert.Equal(StatusCodes.Status403Forbidden, deniedMetadata.Status);
        var deniedProperties = await Assert.ThrowsAsync<RequestFailedException>(() =>
            writer.GetBlobClient(root.Name).SetHttpHeadersAsync(new BlobHttpHeaders { ContentType = "text/plain" }));
        Assert.Equal(StatusCodes.Status403Forbidden, deniedProperties.Status);
    }

    [Fact]
    public async Task HierarchicalAclAuthorizesPutAndDeleteFromTheParentDirectory()
    {
        const string writerObjectId = "97f5af62-55de-46b3-86b5-151988d1ae11";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var applicationDisposal12 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-write-acl-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("folder/existing.txt").UploadAsync(BinaryData.FromString("old"));
        await container.GetBlobClient("folder/delete.txt").UploadAsync(BinaryData.FromString("delete"));
        await container.GetBlobClient("root-existing.txt").UploadAsync(BinaryData.FromString("root"));
        var rootAcl = $"user::rwx,user:{writerObjectId}:--x,group::r-x,mask::-wx,other::---";
        var folderAcl = $"user::rwx,user:{writerObjectId}:-wx,group::r-x,mask::rwx,other::---";
        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = string.Empty,
                AccessAcl = rootAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "folder",
                AccessAcl = folderAcl
            });

        await AssertParentAclMutationsAsync(application, container, writerObjectId);
    }

    [Fact]
    public async Task HierarchicalStickyDirectoryRejectsDeletionOfAnotherOwnersChild()
    {
        const string writerObjectId = "e054929f-c734-4e3b-b19b-b810eb24be22";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-sticky-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var foreign = container.GetBlobClient("sticky/foreign.txt");
        await foreign.UploadAsync(BinaryData.FromString("retained"));
        var rootAcl = $"user::rwx,user:{writerObjectId}:--x,group::r-x,mask::r-x,other::---";
        var stickyAcl = $"user::rwx,user:{writerObjectId}:-wx,group::r-x,mask::rwx,other::---";
        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = string.Empty,
                AccessAcl = rootAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "sticky",
                AccessAcl = stickyAcl,
                StickyBit = true
            });

        var directory = container.GetBlobClient("sticky");
        Assert.True((await directory.GetPropertiesAsync()).GetRawResponse().Headers
            .TryGetValue("x-ms-permissions", out var mode));
        Assert.Equal("rwxr-x--T", mode);
        var writer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, writerObjectId))
            .GetBlobContainerClient(container.Name);
        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            writer.GetBlobClient(foreign.Name).DeleteAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, denied.Status);
        Assert.Equal("retained", (await foreign.DownloadContentAsync()).Value.Content.ToString());

        var owned = writer.GetBlobClient("sticky/owned.txt");
        await owned.UploadAsync(BinaryData.FromString("owned"));
        await owned.DeleteAsync();
        Assert.False((await container.GetBlobClient(owned.Name).ExistsAsync()).Value);

        await ApplyAclManifestAsync(application, new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "sticky",
            AccessAcl = stickyAcl,
            StickyBit = false
        });
        await writer.GetBlobClient(foreign.Name).DeleteAsync();
        Assert.False((await foreign.ExistsAsync()).Value);
        await AssertStickyRootDeletionAsync(application, container, writer, writerObjectId);
    }

    private static async Task AssertStickyRootDeletionAsync(
        SavaWebApplicationFactory application, BlobContainerClient container,
        BlobContainerClient writer, string writerObjectId)
    {
        var foreign = container.GetBlobClient("root-foreign.txt");
        await foreign.UploadAsync(BinaryData.FromString("root-retained")).ConfigureAwait(false);
        var rootAcl = $"user::rwx,user:{writerObjectId}:-wx,group::r-x,mask::rwx,other::---";
        var entry = new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = string.Empty,
            AccessAcl = rootAcl,
            StickyBit = true
        };
        await ApplyAclManifestAsync(application, entry).ConfigureAwait(false);
        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            writer.GetBlobClient(foreign.Name).DeleteAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, denied.Status);
        Assert.Equal("root-retained",
            (await foreign.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var owned = writer.GetBlobClient("root-owned.txt");
        await owned.UploadAsync(BinaryData.FromString("owned")).ConfigureAwait(false);
        await owned.DeleteAsync().ConfigureAwait(false);
        await ApplyAclManifestAsync(application, entry with { StickyBit = false }).ConfigureAwait(false);
        await writer.GetBlobClient(foreign.Name).DeleteAsync().ConfigureAwait(false);
        Assert.False((await foreign.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task HierarchicalStickyDirectoryOwnerCanDeleteAnotherOwnersChild()
    {
        const string directoryOwnerId = "754ce7e8-1858-4837-8c20-05a3dbe0a602";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-sticky-parent-owner-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await ApplyAclManifestAsync(application, new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = string.Empty,
            AccessAcl = $"user::rwx,user:{directoryOwnerId}:-wx,group::r-x,mask::rwx,other::---"
        });

        var owner = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, directoryOwnerId))
            .GetBlobContainerClient(container.Name);
        await container.GetBlobClient("owned-parent/seed.txt").UploadAsync(BinaryData.FromString("seed"));
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var parent = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, "owned-parent",
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None);
        Assert.NotNull(parent);
        await metadata.PutBlobRecordAsync(
            parent with { Owner = directoryOwnerId }, parent.Revision, CancellationToken.None);
        parent = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, "owned-parent",
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None);
        Assert.NotNull(parent);
        Assert.Equal(directoryOwnerId, parent.Owner);

        var foreign = container.GetBlobClient("owned-parent/foreign.txt");
        await foreign.UploadAsync(BinaryData.FromString("foreign"));
        var foreignRecord = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, foreign.Name,
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None);
        Assert.NotNull(foreignRecord);
        Assert.Equal("$superuser", foreignRecord.Owner);
        await ApplyAclManifestAsync(application, new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "owned-parent",
            AccessAcl = "user::rwx,group::r-x,other::---",
            StickyBit = true
        });
        await owner.GetBlobClient(foreign.Name).DeleteAsync();
        Assert.False((await foreign.ExistsAsync()).Value);
    }

    [Fact]
    public async Task HierarchicalStickyDirectoryHonorsSignedSuoidOwnership()
    {
        const string writerObjectId = "fabcedf4-22e7-45cf-8832-f58f4fe1bd9c";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true",
            [$"Sava:BearerAuthentication:Principals:{SavaWebApplicationFactory.DelegatorObjectId}:Permissions"] = "wd",
            [$"Sava:BearerAuthentication:Principals:{SavaWebApplicationFactory.DelegatorObjectId}:CanGenerateUserDelegationKey"] = "true",
            [$"Sava:BearerAuthentication:Principals:{SavaWebApplicationFactory.DelegatorObjectId}:CanManageOwnership"] = "true"
        });
        await using var disposal = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-sticky-suoid-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var foreign = container.GetBlobClient("sticky/foreign.txt");
        await foreign.UploadAsync(BinaryData.FromString("retained"));
        var rootAcl = $"user::rwx,user:{writerObjectId}:--x,group::r-x,mask::r-x,other::---";
        var stickyAcl = $"user::rwx,user:{writerObjectId}:-wx,group::r-x,mask::rwx,other::---";
        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = string.Empty,
                AccessAcl = rootAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "sticky",
                AccessAcl = stickyAcl,
                StickyBit = true
            });

        var writer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, writerObjectId))
            .GetBlobContainerClient(container.Name);
        var owned = writer.GetBlobClient("sticky/owned.txt");
        await owned.UploadAsync(BinaryData.FromString("owned"));
        var batchOwned = writer.GetBlobClient("sticky/batch-owned.txt");
        await batchOwned.UploadAsync(BinaryData.FromString("batch-owned"));
        await AssertSignedStickyDeletionAsync(application, container, foreign, owned, batchOwned, writerObjectId);
    }

    private static async Task AssertSignedStickyDeletionAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient foreign,
        BlobClient owned,
        BlobClient batchOwned,
        string writerObjectId)
    {
        var delegator = CreateBearerClient(application, CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId));
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(5);
        var key = (await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn }).ConfigureAwait(false)).Value;
        BlobClient Signed(string name) => CreateSuoidBlobClient(
            application, key, container.Name, name, writerObjectId, "d", startsOn, expiresOn);

        var denied = await Assert.ThrowsAsync<RequestFailedException>(() => Signed(foreign.Name).DeleteAsync())
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, denied.Status);
        Assert.Equal("AuthorizationFailure", denied.ErrorCode);
        Assert.Equal("retained", (await foreign.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        await Signed(owned.Name).DeleteAsync().ConfigureAwait(false);
        Assert.False((await owned.ExistsAsync().ConfigureAwait(false)).Value);

        var deniedBatch = await SubmitSignedBatchAsync(
            application, container, Signed(foreign.Name).Uri, "DELETE", null, null).ConfigureAwait(false);
        Assert.Contains("HTTP/1.1 403 Forbidden", deniedBatch, StringComparison.Ordinal);
        Assert.Contains("AuthorizationFailure", deniedBatch, StringComparison.Ordinal);
        Assert.Equal("retained", (await foreign.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var allowedBatch = await SubmitSignedBatchAsync(
            application, container, Signed(batchOwned.Name).Uri, "DELETE", null, null).ConfigureAwait(false);
        Assert.Contains("HTTP/1.1 202 Accepted", allowedBatch, StringComparison.Ordinal);
        Assert.False((await batchOwned.ExistsAsync().ConfigureAwait(false)).Value);

        var signedTier = CreateSuoidBlobClient(
            application, key, container.Name, foreign.Name, writerObjectId, "w", startsOn, expiresOn);
        await signedTier.SetAccessTierAsync(AccessTier.Cool).ConfigureAwait(false);
        var tierBatch = await SubmitSignedBatchAsync(
            application, container, signedTier.Uri, "PUT", "tier", "x-ms-access-tier: Hot\r\n")
            .ConfigureAwait(false);
        Assert.True(tierBatch.Contains("HTTP/1.1 200 OK", StringComparison.Ordinal), tierBatch);
        Assert.Equal(AccessTier.Hot, (await foreign.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);
    }

    private static async Task<string> SubmitSignedBatchAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        Uri signedBlobUri,
        string method,
        string? component,
        string? subrequestHeader)
    {
        var boundary = $"batch_{Guid.NewGuid():N}";
        var target = component is null ? signedBlobUri : AppendQuery(signedBlobUri, $"comp={component}");
        var payload = $"--{boundary}\r\nContent-Type: application/http\r\n" +
                      "Content-Transfer-Encoding: binary\r\n\r\n" +
                      $"{method} {target.PathAndQuery} HTTP/1.1\r\n{subrequestHeader}\r\n" +
                      $"--{boundary}--\r\n";
        var endpoint = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=batch");
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
        {
            Content = new ByteArrayContent(Encoding.Latin1.GetBytes(payload))
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse($"multipart/mixed; boundary={boundary}");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        return await response.Content.ReadAsStringAsync().ConfigureAwait(false);
    }

    private static async Task AssertParentAclMutationsAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, string writerObjectId)
    {
        var writer = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, writerObjectId))
            .GetBlobContainerClient(container.Name);
        await writer.GetBlobClient("folder/new.txt").UploadAsync(BinaryData.FromString("new")).ConfigureAwait(false);
        await writer.GetBlobClient("folder/existing.txt")
            .UploadAsync(BinaryData.FromString("replaced"), overwrite: true).ConfigureAwait(false);
        await writer.GetBlobClient("folder/delete.txt").DeleteAsync().ConfigureAwait(false);
        var staged = writer.GetBlockBlobClient("folder/staged.txt");
        var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("block-1"));
        await staged.StageBlockAsync(blockId, new MemoryStream("staged"u8.ToArray(), writable: false)).ConfigureAwait(false);
        await staged.CommitBlockListAsync([blockId]).ConfigureAwait(false);
        Assert.Equal("new", (await container.GetBlobClient("folder/new.txt").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
        Assert.Equal("replaced", (await container.GetBlobClient("folder/existing.txt").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
        Assert.False((await container.GetBlobClient("folder/delete.txt").ExistsAsync().ConfigureAwait(false)).Value);
        Assert.Equal("staged", (await container.GetBlobClient("folder/staged.txt").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());

        var rootDenied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            writer.GetBlobClient("root-existing.txt").UploadAsync(
                BinaryData.FromString("forbidden"), overwrite: true)).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, rootDenied.Status);
        var missingParent = await Assert.ThrowsAsync<RequestFailedException>(() =>
            writer.GetBlobClient("missing/child.txt").UploadAsync(BinaryData.FromString("forbidden"))).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, missingParent.Status);
        Assert.Equal("root", (await container.GetBlobClient("root-existing.txt").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());

        await ApplyAclManifestAsync(application, new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = string.Empty,
            AccessAcl = $"user::rwx,user:{writerObjectId}:-wx,group::r-x,mask::rwx,other::---"
        }).ConfigureAwait(false);
        await writer.GetBlobClient("root-existing.txt")
            .UploadAsync(BinaryData.FromString("root-replaced"), overwrite: true).ConfigureAwait(false);
        Assert.Equal("root-replaced", (await container.GetBlobClient("root-existing.txt").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
    }

    [Fact]
    public async Task HierarchicalAclAuthorizesTierAndExpiryWritesThroughTheParentDirectory()
    {
        const string writerObjectId = "ad1a3990-f8ca-4d15-a42d-4cadca6ef8e6";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var applicationDisposal = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application).GetBlobContainerClient($"hns-tier-acl-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var target = container.GetBlobClient("folder/target.bin");
        await target.UploadAsync(BinaryData.FromString("unchanged"));
        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = string.Empty,
                AccessAcl = $"user::rwx,user:{writerObjectId}:--x,group::r-x,mask::r-x,other::---"
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "folder",
                AccessAcl = $"user::rwx,user:{writerObjectId}:-wx,group::r-x,mask::rwx,other::---"
            });

        var token = CreateJwt(SavaWebApplicationFactory.AccountKey, writerObjectId);
        var bearerTarget = CreateBearerClient(application, token)
            .GetBlobContainerClient(container.Name).GetBlobClient(target.Name);
        await bearerTarget.SetAccessTierAsync(AccessTier.Cool);
        using (var allowedExpiry = await SendBearerExpiryAsync(application, target.Uri, token, "RelativeToNow", "3600000"))
            Assert.Equal(HttpStatusCode.OK, allowedExpiry.StatusCode);
        Assert.Equal(AccessTier.Cool, (await target.GetPropertiesAsync()).Value.AccessTier);
        var before = await application.Services.GetRequiredService<MetadataStore>().GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, target.Name,
            null, null, false, CancellationToken.None);
        Assert.NotNull(before?.ExpiresAt);

        await ApplyAclManifestAsync(application, new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "folder",
            AccessAcl = $"user::rwx,user:{writerObjectId}:--x,group::r-x,mask::r-x,other::---"
        });
        var deniedTier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            bearerTarget.SetAccessTierAsync(AccessTier.Hot));
        Assert.Equal(StatusCodes.Status403Forbidden, deniedTier.Status);
        using (var deniedExpiry = await SendBearerExpiryAsync(application, target.Uri, token, "NeverExpire", null))
            Assert.Equal(HttpStatusCode.Forbidden, deniedExpiry.StatusCode);
        Assert.Equal(AccessTier.Cool, (await target.GetPropertiesAsync()).Value.AccessTier);
        var after = await application.Services.GetRequiredService<MetadataStore>().GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, target.Name,
            null, null, false, CancellationToken.None);
        Assert.Equal(before.ExpiresAt, after?.ExpiresAt);
        Assert.Equal("unchanged", (await target.DownloadContentAsync()).Value.Content.ToString());
    }

    private static async Task<HttpResponseMessage> SendBearerExpiryAsync(
        SavaWebApplicationFactory application, Uri blobUri, string token, string option, string? expiryTime)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Put, AppendQuery(blobUri, "comp=expiry"))
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-expiry-option", option);
        if (expiryTime is not null)
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", expiryTime);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    [Fact]
    public async Task HierarchicalAclRequiresFileReadAndWriteForAppendBlock()
    {
        const string appenderObjectId = "321c78dd-64e5-439f-bf30-748d84eea652";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        await using var applicationDisposal13 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"hns-append-acl-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetAppendBlobClient("folder/log.txt").CreateAsync();
        var appendAcl = $"user::rw-,user:{appenderObjectId}:rw-,group::r--,mask::rw-,other::---";
        await ApplyAppendAclFixtureAsync(application, container.Name, appenderObjectId, appendAcl);

        var appender = CreateBearerClient(application,
            CreateJwt(SavaWebApplicationFactory.AccountKey, appenderObjectId))
            .GetBlobContainerClient(container.Name)
            .GetAppendBlobClient("folder/log.txt");
        await appender.AppendBlockAsync(new MemoryStream("line"u8.ToArray(), writable: false));
        Assert.Equal("line", (await container.GetBlobClient("folder/log.txt").DownloadContentAsync())
            .Value.Content.ToString());

        await ApplyAclManifestAsync(application, new HierarchicalAclManifestEntry
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = container.Name,
            Path = "folder/log.txt",
            AccessAcl = $"user::rw-,user:{appenderObjectId}:-w-,group::r--,mask::rw-,other::---"
        });
        var deniedFileRead = await Assert.ThrowsAsync<RequestFailedException>(() =>
            appender.AppendBlockAsync(new MemoryStream("denied"u8.ToArray(), writable: false)));
        Assert.Equal(StatusCodes.Status403Forbidden, deniedFileRead.Status);

        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "folder/log.txt",
                AccessAcl = appendAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = container.Name,
                Path = "folder",
                AccessAcl = $"user::rwx,user:{appenderObjectId}:---,group::r-x,mask::r-x,other::---"
            });
        var deniedTraversal = await Assert.ThrowsAsync<RequestFailedException>(() =>
            appender.AppendBlockAsync(new MemoryStream("denied"u8.ToArray(), writable: false)));
        Assert.Equal(StatusCodes.Status403Forbidden, deniedTraversal.Status);
        Assert.Equal("line", (await container.GetBlobClient("folder/log.txt").DownloadContentAsync())
            .Value.Content.ToString());
    }

    private static async Task ApplyAppendAclFixtureAsync(
        SavaWebApplicationFactory application,
        string containerName,
        string appenderObjectId,
        string appendAcl)
    {
        var traverseAcl = $"user::rwx,user:{appenderObjectId}:--x,group::r-x,mask::r-x,other::---";
        await ApplyAclManifestAsync(application,
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = containerName,
                Path = string.Empty,
                AccessAcl = traverseAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = containerName,
                Path = "folder",
                AccessAcl = traverseAcl
            },
            new HierarchicalAclManifestEntry
            {
                Account = SavaWebApplicationFactory.AccountName,
                Container = containerName,
                Path = "folder/log.txt",
                AccessAcl = appendAcl
            }).ConfigureAwait(false);
    }

    [Fact]
    public async Task HierarchicalAclOperatorCommandAppliesManifestAcrossProcessRestart()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-acl-command-{Guid.NewGuid():N}");
        var manifestPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-acl-command-{Guid.NewGuid():N}.json");
        var settings = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        };
        var containerName = $"hns-command-{Guid.NewGuid():N}";
        const string accessAcl = "user::rwx,group::r-x,other::r-x";
        try
        {
            {
                var initial = new SavaWebApplicationFactory(dataPath, settings, deleteDataPath: false);
                await using (initial.ConfigureAwait(false))
                {
                    await initial.InitializeAsync();
                    await CreateClient(initial).GetBlobContainerClient(containerName).CreateAsync();
                }
            }

            var manifest = new HierarchicalAclManifest
            {
                SchemaVersion = 1,
                Entries =
                [
                    new HierarchicalAclManifestEntry
                    {
                        Account = SavaWebApplicationFactory.AccountName,
                        Container = containerName,
                        Path = string.Empty,
                        AccessAcl = accessAcl
                    }
                ]
            };
            await File.WriteAllTextAsync(manifestPath,
                JsonSerializer.Serialize(manifest, JsonSerializerOptions.Web));
            await ApplyAclManifestWithOperatorProcessAsync(manifestPath, dataPath);

            var reopened = new SavaWebApplicationFactory(dataPath, settings, deleteDataPath: false);
            await using var reopenedDisposal14 = reopened.ConfigureAwait(false);
            await reopened.InitializeAsync().ConfigureAwait(true);
            var root = await reopened.Services.GetRequiredService<MetadataStore>().GetContainerAsync(
                SavaWebApplicationFactory.AccountName, containerName, includeDeleted: false, CancellationToken.None).ConfigureAwait(true);
            Assert.NotNull(root);
            Assert.Equal(accessAcl, root.AccessAcl);
        }
        finally
        {
            File.Delete(manifestPath);
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
    }

    private static async Task ApplyAclManifestWithOperatorProcessAsync(string manifestPath, string dataPath)
    {
        var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            WorkingDirectory = AppContext.BaseDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        start.ArgumentList.Add(typeof(Program).Assembly.Location);
        start.ArgumentList.Add("--hns-acl-apply");
        start.ArgumentList.Add(manifestPath);
        start.Environment["Sava__DataPath"] = dataPath;
        start.Environment[$"Sava__AccountCapabilities__{SavaWebApplicationFactory.AccountName}__HierarchicalNamespaceEnabled"] = "true";
        using var process = Process.Start(start);
        Assert.NotNull(process);
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        Assert.True(process.ExitCode == 0, await error.ConfigureAwait(false));
        Assert.Contains("Applied HNS access ACLs to 1 existing targets.",
            await output.ConfigureAwait(false), StringComparison.Ordinal);
    }

    [Fact]
    public async Task HierarchicalNamespaceBlobIndexTagsRequireTheExplicitPreviewCapability()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal15 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-tags-disabled-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("plain.bin");
        await blob.UploadAsync(BinaryData.FromString("untagged"));

        static void AssertUnsupported(RequestFailedException exception)
        {
            Assert.Equal(StatusCodes.Status400BadRequest, exception.Status);
            Assert.Equal("BlobTagsNotSupportedForAccountType", exception.ErrorCode);
        }

        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(() => blob.GetTagsAsync()));
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "blocked" })));

        var taggedUpload = container.GetBlobClient("tagged-upload.bin");
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(() =>
            taggedUpload.UploadAsync(
                BinaryData.FromString("must remain unpublished"),
                new BlobUploadOptions
                {
                    Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "blocked" }
                })));
        Assert.False((await taggedUpload.ExistsAsync()).Value);

        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await foreach (var _ in container.GetBlobsAsync(new GetBlobsOptions { Traits = BlobTraits.Tags }).ConfigureAwait(false))
            {
            }
        }));
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await foreach (var _ in service.FindBlobsByTagsAsync("\"state\" = 'blocked'").ConfigureAwait(false))
            {
            }
        }));
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions { TagConditions = "\"state\" = 'blocked'" }
            })));
    }

    [Fact]
    public async Task HierarchicalNamespaceBlobIndexTagPreviewMatchesTheBlobApiFromVersion20241104()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceBlobIndexTagsEnabled"] = "true"
            });
        await using var applicationDisposal16 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-tags-preview-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("folder/tagged.bin");
        await blob.UploadAsync(
            BinaryData.FromString("preview tags"),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "initial" }
            });

        Assert.Equal("initial", (await blob.GetTagsAsync()).Value.Tags["state"]);
        await blob.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "updated" });
        Assert.Equal("updated", (await blob.GetTagsAsync()).Value.Tags["state"]);

        var listed = await container.GetBlobsAsync(new GetBlobsOptions
        {
            Traits = BlobTraits.Tags,
            Prefix = blob.Name
        }).SingleAsync();
        Assert.Equal("updated", listed.Tags["state"]);
        var found = await service.FindBlobsByTagsAsync("\"state\" = 'updated'").ToListAsync();
        Assert.Contains(found, item => string.Equals(item.BlobContainerName, container.Name, StringComparison.Ordinal) && string.Equals(item.BlobName, blob.Name, StringComparison.Ordinal));

        var tagSas = blob.GenerateSasUri(
            BlobSasPermissions.Tag,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var transport = new HttpClient(application.Server.CreateHandler());
        using (var boundaryRequest = new HttpRequestMessage(HttpMethod.Get, AppendQuery(tagSas, "comp=tags")))
        {
            boundaryRequest.Headers.TryAddWithoutValidation("x-ms-version", "2024-11-04");
            using var boundaryResponse = await transport.SendAsync(boundaryRequest);
            Assert.Equal(HttpStatusCode.OK, boundaryResponse.StatusCode);
            Assert.Contains("<Key>state</Key><Value>updated</Value>",
                await boundaryResponse.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        }

        using var legacyRequest = new HttpRequestMessage(HttpMethod.Get, AppendQuery(tagSas, "comp=tags"));
        legacyRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var legacyResponse = await transport.SendAsync(legacyRequest);
        Assert.Equal(HttpStatusCode.Conflict, legacyResponse.StatusCode);
        Assert.Equal(
            "FeatureVersionMismatch",
            legacyResponse.Headers.GetValues("x-ms-error-code").Single());
    }

    [Fact]
    public async Task HierarchicalNamespaceBlobSnapshotsRequireTheExplicitPreviewCapability()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal17 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-snapshots-disabled-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("folder/plain.bin");
        await blob.UploadAsync(BinaryData.FromString("current"));

        static void AssertUnsupported(RequestFailedException exception)
        {
            Assert.Equal(StatusCodes.Status409Conflict, exception.Status);
            Assert.Equal("BlobOperationNotSupported", exception.ErrorCode);
        }

        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(() => blob.CreateSnapshotAsync()));
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.WithSnapshot("2026-09-22T12:00:00.0000000Z").GetPropertiesAsync()));
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await foreach (var _ in container.GetBlobsAsync(new GetBlobsOptions { States = BlobStates.Snapshots }).ConfigureAwait(false))
            {
            }
        }));
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots)));
        Assert.True((await blob.ExistsAsync()).Value);
    }

    [Fact]
    public async Task HierarchicalNamespaceBlobSnapshotPreviewPreservesSnapshotsWithoutCreatingVersions()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceBlobSnapshotsEnabled"] = "true"
            });
        await using var applicationDisposal18 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var serviceProperties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.SecondAccountName,
            CancellationToken.None);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.SecondAccountName,
            serviceProperties with
            {
                BlobSoftDeleteEnabled = true,
                BlobSoftDeleteRetentionDays = 7,
                VersioningEnabled = true
            },
            CancellationToken.None);

        var container = service.GetBlobContainerClient($"hns-snapshots-preview-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("folder/state.bin");
        await blob.UploadAsync(BinaryData.FromString("before snapshot"));
        var snapshotInfo = (await blob.CreateSnapshotAsync()).Value;
        Assert.Null(snapshotInfo.VersionId);
        var snapshot = blob.WithSnapshot(snapshotInfo.Snapshot);

        await blob.UploadAsync(BinaryData.FromString("after snapshot"), overwrite: true);
        Assert.Equal("before snapshot", (await snapshot.DownloadContentAsync()).Value.Content.ToString());
        Assert.Equal("after snapshot", (await blob.DownloadContentAsync()).Value.Content.ToString());

        await AssertHierarchicalSnapshotFamilyAsync(container, blob, snapshot, snapshotInfo.Snapshot, metadata);
    }

    private static async Task AssertHierarchicalSnapshotFamilyAsync(
        BlobContainerClient container, BlobClient blob, BlobClient snapshot, string snapshotId, MetadataStore metadata)
    {
        var listed = await container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Snapshots,
            Prefix = blob.Name
        }).ToListAsync().ConfigureAwait(false);
        Assert.Contains(listed, item => string.Equals(item.Snapshot, snapshotId, StringComparison.Ordinal));

        var family = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.All(family, item => Assert.Null(item.VersionId));
        Assert.Single(family, item => string.Equals(item.Snapshot, snapshotId, StringComparison.Ordinal));

        await snapshot.DeleteAsync().ConfigureAwait(false);
        var deletedSnapshot = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            versionId: null,
            snapshotId,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(deletedSnapshot);
        Assert.True(deletedSnapshot.IsDeleted);
        Assert.Equal(snapshotId, deletedSnapshot.Snapshot);
        Assert.Null(deletedSnapshot.DeletionId);

        var directorySnapshot = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("folder").CreateSnapshotAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, directorySnapshot.Status);
        Assert.Equal("BlobOperationNotSupported", directorySnapshot.ErrorCode);
    }

    [Fact]
    public async Task HierarchicalNamespaceNeverExposesBlobVersions()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal19 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var container = service.GetBlobContainerClient($"hns-versions-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("state.bin");
        await blob.UploadAsync(BinaryData.FromString("legacy state"));

        var current = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        Assert.NotNull(current);
        var legacyVersionId = MetadataStore.CreateVersionId(DateTimeOffset.UtcNow);
        await metadata.PutBlobRecordAsync(
            current with
            {
                VersionId = legacyVersionId,
                Revision = MetadataStore.NewRevision()
            },
            current.Revision,
            CancellationToken.None);

        Assert.Null((await blob.GetPropertiesAsync()).Value.VersionId);
        var versionListing = await container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Version,
            Prefix = blob.Name
        }).ToListAsync();
        Assert.Null(Assert.Single(versionListing).VersionId);

        var historical = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.WithVersion(legacyVersionId).GetPropertiesAsync());
        Assert.Equal(StatusCodes.Status404NotFound, historical.Status);
        Assert.Equal("BlobNotFound", historical.ErrorCode);

        await blob.UploadAsync(BinaryData.FromString("current HNS state"), overwrite: true);
        var family = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            includeDeleted: true,
            CancellationToken.None);
        Assert.Null(Assert.Single(family).VersionId);
    }

    [Fact]
    public async Task HierarchicalNamespaceEncryptionContextHonorsBlobRestWriteAndReadContracts()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal20 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-encryption-context-{Guid.NewGuid():N}");
        await container.CreateAsync();
        using var transport = new HttpClient(application.Server.CreateHandler());

        const string context = "tenant=alpha;key=v1";
        var blob = container.GetBlobClient("folder/context.bin");
        await AssertHnsEncryptionContextReadsAsync(transport, container, blob, context);

        await AssertHnsEncryptionContextCommitAndOverwriteAsync(transport, container, blob);

        await AssertHnsEncryptionContextInvalidWritesAsync(transport, container, blob, context);
        await AssertFlatEncryptionContextRejectionAsync(context);
    }

    private static HttpRequestMessage CreateEncryptionContextPutRequest(
        BlobClient blob,
        string version,
        string? encryptionContext,
        string payload)
    {
        var request = new HttpRequestMessage(
            HttpMethod.Put,
            blob.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write | BlobSasPermissions.Read,
                DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new StringContent(payload, Encoding.UTF8, "application/octet-stream")
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        if (encryptionContext is not null)
            request.Headers.TryAddWithoutValidation("x-ms-encryption-context", encryptionContext);
        return request;
    }

    private static async Task<HttpResponseMessage> GetEncryptionContextPropertiesAsync(
        HttpClient transport,
        BlobBaseClient blob,
        string version)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Head,
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task<System.Xml.Linq.XDocument> ListEncryptionContextAsync(
        HttpClient transport,
        BlobContainerClient container,
        string version)
    {
        var uri = AppendQuery(
            container.GenerateSasUri(
                BlobContainerSasPermissions.List,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=list");
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
    }

    private static async Task AssertHnsEncryptionContextReadsAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobClient blob,
        string context)
    {
        using (var put = CreateEncryptionContextPutRequest(blob, "2021-08-06", context, "context payload"))
        using (var response = await transport.SendAsync(put).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        using (var properties = await GetEncryptionContextPropertiesAsync(transport, blob, "2021-08-06")
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, properties.StatusCode);
            Assert.Equal(context, GetResponseHeader(properties, "x-ms-encryption-context"));
        }
        using (var legacyProperties = await GetEncryptionContextPropertiesAsync(transport, blob, "2021-06-08")
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, legacyProperties.StatusCode);
            Assert.Null(GetResponseHeaderOrDefault(legacyProperties, "x-ms-encryption-context"));
        }

        var modernListing = await ListEncryptionContextAsync(transport, container, "2021-06-08")
            .ConfigureAwait(false);
        var modernEntry = modernListing.Descendants("Blob").Single(element =>
            string.Equals(element.Element("Name")?.Value, blob.Name, StringComparison.Ordinal));
        Assert.Equal(context, modernEntry.Element("Properties")?.Element("EncryptionContext")?.Value);
        var legacyListing = await ListEncryptionContextAsync(transport, container, "2021-04-10")
            .ConfigureAwait(false);
        var legacyEntry = legacyListing.Descendants("Blob").Single(element =>
            string.Equals(element.Element("Name")?.Value, blob.Name, StringComparison.Ordinal));
        Assert.Null(legacyEntry.Element("Properties")?.Element("EncryptionContext"));
    }

    private static async Task AssertHnsEncryptionContextCommitAndOverwriteAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobClient blob)
    {
        var block = container.GetBlockBlobClient("folder/committed.bin");
        var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("block-0001"));
        using var payload = new MemoryStream(Encoding.UTF8.GetBytes("committed payload"));
        await block.StageBlockAsync(blockId, payload).ConfigureAwait(false);
        var commitUri = AppendQuery(
            block.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write | BlobSasPermissions.Read,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=blocklist");
        using (var commit = new HttpRequestMessage(HttpMethod.Put, commitUri)
        {
            Content = new StringContent(
                $"<BlockList><Latest>{blockId}</Latest></BlockList>",
                Encoding.UTF8,
                "application/xml")
        })
        {
            commit.Headers.TryAddWithoutValidation("x-ms-version", "2021-08-06");
            commit.Headers.TryAddWithoutValidation("x-ms-encryption-context", "commit-context");
            using var response = await transport.SendAsync(commit).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        using (var properties = await GetEncryptionContextPropertiesAsync(transport, block, "2021-08-06")
            .ConfigureAwait(false))
            Assert.Equal("commit-context", GetResponseHeader(properties, "x-ms-encryption-context"));

        using (var overwrite = CreateEncryptionContextPutRequest(blob, "2021-08-06", null, "replacement"))
        using (var response = await transport.SendAsync(overwrite).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        using (var properties = await GetEncryptionContextPropertiesAsync(transport, blob, "2021-08-06")
            .ConfigureAwait(false))
            Assert.Null(GetResponseHeaderOrDefault(properties, "x-ms-encryption-context"));
    }

    private static async Task AssertHnsEncryptionContextInvalidWritesAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobClient blob,
        string context)
    {
        using (var oversized = CreateEncryptionContextPutRequest(blob, "2021-08-06", new string('x', 1025), "rejected"))
        using (var response = await transport.SendAsync(oversized).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.Equal("replacement", (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var legacyTarget = container.GetBlobClient("legacy.bin");
        using (var legacyPut = CreateEncryptionContextPutRequest(legacyTarget, "2021-06-08", context, "rejected"))
        using (var response = await transport.SendAsync(legacyPut).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.False((await legacyTarget.ExistsAsync().ConfigureAwait(false)).Value);

        var copyTarget = container.GetBlobClient("copy.bin");
        using var copy = new HttpRequestMessage(
            HttpMethod.Put,
            copyTarget.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        };
        copy.Headers.TryAddWithoutValidation("x-ms-version", "2021-08-06");
        copy.Headers.TryAddWithoutValidation(
            "x-ms-copy-source",
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)).ToString());
        copy.Headers.TryAddWithoutValidation("x-ms-encryption-context", context);
        using var copyResponse = await transport.SendAsync(copy).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, copyResponse.StatusCode);
        Assert.Equal("UnsupportedHeader", GetResponseHeader(copyResponse, "x-ms-error-code"));
    }

    private static async Task AssertFlatEncryptionContextRejectionAsync(string context)
    {
        var flatApplication = new SavaWebApplicationFactory();
        await using var flatApplicationDisposal21 = flatApplication.ConfigureAwait(false);
        var flatService = CreateClient(flatApplication);
        var flatContainer = flatService.GetBlobContainerClient($"flat-encryption-context-{Guid.NewGuid():N}");
        await flatContainer.CreateAsync().ConfigureAwait(false);
        var flatBlob = flatContainer.GetBlobClient("context.bin");
        using var flatTransport = new HttpClient(flatApplication.Server.CreateHandler());
        using var flatPut = CreateEncryptionContextPutRequest(flatBlob, "2021-08-06", context, "rejected");
        using var flatResponse = await flatTransport.SendAsync(flatPut).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, flatResponse.StatusCode);
        Assert.Equal("InvalidHeaderValue", GetResponseHeader(flatResponse, "x-ms-error-code"));
        Assert.False((await flatBlob.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task HierarchicalNamespaceUpnProjectionValidatesItsDocumentedRequestShape()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal22 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-upn-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("folder/item.bin");
        await blob.UploadAsync(BinaryData.FromString("identity"));
        using var transport = new HttpClient(application.Server.CreateHandler());
        await AssertHnsUpnListShapeAsync(transport, container, blob.Name);

        await AssertHnsUpnHeadShapeAsync(transport, blob);
        await AssertFlatUpnHeadRejectionAsync();
    }

    private static async Task<HttpResponseMessage> SendUpnListAsync(
        HttpClient transport,
        BlobContainerClient container,
        string version,
        string include,
        string upn)
    {
        var uri = AppendQuery(
            container.GenerateSasUri(
                BlobContainerSasPermissions.List,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            $"restype=container&comp=list{include}");
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-upn", upn);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertHnsUpnListShapeAsync(
        HttpClient transport,
        BlobContainerClient container,
        string blobName)
    {
        using (var projected = await SendUpnListAsync(
            transport, container, "2020-06-12", "&include=permissions", "TRUE").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, projected.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(
                await projected.Content.ReadAsStringAsync().ConfigureAwait(false));
            var properties = document.Descendants("Blob").Single(element =>
                string.Equals(element.Element("Name")?.Value, blobName, StringComparison.Ordinal)).Element("Properties");
            Assert.Equal("$superuser", properties?.Element("Owner")?.Value);
            Assert.Equal("$superuser", properties?.Element("Group")?.Value);
            Assert.NotNull(properties?.Element("Acl"));
        }
        using (var missingPermissions = await SendUpnListAsync(
            transport, container, "2023-11-03", string.Empty, "true").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, missingPermissions.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(missingPermissions, "x-ms-error-code"));
        }
        using (var invalid = await SendUpnListAsync(
            transport, container, "2023-11-03", "&include=permissions", "yes").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(invalid, "x-ms-error-code"));
        }
    }

    private static async Task<HttpResponseMessage> SendUpnHeadAsync(
        BlobClient target,
        string version,
        string upn,
        HttpClient client)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Head,
            target.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-upn", upn);
        return await client.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertHnsUpnHeadShapeAsync(HttpClient transport, BlobClient blob)
    {
        using (var projected = await SendUpnHeadAsync(blob, "2023-11-03", "false", transport)
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, projected.StatusCode);
            Assert.Equal("$superuser", GetResponseHeader(projected, "x-ms-owner"));
            Assert.Equal("$superuser", GetResponseHeader(projected, "x-ms-group"));
        }
        using (var legacy = await SendUpnHeadAsync(blob, "2021-08-06", "true", transport)
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, legacy.StatusCode);
            Assert.Equal("FeatureVersionMismatch", GetResponseHeader(legacy, "x-ms-error-code"));
        }
        using (var invalid = await SendUpnHeadAsync(blob, "2023-11-03", "yes", transport)
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(invalid, "x-ms-error-code"));
        }
    }

    private static async Task AssertFlatUpnHeadRejectionAsync()
    {
        var flatApplication = new SavaWebApplicationFactory();
        await using var flatApplicationDisposal23 = flatApplication.ConfigureAwait(false);
        var flatService = CreateClient(flatApplication);
        var flatContainer = flatService.GetBlobContainerClient($"flat-upn-{Guid.NewGuid():N}");
        await flatContainer.CreateAsync().ConfigureAwait(false);
        var flatBlob = flatContainer.GetBlobClient("item.bin");
        await flatBlob.UploadAsync(BinaryData.FromString("flat")).ConfigureAwait(false);
        using var flatTransport = new HttpClient(flatApplication.Server.CreateHandler());
        using var flat = await SendUpnHeadAsync(flatBlob, "2023-11-03", "true", flatTransport)
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, flat.StatusCode);
        Assert.Equal("InvalidHeaderValue", GetResponseHeader(flat, "x-ms-error-code"));
    }

    [Fact]
    public async Task HierarchicalNamespaceBlobExpiryMatchesAzureOperationContracts()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal24 = application.ConfigureAwait(false);
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-expiry-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blobService = application.Services.GetRequiredService<BlobService>();
        using var transport = new HttpClient(application.Server.CreateHandler());

        await AssertExpiryPutValidationAsync(transport, container);

        var (direct, absoluteExpiry) = await AssertDirectExpiryWriteAsync(transport, container, blobService);
        await AssertExpiryReadProjectionAsync(transport, container, direct, absoluteExpiry);

        await AssertExpiryMutationAsync(transport, container, blobService, direct, absoluteExpiry);

        await AssertExpiryBlockCommitAsync(transport, container, blobService, absoluteExpiry);

        await AssertExpiryPutFromUrlAsync(transport, container, blobService);
        await AssertExpiryCopyRejectionAsync(transport, container);

        await AssertFlatExpiryRejectionAsync();
    }

    private static Task<BlobRecord> GetExpiryRecordAsync(
        BlobService blobService,
        BlobContainerClient container,
        string name) =>
        blobService.GetBlobAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);

    private static async Task<HttpResponseMessage> PutExpiryBlobAsync(
        HttpClient client,
        BlobClient target,
        string version,
        string option,
        string? expiryTime,
        byte[] content)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Put,
            target.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent(content)
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        request.Headers.TryAddWithoutValidation("x-ms-expiry-option", option);
        if (expiryTime is not null)
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", expiryTime);
        return await client.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task<HttpResponseMessage> SetBlobExpiryRestAsync(
        HttpClient transport,
        BlobClient target,
        string version,
        string option,
        string? expiryTime)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Put,
            AppendQuery(
                target.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
                "comp=expiry"))
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-expiry-option", option);
        if (expiryTime is not null)
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", expiryTime);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertExpiryPutValidationAsync(HttpClient transport, BlobContainerClient container)
    {
        var oldVersion = container.GetBlobClient("old-version.bin");
        using (var response = await PutExpiryBlobAsync(
            transport, oldVersion, "2021-08-06", "RelativeToNow", "600000", "must not publish"u8.ToArray())
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.False((await oldVersion.ExistsAsync().ConfigureAwait(false)).Value);

        var invalidCreationOption = container.GetBlobClient("relative-to-creation.bin");
        using (var response = await PutExpiryBlobAsync(
            transport, invalidCreationOption, "2023-08-03", "RelativeToCreation", "600000",
            "must not publish"u8.ToArray()).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.False((await invalidCreationOption.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task<(BlobClient Direct, DateTimeOffset AbsoluteExpiry)> AssertDirectExpiryWriteAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobService blobService)
    {
        var absoluteExpiry = DateTimeOffset.UtcNow.AddMinutes(20);
        absoluteExpiry = new DateTimeOffset(
            absoluteExpiry.Year,
            absoluteExpiry.Month,
            absoluteExpiry.Day,
            absoluteExpiry.Hour,
            absoluteExpiry.Minute,
            absoluteExpiry.Second,
            TimeSpan.Zero);
        var direct = container.GetBlobClient("direct.bin");
        using (var response = await PutExpiryBlobAsync(
            transport, direct, "2023-08-03", "Absolute",
            absoluteExpiry.ToString("R", CultureInfo.InvariantCulture), "direct expiry"u8.ToArray())
            .ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        Assert.Equal(absoluteExpiry,
            (await GetExpiryRecordAsync(blobService, container, direct.Name).ConfigureAwait(false)).ExpiresAt);
        return (direct, absoluteExpiry);
    }

    private static async Task<HttpResponseMessage> GetExpiryPropertiesAsync(
        HttpClient transport,
        BlobClient direct,
        string version)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Head,
            direct.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertExpiryReadProjectionAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobClient direct,
        DateTimeOffset absoluteExpiry)
    {
        using (var legacyProperties = await GetExpiryPropertiesAsync(transport, direct, "2019-12-12")
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, legacyProperties.StatusCode);
            Assert.False(legacyProperties.Headers.Contains("x-ms-expiry-time"));
        }
        using (var properties = await GetExpiryPropertiesAsync(transport, direct, "2020-02-10")
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, properties.StatusCode);
            Assert.Equal(absoluteExpiry.ToString("R", CultureInfo.InvariantCulture),
                GetResponseHeader(properties, "x-ms-expiry-time"));
        }

        var listUri = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=list");
        using var request = new HttpRequestMessage(HttpMethod.Get, listUri);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2020-02-10");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var document = System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
        var listed = document.Descendants("Blob").Single(element =>
            string.Equals(element.Element("Name")?.Value, direct.Name, StringComparison.Ordinal));
        Assert.Equal(absoluteExpiry.ToString("R", CultureInfo.InvariantCulture),
            listed.Element("Properties")?.Element("Expiry-Time")?.Value);
    }

    private static async Task AssertExpiryMutationAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobService blobService,
        BlobClient direct,
        DateTimeOffset absoluteExpiry)
    {
        using (var malformed = await SetBlobExpiryRestAsync(
            transport, direct, "2020-02-10", "Absolute",
            DateTimeOffset.UtcNow.AddMinutes(30).ToString("O", CultureInfo.InvariantCulture))
            .ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, malformed.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(malformed, "x-ms-error-code"));
        }
        Assert.Equal(absoluteExpiry,
            (await GetExpiryRecordAsync(blobService, container, direct.Name).ConfigureAwait(false)).ExpiresAt);

        var beforeRelative = await GetExpiryRecordAsync(blobService, container, direct.Name).ConfigureAwait(false);
        using (var response = await SetBlobExpiryRestAsync(
            transport, direct, "2020-02-10", "rElAtIvEtOcReAtIoN", "1800000").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(response.Headers.Contains("ETag"));
            Assert.NotNull(response.Content.Headers.LastModified);
        }
        Assert.Equal(beforeRelative.CreatedAt.AddMinutes(30),
            (await GetExpiryRecordAsync(blobService, container, direct.Name).ConfigureAwait(false)).ExpiresAt);

        using (var invalidNever = await SetBlobExpiryRestAsync(
            transport, direct, "2020-02-10", "NeverExpire", "1").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, invalidNever.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(invalidNever, "x-ms-error-code"));
        }
        using (var cleared = await SetBlobExpiryRestAsync(
            transport, direct, "2020-02-10", "NeverExpire", expiryTime: null).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.OK, cleared.StatusCode);
        Assert.Null((await GetExpiryRecordAsync(blobService, container, direct.Name).ConfigureAwait(false)).ExpiresAt);

        await container.GetBlobClient("folder/child.bin")
            .UploadAsync(BinaryData.FromString("child")).ConfigureAwait(false);
        using var directory = await SetBlobExpiryRestAsync(
            transport, container.GetBlobClient("folder"), "2020-02-10", "RelativeToNow", "600000")
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, directory.StatusCode);
        Assert.Equal("BlobOperationNotSupported", GetResponseHeader(directory, "x-ms-error-code"));
    }

    private static async Task<HttpResponseMessage> CommitExpiryBlocksAsync(
        HttpClient transport,
        BlockBlobClient block,
        string blockId,
        DateTimeOffset blockExpiry,
        bool includeExpiry)
    {
        var mode = includeExpiry ? "Latest" : "Committed";
        using var request = new HttpRequestMessage(
            HttpMethod.Put,
            AppendQuery(
                block.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
                "comp=blocklist"))
        {
            Content = new StringContent(
                $"<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList><{mode}>{blockId}</{mode}></BlockList>",
                Encoding.UTF8,
                "application/xml")
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-08-03");
        if (includeExpiry)
        {
            request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "Absolute");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time",
                blockExpiry.ToString("R", CultureInfo.InvariantCulture));
        }
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertExpiryBlockCommitAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobService blobService,
        DateTimeOffset absoluteExpiry)
    {
        var block = container.GetBlockBlobClient("blocks.bin");
        var blockId = Convert.ToBase64String("expiry-block-0001"u8);
        using var payload = BinaryData.FromString("block expiry").ToStream();
        await block.StageBlockAsync(blockId, payload).ConfigureAwait(false);
        var blockExpiry = absoluteExpiry.AddMinutes(10);
        using (var committed = await CommitExpiryBlocksAsync(
            transport, block, blockId, blockExpiry, includeExpiry: true).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.Created, committed.StatusCode);
        Assert.Equal(blockExpiry,
            (await GetExpiryRecordAsync(blobService, container, block.Name).ConfigureAwait(false)).ExpiresAt);
        using (var recommitted = await CommitExpiryBlocksAsync(
            transport, block, blockId, blockExpiry, includeExpiry: false).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.Created, recommitted.StatusCode);
        Assert.Equal(blockExpiry,
            (await GetExpiryRecordAsync(blobService, container, block.Name).ConfigureAwait(false)).ExpiresAt);
    }

    private static async Task AssertExpiryPutFromUrlAsync(
        HttpClient transport,
        BlobContainerClient container,
        BlobService blobService)
    {
        var urlBytes = "put blob from url expiry"u8.ToArray();
        var source = await LoopbackSource.StartAsync(urlBytes).ConfigureAwait(false);
        await using (source.ConfigureAwait(false))
        {
            var destination = container.GetBlockBlobClient("from-url.bin");
            using var request = new HttpRequestMessage(
                HttpMethod.Put,
                destination.GenerateSasUri(
                    BlobSasPermissions.Create | BlobSasPermissions.Write,
                    DateTimeOffset.UtcNow.AddMinutes(5)))
            {
                Content = new ByteArrayContent([])
            };
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-08-03");
            request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.Uri.ToString());
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "RelativeToNow");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", "1200000");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.Equal(urlBytes,
                (await destination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
            Assert.NotNull((await GetExpiryRecordAsync(blobService, container, destination.Name)
                .ConfigureAwait(false)).ExpiresAt);
        }
    }

    private static async Task AssertExpiryCopyRejectionAsync(HttpClient transport, BlobContainerClient container)
    {
        var copyTarget = container.GetBlobClient("copy-rejects-expiry.bin");
        using var request = new HttpRequestMessage(
            HttpMethod.Put,
            copyTarget.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-08-03");
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", "https://source.invalid/blob");
        request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "RelativeToNow");
        request.Headers.TryAddWithoutValidation("x-ms-expiry-time", "600000");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("UnsupportedHeader", GetResponseHeader(response, "x-ms-error-code"));
    }

    private static async Task AssertFlatExpiryRejectionAsync()
    {
        var flatApplication = new SavaWebApplicationFactory();
        await using (flatApplication.ConfigureAwait(false))
        {
            var flatService = CreateClient(flatApplication);
            var flatContainer = flatService.GetBlobContainerClient($"flat-expiry-{Guid.NewGuid():N}");
            await flatContainer.CreateAsync().ConfigureAwait(false);
            var flat = flatContainer.GetBlobClient("flat.bin");
            using var flatTransport = new HttpClient(flatApplication.Server.CreateHandler());
            using (var response = await PutExpiryBlobAsync(
                flatTransport, flat, "2023-08-03", "RelativeToNow", "600000", "flat"u8.ToArray())
                .ConfigureAwait(false))
            {
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", GetResponseHeader(response, "x-ms-error-code"));
            }
            Assert.False((await flat.ExistsAsync().ConfigureAwait(false)).Value);
            await flat.UploadAsync(BinaryData.FromString("flat")).ConfigureAwait(false);
            var flatRecord = await flatApplication.Services.GetRequiredService<BlobService>().GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                flatContainer.Name,
                flat.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None).ConfigureAwait(false);
            var flatBusinessFailure = await Assert.ThrowsAsync<AzureStorageException>(() =>
                flatApplication.Services.GetRequiredService<BlobService>().SetExpiryAsync(
                    flatRecord,
                    DateTimeOffset.UtcNow.AddMinutes(10),
                    CancellationToken.None)).ConfigureAwait(false);
            Assert.Equal("BlobOperationNotSupported", flatBusinessFailure.ErrorCode);

            var flatExpiryUri = AppendQuery(
                flat.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
                "comp=expiry");
            using var request = new HttpRequestMessage(HttpMethod.Put, flatExpiryUri)
            {
                Content = new ByteArrayContent([])
            };
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-02-10");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "RelativeToNow");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", "600000");
            using var expiryResponse = await flatTransport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, expiryResponse.StatusCode);
            Assert.Equal("BlobOperationNotSupported", GetResponseHeader(expiryResponse, "x-ms-error-code"));
        }
    }

    [Fact]
    public async Task BlobTagSasRequiresTheDedicatedTagPermission()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"tag-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("tagged.bin");
        await blob.UploadAsync(
            BinaryData.FromString("tag permission payload"),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "initial" }
            });

        var readOnly = CreateBlobClient(
            factory,
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        var deniedRead = await Assert.ThrowsAsync<RequestFailedException>(() => readOnly.GetTagsAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedRead.Status);
        Assert.Equal("AuthorizationPermissionMismatch", deniedRead.ErrorCode);

        var writeOnly = CreateBlobClient(
            factory,
            blob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)));
        var deniedWrite = await Assert.ThrowsAsync<RequestFailedException>(() =>
            writeOnly.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "wrong" }));
        Assert.Equal(StatusCodes.Status403Forbidden, deniedWrite.Status);
        Assert.Equal("AuthorizationPermissionMismatch", deniedWrite.ErrorCode);

        var tagOnly = CreateBlobClient(
            factory,
            blob.GenerateSasUri(BlobSasPermissions.Tag, DateTimeOffset.UtcNow.AddMinutes(5)));
        Assert.Equal("initial", (await tagOnly.GetTagsAsync()).Value.Tags["state"]);
        await tagOnly.SetTagsAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "updated" });
        Assert.Equal("updated", (await tagOnly.GetTagsAsync()).Value.Tags["state"]);

        var deniedContentRead = await Assert.ThrowsAsync<RequestFailedException>(() =>
            tagOnly.DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedContentRead.Status);
        Assert.Equal("AuthorizationPermissionMismatch", deniedContentRead.ErrorCode);
    }

    [Fact]
    public async Task ObjectScopedAccountSasAuthorizesFindAndBatchParentRequests()
    {
        var owner = CreateClient(factory);
        var container = owner.GetBlobContainerClient($"account-object-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var tagged = container.GetBlobClient("tagged.bin");
        await tagged.UploadAsync(
            BinaryData.FromString("tagged"),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = "account-object" }
            });
        var deleteTarget = container.GetBlobClient("delete.bin");
        await deleteTarget.UploadAsync(BinaryData.FromString("delete"));
        var tierTarget = container.GetBlobClient("tier.bin");
        await tierTarget.UploadAsync(BinaryData.FromString("tier"));

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var builder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Object,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(
            AccountSasPermissions.Read |
            AccountSasPermissions.Write |
            AccountSasPermissions.Delete |
            AccountSasPermissions.Filter);
        var endpoint = new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost" +
            $"?{builder.ToSasQueryParameters(credential)}");
        var sasService = new BlobServiceClient(
            endpoint,
            new BlobClientOptions
            {
                Transport = new HttpClientTransport(factory.Server.CreateHandler()),
                Retry = { MaxRetries = 0 }
            });

        var matches = new List<string>();
        await foreach (var item in sasService.FindBlobsByTagsAsync("\"scope\" = 'account-object'"))
            matches.Add($"{item.BlobContainerName}/{item.BlobName}");
        Assert.Equal([$"{container.Name}/{tagged.Name}"], matches);

        await AssertObjectScopedAccountSasBatchAsync(sasService, container, deleteTarget, tierTarget);

        var deniedServiceOperation = await Assert.ThrowsAsync<RequestFailedException>(() =>
            sasService.GetPropertiesAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedServiceOperation.Status);
        Assert.Equal("AuthorizationResourceTypeMismatch", deniedServiceOperation.ErrorCode);
    }

    private static async Task AssertObjectScopedAccountSasBatchAsync(
        BlobServiceClient sasService,
        BlobContainerClient container,
        BlobClient deleteTarget,
        BlobClient tierTarget)
    {
        var serviceBatchClient = sasService.GetBlobBatchClient();
        using (var batch = serviceBatchClient.CreateBatch())
        {
            var deleted = batch.DeleteBlob(container.Name, deleteTarget.Name);
            var submitted = await serviceBatchClient.SubmitBatchAsync(batch).ConfigureAwait(false);
            Assert.Equal(StatusCodes.Status202Accepted, submitted.Status);
            Assert.Equal(StatusCodes.Status202Accepted, deleted.Status);
        }
        Assert.False((await deleteTarget.ExistsAsync().ConfigureAwait(false)).Value);

        var containerBatchClient = sasService
            .GetBlobContainerClient(container.Name)
            .GetBlobBatchClient();
        using (var batch = containerBatchClient.CreateBatch())
        {
            var tiered = batch.SetBlobAccessTier(container.Name, tierTarget.Name, AccessTier.Cool);
            var submitted = await containerBatchClient.SubmitBatchAsync(batch).ConfigureAwait(false);
            Assert.Equal(StatusCodes.Status202Accepted, submitted.Status);
            Assert.Equal(StatusCodes.Status200OK, tiered.Status);
        }
        Assert.Equal(AccessTier.Cool, (await tierTarget.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);
    }

    [Fact]
    public async Task ServiceAnalyticsPropertiesRoundTripAndPartialUpdatesPreserveOtherGroups()
    {
        var service = CreateClient(factory);
        var original = (await service.GetPropertiesAsync()).Value;
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var sasBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Service,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        sasBuilder.SetPermissions(AccountSasPermissions.Read | AccountSasPermissions.Write);
        var sas = sasBuilder.ToSasQueryParameters(credential);
        var propertiesUri = new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/" +
            $"?restype=service&comp=properties&{sas}");

        try
        {
            await ConfigureAndAssertAnalyticsPropertiesAsync(service);
            using var transport = new HttpClient(factory.Server.CreateHandler());
            await AssertAnalyticsPartialUpdateAsync(service, transport, propertiesUri);
            await AssertAnalyticsInvalidAndLegacyRequestsAsync(transport, propertiesUri);
        }
        finally
        {
            await service.SetPropertiesAsync(original);
        }
    }

    private static async Task ConfigureAndAssertAnalyticsPropertiesAsync(BlobServiceClient service)
    {
        await ConfigureAnalyticsPropertiesAsync(service).ConfigureAwait(false);
        var roundTrip = (await service.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal("1.0", roundTrip.Logging.Version);
        Assert.True(roundTrip.Logging.Delete);
        Assert.True(roundTrip.Logging.Read);
        Assert.False(roundTrip.Logging.Write);
        Assert.True(roundTrip.Logging.RetentionPolicy.Enabled);
        Assert.Equal(11, roundTrip.Logging.RetentionPolicy.Days);
        Assert.True(roundTrip.HourMetrics.Enabled);
        Assert.True(roundTrip.HourMetrics.IncludeApis);
        Assert.Equal(12, roundTrip.HourMetrics.RetentionPolicy.Days);
        Assert.True(roundTrip.MinuteMetrics.Enabled);
        Assert.False(roundTrip.MinuteMetrics.IncludeApis);
        Assert.False(roundTrip.MinuteMetrics.RetentionPolicy.Enabled);
        Assert.Equal("https://example.test", Assert.Single(roundTrip.Cors).AllowedOrigins);
    }

    private static async Task ConfigureAnalyticsPropertiesAsync(BlobServiceClient service)
    {
        var configured = (await service.GetPropertiesAsync().ConfigureAwait(false)).Value;
        configured.Logging = new BlobAnalyticsLogging
        {
            Version = "1.0",
            Delete = true,
            Read = true,
            Write = false,
            RetentionPolicy = new BlobRetentionPolicy { Enabled = true, Days = 11 }
        };
        configured.HourMetrics = new BlobMetrics
        {
            Version = "1.0",
            Enabled = true,
            IncludeApis = true,
            RetentionPolicy = new BlobRetentionPolicy { Enabled = true, Days = 12 }
        };
        configured.MinuteMetrics = new BlobMetrics
        {
            Version = "1.0",
            Enabled = true,
            IncludeApis = false,
            RetentionPolicy = new BlobRetentionPolicy { Enabled = false }
        };
        configured.Cors.Clear();
        configured.Cors.Add(new BlobCorsRule
        {
            AllowedOrigins = "https://example.test",
            AllowedMethods = "GET,HEAD",
            AllowedHeaders = "x-ms-meta-*",
            ExposedHeaders = "x-ms-request-id",
            MaxAgeInSeconds = 321
        });
        await service.SetPropertiesAsync(configured).ConfigureAwait(false);
    }

    private static async Task AssertAnalyticsPartialUpdateAsync(
        BlobServiceClient service,
        HttpClient transport,
        Uri propertiesUri)
    {
        using (var partialUpdate = new HttpRequestMessage(HttpMethod.Put, propertiesUri)
        {
            Content = new StringContent(
                """
                <?xml version="1.0" encoding="utf-8"?>
                <StorageServiceProperties>
                  <Logging>
                    <Version>1.0</Version>
                    <Delete>false</Delete>
                    <Read>false</Read>
                    <Write>true</Write>
                    <RetentionPolicy><Enabled>false</Enabled></RetentionPolicy>
                  </Logging>
                </StorageServiceProperties>
                """,
                Encoding.UTF8,
                "application/xml")
        })
        {
            partialUpdate.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(partialUpdate).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        }

        var roundTrip = (await service.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.False(roundTrip.Logging.Delete);
        Assert.False(roundTrip.Logging.Read);
        Assert.True(roundTrip.Logging.Write);
        Assert.False(roundTrip.Logging.RetentionPolicy.Enabled);
        Assert.True(roundTrip.HourMetrics.Enabled);
        Assert.Equal(12, roundTrip.HourMetrics.RetentionPolicy.Days);
        Assert.Equal("https://example.test", Assert.Single(roundTrip.Cors).AllowedOrigins);
    }

    private static async Task AssertAnalyticsInvalidAndLegacyRequestsAsync(HttpClient transport, Uri propertiesUri)
    {
        using (var invalidMetrics = new HttpRequestMessage(HttpMethod.Put, propertiesUri)
        {
            Content = new StringContent(
                """
                <StorageServiceProperties>
                  <HourMetrics>
                    <Version>1.0</Version>
                    <Enabled>true</Enabled>
                    <RetentionPolicy><Enabled>false</Enabled></RetentionPolicy>
                  </HourMetrics>
                </StorageServiceProperties>
                """,
                Encoding.UTF8,
                "application/xml")
        })
        {
            invalidMetrics.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(invalidMetrics).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidXmlDocument", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using var legacyGet = new HttpRequestMessage(HttpMethod.Get, propertiesUri);
        legacyGet.Headers.TryAddWithoutValidation("x-ms-version", "2012-02-12");
        using var legacyResponse = await transport.SendAsync(legacyGet).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, legacyResponse.StatusCode);
        var xml = await legacyResponse.Content.ReadAsStringAsync().ConfigureAwait(false);
        Assert.Contains("<Logging>", xml, StringComparison.Ordinal);
        Assert.Contains("<Metrics>", xml, StringComparison.Ordinal);
        Assert.DoesNotContain("<HourMetrics>", xml, StringComparison.Ordinal);
        Assert.DoesNotContain("<MinuteMetrics>", xml, StringComparison.Ordinal);
        Assert.DoesNotContain("<Cors>", xml, StringComparison.Ordinal);
        Assert.DoesNotContain("<DeleteRetentionPolicy>", xml, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StorageAnalyticsLoggingMaterializesProtectedAzureFormatSystemBlobs()
    {
        var application = new SavaWebApplicationFactory();
        await using var applicationDisposal25 = application.ConfigureAwait(false);
        var service = CreateClient(application);
        var configured = (await service.GetPropertiesAsync()).Value;
        configured.Logging = new BlobAnalyticsLogging
        {
            Version = "2.0",
            Delete = true,
            Read = true,
            Write = true,
            RetentionPolicy = new BlobRetentionPolicy { Enabled = true, Days = 7 }
        };
        await service.SetPropertiesAsync(configured);

        var container = service.GetBlobContainerClient($"analytics-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("payload.txt");
        await blob.UploadAsync(BinaryData.FromString("analytics payload"));
        Assert.Equal("analytics payload", (await blob.DownloadContentAsync()).Value.Content.ToString());
        await Task.WhenAll(Enumerable.Range(0, 8).Select(index =>
            container.GetBlobClient($"parallel-{index}.txt").UploadAsync(BinaryData.FromString($"payload-{index}"))));
        await blob.DeleteAsync();

        var logs = service.GetBlobContainerClient(StorageAnalyticsService.LogsContainerName);
        Assert.True((await logs.ExistsAsync()).Value);
        var logItems = await AssertAnalyticsLogItemsAsync(logs);
        await AssertAnalyticsSystemVisibilityAndProtectionAsync(application, service, logs, logItems[0].Name);
    }

    private static async Task<List<BlobItem>> AssertAnalyticsLogItemsAsync(BlobContainerClient logs)
    {
        var logItems = new List<BlobItem>();
        await foreach (var item in logs.GetBlobsAsync(new GetBlobsOptions { Traits = BlobTraits.Metadata })
            .ConfigureAwait(false))
            logItems.Add(item);
        Assert.NotEmpty(logItems);
        Assert.All(logItems, item =>
        {
            Assert.Matches("^blob/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{4}/[0-9]{6}\\.log$", item.Name);
            Assert.Equal("2.0", item.Metadata["LogVersion"]);
            Assert.Contains(item.Metadata["LogType"], AnalyticsLogTypes, StringComparer.Ordinal);
            Assert.EndsWith("Z", item.Metadata["StartTime"], StringComparison.Ordinal);
            Assert.EndsWith("Z", item.Metadata["EndTime"], StringComparison.Ordinal);
        });

        var operations = new List<string>();
        foreach (var item in logItems)
        {
            var text = (await logs.GetBlobClient(item.Name).DownloadContentAsync().ConfigureAwait(false))
                .Value.Content.ToString();
            var fields = ParseAnalyticsLogFields(Assert.Single(text.Split('\n', StringSplitOptions.RemoveEmptyEntries)));
            Assert.Equal(38, fields.Count);
            Assert.Equal("2.0", fields[0]);
            Assert.Equal("blob", fields[10]);
            Assert.Equal(SavaWebApplicationFactory.AccountName, fields[9]);
            Assert.DoesNotContain(SavaWebApplicationFactory.AccountKey, text, StringComparison.Ordinal);
            operations.Add(fields[2]);
        }
        Assert.Contains("SetBlobServiceProperties", operations, StringComparer.Ordinal);
        Assert.Contains("CreateContainer", operations, StringComparer.Ordinal);
        Assert.Contains("PutBlob", operations, StringComparer.Ordinal);
        Assert.True(operations.Count(operation => string.Equals(operation, "PutBlob", StringComparison.Ordinal)) >= 9);
        Assert.Contains("GetBlob", operations, StringComparer.Ordinal);
        Assert.Contains("DeleteBlob", operations, StringComparer.Ordinal);
        return logItems;
    }

    private static async Task AssertAnalyticsSystemVisibilityAndProtectionAsync(
        SavaWebApplicationFactory application,
        BlobServiceClient service,
        BlobContainerClient logs,
        string logItemName)
    {
        var ordinaryContainers = new List<string>();
        await foreach (var item in service.GetBlobContainersAsync().ConfigureAwait(false))
            ordinaryContainers.Add(item.Name);
        Assert.DoesNotContain(StorageAnalyticsService.LogsContainerName, ordinaryContainers, StringComparer.Ordinal);

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var sasBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Service,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        sasBuilder.SetPermissions(AccountSasPermissions.List);
        var systemListUri = new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/" +
            $"?comp=list&include=system&{sasBuilder.ToSasQueryParameters(credential)}");
        using (var transport = new HttpClient(application.Server.CreateHandler()))
        using (var systemList = await transport.GetAsync(systemListUri).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, systemList.StatusCode);
            Assert.Contains(
                $"<Name>{StorageAnalyticsService.LogsContainerName}</Name>",
                await systemList.Content.ReadAsStringAsync().ConfigureAwait(false),
                StringComparison.Ordinal);
        }

        var deniedWrite = await Assert.ThrowsAsync<RequestFailedException>(() =>
            logs.GetBlobClient("manual.log").UploadAsync(BinaryData.FromString("not service-owned")))
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedWrite.Status);
        Assert.Equal("AuthorizationPermissionMismatch", deniedWrite.ErrorCode);

        var deniedContainerDelete = await Assert.ThrowsAsync<RequestFailedException>(() => logs.DeleteAsync())
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedContainerDelete.Status);
        Assert.Equal("ContainerOperationFailure", deniedContainerDelete.ErrorCode);

        Assert.True((await logs.GetBlobClient(logItemName).DeleteIfExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task StorageAnalyticsRetentionPurgesExpiredLogBlobsWhenLoggingIsDisabledForReads()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 20, 8, 30, 0, TimeSpan.Zero));
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal));
        await using var applicationDisposal26 = application.ConfigureAwait(false);
        var service = CreateClient(application);
        var configured = (await service.GetPropertiesAsync()).Value;
        configured.Logging = new BlobAnalyticsLogging
        {
            Version = "1.0",
            Delete = false,
            Read = false,
            Write = true,
            RetentionPolicy = new BlobRetentionPolicy { Enabled = true, Days = 1 }
        };
        await service.SetPropertiesAsync(configured);
        await service.GetBlobContainerClient($"retention-{Guid.NewGuid():N}").CreateAsync();

        var logs = service.GetBlobContainerClient(StorageAnalyticsService.LogsContainerName);
        var before = new List<string>();
        await foreach (var item in logs.GetBlobsAsync())
            before.Add(item.Name);
        Assert.NotEmpty(before);

        clock.Advance(TimeSpan.FromDays(2));
        await application.Services.GetRequiredService<BlobService>().RunMaintenanceAsync(CancellationToken.None);

        var after = new List<string>();
        await foreach (var item in logs.GetBlobsAsync())
            after.Add(item.Name);
        Assert.Empty(after);
    }

    [Fact]
    public async Task BlobServicePropertiesExcludeAndRejectControlPlaneOnlySettings()
    {
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var original = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var sasBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Service,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        sasBuilder.SetPermissions(AccountSasPermissions.Read | AccountSasPermissions.Write);
        var sas = sasBuilder.ToSasQueryParameters(credential);
        var propertiesUri = new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/" +
            $"?restype=service&comp=properties&{sas}");

        using var transport = new HttpClient(factory.Server.CreateHandler());
        try
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original with
                {
                    VersioningEnabled = true,
                    ContainerSoftDeleteEnabled = true,
                    ContainerSoftDeleteRetentionDays = 19
                },
                CancellationToken.None);

            await AssertControlPlanePropertiesExcludedAsync(transport, propertiesUri);
            await AssertControlPlanePropertiesRejectedAsync(transport, propertiesUri);

            var unchanged = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            Assert.True(unchanged.VersioningEnabled);
            Assert.True(unchanged.ContainerSoftDeleteEnabled);
            Assert.Equal(19, unchanged.ContainerSoftDeleteRetentionDays);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original,
                CancellationToken.None);
        }
    }

    private static async Task<(BlobContainerClient Source, string CommittedName, string StagedName,
        string BlockId, string DeletedVersion)> CreateRestoreFixtureAsync(BlobServiceClient service, string sourceName)
    {
        var source = service.GetBlobContainerClient(sourceName);
        await source.CreateAsync(
            PublicAccessType.None,
            new Dictionary<string, string>(StringComparer.Ordinal) { ["purpose"] = "restore" })
            .ConfigureAwait(false);
        var committed = source.GetBlobClient("committed.bin");
        await committed.UploadAsync(
            BinaryData.FromString("preserved container content"),
            new BlobUploadOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "committed" },
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["kind"] = "restored" }
            }).ConfigureAwait(false);
        var staged = source.GetBlockBlobClient("staged.bin");
        var blockId = Convert.ToBase64String("restore-block"u8);
        using var payload = BinaryData.FromString("uncommitted content").ToStream();
        await staged.StageBlockAsync(blockId, payload).ConfigureAwait(false);
        await source.DeleteAsync().ConfigureAwait(false);

        var deleted = await service.GetBlobContainersAsync(
                states: BlobContainerStates.Deleted,
                prefix: sourceName)
            .SingleAsync().ConfigureAwait(false);
        Assert.True(deleted.IsDeleted);
        Assert.False(string.IsNullOrWhiteSpace(deleted.VersionId));
        return (source, committed.Name, staged.Name, blockId, deleted.VersionId);
    }

    private static async Task AssertDestinationContainerRestoreAsync(
        BlobServiceClient service,
        BlobContainerClient source,
        string destinationName,
        string committedName,
        string stagedName,
        string blockId,
        string deletedVersion)
    {
#pragma warning disable AZC0015
        var restore = await service.UndeleteBlobContainerAsync(
            source.Name, deletedVersion, destinationName, CancellationToken.None).ConfigureAwait(false);
#pragma warning restore AZC0015
        Assert.Equal(201, restore.GetRawResponse().Status);
        Assert.Equal(destinationName, restore.Value.Name);
        Assert.False(restore.GetRawResponse().Headers.TryGetValue("ETag", out _));
        Assert.False(restore.GetRawResponse().Headers.TryGetValue("Last-Modified", out _));
        Assert.True(restore.GetRawResponse().Headers.TryGetValue("Content-Length", out var restoreLength));
        Assert.Equal("0", restoreLength);

        Assert.False((await source.ExistsAsync().ConfigureAwait(false)).Value);
        Assert.Empty(await service.GetBlobContainersAsync(
                states: BlobContainerStates.Deleted,
                prefix: source.Name)
            .ToListAsync().ConfigureAwait(false));

        var destination = service.GetBlobContainerClient(destinationName);
        var destinationProperties = (await destination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal("restore", destinationProperties.Metadata["purpose"]);
        var restoredBlob = destination.GetBlobClient(committedName);
        Assert.Equal("preserved container content",
            (await restoredBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        Assert.Equal("committed", (await restoredBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["state"]);
        Assert.Equal("restored", (await restoredBlob.GetTagsAsync().ConfigureAwait(false)).Value.Tags["kind"]);
        var tagged = await service.FindBlobsByTagsAsync("\"kind\" = 'restored'").ToListAsync().ConfigureAwait(false);
        var restoredTag = Assert.Single(tagged, item =>
            string.Equals(item.BlobName, committedName, StringComparison.Ordinal));
        Assert.Equal(destinationName, restoredTag.BlobContainerName);
        var restoredBlocks = (await destination.GetBlockBlobClient(stagedName)
            .GetBlockListAsync(BlockListTypes.Uncommitted).ConfigureAwait(false)).Value;
        Assert.Equal(blockId, Assert.Single(restoredBlocks.UncommittedBlocks).Name);
    }

    private static async Task AssertSameNameContainerRestoreAsync(BlobServiceClient service)
    {
        var sameName = service.GetBlobContainerClient($"restore-same-{Guid.NewGuid():N}");
        await sameName.CreateAsync().ConfigureAwait(false);
        await sameName.GetBlobClient("same.bin")
            .UploadAsync(BinaryData.FromString("same-name content")).ConfigureAwait(false);
        await sameName.DeleteAsync().ConfigureAwait(false);
        var sameDeleted = await service.GetBlobContainersAsync(
                states: BlobContainerStates.Deleted,
                prefix: sameName.Name)
            .SingleAsync().ConfigureAwait(false);
        var sameRestore = await service.UndeleteBlobContainerAsync(sameName.Name, sameDeleted.VersionId)
            .ConfigureAwait(false);
        Assert.Equal(201, sameRestore.GetRawResponse().Status);
        Assert.Equal("same-name content",
            (await sameName.GetBlobClient("same.bin").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
    }

    private static Uri CreateRestoreSasUri(
        StorageSharedKeyCredential credential,
        string target,
        AccountSasPermissions permissions)
    {
        var builder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(permissions);
        return new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{target}" +
            $"?restype=container&comp=undelete&{builder.ToSasQueryParameters(credential)}");
    }

    private static async Task AssertInvalidContainerRestoreRequestsAsync(SavaWebApplicationFactory application)
    {
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        using var transport = new HttpClient(application.Server.CreateHandler());
        await AssertMissingRestoreHeadersAsync(transport, credential).ConfigureAwait(false);
        await AssertRestoreSasPermissionAsync(transport, credential).ConfigureAwait(false);
    }

    private static async Task AssertMissingRestoreHeadersAsync(
        HttpClient transport,
        StorageSharedKeyCredential credential)
    {
        using (var missingName = new HttpRequestMessage(
            HttpMethod.Put,
            CreateRestoreSasUri(credential, $"missing-name-{Guid.NewGuid():N}", AccountSasPermissions.Write))
        {
            Content = new ByteArrayContent([])
        })
        {
            missingName.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            missingName.Headers.TryAddWithoutValidation("x-ms-deleted-container-version", "missing");
            using var response = await transport.SendAsync(missingName).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
            Assert.Contains("<HeaderName>x-ms-deleted-container-name</HeaderName>",
                await response.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
        }
        using var missingVersion = new HttpRequestMessage(
            HttpMethod.Put,
            CreateRestoreSasUri(credential, $"missing-version-{Guid.NewGuid():N}", AccountSasPermissions.Write))
        {
            Content = new ByteArrayContent([])
        };
        missingVersion.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        missingVersion.Headers.TryAddWithoutValidation("x-ms-deleted-container-name", "missing");
        using var missingVersionResponse = await transport.SendAsync(missingVersion).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, missingVersionResponse.StatusCode);
        Assert.Equal("MissingRequiredHeader", missingVersionResponse.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task AssertRestoreSasPermissionAsync(
        HttpClient transport,
        StorageSharedKeyCredential credential)
    {
        using var createOnly = new HttpRequestMessage(
            HttpMethod.Put,
            CreateRestoreSasUri(credential, $"create-only-{Guid.NewGuid():N}", AccountSasPermissions.Create))
        {
            Content = new ByteArrayContent([])
        };
        createOnly.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        createOnly.Headers.TryAddWithoutValidation("x-ms-deleted-container-name", "missing");
        createOnly.Headers.TryAddWithoutValidation("x-ms-deleted-container-version", "missing");
        using var response = await transport.SendAsync(createOnly).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("AuthorizationPermissionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task AssertContainerRestoreCollisionAsync(BlobServiceClient service)
    {
        var collisionSource = service.GetBlobContainerClient($"restore-collision-source-{Guid.NewGuid():N}");
        var collisionDestination = service.GetBlobContainerClient($"restore-collision-target-{Guid.NewGuid():N}");
        await collisionSource.CreateAsync().ConfigureAwait(false);
        await collisionSource.GetBlobClient("retained.bin")
            .UploadAsync(BinaryData.FromString("retained")).ConfigureAwait(false);
        await collisionSource.DeleteAsync().ConfigureAwait(false);
        await collisionDestination.CreateAsync().ConfigureAwait(false);
        var collisionDeleted = await service.GetBlobContainersAsync(
                states: BlobContainerStates.Deleted,
                prefix: collisionSource.Name)
            .SingleAsync().ConfigureAwait(false);
#pragma warning disable AZC0015
        var collision = await Assert.ThrowsAsync<RequestFailedException>(() =>
            service.UndeleteBlobContainerAsync(
                collisionSource.Name,
                collisionDeleted.VersionId,
                collisionDestination.Name,
                CancellationToken.None)).ConfigureAwait(false);
#pragma warning restore AZC0015
        Assert.Equal(409, collision.Status);
        Assert.Equal("ContainerAlreadyExists", collision.ErrorCode);
        Assert.Single(await service.GetBlobContainersAsync(
                states: BlobContainerStates.Deleted,
                prefix: collisionSource.Name)
            .ToListAsync().ConfigureAwait(false));
    }

    private static async Task AssertConsumedContainerRestoreAsync(
        BlobServiceClient service,
        string sourceName,
        string deletedVersion)
    {
        var consumed = await Assert.ThrowsAsync<RequestFailedException>(() =>
            service.UndeleteBlobContainerAsync(sourceName, deletedVersion)).ConfigureAwait(false);
        Assert.Equal(404, consumed.Status);
        Assert.Equal("ContainerNotFound", consumed.ErrorCode);
    }

    private static async Task AssertControlPlanePropertiesExcludedAsync(HttpClient transport, Uri propertiesUri)
    {
        using var get = new HttpRequestMessage(HttpMethod.Get, propertiesUri);
        get.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
        using var response = await transport.SendAsync(get).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var xml = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        Assert.DoesNotContain("<ContainerDeleteRetentionPolicy>", xml, StringComparison.Ordinal);
        Assert.DoesNotContain("<IsVersioningEnabled>", xml, StringComparison.Ordinal);
    }

    private static async Task AssertControlPlanePropertiesRejectedAsync(HttpClient transport, Uri propertiesUri)
    {
        foreach (var xml in new[]
                 {
                     "<StorageServiceProperties />",
                     "<StorageServiceProperties><ContainerDeleteRetentionPolicy><Enabled>true</Enabled><Days>19</Days></ContainerDeleteRetentionPolicy></StorageServiceProperties>",
                     "<StorageServiceProperties><IsVersioningEnabled>false</IsVersioningEnabled></StorageServiceProperties>"
                 })
        {
            using var put = new HttpRequestMessage(HttpMethod.Put, propertiesUri)
            {
                Content = new StringContent(xml, Encoding.UTF8, "application/xml")
            };
            put.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            using var response = await transport.SendAsync(put).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidXmlDocument", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    [Fact]
    public async Task ServiceVersionSelectionMatchesSharedKeySasBearerAndDefaultRules()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var original = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var container = service.GetBlobContainerClient($"versions-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("selection.txt");
        await blob.UploadAsync(BinaryData.FromString("service version selection"));
        var blobUri = blob.Uri;
        using var transport = new HttpClient(factory.Server.CreateHandler());

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var accountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Service,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountSas.SetPermissions(AccountSasPermissions.Write);
        var servicePropertiesUri = new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/" +
            $"?restype=service&comp=properties&{accountSas.ToSasQueryParameters(credential)}");

        try
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original with { DefaultServiceVersion = null },
                CancellationToken.None);
            await AssertInvalidServiceVersionSelectionsAsync(transport, blobUri, servicePropertiesUri, metadata);

            await AssertDefaultServiceVersionAsync(transport, blobUri, servicePropertiesUri);

            await AssertSasServiceVersionSelectionAsync(transport, blob);

            await AssertBearerServiceVersionSelectionAsync(transport, blobUri);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original,
                CancellationToken.None);
        }
    }

    private static async Task AssertInvalidServiceVersionSelectionsAsync(
        HttpClient transport,
        Uri blobUri,
        Uri servicePropertiesUri,
        MetadataStore metadata)
    {
        using (var unsupported = new HttpRequestMessage(HttpMethod.Head, blobUri))
        {
            unsupported.Headers.TryAddWithoutValidation("x-ms-version", "9999-01-01");
            using var response = await transport.SendAsync(unsupported).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var missing = new HttpRequestMessage(HttpMethod.Head, blobUri))
        {
            AddSharedKeyLiteAuthorization(missing);
            using var response = await transport.SendAsync(missing).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            await AssertVersionedErrorAsync(response, "MissingRequiredHeader").ConfigureAwait(false);
        }

        using (var invalidDefault = new HttpRequestMessage(HttpMethod.Put, servicePropertiesUri)
        {
            Content = new StringContent(
                "<StorageServiceProperties><DefaultServiceVersion>9999-01-01</DefaultServiceVersion></StorageServiceProperties>",
                Encoding.UTF8,
                "application/xml")
        })
        {
            invalidDefault.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(invalidDefault).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidXmlDocument", response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.Null((await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None).ConfigureAwait(false)).DefaultServiceVersion);
    }

    private static async Task AssertDefaultServiceVersionAsync(
        HttpClient transport,
        Uri blobUri,
        Uri servicePropertiesUri)
    {
        using (var setDefault = new HttpRequestMessage(HttpMethod.Put, servicePropertiesUri)
        {
            Content = new StringContent(
                "<StorageServiceProperties><DefaultServiceVersion>2018-03-28</DefaultServiceVersion></StorageServiceProperties>",
                Encoding.UTF8,
                "application/xml")
        })
        {
            setDefault.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(setDefault).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        }

        using var defaulted = new HttpRequestMessage(HttpMethod.Head, blobUri);
        AddSharedKeyLiteAuthorization(defaulted);
        using var defaultedResponse = await transport.SendAsync(defaulted).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, defaultedResponse.StatusCode);
        Assert.Equal("2018-03-28", defaultedResponse.Headers.GetValues("x-ms-version").Single());
        Assert.True(defaultedResponse.Headers.Contains("x-ms-creation-time"));
        Assert.False(defaultedResponse.Headers.Contains("x-ms-legal-hold"));
    }

    private static async Task AssertSasServiceVersionSelectionAsync(HttpClient transport, BlobClient blob)
    {
        var sasUri = blob.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var signedVersion = ReadQueryParameter(sasUri, "sv");
        using (var signedVersionRequest = new HttpRequestMessage(HttpMethod.Head, sasUri))
        {
            using var response = await transport.SendAsync(signedVersionRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(signedVersion, response.Headers.GetValues("x-ms-version").Single());
            Assert.True(response.Headers.Contains("x-ms-legal-hold"));
        }

        using var apiVersionRequest = new HttpRequestMessage(
            HttpMethod.Head,
            AppendQuery(sasUri, "api-version=2012-02-12"));
        using var apiVersionResponse = await transport.SendAsync(apiVersionRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, apiVersionResponse.StatusCode);
        Assert.Equal("2012-02-12", apiVersionResponse.Headers.GetValues("x-ms-version").Single());
        Assert.False(apiVersionResponse.Headers.Contains("x-ms-creation-time"));
        Assert.True(apiVersionResponse.Headers.Contains("Accept-Ranges"));
    }

    private static async Task AssertBearerServiceVersionSelectionAsync(HttpClient transport, Uri blobUri)
    {
        var bearerToken = CreateJwt(SavaWebApplicationFactory.AccountKey, "reader-1");
        using (var missingBearerVersion = new HttpRequestMessage(HttpMethod.Head, blobUri))
        {
            missingBearerVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            using var response = await transport.SendAsync(missingBearerVersion).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldBearerVersion = new HttpRequestMessage(HttpMethod.Head, blobUri))
        {
            oldBearerVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            oldBearerVersion.Headers.TryAddWithoutValidation("x-ms-version", "2017-07-29");
            using var response = await transport.SendAsync(oldBearerVersion).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var supportedBearerVersion = new HttpRequestMessage(HttpMethod.Head, blobUri))
        {
            supportedBearerVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            supportedBearerVersion.Headers.TryAddWithoutValidation("x-ms-version", "2017-11-09");
            using var response = await transport.SendAsync(supportedBearerVersion).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("2017-11-09", response.Headers.GetValues("x-ms-version").Single());
        }
        using (var currentBearerVersion = new HttpRequestMessage(HttpMethod.Head, blobUri))
        {
            currentBearerVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            currentBearerVersion.Headers.TryAddWithoutValidation("x-ms-version", "2026-12-06");
            using var response = await transport.SendAsync(currentBearerVersion).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("2026-12-06", response.Headers.GetValues("x-ms-version").Single());
        }
        using var unknownFutureVersion = new HttpRequestMessage(HttpMethod.Head, blobUri);
        unknownFutureVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
        unknownFutureVersion.Headers.TryAddWithoutValidation("x-ms-version", "2027-01-01");
        using var futureResponse = await transport.SendAsync(unknownFutureVersion).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, futureResponse.StatusCode);
        Assert.Equal("InvalidHeaderValue", futureResponse.Headers.GetValues("x-ms-error-code").Single());
    }

    [Fact]
    public async Task Version20110818ShapesEtagsConditionsRangesAndChecksums()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"version-20110818-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("history.bin");
        var content = "0123456789"u8.ToArray();
        await blob.UploadAsync(new BinaryData(content));
        var blobSas = blob.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var transport = new HttpClient(factory.Server.CreateHandler());
        var (legacyEtag, modernEtag) = await AssertHistoricalEtagsAsync(transport, blobSas);
        await AssertHistoricalMatchConditionsAsync(transport, blobSas, content, legacyEtag, modernEtag);

        await AssertHistoricalRangeShapesAsync(transport, blobSas, content.Length);

        await AssertHistoricalChecksumShapesAsync(transport, blobSas, content);

        await AssertHistoricalPageRangesAsync(transport, container);

        await AssertHistoricalListingEtagsAsync(transport, container, legacyEtag, modernEtag);
    }

    private static async Task<HttpResponseMessage> SendHistoricalBlobAsync(
        HttpClient transport,
        Uri blobSas,
        HttpMethod method,
        string version,
        Action<HttpRequestMessage>? configure = null)
    {
        using var request = new HttpRequestMessage(method, blobSas);
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        configure?.Invoke(request);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static string HistoricalHeader(HttpResponseMessage response, string name)
    {
        if (response.Headers.TryGetValues(name, out var responseValues))
            return responseValues.Single();
        Assert.True(response.Content.Headers.TryGetValues(name, out var contentValues),
            $"Missing response header: {name}");
        return contentValues.Single();
    }

    private static async Task<(string LegacyEtag, string ModernEtag)> AssertHistoricalEtagsAsync(
        HttpClient transport,
        Uri blobSas)
    {
        using var legacyHead = await SendHistoricalBlobAsync(transport, blobSas, HttpMethod.Head, "2009-09-19")
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, legacyHead.StatusCode);
        var legacyEtag = HistoricalHeader(legacyHead, "ETag");
        Assert.StartsWith("0x", legacyEtag, StringComparison.Ordinal);
        Assert.DoesNotContain('"', legacyEtag);
        Assert.False(legacyHead.Headers.Contains("Accept-Ranges"));

        using var modernHead = await SendHistoricalBlobAsync(transport, blobSas, HttpMethod.Head, "2011-08-18")
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, modernHead.StatusCode);
        var modernEtag = HistoricalHeader(modernHead, "ETag");
        Assert.Equal($"\"{legacyEtag}\"", modernEtag);
        Assert.Equal("bytes", HistoricalHeader(modernHead, "Accept-Ranges"));
        return (legacyEtag, modernEtag);
    }

    private static async Task AssertHistoricalMatchConditionsAsync(
        HttpClient transport,
        Uri blobSas,
        byte[] content,
        string legacyEtag,
        string modernEtag)
    {
        using (var legacyMatch = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2009-09-19",
            request => request.Headers.TryAddWithoutValidation("If-Match", legacyEtag)).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, legacyMatch.StatusCode);
            Assert.Equal(content, await legacyMatch.Content.ReadAsByteArrayAsync().ConfigureAwait(false));
        }
        using (var legacyQuotedMatch = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2009-09-19",
            request => request.Headers.TryAddWithoutValidation("If-Match", modernEtag)).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.PreconditionFailed, legacyQuotedMatch.StatusCode);
            await AssertVersionedErrorAsync(legacyQuotedMatch, "ConditionNotMet").ConfigureAwait(false);
        }
        using var modernUnquotedMatch = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2011-08-18",
            request => request.Headers.TryAddWithoutValidation("If-Match", legacyEtag)).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, modernUnquotedMatch.StatusCode);
    }

    private static async Task AssertHistoricalRangeShapesAsync(HttpClient transport, Uri blobSas, int contentLength)
    {
        using (var legacyBoundedRange = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2009-09-19",
            request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4")).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.PartialContent, legacyBoundedRange.StatusCode);
            Assert.Equal("234", await legacyBoundedRange.Content.ReadAsStringAsync().ConfigureAwait(false));
        }
        using (var legacyOpenRange = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2009-09-19",
            request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-")).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, legacyOpenRange.StatusCode);
            await AssertVersionedErrorAsync(legacyOpenRange, "InvalidRange").ConfigureAwait(false);
        }
        using (var modernOpenRange = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2011-08-18",
            request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-")).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.PartialContent, modernOpenRange.StatusCode);
            Assert.Equal("23456789", await modernOpenRange.Content.ReadAsStringAsync().ConfigureAwait(false));
        }
        using (var unsupportedSuffixRange = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2023-11-03",
            request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=-3")).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, unsupportedSuffixRange.StatusCode);
            Assert.Equal("InvalidRange", unsupportedSuffixRange.Headers.GetValues("x-ms-error-code").Single());
        }
        using var rangedHead = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Head, "2023-11-03",
            request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4")).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, rangedHead.StatusCode);
        Assert.Equal(contentLength, rangedHead.Content.Headers.ContentLength);
        Assert.Null(rangedHead.Content.Headers.ContentRange);
    }

    private static async Task AssertHistoricalChecksumShapesAsync(HttpClient transport, Uri blobSas, byte[] content)
    {
        var wholeMd5 = Convert.ToBase64String(AzureProtocolChecksum.Md5(content));
        using (var oldRangeMd5Shape = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2015-12-11",
            request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4")).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.PartialContent, oldRangeMd5Shape.StatusCode);
            Assert.False(oldRangeMd5Shape.Content.Headers.Contains("Content-MD5"));
            Assert.False(oldRangeMd5Shape.Headers.Contains("x-ms-blob-content-md5"));
        }
        using (var rangedMd5Shape = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2016-05-31",
            request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4")).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.PartialContent, rangedMd5Shape.StatusCode);
            Assert.False(rangedMd5Shape.Content.Headers.Contains("Content-MD5"));
            Assert.Equal(wholeMd5, HistoricalHeader(rangedMd5Shape, "x-ms-blob-content-md5"));
        }
        using (var transactionalMd5 = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2016-05-31",
            request =>
            {
                request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4");
                request.Headers.TryAddWithoutValidation("x-ms-range-get-content-md5", "true");
            }).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.PartialContent, transactionalMd5.StatusCode);
            Assert.Equal(Convert.ToBase64String(AzureProtocolChecksum.Md5(content.AsSpan(2, 3))),
                HistoricalHeader(transactionalMd5, "Content-MD5"));
            Assert.Equal(wholeMd5, HistoricalHeader(transactionalMd5, "x-ms-blob-content-md5"));
        }
        using var oldCrc64 = await SendHistoricalBlobAsync(
            transport, blobSas, HttpMethod.Get, "2018-11-09",
            request =>
            {
                request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4");
                request.Headers.TryAddWithoutValidation("x-ms-range-get-content-crc64", "true");
            }).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, oldCrc64.StatusCode);
        Assert.Equal("FeatureVersionMismatch", oldCrc64.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task AssertInvalidHistoricalPageRangeAsync(HttpClient transport, Uri pageSas, string range)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Put,
            AppendQuery(pageSas, "comp=page"))
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        request.Headers.TryAddWithoutValidation("x-ms-page-write", "clear");
        request.Headers.TryAddWithoutValidation("x-ms-range", range);
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, response.StatusCode);
        Assert.Equal("InvalidPageRange", response.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task<HttpResponseMessage> GetHistoricalPageRangesAsync(
        HttpClient transport,
        Uri pageSas,
        string version,
        string query)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            AppendQuery(pageSas, $"comp=pagelist&{query}"));
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertHistoricalPageRangesAsync(HttpClient transport, BlobContainerClient container)
    {
        var page = container.GetPageBlobClient("strict-range.vhd");
        await page.CreateAsync(512).ConfigureAwait(false);
        var pageSas = page.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(5));
        await AssertInvalidHistoricalPageRangeAsync(transport, pageSas, "bytes=0-").ConfigureAwait(false);
        await AssertInvalidHistoricalPageRangeAsync(transport, pageSas, "bytes=0-1023").ConfigureAwait(false);
        await AssertInvalidHistoricalPageRangeAsync(transport, pageSas, "bytes=1-511").ConfigureAwait(false);
        Assert.All((await page.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray(), value => Assert.Equal(0, value));

        await page.ResizeAsync(2560).ConfigureAwait(false);
        foreach (var offset in new long[] { 0, 1024, 2048 })
        {
            using var payload = new MemoryStream(Enumerable.Repeat((byte)(offset / 512 + 1), 512).ToArray());
            await page.UploadPagesAsync(payload, offset).ConfigureAwait(false);
        }
        await AssertHistoricalPagedPageRangesAsync(transport, pageSas).ConfigureAwait(false);
    }

    private static async Task AssertHistoricalPagedPageRangesAsync(HttpClient transport, Uri pageSas)
    {
        using (var oldPagedRanges = await GetHistoricalPageRangesAsync(
            transport, pageSas, "2020-08-04", "maxresults=1").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldPagedRanges.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldPagedRanges.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var clampedRanges = await GetHistoricalPageRangesAsync(
            transport, pageSas, "2020-10-02", "maxresults=10001").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, clampedRanges.StatusCode);
            var xml = await clampedRanges.Content.ReadAsStringAsync().ConfigureAwait(false);
            Assert.Equal(3, xml.Split("<PageRange>", StringSplitOptions.None).Length - 1);
            Assert.Contains("<NextMarker />", xml, StringComparison.Ordinal);
        }
        string marker;
        using (var firstRangePage = await GetHistoricalPageRangesAsync(
            transport, pageSas, "2020-10-02", "maxresults=1").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, firstRangePage.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(
                await firstRangePage.Content.ReadAsStringAsync().ConfigureAwait(false));
            Assert.Single(document.Root!.Elements("PageRange"));
            marker = Assert.IsType<string>(document.Root.Element("NextMarker")?.Value);
            Assert.NotEmpty(marker);
        }
        using var secondRangePage = await GetHistoricalPageRangesAsync(
            transport, pageSas, "2020-10-02", $"maxresults=1&marker={Uri.EscapeDataString(marker)}")
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, secondRangePage.StatusCode);
        var secondDocument = System.Xml.Linq.XDocument.Parse(
            await secondRangePage.Content.ReadAsStringAsync().ConfigureAwait(false));
        Assert.Single(secondDocument.Root!.Elements("PageRange"));
    }

    private static async Task AssertHistoricalListingEtagsAsync(
        HttpClient transport,
        BlobContainerClient container,
        string legacyEtag,
        string modernEtag)
    {
        var listSas = container.GenerateSasUri(
            BlobContainerSasPermissions.Read | BlobContainerSasPermissions.List,
            DateTimeOffset.UtcNow.AddMinutes(5));
        var listUri = AppendQuery(listSas, "restype=container&comp=list");
        using (var legacyListRequest = new HttpRequestMessage(HttpMethod.Get, listUri))
        {
            legacyListRequest.Headers.TryAddWithoutValidation("x-ms-version", "2009-09-19");
            using var response = await transport.SendAsync(legacyListRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Contains($"<Etag>{legacyEtag}</Etag>",
                await response.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
        }
        using var modernListRequest = new HttpRequestMessage(HttpMethod.Get, listUri);
        modernListRequest.Headers.TryAddWithoutValidation("x-ms-version", "2011-08-18");
        using var modernResponse = await transport.SendAsync(modernListRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, modernResponse.StatusCode);
        Assert.Contains($"<Etag>{modernEtag}</Etag>",
            await modernResponse.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ListingSchemasFollowHistoricalVersionsAndClampPageSizes()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:AllowAnonymousPublicAccess"] = "true"
        });
        try
        {
            var (container, deletedContainer) = await CreateHistoricalListingFixtureAsync(application);

            var credential = new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey);
            var accountSas = new AccountSasBuilder
            {
                Services = AccountSasServices.Blobs,
                ResourceTypes = AccountSasResourceTypes.Service,
                StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
                Protocol = SasProtocol.HttpsAndHttp
            };
            accountSas.SetPermissions(AccountSasPermissions.List);
            var serviceSasUri = new Uri(
                $"http://{SavaWebApplicationFactory.AccountName}.localhost/" +
                $"?{accountSas.ToSasQueryParameters(credential)}");
            var containerSasUri = container.GenerateSasUri(
                BlobContainerSasPermissions.Read | BlobContainerSasPermissions.List,
                DateTimeOffset.UtcNow.AddMinutes(10));
            using var transport = new HttpClient(application.Server.CreateHandler());

            await AssertModernContainerListingAsync(transport, serviceSasUri, container.Name, deletedContainer.Name);

            await AssertLegacyContainerListingsAsync(transport, serviceSasUri, container.Name);

            await AssertHistoricalBlobListingsAsync(transport, containerSasUri, container.Name);

            await AssertInvalidHistoricalListingsAsync(transport, serviceSasUri, containerSasUri);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task RenameContainerMovesCompleteStateWithoutCopyingContent()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var sourceName = $"rename-source-{Guid.NewGuid():N}";
        var destinationName = $"rename-destination-{Guid.NewGuid():N}";
        var (source, committed, staged, blockId, leaseId) = await CreateRenameFixtureAsync(service, sourceName);
        var before = await metadata.GetStorageInventoryAsync(CancellationToken.None);
        var sas = CreateContainerRenameSas();
        using var transport = new HttpClient(factory.Server.CreateHandler());

        await AssertInvalidContainerRenameRequestsAsync(transport, sas, sourceName, destinationName);

        using (var rename = await SendContainerRenameAsync(transport, sas, destinationName, sourceName, leaseId))
        {
            Assert.Equal(HttpStatusCode.OK, rename.StatusCode);
            Assert.Null(rename.Headers.ETag);
            Assert.Null(rename.Content.Headers.LastModified);
            Assert.Equal(0, rename.Content.Headers.ContentLength);
        }

        await AssertContainerRenamePreservedStateAsync(
            service, source, destinationName, committed.Name, staged.Name, blockId);

        var after = await metadata.GetStorageInventoryAsync(CancellationToken.None);
        Assert.Equal(before.LogicalBlobBytes, after.LogicalBlobBytes);
        Assert.Equal(before.LogicalStagedBlockBytes, after.LogicalStagedBlockBytes);
        Assert.Equal(before.BlobRecordCount, after.BlobRecordCount);
        Assert.Equal(before.StagedBlockCount, after.StagedBlockCount);
        Assert.True(before.ReachableChunkIds.SetEquals(after.ReachableChunkIds));

        await AssertContainerRenameCollisionAsync(service, transport, sas);
    }

    private static async Task<(BlobContainerClient Source, BlobClient Committed, BlockBlobClient Staged,
        string BlockId, string LeaseId)> CreateRenameFixtureAsync(BlobServiceClient service, string sourceName)
    {
        var source = service.GetBlobContainerClient(sourceName);
        await source.CreateAsync(
            PublicAccessType.None,
            new Dictionary<string, string>(StringComparer.Ordinal) { ["purpose"] = "rename" })
            .ConfigureAwait(false);
        var committed = source.GetBlobClient("committed.bin");
        await committed.UploadAsync(
            BinaryData.FromString("container rename preserves content"),
            new BlobUploadOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "committed" },
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["kind"] = "renamed" }
            }).ConfigureAwait(false);
        var staged = source.GetBlockBlobClient("staged.bin");
        var blockId = Convert.ToBase64String("rename-block"u8);
        using var payload = BinaryData.FromString("staged rename content").ToStream();
        await staged.StageBlockAsync(blockId, payload).ConfigureAwait(false);

        var leaseId = Guid.NewGuid().ToString();
        var lease = source.GetBlobLeaseClient(leaseId);
        await lease.AcquireAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
        return (source, committed, staged, blockId, leaseId);
    }

    private static string CreateContainerRenameSas()
    {
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var accountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountSas.SetPermissions(AccountSasPermissions.Write);
        return accountSas.ToSasQueryParameters(credential).ToString();
    }

    private static async Task<HttpResponseMessage> SendContainerRenameAsync(
        HttpClient transport,
        string sas,
        string target,
        string? sourceContainer,
        string? sourceLeaseId = null,
        string serviceVersion = "2026-02-06")
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Put,
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{target}" +
            $"?restype=container&comp=rename&{sas}")
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", serviceVersion);
        if (sourceContainer is not null)
            request.Headers.TryAddWithoutValidation("x-ms-source-container-name", sourceContainer);
        if (sourceLeaseId is not null)
            request.Headers.TryAddWithoutValidation("x-ms-source-lease-id", sourceLeaseId);
        return await transport.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertInvalidContainerRenameRequestsAsync(
        HttpClient transport,
        string sas,
        string sourceName,
        string destinationName)
    {
        using (var oldVersion = await SendContainerRenameAsync(
            transport, sas, destinationName, sourceName, serviceVersion: "2020-04-08").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldVersion.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldVersion.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var missingSource = await SendContainerRenameAsync(
            transport, sas, destinationName, sourceContainer: null).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, missingSource.StatusCode);
            Assert.Equal("MissingRequiredHeader", missingSource.Headers.GetValues("x-ms-error-code").Single());
            Assert.Contains("<HeaderName>x-ms-source-container-name</HeaderName>",
                await missingSource.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
        }
        using (var malformedLease = await SendContainerRenameAsync(
            transport, sas, destinationName, sourceName, "not-a-lease-id").ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, malformedLease.StatusCode);
            Assert.Equal("InvalidHeaderValue", malformedLease.Headers.GetValues("x-ms-error-code").Single());
            Assert.Contains("<HeaderName>x-ms-source-lease-id</HeaderName>",
                await malformedLease.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
        }
        using (var missingLease = await SendContainerRenameAsync(
            transport, sas, destinationName, sourceName).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.PreconditionFailed, missingLease.StatusCode);
            Assert.Equal("LeaseIdMissing", missingLease.Headers.GetValues("x-ms-error-code").Single());
        }
        using var mismatchedLease = await SendContainerRenameAsync(
            transport, sas, destinationName, sourceName, Guid.NewGuid().ToString()).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.PreconditionFailed, mismatchedLease.StatusCode);
        Assert.Equal("LeaseIdMismatchWithContainerOperation",
            mismatchedLease.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task AssertContainerRenamePreservedStateAsync(
        BlobServiceClient service,
        BlobContainerClient source,
        string destinationName,
        string committedName,
        string stagedName,
        string blockId)
    {
        Assert.False((await source.ExistsAsync().ConfigureAwait(false)).Value);
        var destination = service.GetBlobContainerClient(destinationName);
        var properties = (await destination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal("rename", properties.Metadata["purpose"]);
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Leased, properties.LeaseState);
        var renamedBlob = destination.GetBlobClient(committedName);
        Assert.Equal("container rename preserves content",
            (await renamedBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        Assert.Equal("committed", (await renamedBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["state"]);
        Assert.Equal("renamed", (await renamedBlob.GetTagsAsync().ConfigureAwait(false)).Value.Tags["kind"]);
        var renamedBlocks = (await destination.GetBlockBlobClient(stagedName)
            .GetBlockListAsync(BlockListTypes.Uncommitted).ConfigureAwait(false)).Value;
        Assert.Equal(blockId, Assert.Single(renamedBlocks.UncommittedBlocks).Name);
        var tagged = await service.FindBlobsByTagsAsync("\"kind\" = 'renamed'").ToListAsync().ConfigureAwait(false);
        Assert.Equal(destinationName,
            Assert.Single(tagged, item => string.Equals(item.BlobName, committedName, StringComparison.Ordinal))
                .BlobContainerName);
    }

    private static async Task AssertContainerRenameCollisionAsync(
        BlobServiceClient service,
        HttpClient transport,
        string sas)
    {
        var collisionSource = service.GetBlobContainerClient($"rename-collision-source-{Guid.NewGuid():N}");
        var collisionDestination = service.GetBlobContainerClient($"rename-collision-target-{Guid.NewGuid():N}");
        await collisionSource.CreateAsync().ConfigureAwait(false);
        await collisionDestination.CreateAsync().ConfigureAwait(false);
        using var collision = await SendContainerRenameAsync(
            transport, sas, collisionDestination.Name, collisionSource.Name).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, collision.StatusCode);
        Assert.Equal("ContainerAlreadyExists", collision.Headers.GetValues("x-ms-error-code").Single());
        Assert.True((await collisionSource.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task RenameContainerRequiresBearerAccessToBothNames()
    {
        var sourceName = $"rename-bearer-source-{Guid.NewGuid():N}";
        var destinationName = $"rename-bearer-target-{Guid.NewGuid():N}";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:BearerAuthentication:Principals:rename-writer:Permissions"] = "w",
            ["Sava:BearerAuthentication:Principals:rename-writer:Accounts:0"] =
                SavaWebApplicationFactory.AccountName,
            ["Sava:BearerAuthentication:Principals:rename-writer:Containers:0"] = destinationName
        });
        try
        {
            var owner = CreateClient(application);
            var source = owner.GetBlobContainerClient(sourceName);
            await source.CreateAsync();
            var token = CreateJwt(SavaWebApplicationFactory.AccountKey, "rename-writer");
            using var transport = new HttpClient(application.Server.CreateHandler());
            using var request = new HttpRequestMessage(
                HttpMethod.Put,
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/{destinationName}" +
                "?restype=container&comp=rename")
            {
                Content = new ByteArrayContent([])
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Headers.TryAddWithoutValidation("x-ms-version", "2026-02-06");
            request.Headers.TryAddWithoutValidation("x-ms-source-container-name", sourceName);

            using var response = await transport.SendAsync(request);

            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal(
                "AuthorizationPermissionMismatch",
                response.Headers.GetValues("x-ms-error-code").Single());
            Assert.True((await source.ExistsAsync()).Value);
            Assert.False((await owner.GetBlobContainerClient(destinationName).ExistsAsync()).Value);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task RestoreContainerSupportsDestinationNamesAndPreservesTheCompleteContainerState()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var original = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            original with
            {
                ContainerSoftDeleteEnabled = true,
                ContainerSoftDeleteRetentionDays = 7
            },
            CancellationToken.None);

        try
        {
            var sourceName = $"restore-source-{Guid.NewGuid():N}";
            var destinationName = $"restore-destination-{Guid.NewGuid():N}";
            var (source, committedName, stagedName, blockId, deletedVersion) =
                await CreateRestoreFixtureAsync(service, sourceName);
            await AssertDestinationContainerRestoreAsync(
                service, source, destinationName, committedName, stagedName, blockId, deletedVersion);

            await AssertSameNameContainerRestoreAsync(service);

            await AssertInvalidContainerRestoreRequestsAsync(factory);

            await AssertContainerRestoreCollisionAsync(service);
            await AssertConsumedContainerRestoreAsync(service, sourceName, deletedVersion);

            var inventory = await metadata.GetStorageInventoryAsync(CancellationToken.None);
            Assert.True(inventory.BlobRecordCount >= 3);
            Assert.True(inventory.StagedBlockCount >= 1);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original,
                CancellationToken.None);
        }
    }

    [Fact]
    public async Task RestoreContainerRejectsTheExactRetentionDeadline()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 22, 10, 0, 0, TimeSpan.Zero));
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "1.00:00:00"
            });
        try
        {
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            var properties = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                properties with
                {
                    ContainerSoftDeleteEnabled = true,
                    ContainerSoftDeleteRetentionDays = 1
                },
                CancellationToken.None);
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"restore-deadline-{Guid.NewGuid():N}");
            await container.CreateAsync();
            await container.DeleteAsync();
            var deleted = await service.GetBlobContainersAsync(
                    states: BlobContainerStates.Deleted,
                    prefix: container.Name)
                .SingleAsync();

            clock.Advance(TimeSpan.FromDays(1));
            var expired = await Assert.ThrowsAsync<RequestFailedException>(() =>
                service.UndeleteBlobContainerAsync(container.Name, deleted.VersionId));
            Assert.Equal(404, expired.Status);
            Assert.Equal("ContainerNotFound", expired.ErrorCode);

            var retained = await metadata.GetContainerAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                includeDeleted: true,
                CancellationToken.None);
            Assert.NotNull(retained?.DeletedAt);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task PermanentDeletePurgesOnlySoftDeletedSnapshotsWithDedicatedPermission()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var original = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var container = service.GetBlobContainerClient($"permanent-delete-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var propertiesUri = CreatePermanentDeletePropertiesUri();
        using var transport = new HttpClient(factory.Server.CreateHandler());

        try
        {
            await AssertPermanentDeletePropertiesAsync(transport, propertiesUri, metadata);

            var (blob, snapshot, snapshotId) = await CreateSoftDeletedSnapshotAsync(container, metadata);

            var (permanentUri, activeSnapshot) = await AssertPermanentDeleteGuardsAsync(transport, blob, snapshot);

            await AssertDisabledPermanentDeleteAsync(
                transport, propertiesUri, permanentUri, metadata, container, blob, snapshotId);
            await AssertPermanentSnapshotPurgeAsync(
                transport, propertiesUri, permanentUri, metadata, container, blob, snapshot, snapshotId, activeSnapshot);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original,
                CancellationToken.None);
        }
    }

    private static Uri CreatePermanentDeletePropertiesUri()
    {
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var accountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Service,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountSas.SetPermissions(AccountSasPermissions.Read | AccountSasPermissions.Write);
        return new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/" +
            $"?restype=service&comp=properties&{accountSas.ToSasQueryParameters(credential)}");
    }

    private static async Task SetPermanentDeleteAsync(HttpClient transport, Uri propertiesUri, bool enabled)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, propertiesUri)
        {
            Content = new StringContent(
                $"""
                <StorageServiceProperties>
                  <DeleteRetentionPolicy>
                    <Enabled>true</Enabled>
                    <Days>7</Days>
                    <AllowPermanentDelete>{(enabled ? "true" : "false")}</AllowPermanentDelete>
                  </DeleteRetentionPolicy>
                </StorageServiceProperties>
                """,
                Encoding.UTF8,
                "application/xml")
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    private static async Task AssertOldPermanentDeleteSettingAsync(HttpClient transport, Uri propertiesUri)
    {
        using var oldSetting = new HttpRequestMessage(HttpMethod.Put, propertiesUri)
        {
            Content = new StringContent(
                """
                <StorageServiceProperties>
                  <DeleteRetentionPolicy>
                    <Enabled>true</Enabled>
                    <Days>7</Days>
                    <AllowPermanentDelete>true</AllowPermanentDelete>
                  </DeleteRetentionPolicy>
                </StorageServiceProperties>
                """,
                Encoding.UTF8,
                "application/xml")
        };
        oldSetting.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
        using var response = await transport.SendAsync(oldSetting).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
    }

    private static async Task AssertPermanentDeletePropertiesAsync(
        HttpClient transport,
        Uri propertiesUri,
        MetadataStore metadata)
    {
        await AssertOldPermanentDeleteSettingAsync(transport, propertiesUri).ConfigureAwait(false);
        await SetPermanentDeleteAsync(transport, propertiesUri, enabled: true).ConfigureAwait(false);
        var roundTrip = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None).ConfigureAwait(false);
        Assert.True(roundTrip.BlobSoftDeleteEnabled);
        Assert.Equal(7, roundTrip.BlobSoftDeleteRetentionDays);
        Assert.True(roundTrip.BlobPermanentDeleteEnabled);
        using (var propertiesRequest = new HttpRequestMessage(HttpMethod.Get, propertiesUri))
        {
            propertiesRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(propertiesRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Contains("<AllowPermanentDelete>true</AllowPermanentDelete>",
                await response.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
        }
        using var oldPropertiesRequest = new HttpRequestMessage(HttpMethod.Get, propertiesUri);
        oldPropertiesRequest.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
        using var oldResponse = await transport.SendAsync(oldPropertiesRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, oldResponse.StatusCode);
        Assert.DoesNotContain("<AllowPermanentDelete>",
            await oldResponse.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
    }

    private static async Task<(BlobClient Blob, BlobClient Snapshot, string SnapshotId)>
        CreateSoftDeletedSnapshotAsync(BlobContainerClient container, MetadataStore metadata)
    {
        var blob = container.GetBlobClient("purge.txt");
        await blob.UploadAsync(BinaryData.FromString("retained snapshot")).ConfigureAwait(false);
        var snapshotId = (await blob.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var snapshot = blob.WithSnapshot(snapshotId);
        var softDelete = await snapshot.DeleteAsync().ConfigureAwait(false);
        Assert.True(softDelete.Headers.TryGetValue("x-ms-delete-type-permanent", out var softDeleteHeader));
        Assert.Equal("false", softDeleteHeader);
        var deletedSnapshot = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            blob.Name,
            versionId: null,
            snapshot: snapshotId,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.True(deletedSnapshot?.IsDeleted);
        return (blob, snapshot, snapshotId);
    }

    private static async Task<(Uri PermanentUri, BlobClient ActiveSnapshot)> AssertPermanentDeleteGuardsAsync(
        HttpClient transport,
        BlobClient blob,
        BlobClient snapshot)
    {
        using (var denied = new HttpRequestMessage(
            HttpMethod.Delete,
            AppendQuery(
                snapshot.GenerateSasUri(BlobSasPermissions.Delete, DateTimeOffset.UtcNow.AddMinutes(5)),
                "deletetype=permanent")))
        {
            denied.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(denied).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        }

        var permanentUri = AppendQuery(
            snapshot.GenerateSasUri(BlobSasPermissions.PermanentDelete, DateTimeOffset.UtcNow.AddMinutes(5)),
            "deletetype=permanent");
        using (var oldVersion = new HttpRequestMessage(HttpMethod.Delete, permanentUri))
        {
            oldVersion.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
            using var response = await transport.SendAsync(oldVersion).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var rootDelete = new HttpRequestMessage(
            HttpMethod.Delete,
            AppendQuery(
                blob.GenerateSasUri(BlobSasPermissions.PermanentDelete, DateTimeOffset.UtcNow.AddMinutes(5)),
                "deletetype=permanent")))
        {
            rootDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(rootDelete).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("PermanentDeleteNotSupportedOnRootBlob",
                response.Headers.GetValues("x-ms-error-code").Single());
        }

        var activeSnapshotId = (await blob.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var activeSnapshot = blob.WithSnapshot(activeSnapshotId);
        using var activeDelete = new HttpRequestMessage(
            HttpMethod.Delete,
            AppendQuery(
                activeSnapshot.GenerateSasUri(BlobSasPermissions.PermanentDelete, DateTimeOffset.UtcNow.AddMinutes(5)),
                "deletetype=permanent"));
        activeDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var activeResponse = await transport.SendAsync(activeDelete).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, activeResponse.StatusCode);
        return (permanentUri, activeSnapshot);
    }

    private static async Task AssertDisabledPermanentDeleteAsync(
        HttpClient transport,
        Uri propertiesUri,
        Uri permanentUri,
        MetadataStore metadata,
        BlobContainerClient container,
        BlobClient blob,
        string snapshotId)
    {
        await SetPermanentDeleteAsync(transport, propertiesUri, enabled: false).ConfigureAwait(false);
        using (var disabledDelete = new HttpRequestMessage(HttpMethod.Delete, permanentUri))
        {
            disabledDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(disabledDelete).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        }
        Assert.NotNull(await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            blob.Name,
            versionId: null,
            snapshot: snapshotId,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false));
    }

    private static async Task AssertPermanentSnapshotPurgeAsync(
        HttpClient transport,
        Uri propertiesUri,
        Uri permanentUri,
        MetadataStore metadata,
        BlobContainerClient container,
        BlobClient blob,
        BlobClient snapshot,
        string snapshotId,
        BlobClient activeSnapshot)
    {
        await SetPermanentDeleteAsync(transport, propertiesUri, enabled: true).ConfigureAwait(false);
        using (var invalidDeleteType = new HttpRequestMessage(
            HttpMethod.Delete,
            AppendQuery(
                snapshot.GenerateSasUri(BlobSasPermissions.PermanentDelete, DateTimeOffset.UtcNow.AddMinutes(5)),
                "deletetype=Permanent")))
        {
            invalidDeleteType.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(invalidDeleteType).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidQueryParameterValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var permanentDelete = new HttpRequestMessage(HttpMethod.Delete, permanentUri))
        {
            permanentDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(permanentDelete).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            Assert.Equal("true", response.Headers.GetValues("x-ms-delete-type-permanent").Single());
        }
        Assert.Null(await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            blob.Name,
            versionId: null,
            snapshot: snapshotId,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false));
        Assert.True((await activeSnapshot.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task DeleteBlobEnforcesSnapshotHeaderScopeAndValues()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var original = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var container = service.GetBlobContainerClient($"delete-snapshots-{Guid.NewGuid():N}");
        await container.CreateAsync();

        try
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original with { VersioningEnabled = false, BlobSoftDeleteEnabled = false },
                CancellationToken.None);
            var blob = container.GetBlobClient("family.txt");
            await blob.UploadAsync(BinaryData.FromString("family"));
            var firstSnapshotId = (await blob.CreateSnapshotAsync()).Value.Snapshot;
            var secondSnapshotId = (await blob.CreateSnapshotAsync()).Value.Snapshot;
            var firstSnapshot = blob.WithSnapshot(firstSnapshotId);
            var secondSnapshot = blob.WithSnapshot(secondSnapshotId);
            await AssertSnapshotDeleteHeaderValidationAsync(blob, firstSnapshot, factory);

            var snapshotsPresent = await Assert.ThrowsAsync<RequestFailedException>(() => blob.DeleteAsync());
            Assert.Equal(409, snapshotsPresent.Status);
            Assert.Equal("SnapshotsPresent", snapshotsPresent.ErrorCode);
            Assert.True((await blob.ExistsAsync()).Value);
            Assert.True((await firstSnapshot.ExistsAsync()).Value);
            Assert.True((await secondSnapshot.ExistsAsync()).Value);

            var only = await blob.DeleteAsync(DeleteSnapshotsOption.OnlySnapshots);
            Assert.Equal(202, only.Status);
            Assert.True((await blob.ExistsAsync()).Value);
            Assert.False((await firstSnapshot.ExistsAsync()).Value);
            Assert.False((await secondSnapshot.ExistsAsync()).Value);

            var includedSnapshotId = (await blob.CreateSnapshotAsync()).Value.Snapshot;
            var includedSnapshot = blob.WithSnapshot(includedSnapshotId);
            var include = await blob.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots);
            Assert.Equal(202, include.Status);
            Assert.True(include.Headers.TryGetValue("x-ms-delete-type-permanent", out var permanentHeader));
            Assert.Equal("true", permanentHeader);
            Assert.False((await blob.ExistsAsync()).Value);
            Assert.False((await includedSnapshot.ExistsAsync()).Value);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original,
                CancellationToken.None);
        }
    }

    private static async Task AssertSnapshotDeleteHeaderValidationAsync(
        BlobClient blob, BlobClient firstSnapshot, SavaWebApplicationFactory application)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        using (var invalidValue = new HttpRequestMessage(
                   HttpMethod.Delete,
                   blob.GenerateSasUri(BlobSasPermissions.Delete, DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            invalidValue.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            invalidValue.Headers.TryAddWithoutValidation("x-ms-delete-snapshots", "Include");
            using var response = await transport.SendAsync(invalidValue).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var scopedToSnapshot = new HttpRequestMessage(
                   HttpMethod.Delete,
                   firstSnapshot.GenerateSasUri(BlobSasPermissions.Delete, DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            scopedToSnapshot.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            scopedToSnapshot.Headers.TryAddWithoutValidation("x-ms-delete-snapshots", "include");
            using var response = await transport.SendAsync(scopedToSnapshot).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    [Fact]
    public async Task DeleteBlobUsesExplicitVersionTargetAndDeleteVersionPermission()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var original = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var container = service.GetBlobContainerClient($"delete-version-{Guid.NewGuid():N}");
        await container.CreateAsync();

        try
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original with { VersioningEnabled = true, BlobSoftDeleteEnabled = false },
                CancellationToken.None);
            var blob = container.GetBlobClient("versioned.txt");
            var firstVersionId = (await blob.UploadAsync(BinaryData.FromString("first"))).Value.VersionId;
            var secondVersionId = (await blob.UploadAsync(BinaryData.FromString("second"), overwrite: true)).Value.VersionId;
            Assert.False(string.IsNullOrEmpty(firstVersionId));
            Assert.False(string.IsNullOrEmpty(secondVersionId));
            var snapshotId = (await blob.CreateSnapshotAsync()).Value.Snapshot;
            var snapshot = blob.WithSnapshot(snapshotId);
            var currentVersionId = (await blob.GetPropertiesAsync()).Value.VersionId;
            Assert.False(string.IsNullOrEmpty(currentVersionId));

            var historicalDelete = await blob.WithVersion(firstVersionId).DeleteAsync();
            Assert.Equal(202, historicalDelete.Status);
            Assert.False((await blob.WithVersion(firstVersionId).ExistsAsync()).Value);
            Assert.True((await blob.WithVersion(secondVersionId).ExistsAsync()).Value);
            Assert.True((await snapshot.ExistsAsync()).Value);
            Assert.True((await blob.ExistsAsync()).Value);

            var currentVersion = blob.WithVersion(currentVersionId);
            await AssertVersionDeletePermissionAsync(currentVersion, factory);
            Assert.False((await currentVersion.ExistsAsync()).Value);
            Assert.False((await blob.ExistsAsync()).Value);
            Assert.True((await snapshot.ExistsAsync()).Value);
            Assert.True((await blob.WithVersion(secondVersionId).ExistsAsync()).Value);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original,
                CancellationToken.None);
        }
    }

    private static async Task AssertVersionDeletePermissionAsync(
        BlobClient currentVersion, SavaWebApplicationFactory application)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        using (var ordinaryDelete = new HttpRequestMessage(
                   HttpMethod.Delete,
                   currentVersion.GenerateSasUri(
                       BlobSasPermissions.Delete,
                       DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            ordinaryDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(ordinaryDelete).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        }
        Assert.True((await currentVersion.ExistsAsync().ConfigureAwait(false)).Value);

        var versionDeleteUri = currentVersion.GenerateSasUri(
            BlobSasPermissions.DeleteBlobVersion,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using (var versionDelete = new HttpRequestMessage(HttpMethod.Delete, versionDeleteUri))
        {
            versionDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(versionDelete).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            Assert.Equal("true", response.Headers.GetValues("x-ms-delete-type-permanent").Single());
        }
    }

    [Fact]
    public async Task VersionedReadsExposeCurrentVersionHeaders()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var original = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var container = service.GetBlobContainerClient($"version-headers-{Guid.NewGuid():N}");
        await container.CreateAsync();

        try
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original with { VersioningEnabled = true },
                CancellationToken.None);
            var blob = container.GetBlobClient("versioned.txt");
            var historicalVersionId = (await blob.UploadAsync(BinaryData.FromString("first"))).Value.VersionId;
            var currentVersionId = (await blob.UploadAsync(BinaryData.FromString("second"), overwrite: true)).Value.VersionId;
            Assert.False(string.IsNullOrEmpty(historicalVersionId));
            Assert.False(string.IsNullOrEmpty(currentVersionId));

            var currentProperties = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(currentVersionId, currentProperties.VersionId);
            Assert.True(currentProperties.IsLatestVersion);

            var historical = blob.WithVersion(historicalVersionId);
            var historicalProperties = (await historical.GetPropertiesAsync()).Value;
            Assert.Equal(historicalVersionId, historicalProperties.VersionId);
            Assert.False(historicalProperties.IsLatestVersion);

            await AssertVersionedReadHeadersAsync(
                blob, historical, currentVersionId, historicalVersionId, factory);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original,
                CancellationToken.None);
        }
    }

    private static async Task AssertVersionedReadHeadersAsync(
        BlobClient blob,
        BlobClient historical,
        string currentVersionId,
        string historicalVersionId,
        SavaWebApplicationFactory application)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        using (var currentRequest = new HttpRequestMessage(
                   HttpMethod.Get,
                   blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            currentRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(currentRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(currentVersionId, response.Headers.GetValues("x-ms-version-id").Single());
            Assert.Equal("true", response.Headers.GetValues("x-ms-is-current-version").Single());
        }

        using (var historicalRequest = new HttpRequestMessage(
                   HttpMethod.Get,
                   historical.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            historicalRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(historicalRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(historicalVersionId, response.Headers.GetValues("x-ms-version-id").Single());
            Assert.Equal("false", response.Headers.GetValues("x-ms-is-current-version").Single());
        }

        using (var legacyRequest = new HttpRequestMessage(
                   HttpMethod.Head,
                   blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            legacyRequest.Headers.TryAddWithoutValidation("x-ms-version", "2019-07-07");
            using var response = await transport.SendAsync(legacyRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.False(response.Headers.Contains("x-ms-version-id"));
            Assert.False(response.Headers.Contains("x-ms-is-current-version"));
        }
    }

    [Fact]
    public async Task BlobFamilyMutationsAreIndexedAndAtomic()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"family-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            await AssertIndexedFamilyDeletionIgnoresCorruptionAsync(application, container, metadata);

            await AssertAtomicFamilyMutationRollbackAsync(container, metadata);

            await AssertSoftDeletedFamilyRestoreAsync(container, metadata);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task ExecuteRawBlobRecordCommandAsync(
        string directConnectionString,
        BlobContainerClient container,
        string blobName,
        bool corrupt)
    {
        var connection = new SqliteConnection(directConnectionString);
        await using (connection.ConfigureAwait(false))
        {
            await connection.OpenAsync().ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using (command.ConfigureAwait(false))
            {
                if (corrupt)
                    command.CommandText = """
                        UPDATE blobs SET data = 'not-json'
                        WHERE account = $account AND container = $container AND name = $name;
                        """;
                else
                    command.CommandText = """
                        DELETE FROM blobs
                        WHERE account = $account AND container = $container AND name = $name;
                        """;
                command.Parameters.AddWithValue("$account", SavaWebApplicationFactory.AccountName);
                command.Parameters.AddWithValue("$container", container.Name);
                command.Parameters.AddWithValue("$name", blobName);
                Assert.Equal(1, await command.ExecuteNonQueryAsync().ConfigureAwait(false));
            }
        }
    }

    private static async Task AssertIndexedFamilyDeletionIgnoresCorruptionAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        MetadataStore metadata)
    {
        var deleted = container.GetBlobClient("delete-with-snapshots");
        await deleted.UploadAsync(BinaryData.FromString("delete me")).ConfigureAwait(false);
        await deleted.CreateSnapshotAsync().ConfigureAwait(false);
        await deleted.CreateSnapshotAsync().ConfigureAwait(false);
        var unrelated = container.GetBlobClient("unrelated-corrupt-record");
        await unrelated.UploadAsync(BinaryData.FromString("unrelated")).ConfigureAwait(false);

        var directConnectionString = new SqliteConnectionStringBuilder
        {
            DataSource = Path.Combine(application.DataPath, "metadata.db"),
            ForeignKeys = true
        }.ToString();
        await ExecuteRawBlobRecordCommandAsync(
            directConnectionString, container, unrelated.Name, corrupt: true).ConfigureAwait(false);

        await deleted.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots).ConfigureAwait(false);
        Assert.Empty(await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            deleted.Name,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false));

        await ExecuteRawBlobRecordCommandAsync(
            directConnectionString, container, unrelated.Name, corrupt: false).ConfigureAwait(false);
    }

    private static async Task AssertAtomicFamilyMutationRollbackAsync(
        BlobContainerClient container,
        MetadataStore metadata)
    {
        var atomic = container.GetBlobClient("atomic-family");
        await atomic.UploadAsync(BinaryData.FromString("atomic")).ConfigureAwait(false);
        await atomic.CreateSnapshotAsync().ConfigureAwait(false);
        var before = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            atomic.Name,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, before.Count);
        var first = before[0];
        var second = before[1];
        var failedBatch = new[]
        {
            new BlobRecordMutation(
                first.GenerationId,
                first.Revision,
                first with
                {
                    Revision = MetadataStore.NewRevision(),
                    Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["mutated"] = "true" }
                }),
            new BlobRecordMutation(
                second.GenerationId,
                "stale-revision",
                second with
                {
                    Revision = MetadataStore.NewRevision(),
                    Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["mutated"] = "true" }
                })
        };
        await Assert.ThrowsAsync<StorageConcurrencyException>(() =>
            metadata.ApplyBlobRecordMutationsAsync(failedBatch, CancellationToken.None)).ConfigureAwait(false);
        var afterFailedBatch = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            atomic.Name,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(
            before.Select(item => (item.GenerationId, item.Revision)),
            afterFailedBatch.Select(item => (item.GenerationId, item.Revision)));
        Assert.All(afterFailedBatch, item => Assert.False(item.Metadata.ContainsKey("mutated")));
    }

    private static async Task AssertSoftDeletedFamilyRestoreAsync(
        BlobContainerClient container,
        MetadataStore metadata)
    {
        var properties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None).ConfigureAwait(false);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            properties with { BlobSoftDeleteEnabled = true, BlobSoftDeleteRetentionDays = 7 },
            CancellationToken.None).ConfigureAwait(false);
        var restored = container.GetBlobClient("restore-family");
        await restored.UploadAsync(BinaryData.FromString("restore")).ConfigureAwait(false);
        await restored.CreateSnapshotAsync().ConfigureAwait(false);
        await restored.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots).ConfigureAwait(false);
        var softDeleted = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            restored.Name,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, softDeleted.Count);
        Assert.All(softDeleted, item => Assert.True(item.IsDeleted));

        await restored.UndeleteAsync().ConfigureAwait(false);
        var undeleted = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.AccountName,
            container.Name,
            restored.Name,
            includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, undeleted.Count);
        Assert.All(undeleted, item => Assert.False(item.IsDeleted));
    }

    [Fact]
    public async Task IdenticalContentIsCompressedAndPhysicallyDeduplicated()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"space-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var content = Enumerable.Repeat((byte)'A', 512 * 1024).ToArray();

        await container.GetBlobClient("one").UploadAsync(BinaryData.FromBytes(content));
        var afterFirst = EnumerateChunkFiles(factory.DataPath).ToArray();
        await container.GetBlobClient("two").UploadAsync(BinaryData.FromBytes(content));
        var afterSecond = EnumerateChunkFiles(factory.DataPath).ToArray();

        Assert.NotEmpty(afterFirst);
        Assert.Equal(afterFirst.Length, afterSecond.Length);
        Assert.True(afterFirst.Sum(file => file.Length) < content.LongLength / 4);
        Assert.Equal(content, (await container.GetBlobClient("two").DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task ColdChunksAreRecompressedAtomicallyWithoutChangingLogicalState()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:CompressionQuality"] = "0",
            ["Sava:CompressionMinimumSavingsBytes"] = "0",
            ["Sava:BackgroundCompressionQuality"] = "11",
            ["Sava:BackgroundCompressionMinimumSavingsBytes"] = "1",
            ["Sava:BackgroundCompressionMinimumAge"] = "00:00:00",
            ["Sava:BackgroundCompressionChunksPerMaintenancePass"] = "1000",
            ["Sava:MaintenanceScanInterval"] = "01:00:00"
        });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var containerName = $"recompress-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var (blob, customerBlob, content, snapshotId, etag, lastModified) =
                await CreateRecompressionBlobsAsync(application, container);

            var blobService = application.Services.GetRequiredService<BlobService>();
            var chunkStore = application.Services.GetRequiredService<ChunkStore>();
            var record = await blobService.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                blob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);
            var customerRecord = await blobService.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                customerBlob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);
            await AssertColdChunkMaintenanceAsync(application, blobService, chunkStore, record, customerRecord);
            await AssertRecompressedStateAsync(
                application, blob, customerBlob, snapshotId, content, etag, lastModified);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task<(BlobClient Blob, BlobClient CustomerBlob, byte[] Content,
        string SnapshotId, ETag Etag, DateTimeOffset LastModified)> CreateRecompressionBlobsAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container)
    {
        var content = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, 5000).Select(index =>
            $"{{\"tenant\":\"stable-tenant\",\"category\":\"storage-event\",\"sequence\":{index % 97},\"payload\":\"alpha-beta-gamma-delta\"}}\n")));
        var blob = container.GetBlobClient("cold.jsonl");
        await blob.UploadAsync(BinaryData.FromBytes(content)).ConfigureAwait(false);
        var snapshotId = (await blob.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var propertiesBefore = await blob.GetPropertiesAsync().ConfigureAwait(false);

        var customerKey = RandomNumberGenerator.GetBytes(32);
        var customerBlob = CreateEncryptedClient(
                application,
                new CustomerProvidedKey(customerKey),
                encryptionScope: null)
            .GetBlobContainerClient(container.Name)
            .GetBlobClient("customer-key.jsonl");
        await customerBlob.UploadAsync(BinaryData.FromBytes(content)).ConfigureAwait(false);
        return (blob, customerBlob, content, snapshotId,
            propertiesBefore.Value.ETag, propertiesBefore.Value.LastModified);
    }

    private static async Task AssertColdChunkMaintenanceAsync(
        SavaWebApplicationFactory application,
        BlobService blobService,
        ChunkStore chunkStore,
        BlobRecord record,
        BlobRecord customerRecord)
    {
        var paths = record.Content.Chunks
            .Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal))
            .Select(chunk => ChunkPath(application.DataPath, chunk.Id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var customerPaths = customerRecord.Content.Chunks
            .Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal))
            .Select(chunk => ChunkPath(application.DataPath, chunk.Id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var beforeBytes = paths.Sum(path => new FileInfo(path).Length);
        var customerBytes = customerPaths.Sum(path => new FileInfo(path).Length);

        using (chunkStore.Pin(record.Content))
        {
            var pinnedPass = await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(0, pinnedPass.RecompressedChunks);
            Assert.Equal(beforeBytes, paths.Sum(path => new FileInfo(path).Length));
        }

        var optimized = await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        var afterBytes = paths.Sum(path => new FileInfo(path).Length);
        Assert.True(optimized.RecompressedChunks > 0);
        Assert.Equal(beforeBytes - afterBytes, optimized.RecompressionBytesSaved);
        Assert.True(afterBytes < beforeBytes);
        Assert.Equal(customerBytes, customerPaths.Sum(path => new FileInfo(path).Length));
    }

    private static async Task AssertRecompressedStateAsync(
        SavaWebApplicationFactory application,
        BlobClient blob,
        BlobClient customerBlob,
        string snapshotId,
        byte[] content,
        ETag etag,
        DateTimeOffset lastModified)
    {
        var propertiesAfter = await blob.GetPropertiesAsync().ConfigureAwait(false);
        Assert.Equal(etag, propertiesAfter.Value.ETag);
        Assert.Equal(lastModified, propertiesAfter.Value.LastModified);
        Assert.Equal(content, (await blob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        Assert.Equal(content,
            (await blob.WithSnapshot(snapshotId).DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        Assert.Equal(content,
            (await customerBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());

        using var operatorClient = application.CreateClient();
        var metrics = await operatorClient.GetStringAsync(new Uri("/metrics", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(false);
        Assert.Contains("mk8_sava_maintenance_recompressed_chunks_total", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_maintenance_recompression_bytes_saved_total", metrics, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CommittedBlocksCanBeReusedAndReorderedWithoutRestaging()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"blocks-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("composed.bin");
        var firstId = Convert.ToBase64String("000001"u8);
        var secondId = Convert.ToBase64String("000002"u8);
        var first = Enumerable.Repeat((byte)'A', 24_000).ToArray();
        var second = Enumerable.Repeat((byte)'B', 17_000).ToArray();

        await blob.StageBlockAsync(firstId, new MemoryStream(first));
        await blob.StageBlockAsync(secondId, new MemoryStream(second));
        await blob.CommitBlockListAsync([firstId, secondId]);
        await blob.CommitBlockListAsync([secondId, firstId]);

        var expected = second.Concat(first).ToArray();
        Assert.Equal(expected, (await blob.DownloadContentAsync()).Value.Content.ToArray());
        var blocks = await blob.GetBlockListAsync(BlockListTypes.Committed);
        Assert.Equal([secondId, firstId], blocks.Value.CommittedBlocks.Select(item => item.Name), StringComparer.Ordinal);
        Assert.Equal([second.LongLength, first.LongLength], blocks.Value.CommittedBlocks.Select(item => item.SizeLong));
    }

    [Fact]
    public async Task AppendCountAndLeaseStateMatchSdkExpectations()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"lease-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var append = container.GetAppendBlobClient("events.log");
        await append.CreateAsync();
        await append.AppendBlockAsync(new MemoryStream("first\n"u8.ToArray()));
        await append.AppendBlockAsync(new MemoryStream("second\n"u8.ToArray()));
        Assert.Equal(2, (await append.GetPropertiesAsync()).Value.BlobCommittedBlockCount);

        var beforeLease = await append.GetPropertiesAsync();
        var lease = append.GetBlobLeaseClient();
        var acquired = await lease.AcquireAsync(TimeSpan.FromSeconds(15));
        var afterLease = await append.GetPropertiesAsync();
        Assert.Equal(beforeLease.Value.ETag, afterLease.Value.ETag);
        Assert.Equal(beforeLease.Value.LastModified, afterLease.Value.LastModified);

        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            append.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["locked"] = "true" }));
        Assert.Equal(412, denied.Status);
        await append.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["locked"] = "true" },
            new BlobRequestConditions { LeaseId = acquired.Value.LeaseId });
        await lease.ReleaseAsync();

        await AssertAppendSealConditionsAsync(append, factory);
    }

    private static async Task AssertAppendSealConditionsAsync(
        AppendBlobClient append, SavaWebApplicationFactory application)
    {
        var beforeSeal = (await append.GetPropertiesAsync().ConfigureAwait(false)).Value;
        var wrongPosition = await Assert.ThrowsAsync<RequestFailedException>(() =>
            append.SealAsync(new AppendBlobRequestConditions
            {
                IfAppendPositionEqual = beforeSeal.ContentLength + 1
            })).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status412PreconditionFailed, wrongPosition.Status);
        Assert.Equal("AppendPositionConditionNotMet", wrongPosition.ErrorCode);

        var wrongEtag = await Assert.ThrowsAsync<RequestFailedException>(() =>
            append.SealAsync(new AppendBlobRequestConditions
            {
                IfMatch = new ETag("\"not-the-current-etag\"")
            })).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status412PreconditionFailed, wrongEtag.Status);
        Assert.Equal("ConditionNotMet", wrongEtag.ErrorCode);

        var sealUri = AppendQuery(
            append.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=seal");
        using (var transport = new HttpClient(application.Server.CreateHandler()))
        using (var bodyRequest = new HttpRequestMessage(HttpMethod.Put, sealUri)
        {
            Content = new ByteArrayContent([1])
        })
        {
            bodyRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var bodyResponse = await transport.SendAsync(bodyRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, bodyResponse.StatusCode);
            Assert.Equal("InvalidHeaderValue", bodyResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        await append.SealAsync(new AppendBlobRequestConditions
        {
            IfAppendPositionEqual = beforeSeal.ContentLength
        }).ConfigureAwait(false);
        Assert.True((await append.GetPropertiesAsync().ConfigureAwait(false)).Value.IsSealed);
    }

    [Fact]
    public async Task BlobLeaseTransitionsMatchAzureStateAndErrorContracts()
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"lease-state-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var blob = container.GetBlobClient("state.bin");
            await blob.UploadAsync(BinaryData.FromString("leased payload"));

            var leaseId = Guid.NewGuid().ToString();
            var lease = blob.GetBlobLeaseClient(leaseId);
            await AssertBlobLeaseAcquisitionAndExpiryAsync(blob, lease, clock, leaseId);
            await AssertBlobLeaseBreakAndRecoveryAsync(blob, lease, clock);
            await AssertInfiniteBlobLeaseValidationAsync(blob, application);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task AssertBlobLeaseAcquisitionAndExpiryAsync(
        BlobClient blob, BlobLeaseClient lease, AdjustableTimeProvider clock, string leaseId)
    {
        var acquired = await lease.AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        Assert.Equal(leaseId, acquired.Value.LeaseId);
        var leased = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Leased, leased.LeaseState);
        Assert.Equal(LeaseStatus.Locked, leased.LeaseStatus);
        Assert.Equal(LeaseDurationType.Fixed, leased.LeaseDuration);

        await blob.UploadAsync(
            BinaryData.FromString("authorized overwrite"),
            new BlobUploadOptions { Conditions = new BlobRequestConditions { LeaseId = leaseId } }).ConfigureAwait(false);
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Leased,
            (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.LeaseState);

        await lease.AcquireAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
        var competing = blob.GetBlobLeaseClient(Guid.NewGuid().ToString());
        var alreadyLeased = await Assert.ThrowsAsync<RequestFailedException>(() =>
            competing.AcquireAsync(TimeSpan.FromSeconds(15))).ConfigureAwait(false);
        Assert.Equal(409, alreadyLeased.Status);
        Assert.Equal("LeaseAlreadyPresent", alreadyLeased.ErrorCode);

        clock.Advance(TimeSpan.FromSeconds(31));
        var expired = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Expired, expired.LeaseState);
        Assert.Equal(LeaseStatus.Unlocked, expired.LeaseStatus);
        Assert.Equal(default, expired.LeaseDuration);

        await lease.RenewAsync().ConfigureAwait(false);
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Leased,
            (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.LeaseState);
    }

    private static async Task AssertBlobLeaseBreakAndRecoveryAsync(
        BlobClient blob, BlobLeaseClient lease, AdjustableTimeProvider clock)
    {
        var leaseId = Guid.NewGuid().ToString();
        var changed = await lease.ChangeAsync(leaseId).ConfigureAwait(false);
        Assert.Equal(leaseId, changed.Value.LeaseId);
        lease = blob.GetBlobLeaseClient(leaseId);

        var initialBreak = await lease.BreakAsync().ConfigureAwait(false);
        Assert.Equal(30, initialBreak.Value.LeaseTime);
        var breaking = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Breaking, breaking.LeaseState);
        Assert.Equal(LeaseStatus.Locked, breaking.LeaseStatus);
        Assert.Equal(default, breaking.LeaseDuration);

        var missing = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["breaking"] = "missing" }))
            .ConfigureAwait(false);
        Assert.Equal(412, missing.Status);
        Assert.Equal("LeaseIdMissing", missing.ErrorCode);
        await blob.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["breaking"] = "authorized" },
            new BlobRequestConditions { LeaseId = leaseId }).ConfigureAwait(false);

        var unchangedBreak = await lease.BreakAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
        Assert.Equal(30, unchangedBreak.Value.LeaseTime);
        var shortenedBreak = await lease.BreakAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        Assert.Equal(2, shortenedBreak.Value.LeaseTime);
        clock.Advance(TimeSpan.FromSeconds(3));
        var broken = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Broken, broken.LeaseState);
        Assert.Equal(LeaseStatus.Unlocked, broken.LeaseStatus);

        var brokenRenew = await Assert.ThrowsAsync<RequestFailedException>(() => lease.RenewAsync())
            .ConfigureAwait(false);
        Assert.Equal(409, brokenRenew.Status);
        Assert.Equal("LeaseIsBrokenAndCannotBeRenewed", brokenRenew.ErrorCode);
        await lease.ReleaseAsync().ConfigureAwait(false);

        await lease.AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        clock.Advance(TimeSpan.FromSeconds(16));
        await blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["expired"] = "rewritten" })
            .ConfigureAwait(false);
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Available,
            (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.LeaseState);
        var invalidatedRenew = await Assert.ThrowsAsync<RequestFailedException>(() => lease.RenewAsync())
            .ConfigureAwait(false);
        Assert.Equal(409, invalidatedRenew.Status);
        Assert.Equal("LeaseIdMismatchWithLeaseOperation", invalidatedRenew.ErrorCode);
    }

    private static async Task AssertInfiniteBlobLeaseValidationAsync(
        BlobClient blob, SavaWebApplicationFactory application)
    {
        var infinite = blob.GetBlobLeaseClient(Guid.NewGuid().ToString());
        await infinite.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration).ConfigureAwait(false);
        using var transport = new HttpClient(application.Server.CreateHandler());
        var leaseUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=lease");
        using (var invalidBreak = new HttpRequestMessage(HttpMethod.Put, leaseUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            invalidBreak.Headers.Add("x-ms-version", "2025-11-05");
            invalidBreak.Headers.Add("x-ms-lease-action", "break");
            invalidBreak.Headers.Add("x-ms-lease-break-period", "61");
            using var response = await transport.SendAsync(invalidBreak).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var immediateBreak = await infinite.BreakAsync().ConfigureAwait(false);
        Assert.Equal(0, immediateBreak.Value.LeaseTime);
        using (var invalidProposed = new HttpRequestMessage(HttpMethod.Put, leaseUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            invalidProposed.Headers.Add("x-ms-version", "2025-11-05");
            invalidProposed.Headers.Add("x-ms-lease-action", "acquire");
            invalidProposed.Headers.Add("x-ms-lease-duration", "15");
            invalidProposed.Headers.Add("x-ms-proposed-lease-id", "not-a-guid");
            using var response = await transport.SendAsync(invalidProposed).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var missingDuration = new HttpRequestMessage(HttpMethod.Put, leaseUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            missingDuration.Headers.Add("x-ms-version", "2025-11-05");
            missingDuration.Headers.Add("x-ms-lease-action", "acquire");
            using var response = await transport.SendAsync(missingDuration).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    [Fact]
    public async Task ContainerLeasesGuardDeletionButNotOrdinaryContainerWrites()
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"container-lease-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var leaseId = Guid.NewGuid().ToString();
            var lease = container.GetBlobLeaseClient(leaseId);
            await lease.AcquireAsync(TimeSpan.FromSeconds(15));

            await container.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["active"] = "allowed" });
            var missing = await Assert.ThrowsAsync<RequestFailedException>(() => container.DeleteAsync());
            Assert.Equal(412, missing.Status);
            Assert.Equal("LeaseIdMissing", missing.ErrorCode);

            clock.Advance(TimeSpan.FromSeconds(16));
            Assert.Equal(
                Azure.Storage.Blobs.Models.LeaseState.Expired,
                (await container.GetPropertiesAsync()).Value.LeaseState);
            await container.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["expired"] = "retained" });
            await lease.RenewAsync();
            Assert.Equal(
                Azure.Storage.Blobs.Models.LeaseState.Leased,
                (await container.GetPropertiesAsync()).Value.LeaseState);

            await container.DeleteAsync(new BlobRequestConditions { LeaseId = leaseId });
            Assert.False((await container.ExistsAsync()).Value);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task SnapshotContractsHonorHistoricalVersionsMetadataAndArchiveState()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 22, 8, 0, 0, TimeSpan.Zero));
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"snapshot-contract-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var blob = container.GetBlobClient("state.bin");
            await blob.UploadAsync(
                BinaryData.FromString("snapshot payload"),
                new BlobUploadOptions
                {
                    Metadata = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["owner"] = "base",
                        ["retained"] = "base-only"
                    },
                    Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["kind"] = "snapshot" }
                });
            var original = (await blob.GetPropertiesAsync()).Value;
            var snapshotUri = AppendQuery(
                blob.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(10)),
                "comp=snapshot");

            using var transport = new HttpClient(application.Server.CreateHandler());
            await AssertInheritedSnapshotContractsAsync(clock, blob, original, snapshotUri, transport);
            await AssertReplacedSnapshotContractsAsync(clock, blob, original, snapshotUri, transport);
            await AssertSnapshotLeaseAndArchiveContractsAsync(blob, container, snapshotUri, transport);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task AssertInheritedSnapshotContractsAsync(
        AdjustableTimeProvider clock, BlobClient blob, BlobProperties original, Uri snapshotUri, HttpClient transport)
    {
        using (var unavailable = new HttpRequestMessage(HttpMethod.Put, snapshotUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            unavailable.Headers.TryAddWithoutValidation("x-ms-version", "2008-10-27");
            using var response = await transport.SendAsync(unavailable).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
            Assert.False(response.Headers.Contains("x-ms-version"));
        }

        clock.Advance(TimeSpan.FromMinutes(1));
        string inheritedSnapshot;
        using (var inherit = new HttpRequestMessage(HttpMethod.Put, snapshotUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            inherit.Headers.TryAddWithoutValidation("x-ms-version", "2013-08-15");
            using var response = await transport.SendAsync(inherit).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            inheritedSnapshot = response.Headers.GetValues("x-ms-snapshot").Single();
            Assert.Equal(original.ETag.ToString(), GetResponseHeader(response, "ETag"));
            Assert.Equal(
                original.LastModified.ToString("R", CultureInfo.InvariantCulture),
                GetResponseHeader(response, "Last-Modified"));
        }

        var inherited = blob.WithSnapshot(inheritedSnapshot);
        var inheritedProperties = (await inherited.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(original.ETag, inheritedProperties.ETag);
        Assert.Equal(original.LastModified, inheritedProperties.LastModified);
        Assert.Equal("base", inheritedProperties.Metadata["owner"]);
        Assert.Equal("base-only", inheritedProperties.Metadata["retained"]);
        Assert.Equal("snapshot", (await inherited.GetTagsAsync().ConfigureAwait(false)).Value.Tags["kind"]);
        using (var oldSnapshotRead = new HttpRequestMessage(
                   HttpMethod.Head,
                   inherited.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(10))))
        {
            oldSnapshotRead.Headers.TryAddWithoutValidation("x-ms-version", "2008-10-27");
            using var response = await transport.SendAsync(oldSnapshotRead).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
            Assert.False(response.Headers.Contains("x-ms-version"));
        }
    }

    private static async Task AssertReplacedSnapshotContractsAsync(
        AdjustableTimeProvider clock, BlobClient blob, BlobProperties original, Uri snapshotUri, HttpClient transport)
    {
        clock.Advance(TimeSpan.FromMinutes(1));
        string replacedSnapshot;
        ETag replacedEtag;
        DateTimeOffset replacedLastModified;
        using (var replace = new HttpRequestMessage(HttpMethod.Put, snapshotUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            replace.Headers.TryAddWithoutValidation("x-ms-version", "2013-08-15");
            replace.Headers.TryAddWithoutValidation("x-ms-meta-owner", "snapshot");
            using var response = await transport.SendAsync(replace).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            replacedSnapshot = response.Headers.GetValues("x-ms-snapshot").Single();
            replacedEtag = new ETag(GetResponseHeader(response, "ETag"));
            replacedLastModified = DateTimeOffset.ParseExact(
                GetResponseHeader(response, "Last-Modified"),
                "R",
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
            Assert.NotEqual(original.ETag, replacedEtag);
            Assert.NotEqual(original.LastModified, replacedLastModified);
        }

        var replaced = blob.WithSnapshot(replacedSnapshot);
        var replacedProperties = (await replaced.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(replacedEtag, replacedProperties.ETag);
        Assert.Equal(replacedLastModified, replacedProperties.LastModified);
        Assert.Equal("snapshot", replacedProperties.Metadata["owner"]);
        Assert.False(replacedProperties.Metadata.ContainsKey("retained"));
        Assert.Equal("snapshot", (await replaced.GetTagsAsync().ConfigureAwait(false)).Value.Tags["kind"]);

        var unchangedBase = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(original.ETag, unchangedBase.ETag);
        Assert.Equal(original.LastModified, unchangedBase.LastModified);
        Assert.Equal("base", unchangedBase.Metadata["owner"]);
        Assert.Equal("base-only", unchangedBase.Metadata["retained"]);
    }

    private static async Task AssertSnapshotLeaseAndArchiveContractsAsync(
        BlobClient blob, BlobContainerClient container, Uri snapshotUri, HttpClient transport)
    {
        var lease = blob.GetBlobLeaseClient();
        await lease.AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        var createOnlySnapshotUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Create, DateTimeOffset.UtcNow.AddMinutes(10)),
            "comp=snapshot");
        using (var createOnly = new HttpRequestMessage(HttpMethod.Put, createOnlySnapshotUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            createOnly.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(createOnly).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.Equal("true", response.Headers.GetValues("x-ms-request-server-encrypted").Single());
        }
        using (var wrongLease = new HttpRequestMessage(HttpMethod.Put, snapshotUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            wrongLease.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            wrongLease.Headers.TryAddWithoutValidation("x-ms-lease-id", Guid.NewGuid().ToString());
            using var response = await transport.SendAsync(wrongLease).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
            Assert.Equal("LeaseIdMismatchWithBlobOperation", response.Headers.GetValues("x-ms-error-code").Single());
        }
        await lease.ReleaseAsync().ConfigureAwait(false);

        var archived = container.GetBlobClient("archived.bin");
        await archived.UploadAsync(BinaryData.FromString("offline payload")).ConfigureAwait(false);
        await archived.SetAccessTierAsync(AccessTier.Archive).ConfigureAwait(false);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => archived.CreateSnapshotAsync())
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, (HttpStatusCode)rejected.Status);
        Assert.Equal("BlobArchived", rejected.ErrorCode);
    }

    [Fact]
    public async Task LeaseContractsHonorHistoricalDurationsHeadersAndContainerMutationRules()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 22, 9, 0, 0, TimeSpan.Zero));
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"lease-contract-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var blob = container.GetBlobClient("state.bin");
            await blob.UploadAsync(BinaryData.FromString("lease payload"));
            var initialBlob = (await blob.GetPropertiesAsync()).Value;
            var blobLeaseUri = AppendQuery(
                blob.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(10)),
                "comp=lease");

            using var transport = new HttpClient(application.Server.CreateHandler());
            await AssertLegacyBlobLeaseContractAsync(blobLeaseUri, transport);
            await AssertModernBlobLeaseContractAsync(blob, initialBlob, blobLeaseUri, transport);

            var credential = new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey);
            var accountSas = new AccountSasBuilder
            {
                Services = AccountSasServices.Blobs,
                ResourceTypes = AccountSasResourceTypes.Container,
                StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
                Protocol = SasProtocol.HttpsAndHttp
            };
            accountSas.SetPermissions(AccountSasPermissions.Write);
            var accountSasQuery = accountSas.ToSasQueryParameters(credential).ToString();
            var containerLeaseUri = AppendQuery(
                container.Uri,
                $"restype=container&comp=lease&{accountSasQuery}");
            await AssertLegacyContainerLeaseContractAsync(clock, container, containerLeaseUri, transport);
            await AssertModernContainerLeaseAndLegacyAclAsync(clock, container, containerLeaseUri, transport);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task AssertLegacyBlobLeaseContractAsync(Uri blobLeaseUri, HttpClient transport)
    {
        string legacyLeaseId;
        using (var acquire = CreateLeaseRequest(blobLeaseUri, "2009-09-19", "acquire"))
        using (var response = await transport.SendAsync(acquire).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            legacyLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
            Assert.Null(GetResponseHeaderOrDefault(response, "ETag"));
            Assert.Null(GetResponseHeaderOrDefault(response, "Last-Modified"));
        }

        using (var duration = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "acquire", duration: 30))
        using (var response = await transport.SendAsync(duration).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            await AssertVersionedErrorAsync(response, "UnsupportedHeader").ConfigureAwait(false);
        }

        using (var change = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "change", legacyLeaseId))
        using (var response = await transport.SendAsync(change).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
        }

        using (var release = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "release", legacyLeaseId))
        using (var response = await transport.SendAsync(release).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using (var renew = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "renew", legacyLeaseId))
        using (var response = await transport.SendAsync(renew).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(legacyLeaseId, response.Headers.GetValues("x-ms-lease-id").Single());
        }
        using (var release = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "release", legacyLeaseId))
        using (var response = await transport.SendAsync(release).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using (var breakLease = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "break"))
        using (var response = await transport.SendAsync(breakLease).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            Assert.Equal("0", response.Headers.GetValues("x-ms-lease-time").Single());
        }
    }

    private static async Task AssertModernBlobLeaseContractAsync(
        BlobClient blob, BlobProperties initialBlob, Uri blobLeaseUri, HttpClient transport)
    {
        using (var missingDuration = CreateLeaseRequest(blobLeaseUri, "2012-02-12", "acquire"))
        using (var response = await transport.SendAsync(missingDuration).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            await AssertVersionedErrorAsync(response, "MissingRequiredHeader").ConfigureAwait(false);
        }

        string modernLeaseId;
        using (var acquire = CreateLeaseRequest(blobLeaseUri, "2012-02-12", "acquire", duration: 15))
        using (var response = await transport.SendAsync(acquire).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            modernLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
            Assert.Null(GetResponseHeaderOrDefault(response, "ETag"));
            Assert.Null(GetResponseHeaderOrDefault(response, "Last-Modified"));
        }
        using (var release = CreateLeaseRequest(blobLeaseUri, "2012-02-12", "release", modernLeaseId))
        using (var response = await transport.SendAsync(release).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        using (var acquire = CreateLeaseRequest(blobLeaseUri, "2013-08-15", "acquire", duration: 15))
        using (var response = await transport.SendAsync(acquire).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            modernLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
            Assert.Equal(initialBlob.ETag.ToString(), GetResponseHeader(response, "ETag"));
            Assert.Equal(initialBlob.LastModified.ToString("R", CultureInfo.InvariantCulture),
                GetResponseHeader(response, "Last-Modified"));
        }
        using (var release = CreateLeaseRequest(blobLeaseUri, "2013-08-15", "release", modernLeaseId))
        using (var response = await transport.SendAsync(release).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var afterBlobLeases = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(initialBlob.ETag, afterBlobLeases.ETag);
        Assert.Equal(initialBlob.LastModified, afterBlobLeases.LastModified);
    }

    private static async Task AssertLegacyContainerLeaseContractAsync(
        AdjustableTimeProvider clock, BlobContainerClient container, Uri containerLeaseUri, HttpClient transport)
    {
        using (var unavailable = CreateLeaseRequest(containerLeaseUri, "2011-08-18", "acquire", duration: 15))
        using (var response = await transport.SendAsync(unavailable).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
        }

        var before = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value;
        clock.Advance(TimeSpan.FromMinutes(1));
        string containerLeaseId;
        using (var acquire = CreateLeaseRequest(containerLeaseUri, "2012-02-12", "acquire", duration: 15))
        using (var response = await transport.SendAsync(acquire).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            containerLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
            Assert.Null(GetResponseHeaderOrDefault(response, "ETag"));
            Assert.Null(GetResponseHeaderOrDefault(response, "Last-Modified"));
        }
        var after = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.NotEqual(before.ETag, after.ETag);
        Assert.NotEqual(before.LastModified, after.LastModified);
        using (var release = CreateLeaseRequest(containerLeaseUri, "2012-02-12", "release", containerLeaseId))
        using (var response = await transport.SendAsync(release).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static async Task AssertModernContainerLeaseAndLegacyAclAsync(
        AdjustableTimeProvider clock, BlobContainerClient container, Uri containerLeaseUri, HttpClient transport)
    {
        var before = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value;
        clock.Advance(TimeSpan.FromMinutes(1));
        string containerLeaseId;
        using (var acquire = CreateLeaseRequest(containerLeaseUri, "2013-08-15", "acquire", duration: 15))
        using (var response = await transport.SendAsync(acquire).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            containerLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
            Assert.Equal(before.ETag.ToString(), GetResponseHeader(response, "ETag"));
            Assert.Equal(before.LastModified.ToString("R", CultureInfo.InvariantCulture),
                GetResponseHeader(response, "Last-Modified"));
        }
        var after = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(before.ETag, after.ETag);
        Assert.Equal(before.LastModified, after.LastModified);
        using (var release = CreateLeaseRequest(containerLeaseUri, "2013-08-15", "release", containerLeaseId))
        using (var response = await transport.SendAsync(release).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var aclUri = AppendQuery(container.Uri, "restype=container&comp=acl");
        using (var acl = new HttpRequestMessage(HttpMethod.Put, aclUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            acl.Headers.TryAddWithoutValidation("x-ms-version", "2008-10-27");
            acl.Headers.TryAddWithoutValidation("x-ms-blob-public-access", "blob");
            AddSharedKeyLiteAuthorization(acl);
            using var response = await transport.SendAsync(acl).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
            Assert.False(response.Headers.Contains("x-ms-version"));
        }
    }

    [Fact]
    public async Task DeduplicationIsIsolatedAcrossAccountsAndChunksAreEncryptedAtRest()
    {
        var content = Enumerable.Repeat((byte)'Q', 96 * 1024).ToArray();
        var first = CreateClient(factory, SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var second = CreateClient(factory, SavaWebApplicationFactory.SecondAccountName, SavaWebApplicationFactory.SecondAccountKey);
        var firstContainer = first.GetBlobContainerClient($"isolation-{Guid.NewGuid():N}");
        var secondContainer = second.GetBlobContainerClient($"isolation-{Guid.NewGuid():N}");
        await firstContainer.CreateAsync();
        await secondContainer.CreateAsync();
        await firstContainer.GetBlobClient("same").UploadAsync(BinaryData.FromBytes(content));
        await secondContainer.GetBlobClient("same").UploadAsync(BinaryData.FromBytes(content));

        var accountRoots = Directory.EnumerateDirectories(Path.Combine(factory.DataPath, "chunks"))
            .Select(Path.GetFileName)
            .ToHashSet(StringComparer.Ordinal);
        Assert.Contains(SavaWebApplicationFactory.AccountName, accountRoots);
        Assert.Contains(SavaWebApplicationFactory.SecondAccountName, accountRoots);
        foreach (var path in Directory.EnumerateFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories))
        {
            var stored = await File.ReadAllBytesAsync(path);
            Assert.DoesNotContain(Enumerable.Repeat((byte)'Q', 64).ToArray(), stored);
        }
    }

    [Fact]
    public async Task ServiceAndAccountSasSignaturesScopesAndPermissionsAreEnforced()
    {
        var service = CreateClient(factory);
        var containerName = $"sas-{Guid.NewGuid():N}";
        var blobName = "protected.txt";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString("sas payload"));
        var credential = new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);

        var serviceBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp,
            ContentType = "text/x-sas-override"
        };
        serviceBuilder.SetPermissions(BlobSasPermissions.Read);
        var serviceSas = serviceBuilder.ToSasQueryParameters(credential);
        var sasBlob = CreateBlobClient(factory, new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{serviceSas}"));
        var sasDownload = await sasBlob.DownloadContentAsync();
        Assert.Equal("sas payload", sasDownload.Value.Content.ToString());
        Assert.Equal("text/x-sas-override", sasDownload.Value.Details.ContentType);
        var deniedDelete = await Assert.ThrowsAsync<RequestFailedException>(() => sasBlob.DeleteAsync());
        Assert.Equal(403, deniedDelete.Status);

        var accountBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Object,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountBuilder.SetPermissions(AccountSasPermissions.Read);
        var accountSas = accountBuilder.ToSasQueryParameters(credential);
        var accountBlob = CreateBlobClient(factory, new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{accountSas}"));
        Assert.Equal("sas payload", (await accountBlob.DownloadContentAsync()).Value.Content.ToString());
    }

    [Fact]
    public async Task SasProtocolAndIpRestrictionsReturnAzureErrorCodes()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"sas-restrictions-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("restricted.txt");
        await blob.UploadAsync(BinaryData.FromString("restricted payload"));

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);

        BlobSasBuilder CreateBuilder() => new()
        {
            BlobContainerName = container.Name,
            BlobName = blob.Name,
            Resource = "b",
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10)
        };

        Uri CreateUri(BlobSasBuilder builder)
        {
            builder.SetPermissions(BlobSasPermissions.Read);
            return new Uri(
                $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}" +
                $"?{builder.ToSasQueryParameters(credential)}");
        }

        async Task AssertErrorAsync(Uri uri, string code)
        {
            var client = CreateBlobClient(factory, uri);
            var failure = await Assert.ThrowsAsync<RequestFailedException>(() => client.DownloadContentAsync()).ConfigureAwait(false);
            Assert.Equal(StatusCodes.Status403Forbidden, failure.Status);
            Assert.Equal(code, failure.ErrorCode);
        }

        var protocolBuilder = CreateBuilder();
        protocolBuilder.Protocol = SasProtocol.Https;
        var protocolUri = CreateUri(protocolBuilder);
        await AssertErrorAsync(protocolUri, "AuthorizationProtocolMismatch");
        await AssertErrorAsync(ReplaceSasQueryValue(protocolUri, "spr", "ftp"), "AuthenticationFailed");

        var ipBuilder = CreateBuilder();
        ipBuilder.Protocol = SasProtocol.HttpsAndHttp;
        ipBuilder.IPRange = new SasIPRange(IPAddress.Parse("203.0.113.10"), IPAddress.None);
        var ipUri = CreateUri(ipBuilder);
        await AssertErrorAsync(ipUri, "AuthorizationSourceIPMismatch");
        await AssertErrorAsync(ReplaceSasQueryValue(ipUri, "sip", "not-an-ip"), "AuthenticationFailed");
    }

    private static Uri ReplaceSasQueryValue(Uri uri, string name, string value)
    {
        var pairs = uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries);
        var replaced = false;
        for (var index = 0; index < pairs.Length; index++)
        {
            var separator = pairs[index].IndexOf('=', StringComparison.Ordinal);
            var encodedName = separator < 0 ? pairs[index] : pairs[index][..separator];
            if (!string.Equals(Uri.UnescapeDataString(encodedName), name, StringComparison.Ordinal))
                continue;

            pairs[index] = $"{encodedName}={Uri.EscapeDataString(value)}";
            replaced = true;
            break;
        }

        Assert.True(replaced, $"The {name} query parameter was not present.");
        return new UriBuilder(uri) { Query = string.Join('&', pairs) }.Uri;
    }

    [Theory]
    [InlineData("a")]
    [InlineData("c")]
    public async Task LegacyGetContainerAclRejectsStoredCreateAndAddPermissions(string permission)
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"acl-version-{Guid.NewGuid():N}");
        await container.CreateAsync();

        async Task SetPolicyAsync(string value) => await container.SetAccessPolicyAsync(
            PublicAccessType.None,
            [new BlobSignedIdentifier
            {
                Id = "versioned-policy",
                AccessPolicy = new BlobAccessPolicy { Permissions = value }
            }]).ConfigureAwait(false);

        async Task<HttpResponseMessage> GetAclAsync(string version)
        {
            var uri = AppendQuery(container.Uri, "restype=container&comp=acl");
            using var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            AddSharedKeyLiteAuthorization(request);
            using var client = new HttpClient(factory.Server.CreateHandler());
            return await client.SendAsync(request).ConfigureAwait(false);
        }

        await SetPolicyAsync(permission).ConfigureAwait(true);
        using (var legacy = await GetAclAsync("2014-02-14").ConfigureAwait(true))
        {
            Assert.Equal(HttpStatusCode.Conflict, legacy.StatusCode);
            await AssertVersionedErrorAsync(legacy, "FeatureVersionMismatch").ConfigureAwait(true);
        }
        using (var modern = await GetAclAsync("2015-04-05").ConfigureAwait(true))
        {
            Assert.Equal(HttpStatusCode.OK, modern.StatusCode);
            Assert.Contains($"<Permission>{permission}</Permission>",
                await modern.Content.ReadAsStringAsync().ConfigureAwait(true), StringComparison.Ordinal);
        }

        await SetPolicyAsync("r").ConfigureAwait(true);
        using var legacyReadOnly = await GetAclAsync("2014-02-14").ConfigureAwait(true);
        Assert.Equal(HttpStatusCode.OK, legacyReadOnly.StatusCode);
    }

    [Theory]
    [InlineData(false, null)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task DisabledSharedKeyAccessRejectsKeyBasedAuthButAllowsUserDelegationSas(
        bool accountSharedKeyEnabled,
        bool? blobSharedKeyEnabled)
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:AllowSharedKeyAccess"] =
                    accountSharedKeyEnabled.ToString(),
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:AllowSharedKeyAccessForServices:Blob:Enabled"] =
                    blobSharedKeyEnabled?.ToString(),
                [$"Sava:BearerAuthentication:Principals:{SavaWebApplicationFactory.DelegatorObjectId}:Permissions"] =
                    "racwdxytlfmeiopk"
            });
        await using var applicationDisposal27 = application.ConfigureAwait(false);
        var token = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var bearer = CreateBearerClient(application, token);
        var container = bearer.GetBlobContainerClient($"no-shared-key-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("protected.txt");
        await blob.UploadAsync(BinaryData.FromString("delegated only"));

        var sharedKeyBlob = CreateClient(application)
            .GetBlobContainerClient(container.Name)
            .GetBlobClient(blob.Name);
        AssertKeyBasedAuthenticationNotPermitted(
            await Assert.ThrowsAsync<RequestFailedException>(() => sharedKeyBlob.DownloadContentAsync()));

        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(10);
        await AssertSharedKeySasDeniedAsync(application, container.Name, blob.Name, startsOn, expiresOn);

        var key = (await bearer.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn })).Value;
        var delegationBuilder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = blob.Name,
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        delegationBuilder.SetPermissions(BlobSasPermissions.Read);
        var delegationSasBlob = CreateBlobClient(
            application,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}" +
                $"?{delegationBuilder.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName)}"));
        Assert.Equal(
            "delegated only",
            (await delegationSasBlob.DownloadContentAsync()).Value.Content.ToString());
    }

    private static void AssertKeyBasedAuthenticationNotPermitted(RequestFailedException exception)
    {
        Assert.Equal(StatusCodes.Status403Forbidden, exception.Status);
        Assert.Equal("KeyBasedAuthenticationNotPermitted", exception.ErrorCode);
    }

    private static async Task AssertSharedKeySasDeniedAsync(
        SavaWebApplicationFactory application,
        string containerName,
        string blobName,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn)
    {
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var serviceBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        serviceBuilder.SetPermissions(BlobSasPermissions.Read);
        var serviceSasBlob = CreateBlobClient(
            application,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                $"?{serviceBuilder.ToSasQueryParameters(credential)}"));
        AssertKeyBasedAuthenticationNotPermitted(
            await Assert.ThrowsAsync<RequestFailedException>(() => serviceSasBlob.DownloadContentAsync())
                .ConfigureAwait(false));

        var accountBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Object,
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountBuilder.SetPermissions(AccountSasPermissions.Read);
        var accountSasBlob = CreateBlobClient(
            application,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                $"?{accountBuilder.ToSasQueryParameters(credential)}"));
        AssertKeyBasedAuthenticationNotPermitted(
            await Assert.ThrowsAsync<RequestFailedException>(() => accountSasBlob.DownloadContentAsync())
                .ConfigureAwait(false));
    }

    [Fact]
    public async Task HttpsOnlyAccountRejectsInsecureBlobRequestsBeforeAuthorization()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:EnableHttpsTrafficOnly"] = "true"
            });
        await using var applicationDisposal28 = application.ConfigureAwait(false);
        var secureService = CreateEncryptedClient(application, null, null);
        var container = secureService.GetBlobContainerClient($"https-only-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var secureBlob = container.GetBlobClient("content.txt");
        await secureBlob.UploadAsync(BinaryData.FromString("secure content"));
        Assert.Equal("secure content", (await secureBlob.DownloadContentAsync()).Value.Content.ToString());

        var insecureBlob = CreateClient(application)
            .GetBlobContainerClient(container.Name)
            .GetBlobClient(secureBlob.Name);
        var sdkError = await Assert.ThrowsAsync<RequestFailedException>(() => insecureBlob.DownloadContentAsync());
        Assert.Equal(StatusCodes.Status400BadRequest, sdkError.Status);
        Assert.Equal("AccountRequiresHttps", sdkError.ErrorCode);

        using var rawClient = new HttpClient(application.Server.CreateHandler());
        foreach (var request in new[]
        {
            new HttpRequestMessage(HttpMethod.Get,
                $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{secureBlob.Name}"),
            new HttpRequestMessage(HttpMethod.Options,
                $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}?restype=container"),
            new HttpRequestMessage(HttpMethod.Get,
                $"http://{SavaWebApplicationFactory.AccountName}.z1.web.localhost/{secureBlob.Name}"),
            new HttpRequestMessage(HttpMethod.Get,
                $"http://localhost/{SavaWebApplicationFactory.AccountName}/{container.Name}/{secureBlob.Name}")
        })
        {
            using (request)
            {
                request.Headers.TryAddWithoutValidation("x-forwarded-proto", "https");
                using var response = await rawClient.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                await AssertVersionedErrorAsync(response, "AccountRequiresHttps");
                Assert.Contains("The account being accessed does not support http.",
                    await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
            }
        }

        var otherAccount = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        await otherAccount.GetBlobContainerClient($"http-allowed-{Guid.NewGuid():N}").CreateAsync();

        using var health = await rawClient.GetAsync(new Uri("http://localhost/health/live", UriKind.RelativeOrAbsolute));
        Assert.Equal(HttpStatusCode.OK, health.StatusCode);
    }

    [Fact]
    public async Task DirectoryServiceAndUserDelegationSasAreBoundToAnHnsPrefix()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal29 = application.ConfigureAwait(false);
        var owner = CreateClient(application);
        var container = owner.GetBlobContainerClient($"directory-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("signed/directory/child.txt").UploadAsync(BinaryData.FromString("child"));
        await container.GetBlobClient("signed/directory/nested/leaf.txt").UploadAsync(BinaryData.FromString("leaf"));
        await container.GetBlobClient("signed/sibling.txt").UploadAsync(BinaryData.FromString("sibling"));

        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(10);
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        await AssertServiceDirectorySasAsync(application, container, startsOn, expiresOn, credential);
        await AssertUserDelegationDirectorySasAsync(application, container, startsOn, expiresOn);
        await AssertFlatDirectorySasRejectedAsync(factory, startsOn, expiresOn, credential);
    }

    private static async Task AssertServiceDirectorySasAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn,
        StorageSharedKeyCredential credential)
    {
        var serviceBuilder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = "signed/directory",
            Resource = "d",
            IsDirectory = true,
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        serviceBuilder.SetPermissions(BlobContainerSasPermissions.Read);
        var serviceSas = serviceBuilder.ToSasQueryParameters(credential);
        Assert.Equal(2, serviceSas.DirectoryDepth);

        BlobClient GetServiceSasBlob(string name) => CreateBlobClient(
            application,
            new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{name}?{serviceSas}"));

        var directoryProperties = await GetServiceSasBlob("signed/directory").GetPropertiesAsync()
            .ConfigureAwait(false);
        Assert.Equal("directory",
            directoryProperties.GetRawResponse().Headers.TryGetValue("x-ms-resource-type", out var resourceType)
                ? resourceType
                : null);
        Assert.Equal("child",
            (await GetServiceSasBlob("signed/directory/child.txt").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
        Assert.Equal("leaf",
            (await GetServiceSasBlob("signed/directory/nested/leaf.txt").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
        var deniedSibling = await Assert.ThrowsAsync<RequestFailedException>(() =>
            GetServiceSasBlob("signed/sibling.txt").DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedSibling.Status);
        var deniedParent = await Assert.ThrowsAsync<RequestFailedException>(() =>
            GetServiceSasBlob("signed").GetPropertiesAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedParent.Status);
    }

    private static async Task AssertUserDelegationDirectorySasAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn)
    {
        var token = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var delegator = CreateBearerClient(application, token);
        var key = await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn }).ConfigureAwait(false);
        var delegatedBuilder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = "signed/directory",
            Resource = "d",
            IsDirectory = true,
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp,
            CorrelationId = Guid.NewGuid().ToString()
        };
        delegatedBuilder.SetPermissions(BlobContainerSasPermissions.Read);
        var delegatedSas = delegatedBuilder.ToSasQueryParameters(
            key.Value,
            SavaWebApplicationFactory.AccountName);

        BlobClient GetDelegatedBlob(string name) => CreateBlobClient(
            application,
            new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{name}?{delegatedSas}"));

        Assert.Equal("child",
            (await GetDelegatedBlob("signed/directory/child.txt").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
        var deniedSibling = await Assert.ThrowsAsync<RequestFailedException>(() =>
            GetDelegatedBlob("signed/sibling.txt").DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, deniedSibling.Status);

        delegatedBuilder.PreauthorizedAgentObjectId = Guid.NewGuid().ToString();
        var impersonationSas = delegatedBuilder.ToSasQueryParameters(
            key.Value,
            SavaWebApplicationFactory.AccountName);
        var impersonationBlob = CreateBlobClient(
            application,
            new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/signed/directory/child.txt?{impersonationSas}"));
        var deniedImpersonation = await Assert.ThrowsAsync<RequestFailedException>(() =>
            impersonationBlob.DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal("AuthorizationFailure", deniedImpersonation.ErrorCode);
    }

    private static async Task AssertFlatDirectorySasRejectedAsync(
        SavaWebApplicationFactory flatApplication,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn,
        StorageSharedKeyCredential credential)
    {
        var flatOwner = CreateClient(flatApplication);
        var flatContainer = flatOwner.GetBlobContainerClient($"flat-directory-sas-{Guid.NewGuid():N}");
        await flatContainer.CreateAsync().ConfigureAwait(false);
        await flatContainer.GetBlobClient("signed/directory").UploadAsync(BinaryData.FromString("flat"))
            .ConfigureAwait(false);
        var flatBuilder = new BlobSasBuilder
        {
            BlobContainerName = flatContainer.Name,
            BlobName = "signed/directory",
            Resource = "d",
            IsDirectory = true,
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        flatBuilder.SetPermissions(BlobContainerSasPermissions.Read);
        var flatSas = flatBuilder.ToSasQueryParameters(credential);
        var flatDirectory = CreateBlobClient(
            flatApplication,
            new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{flatContainer.Name}/signed/directory?{flatSas}"));
        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            flatDirectory.DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, denied.Status);
    }

    [Fact]
    public async Task ContainerSasReadsSnapshotsWithoutSigningTheSnapshotIdentifier()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"container-snapshot-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("state.txt");
        await blob.UploadAsync(BinaryData.FromString("snapshot state"));
        var snapshot = (await blob.CreateSnapshotAsync()).Value.Snapshot;
        await blob.UploadAsync(BinaryData.FromString("current state"), overwrite: true);

        var builder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            Resource = "c",
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(BlobContainerSasPermissions.Read);
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var sas = builder.ToSasQueryParameters(credential);
        var snapshotClient = CreateBlobClient(
            factory,
            new Uri(
                $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}" +
                $"?snapshot={Uri.EscapeDataString(snapshot)}&{sas}"));

        Assert.Equal(
            "snapshot state",
            (await snapshotClient.DownloadContentAsync()).Value.Content.ToString());
    }

    [Fact]
    public async Task SasEncryptionScopeMustBeSignedByASupportedVersion()
    {
        var owner = CreateClient(factory);
        var container = owner.GetBlobContainerClient($"sas-scope-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        using var transport = new HttpClient(factory.Server.CreateHandler());
        await AssertCurrentEncryptionScopeSasAsync(factory, container, credential, transport);

        const string legacyBlobName = "legacy-scope.txt";
        var legacyBlobUri = CreateLegacyEncryptionScopeSasUri(container, legacyBlobName);

        using (var validLegacy = new HttpRequestMessage(HttpMethod.Put, legacyBlobUri)
        {
            Content = new StringContent("legacy signature is valid")
        })
        {
            validLegacy.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(validLegacy);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        using (var unsignedScope = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(legacyBlobUri, "ses=unsigned-scope"))
        {
            Content = new StringContent("must not replace")
        })
        {
            unsignedScope.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(unsignedScope);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var legacyProperties = await container.GetBlobClient(legacyBlobName).GetPropertiesAsync();
        Assert.Null(legacyProperties.Value.EncryptionScope);
        Assert.Equal(
            "legacy signature is valid",
            (await container.GetBlobClient(legacyBlobName).DownloadContentAsync()).Value.Content.ToString());
    }

    private static async Task AssertCurrentEncryptionScopeSasAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        StorageSharedKeyCredential credential,
        HttpClient transport)
    {
        const string signedBlobName = "signed-scope.txt";
        const string signedScope = "signed-scope";
        var builder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = signedBlobName,
            Resource = "b",
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp,
            EncryptionScope = signedScope
        };
        builder.SetPermissions(BlobSasPermissions.Create | BlobSasPermissions.Write);
        var sas = builder.ToSasQueryParameters(credential);
        var signedBlobUri = new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{signedBlobName}?{sas}");
        var signedBlob = CreateBlobClient(application, signedBlobUri);
        await signedBlob.UploadAsync(BinaryData.FromString("signed encryption scope")).ConfigureAwait(false);
        Assert.Equal(signedScope,
            (await container.GetBlobClient(signedBlobName).GetPropertiesAsync().ConfigureAwait(false)).Value.EncryptionScope);

        using (var wrongScope = new HttpRequestMessage(HttpMethod.Put, signedBlobUri)
        {
            Content = new StringContent("wrong scope")
        })
        {
            wrongScope.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            wrongScope.Headers.TryAddWithoutValidation("x-ms-encryption-scope", "wrong-scope");
            using var response = await transport.SendAsync(wrongScope).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private static Uri CreateLegacyEncryptionScopeSasUri(BlobContainerClient container, string blobName)
    {
        const string legacyVersion = "2019-12-12";
        const string permissions = "cw";
        const string protocol = "https";
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1)
            .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(10)
            .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var canonicalResource = $"/blob/{SavaWebApplicationFactory.AccountName}/{container.Name}/{blobName}";
        var stringToSign = string.Join(
            '\n',
            permissions,
            startsOn,
            expiresOn,
            canonicalResource,
            string.Empty,
            string.Empty,
            protocol,
            legacyVersion,
            "b",
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty);
        using var hmac = new HMACSHA256(Convert.FromBase64String(SavaWebApplicationFactory.AccountKey));
        var signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
        return new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blobName}" +
            $"?sp={permissions}" +
            $"&st={Uri.EscapeDataString(startsOn)}" +
            $"&se={Uri.EscapeDataString(expiresOn)}" +
            $"&spr={protocol}" +
            $"&sv={legacyVersion}" +
            "&sr=b" +
            $"&sig={Uri.EscapeDataString(signature)}");
    }

    [Fact]
    public async Task LegacyServiceSasUsesVersionedFieldsAndCanonicalResources()
    {
        var owner = CreateClient(factory);
        var container = owner.GetBlobContainerClient($"legacy-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("legacy.txt");
        await blob.UploadAsync(BinaryData.FromString("legacy sas payload"));
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1)
            .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(29)
            .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

        Uri CreateSasUri(string? version, string? contentType = null, DateTimeOffset? expiry = null) =>
            CreateLegacyServiceSasUri(container, blob, startsOn, expiresOn, version, contentType, expiry);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        foreach (var uri in new[]
                 {
                     CreateSasUri(version: null),
                     CreateSasUri("2012-02-12"),
                     CreateSasUri("2013-08-15", "text/x-2013-sas"),
                     CreateSasUri("2015-02-21", "text/x-2015-sas")
                 })
        {
            using var response = await transport.GetAsync(uri);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("legacy sas payload", await response.Content.ReadAsStringAsync());
            if (uri.Query.Contains("rsct=", StringComparison.Ordinal))
            {
                Assert.StartsWith(
                    uri.Query.Contains("2013-08-15", StringComparison.Ordinal)
                        ? "text/x-2013-sas"
                        : "text/x-2015-sas",
                    response.Content.Headers.ContentType?.ToString(),
                    StringComparison.Ordinal);
            }
        }

        using (var unsignedOverride = await transport.GetAsync(
                   AppendQuery(CreateSasUri("2012-02-12"), "rsct=unsigned")))
        {
            Assert.Equal(HttpStatusCode.Forbidden, unsignedOverride.StatusCode);
            await AssertVersionedErrorAsync(unsignedOverride, "AuthenticationFailed");
        }
        using (var unsignedProtocol = await transport.GetAsync(
                   AppendQuery(CreateSasUri("2013-08-15"), "spr=https")))
        {
            Assert.Equal(HttpStatusCode.Forbidden, unsignedProtocol.StatusCode);
            await AssertVersionedErrorAsync(unsignedProtocol, "AuthenticationFailed");
        }
        using (var excessiveLegacyLifetime = await transport.GetAsync(
                   CreateSasUri(version: null, expiry: DateTimeOffset.UtcNow.AddHours(2))))
        {
            Assert.Equal(HttpStatusCode.Forbidden, excessiveLegacyLifetime.StatusCode);
            await AssertVersionedErrorAsync(excessiveLegacyLifetime, "AuthenticationFailed");
        }
    }

    private static Uri CreateLegacyServiceSasUri(
        BlobContainerClient container,
        BlobClient blob,
        string startsOn,
        string expiresOn,
        string? version,
        string? contentType,
        DateTimeOffset? expiry)
    {
        var signedExpiry = (expiry ?? DateTimeOffset.Parse(expiresOn, CultureInfo.InvariantCulture))
            .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var parsedVersion = version is null
            ? new DateOnly(2009, 9, 19)
            : DateOnly.ParseExact(version, "yyyy-MM-dd", CultureInfo.InvariantCulture);
        var canonicalResource = parsedVersion >= new DateOnly(2015, 2, 21)
            ? $"/blob/{SavaWebApplicationFactory.AccountName}/{container.Name}/{blob.Name}"
            : $"/{SavaWebApplicationFactory.AccountName}/{container.Name}/{blob.Name}";
        var fields = new List<string>
        {
            "r",
            startsOn,
            signedExpiry,
            canonicalResource,
            string.Empty
        };
        if (version is not null)
        {
            if (parsedVersion >= new DateOnly(2015, 4, 5))
            {
                fields.Add(string.Empty);
                fields.Add("https,http");
            }
            fields.Add(version);
            if (parsedVersion >= new DateOnly(2018, 11, 9))
            {
                fields.Add("b");
                fields.Add(string.Empty);
            }
            if (parsedVersion >= new DateOnly(2020, 12, 6))
                fields.Add(string.Empty);
            if (parsedVersion >= new DateOnly(2013, 8, 15))
            {
                fields.Add(string.Empty);
                fields.Add(string.Empty);
                fields.Add(string.Empty);
                fields.Add(string.Empty);
                fields.Add(contentType ?? string.Empty);
            }
        }

        using var hmac = new HMACSHA256(Convert.FromBase64String(SavaWebApplicationFactory.AccountKey));
        var signature = Convert.ToBase64String(
            hmac.ComputeHash(Encoding.UTF8.GetBytes(string.Join('\n', fields))));
        var query =
            $"sp=r&st={Uri.EscapeDataString(startsOn)}&se={Uri.EscapeDataString(signedExpiry)}" +
            (version is null ? string.Empty : $"&sv={version}") +
            "&sr=b" +
            (parsedVersion >= new DateOnly(2015, 4, 5) ? "&spr=https%2Chttp" : string.Empty) +
            (contentType is null ? string.Empty : $"&rsct={Uri.EscapeDataString(contentType)}") +
            $"&sig={Uri.EscapeDataString(signature)}";
        return new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}?{query}");
    }

    [Fact]
    public async Task UserDelegationSasCanonicalizesSignedHeadersAndEncodedQueryNames()
    {
        var owner = CreateClient(factory);
        var container = owner.GetBlobContainerClient($"dynamic-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("dynamic.txt");
        await blob.UploadAsync(BinaryData.FromString("dynamic sas payload"));

        var bearerToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var delegator = CreateBearerClient(factory, bearerToken);
        var startsAt = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(10);
        var key = (await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresAt) { StartsOn = startsAt })).Value;
        var uri = CreateDynamicUserDelegationSasUri(container, blob, key, startsAt, expiresAt);
        using var transport = new HttpClient(factory.Server.CreateHandler());

        static HttpRequestMessage CreateRequest(Uri target, string firstHeader)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, target);
            request.Headers.TryAddWithoutValidation("x-dynamic-first", [firstHeader, "789"]);
            request.Headers.TryAddWithoutValidation("x-dynamic-second", "456");
            return request;
        }

        using (var request = CreateRequest(uri, "123"))
        using (var response = await transport.SendAsync(request))
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("dynamic sas payload", await response.Content.ReadAsStringAsync());
        }
        using (var request = CreateRequest(uri, "changed"))
        using (var response = await transport.SendAsync(request))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthenticationFailed", response.Headers.GetValues("x-ms-error-code").Single());
        }
        var missingEncodedName = new Uri(uri.AbsoluteUri.Replace("&day%2Cid=mon123", string.Empty, StringComparison.Ordinal));
        using (var request = CreateRequest(missingEncodedName, "123"))
        using (var response = await transport.SendAsync(request))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthenticationFailed", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private static Uri CreateDynamicUserDelegationSasUri(
        BlobContainerClient container,
        BlobClient blob,
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        DateTimeOffset startsAt,
        DateTimeOffset expiresAt)
    {
        var signedStart = startsAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var signedExpiry = expiresAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var keyStart = key.SignedStartsOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var keyExpiry = key.SignedExpiresOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        const string signedVersion = "2026-04-06";
        const string signedHeaders = "x-dynamic-first,x-dynamic-second";
        const string signedQuery = "operation,day%2Cid";
        var canonicalResource = $"/blob/{SavaWebApplicationFactory.AccountName}/{container.Name}/{blob.Name}";
        var signature = SignDynamicUserDelegationSas(
            key, signedStart, signedExpiry, keyStart, keyExpiry, canonicalResource, signedVersion);
        var query =
            $"sp=r&st={Uri.EscapeDataString(signedStart)}&se={Uri.EscapeDataString(signedExpiry)}" +
            $"&skoid={key.SignedObjectId}&sktid={key.SignedTenantId}" +
            $"&skt={Uri.EscapeDataString(keyStart)}&ske={Uri.EscapeDataString(keyExpiry)}" +
            $"&sks={key.SignedService}&skv={key.SignedVersion}" +
            "&spr=https%2Chttp" +
            $"&sv={signedVersion}&sr=b&srh={signedHeaders}&srq={signedQuery}" +
            "&operation=update&day%2Cid=mon123" +
            $"&sig={Uri.EscapeDataString(signature)}";
        return new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}?{query}");
    }

    private static string SignDynamicUserDelegationSas(
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        string signedStart,
        string signedExpiry,
        string keyStart,
        string keyExpiry,
        string canonicalResource,
        string signedVersion)
    {
        const string canonicalizedHeaders = "x-dynamic-first:123,789\nx-dynamic-second:456\n";
        const string canonicalizedQuery = "\noperation=update\nday,id=mon123";
        var stringToSign = string.Join(
            '\n',
            "r",
            signedStart,
            signedExpiry,
            canonicalResource,
            key.SignedObjectId,
            key.SignedTenantId,
            keyStart,
            keyExpiry,
            key.SignedService,
            key.SignedVersion,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            "https,http",
            signedVersion,
            "b",
            string.Empty,
            string.Empty,
            canonicalizedHeaders,
            canonicalizedQuery,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty);
        using var hmac = new HMACSHA256(Convert.FromBase64String(key.Value));
        return Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
    }

    [Fact]
    public async Task UserBoundDelegationSasRequiresTheSignedBearerIdentityAndTenant()
    {
        var owner = CreateClient(factory);
        var container = owner.GetBlobContainerClient($"user-bound-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("bound.txt");
        await blob.UploadAsync(BinaryData.FromString("user-bound payload"));

        var delegatorToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var delegator = CreateBearerClient(factory, delegatorToken);
        var startsAt = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(10);
        var key = (await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresAt) { StartsOn = startsAt })).Value;
        var delegatedUserObjectId = Guid.NewGuid().ToString();
        const string signedVersion = "2025-07-05";
        var sasUri = CreateUserBoundDelegationSasUri(
            container, blob, key, startsAt, expiresAt, delegatedUserObjectId, signedVersion);
        var directUri = new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}");
        using var transport = new HttpClient(factory.Server.CreateHandler());
        await AssertUserBoundSasRequestsAsync(
            transport, directUri, sasUri, delegatedUserObjectId, signedVersion);
    }

    private static async Task AssertUserBoundSasRequestsAsync(
        HttpClient transport,
        Uri directUri,
        Uri sasUri,
        string delegatedUserObjectId,
        string signedVersion)
    {
        static HttpRequestMessage CreateRequest(Uri target, string token, string version)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, target);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            return request;
        }

        var targetToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            delegatedUserObjectId,
            SavaWebApplicationFactory.TenantId);
        using (var directRequest = CreateRequest(directUri, targetToken, signedVersion))
        using (var response = await transport.SendAsync(directRequest).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var validRequest = CreateRequest(sasUri, targetToken, signedVersion))
        using (var response = await transport.SendAsync(validRequest).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("user-bound payload", await response.Content.ReadAsStringAsync().ConfigureAwait(false));
        }

        var wrongObjectToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            Guid.NewGuid().ToString(),
            SavaWebApplicationFactory.TenantId);
        using (var wrongObjectRequest = CreateRequest(sasUri, wrongObjectToken, signedVersion))
        using (var response = await transport.SendAsync(wrongObjectRequest).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var wrongTenantToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            delegatedUserObjectId,
            Guid.NewGuid().ToString());
        using (var wrongTenantRequest = CreateRequest(sasUri, wrongTenantToken, signedVersion))
        using (var response = await transport.SendAsync(wrongTenantRequest).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var response = await transport.GetAsync(sasUri).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private static Uri CreateUserBoundDelegationSasUri(
        BlobContainerClient container,
        BlobClient blob,
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        DateTimeOffset startsAt,
        DateTimeOffset expiresAt,
        string delegatedUserObjectId,
        string signedVersion)
    {
        var signedStart = startsAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var signedExpiry = expiresAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var keyStart = key.SignedStartsOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var keyExpiry = key.SignedExpiresOn.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var canonicalResource = $"/blob/{SavaWebApplicationFactory.AccountName}/{container.Name}/{blob.Name}";
        var signature = SignUserBoundDelegationSas(
            key, signedStart, signedExpiry, keyStart, keyExpiry, canonicalResource, delegatedUserObjectId, signedVersion);
        var query =
            $"sp=r&st={Uri.EscapeDataString(signedStart)}&se={Uri.EscapeDataString(signedExpiry)}" +
            $"&skoid={key.SignedObjectId}&sktid={key.SignedTenantId}" +
            $"&skt={Uri.EscapeDataString(keyStart)}&ske={Uri.EscapeDataString(keyExpiry)}" +
            $"&sks={key.SignedService}&skv={key.SignedVersion}" +
            $"&sduoid={delegatedUserObjectId}&spr=https%2Chttp" +
            $"&sv={signedVersion}&sr=b&sig={Uri.EscapeDataString(signature)}";
        return new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}?{query}");
    }

    private static string SignUserBoundDelegationSas(
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        string signedStart,
        string signedExpiry,
        string keyStart,
        string keyExpiry,
        string canonicalResource,
        string delegatedUserObjectId,
        string signedVersion)
    {
        var stringToSign = string.Join(
            '\n',
            "r",
            signedStart,
            signedExpiry,
            canonicalResource,
            key.SignedObjectId,
            key.SignedTenantId,
            keyStart,
            keyExpiry,
            key.SignedService,
            key.SignedVersion,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            delegatedUserObjectId,
            string.Empty,
            "https,http",
            signedVersion,
            "b",
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty);
        using var hmac = new HMACSHA256(Convert.FromBase64String(key.Value));
        return Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
    }

    [Fact]
    public async Task UserBoundSasAccountPolicyLogsOrBlocksUnboundTokens()
    {
        await AssertUserBoundSasLogPolicyAsync();

        var blockApplication = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:RequireUserBoundUserDelegationSas"] =
                    "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:RequireUserBoundUserDelegationSasAction"] =
                    "Block"
            });
        await using var blockApplicationDisposal30 = blockApplication.ConfigureAwait(false);
        var blockOwner = CreateClient(blockApplication);
        var blockContainer = blockOwner.GetBlobContainerClient($"user-bound-block-{Guid.NewGuid():N}");
        await blockContainer.CreateAsync();
        var blockBlob = blockContainer.GetBlobClient("protected.txt");
        await blockBlob.UploadAsync(BinaryData.FromString("bound policy payload"));

        var serviceSas = CreateBlobClient(
            blockApplication,
            HttpsSasUri(blockBlob, BlobSasPermissions.Read));
        AssertSasPolicyAuthorizationFailure(
            await Assert.ThrowsAsync<RequestFailedException>(() => serviceSas.DownloadContentAsync()));

        var delegatorToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var delegator = CreateBearerClient(blockApplication, delegatorToken);
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(10);
        var key = (await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn })).Value;
        await AssertUnboundUserDelegationBlockedAsync(
            blockApplication, blockContainer, blockBlob, key, startsOn, expiresOn);
        await AssertBoundUserDelegationAcceptedAsync(
            blockApplication, blockContainer, blockBlob, key, startsOn, expiresOn);
    }

    private static async Task AssertUserBoundSasLogPolicyAsync()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:RequireUserBoundUserDelegationSas"] =
                    "true"
            });
        await using (application.ConfigureAwait(false))
        {
            var owner = CreateClient(application);
            var container = owner.GetBlobContainerClient($"user-bound-log-{Guid.NewGuid():N}");
            await container.CreateAsync().ConfigureAwait(false);
            var blob = container.GetBlobClient("allowed.txt");
            await blob.UploadAsync(BinaryData.FromString("log-only policy")).ConfigureAwait(false);
            var unboundServiceSas = CreateBlobClient(application, HttpsSasUri(blob, BlobSasPermissions.Read));
            Assert.Equal("log-only policy",
                (await unboundServiceSas.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        }
    }

    private static void AssertSasPolicyAuthorizationFailure(RequestFailedException exception)
    {
        Assert.Equal(StatusCodes.Status403Forbidden, exception.Status);
        Assert.Equal("AuthorizationFailure", exception.ErrorCode);
    }

    private static async Task AssertUnboundUserDelegationBlockedAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient blob,
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn)
    {
        var builder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = blob.Name,
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(BlobSasPermissions.Read);
        var sas = CreateBlobClient(
            application,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/" +
                $"{container.Name}/{blob.Name}?" +
                builder.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName)));
        AssertSasPolicyAuthorizationFailure(
            await Assert.ThrowsAsync<RequestFailedException>(() => sas.DownloadContentAsync()).ConfigureAwait(false));
    }

    private static async Task AssertBoundUserDelegationAcceptedAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient blob,
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn)
    {
        var delegatedUserObjectId = Guid.NewGuid().ToString();
        var builder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = blob.Name,
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp,
            DelegatedUserObjectId = delegatedUserObjectId
        };
        builder.SetPermissions(BlobSasPermissions.Read);
        var boundSas = builder.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName);
        var boundUri = new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}?{boundSas}");
        var delegatedUserToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            delegatedUserObjectId,
            SavaWebApplicationFactory.TenantId);
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Get, boundUri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", delegatedUserToken);
        request.Headers.TryAddWithoutValidation("x-ms-version", "2025-07-05");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("bound policy payload", await response.Content.ReadAsStringAsync().ConfigureAwait(false));
    }

    [Fact]
    public async Task SasExpirationPolicyLogsOrBlocksEveryAdHocSasType()
    {
        await AssertSasExpirationLogPolicyAsync();

        var blockApplication = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:SasExpirationPeriod"] =
                    "00:05:00",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:SasExpirationAction"] =
                    "Block"
            });
        await using var blockApplicationDisposal31 = blockApplication.ConfigureAwait(false);
        var ownerClient = CreateClient(blockApplication);
        var blockContainer = ownerClient.GetBlobContainerClient($"sas-expiry-block-{Guid.NewGuid():N}");
        await blockContainer.CreateAsync();
        var blockBlob = blockContainer.GetBlobClient("protected.txt");
        await blockBlob.UploadAsync(BinaryData.FromString("expiration policy payload"));
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
        await AssertSasExpirationServiceCasesAsync(
            blockApplication, blockContainer, blockBlob, credential, startsOn);
        await AssertSasExpirationAdHocDenialsAsync(
            blockApplication, blockContainer, blockBlob, credential, startsOn);
        await AssertSasExpirationStoredPolicyExemptionAsync(
            blockApplication, blockContainer, blockBlob, credential, startsOn);
    }

    private static async Task AssertSasExpirationLogPolicyAsync()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:SasExpirationPeriod"] = "00:05:00"
            });
        await using (application.ConfigureAwait(false))
        {
            var owner = CreateClient(application);
            var container = owner.GetBlobContainerClient($"sas-expiry-log-{Guid.NewGuid():N}");
            await container.CreateAsync().ConfigureAwait(false);
            var blob = container.GetBlobClient("allowed.txt");
            await blob.UploadAsync(BinaryData.FromString("log-only expiration policy")).ConfigureAwait(false);
            var loggedMissingStart = CreateBlobClient(application, HttpsSasUri(blob, BlobSasPermissions.Read));
            Assert.Equal("log-only expiration policy",
                (await loggedMissingStart.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        }
    }

    private static async Task AssertSasExpirationServiceCasesAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient blob,
        StorageSharedKeyCredential credential,
        DateTimeOffset startsOn)
    {
        BlobClient CreateServiceSas(DateTimeOffset? start, DateTimeOffset expiry)
        {
            var builder = new BlobSasBuilder
            {
                BlobContainerName = container.Name,
                BlobName = blob.Name,
                Resource = "b",
                StartsOn = start ?? default,
                ExpiresOn = expiry,
                Protocol = SasProtocol.HttpsAndHttp
            };
            builder.SetPermissions(BlobSasPermissions.Read);
            return CreateBlobClient(
                application,
                new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/" +
                        $"{container.Name}/{blob.Name}?{builder.ToSasQueryParameters(credential)}"));
        }

        var compliant = CreateServiceSas(startsOn, startsOn.AddMinutes(5));
        Assert.Equal("expiration policy payload",
            (await compliant.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var overlong = CreateServiceSas(startsOn, startsOn.AddMinutes(5).AddSeconds(1));
        AssertSasPolicyAuthorizationFailure(
            await Assert.ThrowsAsync<RequestFailedException>(() => overlong.DownloadContentAsync())
                .ConfigureAwait(false));
        var missingStart = CreateServiceSas(null, DateTimeOffset.UtcNow.AddMinutes(4));
        AssertSasPolicyAuthorizationFailure(
            await Assert.ThrowsAsync<RequestFailedException>(() => missingStart.DownloadContentAsync())
                .ConfigureAwait(false));
    }

    private static async Task AssertSasExpirationAdHocDenialsAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient blob,
        StorageSharedKeyCredential credential,
        DateTimeOffset startsOn)
    {
        var accountBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Object,
            StartsOn = startsOn,
            ExpiresOn = startsOn.AddMinutes(6),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountBuilder.SetPermissions(AccountSasPermissions.Read);
        var accountSas = CreateBlobClient(
            application,
            new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/" +
                    $"{container.Name}/{blob.Name}?{accountBuilder.ToSasQueryParameters(credential)}"));
        AssertSasPolicyAuthorizationFailure(
            await Assert.ThrowsAsync<RequestFailedException>(() => accountSas.DownloadContentAsync())
                .ConfigureAwait(false));

        var delegatorToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var delegator = CreateBearerClient(application, delegatorToken);
        var key = (await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(startsOn.AddMinutes(10)) { StartsOn = startsOn })
            .ConfigureAwait(false)).Value;
        var delegationBuilder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = blob.Name,
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = startsOn.AddMinutes(6),
            Protocol = SasProtocol.HttpsAndHttp
        };
        delegationBuilder.SetPermissions(BlobSasPermissions.Read);
        var delegationSas = CreateBlobClient(
            application,
            new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/" +
                    $"{container.Name}/{blob.Name}?" +
                    delegationBuilder.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName)));
        AssertSasPolicyAuthorizationFailure(
            await Assert.ThrowsAsync<RequestFailedException>(() => delegationSas.DownloadContentAsync())
                .ConfigureAwait(false));
    }

    private static async Task AssertSasExpirationStoredPolicyExemptionAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        BlobClient blob,
        StorageSharedKeyCredential credential,
        DateTimeOffset startsOn)
    {
        await container.SetAccessPolicyAsync(
            PublicAccessType.None,
            [new BlobSignedIdentifier
            {
                Id = "long-lived-policy",
                AccessPolicy = new BlobAccessPolicy
                {
                    StartsOn = startsOn,
                    ExpiresOn = startsOn.AddMinutes(10),
                    Permissions = "r"
                }
            }]).ConfigureAwait(false);
        var policyBuilder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = blob.Name,
            Resource = "b",
            Identifier = "long-lived-policy",
            Protocol = SasProtocol.HttpsAndHttp
        };
        var policySas = CreateBlobClient(
            application,
            new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/" +
                    $"{container.Name}/{blob.Name}?{policyBuilder.ToSasQueryParameters(credential)}"));
        Assert.Equal("expiration policy payload",
            (await policySas.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    [Fact]
    public async Task SasRejectsMissingRequiredFieldsAndNoncanonicalAuthorizationSets()
    {
        var owner = CreateClient(factory);
        var container = owner.GetBlobContainerClient($"sas-shape-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("shape.txt");
        await blob.UploadAsync(BinaryData.FromString("sas shape payload"));
        var startsAt = DateTimeOffset.UtcNow.AddMinutes(-1);
        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(10);
        var signedStart = startsAt.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var signedExpiry = expiresAt.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var canonicalResource =
            $"/blob/{SavaWebApplicationFactory.AccountName}/{container.Name}/{blob.Name}";
        var accountKey = Convert.FromBase64String(SavaWebApplicationFactory.AccountKey);

        var delegatorToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var delegator = CreateBearerClient(factory, delegatorToken);
        var key = (await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresAt) { StartsOn = startsAt })).Value;
        var keyStart = key.SignedStartsOn.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var keyExpiry = key.SignedExpiresOn.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var sasFixture = new SasShapeFixture(
            container.Name, blob.Name, signedStart, signedExpiry,
            canonicalResource, accountKey, key, keyStart, keyExpiry);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        async Task AssertAuthenticationFailedAsync(Uri uri)
        {
            using var response = await transport.GetAsync(uri).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal(
                "AuthenticationFailed",
                response.Headers.GetValues("x-ms-error-code").Single());
        }

        await AssertAuthenticationFailedAsync(CreateMalformedServiceSasUri(sasFixture, "wr", "2023-11-03"));
        await AssertAuthenticationFailedAsync(CreateMalformedServiceSasUri(sasFixture, "rr", "2023-11-03"));
        await AssertAuthenticationFailedAsync(CreateMalformedServiceSasUri(sasFixture, "rz", "2023-11-03"));
        await AssertAuthenticationFailedAsync(CreateMalformedServiceSasUri(sasFixture, "rt", "2018-11-09"));
        await AssertAuthenticationFailedAsync(CreateMalformedAccountSasUri(sasFixture, "qb", "o", "r"));
        await AssertAuthenticationFailedAsync(CreateMalformedAccountSasUri(sasFixture, "b", "oc", "r"));
        await AssertAuthenticationFailedAsync(CreateMalformedAccountSasUri(sasFixture, "b", "o", "rr"));
        await AssertAuthenticationFailedAsync(CreateMalformedUserDelegationSasUri(sasFixture, null, signedExpiry));
        await AssertAuthenticationFailedAsync(CreateMalformedUserDelegationSasUri(sasFixture, "r", null));
    }

    private sealed record SasShapeFixture(
        string ContainerName,
        string BlobName,
        string SignedStart,
        string SignedExpiry,
        string CanonicalResource,
        byte[] AccountKey,
        Azure.Storage.Blobs.Models.UserDelegationKey DelegationKey,
        string KeyStart,
        string KeyExpiry);

    private static string SignSasShape(byte[] key, string stringToSign)
    {
        using var hmac = new HMACSHA256(key);
        return Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
    }

    private static Uri CreateMalformedServiceSasUri(SasShapeFixture fixture, string permissions, string version)
    {
        var parsedVersion = DateOnly.ParseExact(version, "yyyy-MM-dd", CultureInfo.InvariantCulture);
        var fields = new List<string>
        {
            permissions,
            fixture.SignedStart,
            fixture.SignedExpiry,
            fixture.CanonicalResource,
            string.Empty,
            string.Empty,
            "https,http",
            version,
            "b",
            string.Empty
        };
        if (parsedVersion >= new DateOnly(2020, 12, 6))
            fields.Add(string.Empty);
        fields.AddRange([string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
        var signature = SignSasShape(fixture.AccountKey, string.Join('\n', fields));
        return new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{fixture.ContainerName}/{fixture.BlobName}" +
            $"?sp={permissions}&st={Uri.EscapeDataString(fixture.SignedStart)}" +
            $"&se={Uri.EscapeDataString(fixture.SignedExpiry)}&spr=https%2Chttp" +
            $"&sv={version}&sr=b&sig={Uri.EscapeDataString(signature)}");
    }

    private static Uri CreateMalformedAccountSasUri(
        SasShapeFixture fixture, string services, string resourceTypes, string permissions)
    {
        const string version = "2023-11-03";
        var stringToSign = string.Join(
                               '\n',
                               SavaWebApplicationFactory.AccountName,
                               permissions,
                               services,
                               resourceTypes,
                               fixture.SignedStart,
                               fixture.SignedExpiry,
                               string.Empty,
                               "https,http",
                               version,
                               string.Empty) + "\n";
        var signature = SignSasShape(fixture.AccountKey, stringToSign);
        return new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{fixture.ContainerName}/{fixture.BlobName}" +
            $"?sp={permissions}&ss={services}&srt={resourceTypes}" +
            $"&st={Uri.EscapeDataString(fixture.SignedStart)}&se={Uri.EscapeDataString(fixture.SignedExpiry)}" +
            $"&spr=https%2Chttp&sv={version}&sig={Uri.EscapeDataString(signature)}");
    }

    private static Uri CreateMalformedUserDelegationSasUri(
        SasShapeFixture fixture, string? permissions, string? expiry)
    {
        const string version = "2023-11-03";
        var key = fixture.DelegationKey;
        var stringToSign = string.Join(
            '\n',
            permissions ?? string.Empty,
            fixture.SignedStart,
            expiry ?? string.Empty,
            fixture.CanonicalResource,
            key.SignedObjectId,
            key.SignedTenantId,
            fixture.KeyStart,
            fixture.KeyExpiry,
            key.SignedService,
            key.SignedVersion,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            "https,http",
            version,
            "b",
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty,
            string.Empty);
        var signature = SignSasShape(Convert.FromBase64String(key.Value), stringToSign);
        var query =
            (permissions is null ? string.Empty : $"sp={permissions}&") +
            $"st={Uri.EscapeDataString(fixture.SignedStart)}&" +
            (expiry is null ? string.Empty : $"se={Uri.EscapeDataString(expiry)}&") +
            $"skoid={key.SignedObjectId}&sktid={key.SignedTenantId}" +
            $"&skt={Uri.EscapeDataString(fixture.KeyStart)}&ske={Uri.EscapeDataString(fixture.KeyExpiry)}" +
            $"&sks={key.SignedService}&skv={key.SignedVersion}" +
            $"&spr=https%2Chttp&sv={version}&sr=b&sig={Uri.EscapeDataString(signature)}";
        return new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{fixture.ContainerName}/{fixture.BlobName}?{query}");
    }

    [Fact]
    public async Task ContainerOwnerOperationsRequireAccountSasAndReportScopeMismatches()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"container-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("listed.txt").UploadAsync(BinaryData.FromString("listed"));

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        using var transport = new HttpClient(factory.Server.CreateHandler());

        await AssertServiceSasExistingContainerDeniedAsync(container, transport);
        await AssertServiceSasCreateRestoreDeniedAsync(service, credential, transport);
        await AssertServiceSasListAllowedAsync(container, transport);
        await AssertAccountSasContainerOperationsAllowedAsync(service, container, credential, transport);
        await AssertAccountSasContainerScopeMismatchesAsync(service, credential, transport);
        await AssertAccountSasContainerLeasePermissionsAsync(service, container, credential, transport);
    }

    private static Uri BuildServiceSasContainerUri(
        BlobServiceClient service, StorageSharedKeyCredential credential, string name)
    {
        var builder = new BlobSasBuilder
        {
            BlobContainerName = name,
            Resource = "c",
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(BlobContainerSasPermissions.All);
        return new Uri(
            $"{service.Uri.AbsoluteUri.TrimEnd('/')}/{name}" +
            $"?restype=container&{builder.ToSasQueryParameters(credential)}");
    }

    private static Uri BuildAccountSasContainerUri(
        BlobServiceClient service,
        StorageSharedKeyCredential credential,
        string name,
        AccountSasPermissions permissions,
        AccountSasServices services,
        AccountSasResourceTypes resourceTypes,
        string? component = null)
    {
        var builder = new AccountSasBuilder
        {
            Services = services,
            ResourceTypes = resourceTypes,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(permissions);
        var query = $"restype=container&{builder.ToSasQueryParameters(credential)}";
        if (component is not null)
            query = $"{query}&comp={component}";
        return new Uri($"{service.Uri.AbsoluteUri.TrimEnd('/')}/{name}?{query}");
    }

    private static async Task AssertServiceSasOwnerRequestRejectedAsync(
        HttpClient transport, HttpRequestMessage request)
    {
        using (request)
        {
            if (!request.Headers.Contains("x-ms-version"))
                request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private static async Task AssertServiceSasExistingContainerDeniedAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var serviceSasUri = container.GenerateSasUri(
            BlobContainerSasPermissions.All,
            DateTimeOffset.UtcNow.AddMinutes(10));
        await AssertServiceSasOwnerRequestRejectedAsync(transport, new HttpRequestMessage(
            HttpMethod.Head,
            AppendQuery(serviceSasUri, "restype=container"))).ConfigureAwait(false);
        await AssertServiceSasOwnerRequestRejectedAsync(transport, new HttpRequestMessage(
            HttpMethod.Get,
            AppendQuery(serviceSasUri, "restype=container&comp=metadata"))).ConfigureAwait(false);
        await AssertServiceSasOwnerRequestRejectedAsync(transport, new HttpRequestMessage(
            HttpMethod.Put,
            AppendQuery(serviceSasUri, "restype=container&comp=metadata"))
        {
            Content = new ByteArrayContent([])
        }).ConfigureAwait(false);
        await AssertServiceSasOwnerRequestRejectedAsync(transport, CreateLeaseRequest(
            AppendQuery(serviceSasUri, "restype=container&comp=lease"),
            "2023-11-03",
            "acquire",
            duration: 15)).ConfigureAwait(false);
        await AssertServiceSasOwnerRequestRejectedAsync(transport, new HttpRequestMessage(
            HttpMethod.Delete,
            AppendQuery(serviceSasUri, "restype=container"))).ConfigureAwait(false);
    }

    private static async Task AssertServiceSasCreateRestoreDeniedAsync(
        BlobServiceClient service, StorageSharedKeyCredential credential, HttpClient transport)
    {
        var deniedTargetName = $"service-sas-create-{Guid.NewGuid():N}";
        var deniedTargetUri = BuildServiceSasContainerUri(service, credential, deniedTargetName);
        await AssertServiceSasOwnerRequestRejectedAsync(transport, new HttpRequestMessage(HttpMethod.Put, deniedTargetUri)
        {
            Content = new ByteArrayContent([])
        }).ConfigureAwait(false);
        using (var restore = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(deniedTargetUri, "comp=undelete"))
        {
            Content = new ByteArrayContent([])
        })
        {
            restore.Headers.TryAddWithoutValidation("x-ms-deleted-container-name", "deleted-source");
            restore.Headers.TryAddWithoutValidation("x-ms-deleted-container-version", "01D8B4E2CFD1A4B00000000000000000");
            await AssertServiceSasOwnerRequestRejectedAsync(transport, restore).ConfigureAwait(false);
        }
    }

    private static async Task AssertServiceSasListAllowedAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var serviceSasUri = container.GenerateSasUri(
            BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(10));
        using var list = new HttpRequestMessage(
            HttpMethod.Get,
            AppendQuery(serviceSasUri, "restype=container&comp=list"));
        list.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var response = await transport.SendAsync(list).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("<Name>listed.txt</Name>",
            await response.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
    }

    private static async Task AssertAccountSasContainerOperationsAllowedAsync(
        BlobServiceClient service,
        BlobContainerClient container,
        StorageSharedKeyCredential credential,
        HttpClient transport)
    {
        var createdName = $"account-sas-create-{Guid.NewGuid():N}";
        var createUri = BuildAccountSasContainerUri(service, credential, createdName,
            AccountSasPermissions.Create, AccountSasServices.Blobs, AccountSasResourceTypes.Container);
        using (var create = new HttpRequestMessage(HttpMethod.Put, createUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            create.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(create).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var readUri = BuildAccountSasContainerUri(service, credential, container.Name,
            AccountSasPermissions.Read, AccountSasServices.Blobs, AccountSasResourceTypes.Container);
        using (var properties = new HttpRequestMessage(HttpMethod.Head, readUri))
        {
            properties.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(properties).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        var metadataUri = BuildAccountSasContainerUri(service, credential, container.Name,
            AccountSasPermissions.Write, AccountSasServices.Blobs, AccountSasResourceTypes.Container, "metadata");
        using (var setMetadata = new HttpRequestMessage(HttpMethod.Put, metadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            setMetadata.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            setMetadata.Headers.TryAddWithoutValidation("x-ms-meta-authorized", "account-sas");
            using var response = await transport.SendAsync(setMetadata).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        Assert.Equal("account-sas",
            (await container.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["authorized"]);
    }

    private static async Task AssertAccountSasContainerScopeMismatchesAsync(
        BlobServiceClient service, StorageSharedKeyCredential credential, HttpClient transport)
    {
        var wrongResourceUri = BuildAccountSasContainerUri(service, credential,
            $"wrong-resource-{Guid.NewGuid():N}", AccountSasPermissions.Create,
            AccountSasServices.Blobs, AccountSasResourceTypes.Service);
        using (var wrongResource = new HttpRequestMessage(HttpMethod.Put, wrongResourceUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            wrongResource.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(wrongResource).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationResourceTypeMismatch",
                response.Headers.GetValues("x-ms-error-code").Single());
        }

        var wrongServiceUri = BuildAccountSasContainerUri(service, credential,
            $"wrong-service-{Guid.NewGuid():N}", AccountSasPermissions.Create,
            AccountSasServices.Queues, AccountSasResourceTypes.Container);
        using (var wrongService = new HttpRequestMessage(HttpMethod.Put, wrongServiceUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            wrongService.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(wrongService).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationServiceMismatch",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private static async Task AssertAccountSasContainerLeasePermissionsAsync(
        BlobServiceClient service,
        BlobContainerClient container,
        StorageSharedKeyCredential credential,
        HttpClient transport)
    {
        var leaseClient = container.GetBlobLeaseClient();
        await leaseClient.AcquireAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        var deleteLeaseUri = BuildAccountSasContainerUri(service, credential,
            container.Name, AccountSasPermissions.Delete, AccountSasServices.Blobs,
            AccountSasResourceTypes.Container, "lease");
        using (var deniedAcquire = CreateLeaseRequest(
                   deleteLeaseUri,
                   "2017-07-29",
                   "acquire",
                   duration: 15))
        using (var response = await transport.SendAsync(deniedAcquire).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationPermissionMismatch",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var breakLease = CreateLeaseRequest(deleteLeaseUri, "2017-07-29", "break"))
        using (var response = await transport.SendAsync(breakLease).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task StoredAccessPolicyCanAuthorizeAndImmediatelyRevokeServiceSas()
    {
        var service = CreateClient(factory);
        var containerName = $"policy-{Guid.NewGuid():N}";
        var blobName = "policy.txt";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString("policy payload"));
        var policyStart = DateTimeOffset.UtcNow.AddMinutes(-1);
        var policyExpiry = DateTimeOffset.UtcNow.AddMinutes(10);
        await SetStoredAccessPoliciesAsync(container, policyStart, policyExpiry);
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var policyBlob = await AssertStoredPolicyValidTokensAsync(
            factory, containerName, blobName, credential, policyStart, policyExpiry);
        await AssertStoredPolicyInvalidTokensAsync(
            factory, containerName, blobName, credential, policyStart, policyExpiry);
        await AssertStoredPolicyRevocationAndAclAsync(factory, container, policyBlob);
    }

    private static async Task SetStoredAccessPoliciesAsync(
        BlobContainerClient container, DateTimeOffset policyStart, DateTimeOffset policyExpiry)
    {
        await container.SetAccessPolicyAsync(
            PublicAccessType.None,
            [
                new BlobSignedIdentifier
                {
                    Id = "read-policy",
                    AccessPolicy = new BlobAccessPolicy
                    {
                        StartsOn = policyStart,
                        ExpiresOn = policyExpiry,
                        Permissions = "r"
                    }
                },
                new BlobSignedIdentifier
                {
                    Id = "time-policy",
                    AccessPolicy = new BlobAccessPolicy
                    {
                        StartsOn = policyStart,
                        ExpiresOn = policyExpiry
                    }
                },
                new BlobSignedIdentifier
                {
                    Id = "permission-policy",
                    AccessPolicy = new BlobAccessPolicy { Permissions = "r" }
                },
                new BlobSignedIdentifier
                {
                    Id = "duplicate-policy",
                    AccessPolicy = new BlobAccessPolicy
                    {
                        StartsOn = policyStart,
                        ExpiresOn = policyExpiry,
                        Permissions = "r"
                    }
                }
            ]).ConfigureAwait(false);
    }

    private static async Task<BlobClient> AssertStoredPolicyValidTokensAsync(
        SavaWebApplicationFactory application,
        string containerName,
        string blobName,
        StorageSharedKeyCredential credential,
        DateTimeOffset policyStart,
        DateTimeOffset policyExpiry)
    {
        var builder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            Identifier = "read-policy",
            Protocol = SasProtocol.HttpsAndHttp
        };
        var sas = builder.ToSasQueryParameters(credential);
        var policyBlob = CreateBlobClient(application,
            new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{sas}"));
        Assert.Equal("policy payload",
            (await policyBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var tokenPermissionBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            Identifier = "time-policy",
            Protocol = SasProtocol.HttpsAndHttp
        };
        tokenPermissionBuilder.SetPermissions(BlobSasPermissions.Read);
        var tokenPermissionBlob = CreateBlobClient(application,
            new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                    $"?{tokenPermissionBuilder.ToSasQueryParameters(credential)}"));
        Assert.Equal("policy payload",
            (await tokenPermissionBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var tokenTimeBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            Identifier = "permission-policy",
            StartsOn = policyStart,
            ExpiresOn = policyExpiry,
            Protocol = SasProtocol.HttpsAndHttp
        };
        var tokenTimeBlob = CreateBlobClient(application,
            new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                    $"?{tokenTimeBuilder.ToSasQueryParameters(credential)}"));
        Assert.Equal("policy payload",
            (await tokenTimeBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        return policyBlob;
    }

    private static async Task AssertStoredPolicyInvalidTokensAsync(
        SavaWebApplicationFactory application,
        string containerName,
        string blobName,
        StorageSharedKeyCredential credential,
        DateTimeOffset policyStart,
        DateTimeOffset policyExpiry)
    {
        var duplicateBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            Identifier = "duplicate-policy",
            StartsOn = policyStart,
            ExpiresOn = policyExpiry,
            Protocol = SasProtocol.HttpsAndHttp
        };
        duplicateBuilder.SetPermissions(BlobSasPermissions.Read);
        var duplicateBlob = CreateBlobClient(application,
            new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                    $"?{duplicateBuilder.ToSasQueryParameters(credential)}"));
        var duplicate = await Assert.ThrowsAsync<RequestFailedException>(() => duplicateBlob.DownloadContentAsync())
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status400BadRequest, duplicate.Status);
        Assert.Equal("InvalidQueryParameterValue", duplicate.ErrorCode);

        var accountBuilder = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Object,
            StartsOn = policyStart,
            ExpiresOn = policyExpiry,
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountBuilder.SetPermissions(AccountSasPermissions.Read);
        var accountSasWithIdentifier = CreateBlobClient(application,
            AppendQuery(
                new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                        $"?{accountBuilder.ToSasQueryParameters(credential)}"),
                "si=read-policy"));
        var unsupportedIdentifier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            accountSasWithIdentifier.DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, unsupportedIdentifier.Status);
        Assert.Equal("AuthenticationFailed", unsupportedIdentifier.ErrorCode);
    }

    private static async Task AssertStoredPolicyRevocationAndAclAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, BlobClient policyBlob)
    {
        await container.SetAccessPolicyAsync(PublicAccessType.None, []).ConfigureAwait(false);
        var revoked = await Assert.ThrowsAsync<RequestFailedException>(() => policyBlob.DownloadContentAsync())
            .ConfigureAwait(false);
        Assert.Equal(403, revoked.Status);

        var aclUri = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=acl");
        using var transport = new HttpClient(application.Server.CreateHandler());
        using (var getAcl = new HttpRequestMessage(HttpMethod.Get, aclUri))
        {
            getAcl.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(getAcl).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var setAcl = new HttpRequestMessage(HttpMethod.Put, aclUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            setAcl.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(setAcl).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    [Fact]
    public async Task BearerTokensRequireTrustedSignatureIssuerAudiencePrincipalAndPermission()
    {
        var owner = CreateClient(factory);
        var containerName = $"bearer-{Guid.NewGuid():N}";
        var container = owner.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        await container.GetBlobClient("readable.txt").UploadAsync(BinaryData.FromString("bearer payload"));

        var token = CreateJwt(SavaWebApplicationFactory.AccountKey, "reader-1");
        var reader = CreateBearerClient(factory, token);
        Assert.Equal("bearer payload", (await reader.GetBlobContainerClient(containerName).GetBlobClient("readable.txt").DownloadContentAsync()).Value.Content.ToString());
        var names = new List<string>();
        await foreach (var item in reader.GetBlobContainerClient(containerName).GetBlobsAsync())
            names.Add(item.Name);
        Assert.Contains("readable.txt", names, StringComparer.Ordinal);
        var deniedWrite = await Assert.ThrowsAsync<RequestFailedException>(() =>
            reader.GetBlobContainerClient(containerName).GetBlobClient("denied.txt").UploadAsync(BinaryData.FromString("no")));
        Assert.Equal(403, deniedWrite.Status);

        var invalidToken = CreateJwt(SavaWebApplicationFactory.SecondAccountKey, "reader-1");
        var invalid = CreateBearerClient(factory, invalidToken);
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            invalid.GetBlobContainerClient(containerName).GetBlobClient("readable.txt").DownloadContentAsync());
        Assert.Equal(401, rejected.Status);
    }

    [Fact]
    public async Task InternalCopyReauthorizesBearerAgainstSourceContainer()
    {
        var allowedContainerName = $"copy-bearer-allowed-{Guid.NewGuid():N}";
        var privateContainerName = $"copy-bearer-private-{Guid.NewGuid():N}";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:BearerAuthentication:Principals:copy-agent:Permissions"] = "rcw",
            ["Sava:BearerAuthentication:Principals:copy-agent:Accounts:0"] =
                SavaWebApplicationFactory.AccountName,
            ["Sava:BearerAuthentication:Principals:copy-agent:Containers:0"] = allowedContainerName
        });
        await using var applicationDisposal32 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var owner = CreateClient(application);
        var allowed = owner.GetBlobContainerClient(allowedContainerName);
        var privateContainer = owner.GetBlobContainerClient(privateContainerName);
        await allowed.CreateAsync();
        await privateContainer.CreateAsync();
        var publicSource = allowed.GetBlobClient("source.txt");
        var privateSource = privateContainer.GetBlobClient("secret.txt");
        await publicSource.UploadAsync(BinaryData.FromString("allowed bytes"));
        await privateSource.UploadAsync(BinaryData.FromString("private bytes"));

        var token = CreateJwt(SavaWebApplicationFactory.AccountKey, "copy-agent");
        var bearerDestination = CreateBearerClient(application, token)
            .GetBlobContainerClient(allowedContainerName);
        var rejected = bearerDestination.GetBlobClient("rejected.txt");
        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            rejected.SyncCopyFromUriAsync(privateSource.Uri));
        Assert.Equal(403, denied.Status);
        Assert.Equal("AuthorizationFailure", denied.ErrorCode);
        Assert.False((await rejected.ExistsAsync()).Value);

        var copied = bearerDestination.GetBlobClient("copied.txt");
        await copied.SyncCopyFromUriAsync(publicSource.Uri);
        Assert.Equal("allowed bytes", (await copied.DownloadContentAsync()).Value.Content.ToString());

        var delegated = bearerDestination.GetBlobClient("delegated.txt");
        await delegated.SyncCopyFromUriAsync(privateSource.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(5)));
        Assert.Equal("private bytes", (await delegated.DownloadContentAsync()).Value.Content.ToString());
    }

    [Fact]
    public async Task BearerDelegationKeysProduceScopedUserDelegationSasTokens()
    {
        var owner = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var original = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var containerName = $"delegation-{Guid.NewGuid():N}";
        var blobName = "delegated.txt";
        var container = owner.GetBlobContainerClient(containerName);
        await container.CreateAsync();

        try
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original with { VersioningEnabled = true },
                CancellationToken.None);
            var versionId = (await container.GetBlobClient(blobName).UploadAsync(
                BinaryData.FromString("delegated payload"))).Value.VersionId;
            Assert.False(string.IsNullOrEmpty(versionId));

            var token = CreateJwt(
                SavaWebApplicationFactory.AccountKey,
                SavaWebApplicationFactory.DelegatorObjectId,
                SavaWebApplicationFactory.TenantId);
            var delegator = CreateBearerClient(factory, token);
            var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1);
            var expiresOn = DateTimeOffset.UtcNow.AddHours(1);
            var key = await delegator.GetUserDelegationKeyAsync(
                new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn });
            Assert.Equal(SavaWebApplicationFactory.DelegatorObjectId, key.Value.SignedObjectId);
            Assert.Equal(SavaWebApplicationFactory.TenantId, key.Value.SignedTenantId);

            await AssertBlobScopedUserDelegationSasAsync(
                containerName, blobName, key.Value, startsOn, expiresOn);
            await AssertVersionScopedUserDelegationSasAsync(
                containerName, blobName, versionId!, key.Value, startsOn, expiresOn);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                original,
                CancellationToken.None);
        }
    }

    private async Task AssertBlobScopedUserDelegationSasAsync(
        string containerName,
        string blobName,
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn)
    {
        var builder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        builder.SetPermissions(BlobSasPermissions.Read | BlobSasPermissions.Delete);
        var sas = builder.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName);
        var delegatedBlob = CreateBlobClient(
            factory,
            new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{sas}"));

        Assert.Equal("delegated payload",
            (await delegatedBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        var deniedDelete = await Assert.ThrowsAsync<RequestFailedException>(() => delegatedBlob.DeleteAsync())
            .ConfigureAwait(false);
        Assert.Equal(403, deniedDelete.Status);
    }

    private async Task AssertVersionScopedUserDelegationSasAsync(
        string containerName,
        string blobName,
        string versionId,
        Azure.Storage.Blobs.Models.UserDelegationKey key,
        DateTimeOffset startsOn,
        DateTimeOffset expiresOn)
    {
        var versionBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            BlobVersionId = versionId,
            Resource = "bv",
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp
        };
        versionBuilder.SetPermissions(BlobSasPermissions.Read);
        var versionSas = versionBuilder.ToSasQueryParameters(key, SavaWebApplicationFactory.AccountName);
        var delegatedVersion = CreateBlobClient(
            factory,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                $"?versionid={Uri.EscapeDataString(versionId)}&{versionSas}"));
        Assert.Equal("delegated payload", (await delegatedVersion.DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());

        var versionTokenOnBaseBlob = CreateBlobClient(
            factory,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                $"?{versionSas}"));
        var wrongResource = await Assert.ThrowsAsync<RequestFailedException>(() =>
            versionTokenOnBaseBlob.DownloadContentAsync()).ConfigureAwait(false);
        Assert.Equal(403, wrongResource.Status);
    }

    [Fact]
    public async Task UserDelegationKeyRequiresHttpsAndHonorsItsVersionedXmlContract()
    {
        var token = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var startsAt = DateTimeOffset.UtcNow.AddMinutes(-1).ToString("O", CultureInfo.InvariantCulture);
        var expiresAt = DateTimeOffset.UtcNow.AddHours(1).ToString("O", CultureInfo.InvariantCulture);
        var delegatedTenant = Guid.NewGuid().ToString();
        var baseBody = $"<KeyInfo><Start>{startsAt}</Start><Expiry>{expiresAt}</Expiry></KeyInfo>";
        var delegatedBody = $"<KeyInfo><Start>{startsAt}</Start><Expiry>{expiresAt}</Expiry><DelegatedUserTid>{delegatedTenant}</DelegatedUserTid></KeyInfo>";
        var httpsUri = new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/" +
            "?restype=service&comp=userdelegationkey");
        var httpUri = new UriBuilder(httpsUri) { Scheme = Uri.UriSchemeHttp, Port = -1 }.Uri;
        using var transport = new HttpClient(factory.Server.CreateHandler());
        await AssertUserDelegationKeyVersionAndXmlErrorsAsync(
            transport, token, httpsUri, httpUri, baseBody, delegatedBody, startsAt, expiresAt);
        await AssertUserDelegationKeyTenantPolicyAsync(
            transport, token, httpsUri, baseBody, delegatedBody, delegatedTenant);
    }

    private static async Task<HttpResponseMessage> SendUserDelegationKeyRequestAsync(
        HttpClient client, Uri uri, string version, string body, string token)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, uri)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/xml")
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        return await client.SendAsync(request).ConfigureAwait(false);
    }

    private static async Task AssertUserDelegationKeyVersionAndXmlErrorsAsync(
        HttpClient transport, string token, Uri httpsUri, Uri httpUri,
        string baseBody, string delegatedBody, string startsAt, string expiresAt)
    {
        using (var oldVersion = await SendUserDelegationKeyRequestAsync(
                   transport, httpsUri, "2018-03-28", baseBody, token).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldVersion.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldVersion.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var insecure = await SendUserDelegationKeyRequestAsync(
                   transport, httpUri, "2025-07-05", baseBody, token).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, insecure.StatusCode);
            Assert.Equal("InvalidRequest", insecure.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var prematureDelegation = await SendUserDelegationKeyRequestAsync(
                   transport, httpsUri, "2023-11-03", delegatedBody, token).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Conflict, prematureDelegation.StatusCode);
            Assert.Equal("FeatureVersionMismatch", prematureDelegation.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var unknownElement = await SendUserDelegationKeyRequestAsync(
                   transport, httpsUri, "2025-07-05",
                   $"<KeyInfo><Start>{startsAt}</Start><Expiry>{expiresAt}</Expiry><Unknown /></KeyInfo>",
                   token).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, unknownElement.StatusCode);
            Assert.Equal("InvalidXmlDocument", unknownElement.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var duplicateElement = await SendUserDelegationKeyRequestAsync(
                   transport, httpsUri, "2025-07-05",
                   $"<KeyInfo><Start>{startsAt}</Start><Start>{startsAt}</Start><Expiry>{expiresAt}</Expiry></KeyInfo>",
                   token).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, duplicateElement.StatusCode);
            Assert.Equal("InvalidXmlDocument", duplicateElement.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private static async Task AssertUserDelegationKeyTenantPolicyAsync(
        HttpClient transport, string token, Uri httpsUri,
        string baseBody, string delegatedBody, string delegatedTenant)
    {
        using (var accepted = await SendUserDelegationKeyRequestAsync(
                   transport, httpsUri, "2025-07-05", baseBody, token).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(
                await accepted.Content.ReadAsStringAsync().ConfigureAwait(false));
            Assert.Empty(document.Root!.Elements("SignedDelegatedUserTid"));
        }

        using (var deniedCrossTenant = await SendUserDelegationKeyRequestAsync(
                   transport, httpsUri, "2025-07-05", delegatedBody, token).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Forbidden, deniedCrossTenant.StatusCode);
            Assert.Equal("AuthorizationFailure",
                deniedCrossTenant.Headers.GetValues("x-ms-error-code").Single());
        }

        var crossTenantApplication = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:AllowCrossTenantDelegationSas"] =
                    "true"
            });
        await using var disposal = crossTenantApplication.ConfigureAwait(false);
        using var crossTenantTransport = new HttpClient(crossTenantApplication.Server.CreateHandler());
        using var crossTenantAccepted = await SendUserDelegationKeyRequestAsync(
            crossTenantTransport, httpsUri, "2025-07-05", delegatedBody, token).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, crossTenantAccepted.StatusCode);
        var crossTenantDocument = System.Xml.Linq.XDocument.Parse(
            await crossTenantAccepted.Content.ReadAsStringAsync().ConfigureAwait(false));
        Assert.Equal(delegatedTenant,
            Assert.Single(crossTenantDocument.Root!.Elements("SignedDelegatedUserTid")).Value);
    }

    [Fact]
    public async Task UrlTransfersSupportBlockAppendPageAndWholeBlobOperations()
    {
        var sourceBytes = Enumerable.Range(0, 16 * 1024).Select(index => (byte)(index % 251)).ToArray();
        var source = (await LoopbackSource.StartAsync(sourceBytes));
        await using var sourceDisposal34 = source.ConfigureAwait(false);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"url-{Guid.NewGuid():N}");
        await container.CreateAsync();

        await AssertWholeUrlUploadAsync(container, source.Uri, sourceBytes);
        await AssertBlockUrlStageAsync(container, source.Uri, sourceBytes);
        await AssertAppendUrlWriteAsync(container, source.Uri, sourceBytes);
        await AssertPageUrlWriteAsync(container, source.Uri, sourceBytes);
    }

    private static async Task AssertWholeUrlUploadAsync(BlobContainerClient container, Uri sourceUri, byte[] sourceBytes)
    {
        var whole = container.GetBlockBlobClient("whole.bin");
        var wholeUpload = await whole.SyncUploadFromUriAsync(
            sourceUri,
            new BlobSyncUploadFromUriOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["marker"] = "must-not-leak" }
            }).ConfigureAwait(false);
        var wholeUploadHeaders = wholeUpload.GetRawResponse().Headers;
        Assert.True(wholeUploadHeaders.TryGetValue("ETag", out _));
        Assert.True(wholeUploadHeaders.TryGetValue("Last-Modified", out _));
        Assert.True(wholeUploadHeaders.TryGetValue("x-ms-request-server-encrypted", out var wholeUploadEncrypted));
        Assert.Equal("true", wholeUploadEncrypted);
        Assert.False(wholeUploadHeaders.TryGetValue("x-ms-copy-status", out _));
        Assert.False(wholeUploadHeaders.TryGetValue("x-ms-blob-type", out _));
        Assert.False(wholeUploadHeaders.TryGetValue("x-ms-lease-status", out _));
        Assert.False(wholeUploadHeaders.TryGetValue("x-ms-meta-marker", out _));
        Assert.Equal(
            Convert.ToBase64String(AzureProtocolChecksum.Md5(sourceBytes)),
            ResponseHeader(wholeUpload.GetRawResponse(), "Content-MD5"));
        Assert.Equal(
            StorageCrc64Base64(sourceBytes),
            ResponseHeader(wholeUpload.GetRawResponse(), "x-ms-content-crc64"));
        Assert.Equal(sourceBytes, (await whole.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertBlockUrlStageAsync(BlobContainerClient container, Uri sourceUri, byte[] sourceBytes)
    {
        const int blockOffset = 900;
        const int blockLength = 2_300;
        var block = container.GetBlockBlobClient("block.bin");
        var blockId = Convert.ToBase64String("url-block-1"u8);
        var blockSlice = sourceBytes.AsSpan(blockOffset, blockLength).ToArray();
        var stagedFromUri = await block.StageBlockFromUriAsync(sourceUri, blockId, new StageBlockFromUriOptions
        {
            SourceRange = new HttpRange(blockOffset, blockLength),
            SourceContentHash = AzureProtocolChecksum.Md5(blockSlice)
        }).ConfigureAwait(false);
        Assert.Equal(
            Convert.ToBase64String(AzureProtocolChecksum.Md5(blockSlice)),
            ResponseHeader(stagedFromUri.GetRawResponse(), "Content-MD5"));
        Assert.Equal(
            StorageCrc64Base64(blockSlice),
            ResponseHeader(stagedFromUri.GetRawResponse(), "x-ms-content-crc64"));
        await block.CommitBlockListAsync([blockId]).ConfigureAwait(false);
        Assert.Equal(blockSlice, (await block.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertAppendUrlWriteAsync(BlobContainerClient container, Uri sourceUri, byte[] sourceBytes)
    {
        const int appendOffset = 4_000;
        const int appendLength = 1_500;
        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync().ConfigureAwait(false);
        var appendSlice = sourceBytes.AsSpan(appendOffset, appendLength).ToArray();
        var appendedFromUri = await append.AppendBlockFromUriAsync(sourceUri, new AppendBlobAppendBlockFromUriOptions
        {
            SourceRange = new HttpRange(appendOffset, appendLength),
            SourceContentHash = AzureProtocolChecksum.Md5(appendSlice)
        }).ConfigureAwait(false);
        Assert.Equal(
            Convert.ToBase64String(AzureProtocolChecksum.Md5(appendSlice)),
            ResponseHeader(appendedFromUri.GetRawResponse(), "Content-MD5"));
        Assert.Equal(
            StorageCrc64Base64(appendSlice),
            ResponseHeader(appendedFromUri.GetRawResponse(), "x-ms-content-crc64"));
        Assert.Equal(appendSlice, (await append.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertPageUrlWriteAsync(BlobContainerClient container, Uri sourceUri, byte[] sourceBytes)
    {
        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(1024).ConfigureAwait(false);
        var pageSlice = sourceBytes.AsSpan(0, 512).ToArray();
        var pagesFromUri = await page.UploadPagesFromUriAsync(
            sourceUri,
            new HttpRange(0, 512),
            new HttpRange(512, 512),
            new PageBlobUploadPagesFromUriOptions { SourceContentHash = AzureProtocolChecksum.Md5(pageSlice) })
            .ConfigureAwait(false);
        Assert.Equal(
            Convert.ToBase64String(AzureProtocolChecksum.Md5(pageSlice)),
            ResponseHeader(pagesFromUri.GetRawResponse(), "Content-MD5"));
        Assert.Equal(
            StorageCrc64Base64(pageSlice),
            ResponseHeader(pagesFromUri.GetRawResponse(), "x-ms-content-crc64"));
        var expectedPage = new byte[1024];
        pageSlice.CopyTo(expectedPage, 512);
        Assert.Equal(expectedPage, (await page.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    [Fact]
    public async Task UrlTransferRejectsRedirectWithoutContactingRedirectTarget()
    {
        var source = (await LoopbackSource.StartAsync("redirect target content"u8.ToArray()));
        await using var sourceDisposal35 = source.ConfigureAwait(false);
        var application = new SavaWebApplicationFactory();
        await using var applicationDisposal36 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"url-redirect-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var destination = container.GetBlockBlobClient("must-not-exist.bin");

        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.SyncUploadFromUriAsync(source.RedirectUri));

        Assert.Equal(500, rejected.Status);
        Assert.Equal("CannotVerifyCopySource", rejected.ErrorCode);
        Assert.Equal(0, source.RedirectTargetRequests);
        Assert.False((await destination.ExistsAsync()).Value);
    }

    [Fact]
    public async Task UrlTransferRequiresExplicitTrustForPrivateResolvedSource()
    {
        var source = (await LoopbackSource.StartAsync("trusted loopback"u8.ToArray()));
        await using var sourceDisposal37 = source.ConfigureAwait(false);
        var application = new SavaWebApplicationFactory();
        await using var applicationDisposal38 = application.ConfigureAwait(false);
        await application.InitializeAsync();
        var container = CreateClient(application)
            .GetBlobContainerClient($"url-egress-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var destination = container.GetBlockBlobClient("copied.bin");
        var untrustedAlias = new UriBuilder(source.Uri) { Host = "localhost" }.Uri;

        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.SyncUploadFromUriAsync(untrustedAlias));
        Assert.Equal(500, denied.Status);
        Assert.Equal("CannotVerifyCopySource", denied.ErrorCode);
        Assert.Equal(0, source.SourceRequestCount);
        Assert.False((await destination.ExistsAsync()).Value);

        await destination.SyncUploadFromUriAsync(source.Uri);
        Assert.Equal(1, source.SourceRequestCount);
        Assert.Equal("trusted loopback", (await destination.DownloadContentAsync()).Value.Content.ToString());
    }

    [Fact]
    public async Task FileRequestIntentIsValidatedAndForwardedForEverySupportedUrlOperation()
    {
        var sourceBytes = Enumerable.Range(0, 512).Select(index => (byte)(index % 251)).ToArray();
        using var source = new FileIntentSourceHandler(sourceBytes);
        var application = new SavaWebApplicationFactory(() => source);
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"file-intent-{Guid.NewGuid():N}");
            await container.CreateAsync();
            using var transport = new HttpClient(application.Server.CreateHandler());
            var firstBlobs = await AssertFileIntentWholeAndBlockAsync(container, transport);
            var remainingBlobs = await AssertFileIntentAppendPageAndCopyAsync(container, transport);

            Assert.Equal(5, source.RequestCount);
            foreach (var blob in firstBlobs.Concat(remainingBlobs))
                Assert.Equal(sourceBytes, (await blob.DownloadContentAsync()).Value.Content.ToArray());
            await AssertFileIntentHeaderErrorsAsync(container, transport);
            Assert.Equal(5, source.RequestCount);
            await AssertFileIntentAlternateSourcesAsync(container, transport, source, sourceBytes);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static HttpRequestMessage CreateFileIntentRequest(
        Uri destination,
        string? intent = "backup",
        string version = "2025-07-05",
        string sourceValue = "https://source.file.core.windows.net/share/source.bin",
        bool includeSourceAuthorization = true)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, destination)
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", sourceValue);
        if (includeSourceAuthorization)
        {
            request.Headers.TryAddWithoutValidation(
                "x-ms-copy-source-authorization",
                "Bearer azure-files-source-token");
        }
        if (intent is not null)
            request.Headers.TryAddWithoutValidation("x-ms-file-request-intent", intent);
        return request;
    }

    private static async Task<BlobBaseClient[]> AssertFileIntentWholeAndBlockAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var whole = container.GetBlockBlobClient("whole.bin");
        using (var request = CreateFileIntentRequest(whole.GenerateSasUri(
                   BlobSasPermissions.Create | BlobSasPermissions.Write,
                   DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var block = container.GetBlockBlobClient("block.bin");
        var blockId = Convert.ToBase64String("file-intent-block"u8);
        var blockUri = AppendQuery(
            block.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            $"comp=block&blockid={Uri.EscapeDataString(blockId)}");
        using (var request = CreateFileIntentRequest(blockUri))
        {
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        await block.CommitBlockListAsync([blockId]).ConfigureAwait(false);
        return [whole, block];
    }

    private static async Task<BlobBaseClient[]> AssertFileIntentAppendPageAndCopyAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync().ConfigureAwait(false);
        var appendUri = AppendQuery(
            append.GenerateSasUri(
                BlobSasPermissions.Add | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=appendblock");
        using (var request = CreateFileIntentRequest(appendUri))
        {
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(512).ConfigureAwait(false);
        var pageUri = AppendQuery(
            page.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=page");
        using (var request = CreateFileIntentRequest(pageUri))
        {
            request.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
            request.Headers.TryAddWithoutValidation("x-ms-source-range", "bytes=0-511");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var copied = container.GetBlockBlobClient("copied.bin");
        using (var request = CreateFileIntentRequest(copied.GenerateSasUri(
                   BlobSasPermissions.Create | BlobSasPermissions.Write,
                   DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        }
        return [append, page, copied];
    }

    private static async Task AssertFileIntentHeaderErrorsAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var rejected = container.GetBlockBlobClient("rejected.bin");
        var rejectedUri = rejected.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using (var request = CreateFileIntentRequest(rejectedUri, intent: null))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
            Assert.Contains("<HeaderName>x-ms-file-request-intent</HeaderName>",
                await response.Content.ReadAsStringAsync().ConfigureAwait(false), StringComparison.Ordinal);
        }
        using (var request = CreateFileIntentRequest(rejectedUri, intent: "restore"))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var request = CreateFileIntentRequest(rejectedUri, version: "2025-01-05"))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await rejected.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertFileIntentAlternateSourcesAsync(
        BlobContainerClient container, HttpClient transport, FileIntentSourceHandler source, byte[] sourceBytes)
    {
        var asynchronous = container.GetBlockBlobClient("asynchronous.bin");
        using (var request = CreateFileIntentRequest(asynchronous.GenerateSasUri(
                   BlobSasPermissions.Create | BlobSasPermissions.Write,
                   DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("UnsupportedHeader", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var anonymousFile = container.GetBlockBlobClient("anonymous-file.bin");
        using (var request = CreateFileIntentRequest(
                   anonymousFile.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)),
                   intent: null,
                   sourceValue: "https://source.file.core.windows.net/share/anonymous.bin",
                   includeSourceAuthorization: false))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var bearerBlob = container.GetBlockBlobClient("bearer-blob.bin");
        using (var request = CreateFileIntentRequest(
                   bearerBlob.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)),
                   intent: null,
                   sourceValue: "https://source.blob.core.windows.net/container/source.bin"))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        Assert.Equal(7, source.RequestCount);
        Assert.False((await asynchronous.ExistsAsync().ConfigureAwait(false)).Value);
        Assert.Equal(sourceBytes,
            (await anonymousFile.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        Assert.Equal(sourceBytes,
            (await bearerBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    [Fact]
    public async Task UrlSourceConditionsAndFailuresKeepAzureErrorSemantics()
    {
        var sourceBytes = Enumerable.Range(0, 2048).Select(index => (byte)(index % 241)).ToArray();
        var source = (await LoopbackSource.StartAsync(sourceBytes));
        await using var sourceDisposal39 = source.ConfigureAwait(false);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"url-errors-{Guid.NewGuid():N}");
        await container.CreateAsync();

        await AssertUrlSourcePreconditionsAsync(container, source.Uri);
        await AssertMissingUrlSourceErrorAsync(container, source.MissingUri);
    }

    private static async Task AssertUrlSourcePreconditionsAsync(BlobContainerClient container, Uri sourceUri)
    {
        var missingEtag = new ETag("\"not-the-source-etag\"");

        static async Task AssertSourceConditionAsync(Func<Task> operation)
        {
            var failure = await Assert.ThrowsAsync<RequestFailedException>(operation).ConfigureAwait(false);
            Assert.Equal(StatusCodes.Status412PreconditionFailed, failure.Status);
            Assert.Equal("SourceConditionNotMet", failure.ErrorCode);
        }

        var whole = container.GetBlockBlobClient("whole.bin");
        await AssertSourceConditionAsync(() => whole.SyncUploadFromUriAsync(
            sourceUri,
            new BlobSyncUploadFromUriOptions
            {
                SourceConditions = new BlobRequestConditions { IfMatch = missingEtag }
            })).ConfigureAwait(false);
        Assert.False((await whole.ExistsAsync().ConfigureAwait(false)).Value);

        var block = container.GetBlockBlobClient("block.bin");
        await AssertSourceConditionAsync(() => block.StageBlockFromUriAsync(
            sourceUri,
            Convert.ToBase64String("conditioned-block"u8),
            new StageBlockFromUriOptions
            {
                SourceConditions = new RequestConditions { IfMatch = missingEtag }
            })).ConfigureAwait(false);
        Assert.False((await block.ExistsAsync().ConfigureAwait(false)).Value);

        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync().ConfigureAwait(false);
        await AssertSourceConditionAsync(() => append.AppendBlockFromUriAsync(
            sourceUri,
            new AppendBlobAppendBlockFromUriOptions
            {
                SourceConditions = new AppendBlobRequestConditions { IfMatch = missingEtag }
            })).ConfigureAwait(false);
        Assert.Equal(0, (await append.GetPropertiesAsync().ConfigureAwait(false)).Value.ContentLength);

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(512).ConfigureAwait(false);
        await AssertSourceConditionAsync(() => page.UploadPagesFromUriAsync(
            sourceUri,
            new HttpRange(0, 512),
            new HttpRange(0, 512),
            new PageBlobUploadPagesFromUriOptions
            {
                SourceConditions = new PageBlobRequestConditions { IfMatch = missingEtag }
            })).ConfigureAwait(false);
        Assert.Equal(new byte[512], (await page.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());

        var copied = container.GetBlobClient("copied.bin");
        await AssertSourceConditionAsync(() => copied.StartCopyFromUriAsync(
            sourceUri,
            new BlobCopyFromUriOptions
            {
                SourceConditions = new BlobRequestConditions { IfMatch = missingEtag }
            })).ConfigureAwait(false);
        Assert.False((await copied.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private async Task AssertMissingUrlSourceErrorAsync(BlobContainerClient container, Uri missingSourceUri)
    {
        var missing = container.GetBlobClient("missing-source.bin");
        var missingUri = missing.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using var request = new HttpRequestMessage(HttpMethod.Put, missingUri)
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", missingSourceUri.ToString());
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Equal("CannotVerifyCopySource", response.Headers.GetValues("x-ms-error-code").Single());
        Assert.Equal("404", response.Headers.GetValues("x-ms-copy-source-status-code").Single());
        Assert.Equal("BlobNotFound", response.Headers.GetValues("x-ms-copy-source-error-code").Single());
        var error = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        Assert.Contains("<CopySourceStatusCode>404</CopySourceStatusCode>", error, StringComparison.Ordinal);
        Assert.Contains("<CopySourceErrorCode>BlobNotFound</CopySourceErrorCode>", error, StringComparison.Ordinal);
        Assert.Contains("<CopySourceErrorMessage>The specified blob does not exist.</CopySourceErrorMessage>", error, StringComparison.Ordinal);
        Assert.False((await missing.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task SourceCustomerKeysAreValidatedAndForwardedForEveryUrlWriteOperation()
    {
        var sourceBytes = Enumerable.Range(0, 2048).Select(index => (byte)(index % 239)).ToArray();
        var sourceKey = RandomNumberGenerator.GetBytes(32);
        var sourceHash = SHA256.HashData(sourceKey);
        using var source = new EncryptedSourceHandler(sourceBytes, sourceKey, sourceHash);
        var application = new SavaWebApplicationFactory(() => source);
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var containerName = $"source-cpk-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            using var transport = new HttpClient(application.Server.CreateHandler());

            var destinationKey = RandomNumberGenerator.GetBytes(32);
            var destinationHash = SHA256.HashData(destinationKey);
            await AssertSourceCustomerKeyWholeAsync(
                application, container, transport, sourceBytes, sourceKey, sourceHash, destinationKey, destinationHash);
            await AssertSourceCustomerKeyBlockAsync(container, transport, sourceBytes, sourceKey, sourceHash);
            await AssertSourceCustomerKeyAppendAndPageAsync(container, transport, sourceBytes, sourceKey, sourceHash);
            Assert.Equal(4, source.RequestCount);
            await AssertSourceCustomerKeyInvalidRequestsAsync(container, transport, sourceKey, sourceHash);
            Assert.Equal(4, source.RequestCount);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task AssertSourceCustomerKeyWholeAsync(
        SavaWebApplicationFactory application,
        BlobContainerClient container,
        HttpClient transport,
        byte[] sourceBytes,
        byte[] sourceKey,
        byte[] sourceHash,
        byte[] destinationKey,
        byte[] destinationHash)
    {
        var whole = container.GetBlobClient("whole.bin");
        using (var request = CreateSourceKeyRequest(
                   HttpsSasUri(whole, BlobSasPermissions.Create | BlobSasPermissions.Write),
                   sourceKey,
                   sourceHash))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            AddCustomerKeyHeaders(request, destinationKey, destinationHash);
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        var encryptedWhole = CreateEncryptedClient(
                application,
                new CustomerProvidedKey(destinationKey),
                encryptionScope: null)
            .GetBlobContainerClient(container.Name)
            .GetBlobClient(whole.Name);
        Assert.Equal(sourceBytes,
            (await encryptedWhole.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertSourceCustomerKeyBlockAsync(
        BlobContainerClient container, HttpClient transport,
        byte[] sourceBytes, byte[] sourceKey, byte[] sourceHash)
    {
        const int blockStart = 128;
        const int blockEnd = 1023;
        var block = container.GetBlockBlobClient("block.bin");
        var blockId = Convert.ToBase64String("source-cpk-block"u8);
        var blockUri = AppendQuery(
            HttpsSasUri(block, BlobSasPermissions.Create | BlobSasPermissions.Write),
            $"comp=block&blockid={Uri.EscapeDataString(blockId)}");
        using (var request = CreateSourceKeyRequest(blockUri, sourceKey, sourceHash))
        {
            request.Headers.TryAddWithoutValidation("x-ms-source-range", $"bytes={blockStart}-{blockEnd}");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        await block.CommitBlockListAsync([blockId]).ConfigureAwait(false);
        Assert.Equal(sourceBytes[blockStart..(blockEnd + 1)],
            (await block.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertSourceCustomerKeyAppendAndPageAsync(
        BlobContainerClient container, HttpClient transport,
        byte[] sourceBytes, byte[] sourceKey, byte[] sourceHash)
    {
        const int appendStart = 256;
        const int appendEnd = 767;
        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync().ConfigureAwait(false);
        var appendUri = AppendQuery(
            HttpsSasUri(append, BlobSasPermissions.Add | BlobSasPermissions.Write),
            "comp=appendblock");
        using (var request = CreateSourceKeyRequest(appendUri, sourceKey, sourceHash))
        {
            request.Headers.TryAddWithoutValidation("x-ms-source-range", $"bytes={appendStart}-{appendEnd}");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        Assert.Equal(sourceBytes[appendStart..(appendEnd + 1)],
            (await append.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(1024).ConfigureAwait(false);
        var pageUri = AppendQuery(HttpsSasUri(page, BlobSasPermissions.Write), "comp=page");
        using (var request = CreateSourceKeyRequest(pageUri, sourceKey, sourceHash))
        {
            request.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
            request.Headers.TryAddWithoutValidation("x-ms-source-range", "bytes=512-1023");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        var expectedPage = new byte[1024];
        sourceBytes.AsSpan(512, 512).CopyTo(expectedPage);
        Assert.Equal(expectedPage,
            (await page.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertSourceCustomerKeyInvalidRequestsAsync(
        BlobContainerClient container, HttpClient transport, byte[] sourceKey, byte[] sourceHash)
    {
        var invalid = container.GetBlobClient("invalid.bin");
        using (var request = CreateSourceKeyRequest(
                   HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                   sourceKey, sourceHash, version: "2025-11-05"))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var request = CreateSourceKeyRequest(
                   HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                   sourceKey, RandomNumberGenerator.GetBytes(32)))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var request = CreateSourceKeyRequest(
                   HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                   sourceKey, sourceHash, sourceUri: "http://source.example/encrypted"))
        {
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidRequest", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var request = CreateSourceKeyRequest(
                   HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                   sourceKey, sourceHash))
        {
            request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    [Fact]
    public async Task PageBlobsUseSparseCapacityAndTrackAllocatedZeroPages()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"pages-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var page = container.GetPageBlobClient("nested//disk.vhd");
        var chunksBeforeCreate = EnumerateChunkFiles(factory.DataPath).Count();
        const long initialLength = 1L * 1024 * 1024 * 1024 * 1024;
        const long allocationOffset = 4L * 1024 * 1024 * 1024;

        await page.CreateAsync(initialLength);
        Assert.Equal(chunksBeforeCreate, EnumerateChunkFiles(factory.DataPath).Count());

        await page.UploadPagesAsync(new MemoryStream(new byte[512]), allocationOffset);
        Assert.Equal(chunksBeforeCreate, EnumerateChunkFiles(factory.DataPath).Count());
        var allocated = (await page.GetPageRangesAsync()).Value.PageRanges.ToArray();
        var allocatedRange = Assert.Single(allocated);
        Assert.Equal(allocationOffset, allocatedRange.Offset);
        Assert.Equal(512, allocatedRange.Length);

        var payload = Enumerable.Repeat((byte)0x5a, 512).ToArray();
        await page.UploadPagesAsync(new MemoryStream(payload), allocationOffset + 512);
        var merged = Assert.Single((await page.GetPageRangesAsync()).Value.PageRanges);
        Assert.Equal(allocationOffset, merged.Offset);
        Assert.Equal(1024, merged.Length);
        var downloaded = await page.DownloadStreamingAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(allocationOffset, 1024)
        });
        using var bytes = new MemoryStream();
        await downloaded.Value.Content.CopyToAsync(bytes);
        Assert.Equal(new byte[512].Concat(payload).ToArray(), bytes.ToArray());

        await page.ClearPagesAsync(new HttpRange(allocationOffset, 512));
        var remaining = Assert.Single((await page.GetPageRangesAsync()).Value.PageRanges);
        Assert.Equal(allocationOffset + 512, remaining.Offset);
        Assert.Equal(512, remaining.Length);

        await page.ResizeAsync(4096);
        Assert.Empty((await page.GetPageRangesAsync()).Value.PageRanges);
        Assert.Equal(4096, (await page.GetPropertiesAsync()).Value.ContentLength);
    }

    [Fact]
    public async Task PageBlobSequenceConditionsAndSnapshotDiffsMatchTheSdk()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"page-diff-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var page = container.GetPageBlobClient("nested//disk.vhd");
        await page.CreateAsync(2048, new PageBlobCreateOptions { SequenceNumber = 7 });

        var first = Enumerable.Repeat((byte)0x11, 512).ToArray();
        var second = Enumerable.Repeat((byte)0x22, 512).ToArray();
        await page.UploadPagesAsync(
            new MemoryStream(first),
            offset: 0,
            new PageBlobUploadPagesOptions
            {
                Conditions = new PageBlobRequestConditions { IfSequenceNumberEqual = 7 }
            });
        await page.UploadPagesAsync(new MemoryStream(second), offset: 512);
        var snapshot = (await page.CreateSnapshotAsync()).Value.Snapshot;

        var changedFirst = Enumerable.Repeat((byte)0x33, 512).ToArray();
        var third = Enumerable.Repeat((byte)0x44, 512).ToArray();
        await page.UploadPagesAsync(
            new MemoryStream(changedFirst),
            offset: 0,
            new PageBlobUploadPagesOptions
            {
                Conditions = new PageBlobRequestConditions { IfSequenceNumberLessThanOrEqual = 7 }
            });
        await page.ClearPagesAsync(new HttpRange(512, 512));
        await page.UploadPagesAsync(new MemoryStream(third), offset: 1024);

        var failed = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            await page.UploadPagesAsync(
                new MemoryStream(first),
                offset: 1536,
                new PageBlobUploadPagesOptions
                {
                    Conditions = new PageBlobRequestConditions { IfSequenceNumberLessThan = 7 }
                }).ConfigureAwait(false));
        Assert.Equal(412, failed.Status);
        Assert.Equal("SequenceNumberConditionNotMet", failed.ErrorCode);

        var diff = (await page.GetPageRangesDiffAsync(previousSnapshot: snapshot).ConfigureAwait(true)).Value;
        Assert.Collection(
            diff.PageRanges,
            range =>
            {
                Assert.Equal(0, range.Offset);
                Assert.Equal(512, range.Length);
            },
            range =>
            {
                Assert.Equal(1024, range.Offset);
                Assert.Equal(512, range.Length);
            });
        var cleared = Assert.Single(diff.ClearRanges);
        Assert.Equal(512, cleared.Offset);
        Assert.Equal(512, cleared.Length);
    }

    [Fact]
    public async Task IncrementalPageCopiesCreateReadableSnapshotsAndReuseExtents()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"incremental-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetPageBlobClient("source.vhd");
        await source.CreateAsync(2048);
        var firstPage = Enumerable.Repeat((byte)0x51, 512).ToArray();
        var secondPage = Enumerable.Repeat((byte)0x62, 512).ToArray();
        await source.UploadPagesAsync(new MemoryStream(firstPage), offset: 0);
        await source.UploadPagesAsync(new MemoryStream(secondPage), offset: 512);
        var firstSourceSnapshot = (await source.CreateSnapshotAsync()).Value.Snapshot;
        var chunksBeforeFirstCopy = EnumerateChunkFiles(factory.DataPath).Count();

        var destination = container.GetPageBlobClient("backup.vhd");
        var firstExpected = new byte[2048];
        firstPage.CopyTo(firstExpected, 0);
        secondPage.CopyTo(firstExpected, 512);
        var firstDestinationSnapshot = await AssertFirstIncrementalCopyAsync(
            destination, source.Uri, firstSourceSnapshot, firstExpected, chunksBeforeFirstCopy, factory.DataPath);

        var changedPage = Enumerable.Repeat((byte)0x73, 512).ToArray();
        await source.UploadPagesAsync(new MemoryStream(changedPage), offset: 0);
        await source.ClearPagesAsync(new HttpRange(512, 512));
        var secondSourceSnapshot = (await source.CreateSnapshotAsync()).Value.Snapshot;
        var chunksBeforeSecondCopy = EnumerateChunkFiles(factory.DataPath).Count();
        var secondCopy = await destination.StartCopyIncrementalAsync(source.Uri, secondSourceSnapshot);
        await secondCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        var secondProperties = (await destination.GetPropertiesAsync()).Value;
        Assert.NotEqual(firstDestinationSnapshot, secondProperties.DestinationSnapshot, StringComparer.Ordinal);
        Assert.Equal(chunksBeforeSecondCopy, EnumerateChunkFiles(factory.DataPath).Count());

        var secondExpected = new byte[2048];
        changedPage.CopyTo(secondExpected, 0);
        Assert.Equal(
            secondExpected,
            (await destination.WithSnapshot(secondProperties.DestinationSnapshot!).DownloadContentAsync()).Value.Content.ToArray());
        Assert.Equal(
            firstExpected,
            (await destination.WithSnapshot(firstDestinationSnapshot).DownloadContentAsync()).Value.Content.ToArray());

        await AssertIncrementalCopyRejectedTransitionsAsync(source, destination, firstSourceSnapshot);
        await AssertIncrementalCopyListingsAsync(container, destination, secondProperties.DestinationSnapshot);
    }

    private static async Task<string> AssertFirstIncrementalCopyAsync(
        PageBlobClient destination,
        Uri sourceUri,
        string sourceSnapshot,
        byte[] expected,
        int chunkCount,
        string dataPath)
    {
        var firstCopy = await destination.StartCopyIncrementalAsync(sourceUri, sourceSnapshot).ConfigureAwait(false);
        Assert.Equal(202, firstCopy.GetRawResponse().Status);
        await firstCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None).ConfigureAwait(false);
        var firstProperties = (await destination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.True(firstProperties.IsIncrementalCopy);
        Assert.Equal(CopyStatus.Success, firstProperties.CopyStatus);
        Assert.NotNull(firstProperties.DestinationSnapshot);
        Assert.Equal(chunkCount, EnumerateChunkFiles(dataPath).Count());

        var baseRead = await Assert.ThrowsAsync<RequestFailedException>(() => destination.DownloadContentAsync())
            .ConfigureAwait(false);
        Assert.Equal(409, baseRead.Status);
        Assert.Equal("OperationNotAllowedOnIncrementalCopyBlob", baseRead.ErrorCode);
        var snapshot = firstProperties.DestinationSnapshot!;
        Assert.Equal(
            expected,
            (await destination.WithSnapshot(snapshot).DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        return snapshot;
    }

    private static async Task AssertIncrementalCopyRejectedTransitionsAsync(
        PageBlobClient source,
        PageBlobClient destination,
        string firstSourceSnapshot)
    {
        var earlier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.StartCopyIncrementalAsync(source.Uri, firstSourceSnapshot)).ConfigureAwait(false);
        Assert.Equal(409, earlier.Status);
        Assert.Equal("IncrementalCopyOfEarlierVersionSnapshotNotAllowed", earlier.ErrorCode);

        await source.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots).ConfigureAwait(false);
        await source.CreateAsync(2048).ConfigureAwait(false);
        var replacementSnapshot = (await source.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;
        var replacedSource = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.StartCopyIncrementalAsync(source.Uri, replacementSnapshot)).ConfigureAwait(false);
        Assert.Equal(409, replacedSource.Status);
        Assert.Equal("IncrementalCopyBlobMismatch", replacedSource.ErrorCode);
    }

    private async Task AssertIncrementalCopyListingsAsync(
        BlobContainerClient container,
        PageBlobClient destination,
        string? secondDestinationSnapshot)
    {
        var listed = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Snapshots,
            Prefix = destination.Name
        }).ConfigureAwait(false))
            listed.Add(item);
        var listedBase = Assert.Single(listed, item => item.Snapshot is null);
        Assert.Equal(secondDestinationSnapshot, listedBase.Properties.DestinationSnapshot);
        Assert.Equal(2, listed.Count(item => item.Snapshot is not null));

        var listUri = new UriBuilder(container.GenerateSasUri(
            BlobContainerSasPermissions.List,
            DateTimeOffset.UtcNow.AddMinutes(5)));
        listUri.Query = $"{listUri.Query.TrimStart('?')}&restype=container&comp=list&include=snapshots&prefix={destination.Name}";
        using var httpClient = new HttpClient(factory.Server.CreateHandler());
        var listXml = await httpClient.GetStringAsync(listUri.Uri).ConfigureAwait(false);
        Assert.Equal(3, listXml.Split("<IncrementalCopy>true</IncrementalCopy>", StringSplitOptions.None).Length - 1);
    }

    [Fact]
    public async Task IncrementalCopyRejectsHeadersOutsideItsProtocolSurfaceBeforeResolvingTheSource()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"incremental-headers-{Guid.NewGuid():N}");
        await container.CreateAsync();
        using var transport = new HttpClient(factory.Server.CreateHandler());

        foreach (var (header, value) in new[]
                 {
                     ("x-ms-meta-unsupported", "value"),
                     ("x-ms-source-if-match", "\"etag\"")
                 })
        {
            var destination = container.GetPageBlobClient($"{Guid.NewGuid():N}.vhd");
            var destinationSas = destination.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5));
            using var request = new HttpRequestMessage(
                HttpMethod.Put,
                AppendQuery(destinationSas, "comp=incrementalcopy"))
            {
                Content = new ByteArrayContent([])
            };
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            request.Headers.TryAddWithoutValidation(
                "x-ms-copy-source",
                "https://source.invalid/disk.vhd?snapshot=2026-09-22T00%3A00%3A00.0000000Z");
            request.Headers.TryAddWithoutValidation(header, value);

            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("UnsupportedHeader", response.Headers.GetValues("x-ms-error-code").Single());
            Assert.False((await destination.ExistsAsync()).Value);
        }
    }

    [Fact]
    public async Task ImmutabilityPoliciesAndLegalHoldsArePersistedAndEnforced()
    {
        var containerName = $"worm-{Guid.NewGuid():N}";
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:ImmutableStorageWithVersioningContainers:0"] = containerName
            });
        await using var applicationDisposal40 = application.ConfigureAwait(false);
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blob = container.GetBlobClient("retained.txt");
        var expiresOn = DateTimeOffset.UtcNow.AddHours(2);

        await blob.UploadAsync(BinaryData.FromString("retained payload"), new BlobUploadOptions
        {
            ImmutabilityPolicy = new BlobImmutabilityPolicy
            {
                ExpiresOn = expiresOn,
                PolicyMode = BlobImmutabilityPolicyMode.Unlocked
            },
            LegalHold = true
        });

        var properties = (await blob.GetPropertiesAsync()).Value;
        Assert.True(properties.HasLegalHold);
        Assert.Equal(BlobImmutabilityPolicyMode.Unlocked, properties.ImmutabilityPolicy.PolicyMode);
        Assert.Equal(expiresOn.ToUnixTimeSeconds(), properties.ImmutabilityPolicy.ExpiresOn!.Value.ToUnixTimeSeconds());

        var held = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["attempt"] = "held" }));
        Assert.Equal(409, held.Status);
        Assert.Equal("BlobImmutableDueToLegalHold", held.ErrorCode);

        await blob.SetLegalHoldAsync(false);
        var retained = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["attempt"] = "retained" }));
        Assert.Equal(409, retained.Status);
        Assert.Equal("BlobImmutableDueToPolicy", retained.ErrorCode);

        await blob.DeleteImmutabilityPolicyAsync();
        await blob.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "mutable" });
        Assert.Equal("mutable", (await blob.GetPropertiesAsync()).Value.Metadata["state"]);

        await AssertLockedImmutabilityCannotBeRemovedAsync(container, expiresOn);
    }

    private static async Task AssertLockedImmutabilityCannotBeRemovedAsync(
        BlobContainerClient container, DateTimeOffset expiresOn)
    {
        var locked = container.GetBlobClient("locked.txt");
        await locked.UploadAsync(BinaryData.FromString("locked payload")).ConfigureAwait(false);
        await locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
        {
            ExpiresOn = expiresOn,
            PolicyMode = BlobImmutabilityPolicyMode.Unlocked
        }).ConfigureAwait(false);
        await locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
        {
            ExpiresOn = expiresOn.AddHours(1),
            PolicyMode = BlobImmutabilityPolicyMode.Locked
        }).ConfigureAwait(false);
        var cannotUnlock = await Assert.ThrowsAsync<RequestFailedException>(() =>
            locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
            {
                ExpiresOn = expiresOn.AddHours(2),
                PolicyMode = BlobImmutabilityPolicyMode.Unlocked
            })).ConfigureAwait(false);
        Assert.Equal("BlobImmutableDueToPolicy", cannotUnlock.ErrorCode);
        var cannotDelete = await Assert.ThrowsAsync<RequestFailedException>(() =>
            locked.DeleteImmutabilityPolicyAsync()).ConfigureAwait(false);
        Assert.Equal("BlobImmutableDueToPolicy", cannotDelete.ErrorCode);
    }

    [Fact]
    public async Task ImmutableStorageWithVersioningCapabilityMatchesContainerAndPolicySemantics()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:ImmutableStorageWithVersioningEnabled"] = "true"
            });
        await using var applicationDisposal41 = application.ConfigureAwait(false);
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient($"version-worm-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await AssertImmutableContainerCapabilityAsync(service, container);
        await AssertImmutableVersionSnapshotsAsync(container);

        using var transport = new HttpClient(application.Server.CreateHandler());
        await AssertImmutableCapabilityHeaderAsync(container, transport);
        await AssertImmutableProtectedDeletionAsync(service);
        await AssertOrdinaryAccountWithoutImmutabilityAsync(application, transport);
    }

    private static async Task AssertImmutableContainerCapabilityAsync(
        BlobServiceClient service, BlobContainerClient container)
    {
        Assert.True((await container.GetPropertiesAsync().ConfigureAwait(false)).Value.HasImmutableStorageWithVersioning);
        BlobContainerItem? listed = null;
        await foreach (var item in service.GetBlobContainersAsync(prefix: container.Name).ConfigureAwait(false))
            listed = item;
        Assert.NotNull(listed);
        Assert.True(listed!.Properties.HasImmutableStorageWithVersioning);
    }

    private static async Task AssertImmutableVersionSnapshotsAsync(BlobContainerClient container)
    {
        var versioned = container.GetBlobClient("versioned.txt");
        await versioned.UploadAsync(BinaryData.FromString("first")).ConfigureAwait(false);
        await versioned.UploadAsync(BinaryData.FromString("second"), overwrite: true).ConfigureAwait(false);
        var versions = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = versioned.Name,
            States = BlobStates.Version
        }).ConfigureAwait(false))
        {
            versions.Add(item);
        }
        Assert.Equal(2, versions.Count);
        Assert.All(versions, item => Assert.False(string.IsNullOrEmpty(item.VersionId)));

        var append = container.GetAppendBlobClient("append-versioned.bin");
        var appendVersion = (await append.CreateAsync().ConfigureAwait(false)).Value.VersionId;
        await append.AppendBlockAsync(BinaryData.FromString("append").ToStream()).ConfigureAwait(false);
        Assert.Equal(appendVersion, (await append.GetPropertiesAsync().ConfigureAwait(false)).Value.VersionId);
        var appendVersions = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = append.Name,
            States = BlobStates.Version
        }).ConfigureAwait(false))
        {
            appendVersions.Add(item);
        }
        Assert.Single(appendVersions);

        var page = container.GetPageBlobClient("page-versioned.bin");
        var pageVersion = (await page.CreateAsync(512).ConfigureAwait(false)).Value.VersionId;
        await page.UploadPagesAsync(BinaryData.FromBytes(new byte[512]).ToStream(), 0).ConfigureAwait(false);
        Assert.Equal(pageVersion, (await page.GetPropertiesAsync().ConfigureAwait(false)).Value.VersionId);
        var pageVersions = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = page.Name,
            States = BlobStates.Version
        }).ConfigureAwait(false))
        {
            pageVersions.Add(item);
        }
        Assert.Single(pageVersions);
    }

    private static Uri ImmutableCapabilitySasUri(
        BlobContainerClient container, string accountName, string accountKey)
    {
        var accountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddDays(7),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountSas.SetPermissions(AccountSasPermissions.Read);
        return AppendQuery(container.Uri,
            accountSas.ToSasQueryParameters(new StorageSharedKeyCredential(accountName, accountKey)).ToString());
    }

    private static async Task AssertImmutableCapabilityHeaderAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var containerSas = ImmutableCapabilitySasUri(container,
            SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        using (var legacy = new HttpRequestMessage(HttpMethod.Head, AppendQuery(containerSas, "restype=container")))
        {
            legacy.Headers.TryAddWithoutValidation("x-ms-version", "2020-06-12");
            using var response = await transport.SendAsync(legacy).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.False(response.Headers.Contains("x-ms-immutable-storage-with-versioning-enabled"));
        }
        using (var supported = new HttpRequestMessage(HttpMethod.Head, AppendQuery(containerSas, "restype=container")))
        {
            supported.Headers.TryAddWithoutValidation("x-ms-version", "2020-10-02");
            using var response = await transport.SendAsync(supported).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("true",
                response.Headers.GetValues("x-ms-immutable-storage-with-versioning-enabled").Single());
        }
    }

    private static async Task AssertImmutableProtectedDeletionAsync(BlobServiceClient service)
    {
        var protectedEmpty = service.GetBlobContainerClient($"version-worm-empty-{Guid.NewGuid():N}");
        await protectedEmpty.CreateAsync().ConfigureAwait(false);
        var protectedDelete = await Assert.ThrowsAsync<RequestFailedException>(() => protectedEmpty.DeleteAsync())
            .ConfigureAwait(false);
        Assert.Equal(409, protectedDelete.Status);
        Assert.Equal("ContainerImmutableStorageWithVersioningEnabled", protectedDelete.ErrorCode);
        Assert.True((await protectedEmpty.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertOrdinaryAccountWithoutImmutabilityAsync(
        SavaWebApplicationFactory application, HttpClient transport)
    {
        var ordinaryService = CreateClient(application,
            SavaWebApplicationFactory.SecondAccountName, SavaWebApplicationFactory.SecondAccountKey);
        var ordinaryContainer = ordinaryService.GetBlobContainerClient($"ordinary-worm-{Guid.NewGuid():N}");
        await ordinaryContainer.CreateAsync().ConfigureAwait(false);
        Assert.False((await ordinaryContainer.GetPropertiesAsync().ConfigureAwait(false)).Value.HasImmutableStorageWithVersioning);
        var ordinarySas = ImmutableCapabilitySasUri(ordinaryContainer,
            SavaWebApplicationFactory.SecondAccountName, SavaWebApplicationFactory.SecondAccountKey);
        using (var ordinaryProperties = new HttpRequestMessage(
                   HttpMethod.Head, AppendQuery(ordinarySas, "restype=container")))
        {
            ordinaryProperties.Headers.TryAddWithoutValidation("x-ms-version", "2020-10-02");
            using var response = await transport.SendAsync(ordinaryProperties).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("false",
                response.Headers.GetValues("x-ms-immutable-storage-with-versioning-enabled").Single());
        }

        var unsupported = ordinaryContainer.GetBlobClient("unsupported.txt");
        var unsupportedPolicy = await Assert.ThrowsAsync<RequestFailedException>(() =>
            unsupported.UploadAsync(BinaryData.FromString("must not publish"), new BlobUploadOptions
            {
                ImmutabilityPolicy = new BlobImmutabilityPolicy
                {
                    ExpiresOn = DateTimeOffset.UtcNow.AddDays(1),
                    PolicyMode = BlobImmutabilityPolicyMode.Unlocked
                }
            })).ConfigureAwait(false);
        Assert.Equal(409, unsupportedPolicy.Status);
        Assert.Equal("BlobOperationNotSupported", unsupportedPolicy.ErrorCode);
        Assert.False((await unsupported.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task ArchiveTierBlocksReadsAndRehydratesThroughPendingState()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"archive-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("cold.bin");
        var content = Enumerable.Range(0, 32 * 1024).Select(index => (byte)(index % 251)).ToArray();
        await blob.UploadAsync(BinaryData.FromBytes(content));

        var archived = await blob.SetAccessTierAsync(AccessTier.Archive);
        Assert.Equal(200, archived.Status);
        var archivedProperties = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Archive, archivedProperties.AccessTier);
        Assert.Null(archivedProperties.ArchiveStatus);
        var offline = await Assert.ThrowsAsync<RequestFailedException>(() => blob.DownloadContentAsync());
        Assert.Equal(409, offline.Status);
        Assert.Equal("BlobArchived", offline.ErrorCode);

        var pending = await blob.SetAccessTierAsync(
            AccessTier.Hot,
            rehydratePriority: RehydratePriority.Standard);
        Assert.Equal(202, pending.Status);
        var pendingProperties = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Archive, pendingProperties.AccessTier);
        Assert.Equal("rehydrate-pending-to-hot", pendingProperties.ArchiveStatus);
        Assert.Equal("Standard", pendingProperties.RehydratePriority);

        var wrongTarget = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetAccessTierAsync(AccessTier.Cool, rehydratePriority: RehydratePriority.High));
        Assert.Equal("BlobBeingRehydrated", wrongTarget.ErrorCode);

        var prioritized = await blob.SetAccessTierAsync(
            AccessTier.Hot,
            rehydratePriority: RehydratePriority.High);
        Assert.Equal(202, prioritized.Status);
        Assert.Equal("High", (await blob.GetPropertiesAsync()).Value.RehydratePriority);

        await Task.Delay(300);
        var online = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Hot, online.AccessTier);
        Assert.Null(online.ArchiveStatus);
        Assert.Equal(content, (await blob.DownloadContentAsync()).Value.Content.ToArray());

        await AssertSmartTierPropertiesAsync(container, blob);
        await AssertSmartTierVersionBoundaryAsync(factory, blob);
    }

    private static async Task AssertSmartTierPropertiesAsync(
        BlobContainerClient container, BlobClient blob)
    {
        var smart = await blob.SetAccessTierAsync(AccessTier.Smart).ConfigureAwait(false);
        Assert.Equal(200, smart.Status);
        var smartProperties = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(AccessTier.Smart, smartProperties.AccessTier);
        Assert.Equal("Hot", smartProperties.SmartAccessTier);
        var smartItems = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(
                           new GetBlobsOptions { Prefix = blob.Name }).ConfigureAwait(false))
            smartItems.Add(item);
        var smartItem = Assert.Single(smartItems);
        Assert.Equal(AccessTier.Smart, smartItem.Properties.AccessTier);
        Assert.Equal("Hot", smartItem.Properties.SmartAccessTier);
    }

    private static async Task AssertSmartTierVersionBoundaryAsync(
        SavaWebApplicationFactory application, BlobClient blob)
    {
        using var transport = new HttpClient(application.Server.CreateHandler());
        var tierUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=tier");
        using var oldVersionTierRequest = new HttpRequestMessage(HttpMethod.Put, tierUri)
        {
            Content = new ByteArrayContent([])
        };
        oldVersionTierRequest.Headers.Add("x-ms-version", "2023-11-03");
        oldVersionTierRequest.Headers.Add("x-ms-access-tier", "Smart");
        using var oldVersionTierResponse = await transport.SendAsync(oldVersionTierRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, oldVersionTierResponse.StatusCode);
        Assert.Equal("FeatureVersionMismatch", oldVersionTierResponse.Headers.GetValues("x-ms-error-code").Single());

        using var oldVersionPropertiesRequest = new HttpRequestMessage(
            HttpMethod.Head,
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        oldVersionPropertiesRequest.Headers.Add("x-ms-version", "2023-11-03");
        using var oldVersionPropertiesResponse = await transport.SendAsync(oldVersionPropertiesRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, oldVersionPropertiesResponse.StatusCode);
        Assert.False(oldVersionPropertiesResponse.Headers.Contains("x-ms-smart-access-tier"));
    }

    [Fact]
    public async Task SmartTierTracksDataAccessAndTransitionsOnlyEligibleBlobs()
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"smart-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            var blobs = application.Services.GetRequiredService<BlobService>();
            var scenario = await CreateSmartTierScenarioAsync(metadata, container);
            await AssertSmartTierCoolingAsync(clock, blobs, metadata, container, scenario);
            await AssertSmartTierReheatAndRewriteAsync(clock, blobs, metadata, container, scenario);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private sealed record SmartTierScenario(
        byte[] ManagedBytes, BlobClient Managed, BlobClient Small, BlobClient Deleted, BlobProperties Original);

    private static async Task<SmartTierScenario> CreateSmartTierScenarioAsync(
        MetadataStore metadata, BlobContainerClient container)
    {
        var serviceProperties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName, CancellationToken.None).ConfigureAwait(false);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            serviceProperties with { BlobSoftDeleteEnabled = true, BlobSoftDeleteRetentionDays = 365 },
            CancellationToken.None).ConfigureAwait(false);

        var managedBytes = Enumerable.Range(0, 128 * 1024 + 1)
            .Select(index => (byte)(index % 251)).ToArray();
        var managed = container.GetBlobClient("managed.bin");
        await managed.UploadAsync(BinaryData.FromBytes(managedBytes),
            new BlobUploadOptions { AccessTier = AccessTier.Smart }).ConfigureAwait(false);
        var small = container.GetBlobClient("small.bin");
        await small.UploadAsync(BinaryData.FromBytes(new byte[128 * 1024]),
            new BlobUploadOptions { AccessTier = AccessTier.Smart }).ConfigureAwait(false);
        var deleted = container.GetBlobClient("deleted.bin");
        await deleted.UploadAsync(BinaryData.FromBytes(managedBytes),
            new BlobUploadOptions { AccessTier = AccessTier.Smart }).ConfigureAwait(false);
        await deleted.DeleteAsync().ConfigureAwait(false);

        var original = (await managed.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(AccessTier.Smart, original.AccessTier);
        Assert.Equal("Hot", original.SmartAccessTier);
        return new SmartTierScenario(managedBytes, managed, small, deleted, original);
    }

    private static async Task AssertSmartTierCoolingAsync(
        AdjustableTimeProvider clock, BlobService blobs, MetadataStore metadata,
        BlobContainerClient container, SmartTierScenario scenario)
    {
        clock.Advance(TimeSpan.FromDays(30) - TimeSpan.FromTicks(1));
        var early = await blobs.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(0, early.CompletedSmartTierTransitions);
        Assert.Equal("Hot", (await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false)).Value.SmartAccessTier);

        clock.Advance(TimeSpan.FromTicks(1));
        var cooled = await blobs.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, cooled.CompletedSmartTierTransitions);
        var coolProperties = (await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal("Cool", coolProperties.SmartAccessTier);
        Assert.Equal(scenario.Original.ETag, coolProperties.ETag);
        Assert.Equal(scenario.Original.LastModified, coolProperties.LastModified);
        Assert.Equal("Hot", (await scenario.Small.GetPropertiesAsync().ConfigureAwait(false)).Value.SmartAccessTier);
        var deletedRecord = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, scenario.Deleted.Name,
            versionId: null, snapshot: null, includeDeleted: true, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal("Cool", deletedRecord?.SmartAccessTier);

        _ = await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false);
        _ = await scenario.Managed.GetTagsAsync().ConfigureAwait(false);
        await scenario.Managed.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["observed"] = "without-access" })
            .ConfigureAwait(false);
        clock.Advance(TimeSpan.FromDays(60));
        var chilled = await blobs.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, chilled.CompletedSmartTierTransitions);
        Assert.Equal("Cold", (await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false)).Value.SmartAccessTier);
        Assert.Equal("Hot", (await scenario.Small.GetPropertiesAsync().ConfigureAwait(false)).Value.SmartAccessTier);
    }

    private static async Task AssertSmartTierReheatAndRewriteAsync(
        AdjustableTimeProvider clock, BlobService blobs, MetadataStore metadata,
        BlobContainerClient container, SmartTierScenario scenario)
    {
        Assert.Equal(scenario.ManagedBytes,
            (await scenario.Managed.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var reheated = (await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal("Hot", reheated.SmartAccessTier);
        var accessedRecord = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, container.Name, scenario.Managed.Name,
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(clock.GetUtcNow(), accessedRecord?.SmartTierLastAccessedAt);

        clock.Advance(TimeSpan.FromDays(30) - TimeSpan.FromTicks(1));
        _ = await blobs.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal("Hot", (await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false)).Value.SmartAccessTier);
        clock.Advance(TimeSpan.FromTicks(1));
        _ = await blobs.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal("Cool", (await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false)).Value.SmartAccessTier);

        clock.Advance(TimeSpan.FromDays(60));
        _ = await blobs.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal("Cold", (await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false)).Value.SmartAccessTier);
        await scenario.Managed.UploadAsync(BinaryData.FromBytes(scenario.ManagedBytes), overwrite: true)
            .ConfigureAwait(false);
        var rewritten = (await scenario.Managed.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(AccessTier.Smart, rewritten.AccessTier);
        Assert.Equal("Hot", rewritten.SmartAccessTier);
    }

    [Fact]
    public async Task LastAccessTimeTrackingMatchesAzureReadWriteAndListingSemantics()
    {
        var initialTime = new DateTimeOffset(2026, 9, 22, 14, 0, 0, TimeSpan.Zero);
        var clock = new AdjustableTimeProvider(initialTime);
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:LastAccessTimeTrackingEnabled"] = "true",
                ["Sava:MaintenanceScanInterval"] = "01:00:00"
            });
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"last-access-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var blob = container.GetBlobClient("tracked.bin");
            await blob.UploadAsync(BinaryData.FromString("first"));
            var copiedAt = await AssertLastAccessReadWriteAndCopyAsync(clock, container, blob, initialTime);
            using var transport = new HttpClient(application.Server.CreateHandler());
            await AssertLastAccessArrowAndVersionHeadersAsync(container, blob, transport, copiedAt);
            await AssertUntrackedLastAccessAsync(application);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task<DateTimeOffset> AssertLastAccessReadWriteAndCopyAsync(
        AdjustableTimeProvider clock, BlobContainerClient container, BlobClient blob, DateTimeOffset initialTime)
    {
        var created = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(initialTime, created.LastAccessed);

        clock.Advance(TimeSpan.FromHours(23));
        var propertiesOnly = (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(initialTime, propertiesOnly.LastAccessed);
        var firstRead = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value;
        Assert.Equal("first", firstRead.Content.ToString());
        Assert.Equal(initialTime, firstRead.Details.LastAccessed);

        clock.Advance(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(1));
        var secondAccess = clock.GetUtcNow();
        var secondRead = (await blob.DownloadContentAsync().ConfigureAwait(false)).Value;
        Assert.Equal(secondAccess, secondRead.Details.LastAccessed);
        Assert.Equal(secondAccess, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.LastAccessed);

        BlobItem? listed = null;
        await foreach (var item in container.GetBlobsAsync(
                           new GetBlobsOptions { Prefix = blob.Name }).ConfigureAwait(false))
            listed = item;
        Assert.NotNull(listed);
        Assert.Equal(secondAccess, listed!.Properties.LastAccessedOn);

        clock.Advance(TimeSpan.FromMinutes(5));
        var rewrittenAt = clock.GetUtcNow();
        await blob.UploadAsync(BinaryData.FromString("second"), overwrite: true).ConfigureAwait(false);
        Assert.Equal(rewrittenAt, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.LastAccessed);

        clock.Advance(TimeSpan.FromHours(24) + TimeSpan.FromSeconds(1));
        var copiedAt = clock.GetUtcNow();
        var copied = container.GetBlobClient("copied.bin");
        await copied.SyncCopyFromUriAsync(
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddDays(7))).ConfigureAwait(false);
        Assert.Equal(copiedAt, (await blob.GetPropertiesAsync().ConfigureAwait(false)).Value.LastAccessed);
        Assert.Equal(copiedAt, (await copied.GetPropertiesAsync().ConfigureAwait(false)).Value.LastAccessed);
        return copiedAt;
    }

    private static async Task AssertLastAccessArrowAndVersionHeadersAsync(
        BlobContainerClient container, BlobClient blob, HttpClient transport, DateTimeOffset copiedAt)
    {
        var arrowUri = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddDays(7)),
            "restype=container&comp=list");
        using (var arrowRequest = new HttpRequestMessage(HttpMethod.Get, arrowUri))
        {
            arrowRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            arrowRequest.Headers.TryAddWithoutValidation("Accept", AzureResponseWriter.ArrowStreamContentType);
            using var response = await transport.SendAsync(arrowRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            await using var streamDisposal42 = stream.ConfigureAwait(false);
            using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(stream);
            using var batch = await reader.ReadNextRecordBatchAsync().ConfigureAwait(false);
            Assert.NotNull(batch);
            var names = Assert.IsType<Apache.Arrow.StringArray>(batch.Column("Name", StringComparer.Ordinal));
            var accesses = Assert.IsType<Apache.Arrow.TimestampArray>(batch.Column("LastAccessTime", StringComparer.Ordinal));
            var sourceIndex = Enumerable.Range(0, batch.Length).Single(index =>
                string.Equals(names.GetString(index), blob.Name, StringComparison.Ordinal));
            Assert.Equal(copiedAt, accesses.GetTimestamp(sourceIndex));
        }

        using (var legacy = new HttpRequestMessage(
                   HttpMethod.Head,
                   blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddDays(7))))
        {
            legacy.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
            using var response = await transport.SendAsync(legacy).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.False(response.Headers.Contains("x-ms-last-access-time"));
        }
    }

    private static async Task AssertUntrackedLastAccessAsync(SavaWebApplicationFactory application)
    {
        var untrackedService = CreateClient(application,
            SavaWebApplicationFactory.SecondAccountName, SavaWebApplicationFactory.SecondAccountKey);
        var untrackedContainer = untrackedService.GetBlobContainerClient($"last-access-off-{Guid.NewGuid():N}");
        await untrackedContainer.CreateAsync().ConfigureAwait(false);
        var untracked = untrackedContainer.GetBlobClient("untracked.bin");
        await untracked.UploadAsync(BinaryData.FromString("untracked")).ConfigureAwait(false);
        Assert.Equal(default, (await untracked.GetPropertiesAsync().ConfigureAwait(false)).Value.LastAccessed);
        Assert.Equal(default, (await untracked.DownloadContentAsync().ConfigureAwait(false)).Value.Details.LastAccessed);
    }

    [Fact]
    public async Task AsynchronousCopiesCompleteDurablyAndCanBeAborted()
    {
        var remoteBytes = Enumerable.Range(0, 48 * 1024).Select(index => (byte)(index % 241)).ToArray();
        var remote = (await LoopbackSource.StartAsync(remoteBytes));
        await using var remoteDisposal43 = remote.ConfigureAwait(false);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"copy-{Guid.NewGuid():N}");
        await container.CreateAsync();

        await AssertAbortableRemoteCopyAsync(container, remote.Uri);
        await AssertCompletedSameAccountCopyAsync(container, remoteBytes);
    }

    private static async Task AssertAbortableRemoteCopyAsync(BlobContainerClient container, Uri remoteUri)
    {
        var abortDestination = container.GetBlobClient("abort.bin");
        var credentialedSource = new UriBuilder(remoteUri) { Query = "source=remote&sig=must-not-leak" }.Uri;
        var abortOperation = await abortDestination.StartCopyFromUriAsync(
            credentialedSource,
            new BlobCopyFromUriOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["copy"] = "abort" }
            }).ConfigureAwait(false);
        Assert.Equal(202, abortOperation.GetRawResponse().Status);
        Assert.False(abortOperation.HasCompleted);
        var pending = (await abortDestination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(CopyStatus.Pending, pending.CopyStatus);
        Assert.Equal(0, pending.ContentLength);
        Assert.Contains("source=remote", pending.CopySource.AbsoluteUri, StringComparison.Ordinal);
        Assert.DoesNotContain("sig=", pending.CopySource.AbsoluteUri, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("abort", pending.Metadata["copy"]);

        var aborted = await abortDestination.AbortCopyFromUriAsync(abortOperation.Id).ConfigureAwait(false);
        Assert.Equal(204, aborted.Status);
        var abortedProperties = (await abortDestination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(CopyStatus.Aborted, abortedProperties.CopyStatus);
        Assert.Equal(0, abortedProperties.ContentLength);
        Assert.Equal("abort", abortedProperties.Metadata["copy"]);
    }

    private static async Task AssertCompletedSameAccountCopyAsync(BlobContainerClient container, byte[] remoteBytes)
    {
        var source = container.GetBlobClient("nested//source%.bin");
        await source.UploadAsync(BinaryData.FromBytes(remoteBytes), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["origin"] = "internal" },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["class"] = "copy" }
        }).ConfigureAwait(false);
        var completedDestination = container.GetBlobClient("completed.bin");
        var completion = await completedDestination.StartCopyFromUriAsync(source.Uri).ConfigureAwait(false);
        Assert.Equal(CopyStatus.Pending,
            (await completedDestination.GetPropertiesAsync().ConfigureAwait(false)).Value.CopyStatus);
        await completion.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .ConfigureAwait(false);

        var completed = (await completedDestination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(CopyStatus.Success, completed.CopyStatus);
        Assert.Equal($"{remoteBytes.LongLength}/{remoteBytes.LongLength}", completed.CopyProgress);
        Assert.Equal("internal", completed.Metadata["origin"]);
        Assert.Empty((await completedDestination.GetTagsAsync().ConfigureAwait(false)).Value.Tags);
        Assert.Equal(remoteBytes,
            (await completedDestination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    [Fact]
    public async Task LegacyCopyBlobIsSynchronousSameAccountAndHonorsItsSourceLeaseHeader()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"legacy-copy-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var (source, firstBlockId, secondBlockId) = await CreateLegacyCopySourceAsync(container);
        var sourcePath = $"/{SavaWebApplicationFactory.AccountName}/{container.Name}/{source.Name}";
        var sourceLeaseId = Guid.NewGuid().ToString();
        var sourceLease = source.GetBlobLeaseClient(sourceLeaseId);
        await sourceLease.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration);
        using var transport = new HttpClient(factory.Server.CreateHandler());

        var mismatchedTarget = container.GetBlobClient("mismatched-source-lease.bin");
        using (var request = CreateLegacyCopyRequest(mismatchedTarget.Uri, sourcePath, Guid.NewGuid().ToString()))
        using (var response = await transport.SendAsync(request))
        {
            Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
            await AssertVersionedErrorAsync(response, "LeaseIdMismatchWithBlobOperation");
        }
        Assert.False((await mismatchedTarget.ExistsAsync()).Value);

        await AssertLegacyBlockCopyAsync(
            container, transport, sourcePath, sourceLeaseId, firstBlockId, secondBlockId);

        var noSourceLeaseHeaderTarget = container.GetBlobClient("source-lease-optional.bin");
        using (var request = CreateLegacyCopyRequest(noSourceLeaseHeaderTarget.Uri, sourcePath))
        using (var response = await transport.SendAsync(request))
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        await sourceLease.ReleaseAsync();
        var absentSourceLeaseTarget = container.GetBlobClient("absent-source-lease.bin");
        using (var request = CreateLegacyCopyRequest(absentSourceLeaseTarget.Uri, sourcePath, sourceLeaseId))
        using (var response = await transport.SendAsync(request))
        {
            Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
            await AssertVersionedErrorAsync(response, "LeaseNotPresentWithBlobOperation");
        }
        Assert.False((await absentSourceLeaseTarget.ExistsAsync()).Value);

        await AssertLegacyPageAndCrossAccountCopyAsync(container, source.Name, transport);
    }

    private static async Task<(BlockBlobClient Source, string FirstBlockId, string SecondBlockId)>
        CreateLegacyCopySourceAsync(BlobContainerClient container)
    {
        var source = container.GetBlockBlobClient("source.bin");
        var firstBlockId = Convert.ToBase64String("legacy-copy-block-0001"u8);
        var secondBlockId = Convert.ToBase64String("legacy-copy-block-0002"u8);
        await source.StageBlockAsync(firstBlockId, BinaryData.FromString("first|").ToStream()).ConfigureAwait(false);
        await source.StageBlockAsync(secondBlockId, BinaryData.FromString("second").ToStream()).ConfigureAwait(false);
        await source.CommitBlockListAsync(
            [firstBlockId, secondBlockId],
            new CommitBlockListOptions
            {
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["origin"] = "legacy" },
                HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-legacy-copy" }
            }).ConfigureAwait(false);
        return (source, firstBlockId, secondBlockId);
    }

    private static async Task AssertLegacyBlockCopyAsync(
        BlobContainerClient container,
        HttpClient transport,
        string sourcePath,
        string sourceLeaseId,
        string firstBlockId,
        string secondBlockId)
    {
        var destination = container.GetBlockBlobClient("destination.bin");
        using (var request = CreateLegacyCopyRequest(destination.Uri, sourcePath, sourceLeaseId))
        using (var response = await transport.SendAsync(request).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.NotNull(response.Headers.ETag);
            Assert.True(response.Content.Headers.LastModified.HasValue);
            Assert.False(response.Headers.Contains("x-ms-copy-id"));
            Assert.False(response.Headers.Contains("x-ms-copy-status"));
        }

        var properties = (await destination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(default, properties.CopyStatus);
        Assert.Equal("application/x-legacy-copy", properties.ContentType);
        Assert.Equal("legacy", properties.Metadata["origin"]);
        Assert.Equal("first|second", (await destination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        var blocks = (await destination.GetBlockListAsync(BlockListTypes.Committed).ConfigureAwait(false)).Value.CommittedBlocks;
        Assert.Equal([firstBlockId, secondBlockId], blocks.Select(block => block.Name), StringComparer.Ordinal);
    }

    private static async Task AssertLegacyPageAndCrossAccountCopyAsync(
        BlobContainerClient container,
        string blockSourceName,
        HttpClient transport)
    {
        var pageSource = container.GetPageBlobClient("source.vhd");
        await pageSource.CreateAsync(1024, new PageBlobCreateOptions { SequenceNumber = 9 }).ConfigureAwait(false);
        await pageSource.UploadPagesAsync(
            new MemoryStream(Enumerable.Repeat((byte)0x51, 512).ToArray()),
            offset: 512).ConfigureAwait(false);
        var pageDestination = container.GetPageBlobClient("destination.vhd");
        var pageSourcePath = $"/{SavaWebApplicationFactory.AccountName}/{container.Name}/{pageSource.Name}";
        using (var request = CreateLegacyCopyRequest(pageDestination.Uri, pageSourcePath))
        using (var response = await transport.SendAsync(request).ConfigureAwait(false))
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var pageProperties = (await pageDestination.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(BlobType.Page, pageProperties.BlobType);
        Assert.Equal(9, pageProperties.BlobSequenceNumber);
        Assert.Equal(default, pageProperties.CopyStatus);
        var pageRange = Assert.Single((await pageDestination.GetPageRangesAsync().ConfigureAwait(false)).Value.PageRanges);
        Assert.Equal(512, pageRange.Offset);
        Assert.Equal(512, pageRange.Length);

        var crossAccountTarget = container.GetBlobClient("cross-account.bin");
        var crossAccountSource = $"/{SavaWebApplicationFactory.SecondAccountName}/{container.Name}/{blockSourceName}";
        using (var request = CreateLegacyCopyRequest(crossAccountTarget.Uri, crossAccountSource))
        using (var response = await transport.SendAsync(request).ConfigureAwait(false))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            await AssertVersionedErrorAsync(response, "CopyAcrossAccountsNotSupported").ConfigureAwait(false);
        }
        Assert.False((await crossAccountTarget.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task CopyBlobAuthenticatesSourceIndependentlyForSasDestinations()
    {
        var owner = CreateClient(factory);
        var container = owner.GetBlobContainerClient($"copy-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlobClient("source.bin");
        var content = Enumerable.Range(0, 32 * 1024).Select(index => (byte)(index % 239)).ToArray();
        await source.UploadAsync(BinaryData.FromBytes(content), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["origin"] = "source-sas" },
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["class"] = "tagged" }
        });

        var sourceSas = await AssertCopySourceSasAsync(factory, container, source, content);

        await AssertCopySourceTagConditionsAsync(factory, container, source, sourceSas, content);

        await AssertCopyBearerSourceAsync(factory, container, source, content);

        await AssertCopyUnsignedAndWrongResourceSourcesAsync(factory, container, source);
        await AssertCopySourceLeaseHeaderRejectedAsync(factory, container, sourceSas);
    }

    private static async Task<Uri> AssertCopySourceSasAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, BlobClient source, byte[] content)
    {
        var sourceSas = source.GenerateSasUri(
            BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(10));
        var destination = container.GetBlobClient("destination.bin");
        var destinationSas = destination.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var delegatedDestination = CreateBlobClient(application, destinationSas);
        var copy = await delegatedDestination.StartCopyFromUriAsync(sourceSas).ConfigureAwait(false);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .ConfigureAwait(false);
        Assert.Equal(content, (await destination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        Assert.Equal("source-sas", (await destination.GetPropertiesAsync().ConfigureAwait(false)).Value.Metadata["origin"]);
        return sourceSas;
    }

    private static async Task AssertCopySourceTagConditionsAsync(
        SavaWebApplicationFactory application, BlobContainerClient container,
        BlobClient source, Uri sourceSas, byte[] content)
    {
        var taggedSourceSas = source.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Tag, DateTimeOffset.UtcNow.AddMinutes(10));
        var taggedDestination = container.GetBlobClient("tag-conditioned.bin");
        var taggedDestinationSas = taggedDestination.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var taggedCopy = await CreateBlobClient(application, taggedDestinationSas).StartCopyFromUriAsync(
            taggedSourceSas,
            new BlobCopyFromUriOptions
            {
                SourceConditions = new BlobRequestConditions { TagConditions = "\"class\" = 'tagged'" }
            }).ConfigureAwait(false);
        await taggedCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None)
            .ConfigureAwait(false);
        Assert.Equal(content, (await taggedDestination.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());

        var untaggedSourceTarget = container.GetBlobClient("tag-condition-without-source-permission.bin");
        var untaggedSourceTargetSas = untaggedSourceTarget.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var missingSourceTagPermission = await Assert.ThrowsAsync<RequestFailedException>(() =>
            CreateBlobClient(application, untaggedSourceTargetSas).StartCopyFromUriAsync(
                sourceSas,
                new BlobCopyFromUriOptions
                {
                    SourceConditions = new BlobRequestConditions { TagConditions = "\"class\" = 'tagged'" }
                })).ConfigureAwait(false);
        Assert.Equal(403, missingSourceTagPermission.Status);
        Assert.False((await untaggedSourceTarget.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertCopyBearerSourceAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, BlobClient source, byte[] content)
    {
        var bearerTarget = container.GetBlobClient("bearer-source.bin");
        var bearerTargetSas = bearerTarget.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(10));
        var sourceToken = CreateJwt(SavaWebApplicationFactory.AccountKey, "reader-1");
        using (var transport = new HttpClient(application.Server.CreateHandler()))
        using (var request = new HttpRequestMessage(HttpMethod.Put, bearerTargetSas)
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.Uri.AbsoluteUri);
            request.Headers.TryAddWithoutValidation("x-ms-copy-source-authorization", $"Bearer {sourceToken}");
            using var response = await transport.SendAsync(request).ConfigureAwait(false);
            Assert.True(response.StatusCode == HttpStatusCode.Accepted,
                $"Expected 202 but received {(int)response.StatusCode}: " +
                await response.Content.ReadAsStringAsync().ConfigureAwait(false));
        }
        for (var attempt = 0; attempt < 60; attempt++)
        {
            if ((await bearerTarget.GetPropertiesAsync().ConfigureAwait(false)).Value.CopyStatus == CopyStatus.Success)
                break;
            await Task.Delay(50).ConfigureAwait(false);
        }
        Assert.Equal(content, (await bearerTarget.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertCopyUnsignedAndWrongResourceSourcesAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, BlobClient source)
    {
        var unsignedTarget = container.GetBlobClient("unsigned-source.bin");
        var unsignedTargetSas = unsignedTarget.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var unsignedSource = await Assert.ThrowsAsync<RequestFailedException>(() =>
            CreateBlobClient(application, unsignedTargetSas).StartCopyFromUriAsync(source.Uri)).ConfigureAwait(false);
        Assert.Equal(403, unsignedSource.Status);
        Assert.False((await unsignedTarget.ExistsAsync().ConfigureAwait(false)).Value);

        var other = container.GetBlobClient("other.bin");
        await other.UploadAsync(BinaryData.FromString("other")).ConfigureAwait(false);
        var otherSas = other.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(10));
        var wrongResourceSource = new UriBuilder(source.Uri) { Query = otherSas.Query.TrimStart('?') }.Uri;
        var wrongResourceTarget = container.GetBlobClient("wrong-resource.bin");
        var wrongResourceTargetSas = wrongResourceTarget.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var wrongResource = await Assert.ThrowsAsync<RequestFailedException>(() =>
            CreateBlobClient(application, wrongResourceTargetSas).StartCopyFromUriAsync(wrongResourceSource))
            .ConfigureAwait(false);
        Assert.Equal(403, wrongResource.Status);
        Assert.False((await wrongResourceTarget.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertCopySourceLeaseHeaderRejectedAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, Uri sourceSas)
    {
        var sourceLeaseTarget = container.GetBlobClient("source-lease-header.bin");
        var sourceLeaseTargetSas = sourceLeaseTarget.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(10));
        using var sourceLeaseTransport = new HttpClient(application.Server.CreateHandler());
        using var sourceLeaseRequest = new HttpRequestMessage(HttpMethod.Put, sourceLeaseTargetSas)
        {
            Content = new ByteArrayContent([])
        };
        sourceLeaseRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        sourceLeaseRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", sourceSas.AbsoluteUri);
        sourceLeaseRequest.Headers.TryAddWithoutValidation("x-ms-source-lease-id", Guid.NewGuid().ToString());
        using var sourceLeaseResponse = await sourceLeaseTransport.SendAsync(sourceLeaseRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.BadRequest, sourceLeaseResponse.StatusCode);
        Assert.Equal("UnsupportedHeader", sourceLeaseResponse.Headers.GetValues("x-ms-error-code").Single());
        Assert.False((await sourceLeaseTarget.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task BlobBatchesExecuteIndependentDeleteAndTierSubrequests()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"batch-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await AssertBatchDeleteResponsesAsync(service, container);
        await AssertBatchTierResponsesAsync(container);
    }

    private static async Task AssertBatchDeleteResponsesAsync(
        BlobServiceClient service, BlobContainerClient container)
    {
        var deleteTarget = container.GetBlobClient("nested//delete.bin");
        await deleteTarget.UploadAsync(BinaryData.FromString("delete me")).ConfigureAwait(false);

        var serviceBatchClient = service.GetBlobBatchClient();
        using (var deleteBatch = serviceBatchClient.CreateBatch())
        {
            var deleted = deleteBatch.DeleteBlob(container.Name, deleteTarget.Name);
            var missing = deleteBatch.DeleteBlob(container.Name, "missing.bin");
            var submitted = await serviceBatchClient.SubmitBatchAsync(
                deleteBatch, throwOnAnyFailure: false).ConfigureAwait(false);

            Assert.Equal(202, submitted.Status);
            Assert.Equal(202, deleted.Status);
            Assert.Equal(404, missing.Status);
        }
        Assert.False((await deleteTarget.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertBatchTierResponsesAsync(BlobContainerClient container)
    {
        var tierTarget = container.GetBlobClient("nested//tier.bin");
        await tierTarget.UploadAsync(BinaryData.FromString("tier me")).ConfigureAwait(false);
        var tierLeaseId = Guid.NewGuid().ToString();
        var tierLease = tierTarget.GetBlobLeaseClient(tierLeaseId);
        await tierLease.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration).ConfigureAwait(false);
        var rejectedDirectTier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            tierTarget.SetAccessTierAsync(
                AccessTier.Cool,
                new BlobRequestConditions { LeaseId = Guid.NewGuid().ToString() })).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status412PreconditionFailed, rejectedDirectTier.Status);
        Assert.Equal("LeaseIdMismatchWithBlobOperation", rejectedDirectTier.ErrorCode);
        Assert.Equal(AccessTier.Hot, (await tierTarget.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);
        await tierTarget.SetAccessTierAsync(AccessTier.Cool).ConfigureAwait(false);

        var containerBatchClient = container.GetBlobBatchClient();
        using (var tierBatch = containerBatchClient.CreateBatch())
        {
            var changed = tierBatch.SetBlobAccessTier(container.Name, tierTarget.Name, AccessTier.Cool);
            var missing = tierBatch.SetBlobAccessTier(
                container.Name,
                "missing-tier.bin",
                AccessTier.Hot);
            var submitted = await containerBatchClient.SubmitBatchAsync(
                tierBatch, throwOnAnyFailure: false).ConfigureAwait(false);

            Assert.Equal(202, submitted.Status);
            Assert.Equal(200, changed.Status);
            Assert.Equal(404, missing.Status);
        }
        Assert.Equal(AccessTier.Cool, (await tierTarget.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);

        using (var conditionalBatch = containerBatchClient.CreateBatch())
        {
            var rejected = conditionalBatch.SetBlobAccessTier(
                container.Name,
                tierTarget.Name,
                AccessTier.Hot,
                null,
                new BlobRequestConditions { LeaseId = Guid.NewGuid().ToString() });
            var accepted = conditionalBatch.SetBlobAccessTier(
                container.Name,
                tierTarget.Name,
                AccessTier.Hot,
                null,
                new BlobRequestConditions { LeaseId = tierLeaseId });
            var submitted = await containerBatchClient.SubmitBatchAsync(
                conditionalBatch,
                throwOnAnyFailure: false).ConfigureAwait(false);

            Assert.Equal(StatusCodes.Status202Accepted, submitted.Status);
            Assert.Equal(StatusCodes.Status412PreconditionFailed, rejected.Status);
            Assert.Equal(StatusCodes.Status200OK, accepted.Status);
        }
        Assert.Equal(AccessTier.Hot, (await tierTarget.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);
        await tierLease.ReleaseAsync().ConfigureAwait(false);
    }

    [Fact]
    public async Task CustomerProvidedKeysAndEncryptionScopesAreEnforcedAndIsolated()
    {
        var normalService = CreateClient(factory);
        var containerName = $"encryption-{Guid.NewGuid():N}";
        await normalService.GetBlobContainerClient(containerName).CreateAsync();
        var content = RandomNumberGenerator.GetBytes(64 * 1024);
        var key = RandomNumberGenerator.GetBytes(32);
        var wrongKey = RandomNumberGenerator.GetBytes(32);
        var expectedHash = Convert.ToBase64String(SHA256.HashData(key));
        var chunksBefore = Directory.GetFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Length;

        var keyedService = CreateEncryptedClient(factory, new CustomerProvidedKey(key), encryptionScope: null);
        var keyedBlob = keyedService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin");
        await keyedBlob.UploadAsync(BinaryData.FromBytes(content), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["private"] = "metadata" }
        });
        var chunksAfterKeyed = Directory.GetFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Length;
        var keyedProperties = (await keyedBlob.GetPropertiesAsync()).Value;
        Assert.Equal(expectedHash, keyedProperties.EncryptionKeySha256);
        Assert.Equal(content, (await keyedBlob.DownloadContentAsync()).Value.Content.ToArray());
        await AssertCustomerKeyHistoricalTierRejectsAsync(factory, containerName, keyedBlob);
        await AssertCustomerKeyCurrentTierAllowsAsync(normalService, containerName, keyedBlob);

        await AssertCustomerKeyDenialsAsync(factory, normalService, containerName, wrongKey);

        const string scope = "records-scope";
        var scopedService = CreateEncryptedClient(factory, customerProvidedKey: null, scope);
        var chunksAfterScoped = await AssertEncryptionScopeMetadataAsync(
            factory, normalService, scopedService, containerName, content, scope);

        await AssertEncryptionScopeSpecializedOperationsAsync(normalService, scopedService, containerName);
        await AssertEncryptionListingAndPhysicalIsolationAsync(
            factory, normalService, containerName, expectedHash, scope,
            chunksBefore, chunksAfterKeyed, chunksAfterScoped, key);
    }

    private static async Task AssertCustomerKeyHistoricalTierRejectsAsync(
        SavaWebApplicationFactory application, string containerName, BlobClient keyedBlob)
    {
        using (var transport = new HttpClient(application.Server.CreateHandler()))
        using (var oldTierRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(keyedBlob.GenerateSasUri(
                           BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)), "comp=tier"))
        {
            Content = new ByteArrayContent([])
        })
        {
            oldTierRequest.Headers.TryAddWithoutValidation("x-ms-version", "2021-12-02");
            oldTierRequest.Headers.TryAddWithoutValidation("x-ms-access-tier", "Cool");
            using var oldTierResponse = await transport.SendAsync(oldTierRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldTierResponse.StatusCode);
            Assert.Equal("BlobUsesCustomerSpecifiedEncryption",
                oldTierResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.Equal(AccessTier.Hot, (await keyedBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);

        var oldEndpoint = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost");
        var oldOptions = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2021_12_02)
        {
            Transport = new HttpClientTransport(application.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        };
        var oldService = new BlobServiceClient(oldEndpoint,
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            oldOptions);
        var oldBatchClient = oldService.GetBlobBatchClient();
        using (var oldBatch = oldBatchClient.CreateBatch())
        {
            var rejectedTier = oldBatch.SetBlobAccessTier(containerName, keyedBlob.Name, AccessTier.Cool);
            var batchResponse = await oldBatchClient.SubmitBatchAsync(oldBatch, throwOnAnyFailure: false)
                .ConfigureAwait(false);
            Assert.Equal(StatusCodes.Status202Accepted, batchResponse.Status);
            Assert.Equal(StatusCodes.Status409Conflict, rejectedTier.Status);
            Assert.True(rejectedTier.Headers.TryGetValue("x-ms-error-code", out var errorCode));
            Assert.Equal("BlobUsesCustomerSpecifiedEncryption", errorCode);
        }
        Assert.Equal(AccessTier.Hot, (await keyedBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);
    }

    private static async Task AssertCustomerKeyCurrentTierAllowsAsync(
        BlobServiceClient normalService, string containerName, BlobClient keyedBlob)
    {
        var keyedTier = await keyedBlob.SetAccessTierAsync(AccessTier.Cool).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status200OK, keyedTier.Status);
        Assert.Equal(AccessTier.Cool, (await keyedBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);

        var batchClient = normalService.GetBlobBatchClient();
        using (var batch = batchClient.CreateBatch())
        {
            var tiered = batch.SetBlobAccessTier(containerName, keyedBlob.Name, AccessTier.Hot);
            var batchResponse = await batchClient.SubmitBatchAsync(batch, throwOnAnyFailure: false)
                .ConfigureAwait(false);
            Assert.Equal(StatusCodes.Status202Accepted, batchResponse.Status);
            Assert.Equal(StatusCodes.Status200OK, tiered.Status);
        }
        Assert.Equal(AccessTier.Hot, (await keyedBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.AccessTier);
    }

    private static async Task AssertCustomerKeyDenialsAsync(
        SavaWebApplicationFactory application, BlobServiceClient normalService,
        string containerName, byte[] wrongKey)
    {
        var missingKey = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin").GetPropertiesAsync())
            .ConfigureAwait(false);
        Assert.Equal(409, missingKey.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingKey.ErrorCode);
        var wrongKeyService = CreateEncryptedClient(application,
            new CustomerProvidedKey(wrongKey), encryptionScope: null);
        var mismatchedKey = await Assert.ThrowsAsync<RequestFailedException>(() =>
            wrongKeyService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin").DownloadContentAsync())
            .ConfigureAwait(false);
        Assert.Equal(409, mismatchedKey.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", mismatchedKey.ErrorCode);
    }

    private static async Task<int> AssertEncryptionScopeMetadataAsync(
        SavaWebApplicationFactory application, BlobServiceClient normalService,
        BlobServiceClient scopedService, string containerName, byte[] content, string scope)
    {
        var scopedBlob = scopedService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin");
        await scopedBlob.UploadAsync(BinaryData.FromBytes(content)).ConfigureAwait(false);
        var chunksAfterScoped = Directory.GetFiles(Path.Combine(application.DataPath, "chunks"),
            "*.chunk", SearchOption.AllDirectories).Length;
        var scopedProperties = (await scopedBlob.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(scope, scopedProperties.EncryptionScope);
        Assert.Equal(content, (await normalService.GetBlobContainerClient(containerName)
            .GetBlobClient("scoped.bin").DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var missingScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin")
                .SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = "missing" }))
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, missingScope.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingScope.ErrorCode);
        var wrongScopeService = CreateEncryptedClient(application, customerProvidedKey: null, "wrong-scope");
        var wrongScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            wrongScopeService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin")
                .SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = "wrong" }))
            .ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, wrongScope.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", wrongScope.ErrorCode);
        await scopedBlob.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = "matched" }).ConfigureAwait(false);
        return chunksAfterScoped;
    }

    private static async Task AssertEncryptionScopeSpecializedOperationsAsync(
        BlobServiceClient normalService, BlobServiceClient scopedService, string containerName)
    {
        var scopedAppend = scopedService.GetBlobContainerClient(containerName).GetAppendBlobClient("scoped-append.bin");
        await scopedAppend.CreateAsync().ConfigureAwait(false);
        var missingAppendScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName).GetAppendBlobClient(scopedAppend.Name)
                .AppendBlockAsync(BinaryData.FromString("must fail").ToStream())).ConfigureAwait(false);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingAppendScope.ErrorCode);
        await scopedAppend.AppendBlockAsync(BinaryData.FromString("matched append").ToStream()).ConfigureAwait(false);

        var scopedPage = scopedService.GetBlobContainerClient(containerName).GetPageBlobClient("scoped-page.bin");
        await scopedPage.CreateAsync(512).ConfigureAwait(false);
        var missingPageScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName).GetPageBlobClient(scopedPage.Name)
                .UploadPagesAsync(new MemoryStream(new byte[512]), 0)).ConfigureAwait(false);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingPageScope.ErrorCode);
        await scopedPage.UploadPagesAsync(new MemoryStream(new byte[512]), 0).ConfigureAwait(false);

        var scopedBlock = scopedService.GetBlobContainerClient(containerName).GetBlockBlobClient("scoped-block.bin");
        await scopedBlock.UploadAsync(BinaryData.FromString("initial block").ToStream()).ConfigureAwait(false);
        var missingBlockScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName).GetBlockBlobClient(scopedBlock.Name)
                .StageBlockAsync(Convert.ToBase64String("scope-block"u8),
                    BinaryData.FromString("must fail").ToStream())).ConfigureAwait(false);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingBlockScope.ErrorCode);
    }

    private static async Task AssertEncryptionListingAndPhysicalIsolationAsync(
        SavaWebApplicationFactory application, BlobServiceClient normalService, string containerName,
        string expectedHash, string scope, int chunksBefore, int chunksAfterKeyed, int chunksAfterScoped, byte[] key)
    {
        var listed = new List<BlobItem>();
        await foreach (var item in normalService.GetBlobContainerClient(containerName).GetBlobsAsync(
                           new GetBlobsOptions { Traits = BlobTraits.Metadata }).ConfigureAwait(false))
            listed.Add(item);
        var listedKeyed = Assert.Single(listed, item => string.Equals(item.Name, "keyed.bin", StringComparison.Ordinal));
        Assert.Equal(expectedHash, listedKeyed.Properties.CustomerProvidedKeySha256);
        Assert.Empty(listedKeyed.Metadata);
        Assert.Equal(scope, Assert.Single(listed,
            item => string.Equals(item.Name, "scoped.bin", StringComparison.Ordinal)).Properties.EncryptionScope);
        Assert.True(chunksAfterKeyed > chunksBefore);
        Assert.True(chunksAfterScoped > chunksAfterKeyed);
        var metadataBytes = await File.ReadAllBytesAsync(Path.Combine(application.DataPath, "metadata.db"))
            .ConfigureAwait(false);
        Assert.DoesNotContain(Convert.ToBase64String(key), Encoding.Latin1.GetString(metadataBytes),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ContainerEncryptionScopePoliciesDefaultWritesAndRejectOverrides()
    {
        const string defaultScope = "tenant-default-scope";
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"scope-policy-{Guid.NewGuid():N}");
        await container.CreateAsync(
            PublicAccessType.None,
            metadata: null,
            new BlobContainerEncryptionScopeOptions
            {
                DefaultEncryptionScope = defaultScope,
                PreventEncryptionScopeOverride = true
            });
        await AssertContainerScopePolicyPropertiesAsync(service, container, defaultScope);

        await AssertDefaultScopeBlobAndTierRulesAsync(container, defaultScope);

        await AssertDefaultScopeSpecializedWritesAsync(factory, container, defaultScope);

        var otherScopeService = CreateEncryptedClient(factory, customerProvidedKey: null, "other-scope");
        await AssertContainerScopeOverrideRulesAsync(service, container, otherScopeService, defaultScope);

        await AssertContainerScopeInvalidCreateRequestsAsync(factory, service, defaultScope);
    }

    private static async Task AssertContainerScopePolicyPropertiesAsync(
        BlobServiceClient service, BlobContainerClient container, string defaultScope)
    {
        var containerProperties = (await container.GetPropertiesAsync().ConfigureAwait(false)).Value;
        Assert.Equal(defaultScope, containerProperties.DefaultEncryptionScope);
        Assert.True(containerProperties.PreventEncryptionScopeOverride);

        BlobContainerItem? listedContainer = null;
        await foreach (var item in service.GetBlobContainersAsync(prefix: container.Name).ConfigureAwait(false))
        {
            if (string.Equals(item.Name, container.Name, StringComparison.Ordinal))
                listedContainer = item;
        }
        Assert.NotNull(listedContainer);
        Assert.Equal(defaultScope, listedContainer.Properties.DefaultEncryptionScope);
        Assert.True(listedContainer.Properties.PreventEncryptionScopeOverride);
    }

    private static async Task AssertDefaultScopeBlobAndTierRulesAsync(
        BlobContainerClient container, string defaultScope)
    {
        var defaultBlob = container.GetBlobClient("default.bin");
        await defaultBlob.UploadAsync(BinaryData.FromString("default encryption scope")).ConfigureAwait(false);
        Assert.Equal(defaultScope, (await defaultBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.EncryptionScope);
        await defaultBlob.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["scope"] = "container-default" })
            .ConfigureAwait(false);
        var defaultSnapshot = await defaultBlob.CreateSnapshotAsync().ConfigureAwait(false);
        Assert.Equal(defaultScope,
            (await defaultBlob.WithSnapshot(defaultSnapshot.Value.Snapshot).GetPropertiesAsync().ConfigureAwait(false))
            .Value.EncryptionScope);
        var tierChange = await Assert.ThrowsAsync<RequestFailedException>(() =>
            defaultBlob.SetAccessTierAsync(AccessTier.Cool)).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, tierChange.Status);
        Assert.Equal("BlobOperationNotSupported", tierChange.ErrorCode);
        var explicitTier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("explicit-tier.bin").UploadAsync(
                BinaryData.FromString("scope with explicit tier"),
                new BlobUploadOptions { AccessTier = AccessTier.Cool })).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status409Conflict, explicitTier.Status);
        Assert.Equal("BlobOperationNotSupported", explicitTier.ErrorCode);
    }

    private static async Task AssertDefaultScopeSpecializedWritesAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, string defaultScope)
    {
        var block = container.GetBlockBlobClient("staged.bin");
        var blockId = Convert.ToBase64String("block-0001"u8);
        var staged = await block.StageBlockAsync(blockId, BinaryData.FromString("staged scope").ToStream())
            .ConfigureAwait(false);
        Assert.Equal(defaultScope, staged.Value.EncryptionScope);
        await block.CommitBlockListAsync([blockId]).ConfigureAwait(false);
        Assert.Equal(defaultScope, (await block.GetPropertiesAsync().ConfigureAwait(false)).Value.EncryptionScope);

        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync().ConfigureAwait(false);
        await append.AppendBlockAsync(BinaryData.FromString("append scope").ToStream()).ConfigureAwait(false);
        Assert.Equal(defaultScope, (await append.GetPropertiesAsync().ConfigureAwait(false)).Value.EncryptionScope);

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(512).ConfigureAwait(false);
        await page.UploadPagesAsync(new MemoryStream(new byte[512]), 0).ConfigureAwait(false);
        Assert.Equal(defaultScope, (await page.GetPropertiesAsync().ConfigureAwait(false)).Value.EncryptionScope);

        var scopedService = CreateEncryptedClient(application, customerProvidedKey: null, defaultScope);
        var matchingBlob = scopedService.GetBlobContainerClient(container.Name).GetBlobClient("matching.bin");
        await matchingBlob.UploadAsync(BinaryData.FromString("matching encryption scope")).ConfigureAwait(false);
        Assert.Equal(defaultScope, (await matchingBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.EncryptionScope);
    }

    private static async Task AssertContainerScopeOverrideRulesAsync(
        BlobServiceClient service, BlobContainerClient container,
        BlobServiceClient otherScopeService, string defaultScope)
    {
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            otherScopeService.GetBlobContainerClient(container.Name).GetBlobClient("rejected.bin")
                .UploadAsync(BinaryData.FromString("rejected encryption scope"))).ConfigureAwait(false);
        Assert.Equal(StatusCodes.Status403Forbidden, rejected.Status);
        Assert.Equal("RequestForbiddenByContainerEncryptionPolicy", rejected.ErrorCode);

        var permissive = service.GetBlobContainerClient($"scope-override-{Guid.NewGuid():N}");
        await permissive.CreateAsync(PublicAccessType.None, metadata: null,
            new BlobContainerEncryptionScopeOptions
            {
                DefaultEncryptionScope = defaultScope,
                PreventEncryptionScopeOverride = false
            }).ConfigureAwait(false);
        var overridden = otherScopeService.GetBlobContainerClient(permissive.Name).GetBlobClient("override.bin");
        await overridden.UploadAsync(BinaryData.FromString("overridden encryption scope")).ConfigureAwait(false);
        Assert.Equal("other-scope", (await overridden.GetPropertiesAsync().ConfigureAwait(false)).Value.EncryptionScope);
    }

    private static async Task AssertContainerScopeInvalidCreateRequestsAsync(
        SavaWebApplicationFactory application, BlobServiceClient service, string defaultScope)
    {
        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var accountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(5),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountSas.SetPermissions(AccountSasPermissions.Create | AccountSasPermissions.Write);
        var sas = accountSas.ToSasQueryParameters(credential);
        Uri RawContainerUri(string name) => new(
            $"{service.Uri.AbsoluteUri.TrimEnd('/')}/{name}?restype=container&{sas}");
        using var transport = new HttpClient(application.Server.CreateHandler());

        var oldName = $"scope-old-{Guid.NewGuid():N}";
        using (var oldVersion = new HttpRequestMessage(HttpMethod.Put, RawContainerUri(oldName))
        {
            Content = new ByteArrayContent([])
        })
        {
            oldVersion.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
            oldVersion.Headers.TryAddWithoutValidation("x-ms-default-encryption-scope", defaultScope);
            oldVersion.Headers.TryAddWithoutValidation("x-ms-deny-encryption-scope-override", "true");
            using var response = await transport.SendAsync(oldVersion).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var incompleteName = $"scope-incomplete-{Guid.NewGuid():N}";
        using (var incomplete = new HttpRequestMessage(HttpMethod.Put, RawContainerUri(incompleteName))
        {
            Content = new ByteArrayContent([])
        })
        {
            incomplete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            incomplete.Headers.TryAddWithoutValidation("x-ms-default-encryption-scope", defaultScope);
            using var response = await transport.SendAsync(incomplete).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var bodyName = $"scope-body-{Guid.NewGuid():N}";
        using (var body = new HttpRequestMessage(HttpMethod.Put, RawContainerUri(bodyName))
        {
            Content = new ByteArrayContent([1])
        })
        {
            body.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            body.Headers.TryAddWithoutValidation("x-ms-default-encryption-scope", defaultScope);
            body.Headers.TryAddWithoutValidation("x-ms-deny-encryption-scope-override", "true");
            using var response = await transport.SendAsync(body).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    [Fact]
    public async Task CustomerProvidedKeysFlowThroughSpecializedBlobOperations()
    {
        var key = RandomNumberGenerator.GetBytes(32);
        var service = CreateEncryptedClient(factory, new CustomerProvidedKey(key), encryptionScope: null);
        var container = service.GetBlobContainerClient($"cpk-specialized-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var block = container.GetBlockBlobClient("blocks.bin");
        var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("block-0001"));
        var blockBytes = RandomNumberGenerator.GetBytes(12 * 1024);
        await block.StageBlockAsync(blockId, new MemoryStream(blockBytes));
        await block.CommitBlockListAsync([blockId]);
        await block.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["protected"] = "true" });
        var snapshot = await block.CreateSnapshotAsync();
        Assert.Equal(blockBytes, (await block.WithSnapshot(snapshot.Value.Snapshot).DownloadContentAsync()).Value.Content.ToArray());

        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync();
        var appendBytes = RandomNumberGenerator.GetBytes(4096);
        await append.AppendBlockAsync(new MemoryStream(appendBytes));
        Assert.Equal(appendBytes, (await append.DownloadContentAsync()).Value.Content.ToArray());

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(1024);
        var pageBytes = RandomNumberGenerator.GetBytes(1024);
        await page.UploadPagesAsync(new MemoryStream(pageBytes), offset: 0);
        var downloaded = (await page.DownloadContentAsync()).Value.Content.ToArray();
        Assert.Equal(pageBytes, downloaded);
        await page.ResizeAsync(512);
        Assert.Equal(512, (await page.GetPropertiesAsync()).Value.ContentLength);
        Assert.Equal(pageBytes[..512], (await page.DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task MaintenanceExpiresUncommittedBlocksAndNeverReclaimsPinnedContent()
    {
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        await using var applicationDisposal44 = application.ConfigureAwait(false);
        var client = CreateClient(application);
        var containerName = $"maintenance-{Guid.NewGuid():N}";
        var container = client.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blobService = application.Services.GetRequiredService<BlobService>();
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var chunkStore = application.Services.GetRequiredService<ChunkStore>();

        await AssertPinnedChunksSurviveMaintenanceAsync(
            container, containerName, blobService, chunkStore).ConfigureAwait(true);
        await AssertBlobExpiryMaintenanceAsync(container, containerName, blobService).ConfigureAwait(true);
        await AssertStaleBlocksExpireAsync(
            application, container, containerName, blobService, metadata).ConfigureAwait(true);
    }

    private static async Task AssertPinnedChunksSurviveMaintenanceAsync(
        BlobContainerClient container, string containerName, BlobService blobService, ChunkStore chunkStore)
    {
        var protectedBlob = container.GetBlobClient("active-reader.bin");
        var protectedBytes = RandomNumberGenerator.GetBytes(48 * 1024);
        await protectedBlob.UploadAsync(BinaryData.FromBytes(protectedBytes)).ConfigureAwait(false);
        var protectedRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            protectedBlob.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false);
        var protectedChunkIds = protectedRecord.Content.Chunks
            .Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal))
            .Select(chunk => chunk.Id)
            .ToArray();

        using (chunkStore.Pin(protectedRecord.Content))
        {
            await protectedBlob.DeleteAsync().ConfigureAwait(false);
            await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
            foreach (var chunkId in protectedChunkIds)
                Assert.Equal(ChunkIntegrityStatus.Verified,
                    await chunkStore.VerifyChunkAsync(chunkId, CancellationToken.None).ConfigureAwait(false));
        }

        await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        foreach (var chunkId in protectedChunkIds)
            Assert.Equal(ChunkIntegrityStatus.Missing,
                await chunkStore.VerifyChunkAsync(chunkId, CancellationToken.None).ConfigureAwait(false));
    }

    private static async Task AssertBlobExpiryMaintenanceAsync(
        BlobContainerClient container, string containerName, BlobService blobService)
    {
        var expiring = container.GetBlobClient("expiring.bin");
        await expiring.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(20 * 1024))).ConfigureAwait(false);
        var expiringRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            expiring.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None).ConfigureAwait(false);
        await blobService.SetExpiryAsync(
            expiringRecord,
            DateTimeOffset.UtcNow.AddMilliseconds(100),
            CancellationToken.None).ConfigureAwait(false);
        await Task.Delay(150).ConfigureAwait(false);
        await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.False((await expiring.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertStaleBlocksExpireAsync(
        SavaWebApplicationFactory application, BlobContainerClient container, string containerName,
        BlobService blobService, MetadataStore metadata)
    {
        var uncommitted = container.GetBlockBlobClient("uncommitted.bin");
        var blockId = Convert.ToBase64String("stale-block-0001"u8);
        await uncommitted.StageBlockAsync(blockId, new MemoryStream(RandomNumberGenerator.GetBytes(32 * 1024))).ConfigureAwait(false);
        var staged = Assert.Single(await blobService.ListStagedBlocksAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            uncommitted.Name,
            CancellationToken.None).ConfigureAwait(false));
        await metadata.PutStagedBlockAsync(
            staged with { CreatedAt = DateTimeOffset.UtcNow.AddDays(-8) },
            CancellationToken.None).ConfigureAwait(false);

        await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Empty(await blobService.ListStagedBlocksAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            uncommitted.Name,
            CancellationToken.None).ConfigureAwait(false));
        Assert.All(
            staged.Content.Chunks.Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal)),
            chunk => Assert.False(File.Exists(ChunkPath(application.DataPath, chunk.Id))));
    }

    [Fact]
    public async Task MaintenanceReclaimsAbandonedStagingAndSurfacesChunkCorruption()
    {
        var blobService = factory.Services.GetRequiredService<BlobService>();
        var freshPath = await AssertAbandonedStagingCleanupAsync(factory, blobService);

        var service = CreateClient(factory);
        var containerName = $"integrity-{Guid.NewGuid():N}";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var (blob, chunkPath) = await CorruptBlobChunkAsync(factory, blobService, container, containerName);

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        using var operatorClient = factory.CreateClient();
        await AssertCorruptChunkStatusAndRecoveryAsync(blobService, blob, chunkPath, operatorClient);
        await AssertMissingChunkStatusAndRecoveryAsync(factory, blobService, container, containerName, operatorClient);

        File.Delete(freshPath);
    }

    private static async Task<string> AssertAbandonedStagingCleanupAsync(
        SavaWebApplicationFactory application, BlobService blobService)
    {
        var staging = Path.Combine(application.DataPath, "staging");
        var abandonedPath = Path.Combine(staging, $"abandoned-{Guid.NewGuid():N}.tmp");
        var activePath = Path.Combine(staging, $"active-{Guid.NewGuid():N}.tmp");
        var freshPath = Path.Combine(staging, $"fresh-{Guid.NewGuid():N}.tmp");
        await File.WriteAllBytesAsync(abandonedPath, "abandoned"u8.ToArray()).ConfigureAwait(false);
        await File.WriteAllBytesAsync(freshPath, "fresh"u8.ToArray()).ConfigureAwait(false);
        File.SetLastWriteTimeUtc(abandonedPath, DateTime.UtcNow.AddDays(-2));

        var active = new FileStream(activePath, FileMode.CreateNew, FileAccess.ReadWrite,
            FileShare.None, bufferSize: 4096, FileOptions.Asynchronous);
        await using (active.ConfigureAwait(false))
        {
            await active.WriteAsync("active"u8.ToArray()).ConfigureAwait(false);
            await active.FlushAsync().ConfigureAwait(false);
            File.SetLastWriteTimeUtc(activePath, DateTime.UtcNow.AddDays(-2));

            await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.False(File.Exists(abandonedPath));
            Assert.True(File.Exists(activePath));
            Assert.True(File.Exists(freshPath));
        }

        await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.False(File.Exists(activePath));
        Assert.True(File.Exists(freshPath));
        return freshPath;
    }

    private static async Task<(BlobClient Blob, string ChunkPath)> CorruptBlobChunkAsync(
        SavaWebApplicationFactory application, BlobService blobService,
        BlobContainerClient container, string containerName)
    {
        var blob = container.GetBlobClient("corrupt.bin");
        await blob.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(48 * 1024)))
            .ConfigureAwait(false);
        var record = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, containerName, blob.Name,
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        var chunk = record.Content.Chunks.First(item => !item.Id.EndsWith("/$zero", StringComparison.Ordinal));
        var chunkPath = ChunkPath(application.DataPath, chunk.Id);
        var file = new FileStream(chunkPath, FileMode.Open, FileAccess.ReadWrite,
            FileShare.None, bufferSize: 4096, FileOptions.Asynchronous);
        await using (file.ConfigureAwait(false))
        {
            file.Position = file.Length - 1;
            var value = file.ReadByte();
            Assert.NotEqual(-1, value);
            file.Position--;
            file.WriteByte((byte)(value ^ 0xff));
            // Durability tests require a media flush; FlushAsync cannot request one.
#pragma warning disable CA1849
            file.Flush(flushToDisk: true);
#pragma warning restore CA1849
        }
        return (blob, chunkPath);
    }

    private static async Task AssertCorruptChunkStatusAndRecoveryAsync(
        BlobService blobService, BlobClient blob, string chunkPath, HttpClient operatorClient)
    {
        using var unavailable = await operatorClient.GetAsync(new Uri("/health/ready", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode);
        var metrics = await operatorClient.GetStringAsync(new Uri("/metrics", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(false);
        Assert.Contains("mk8_sava_integrity_corrupt_chunks 1", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_storage_physical_chunk_bytes", metrics, StringComparison.Ordinal);
        Assert.Contains($"mk8_sava_storage_allocation_available {(OperatingSystem.IsLinux() ? 1 : 0)}",
            metrics, StringComparison.Ordinal);
        if (OperatingSystem.IsLinux())
            Assert.Contains("mk8_sava_storage_allocated_root_bytes", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_http_request_duration_seconds_sum", metrics, StringComparison.Ordinal);

        await blob.DeleteAsync().ConfigureAwait(false);
        await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.False(File.Exists(chunkPath));
        using var recovered = await operatorClient.GetAsync(new Uri("/health/ready", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, recovered.StatusCode);
    }

    private static async Task AssertMissingChunkStatusAndRecoveryAsync(
        SavaWebApplicationFactory application, BlobService blobService,
        BlobContainerClient container, string containerName, HttpClient operatorClient)
    {
        var missing = container.GetBlobClient("missing.bin");
        await missing.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(40 * 1024)))
            .ConfigureAwait(false);
        var missingRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, containerName, missing.Name,
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        var missingChunk = missingRecord.Content.Chunks.First(item =>
            !item.Id.EndsWith("/$zero", StringComparison.Ordinal));
        File.Delete(ChunkPath(application.DataPath, missingChunk.Id));
        await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        var missingMetrics = await operatorClient.GetStringAsync(new Uri("/metrics", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(false);
        Assert.Contains("mk8_sava_integrity_missing_chunks 1", missingMetrics, StringComparison.Ordinal);
        using var unavailable = await operatorClient.GetAsync(new Uri("/health/ready", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode);

        await missing.DeleteAsync().ConfigureAwait(false);
        await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        using var recovered = await operatorClient.GetAsync(new Uri("/health/ready", UriKind.RelativeOrAbsolute))
            .ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.OK, recovered.StatusCode);
    }

    [Fact]
    public async Task ChunkMaintenanceUsesIndependentBoundedKeysetPasses()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:GarbageCollectionChunksPerMaintenancePass"] = "1",
            ["Sava:IntegrityScanChunksPerMaintenancePass"] = "1",
            ["Sava:BackgroundCompressionChunksPerMaintenancePass"] = "1",
            ["Sava:EnableSmallChunkPacking"] = "false"
        });
        try
        {
            await application.InitializeAsync();
            var blobService = application.Services.GetRequiredService<BlobService>();
            var telemetry = application.Services.GetRequiredService<StorageTelemetry>();

            // Synchronize with the hosted service's initial empty pass before creating content.
            await blobService.RunMaintenanceAsync(CancellationToken.None);

            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"bounded-maintenance-{Guid.NewGuid():N}");
            await container.CreateAsync();
            var blobs = Enumerable.Range(0, 3)
                .Select(index => container.GetBlobClient($"chunk-{index}.bin"))
                .ToArray();
            var payloads = Enumerable.Range(0, blobs.Length)
                .Select(_ => RandomNumberGenerator.GetBytes(2048))
                .ToArray();
            for (var index = 0; index < blobs.Length; index++)
                await blobs[index].UploadAsync(BinaryData.FromBytes(payloads[index]));

            Assert.Equal(3, EnumerateChunkFiles(application.DataPath).Count());
            for (var expectedChecked = 1; expectedChecked <= 3; expectedChecked++)
            {
                var result = await blobService.RunMaintenanceAsync(CancellationToken.None);
                var integrity = telemetry.Integrity;
                Assert.Equal(3, integrity.ReachableChunks);
                Assert.Equal(expectedChecked, integrity.CheckedChunks);
                Assert.Equal(expectedChecked, integrity.VerifiedChunks);
                Assert.Equal(expectedChecked == 3, integrity.Complete);
                Assert.InRange(result.RecompressedChunks, 0, 1);
            }

            for (var index = 0; index < blobs.Length; index++)
                Assert.Equal(payloads[index], (await blobs[index].DownloadContentAsync()).Value.Content.ToArray());
            foreach (var blob in blobs)
                await blob.DeleteAsync();

            for (var expectedRemaining = 2; expectedRemaining >= 0; expectedRemaining--)
            {
                var result = await blobService.RunMaintenanceAsync(CancellationToken.None);
                Assert.Equal(1, result.ReclaimedChunks);
                Assert.Equal(expectedRemaining, EnumerateChunkFiles(application.DataPath).Count());
            }
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    // xUnit1030 requires the test continuation to remain on xUnit's synchronization context.
#pragma warning disable MA0004
    [Fact]
    public async Task SmallChunksArePackedDeduplicatedCompactedAndBackupSafeAcrossRestart()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-packed-{Guid.NewGuid():N}");
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-packed-backup-{Guid.NewGuid():N}");
        var configuration = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:SmallChunkPackingThresholdBytes"] = "4096",
            ["Sava:ChunkPackTargetBytes"] = "8192",
            ["Sava:ChunkPackMaximumRecords"] = "2",
            ["Sava:ChunkPackSealAge"] = "00:00:00",
            ["Sava:ChunkPacksPerMaintenancePass"] = "16",
            ["Sava:ChunkPackCompactionMinimumSavingsBytes"] = "1",
            ["Sava:ChunkPackCompactionMinimumDeadRatio"] = "0.01"
        };
        var initial = new SavaWebApplicationFactory(dataPath, configuration, deleteDataPath: false);
        var initialDisposed = false;
        try
        {
            await initial.InitializeAsync().ConfigureAwait(true);
            var scenario = await AssertInitialPackedChunksAsync(initial, dataPath).ConfigureAwait(true);

            await initial.DisposeAsync().ConfigureAwait(true);
            initialDisposed = true;

            var restarted = new SavaWebApplicationFactory(dataPath, configuration, deleteDataPath: false);
            await using var restartedDisposal = restarted.ConfigureAwait(true);
            await restarted.InitializeAsync().ConfigureAwait(true);
            var (second, blobs) = await AssertPackedRestartAndCompactionAsync(restarted, dataPath, scenario)
                .ConfigureAwait(true);

            await AssertOrphanPackRecoveryAsync(dataPath, blobs).ConfigureAwait(true);

            await AssertPortablePackedBackupAsync(restarted, backupPath).ConfigureAwait(true);

            await AssertPackedChunkCorruptionAsync(restarted, dataPath, blobs, scenario.ContainerName, second)
                .ConfigureAwait(true);
        }
        finally
        {
            if (!initialDisposed)
                await initial.DisposeAsync().ConfigureAwait(true);
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
        }
    }
#pragma warning restore MA0004

    private sealed record PackedChunkScenario(
        string ContainerName, string FirstName, string DuplicateName, string SecondName, byte[] SecondBytes);

    private static async Task<PackedChunkScenario> AssertInitialPackedChunksAsync(
        SavaWebApplicationFactory application, string dataPath)
    {
        var service = CreateClient(application);
        var containerName = $"packed-{Guid.NewGuid():N}";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync().ConfigureAwait(false);
        var firstBytes = RandomNumberGenerator.GetBytes(1024);
        var secondBytes = RandomNumberGenerator.GetBytes(1536);
        var first = container.GetBlobClient("first.bin");
        var duplicate = container.GetBlobClient("duplicate.bin");
        var second = container.GetBlobClient("second.bin");
        await first.UploadAsync(BinaryData.FromBytes(firstBytes)).ConfigureAwait(false);
        await duplicate.UploadAsync(BinaryData.FromBytes(firstBytes)).ConfigureAwait(false);
        await second.UploadAsync(BinaryData.FromBytes(secondBytes)).ConfigureAwait(false);

        var metadata = application.Services.GetRequiredService<MetadataStore>();
        Assert.Equal(2, await metadata.CountPackedChunksAsync(CancellationToken.None).ConfigureAwait(false));
        Assert.Empty(EnumerateChunkFiles(dataPath));
        Assert.Single(EnumeratePackFiles(dataPath));
        Assert.Equal(firstBytes,
            (await duplicate.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var range = await second.DownloadContentAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(123, 456)
        }).ConfigureAwait(false);
        Assert.Equal(secondBytes.AsSpan(123, 456).ToArray(), range.Value.Content.ToArray());
        return new PackedChunkScenario(containerName, first.Name, duplicate.Name, second.Name, secondBytes);
    }

    private static async Task<(BlobClient Second, BlobService Blobs)> AssertPackedRestartAndCompactionAsync(
        SavaWebApplicationFactory application, string dataPath, PackedChunkScenario scenario)
    {
        var restartedContainer = CreateClient(application).GetBlobContainerClient(scenario.ContainerName);
        var first = restartedContainer.GetBlobClient(scenario.FirstName);
        var duplicate = restartedContainer.GetBlobClient(scenario.DuplicateName);
        var second = restartedContainer.GetBlobClient(scenario.SecondName);
        Assert.Equal(scenario.SecondBytes,
            (await second.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());

        await first.DeleteAsync().ConfigureAwait(false);
        await duplicate.DeleteAsync().ConfigureAwait(false);
        var blobs = application.Services.GetRequiredService<BlobService>();
        var maintenance = await blobs.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(1, maintenance.ReclaimedChunks);
        Assert.Equal(1, maintenance.CompactedChunkPacks);
        Assert.True(maintenance.PackCompactionBytesSaved > 0);
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        Assert.Equal(1, await metadata.CountPackedChunksAsync(CancellationToken.None).ConfigureAwait(false));
        Assert.Empty(EnumerateChunkFiles(dataPath));
        Assert.Single(EnumeratePackFiles(dataPath));
        Assert.Equal(scenario.SecondBytes,
            (await second.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        return (second, blobs);
    }

    private static async Task AssertOrphanPackRecoveryAsync(string dataPath, BlobService blobs)
    {
        var orphanPath = Path.Combine(dataPath, "packs", SavaWebApplicationFactory.AccountName,
            $"orphan-{Guid.NewGuid():N}.pack");
        await File.WriteAllBytesAsync(orphanPath, RandomNumberGenerator.GetBytes(257)).ConfigureAwait(false);
        File.SetLastWriteTimeUtc(orphanPath, DateTime.UtcNow.AddMinutes(-1));
        var recovery = await blobs.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(1, recovery.CompactedChunkPacks);
        Assert.True(recovery.PackCompactionBytesSaved >= 257);
        Assert.False(File.Exists(orphanPath));
    }

    private static async Task AssertPortablePackedBackupAsync(
        SavaWebApplicationFactory application, string backupPath)
    {
        var backup = application.Services.GetRequiredService<StorageBackupService>();
        var created = await backup.CreateAsync(backupPath, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(1, created.ChunkCount);
        Assert.Equal(created, await backup.ValidateAsync(backupPath, CancellationToken.None).ConfigureAwait(false));
        Assert.Single(EnumerateChunkFiles(backupPath));
        Assert.False(Directory.Exists(Path.Combine(backupPath, "packs")));
        var connection = new SqliteConnection($"Data Source={Path.Combine(backupPath, "metadata.db")}");
        await using (connection.ConfigureAwait(false))
        {
            await connection.OpenAsync().ConfigureAwait(false);
            var command = connection.CreateCommand();
            await using (command.ConfigureAwait(false))
            {
                command.CommandText =
                    "SELECT (SELECT COUNT(*) FROM packed_chunks) + (SELECT COUNT(*) FROM chunk_packs);";
                Assert.Equal(0L,
                    Convert.ToInt64(await command.ExecuteScalarAsync().ConfigureAwait(false), CultureInfo.InvariantCulture));
            }
        }
    }

    private static async Task AssertPackedChunkCorruptionAsync(
        SavaWebApplicationFactory application, string dataPath, BlobService blobs,
        string containerName, BlobClient second)
    {
        var packPath = Assert.Single(EnumeratePackFiles(dataPath)).FullName;
        var corrupt = new FileStream(packPath, FileMode.Open, FileAccess.ReadWrite,
            FileShare.None, 4096, FileOptions.Asynchronous);
        await using (corrupt.ConfigureAwait(false))
        {
            var value = corrupt.ReadByte();
            Assert.NotEqual(-1, value);
            corrupt.Position = 0;
            corrupt.WriteByte((byte)(value ^ 0xff));
            // Durability tests require a media flush; FlushAsync cannot request one.
#pragma warning disable CA1849
            corrupt.Flush(flushToDisk: true);
#pragma warning restore CA1849
        }
        var record = await blobs.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, containerName, second.Name,
            versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None).ConfigureAwait(false);
        var chunkId = Assert.Single(record.Content.Chunks).Id;
        Assert.Equal(ChunkIntegrityStatus.Corrupt,
            await application.Services.GetRequiredService<ChunkStore>()
                .VerifyChunkAsync(chunkId, CancellationToken.None).ConfigureAwait(false));
    }

    [Fact]
    public async Task LifecycleMaintenanceUsesBoundedMetadataPages()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Sava:MaintenanceScanInterval"] = "01:00:00",
            ["Sava:BlobRecordsPerMaintenancePass"] = "1",
            ["Sava:ContainerRecordsPerMaintenancePass"] = "1",
            ["Sava:UncommittedBlocksPerMaintenancePass"] = "1",
            [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
        });
        try
        {
            await application.InitializeAsync();
            var blobService = application.Services.GetRequiredService<BlobService>();
            var metadata = application.Services.GetRequiredService<MetadataStore>();

            // Synchronize with the hosted service's initial empty pass before creating content.
            await blobService.RunMaintenanceAsync(CancellationToken.None);

            var service = CreateClient(application);
            var containerName = $"bounded-lifecycle-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var uncommitted = await CreateBoundedExpiryFixtureAsync(
                blobService, metadata, container, containerName);

            await AssertBoundedExpiryPassesAsync(blobService, metadata, containerName, uncommitted);

            var deletedContainerNames = await CreateExpiredDeletedContainersAsync(service, blobService, metadata);

            await AssertBoundedContainerPurgeAsync(blobService, metadata, deletedContainerNames);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    private static async Task<BlockBlobClient> CreateBoundedExpiryFixtureAsync(
        BlobService blobService, MetadataStore metadata,
        BlobContainerClient container, string containerName)
    {
        for (var index = 0; index < 3; index++)
        {
            var blob = container.GetBlobClient($"expiring-{index}.bin");
            await blob.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(2048)))
                .ConfigureAwait(false);
            var record = await blobService.GetBlobAsync(
                SavaWebApplicationFactory.AccountName, containerName, blob.Name,
                versionId: null, snapshot: null, includeDeleted: false, CancellationToken.None)
                .ConfigureAwait(false);
            await blobService.SetExpiryAsync(record, DateTimeOffset.UtcNow.AddMilliseconds(100),
                CancellationToken.None).ConfigureAwait(false);
        }

        var uncommitted = container.GetBlockBlobClient("uncommitted.bin");
        for (var index = 0; index < 3; index++)
        {
            var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes($"bounded-block-{index:D2}"));
            await uncommitted.StageBlockAsync(blockId, new MemoryStream(RandomNumberGenerator.GetBytes(2048)))
                .ConfigureAwait(false);
        }
        foreach (var block in await blobService.ListStagedBlocksAsync(
                     SavaWebApplicationFactory.AccountName, containerName, uncommitted.Name,
                     CancellationToken.None).ConfigureAwait(false))
        {
            await metadata.PutStagedBlockAsync(
                block with { CreatedAt = DateTimeOffset.UtcNow.AddDays(-8) }, CancellationToken.None)
                .ConfigureAwait(false);
        }
        return uncommitted;
    }

    private static async Task AssertBoundedExpiryPassesAsync(
        BlobService blobService, MetadataStore metadata,
        string containerName, BlockBlobClient uncommitted)
    {
        await Task.Delay(150).ConfigureAwait(false);
        for (var expectedRemaining = 2; expectedRemaining >= 0; expectedRemaining--)
        {
            var result = await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(1, result.ExpiredBlobs);
            Assert.Equal(1, result.ExpiredUncommittedBlocks);
            Assert.Equal(expectedRemaining,
                (await metadata.ListBlobsAsync(
                    SavaWebApplicationFactory.AccountName, containerName,
                    includeVersions: false, includeSnapshots: false, includeDeleted: false,
                    CancellationToken.None).ConfigureAwait(false)).Count);
            Assert.Equal(expectedRemaining,
                (await blobService.ListStagedBlocksAsync(
                    SavaWebApplicationFactory.AccountName, containerName, uncommitted.Name,
                    CancellationToken.None).ConfigureAwait(false)).Count);
        }
    }

    private static async Task<string[]> CreateExpiredDeletedContainersAsync(
        BlobServiceClient service, BlobService blobService, MetadataStore metadata)
    {
        var properties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName, CancellationToken.None).ConfigureAwait(false);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            properties with { ContainerSoftDeleteEnabled = true, ContainerSoftDeleteRetentionDays = 1 },
            CancellationToken.None).ConfigureAwait(false);
        var deletedContainerNames = Enumerable.Range(0, 3)
            .Select(index => $"bounded-purge-{Guid.NewGuid():N}-{index}").ToArray();
        foreach (var name in deletedContainerNames)
        {
            var client = service.GetBlobContainerClient(name);
            await client.CreateAsync().ConfigureAwait(false);
            var record = await metadata.GetContainerAsync(
                SavaWebApplicationFactory.AccountName, name, includeDeleted: false,
                CancellationToken.None).ConfigureAwait(false);
            Assert.NotNull(record);
            await blobService.DeleteContainerAsync(record!, CancellationToken.None).ConfigureAwait(false);
            var deleted = await metadata.GetContainerAsync(
                SavaWebApplicationFactory.AccountName, name, includeDeleted: true,
                CancellationToken.None).ConfigureAwait(false);
            Assert.NotNull(deleted);
            await metadata.PutContainerAsync(
                deleted! with
                {
                    Revision = MetadataStore.NewRevision(),
                    DeleteRetentionUntil = DateTimeOffset.UtcNow.AddMinutes(-1)
                },
                deleted.Revision, CancellationToken.None).ConfigureAwait(false);
        }
        return deletedContainerNames;
    }

    private static async Task AssertBoundedContainerPurgeAsync(
        BlobService blobService, MetadataStore metadata, string[] deletedContainerNames)
    {
        var purgedContainers = 0;
        for (var pass = 0; pass < 8 && purgedContainers < deletedContainerNames.Length; pass++)
        {
            var result = await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.InRange(result.PurgedSoftDeletedContainers, 0, 1);
            purgedContainers += result.PurgedSoftDeletedContainers;
        }
        Assert.Equal(deletedContainerNames.Length, purgedContainers);
        foreach (var name in deletedContainerNames)
        {
            Assert.Null(await metadata.GetContainerAsync(
                SavaWebApplicationFactory.AccountName, name, includeDeleted: true,
                CancellationToken.None).ConfigureAwait(false));
        }
    }

    [Fact]
    public async Task ConsistentBackupRestoresExactSharedSnapshotAndUncommittedContent()
    {
        var source = new SavaWebApplicationFactory();
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-backup-{Guid.NewGuid():N}");
        var restoredPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-restored-{Guid.NewGuid():N}");
        try
        {
            await source.InitializeAsync();
            var scenario = await CreateBackupSourceScenarioAsync(source);

            var backup = source.Services.GetRequiredService<StorageBackupService>();
            var created = await AssertBackupManifestAsync(backup, backupPath);

            await AssertBackupRestoreGuardsAndLayoutAsync(source, backupPath, restoredPath, created);

            var restored = new SavaWebApplicationFactory(restoredPath);
            // xUnit1030 requires the Fact's continuation to retain its synchronization context.
#pragma warning disable MA0004
            await using var restoredDisposal = restored.ConfigureAwait(true);
#pragma warning restore MA0004
            await restored.InitializeAsync();
            await AssertRestoredBackupContentAsync(restored, scenario);

            await AssertCorruptedBackupRejectedAsync(backup, backupPath);
        }
        finally
        {
            await source.DisposeAsync();
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
            if (Directory.Exists(restoredPath))
                Directory.Delete(restoredPath, recursive: true);
        }
    }

    private sealed record BackupSourceScenario(
        string ContainerName, string FirstName, string SecondName, byte[] SharedBytes, string Snapshot,
        string UncommittedName, string BlockId, byte[] UncommittedBytes,
        byte[] CustomerKey, string EncryptedName, byte[] EncryptedBytes);

    private static async Task<BackupSourceScenario> CreateBackupSourceScenarioAsync(
        SavaWebApplicationFactory source)
    {
        var service = CreateClient(source);
        var containerName = $"backup-{Guid.NewGuid():N}";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync().ConfigureAwait(false);
        var sharedBytes = Enumerable.Range(0, 96 * 1024).Select(index => (byte)(index % 251)).ToArray();
        var first = container.GetBlobClient("first.bin");
        var second = container.GetBlobClient("second.bin");
        await first.UploadAsync(BinaryData.FromBytes(sharedBytes)).ConfigureAwait(false);
        await second.UploadAsync(BinaryData.FromBytes(sharedBytes)).ConfigureAwait(false);
        var snapshot = (await first.CreateSnapshotAsync().ConfigureAwait(false)).Value.Snapshot;

        var uncommitted = container.GetBlockBlobClient("uncommitted.bin");
        var blockId = Convert.ToBase64String("backup-block-0001"u8);
        var uncommittedBytes = RandomNumberGenerator.GetBytes(24 * 1024);
        await uncommitted.StageBlockAsync(blockId, new MemoryStream(uncommittedBytes)).ConfigureAwait(false);

        var customerKey = RandomNumberGenerator.GetBytes(32);
        var encrypted = CreateEncryptedClient(source, new CustomerProvidedKey(customerKey), encryptionScope: null)
            .GetBlobContainerClient(containerName).GetBlobClient("customer-key.bin");
        var encryptedBytes = RandomNumberGenerator.GetBytes(20 * 1024);
        await encrypted.UploadAsync(BinaryData.FromBytes(encryptedBytes)).ConfigureAwait(false);
        return new BackupSourceScenario(containerName, first.Name, second.Name, sharedBytes, snapshot,
            uncommitted.Name, blockId, uncommittedBytes, customerKey, encrypted.Name, encryptedBytes);
    }

    private static async Task<StorageBackupValidation> AssertBackupManifestAsync(
        StorageBackupService backup, string backupPath)
    {
        var created = await backup.CreateAsync(backupPath, CancellationToken.None).ConfigureAwait(false);
        Assert.True(created.BlobRecordCount >= 4);
        Assert.True(created.ChunkCount > 0);
        Assert.Equal(created, await backup.ValidateAsync(backupPath, CancellationToken.None).ConfigureAwait(false));
        return created;
    }

    private static async Task AssertBackupRestoreGuardsAndLayoutAsync(
        SavaWebApplicationFactory source, string backupPath,
        string restoredPath, StorageBackupValidation created)
    {
        var options = source.Services
            .GetRequiredService<Microsoft.Extensions.Options.IOptions<Mk8.Sava.Configuration.SavaOptions>>()
            .Value;
        var wrongKeyOptions = new Mk8.Sava.Configuration.SavaOptions
        {
            DefaultAccount = SavaWebApplicationFactory.AccountName,
            Accounts = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [SavaWebApplicationFactory.AccountName] = Convert.ToBase64String(RandomNumberGenerator.GetBytes(64))
            }
        };
        await Assert.ThrowsAsync<InvalidDataException>(() => StorageBackupService.ValidateBackupAsync(
            backupPath, wrongKeyOptions, CancellationToken.None)).ConfigureAwait(false);
        var existingTarget = Path.Combine(Path.GetTempPath(), $"mk8-sava-existing-{Guid.NewGuid():N}");
        Directory.CreateDirectory(existingTarget);
        try
        {
            await Assert.ThrowsAsync<IOException>(() => StorageBackupService.RestoreAsync(
                backupPath, existingTarget, options, CancellationToken.None)).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(existingTarget);
        }
        var restoredBackup = await StorageBackupService.RestoreAsync(
            backupPath, restoredPath, options, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(created.ChunkCount, restoredBackup.ChunkCount);
        Assert.Equal(created.ChunkCount, EnumerateChunkFiles(restoredPath).Count());
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(restoredPath, "staging")));
        Assert.Equal(["metadata.db"],
            Directory.EnumerateFiles(restoredPath).Select(Path.GetFileName).Order(StringComparer.Ordinal),
            StringComparer.Ordinal);
    }

    private static async Task AssertRestoredBackupContentAsync(
        SavaWebApplicationFactory restored, BackupSourceScenario scenario)
    {
        var restoredContainer = CreateClient(restored).GetBlobContainerClient(scenario.ContainerName);
        Assert.Equal(scenario.SharedBytes,
            (await restoredContainer.GetBlobClient(scenario.FirstName).DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToArray());
        Assert.Equal(scenario.SharedBytes,
            (await restoredContainer.GetBlobClient(scenario.SecondName).DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToArray());
        Assert.Equal(scenario.SharedBytes,
            (await restoredContainer.GetBlobClient(scenario.FirstName).WithSnapshot(scenario.Snapshot)
                .DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());

        var restoredBlocks = await restoredContainer.GetBlockBlobClient(scenario.UncommittedName)
            .GetBlockListAsync(BlockListTypes.Uncommitted).ConfigureAwait(false);
        var restoredBlock = Assert.Single(restoredBlocks.Value.UncommittedBlocks);
        Assert.Equal(scenario.BlockId, restoredBlock.Name);
        Assert.Equal(scenario.UncommittedBytes.Length, restoredBlock.SizeLong);
        await restoredContainer.GetBlockBlobClient(scenario.UncommittedName)
            .CommitBlockListAsync([scenario.BlockId]).ConfigureAwait(false);
        Assert.Equal(scenario.UncommittedBytes,
            (await restoredContainer.GetBlobClient(scenario.UncommittedName)
                .DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var restoredEncrypted = CreateEncryptedClient(
                restored, new CustomerProvidedKey(scenario.CustomerKey), encryptionScope: null)
            .GetBlobContainerClient(scenario.ContainerName).GetBlobClient(scenario.EncryptedName);
        Assert.Equal(scenario.EncryptedBytes,
            (await restoredEncrypted.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertCorruptedBackupRejectedAsync(StorageBackupService backup, string backupPath)
    {
        var backedUpChunk = Directory.EnumerateFiles(
            Path.Combine(backupPath, "chunks"), "*.chunk", SearchOption.AllDirectories).First();
        var corrupt = new FileStream(backedUpChunk, FileMode.Open, FileAccess.ReadWrite,
            FileShare.None, 4096, FileOptions.Asynchronous);
        await using (corrupt.ConfigureAwait(false))
        {
            corrupt.Position = corrupt.Length - 1;
            var value = corrupt.ReadByte();
            Assert.NotEqual(-1, value);
            corrupt.Position--;
            corrupt.WriteByte((byte)(value ^ 0xff));
            // Durability tests require a media flush; FlushAsync cannot request one.
#pragma warning disable CA1849
            corrupt.Flush(flushToDisk: true);
#pragma warning restore CA1849
        }
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            backup.ValidateAsync(backupPath, CancellationToken.None)).ConfigureAwait(false);
    }

    [Fact]
    public async Task SchemaOneMetadataMigratesAndBackfillsAuthoritativeChunkReferences()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-v1-{Guid.NewGuid():N}");
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-v1-backup-{Guid.NewGuid():N}");
        var legacyBackupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-legacy-backup-{Guid.NewGuid():N}");
        var rejectedBackupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-index-mismatch-{Guid.NewGuid():N}");
        const string containerName = "migrated-container";
        await CreateVersionOneDatabaseAsync(dataPath, containerName);
        await CreateVersionOneBackupAsync(dataPath, legacyBackupPath);
        var application = new SavaWebApplicationFactory(dataPath);
        try
        {
            await application.InitializeAsync();
            var metadata = application.Services.GetRequiredService<MetadataStore>();
            await AssertMigratedLegacyInventoryAsync(application, metadata, legacyBackupPath);

            var container = CreateClient(application).GetBlobContainerClient(containerName);
            await AssertMigratedBlobAndStagedBlockAsync(container);
            await AssertMigratedSchemaTablesAsync(dataPath);

            await AssertMigratedBackupRejectsBrokenIndexAsync(application, dataPath, backupPath, rejectedBackupPath);
        }
        finally
        {
            await application.DisposeAsync();
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
            if (Directory.Exists(legacyBackupPath))
                Directory.Delete(legacyBackupPath, recursive: true);
            if (Directory.Exists(rejectedBackupPath))
                Directory.Delete(rejectedBackupPath, recursive: true);
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
    }

    private static async Task AssertMigratedLegacyInventoryAsync(
        SavaWebApplicationFactory application, MetadataStore metadata, string legacyBackupPath)
    {
        var configuredOptions = application.Services
            .GetRequiredService<Microsoft.Extensions.Options.IOptions<Mk8.Sava.Configuration.SavaOptions>>()
            .Value;
        var legacy = await StorageBackupService.ValidateBackupAsync(
            legacyBackupPath, configuredOptions, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(1, legacy.BlobRecordCount);
        Assert.Equal(1, legacy.StagedBlockCount);
        var inventory = await metadata.GetStorageInventoryAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(1, inventory.BlobRecordCount);
        Assert.Equal(1, inventory.StagedBlockCount);
        Assert.Equal(1024, inventory.LogicalBlobBytes);
        Assert.Equal(512, inventory.LogicalStagedBlockBytes);
        Assert.Equal(new HashSet<string>([SavaWebApplicationFactory.AccountName + "/$zero"],
            StringComparer.Ordinal), inventory.ReachableChunkIds);
    }

    private static async Task AssertMigratedBlobAndStagedBlockAsync(BlobContainerClient container)
    {
        Assert.Equal(new byte[1024],
            (await container.GetPageBlobClient("sparse.bin").DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToArray());
        var blocks = await container.GetBlockBlobClient("staged.bin")
            .GetBlockListAsync(BlockListTypes.Uncommitted).ConfigureAwait(false);
        var staged = Assert.Single(blocks.Value.UncommittedBlocks);
        Assert.Equal(Convert.ToBase64String("migrated-block"u8), staged.Name);
        Assert.Equal(512, staged.SizeLong);
    }

    private static async Task AssertMigratedSchemaTablesAsync(string dataPath)
    {
        var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}");
        await using (connection.ConfigureAwait(false))
        {
            await connection.OpenAsync().ConfigureAwait(false);
            var version = connection.CreateCommand();
            await using (version.ConfigureAwait(false))
            {
                version.CommandText = "PRAGMA user_version;";
                Assert.Equal(MetadataStore.CurrentSchemaVersion,
                    Convert.ToInt32(await version.ExecuteScalarAsync().ConfigureAwait(false),
                        CultureInfo.InvariantCulture));
            }
            var references = connection.CreateCommand();
            await using (references.ConfigureAwait(false))
            {
                references.CommandText = """
                    SELECT
                        (SELECT COUNT(*) FROM blob_chunk_references) +
                        (SELECT COUNT(*) FROM staged_block_chunk_references);
                    """;
                Assert.Equal(2L, Convert.ToInt64(
                    await references.ExecuteScalarAsync().ConfigureAwait(false), CultureInfo.InvariantCulture));
            }
            var tags = connection.CreateCommand();
            await using (tags.ConfigureAwait(false))
            {
                tags.CommandText = "SELECT COUNT(*) FROM blob_tags;";
                Assert.Equal(1L, Convert.ToInt64(
                    await tags.ExecuteScalarAsync().ConfigureAwait(false), CultureInfo.InvariantCulture));
            }
        }
    }

    private static async Task AssertMigratedBackupRejectsBrokenIndexAsync(
        SavaWebApplicationFactory application, string dataPath,
        string backupPath, string rejectedBackupPath)
    {
        var backup = application.Services.GetRequiredService<StorageBackupService>();
        var created = await backup.CreateAsync(backupPath, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(1, created.BlobRecordCount);
        Assert.Equal(1, created.StagedBlockCount);
        Assert.Equal(created, await backup.ValidateAsync(backupPath, CancellationToken.None).ConfigureAwait(false));

        var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}");
        await using (connection.ConfigureAwait(false))
        {
            await connection.OpenAsync().ConfigureAwait(false);
            var corruptIndex = connection.CreateCommand();
            await using (corruptIndex.ConfigureAwait(false))
            {
                corruptIndex.CommandText = """
                    DELETE FROM blob_chunk_references;
                    DELETE FROM staged_block_chunk_references;
                    """;
                await corruptIndex.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
        var mismatch = await Assert.ThrowsAsync<InvalidDataException>(() =>
            backup.CreateAsync(rejectedBackupPath, CancellationToken.None)).ConfigureAwait(false);
        Assert.Contains("chunk-reference index", mismatch.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SoftDeleteRetentionSurvivesPolicyChangesAndExpiredRecordsArePurged()
    {
        var client = CreateClient(factory);
        var original = (await client.GetPropertiesAsync()).Value;
        var containerName = $"retention-{Guid.NewGuid():N}";
        var container = client.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blob = container.GetBlobClient("retained.bin");
        await blob.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(24 * 1024)));
        var blobService = factory.Services.GetRequiredService<BlobService>();
        var metadata = factory.Services.GetRequiredService<MetadataStore>();

        try
        {
            await AssertRetentionSurvivesPolicyChangeAsync(client, blob, metadata, containerName);
            await AssertExpiredSoftDeleteIsPurgedAsync(client, blob, blobService, metadata, containerName);
        }
        finally
        {
            await client.SetPropertiesAsync(original);
        }
    }

    private static async Task AssertRetentionSurvivesPolicyChangeAsync(
        BlobServiceClient client, BlobClient blob, MetadataStore metadata, string containerName)
    {
        var enabled = (await client.GetPropertiesAsync().ConfigureAwait(false)).Value;
        enabled.DeleteRetentionPolicy.Enabled = true;
        enabled.DeleteRetentionPolicy.Days = 1;
        await client.SetPropertiesAsync(enabled).ConfigureAwait(false);
        await blob.DeleteAsync().ConfigureAwait(false);

        var deleted = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, containerName, blob.Name,
            versionId: null, snapshot: null, includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(deleted?.DeleteRetentionUntil);
        Assert.InRange(
            deleted!.DeleteRetentionUntil!.Value - deleted.DeletedAt!.Value,
            TimeSpan.FromHours(23.9), TimeSpan.FromHours(24.1));

        var disabled = (await client.GetPropertiesAsync().ConfigureAwait(false)).Value;
        disabled.DeleteRetentionPolicy.Enabled = false;
        await client.SetPropertiesAsync(disabled).ConfigureAwait(false);
        await blob.UndeleteAsync().ConfigureAwait(false);
        Assert.True((await blob.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertExpiredSoftDeleteIsPurgedAsync(
        BlobServiceClient client, BlobClient blob, BlobService blobService,
        MetadataStore metadata, string containerName)
    {
        var enabled = (await client.GetPropertiesAsync().ConfigureAwait(false)).Value;
        enabled.DeleteRetentionPolicy.Enabled = true;
        enabled.DeleteRetentionPolicy.Days = 1;
        await client.SetPropertiesAsync(enabled).ConfigureAwait(false);
        await blob.DeleteAsync().ConfigureAwait(false);
        var deleted = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, containerName, blob.Name,
            versionId: null, snapshot: null, includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(deleted);
        await metadata.PutBlobRecordAsync(
            deleted! with
            {
                Revision = MetadataStore.NewRevision(),
                DeleteRetentionUntil = DateTimeOffset.UtcNow.AddMinutes(-1)
            },
            deleted.Revision,
            CancellationToken.None).ConfigureAwait(false);

        await blobService.RunMaintenanceAsync(CancellationToken.None).ConfigureAwait(false);
        Assert.Null(await metadata.GetBlobAsync(
            SavaWebApplicationFactory.AccountName, containerName, blob.Name,
            versionId: null, snapshot: null, includeDeleted: true,
            CancellationToken.None).ConfigureAwait(false));
    }

    [Fact]
    public async Task SoftDeleteProtectsOverwritesAndVersioningRemovesTheCurrentVersionOnDelete()
    {
        var service = CreateClient(factory);
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var originalProperties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName,
            CancellationToken.None);
        var container = service.GetBlobContainerClient($"overwrite-retention-{Guid.NewGuid():N}");
        await container.CreateAsync();

        try
        {
            var softDelete = (await service.GetPropertiesAsync()).Value;
            softDelete.DeleteRetentionPolicy.Enabled = true;
            softDelete.DeleteRetentionPolicy.Days = 7;
            await service.SetPropertiesAsync(softDelete);
            await SetTestVersioningAsync(metadata, enabled: false);
            await AssertOverwriteRetainsDeletedSnapshotAsync(container);

            await AssertRecreatedBlobKeepsDeletedSnapshotAsync(container);

            await AssertBlobTypeReplacementDoesNotRetainOtherTypeAsync(container);
            await SetTestVersioningAsync(metadata, enabled: true);
            await AssertVersionedDeleteRetainsNoncurrentHistoryAsync(container);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                originalProperties,
                CancellationToken.None);
        }
    }

    private static async Task SetTestVersioningAsync(MetadataStore metadata, bool enabled)
    {
        var configured = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName, CancellationToken.None).ConfigureAwait(false);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.AccountName, configured with { VersioningEnabled = enabled },
            CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task AssertOverwriteRetainsDeletedSnapshotAsync(BlobContainerClient container)
    {
        var overwritten = container.GetBlockBlobClient("overwritten.txt");
        await overwritten.UploadAsync(new MemoryStream("before overwrite"u8.ToArray())).ConfigureAwait(false);
        await overwritten.UploadAsync(new MemoryStream("after overwrite"u8.ToArray())).ConfigureAwait(false);
        Assert.Equal("after overwrite",
            (await overwritten.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());

        var deletedSnapshots = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Deleted | BlobStates.Snapshots,
            Prefix = overwritten.Name
        }).ConfigureAwait(false))
        {
            if (item.Deleted && item.Snapshot is not null)
                deletedSnapshots.Add(item);
        }
        var overwrittenSnapshot = Assert.Single(deletedSnapshots);
        await overwritten.UndeleteAsync().ConfigureAwait(false);
        var restoredSnapshot = overwritten.WithSnapshot(overwrittenSnapshot.Snapshot);
        Assert.Equal("before overwrite",
            (await restoredSnapshot.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        Assert.Equal("after overwrite",
            (await overwritten.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
    }

    private static async Task AssertRecreatedBlobKeepsDeletedSnapshotAsync(BlobContainerClient container)
    {
        var recreated = container.GetBlockBlobClient("recreated.txt");
        await recreated.UploadAsync(new MemoryStream("soft-deleted original"u8.ToArray())).ConfigureAwait(false);
        await recreated.DeleteAsync().ConfigureAwait(false);
        await recreated.UploadAsync(new MemoryStream("replacement"u8.ToArray())).ConfigureAwait(false);
        Assert.Equal("replacement",
            (await recreated.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        await recreated.UndeleteAsync().ConfigureAwait(false);

        BlobItem? recreatedSnapshot = null;
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Snapshots,
            Prefix = recreated.Name
        }).ConfigureAwait(false))
        {
            if (string.Equals(item.Name, recreated.Name, StringComparison.Ordinal) && item.Snapshot is not null)
                recreatedSnapshot = item;
        }
        Assert.NotNull(recreatedSnapshot);
        Assert.Equal("soft-deleted original",
            (await recreated.WithSnapshot(recreatedSnapshot!.Snapshot).DownloadContentAsync().ConfigureAwait(false))
            .Value.Content.ToString());
    }

    private static async Task AssertBlobTypeReplacementDoesNotRetainOtherTypeAsync(BlobContainerClient container)
    {
        const string typeChangedName = "type-changed";
        var typeChangedAppend = container.GetAppendBlobClient(typeChangedName);
        await typeChangedAppend.CreateAsync().ConfigureAwait(false);
        await typeChangedAppend.AppendBlockAsync(new MemoryStream("append state"u8.ToArray()))
            .ConfigureAwait(false);
        await typeChangedAppend.DeleteAsync().ConfigureAwait(false);
        var typeChangedBlock = container.GetBlockBlobClient(typeChangedName);
        await typeChangedBlock.UploadAsync(new MemoryStream("block replacement"u8.ToArray()))
            .ConfigureAwait(false);
        var retainedDifferentType = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Deleted | BlobStates.Snapshots,
            Prefix = typeChangedName
        }).ConfigureAwait(false))
        {
            if (string.Equals(item.Name, typeChangedName, StringComparison.Ordinal) && item.Deleted)
                retainedDifferentType.Add(item);
        }
        Assert.Empty(retainedDifferentType);
    }

    private static async Task AssertVersionedDeleteRetainsNoncurrentHistoryAsync(BlobContainerClient container)
    {
        var versioned = container.GetBlockBlobClient("versioned.txt");
        await versioned.UploadAsync(new MemoryStream("version one"u8.ToArray())).ConfigureAwait(false);
        await versioned.UploadAsync(new MemoryStream("version two"u8.ToArray())).ConfigureAwait(false);
        await versioned.SetMetadataAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["revision"] = "metadata-write" })
            .ConfigureAwait(false);
        var versionedSnapshot = await versioned.CreateSnapshotAsync(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["snapshot"] = "override" })
            .ConfigureAwait(false);
        Assert.False(string.IsNullOrEmpty(versionedSnapshot.Value.VersionId));
        var snapshotProperties = await versioned.WithSnapshot(versionedSnapshot.Value.Snapshot)
            .GetPropertiesAsync().ConfigureAwait(false);
        Assert.Equal("override", snapshotProperties.Value.Metadata["snapshot"]);
        await versioned.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots).ConfigureAwait(false);
        Assert.False((await versioned.ExistsAsync().ConfigureAwait(false)).Value);

        var versions = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Version,
            Prefix = versioned.Name
        }).ConfigureAwait(false))
        {
            if (string.Equals(item.Name, versioned.Name, StringComparison.Ordinal) && item.VersionId is not null)
                versions.Add(item);
        }
        Assert.Equal(4, versions.Count);
        Assert.All(versions, item => Assert.False(item.Deleted));
        Assert.All(versions, item => Assert.False(item.IsLatestVersion));
        var contents = new HashSet<string>(StringComparer.Ordinal);
        foreach (var version in versions)
        {
            contents.Add((await versioned.WithVersion(version.VersionId).DownloadContentAsync()
                .ConfigureAwait(false)).Value.Content.ToString());
        }
        Assert.Equal(new HashSet<string>(["version one", "version two"], StringComparer.Ordinal), contents);
    }

    [Fact]
    public async Task ConcurrentPublicationAndReclamationPreserveEveryAcknowledgedBlob()
    {
        var client = CreateClient(factory);
        var container = client.GetBlobContainerClient($"gc-race-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var content = RandomNumberGenerator.GetBytes(96 * 1024);
        var blobService = factory.Services.GetRequiredService<BlobService>();
        using var stop = new CancellationTokenSource();
        var sweeper = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    stop.Token.ThrowIfCancellationRequested();
                    await blobService.CollectGarbageAsync(stop.Token).ConfigureAwait(false);
                    await Task.Yield();
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
            }
        });

        try
        {
            await Task.WhenAll(Enumerable.Range(0, 16).Select(index =>
                container.GetBlobClient($"published-{index:D2}.bin").UploadAsync(BinaryData.FromBytes(content))));
        }
        finally
        {
            await stop.CancelAsync();
            await sweeper.ConfigureAwait(true);
        }

        foreach (var index in Enumerable.Range(0, 16))
        {
            var downloaded = await container.GetBlobClient($"published-{index:D2}.bin").DownloadContentAsync().ConfigureAwait(true);
            Assert.Equal(content, downloaded.Value.Content.ToArray());
        }
    }

    [Fact]
    public async Task StructuredAndTransactionalCrc64AreValidatedBeforePublication()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"crc-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var content = new byte[4 * 1024 * 1024 + 257];
        for (var index = 0; index < content.Length; index++)
            content[index] = (byte)(index % 239);
        await AssertCrc64BlobDownloadsAsync(container, content);
        await AssertCrc64EmptyDownloadAsync(container);
        await AssertCrc64StagedBlockAsync(container);
        await AssertCrc64AppendBlockAsync(container);
        await AssertCrc64PageBlobAsync(container);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        await AssertInvalidStructuredCrc64BeforePublicationAsync(container, transport);
        await AssertInvalidTransactionalCrc64BeforePublicationAsync(container, transport);
        await AssertCrc64RawRangeAsync(container, transport, content);
    }

    private static async Task AssertCrc64BlobDownloadsAsync(BlobContainerClient container, byte[] content)
    {
        var valid = container.GetBlobClient("valid.bin");
        await valid.UploadAsync(new MemoryStream(content), new BlobUploadOptions
        {
            TransferValidation = new UploadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        }).ConfigureAwait(false);
        Assert.Equal(content, (await valid.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
        var validatedDownload = await valid.DownloadContentAsync(new BlobDownloadOptions
        {
            TransferValidation = new DownloadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        }).ConfigureAwait(false);
        Assert.Equal(content, validatedDownload.Value.Content.ToArray());
        var validatedRange = await valid.DownloadContentAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(4 * 1024 * 1024 - 127, 384),
            TransferValidation = new DownloadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        }).ConfigureAwait(false);
        Assert.Equal(content.AsSpan(4 * 1024 * 1024 - 127, 384).ToArray(), validatedRange.Value.Content.ToArray());
    }

    private static async Task AssertCrc64EmptyDownloadAsync(BlobContainerClient container)
    {
        var empty = container.GetBlobClient("empty.bin");
        await empty.UploadAsync(new MemoryStream([]), new BlobUploadOptions
        {
            TransferValidation = new UploadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        }).ConfigureAwait(false);
        var validatedEmptyDownload = await empty.DownloadContentAsync(new BlobDownloadOptions
        {
            TransferValidation = new DownloadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        }).ConfigureAwait(false);
        Assert.Empty(validatedEmptyDownload.Value.Content.ToArray());
    }

    private static async Task AssertCrc64StagedBlockAsync(BlobContainerClient container)
    {
        var stagedContent = "structured staged block"u8.ToArray();
        var blockId = Convert.ToBase64String("block-0001"u8);
        var blockBlob = container.GetBlockBlobClient("staged.bin");
        await blockBlob.StageBlockAsync(
            blockId,
            new MemoryStream(stagedContent),
            new BlockBlobStageBlockOptions
            {
                TransferValidation = new UploadTransferValidationOptions
                {
                    ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
                }
            }).ConfigureAwait(false);
        await blockBlob.CommitBlockListAsync([blockId]).ConfigureAwait(false);
        Assert.Equal(stagedContent,
            (await blockBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertCrc64AppendBlockAsync(BlobContainerClient container)
    {
        var appendContent = "structured append block"u8.ToArray();
        var appendBlob = container.GetAppendBlobClient("append.bin");
        await appendBlob.CreateAsync().ConfigureAwait(false);
        await appendBlob.AppendBlockAsync(
            new MemoryStream(appendContent),
            new AppendBlobAppendBlockOptions
            {
                TransferValidation = new UploadTransferValidationOptions
                {
                    ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
                }
            }).ConfigureAwait(false);
        Assert.Equal(appendContent,
            (await appendBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertCrc64PageBlobAsync(BlobContainerClient container)
    {
        var pageContent = Enumerable.Range(0, 512).Select(index => (byte)(index % 251)).ToArray();
        var pageBlob = container.GetPageBlobClient("page.bin");
        await pageBlob.CreateAsync(pageContent.Length).ConfigureAwait(false);
        await pageBlob.UploadPagesAsync(
            new MemoryStream(pageContent),
            0,
            new PageBlobUploadPagesOptions
            {
                TransferValidation = new UploadTransferValidationOptions
                {
                    ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
                }
            }).ConfigureAwait(false);
        Assert.Equal(pageContent,
            (await pageBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertInvalidStructuredCrc64BeforePublicationAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var invalid = container.GetBlobClient("invalid.bin");
        var invalidContent = "structured checksum mismatch"u8.ToArray();
        var invalidStructuredBody = EncodeStructuredBody(invalidContent);
        invalidStructuredBody[^1] ^= 0x01;
        using (var structuredRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   invalid.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent(invalidStructuredBody)
        })
        {
            structuredRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            structuredRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            structuredRequest.Headers.TryAddWithoutValidation(
                "x-ms-structured-body",
                StructuredBodyDecoder.ContentType);
            structuredRequest.Headers.TryAddWithoutValidation(
                "x-ms-structured-content-length",
                invalidContent.Length.ToString(CultureInfo.InvariantCulture));
            using var structuredResponse = await transport.SendAsync(structuredRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, structuredResponse.StatusCode);
            Assert.Equal("Crc64Mismatch", structuredResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await invalid.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertInvalidTransactionalCrc64BeforePublicationAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var invalidContent = "structured checksum mismatch"u8.ToArray();
        var invalidTransactional = container.GetBlobClient("invalid-transactional.bin");
        using (var transactionalRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   invalidTransactional.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent(invalidContent)
        })
        {
            transactionalRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            transactionalRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            transactionalRequest.Headers.TryAddWithoutValidation(
                "x-ms-content-crc64",
                Convert.ToBase64String(new byte[8]));
            using var transactionalResponse = await transport.SendAsync(transactionalRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, transactionalResponse.StatusCode);
            Assert.Equal("Crc64Mismatch", transactionalResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await invalidTransactional.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertCrc64RawRangeAsync(
        BlobContainerClient container, HttpClient transport, byte[] content)
    {
        const int rawRangeStart = 731;
        const int rawRangeLength = 2048;
        using var rangeRequest = new HttpRequestMessage(
            HttpMethod.Get,
            container.GetBlobClient("valid.bin").GenerateSasUri(
                BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        rangeRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
        rangeRequest.Headers.TryAddWithoutValidation("x-ms-range", $"bytes={rawRangeStart}-{rawRangeStart + rawRangeLength - 1}");
        rangeRequest.Headers.TryAddWithoutValidation("x-ms-range-get-content-crc64", "true");
        using var rangeResponse = await transport.SendAsync(rangeRequest).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.PartialContent, rangeResponse.StatusCode);
        var rangeBytes = await rangeResponse.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
        Assert.Equal(content.AsSpan(rawRangeStart, rawRangeLength).ToArray(), rangeBytes);
        var rangeCrc64 = new Mk8.Sava.Protocol.StorageCrc64();
        rangeCrc64.Append(rangeBytes);
        Assert.Equal(
            Convert.ToBase64String(rangeCrc64.GetHash()),
            rangeResponse.Headers.GetValues("x-ms-content-crc64").Single());
    }

    [Fact]
    public async Task ControlBodyChecksumsAndChecksumFeatureVersionsAreEnforcedBeforeMutation()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"control-integrity-{Guid.NewGuid():N}");
        await container.CreateAsync();
        using var transport = new HttpClient(factory.Server.CreateHandler());
        await AssertBlockListChecksumBeforeCommitAsync(container, transport);
        await AssertCrc64FeatureVersionAsync(container, transport);
        await AssertStructuredBodyFeatureVersionAsync(container, transport);
        await AssertCreateOnlyPermissionVersionAsync(container, transport);
    }

    private static async Task AssertBlockListChecksumBeforeCommitAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var blockBlob = container.GetBlockBlobClient("committed.bin");
        var blockId = Convert.ToBase64String("control-block-0001"u8);
        var blockContent = "validated block-list payload"u8.ToArray();
        await blockBlob.StageBlockAsync(blockId, new MemoryStream(blockContent)).ConfigureAwait(false);

        var blockList = Encoding.UTF8.GetBytes(
            $"<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList><Latest>{blockId}</Latest></BlockList>");
        var crc64 = new Mk8.Sava.Protocol.StorageCrc64();
        crc64.Append(blockList);
        var expectedCrc64 = Convert.ToBase64String(crc64.GetHash());
        var blockListUri = new Uri(
            blockBlob.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)) + "&comp=blocklist");

        using (var corruptRequest = new HttpRequestMessage(HttpMethod.Put, blockListUri)
        {
            Content = new ByteArrayContent(blockList)
        })
        {
            corruptRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            corruptRequest.Headers.TryAddWithoutValidation("x-ms-content-crc64", Convert.ToBase64String(new byte[8]));
            using var corruptResponse = await transport.SendAsync(corruptRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, corruptResponse.StatusCode);
            Assert.Equal("Crc64Mismatch", corruptResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await blockBlob.ExistsAsync().ConfigureAwait(false)).Value);

        using (var validRequest = new HttpRequestMessage(HttpMethod.Put, blockListUri)
        {
            Content = new ByteArrayContent(blockList)
        })
        {
            validRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            validRequest.Headers.TryAddWithoutValidation("x-ms-content-crc64", expectedCrc64);
            using var validResponse = await transport.SendAsync(validRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, validResponse.StatusCode);
            Assert.Equal(expectedCrc64, validResponse.Headers.GetValues("x-ms-content-crc64").Single());
        }
        Assert.Equal(blockContent,
            (await blockBlob.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray());
    }

    private static async Task AssertCrc64FeatureVersionAsync(BlobContainerClient container, HttpClient transport)
    {
        var oldCrcBlob = container.GetBlobClient("old-crc.bin");
        var oldCrcContent = "version-gated-crc"u8.ToArray();
        var crc64 = new Mk8.Sava.Protocol.StorageCrc64();
        crc64.Append(oldCrcContent);
        using (var oldCrcRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   oldCrcBlob.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent(oldCrcContent)
        })
        {
            oldCrcRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-11-09");
            oldCrcRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            oldCrcRequest.Headers.TryAddWithoutValidation("x-ms-content-crc64", Convert.ToBase64String(crc64.GetHash()));
            using var oldCrcResponse = await transport.SendAsync(oldCrcRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldCrcResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldCrcResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldCrcBlob.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertStructuredBodyFeatureVersionAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var oldStructuredBlob = container.GetBlobClient("old-structured.bin");
        var structuredContent = "version-gated-structured-body"u8.ToArray();
        var encodedStructuredContent = EncodeStructuredBody(structuredContent);
        using (var oldStructuredRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   oldStructuredBlob.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent(encodedStructuredContent)
        })
        {
            oldStructuredRequest.Headers.TryAddWithoutValidation("x-ms-version", "2024-11-04");
            oldStructuredRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            oldStructuredRequest.Headers.TryAddWithoutValidation("x-ms-structured-body", StructuredBodyDecoder.ContentType);
            oldStructuredRequest.Headers.TryAddWithoutValidation(
                "x-ms-structured-content-length",
                structuredContent.Length.ToString(CultureInfo.InvariantCulture));
            using var oldStructuredResponse = await transport.SendAsync(oldStructuredRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldStructuredResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldStructuredResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldStructuredBlob.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertCreateOnlyPermissionVersionAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var createOnlyBlob = container.GetBlockBlobClient("create-only.bin");
        var createOnlySas = createOnlyBlob.GenerateSasUri(
            BlobSasPermissions.Create,
            DateTimeOffset.UtcNow.AddMinutes(5));
        var createOnlyBlockId = Convert.ToBase64String("create-only-block"u8);
        var createOnlyBlockUri = new Uri(
            createOnlySas + "&comp=block&blockid=" + Uri.EscapeDataString(createOnlyBlockId));
        using (var oldCreatePermissionRequest = new HttpRequestMessage(HttpMethod.Put, createOnlyBlockUri)
        {
            Content = new ByteArrayContent("create-only-content"u8.ToArray())
        })
        {
            oldCreatePermissionRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-02-06");
            using var oldCreatePermissionResponse = await transport.SendAsync(oldCreatePermissionRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, oldCreatePermissionResponse.StatusCode);
        }

        using (var createPermissionRequest = new HttpRequestMessage(HttpMethod.Put, createOnlyBlockUri)
        {
            Content = new ByteArrayContent("create-only-content"u8.ToArray())
        })
        {
            createPermissionRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-04-06");
            using var createPermissionResponse = await transport.SendAsync(createPermissionRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, createPermissionResponse.StatusCode);
        }

        var createOnlyBlockList = Encoding.UTF8.GetBytes(
            $"<BlockList><Latest>{createOnlyBlockId}</Latest></BlockList>");
        using (var createPermissionCommitRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   new Uri(createOnlySas + "&comp=blocklist"))
        {
            Content = new ByteArrayContent(createOnlyBlockList)
        })
        {
            createPermissionCommitRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-04-06");
            using var createPermissionCommitResponse = await transport.SendAsync(createPermissionCommitRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, createPermissionCommitResponse.StatusCode);
        }

        using (var existingBlobCreateRequest = new HttpRequestMessage(HttpMethod.Put, createOnlyBlockUri)
        {
            Content = new ByteArrayContent("replacement"u8.ToArray())
        })
        {
            existingBlobCreateRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-04-06");
            using var existingBlobCreateResponse = await transport.SendAsync(existingBlobCreateRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Forbidden, existingBlobCreateResponse.StatusCode);
        }
    }

    [Fact]
    public async Task HistoricalWriteLimitsAndUrlOperationVersionsRejectBeforeMutation()
    {
        const int mebibyte = 1024 * 1024;
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"write-limits-{Guid.NewGuid():N}");
        await container.CreateAsync();
        using var transport = new HttpClient(factory.Server.CreateHandler());
        await AssertHistoricalBlockWriteLimitAsync(container, transport, mebibyte);
        await AssertHistoricalPutBlobLimitAsync(container, transport, mebibyte);
        var appendUri = await AssertHistoricalAppendWriteLimitAsync(container, transport, mebibyte);
        var pageUri = await AssertHistoricalPageWriteLimitAsync(container, transport, mebibyte);
        await AssertHistoricalPageCreateValidationAsync(container, transport);
        await AssertHistoricalAppendTypeVersionAsync(container, transport);
        var blockFromUrlUri = await AssertOldBlockFromUrlVersionAsync(container, transport);
        await AssertOldBlockSourceHeaderVersionsAsync(transport, blockFromUrlUri);
        await AssertOldAppendAndPageFromUrlVersionsAsync(transport, appendUri, pageUri);
        await AssertOldPutBlobFromUrlVersionAsync(container, transport);
    }

    private static async Task AssertHistoricalBlockWriteLimitAsync(
        BlobContainerClient container, HttpClient transport, int mebibyte)
    {
        var blockBlob = container.GetBlockBlobClient("blocks.bin");
        var blockSas = blockBlob.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(5));
        var firstBlockId = Convert.ToBase64String("limit-block-0001"u8);
        using (var boundaryBlockRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   new Uri(blockSas + "&comp=block&blockid=" + Uri.EscapeDataString(firstBlockId)))
        {
            Content = new ByteArrayContent(new byte[4 * mebibyte])
        })
        {
            boundaryBlockRequest.Headers.TryAddWithoutValidation("x-ms-version", "2015-04-05");
            using var boundaryBlockResponse = await transport.SendAsync(boundaryBlockRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, boundaryBlockResponse.StatusCode);
        }

        var oversizedBlockId = Convert.ToBase64String("limit-block-0002"u8);
        using (var oversizedBlockRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   new Uri(blockSas + "&comp=block&blockid=" + Uri.EscapeDataString(oversizedBlockId)))
        {
            Content = new DeclaredLengthContent(4L * mebibyte + 1)
        })
        {
            oversizedBlockRequest.Headers.TryAddWithoutValidation("x-ms-version", "2015-04-05");
            using var oversizedBlockResponse = await transport.SendAsync(oversizedBlockRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedBlockResponse.StatusCode);
            await AssertVersionedErrorAsync(oversizedBlockResponse, "RequestBodyTooLarge").ConfigureAwait(false);
        }
        var staged = await blockBlob.GetBlockListAsync(BlockListTypes.Uncommitted).ConfigureAwait(false);
        Assert.Equal([firstBlockId], staged.Value.UncommittedBlocks.Select(block => block.Name), StringComparer.Ordinal);
    }

    private static async Task AssertHistoricalPutBlobLimitAsync(
        BlobContainerClient container, HttpClient transport, int mebibyte)
    {
        var oversizedPutBlob = container.GetBlobClient("oversized-put.bin");
        using (var oversizedPutRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   oversizedPutBlob.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new DeclaredLengthContent(64L * mebibyte + 1)
        })
        {
            oversizedPutRequest.Headers.TryAddWithoutValidation("x-ms-version", "2015-04-05");
            oversizedPutRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var oversizedPutResponse = await transport.SendAsync(oversizedPutRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedPutResponse.StatusCode);
        }
        Assert.False((await oversizedPutBlob.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task<Uri> AssertHistoricalAppendWriteLimitAsync(
        BlobContainerClient container, HttpClient transport, int mebibyte)
    {
        var appendBlob = container.GetAppendBlobClient("append.bin");
        await appendBlob.CreateAsync().ConfigureAwait(false);
        var appendUri = new Uri(
            appendBlob.GenerateSasUri(
                BlobSasPermissions.Add | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)) + "&comp=appendblock");
        using (var boundaryAppendRequest = new HttpRequestMessage(HttpMethod.Put, appendUri)
        {
            Content = new ByteArrayContent(new byte[4 * mebibyte])
        })
        {
            boundaryAppendRequest.Headers.TryAddWithoutValidation("x-ms-version", "2021-12-02");
            using var boundaryAppendResponse = await transport.SendAsync(boundaryAppendRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, boundaryAppendResponse.StatusCode);
        }
        using (var oversizedAppendRequest = new HttpRequestMessage(HttpMethod.Put, appendUri)
        {
            Content = new DeclaredLengthContent(4L * mebibyte + 1)
        })
        {
            oversizedAppendRequest.Headers.TryAddWithoutValidation("x-ms-version", "2021-12-02");
            using var oversizedAppendResponse = await transport.SendAsync(oversizedAppendRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedAppendResponse.StatusCode);
        }
        Assert.Equal(4L * mebibyte,
            (await appendBlob.GetPropertiesAsync().ConfigureAwait(false)).Value.ContentLength);
        return appendUri;
    }

    private static async Task<Uri> AssertHistoricalPageWriteLimitAsync(
        BlobContainerClient container, HttpClient transport, int mebibyte)
    {
        var pageBlob = container.GetPageBlobClient("pages.bin");
        await pageBlob.CreateAsync(8L * mebibyte).ConfigureAwait(false);
        var pageUri = new Uri(
            pageBlob.GenerateSasUri(
                BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)) + "&comp=page");
        using (var boundaryPageRequest = new HttpRequestMessage(HttpMethod.Put, pageUri)
        {
            Content = new ByteArrayContent(new byte[4 * mebibyte])
        })
        {
            boundaryPageRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            boundaryPageRequest.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            boundaryPageRequest.Headers.TryAddWithoutValidation("x-ms-range", $"bytes=0-{4 * mebibyte - 1}");
            using var boundaryPageResponse = await transport.SendAsync(boundaryPageRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Created, boundaryPageResponse.StatusCode);
        }
        using (var oversizedPageRequest = new HttpRequestMessage(HttpMethod.Put, pageUri)
        {
            Content = new DeclaredLengthContent(4L * mebibyte + 512)
        })
        {
            oversizedPageRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            oversizedPageRequest.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            oversizedPageRequest.Headers.TryAddWithoutValidation("x-ms-range", $"bytes=0-{4 * mebibyte + 511}");
            using var oversizedPageResponse = await transport.SendAsync(oversizedPageRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedPageResponse.StatusCode);
        }
        var pageRanges = await pageBlob.GetPageRangesAsync().ConfigureAwait(false);
        Assert.Equal([new HttpRange(0, 4L * mebibyte)], pageRanges.Value.PageRanges);
        return pageUri;
    }

    private static async Task AssertHistoricalPageCreateValidationAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var oversizedPageBlob = container.GetPageBlobClient("oversized-page.bin");
        using (var oversizedPageCreateRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   oversizedPageBlob.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        })
        {
            oversizedPageCreateRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            oversizedPageCreateRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "PageBlob");
            oversizedPageCreateRequest.Headers.TryAddWithoutValidation(
                "x-ms-blob-content-length",
                (8L * 1024 * 1024 * 1024 * 1024 + 512).ToString(CultureInfo.InvariantCulture));
            using var oversizedPageCreateResponse = await transport.SendAsync(oversizedPageCreateRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedPageCreateResponse.StatusCode);
        }
        Assert.False((await oversizedPageBlob.ExistsAsync().ConfigureAwait(false)).Value);

        var negativeSequenceBlob = container.GetPageBlobClient("negative-sequence.bin");
        using (var negativeSequenceRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   negativeSequenceBlob.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        })
        {
            negativeSequenceRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            negativeSequenceRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "PageBlob");
            negativeSequenceRequest.Headers.TryAddWithoutValidation("x-ms-blob-content-length", "512");
            negativeSequenceRequest.Headers.TryAddWithoutValidation("x-ms-blob-sequence-number", "-1");
            using var negativeSequenceResponse = await transport.SendAsync(negativeSequenceRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, negativeSequenceResponse.StatusCode);
        }
        Assert.False((await negativeSequenceBlob.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertHistoricalAppendTypeVersionAsync(
        BlobContainerClient container, HttpClient transport)
    {
        var oldAppendBlob = container.GetAppendBlobClient("old-append.bin");
        using (var oldAppendCreateRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   oldAppendBlob.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        })
        {
            oldAppendCreateRequest.Headers.TryAddWithoutValidation("x-ms-version", "2014-02-14");
            oldAppendCreateRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "AppendBlob");
            using var oldAppendCreateResponse = await transport.SendAsync(oldAppendCreateRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldAppendCreateResponse.StatusCode);
            await AssertVersionedErrorAsync(oldAppendCreateResponse, "FeatureVersionMismatch")
                .ConfigureAwait(false);
        }
        Assert.False((await oldAppendBlob.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task<Uri> AssertOldBlockFromUrlVersionAsync(
        BlobContainerClient container, HttpClient transport)
    {
        const string unreachableSource = "https://source.invalid/blob";
        var oldUrlBlock = container.GetBlockBlobClient("old-url-block.bin");
        var oldUrlBlockId = Convert.ToBase64String("old-url-block-id"u8);
        using (var oldBlockFromUrlRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   new Uri(
                       oldUrlBlock.GenerateSasUri(
                           BlobSasPermissions.Create | BlobSasPermissions.Write,
                           DateTimeOffset.UtcNow.AddMinutes(5)) +
                       "&comp=block&blockid=" + Uri.EscapeDataString(oldUrlBlockId))))
        {
            oldBlockFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-version", "2017-07-29");
            oldBlockFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            using var oldBlockFromUrlResponse = await transport.SendAsync(oldBlockFromUrlRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldBlockFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldBlockFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        var supportedBlockFromUrlUri = new Uri(
            oldUrlBlock.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)) +
            "&comp=block&blockid=" + Uri.EscapeDataString(oldUrlBlockId));
        return supportedBlockFromUrlUri;
    }

    private static async Task AssertOldBlockSourceHeaderVersionsAsync(HttpClient transport, Uri supportedBlockFromUrlUri)
    {
        const string unreachableSource = "https://source.invalid/blob";
        using (var oldSourceCrcRequest = new HttpRequestMessage(HttpMethod.Put, supportedBlockFromUrlUri))
        {
            oldSourceCrcRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-11-09");
            oldSourceCrcRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldSourceCrcRequest.Headers.TryAddWithoutValidation("x-ms-source-content-crc64", Convert.ToBase64String(new byte[8]));
            using var oldSourceCrcResponse = await transport.SendAsync(oldSourceCrcRequest).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldSourceCrcResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceCrcResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldSourceAuthorizationRequest = new HttpRequestMessage(HttpMethod.Put, supportedBlockFromUrlUri))
        {
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-version", "2020-04-08");
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-copy-source-authorization", "Bearer opaque-token");
            using var oldSourceAuthorizationResponse = await transport.SendAsync(oldSourceAuthorizationRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldSourceAuthorizationResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceAuthorizationResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldSourceTagConditionRequest = new HttpRequestMessage(HttpMethod.Put, supportedBlockFromUrlUri))
        {
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-source-if-tags", "\"project\" = 'mk8'");
            using var oldSourceTagConditionResponse = await transport.SendAsync(oldSourceTagConditionRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldSourceTagConditionResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceTagConditionResponse.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private static async Task AssertOldAppendAndPageFromUrlVersionsAsync(
        HttpClient transport, Uri appendUri, Uri pageUri)
    {
        const string unreachableSource = "https://source.invalid/blob";
        using (var oldAppendFromUrlRequest = new HttpRequestMessage(HttpMethod.Put, appendUri))
        {
            oldAppendFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-03-28");
            oldAppendFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            using var oldAppendFromUrlResponse = await transport.SendAsync(oldAppendFromUrlRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldAppendFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldAppendFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var oldPageFromUrlRequest = new HttpRequestMessage(HttpMethod.Put, pageUri))
        {
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-03-28");
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
            using var oldPageFromUrlResponse = await transport.SendAsync(oldPageFromUrlRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldPageFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldPageFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    private static async Task AssertOldPutBlobFromUrlVersionAsync(
        BlobContainerClient container, HttpClient transport)
    {
        const string unreachableSource = "https://source.invalid/blob";
        var oldPutBlobFromUrl = container.GetBlockBlobClient("old-put-blob-url.bin");
        using (var oldPutBlobFromUrlRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   oldPutBlobFromUrl.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5))))
        {
            oldPutBlobFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
            oldPutBlobFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldPutBlobFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var oldPutBlobFromUrlResponse = await transport.SendAsync(oldPutBlobFromUrlRequest)
                .ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, oldPutBlobFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldPutBlobFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldPutBlobFromUrl.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task HistoricalServiceVersionsRejectNewerBlobTypesBeforeMutation()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"historical-types-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var append = container.GetAppendBlobClient("events.log");
        await append.CreateAsync();
        await append.AppendBlockAsync(BinaryData.FromString("retained append payload").ToStream());
        var page = container.GetPageBlobClient("disk.vhd");
        await page.CreateAsync(512);
        using var transport = new HttpClient(factory.Server.CreateHandler());
        var appendSas = await AssertHistoricalAppendBlobTypeAsync(append, transport);
        await AssertHistoricalPageBlobTypeAsync(page, container, transport);
        var containerSas = container.GenerateSasUri(
            BlobContainerSasPermissions.All,
            DateTimeOffset.UtcNow.AddMinutes(5));
        await AssertHistoricalListRejectsAsync(containerSas, transport, append.Name, "2014-02-14");
        await AssertHistoricalListRejectsAsync(containerSas, transport, page.Name, "2009-07-17");
        await AssertHistoricalCopySourceTypeAsync(container, transport, appendSas);
    }

    private static async Task<Uri> AssertHistoricalAppendBlobTypeAsync(
        AppendBlobClient append, HttpClient transport)
    {
        var appendSas = append.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5));
        using (var oldRead = new HttpRequestMessage(HttpMethod.Head, appendSas))
        {
            oldRead.Headers.TryAddWithoutValidation("x-ms-version", "2014-02-14");
            using var response = await transport.SendAsync(oldRead).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
        }
        using (var oldAppend = new HttpRequestMessage(HttpMethod.Put, AppendQuery(appendSas, "comp=appendblock"))
        {
            Content = new ByteArrayContent("must not append"u8.ToArray())
        })
        {
            oldAppend.Headers.TryAddWithoutValidation("x-ms-version", "2014-02-14");
            using var response = await transport.SendAsync(oldAppend).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
        }
        using (var oldOverwrite = new HttpRequestMessage(HttpMethod.Put, appendSas)
        {
            Content = new ByteArrayContent("must not overwrite"u8.ToArray())
        })
        {
            oldOverwrite.Headers.TryAddWithoutValidation("x-ms-version", "2014-02-14");
            oldOverwrite.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(oldOverwrite).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
        }
        Assert.Equal(
            "retained append payload",
            (await append.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToString());
        return appendSas;
    }

    private static async Task AssertHistoricalPageBlobTypeAsync(
        PageBlobClient page, BlobContainerClient container, HttpClient transport)
    {
        var pageSas = page.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5));
        using (var oldRead = new HttpRequestMessage(HttpMethod.Head, pageSas))
        {
            oldRead.Headers.TryAddWithoutValidation("x-ms-version", "2009-07-17");
            using var response = await transport.SendAsync(oldRead).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            await AssertVersionedErrorAsync(response, "InvalidVersionForPageBlobOperation")
                .ConfigureAwait(false);
        }
        using (var oldPageWrite = new HttpRequestMessage(HttpMethod.Put, AppendQuery(pageSas, "comp=page"))
        {
            Content = new ByteArrayContent(Enumerable.Repeat((byte)0x5A, 512).ToArray())
        })
        {
            oldPageWrite.Headers.TryAddWithoutValidation("x-ms-version", "2009-07-17");
            oldPageWrite.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            oldPageWrite.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
            using var response = await transport.SendAsync(oldPageWrite).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            await AssertVersionedErrorAsync(response, "InvalidVersionForPageBlobOperation")
                .ConfigureAwait(false);
        }
        Assert.All((await page.DownloadContentAsync().ConfigureAwait(false)).Value.Content.ToArray(),
            value => Assert.Equal(0, value));

        var oldPageCreate = container.GetPageBlobClient("old-disk.vhd");
        using (var oldCreate = new HttpRequestMessage(
                   HttpMethod.Put,
                   oldPageCreate.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        })
        {
            oldCreate.Headers.TryAddWithoutValidation("x-ms-version", "2009-07-17");
            oldCreate.Headers.TryAddWithoutValidation("x-ms-blob-type", "PageBlob");
            oldCreate.Headers.TryAddWithoutValidation("x-ms-blob-content-length", "512");
            using var response = await transport.SendAsync(oldCreate).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            await AssertVersionedErrorAsync(response, "InvalidVersionForPageBlobOperation")
                .ConfigureAwait(false);
        }
        Assert.False((await oldPageCreate.ExistsAsync().ConfigureAwait(false)).Value);
    }

    private static async Task AssertHistoricalListRejectsAsync(
        Uri containerSas, HttpClient transport, string prefix, string version)
    {
        var uri = AppendQuery(
            containerSas,
            $"restype=container&comp=list&prefix={Uri.EscapeDataString(prefix)}");
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        using var response = await transport.SendAsync(request).ConfigureAwait(false);
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
    }

    private static async Task AssertHistoricalCopySourceTypeAsync(
        BlobContainerClient container, HttpClient transport, Uri appendSas)
    {
        var copied = container.GetBlobClient("copied-events.log");
        using (var oldCopy = new HttpRequestMessage(
                   HttpMethod.Put,
                   copied.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        })
        {
            oldCopy.Headers.TryAddWithoutValidation("x-ms-version", "2014-02-14");
            oldCopy.Headers.TryAddWithoutValidation("x-ms-copy-source", appendSas.ToString());
            using var response = await transport.SendAsync(oldCopy).ConfigureAwait(false);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
        }
        Assert.False((await copied.ExistsAsync().ConfigureAwait(false)).Value);
    }

    [Fact]
    public async Task VersionedBlobFeaturesRejectBeforeMutationAndHideNewerResponseFields()
    {
        var containerName = $"feature-versions-{Guid.NewGuid():N}";
        var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:ImmutableStorageWithVersioningContainers:0"] = containerName
            });
        await using var applicationDisposal45 = application.ConfigureAwait(false);
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        using var transport = new HttpClient(application.Server.CreateHandler());

        static async Task AssertFeatureVersionMismatchAsync(HttpClient client, HttpRequestMessage request)
        {
            using (request)
            using (var response = await client.SendAsync(request).ConfigureAwait(false))
            {
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                await AssertVersionedErrorAsync(response, "FeatureVersionMismatch").ConfigureAwait(false);
            }
        }

        var tagged = container.GetBlobClient("tagged.bin");
        await tagged.UploadAsync(
            BinaryData.FromString("tagged payload"),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["project"] = "mk8" }
            });
        var taggedSas = tagged.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5));

        using (var oldTagWrite = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(taggedSas, "comp=tags"))
        {
            Content = new ByteArrayContent("<Tags><TagSet /></Tags>"u8.ToArray())
        })
        {
            oldTagWrite.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
            await AssertFeatureVersionMismatchAsync(transport, oldTagWrite);
        }
        Assert.Equal("mk8", (await tagged.GetTagsAsync()).Value.Tags["project"]);

        var oldTagRead = new HttpRequestMessage(HttpMethod.Get, AppendQuery(taggedSas, "comp=tags"));
        oldTagRead.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
        await AssertFeatureVersionMismatchAsync(transport, oldTagRead);

        var oldTaggedCreate = container.GetBlobClient("old-tag-header.bin");
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   oldTaggedCreate.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent("must not publish"u8.ToArray())
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            request.Headers.TryAddWithoutValidation("x-ms-tags", "project=old");
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        Assert.False((await oldTaggedCreate.ExistsAsync()).Value);

        var oldTierCreate = container.GetBlobClient("old-tier-header.bin");
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   oldTierCreate.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent("must not publish"u8.ToArray())
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2017-07-29");
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            request.Headers.TryAddWithoutValidation("x-ms-access-tier", "Cool");
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        Assert.False((await oldTierCreate.ExistsAsync()).Value);

        var oldImmutableCreate = container.GetBlobClient("old-immutability-header.bin");
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   oldImmutableCreate.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent("must not publish"u8.ToArray())
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-06-12");
            request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            request.Headers.TryAddWithoutValidation(
                "x-ms-immutability-policy-until-date",
                DateTimeOffset.UtcNow.AddDays(1).ToString("R", CultureInfo.InvariantCulture));
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        Assert.False((await oldImmutableCreate.ExistsAsync()).Value);

        var tiered = container.GetBlobClient("tiered.bin");
        await tiered.UploadAsync(BinaryData.FromString("tier payload"));
        await tiered.SetAccessTierAsync(AccessTier.Cool);
        var tierUri = AppendQuery(
            tiered.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=tier");
        using (var request = new HttpRequestMessage(HttpMethod.Put, tierUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2017-07-29");
            request.Headers.TryAddWithoutValidation("x-ms-access-tier", "Hot");
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        using (var request = new HttpRequestMessage(HttpMethod.Put, tierUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-10-02");
            request.Headers.TryAddWithoutValidation("x-ms-access-tier", "Cold");
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        using (var request = new HttpRequestMessage(HttpMethod.Put, tierUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2018-11-09");
            request.Headers.TryAddWithoutValidation("x-ms-access-tier", "Archive");
            request.Headers.TryAddWithoutValidation("x-ms-rehydrate-priority", "High");
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        Assert.Equal(AccessTier.Cool, (await tiered.GetPropertiesAsync()).Value.AccessTier);

        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync();
        var appendSas = append.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5));
        var oldSeal = new HttpRequestMessage(HttpMethod.Put, AppendQuery(appendSas, "comp=seal"))
        {
            Content = new ByteArrayContent([])
        };
        oldSeal.Headers.TryAddWithoutValidation("x-ms-version", "2019-07-07");
        await AssertFeatureVersionMismatchAsync(transport, oldSeal);
        await append.AppendBlockAsync(BinaryData.FromString("still writable").ToStream());
        await append.SealAsync();

        using (var oldAppendProperties = new HttpRequestMessage(HttpMethod.Head, appendSas))
        {
            oldAppendProperties.Headers.TryAddWithoutValidation("x-ms-version", "2019-07-07");
            using var response = await transport.SendAsync(oldAppendProperties);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.False(response.Headers.Contains("x-ms-blob-sealed"));
        }

        var expiring = container.GetBlobClient("expiring.bin");
        await expiring.UploadAsync(BinaryData.FromString("expiry payload"));
        var expiryUri = AppendQuery(
            expiring.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=expiry");
        using (var request = new HttpRequestMessage(HttpMethod.Put, expiryUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "RelativeToNow");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", "60000");
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        using (var request = new HttpRequestMessage(HttpMethod.Put, expiryUri)
        {
            Content = new ByteArrayContent([1])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-02-10");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "RelativeToNow");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", "60000");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var request = new HttpRequestMessage(HttpMethod.Put, expiryUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-02-10");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "RelativeToNow");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", "60000");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("BlobOperationNotSupported", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var mutable = container.GetBlobClient("mutable.bin");
        await mutable.UploadAsync(BinaryData.FromString("mutable payload"));
        var mutableSas = mutable.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5));
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(mutableSas, "comp=immutabilityPolicies"))
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-06-12");
            request.Headers.TryAddWithoutValidation(
                "x-ms-immutability-policy-until-date",
                DateTimeOffset.UtcNow.AddDays(1).ToString("R", CultureInfo.InvariantCulture));
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(mutableSas, "comp=legalhold"))
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-06-12");
            request.Headers.TryAddWithoutValidation("x-ms-legal-hold", "true");
            await AssertFeatureVersionMismatchAsync(transport, request);
        }
        await mutable.SetMetadataAsync(new Dictionary<string, string>(StringComparer.Ordinal) { ["state"] = "still-mutable" });

        var deleted = container.GetBlobClient("deleted.bin");
        await deleted.UploadAsync(BinaryData.FromString("deleted payload"));
        await deleted.DeleteAsync();
        var oldUndelete = new HttpRequestMessage(
            HttpMethod.Put,
            AppendQuery(
                deleted.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5)),
                "comp=undelete"))
        {
            Content = new ByteArrayContent([])
        };
        oldUndelete.Headers.TryAddWithoutValidation("x-ms-version", "2017-04-17");
        await AssertFeatureVersionMismatchAsync(transport, oldUndelete);
        Assert.False((await deleted.ExistsAsync()).Value);

        var oldVersionRead = new HttpRequestMessage(
            HttpMethod.Head,
            AppendQuery(
                taggedSas,
                "versionid=" + Uri.EscapeDataString("2026-01-01T00:00:00.0000000Z")));
        oldVersionRead.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
        await AssertFeatureVersionMismatchAsync(transport, oldVersionRead);

        var containerSas = container.GenerateSasUri(
            BlobContainerSasPermissions.All,
            DateTimeOffset.UtcNow.AddMinutes(5));
        var oldTagList = new HttpRequestMessage(
            HttpMethod.Get,
            AppendQuery(containerSas, "restype=container&comp=list&include=tags"));
        oldTagList.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
        await AssertFeatureVersionMismatchAsync(transport, oldTagList);

        using (var oldList = new HttpRequestMessage(
                   HttpMethod.Get,
                   AppendQuery(containerSas, "restype=container&comp=list&prefix=tagged.bin")))
        {
            oldList.Headers.TryAddWithoutValidation("x-ms-version", "2015-04-05");
            using var response = await transport.SendAsync(oldList);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var xml = await response.Content.ReadAsStringAsync();
            Assert.DoesNotContain("Creation-Time", xml, StringComparison.Ordinal);
            Assert.DoesNotContain("AccessTier", xml, StringComparison.Ordinal);
            Assert.DoesNotContain("ServerEncrypted", xml, StringComparison.Ordinal);
            Assert.DoesNotContain("TagCount", xml, StringComparison.Ordinal);
        }

        using (var currentList = new HttpRequestMessage(
                   HttpMethod.Get,
                   AppendQuery(containerSas, "restype=container&comp=list&prefix=tagged.bin&include=tags")))
        {
            currentList.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            using var response = await transport.SendAsync(currentList);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var xml = await response.Content.ReadAsStringAsync();
            Assert.Contains("<TagCount>1</TagCount>", xml, StringComparison.Ordinal);
            Assert.Contains("<Key>project</Key>", xml, StringComparison.Ordinal);
        }

        await tagged.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
        {
            ExpiresOn = DateTimeOffset.UtcNow.AddDays(1),
            PolicyMode = BlobImmutabilityPolicyMode.Unlocked
        });
        await tagged.SetLegalHoldAsync(true);
        using (var historicalProperties = new HttpRequestMessage(HttpMethod.Head, taggedSas))
        {
            historicalProperties.Headers.TryAddWithoutValidation("x-ms-version", "2018-03-28");
            using var response = await transport.SendAsync(historicalProperties);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(response.Headers.Contains("x-ms-access-tier"));
            Assert.True(response.Headers.Contains("x-ms-creation-time"));
            Assert.False(response.Headers.Contains("x-ms-tag-count"));
            Assert.False(response.Headers.Contains("x-ms-version-id"));
            Assert.False(response.Headers.Contains("x-ms-immutability-policy-until-date"));
            Assert.False(response.Headers.Contains("x-ms-legal-hold"));
        }
    }

    [Fact]
    public void StorageCrc64MatchesTheAzureSdkImplementation()
    {
        var content = Enumerable.Range(0, 4_097).Select(index => (byte)(index % 233)).ToArray();
        var implementationType = typeof(StorageSharedKeyCredential).Assembly.GetType("Azure.Storage.StorageCrc64HashAlgorithm")
                                 ?? throw new InvalidOperationException("The Azure SDK CRC64 implementation was not found.");
        var constructor = implementationType.GetConstructors(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            .Single();
        var arguments = constructor.GetParameters()
            .Select(parameter => parameter.ParameterType == typeof(ulong) ? (object)0UL : throw new InvalidOperationException(parameter.ParameterType.FullName))
            .ToArray();
        var sdk = constructor.Invoke(arguments);
        var methods = implementationType.GetMethods(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
        var append = methods.Single(method => string.Equals(method.Name, "Append", StringComparison.Ordinal) &&
            method.GetParameters() is [{ ParameterType: var parameterType }] &&
            parameterType == typeof(byte[]));
        var getHash = methods.Single(method => string.Equals(method.Name, "GetHashAndReset", StringComparison.Ordinal) && method.GetParameters().Length == 0);
        append.Invoke(sdk, [content]);
        var expected = (byte[]?)getHash.Invoke(sdk, null)
                       ?? throw new InvalidOperationException("The Azure SDK CRC64 implementation returned no hash.");
        var actual = new Mk8.Sava.Protocol.StorageCrc64();
        actual.Append(content.AsSpan(0, 1_111));
        actual.Append(content.AsSpan(1_111));
        Assert.Equal(expected, actual.GetHash());
    }

    [Fact]
    public async Task AccountPolicyBlocksPublicContainers()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"private-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var denied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.SetAccessPolicyAsync(PublicAccessType.Blob));
        Assert.Equal("PublicAccessNotPermitted", denied.ErrorCode);
        Assert.Equal(409, denied.Status);
    }

    [Fact]
    public async Task PerAccountPublicAccessPolicyOverridesGlobalSettingWithoutBlockingStaticWebsite()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-public-policy-{Guid.NewGuid():N}");
        var containerName = $"public-policy-{Guid.NewGuid():N}";
        const string blobName = "content.txt";
        {
            var initial = new SavaWebApplicationFactory(
                         dataPath,
                         new Dictionary<string, string?>(StringComparer.Ordinal) { ["Sava:AllowAnonymousPublicAccess"] = "true" },
                         deleteDataPath: false);
            await using (initial.ConfigureAwait(false))
            {
                await initial.InitializeAsync();
                foreach (var (account, key) in new[]
                {
                    (SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
                    (SavaWebApplicationFactory.SecondAccountName, SavaWebApplicationFactory.SecondAccountKey)
                })
                {
                    var client = CreateClient(initial, account, key);
                    var container = client.GetBlobContainerClient(containerName);
                    await container.CreateAsync(
string.Equals(account, SavaWebApplicationFactory.AccountName
, StringComparison.Ordinal) ? PublicAccessType.Blob
                            : PublicAccessType.BlobContainer);
                    await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(account));
                }

                var secondAccount = CreateClient(
                    initial,
                    SavaWebApplicationFactory.SecondAccountName,
                    SavaWebApplicationFactory.SecondAccountKey);
                var properties = (await secondAccount.GetPropertiesAsync()).Value;
                properties.StaticWebsite.Enabled = true;
                properties.StaticWebsite.IndexDocument = "index.html";
                await secondAccount.SetPropertiesAsync(properties);
                await secondAccount.GetBlobContainerClient("$web")
                    .GetBlobClient("index.html")
                    .UploadAsync(BinaryData.FromString("website remains public"));
            }
        }

        var restarted = new SavaWebApplicationFactory(
            dataPath,
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["Sava:AllowAnonymousPublicAccess"] = "false",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:AllowBlobPublicAccess"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:AllowBlobPublicAccess"] = "false"
            },
            deleteDataPath: true);
        await using var restartedDisposal46 = restarted.ConfigureAwait(false);
        await restarted.InitializeAsync();
        using var anonymous = new HttpClient(restarted.Server.CreateHandler());
        using (var allowed = await anonymous.GetAsync(new Uri(
                   $"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}", UriKind.RelativeOrAbsolute)))
        {
            Assert.Equal(HttpStatusCode.OK, allowed.StatusCode);
            Assert.Equal(SavaWebApplicationFactory.AccountName, await allowed.Content.ReadAsStringAsync());
        }
        using (var denied = await anonymous.GetAsync(new Uri(
                   $"http://{SavaWebApplicationFactory.SecondAccountName}.localhost/{containerName}/{blobName}", UriKind.RelativeOrAbsolute)))
        {
            Assert.Equal(HttpStatusCode.Conflict, denied.StatusCode);
            await AssertVersionedErrorAsync(denied, "PublicAccessNotPermitted");
        }
        using (var deniedList = await anonymous.GetAsync(new Uri(
                   $"http://{SavaWebApplicationFactory.SecondAccountName}.localhost/{containerName}?restype=container&comp=list", UriKind.RelativeOrAbsolute)))
        {
            Assert.Equal(HttpStatusCode.Conflict, deniedList.StatusCode);
            await AssertVersionedErrorAsync(deniedList, "PublicAccessNotPermitted");
        }

        var authorized = CreateClient(
            restarted,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey)
            .GetBlobContainerClient(containerName);
        Assert.Equal(
            SavaWebApplicationFactory.SecondAccountName,
            (await authorized.GetBlobClient(blobName).DownloadContentAsync()).Value.Content.ToString());
        var aclDenied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            authorized.SetAccessPolicyAsync(PublicAccessType.Blob));
        Assert.Equal(409, aclDenied.Status);
        Assert.Equal("PublicAccessNotPermitted", aclDenied.ErrorCode);
        var createDenied = await Assert.ThrowsAsync<RequestFailedException>(() =>
            CreateClient(
                restarted,
                SavaWebApplicationFactory.SecondAccountName,
                SavaWebApplicationFactory.SecondAccountKey)
                .GetBlobContainerClient($"denied-public-{Guid.NewGuid():N}")
                .CreateAsync(PublicAccessType.Blob));
        Assert.Equal(409, createDenied.Status);
        Assert.Equal("PublicAccessNotPermitted", createDenied.ErrorCode);

        using var website = await anonymous.GetAsync(new Uri(
            $"http://{SavaWebApplicationFactory.SecondAccountName}.z1.web.local/", UriKind.RelativeOrAbsolute));
        Assert.Equal(HttpStatusCode.OK, website.StatusCode);
        Assert.Equal("website remains public", await website.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task ConditionalHeadersMatchAzureCombinationPriorityAndOperationRules()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"conditions-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("condition-matrix.bin");
        await blob.UploadAsync(BinaryData.FromString("conditional payload"));
        var properties = (await blob.GetPropertiesAsync()).Value;
        var etag = properties.ETag.ToString();
        var before = properties.LastModified.AddMinutes(-1).ToString("R", CultureInfo.InvariantCulture);
        var same = properties.LastModified.ToString("R", CultureInfo.InvariantCulture);
        var after = properties.LastModified.AddMinutes(1).ToString("R", CultureInfo.InvariantCulture);
        var blobUri = blob.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5));
        using var transport = new HttpClient(factory.Server.CreateHandler());

        static void AddVersion(HttpRequestMessage request, string version = "2023-11-03") =>
            request.Headers.TryAddWithoutValidation("x-ms-version", version);

        using (var request = new HttpRequestMessage(HttpMethod.Get, blobUri))
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-None-Match", etag);
            request.Headers.TryAddWithoutValidation("If-Modified-Since", before);
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        using (var request = new HttpRequestMessage(HttpMethod.Get, blobUri))
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-None-Match", etag);
            request.Headers.TryAddWithoutValidation("If-Modified-Since", same);
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
            Assert.Equal("ConditionNotMet", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var request = new HttpRequestMessage(HttpMethod.Get, blobUri))
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Match", "\"missing\"");
            request.Headers.TryAddWithoutValidation("If-Modified-Since", before);
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
            Assert.Equal("ConditionNotMet", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var request = new HttpRequestMessage(HttpMethod.Get, blobUri))
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Match", $"\"missing\", {etag}");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        using (var request = new HttpRequestMessage(HttpMethod.Get, blobUri))
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Modified-Since", [same, before]);
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal(
                "MultipleConditionHeadersNotSupported",
                response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var request = new HttpRequestMessage(HttpMethod.Get, blobUri))
        {
            AddVersion(request, "2012-02-12");
            request.Headers.TryAddWithoutValidation("If-None-Match", etag);
            request.Headers.TryAddWithoutValidation("If-Modified-Since", before);
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.NotModified, response.StatusCode);
        }

        var metadataUri = AppendQuery(blobUri, "comp=metadata");
        using (var request = new HttpRequestMessage(HttpMethod.Put, metadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-None-Match", "\"missing\"");
            request.Headers.TryAddWithoutValidation("If-Modified-Since", after);
            request.Headers.TryAddWithoutValidation("x-ms-meta-state", "none-match-priority");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        properties = (await blob.GetPropertiesAsync()).Value;
        etag = properties.ETag.ToString();
        using (var request = new HttpRequestMessage(HttpMethod.Put, metadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Match", etag);
            request.Headers.TryAddWithoutValidation("If-Unmodified-Since", before);
            request.Headers.TryAddWithoutValidation("x-ms-meta-state", "match-priority");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        properties = (await blob.GetPropertiesAsync()).Value;
        etag = properties.ETag.ToString();
        using (var request = new HttpRequestMessage(HttpMethod.Put, metadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Match", etag);
            request.Headers.TryAddWithoutValidation("If-Modified-Since", before);
            request.Headers.TryAddWithoutValidation("x-ms-meta-state", "must-not-apply");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal(
                "MultipleConditionHeadersNotSupported",
                response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var request = new HttpRequestMessage(HttpMethod.Put, metadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Match", $"{etag}, \"missing\"");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal(
                "MultipleConditionHeadersNotSupported",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.Equal("match-priority", (await blob.GetPropertiesAsync()).Value.Metadata["state"]);

        var accountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(5),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountSas.SetPermissions(AccountSasPermissions.Read);
        var accountCredential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
        var containerUri = new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}" +
            $"?restype=container&{accountSas.ToSasQueryParameters(accountCredential)}");
        using (var request = new HttpRequestMessage(
                   HttpMethod.Get,
                   containerUri))
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Modified-Since", after);
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        using (var request = new HttpRequestMessage(
                   HttpMethod.Get,
                   AppendQuery(blobUri, "comp=blocklist&blocklisttype=all")))
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Match", "\"missing\"");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        using (var request = new HttpRequestMessage(HttpMethod.Get, AppendQuery(blobUri, "comp=tags")))
        {
            AddVersion(request);
            request.Headers.TryAddWithoutValidation("If-Match", "\"missing\"");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
    }

    [Fact]
    public async Task CorsAndConditionalResponsesMatchHttpSemantics()
    {
        var service = CreateClient(factory);
        var properties = (await service.GetPropertiesAsync()).Value;
        properties.Cors.Clear();
        properties.Cors.Add(new BlobCorsRule
        {
            AllowedOrigins = "https://client.example,https://*.trusted.example",
            AllowedMethods = "GET,HEAD",
            AllowedHeaders = "*",
            ExposedHeaders = "ETag,x-ms-request-id",
            MaxAgeInSeconds = 120
        });
        await service.SetPropertiesAsync(properties);

        var containerName = $"cors-{Guid.NewGuid():N}";
        var blobName = "conditional.txt";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blob = container.GetBlobClient(blobName);
        await blob.UploadAsync(BinaryData.FromString("conditional payload"));
        var etag = (await blob.GetPropertiesAsync()).Value.ETag;

        var sasBuilder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
            Protocol = SasProtocol.HttpsAndHttp
        };
        sasBuilder.SetPermissions(BlobSasPermissions.Read);
        var credential = new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var sas = sasBuilder.ToSasQueryParameters(credential);
        var uri = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{sas}");
        using var client = new HttpClient(factory.Server.CreateHandler());

        using var corsRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        corsRequest.Headers.Add("Origin", "https://client.example");
        using var corsResponse = await client.SendAsync(corsRequest);
        Assert.Equal(HttpStatusCode.OK, corsResponse.StatusCode);
        Assert.Equal("https://client.example", corsResponse.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Contains("ETag", corsResponse.Headers.GetValues("Access-Control-Expose-Headers").Single(),
            StringComparison.Ordinal);
        Assert.Contains("Origin", corsResponse.Headers.Vary, StringComparer.Ordinal);

        using var wildcardRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        wildcardRequest.Headers.Add("Origin", "https://nested.app.trusted.example");
        using var wildcardResponse = await client.SendAsync(wildcardRequest);
        Assert.Equal(HttpStatusCode.OK, wildcardResponse.StatusCode);
        Assert.Equal(
            "https://nested.app.trusted.example",
            wildcardResponse.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Contains("Origin", wildcardResponse.Headers.Vary, StringComparer.Ordinal);

        using var caseMismatchRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        caseMismatchRequest.Headers.Add("Origin", "https://CLIENT.example");
        using var caseMismatchResponse = await client.SendAsync(caseMismatchRequest);
        Assert.Equal(HttpStatusCode.OK, caseMismatchResponse.StatusCode);
        Assert.False(caseMismatchResponse.Headers.Contains("Access-Control-Allow-Origin"));
        Assert.Contains("Origin", caseMismatchResponse.Headers.Vary, StringComparer.Ordinal);

        using var preflightRequest = new HttpRequestMessage(HttpMethod.Options, uri);
        preflightRequest.Headers.Add("Origin", "https://app.trusted.example");
        preflightRequest.Headers.Add("Access-Control-Request-Method", "HEAD");
        preflightRequest.Headers.Add("Access-Control-Request-Headers", "x-client-header");
        preflightRequest.Headers.TryAddWithoutValidation("Authorization", "SharedKey deliberately-invalid");
        using var preflightResponse = await client.SendAsync(preflightRequest);
        Assert.Equal(HttpStatusCode.OK, preflightResponse.StatusCode);
        Assert.Equal(
            "https://app.trusted.example",
            preflightResponse.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Equal("true", preflightResponse.Headers.GetValues("Access-Control-Allow-Credentials").Single());
        Assert.Equal("HEAD", preflightResponse.Headers.GetValues("Access-Control-Allow-Methods").Single());
        Assert.False(preflightResponse.Headers.Contains("Access-Control-Expose-Headers"));
        Assert.Equal(0, preflightResponse.Content.Headers.ContentLength);
        Assert.Empty(await preflightResponse.Content.ReadAsByteArrayAsync());

        using var missingOriginRequest = new HttpRequestMessage(HttpMethod.Options, uri);
        missingOriginRequest.Headers.Add("Access-Control-Request-Method", "HEAD");
        missingOriginRequest.Headers.TryAddWithoutValidation("Authorization", "Bearer deliberately-invalid");
        using var missingOriginResponse = await client.SendAsync(missingOriginRequest);
        Assert.Equal(HttpStatusCode.BadRequest, missingOriginResponse.StatusCode);
        Assert.Equal(
            "InvalidHeaderValue",
            missingOriginResponse.Headers.GetValues("x-ms-error-code").Single());

        using var lowerCaseMethodRequest = new HttpRequestMessage(HttpMethod.Options, uri);
        lowerCaseMethodRequest.Headers.Add("Origin", "https://app.trusted.example");
        lowerCaseMethodRequest.Headers.Add("Access-Control-Request-Method", "head");
        using var lowerCaseMethodResponse = await client.SendAsync(lowerCaseMethodRequest);
        Assert.Equal(HttpStatusCode.Forbidden, lowerCaseMethodResponse.StatusCode);
        Assert.Equal(
            "CorsPreflightFailure",
            lowerCaseMethodResponse.Headers.GetValues("x-ms-error-code").Single());

        using var conditionalRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        conditionalRequest.Headers.IfNoneMatch.Add(new System.Net.Http.Headers.EntityTagHeaderValue(etag.ToString()));
        using var conditionalResponse = await client.SendAsync(conditionalRequest);
        Assert.Equal(HttpStatusCode.NotModified, conditionalResponse.StatusCode);
        Assert.Equal("ConditionNotMet", conditionalResponse.Headers.GetValues("x-ms-error-code").Single());
        Assert.Empty(await conditionalResponse.Content.ReadAsByteArrayAsync());

        var invalid = (await service.GetPropertiesAsync()).Value;
        invalid.Cors.Clear();
        invalid.Cors.Add(new BlobCorsRule
        {
            AllowedOrigins = "https://client.example",
            AllowedMethods = "GET,TRACE",
            AllowedHeaders = "*",
            ExposedHeaders = string.Empty,
            MaxAgeInSeconds = 1
        });
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => service.SetPropertiesAsync(invalid));
        Assert.Equal(HttpStatusCode.BadRequest, (HttpStatusCode)rejected.Status);
        Assert.Equal("InvalidXmlDocument", rejected.ErrorCode);
        Assert.Equal(
            "https://client.example,https://*.trusted.example",
            Assert.Single((await service.GetPropertiesAsync()).Value.Cors).AllowedOrigins);
    }

    [Fact]
    public async Task QueryBlobContentsStreamsSdkCompatibleAvroForCsvAndJsonResults()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"query-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("rows.csv");
        await blob.UploadAsync(
            new MemoryStream("100,200,300,400\n300,400,500,600\n"u8.ToArray()),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["kind"] = "query" },
                Metadata = new Dictionary<string, string>(StringComparer.Ordinal) { ["marker"] = "must-not-leak" },
                HttpHeaders = new BlobHttpHeaders
                {
                    ContentType = "text/csv",
                    ContentLanguage = "en-US",
                    CacheControl = "no-store",
                    ContentDisposition = "inline"
                }
            });
        var properties = await blob.GetPropertiesAsync();

        var progress = new CaptureProgress();
        var csvResponse = await blob.QueryAsync(
            "SELECT _2 FROM BlobStorage WHERE _1 > 250;",
            new BlobQueryOptions
            {
                Conditions = new BlobRequestConditions
                {
                    IfMatch = properties.Value.ETag,
                    TagConditions = "\"kind\" = 'query'"
                },
                ProgressHandler = progress
            });
        using (var reader = new StreamReader(csvResponse.Value.Content))
            Assert.Equal("400\n", await reader.ReadToEndAsync());
        Assert.Equal(200, csvResponse.GetRawResponse().Status);
        Assert.Equal(properties.Value.ETag, csvResponse.Value.Details.ETag);
        var queryHeaders = csvResponse.GetRawResponse().Headers;
        Assert.True(queryHeaders.TryGetValue("x-ms-blob-type", out var queryBlobType));
        Assert.Equal("BlockBlob", queryBlobType);
        Assert.True(queryHeaders.TryGetValue("x-ms-server-encrypted", out var queryEncrypted));
        Assert.Equal("true", queryEncrypted);
        Assert.True(queryHeaders.TryGetValue("Content-Language", out var queryLanguage));
        Assert.Equal("en-US", queryLanguage);
        Assert.True(queryHeaders.TryGetValue("Cache-Control", out var queryCacheControl));
        Assert.Equal("no-store", queryCacheControl);
        Assert.True(queryHeaders.TryGetValue("Content-Disposition", out var queryDisposition));
        Assert.Equal("inline", queryDisposition);
        Assert.False(queryHeaders.TryGetValue("x-ms-meta-marker", out _));
        Assert.False(queryHeaders.TryGetValue("x-ms-tag-count", out _));
        Assert.False(queryHeaders.TryGetValue("x-ms-lease-status", out _));
        Assert.False(queryHeaders.TryGetValue("x-ms-access-tier", out _));
        Assert.False(queryHeaders.TryGetValue("x-ms-copy-status", out _));
        Assert.Equal([32L, 32L], progress.Values);

        var jsonResponse = await blob.QueryAsync(
            "SELECT _2 FROM BlobStorage WHERE _1 >= 300;",
            new BlobQueryOptions
            {
                InputTextConfiguration = new BlobQueryCsvTextOptions
                {
                    ColumnSeparator = ",",
                    QuotationCharacter = '"',
                    EscapeCharacter = '\\',
                    RecordSeparator = "\n"
                },
                OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
            });
        using var jsonReader = new StreamReader(jsonResponse.Value.Content);
        Assert.Equal("{\"_1\":\"400\"}\n", await jsonReader.ReadToEndAsync());

        var arrowResponse = await blob.QueryAsync(
            "SELECT _1, _2, _3, _4, true, '2026-09-22T12:34:56.789Z' FROM BlobStorage WHERE _1 >= 300;",
            new BlobQueryOptions
            {
                OutputTextConfiguration = new BlobQueryArrowOptions
                {
                    Schema =
                    {
                        new BlobQueryArrowField { Name = "first", Type = BlobQueryArrowFieldType.Int64 },
                        new BlobQueryArrowField { Name = "second", Type = BlobQueryArrowFieldType.Double },
                        new BlobQueryArrowField
                        {
                            Name = "third",
                            Type = BlobQueryArrowFieldType.Decimal,
                            Precision = 6,
                            Scale = 2
                        },
                        new BlobQueryArrowField { Name = "fourth", Type = BlobQueryArrowFieldType.String },
                        new BlobQueryArrowField { Name = "enabled", Type = BlobQueryArrowFieldType.Bool },
                        new BlobQueryArrowField { Name = "observed", Type = BlobQueryArrowFieldType.Timestamp }
                    }
                }
            });
        using (var arrowReader = new Apache.Arrow.Ipc.ArrowStreamReader(arrowResponse.Value.Content))
        {
            using var batch = await arrowReader.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            Assert.Equal(300L, Assert.IsType<Apache.Arrow.Int64Array>(batch.Column("first", StringComparer.Ordinal)).GetValue(0));
            Assert.Equal(400D, Assert.IsType<Apache.Arrow.DoubleArray>(batch.Column("second", StringComparer.Ordinal)).GetValue(0));
            Assert.Equal("500.00", Assert.IsType<Apache.Arrow.Decimal128Array>(batch.Column("third", StringComparer.Ordinal)).GetString(0));
            Assert.Equal("600", Assert.IsType<Apache.Arrow.StringArray>(batch.Column("fourth", StringComparer.Ordinal)).GetString(0));
            Assert.True(Assert.IsType<Apache.Arrow.BooleanArray>(batch.Column("enabled", StringComparer.Ordinal)).GetValue(0));
            Assert.Equal(
                new DateTimeOffset(2026, 9, 22, 12, 34, 56, 789, TimeSpan.Zero),
                Assert.IsType<Apache.Arrow.TimestampArray>(batch.Column("observed", StringComparer.Ordinal)).GetTimestamp(0));
            Assert.Null(await arrowReader.ReadNextRecordBatchAsync());
        }

        var emptyArrowResponse = await blob.QueryAsync(
            "SELECT _1 FROM BlobStorage WHERE _1 > 999;",
            new BlobQueryOptions
            {
                OutputTextConfiguration = new BlobQueryArrowOptions
                {
                    Schema =
                    {
                        new BlobQueryArrowField { Name = "value", Type = BlobQueryArrowFieldType.Int64 }
                    }
                }
            });
        using (var emptyArrowReader = new Apache.Arrow.Ipc.ArrowStreamReader(emptyArrowResponse.Value.Content))
        {
            Assert.Equal("value", emptyArrowReader.Schema.GetFieldByIndex(0).Name);
            Assert.Null(await emptyArrowReader.ReadNextRecordBatchAsync());
        }

        var append = container.GetAppendBlobClient("not-queryable");
        await append.CreateAsync();
        var invalidType = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlockBlobClient("not-queryable").QueryAsync("SELECT * FROM BlobStorage"));
        Assert.Equal(409, invalidType.Status);
        Assert.Equal("InvalidBlobType", invalidType.ErrorCode);
    }

    [Fact]
    public async Task QueryBlobContentsReadsParquetRowGroupsThroughTheOfficialSdk()
    {
        using var content = new MemoryStream(await CreateParquetQueryFixtureAsync());
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"parquet-query-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("rows.parquet");
        await blob.UploadAsync(content);

        var response = await blob.QueryAsync(
            "SELECT id AS id, name AS name, enabled AS enabled, score AS score, " +
            "observed AS observed, maybe AS maybe FROM BlobStorage WHERE id >= 2;",
            new BlobQueryOptions
            {
                InputTextConfiguration = new BlobQueryParquetTextOptions(),
                OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
            });
        using var resultReader = new StreamReader(response.Value.Content);
        var result = await resultReader.ReadToEndAsync();
        var lines = result.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        AssertParquetQueryRows(lines);
    }

    private static async Task<byte[]> CreateParquetQueryFixtureAsync()
    {
        var id = new Parquet.Schema.DataField<int>("id");
        var name = new Parquet.Schema.DataField<string>("name");
        var enabled = new Parquet.Schema.DataField<bool>("enabled");
        var score = new Parquet.Schema.DataField<double>("score");
        var observed = new Parquet.Schema.DataField<DateTime>("observed");
        var maybe = new Parquet.Schema.DataField<int?>("maybe");
        var schema = new Parquet.Schema.ParquetSchema(id, name, enabled, score, observed, maybe);

        using var content = new MemoryStream();
        {
            var writer = await Parquet.ParquetWriter.CreateAsync(schema, content).ConfigureAwait(false);
            await using (writer.ConfigureAwait(false))
            {
                using (var group = writer.CreateRowGroup())
                {
                    await group.WriteAsync<int>(id, FirstParquetIds.AsMemory()).ConfigureAwait(false);
                    await group.WriteAsync(name, FirstParquetNames).ConfigureAwait(false);
                    await group.WriteAsync<bool>(enabled, FirstParquetEnabled.AsMemory()).ConfigureAwait(false);
                    await group.WriteAsync<double>(score, FirstParquetScores.AsMemory()).ConfigureAwait(false);
                    await group.WriteAsync<DateTime>(
                        observed,
                        FirstParquetObserved.AsMemory()).ConfigureAwait(false);
                    await group.WriteAsync<int>(maybe, new int?[] { null, 20 }.AsMemory()).ConfigureAwait(false);
                    group.CompleteValidate();
                }

                using (var group = writer.CreateRowGroup())
                {
                    await group.WriteAsync<int>(id, SecondParquetIds.AsMemory()).ConfigureAwait(false);
                    await group.WriteAsync(name, SecondParquetNames).ConfigureAwait(false);
                    await group.WriteAsync<bool>(enabled, SecondParquetEnabled.AsMemory()).ConfigureAwait(false);
                    await group.WriteAsync<double>(score, SecondParquetScores.AsMemory()).ConfigureAwait(false);
                    await group.WriteAsync<DateTime>(
                        observed,
                        SecondParquetObserved.AsMemory()).ConfigureAwait(false);
                    await group.WriteAsync<int>(maybe, new int?[] { null }.AsMemory()).ConfigureAwait(false);
                    group.CompleteValidate();
                }
            }
        }
        return content.ToArray();
    }

    private static void AssertParquetQueryRows(string[] lines)
    {
        Assert.Equal(2, lines.Length);
        using var second = JsonDocument.Parse(lines[0]);
        Assert.Equal(2, second.RootElement.GetProperty("id").GetInt64());
        Assert.Equal("two", second.RootElement.GetProperty("name").GetString());
        Assert.False(second.RootElement.GetProperty("enabled").GetBoolean());
        Assert.Equal(2.5D, second.RootElement.GetProperty("score").GetDouble());
        Assert.Equal(
            new DateTimeOffset(2026, 9, 21, 11, 30, 0, TimeSpan.Zero),
            second.RootElement.GetProperty("observed").GetDateTimeOffset());
        Assert.Equal(20, second.RootElement.GetProperty("maybe").GetInt64());

        using var third = JsonDocument.Parse(lines[1]);
        Assert.Equal(3, third.RootElement.GetProperty("id").GetInt64());
        Assert.Equal("three", third.RootElement.GetProperty("name").GetString());
        Assert.True(third.RootElement.GetProperty("enabled").GetBoolean());
        Assert.Equal(3.75D, third.RootElement.GetProperty("score").GetDouble());
        Assert.Equal(
            new DateTimeOffset(2026, 9, 22, 12, 45, 0, TimeSpan.Zero),
            third.RootElement.GetProperty("observed").GetDateTimeOffset());
        Assert.Equal(JsonValueKind.Null, third.RootElement.GetProperty("maybe").ValueKind);
    }

    [Fact]
    public async Task QueryBlobContentsEvaluatesScalarSqlAndStopsAtLimit()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"scalar-query-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("rows.csv");
        await blob.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(
            "name,quantity,price,category\n" +
            "Apple,2,3.5,Fruit\n" +
            "\"unterminated,record,that,must-not-be-scanned")));

        var response = await blob.QueryAsync(
            "SELECT UPPER(name) AS product, " +
            "CAST(quantity AS INT) * CAST(price AS FLOAT) AS total, " +
            "COALESCE(NULLIF(category, 'Fruit'), 'produce') AS bucket, " +
            "SUBSTRING(name, 1, 3) AS fragment, CHAR_LENGTH(name) AS length " +
            "FROM BlobStorage WHERE CAST(quantity AS INT) BETWEEN 2 AND 5 " +
            "AND LOWER(name) IN ('apple', 'pear') LIMIT 1;",
            new BlobQueryOptions
            {
                InputTextConfiguration = new BlobQueryCsvTextOptions
                {
                    HasHeaders = true,
                    RecordSeparator = "\n"
                },
                OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
            });
        using var reader = new StreamReader(response.Value.Content);
        using var result = JsonDocument.Parse((await reader.ReadToEndAsync()).Trim());

        Assert.Equal("APPLE", result.RootElement.GetProperty("product").GetString());
        Assert.Equal(7D, result.RootElement.GetProperty("total").GetDouble());
        Assert.Equal("produce", result.RootElement.GetProperty("bucket").GetString());
        Assert.Equal("ppl", result.RootElement.GetProperty("fragment").GetString());
        Assert.Equal(5, result.RootElement.GetProperty("length").GetInt64());
    }

    [Fact]
    public async Task QueryBlobContentsEvaluatesDocumentedDateAndTrimFunctions()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"date-query-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("row.csv");
        await blob.UploadAsync(new MemoryStream("ignored\n"u8.ToArray()));

        var response = await blob.QueryAsync(
            "SELECT DATE_ADD('day', 2, TO_TIMESTAMP('2026-09-20T10:15:30+02:30')) AS added, " +
            "DATE_DIFF('hour', '2026-09-20T10:00:00Z', '2026-09-20T12:00:00Z') AS hours, " +
            "EXTRACT(TIMEZONE_HOUR FROM TO_TIMESTAMP('2026-09-20T10:15:30+02:30')) AS zone_hour, " +
            "EXTRACT(TIMEZONE_MINUTE FROM TO_TIMESTAMP('2026-09-20T10:15:30+02:30')) AS zone_minute, " +
            "TO_STRING(TO_TIMESTAMP('2026-09-20T10:15:30+02:30'), 'yyyy-MM-dd HH:mm XXX') AS formatted, " +
            "TRIM(BOTH 'xy' FROM 'xyvaluey') AS trimmed FROM BlobStorage LIMIT 1;",
            new BlobQueryOptions
            {
                OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
            });
        using var reader = new StreamReader(response.Value.Content);
        using var result = JsonDocument.Parse((await reader.ReadToEndAsync()).Trim());

        Assert.Equal(
            new DateTimeOffset(2026, 9, 22, 7, 45, 30, TimeSpan.Zero),
            result.RootElement.GetProperty("added").GetDateTimeOffset());
        Assert.Equal(2, result.RootElement.GetProperty("hours").GetInt64());
        Assert.Equal(2, result.RootElement.GetProperty("zone_hour").GetInt64());
        Assert.Equal(30, result.RootElement.GetProperty("zone_minute").GetInt64());
        Assert.Equal("2026-09-20 10:15 +02:30", result.RootElement.GetProperty("formatted").GetString());
        Assert.Equal("value", result.RootElement.GetProperty("trimmed").GetString());
    }

    [Fact]
    public async Task QueryBlobContentsStreamsDocumentedAggregateExpressions()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"aggregate-query-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("rows.json");
        await blob.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(
            "{\"name\":\"apple\",\"qty\":2,\"price\":3.5}\n" +
            "{\"name\":\"pear\",\"qty\":4,\"price\":1.25}\n" +
            "{\"name\":\"plum\",\"qty\":null,\"price\":2.0}\n")));

        async Task<JsonElement> QueryValueAsync(string aggregate)
        {
            var response = await blob.QueryAsync(
                $"SELECT {aggregate} AS value FROM BlobStorage;",
                new BlobQueryOptions
                {
                    InputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" },
                    OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
                }).ConfigureAwait(false);
            using var reader = new StreamReader(response.Value.Content);
            using var document = JsonDocument.Parse((await reader.ReadToEndAsync().ConfigureAwait(false)).Trim());
            return document.RootElement.GetProperty("value").Clone();
        }

        Assert.Equal(3, (await QueryValueAsync("COUNT(*)")).GetInt64());
        Assert.Equal(2, (await QueryValueAsync("COUNT(qty)")).GetInt64());
        Assert.Equal(6, (await QueryValueAsync("SUM(qty)")).GetInt64());
        Assert.Equal(2.25D, (await QueryValueAsync("AVG(price)")).GetDouble());
        Assert.Equal("apple", (await QueryValueAsync("MIN(name)")).GetString());
        Assert.Equal(3.5D, (await QueryValueAsync("MAX(price)")).GetDouble());
    }

    [Fact]
    public async Task QueryBlobContentsTraversesNestedJsonAndDistinguishesMissingFields()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"nested-query-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("rows.json");
        await blob.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(
            "{\"id\":1,\"weight\":0.2,\"tags\":[\"wireless\",\"accessory\"]," +
            "\"dimensions\":{\"length\":3},\"warehouses\":[" +
            "{\"latitude\":41.8,\"longitude\":-87.6}," +
            "{\"latitude\":45.1}," +
            "{\"latitude\":46.0,\"longitude\":null}]}\n")));

        var options = new BlobQueryOptions
        {
            InputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" },
            OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
        };
        var nested = await blob.QueryAsync(
            "SELECT source.weight AS weight, source.warehouses[0].longitude AS longitude, " +
            "source.tags[1] AS tag, source.dimensions.length AS length " +
            "FROM BlobStorage[*] AS source;",
            options);
        using (var reader = new StreamReader(nested.Value.Content))
        using (var result = JsonDocument.Parse((await reader.ReadToEndAsync()).Trim()))
        {
            Assert.Equal(0.2D, result.RootElement.GetProperty("weight").GetDouble());
            Assert.Equal(-87.6D, result.RootElement.GetProperty("longitude").GetDouble());
            Assert.Equal("accessory", result.RootElement.GetProperty("tag").GetString());
            Assert.Equal(3, result.RootElement.GetProperty("length").GetInt64());
        }

        async Task<long> CountAsync(string predicate)
        {
            var response = await blob.QueryAsync(
                $"SELECT COUNT(*) AS value FROM BlobStorage[*].warehouses[*] WHERE {predicate};",
                options).ConfigureAwait(false);
            using var reader = new StreamReader(response.Value.Content);
            using var result = JsonDocument.Parse((await reader.ReadToEndAsync().ConfigureAwait(false)).Trim());
            return result.RootElement.GetProperty("value").GetInt64();
        }

        Assert.Equal(1, await CountAsync("longitude IS MISSING"));
        Assert.Equal(1, await CountAsync("longitude IS NULL"));
        Assert.Equal(2, await CountAsync("longitude IS NOT MISSING"));
    }

    [Fact]
    public async Task QueryBlobContentsSysSplitReturnsExactCsvRecordBatches()
    {
        const int mebibyte = 1024 * 1024;
        const int firstRecordLength = 6 * mebibyte;
        const int secondRecordLength = 6 * mebibyte;
        const int thirdRecordLength = mebibyte;
        var content = new byte[firstRecordLength + secondRecordLength + thirdRecordLength];
        Array.Fill(content, (byte)'a');
        content[firstRecordLength - 1] = (byte)'\n';

        var secondStart = firstRecordLength;
        Array.Fill(content, (byte)'b', secondStart, secondRecordLength);
        content[secondStart] = (byte)'"';
        content[secondStart + secondRecordLength / 2] = (byte)'\n';
        content[secondStart + secondRecordLength - 2] = (byte)'"';
        content[secondStart + secondRecordLength - 1] = (byte)'\n';

        var thirdStart = firstRecordLength + secondRecordLength;
        Array.Fill(content, (byte)'c', thirdStart, thirdRecordLength);
        content[^1] = (byte)'\n';

        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"split-query-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlockBlobClient("rows.csv");
        await blob.UploadAsync(new MemoryStream(content, writable: false));
        var options = new BlobQueryOptions
        {
            InputTextConfiguration = new BlobQueryCsvTextOptions
            {
                ColumnSeparator = ",",
                QuotationCharacter = '"',
                EscapeCharacter = '\\',
                RecordSeparator = "\n"
            },
            OutputTextConfiguration = new BlobQueryJsonTextOptions { RecordSeparator = "\n" }
        };

        var response = await blob.QueryAsync(
            "SELECT sys.split(10485760) AS bytes FROM BlobStorage;",
            options);
        using var reader = new StreamReader(response.Value.Content);
        var lines = (await reader.ReadToEndAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal(2, lines.Length);
        using var firstBatch = JsonDocument.Parse(lines[0]);
        using var secondBatch = JsonDocument.Parse(lines[1]);
        Assert.Equal(
            firstRecordLength + secondRecordLength,
            firstBatch.RootElement.GetProperty("bytes").GetInt64());
        Assert.Equal(thirdRecordLength, secondBatch.RootElement.GetProperty("bytes").GetInt64());

        var invalid = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.QueryAsync("SELECT sys.split(1024) FROM BlobStorage;", options));
        Assert.Equal(400, invalid.Status);
        Assert.Equal("InvalidQueryParameterValue", invalid.ErrorCode);
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory app) =>
        CreateClient(app, SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);

    private static async Task<int> ApplyAclManifestAsync(
        SavaWebApplicationFactory application,
        params HierarchicalAclManifestEntry[] entries)
    {
        var manifestPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-acl-{Guid.NewGuid():N}.json");
        try
        {
            var manifest = new HierarchicalAclManifest { SchemaVersion = 1, Entries = [.. entries] };
            await File.WriteAllTextAsync(manifestPath,
                JsonSerializer.Serialize(manifest, JsonSerializerOptions.Web)).ConfigureAwait(false);
            return await application.Services.GetRequiredService<BlobService>()
                .ApplyHierarchicalAclManifestAsync(manifestPath, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            File.Delete(manifestPath);
        }
    }

    private static byte[] EncodeStructuredBody(ReadOnlySpan<byte> content)
    {
        const int headerLength = 13;
        const int segmentHeaderLength = 10;
        const int checksumLength = 8;
        var encoded = new byte[headerLength + segmentHeaderLength + content.Length + checksumLength * 2];
        encoded[0] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(encoded.AsSpan(1), (ulong)encoded.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(encoded.AsSpan(9), 0x0001);
        BinaryPrimitives.WriteUInt16LittleEndian(encoded.AsSpan(11), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(encoded.AsSpan(headerLength), 1);
        BinaryPrimitives.WriteUInt64LittleEndian(encoded.AsSpan(headerLength + sizeof(ushort)), (ulong)content.Length);
        content.CopyTo(encoded.AsSpan(headerLength + segmentHeaderLength));

        var crc64 = new Mk8.Sava.Protocol.StorageCrc64();
        crc64.Append(content);
        var checksum = crc64.GetHash();
        checksum.CopyTo(encoded.AsSpan(headerLength + segmentHeaderLength + content.Length));
        checksum.CopyTo(encoded.AsSpan(encoded.Length - checksumLength));
        return encoded;
    }

    private static string StorageCrc64Base64(ReadOnlySpan<byte> content)
    {
        var crc64 = new Mk8.Sava.Protocol.StorageCrc64();
        crc64.Append(content);
        return Convert.ToBase64String(crc64.GetHash());
    }

    private static string ResponseHeader(Response response, string name)
    {
        Assert.True(response.Headers.TryGetValue(name, out var value), $"Missing response header: {name}");
        return value;
    }

    private static async Task CreateVersionOneDatabaseAsync(string dataPath, string containerName)
    {
        Directory.CreateDirectory(dataPath);
        var now = DateTimeOffset.UtcNow;
        var domain = SavaWebApplicationFactory.AccountName;
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new JsonStringEnumConverter());
        var container = new ContainerRecord
        {
            Account = SavaWebApplicationFactory.AccountName,
            Name = containerName,
            Revision = MetadataStore.NewRevision(),
            ETag = MetadataStore.NewETag(),
            CreatedAt = now,
            LastModified = now
        };
        var blob = new BlobRecord
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = containerName,
            Name = "sparse.bin",
            GenerationId = Guid.NewGuid().ToString("N"),
            Revision = MetadataStore.NewRevision(),
            IsCurrent = true,
            Kind = BlobKind.PageBlob,
            Content = new ContentManifest(
                domain,
                1024,
                ContentManifest.SparseHash,
                [new ChunkReference(domain + "/$zero", 0, 1024)]),
            ETag = MetadataStore.NewETag(),
            CreatedAt = now,
            LastModified = now,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["legacy"] = "indexed"
            }
        };
        var block = new StagedBlockRecord
        {
            Account = SavaWebApplicationFactory.AccountName,
            Container = containerName,
            BlobName = "staged.bin",
            BlockId = Convert.ToBase64String("migrated-block"u8),
            Content = new ContentManifest(
                domain,
                512,
                ContentManifest.SparseHash,
                [new ChunkReference(domain + "/$zero", 0, 512)]),
            CreatedAt = now
        };

        var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}");
        await using (connection.ConfigureAwait(false))
        {
            await connection.OpenAsync().ConfigureAwait(false);
            var schema = connection.CreateCommand();
            await using (schema.ConfigureAwait(false))
            {
                schema.CommandText = """
                CREATE TABLE containers (
                    account TEXT NOT NULL,
                    name TEXT NOT NULL,
                    deleted INTEGER NOT NULL,
                    modified_ticks INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (account, name)
                );
                CREATE TABLE blobs (
                    generation_id TEXT PRIMARY KEY,
                    account TEXT NOT NULL,
                    container TEXT NOT NULL,
                    name TEXT NOT NULL,
                    version_id TEXT NULL,
                    snapshot TEXT NULL,
                    is_current INTEGER NOT NULL,
                    is_deleted INTEGER NOT NULL,
                    modified_ticks INTEGER NOT NULL,
                    data TEXT NOT NULL
                );
                CREATE TABLE staged_blocks (
                    account TEXT NOT NULL,
                    container TEXT NOT NULL,
                    blob_name TEXT NOT NULL,
                    block_id TEXT NOT NULL,
                    created_ticks INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (account, container, blob_name, block_id)
                );
                CREATE TABLE service_properties (
                    account TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                );
                PRAGMA user_version=1;
                """;
                await schema.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            var insert = connection.CreateCommand();
            await using (insert.ConfigureAwait(false))
            {
                insert.CommandText = """
                INSERT INTO containers(account, name, deleted, modified_ticks, data)
                VALUES ($account, $name, 0, $modified, $data);
                INSERT INTO blobs(
                    generation_id, account, container, name, version_id, snapshot,
                    is_current, is_deleted, modified_ticks, data)
                VALUES ($generation, $account, $name, $blob, NULL, NULL, 1, 0, $modified, $blob_data);
                INSERT INTO staged_blocks(account, container, blob_name, block_id, created_ticks, data)
                VALUES ($account, $name, $staged_blob, $block, $modified, $block_data);
                """;
                insert.Parameters.AddWithValue("$account", SavaWebApplicationFactory.AccountName);
                insert.Parameters.AddWithValue("$name", containerName);
                insert.Parameters.AddWithValue("$modified", now.UtcTicks);
                insert.Parameters.AddWithValue("$data", JsonSerializer.Serialize(container, options));
                insert.Parameters.AddWithValue("$generation", blob.GenerationId);
                insert.Parameters.AddWithValue("$blob", blob.Name);
                insert.Parameters.AddWithValue("$blob_data", JsonSerializer.Serialize(blob, options));
                insert.Parameters.AddWithValue("$staged_blob", block.BlobName);
                insert.Parameters.AddWithValue("$block", block.BlockId);
                insert.Parameters.AddWithValue("$block_data", JsonSerializer.Serialize(block, options));
                await insert.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
    }

    private static async Task CreateVersionOneBackupAsync(string dataPath, string backupPath)
    {
        Directory.CreateDirectory(backupPath);
        Directory.CreateDirectory(Path.Combine(backupPath, "chunks"));
        var source = Path.Combine(dataPath, "metadata.db");
        var destination = Path.Combine(backupPath, "metadata.db");
        File.Copy(source, destination);
        var metadata = await File.ReadAllBytesAsync(destination).ConfigureAwait(false);
        var manifest = new
        {
            format = "mk8.sava.backup",
            formatVersion = 1,
            metadataSchemaVersion = 1,
            createdAt = DateTimeOffset.UtcNow,
            metadata = new
            {
                length = metadata.LongLength,
                sha256 = Convert.ToHexStringLower(SHA256.HashData(metadata))
            },
            blobRecordCount = 1,
            stagedBlockCount = 1,
            logicalBlobBytes = 1024,
            logicalStagedBlockBytes = 512,
            keyRequirements = new Dictionary<string, string>(StringComparer.Ordinal),
            chunks = Array.Empty<object>()
        };
        await File.WriteAllBytesAsync(
            Path.Combine(backupPath, "backup-manifest.json"),
            JsonSerializer.SerializeToUtf8Bytes(manifest, IndentedWebJsonOptions)).ConfigureAwait(false);
    }

    private static string ListIdentity(BlobRecord item) =>
        $"{item.Name}|{item.VersionId}|{item.Snapshot}|{(item.VersionId is null ? null : item.IsCurrent)}";

    private static string ListIdentity(BlobItem item) =>
        $"{item.Name}|{item.VersionId}|{item.Snapshot}|{item.IsLatestVersion}";

    private static HttpRequestMessage CreateLegacyCopyRequest(
        Uri destination,
        string sourcePath,
        string? sourceLeaseId = null)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, destination)
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", sourcePath);
        request.Headers.TryAddWithoutValidation("x-ms-date", DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture));
        request.Headers.TryAddWithoutValidation("x-ms-version", "2011-08-18");
        if (sourceLeaseId is not null)
            request.Headers.TryAddWithoutValidation("x-ms-source-lease-id", sourceLeaseId);

        AddSharedKeyLiteAuthorization(request);
        return request;
    }

    private static void AddSharedKeyLiteAuthorization(HttpRequestMessage request)
    {
        var requestUri = request.RequestUri
                         ?? throw new InvalidOperationException("A request URI is required for Shared Key Lite signing.");
        if (!request.Headers.Contains("x-ms-date"))
        {
            request.Headers.TryAddWithoutValidation(
                "x-ms-date",
                DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture));
        }

        var canonicalHeaders = new StringBuilder();
        foreach (var header in request.Headers
                     .Where(header => header.Key.StartsWith("x-ms-", StringComparison.OrdinalIgnoreCase))
                     .OrderBy(header => header.Key, StringComparer.OrdinalIgnoreCase))
        {
            canonicalHeaders
                // Azure Shared Key Lite signs canonical lowercase header names.
#pragma warning disable CA1308
                .Append(header.Key.ToLowerInvariant())
#pragma warning restore CA1308
                .Append(':')
                .AppendJoin(',', header.Value.Select(value => string.Join(
                    ' ',
                    value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))))
                .Append('\n');
        }

        var canonicalResource = new StringBuilder()
            .Append('/')
            .Append(SavaWebApplicationFactory.AccountName)
            .Append(requestUri.AbsolutePath);
        foreach (var parameter in Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(requestUri.Query)
                     .OrderBy(parameter => parameter.Key, StringComparer.OrdinalIgnoreCase))
        {
            canonicalResource
                .Append('\n')
                // Azure Shared Key Lite signs canonical lowercase query names.
#pragma warning disable CA1308
                .Append(parameter.Key.ToLowerInvariant())
#pragma warning restore CA1308
                .Append(':')
                .AppendJoin(',', parameter.Value.OrderBy(value => value, StringComparer.Ordinal));
        }
        var stringToSign = "\n" + canonicalHeaders + canonicalResource;
        using var hmac = new HMACSHA256(Convert.FromBase64String(SavaWebApplicationFactory.AccountKey));
        var signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
        request.Headers.TryAddWithoutValidation(
            "Authorization",
            $"SharedKeyLite {SavaWebApplicationFactory.AccountName}:{signature}");
    }

    private static HttpRequestMessage CreateSourceKeyRequest(
        Uri destination,
        byte[] sourceKey,
        byte[] sourceHash,
        string version = "2026-02-06",
        string sourceUri = "https://source.example/encrypted")
    {
        var request = new HttpRequestMessage(HttpMethod.Put, destination)
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", sourceUri);
        request.Headers.TryAddWithoutValidation("x-ms-source-encryption-key", Convert.ToBase64String(sourceKey));
        request.Headers.TryAddWithoutValidation("x-ms-source-encryption-key-sha256", Convert.ToBase64String(sourceHash));
        request.Headers.TryAddWithoutValidation("x-ms-source-encryption-algorithm", "AES256");
        return request;
    }

    private static void AddCustomerKeyHeaders(HttpRequestMessage request, byte[] key, byte[] hash)
    {
        request.Headers.TryAddWithoutValidation("x-ms-encryption-key", Convert.ToBase64String(key));
        request.Headers.TryAddWithoutValidation("x-ms-encryption-key-sha256", Convert.ToBase64String(hash));
        request.Headers.TryAddWithoutValidation("x-ms-encryption-algorithm", "AES256");
    }

    private static Uri HttpsSasUri(BlobBaseClient blob, BlobSasPermissions permissions)
    {
        var builder = new UriBuilder(blob.GenerateSasUri(permissions, DateTimeOffset.UtcNow.AddMinutes(10)))
        {
            Scheme = Uri.UriSchemeHttps,
            Port = -1
        };
        return builder.Uri;
    }

    private static Uri AppendQuery(Uri uri, string query)
    {
        var builder = new UriBuilder(uri)
        {
            Query = string.IsNullOrEmpty(uri.Query)
                ? query
                : uri.Query.TrimStart('?') + "&" + query
        };
        return builder.Uri;
    }

    private static HttpRequestMessage CreateLeaseRequest(
        Uri uri,
        string version,
        string action,
        string? leaseId = null,
        int? duration = null)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, uri)
        {
            Content = new ByteArrayContent([])
        };
        request.Headers.TryAddWithoutValidation("x-ms-version", version);
        request.Headers.TryAddWithoutValidation("x-ms-lease-action", action);
        if (leaseId is not null)
            request.Headers.TryAddWithoutValidation("x-ms-lease-id", leaseId);
        if (duration.HasValue)
        {
            request.Headers.TryAddWithoutValidation(
                "x-ms-lease-duration",
                duration.Value.ToString(CultureInfo.InvariantCulture));
        }
        return request;
    }

    private static string GetResponseHeader(HttpResponseMessage response, string name) =>
        GetResponseHeaderOrDefault(response, name)
        ?? throw new Xunit.Sdk.XunitException($"Missing response header: {name}");

    private static async Task AssertVersionedErrorAsync(HttpResponseMessage response, string expectedCode)
    {
        var version = GetResponseHeaderOrDefault(response, "x-ms-version");
        var hasErrorCodeHeader = version is not null &&
                                 DateOnly.TryParse(version, CultureInfo.InvariantCulture, out var parsed) &&
                                 parsed >= new DateOnly(2017, 7, 29);
        if (hasErrorCodeHeader)
            Assert.Equal(expectedCode, GetResponseHeader(response, "x-ms-error-code"));
        else
            Assert.False(response.Headers.Contains("x-ms-error-code"));

        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (response.RequestMessage?.Method == HttpMethod.Head || response.StatusCode == HttpStatusCode.NotModified)
            Assert.Empty(body);
        else
            Assert.Contains($"<Code>{expectedCode}</Code>", body, StringComparison.Ordinal);
    }

    private static string? GetResponseHeaderOrDefault(HttpResponseMessage response, string name)
    {
        if (response.Headers.TryGetValues(name, out var values))
            return values.Single();
        return response.Content.Headers.TryGetValues(name, out values) ? values.Single() : null;
    }

    private static string ReadQueryParameter(Uri uri, string name)
    {
        foreach (var pair in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var components = pair.Split('=', 2);
            if (string.Equals(Uri.UnescapeDataString(components[0]), name, StringComparison.Ordinal))
                return components.Length == 2 ? Uri.UnescapeDataString(components[1]) : string.Empty;
        }
        throw new InvalidOperationException($"The {name} query parameter is missing.");
    }

    private static BlobServiceClient CreateClient(SavaWebApplicationFactory app, string accountName, string accountKey)
    {
        var endpoint = new Uri($"http://{accountName}.localhost");
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(app.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        };
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(accountName, accountKey),
            options);
    }

    private static BlobClient CreateBlobClient(SavaWebApplicationFactory app, Uri uri) =>
        new(uri, new BlobClientOptions
        {
            Transport = new HttpClientTransport(app.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });

    private static BlobServiceClient CreateEncryptedClient(
        SavaWebApplicationFactory app,
        CustomerProvidedKey? customerProvidedKey,
        string? encryptionScope)
    {
        var endpoint = new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost");
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(app.Server.CreateHandler()),
            CustomerProvidedKey = customerProvidedKey,
            EncryptionScope = encryptionScope,
            Retry = { MaxRetries = 0 }
        };
        return new BlobServiceClient(
            endpoint,
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            options);
    }

    private static BlobServiceClient CreateBearerClient(SavaWebApplicationFactory app, string token)
    {
        var endpoint = new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost");
        return new BlobServiceClient(endpoint, new StaticTokenCredential(token), new BlobClientOptions
        {
            Transport = new HttpClientTransport(app.Server.CreateHandler()),
            Retry = { MaxRetries = 0 }
        });
    }

    private static string CreateJwt(
        string base64Key,
        string objectId,
        string? tenantId = null,
        IEnumerable<string>? groups = null)
    {
        var key = new SymmetricSecurityKey(Convert.FromBase64String(base64Key)) { KeyId = "test-key" };
        var claims = new List<Claim> { new("oid", objectId) };
        if (tenantId is not null)
            claims.Add(new Claim("tid", tenantId));
        if (groups is not null)
            claims.AddRange(groups.Select(group => new Claim("groups", group)));
        var token = new JwtSecurityToken(
            issuer: "https://issuer.mk8.test",
            audience: "https://storage.azure.com/",
            claims: claims,
            notBefore: DateTime.UtcNow.AddMinutes(-1),
            expires: DateTime.UtcNow.AddMinutes(10),
            signingCredentials: new SigningCredentials(key, SecurityAlgorithms.HmacSha256));
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private sealed class StaticTokenCredential(string token) : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new(token, DateTimeOffset.UtcNow.AddMinutes(10));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }

    private sealed class CaptureProgress : IProgress<long>
    {
        public List<long> Values { get; } = [];

        public void Report(long value) => Values.Add(value);
    }

    private sealed class AdjustableTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private long _utcTicks = utcNow.UtcDateTime.Ticks;

        public override DateTimeOffset GetUtcNow() =>
            new(Interlocked.Read(ref _utcTicks), TimeSpan.Zero);

        public void Advance(TimeSpan value) =>
            Interlocked.Add(ref _utcTicks, value.Ticks);
    }

    private static List<string> ParseAnalyticsLogFields(string record)
    {
        var fields = new List<string>();
        var field = new StringBuilder();
        var quoted = false;
        foreach (var character in record)
        {
            if (character == '"')
            {
                quoted = !quoted;
                continue;
            }
            if (character == ';' && !quoted)
            {
                fields.Add(WebUtility.HtmlDecode(field.ToString()));
                field.Clear();
                continue;
            }
            field.Append(character);
        }
        Assert.False(quoted);
        fields.Add(WebUtility.HtmlDecode(field.ToString()));
        return fields;
    }

    private sealed class DeclaredLengthContent(long length) : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
            Task.CompletedTask;

        protected override bool TryComputeLength(out long computedLength)
        {
            computedLength = length;
            return true;
        }
    }

    private static IEnumerable<FileInfo> EnumerateChunkFiles(string dataPath) =>
        Directory.EnumerateFiles(Path.Combine(dataPath, "chunks"), "*.chunk", SearchOption.AllDirectories)
            .Select(path => new FileInfo(path));

    private static IEnumerable<FileInfo> EnumeratePackFiles(string dataPath) =>
        Directory.EnumerateFiles(Path.Combine(dataPath, "packs"), "*.pack", SearchOption.AllDirectories)
            .Select(path => new FileInfo(path));

    private static string ChunkPath(string dataPath, string chunkId) =>
        Path.Combine(dataPath, "chunks", chunkId.Replace('/', Path.DirectorySeparatorChar) + ".chunk");

    private sealed class EncryptedSourceHandler(byte[] content, byte[] key, byte[] hash) : HttpMessageHandler
    {
        private readonly string _encodedKey = Convert.ToBase64String(key);
        private readonly string _encodedHash = Convert.ToBase64String(hash);
        private int _requestCount;

        public int RequestCount => Volatile.Read(ref _requestCount);

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(Uri.UriSchemeHttps, request.RequestUri?.Scheme);
            Assert.Equal(_encodedKey, request.Headers.GetValues("x-ms-encryption-key").Single());
            Assert.Equal(_encodedHash, request.Headers.GetValues("x-ms-encryption-key-sha256").Single());
            Assert.Equal("AES256", request.Headers.GetValues("x-ms-encryption-algorithm").Single());
            Assert.Equal("2026-02-06", request.Headers.GetValues("x-ms-version").Single());
            Assert.False(request.Headers.Contains("x-ms-source-encryption-key"));
            Interlocked.Increment(ref _requestCount);

            var start = 0;
            var end = content.Length - 1;
            var status = HttpStatusCode.OK;
            var range = request.Headers.Range?.Ranges.SingleOrDefault();
            if (range is not null)
            {
                start = checked((int)(range.From ?? 0));
                end = checked((int)(range.To ?? end));
                Assert.InRange(start, 0, content.Length - 1);
                Assert.InRange(end, start, content.Length - 1);
                status = HttpStatusCode.PartialContent;
            }

            var response = new HttpResponseMessage(status)
            {
                Content = new ByteArrayContent(content[start..(end + 1)]),
                RequestMessage = request
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            if (status == HttpStatusCode.PartialContent)
                response.Content.Headers.ContentRange = new ContentRangeHeaderValue(start, end, content.Length);
            response.Headers.ETag = new EntityTagHeaderValue("\"encrypted-source\"");
            return Task.FromResult(response);
        }
    }

    private sealed class FileIntentSourceHandler(byte[] content) : HttpMessageHandler
    {
        private int _requestCount;

        public int RequestCount => Volatile.Read(ref _requestCount);

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sourceUri = Assert.IsType<Uri>(request.RequestUri);
            if (string.Equals(sourceUri.AbsolutePath, "/share/anonymous.bin", StringComparison.Ordinal))
            {
                Assert.Equal("source.file.core.windows.net", sourceUri.Host);
                Assert.Null(request.Headers.Authorization);
                Assert.False(request.Headers.Contains("x-ms-file-request-intent"));
            }
            else if (string.Equals(sourceUri.Host, "source.blob.core.windows.net", StringComparison.Ordinal))
            {
                Assert.Equal("Bearer", request.Headers.Authorization?.Scheme);
                Assert.Equal("azure-files-source-token", request.Headers.Authorization?.Parameter);
                Assert.False(request.Headers.Contains("x-ms-file-request-intent"));
            }
            else
            {
                Assert.Equal("source.file.core.windows.net", sourceUri.Host);
                Assert.Equal("Bearer", request.Headers.Authorization?.Scheme);
                Assert.Equal("azure-files-source-token", request.Headers.Authorization?.Parameter);
                Assert.Equal("backup", request.Headers.GetValues("x-ms-file-request-intent").Single());
            }
            Assert.Equal("2025-07-05", request.Headers.GetValues("x-ms-version").Single());
            Interlocked.Increment(ref _requestCount);

            var start = 0;
            var end = content.Length - 1;
            var status = HttpStatusCode.OK;
            var range = request.Headers.Range?.Ranges.SingleOrDefault();
            if (range is not null)
            {
                start = checked((int)(range.From ?? 0));
                end = checked((int)(range.To ?? end));
                status = HttpStatusCode.PartialContent;
            }

            var response = new HttpResponseMessage(status)
            {
                Content = new ByteArrayContent(content[start..(end + 1)]),
                RequestMessage = request
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            if (status == HttpStatusCode.PartialContent)
                response.Content.Headers.ContentRange = new ContentRangeHeaderValue(start, end, content.Length);
            response.Headers.ETag = new EntityTagHeaderValue("\"file-source\"");
            return Task.FromResult(response);
        }
    }

    private sealed class LoopbackSource(
        WebApplication application,
        Uri uri,
        int[] sourceRequests,
        int[] redirectTargetRequests) : IAsyncDisposable
    {
        public Uri Uri { get; } = uri;
        public Uri MissingUri { get; } = new(uri, "/missing");
        public Uri RedirectUri { get; } = new(uri, "/redirect");
        public int SourceRequestCount => Volatile.Read(ref sourceRequests[0]);
        public int RedirectTargetRequests => Volatile.Read(ref redirectTargetRequests[0]);

        public static async Task<LoopbackSource> StartAsync(byte[] content)
        {
            var sourceRequests = new int[1];
            var redirectTargetRequests = new int[1];
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, 0));
            var application = builder.Build();
            application.MapGet("/source", context => WriteSourceContentAsync(context, content, sourceRequests));
            application.MapGet("/missing", context =>
                WriteSourceErrorAsync(context, StatusCodes.Status404NotFound, "BlobNotFound"));
            application.MapGet("/redirect", context =>
            {
                context.Response.Redirect("/redirect-target");
                return Task.CompletedTask;
            });
            application.MapGet("/redirect-target", async context =>
            {
                Interlocked.Increment(ref redirectTargetRequests[0]);
                context.Response.ContentLength = content.Length;
                await context.Response.Body.WriteAsync(content).ConfigureAwait(false);
            });
            await application.StartAsync().ConfigureAwait(false);
            var addresses = application.Services
                .GetRequiredService<IServer>()
                .Features
                .Get<IServerAddressesFeature>()
                ?.Addresses;
            var address = addresses?.Single() ?? throw new InvalidOperationException("The source server did not publish an address.");
            return new LoopbackSource(
                application,
                new Uri(new Uri(address), "/source"),
                sourceRequests,
                redirectTargetRequests);
        }

        private static async Task WriteSourceContentAsync(
            HttpContext context, byte[] content, int[] sourceRequests)
        {
            Interlocked.Increment(ref sourceRequests[0]);
            const string sourceEtag = "\"source-etag\"";
            var ifMatch = context.Request.Headers.IfMatch.ToString();
            if (!string.IsNullOrEmpty(ifMatch) &&
                !ifMatch.Split(',', StringSplitOptions.TrimEntries).Any(value => value is "*" or sourceEtag))
            {
                await WriteSourceErrorAsync(context, StatusCodes.Status412PreconditionFailed, "ConditionNotMet").ConfigureAwait(false);
                return;
            }
            var ifNoneMatch = context.Request.Headers.IfNoneMatch.ToString();
            if (!string.IsNullOrEmpty(ifNoneMatch) &&
                ifNoneMatch.Split(',', StringSplitOptions.TrimEntries).Any(value => value is "*" or sourceEtag))
            {
                context.Response.StatusCode = StatusCodes.Status304NotModified;
                return;
            }

            var start = 0;
            var end = content.Length - 1;
            var range = context.Request.Headers.Range.ToString();
            if (!string.IsNullOrEmpty(range))
            {
                var bounds = range[6..].Split('-', 2);
                start = int.Parse(bounds[0], CultureInfo.InvariantCulture);
                end = string.IsNullOrEmpty(bounds[1])
                    ? end
                    : int.Parse(bounds[1], CultureInfo.InvariantCulture);
                if (start < 0 || end < start || end >= content.Length)
                {
                    context.Response.StatusCode = StatusCodes.Status416RangeNotSatisfiable;
                    return;
                }
                context.Response.StatusCode = StatusCodes.Status206PartialContent;
                context.Response.Headers.ContentRange = $"bytes {start}-{end}/{content.Length}";
            }
            context.Response.ContentType = "application/x-url-source";
            context.Response.ContentLength = end - start + 1;
            context.Response.Headers.ETag = sourceEtag;
            await context.Response.Body.WriteAsync(content.AsMemory(start, end - start + 1)).ConfigureAwait(false);
        }

        private static async Task WriteSourceErrorAsync(HttpContext context, int statusCode, string errorCode)
        {
            var message = string.Equals(errorCode, "BlobNotFound"
, StringComparison.Ordinal) ? "The specified blob does not exist."
                : "The condition specified using HTTP conditional header(s) is not met.";
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "application/xml";
            context.Response.Headers["x-ms-error-code"] = errorCode;
            await context.Response.WriteAsync(
                $"<Error><Code>{errorCode}</Code><Message>{message}</Message></Error>").ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            await application.StopAsync().ConfigureAwait(false);
            await application.DisposeAsync().ConfigureAwait(false);
        }
    }
}
