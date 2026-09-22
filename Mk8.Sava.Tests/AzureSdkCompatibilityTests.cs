using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using System.Buffers.Binary;
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
    [Fact]
    public async Task PathStyleServiceEndpointAcceptsTerminalAccountSeparator()
    {
        var endpoint = new Uri($"http://localhost/{SavaWebApplicationFactory.AccountName}");
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(new HttpClient(factory.Server.CreateHandler())
            {
                BaseAddress = endpoint
            }),
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
        var created = await container.CreateAsync(PublicAccessType.None, new Dictionary<string, string> { ["scope"] = "tests" });
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
            Metadata = new Dictionary<string, string> { ["owner"] = "compatibility" },
            Tags = new Dictionary<string, string> { ["kind"] = "fixture" }
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
        Assert.Contains("folder/blob.bin", names);
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
            Metadata = new Dictionary<string, string>
            {
                ["Original"] = "preserved",
                ["_leading"] = "allowed"
            }
        });

        foreach (var invalidName in new[] { "1starts_with_digit", "contains-hyphen", "contains.dot" })
        {
            var invalid = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.SetMetadataAsync(new Dictionary<string, string> { [invalidName] = "value" }));
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
        await blob.SetMetadataAsync(new Dictionary<string, string> { ["k"] = maximumValue });
        var maximumProperties = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(maximumValue, maximumProperties.Metadata["k"]);

        var tooLarge = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(new Dictionary<string, string> { ["k"] = maximumValue + "x" }));
        Assert.Equal(400, tooLarge.Status);
        Assert.Equal("MetadataTooLarge", tooLarge.ErrorCode);
        var afterTooLarge = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(maximumProperties.ETag, afterTooLarge.ETag);
        Assert.Equal(maximumValue, afterTooLarge.Metadata["k"]);

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
            using var response = await transport.SendAsync(duplicateRequest);
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
            using var response = await transport.SendAsync(nonAsciiRequest);
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
            using var response = await transport.SendAsync(nonemptyMetadata);
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
            using var response = await transport.SendAsync(nonemptyProperties);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var containerMetadataUri = AppendQuery(
            container.Uri,
            "restype=container&comp=metadata");
        using (var nonemptyContainerMetadata = new HttpRequestMessage(HttpMethod.Put, containerMetadataUri)
        {
            Content = new ByteArrayContent([1])
        })
        {
            nonemptyContainerMetadata.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            nonemptyContainerMetadata.Headers.TryAddWithoutValidation("x-ms-meta-k", "must-not-publish");
            AddSharedKeyLiteAuthorization(nonemptyContainerMetadata);
            using var response = await transport.SendAsync(nonemptyContainerMetadata);
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
            using var response = await transport.SendAsync(validContainerMetadata);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        Assert.Equal(maximumProperties.ETag, (await blob.GetPropertiesAsync()).Value.ETag);
        Assert.False((await container.GetPropertiesAsync()).Value.Metadata.ContainsKey("k"));
        Assert.Equal("allowed", (await container.GetPropertiesAsync()).Value.Metadata["fromowner"]);

        var invalidContainer = service.GetBlobContainerClient($"metadata-invalid-{Guid.NewGuid():N}");
        var invalidContainerCreate = await Assert.ThrowsAsync<RequestFailedException>(() =>
            invalidContainer.CreateAsync(
                PublicAccessType.None,
                new Dictionary<string, string> { ["invalid-name"] = "value" }));
        Assert.Equal("InvalidMetadata", invalidContainerCreate.ErrorCode);
        Assert.False((await invalidContainer.ExistsAsync()).Value);

        var invalidUpload = container.GetBlobClient("invalid-upload.bin");
        var invalidBlobCreate = await Assert.ThrowsAsync<RequestFailedException>(() =>
            invalidUpload.UploadAsync(BinaryData.FromString("must not publish"), new BlobUploadOptions
            {
                Metadata = new Dictionary<string, string> { ["invalid-name"] = "value" }
            }));
        Assert.Equal("InvalidMetadata", invalidBlobCreate.ErrorCode);
        Assert.False((await invalidUpload.ExistsAsync()).Value);

        var invalidSnapshot = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.CreateSnapshotAsync(
                new Dictionary<string, string> { ["invalid-name"] = "value" }));
        Assert.Equal("InvalidMetadata", invalidSnapshot.ErrorCode);
        var snapshots = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions { States = BlobStates.Snapshots }))
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
            new Dictionary<string, string> { ["scope"] = "container" });
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
            new Dictionary<string, string> { ["scope"] = "updated" });
        Assert.False(setContainerMetadata.GetRawResponse().Headers.TryGetValue("x-ms-meta-scope", out _));
        Assert.False(setContainerMetadata.GetRawResponse().Headers.TryGetValue("x-ms-lease-state", out _));

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
        var containerMetadataUri = AppendQuery(
            container.Uri,
            $"restype=container&comp=metadata&{accountSas.ToSasQueryParameters(credential)}");

        var blob = container.GetBlobClient("metadata.bin");
        await blob.UploadAsync(
            BinaryData.FromString("metadata response payload"),
            new BlobUploadOptions
            {
                Metadata = new Dictionary<string, string> { ["owner"] = "mk8" },
                Tags = new Dictionary<string, string> { ["class"] = "response" },
                HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-response-test" }
            });
        var setBlobMetadata = await blob.SetMetadataAsync(
            new Dictionary<string, string> { ["owner"] = "sava" });
        var setBlobHeaders = setBlobMetadata.GetRawResponse().Headers;
        Assert.True(setBlobHeaders.TryGetValue("x-ms-request-server-encrypted", out var encrypted));
        Assert.Equal("true", encrypted);
        Assert.False(setBlobHeaders.TryGetValue("x-ms-meta-owner", out _));
        Assert.False(setBlobHeaders.TryGetValue("x-ms-blob-type", out _));
        Assert.False(setBlobHeaders.TryGetValue("x-ms-lease-status", out _));

        var blobMetadataUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=metadata");
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
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(metadataValue, response.Headers.GetValues(metadataName).Single());
            Assert.False(response.Headers.Contains("x-ms-blob-type"));
            Assert.False(response.Headers.Contains("x-ms-lease-status"));
            Assert.False(response.Headers.Contains("x-ms-lease-state"));
            Assert.False(response.Headers.Contains("x-ms-server-encrypted"));
            Assert.False(response.Headers.Contains("x-ms-tag-count"));
            Assert.Empty(await response.Content.ReadAsByteArrayAsync());
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
            ["%41"] = "literal escape",
            ["A"] = "decoded character",
            ["reserved ?#% ü.bin"] = "reserved and unicode"
        };

        foreach (var pair in expected)
            await container.GetBlobClient(pair.Key).UploadAsync(BinaryData.FromString(pair.Value));

        foreach (var pair in expected)
        {
            var content = await container.GetBlobClient(pair.Key).DownloadContentAsync();
            Assert.Equal(pair.Value, content.Value.Content.ToString());
        }

        async Task PutAndReadDirectAsync(string blobName, string escapedBlobPath, string value)
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
            using var putResponse = await transport.SendAsync(put);
            Assert.Equal(HttpStatusCode.Created, putResponse.StatusCode);

            using var getResponse = await transport.GetAsync(uri);
            Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
            Assert.Equal(value, await getResponse.Content.ReadAsStringAsync());
        }

        await PutAndReadDirectAsync("/a", "/a", "leading separator");
        await PutAndReadDirectAsync("tail/", "tail/", "trailing separator");
        expected.Add("/a", "leading separator");
        expected.Add("tail/", "trailing separator");

        var listed = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var item in container.GetBlobsAsync())
            listed.Add(item.Name);
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), listed.Order(StringComparer.Ordinal));

        var root = service.GetBlobContainerClient("$root");
        await root.CreateIfNotExistsAsync();
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
            await implicitRoot.UploadAsync(BinaryData.FromString("root payload"));
        }
        Assert.Equal(
            "root payload",
            (await root.GetBlobClient(rootName).DownloadContentAsync()).Value.Content.ToString());

        var maximumUnicodeName = new string('é', 1024);
        await container.GetBlobClient(maximumUnicodeName).UploadAsync(BinaryData.FromString("maximum name"));
        Assert.Equal(
            "maximum name",
            (await container.GetBlobClient(maximumUnicodeName).DownloadContentAsync()).Value.Content.ToString());

        var oversizedName = new string('é', 1025);
        var oversized = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient(oversizedName).UploadAsync(BinaryData.FromString("must not publish")));
        Assert.Equal(400, oversized.Status);
        Assert.Equal("InvalidResourceName", oversized.ErrorCode);

        var maximumSegments = string.Join('/', Enumerable.Repeat("s", 254));
        await container.GetBlobClient(maximumSegments).UploadAsync(BinaryData.FromString("maximum segments"));
        Assert.Equal(
            "maximum segments",
            (await container.GetBlobClient(maximumSegments).DownloadContentAsync()).Value.Content.ToString());

        var tooManySegments = string.Join('/', Enumerable.Repeat("s", 255));
        var tooMany = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient(tooManySegments).UploadAsync(BinaryData.FromString("must not publish")));
        Assert.Equal(400, tooMany.Status);
        Assert.Equal("InvalidResourceName", tooMany.ErrorCode);
    }

    [Fact]
    public async Task IndexedTagQueriesAreBoundedScopedAndTransactionallyVerified()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
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

            async Task UploadTaggedAsync(BlobContainerClient container, string name, string project, string rank)
            {
                await container.GetBlobClient(name).UploadAsync(
                    BinaryData.FromString(name),
                    new BlobUploadOptions
                    {
                        Tags = new Dictionary<string, string>
                        {
                            ["project"] = project,
                            ["rank"] = rank,
                            ["unselected"] = "not returned"
                        }
                    });
            }

            await UploadTaggedAsync(firstContainer, "a", token, "010");
            await UploadTaggedAsync(firstContainer, "b", token, "050");
            await UploadTaggedAsync(firstContainer, "c", token, "150");
            await UploadTaggedAsync(secondContainer, "d", token, "075");
            await UploadTaggedAsync(secondContainer, "e", "another-project", "020");

            var expression = $"\"project\" = '{token}' AND rank >= '010' AND rank < '100'";
            var matches = new List<TaggedBlobItem>();
            var markers = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var page in service.FindBlobsByTagsAsync(expression).AsPages(pageSizeHint: 1))
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
                matches.Select(item => $"{item.BlobContainerName}/{item.BlobName}"));
            Assert.Equal(2, markers.Count);
            Assert.All(matches, item =>
            {
                Assert.Equal(2, item.Tags.Count);
                Assert.Equal(token, item.Tags["project"]);
                Assert.False(item.Tags.ContainsKey("unselected"));
            });

            var scoped = new List<string>();
            await foreach (var item in firstContainer.FindBlobsByTagsAsync($"\"project\" = '{token}'"))
                scoped.Add(item.BlobName);
            Assert.Equal(["a", "b", "c"], scoped);

            await firstContainer.GetBlobClient("b").SetTagsAsync(new Dictionary<string, string>
            {
                ["project"] = "changed",
                ["rank"] = "050"
            });
            var afterUpdate = new List<string>();
            await foreach (var item in service.FindBlobsByTagsAsync(expression))
                afterUpdate.Add($"{item.BlobContainerName}/{item.BlobName}");
            Assert.Equal([$"{firstContainer.Name}/a", $"{secondContainer.Name}/d"], afterUpdate);

            var invalidExpression = await Assert.ThrowsAsync<RequestFailedException>(async () =>
            {
                await foreach (var _ in service.FindBlobsByTagsAsync(
                                   $"\"project\" > '{token}' AND \"project\" >= '{token}'"))
                {
                }
            });
            Assert.Equal(400, invalidExpression.Status);

            var invalidTag = await Assert.ThrowsAsync<RequestFailedException>(() =>
                firstContainer.GetBlobClient("a").SetTagsAsync(
                    new Dictionary<string, string> { ["bad?"] = "value" }));
            Assert.Equal(400, invalidTag.Status);
            Assert.Equal("InvalidTag", invalidTag.ErrorCode);

            var directConnectionString = new SqliteConnectionStringBuilder
            {
                DataSource = Path.Combine(application.DataPath, "metadata.db"),
                ForeignKeys = true
            }.ToString();
            await using (var connection = new SqliteConnection(directConnectionString))
            {
                await connection.OpenAsync();
                await using var corruptIndex = connection.CreateCommand();
                corruptIndex.CommandText = "DELETE FROM blob_tags WHERE tag_key = 'project';";
                Assert.True(await corruptIndex.ExecuteNonQueryAsync() > 0);
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
                Tags = new Dictionary<string, string>
                {
                    ["Status"] = "Done",
                    ["Priority"] = "07",
                    ["special key"] = "yes"
                }
            });

        var successfulRead = await source.DownloadContentAsync(new BlobDownloadOptions
        {
            Conditions = new BlobRequestConditions
            {
                TagConditions = "(Status <> 'Pending' AND Priority >= '05') OR \"special key\" = 'no'"
            }
        });
        Assert.Equal("conditioned", successfulRead.Value.Content.ToString());

        var metadata = await source.SetMetadataAsync(
            new Dictionary<string, string> { ["condition"] = "passed" },
            new BlobRequestConditions
            {
                TagConditions = "Status = 'Pending' AND Priority >= '05' OR \"special key\" = 'yes'"
            });
        Assert.Equal(200, metadata.GetRawResponse().Status);

        var currentEtag = (await source.GetPropertiesAsync()).Value.ETag;
        var conditionedTags = await source.GetTagsAsync(new BlobRequestConditions { IfMatch = currentEtag });
        Assert.Equal("Done", conditionedTags.Value.Tags["Status"]);
        var rejectedTagRead = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.GetTagsAsync(new BlobRequestConditions { IfMatch = new ETag("\"not-the-current-etag\"") }));
        Assert.Equal(412, rejectedTagRead.Status);
        Assert.Equal("ConditionNotMet", rejectedTagRead.ErrorCode);
        await source.SetTagsAsync(
            new Dictionary<string, string>
            {
                ["Status"] = "Done",
                ["Priority"] = "07",
                ["special key"] = "yes"
            },
            new BlobRequestConditions { IfMatch = currentEtag });
        var rejectedTagWrite = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.SetTagsAsync(
                new Dictionary<string, string> { ["Status"] = "rejected" },
                new BlobRequestConditions { IfMatch = new ETag("\"not-the-current-etag\"") }));
        Assert.Equal(412, rejectedTagWrite.Status);
        Assert.Equal("ConditionNotMet", rejectedTagWrite.ErrorCode);

        var falseCondition = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions
                {
                    TagConditions = "(Status = 'Pending' OR Priority < '05') AND \"special key\" = 'yes'"
                }
            }));
        Assert.Equal(412, falseCondition.Status);
        Assert.Equal("ConditionNotMet", falseCondition.ErrorCode);

        var missingTagIsNotUnequal = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions { TagConditions = "Missing <> 'value'" }
            }));
        Assert.Equal(412, missingTagIsNotUnequal.Status);

        var invalidCondition = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions { TagConditions = "Status = 'Done' OR OR Priority = '07'" }
            }));
        Assert.Equal(400, invalidCondition.Status);
        Assert.Equal("InvalidHeaderValue", invalidCondition.ErrorCode);

        var excessiveCondition = string.Join(
            " OR ",
            Enumerable.Range(0, 12).Select(index => $"Status = 'value-{index}'"));
        var excessiveOperations = await Assert.ThrowsAsync<RequestFailedException>(() =>
            source.DownloadContentAsync(new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions { TagConditions = excessiveCondition }
            }));
        Assert.Equal(400, excessiveOperations.Status);
        Assert.Equal("InvalidHeaderValue", excessiveOperations.ErrorCode);

        var destination = container.GetBlobClient("destination.txt");
        var copy = await destination.StartCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions
            {
                SourceConditions = new BlobRequestConditions
                {
                    TagConditions = "Status = 'Done' AND Priority <= '07'"
                }
            });
        Assert.Equal(202, copy.GetRawResponse().Status);

        var rejectedCopy = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("rejected-copy.txt").StartCopyFromUriAsync(
                source.Uri,
                new BlobCopyFromUriOptions
                {
                    SourceConditions = new BlobRequestConditions { TagConditions = "Status = 'Pending'" }
                }));
        Assert.Equal(412, rejectedCopy.Status);
        Assert.Equal("SourceConditionNotMet", rejectedCopy.ErrorCode);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        var readOnlyUri = source.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var unauthorizedRequest = new HttpRequestMessage(HttpMethod.Get, readOnlyUri);
        unauthorizedRequest.Headers.Add("x-ms-version", "2023-11-03");
        unauthorizedRequest.Headers.Add("x-ms-if-tags", "Status = 'Done'");
        using var unauthorizedResponse = await transport.SendAsync(unauthorizedRequest);
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
        using var authorizedResponse = await transport.SendAsync(authorizedRequest);
        Assert.Equal(HttpStatusCode.OK, authorizedResponse.StatusCode);

        var oldVersionTagsUri = AppendQuery(
            source.GenerateSasUri(
                BlobSasPermissions.Read | BlobSasPermissions.Tag,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=tags");
        using var oldVersionCondition = new HttpRequestMessage(HttpMethod.Get, oldVersionTagsUri);
        oldVersionCondition.Headers.Add("x-ms-version", "2023-11-03");
        oldVersionCondition.Headers.Add("x-ms-blob-if-match", currentEtag.ToString());
        using var oldVersionConditionResponse = await transport.SendAsync(oldVersionCondition);
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
        using var badChecksumTagsResponse = await transport.SendAsync(badChecksumTags);
        Assert.Equal(HttpStatusCode.BadRequest, badChecksumTagsResponse.StatusCode);
        Assert.Equal("Crc64Mismatch", badChecksumTagsResponse.Headers.GetValues("x-ms-error-code").Single());
        Assert.Equal("Done", (await source.GetTagsAsync()).Value.Tags["Status"]);
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
            var properties = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                properties with { VersioningEnabled = true },
                CancellationToken.None);

            var versioned = container.GetBlobClient("paged/same-name.txt");
            await versioned.UploadAsync(BinaryData.FromString("one"), overwrite: true);
            await versioned.UploadAsync(BinaryData.FromString("two"), overwrite: true);
            await versioned.UploadAsync(BinaryData.FromString("three"), overwrite: true);
            await versioned.CreateSnapshotAsync();
            await versioned.CreateSnapshotAsync();

            var expectedRecords = (await metadata.ListBlobsAsync(
                    SavaWebApplicationFactory.AccountName,
                    containerName,
                    includeVersions: true,
                    includeSnapshots: true,
                    includeDeleted: false,
                    CancellationToken.None))
                .Where(item => item.Name == versioned.Name)
                .ToArray();
            var listed = new List<BlobItem>();
            var flatTokens = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var page in container
                               .GetBlobsAsync(new GetBlobsOptions
                               {
                                   States = BlobStates.Version | BlobStates.Snapshots,
                                   Prefix = versioned.Name
                               })
                               .AsPages(pageSizeHint: 1))
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
            using (var transport = new HttpClient(application.Server.CreateHandler()))
            using (var response = await transport.GetAsync(reboundMarkerUri))
            {
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidQueryParameterValue", response.Headers.GetValues("x-ms-error-code").Single());
            }

            await container.GetBlobClient("folders/a/one").UploadAsync(BinaryData.FromString("a1"));
            await container.GetBlobClient("folders/a/two").UploadAsync(BinaryData.FromString("a2"));
            await container.GetBlobClient("folders/b/one").UploadAsync(BinaryData.FromString("b1"));
            await container.GetBlobClient("folders/root").UploadAsync(BinaryData.FromString("root"));
            var hierarchy = new List<string>();
            var hierarchyTokens = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var page in container
                               .GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
                               {
                                   Delimiter = "/",
                                   Prefix = "folders/"
                               })
                               .AsPages(pageSizeHint: 1))
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
            }))
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
            }))
            {
                startedHierarchy.Add(item.IsPrefix ? $"P:{item.Prefix}" : $"B:{item.Blob.Name}");
            }
            Assert.Equal(["P:folders/b/", "B:folders/root"], startedHierarchy);

            var livePrefix = $"live-{Guid.NewGuid():N}-";
            await container.GetBlobClient(livePrefix + "b").UploadAsync(BinaryData.FromString("b"));
            await container.GetBlobClient(livePrefix + "d").UploadAsync(BinaryData.FromString("d"));
            Page<BlobItem>? firstLivePage = null;
            await foreach (var page in container
                               .GetBlobsAsync(new GetBlobsOptions { Prefix = livePrefix })
                               .AsPages(pageSizeHint: 1))
            {
                firstLivePage = page;
                break;
            }
            Assert.NotNull(firstLivePage);
            Assert.Equal(livePrefix + "b", Assert.Single(firstLivePage!.Values).Name);
            Assert.NotNull(firstLivePage.ContinuationToken);
            await container.GetBlobClient(livePrefix + "a").UploadAsync(BinaryData.FromString("a"));
            await container.GetBlobClient(livePrefix + "c").UploadAsync(BinaryData.FromString("c"));
            var resumedLiveNames = new List<string>();
            await foreach (var page in container
                               .GetBlobsAsync(new GetBlobsOptions { Prefix = livePrefix })
                               .AsPages(firstLivePage.ContinuationToken, pageSizeHint: 1))
            {
                resumedLiveNames.AddRange(page.Values.Select(item => item.Name));
            }
            Assert.Equal([livePrefix + "c", livePrefix + "d"], resumedLiveNames);

            var containerPrefix = $"listed-{Guid.NewGuid():N}-";
            var expectedContainers = Enumerable.Range(0, 3)
                .Select(index => containerPrefix + index)
                .ToArray();
            foreach (var name in expectedContainers)
                await service.GetBlobContainerClient(name).CreateAsync();
            var listedContainers = new List<string>();
            var containerTokens = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var page in service
                               .GetBlobContainersAsync(prefix: containerPrefix)
                               .AsPages(pageSizeHint: 1))
            {
                Assert.Single(page.Values);
                listedContainers.Add(page.Values[0].Name);
                if (!string.IsNullOrEmpty(page.ContinuationToken))
                    Assert.True(containerTokens.Add(page.ContinuationToken));
            }
            Assert.Equal(expectedContainers, listedContainers);
            Assert.Equal(2, containerTokens.Count);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task ApacheArrowListingsHonorSchemaRangesHierarchyAndScopedPaging()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"arrow-{Guid.NewGuid():N}");
        await container.CreateAsync();
        await container.GetBlobClient("a.txt").UploadAsync(BinaryData.FromString("a"));
        await container.GetBlobClient("b.txt").UploadAsync(
            BinaryData.FromString("b"),
            new BlobUploadOptions
            {
                HttpHeaders = new BlobHttpHeaders { ContentType = "text/x-arrow-fixture" },
                Metadata = new Dictionary<string, string> { ["owner"] = "arrow" },
                Tags = new Dictionary<string, string> { ["kind"] = "boundary" },
                AccessTier = AccessTier.Cool
            });
        await container.GetBlobClient("c/one.txt").UploadAsync(BinaryData.FromString("c1"));
        await container.GetBlobClient("c/two.txt").UploadAsync(BinaryData.FromString("c2"));
        await container.GetBlobClient("d.txt").UploadAsync(BinaryData.FromString("d"));

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
                           .AsPages(pageSizeHint: 1))
        {
            Assert.Single(page.Values);
            listed.Add(page.Values[0]);
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                continuationTokens.Add(page.ContinuationToken);
        }
        Assert.Equal(["b.txt", "c/one.txt", "c/two.txt"], listed.Select(item => item.Name));
        Assert.Equal("arrow", listed[0].Metadata["owner"]);
        Assert.Equal("boundary", listed[0].Tags["kind"]);
        Assert.Equal("text/x-arrow-fixture", listed[0].Properties.ContentType);
        Assert.Equal(AccessTier.Cool, listed[0].Properties.AccessTier);
        Assert.False(listed[0].Properties.AccessTierInferred);
        Assert.True((await container.GetBlobClient("a.txt").GetPropertiesAsync()).Value.AccessTierInferred);
        Assert.Equal(2, continuationTokens.Count);
        Assert.Equal(2, continuationTokens.Distinct(StringComparer.Ordinal).Count());

        var hierarchy = new List<string>();
        await foreach (var item in container.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
        {
            Delimiter = "/",
            ResponseFormat = StorageResponseFormat.Arrow,
            StartFrom = "b.txt",
            EndBefore = "d.txt"
        }))
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
        }))
        {
            empty.Add(item);
        }
        Assert.Empty(empty);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        var listSas = container.GenerateSasUri(
            BlobContainerSasPermissions.List,
            DateTimeOffset.UtcNow.AddMinutes(5));
        var arrowUri = AppendQuery(
            listSas,
            "restype=container&comp=list&include=metadata%2Ctags&startfrom=b.txt&endbefore=d.txt&maxresults=2");
        using (var arrowRequest = new HttpRequestMessage(HttpMethod.Get, arrowUri))
        {
            arrowRequest.Headers.Add("x-ms-version", "2026-06-06");
            arrowRequest.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(AzureResponseWriter.ArrowStreamContentType));
            using var arrowResponse = await transport.SendAsync(
                arrowRequest,
                HttpCompletionOption.ResponseHeadersRead);
            Assert.Equal(HttpStatusCode.OK, arrowResponse.StatusCode);
            Assert.Equal(AzureResponseWriter.ArrowStreamContentType, arrowResponse.Content.Headers.ContentType?.MediaType);
            await using var responseStream = await arrowResponse.Content.ReadAsStreamAsync();
            using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(responseStream);
            Assert.Equal("2", reader.Schema.Metadata["NumberOfRecords"]);
            Assert.False(string.IsNullOrEmpty(reader.Schema.Metadata["NextMarker"]));
            Assert.False(reader.Schema["Name"].IsNullable);
            Assert.False(reader.Schema["ResourceType"].IsNullable);
            Assert.Equal(
                Apache.Arrow.Types.TimeUnit.Second,
                Assert.IsType<Apache.Arrow.Types.TimestampType>(reader.Schema["Creation-Time"].DataType).Unit);
            using var batch = await reader.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(2, batch.Length);
            var names = Assert.IsType<Apache.Arrow.StringArray>(batch.Column("Name"));
            Assert.Equal("b.txt", names.GetString(0));
            Assert.Equal("c/one.txt", names.GetString(1));
            var inferred = Assert.IsType<Apache.Arrow.BooleanArray>(batch.Column("AccessTierInferred"));
            Assert.True(inferred.IsNull(0));
            Assert.True(inferred.GetValue(1));
            Assert.NotNull(batch.Column("Metadata"));
            Assert.NotNull(batch.Column("Tags"));
            Assert.Null(await reader.ReadNextRecordBatchAsync());
        }

        var reboundUri = AppendQuery(
            listSas,
            "restype=container&comp=list&startfrom=b.txt&endbefore=c%2Ftwo.txt&maxresults=1" +
            $"&marker={Uri.EscapeDataString(continuationTokens[0])}");
        using (var reboundRequest = new HttpRequestMessage(HttpMethod.Get, reboundUri))
        {
            reboundRequest.Headers.Add("x-ms-version", "2026-06-06");
            reboundRequest.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(AzureResponseWriter.ArrowStreamContentType));
            using var reboundResponse = await transport.SendAsync(reboundRequest);
            Assert.Equal(HttpStatusCode.BadRequest, reboundResponse.StatusCode);
            Assert.Equal("InvalidQueryParameterValue", reboundResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        async Task AssertRejectedAsync(
            Uri uri,
            string version,
            bool arrow,
            HttpStatusCode expectedStatus = HttpStatusCode.BadRequest)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.Add("x-ms-version", version);
            if (arrow)
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(AzureResponseWriter.ArrowStreamContentType));
            using var response = await transport.SendAsync(request);
            Assert.Equal(expectedStatus, response.StatusCode);
            Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        }

        await AssertRejectedAsync(
            arrowUri,
            "2026-04-06",
            arrow: true,
            expectedStatus: HttpStatusCode.Conflict);
        await AssertRejectedAsync(
            AppendQuery(listSas, "restype=container&comp=list&endbefore=d.txt"),
            "2026-06-06",
            arrow: false);
        await AssertRejectedAsync(
            AppendQuery(listSas, "restype=container&comp=list&startfrom=d.txt&endbefore=b.txt"),
            "2026-06-06",
            arrow: true);
    }

    [Fact]
    public async Task UncommittedBlobsAreListedPagedAndDiscardedByReplacementOrDeletion()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"uncommitted-{Guid.NewGuid():N}");
        await container.CreateAsync();
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
                new MemoryStream(Encoding.UTF8.GetBytes(name)));
        }

        var committed = container.GetBlockBlobClient(prefix + "committed.bin");
        await committed.UploadAsync(new MemoryStream("committed"u8.ToArray()));
        await committed.StageBlockAsync(
            Convert.ToBase64String("uncommitted-0002"u8),
            new MemoryStream("future block"u8.ToArray()));

        var ordinary = new List<string>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions { Prefix = prefix }))
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
                           .AsPages(pageSizeHint: 1))
        {
            listed.Add(Assert.Single(page.Values));
            if (!string.IsNullOrEmpty(page.ContinuationToken))
                Assert.True(markers.Add(page.ContinuationToken));
        }
        Assert.Equal(pendingNames.Append(committed.Name).Order(StringComparer.Ordinal), listed.Select(item => item.Name));
        Assert.Equal(listed.Count - 1, markers.Count);
        foreach (var pending in listed.Where(item => item.Name != committed.Name))
        {
            Assert.Equal(BlobType.Block, pending.Properties.BlobType);
            Assert.Equal(0, pending.Properties.ContentLength);
            Assert.Null(pending.Properties.ContentType);
        }
        Assert.Equal("committed"u8.Length, listed.Single(item => item.Name == committed.Name).Properties.ContentLength);

        var hierarchical = new List<string>();
        await foreach (var item in container.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions
        {
            Delimiter = "/",
            Prefix = prefix,
            States = BlobStates.Uncommitted
        }))
        {
            hierarchical.Add(item.IsPrefix ? $"P:{item.Prefix}" : $"B:{item.Blob.Name}");
        }
        Assert.Contains($"P:{prefix}folder/", hierarchical);
        Assert.DoesNotContain($"B:{prefix}folder/child.bin", hierarchical);

        var arrowNames = new List<string>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = prefix,
            States = BlobStates.Uncommitted,
            ResponseFormat = StorageResponseFormat.Arrow
        }))
        {
            arrowNames.Add(item.Name);
        }
        Assert.Equal(listed.Select(item => item.Name), arrowNames);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        var rawUri = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddMinutes(5)),
            $"restype=container&comp=list&include=uncommittedblobs&prefix={Uri.EscapeDataString(prefix)}");
        using (var rawRequest = new HttpRequestMessage(HttpMethod.Get, rawUri))
        {
            rawRequest.Headers.Add("x-ms-version", "2026-06-06");
            using var rawResponse = await transport.SendAsync(rawRequest);
            Assert.Equal(HttpStatusCode.OK, rawResponse.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(await rawResponse.Content.ReadAsStringAsync());
            var pending = document.Descendants("Blob")
                .Single(element => element.Element("Name")?.Value == pendingNames[0]);
            var properties = Assert.IsType<System.Xml.Linq.XElement>(pending.Element("Properties"));
            Assert.Equal("0", properties.Element("Content-Length")?.Value);
            Assert.Equal("BlockBlob", properties.Element("BlobType")?.Value);
            Assert.Null(properties.Element("Last-Modified"));
            Assert.Null(properties.Element("Etag"));
            Assert.Null(properties.Element("Content-Type"));
            Assert.Null(pending.Element("Metadata"));
        }

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
            var configured = (await service.GetPropertiesAsync()).Value;
            configured.StaticWebsite.Enabled = true;
            configured.StaticWebsite.IndexDocument = indexName;
            configured.StaticWebsite.DefaultIndexDocumentPath = null;
            configured.StaticWebsite.ErrorDocument404Path = errorName;
            await service.SetPropertiesAsync(configured);

            var roundTrip = (await service.GetPropertiesAsync()).Value.StaticWebsite;
            Assert.True(roundTrip.Enabled);
            Assert.Equal(indexName, roundTrip.IndexDocument);
            Assert.Equal(errorName, roundTrip.ErrorDocument404Path);
            Assert.Null(roundTrip.DefaultIndexDocumentPath);

            var website = service.GetBlobContainerClient("$web");
            Assert.True((await website.ExistsAsync()).Value);
            await website.GetBlobClient(indexName).UploadAsync(
                BinaryData.FromString("root index"),
                new BlobUploadOptions
                {
                    HttpHeaders = new BlobHttpHeaders
                    {
                        ContentType = "text/html; charset=utf-8",
                        CacheControl = "public,max-age=60"
                    }
                });
            await website.GetBlobClient($"folder/{indexName}").UploadAsync(BinaryData.FromString("folder index"));
            await website.GetBlobClient(errorName).UploadAsync(
                BinaryData.FromString("custom not found"),
                new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = "text/html" } });
            await website.GetBlobClient(assetName).UploadAsync(
                BinaryData.FromString("0123456789"),
                new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = "text/plain" } });
            await website.GetBlobClient(defaultName).UploadAsync(
                BinaryData.FromString("single page fallback"),
                new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = "text/html" } });

            using var web = new HttpClient(factory.Server.CreateHandler());
            var endpoint = $"http://{SavaWebApplicationFactory.AccountName}.z99.web.local";

            using (var root = await web.GetAsync(endpoint + "/"))
            {
                Assert.Equal(HttpStatusCode.OK, root.StatusCode);
                Assert.Equal("root index", await root.Content.ReadAsStringAsync());
                Assert.Equal("text/html", root.Content.Headers.ContentType?.MediaType);
                Assert.Equal("public, max-age=60", root.Headers.CacheControl?.ToString());
            }
            using (var folder = await web.GetAsync(endpoint + "/folder/"))
            {
                Assert.Equal(HttpStatusCode.OK, folder.StatusCode);
                Assert.Equal("folder index", await folder.Content.ReadAsStringAsync());
            }
            using (var missing = await web.GetAsync(endpoint + "/missing"))
            {
                Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
                Assert.Equal("custom not found", await missing.Content.ReadAsStringAsync());
                Assert.Equal("text/html", missing.Content.Headers.ContentType?.MediaType);
            }
            using (var rangeRequest = new HttpRequestMessage(HttpMethod.Get, endpoint + "/" + assetName))
            {
                rangeRequest.Headers.Range = new RangeHeaderValue(2, 5);
                using var range = await web.SendAsync(rangeRequest);
                Assert.Equal(HttpStatusCode.PartialContent, range.StatusCode);
                Assert.Equal("2345", await range.Content.ReadAsStringAsync());
                Assert.Equal($"bytes 2-5/10", range.Content.Headers.ContentRange?.ToString());
            }
            using (var headRequest = new HttpRequestMessage(HttpMethod.Head, endpoint + "/" + assetName))
            using (var head = await web.SendAsync(headRequest))
            {
                Assert.Equal(HttpStatusCode.OK, head.StatusCode);
                Assert.Equal(10, head.Content.Headers.ContentLength);
                Assert.Empty(await head.Content.ReadAsByteArrayAsync());
            }
            using (var post = await web.PostAsync(endpoint + "/" + assetName, new ByteArrayContent([])))
            {
                Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);
                Assert.True(post.Content.Headers.TryGetValues("Allow", out var allowedMethods));
                Assert.Equal(["GET", "HEAD"], allowedMethods);
                Assert.Equal("text/html", post.Content.Headers.ContentType?.MediaType);
            }

            configured = (await service.GetPropertiesAsync()).Value;
            configured.StaticWebsite.IndexDocument = null;
            configured.StaticWebsite.DefaultIndexDocumentPath = defaultName;
            await service.SetPropertiesAsync(configured);
            roundTrip = (await service.GetPropertiesAsync()).Value.StaticWebsite;
            Assert.Null(roundTrip.IndexDocument);
            Assert.Equal(defaultName, roundTrip.DefaultIndexDocumentPath);
            using (var fallback = await web.GetAsync(endpoint + "/client/side/route"))
            {
                Assert.Equal(HttpStatusCode.OK, fallback.StatusCode);
                Assert.Equal("single page fallback", await fallback.Content.ReadAsStringAsync());
            }

            configured.StaticWebsite.Enabled = false;
            await service.SetPropertiesAsync(configured);
            using (var disabled = await web.GetAsync(endpoint + "/"))
            {
                Assert.Equal(HttpStatusCode.NotFound, disabled.StatusCode);
                Assert.Equal("text/html", disabled.Content.Headers.ContentType?.MediaType);
                Assert.Contains("requested content does not exist", await disabled.Content.ReadAsStringAsync(), StringComparison.Ordinal);
            }
        }
        finally
        {
            await service.SetPropertiesAsync(original);
        }
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
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
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

        var properties = await blob.GetPropertiesAsync();
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

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
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
            using var response = await transport.SendAsync(list);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var xml = await response.Content.ReadAsStringAsync();
            Assert.Contains("<Owner>$superuser</Owner>", xml, StringComparison.Ordinal);
            Assert.Contains("<Group>$superuser</Group>", xml, StringComparison.Ordinal);
            Assert.Contains("<Permissions>rw-r-----</Permissions>", xml, StringComparison.Ordinal);
            Assert.Contains("<Acl>user::rw-,group::r--,other::---</Acl>", xml, StringComparison.Ordinal);
            Assert.Contains("<ResourceType>file</ResourceType>", xml, StringComparison.Ordinal);
        }

        using (var invalidDelimiter = new HttpRequestMessage(
                   HttpMethod.Get,
                   AppendQuery(listUri, "delimiter=:")))
        {
            invalidDelimiter.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(invalidDelimiter);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal(
                "InvalidQueryParameterValue",
                response.Headers.GetValues("x-ms-error-code").Single());
        }

        var pageFailure = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetPageBlobClient("page.bin").CreateAsync(512));
        Assert.Equal(StatusCodes.Status409Conflict, pageFailure.Status);
        Assert.Equal("BlobOperationNotSupported", pageFailure.ErrorCode);

        var append = container.GetAppendBlobClient("append.log");
        await append.CreateAsync();
        await append.AppendBlockAsync(BinaryData.FromString("entry").ToStream());
        var sealFailure = await Assert.ThrowsAsync<RequestFailedException>(() => append.SealAsync());
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
        using (var incrementalCopy = new HttpRequestMessage(HttpMethod.Put, incrementalCopyUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            incrementalCopy.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            incrementalCopy.Headers.TryAddWithoutValidation(
                "x-ms-copy-source",
                "/source/source.vhd?snapshot=2026-09-22T00:00:00.0000000Z");
            using var response = await transport.SendAsync(incrementalCopy);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal(
                "BlobOperationNotSupported",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
    }

    [Fact]
    public async Task HierarchicalNamespaceSoftDeleteUsesDeletionIdsAndRestoresASelectedGeneration()
    {
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var properties = await metadata.GetServicePropertiesAsync(
            SavaWebApplicationFactory.SecondAccountName,
            CancellationToken.None);
        await metadata.PutServicePropertiesAsync(
            SavaWebApplicationFactory.SecondAccountName,
            properties with
            {
                BlobSoftDeleteEnabled = true,
                BlobSoftDeleteRetentionDays = 7,
                VersioningEnabled = true
            },
            CancellationToken.None);

        var container = service.GetBlobContainerClient($"hns-delete-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("folder/repeated.txt");
        await blob.UploadAsync(BinaryData.FromString("first"));
        await blob.UploadAsync(BinaryData.FromString("second"), overwrite: true);

        var family = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            includeDeleted: true,
            CancellationToken.None);
        var current = Assert.Single(family);
        Assert.True(current.IsCurrent);
        Assert.Null(current.VersionId);
        Assert.False(current.IsDeleted);

        await blob.DeleteAsync();
        await blob.UploadAsync(BinaryData.FromString("third"));
        await blob.DeleteAsync();
        await blob.UploadAsync(BinaryData.FromString("active"));

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
        var listUri = AppendQuery(
            container.Uri,
            $"restype=container&comp=list&showonly=deleted&{sas}");
        using var transport = new HttpClient(application.Server.CreateHandler());
        using var listRequest = new HttpRequestMessage(HttpMethod.Get, listUri);
        listRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        using var listResponse = await transport.SendAsync(listRequest);
        Assert.Equal(HttpStatusCode.OK, listResponse.StatusCode);
        var document = System.Xml.Linq.XDocument.Parse(await listResponse.Content.ReadAsStringAsync());
        var deleted = document.Descendants("Blob")
            .Where(element => element.Element("Name")?.Value == blob.Name)
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

        using (var invalidMix = new HttpRequestMessage(
                   HttpMethod.Get,
                   AppendQuery(listUri, "include=deleted")))
        {
            invalidMix.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(invalidMix);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidQueryParameterValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var arrow = new HttpRequestMessage(HttpMethod.Get, AppendQuery(
                   container.Uri,
                   $"restype=container&comp=list&{sas}")))
        {
            arrow.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            arrow.Headers.TryAddWithoutValidation("Accept", AzureResponseWriter.ArrowStreamContentType);
            using var response = await transport.SendAsync(arrow);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("BlobOperationNotSupported", response.Headers.GetValues("x-ms-error-code").Single());
        }

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
                $"{Uri.EscapeDataString(blob.Name)}?deletionid={deletionIds[0].ToString(CultureInfo.InvariantCulture)}");
            using var response = await transport.SendAsync(undelete);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        var restoredText = (await restored.DownloadContentAsync()).Value.Content.ToString();
        Assert.Contains(restoredText, new[] { "second", "third" });
        Assert.Equal("active", (await blob.DownloadContentAsync()).Value.Content.ToString());

        family = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            includeDeleted: true,
            CancellationToken.None);
        Assert.Single(family, item => item.IsDeleted);
    }

    [Fact]
    public async Task HierarchicalNamespacePersistsDirectoriesAndListsTheirAzureProperties()
    {
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
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

        async Task<System.Xml.Linq.XDocument> ListAsync(string query)
        {
            using var request = new HttpRequestMessage(
                HttpMethod.Get,
                AppendQuery(listSas, $"restype=container&comp=list&{query}"));
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            return System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync());
        }

        var recursive = await ListAsync("include=permissions");
        var recursiveEntries = recursive.Descendants("Blob").ToDictionary(
            element => Assert.IsType<System.Xml.Linq.XElement>(element.Element("Name")).Value,
            StringComparer.Ordinal);
        Assert.Equal(5, recursiveEntries.Count);
        Assert.Equal("directory", recursiveEntries["alpha"].Element("Properties")?.Element("ResourceType")?.Value);
        Assert.Equal("rwxr-x---", recursiveEntries["alpha"].Element("Properties")?.Element("Permissions")?.Value);
        Assert.Equal("directory", recursiveEntries["alpha/beta"].Element("Properties")?.Element("ResourceType")?.Value);
        Assert.Equal("file", recursiveEntries["alpha/beta/file.txt"].Element("Properties")?.Element("ResourceType")?.Value);

        var onlyDirectories = await ListAsync("showonly=directories&include=permissions");
        Assert.Equal(
            new[] { "alpha", "alpha/beta" },
            onlyDirectories.Descendants("Blob")
                .Select(element => element.Element("Name")?.Value)
                .ToArray());
        var onlyFiles = await ListAsync("showonly=files");
        Assert.Equal(
            new[] { "alpha/beta/file.txt", "alpha/root.txt", "zeta.txt" },
            onlyFiles.Descendants("Blob")
                .Select(element => element.Element("Name")?.Value)
                .ToArray());

        var pagedNames = new List<string>();
        var marker = string.Empty;
        do
        {
            var page = await ListAsync(
                "maxresults=1" +
                (string.IsNullOrEmpty(marker) ? string.Empty : $"&marker={Uri.EscapeDataString(marker)}"));
            pagedNames.Add(Assert.Single(page.Descendants("Blob")).Element("Name")!.Value);
            marker = page.Root?.Element("NextMarker")?.Value ?? string.Empty;
        }
        while (!string.IsNullOrEmpty(marker));
        Assert.Equal(recursiveEntries.Keys, pagedNames);

        var root = await ListAsync("delimiter=/&include=permissions");
        var rootPrefix = Assert.Single(root.Descendants("BlobPrefix"));
        Assert.Equal("alpha/", rootPrefix.Element("Name")?.Value);
        var prefixProperties = Assert.IsType<System.Xml.Linq.XElement>(rootPrefix.Element("Properties"));
        Assert.Equal("directory", prefixProperties.Element("ResourceType")?.Value);
        Assert.Equal("rwxr-x---", prefixProperties.Element("Permissions")?.Value);
        Assert.Equal("alpha/", rootPrefix.Element("Name")?.Value);
        Assert.Equal(
            new[] { "zeta.txt" },
            root.Descendants("Blob").Select(element => element.Element("Name")?.Value).ToArray());

        var alpha = await ListAsync($"delimiter=/&prefix={Uri.EscapeDataString("alpha/")}&include=permissions");
        Assert.Equal("alpha/beta/", Assert.Single(alpha.Descendants("BlobPrefix")).Element("Name")?.Value);
        Assert.Equal("alpha/root.txt", Assert.Single(alpha.Descendants("Blob")).Element("Name")?.Value);

        var directoryProperties = await container.GetBlobClient("alpha").GetPropertiesAsync();
        Assert.True(directoryProperties.GetRawResponse().Headers.TryGetValue("x-ms-resource-type", out var resourceType));
        Assert.Equal("directory", resourceType);
        Assert.True(directoryProperties.GetRawResponse().Headers.TryGetValue("x-ms-permissions", out var permissions));
        Assert.Equal("rwxr-x---", permissions);

        var notEmpty = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("alpha").DeleteAsync());
        Assert.Equal(StatusCodes.Status409Conflict, notEmpty.Status);
        Assert.Equal("DirectoryIsNotEmpty", notEmpty.ErrorCode);

        await container.GetBlobClient("conflict").UploadAsync(BinaryData.FromString("file"));
        var fileAncestor = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("conflict/child.txt").UploadAsync(BinaryData.FromString("child")));
        Assert.Equal(StatusCodes.Status409Conflict, fileAncestor.Status);
        Assert.Equal("PathAlreadyExists", fileAncestor.ErrorCode);

        await container.GetBlobClient("branch/child.txt").UploadAsync(BinaryData.FromString("child"));
        var directoryTarget = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("branch").UploadAsync(BinaryData.FromString("replacement"), overwrite: true));
        Assert.Equal(StatusCodes.Status409Conflict, directoryTarget.Status);
        Assert.Equal("PathAlreadyExists", directoryTarget.ErrorCode);

        await container.GetBlobClient("alpha/beta/file.txt").DeleteAsync();
        await container.GetBlobClient("alpha/beta").DeleteAsync();
        await container.GetBlobClient("alpha/root.txt").DeleteAsync();
        var deleteEmptyDirectory = await container.GetBlobClient("alpha").DeleteAsync();
        Assert.Equal(StatusCodes.Status202Accepted, deleteEmptyDirectory.Status);
    }

    [Fact]
    public async Task HierarchicalNamespaceBlobIndexTagsRequireTheExplicitPreviewCapability()
    {
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
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
            blob.SetTagsAsync(new Dictionary<string, string> { ["state"] = "blocked" })));

        var taggedUpload = container.GetBlobClient("tagged-upload.bin");
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(() =>
            taggedUpload.UploadAsync(
                BinaryData.FromString("must remain unpublished"),
                new BlobUploadOptions
                {
                    Tags = new Dictionary<string, string> { ["state"] = "blocked" }
                })));
        Assert.False((await taggedUpload.ExistsAsync()).Value);

        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await foreach (var _ in container.GetBlobsAsync(new GetBlobsOptions { Traits = BlobTraits.Tags }))
            {
            }
        }));
        AssertUnsupported(await Assert.ThrowsAsync<RequestFailedException>(async () =>
        {
            await foreach (var _ in service.FindBlobsByTagsAsync("\"state\" = 'blocked'"))
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
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceBlobIndexTagsEnabled"] = "true"
            });
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
                Tags = new Dictionary<string, string> { ["state"] = "initial" }
            });

        Assert.Equal("initial", (await blob.GetTagsAsync()).Value.Tags["state"]);
        await blob.SetTagsAsync(new Dictionary<string, string> { ["state"] = "updated" });
        Assert.Equal("updated", (await blob.GetTagsAsync()).Value.Tags["state"]);

        var listed = await container.GetBlobsAsync(new GetBlobsOptions
        {
            Traits = BlobTraits.Tags,
            Prefix = blob.Name
        }).SingleAsync();
        Assert.Equal("updated", listed.Tags["state"]);
        var found = await service.FindBlobsByTagsAsync("\"state\" = 'updated'").ToListAsync();
        Assert.Contains(found, item => item.BlobContainerName == container.Name && item.BlobName == blob.Name);

        var tagSas = blob.GenerateSasUri(
            BlobSasPermissions.Tag,
            DateTimeOffset.UtcNow.AddMinutes(5));
        using var transport = new HttpClient(application.Server.CreateHandler());
        using (var boundaryRequest = new HttpRequestMessage(HttpMethod.Get, AppendQuery(tagSas, "comp=tags")))
        {
            boundaryRequest.Headers.TryAddWithoutValidation("x-ms-version", "2024-11-04");
            using var boundaryResponse = await transport.SendAsync(boundaryRequest);
            Assert.Equal(HttpStatusCode.OK, boundaryResponse.StatusCode);
            Assert.Contains("<Key>state</Key><Value>updated</Value>", await boundaryResponse.Content.ReadAsStringAsync());
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
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
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
            await foreach (var _ in container.GetBlobsAsync(new GetBlobsOptions { States = BlobStates.Snapshots }))
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
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceBlobSnapshotsEnabled"] = "true"
            });
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

        var listed = await container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Snapshots,
            Prefix = blob.Name
        }).ToListAsync();
        Assert.Contains(listed, item => item.Snapshot == snapshotInfo.Snapshot);

        var family = await metadata.ListBlobFamilyAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            includeDeleted: true,
            CancellationToken.None);
        Assert.All(family, item => Assert.Null(item.VersionId));
        Assert.Single(family, item => item.Snapshot == snapshotInfo.Snapshot);

        await snapshot.DeleteAsync();
        var deletedSnapshot = await metadata.GetBlobAsync(
            SavaWebApplicationFactory.SecondAccountName,
            container.Name,
            blob.Name,
            versionId: null,
            snapshotInfo.Snapshot,
            includeDeleted: true,
            CancellationToken.None);
        Assert.NotNull(deletedSnapshot);
        Assert.True(deletedSnapshot.IsDeleted);
        Assert.Equal(snapshotInfo.Snapshot, deletedSnapshot.Snapshot);
        Assert.Null(deletedSnapshot.DeletionId);

        var directorySnapshot = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("folder").CreateSnapshotAsync());
        Assert.Equal(StatusCodes.Status409Conflict, directorySnapshot.Status);
        Assert.Equal("BlobOperationNotSupported", directorySnapshot.ErrorCode);
    }

    [Fact]
    public async Task HierarchicalNamespaceNeverExposesBlobVersions()
    {
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
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
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-encryption-context-{Guid.NewGuid():N}");
        await container.CreateAsync();
        using var transport = new HttpClient(application.Server.CreateHandler());

        static HttpRequestMessage CreatePutRequest(
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

        async Task<HttpResponseMessage> GetPropertiesAsync(BlobBaseClient blob, string version)
        {
            var request = new HttpRequestMessage(
                HttpMethod.Head,
                blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            return await transport.SendAsync(request);
        }

        async Task<System.Xml.Linq.XDocument> ListAsync(string version)
        {
            var uri = AppendQuery(
                container.GenerateSasUri(
                    BlobContainerSasPermissions.List,
                    DateTimeOffset.UtcNow.AddMinutes(5)),
                "restype=container&comp=list");
            using var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            return System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync());
        }

        const string context = "tenant=alpha;key=v1";
        var blob = container.GetBlobClient("folder/context.bin");
        using (var put = CreatePutRequest(blob, "2021-08-06", context, "context payload"))
        using (var response = await transport.SendAsync(put))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        using (var properties = await GetPropertiesAsync(blob, "2021-08-06"))
        {
            Assert.Equal(HttpStatusCode.OK, properties.StatusCode);
            Assert.Equal(context, GetResponseHeader(properties, "x-ms-encryption-context"));
        }
        using (var legacyProperties = await GetPropertiesAsync(blob, "2021-06-08"))
        {
            Assert.Equal(HttpStatusCode.OK, legacyProperties.StatusCode);
            Assert.Null(GetResponseHeaderOrDefault(legacyProperties, "x-ms-encryption-context"));
        }

        var modernListing = await ListAsync("2021-06-08");
        var modernEntry = modernListing.Descendants("Blob").Single(element =>
            element.Element("Name")?.Value == blob.Name);
        Assert.Equal(context, modernEntry.Element("Properties")?.Element("EncryptionContext")?.Value);
        var legacyListing = await ListAsync("2021-04-10");
        var legacyEntry = legacyListing.Descendants("Blob").Single(element =>
            element.Element("Name")?.Value == blob.Name);
        Assert.Null(legacyEntry.Element("Properties")?.Element("EncryptionContext"));

        var block = container.GetBlockBlobClient("folder/committed.bin");
        var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("block-0001"));
        await block.StageBlockAsync(blockId, new MemoryStream(Encoding.UTF8.GetBytes("committed payload")));
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
            using var response = await transport.SendAsync(commit);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        using (var properties = await GetPropertiesAsync(block, "2021-08-06"))
        {
            Assert.Equal("commit-context", GetResponseHeader(properties, "x-ms-encryption-context"));
        }

        using (var overwrite = CreatePutRequest(blob, "2021-08-06", null, "replacement"))
        using (var response = await transport.SendAsync(overwrite))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        using (var properties = await GetPropertiesAsync(blob, "2021-08-06"))
        {
            Assert.Null(GetResponseHeaderOrDefault(properties, "x-ms-encryption-context"));
        }

        using (var oversized = CreatePutRequest(blob, "2021-08-06", new string('x', 1025), "rejected"))
        using (var response = await transport.SendAsync(oversized))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.Equal("replacement", (await blob.DownloadContentAsync()).Value.Content.ToString());

        var legacyTarget = container.GetBlobClient("legacy.bin");
        using (var legacyPut = CreatePutRequest(legacyTarget, "2021-06-08", context, "rejected"))
        using (var response = await transport.SendAsync(legacyPut))
        {
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.False((await legacyTarget.ExistsAsync()).Value);

        var copyTarget = container.GetBlobClient("copy.bin");
        using (var copy = new HttpRequestMessage(
            HttpMethod.Put,
            copyTarget.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        })
        {
            copy.Headers.TryAddWithoutValidation("x-ms-version", "2021-08-06");
            copy.Headers.TryAddWithoutValidation(
                "x-ms-copy-source",
                blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)).ToString());
            copy.Headers.TryAddWithoutValidation("x-ms-encryption-context", context);
            using var response = await transport.SendAsync(copy);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("UnsupportedHeader", GetResponseHeader(response, "x-ms-error-code"));
        }

        await using var flatApplication = new SavaWebApplicationFactory();
        var flatService = CreateClient(flatApplication);
        var flatContainer = flatService.GetBlobContainerClient($"flat-encryption-context-{Guid.NewGuid():N}");
        await flatContainer.CreateAsync();
        var flatBlob = flatContainer.GetBlobClient("context.bin");
        using var flatTransport = new HttpClient(flatApplication.Server.CreateHandler());
        using var flatPut = CreatePutRequest(flatBlob, "2021-08-06", context, "rejected");
        using var flatResponse = await flatTransport.SendAsync(flatPut);
        Assert.Equal(HttpStatusCode.BadRequest, flatResponse.StatusCode);
        Assert.Equal("InvalidHeaderValue", GetResponseHeader(flatResponse, "x-ms-error-code"));
        Assert.False((await flatBlob.ExistsAsync()).Value);
    }

    [Fact]
    public async Task HierarchicalNamespaceUpnProjectionValidatesItsDocumentedRequestShape()
    {
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-upn-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blob = container.GetBlobClient("folder/item.bin");
        await blob.UploadAsync(BinaryData.FromString("identity"));
        using var transport = new HttpClient(application.Server.CreateHandler());

        async Task<HttpResponseMessage> ListAsync(string version, string include, string upn)
        {
            var uri = AppendQuery(
                container.GenerateSasUri(
                    BlobContainerSasPermissions.List,
                    DateTimeOffset.UtcNow.AddMinutes(5)),
                $"restype=container&comp=list{include}");
            var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            request.Headers.TryAddWithoutValidation("x-ms-upn", upn);
            return await transport.SendAsync(request);
        }

        using (var projected = await ListAsync("2020-06-12", "&include=permissions", "TRUE"))
        {
            Assert.Equal(HttpStatusCode.OK, projected.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(await projected.Content.ReadAsStringAsync());
            var properties = document.Descendants("Blob").Single(element =>
                element.Element("Name")?.Value == blob.Name).Element("Properties");
            Assert.Equal("$superuser", properties?.Element("Owner")?.Value);
            Assert.Equal("$superuser", properties?.Element("Group")?.Value);
            Assert.NotNull(properties?.Element("Acl"));
        }
        using (var missingPermissions = await ListAsync("2023-11-03", string.Empty, "true"))
        {
            Assert.Equal(HttpStatusCode.BadRequest, missingPermissions.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(missingPermissions, "x-ms-error-code"));
        }
        using (var invalid = await ListAsync("2023-11-03", "&include=permissions", "yes"))
        {
            Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(invalid, "x-ms-error-code"));
        }

        async Task<HttpResponseMessage> HeadAsync(
            BlobClient target,
            string version,
            string upn,
            HttpClient client)
        {
            var request = new HttpRequestMessage(
                HttpMethod.Head,
                target.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            request.Headers.TryAddWithoutValidation("x-ms-upn", upn);
            return await client.SendAsync(request);
        }

        using (var projected = await HeadAsync(blob, "2023-11-03", "false", transport))
        {
            Assert.Equal(HttpStatusCode.OK, projected.StatusCode);
            Assert.Equal("$superuser", GetResponseHeader(projected, "x-ms-owner"));
            Assert.Equal("$superuser", GetResponseHeader(projected, "x-ms-group"));
        }
        using (var legacy = await HeadAsync(blob, "2021-08-06", "true", transport))
        {
            Assert.Equal(HttpStatusCode.Conflict, legacy.StatusCode);
            Assert.Equal("FeatureVersionMismatch", GetResponseHeader(legacy, "x-ms-error-code"));
        }
        using (var invalid = await HeadAsync(blob, "2023-11-03", "yes", transport))
        {
            Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(invalid, "x-ms-error-code"));
        }

        await using var flatApplication = new SavaWebApplicationFactory();
        var flatService = CreateClient(flatApplication);
        var flatContainer = flatService.GetBlobContainerClient($"flat-upn-{Guid.NewGuid():N}");
        await flatContainer.CreateAsync();
        var flatBlob = flatContainer.GetBlobClient("item.bin");
        await flatBlob.UploadAsync(BinaryData.FromString("flat"));
        using var flatTransport = new HttpClient(flatApplication.Server.CreateHandler());
        using (var flat = await HeadAsync(
                   flatBlob,
                   "2023-11-03",
                   "true",
                   flatTransport))
        {
            Assert.Equal(HttpStatusCode.BadRequest, flat.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(flat, "x-ms-error-code"));
        }
    }

    [Fact]
    public async Task HierarchicalNamespaceBlobExpiryMatchesAzureOperationContracts()
    {
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.SecondAccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        var service = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var container = service.GetBlobContainerClient($"hns-expiry-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var blobService = application.Services.GetRequiredService<BlobService>();
        using var transport = new HttpClient(application.Server.CreateHandler());

        async Task<BlobRecord> GetRecordAsync(string name) =>
            await blobService.GetBlobAsync(
                SavaWebApplicationFactory.SecondAccountName,
                container.Name,
                name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);

        async Task<HttpResponseMessage> PutBlobAsync(
            HttpClient client,
            BlobClient target,
            string version,
            string option,
            string? expiryTime,
            byte[] content)
        {
            var request = new HttpRequestMessage(
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
            return await client.SendAsync(request);
        }

        async Task<HttpResponseMessage> SetExpiryAsync(
            BlobClient target,
            string version,
            string option,
            string? expiryTime)
        {
            var request = new HttpRequestMessage(
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
            return await transport.SendAsync(request);
        }

        var oldVersion = container.GetBlobClient("old-version.bin");
        using (var response = await PutBlobAsync(
                   transport,
                   oldVersion,
                   "2021-08-06",
                   "RelativeToNow",
                   "600000",
                   "must not publish"u8.ToArray()))
        {
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.False((await oldVersion.ExistsAsync()).Value);

        var invalidCreationOption = container.GetBlobClient("relative-to-creation.bin");
        using (var response = await PutBlobAsync(
                   transport,
                   invalidCreationOption,
                   "2023-08-03",
                   "RelativeToCreation",
                   "600000",
                   "must not publish"u8.ToArray()))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.False((await invalidCreationOption.ExistsAsync()).Value);

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
        using (var response = await PutBlobAsync(
                   transport,
                   direct,
                   "2023-08-03",
                   "Absolute",
                   absoluteExpiry.ToString("R", CultureInfo.InvariantCulture),
                   "direct expiry"u8.ToArray()))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        Assert.Equal(absoluteExpiry, (await GetRecordAsync(direct.Name)).ExpiresAt);

        async Task<HttpResponseMessage> GetPropertiesAsync(string version)
        {
            var request = new HttpRequestMessage(
                HttpMethod.Head,
                direct.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            return await transport.SendAsync(request);
        }

        using (var legacyProperties = await GetPropertiesAsync("2019-12-12"))
        {
            Assert.Equal(HttpStatusCode.OK, legacyProperties.StatusCode);
            Assert.False(legacyProperties.Headers.Contains("x-ms-expiry-time"));
        }
        using (var properties = await GetPropertiesAsync("2020-02-10"))
        {
            Assert.Equal(HttpStatusCode.OK, properties.StatusCode);
            Assert.Equal(
                absoluteExpiry.ToString("R", CultureInfo.InvariantCulture),
                GetResponseHeader(properties, "x-ms-expiry-time"));
        }

        var listUri = AppendQuery(
            container.GenerateSasUri(BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=list");
        using (var request = new HttpRequestMessage(HttpMethod.Get, listUri))
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-02-10");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync());
            var listed = document.Descendants("Blob").Single(element =>
                element.Element("Name")?.Value == direct.Name);
            Assert.Equal(
                absoluteExpiry.ToString("R", CultureInfo.InvariantCulture),
                listed.Element("Properties")?.Element("Expiry-Time")?.Value);
        }

        using (var malformed = await SetExpiryAsync(
                   direct,
                   "2020-02-10",
                   "Absolute",
                   DateTimeOffset.UtcNow.AddMinutes(30).ToString("O", CultureInfo.InvariantCulture)))
        {
            Assert.Equal(HttpStatusCode.BadRequest, malformed.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(malformed, "x-ms-error-code"));
        }
        Assert.Equal(absoluteExpiry, (await GetRecordAsync(direct.Name)).ExpiresAt);

        var beforeRelative = await GetRecordAsync(direct.Name);
        using (var response = await SetExpiryAsync(
                   direct,
                   "2020-02-10",
                   "rElAtIvEtOcReAtIoN",
                   "1800000"))
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(response.Headers.Contains("ETag"));
            Assert.NotNull(response.Content.Headers.LastModified);
        }
        Assert.Equal(beforeRelative.CreatedAt.AddMinutes(30), (await GetRecordAsync(direct.Name)).ExpiresAt);

        using (var invalidNever = await SetExpiryAsync(direct, "2020-02-10", "NeverExpire", "1"))
        {
            Assert.Equal(HttpStatusCode.BadRequest, invalidNever.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(invalidNever, "x-ms-error-code"));
        }
        using (var cleared = await SetExpiryAsync(direct, "2020-02-10", "NeverExpire", expiryTime: null))
        {
            Assert.Equal(HttpStatusCode.OK, cleared.StatusCode);
        }
        Assert.Null((await GetRecordAsync(direct.Name)).ExpiresAt);

        await container.GetBlobClient("folder/child.bin").UploadAsync(BinaryData.FromString("child"));
        using (var directory = await SetExpiryAsync(
                   container.GetBlobClient("folder"),
                   "2020-02-10",
                   "RelativeToNow",
                   "600000"))
        {
            Assert.Equal(HttpStatusCode.Conflict, directory.StatusCode);
            Assert.Equal("BlobOperationNotSupported", GetResponseHeader(directory, "x-ms-error-code"));
        }

        var block = container.GetBlockBlobClient("blocks.bin");
        var blockId = Convert.ToBase64String("expiry-block-0001"u8);
        await block.StageBlockAsync(blockId, BinaryData.FromString("block expiry").ToStream());
        var blockExpiry = absoluteExpiry.AddMinutes(10);
        async Task<HttpResponseMessage> CommitBlocksAsync(bool includeExpiry)
        {
            var mode = includeExpiry ? "Latest" : "Committed";
            var request = new HttpRequestMessage(
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
                request.Headers.TryAddWithoutValidation(
                    "x-ms-expiry-time",
                    blockExpiry.ToString("R", CultureInfo.InvariantCulture));
            }
            return await transport.SendAsync(request);
        }

        using (var committed = await CommitBlocksAsync(includeExpiry: true))
            Assert.Equal(HttpStatusCode.Created, committed.StatusCode);
        Assert.Equal(blockExpiry, (await GetRecordAsync(block.Name)).ExpiresAt);
        using (var recommitted = await CommitBlocksAsync(includeExpiry: false))
            Assert.Equal(HttpStatusCode.Created, recommitted.StatusCode);
        Assert.Equal(blockExpiry, (await GetRecordAsync(block.Name)).ExpiresAt);

        var urlBytes = "put blob from url expiry"u8.ToArray();
        await using (var source = await LoopbackSource.StartAsync(urlBytes))
        {
            var destination = container.GetBlockBlobClient("from-url.bin");
            var request = new HttpRequestMessage(
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
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.Equal(urlBytes, (await destination.DownloadContentAsync()).Value.Content.ToArray());
            Assert.NotNull((await GetRecordAsync(destination.Name)).ExpiresAt);
        }

        var copyTarget = container.GetBlobClient("copy-rejects-expiry.bin");
        using (var request = new HttpRequestMessage(
                   HttpMethod.Put,
                   copyTarget.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5)))
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-08-03");
            request.Headers.TryAddWithoutValidation("x-ms-copy-source", "https://source.invalid/blob");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "RelativeToNow");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", "600000");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("UnsupportedHeader", GetResponseHeader(response, "x-ms-error-code"));
        }

        await using var flatApplication = new SavaWebApplicationFactory();
        var flatService = CreateClient(flatApplication);
        var flatContainer = flatService.GetBlobContainerClient($"flat-expiry-{Guid.NewGuid():N}");
        await flatContainer.CreateAsync();
        var flat = flatContainer.GetBlobClient("flat.bin");
        using var flatTransport = new HttpClient(flatApplication.Server.CreateHandler());
        using (var response = await PutBlobAsync(
                   flatTransport,
                   flat,
                   "2023-08-03",
                   "RelativeToNow",
                   "600000",
                   "flat"u8.ToArray()))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", GetResponseHeader(response, "x-ms-error-code"));
        }
        Assert.False((await flat.ExistsAsync()).Value);
        await flat.UploadAsync(BinaryData.FromString("flat"));
        var flatRecord = await flatApplication.Services.GetRequiredService<BlobService>().GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            flatContainer.Name,
            flat.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        var flatBusinessFailure = await Assert.ThrowsAsync<AzureStorageException>(() =>
            flatApplication.Services.GetRequiredService<BlobService>().SetExpiryAsync(
                flatRecord,
                DateTimeOffset.UtcNow.AddMinutes(10),
                CancellationToken.None));
        Assert.Equal("BlobOperationNotSupported", flatBusinessFailure.ErrorCode);

        var flatExpiryUri = AppendQuery(
            flat.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=expiry");
        using (var request = new HttpRequestMessage(HttpMethod.Put, flatExpiryUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-02-10");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-option", "RelativeToNow");
            request.Headers.TryAddWithoutValidation("x-ms-expiry-time", "600000");
            using var response = await flatTransport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("BlobOperationNotSupported", GetResponseHeader(response, "x-ms-error-code"));
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
                Tags = new Dictionary<string, string> { ["state"] = "initial" }
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
            writeOnly.SetTagsAsync(new Dictionary<string, string> { ["state"] = "wrong" }));
        Assert.Equal(StatusCodes.Status403Forbidden, deniedWrite.Status);
        Assert.Equal("AuthorizationPermissionMismatch", deniedWrite.ErrorCode);

        var tagOnly = CreateBlobClient(
            factory,
            blob.GenerateSasUri(BlobSasPermissions.Tag, DateTimeOffset.UtcNow.AddMinutes(5)));
        Assert.Equal("initial", (await tagOnly.GetTagsAsync()).Value.Tags["state"]);
        await tagOnly.SetTagsAsync(new Dictionary<string, string> { ["state"] = "updated" });
        Assert.Equal("updated", (await tagOnly.GetTagsAsync()).Value.Tags["state"]);

        var deniedContentRead = await Assert.ThrowsAsync<RequestFailedException>(() =>
            tagOnly.DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedContentRead.Status);
        Assert.Equal("AuthorizationPermissionMismatch", deniedContentRead.ErrorCode);
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
            var configured = (await service.GetPropertiesAsync()).Value;
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
            await service.SetPropertiesAsync(configured);

            var roundTrip = (await service.GetPropertiesAsync()).Value;
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

            using var transport = new HttpClient(factory.Server.CreateHandler());
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
                using var response = await transport.SendAsync(partialUpdate);
                Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            }

            roundTrip = (await service.GetPropertiesAsync()).Value;
            Assert.False(roundTrip.Logging.Delete);
            Assert.False(roundTrip.Logging.Read);
            Assert.True(roundTrip.Logging.Write);
            Assert.False(roundTrip.Logging.RetentionPolicy.Enabled);
            Assert.True(roundTrip.HourMetrics.Enabled);
            Assert.Equal(12, roundTrip.HourMetrics.RetentionPolicy.Days);
            Assert.Equal("https://example.test", Assert.Single(roundTrip.Cors).AllowedOrigins);

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
                using var response = await transport.SendAsync(invalidMetrics);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidXmlDocument", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var legacyGet = new HttpRequestMessage(HttpMethod.Get, propertiesUri))
            {
                legacyGet.Headers.TryAddWithoutValidation("x-ms-version", "2012-02-12");
                using var response = await transport.SendAsync(legacyGet);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                var xml = await response.Content.ReadAsStringAsync();
                Assert.Contains("<Logging>", xml, StringComparison.Ordinal);
                Assert.Contains("<Metrics>", xml, StringComparison.Ordinal);
                Assert.DoesNotContain("<HourMetrics>", xml, StringComparison.Ordinal);
                Assert.DoesNotContain("<MinuteMetrics>", xml, StringComparison.Ordinal);
                Assert.DoesNotContain("<Cors>", xml, StringComparison.Ordinal);
                Assert.DoesNotContain("<DeleteRetentionPolicy>", xml, StringComparison.Ordinal);
            }
        }
        finally
        {
            await service.SetPropertiesAsync(original);
        }
    }

    [Fact]
    public async Task StorageAnalyticsLoggingMaterializesProtectedAzureFormatSystemBlobs()
    {
        await using var application = new SavaWebApplicationFactory();
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
        var logItems = new List<BlobItem>();
        await foreach (var item in logs.GetBlobsAsync(new GetBlobsOptions { Traits = BlobTraits.Metadata }))
            logItems.Add(item);
        Assert.NotEmpty(logItems);
        Assert.All(logItems, item =>
        {
            Assert.Matches("^blob/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{4}/[0-9]{6}\\.log$", item.Name);
            Assert.Equal("2.0", item.Metadata["LogVersion"]);
            Assert.Contains(item.Metadata["LogType"], new[] { "read", "write", "delete" });
            Assert.EndsWith("Z", item.Metadata["StartTime"], StringComparison.Ordinal);
            Assert.EndsWith("Z", item.Metadata["EndTime"], StringComparison.Ordinal);
        });

        var operations = new List<string>();
        foreach (var item in logItems)
        {
            var text = (await logs.GetBlobClient(item.Name).DownloadContentAsync()).Value.Content.ToString();
            var fields = ParseAnalyticsLogFields(Assert.Single(text.Split('\n', StringSplitOptions.RemoveEmptyEntries)));
            Assert.Equal(38, fields.Count);
            Assert.Equal("2.0", fields[0]);
            Assert.Equal("blob", fields[10]);
            Assert.Equal(SavaWebApplicationFactory.AccountName, fields[9]);
            Assert.DoesNotContain(SavaWebApplicationFactory.AccountKey, text, StringComparison.Ordinal);
            operations.Add(fields[2]);
        }
        Assert.Contains("SetBlobServiceProperties", operations);
        Assert.Contains("CreateContainer", operations);
        Assert.Contains("PutBlob", operations);
        Assert.True(operations.Count(operation => operation == "PutBlob") >= 9);
        Assert.Contains("GetBlob", operations);
        Assert.Contains("DeleteBlob", operations);

        var ordinaryContainers = new List<string>();
        await foreach (var item in service.GetBlobContainersAsync())
            ordinaryContainers.Add(item.Name);
        Assert.DoesNotContain(StorageAnalyticsService.LogsContainerName, ordinaryContainers);

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
        using (var systemList = await transport.GetAsync(systemListUri))
        {
            Assert.Equal(HttpStatusCode.OK, systemList.StatusCode);
            Assert.Contains(
                $"<Name>{StorageAnalyticsService.LogsContainerName}</Name>",
                await systemList.Content.ReadAsStringAsync(),
                StringComparison.Ordinal);
        }

        var deniedWrite = await Assert.ThrowsAsync<RequestFailedException>(() =>
            logs.GetBlobClient("manual.log").UploadAsync(BinaryData.FromString("not service-owned")));
        Assert.Equal(StatusCodes.Status403Forbidden, deniedWrite.Status);
        Assert.Equal("AuthorizationPermissionMismatch", deniedWrite.ErrorCode);

        var deniedContainerDelete = await Assert.ThrowsAsync<RequestFailedException>(() => logs.DeleteAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedContainerDelete.Status);
        Assert.Equal("ContainerOperationFailure", deniedContainerDelete.ErrorCode);

        Assert.True((await logs.GetBlobClient(logItems[0].Name).DeleteIfExistsAsync()).Value);
    }

    [Fact]
    public async Task StorageAnalyticsRetentionPurgesExpiredLogBlobsWhenLoggingIsDisabledForReads()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 20, 8, 30, 0, TimeSpan.Zero));
        await using var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>());
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

            using (var get = new HttpRequestMessage(HttpMethod.Get, propertiesUri))
            {
                get.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
                using var response = await transport.SendAsync(get);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                var xml = await response.Content.ReadAsStringAsync();
                Assert.DoesNotContain("<ContainerDeleteRetentionPolicy>", xml, StringComparison.Ordinal);
                Assert.DoesNotContain("<IsVersioningEnabled>", xml, StringComparison.Ordinal);
            }

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
                using var response = await transport.SendAsync(put);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidXmlDocument", response.Headers.GetValues("x-ms-error-code").Single());
            }

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

            using (var unsupported = new HttpRequestMessage(HttpMethod.Head, blobUri))
            {
                unsupported.Headers.TryAddWithoutValidation("x-ms-version", "9999-01-01");
                using var response = await transport.SendAsync(unsupported);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var missing = new HttpRequestMessage(HttpMethod.Head, blobUri))
            {
                AddSharedKeyLiteAuthorization(missing);
                using var response = await transport.SendAsync(missing);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
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
                using var response = await transport.SendAsync(invalidDefault);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidXmlDocument", response.Headers.GetValues("x-ms-error-code").Single());
            }
            Assert.Null((await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None)).DefaultServiceVersion);

            using (var setDefault = new HttpRequestMessage(HttpMethod.Put, servicePropertiesUri)
            {
                Content = new StringContent(
                    "<StorageServiceProperties><DefaultServiceVersion>2018-03-28</DefaultServiceVersion></StorageServiceProperties>",
                    Encoding.UTF8,
                    "application/xml")
            })
            {
                setDefault.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(setDefault);
                Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            }

            using (var defaulted = new HttpRequestMessage(HttpMethod.Head, blobUri))
            {
                AddSharedKeyLiteAuthorization(defaulted);
                using var response = await transport.SendAsync(defaulted);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal("2018-03-28", response.Headers.GetValues("x-ms-version").Single());
                Assert.True(response.Headers.Contains("x-ms-creation-time"));
                Assert.False(response.Headers.Contains("x-ms-legal-hold"));
            }

            var sasUri = blob.GenerateSasUri(
                BlobSasPermissions.Read,
                DateTimeOffset.UtcNow.AddMinutes(10));
            var signedVersion = ReadQueryParameter(sasUri, "sv");
            using (var signedVersionRequest = new HttpRequestMessage(HttpMethod.Head, sasUri))
            {
                using var response = await transport.SendAsync(signedVersionRequest);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal(signedVersion, response.Headers.GetValues("x-ms-version").Single());
                Assert.True(response.Headers.Contains("x-ms-legal-hold"));
            }

            using (var apiVersionRequest = new HttpRequestMessage(
                       HttpMethod.Head,
                       AppendQuery(sasUri, "api-version=2012-02-12")))
            {
                using var response = await transport.SendAsync(apiVersionRequest);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal("2012-02-12", response.Headers.GetValues("x-ms-version").Single());
                Assert.False(response.Headers.Contains("x-ms-creation-time"));
                Assert.True(response.Headers.Contains("Accept-Ranges"));
            }

            var bearerToken = CreateJwt(SavaWebApplicationFactory.AccountKey, "reader-1");
            using (var missingBearerVersion = new HttpRequestMessage(HttpMethod.Head, blobUri))
            {
                missingBearerVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
                using var response = await transport.SendAsync(missingBearerVersion);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
            }
            using (var oldBearerVersion = new HttpRequestMessage(HttpMethod.Head, blobUri))
            {
                oldBearerVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
                oldBearerVersion.Headers.TryAddWithoutValidation("x-ms-version", "2017-07-29");
                using var response = await transport.SendAsync(oldBearerVersion);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }
            using (var supportedBearerVersion = new HttpRequestMessage(HttpMethod.Head, blobUri))
            {
                supportedBearerVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
                supportedBearerVersion.Headers.TryAddWithoutValidation("x-ms-version", "2017-11-09");
                using var response = await transport.SendAsync(supportedBearerVersion);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal("2017-11-09", response.Headers.GetValues("x-ms-version").Single());
            }
            using (var currentBearerVersion = new HttpRequestMessage(HttpMethod.Head, blobUri))
            {
                currentBearerVersion.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
                currentBearerVersion.Headers.TryAddWithoutValidation("x-ms-version", "2026-12-06");
                using var response = await transport.SendAsync(currentBearerVersion);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal("2026-12-06", response.Headers.GetValues("x-ms-version").Single());
            }
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

        async Task<HttpResponseMessage> SendBlobAsync(
            HttpMethod method,
            string version,
            Action<HttpRequestMessage>? configure = null)
        {
            using var request = new HttpRequestMessage(method, blobSas);
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            configure?.Invoke(request);
            return await transport.SendAsync(request);
        }

        static string Header(HttpResponseMessage response, string name)
        {
            if (response.Headers.TryGetValues(name, out var responseValues))
                return responseValues.Single();
            Assert.True(response.Content.Headers.TryGetValues(name, out var contentValues), $"Missing response header: {name}");
            return contentValues.Single();
        }

        using var legacyHead = await SendBlobAsync(HttpMethod.Head, "2009-09-19");
        Assert.Equal(HttpStatusCode.OK, legacyHead.StatusCode);
        var legacyEtag = Header(legacyHead, "ETag");
        Assert.StartsWith("0x", legacyEtag, StringComparison.Ordinal);
        Assert.DoesNotContain('"', legacyEtag);
        Assert.False(legacyHead.Headers.Contains("Accept-Ranges"));

        using var modernHead = await SendBlobAsync(HttpMethod.Head, "2011-08-18");
        Assert.Equal(HttpStatusCode.OK, modernHead.StatusCode);
        var modernEtag = Header(modernHead, "ETag");
        Assert.Equal($"\"{legacyEtag}\"", modernEtag);
        Assert.Equal("bytes", Header(modernHead, "Accept-Ranges"));

        using (var legacyMatch = await SendBlobAsync(
                   HttpMethod.Get,
                   "2009-09-19",
                   request => request.Headers.TryAddWithoutValidation("If-Match", legacyEtag)))
        {
            Assert.Equal(HttpStatusCode.OK, legacyMatch.StatusCode);
            Assert.Equal(content, await legacyMatch.Content.ReadAsByteArrayAsync());
        }
        using (var legacyQuotedMatch = await SendBlobAsync(
                   HttpMethod.Get,
                   "2009-09-19",
                   request => request.Headers.TryAddWithoutValidation("If-Match", modernEtag)))
        {
            Assert.Equal(HttpStatusCode.PreconditionFailed, legacyQuotedMatch.StatusCode);
            Assert.Equal("ConditionNotMet", legacyQuotedMatch.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var modernUnquotedMatch = await SendBlobAsync(
                   HttpMethod.Get,
                   "2011-08-18",
                   request => request.Headers.TryAddWithoutValidation("If-Match", legacyEtag)))
        {
            Assert.Equal(HttpStatusCode.OK, modernUnquotedMatch.StatusCode);
        }

        using (var legacyBoundedRange = await SendBlobAsync(
                   HttpMethod.Get,
                   "2009-09-19",
                   request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4")))
        {
            Assert.Equal(HttpStatusCode.PartialContent, legacyBoundedRange.StatusCode);
            Assert.Equal("234", await legacyBoundedRange.Content.ReadAsStringAsync());
        }
        using (var legacyOpenRange = await SendBlobAsync(
                   HttpMethod.Get,
                   "2009-09-19",
                   request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-")))
        {
            Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, legacyOpenRange.StatusCode);
            Assert.Equal("InvalidRange", legacyOpenRange.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var modernOpenRange = await SendBlobAsync(
                   HttpMethod.Get,
                   "2011-08-18",
                   request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-")))
        {
            Assert.Equal(HttpStatusCode.PartialContent, modernOpenRange.StatusCode);
            Assert.Equal("23456789", await modernOpenRange.Content.ReadAsStringAsync());
        }
        using (var unsupportedSuffixRange = await SendBlobAsync(
                   HttpMethod.Get,
                   "2023-11-03",
                   request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=-3")))
        {
            Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, unsupportedSuffixRange.StatusCode);
            Assert.Equal("InvalidRange", unsupportedSuffixRange.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var rangedHead = await SendBlobAsync(
                   HttpMethod.Head,
                   "2023-11-03",
                   request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4")))
        {
            Assert.Equal(HttpStatusCode.OK, rangedHead.StatusCode);
            Assert.Equal(content.Length, rangedHead.Content.Headers.ContentLength);
            Assert.Null(rangedHead.Content.Headers.ContentRange);
        }

        var wholeMd5 = Convert.ToBase64String(MD5.HashData(content));
        using (var oldRangeMd5Shape = await SendBlobAsync(
                   HttpMethod.Get,
                   "2015-12-11",
                   request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4")))
        {
            Assert.Equal(HttpStatusCode.PartialContent, oldRangeMd5Shape.StatusCode);
            Assert.False(oldRangeMd5Shape.Content.Headers.Contains("Content-MD5"));
            Assert.False(oldRangeMd5Shape.Headers.Contains("x-ms-blob-content-md5"));
        }
        using (var rangedMd5Shape = await SendBlobAsync(
                   HttpMethod.Get,
                   "2016-05-31",
                   request => request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4")))
        {
            Assert.Equal(HttpStatusCode.PartialContent, rangedMd5Shape.StatusCode);
            Assert.False(rangedMd5Shape.Content.Headers.Contains("Content-MD5"));
            Assert.Equal(wholeMd5, Header(rangedMd5Shape, "x-ms-blob-content-md5"));
        }
        using (var transactionalMd5 = await SendBlobAsync(
                   HttpMethod.Get,
                   "2016-05-31",
                   request =>
                   {
                       request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4");
                       request.Headers.TryAddWithoutValidation("x-ms-range-get-content-md5", "true");
                   }))
        {
            Assert.Equal(HttpStatusCode.PartialContent, transactionalMd5.StatusCode);
            Assert.Equal(
                Convert.ToBase64String(MD5.HashData(content.AsSpan(2, 3))),
                Header(transactionalMd5, "Content-MD5"));
            Assert.Equal(wholeMd5, Header(transactionalMd5, "x-ms-blob-content-md5"));
        }
        using (var oldCrc64 = await SendBlobAsync(
                   HttpMethod.Get,
                   "2018-11-09",
                   request =>
                   {
                       request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=2-4");
                       request.Headers.TryAddWithoutValidation("x-ms-range-get-content-crc64", "true");
                   }))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldCrc64.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldCrc64.Headers.GetValues("x-ms-error-code").Single());
        }

        var page = container.GetPageBlobClient("strict-range.vhd");
        await page.CreateAsync(512);
        var pageSas = page.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(5));
        async Task AssertInvalidPageRangeAsync(string range)
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
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.RequestedRangeNotSatisfiable, response.StatusCode);
            Assert.Equal("InvalidPageRange", response.Headers.GetValues("x-ms-error-code").Single());
        }
        await AssertInvalidPageRangeAsync("bytes=0-");
        await AssertInvalidPageRangeAsync("bytes=0-1023");
        await AssertInvalidPageRangeAsync("bytes=1-511");
        Assert.All((await page.DownloadContentAsync()).Value.Content.ToArray(), value => Assert.Equal(0, value));

        await page.ResizeAsync(2560);
        foreach (var offset in new long[] { 0, 1024, 2048 })
        {
            await page.UploadPagesAsync(
                new MemoryStream(Enumerable.Repeat((byte)(offset / 512 + 1), 512).ToArray()),
                offset);
        }

        async Task<HttpResponseMessage> GetPageRangesAsync(string version, string query)
        {
            using var request = new HttpRequestMessage(
                HttpMethod.Get,
                AppendQuery(pageSas, $"comp=pagelist&{query}"));
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            return await transport.SendAsync(request);
        }

        using (var oldPagedRanges = await GetPageRangesAsync("2020-08-04", "maxresults=1"))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldPagedRanges.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldPagedRanges.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var clampedRanges = await GetPageRangesAsync("2020-10-02", "maxresults=10001"))
        {
            Assert.Equal(HttpStatusCode.OK, clampedRanges.StatusCode);
            var xml = await clampedRanges.Content.ReadAsStringAsync();
            Assert.Equal(3, xml.Split("<PageRange>", StringSplitOptions.None).Length - 1);
            Assert.Contains("<NextMarker />", xml, StringComparison.Ordinal);
        }
        string marker;
        using (var firstRangePage = await GetPageRangesAsync("2020-10-02", "maxresults=1"))
        {
            Assert.Equal(HttpStatusCode.OK, firstRangePage.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(await firstRangePage.Content.ReadAsStringAsync());
            Assert.Single(document.Root!.Elements("PageRange"));
            marker = Assert.IsType<string>(document.Root.Element("NextMarker")?.Value);
            Assert.NotEmpty(marker);
        }
        using (var secondRangePage = await GetPageRangesAsync(
                   "2020-10-02",
                   $"maxresults=1&marker={Uri.EscapeDataString(marker)}"))
        {
            Assert.Equal(HttpStatusCode.OK, secondRangePage.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(await secondRangePage.Content.ReadAsStringAsync());
            Assert.Single(document.Root!.Elements("PageRange"));
        }

        var listSas = container.GenerateSasUri(
            BlobContainerSasPermissions.Read | BlobContainerSasPermissions.List,
            DateTimeOffset.UtcNow.AddMinutes(5));
        var listUri = AppendQuery(listSas, "restype=container&comp=list");
        using (var legacyListRequest = new HttpRequestMessage(HttpMethod.Get, listUri))
        {
            legacyListRequest.Headers.TryAddWithoutValidation("x-ms-version", "2009-09-19");
            using var response = await transport.SendAsync(legacyListRequest);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Contains($"<Etag>{legacyEtag}</Etag>", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        }
        using (var modernListRequest = new HttpRequestMessage(HttpMethod.Get, listUri))
        {
            modernListRequest.Headers.TryAddWithoutValidation("x-ms-version", "2011-08-18");
            using var response = await transport.SendAsync(modernListRequest);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Contains($"<Etag>{modernEtag}</Etag>", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task ListingSchemasFollowHistoricalVersionsAndClampPageSizes()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
        {
            ["Sava:AllowAnonymousPublicAccess"] = "true"
        });
        try
        {
            var service = CreateClient(application);
            var containerName = $"listing-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync(
                PublicAccessType.Blob,
                new Dictionary<string, string> { ["purpose"] = "listing" });
            await container.GetBlobClient("folder/a b.txt").UploadAsync(
                BinaryData.FromString("listing payload"),
                new BlobUploadOptions
                {
                    HttpHeaders = new BlobHttpHeaders
                    {
                        ContentType = "text/plain",
                        ContentEncoding = "gzip"
                    }
                });
            await container.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(60));

            var metadata = application.Services.GetRequiredService<MetadataStore>();
            var serviceProperties = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                serviceProperties with
                {
                    ContainerSoftDeleteEnabled = true,
                    ContainerSoftDeleteRetentionDays = 7
                },
                CancellationToken.None);
            var deletedContainer = service.GetBlobContainerClient($"deleted-{Guid.NewGuid():N}");
            await deletedContainer.CreateAsync();
            await deletedContainer.DeleteAsync();

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

            async Task<HttpResponseMessage> ListContainersAsync(string version, string query)
            {
                var request = new HttpRequestMessage(
                    HttpMethod.Get,
                    AppendQuery(serviceSasUri, $"comp=list&{query}"));
                request.Headers.TryAddWithoutValidation("x-ms-version", version);
                return await transport.SendAsync(request);
            }

            async Task<HttpResponseMessage> ListBlobsAsync(string version, string query)
            {
                var request = new HttpRequestMessage(
                    HttpMethod.Get,
                    AppendQuery(containerSasUri, $"restype=container&comp=list&{query}"));
                request.Headers.TryAddWithoutValidation("x-ms-version", version);
                return await transport.SendAsync(request);
            }

            using (var modernContainers = await ListContainersAsync(
                       "2019-12-12",
                       "include=metadata,deleted&maxresults=6000"))
            {
                Assert.Equal(HttpStatusCode.OK, modernContainers.StatusCode);
                var document = System.Xml.Linq.XDocument.Parse(
                    await modernContainers.Content.ReadAsStringAsync());
                Assert.Equal(
                    $"http://{SavaWebApplicationFactory.AccountName}.localhost/",
                    document.Root?.Attribute("ServiceEndpoint")?.Value);
                Assert.Null(document.Root?.Attribute("AccountName"));
                Assert.Equal("5000", document.Root?.Element("MaxResults")?.Value);

                var active = document.Descendants("Container")
                    .Single(item => item.Element("Name")?.Value == containerName);
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
                    .Single(item => item.Element("Name")?.Value == deletedContainer.Name);
                Assert.Equal("true", deleted.Element("Deleted")?.Value);
                Assert.NotEmpty(Assert.IsType<string>(deleted.Element("Version")?.Value));
                Assert.Null(deleted.Element("Properties")?.Element("Deleted"));
                Assert.NotNull(deleted.Element("Properties")?.Element("DeletedTime"));
                Assert.NotNull(deleted.Element("Properties")?.Element("RemainingRetentionDays"));
                Assert.Null(deleted.Element("Properties")?.Element("LeaseStatus"));
            }

            using (var legacyContainers = await ListContainersAsync("2012-02-12", "prefix=listing-&maxresults=1"))
            {
                Assert.Equal(HttpStatusCode.OK, legacyContainers.StatusCode);
                var document = System.Xml.Linq.XDocument.Parse(
                    await legacyContainers.Content.ReadAsStringAsync());
                Assert.Equal(
                    $"http://{SavaWebApplicationFactory.AccountName}.localhost/",
                    document.Root?.Attribute("AccountName")?.Value);
                Assert.Null(document.Root?.Attribute("ServiceEndpoint"));
                var listed = Assert.Single(document.Descendants("Container"));
                Assert.Equal(
                    $"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}",
                    listed.Element("Url")?.Value);
                Assert.NotNull(listed.Element("Properties")?.Element("Last-Modified"));
                Assert.NotNull(listed.Element("Properties")?.Element("LeaseState"));
                Assert.Null(listed.Element("Properties")?.Element("PublicAccess"));
            }

            using (var oldestContainers = await ListContainersAsync("2008-10-27", "prefix=listing-&maxresults=1"))
            {
                Assert.Equal(HttpStatusCode.OK, oldestContainers.StatusCode);
                var document = System.Xml.Linq.XDocument.Parse(
                    await oldestContainers.Content.ReadAsStringAsync());
                var properties = Assert.Single(document.Descendants("Container")).Element("Properties");
                Assert.NotNull(properties?.Element("LastModified"));
                Assert.Null(properties?.Element("Last-Modified"));
                Assert.DoesNotContain('"', Assert.IsType<string>(properties?.Element("Etag")?.Value));
                Assert.Null(properties?.Element("LeaseStatus"));
            }

            using (var oldestBlobs = await ListBlobsAsync("2008-10-27", "prefix=folder/&maxresults=6001"))
            {
                Assert.Equal(HttpStatusCode.OK, oldestBlobs.StatusCode);
                var document = System.Xml.Linq.XDocument.Parse(await oldestBlobs.Content.ReadAsStringAsync());
                Assert.Equal(
                    $"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}",
                    document.Root?.Attribute("ContainerName")?.Value);
                Assert.Null(document.Root?.Attribute("ServiceEndpoint"));
                Assert.Equal("5000", document.Root?.Element("MaxResults")?.Value);
                var listed = Assert.Single(document.Descendants("Blob"));
                Assert.Equal(
                    $"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/folder/a%20b.txt",
                    listed.Element("Url")?.Value);
                Assert.NotNull(listed.Element("LastModified"));
                Assert.Equal("15", listed.Element("Size")?.Value);
                Assert.Equal("text/plain", listed.Element("ContentType")?.Value);
                Assert.Equal("gzip", listed.Element("ContentEncoding")?.Value);
                Assert.Null(listed.Element("Properties"));
                Assert.Null(listed.Element("BlobType"));
            }

            using (var transitionalBlobs = await ListBlobsAsync("2009-09-19", "prefix=folder/"))
            {
                Assert.Equal(HttpStatusCode.OK, transitionalBlobs.StatusCode);
                var document = System.Xml.Linq.XDocument.Parse(await transitionalBlobs.Content.ReadAsStringAsync());
                var listed = Assert.Single(document.Descendants("Blob"));
                Assert.NotNull(listed.Element("Url"));
                Assert.NotNull(listed.Element("Properties")?.Element("Last-Modified"));
                Assert.Equal("BlockBlob", listed.Element("Properties")?.Element("BlobType")?.Value);
                Assert.Equal("unlocked", listed.Element("Properties")?.Element("LeaseStatus")?.Value);
            }

            using (var modernBlobs = await ListBlobsAsync("2013-08-15", "prefix=folder/"))
            {
                Assert.Equal(HttpStatusCode.OK, modernBlobs.StatusCode);
                var document = System.Xml.Linq.XDocument.Parse(await modernBlobs.Content.ReadAsStringAsync());
                Assert.Equal(
                    $"http://{SavaWebApplicationFactory.AccountName}.localhost/",
                    document.Root?.Attribute("ServiceEndpoint")?.Value);
                Assert.Equal(containerName, document.Root?.Attribute("ContainerName")?.Value);
                Assert.Null(Assert.Single(document.Descendants("Blob")).Element("Url"));
            }

            using (var oldDeletedContainers = await ListContainersAsync("2019-07-07", "include=deleted"))
            {
                Assert.Equal(HttpStatusCode.Conflict, oldDeletedContainers.StatusCode);
                Assert.Equal(
                    "FeatureVersionMismatch",
                    oldDeletedContainers.Headers.GetValues("x-ms-error-code").Single());
            }
            using (var oldCopyListing = await ListBlobsAsync("2011-08-18", "include=copy"))
            {
                Assert.Equal(HttpStatusCode.Conflict, oldCopyListing.StatusCode);
                Assert.Equal("FeatureVersionMismatch", oldCopyListing.Headers.GetValues("x-ms-error-code").Single());
            }
            using (var unknownListing = await ListBlobsAsync("2023-11-03", "include=unknown"))
            {
                Assert.Equal(HttpStatusCode.BadRequest, unknownListing.StatusCode);
                Assert.Equal("InvalidQueryParameterValue", unknownListing.Headers.GetValues("x-ms-error-code").Single());
            }
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
        var source = service.GetBlobContainerClient(sourceName);
        await source.CreateAsync(
            PublicAccessType.None,
            new Dictionary<string, string> { ["purpose"] = "rename" });
        var committed = source.GetBlobClient("committed.bin");
        await committed.UploadAsync(
            BinaryData.FromString("container rename preserves content"),
            new BlobUploadOptions
            {
                Metadata = new Dictionary<string, string> { ["state"] = "committed" },
                Tags = new Dictionary<string, string> { ["kind"] = "renamed" }
            });
        var staged = source.GetBlockBlobClient("staged.bin");
        var blockId = Convert.ToBase64String("rename-block"u8);
        await staged.StageBlockAsync(blockId, BinaryData.FromString("staged rename content").ToStream());

        var leaseId = Guid.NewGuid().ToString();
        var lease = source.GetBlobLeaseClient(leaseId);
        await lease.AcquireAsync(TimeSpan.FromSeconds(60));
        var before = await metadata.GetStorageInventoryAsync(CancellationToken.None);

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
        var sas = accountSas.ToSasQueryParameters(credential);
        using var transport = new HttpClient(factory.Server.CreateHandler());

        async Task<HttpResponseMessage> RenameAsync(
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
            return await transport.SendAsync(request);
        }

        using (var oldVersion = await RenameAsync(destinationName, sourceName, serviceVersion: "2020-04-08"))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldVersion.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldVersion.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var missingSource = await RenameAsync(destinationName, sourceContainer: null))
        {
            Assert.Equal(HttpStatusCode.BadRequest, missingSource.StatusCode);
            Assert.Equal("MissingRequiredHeader", missingSource.Headers.GetValues("x-ms-error-code").Single());
            Assert.Contains(
                "<HeaderName>x-ms-source-container-name</HeaderName>",
                await missingSource.Content.ReadAsStringAsync(),
                StringComparison.Ordinal);
        }
        using (var malformedLease = await RenameAsync(destinationName, sourceName, "not-a-lease-id"))
        {
            Assert.Equal(HttpStatusCode.BadRequest, malformedLease.StatusCode);
            Assert.Equal("InvalidHeaderValue", malformedLease.Headers.GetValues("x-ms-error-code").Single());
            Assert.Contains(
                "<HeaderName>x-ms-source-lease-id</HeaderName>",
                await malformedLease.Content.ReadAsStringAsync(),
                StringComparison.Ordinal);
        }
        using (var missingLease = await RenameAsync(destinationName, sourceName))
        {
            Assert.Equal(HttpStatusCode.PreconditionFailed, missingLease.StatusCode);
            Assert.Equal("LeaseIdMissing", missingLease.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var mismatchedLease = await RenameAsync(destinationName, sourceName, Guid.NewGuid().ToString()))
        {
            Assert.Equal(HttpStatusCode.PreconditionFailed, mismatchedLease.StatusCode);
            Assert.Equal(
                "LeaseIdMismatchWithContainerOperation",
                mismatchedLease.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var rename = await RenameAsync(destinationName, sourceName, leaseId))
        {
            Assert.Equal(HttpStatusCode.OK, rename.StatusCode);
            Assert.Null(rename.Headers.ETag);
            Assert.Null(rename.Content.Headers.LastModified);
            Assert.Equal(0, rename.Content.Headers.ContentLength);
        }

        Assert.False((await source.ExistsAsync()).Value);
        var destination = service.GetBlobContainerClient(destinationName);
        var properties = (await destination.GetPropertiesAsync()).Value;
        Assert.Equal("rename", properties.Metadata["purpose"]);
        Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Leased, properties.LeaseState);
        var renamedBlob = destination.GetBlobClient(committed.Name);
        Assert.Equal(
            "container rename preserves content",
            (await renamedBlob.DownloadContentAsync()).Value.Content.ToString());
        Assert.Equal("committed", (await renamedBlob.GetPropertiesAsync()).Value.Metadata["state"]);
        Assert.Equal("renamed", (await renamedBlob.GetTagsAsync()).Value.Tags["kind"]);
        var renamedBlocks = (await destination.GetBlockBlobClient(staged.Name)
            .GetBlockListAsync(BlockListTypes.Uncommitted)).Value;
        Assert.Equal(blockId, Assert.Single(renamedBlocks.UncommittedBlocks).Name);
        var tagged = await service.FindBlobsByTagsAsync("\"kind\" = 'renamed'").ToListAsync();
        Assert.Equal(destinationName, Assert.Single(tagged, item => item.BlobName == committed.Name).BlobContainerName);

        var after = await metadata.GetStorageInventoryAsync(CancellationToken.None);
        Assert.Equal(before.LogicalBlobBytes, after.LogicalBlobBytes);
        Assert.Equal(before.LogicalStagedBlockBytes, after.LogicalStagedBlockBytes);
        Assert.Equal(before.BlobRecordCount, after.BlobRecordCount);
        Assert.Equal(before.StagedBlockCount, after.StagedBlockCount);
        Assert.True(before.ReachableChunkIds.SetEquals(after.ReachableChunkIds));

        var collisionSource = service.GetBlobContainerClient($"rename-collision-source-{Guid.NewGuid():N}");
        var collisionDestination = service.GetBlobContainerClient($"rename-collision-target-{Guid.NewGuid():N}");
        await collisionSource.CreateAsync();
        await collisionDestination.CreateAsync();
        using var collision = await RenameAsync(collisionDestination.Name, collisionSource.Name);
        Assert.Equal(HttpStatusCode.Conflict, collision.StatusCode);
        Assert.Equal("ContainerAlreadyExists", collision.Headers.GetValues("x-ms-error-code").Single());
        Assert.True((await collisionSource.ExistsAsync()).Value);
    }

    [Fact]
    public async Task RenameContainerRequiresBearerAccessToBothNames()
    {
        var sourceName = $"rename-bearer-source-{Guid.NewGuid():N}";
        var destinationName = $"rename-bearer-target-{Guid.NewGuid():N}";
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
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
            var source = service.GetBlobContainerClient(sourceName);
            await source.CreateAsync(
                PublicAccessType.None,
                new Dictionary<string, string> { ["purpose"] = "restore" });
            var committed = source.GetBlobClient("committed.bin");
            await committed.UploadAsync(
                BinaryData.FromString("preserved container content"),
                new BlobUploadOptions
                {
                    Metadata = new Dictionary<string, string> { ["state"] = "committed" },
                    Tags = new Dictionary<string, string> { ["kind"] = "restored" }
                });
            var staged = source.GetBlockBlobClient("staged.bin");
            var blockId = Convert.ToBase64String("restore-block"u8);
            await staged.StageBlockAsync(blockId, BinaryData.FromString("uncommitted content").ToStream());
            await source.DeleteAsync();

            var deleted = await service.GetBlobContainersAsync(
                    states: BlobContainerStates.Deleted,
                    prefix: sourceName)
                .SingleAsync();
            Assert.True(deleted.IsDeleted);
            Assert.False(string.IsNullOrWhiteSpace(deleted.VersionId));

#pragma warning disable AZC0015
            var restore = await service.UndeleteBlobContainerAsync(
                sourceName,
                deleted.VersionId,
                destinationName,
                CancellationToken.None);
#pragma warning restore AZC0015
            Assert.Equal(201, restore.GetRawResponse().Status);
            Assert.Equal(destinationName, restore.Value.Name);
            Assert.False(restore.GetRawResponse().Headers.TryGetValue("ETag", out _));
            Assert.False(restore.GetRawResponse().Headers.TryGetValue("Last-Modified", out _));
            Assert.True(restore.GetRawResponse().Headers.TryGetValue("Content-Length", out var restoreLength));
            Assert.Equal("0", restoreLength);

            Assert.False((await source.ExistsAsync()).Value);
            Assert.Empty(await service.GetBlobContainersAsync(
                    states: BlobContainerStates.Deleted,
                    prefix: sourceName)
                .ToListAsync());

            var destination = service.GetBlobContainerClient(destinationName);
            var destinationProperties = (await destination.GetPropertiesAsync()).Value;
            Assert.Equal("restore", destinationProperties.Metadata["purpose"]);
            var restoredBlob = destination.GetBlobClient(committed.Name);
            Assert.Equal("preserved container content", (await restoredBlob.DownloadContentAsync()).Value.Content.ToString());
            Assert.Equal("committed", (await restoredBlob.GetPropertiesAsync()).Value.Metadata["state"]);
            Assert.Equal("restored", (await restoredBlob.GetTagsAsync()).Value.Tags["kind"]);
            var tagged = await service.FindBlobsByTagsAsync("\"kind\" = 'restored'").ToListAsync();
            var restoredTag = Assert.Single(tagged, item => item.BlobName == committed.Name);
            Assert.Equal(destinationName, restoredTag.BlobContainerName);
            var restoredBlocks = (await destination.GetBlockBlobClient(staged.Name)
                .GetBlockListAsync(BlockListTypes.Uncommitted)).Value;
            Assert.Equal(blockId, Assert.Single(restoredBlocks.UncommittedBlocks).Name);

            var sameName = service.GetBlobContainerClient($"restore-same-{Guid.NewGuid():N}");
            await sameName.CreateAsync();
            await sameName.GetBlobClient("same.bin").UploadAsync(BinaryData.FromString("same-name content"));
            await sameName.DeleteAsync();
            var sameDeleted = await service.GetBlobContainersAsync(
                    states: BlobContainerStates.Deleted,
                    prefix: sameName.Name)
                .SingleAsync();
            var sameRestore = await service.UndeleteBlobContainerAsync(sameName.Name, sameDeleted.VersionId);
            Assert.Equal(201, sameRestore.GetRawResponse().Status);
            Assert.Equal("same-name content", (await sameName.GetBlobClient("same.bin").DownloadContentAsync()).Value.Content.ToString());

            var credential = new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey);
            Uri RestoreSasUri(string target, AccountSasPermissions permissions)
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

            using var transport = new HttpClient(factory.Server.CreateHandler());
            using (var missingName = new HttpRequestMessage(
                       HttpMethod.Put,
                       RestoreSasUri($"missing-name-{Guid.NewGuid():N}", AccountSasPermissions.Write))
            {
                Content = new ByteArrayContent([])
            })
            {
                missingName.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                missingName.Headers.TryAddWithoutValidation("x-ms-deleted-container-version", "missing");
                using var response = await transport.SendAsync(missingName);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
                Assert.Contains(
                    "<HeaderName>x-ms-deleted-container-name</HeaderName>",
                    await response.Content.ReadAsStringAsync(),
                    StringComparison.Ordinal);
            }
            using (var missingVersion = new HttpRequestMessage(
                       HttpMethod.Put,
                       RestoreSasUri($"missing-version-{Guid.NewGuid():N}", AccountSasPermissions.Write))
            {
                Content = new ByteArrayContent([])
            })
            {
                missingVersion.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                missingVersion.Headers.TryAddWithoutValidation("x-ms-deleted-container-name", "missing");
                using var response = await transport.SendAsync(missingVersion);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
            }
            using (var createOnly = new HttpRequestMessage(
                       HttpMethod.Put,
                       RestoreSasUri($"create-only-{Guid.NewGuid():N}", AccountSasPermissions.Create))
            {
                Content = new ByteArrayContent([])
            })
            {
                createOnly.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                createOnly.Headers.TryAddWithoutValidation("x-ms-deleted-container-name", "missing");
                createOnly.Headers.TryAddWithoutValidation("x-ms-deleted-container-version", "missing");
                using var response = await transport.SendAsync(createOnly);
                Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
                Assert.Equal(
                    "AuthorizationPermissionMismatch",
                    response.Headers.GetValues("x-ms-error-code").Single());
            }

            var collisionSource = service.GetBlobContainerClient($"restore-collision-source-{Guid.NewGuid():N}");
            var collisionDestination = service.GetBlobContainerClient($"restore-collision-target-{Guid.NewGuid():N}");
            await collisionSource.CreateAsync();
            await collisionSource.GetBlobClient("retained.bin").UploadAsync(BinaryData.FromString("retained"));
            await collisionSource.DeleteAsync();
            await collisionDestination.CreateAsync();
            var collisionDeleted = await service.GetBlobContainersAsync(
                    states: BlobContainerStates.Deleted,
                    prefix: collisionSource.Name)
                .SingleAsync();
#pragma warning disable AZC0015
            var collision = await Assert.ThrowsAsync<RequestFailedException>(() =>
                service.UndeleteBlobContainerAsync(
                    collisionSource.Name,
                    collisionDeleted.VersionId,
                    collisionDestination.Name,
                    CancellationToken.None));
#pragma warning restore AZC0015
            Assert.Equal(409, collision.Status);
            Assert.Equal("ContainerAlreadyExists", collision.ErrorCode);
            Assert.Single(await service.GetBlobContainersAsync(
                    states: BlobContainerStates.Deleted,
                    prefix: collisionSource.Name)
                .ToListAsync());

            var consumed = await Assert.ThrowsAsync<RequestFailedException>(() =>
                service.UndeleteBlobContainerAsync(sourceName, deleted.VersionId));
            Assert.Equal(404, consumed.Status);
            Assert.Equal("ContainerNotFound", consumed.ErrorCode);

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
            new Dictionary<string, string?>
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
        var propertiesUri = new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/" +
            $"?restype=service&comp=properties&{accountSas.ToSasQueryParameters(credential)}");
        using var transport = new HttpClient(factory.Server.CreateHandler());

        async Task SetPermanentDeleteAsync(bool enabled)
        {
            using var request = new HttpRequestMessage(HttpMethod.Put, propertiesUri)
            {
                Content = new StringContent(
                    $"""
                    <StorageServiceProperties>
                      <DeleteRetentionPolicy>
                        <Enabled>true</Enabled>
                        <Days>7</Days>
                        <AllowPermanentDelete>{enabled.ToString().ToLowerInvariant()}</AllowPermanentDelete>
                      </DeleteRetentionPolicy>
                    </StorageServiceProperties>
                    """,
                    Encoding.UTF8,
                    "application/xml")
            };
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        }

        try
        {
            using (var oldSetting = new HttpRequestMessage(HttpMethod.Put, propertiesUri)
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
            })
            {
                oldSetting.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
                using var response = await transport.SendAsync(oldSetting);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
            }

            await SetPermanentDeleteAsync(enabled: true);
            var roundTrip = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            Assert.True(roundTrip.BlobSoftDeleteEnabled);
            Assert.Equal(7, roundTrip.BlobSoftDeleteRetentionDays);
            Assert.True(roundTrip.BlobPermanentDeleteEnabled);
            using (var propertiesRequest = new HttpRequestMessage(HttpMethod.Get, propertiesUri))
            {
                propertiesRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(propertiesRequest);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Contains(
                    "<AllowPermanentDelete>true</AllowPermanentDelete>",
                    await response.Content.ReadAsStringAsync(),
                    StringComparison.Ordinal);
            }
            using (var oldPropertiesRequest = new HttpRequestMessage(HttpMethod.Get, propertiesUri))
            {
                oldPropertiesRequest.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
                using var response = await transport.SendAsync(oldPropertiesRequest);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.DoesNotContain(
                    "<AllowPermanentDelete>",
                    await response.Content.ReadAsStringAsync(),
                    StringComparison.Ordinal);
            }

            var blob = container.GetBlobClient("purge.txt");
            await blob.UploadAsync(BinaryData.FromString("retained snapshot"));
            var snapshotId = (await blob.CreateSnapshotAsync()).Value.Snapshot;
            var snapshot = blob.WithSnapshot(snapshotId);
            var softDelete = await snapshot.DeleteAsync();
            Assert.True(softDelete.Headers.TryGetValue("x-ms-delete-type-permanent", out var softDeleteHeader));
            Assert.Equal("false", softDeleteHeader);

            var deletedSnapshot = await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                blob.Name,
                versionId: null,
                snapshot: snapshotId,
                includeDeleted: true,
                CancellationToken.None);
            Assert.True(deletedSnapshot?.IsDeleted);

            using (var denied = new HttpRequestMessage(
                       HttpMethod.Delete,
                       AppendQuery(
                           snapshot.GenerateSasUri(BlobSasPermissions.Delete, DateTimeOffset.UtcNow.AddMinutes(5)),
                           "deletetype=permanent")))
            {
                denied.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(denied);
                Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            }

            var permanentUri = AppendQuery(
                snapshot.GenerateSasUri(BlobSasPermissions.PermanentDelete, DateTimeOffset.UtcNow.AddMinutes(5)),
                "deletetype=permanent");
            using (var oldVersion = new HttpRequestMessage(HttpMethod.Delete, permanentUri))
            {
                oldVersion.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
                using var response = await transport.SendAsync(oldVersion);
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
                using var response = await transport.SendAsync(rootDelete);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal(
                    "PermanentDeleteNotSupportedOnRootBlob",
                    response.Headers.GetValues("x-ms-error-code").Single());
            }

            var activeSnapshotId = (await blob.CreateSnapshotAsync()).Value.Snapshot;
            var activeSnapshot = blob.WithSnapshot(activeSnapshotId);
            using (var activeDelete = new HttpRequestMessage(
                       HttpMethod.Delete,
                       AppendQuery(
                           activeSnapshot.GenerateSasUri(
                               BlobSasPermissions.PermanentDelete,
                               DateTimeOffset.UtcNow.AddMinutes(5)),
                           "deletetype=permanent")))
            {
                activeDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(activeDelete);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            }

            await SetPermanentDeleteAsync(enabled: false);
            using (var disabledDelete = new HttpRequestMessage(HttpMethod.Delete, permanentUri))
            {
                disabledDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(disabledDelete);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            }
            Assert.NotNull(await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                blob.Name,
                versionId: null,
                snapshot: snapshotId,
                includeDeleted: true,
                CancellationToken.None));

            await SetPermanentDeleteAsync(enabled: true);
            using (var invalidDeleteType = new HttpRequestMessage(
                       HttpMethod.Delete,
                       AppendQuery(
                           snapshot.GenerateSasUri(
                               BlobSasPermissions.PermanentDelete,
                               DateTimeOffset.UtcNow.AddMinutes(5)),
                           "deletetype=Permanent")))
            {
                invalidDeleteType.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(invalidDeleteType);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidQueryParameterValue", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var permanentDelete = new HttpRequestMessage(HttpMethod.Delete, permanentUri))
            {
                permanentDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(permanentDelete);
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
                CancellationToken.None));
            Assert.True((await activeSnapshot.ExistsAsync()).Value);
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
            using var transport = new HttpClient(factory.Server.CreateHandler());

            using (var invalidValue = new HttpRequestMessage(
                       HttpMethod.Delete,
                       blob.GenerateSasUri(BlobSasPermissions.Delete, DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                invalidValue.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                invalidValue.Headers.TryAddWithoutValidation("x-ms-delete-snapshots", "Include");
                using var response = await transport.SendAsync(invalidValue);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var scopedToSnapshot = new HttpRequestMessage(
                       HttpMethod.Delete,
                       firstSnapshot.GenerateSasUri(BlobSasPermissions.Delete, DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                scopedToSnapshot.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                scopedToSnapshot.Headers.TryAddWithoutValidation("x-ms-delete-snapshots", "include");
                using var response = await transport.SendAsync(scopedToSnapshot);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }

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
            using var transport = new HttpClient(factory.Server.CreateHandler());
            using (var ordinaryDelete = new HttpRequestMessage(
                       HttpMethod.Delete,
                       currentVersion.GenerateSasUri(
                           BlobSasPermissions.Delete,
                           DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                ordinaryDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(ordinaryDelete);
                Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            }
            Assert.True((await currentVersion.ExistsAsync()).Value);

            var versionDeleteUri = currentVersion.GenerateSasUri(
                BlobSasPermissions.DeleteBlobVersion,
                DateTimeOffset.UtcNow.AddMinutes(5));
            using (var versionDelete = new HttpRequestMessage(HttpMethod.Delete, versionDeleteUri))
            {
                versionDelete.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(versionDelete);
                Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
                Assert.Equal("true", response.Headers.GetValues("x-ms-delete-type-permanent").Single());
            }
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

            using var transport = new HttpClient(factory.Server.CreateHandler());
            using (var currentRequest = new HttpRequestMessage(
                       HttpMethod.Get,
                       blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                currentRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(currentRequest);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal(currentVersionId, response.Headers.GetValues("x-ms-version-id").Single());
                Assert.Equal("true", response.Headers.GetValues("x-ms-is-current-version").Single());
            }

            using (var historicalRequest = new HttpRequestMessage(
                       HttpMethod.Get,
                       historical.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                historicalRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(historicalRequest);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal(historicalVersionId, response.Headers.GetValues("x-ms-version-id").Single());
                Assert.Equal("false", response.Headers.GetValues("x-ms-is-current-version").Single());
            }

            using (var legacyRequest = new HttpRequestMessage(
                       HttpMethod.Head,
                       blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                legacyRequest.Headers.TryAddWithoutValidation("x-ms-version", "2019-07-07");
                using var response = await transport.SendAsync(legacyRequest);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.False(response.Headers.Contains("x-ms-version-id"));
                Assert.False(response.Headers.Contains("x-ms-is-current-version"));
            }
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
    public async Task BlobFamilyMutationsAreIndexedAndAtomic()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
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

            var deleted = container.GetBlobClient("delete-with-snapshots");
            await deleted.UploadAsync(BinaryData.FromString("delete me"));
            await deleted.CreateSnapshotAsync();
            await deleted.CreateSnapshotAsync();
            var unrelated = container.GetBlobClient("unrelated-corrupt-record");
            await unrelated.UploadAsync(BinaryData.FromString("unrelated"));

            var directConnectionString = new SqliteConnectionStringBuilder
            {
                DataSource = Path.Combine(application.DataPath, "metadata.db"),
                ForeignKeys = true
            }.ToString();
            await using (var connection = new SqliteConnection(directConnectionString))
            {
                await connection.OpenAsync();
                await using var corrupt = connection.CreateCommand();
                corrupt.CommandText = """
                    UPDATE blobs SET data = 'not-json'
                    WHERE account = $account AND container = $container AND name = $name;
                    """;
                corrupt.Parameters.AddWithValue("$account", SavaWebApplicationFactory.AccountName);
                corrupt.Parameters.AddWithValue("$container", container.Name);
                corrupt.Parameters.AddWithValue("$name", unrelated.Name);
                Assert.Equal(1, await corrupt.ExecuteNonQueryAsync());
            }

            await deleted.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots);
            Assert.Empty(await metadata.ListBlobFamilyAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                deleted.Name,
                includeDeleted: true,
                CancellationToken.None));

            await using (var connection = new SqliteConnection(directConnectionString))
            {
                await connection.OpenAsync();
                await using var removeCorruptRecord = connection.CreateCommand();
                removeCorruptRecord.CommandText = """
                    DELETE FROM blobs
                    WHERE account = $account AND container = $container AND name = $name;
                    """;
                removeCorruptRecord.Parameters.AddWithValue("$account", SavaWebApplicationFactory.AccountName);
                removeCorruptRecord.Parameters.AddWithValue("$container", container.Name);
                removeCorruptRecord.Parameters.AddWithValue("$name", unrelated.Name);
                Assert.Equal(1, await removeCorruptRecord.ExecuteNonQueryAsync());
            }

            var atomic = container.GetBlobClient("atomic-family");
            await atomic.UploadAsync(BinaryData.FromString("atomic"));
            await atomic.CreateSnapshotAsync();
            var before = await metadata.ListBlobFamilyAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                atomic.Name,
                includeDeleted: true,
                CancellationToken.None);
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
                        Metadata = new Dictionary<string, string> { ["mutated"] = "true" }
                    }),
                new BlobRecordMutation(
                    second.GenerationId,
                    "stale-revision",
                    second with
                    {
                        Revision = MetadataStore.NewRevision(),
                        Metadata = new Dictionary<string, string> { ["mutated"] = "true" }
                    })
            };
            await Assert.ThrowsAsync<StorageConcurrencyException>(() =>
                metadata.ApplyBlobRecordMutationsAsync(failedBatch, CancellationToken.None));
            var afterFailedBatch = await metadata.ListBlobFamilyAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                atomic.Name,
                includeDeleted: true,
                CancellationToken.None);
            Assert.Equal(
                before.Select(item => (item.GenerationId, item.Revision)),
                afterFailedBatch.Select(item => (item.GenerationId, item.Revision)));
            Assert.All(afterFailedBatch, item => Assert.False(item.Metadata.ContainsKey("mutated")));

            var properties = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                properties with { BlobSoftDeleteEnabled = true, BlobSoftDeleteRetentionDays = 7 },
                CancellationToken.None);
            var restored = container.GetBlobClient("restore-family");
            await restored.UploadAsync(BinaryData.FromString("restore"));
            await restored.CreateSnapshotAsync();
            await restored.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots);
            var softDeleted = await metadata.ListBlobFamilyAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                restored.Name,
                includeDeleted: true,
                CancellationToken.None);
            Assert.Equal(2, softDeleted.Count);
            Assert.All(softDeleted, item => Assert.True(item.IsDeleted));

            await restored.UndeleteAsync();
            var undeleted = await metadata.ListBlobFamilyAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                restored.Name,
                includeDeleted: true,
                CancellationToken.None);
            Assert.Equal(2, undeleted.Count);
            Assert.All(undeleted, item => Assert.False(item.IsDeleted));
        }
        finally
        {
            await application.DisposeAsync();
        }
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
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
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
            var content = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, 5000).Select(index =>
                $"{{\"tenant\":\"stable-tenant\",\"category\":\"storage-event\",\"sequence\":{index % 97},\"payload\":\"alpha-beta-gamma-delta\"}}\n")));
            var blob = container.GetBlobClient("cold.jsonl");
            await blob.UploadAsync(BinaryData.FromBytes(content));
            var snapshotId = (await blob.CreateSnapshotAsync()).Value.Snapshot;
            var propertiesBefore = await blob.GetPropertiesAsync();

            var customerKey = RandomNumberGenerator.GetBytes(32);
            var customerBlob = CreateEncryptedClient(
                    application,
                    new CustomerProvidedKey(customerKey),
                    encryptionScope: null)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("customer-key.jsonl");
            await customerBlob.UploadAsync(BinaryData.FromBytes(content));

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
                var pinnedPass = await blobService.RunMaintenanceAsync(CancellationToken.None);
                Assert.Equal(0, pinnedPass.RecompressedChunks);
                Assert.Equal(beforeBytes, paths.Sum(path => new FileInfo(path).Length));
            }

            var optimized = await blobService.RunMaintenanceAsync(CancellationToken.None);
            var afterBytes = paths.Sum(path => new FileInfo(path).Length);
            Assert.True(optimized.RecompressedChunks > 0);
            Assert.Equal(beforeBytes - afterBytes, optimized.RecompressionBytesSaved);
            Assert.True(afterBytes < beforeBytes);
            Assert.Equal(customerBytes, customerPaths.Sum(path => new FileInfo(path).Length));

            var propertiesAfter = await blob.GetPropertiesAsync();
            Assert.Equal(propertiesBefore.Value.ETag, propertiesAfter.Value.ETag);
            Assert.Equal(propertiesBefore.Value.LastModified, propertiesAfter.Value.LastModified);
            Assert.Equal(content, (await blob.DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(
                content,
                (await blob.WithSnapshot(snapshotId).DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(content, (await customerBlob.DownloadContentAsync()).Value.Content.ToArray());

            using var operatorClient = application.CreateClient();
            var metrics = await operatorClient.GetStringAsync("/metrics");
            Assert.Contains("mk8_sava_maintenance_recompressed_chunks_total", metrics, StringComparison.Ordinal);
            Assert.Contains("mk8_sava_maintenance_recompression_bytes_saved_total", metrics, StringComparison.Ordinal);
        }
        finally
        {
            await application.DisposeAsync();
        }
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
        Assert.Equal([secondId, firstId], blocks.Value.CommittedBlocks.Select(item => item.Name));
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
            append.SetMetadataAsync(new Dictionary<string, string> { ["locked"] = "true" }));
        Assert.Equal(412, denied.Status);
        await append.SetMetadataAsync(
            new Dictionary<string, string> { ["locked"] = "true" },
            new BlobRequestConditions { LeaseId = acquired.Value.LeaseId });
        await lease.ReleaseAsync();

        var beforeSeal = (await append.GetPropertiesAsync()).Value;
        var wrongPosition = await Assert.ThrowsAsync<RequestFailedException>(() =>
            append.SealAsync(new AppendBlobRequestConditions
            {
                IfAppendPositionEqual = beforeSeal.ContentLength + 1
            }));
        Assert.Equal(StatusCodes.Status412PreconditionFailed, wrongPosition.Status);
        Assert.Equal("AppendPositionConditionNotMet", wrongPosition.ErrorCode);

        var wrongEtag = await Assert.ThrowsAsync<RequestFailedException>(() =>
            append.SealAsync(new AppendBlobRequestConditions
            {
                IfMatch = new ETag("\"not-the-current-etag\"")
            }));
        Assert.Equal(StatusCodes.Status412PreconditionFailed, wrongEtag.Status);
        Assert.Equal("ConditionNotMet", wrongEtag.ErrorCode);

        var sealUri = AppendQuery(
            append.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=seal");
        using (var transport = new HttpClient(factory.Server.CreateHandler()))
        using (var bodyRequest = new HttpRequestMessage(HttpMethod.Put, sealUri)
        {
            Content = new ByteArrayContent([1])
        })
        {
            bodyRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var bodyResponse = await transport.SendAsync(bodyRequest);
            Assert.Equal(HttpStatusCode.BadRequest, bodyResponse.StatusCode);
            Assert.Equal("InvalidHeaderValue", bodyResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        await append.SealAsync(new AppendBlobRequestConditions
        {
            IfAppendPositionEqual = beforeSeal.ContentLength
        });
        Assert.True((await append.GetPropertiesAsync()).Value.IsSealed);
    }

    [Fact]
    public async Task BlobLeaseTransitionsMatchAzureStateAndErrorContracts()
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>
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
            var acquired = await lease.AcquireAsync(TimeSpan.FromSeconds(15));
            Assert.Equal(leaseId, acquired.Value.LeaseId);
            var leased = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Leased, leased.LeaseState);
            Assert.Equal(LeaseStatus.Locked, leased.LeaseStatus);
            Assert.Equal(LeaseDurationType.Fixed, leased.LeaseDuration);

            await blob.UploadAsync(
                BinaryData.FromString("authorized overwrite"),
                new BlobUploadOptions
                {
                    Conditions = new BlobRequestConditions { LeaseId = leaseId }
                });
            Assert.Equal(
                Azure.Storage.Blobs.Models.LeaseState.Leased,
                (await blob.GetPropertiesAsync()).Value.LeaseState);

            await lease.AcquireAsync(TimeSpan.FromSeconds(30));
            var competing = blob.GetBlobLeaseClient(Guid.NewGuid().ToString());
            var alreadyLeased = await Assert.ThrowsAsync<RequestFailedException>(() =>
                competing.AcquireAsync(TimeSpan.FromSeconds(15)));
            Assert.Equal(409, alreadyLeased.Status);
            Assert.Equal("LeaseAlreadyPresent", alreadyLeased.ErrorCode);

            clock.Advance(TimeSpan.FromSeconds(31));
            var expired = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Expired, expired.LeaseState);
            Assert.Equal(LeaseStatus.Unlocked, expired.LeaseStatus);
            Assert.Equal(default, expired.LeaseDuration);

            await lease.RenewAsync();
            Assert.Equal(
                Azure.Storage.Blobs.Models.LeaseState.Leased,
                (await blob.GetPropertiesAsync()).Value.LeaseState);

            var changedId = Guid.NewGuid().ToString();
            var changed = await lease.ChangeAsync(changedId);
            Assert.Equal(changedId, changed.Value.LeaseId);
            leaseId = changedId;
            lease = blob.GetBlobLeaseClient(leaseId);

            var initialBreak = await lease.BreakAsync();
            Assert.Equal(30, initialBreak.Value.LeaseTime);
            var breaking = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Breaking, breaking.LeaseState);
            Assert.Equal(LeaseStatus.Locked, breaking.LeaseStatus);
            Assert.Equal(default, breaking.LeaseDuration);

            var missing = await Assert.ThrowsAsync<RequestFailedException>(() =>
                blob.SetMetadataAsync(new Dictionary<string, string> { ["breaking"] = "missing" }));
            Assert.Equal(412, missing.Status);
            Assert.Equal("LeaseIdMissing", missing.ErrorCode);
            await blob.SetMetadataAsync(
                new Dictionary<string, string> { ["breaking"] = "authorized" },
                new BlobRequestConditions { LeaseId = leaseId });

            var unchangedBreak = await lease.BreakAsync(TimeSpan.FromSeconds(60));
            Assert.Equal(30, unchangedBreak.Value.LeaseTime);
            var shortenedBreak = await lease.BreakAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(2, shortenedBreak.Value.LeaseTime);
            clock.Advance(TimeSpan.FromSeconds(3));
            var broken = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(Azure.Storage.Blobs.Models.LeaseState.Broken, broken.LeaseState);
            Assert.Equal(LeaseStatus.Unlocked, broken.LeaseStatus);

            var brokenRenew = await Assert.ThrowsAsync<RequestFailedException>(() => lease.RenewAsync());
            Assert.Equal(409, brokenRenew.Status);
            Assert.Equal("LeaseIsBrokenAndCannotBeRenewed", brokenRenew.ErrorCode);
            await lease.ReleaseAsync();

            await lease.AcquireAsync(TimeSpan.FromSeconds(15));
            clock.Advance(TimeSpan.FromSeconds(16));
            await blob.SetMetadataAsync(new Dictionary<string, string> { ["expired"] = "rewritten" });
            Assert.Equal(
                Azure.Storage.Blobs.Models.LeaseState.Available,
                (await blob.GetPropertiesAsync()).Value.LeaseState);
            var invalidatedRenew = await Assert.ThrowsAsync<RequestFailedException>(() => lease.RenewAsync());
            Assert.Equal(409, invalidatedRenew.Status);
            Assert.Equal("LeaseIdMismatchWithLeaseOperation", invalidatedRenew.ErrorCode);

            var infiniteId = Guid.NewGuid().ToString();
            var infinite = blob.GetBlobLeaseClient(infiniteId);
            await infinite.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration);
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
                using var invalidBreakResponse = await transport.SendAsync(invalidBreak);
                Assert.Equal(HttpStatusCode.BadRequest, invalidBreakResponse.StatusCode);
                Assert.Equal("InvalidHeaderValue", invalidBreakResponse.Headers.GetValues("x-ms-error-code").Single());
            }

            var immediateBreak = await infinite.BreakAsync();
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
                using var invalidProposedResponse = await transport.SendAsync(invalidProposed);
                Assert.Equal(HttpStatusCode.BadRequest, invalidProposedResponse.StatusCode);
                Assert.Equal("InvalidHeaderValue", invalidProposedResponse.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var missingDuration = new HttpRequestMessage(HttpMethod.Put, leaseUri)
            {
                Content = new ByteArrayContent([])
            })
            {
                missingDuration.Headers.Add("x-ms-version", "2025-11-05");
                missingDuration.Headers.Add("x-ms-lease-action", "acquire");
                using var missingDurationResponse = await transport.SendAsync(missingDuration);
                Assert.Equal(HttpStatusCode.BadRequest, missingDurationResponse.StatusCode);
                Assert.Equal("MissingRequiredHeader", missingDurationResponse.Headers.GetValues("x-ms-error-code").Single());
            }
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task ContainerLeasesGuardDeletionButNotOrdinaryContainerWrites()
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>
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

            await container.SetMetadataAsync(new Dictionary<string, string> { ["active"] = "allowed" });
            var missing = await Assert.ThrowsAsync<RequestFailedException>(() => container.DeleteAsync());
            Assert.Equal(412, missing.Status);
            Assert.Equal("LeaseIdMissing", missing.ErrorCode);

            clock.Advance(TimeSpan.FromSeconds(16));
            Assert.Equal(
                Azure.Storage.Blobs.Models.LeaseState.Expired,
                (await container.GetPropertiesAsync()).Value.LeaseState);
            await container.SetMetadataAsync(new Dictionary<string, string> { ["expired"] = "retained" });
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
            new Dictionary<string, string?>
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
                    Metadata = new Dictionary<string, string>
                    {
                        ["owner"] = "base",
                        ["retained"] = "base-only"
                    },
                    Tags = new Dictionary<string, string> { ["kind"] = "snapshot" }
                });
            var original = (await blob.GetPropertiesAsync()).Value;
            var snapshotUri = AppendQuery(
                blob.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(10)),
                "comp=snapshot");

            using var transport = new HttpClient(application.Server.CreateHandler());
            using (var unavailable = new HttpRequestMessage(HttpMethod.Put, snapshotUri)
            {
                Content = new ByteArrayContent([])
            })
            {
                unavailable.Headers.TryAddWithoutValidation("x-ms-version", "2008-10-27");
                using var response = await transport.SendAsync(unavailable);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
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
                using var response = await transport.SendAsync(inherit);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                inheritedSnapshot = response.Headers.GetValues("x-ms-snapshot").Single();
                Assert.Equal(original.ETag.ToString(), GetResponseHeader(response, "ETag"));
                Assert.Equal(
                    original.LastModified.ToString("R", CultureInfo.InvariantCulture),
                    GetResponseHeader(response, "Last-Modified"));
            }

            var inherited = blob.WithSnapshot(inheritedSnapshot);
            var inheritedProperties = (await inherited.GetPropertiesAsync()).Value;
            Assert.Equal(original.ETag, inheritedProperties.ETag);
            Assert.Equal(original.LastModified, inheritedProperties.LastModified);
            Assert.Equal("base", inheritedProperties.Metadata["owner"]);
            Assert.Equal("base-only", inheritedProperties.Metadata["retained"]);
            Assert.Equal("snapshot", (await inherited.GetTagsAsync()).Value.Tags["kind"]);
            using (var oldSnapshotRead = new HttpRequestMessage(
                       HttpMethod.Head,
                       inherited.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(10))))
            {
                oldSnapshotRead.Headers.TryAddWithoutValidation("x-ms-version", "2008-10-27");
                using var response = await transport.SendAsync(oldSnapshotRead);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
                Assert.False(response.Headers.Contains("x-ms-version"));
            }

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
                using var response = await transport.SendAsync(replace);
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
            var replacedProperties = (await replaced.GetPropertiesAsync()).Value;
            Assert.Equal(replacedEtag, replacedProperties.ETag);
            Assert.Equal(replacedLastModified, replacedProperties.LastModified);
            Assert.Equal("snapshot", replacedProperties.Metadata["owner"]);
            Assert.False(replacedProperties.Metadata.ContainsKey("retained"));
            Assert.Equal("snapshot", (await replaced.GetTagsAsync()).Value.Tags["kind"]);

            var unchangedBase = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(original.ETag, unchangedBase.ETag);
            Assert.Equal(original.LastModified, unchangedBase.LastModified);
            Assert.Equal("base", unchangedBase.Metadata["owner"]);
            Assert.Equal("base-only", unchangedBase.Metadata["retained"]);

            var lease = blob.GetBlobLeaseClient();
            await lease.AcquireAsync(TimeSpan.FromSeconds(15));
            var createOnlySnapshotUri = AppendQuery(
                blob.GenerateSasUri(BlobSasPermissions.Create, DateTimeOffset.UtcNow.AddMinutes(10)),
                "comp=snapshot");
            using (var createOnly = new HttpRequestMessage(HttpMethod.Put, createOnlySnapshotUri)
            {
                Content = new ByteArrayContent([])
            })
            {
                createOnly.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(createOnly);
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
                using var response = await transport.SendAsync(wrongLease);
                Assert.Equal(HttpStatusCode.PreconditionFailed, response.StatusCode);
                Assert.Equal("LeaseIdMismatchWithBlobOperation", response.Headers.GetValues("x-ms-error-code").Single());
            }
            await lease.ReleaseAsync();

            var archived = container.GetBlobClient("archived.bin");
            await archived.UploadAsync(BinaryData.FromString("offline payload"));
            await archived.SetAccessTierAsync(AccessTier.Archive);
            var rejected = await Assert.ThrowsAsync<RequestFailedException>(() => archived.CreateSnapshotAsync());
            Assert.Equal(HttpStatusCode.Conflict, (HttpStatusCode)rejected.Status);
            Assert.Equal("BlobArchived", rejected.ErrorCode);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task LeaseContractsHonorHistoricalDurationsHeadersAndContainerMutationRules()
    {
        var clock = new AdjustableTimeProvider(new DateTimeOffset(2026, 9, 22, 9, 0, 0, TimeSpan.Zero));
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>
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
            string legacyLeaseId;
            using (var acquire = CreateLeaseRequest(blobLeaseUri, "2009-09-19", "acquire"))
            using (var response = await transport.SendAsync(acquire))
            {
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                legacyLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
                Assert.Null(GetResponseHeaderOrDefault(response, "ETag"));
                Assert.Null(GetResponseHeaderOrDefault(response, "Last-Modified"));
            }

            using (var duration = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "acquire", duration: 30))
            using (var response = await transport.SendAsync(duration))
            {
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("UnsupportedHeader", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var change = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "change", legacyLeaseId))
            using (var response = await transport.SendAsync(change))
            {
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var release = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "release", legacyLeaseId))
            using (var response = await transport.SendAsync(release))
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            using (var renew = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "renew", legacyLeaseId))
            using (var response = await transport.SendAsync(renew))
            {
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal(legacyLeaseId, response.Headers.GetValues("x-ms-lease-id").Single());
            }
            using (var release = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "release", legacyLeaseId))
            using (var response = await transport.SendAsync(release))
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            using (var breakLease = CreateLeaseRequest(blobLeaseUri, "2011-08-18", "break"))
            using (var response = await transport.SendAsync(breakLease))
            {
                Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
                Assert.Equal("0", response.Headers.GetValues("x-ms-lease-time").Single());
            }

            using (var missingDuration = CreateLeaseRequest(blobLeaseUri, "2012-02-12", "acquire"))
            using (var response = await transport.SendAsync(missingDuration))
            {
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
            }

            string modernLeaseId;
            using (var acquire = CreateLeaseRequest(blobLeaseUri, "2012-02-12", "acquire", duration: 15))
            using (var response = await transport.SendAsync(acquire))
            {
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                modernLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
                Assert.Null(GetResponseHeaderOrDefault(response, "ETag"));
                Assert.Null(GetResponseHeaderOrDefault(response, "Last-Modified"));
            }
            using (var release = CreateLeaseRequest(blobLeaseUri, "2012-02-12", "release", modernLeaseId))
            using (var response = await transport.SendAsync(release))
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            using (var acquire = CreateLeaseRequest(blobLeaseUri, "2013-08-15", "acquire", duration: 15))
            using (var response = await transport.SendAsync(acquire))
            {
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                modernLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
                Assert.Equal(initialBlob.ETag.ToString(), GetResponseHeader(response, "ETag"));
                Assert.Equal(
                    initialBlob.LastModified.ToString("R", CultureInfo.InvariantCulture),
                    GetResponseHeader(response, "Last-Modified"));
            }
            using (var release = CreateLeaseRequest(blobLeaseUri, "2013-08-15", "release", modernLeaseId))
            using (var response = await transport.SendAsync(release))
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var afterBlobLeases = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(initialBlob.ETag, afterBlobLeases.ETag);
            Assert.Equal(initialBlob.LastModified, afterBlobLeases.LastModified);

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
            using (var unavailable = CreateLeaseRequest(containerLeaseUri, "2011-08-18", "acquire", duration: 15))
            using (var response = await transport.SendAsync(unavailable))
            {
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
            }

            var beforeLegacyContainerLease = (await container.GetPropertiesAsync()).Value;
            clock.Advance(TimeSpan.FromMinutes(1));
            string containerLeaseId;
            using (var acquire = CreateLeaseRequest(containerLeaseUri, "2012-02-12", "acquire", duration: 15))
            using (var response = await transport.SendAsync(acquire))
            {
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                containerLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
                Assert.Null(GetResponseHeaderOrDefault(response, "ETag"));
                Assert.Null(GetResponseHeaderOrDefault(response, "Last-Modified"));
            }
            var afterLegacyContainerLease = (await container.GetPropertiesAsync()).Value;
            Assert.NotEqual(beforeLegacyContainerLease.ETag, afterLegacyContainerLease.ETag);
            Assert.NotEqual(beforeLegacyContainerLease.LastModified, afterLegacyContainerLease.LastModified);
            using (var release = CreateLeaseRequest(containerLeaseUri, "2012-02-12", "release", containerLeaseId))
            using (var response = await transport.SendAsync(release))
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var beforeModernContainerLease = (await container.GetPropertiesAsync()).Value;
            clock.Advance(TimeSpan.FromMinutes(1));
            using (var acquire = CreateLeaseRequest(containerLeaseUri, "2013-08-15", "acquire", duration: 15))
            using (var response = await transport.SendAsync(acquire))
            {
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                containerLeaseId = response.Headers.GetValues("x-ms-lease-id").Single();
                Assert.Equal(beforeModernContainerLease.ETag.ToString(), GetResponseHeader(response, "ETag"));
                Assert.Equal(
                    beforeModernContainerLease.LastModified.ToString("R", CultureInfo.InvariantCulture),
                    GetResponseHeader(response, "Last-Modified"));
            }
            var afterModernContainerLease = (await container.GetPropertiesAsync()).Value;
            Assert.Equal(beforeModernContainerLease.ETag, afterModernContainerLease.ETag);
            Assert.Equal(beforeModernContainerLease.LastModified, afterModernContainerLease.LastModified);
            using (var release = CreateLeaseRequest(containerLeaseUri, "2013-08-15", "release", containerLeaseId))
            using (var response = await transport.SendAsync(release))
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var aclUri = AppendQuery(
                container.Uri,
                "restype=container&comp=acl");
            using (var acl = new HttpRequestMessage(HttpMethod.Put, aclUri)
            {
                Content = new ByteArrayContent([])
            })
            {
                acl.Headers.TryAddWithoutValidation("x-ms-version", "2008-10-27");
                acl.Headers.TryAddWithoutValidation("x-ms-blob-public-access", "blob");
                AddSharedKeyLiteAuthorization(acl);
                using var response = await transport.SendAsync(acl);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
                Assert.False(response.Headers.Contains("x-ms-version"));
            }
        }
        finally
        {
            await application.DisposeAsync();
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
    public async Task DirectoryServiceAndUserDelegationSasAreBoundToAnHnsPrefix()
    {
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
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
            new Uri(
                $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{name}" +
                $"?{serviceSas}"));

        var directoryProperties = await GetServiceSasBlob("signed/directory").GetPropertiesAsync();
        Assert.Equal(
            "directory",
            directoryProperties.GetRawResponse().Headers.TryGetValue("x-ms-resource-type", out var resourceType)
                ? resourceType
                : null);
        Assert.Equal(
            "child",
            (await GetServiceSasBlob("signed/directory/child.txt").DownloadContentAsync()).Value.Content.ToString());
        Assert.Equal(
            "leaf",
            (await GetServiceSasBlob("signed/directory/nested/leaf.txt").DownloadContentAsync()).Value.Content.ToString());
        var deniedSibling = await Assert.ThrowsAsync<RequestFailedException>(() =>
            GetServiceSasBlob("signed/sibling.txt").DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedSibling.Status);
        var deniedParent = await Assert.ThrowsAsync<RequestFailedException>(() =>
            GetServiceSasBlob("signed").GetPropertiesAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedParent.Status);

        var token = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            SavaWebApplicationFactory.DelegatorObjectId,
            SavaWebApplicationFactory.TenantId);
        var delegator = CreateBearerClient(application, token);
        var key = await delegator.GetUserDelegationKeyAsync(
            new BlobGetUserDelegationKeyOptions(expiresOn) { StartsOn = startsOn });
        var delegatedBuilder = new BlobSasBuilder
        {
            BlobContainerName = container.Name,
            BlobName = "signed/directory",
            Resource = "d",
            IsDirectory = true,
            StartsOn = startsOn,
            ExpiresOn = expiresOn,
            Protocol = SasProtocol.HttpsAndHttp,
            PreauthorizedAgentObjectId = Guid.NewGuid().ToString(),
            CorrelationId = Guid.NewGuid().ToString()
        };
        delegatedBuilder.SetPermissions(BlobContainerSasPermissions.Read);
        var delegatedSas = delegatedBuilder.ToSasQueryParameters(
            key.Value,
            SavaWebApplicationFactory.AccountName);

        BlobClient GetDelegatedBlob(string name) => CreateBlobClient(
            application,
            new Uri(
                $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{name}" +
                $"?{delegatedSas}"));

        Assert.Equal(
            "child",
            (await GetDelegatedBlob("signed/directory/child.txt").DownloadContentAsync()).Value.Content.ToString());
        var deniedDelegatedSibling = await Assert.ThrowsAsync<RequestFailedException>(() =>
            GetDelegatedBlob("signed/sibling.txt").DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedDelegatedSibling.Status);

        var flatOwner = CreateClient(factory);
        var flatContainer = flatOwner.GetBlobContainerClient($"flat-directory-sas-{Guid.NewGuid():N}");
        await flatContainer.CreateAsync();
        await flatContainer.GetBlobClient("signed/directory").UploadAsync(BinaryData.FromString("flat"));
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
            factory,
            new Uri(
                $"http://{SavaWebApplicationFactory.AccountName}.localhost/{flatContainer.Name}/signed/directory" +
                $"?{flatSas}"));
        var deniedFlatDirectory = await Assert.ThrowsAsync<RequestFailedException>(() =>
            flatDirectory.DownloadContentAsync());
        Assert.Equal(StatusCodes.Status403Forbidden, deniedFlatDirectory.Status);
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
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{signedBlobName}" +
            $"?{sas}");
        var signedBlob = CreateBlobClient(factory, signedBlobUri);
        await signedBlob.UploadAsync(BinaryData.FromString("signed encryption scope"));
        Assert.Equal(signedScope, (await container.GetBlobClient(signedBlobName).GetPropertiesAsync()).Value.EncryptionScope);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        using (var wrongScope = new HttpRequestMessage(HttpMethod.Put, signedBlobUri)
        {
            Content = new StringContent("wrong scope")
        })
        {
            wrongScope.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            wrongScope.Headers.TryAddWithoutValidation("x-ms-encryption-scope", "wrong-scope");
            using var response = await transport.SendAsync(wrongScope);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
        }

        const string legacyVersion = "2019-12-12";
        const string legacyBlobName = "legacy-scope.txt";
        const string permissions = "cw";
        const string protocol = "https";
        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-1)
            .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var expiresOn = DateTimeOffset.UtcNow.AddMinutes(10)
            .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var canonicalResource =
            $"/blob/{SavaWebApplicationFactory.AccountName}/{container.Name}/{legacyBlobName}";
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
        var legacyBlobUri = new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{legacyBlobName}" +
            $"?sp={permissions}" +
            $"&st={Uri.EscapeDataString(startsOn)}" +
            $"&se={Uri.EscapeDataString(expiresOn)}" +
            $"&spr={protocol}" +
            $"&sv={legacyVersion}" +
            "&sr=b" +
            $"&sig={Uri.EscapeDataString(signature)}");

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

        Uri CreateSasUri(string? version, string? contentType = null, DateTimeOffset? expiry = null)
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

            using var hmac = new HMACSHA256(
                Convert.FromBase64String(SavaWebApplicationFactory.AccountKey));
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
            Assert.Equal(
                "AuthenticationFailed",
                unsignedOverride.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var unsignedProtocol = await transport.GetAsync(
                   AppendQuery(CreateSasUri("2013-08-15"), "spr=https")))
        {
            Assert.Equal(HttpStatusCode.Forbidden, unsignedProtocol.StatusCode);
            Assert.Equal(
                "AuthenticationFailed",
                unsignedProtocol.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var excessiveLegacyLifetime = await transport.GetAsync(
                   CreateSasUri(version: null, expiry: DateTimeOffset.UtcNow.AddHours(2))))
        {
            Assert.Equal(HttpStatusCode.Forbidden, excessiveLegacyLifetime.StatusCode);
            Assert.Equal(
                "AuthenticationFailed",
                excessiveLegacyLifetime.Headers.GetValues("x-ms-error-code").Single());
        }
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
        var signedStart = startsAt.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var signedExpiry = expiresAt.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var keyStart = key.SignedStartsOn.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var keyExpiry = key.SignedExpiresOn.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        const string signedVersion = "2026-04-06";
        const string signedHeaders = "x-dynamic-first,x-dynamic-second";
        const string signedQuery = "operation,day%2Cid";
        const string canonicalizedHeaders =
            "x-dynamic-first:123,789\nx-dynamic-second:456\n";
        const string canonicalizedQuery = "\noperation=update\nday,id=mon123";
        var canonicalResource =
            $"/blob/{SavaWebApplicationFactory.AccountName}/{container.Name}/{blob.Name}";
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
        var signature = Convert.ToBase64String(
            hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
        var query =
            $"sp=r&st={Uri.EscapeDataString(signedStart)}&se={Uri.EscapeDataString(signedExpiry)}" +
            $"&skoid={key.SignedObjectId}&sktid={key.SignedTenantId}" +
            $"&skt={Uri.EscapeDataString(keyStart)}&ske={Uri.EscapeDataString(keyExpiry)}" +
            $"&sks={key.SignedService}&skv={key.SignedVersion}" +
            "&spr=https%2Chttp" +
            $"&sv={signedVersion}&sr=b&srh={signedHeaders}&srq={signedQuery}" +
            "&operation=update&day%2Cid=mon123" +
            $"&sig={Uri.EscapeDataString(signature)}";
        var uri = new Uri(
            $"http://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}?{query}");
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
        var signedStart = startsAt.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var signedExpiry = expiresAt.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var keyStart = key.SignedStartsOn.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var keyExpiry = key.SignedExpiresOn.UtcDateTime.ToString(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            CultureInfo.InvariantCulture);
        var delegatedUserObjectId = Guid.NewGuid().ToString();
        const string signedVersion = "2025-07-05";
        var canonicalResource =
            $"/blob/{SavaWebApplicationFactory.AccountName}/{container.Name}/{blob.Name}";
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
        var signature = Convert.ToBase64String(
            hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
        var query =
            $"sp=r&st={Uri.EscapeDataString(signedStart)}&se={Uri.EscapeDataString(signedExpiry)}" +
            $"&skoid={key.SignedObjectId}&sktid={key.SignedTenantId}" +
            $"&skt={Uri.EscapeDataString(keyStart)}&ske={Uri.EscapeDataString(keyExpiry)}" +
            $"&sks={key.SignedService}&skv={key.SignedVersion}" +
            $"&sduoid={delegatedUserObjectId}&spr=https%2Chttp" +
            $"&sv={signedVersion}&sr=b&sig={Uri.EscapeDataString(signature)}";
        var sasUri = new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}?{query}");
        var directUri = new Uri(
            $"https://{SavaWebApplicationFactory.AccountName}.localhost/{container.Name}/{blob.Name}");
        using var transport = new HttpClient(factory.Server.CreateHandler());

        static HttpRequestMessage CreateRequest(Uri target, string token)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, target);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Headers.TryAddWithoutValidation("x-ms-version", signedVersion);
            return request;
        }

        var targetToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            delegatedUserObjectId,
            SavaWebApplicationFactory.TenantId);
        using (var directRequest = CreateRequest(directUri, targetToken))
        using (var response = await transport.SendAsync(directRequest))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var validRequest = CreateRequest(sasUri, targetToken))
        using (var response = await transport.SendAsync(validRequest))
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("user-bound payload", await response.Content.ReadAsStringAsync());
        }

        var wrongObjectToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            Guid.NewGuid().ToString(),
            SavaWebApplicationFactory.TenantId);
        using (var wrongObjectRequest = CreateRequest(sasUri, wrongObjectToken))
        using (var response = await transport.SendAsync(wrongObjectRequest))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }

        var wrongTenantToken = CreateJwt(
            SavaWebApplicationFactory.AccountKey,
            delegatedUserObjectId,
            Guid.NewGuid().ToString());
        using (var wrongTenantRequest = CreateRequest(sasUri, wrongTenantToken))
        using (var response = await transport.SendAsync(wrongTenantRequest))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var response = await transport.GetAsync(sasUri))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }
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

        Uri CreateServiceSasContainerUri(string name)
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

        Uri CreateAccountSasContainerUri(
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

        async Task AssertRejectedServiceSasAsync(HttpRequestMessage request)
        {
            using (request)
            {
                if (!request.Headers.Contains("x-ms-version"))
                    request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
                Assert.Equal(
                    "AuthorizationFailure",
                    response.Headers.GetValues("x-ms-error-code").Single());
            }
        }

        var serviceSasUri = container.GenerateSasUri(
            BlobContainerSasPermissions.All,
            DateTimeOffset.UtcNow.AddMinutes(10));
        await AssertRejectedServiceSasAsync(new HttpRequestMessage(
            HttpMethod.Head,
            AppendQuery(serviceSasUri, "restype=container")));
        await AssertRejectedServiceSasAsync(new HttpRequestMessage(
            HttpMethod.Get,
            AppendQuery(serviceSasUri, "restype=container&comp=metadata")));
        await AssertRejectedServiceSasAsync(new HttpRequestMessage(
            HttpMethod.Put,
            AppendQuery(serviceSasUri, "restype=container&comp=metadata"))
        {
            Content = new ByteArrayContent([])
        });
        await AssertRejectedServiceSasAsync(CreateLeaseRequest(
            AppendQuery(serviceSasUri, "restype=container&comp=lease"),
            "2023-11-03",
            "acquire",
            duration: 15));
        await AssertRejectedServiceSasAsync(new HttpRequestMessage(
            HttpMethod.Delete,
            AppendQuery(serviceSasUri, "restype=container")));

        var deniedTargetName = $"service-sas-create-{Guid.NewGuid():N}";
        var deniedTargetUri = CreateServiceSasContainerUri(deniedTargetName);
        await AssertRejectedServiceSasAsync(new HttpRequestMessage(HttpMethod.Put, deniedTargetUri)
        {
            Content = new ByteArrayContent([])
        });
        using (var restore = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(deniedTargetUri, "comp=undelete"))
        {
            Content = new ByteArrayContent([])
        })
        {
            restore.Headers.TryAddWithoutValidation("x-ms-deleted-container-name", "deleted-source");
            restore.Headers.TryAddWithoutValidation("x-ms-deleted-container-version", "01D8B4E2CFD1A4B00000000000000000");
            await AssertRejectedServiceSasAsync(restore);
        }

        using (var list = new HttpRequestMessage(
                   HttpMethod.Get,
                   AppendQuery(serviceSasUri, "restype=container&comp=list")))
        {
            list.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(list);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Contains("<Name>listed.txt</Name>", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        }

        var accountCreatedName = $"account-sas-create-{Guid.NewGuid():N}";
        var accountCreateUri = CreateAccountSasContainerUri(
            accountCreatedName,
            AccountSasPermissions.Create,
            AccountSasServices.Blobs,
            AccountSasResourceTypes.Container);
        using (var create = new HttpRequestMessage(HttpMethod.Put, accountCreateUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            create.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(create);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }

        var accountReadUri = CreateAccountSasContainerUri(
            container.Name,
            AccountSasPermissions.Read,
            AccountSasServices.Blobs,
            AccountSasResourceTypes.Container);
        using (var properties = new HttpRequestMessage(HttpMethod.Head, accountReadUri))
        {
            properties.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(properties);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        var accountMetadataUri = CreateAccountSasContainerUri(
            container.Name,
            AccountSasPermissions.Write,
            AccountSasServices.Blobs,
            AccountSasResourceTypes.Container,
            "metadata");
        using (var setMetadata = new HttpRequestMessage(HttpMethod.Put, accountMetadataUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            setMetadata.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            setMetadata.Headers.TryAddWithoutValidation("x-ms-meta-authorized", "account-sas");
            using var response = await transport.SendAsync(setMetadata);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        Assert.Equal("account-sas", (await container.GetPropertiesAsync()).Value.Metadata["authorized"]);

        var wrongResourceUri = CreateAccountSasContainerUri(
            $"wrong-resource-{Guid.NewGuid():N}",
            AccountSasPermissions.Create,
            AccountSasServices.Blobs,
            AccountSasResourceTypes.Service);
        using (var wrongResource = new HttpRequestMessage(HttpMethod.Put, wrongResourceUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            wrongResource.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(wrongResource);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal(
                "AuthorizationResourceTypeMismatch",
                response.Headers.GetValues("x-ms-error-code").Single());
        }

        var wrongServiceUri = CreateAccountSasContainerUri(
            $"wrong-service-{Guid.NewGuid():N}",
            AccountSasPermissions.Create,
            AccountSasServices.Queues,
            AccountSasResourceTypes.Container);
        using (var wrongService = new HttpRequestMessage(HttpMethod.Put, wrongServiceUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            wrongService.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(wrongService);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal(
                "AuthorizationServiceMismatch",
                response.Headers.GetValues("x-ms-error-code").Single());
        }

        var leaseClient = container.GetBlobLeaseClient();
        await leaseClient.AcquireAsync(TimeSpan.FromSeconds(15));
        var deleteLeaseUri = CreateAccountSasContainerUri(
            container.Name,
            AccountSasPermissions.Delete,
            AccountSasServices.Blobs,
            AccountSasResourceTypes.Container,
            "lease");
        using (var deniedAcquire = CreateLeaseRequest(
                   deleteLeaseUri,
                   "2017-07-29",
                   "acquire",
                   duration: 15))
        using (var response = await transport.SendAsync(deniedAcquire))
        {
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal(
                "AuthorizationPermissionMismatch",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var breakLease = CreateLeaseRequest(deleteLeaseUri, "2017-07-29", "break"))
        using (var response = await transport.SendAsync(breakLease))
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
        await container.SetAccessPolicyAsync(
            PublicAccessType.None,
            [new BlobSignedIdentifier
            {
                Id = "read-policy",
                AccessPolicy = new BlobAccessPolicy
                {
                    StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
                    ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(10),
                    Permissions = "r"
                }
            }]);

        var builder = new BlobSasBuilder
        {
            BlobContainerName = containerName,
            BlobName = blobName,
            Resource = "b",
            Identifier = "read-policy",
            Protocol = SasProtocol.HttpsAndHttp
        };
        var credential = new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey);
        var sas = builder.ToSasQueryParameters(credential);
        var policyBlob = CreateBlobClient(factory, new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{sas}"));
        Assert.Equal("policy payload", (await policyBlob.DownloadContentAsync()).Value.Content.ToString());

        await container.SetAccessPolicyAsync(PublicAccessType.None, []);
        var revoked = await Assert.ThrowsAsync<RequestFailedException>(() => policyBlob.DownloadContentAsync());
        Assert.Equal(403, revoked.Status);

        var aclUri = AppendQuery(
            container.GenerateSasUri(
                BlobContainerSasPermissions.All,
                DateTimeOffset.UtcNow.AddMinutes(5)),
            "restype=container&comp=acl");
        using var transport = new HttpClient(factory.Server.CreateHandler());
        using (var getAcl = new HttpRequestMessage(HttpMethod.Get, aclUri))
        {
            getAcl.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(getAcl);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            Assert.Equal("AuthorizationFailure", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var setAcl = new HttpRequestMessage(HttpMethod.Put, aclUri)
        {
            Content = new ByteArrayContent([])
        })
        {
            setAcl.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            using var response = await transport.SendAsync(setAcl);
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
        Assert.Contains("readable.txt", names);
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
            var sas = builder.ToSasQueryParameters(key.Value, SavaWebApplicationFactory.AccountName);
            var delegatedBlob = CreateBlobClient(
                factory,
                new Uri($"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}?{sas}"));

            Assert.Equal("delegated payload", (await delegatedBlob.DownloadContentAsync()).Value.Content.ToString());
            var deniedDelete = await Assert.ThrowsAsync<RequestFailedException>(() => delegatedBlob.DeleteAsync());
            Assert.Equal(403, deniedDelete.Status);

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
            var versionSas = versionBuilder.ToSasQueryParameters(
                key.Value,
                SavaWebApplicationFactory.AccountName);
            var delegatedVersion = CreateBlobClient(
                factory,
                new Uri(
                    $"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                    $"?versionid={Uri.EscapeDataString(versionId!)}&{versionSas}"));
            Assert.Equal(
                "delegated payload",
                (await delegatedVersion.DownloadContentAsync()).Value.Content.ToString());

            var versionTokenOnBaseBlob = CreateBlobClient(
                factory,
                new Uri(
                    $"https://{SavaWebApplicationFactory.AccountName}.localhost/{containerName}/{blobName}" +
                    $"?{versionSas}"));
            var wrongResource = await Assert.ThrowsAsync<RequestFailedException>(() =>
                versionTokenOnBaseBlob.DownloadContentAsync());
            Assert.Equal(403, wrongResource.Status);
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

        async Task<HttpResponseMessage> SendAsync(Uri uri, string version, string body)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, uri)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/xml")
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            return await transport.SendAsync(request);
        }

        using (var oldVersion = await SendAsync(httpsUri, "2018-03-28", baseBody))
        {
            Assert.Equal(HttpStatusCode.Conflict, oldVersion.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldVersion.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var insecure = await SendAsync(httpUri, "2025-07-05", baseBody))
        {
            Assert.Equal(HttpStatusCode.BadRequest, insecure.StatusCode);
            Assert.Equal("InvalidRequest", insecure.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var prematureDelegation = await SendAsync(httpsUri, "2023-11-03", delegatedBody))
        {
            Assert.Equal(HttpStatusCode.Conflict, prematureDelegation.StatusCode);
            Assert.Equal("FeatureVersionMismatch", prematureDelegation.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var unknownElement = await SendAsync(
                   httpsUri,
                   "2025-07-05",
                   $"<KeyInfo><Start>{startsAt}</Start><Expiry>{expiresAt}</Expiry><Unknown /></KeyInfo>"))
        {
            Assert.Equal(HttpStatusCode.BadRequest, unknownElement.StatusCode);
            Assert.Equal("InvalidXmlDocument", unknownElement.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var duplicateElement = await SendAsync(
                   httpsUri,
                   "2025-07-05",
                   $"<KeyInfo><Start>{startsAt}</Start><Start>{startsAt}</Start><Expiry>{expiresAt}</Expiry></KeyInfo>"))
        {
            Assert.Equal(HttpStatusCode.BadRequest, duplicateElement.StatusCode);
            Assert.Equal("InvalidXmlDocument", duplicateElement.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var accepted = await SendAsync(httpsUri, "2025-07-05", delegatedBody))
        {
            Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
            var document = System.Xml.Linq.XDocument.Parse(await accepted.Content.ReadAsStringAsync());
            Assert.Equal(
                delegatedTenant,
                Assert.Single(document.Root!.Elements("SignedDelegatedUserTid")).Value);
        }
    }

    [Fact]
    public async Task UrlTransfersSupportBlockAppendPageAndWholeBlobOperations()
    {
        var sourceBytes = Enumerable.Range(0, 16 * 1024).Select(index => (byte)(index % 251)).ToArray();
        await using var source = await LoopbackSource.StartAsync(sourceBytes);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"url-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var whole = container.GetBlockBlobClient("whole.bin");
        var wholeUpload = await whole.SyncUploadFromUriAsync(
            source.Uri,
            new BlobSyncUploadFromUriOptions
            {
                Metadata = new Dictionary<string, string> { ["marker"] = "must-not-leak" }
            });
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
            Convert.ToBase64String(MD5.HashData(sourceBytes)),
            ResponseHeader(wholeUpload.GetRawResponse(), "Content-MD5"));
        Assert.Equal(
            StorageCrc64Base64(sourceBytes),
            ResponseHeader(wholeUpload.GetRawResponse(), "x-ms-content-crc64"));
        Assert.Equal(sourceBytes, (await whole.DownloadContentAsync()).Value.Content.ToArray());

        const int blockOffset = 900;
        const int blockLength = 2_300;
        var block = container.GetBlockBlobClient("block.bin");
        var blockId = Convert.ToBase64String("url-block-1"u8);
        var blockSlice = sourceBytes.AsSpan(blockOffset, blockLength).ToArray();
        var stagedFromUri = await block.StageBlockFromUriAsync(source.Uri, blockId, new StageBlockFromUriOptions
        {
            SourceRange = new HttpRange(blockOffset, blockLength),
            SourceContentHash = MD5.HashData(blockSlice)
        });
        Assert.Equal(
            Convert.ToBase64String(MD5.HashData(blockSlice)),
            ResponseHeader(stagedFromUri.GetRawResponse(), "Content-MD5"));
        Assert.Equal(
            StorageCrc64Base64(blockSlice),
            ResponseHeader(stagedFromUri.GetRawResponse(), "x-ms-content-crc64"));
        await block.CommitBlockListAsync([blockId]);
        Assert.Equal(blockSlice, (await block.DownloadContentAsync()).Value.Content.ToArray());

        const int appendOffset = 4_000;
        const int appendLength = 1_500;
        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync();
        var appendSlice = sourceBytes.AsSpan(appendOffset, appendLength).ToArray();
        var appendedFromUri = await append.AppendBlockFromUriAsync(source.Uri, new AppendBlobAppendBlockFromUriOptions
        {
            SourceRange = new HttpRange(appendOffset, appendLength),
            SourceContentHash = MD5.HashData(appendSlice)
        });
        Assert.Equal(
            Convert.ToBase64String(MD5.HashData(appendSlice)),
            ResponseHeader(appendedFromUri.GetRawResponse(), "Content-MD5"));
        Assert.Equal(
            StorageCrc64Base64(appendSlice),
            ResponseHeader(appendedFromUri.GetRawResponse(), "x-ms-content-crc64"));
        Assert.Equal(appendSlice, (await append.DownloadContentAsync()).Value.Content.ToArray());

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(1024);
        var pageSlice = sourceBytes.AsSpan(0, 512).ToArray();
        var pagesFromUri = await page.UploadPagesFromUriAsync(
            source.Uri,
            new HttpRange(0, 512),
            new HttpRange(512, 512),
            new PageBlobUploadPagesFromUriOptions { SourceContentHash = MD5.HashData(pageSlice) });
        Assert.Equal(
            Convert.ToBase64String(MD5.HashData(pageSlice)),
            ResponseHeader(pagesFromUri.GetRawResponse(), "Content-MD5"));
        Assert.Equal(
            StorageCrc64Base64(pageSlice),
            ResponseHeader(pagesFromUri.GetRawResponse(), "x-ms-content-crc64"));
        var expectedPage = new byte[1024];
        pageSlice.CopyTo(expectedPage, 512);
        Assert.Equal(expectedPage, (await page.DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task FileRequestIntentIsValidatedAndForwardedForEverySupportedUrlOperation()
    {
        var sourceBytes = Enumerable.Range(0, 512).Select(index => (byte)(index % 251)).ToArray();
        var source = new FileIntentSourceHandler(sourceBytes);
        var application = new SavaWebApplicationFactory(() => source);
        try
        {
            await application.InitializeAsync();
            var service = CreateClient(application);
            var container = service.GetBlobContainerClient($"file-intent-{Guid.NewGuid():N}");
            await container.CreateAsync();
            using var transport = new HttpClient(application.Server.CreateHandler());
            const string sourceUrl = "https://source.file.core.windows.net/share/source.bin";

            HttpRequestMessage CreateRequest(
                Uri destination,
                string? intent = "backup",
                string version = "2025-07-05",
                string sourceValue = sourceUrl,
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

            var whole = container.GetBlockBlobClient("whole.bin");
            using (var request = CreateRequest(whole.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            var block = container.GetBlockBlobClient("block.bin");
            var blockId = Convert.ToBase64String("file-intent-block"u8);
            var blockUri = AppendQuery(
                block.GenerateSasUri(
                    BlobSasPermissions.Create | BlobSasPermissions.Write,
                    DateTimeOffset.UtcNow.AddMinutes(5)),
                $"comp=block&blockid={Uri.EscapeDataString(blockId)}");
            using (var request = CreateRequest(blockUri))
            {
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            await block.CommitBlockListAsync([blockId]);

            var append = container.GetAppendBlobClient("append.bin");
            await append.CreateAsync();
            var appendUri = AppendQuery(
                append.GenerateSasUri(
                    BlobSasPermissions.Add | BlobSasPermissions.Write,
                    DateTimeOffset.UtcNow.AddMinutes(5)),
                "comp=appendblock");
            using (var request = CreateRequest(appendUri))
            {
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            var page = container.GetPageBlobClient("page.bin");
            await page.CreateAsync(512);
            var pageUri = AppendQuery(
                page.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
                "comp=page");
            using (var request = CreateRequest(pageUri))
            {
                request.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
                request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
                request.Headers.TryAddWithoutValidation("x-ms-source-range", "bytes=0-511");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            var copied = container.GetBlockBlobClient("copied.bin");
            using (var request = CreateRequest(copied.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            }

            Assert.Equal(5, source.RequestCount);
            foreach (var blob in new BlobBaseClient[] { whole, block, append, page, copied })
                Assert.Equal(sourceBytes, (await blob.DownloadContentAsync()).Value.Content.ToArray());

            var rejected = container.GetBlockBlobClient("rejected.bin");
            var rejectedUri = rejected.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5));
            using (var request = CreateRequest(rejectedUri, intent: null))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("MissingRequiredHeader", response.Headers.GetValues("x-ms-error-code").Single());
                Assert.Contains(
                    "<HeaderName>x-ms-file-request-intent</HeaderName>",
                    await response.Content.ReadAsStringAsync(),
                    StringComparison.Ordinal);
            }
            using (var request = CreateRequest(rejectedUri, intent: "restore"))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }
            using (var request = CreateRequest(rejectedUri, version: "2025-01-05"))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
            }
            Assert.Equal(5, source.RequestCount);
            Assert.False((await rejected.ExistsAsync()).Value);

            var asynchronous = container.GetBlockBlobClient("asynchronous.bin");
            using (var request = CreateRequest(asynchronous.GenerateSasUri(
                       BlobSasPermissions.Create | BlobSasPermissions.Write,
                       DateTimeOffset.UtcNow.AddMinutes(5))))
            {
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("UnsupportedHeader", response.Headers.GetValues("x-ms-error-code").Single());
            }

            var anonymousFile = container.GetBlockBlobClient("anonymous-file.bin");
            using (var request = CreateRequest(
                       anonymousFile.GenerateSasUri(
                           BlobSasPermissions.Create | BlobSasPermissions.Write,
                           DateTimeOffset.UtcNow.AddMinutes(5)),
                       intent: null,
                       sourceValue: "https://source.file.core.windows.net/share/anonymous.bin",
                       includeSourceAuthorization: false))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }

            var bearerBlob = container.GetBlockBlobClient("bearer-blob.bin");
            using (var request = CreateRequest(
                       bearerBlob.GenerateSasUri(
                           BlobSasPermissions.Create | BlobSasPermissions.Write,
                           DateTimeOffset.UtcNow.AddMinutes(5)),
                       intent: null,
                       sourceValue: "https://source.blob.core.windows.net/container/source.bin"))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            Assert.Equal(7, source.RequestCount);
            Assert.False((await asynchronous.ExistsAsync()).Value);
            Assert.Equal(sourceBytes, (await anonymousFile.DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(sourceBytes, (await bearerBlob.DownloadContentAsync()).Value.Content.ToArray());
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task UrlSourceConditionsAndFailuresKeepAzureErrorSemantics()
    {
        var sourceBytes = Enumerable.Range(0, 2048).Select(index => (byte)(index % 241)).ToArray();
        await using var source = await LoopbackSource.StartAsync(sourceBytes);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"url-errors-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var missingEtag = new ETag("\"not-the-source-etag\"");

        static async Task AssertSourceConditionAsync(Func<Task> operation)
        {
            var failure = await Assert.ThrowsAsync<RequestFailedException>(operation);
            Assert.Equal(StatusCodes.Status412PreconditionFailed, failure.Status);
            Assert.Equal("SourceConditionNotMet", failure.ErrorCode);
        }

        var whole = container.GetBlockBlobClient("whole.bin");
        await AssertSourceConditionAsync(() => whole.SyncUploadFromUriAsync(
            source.Uri,
            new BlobSyncUploadFromUriOptions
            {
                SourceConditions = new BlobRequestConditions { IfMatch = missingEtag }
            }));
        Assert.False((await whole.ExistsAsync()).Value);

        var block = container.GetBlockBlobClient("block.bin");
        await AssertSourceConditionAsync(() => block.StageBlockFromUriAsync(
            source.Uri,
            Convert.ToBase64String("conditioned-block"u8),
            new StageBlockFromUriOptions
            {
                SourceConditions = new RequestConditions { IfMatch = missingEtag }
            }));
        Assert.False((await block.ExistsAsync()).Value);

        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync();
        await AssertSourceConditionAsync(() => append.AppendBlockFromUriAsync(
            source.Uri,
            new AppendBlobAppendBlockFromUriOptions
            {
                SourceConditions = new AppendBlobRequestConditions { IfMatch = missingEtag }
            }));
        Assert.Equal(0, (await append.GetPropertiesAsync()).Value.ContentLength);

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(512);
        await AssertSourceConditionAsync(() => page.UploadPagesFromUriAsync(
            source.Uri,
            new HttpRange(0, 512),
            new HttpRange(0, 512),
            new PageBlobUploadPagesFromUriOptions
            {
                SourceConditions = new PageBlobRequestConditions { IfMatch = missingEtag }
            }));
        Assert.Equal(new byte[512], (await page.DownloadContentAsync()).Value.Content.ToArray());

        var copied = container.GetBlobClient("copied.bin");
        await AssertSourceConditionAsync(() => copied.StartCopyFromUriAsync(
            source.Uri,
            new BlobCopyFromUriOptions
            {
                SourceConditions = new BlobRequestConditions { IfMatch = missingEtag }
            }));
        Assert.False((await copied.ExistsAsync()).Value);

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
        request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.MissingUri.ToString());
        request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
        using var response = await transport.SendAsync(request);
        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Equal("CannotVerifyCopySource", response.Headers.GetValues("x-ms-error-code").Single());
        Assert.Equal("404", response.Headers.GetValues("x-ms-copy-source-status-code").Single());
        Assert.Equal("BlobNotFound", response.Headers.GetValues("x-ms-copy-source-error-code").Single());
        var error = await response.Content.ReadAsStringAsync();
        Assert.Contains("<CopySourceStatusCode>404</CopySourceStatusCode>", error, StringComparison.Ordinal);
        Assert.Contains("<CopySourceErrorCode>BlobNotFound</CopySourceErrorCode>", error, StringComparison.Ordinal);
        Assert.Contains("<CopySourceErrorMessage>The specified blob does not exist.</CopySourceErrorMessage>", error, StringComparison.Ordinal);
        Assert.False((await missing.ExistsAsync()).Value);
    }

    [Fact]
    public async Task SourceCustomerKeysAreValidatedAndForwardedForEveryUrlWriteOperation()
    {
        var sourceBytes = Enumerable.Range(0, 2048).Select(index => (byte)(index % 239)).ToArray();
        var sourceKey = RandomNumberGenerator.GetBytes(32);
        var sourceHash = SHA256.HashData(sourceKey);
        var source = new EncryptedSourceHandler(sourceBytes, sourceKey, sourceHash);
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
            var whole = container.GetBlobClient("whole.bin");
            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(whole, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                AddCustomerKeyHeaders(request, destinationKey, destinationHash);
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            var encryptedWhole = CreateEncryptedClient(
                    application,
                    new CustomerProvidedKey(destinationKey),
                    encryptionScope: null)
                .GetBlobContainerClient(containerName)
                .GetBlobClient(whole.Name);
            Assert.Equal(sourceBytes, (await encryptedWhole.DownloadContentAsync()).Value.Content.ToArray());

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
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            await block.CommitBlockListAsync([blockId]);
            Assert.Equal(
                sourceBytes[blockStart..(blockEnd + 1)],
                (await block.DownloadContentAsync()).Value.Content.ToArray());

            const int appendStart = 256;
            const int appendEnd = 767;
            var append = container.GetAppendBlobClient("append.bin");
            await append.CreateAsync();
            var appendUri = AppendQuery(
                HttpsSasUri(append, BlobSasPermissions.Add | BlobSasPermissions.Write),
                "comp=appendblock");
            using (var request = CreateSourceKeyRequest(appendUri, sourceKey, sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-source-range", $"bytes={appendStart}-{appendEnd}");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            Assert.Equal(
                sourceBytes[appendStart..(appendEnd + 1)],
                (await append.DownloadContentAsync()).Value.Content.ToArray());

            var page = container.GetPageBlobClient("page.bin");
            await page.CreateAsync(1024);
            var pageUri = AppendQuery(
                HttpsSasUri(page, BlobSasPermissions.Write),
                "comp=page");
            using (var request = CreateSourceKeyRequest(pageUri, sourceKey, sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
                request.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
                request.Headers.TryAddWithoutValidation("x-ms-source-range", "bytes=512-1023");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            }
            var expectedPage = new byte[1024];
            sourceBytes.AsSpan(512, 512).CopyTo(expectedPage);
            Assert.Equal(expectedPage, (await page.DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(4, source.RequestCount);

            var invalid = container.GetBlobClient("invalid.bin");
            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       sourceHash,
                       version: "2025-11-05"))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       RandomNumberGenerator.GetBytes(32)))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       sourceHash,
                       sourceUri: "http://source.example/encrypted"))
            {
                request.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidRequest", response.Headers.GetValues("x-ms-error-code").Single());
            }

            using (var request = CreateSourceKeyRequest(
                       HttpsSasUri(invalid, BlobSasPermissions.Create | BlobSasPermissions.Write),
                       sourceKey,
                       sourceHash))
            {
                request.Headers.TryAddWithoutValidation("x-ms-requires-sync", "true");
                using var response = await transport.SendAsync(request);
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
                Assert.Equal("InvalidHeaderValue", response.Headers.GetValues("x-ms-error-code").Single());
            }
            Assert.Equal(4, source.RequestCount);
        }
        finally
        {
            await application.DisposeAsync();
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
                }));
        Assert.Equal(412, failed.Status);
        Assert.Equal("SequenceNumberConditionNotMet", failed.ErrorCode);

        var diff = (await page.GetPageRangesDiffAsync(previousSnapshot: snapshot)).Value;
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
        var firstCopy = await destination.StartCopyIncrementalAsync(source.Uri, firstSourceSnapshot);
        Assert.Equal(202, firstCopy.GetRawResponse().Status);
        await firstCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        var firstProperties = (await destination.GetPropertiesAsync()).Value;
        Assert.True(firstProperties.IsIncrementalCopy);
        Assert.Equal(CopyStatus.Success, firstProperties.CopyStatus);
        Assert.NotNull(firstProperties.DestinationSnapshot);
        Assert.Equal(chunksBeforeFirstCopy, EnumerateChunkFiles(factory.DataPath).Count());

        var baseRead = await Assert.ThrowsAsync<RequestFailedException>(() => destination.DownloadContentAsync());
        Assert.Equal(409, baseRead.Status);
        Assert.Equal("OperationNotAllowedOnIncrementalCopyBlob", baseRead.ErrorCode);
        var firstExpected = new byte[2048];
        firstPage.CopyTo(firstExpected, 0);
        secondPage.CopyTo(firstExpected, 512);
        var firstDestinationSnapshot = firstProperties.DestinationSnapshot!;
        Assert.Equal(
            firstExpected,
            (await destination.WithSnapshot(firstDestinationSnapshot).DownloadContentAsync()).Value.Content.ToArray());

        var changedPage = Enumerable.Repeat((byte)0x73, 512).ToArray();
        await source.UploadPagesAsync(new MemoryStream(changedPage), offset: 0);
        await source.ClearPagesAsync(new HttpRange(512, 512));
        var secondSourceSnapshot = (await source.CreateSnapshotAsync()).Value.Snapshot;
        var chunksBeforeSecondCopy = EnumerateChunkFiles(factory.DataPath).Count();
        var secondCopy = await destination.StartCopyIncrementalAsync(source.Uri, secondSourceSnapshot);
        await secondCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        var secondProperties = (await destination.GetPropertiesAsync()).Value;
        Assert.NotEqual(firstDestinationSnapshot, secondProperties.DestinationSnapshot);
        Assert.Equal(chunksBeforeSecondCopy, EnumerateChunkFiles(factory.DataPath).Count());

        var secondExpected = new byte[2048];
        changedPage.CopyTo(secondExpected, 0);
        Assert.Equal(
            secondExpected,
            (await destination.WithSnapshot(secondProperties.DestinationSnapshot!).DownloadContentAsync()).Value.Content.ToArray());
        Assert.Equal(
            firstExpected,
            (await destination.WithSnapshot(firstDestinationSnapshot).DownloadContentAsync()).Value.Content.ToArray());

        var earlier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.StartCopyIncrementalAsync(source.Uri, firstSourceSnapshot));
        Assert.Equal(409, earlier.Status);
        Assert.Equal("IncrementalCopyOfEarlierVersionSnapshotNotAllowed", earlier.ErrorCode);

        await source.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots);
        await source.CreateAsync(2048);
        var replacementSnapshot = (await source.CreateSnapshotAsync()).Value.Snapshot;
        var replacedSource = await Assert.ThrowsAsync<RequestFailedException>(() =>
            destination.StartCopyIncrementalAsync(source.Uri, replacementSnapshot));
        Assert.Equal(409, replacedSource.Status);
        Assert.Equal("IncrementalCopyBlobMismatch", replacedSource.ErrorCode);

        var listed = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            States = BlobStates.Snapshots,
            Prefix = destination.Name
        }))
            listed.Add(item);
        var listedBase = Assert.Single(listed, item => item.Snapshot is null);
        Assert.Equal(secondProperties.DestinationSnapshot, listedBase.Properties.DestinationSnapshot);
        Assert.Equal(2, listed.Count(item => item.Snapshot is not null));

        var listUri = new UriBuilder(container.GenerateSasUri(
            BlobContainerSasPermissions.List,
            DateTimeOffset.UtcNow.AddMinutes(5)));
        listUri.Query = $"{listUri.Query.TrimStart('?')}&restype=container&comp=list&include=snapshots&prefix={destination.Name}";
        using var httpClient = new HttpClient(factory.Server.CreateHandler());
        var listXml = await httpClient.GetStringAsync(listUri.Uri);
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
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:ImmutableStorageWithVersioningContainers:0"] = containerName
            });
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
            blob.SetMetadataAsync(new Dictionary<string, string> { ["attempt"] = "held" }));
        Assert.Equal(409, held.Status);
        Assert.Equal("BlobImmutableDueToLegalHold", held.ErrorCode);

        await blob.SetLegalHoldAsync(false);
        var retained = await Assert.ThrowsAsync<RequestFailedException>(() =>
            blob.SetMetadataAsync(new Dictionary<string, string> { ["attempt"] = "retained" }));
        Assert.Equal(409, retained.Status);
        Assert.Equal("BlobImmutableDueToPolicy", retained.ErrorCode);

        await blob.DeleteImmutabilityPolicyAsync();
        await blob.SetMetadataAsync(new Dictionary<string, string> { ["state"] = "mutable" });
        Assert.Equal("mutable", (await blob.GetPropertiesAsync()).Value.Metadata["state"]);

        var locked = container.GetBlobClient("locked.txt");
        await locked.UploadAsync(BinaryData.FromString("locked payload"));
        await locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
        {
            ExpiresOn = expiresOn,
            PolicyMode = BlobImmutabilityPolicyMode.Unlocked
        });
        await locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
        {
            ExpiresOn = expiresOn.AddHours(1),
            PolicyMode = BlobImmutabilityPolicyMode.Locked
        });
        var cannotUnlock = await Assert.ThrowsAsync<RequestFailedException>(() =>
            locked.SetImmutabilityPolicyAsync(new BlobImmutabilityPolicy
            {
                ExpiresOn = expiresOn.AddHours(2),
                PolicyMode = BlobImmutabilityPolicyMode.Unlocked
            }));
        Assert.Equal("BlobImmutableDueToPolicy", cannotUnlock.ErrorCode);
        var cannotDelete = await Assert.ThrowsAsync<RequestFailedException>(() => locked.DeleteImmutabilityPolicyAsync());
        Assert.Equal("BlobImmutableDueToPolicy", cannotDelete.ErrorCode);
    }

    [Fact]
    public async Task ImmutableStorageWithVersioningCapabilityMatchesContainerAndPolicySemantics()
    {
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:ImmutableStorageWithVersioningEnabled"] = "true"
            });
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient($"version-worm-{Guid.NewGuid():N}");
        await container.CreateAsync();

        Assert.True((await container.GetPropertiesAsync()).Value.HasImmutableStorageWithVersioning);
        BlobContainerItem? listed = null;
        await foreach (var item in service.GetBlobContainersAsync(prefix: container.Name))
            listed = item;
        Assert.NotNull(listed);
        Assert.True(listed!.Properties.HasImmutableStorageWithVersioning);

        var versioned = container.GetBlobClient("versioned.txt");
        await versioned.UploadAsync(BinaryData.FromString("first"));
        await versioned.UploadAsync(BinaryData.FromString("second"), overwrite: true);
        var versions = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = versioned.Name,
            States = BlobStates.Version
        }))
        {
            versions.Add(item);
        }
        Assert.Equal(2, versions.Count);
        Assert.All(versions, item => Assert.False(string.IsNullOrEmpty(item.VersionId)));

        var append = container.GetAppendBlobClient("append-versioned.bin");
        var appendVersion = (await append.CreateAsync()).Value.VersionId;
        await append.AppendBlockAsync(BinaryData.FromString("append").ToStream());
        Assert.Equal(appendVersion, (await append.GetPropertiesAsync()).Value.VersionId);
        var appendVersions = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = append.Name,
            States = BlobStates.Version
        }))
        {
            appendVersions.Add(item);
        }
        Assert.Single(appendVersions);

        var page = container.GetPageBlobClient("page-versioned.bin");
        var pageVersion = (await page.CreateAsync(512)).Value.VersionId;
        await page.UploadPagesAsync(BinaryData.FromBytes(new byte[512]).ToStream(), 0);
        Assert.Equal(pageVersion, (await page.GetPropertiesAsync()).Value.VersionId);
        var pageVersions = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
        {
            Prefix = page.Name,
            States = BlobStates.Version
        }))
        {
            pageVersions.Add(item);
        }
        Assert.Single(pageVersions);

        using var transport = new HttpClient(application.Server.CreateHandler());
        var accountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddDays(7),
            Protocol = SasProtocol.HttpsAndHttp
        };
        accountSas.SetPermissions(AccountSasPermissions.Read);
        var containerSas = AppendQuery(
            container.Uri,
            accountSas.ToSasQueryParameters(new StorageSharedKeyCredential(
                SavaWebApplicationFactory.AccountName,
                SavaWebApplicationFactory.AccountKey)).ToString());
        using (var legacy = new HttpRequestMessage(HttpMethod.Head, AppendQuery(containerSas, "restype=container")))
        {
            legacy.Headers.TryAddWithoutValidation("x-ms-version", "2020-06-12");
            using var response = await transport.SendAsync(legacy);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.False(response.Headers.Contains("x-ms-immutable-storage-with-versioning-enabled"));
        }
        using (var supported = new HttpRequestMessage(HttpMethod.Head, AppendQuery(containerSas, "restype=container")))
        {
            supported.Headers.TryAddWithoutValidation("x-ms-version", "2020-10-02");
            using var response = await transport.SendAsync(supported);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(
                "true",
                response.Headers.GetValues("x-ms-immutable-storage-with-versioning-enabled").Single());
        }

        var protectedEmpty = service.GetBlobContainerClient($"version-worm-empty-{Guid.NewGuid():N}");
        await protectedEmpty.CreateAsync();
        var protectedDelete = await Assert.ThrowsAsync<RequestFailedException>(() => protectedEmpty.DeleteAsync());
        Assert.Equal(409, protectedDelete.Status);
        Assert.Equal("ContainerImmutableStorageWithVersioningEnabled", protectedDelete.ErrorCode);
        Assert.True((await protectedEmpty.ExistsAsync()).Value);

        var ordinaryService = CreateClient(
            application,
            SavaWebApplicationFactory.SecondAccountName,
            SavaWebApplicationFactory.SecondAccountKey);
        var ordinaryContainer = ordinaryService.GetBlobContainerClient($"ordinary-worm-{Guid.NewGuid():N}");
        await ordinaryContainer.CreateAsync();
        Assert.False((await ordinaryContainer.GetPropertiesAsync()).Value.HasImmutableStorageWithVersioning);
        var ordinaryAccountSas = new AccountSasBuilder
        {
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container,
            StartsOn = DateTimeOffset.UtcNow.AddMinutes(-1),
            ExpiresOn = DateTimeOffset.UtcNow.AddDays(7),
            Protocol = SasProtocol.HttpsAndHttp
        };
        ordinaryAccountSas.SetPermissions(AccountSasPermissions.Read);
        var ordinarySas = AppendQuery(
            ordinaryContainer.Uri,
            ordinaryAccountSas.ToSasQueryParameters(new StorageSharedKeyCredential(
                SavaWebApplicationFactory.SecondAccountName,
                SavaWebApplicationFactory.SecondAccountKey)).ToString());
        using (var ordinaryProperties = new HttpRequestMessage(
                   HttpMethod.Head,
                   AppendQuery(ordinarySas, "restype=container")))
        {
            ordinaryProperties.Headers.TryAddWithoutValidation("x-ms-version", "2020-10-02");
            using var response = await transport.SendAsync(ordinaryProperties);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(
                "false",
                response.Headers.GetValues("x-ms-immutable-storage-with-versioning-enabled").Single());
        }

        var unsupported = ordinaryContainer.GetBlobClient("unsupported.txt");
        var unsupportedPolicy = await Assert.ThrowsAsync<RequestFailedException>(() =>
            unsupported.UploadAsync(
                BinaryData.FromString("must not publish"),
                new BlobUploadOptions
                {
                    ImmutabilityPolicy = new BlobImmutabilityPolicy
                    {
                        ExpiresOn = DateTimeOffset.UtcNow.AddDays(1),
                        PolicyMode = BlobImmutabilityPolicyMode.Unlocked
                    }
                }));
        Assert.Equal(409, unsupportedPolicy.Status);
        Assert.Equal("BlobOperationNotSupported", unsupportedPolicy.ErrorCode);
        Assert.False((await unsupported.ExistsAsync()).Value);
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

        var smart = await blob.SetAccessTierAsync(AccessTier.Smart);
        Assert.Equal(200, smart.Status);
        var smartProperties = (await blob.GetPropertiesAsync()).Value;
        Assert.Equal(AccessTier.Smart, smartProperties.AccessTier);
        Assert.Equal("Hot", smartProperties.SmartAccessTier);
        var smartItems = new List<BlobItem>();
        await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions { Prefix = blob.Name }))
            smartItems.Add(item);
        var smartItem = Assert.Single(smartItems);
        Assert.Equal(AccessTier.Smart, smartItem.Properties.AccessTier);
        Assert.Equal("Hot", smartItem.Properties.SmartAccessTier);

        using var transport = new HttpClient(factory.Server.CreateHandler());
        var tierUri = AppendQuery(
            blob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
            "comp=tier");
        using var oldVersionTierRequest = new HttpRequestMessage(HttpMethod.Put, tierUri)
        {
            Content = new ByteArrayContent([])
        };
        oldVersionTierRequest.Headers.Add("x-ms-version", "2023-11-03");
        oldVersionTierRequest.Headers.Add("x-ms-access-tier", "Smart");
        using var oldVersionTierResponse = await transport.SendAsync(oldVersionTierRequest);
        Assert.Equal(HttpStatusCode.Conflict, oldVersionTierResponse.StatusCode);
        Assert.Equal("FeatureVersionMismatch", oldVersionTierResponse.Headers.GetValues("x-ms-error-code").Single());

        using var oldVersionPropertiesRequest = new HttpRequestMessage(
            HttpMethod.Head,
            blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        oldVersionPropertiesRequest.Headers.Add("x-ms-version", "2023-11-03");
        using var oldVersionPropertiesResponse = await transport.SendAsync(oldVersionPropertiesRequest);
        Assert.Equal(HttpStatusCode.OK, oldVersionPropertiesResponse.StatusCode);
        Assert.False(oldVersionPropertiesResponse.Headers.Contains("x-ms-smart-access-tier"));
    }

    [Fact]
    public async Task SmartTierTracksDataAccessAndTransitionsOnlyEligibleBlobs()
    {
        var clock = new AdjustableTimeProvider(DateTimeOffset.UtcNow);
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>
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
            var serviceProperties = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                serviceProperties with
                {
                    BlobSoftDeleteEnabled = true,
                    BlobSoftDeleteRetentionDays = 365
                },
                CancellationToken.None);

            var managedBytes = Enumerable.Range(0, 128 * 1024 + 1)
                .Select(index => (byte)(index % 251))
                .ToArray();
            var managed = container.GetBlobClient("managed.bin");
            await managed.UploadAsync(
                BinaryData.FromBytes(managedBytes),
                new BlobUploadOptions { AccessTier = AccessTier.Smart });
            var small = container.GetBlobClient("small.bin");
            await small.UploadAsync(
                BinaryData.FromBytes(new byte[128 * 1024]),
                new BlobUploadOptions { AccessTier = AccessTier.Smart });
            var deleted = container.GetBlobClient("deleted.bin");
            await deleted.UploadAsync(
                BinaryData.FromBytes(managedBytes),
                new BlobUploadOptions { AccessTier = AccessTier.Smart });
            await deleted.DeleteAsync();

            var original = (await managed.GetPropertiesAsync()).Value;
            Assert.Equal(AccessTier.Smart, original.AccessTier);
            Assert.Equal("Hot", original.SmartAccessTier);

            clock.Advance(TimeSpan.FromDays(30) - TimeSpan.FromTicks(1));
            var early = await blobs.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal(0, early.CompletedSmartTierTransitions);
            Assert.Equal("Hot", (await managed.GetPropertiesAsync()).Value.SmartAccessTier);

            clock.Advance(TimeSpan.FromTicks(1));
            var cooled = await blobs.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal(2, cooled.CompletedSmartTierTransitions);
            var coolProperties = (await managed.GetPropertiesAsync()).Value;
            Assert.Equal("Cool", coolProperties.SmartAccessTier);
            Assert.Equal(original.ETag, coolProperties.ETag);
            Assert.Equal(original.LastModified, coolProperties.LastModified);
            Assert.Equal("Hot", (await small.GetPropertiesAsync()).Value.SmartAccessTier);
            var deletedRecord = await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                deleted.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: true,
                CancellationToken.None);
            Assert.Equal("Cool", deletedRecord?.SmartAccessTier);

            _ = await managed.GetPropertiesAsync();
            _ = await managed.GetTagsAsync();
            await managed.SetMetadataAsync(new Dictionary<string, string> { ["observed"] = "without-access" });
            clock.Advance(TimeSpan.FromDays(60));
            var chilled = await blobs.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal(2, chilled.CompletedSmartTierTransitions);
            Assert.Equal("Cold", (await managed.GetPropertiesAsync()).Value.SmartAccessTier);
            Assert.Equal("Hot", (await small.GetPropertiesAsync()).Value.SmartAccessTier);

            Assert.Equal(managedBytes, (await managed.DownloadContentAsync()).Value.Content.ToArray());
            var reheated = (await managed.GetPropertiesAsync()).Value;
            Assert.Equal("Hot", reheated.SmartAccessTier);
            var accessedRecord = await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                container.Name,
                managed.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);
            Assert.Equal(clock.GetUtcNow(), accessedRecord?.SmartTierLastAccessedAt);

            clock.Advance(TimeSpan.FromDays(30) - TimeSpan.FromTicks(1));
            _ = await blobs.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal("Hot", (await managed.GetPropertiesAsync()).Value.SmartAccessTier);
            clock.Advance(TimeSpan.FromTicks(1));
            _ = await blobs.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal("Cool", (await managed.GetPropertiesAsync()).Value.SmartAccessTier);

            clock.Advance(TimeSpan.FromDays(60));
            _ = await blobs.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal("Cold", (await managed.GetPropertiesAsync()).Value.SmartAccessTier);
            await managed.UploadAsync(BinaryData.FromBytes(managedBytes), overwrite: true);
            var rewritten = (await managed.GetPropertiesAsync()).Value;
            Assert.Equal(AccessTier.Smart, rewritten.AccessTier);
            Assert.Equal("Hot", rewritten.SmartAccessTier);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task LastAccessTimeTrackingMatchesAzureReadWriteAndListingSemantics()
    {
        var initialTime = new DateTimeOffset(2026, 9, 22, 14, 0, 0, TimeSpan.Zero);
        var clock = new AdjustableTimeProvider(initialTime);
        var application = new SavaWebApplicationFactory(
            clock,
            new Dictionary<string, string?>
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

            var created = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(initialTime, created.LastAccessed);

            clock.Advance(TimeSpan.FromHours(23));
            var propertiesOnly = (await blob.GetPropertiesAsync()).Value;
            Assert.Equal(initialTime, propertiesOnly.LastAccessed);
            var firstRead = (await blob.DownloadContentAsync()).Value;
            Assert.Equal("first", firstRead.Content.ToString());
            Assert.Equal(initialTime, firstRead.Details.LastAccessed);

            clock.Advance(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(1));
            var secondAccess = clock.GetUtcNow();
            var secondRead = (await blob.DownloadContentAsync()).Value;
            Assert.Equal(secondAccess, secondRead.Details.LastAccessed);
            Assert.Equal(secondAccess, (await blob.GetPropertiesAsync()).Value.LastAccessed);

            BlobItem? listed = null;
            await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions { Prefix = blob.Name }))
                listed = item;
            Assert.NotNull(listed);
            Assert.Equal(secondAccess, listed!.Properties.LastAccessedOn);

            clock.Advance(TimeSpan.FromMinutes(5));
            var rewrittenAt = clock.GetUtcNow();
            await blob.UploadAsync(BinaryData.FromString("second"), overwrite: true);
            Assert.Equal(rewrittenAt, (await blob.GetPropertiesAsync()).Value.LastAccessed);

            clock.Advance(TimeSpan.FromHours(24) + TimeSpan.FromSeconds(1));
            var copiedAt = clock.GetUtcNow();
            var copied = container.GetBlobClient("copied.bin");
            await copied.SyncCopyFromUriAsync(
                blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddDays(7)));
            Assert.Equal(copiedAt, (await blob.GetPropertiesAsync()).Value.LastAccessed);
            Assert.Equal(copiedAt, (await copied.GetPropertiesAsync()).Value.LastAccessed);

            using var transport = new HttpClient(application.Server.CreateHandler());
            var arrowUri = AppendQuery(
                container.GenerateSasUri(BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddDays(7)),
                "restype=container&comp=list");
            using (var arrowRequest = new HttpRequestMessage(HttpMethod.Get, arrowUri))
            {
                arrowRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
                arrowRequest.Headers.TryAddWithoutValidation("Accept", AzureResponseWriter.ArrowStreamContentType);
                using var response = await transport.SendAsync(arrowRequest);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                await using var stream = await response.Content.ReadAsStreamAsync();
                using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(stream);
                using var batch = await reader.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                var names = Assert.IsType<Apache.Arrow.StringArray>(batch.Column("Name"));
                var accesses = Assert.IsType<Apache.Arrow.TimestampArray>(batch.Column("LastAccessTime"));
                var sourceIndex = Enumerable.Range(0, batch.Length).Single(index => names.GetString(index) == blob.Name);
                Assert.Equal(copiedAt, accesses.GetTimestamp(sourceIndex));
            }

            using (var legacy = new HttpRequestMessage(
                       HttpMethod.Head,
                       blob.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddDays(7))))
            {
                legacy.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
                using var response = await transport.SendAsync(legacy);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.False(response.Headers.Contains("x-ms-last-access-time"));
            }

            var untrackedService = CreateClient(
                application,
                SavaWebApplicationFactory.SecondAccountName,
                SavaWebApplicationFactory.SecondAccountKey);
            var untrackedContainer = untrackedService.GetBlobContainerClient($"last-access-off-{Guid.NewGuid():N}");
            await untrackedContainer.CreateAsync();
            var untracked = untrackedContainer.GetBlobClient("untracked.bin");
            await untracked.UploadAsync(BinaryData.FromString("untracked"));
            Assert.Equal(default, (await untracked.GetPropertiesAsync()).Value.LastAccessed);
            Assert.Equal(default, (await untracked.DownloadContentAsync()).Value.Details.LastAccessed);
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task AsynchronousCopiesCompleteDurablyAndCanBeAborted()
    {
        var remoteBytes = Enumerable.Range(0, 48 * 1024).Select(index => (byte)(index % 241)).ToArray();
        await using var remote = await LoopbackSource.StartAsync(remoteBytes);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"copy-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var abortDestination = container.GetBlobClient("abort.bin");
        var credentialedSource = new UriBuilder(remote.Uri) { Query = "source=remote&sig=must-not-leak" }.Uri;
        var abortOperation = await abortDestination.StartCopyFromUriAsync(
            credentialedSource,
            new BlobCopyFromUriOptions
            {
                Metadata = new Dictionary<string, string> { ["copy"] = "abort" }
            });
        Assert.Equal(202, abortOperation.GetRawResponse().Status);
        Assert.False(abortOperation.HasCompleted);
        var pending = (await abortDestination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Pending, pending.CopyStatus);
        Assert.Equal(0, pending.ContentLength);
        Assert.Contains("source=remote", pending.CopySource.AbsoluteUri, StringComparison.Ordinal);
        Assert.DoesNotContain("sig=", pending.CopySource.AbsoluteUri, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("abort", pending.Metadata["copy"]);

        var aborted = await abortDestination.AbortCopyFromUriAsync(abortOperation.Id);
        Assert.Equal(204, aborted.Status);
        var abortedProperties = (await abortDestination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Aborted, abortedProperties.CopyStatus);
        Assert.Equal(0, abortedProperties.ContentLength);
        Assert.Equal("abort", abortedProperties.Metadata["copy"]);

        var source = container.GetBlobClient("nested//source%.bin");
        await source.UploadAsync(BinaryData.FromBytes(remoteBytes), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string> { ["origin"] = "internal" },
            Tags = new Dictionary<string, string> { ["class"] = "copy" }
        });
        var completedDestination = container.GetBlobClient("completed.bin");
        var completion = await completedDestination.StartCopyFromUriAsync(source.Uri);
        Assert.Equal(CopyStatus.Pending, (await completedDestination.GetPropertiesAsync()).Value.CopyStatus);
        await completion.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);

        var completed = (await completedDestination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Success, completed.CopyStatus);
        Assert.Equal($"{remoteBytes.LongLength}/{remoteBytes.LongLength}", completed.CopyProgress);
        Assert.Equal("internal", completed.Metadata["origin"]);
        Assert.Empty((await completedDestination.GetTagsAsync()).Value.Tags);
        Assert.Equal(remoteBytes, (await completedDestination.DownloadContentAsync()).Value.Content.ToArray());
    }

    [Fact]
    public async Task LegacyCopyBlobIsSynchronousSameAccountAndHonorsItsSourceLeaseHeader()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"legacy-copy-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlockBlobClient("source.bin");
        var firstBlockId = Convert.ToBase64String("legacy-copy-block-0001"u8);
        var secondBlockId = Convert.ToBase64String("legacy-copy-block-0002"u8);
        await source.StageBlockAsync(firstBlockId, BinaryData.FromString("first|").ToStream());
        await source.StageBlockAsync(secondBlockId, BinaryData.FromString("second").ToStream());
        await source.CommitBlockListAsync(
            [firstBlockId, secondBlockId],
            new CommitBlockListOptions
            {
                Metadata = new Dictionary<string, string> { ["origin"] = "legacy" },
                HttpHeaders = new BlobHttpHeaders { ContentType = "application/x-legacy-copy" }
            });
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
            Assert.Equal("LeaseIdMismatchWithBlobOperation", response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await mismatchedTarget.ExistsAsync()).Value);

        var destination = container.GetBlockBlobClient("destination.bin");
        using (var request = CreateLegacyCopyRequest(destination.Uri, sourcePath, sourceLeaseId))
        using (var response = await transport.SendAsync(request))
        {
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            Assert.NotNull(response.Headers.ETag);
            Assert.True(response.Content.Headers.LastModified.HasValue);
            Assert.False(response.Headers.Contains("x-ms-copy-id"));
            Assert.False(response.Headers.Contains("x-ms-copy-status"));
        }

        var properties = (await destination.GetPropertiesAsync()).Value;
        Assert.Equal(default, properties.CopyStatus);
        Assert.Equal("application/x-legacy-copy", properties.ContentType);
        Assert.Equal("legacy", properties.Metadata["origin"]);
        Assert.Equal("first|second", (await destination.DownloadContentAsync()).Value.Content.ToString());
        var blocks = (await destination.GetBlockListAsync(BlockListTypes.Committed)).Value.CommittedBlocks;
        Assert.Equal([firstBlockId, secondBlockId], blocks.Select(block => block.Name));

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
            Assert.Equal("LeaseNotPresentWithBlobOperation", response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await absentSourceLeaseTarget.ExistsAsync()).Value);

        var pageSource = container.GetPageBlobClient("source.vhd");
        await pageSource.CreateAsync(1024, new PageBlobCreateOptions { SequenceNumber = 9 });
        await pageSource.UploadPagesAsync(
            new MemoryStream(Enumerable.Repeat((byte)0x51, 512).ToArray()),
            offset: 512);
        var pageDestination = container.GetPageBlobClient("destination.vhd");
        var pageSourcePath = $"/{SavaWebApplicationFactory.AccountName}/{container.Name}/{pageSource.Name}";
        using (var request = CreateLegacyCopyRequest(pageDestination.Uri, pageSourcePath))
        using (var response = await transport.SendAsync(request))
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var pageProperties = (await pageDestination.GetPropertiesAsync()).Value;
        Assert.Equal(BlobType.Page, pageProperties.BlobType);
        Assert.Equal(9, pageProperties.BlobSequenceNumber);
        Assert.Equal(default, pageProperties.CopyStatus);
        var pageRange = Assert.Single((await pageDestination.GetPageRangesAsync()).Value.PageRanges);
        Assert.Equal(512, pageRange.Offset);
        Assert.Equal(512, pageRange.Length);

        var crossAccountTarget = container.GetBlobClient("cross-account.bin");
        var crossAccountSource = $"/{SavaWebApplicationFactory.SecondAccountName}/{container.Name}/{source.Name}";
        using (var request = CreateLegacyCopyRequest(crossAccountTarget.Uri, crossAccountSource))
        using (var response = await transport.SendAsync(request))
        {
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("CopyAcrossAccountsNotSupported", response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await crossAccountTarget.ExistsAsync()).Value);
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
            Metadata = new Dictionary<string, string> { ["origin"] = "source-sas" },
            Tags = new Dictionary<string, string> { ["class"] = "tagged" }
        });

        var sourceSas = source.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var destination = container.GetBlobClient("destination.bin");
        var destinationSas = destination.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var delegatedDestination = CreateBlobClient(factory, destinationSas);
        var copy = await delegatedDestination.StartCopyFromUriAsync(sourceSas);
        await copy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);

        Assert.Equal(content, (await destination.DownloadContentAsync()).Value.Content.ToArray());
        Assert.Equal("source-sas", (await destination.GetPropertiesAsync()).Value.Metadata["origin"]);

        var taggedSourceSas = source.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Tag,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var taggedDestination = container.GetBlobClient("tag-conditioned.bin");
        var taggedDestinationSas = taggedDestination.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var taggedCopy = await CreateBlobClient(factory, taggedDestinationSas).StartCopyFromUriAsync(
            taggedSourceSas,
            new BlobCopyFromUriOptions
            {
                SourceConditions = new BlobRequestConditions { TagConditions = "\"class\" = 'tagged'" }
            });
        await taggedCopy.WaitForCompletionAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        Assert.Equal(content, (await taggedDestination.DownloadContentAsync()).Value.Content.ToArray());

        var untaggedSourceTarget = container.GetBlobClient("tag-condition-without-source-permission.bin");
        var untaggedSourceTargetSas = untaggedSourceTarget.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var missingSourceTagPermission = await Assert.ThrowsAsync<RequestFailedException>(() =>
            CreateBlobClient(factory, untaggedSourceTargetSas).StartCopyFromUriAsync(
                sourceSas,
                new BlobCopyFromUriOptions
                {
                    SourceConditions = new BlobRequestConditions { TagConditions = "\"class\" = 'tagged'" }
                }));
        Assert.Equal(403, missingSourceTagPermission.Status);
        Assert.False((await untaggedSourceTarget.ExistsAsync()).Value);

        var bearerTarget = container.GetBlobClient("bearer-source.bin");
        var bearerTargetSas = bearerTarget.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var sourceToken = CreateJwt(SavaWebApplicationFactory.AccountKey, "reader-1");
        using (var transport = new HttpClient(factory.Server.CreateHandler()))
        using (var request = new HttpRequestMessage(HttpMethod.Put, bearerTargetSas)
        {
            Content = new ByteArrayContent([])
        })
        {
            request.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            request.Headers.TryAddWithoutValidation("x-ms-copy-source", source.Uri.AbsoluteUri);
            request.Headers.TryAddWithoutValidation("x-ms-copy-source-authorization", $"Bearer {sourceToken}");
            using var response = await transport.SendAsync(request);
            Assert.True(
                response.StatusCode == HttpStatusCode.Accepted,
                $"Expected 202 but received {(int)response.StatusCode}: {await response.Content.ReadAsStringAsync()}");
        }
        for (var attempt = 0; attempt < 60; attempt++)
        {
            if ((await bearerTarget.GetPropertiesAsync()).Value.CopyStatus == CopyStatus.Success)
                break;
            await Task.Delay(50);
        }
        Assert.Equal(content, (await bearerTarget.DownloadContentAsync()).Value.Content.ToArray());

        var unsignedTarget = container.GetBlobClient("unsigned-source.bin");
        var unsignedTargetSas = unsignedTarget.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var unsignedSource = await Assert.ThrowsAsync<RequestFailedException>(() =>
            CreateBlobClient(factory, unsignedTargetSas).StartCopyFromUriAsync(source.Uri));
        Assert.Equal(403, unsignedSource.Status);
        Assert.False((await unsignedTarget.ExistsAsync()).Value);

        var other = container.GetBlobClient("other.bin");
        await other.UploadAsync(BinaryData.FromString("other"));
        var otherSas = other.GenerateSasUri(
            BlobSasPermissions.Read,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var wrongResourceSource = new UriBuilder(source.Uri)
        {
            Query = otherSas.Query.TrimStart('?')
        }.Uri;
        var wrongResourceTarget = container.GetBlobClient("wrong-resource.bin");
        var wrongResourceTargetSas = wrongResourceTarget.GenerateSasUri(
            BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        var wrongResource = await Assert.ThrowsAsync<RequestFailedException>(() =>
            CreateBlobClient(factory, wrongResourceTargetSas).StartCopyFromUriAsync(wrongResourceSource));
        Assert.Equal(403, wrongResource.Status);
        Assert.False((await wrongResourceTarget.ExistsAsync()).Value);

        var sourceLeaseTarget = container.GetBlobClient("source-lease-header.bin");
        var sourceLeaseTargetSas = sourceLeaseTarget.GenerateSasUri(
            BlobSasPermissions.Create | BlobSasPermissions.Write,
            DateTimeOffset.UtcNow.AddMinutes(10));
        using var sourceLeaseTransport = new HttpClient(factory.Server.CreateHandler());
        using var sourceLeaseRequest = new HttpRequestMessage(HttpMethod.Put, sourceLeaseTargetSas)
        {
            Content = new ByteArrayContent([])
        };
        sourceLeaseRequest.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
        sourceLeaseRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", sourceSas.AbsoluteUri);
        sourceLeaseRequest.Headers.TryAddWithoutValidation("x-ms-source-lease-id", Guid.NewGuid().ToString());
        using var sourceLeaseResponse = await sourceLeaseTransport.SendAsync(sourceLeaseRequest);
        Assert.Equal(HttpStatusCode.BadRequest, sourceLeaseResponse.StatusCode);
        Assert.Equal("UnsupportedHeader", sourceLeaseResponse.Headers.GetValues("x-ms-error-code").Single());
        Assert.False((await sourceLeaseTarget.ExistsAsync()).Value);
    }

    [Fact]
    public async Task BlobBatchesExecuteIndependentDeleteAndTierSubrequests()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"batch-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var deleteTarget = container.GetBlobClient("nested//delete.bin");
        await deleteTarget.UploadAsync(BinaryData.FromString("delete me"));

        var serviceBatchClient = service.GetBlobBatchClient();
        using (var deleteBatch = serviceBatchClient.CreateBatch())
        {
            var deleted = deleteBatch.DeleteBlob(container.Name, deleteTarget.Name);
            var missing = deleteBatch.DeleteBlob(container.Name, "missing.bin");
            var submitted = await serviceBatchClient.SubmitBatchAsync(deleteBatch, throwOnAnyFailure: false);

            Assert.Equal(202, submitted.Status);
            Assert.Equal(202, deleted.Status);
            Assert.Equal(404, missing.Status);
        }
        Assert.False(await deleteTarget.ExistsAsync());

        var tierTarget = container.GetBlobClient("nested//tier.bin");
        await tierTarget.UploadAsync(BinaryData.FromString("tier me"));
        var tierLeaseId = Guid.NewGuid().ToString();
        var tierLease = tierTarget.GetBlobLeaseClient(tierLeaseId);
        await tierLease.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration);
        var rejectedDirectTier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            tierTarget.SetAccessTierAsync(
                AccessTier.Cool,
                new BlobRequestConditions { LeaseId = Guid.NewGuid().ToString() }));
        Assert.Equal(StatusCodes.Status412PreconditionFailed, rejectedDirectTier.Status);
        Assert.Equal("LeaseIdMismatchWithBlobOperation", rejectedDirectTier.ErrorCode);
        Assert.Equal(AccessTier.Hot, (await tierTarget.GetPropertiesAsync()).Value.AccessTier);
        await tierTarget.SetAccessTierAsync(AccessTier.Cool);

        var containerBatchClient = container.GetBlobBatchClient();
        using (var tierBatch = containerBatchClient.CreateBatch())
        {
            var changed = tierBatch.SetBlobAccessTier(container.Name, tierTarget.Name, AccessTier.Cool);
            var missing = tierBatch.SetBlobAccessTier(
                container.Name,
                "missing-tier.bin",
                AccessTier.Hot);
            var submitted = await containerBatchClient.SubmitBatchAsync(tierBatch, throwOnAnyFailure: false);

            Assert.Equal(202, submitted.Status);
            Assert.Equal(200, changed.Status);
            Assert.Equal(404, missing.Status);
        }
        Assert.Equal(AccessTier.Cool, (await tierTarget.GetPropertiesAsync()).Value.AccessTier);

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
                throwOnAnyFailure: false);

            Assert.Equal(StatusCodes.Status202Accepted, submitted.Status);
            Assert.Equal(StatusCodes.Status412PreconditionFailed, rejected.Status);
            Assert.Equal(StatusCodes.Status200OK, accepted.Status);
        }
        Assert.Equal(AccessTier.Hot, (await tierTarget.GetPropertiesAsync()).Value.AccessTier);
        await tierLease.ReleaseAsync();
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
            Metadata = new Dictionary<string, string> { ["private"] = "metadata" }
        });
        var chunksAfterKeyed = Directory.GetFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Length;
        var keyedProperties = (await keyedBlob.GetPropertiesAsync()).Value;
        Assert.Equal(expectedHash, keyedProperties.EncryptionKeySha256);
        Assert.Equal(content, (await keyedBlob.DownloadContentAsync()).Value.Content.ToArray());

        using (var transport = new HttpClient(factory.Server.CreateHandler()))
        using (var oldTierRequest = new HttpRequestMessage(
                   HttpMethod.Put,
                   AppendQuery(
                       keyedBlob.GenerateSasUri(BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(5)),
                       "comp=tier"))
        {
            Content = new ByteArrayContent([])
        })
        {
            oldTierRequest.Headers.TryAddWithoutValidation("x-ms-version", "2021-12-02");
            oldTierRequest.Headers.TryAddWithoutValidation("x-ms-access-tier", "Cool");
            using var oldTierResponse = await transport.SendAsync(oldTierRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldTierResponse.StatusCode);
            Assert.Equal(
                "BlobUsesCustomerSpecifiedEncryption",
                oldTierResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.Equal(AccessTier.Hot, (await keyedBlob.GetPropertiesAsync()).Value.AccessTier);

        var oldEndpoint = new Uri($"http://{SavaWebApplicationFactory.AccountName}.localhost");
        var oldTransport = new HttpClient(factory.Server.CreateHandler()) { BaseAddress = oldEndpoint };
        var oldOptions = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2021_12_02)
        {
            Transport = new HttpClientTransport(oldTransport),
            Retry = { MaxRetries = 0 }
        };
        var oldService = new BlobServiceClient(
            oldEndpoint,
            new StorageSharedKeyCredential(SavaWebApplicationFactory.AccountName, SavaWebApplicationFactory.AccountKey),
            oldOptions);
        var oldBatchClient = oldService.GetBlobBatchClient();
        using (var oldBatch = oldBatchClient.CreateBatch())
        {
            var rejectedTier = oldBatch.SetBlobAccessTier(containerName, keyedBlob.Name, AccessTier.Cool);
            var batchResponse = await oldBatchClient.SubmitBatchAsync(oldBatch, throwOnAnyFailure: false);
            Assert.Equal(StatusCodes.Status202Accepted, batchResponse.Status);
            Assert.Equal(StatusCodes.Status409Conflict, rejectedTier.Status);
            Assert.True(rejectedTier.Headers.TryGetValue("x-ms-error-code", out var errorCode));
            Assert.Equal("BlobUsesCustomerSpecifiedEncryption", errorCode);
        }
        Assert.Equal(AccessTier.Hot, (await keyedBlob.GetPropertiesAsync()).Value.AccessTier);

        var keyedTier = await keyedBlob.SetAccessTierAsync(AccessTier.Cool);
        Assert.Equal(StatusCodes.Status200OK, keyedTier.Status);
        Assert.Equal(AccessTier.Cool, (await keyedBlob.GetPropertiesAsync()).Value.AccessTier);

        var batchClient = normalService.GetBlobBatchClient();
        using (var batch = batchClient.CreateBatch())
        {
            var tiered = batch.SetBlobAccessTier(containerName, keyedBlob.Name, AccessTier.Hot);
            var batchResponse = await batchClient.SubmitBatchAsync(batch, throwOnAnyFailure: false);
            Assert.Equal(StatusCodes.Status202Accepted, batchResponse.Status);
            Assert.Equal(StatusCodes.Status200OK, tiered.Status);
        }
        Assert.Equal(AccessTier.Hot, (await keyedBlob.GetPropertiesAsync()).Value.AccessTier);

        var missingKey = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin").GetPropertiesAsync());
        Assert.Equal(409, missingKey.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingKey.ErrorCode);
        var wrongKeyService = CreateEncryptedClient(factory, new CustomerProvidedKey(wrongKey), encryptionScope: null);
        var mismatchedKey = await Assert.ThrowsAsync<RequestFailedException>(() =>
            wrongKeyService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin").DownloadContentAsync());
        Assert.Equal(409, mismatchedKey.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", mismatchedKey.ErrorCode);

        const string scope = "records-scope";
        var scopedService = CreateEncryptedClient(factory, customerProvidedKey: null, scope);
        var scopedBlob = scopedService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin");
        await scopedBlob.UploadAsync(BinaryData.FromBytes(content));
        var chunksAfterScoped = Directory.GetFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Length;
        var scopedProperties = (await scopedBlob.GetPropertiesAsync()).Value;
        Assert.Equal(scope, scopedProperties.EncryptionScope);
        Assert.Equal(content, (await normalService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin").DownloadContentAsync()).Value.Content.ToArray());
        var missingScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName)
                .GetBlobClient("scoped.bin")
                .SetMetadataAsync(new Dictionary<string, string> { ["scope"] = "missing" }));
        Assert.Equal(StatusCodes.Status409Conflict, missingScope.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingScope.ErrorCode);
        var wrongScopeService = CreateEncryptedClient(factory, customerProvidedKey: null, "wrong-scope");
        var wrongScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            wrongScopeService.GetBlobContainerClient(containerName)
                .GetBlobClient("scoped.bin")
                .SetMetadataAsync(new Dictionary<string, string> { ["scope"] = "wrong" }));
        Assert.Equal(StatusCodes.Status409Conflict, wrongScope.Status);
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", wrongScope.ErrorCode);
        await scopedBlob.SetMetadataAsync(new Dictionary<string, string> { ["scope"] = "matched" });

        var scopedAppend = scopedService.GetBlobContainerClient(containerName).GetAppendBlobClient("scoped-append.bin");
        await scopedAppend.CreateAsync();
        var missingAppendScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName)
                .GetAppendBlobClient(scopedAppend.Name)
                .AppendBlockAsync(BinaryData.FromString("must fail").ToStream()));
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingAppendScope.ErrorCode);
        await scopedAppend.AppendBlockAsync(BinaryData.FromString("matched append").ToStream());

        var scopedPage = scopedService.GetBlobContainerClient(containerName).GetPageBlobClient("scoped-page.bin");
        await scopedPage.CreateAsync(512);
        var missingPageScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName)
                .GetPageBlobClient(scopedPage.Name)
                .UploadPagesAsync(new MemoryStream(new byte[512]), 0));
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingPageScope.ErrorCode);
        await scopedPage.UploadPagesAsync(new MemoryStream(new byte[512]), 0);

        var scopedBlock = scopedService.GetBlobContainerClient(containerName).GetBlockBlobClient("scoped-block.bin");
        await scopedBlock.UploadAsync(BinaryData.FromString("initial block").ToStream());
        var missingBlockScope = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName)
                .GetBlockBlobClient(scopedBlock.Name)
                .StageBlockAsync(
                    Convert.ToBase64String("scope-block"u8),
                    BinaryData.FromString("must fail").ToStream()));
        Assert.Equal("BlobUsesCustomerSpecifiedEncryption", missingBlockScope.ErrorCode);

        var listed = new List<BlobItem>();
        await foreach (var item in normalService.GetBlobContainerClient(containerName).GetBlobsAsync(
                           new GetBlobsOptions { Traits = BlobTraits.Metadata }))
            listed.Add(item);
        var listedKeyed = Assert.Single(listed, item => item.Name == "keyed.bin");
        Assert.Equal(expectedHash, listedKeyed.Properties.CustomerProvidedKeySha256);
        Assert.Empty(listedKeyed.Metadata);
        Assert.Equal(scope, Assert.Single(listed, item => item.Name == "scoped.bin").Properties.EncryptionScope);
        Assert.True(chunksAfterKeyed > chunksBefore);
        Assert.True(chunksAfterScoped > chunksAfterKeyed);
        var metadataBytes = await File.ReadAllBytesAsync(Path.Combine(factory.DataPath, "metadata.db"));
        Assert.DoesNotContain(Convert.ToBase64String(key), Encoding.Latin1.GetString(metadataBytes), StringComparison.Ordinal);
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

        var containerProperties = (await container.GetPropertiesAsync()).Value;
        Assert.Equal(defaultScope, containerProperties.DefaultEncryptionScope);
        Assert.True(containerProperties.PreventEncryptionScopeOverride);

        BlobContainerItem? listedContainer = null;
        await foreach (var item in service.GetBlobContainersAsync(prefix: container.Name))
        {
            if (item.Name == container.Name)
                listedContainer = item;
        }
        Assert.NotNull(listedContainer);
        Assert.Equal(defaultScope, listedContainer.Properties.DefaultEncryptionScope);
        Assert.True(listedContainer.Properties.PreventEncryptionScopeOverride);

        var defaultBlob = container.GetBlobClient("default.bin");
        await defaultBlob.UploadAsync(BinaryData.FromString("default encryption scope"));
        Assert.Equal(defaultScope, (await defaultBlob.GetPropertiesAsync()).Value.EncryptionScope);
        await defaultBlob.SetMetadataAsync(new Dictionary<string, string> { ["scope"] = "container-default" });
        var defaultSnapshot = await defaultBlob.CreateSnapshotAsync();
        Assert.Equal(
            defaultScope,
            (await defaultBlob.WithSnapshot(defaultSnapshot.Value.Snapshot).GetPropertiesAsync()).Value.EncryptionScope);
        var tierChange = await Assert.ThrowsAsync<RequestFailedException>(() =>
            defaultBlob.SetAccessTierAsync(AccessTier.Cool));
        Assert.Equal(StatusCodes.Status409Conflict, tierChange.Status);
        Assert.Equal("BlobOperationNotSupported", tierChange.ErrorCode);
        var explicitTier = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlobClient("explicit-tier.bin").UploadAsync(
                BinaryData.FromString("scope with explicit tier"),
                new BlobUploadOptions { AccessTier = AccessTier.Cool }));
        Assert.Equal(StatusCodes.Status409Conflict, explicitTier.Status);
        Assert.Equal("BlobOperationNotSupported", explicitTier.ErrorCode);

        var block = container.GetBlockBlobClient("staged.bin");
        var blockId = Convert.ToBase64String("block-0001"u8);
        var staged = await block.StageBlockAsync(blockId, BinaryData.FromString("staged scope").ToStream());
        Assert.Equal(defaultScope, staged.Value.EncryptionScope);
        await block.CommitBlockListAsync([blockId]);
        Assert.Equal(defaultScope, (await block.GetPropertiesAsync()).Value.EncryptionScope);

        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync();
        await append.AppendBlockAsync(BinaryData.FromString("append scope").ToStream());
        Assert.Equal(defaultScope, (await append.GetPropertiesAsync()).Value.EncryptionScope);

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(512);
        await page.UploadPagesAsync(new MemoryStream(new byte[512]), 0);
        Assert.Equal(defaultScope, (await page.GetPropertiesAsync()).Value.EncryptionScope);

        var scopedService = CreateEncryptedClient(factory, customerProvidedKey: null, defaultScope);
        var matchingBlob = scopedService.GetBlobContainerClient(container.Name).GetBlobClient("matching.bin");
        await matchingBlob.UploadAsync(BinaryData.FromString("matching encryption scope"));
        Assert.Equal(defaultScope, (await matchingBlob.GetPropertiesAsync()).Value.EncryptionScope);

        var otherScopeService = CreateEncryptedClient(factory, customerProvidedKey: null, "other-scope");
        var rejected = await Assert.ThrowsAsync<RequestFailedException>(() =>
            otherScopeService.GetBlobContainerClient(container.Name)
                .GetBlobClient("rejected.bin")
                .UploadAsync(BinaryData.FromString("rejected encryption scope")));
        Assert.Equal(StatusCodes.Status403Forbidden, rejected.Status);
        Assert.Equal("RequestForbiddenByContainerEncryptionPolicy", rejected.ErrorCode);

        var permissive = service.GetBlobContainerClient($"scope-override-{Guid.NewGuid():N}");
        await permissive.CreateAsync(
            PublicAccessType.None,
            metadata: null,
            new BlobContainerEncryptionScopeOptions
            {
                DefaultEncryptionScope = defaultScope,
                PreventEncryptionScopeOverride = false
            });
        var overridden = otherScopeService.GetBlobContainerClient(permissive.Name).GetBlobClient("override.bin");
        await overridden.UploadAsync(BinaryData.FromString("overridden encryption scope"));
        Assert.Equal("other-scope", (await overridden.GetPropertiesAsync()).Value.EncryptionScope);

        var credential = new StorageSharedKeyCredential(
            SavaWebApplicationFactory.AccountName,
            SavaWebApplicationFactory.AccountKey);
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
        using var transport = new HttpClient(factory.Server.CreateHandler());

        var oldName = $"scope-old-{Guid.NewGuid():N}";
        using (var oldVersion = new HttpRequestMessage(HttpMethod.Put, RawContainerUri(oldName))
        {
            Content = new ByteArrayContent([])
        })
        {
            oldVersion.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
            oldVersion.Headers.TryAddWithoutValidation("x-ms-default-encryption-scope", defaultScope);
            oldVersion.Headers.TryAddWithoutValidation("x-ms-deny-encryption-scope-override", "true");
            using var response = await transport.SendAsync(oldVersion);
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
            using var response = await transport.SendAsync(incomplete);
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
            using var response = await transport.SendAsync(body);
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
        await block.SetMetadataAsync(new Dictionary<string, string> { ["protected"] = "true" });
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
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:HierarchicalNamespaceEnabled"] = "true"
            });
        var client = CreateClient(application);
        var containerName = $"maintenance-{Guid.NewGuid():N}";
        var container = client.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blobService = application.Services.GetRequiredService<BlobService>();
        var metadata = application.Services.GetRequiredService<MetadataStore>();
        var chunkStore = application.Services.GetRequiredService<ChunkStore>();

        var protectedBlob = container.GetBlobClient("active-reader.bin");
        var protectedBytes = RandomNumberGenerator.GetBytes(48 * 1024);
        await protectedBlob.UploadAsync(BinaryData.FromBytes(protectedBytes));
        var protectedRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            protectedBlob.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        var protectedChunkIds = protectedRecord.Content.Chunks
            .Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal))
            .Select(chunk => chunk.Id)
            .ToArray();

        using (chunkStore.Pin(protectedRecord.Content))
        {
            await protectedBlob.DeleteAsync();
            await blobService.RunMaintenanceAsync(CancellationToken.None);
            foreach (var chunkId in protectedChunkIds)
                Assert.Equal(ChunkIntegrityStatus.Verified, await chunkStore.VerifyChunkAsync(chunkId, CancellationToken.None));
        }

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        foreach (var chunkId in protectedChunkIds)
            Assert.Equal(ChunkIntegrityStatus.Missing, await chunkStore.VerifyChunkAsync(chunkId, CancellationToken.None));

        var expiring = container.GetBlobClient("expiring.bin");
        await expiring.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(20 * 1024)));
        var expiringRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            expiring.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        await blobService.SetExpiryAsync(
            expiringRecord,
            DateTimeOffset.UtcNow.AddMilliseconds(100),
            CancellationToken.None);
        await Task.Delay(150);
        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.False((await expiring.ExistsAsync()).Value);

        var uncommitted = container.GetBlockBlobClient("uncommitted.bin");
        var blockId = Convert.ToBase64String("stale-block-0001"u8);
        await uncommitted.StageBlockAsync(blockId, new MemoryStream(RandomNumberGenerator.GetBytes(32 * 1024)));
        var staged = Assert.Single(await blobService.ListStagedBlocksAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            uncommitted.Name,
            CancellationToken.None));
        await metadata.PutStagedBlockAsync(
            staged with { CreatedAt = DateTimeOffset.UtcNow.AddDays(-8) },
            CancellationToken.None);

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.Empty(await blobService.ListStagedBlocksAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            uncommitted.Name,
            CancellationToken.None));
        Assert.All(
            staged.Content.Chunks.Where(chunk => !chunk.Id.EndsWith("/$zero", StringComparison.Ordinal)),
            chunk => Assert.False(File.Exists(ChunkPath(application.DataPath, chunk.Id))));
    }

    [Fact]
    public async Task MaintenanceReclaimsAbandonedStagingAndSurfacesChunkCorruption()
    {
        var blobService = factory.Services.GetRequiredService<BlobService>();
        var staging = Path.Combine(factory.DataPath, "staging");
        var abandonedPath = Path.Combine(staging, $"abandoned-{Guid.NewGuid():N}.tmp");
        var activePath = Path.Combine(staging, $"active-{Guid.NewGuid():N}.tmp");
        var freshPath = Path.Combine(staging, $"fresh-{Guid.NewGuid():N}.tmp");
        await File.WriteAllBytesAsync(abandonedPath, "abandoned"u8.ToArray());
        await File.WriteAllBytesAsync(freshPath, "fresh"u8.ToArray());
        File.SetLastWriteTimeUtc(abandonedPath, DateTime.UtcNow.AddDays(-2));

        await using (var active = new FileStream(
                         activePath,
                         FileMode.CreateNew,
                         FileAccess.ReadWrite,
                         FileShare.None,
                         bufferSize: 4096,
                         FileOptions.Asynchronous))
        {
            await active.WriteAsync("active"u8.ToArray());
            await active.FlushAsync();
            File.SetLastWriteTimeUtc(activePath, DateTime.UtcNow.AddDays(-2));

            await blobService.RunMaintenanceAsync(CancellationToken.None);
            Assert.False(File.Exists(abandonedPath));
            Assert.True(File.Exists(activePath));
            Assert.True(File.Exists(freshPath));
        }

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.False(File.Exists(activePath));
        Assert.True(File.Exists(freshPath));

        var service = CreateClient(factory);
        var containerName = $"integrity-{Guid.NewGuid():N}";
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blob = container.GetBlobClient("corrupt.bin");
        await blob.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(48 * 1024)));
        var record = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            blob.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        var chunk = record.Content.Chunks.First(item => !item.Id.EndsWith("/$zero", StringComparison.Ordinal));
        var chunkPath = ChunkPath(factory.DataPath, chunk.Id);
        await using (var file = new FileStream(
                         chunkPath,
                         FileMode.Open,
                         FileAccess.ReadWrite,
                         FileShare.None,
                         bufferSize: 4096,
                         FileOptions.Asynchronous))
        {
            file.Position = file.Length - 1;
            var value = file.ReadByte();
            Assert.NotEqual(-1, value);
            file.Position--;
            file.WriteByte((byte)(value ^ 0xff));
            file.Flush(flushToDisk: true);
        }

        await blobService.RunMaintenanceAsync(CancellationToken.None);
        using var operatorClient = factory.CreateClient();
        var unavailable = await operatorClient.GetAsync("/health/ready");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode);
        var metrics = await operatorClient.GetStringAsync("/metrics");
        Assert.Contains("mk8_sava_integrity_corrupt_chunks 1", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_storage_physical_chunk_bytes", metrics, StringComparison.Ordinal);
        Assert.Contains("mk8_sava_http_request_duration_seconds_sum", metrics, StringComparison.Ordinal);

        await blob.DeleteAsync();
        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.False(File.Exists(chunkPath));
        var recovered = await operatorClient.GetAsync("/health/ready");
        Assert.Equal(HttpStatusCode.OK, recovered.StatusCode);

        var missing = container.GetBlobClient("missing.bin");
        await missing.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(40 * 1024)));
        var missingRecord = await blobService.GetBlobAsync(
            SavaWebApplicationFactory.AccountName,
            containerName,
            missing.Name,
            versionId: null,
            snapshot: null,
            includeDeleted: false,
            CancellationToken.None);
        var missingChunk = missingRecord.Content.Chunks.First(item => !item.Id.EndsWith("/$zero", StringComparison.Ordinal));
        File.Delete(ChunkPath(factory.DataPath, missingChunk.Id));
        await blobService.RunMaintenanceAsync(CancellationToken.None);
        var missingMetrics = await operatorClient.GetStringAsync("/metrics");
        Assert.Contains("mk8_sava_integrity_missing_chunks 1", missingMetrics, StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await operatorClient.GetAsync("/health/ready")).StatusCode);

        await missing.DeleteAsync();
        await blobService.RunMaintenanceAsync(CancellationToken.None);
        Assert.Equal(HttpStatusCode.OK, (await operatorClient.GetAsync("/health/ready")).StatusCode);

        File.Delete(freshPath);
    }

    [Fact]
    public async Task ChunkMaintenanceUsesIndependentBoundedKeysetPasses()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
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

    [Fact]
    public async Task SmallChunksArePackedDeduplicatedCompactedAndBackupSafeAcrossRestart()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-packed-{Guid.NewGuid():N}");
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-packed-backup-{Guid.NewGuid():N}");
        var configuration = new Dictionary<string, string?>
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
        SavaWebApplicationFactory? restarted = null;
        var initialDisposed = false;
        try
        {
            await initial.InitializeAsync();
            var service = CreateClient(initial);
            var containerName = $"packed-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var firstBytes = RandomNumberGenerator.GetBytes(1024);
            var secondBytes = RandomNumberGenerator.GetBytes(1536);
            var first = container.GetBlobClient("first.bin");
            var duplicate = container.GetBlobClient("duplicate.bin");
            var second = container.GetBlobClient("second.bin");
            await first.UploadAsync(BinaryData.FromBytes(firstBytes));
            await duplicate.UploadAsync(BinaryData.FromBytes(firstBytes));
            await second.UploadAsync(BinaryData.FromBytes(secondBytes));

            var metadata = initial.Services.GetRequiredService<MetadataStore>();
            Assert.Equal(2, metadata.CountPackedChunks());
            Assert.Empty(EnumerateChunkFiles(dataPath));
            Assert.Single(EnumeratePackFiles(dataPath));
            Assert.Equal(firstBytes, (await duplicate.DownloadContentAsync()).Value.Content.ToArray());
            var range = await second.DownloadContentAsync(new BlobDownloadOptions
            {
                Range = new HttpRange(123, 456)
            });
            Assert.Equal(secondBytes.AsSpan(123, 456).ToArray(), range.Value.Content.ToArray());

            await initial.DisposeAsync();
            initialDisposed = true;

            restarted = new SavaWebApplicationFactory(dataPath, configuration, deleteDataPath: false);
            await restarted.InitializeAsync();
            var restartedContainer = CreateClient(restarted).GetBlobContainerClient(containerName);
            first = restartedContainer.GetBlobClient(first.Name);
            duplicate = restartedContainer.GetBlobClient(duplicate.Name);
            second = restartedContainer.GetBlobClient(second.Name);
            Assert.Equal(secondBytes, (await second.DownloadContentAsync()).Value.Content.ToArray());

            await first.DeleteAsync();
            await duplicate.DeleteAsync();
            var blobs = restarted.Services.GetRequiredService<BlobService>();
            var maintenance = await blobs.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal(1, maintenance.ReclaimedChunks);
            Assert.Equal(1, maintenance.CompactedChunkPacks);
            Assert.True(maintenance.PackCompactionBytesSaved > 0);
            metadata = restarted.Services.GetRequiredService<MetadataStore>();
            Assert.Equal(1, metadata.CountPackedChunks());
            Assert.Empty(EnumerateChunkFiles(dataPath));
            Assert.Single(EnumeratePackFiles(dataPath));
            Assert.Equal(secondBytes, (await second.DownloadContentAsync()).Value.Content.ToArray());

            var orphanPath = Path.Combine(
                dataPath,
                "packs",
                SavaWebApplicationFactory.AccountName,
                $"orphan-{Guid.NewGuid():N}.pack");
            await File.WriteAllBytesAsync(orphanPath, RandomNumberGenerator.GetBytes(257));
            File.SetLastWriteTimeUtc(orphanPath, DateTime.UtcNow.AddMinutes(-1));
            var recovery = await blobs.RunMaintenanceAsync(CancellationToken.None);
            Assert.Equal(1, recovery.CompactedChunkPacks);
            Assert.True(recovery.PackCompactionBytesSaved >= 257);
            Assert.False(File.Exists(orphanPath));

            var backup = restarted.Services.GetRequiredService<StorageBackupService>();
            var created = await backup.CreateAsync(backupPath, CancellationToken.None);
            Assert.Equal(1, created.ChunkCount);
            Assert.Equal(created, await backup.ValidateAsync(backupPath, CancellationToken.None));
            Assert.Single(EnumerateChunkFiles(backupPath));
            Assert.False(Directory.Exists(Path.Combine(backupPath, "packs")));
            await using (var connection = new SqliteConnection(
                             $"Data Source={Path.Combine(backupPath, "metadata.db")}"))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    "SELECT (SELECT COUNT(*) FROM packed_chunks) + (SELECT COUNT(*) FROM chunk_packs);";
                Assert.Equal(0L, Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture));
            }

            var packPath = Assert.Single(EnumeratePackFiles(dataPath)).FullName;
            await using (var corrupt = new FileStream(
                             packPath,
                             FileMode.Open,
                             FileAccess.ReadWrite,
                             FileShare.None,
                             4096,
                             FileOptions.Asynchronous))
            {
                var value = corrupt.ReadByte();
                Assert.NotEqual(-1, value);
                corrupt.Position = 0;
                corrupt.WriteByte((byte)(value ^ 0xff));
                corrupt.Flush(flushToDisk: true);
            }
            var record = await blobs.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                second.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: false,
                CancellationToken.None);
            var chunkId = Assert.Single(record.Content.Chunks).Id;
            Assert.Equal(
                ChunkIntegrityStatus.Corrupt,
                await restarted.Services.GetRequiredService<ChunkStore>()
                    .VerifyChunkAsync(chunkId, CancellationToken.None));
        }
        finally
        {
            if (restarted is not null)
                await restarted.DisposeAsync();
            if (!initialDisposed)
                await initial.DisposeAsync();
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
        }
    }

    [Fact]
    public async Task LifecycleMaintenanceUsesBoundedMetadataPages()
    {
        var application = new SavaWebApplicationFactory(new Dictionary<string, string?>
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
            for (var index = 0; index < 3; index++)
            {
                var blob = container.GetBlobClient($"expiring-{index}.bin");
                await blob.UploadAsync(BinaryData.FromBytes(RandomNumberGenerator.GetBytes(2048)));
                var record = await blobService.GetBlobAsync(
                    SavaWebApplicationFactory.AccountName,
                    containerName,
                    blob.Name,
                    versionId: null,
                    snapshot: null,
                    includeDeleted: false,
                    CancellationToken.None);
                await blobService.SetExpiryAsync(
                    record,
                    DateTimeOffset.UtcNow.AddMilliseconds(100),
                    CancellationToken.None);
            }

            var uncommitted = container.GetBlockBlobClient("uncommitted.bin");
            for (var index = 0; index < 3; index++)
            {
                var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes($"bounded-block-{index:D2}"));
                await uncommitted.StageBlockAsync(blockId, new MemoryStream(RandomNumberGenerator.GetBytes(2048)));
            }
            foreach (var block in await blobService.ListStagedBlocksAsync(
                         SavaWebApplicationFactory.AccountName,
                         containerName,
                         uncommitted.Name,
                         CancellationToken.None))
            {
                await metadata.PutStagedBlockAsync(
                    block with { CreatedAt = DateTimeOffset.UtcNow.AddDays(-8) },
                    CancellationToken.None);
            }

            await Task.Delay(150);
            for (var expectedRemaining = 2; expectedRemaining >= 0; expectedRemaining--)
            {
                var result = await blobService.RunMaintenanceAsync(CancellationToken.None);
                Assert.Equal(1, result.ExpiredBlobs);
                Assert.Equal(1, result.ExpiredUncommittedBlocks);
                Assert.Equal(
                    expectedRemaining,
                    (await metadata.ListBlobsAsync(
                        SavaWebApplicationFactory.AccountName,
                        containerName,
                        includeVersions: false,
                        includeSnapshots: false,
                        includeDeleted: false,
                        CancellationToken.None)).Count);
                Assert.Equal(
                    expectedRemaining,
                    (await blobService.ListStagedBlocksAsync(
                        SavaWebApplicationFactory.AccountName,
                        containerName,
                        uncommitted.Name,
                        CancellationToken.None)).Count);
            }

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
            var deletedContainerNames = Enumerable.Range(0, 3)
                .Select(index => $"bounded-purge-{Guid.NewGuid():N}-{index}")
                .ToArray();
            foreach (var name in deletedContainerNames)
            {
                var client = service.GetBlobContainerClient(name);
                await client.CreateAsync();
                var record = await metadata.GetContainerAsync(
                    SavaWebApplicationFactory.AccountName,
                    name,
                    includeDeleted: false,
                    CancellationToken.None);
                Assert.NotNull(record);
                await blobService.DeleteContainerAsync(record!, CancellationToken.None);
                var deleted = await metadata.GetContainerAsync(
                    SavaWebApplicationFactory.AccountName,
                    name,
                    includeDeleted: true,
                    CancellationToken.None);
                Assert.NotNull(deleted);
                await metadata.PutContainerAsync(
                    deleted! with
                    {
                        Revision = MetadataStore.NewRevision(),
                        DeleteRetentionUntil = DateTimeOffset.UtcNow.AddMinutes(-1)
                    },
                    deleted.Revision,
                    CancellationToken.None);
            }

            var purgedContainers = 0;
            for (var pass = 0; pass < 8 && purgedContainers < deletedContainerNames.Length; pass++)
            {
                var result = await blobService.RunMaintenanceAsync(CancellationToken.None);
                Assert.InRange(result.PurgedSoftDeletedContainers, 0, 1);
                purgedContainers += result.PurgedSoftDeletedContainers;
            }
            Assert.Equal(deletedContainerNames.Length, purgedContainers);
            foreach (var name in deletedContainerNames)
            {
                Assert.Null(await metadata.GetContainerAsync(
                    SavaWebApplicationFactory.AccountName,
                    name,
                    includeDeleted: true,
                    CancellationToken.None));
            }
        }
        finally
        {
            await application.DisposeAsync();
        }
    }

    [Fact]
    public async Task ConsistentBackupRestoresExactSharedSnapshotAndUncommittedContent()
    {
        var source = new SavaWebApplicationFactory();
        SavaWebApplicationFactory? restored = null;
        var backupPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-backup-{Guid.NewGuid():N}");
        var restoredPath = Path.Combine(Path.GetTempPath(), $"mk8-sava-restored-{Guid.NewGuid():N}");
        try
        {
            await source.InitializeAsync();
            var service = CreateClient(source);
            var containerName = $"backup-{Guid.NewGuid():N}";
            var container = service.GetBlobContainerClient(containerName);
            await container.CreateAsync();
            var sharedBytes = Enumerable.Range(0, 96 * 1024).Select(index => (byte)(index % 251)).ToArray();
            var first = container.GetBlobClient("first.bin");
            var second = container.GetBlobClient("second.bin");
            await first.UploadAsync(BinaryData.FromBytes(sharedBytes));
            await second.UploadAsync(BinaryData.FromBytes(sharedBytes));
            var snapshot = (await first.CreateSnapshotAsync()).Value.Snapshot;

            var uncommitted = container.GetBlockBlobClient("uncommitted.bin");
            var blockId = Convert.ToBase64String("backup-block-0001"u8);
            var uncommittedBytes = RandomNumberGenerator.GetBytes(24 * 1024);
            await uncommitted.StageBlockAsync(blockId, new MemoryStream(uncommittedBytes));

            var customerKey = RandomNumberGenerator.GetBytes(32);
            var encrypted = CreateEncryptedClient(source, new CustomerProvidedKey(customerKey), encryptionScope: null)
                .GetBlobContainerClient(containerName)
                .GetBlobClient("customer-key.bin");
            var encryptedBytes = RandomNumberGenerator.GetBytes(20 * 1024);
            await encrypted.UploadAsync(BinaryData.FromBytes(encryptedBytes));

            var backup = source.Services.GetRequiredService<StorageBackupService>();
            var created = await backup.CreateAsync(backupPath, CancellationToken.None);
            Assert.True(created.BlobRecordCount >= 4);
            Assert.True(created.ChunkCount > 0);
            Assert.Equal(created, await backup.ValidateAsync(backupPath, CancellationToken.None));

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
                backupPath,
                wrongKeyOptions,
                CancellationToken.None));
            var existingTarget = Path.Combine(Path.GetTempPath(), $"mk8-sava-existing-{Guid.NewGuid():N}");
            Directory.CreateDirectory(existingTarget);
            try
            {
                await Assert.ThrowsAsync<IOException>(() => StorageBackupService.RestoreAsync(
                    backupPath,
                    existingTarget,
                    options,
                    CancellationToken.None));
            }
            finally
            {
                Directory.Delete(existingTarget);
            }
            var restoredBackup = await StorageBackupService.RestoreAsync(
                backupPath,
                restoredPath,
                options,
                CancellationToken.None);
            Assert.Equal(created.ChunkCount, restoredBackup.ChunkCount);
            Assert.Equal(created.ChunkCount, EnumerateChunkFiles(restoredPath).Count());
            Assert.Empty(Directory.EnumerateFiles(Path.Combine(restoredPath, "staging")));
            Assert.Equal(
                ["metadata.db"],
                Directory.EnumerateFiles(restoredPath).Select(Path.GetFileName).Order(StringComparer.Ordinal));

            restored = new SavaWebApplicationFactory(restoredPath);
            await restored.InitializeAsync();
            var restoredContainer = CreateClient(restored).GetBlobContainerClient(containerName);
            Assert.Equal(sharedBytes, (await restoredContainer.GetBlobClient(first.Name).DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(sharedBytes, (await restoredContainer.GetBlobClient(second.Name).DownloadContentAsync()).Value.Content.ToArray());
            Assert.Equal(
                sharedBytes,
                (await restoredContainer.GetBlobClient(first.Name).WithSnapshot(snapshot).DownloadContentAsync()).Value.Content.ToArray());

            var restoredBlocks = await restoredContainer.GetBlockBlobClient(uncommitted.Name)
                .GetBlockListAsync(BlockListTypes.Uncommitted);
            var restoredBlock = Assert.Single(restoredBlocks.Value.UncommittedBlocks);
            Assert.Equal(blockId, restoredBlock.Name);
            Assert.Equal(uncommittedBytes.Length, restoredBlock.SizeLong);
            await restoredContainer.GetBlockBlobClient(uncommitted.Name).CommitBlockListAsync([blockId]);
            Assert.Equal(
                uncommittedBytes,
                (await restoredContainer.GetBlobClient(uncommitted.Name).DownloadContentAsync()).Value.Content.ToArray());
            var restoredEncrypted = CreateEncryptedClient(
                    restored,
                    new CustomerProvidedKey(customerKey),
                    encryptionScope: null)
                .GetBlobContainerClient(containerName)
                .GetBlobClient(encrypted.Name);
            Assert.Equal(encryptedBytes, (await restoredEncrypted.DownloadContentAsync()).Value.Content.ToArray());

            var backedUpChunk = Directory.EnumerateFiles(
                Path.Combine(backupPath, "chunks"),
                "*.chunk",
                SearchOption.AllDirectories).First();
            await using (var corrupt = new FileStream(
                             backedUpChunk,
                             FileMode.Open,
                             FileAccess.ReadWrite,
                             FileShare.None,
                             4096,
                             FileOptions.Asynchronous))
            {
                corrupt.Position = corrupt.Length - 1;
                var value = corrupt.ReadByte();
                Assert.NotEqual(-1, value);
                corrupt.Position--;
                corrupt.WriteByte((byte)(value ^ 0xff));
                corrupt.Flush(flushToDisk: true);
            }
            await Assert.ThrowsAsync<InvalidDataException>(() =>
                backup.ValidateAsync(backupPath, CancellationToken.None));
        }
        finally
        {
            if (restored is not null)
                await restored.DisposeAsync();
            await source.DisposeAsync();
            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, recursive: true);
            if (Directory.Exists(restoredPath))
                Directory.Delete(restoredPath, recursive: true);
        }
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
            var configuredOptions = application.Services
                .GetRequiredService<Microsoft.Extensions.Options.IOptions<Mk8.Sava.Configuration.SavaOptions>>()
                .Value;
            var legacy = await StorageBackupService.ValidateBackupAsync(
                legacyBackupPath,
                configuredOptions,
                CancellationToken.None);
            Assert.Equal(1, legacy.BlobRecordCount);
            Assert.Equal(1, legacy.StagedBlockCount);
            var inventory = await metadata.GetStorageInventoryAsync(CancellationToken.None);
            Assert.Equal(1, inventory.BlobRecordCount);
            Assert.Equal(1, inventory.StagedBlockCount);
            Assert.Equal(1024, inventory.LogicalBlobBytes);
            Assert.Equal(512, inventory.LogicalStagedBlockBytes);
            Assert.Equal(
                new HashSet<string>([SavaWebApplicationFactory.AccountName + "/$zero"], StringComparer.Ordinal),
                inventory.ReachableChunkIds);

            var container = CreateClient(application).GetBlobContainerClient(containerName);
            Assert.Equal(
                new byte[1024],
                (await container.GetPageBlobClient("sparse.bin").DownloadContentAsync()).Value.Content.ToArray());
            var blocks = await container.GetBlockBlobClient("staged.bin").GetBlockListAsync(BlockListTypes.Uncommitted);
            var staged = Assert.Single(blocks.Value.UncommittedBlocks);
            Assert.Equal(Convert.ToBase64String("migrated-block"u8), staged.Name);
            Assert.Equal(512, staged.SizeLong);

            await using (var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}"))
            {
                await connection.OpenAsync();
                await using var version = connection.CreateCommand();
                version.CommandText = "PRAGMA user_version;";
                Assert.Equal(5L, Convert.ToInt64(await version.ExecuteScalarAsync(), CultureInfo.InvariantCulture));
                await using var references = connection.CreateCommand();
                references.CommandText = """
                    SELECT
                        (SELECT COUNT(*) FROM blob_chunk_references) +
                        (SELECT COUNT(*) FROM staged_block_chunk_references);
                    """;
                Assert.Equal(2L, Convert.ToInt64(await references.ExecuteScalarAsync(), CultureInfo.InvariantCulture));
                await using var tags = connection.CreateCommand();
                tags.CommandText = "SELECT COUNT(*) FROM blob_tags;";
                Assert.Equal(1L, Convert.ToInt64(await tags.ExecuteScalarAsync(), CultureInfo.InvariantCulture));
            }

            var backup = application.Services.GetRequiredService<StorageBackupService>();
            var created = await backup.CreateAsync(backupPath, CancellationToken.None);
            Assert.Equal(1, created.BlobRecordCount);
            Assert.Equal(1, created.StagedBlockCount);
            Assert.Equal(created, await backup.ValidateAsync(backupPath, CancellationToken.None));

            await using (var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}"))
            {
                await connection.OpenAsync();
                await using var corruptIndex = connection.CreateCommand();
                corruptIndex.CommandText = """
                    DELETE FROM blob_chunk_references;
                    DELETE FROM staged_block_chunk_references;
                    """;
                await corruptIndex.ExecuteNonQueryAsync();
            }
            var mismatch = await Assert.ThrowsAsync<InvalidDataException>(() =>
                backup.CreateAsync(rejectedBackupPath, CancellationToken.None));
            Assert.Contains("chunk-reference index", mismatch.Message, StringComparison.Ordinal);
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
            var enabled = (await client.GetPropertiesAsync()).Value;
            enabled.DeleteRetentionPolicy.Enabled = true;
            enabled.DeleteRetentionPolicy.Days = 1;
            await client.SetPropertiesAsync(enabled);
            await blob.DeleteAsync();

            var deleted = await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                blob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: true,
                CancellationToken.None);
            Assert.NotNull(deleted?.DeleteRetentionUntil);
            Assert.InRange(
                deleted!.DeleteRetentionUntil!.Value - deleted.DeletedAt!.Value,
                TimeSpan.FromHours(23.9),
                TimeSpan.FromHours(24.1));

            var disabled = (await client.GetPropertiesAsync()).Value;
            disabled.DeleteRetentionPolicy.Enabled = false;
            await client.SetPropertiesAsync(disabled);
            await blob.UndeleteAsync();
            Assert.True((await blob.ExistsAsync()).Value);

            enabled = (await client.GetPropertiesAsync()).Value;
            enabled.DeleteRetentionPolicy.Enabled = true;
            enabled.DeleteRetentionPolicy.Days = 1;
            await client.SetPropertiesAsync(enabled);
            await blob.DeleteAsync();
            deleted = await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                blob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: true,
                CancellationToken.None);
            Assert.NotNull(deleted);
            await metadata.PutBlobRecordAsync(
                deleted! with
                {
                    Revision = MetadataStore.NewRevision(),
                    DeleteRetentionUntil = DateTimeOffset.UtcNow.AddMinutes(-1)
                },
                deleted.Revision,
                CancellationToken.None);

            await blobService.RunMaintenanceAsync(CancellationToken.None);
            Assert.Null(await metadata.GetBlobAsync(
                SavaWebApplicationFactory.AccountName,
                containerName,
                blob.Name,
                versionId: null,
                snapshot: null,
                includeDeleted: true,
                CancellationToken.None));
        }
        finally
        {
            await client.SetPropertiesAsync(original);
        }
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
            var configured = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                configured with { VersioningEnabled = false },
                CancellationToken.None);

            var overwritten = container.GetBlockBlobClient("overwritten.txt");
            await overwritten.UploadAsync(new MemoryStream("before overwrite"u8.ToArray()));
            await overwritten.UploadAsync(new MemoryStream("after overwrite"u8.ToArray()));
            Assert.Equal("after overwrite", (await overwritten.DownloadContentAsync()).Value.Content.ToString());

            var deletedSnapshots = new List<BlobItem>();
            await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
            {
                States = BlobStates.Deleted | BlobStates.Snapshots,
                Prefix = overwritten.Name
            }))
            {
                if (item.Deleted && item.Snapshot is not null)
                    deletedSnapshots.Add(item);
            }
            var overwrittenSnapshot = Assert.Single(deletedSnapshots);

            await overwritten.UndeleteAsync();
            var restoredSnapshot = overwritten.WithSnapshot(overwrittenSnapshot.Snapshot);
            Assert.Equal("before overwrite", (await restoredSnapshot.DownloadContentAsync()).Value.Content.ToString());
            Assert.Equal("after overwrite", (await overwritten.DownloadContentAsync()).Value.Content.ToString());

            var recreated = container.GetBlockBlobClient("recreated.txt");
            await recreated.UploadAsync(new MemoryStream("soft-deleted original"u8.ToArray()));
            await recreated.DeleteAsync();
            await recreated.UploadAsync(new MemoryStream("replacement"u8.ToArray()));
            Assert.Equal("replacement", (await recreated.DownloadContentAsync()).Value.Content.ToString());
            await recreated.UndeleteAsync();

            BlobItem? recreatedSnapshot = null;
            await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
            {
                States = BlobStates.Snapshots,
                Prefix = recreated.Name
            }))
            {
                if (item.Name == recreated.Name && item.Snapshot is not null)
                    recreatedSnapshot = item;
            }
            Assert.NotNull(recreatedSnapshot);
            Assert.Equal(
                "soft-deleted original",
                (await recreated.WithSnapshot(recreatedSnapshot!.Snapshot).DownloadContentAsync()).Value.Content.ToString());

            var typeChangedName = "type-changed";
            var typeChangedAppend = container.GetAppendBlobClient(typeChangedName);
            await typeChangedAppend.CreateAsync();
            await typeChangedAppend.AppendBlockAsync(new MemoryStream("append state"u8.ToArray()));
            await typeChangedAppend.DeleteAsync();
            var typeChangedBlock = container.GetBlockBlobClient(typeChangedName);
            await typeChangedBlock.UploadAsync(new MemoryStream("block replacement"u8.ToArray()));
            var retainedDifferentType = new List<BlobItem>();
            await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
            {
                States = BlobStates.Deleted | BlobStates.Snapshots,
                Prefix = typeChangedName
            }))
            {
                if (item.Name == typeChangedName && item.Deleted)
                    retainedDifferentType.Add(item);
            }
            Assert.Empty(retainedDifferentType);

            configured = await metadata.GetServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                CancellationToken.None);
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                configured with { VersioningEnabled = true },
                CancellationToken.None);
            var versioned = container.GetBlockBlobClient("versioned.txt");
            await versioned.UploadAsync(new MemoryStream("version one"u8.ToArray()));
            await versioned.UploadAsync(new MemoryStream("version two"u8.ToArray()));
            await versioned.SetMetadataAsync(new Dictionary<string, string> { ["revision"] = "metadata-write" });
            var versionedSnapshot = await versioned.CreateSnapshotAsync(
                new Dictionary<string, string> { ["snapshot"] = "override" });
            Assert.False(string.IsNullOrEmpty(versionedSnapshot.Value.VersionId));
            var snapshotProperties = await versioned.WithSnapshot(versionedSnapshot.Value.Snapshot).GetPropertiesAsync();
            Assert.Equal("override", snapshotProperties.Value.Metadata["snapshot"]);
            await versioned.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots);
            Assert.False((await versioned.ExistsAsync()).Value);

            var versions = new List<BlobItem>();
            await foreach (var item in container.GetBlobsAsync(new GetBlobsOptions
            {
                States = BlobStates.Version,
                Prefix = versioned.Name
            }))
            {
                if (item.Name == versioned.Name && item.VersionId is not null)
                    versions.Add(item);
            }
            Assert.Equal(4, versions.Count);
            Assert.All(versions, item => Assert.False(item.Deleted));
            Assert.All(versions, item => Assert.False(item.IsLatestVersion));
            var contents = new HashSet<string>(StringComparer.Ordinal);
            foreach (var version in versions)
            {
                contents.Add((await versioned.WithVersion(version.VersionId).DownloadContentAsync()).Value.Content.ToString());
            }
            Assert.Equal(new HashSet<string>(["version one", "version two"], StringComparer.Ordinal), contents);
        }
        finally
        {
            await metadata.PutServicePropertiesAsync(
                SavaWebApplicationFactory.AccountName,
                originalProperties,
                CancellationToken.None);
        }
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
                    await blobService.CollectGarbageAsync(stop.Token);
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
            await sweeper;
        }

        foreach (var index in Enumerable.Range(0, 16))
        {
            var downloaded = await container.GetBlobClient($"published-{index:D2}.bin").DownloadContentAsync();
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
        var valid = container.GetBlobClient("valid.bin");
        await valid.UploadAsync(new MemoryStream(content), new BlobUploadOptions
        {
            TransferValidation = new UploadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        });
        Assert.Equal(content, (await valid.DownloadContentAsync()).Value.Content.ToArray());
        var validatedDownload = await valid.DownloadContentAsync(new BlobDownloadOptions
        {
            TransferValidation = new DownloadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        });
        Assert.Equal(content, validatedDownload.Value.Content.ToArray());
        var validatedRange = await valid.DownloadContentAsync(new BlobDownloadOptions
        {
            Range = new HttpRange(4 * 1024 * 1024 - 127, 384),
            TransferValidation = new DownloadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        });
        Assert.Equal(content.AsSpan(4 * 1024 * 1024 - 127, 384).ToArray(), validatedRange.Value.Content.ToArray());

        var empty = container.GetBlobClient("empty.bin");
        await empty.UploadAsync(new MemoryStream([]), new BlobUploadOptions
        {
            TransferValidation = new UploadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        });
        var validatedEmptyDownload = await empty.DownloadContentAsync(new BlobDownloadOptions
        {
            TransferValidation = new DownloadTransferValidationOptions
            {
                ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
            }
        });
        Assert.Empty(validatedEmptyDownload.Value.Content.ToArray());

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
            });
        await blockBlob.CommitBlockListAsync([blockId]);
        Assert.Equal(stagedContent, (await blockBlob.DownloadContentAsync()).Value.Content.ToArray());

        var appendContent = "structured append block"u8.ToArray();
        var appendBlob = container.GetAppendBlobClient("append.bin");
        await appendBlob.CreateAsync();
        await appendBlob.AppendBlockAsync(
            new MemoryStream(appendContent),
            new AppendBlobAppendBlockOptions
            {
                TransferValidation = new UploadTransferValidationOptions
                {
                    ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
                }
            });
        Assert.Equal(appendContent, (await appendBlob.DownloadContentAsync()).Value.Content.ToArray());

        var pageContent = Enumerable.Range(0, 512).Select(index => (byte)(index % 251)).ToArray();
        var pageBlob = container.GetPageBlobClient("page.bin");
        await pageBlob.CreateAsync(pageContent.Length);
        await pageBlob.UploadPagesAsync(
            new MemoryStream(pageContent),
            0,
            new PageBlobUploadPagesOptions
            {
                TransferValidation = new UploadTransferValidationOptions
                {
                    ChecksumAlgorithm = StorageChecksumAlgorithm.StorageCrc64
                }
            });
        Assert.Equal(pageContent, (await pageBlob.DownloadContentAsync()).Value.Content.ToArray());

        var invalid = container.GetBlobClient("invalid.bin");
        var invalidContent = "structured checksum mismatch"u8.ToArray();
        var invalidStructuredBody = EncodeStructuredBody(invalidContent);
        invalidStructuredBody[^1] ^= 0x01;
        using var transport = new HttpClient(factory.Server.CreateHandler());
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
            using var structuredResponse = await transport.SendAsync(structuredRequest);
            Assert.Equal(HttpStatusCode.BadRequest, structuredResponse.StatusCode);
            Assert.Equal("Crc64Mismatch", structuredResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await invalid.ExistsAsync()).Value);

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
            using var transactionalResponse = await transport.SendAsync(transactionalRequest);
            Assert.Equal(HttpStatusCode.BadRequest, transactionalResponse.StatusCode);
            Assert.Equal("Crc64Mismatch", transactionalResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await invalidTransactional.ExistsAsync()).Value);

        const int rawRangeStart = 731;
        const int rawRangeLength = 2048;
        using var rangeRequest = new HttpRequestMessage(
            HttpMethod.Get,
            valid.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(5)));
        rangeRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
        rangeRequest.Headers.TryAddWithoutValidation("x-ms-range", $"bytes={rawRangeStart}-{rawRangeStart + rawRangeLength - 1}");
        rangeRequest.Headers.TryAddWithoutValidation("x-ms-range-get-content-crc64", "true");
        using var rangeResponse = await transport.SendAsync(rangeRequest);
        Assert.Equal(HttpStatusCode.PartialContent, rangeResponse.StatusCode);
        var rangeBytes = await rangeResponse.Content.ReadAsByteArrayAsync();
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
        var blockBlob = container.GetBlockBlobClient("committed.bin");
        var blockId = Convert.ToBase64String("control-block-0001"u8);
        var blockContent = "validated block-list payload"u8.ToArray();
        await blockBlob.StageBlockAsync(blockId, new MemoryStream(blockContent));

        var blockList = Encoding.UTF8.GetBytes(
            $"<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList><Latest>{blockId}</Latest></BlockList>");
        var crc64 = new Mk8.Sava.Protocol.StorageCrc64();
        crc64.Append(blockList);
        var expectedCrc64 = Convert.ToBase64String(crc64.GetHash());
        var blockListUri = new Uri(
            blockBlob.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)) + "&comp=blocklist");
        using var transport = new HttpClient(factory.Server.CreateHandler());

        using (var corruptRequest = new HttpRequestMessage(HttpMethod.Put, blockListUri)
        {
            Content = new ByteArrayContent(blockList)
        })
        {
            corruptRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            corruptRequest.Headers.TryAddWithoutValidation("x-ms-content-crc64", Convert.ToBase64String(new byte[8]));
            using var corruptResponse = await transport.SendAsync(corruptRequest);
            Assert.Equal(HttpStatusCode.BadRequest, corruptResponse.StatusCode);
            Assert.Equal("Crc64Mismatch", corruptResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await blockBlob.ExistsAsync()).Value);

        using (var validRequest = new HttpRequestMessage(HttpMethod.Put, blockListUri)
        {
            Content = new ByteArrayContent(blockList)
        })
        {
            validRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-06-06");
            validRequest.Headers.TryAddWithoutValidation("x-ms-content-crc64", expectedCrc64);
            using var validResponse = await transport.SendAsync(validRequest);
            Assert.Equal(HttpStatusCode.Created, validResponse.StatusCode);
            Assert.Equal(expectedCrc64, validResponse.Headers.GetValues("x-ms-content-crc64").Single());
        }
        Assert.Equal(blockContent, (await blockBlob.DownloadContentAsync()).Value.Content.ToArray());

        var oldCrcBlob = container.GetBlobClient("old-crc.bin");
        var oldCrcContent = "version-gated-crc"u8.ToArray();
        crc64 = new Mk8.Sava.Protocol.StorageCrc64();
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
            using var oldCrcResponse = await transport.SendAsync(oldCrcRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldCrcResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldCrcResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldCrcBlob.ExistsAsync()).Value);

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
            using var oldStructuredResponse = await transport.SendAsync(oldStructuredRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldStructuredResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldStructuredResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldStructuredBlob.ExistsAsync()).Value);

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
            using var oldCreatePermissionResponse = await transport.SendAsync(oldCreatePermissionRequest);
            Assert.Equal(HttpStatusCode.Forbidden, oldCreatePermissionResponse.StatusCode);
        }

        using (var createPermissionRequest = new HttpRequestMessage(HttpMethod.Put, createOnlyBlockUri)
        {
            Content = new ByteArrayContent("create-only-content"u8.ToArray())
        })
        {
            createPermissionRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-04-06");
            using var createPermissionResponse = await transport.SendAsync(createPermissionRequest);
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
            using var createPermissionCommitResponse = await transport.SendAsync(createPermissionCommitRequest);
            Assert.Equal(HttpStatusCode.Created, createPermissionCommitResponse.StatusCode);
        }

        using (var existingBlobCreateRequest = new HttpRequestMessage(HttpMethod.Put, createOnlyBlockUri)
        {
            Content = new ByteArrayContent("replacement"u8.ToArray())
        })
        {
            existingBlobCreateRequest.Headers.TryAddWithoutValidation("x-ms-version", "2026-04-06");
            using var existingBlobCreateResponse = await transport.SendAsync(existingBlobCreateRequest);
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
            using var boundaryBlockResponse = await transport.SendAsync(boundaryBlockRequest);
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
            using var oversizedBlockResponse = await transport.SendAsync(oversizedBlockRequest);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedBlockResponse.StatusCode);
            Assert.Equal("RequestBodyTooLarge", oversizedBlockResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        var staged = await blockBlob.GetBlockListAsync(BlockListTypes.Uncommitted);
        Assert.Equal([firstBlockId], staged.Value.UncommittedBlocks.Select(block => block.Name));

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
            using var oversizedPutResponse = await transport.SendAsync(oversizedPutRequest);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedPutResponse.StatusCode);
        }
        Assert.False((await oversizedPutBlob.ExistsAsync()).Value);

        var appendBlob = container.GetAppendBlobClient("append.bin");
        await appendBlob.CreateAsync();
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
            using var boundaryAppendResponse = await transport.SendAsync(boundaryAppendRequest);
            Assert.Equal(HttpStatusCode.Created, boundaryAppendResponse.StatusCode);
        }
        using (var oversizedAppendRequest = new HttpRequestMessage(HttpMethod.Put, appendUri)
        {
            Content = new DeclaredLengthContent(4L * mebibyte + 1)
        })
        {
            oversizedAppendRequest.Headers.TryAddWithoutValidation("x-ms-version", "2021-12-02");
            using var oversizedAppendResponse = await transport.SendAsync(oversizedAppendRequest);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedAppendResponse.StatusCode);
        }
        Assert.Equal(4L * mebibyte, (await appendBlob.GetPropertiesAsync()).Value.ContentLength);

        var pageBlob = container.GetPageBlobClient("pages.bin");
        await pageBlob.CreateAsync(8L * mebibyte);
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
            using var boundaryPageResponse = await transport.SendAsync(boundaryPageRequest);
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
            using var oversizedPageResponse = await transport.SendAsync(oversizedPageRequest);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedPageResponse.StatusCode);
        }
        var pageRanges = await pageBlob.GetPageRangesAsync();
        Assert.Equal([new HttpRange(0, 4L * mebibyte)], pageRanges.Value.PageRanges);

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
            using var oversizedPageCreateResponse = await transport.SendAsync(oversizedPageCreateRequest);
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversizedPageCreateResponse.StatusCode);
        }
        Assert.False((await oversizedPageBlob.ExistsAsync()).Value);

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
            using var negativeSequenceResponse = await transport.SendAsync(negativeSequenceRequest);
            Assert.Equal(HttpStatusCode.BadRequest, negativeSequenceResponse.StatusCode);
        }
        Assert.False((await negativeSequenceBlob.ExistsAsync()).Value);

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
            using var oldAppendCreateResponse = await transport.SendAsync(oldAppendCreateRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldAppendCreateResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldAppendCreateResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldAppendBlob.ExistsAsync()).Value);

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
            using var oldBlockFromUrlResponse = await transport.SendAsync(oldBlockFromUrlRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldBlockFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldBlockFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        var supportedBlockFromUrlUri = new Uri(
            oldUrlBlock.GenerateSasUri(
                BlobSasPermissions.Create | BlobSasPermissions.Write,
                DateTimeOffset.UtcNow.AddMinutes(5)) +
            "&comp=block&blockid=" + Uri.EscapeDataString(oldUrlBlockId));
        using (var oldSourceCrcRequest = new HttpRequestMessage(HttpMethod.Put, supportedBlockFromUrlUri))
        {
            oldSourceCrcRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-11-09");
            oldSourceCrcRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldSourceCrcRequest.Headers.TryAddWithoutValidation("x-ms-source-content-crc64", Convert.ToBase64String(new byte[8]));
            using var oldSourceCrcResponse = await transport.SendAsync(oldSourceCrcRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldSourceCrcResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceCrcResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldSourceAuthorizationRequest = new HttpRequestMessage(HttpMethod.Put, supportedBlockFromUrlUri))
        {
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-version", "2020-04-08");
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-copy-source-authorization", "Bearer opaque-token");
            using var oldSourceAuthorizationResponse = await transport.SendAsync(oldSourceAuthorizationRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldSourceAuthorizationResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceAuthorizationResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldSourceTagConditionRequest = new HttpRequestMessage(HttpMethod.Put, supportedBlockFromUrlUri))
        {
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-source-if-tags", "\"project\" = 'mk8'");
            using var oldSourceTagConditionResponse = await transport.SendAsync(oldSourceTagConditionRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldSourceTagConditionResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceTagConditionResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var oldAppendFromUrlRequest = new HttpRequestMessage(HttpMethod.Put, appendUri))
        {
            oldAppendFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-03-28");
            oldAppendFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            using var oldAppendFromUrlResponse = await transport.SendAsync(oldAppendFromUrlRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldAppendFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldAppendFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var oldPageFromUrlRequest = new HttpRequestMessage(HttpMethod.Put, pageUri))
        {
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-03-28");
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
            using var oldPageFromUrlResponse = await transport.SendAsync(oldPageFromUrlRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldPageFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldPageFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }

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
            using var oldPutBlobFromUrlResponse = await transport.SendAsync(oldPutBlobFromUrlRequest);
            Assert.Equal(HttpStatusCode.Conflict, oldPutBlobFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldPutBlobFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldPutBlobFromUrl.ExistsAsync()).Value);
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

        var appendSas = append.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5));
        using (var oldRead = new HttpRequestMessage(HttpMethod.Head, appendSas))
        {
            oldRead.Headers.TryAddWithoutValidation("x-ms-version", "2014-02-14");
            using var response = await transport.SendAsync(oldRead);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldAppend = new HttpRequestMessage(HttpMethod.Put, AppendQuery(appendSas, "comp=appendblock"))
        {
            Content = new ByteArrayContent("must not append"u8.ToArray())
        })
        {
            oldAppend.Headers.TryAddWithoutValidation("x-ms-version", "2014-02-14");
            using var response = await transport.SendAsync(oldAppend);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldOverwrite = new HttpRequestMessage(HttpMethod.Put, appendSas)
        {
            Content = new ByteArrayContent("must not overwrite"u8.ToArray())
        })
        {
            oldOverwrite.Headers.TryAddWithoutValidation("x-ms-version", "2014-02-14");
            oldOverwrite.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var response = await transport.SendAsync(oldOverwrite);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.Equal(
            "retained append payload",
            (await append.DownloadContentAsync()).Value.Content.ToString());

        var pageSas = page.GenerateSasUri(BlobSasPermissions.All, DateTimeOffset.UtcNow.AddMinutes(5));
        using (var oldRead = new HttpRequestMessage(HttpMethod.Head, pageSas))
        {
            oldRead.Headers.TryAddWithoutValidation("x-ms-version", "2009-07-17");
            using var response = await transport.SendAsync(oldRead);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal(
                "InvalidVersionForPageBlobOperation",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldPageWrite = new HttpRequestMessage(HttpMethod.Put, AppendQuery(pageSas, "comp=page"))
        {
            Content = new ByteArrayContent(Enumerable.Repeat((byte)0x5A, 512).ToArray())
        })
        {
            oldPageWrite.Headers.TryAddWithoutValidation("x-ms-version", "2009-07-17");
            oldPageWrite.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            oldPageWrite.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
            using var response = await transport.SendAsync(oldPageWrite);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal(
                "InvalidVersionForPageBlobOperation",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.All((await page.DownloadContentAsync()).Value.Content.ToArray(), value => Assert.Equal(0, value));

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
            using var response = await transport.SendAsync(oldCreate);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal(
                "InvalidVersionForPageBlobOperation",
                response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldPageCreate.ExistsAsync()).Value);

        var containerSas = container.GenerateSasUri(
            BlobContainerSasPermissions.All,
            DateTimeOffset.UtcNow.AddMinutes(5));
        async Task AssertOldListRejectsAsync(string prefix, string version)
        {
            var uri = AppendQuery(
                containerSas,
                $"restype=container&comp=list&prefix={Uri.EscapeDataString(prefix)}");
            using var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.TryAddWithoutValidation("x-ms-version", version);
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }
        await AssertOldListRejectsAsync(append.Name, "2014-02-14");
        await AssertOldListRejectsAsync(page.Name, "2009-07-17");

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
            using var response = await transport.SendAsync(oldCopy);
            Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
            Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await copied.ExistsAsync()).Value);
    }

    [Fact]
    public async Task VersionedBlobFeaturesRejectBeforeMutationAndHideNewerResponseFields()
    {
        var containerName = $"feature-versions-{Guid.NewGuid():N}";
        await using var application = new SavaWebApplicationFactory(
            new Dictionary<string, string?>
            {
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:VersioningEnabled"] = "true",
                [$"Sava:AccountCapabilities:{SavaWebApplicationFactory.AccountName}:ImmutableStorageWithVersioningContainers:0"] = containerName
            });
        var service = CreateClient(application);
        var container = service.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        using var transport = new HttpClient(application.Server.CreateHandler());

        static async Task AssertFeatureVersionMismatchAsync(HttpClient client, HttpRequestMessage request)
        {
            using (request)
            using (var response = await client.SendAsync(request))
            {
                Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                Assert.Equal("FeatureVersionMismatch", response.Headers.GetValues("x-ms-error-code").Single());
            }
        }

        var tagged = container.GetBlobClient("tagged.bin");
        await tagged.UploadAsync(
            BinaryData.FromString("tagged payload"),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string> { ["project"] = "mk8" }
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
        await mutable.SetMetadataAsync(new Dictionary<string, string> { ["state"] = "still-mutable" });

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
        var append = methods.Single(method =>
            method.Name == "Append" &&
            method.GetParameters() is [{ ParameterType: var parameterType }] &&
            parameterType == typeof(byte[]));
        var getHash = methods.Single(method => method.Name == "GetHashAndReset" && method.GetParameters().Length == 0);
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
            Assert.False(response.Headers.Contains("x-ms-error-code"));
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
        Assert.Contains("ETag", corsResponse.Headers.GetValues("Access-Control-Expose-Headers").Single());
        Assert.Contains("Origin", corsResponse.Headers.Vary);

        using var wildcardRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        wildcardRequest.Headers.Add("Origin", "https://nested.app.trusted.example");
        using var wildcardResponse = await client.SendAsync(wildcardRequest);
        Assert.Equal(HttpStatusCode.OK, wildcardResponse.StatusCode);
        Assert.Equal(
            "https://nested.app.trusted.example",
            wildcardResponse.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Contains("Origin", wildcardResponse.Headers.Vary);

        using var caseMismatchRequest = new HttpRequestMessage(HttpMethod.Get, uri);
        caseMismatchRequest.Headers.Add("Origin", "https://CLIENT.example");
        using var caseMismatchResponse = await client.SendAsync(caseMismatchRequest);
        Assert.Equal(HttpStatusCode.OK, caseMismatchResponse.StatusCode);
        Assert.False(caseMismatchResponse.Headers.Contains("Access-Control-Allow-Origin"));
        Assert.Contains("Origin", caseMismatchResponse.Headers.Vary);

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
        Assert.False(conditionalResponse.Headers.Contains("x-ms-error-code"));
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
                Tags = new Dictionary<string, string> { ["kind"] = "query" },
                Metadata = new Dictionary<string, string> { ["marker"] = "must-not-leak" },
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
            Assert.Equal(300L, Assert.IsType<Apache.Arrow.Int64Array>(batch.Column("first")).GetValue(0));
            Assert.Equal(400D, Assert.IsType<Apache.Arrow.DoubleArray>(batch.Column("second")).GetValue(0));
            Assert.Equal("500.00", Assert.IsType<Apache.Arrow.Decimal128Array>(batch.Column("third")).GetString(0));
            Assert.Equal("600", Assert.IsType<Apache.Arrow.StringArray>(batch.Column("fourth")).GetString(0));
            Assert.True(Assert.IsType<Apache.Arrow.BooleanArray>(batch.Column("enabled")).GetValue(0));
            Assert.Equal(
                new DateTimeOffset(2026, 9, 22, 12, 34, 56, 789, TimeSpan.Zero),
                Assert.IsType<Apache.Arrow.TimestampArray>(batch.Column("observed")).GetTimestamp(0));
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
        var id = new Parquet.Schema.DataField<int>("id");
        var name = new Parquet.Schema.DataField<string>("name");
        var enabled = new Parquet.Schema.DataField<bool>("enabled");
        var score = new Parquet.Schema.DataField<double>("score");
        var observed = new Parquet.Schema.DataField<DateTime>("observed");
        var maybe = new Parquet.Schema.DataField<int?>("maybe");
        var schema = new Parquet.Schema.ParquetSchema(id, name, enabled, score, observed, maybe);

        using var content = new MemoryStream();
        await using (var writer = await Parquet.ParquetWriter.CreateAsync(schema, content))
        {
            using (var group = writer.CreateRowGroup())
            {
                await group.WriteAsync<int>(id, new[] { 1, 2 }.AsMemory());
                await group.WriteAsync(name, new string?[] { "one", "two" });
                await group.WriteAsync<bool>(enabled, new[] { true, false }.AsMemory());
                await group.WriteAsync<double>(score, new[] { 1.25D, 2.5D }.AsMemory());
                await group.WriteAsync<DateTime>(
                    observed,
                    new[]
                    {
                        new DateTime(2026, 9, 20, 10, 15, 0, DateTimeKind.Utc),
                        new DateTime(2026, 9, 21, 11, 30, 0, DateTimeKind.Utc)
                    }.AsMemory());
                await group.WriteAsync<int>(maybe, new int?[] { null, 20 }.AsMemory());
                group.CompleteValidate();
            }

            using (var group = writer.CreateRowGroup())
            {
                await group.WriteAsync<int>(id, new[] { 3 }.AsMemory());
                await group.WriteAsync(name, new string?[] { "three" });
                await group.WriteAsync<bool>(enabled, new[] { true }.AsMemory());
                await group.WriteAsync<double>(score, new[] { 3.75D }.AsMemory());
                await group.WriteAsync<DateTime>(
                    observed,
                    new[] { new DateTime(2026, 9, 22, 12, 45, 0, DateTimeKind.Utc) }.AsMemory());
                await group.WriteAsync<int>(maybe, new int?[] { null }.AsMemory());
                group.CompleteValidate();
            }
        }
        content.Position = 0;

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
                });
            using var reader = new StreamReader(response.Value.Content);
            using var document = JsonDocument.Parse((await reader.ReadToEndAsync()).Trim());
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
                options);
            using var reader = new StreamReader(response.Value.Content);
            using var result = JsonDocument.Parse((await reader.ReadToEndAsync()).Trim());
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

        await using var connection = new SqliteConnection($"Data Source={Path.Combine(dataPath, "metadata.db")}");
        await connection.OpenAsync();
        await using (var schema = connection.CreateCommand())
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
            await schema.ExecuteNonQueryAsync();
        }
        await using (var insert = connection.CreateCommand())
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
            await insert.ExecuteNonQueryAsync();
        }
    }

    private static async Task CreateVersionOneBackupAsync(string dataPath, string backupPath)
    {
        Directory.CreateDirectory(backupPath);
        Directory.CreateDirectory(Path.Combine(backupPath, "chunks"));
        var source = Path.Combine(dataPath, "metadata.db");
        var destination = Path.Combine(backupPath, "metadata.db");
        File.Copy(source, destination);
        var metadata = await File.ReadAllBytesAsync(destination);
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
            keyRequirements = new Dictionary<string, string>(),
            chunks = Array.Empty<object>()
        };
        await File.WriteAllBytesAsync(
            Path.Combine(backupPath, "backup-manifest.json"),
            JsonSerializer.SerializeToUtf8Bytes(manifest, new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                WriteIndented = true
            }));
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
                .Append(header.Key.ToLowerInvariant())
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
                .Append(parameter.Key.ToLowerInvariant())
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
        var transportClient = new HttpClient(app.Server.CreateHandler())
        {
            BaseAddress = new Uri($"http://{accountName}.localhost")
        };
        var options = new BlobClientOptions
        {
            Transport = new HttpClientTransport(transportClient),
            Retry = { MaxRetries = 0 }
        };
        return new BlobServiceClient(
            new Uri($"http://{accountName}.localhost"),
            new StorageSharedKeyCredential(accountName, accountKey),
            options);
    }

    private static BlobClient CreateBlobClient(SavaWebApplicationFactory app, Uri uri) =>
        new(uri, new BlobClientOptions
        {
            Transport = new HttpClientTransport(new HttpClient(app.Server.CreateHandler()) { BaseAddress = uri }),
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
            Transport = new HttpClientTransport(new HttpClient(app.Server.CreateHandler()) { BaseAddress = endpoint }),
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
            Transport = new HttpClientTransport(new HttpClient(app.Server.CreateHandler()) { BaseAddress = endpoint }),
            Retry = { MaxRetries = 0 }
        });
    }

    private static string CreateJwt(string base64Key, string objectId, string? tenantId = null)
    {
        var key = new SymmetricSecurityKey(Convert.FromBase64String(base64Key)) { KeyId = "test-key" };
        var claims = new List<Claim> { new("oid", objectId) };
        if (tenantId is not null)
            claims.Add(new Claim("tid", tenantId));
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

    private static IReadOnlyList<string> ParseAnalyticsLogFields(string record)
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
            if (sourceUri.AbsolutePath == "/share/anonymous.bin")
            {
                Assert.Equal("source.file.core.windows.net", sourceUri.Host);
                Assert.Null(request.Headers.Authorization);
                Assert.False(request.Headers.Contains("x-ms-file-request-intent"));
            }
            else if (sourceUri.Host == "source.blob.core.windows.net")
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

    private sealed class LoopbackSource(WebApplication application, Uri uri) : IAsyncDisposable
    {
        public Uri Uri { get; } = uri;
        public Uri MissingUri { get; } = new(uri, "/missing");

        public static async Task<LoopbackSource> StartAsync(byte[] content)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, 0));
            var application = builder.Build();
            application.MapGet("/source", async context =>
            {
                const string sourceEtag = "\"source-etag\"";
                var ifMatch = context.Request.Headers.IfMatch.ToString();
                if (!string.IsNullOrEmpty(ifMatch) &&
                    !ifMatch.Split(',', StringSplitOptions.TrimEntries).Any(value => value is "*" or sourceEtag))
                {
                    await WriteSourceErrorAsync(context, StatusCodes.Status412PreconditionFailed, "ConditionNotMet");
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
                await context.Response.Body.WriteAsync(content.AsMemory(start, end - start + 1));
            });
            application.MapGet("/missing", context =>
                WriteSourceErrorAsync(context, StatusCodes.Status404NotFound, "BlobNotFound"));
            await application.StartAsync();
            var addresses = application.Services
                .GetRequiredService<IServer>()
                .Features
                .Get<IServerAddressesFeature>()
                ?.Addresses;
            var address = addresses?.Single() ?? throw new InvalidOperationException("The source server did not publish an address.");
            return new LoopbackSource(application, new Uri(new Uri(address), "/source"));
        }

        private static async Task WriteSourceErrorAsync(HttpContext context, int statusCode, string errorCode)
        {
            var message = errorCode == "BlobNotFound"
                ? "The specified blob does not exist."
                : "The condition specified using HTTP conditional header(s) is not met.";
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "application/xml";
            context.Response.Headers["x-ms-error-code"] = errorCode;
            await context.Response.WriteAsync(
                $"<Error><Code>{errorCode}</Code><Message>{message}</Message></Error>");
        }

        public async ValueTask DisposeAsync()
        {
            await application.StopAsync();
            await application.DisposeAsync();
        }
    }
}
