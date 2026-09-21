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
        Assert.Equal(HttpStatusCode.BadRequest, oldVersionConditionResponse.StatusCode);
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

        async Task AssertRejectedAsync(Uri uri, string version, bool arrow)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.Add("x-ms-version", version);
            if (arrow)
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(AzureResponseWriter.ArrowStreamContentType));
            using var response = await transport.SendAsync(request);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal("application/xml", response.Content.Headers.ContentType?.MediaType);
        }

        await AssertRejectedAsync(arrowUri, "2026-04-06", arrow: true);
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
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
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
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
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
    public async Task UrlTransfersSupportBlockAppendPageAndWholeBlobOperations()
    {
        var sourceBytes = Enumerable.Range(0, 16 * 1024).Select(index => (byte)(index % 251)).ToArray();
        await using var source = await LoopbackSource.StartAsync(sourceBytes);
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"url-{Guid.NewGuid():N}");
        await container.CreateAsync();

        var whole = container.GetBlockBlobClient("whole.bin");
        await whole.SyncUploadFromUriAsync(source.Uri, overwrite: true);
        Assert.Equal(sourceBytes, (await whole.DownloadContentAsync()).Value.Content.ToArray());

        const int blockOffset = 900;
        const int blockLength = 2_300;
        var block = container.GetBlockBlobClient("block.bin");
        var blockId = Convert.ToBase64String("url-block-1"u8);
        var blockSlice = sourceBytes.AsSpan(blockOffset, blockLength).ToArray();
        await block.StageBlockFromUriAsync(source.Uri, blockId, new StageBlockFromUriOptions
        {
            SourceRange = new HttpRange(blockOffset, blockLength),
            SourceContentHash = MD5.HashData(blockSlice)
        });
        await block.CommitBlockListAsync([blockId]);
        Assert.Equal(blockSlice, (await block.DownloadContentAsync()).Value.Content.ToArray());

        const int appendOffset = 4_000;
        const int appendLength = 1_500;
        var append = container.GetAppendBlobClient("append.bin");
        await append.CreateAsync();
        var appendSlice = sourceBytes.AsSpan(appendOffset, appendLength).ToArray();
        await append.AppendBlockFromUriAsync(source.Uri, new AppendBlobAppendBlockFromUriOptions
        {
            SourceRange = new HttpRange(appendOffset, appendLength),
            SourceContentHash = MD5.HashData(appendSlice)
        });
        Assert.Equal(appendSlice, (await append.DownloadContentAsync()).Value.Content.ToArray());

        var page = container.GetPageBlobClient("page.bin");
        await page.CreateAsync(1024);
        var pageSlice = sourceBytes.AsSpan(0, 512).ToArray();
        await page.UploadPagesFromUriAsync(
            source.Uri,
            new HttpRange(0, 512),
            new HttpRange(512, 512),
            new PageBlobUploadPagesFromUriOptions { SourceContentHash = MD5.HashData(pageSlice) });
        var expectedPage = new byte[1024];
        pageSlice.CopyTo(expectedPage, 512);
        Assert.Equal(expectedPage, (await page.DownloadContentAsync()).Value.Content.ToArray());
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
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
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
        var page = container.GetPageBlobClient("disk.vhd");
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
        var page = container.GetPageBlobClient("disk.vhd");
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
    public async Task ImmutabilityPoliciesAndLegalHoldsArePersistedAndEnforced()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"worm-{Guid.NewGuid():N}");
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
        Assert.Equal(HttpStatusCode.BadRequest, oldVersionTierResponse.StatusCode);
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
        Assert.DoesNotContain("sig=", pending.CopySource.AbsoluteUri, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("abort", pending.Metadata["copy"]);

        var aborted = await abortDestination.AbortCopyFromUriAsync(abortOperation.Id);
        Assert.Equal(204, aborted.Status);
        var abortedProperties = (await abortDestination.GetPropertiesAsync()).Value;
        Assert.Equal(CopyStatus.Aborted, abortedProperties.CopyStatus);
        Assert.Equal(0, abortedProperties.ContentLength);
        Assert.Equal("abort", abortedProperties.Metadata["copy"]);

        var source = container.GetBlobClient("source.bin");
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
    public async Task CopyBlobAuthenticatesSourceIndependentlyForSasDestinations()
    {
        var owner = CreateClient(factory);
        var container = owner.GetBlobContainerClient($"copy-sas-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var source = container.GetBlobClient("source.bin");
        var content = Enumerable.Range(0, 32 * 1024).Select(index => (byte)(index % 239)).ToArray();
        await source.UploadAsync(BinaryData.FromBytes(content), new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string> { ["origin"] = "source-sas" }
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
    }

    [Fact]
    public async Task BlobBatchesExecuteIndependentDeleteAndTierSubrequests()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"batch-{Guid.NewGuid():N}");
        await container.CreateAsync();
        var deleteTarget = container.GetBlobClient("delete.bin");
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

        var tierTarget = container.GetBlobClient("tier.bin");
        await tierTarget.UploadAsync(BinaryData.FromString("tier me"));
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

        var missingKey = await Assert.ThrowsAsync<RequestFailedException>(() =>
            normalService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin").GetPropertiesAsync());
        Assert.Equal(409, missingKey.Status);
        Assert.Equal("CustomerProvidedKeyInUse", missingKey.ErrorCode);
        var wrongKeyService = CreateEncryptedClient(factory, new CustomerProvidedKey(wrongKey), encryptionScope: null);
        var mismatchedKey = await Assert.ThrowsAsync<RequestFailedException>(() =>
            wrongKeyService.GetBlobContainerClient(containerName).GetBlobClient("keyed.bin").DownloadContentAsync());
        Assert.Equal(409, mismatchedKey.Status);
        Assert.Equal("CustomerProvidedKeyInUse", mismatchedKey.ErrorCode);

        const string scope = "records-scope";
        var scopedService = CreateEncryptedClient(factory, customerProvidedKey: null, scope);
        var scopedBlob = scopedService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin");
        await scopedBlob.UploadAsync(BinaryData.FromBytes(content));
        var chunksAfterScoped = Directory.GetFiles(Path.Combine(factory.DataPath, "chunks"), "*.chunk", SearchOption.AllDirectories).Length;
        var scopedProperties = (await scopedBlob.GetPropertiesAsync()).Value;
        Assert.Equal(scope, scopedProperties.EncryptionScope);
        Assert.Equal(content, (await normalService.GetBlobContainerClient(containerName).GetBlobClient("scoped.bin").DownloadContentAsync()).Value.Content.ToArray());

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
        var client = CreateClient(factory);
        var containerName = $"maintenance-{Guid.NewGuid():N}";
        var container = client.GetBlobContainerClient(containerName);
        await container.CreateAsync();
        var blobService = factory.Services.GetRequiredService<BlobService>();
        var metadata = factory.Services.GetRequiredService<MetadataStore>();
        var chunkStore = factory.Services.GetRequiredService<ChunkStore>();

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
            chunk => Assert.False(File.Exists(ChunkPath(factory.DataPath, chunk.Id))));
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
            ["Sava:UncommittedBlocksPerMaintenancePass"] = "1"
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
                Assert.Equal(4L, Convert.ToInt64(await version.ExecuteScalarAsync(), CultureInfo.InvariantCulture));
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
            Assert.Equal(HttpStatusCode.BadRequest, oldCrcResponse.StatusCode);
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
            Assert.Equal(HttpStatusCode.BadRequest, oldStructuredResponse.StatusCode);
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
            Assert.Equal(HttpStatusCode.BadRequest, oldAppendCreateResponse.StatusCode);
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
            Assert.Equal(HttpStatusCode.BadRequest, oldBlockFromUrlResponse.StatusCode);
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
            Assert.Equal(HttpStatusCode.BadRequest, oldSourceCrcResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceCrcResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldSourceAuthorizationRequest = new HttpRequestMessage(HttpMethod.Put, supportedBlockFromUrlUri))
        {
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-version", "2020-04-08");
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldSourceAuthorizationRequest.Headers.TryAddWithoutValidation("x-ms-copy-source-authorization", "Bearer opaque-token");
            using var oldSourceAuthorizationResponse = await transport.SendAsync(oldSourceAuthorizationRequest);
            Assert.Equal(HttpStatusCode.BadRequest, oldSourceAuthorizationResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceAuthorizationResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        using (var oldSourceTagConditionRequest = new HttpRequestMessage(HttpMethod.Put, supportedBlockFromUrlUri))
        {
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-version", "2019-02-02");
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldSourceTagConditionRequest.Headers.TryAddWithoutValidation("x-ms-source-if-tags", "\"project\" = 'mk8'");
            using var oldSourceTagConditionResponse = await transport.SendAsync(oldSourceTagConditionRequest);
            Assert.Equal(HttpStatusCode.BadRequest, oldSourceTagConditionResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldSourceTagConditionResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var oldAppendFromUrlRequest = new HttpRequestMessage(HttpMethod.Put, appendUri))
        {
            oldAppendFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-03-28");
            oldAppendFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            using var oldAppendFromUrlResponse = await transport.SendAsync(oldAppendFromUrlRequest);
            Assert.Equal(HttpStatusCode.BadRequest, oldAppendFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldAppendFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }

        using (var oldPageFromUrlRequest = new HttpRequestMessage(HttpMethod.Put, pageUri))
        {
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-version", "2018-03-28");
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-copy-source", unreachableSource);
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-page-write", "update");
            oldPageFromUrlRequest.Headers.TryAddWithoutValidation("x-ms-range", "bytes=0-511");
            using var oldPageFromUrlResponse = await transport.SendAsync(oldPageFromUrlRequest);
            Assert.Equal(HttpStatusCode.BadRequest, oldPageFromUrlResponse.StatusCode);
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
            Assert.Equal(HttpStatusCode.BadRequest, oldPutBlobFromUrlResponse.StatusCode);
            Assert.Equal("FeatureVersionMismatch", oldPutBlobFromUrlResponse.Headers.GetValues("x-ms-error-code").Single());
        }
        Assert.False((await oldPutBlobFromUrl.ExistsAsync()).Value);
    }

    [Fact]
    public async Task VersionedBlobFeaturesRejectBeforeMutationAndHideNewerResponseFields()
    {
        var service = CreateClient(factory);
        var container = service.GetBlobContainerClient($"feature-versions-{Guid.NewGuid():N}");
        await container.CreateAsync();
        using var transport = new HttpClient(factory.Server.CreateHandler());

        static async Task AssertFeatureVersionMismatchAsync(HttpClient client, HttpRequestMessage request)
        {
            using (request)
            using (var response = await client.SendAsync(request))
            {
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
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
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-04-08");
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
            request.Headers.TryAddWithoutValidation("x-ms-version", "2020-04-08");
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
            request.Headers.TryAddWithoutValidation("x-ms-version", "2019-12-12");
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
        using var preflightResponse = await client.SendAsync(preflightRequest);
        Assert.Equal(HttpStatusCode.OK, preflightResponse.StatusCode);
        Assert.Equal(
            "https://app.trusted.example",
            preflightResponse.Headers.GetValues("Access-Control-Allow-Origin").Single());
        Assert.Equal("true", preflightResponse.Headers.GetValues("Access-Control-Allow-Credentials").Single());
        Assert.Equal("HEAD", preflightResponse.Headers.GetValues("Access-Control-Allow-Methods").Single());

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
            new BlobUploadOptions { Tags = new Dictionary<string, string> { ["kind"] = "query" } });
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

        var append = container.GetAppendBlobClient("not-queryable");
        await append.CreateAsync();
        var invalidType = await Assert.ThrowsAsync<RequestFailedException>(() =>
            container.GetBlockBlobClient("not-queryable").QueryAsync("SELECT * FROM BlobStorage"));
        Assert.Equal(409, invalidType.Status);
        Assert.Equal("InvalidBlobType", invalidType.ErrorCode);
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

    private sealed class LoopbackSource(WebApplication application, Uri uri) : IAsyncDisposable
    {
        public Uri Uri { get; } = uri;

        public static async Task<LoopbackSource> StartAsync(byte[] content)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, 0));
            var application = builder.Build();
            application.MapGet("/source", async context =>
            {
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
                context.Response.Headers.ETag = "\"source-etag\"";
                await context.Response.Body.WriteAsync(content.AsMemory(start, end - start + 1));
            });
            await application.StartAsync();
            var addresses = application.Services
                .GetRequiredService<IServer>()
                .Features
                .Get<IServerAddressesFeature>()
                ?.Addresses;
            var address = addresses?.Single() ?? throw new InvalidOperationException("The source server did not publish an address.");
            return new LoopbackSource(application, new Uri(new Uri(address), "/source"));
        }

        public async ValueTask DisposeAsync()
        {
            await application.StopAsync();
            await application.DisposeAsync();
        }
    }
}
