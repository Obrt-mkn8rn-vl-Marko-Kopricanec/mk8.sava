using System.Net;
using System.Security.Claims;
using System.Text.Json;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Tests;

public sealed partial class MicrosoftGraphGroupMembershipResolverTests
{
    [Theory]
    [InlineData(100000, true)]
    [InlineData(100001, false)]
    public async Task PagedGroupCountAcceptsTheInclusiveBoundAndRejectsPartialOverage(int count, bool accepted) =>
        await AssertGroupCountBoundAsync(count, accepted).ConfigureAwait(true);

    [Theory]
    [InlineData(128, true)]
    [InlineData(129, false)]
    public async Task PagedResponseCountAcceptsTheInclusiveBoundAndRejectsPartialOverage(int pages, bool accepted) =>
        await AssertPageCountBoundAsync(pages, accepted).ConfigureAwait(true);

    private static async Task AssertGroupCountBoundAsync(int count, bool accepted)
    {
        var membershipPath = $"/v1.0/users/{ReaderObjectId}/transitiveMemberOf/microsoft.graph.group";
        var groupOffset = 0;
        var pages = 0;
        using var handler = new GraphHandler(request =>
        {
            if (TryCreatePagedPrelude(request, out var response))
                return response!;
            Assert.Equal(membershipPath, request.RequestUri?.AbsolutePath);
            Assert.Equal("eventual", request.Headers.GetValues("ConsistencyLevel").Single());
            var groups = Enumerable.Range(groupOffset, Math.Min(1000, count - groupOffset))
                .Select(index => new { id = CreateGroupId(index), securityEnabled = true }).ToArray();
            groupOffset += groups.Length;
            pages++;
            var body = new Dictionary<string, object>(StringComparer.Ordinal) { ["value"] = groups };
            if (groupOffset < count)
                body["@odata.nextLink"] = $"https://graph.microsoft.com{membershipPath}?%24skiptoken=page-{pages}";
            return JsonResponse(HttpStatusCode.OK, JsonSerializer.Serialize(body));
        });
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, new GraphCredential());
        if (accepted)
        {
            var groups = await resolver.ResolveAsync(Principal(new Claim("hasgroups", "true")),
                ReaderObjectId, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(count, groups.Count);
            Assert.True(groups.SetEquals(Enumerable.Range(0, count).Select(CreateGroupId)));
        }
        else
        {
            var error = await Assert.ThrowsAsync<AzureStorageException>(() => resolver.ResolveAsync(
                Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None)).ConfigureAwait(false);
            Assert.Equal("AuthorizationFailure", error.ErrorCode);
        }
        Assert.Equal(count, groupOffset);
        Assert.Equal((count + 999) / 1000, pages);
        Assert.Equal(pages + 2, handler.Calls);
    }

    private static async Task AssertPageCountBoundAsync(int requiredPages, bool accepted)
    {
        var membershipPath = $"/v1.0/users/{ReaderObjectId}/transitiveMemberOf/microsoft.graph.group";
        var pages = 0;
        using var handler = new GraphHandler(request =>
        {
            if (TryCreatePagedPrelude(request, out var response))
                return response!;
            Assert.Equal(membershipPath, request.RequestUri?.AbsolutePath);
            Assert.Equal("eventual", request.Headers.GetValues("ConsistencyLevel").Single());
            pages++;
            var body = new Dictionary<string, object>(StringComparer.Ordinal)
            {
                ["value"] = new[] { new { id = ReaderGroupId, securityEnabled = true } }
            };
            if (pages < requiredPages)
                body["@odata.nextLink"] = $"https://graph.microsoft.com{membershipPath}?%24skiptoken=page-{pages}";
            return JsonResponse(HttpStatusCode.OK, JsonSerializer.Serialize(body));
        });
        using var client = new HttpClient(handler);
        var resolver = CreateResolver(client, new GraphCredential());
        if (accepted)
        {
            var groups = await resolver.ResolveAsync(Principal(new Claim("hasgroups", "true")),
                ReaderObjectId, CancellationToken.None).ConfigureAwait(false);
            Assert.True(groups.SetEquals([ReaderGroupId]));
        }
        else
        {
            var error = await Assert.ThrowsAsync<AzureStorageException>(() => resolver.ResolveAsync(
                Principal(new Claim("hasgroups", "true")), ReaderObjectId, CancellationToken.None)).ConfigureAwait(false);
            Assert.Equal("AuthorizationFailure", error.ErrorCode);
        }
        Assert.Equal(Math.Min(requiredPages, 128), pages);
        Assert.Equal(pages + 2, handler.Calls);
    }

    private static bool TryCreatePagedPrelude(HttpRequestMessage request, out HttpResponseMessage? response)
    {
        response = request.Method == HttpMethod.Post
            ? JsonResponse(HttpStatusCode.BadRequest, "{\"error\":{\"code\":\"Directory_ResultSizeLimitExceeded\"}}")
            : string.Equals(request.RequestUri?.AbsolutePath, $"/v1.0/directoryObjects/{ReaderObjectId}", StringComparison.Ordinal)
                ? JsonResponse(HttpStatusCode.OK, "{\"@odata.type\":\"#microsoft.graph.user\"}")
                : null;
        return response is not null;
    }

    private static string CreateGroupId(int index) => FormattableString.Invariant($"{index:x8}-0000-4000-8000-000000000000");
}
