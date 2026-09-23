# mk8.sava operations

The service keeps its authoritative metadata in `metadata.db` and encrypted,
content-addressed extents under `chunks/` or append-only small-object packs under
`packs/`. Chunk identities are immutable, while their verified physical encoding
may be atomically replaced by background recompression or pack compaction. The
configured `Sava:DataPath` is a single storage root; do not copy a live root with
a generic filesystem command and assume the result is consistent.

## Publication and failure boundaries

Blob content is staged, authenticated, flushed, and published before its logical
metadata can commit. A failure before the SQLite commit therefore exposes no new
blob; any already-published but unreachable extent remains safe for a later
garbage-collection pass. Reclamation obtains an exclusive mutation reservation
and repeats the metadata reachability check before deleting an extent. If that
pass is interrupted, disposing the reservation leaves the extent available to a
future pass.

On Unix, newly used storage directories and published chunk/pack directory
entries are also flushed before SQLite can reference them. Flushing file bytes
alone does not make a new filename durable across a sudden power loss. Windows
uses `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` for same-volume file and
backup-directory publication, without a copy-and-delete fallback. Its directory
creation and active-pack entry paths still lack a verified directory flush;
power-loss durability on Windows and deployment-specific filesystems needs
further validation.

Small-chunk publication serializes pack appends by sharing domain within one
service process and repeats the existing-chunk lookup inside that gate. Concurrent
identical uploads therefore reuse the first verified record without appending a
dead duplicate to the pack.
Before appending to an active pack, the service compares its length with the end
of the last SQLite-indexed record. It rejects a pack shorter than that committed
boundary and truncates any unindexed tail left by an interrupted append before
writing another record. Pack compaction still removes dead records inside packs
left by interrupted publication or other historical failures.

Only one mk8.sava process may open a data root at a time. The service holds an
exclusive `.mk8-sava.lock` file handle for its lifetime and fails startup if
another instance holds it. Leave the file in place: a process crash releases the
operating-system lock, so the next instance can reopen the root and recover.
Do not use a filesystem that does not reliably propagate exclusive file locks
between hosts for a shared data root; multi-writer shared-root deployment is
not supported.

Pack compaction publishes its replacement file before switching SQLite's chunk
locations. If the metadata operation fails or the process exits near commit, it
keeps both pack files until the authoritative locations are known. A subsequent
maintenance pass reclaims whichever pack is unreferenced; it never deletes the
replacement merely because the commit response was lost.

Once the metadata transaction commits, the logical write is authoritative even
if the client connection is lost or the server cannot return the response.
Storage Analytics and ordinary request logging execute outside that transaction;
their failure is logged but cannot roll back or replace the storage response.
The integration suite has deterministic checkpoints at extent publication,
metadata pre-commit and post-commit, and the final reclamation delete so these
boundaries can be exercised without making fault injection a deployment feature.

Run the separate process-termination matrix from the repository root when
changing publication, SQLite, staging, or reclamation code:

```bash
DOTNET_HOST_PATH=/path/to/dotnet Mk8.Sava.Tests/CrashHarness/run.sh
```

The harness creates a distinct temporary storage root for each of the eight
staging-write, pack-append, publication, metadata, pack-compaction, and
reclamation checkpoints,
terminates the worker without unwinding it, and validates recovery in a new test
host before deleting that exact temporary root. Core dumps are disabled for the
intentional terminations.

On Linux hosts that permit unprivileged user and mount namespaces, run the
separate real-ENOSPC lane:

```bash
DOTNET_HOST_PATH=/path/to/dotnet Mk8.Sava.Tests/CrashHarness/run-enospc.sh
```

It mounts a private 32 MiB tmpfs beneath a freshly created temporary directory,
fills it until the kernel returns `ENOSPC`, and runs eight independent boundaries:
a 512 KiB standalone upload with only 256 KiB free, metadata-only container
creation with 128 KiB free, blob metadata and HTTP-property updates with 256 KiB
free each, a packed
upload with 8 KiB free, pack compaction with 4 KiB free, and a blob-publication
transaction where a fault hook fills the tmpfs after staging but immediately
before the SQLite commit, and a pack-compaction transaction where the same
technique fills it after the replacement pack is published but before SQLite
switches the index. The tests confirm an earlier acknowledged object remains
exact, failed metadata/property updates retain the last acknowledged value
and ETag, failed logical publication is
absent after restart, partial/unreachable extents can be reclaimed or discarded,
and retry succeeds. The pack test also records that staging completed and pack
append began before the kernel failure. The compaction test checks that a
failed replacement leaves the old indexed pack authoritative across restart,
then succeeds after capacity is restored. The publication-commit case checks
that staged chunks remain unreachable, the earlier blob survives, garbage
collection reclaims the orphan after restart, and retry publishes exact bytes.
The compaction-commit case checks that the old pack remains authoritative,
the unindexed replacement can be reclaimed after restart, and compaction can
then succeed without changing live bytes.
The mount is private to the harness process and is unmounted on exit. This
does not cover every SQLite commit, filesystem, Windows, or power-loss boundary.

## SDK substitution checks

The normal .NET integration suite uses the official Azure Storage client.
Separate JavaScript, Python, Go, Java, and C++ process lanes prove that endpoint
substitution also works through independent official protocol stacks, including
path-style account URLs, service and blob listings, block/append/page blobs,
staged blocks, snapshots, leases, tags, byte ranges, and SAS authentication.

Run the applicable lane from the repository root with the .NET SDK and the
lane-specific prerequisites described below:

```bash
DOTNET_HOST_PATH=/path/to/dotnet Mk8.Sava.Tests/SdkCompatibility/javascript/run.sh
DOTNET_HOST_PATH=/path/to/dotnet Mk8.Sava.Tests/SdkCompatibility/python/run.sh
DOTNET_HOST_PATH=/path/to/dotnet Mk8.Sava.Tests/SdkCompatibility/go/run.sh
DOTNET_HOST_PATH=/path/to/dotnet Mk8.Sava.Tests/SdkCompatibility/java/run.sh
DOTNET_HOST_PATH=/path/to/dotnet Mk8.Sava.Tests/SdkCompatibility/cpp/run.sh
```

The JavaScript harness requires Node.js 20 or later and Corepack. It pins the
newest Azure client line supporting Node.js 20 and locks its transitive Azure
packages to Node.js 20-compatible releases. The Python harness requires Python
3.10 or later and `curl`; it hash-verifies a pinned pip wheel, installs an entirely
hash-locked dependency graph into a disposable target, and never modifies the
host Python installation. The Go harness hash-verifies the current stable Linux
x86-64 toolchain from `go.dev`; `go.mod` and `go.sum` pin and authenticate the
official SDK and its complete module graph. The Java harness hash-verifies
current Temurin 21 LTS and Maven distributions, pins the official Azure client,
and resolves dependencies into a disposable Maven repository. The C++ harness
requires a Linux x86-64 C/C++ toolchain and kernel headers; it hash-verifies a
pinned vcpkg catalog and `pkgconf-lite` source release, asserts the official
Azure client version, and builds every native dependency in disposable download,
binary-cache, install, and build roots. Each harness starts a real loopback
mk8.sava process with a disposable storage root rather than routing the client
through ASP.NET's in-memory test server.

The Azurite differential lane is run with
`DOTNET_HOST_PATH=/path/to/dotnet Mk8.Sava.Tests/SdkCompatibility/azurite/run.sh`.
It starts a disposable, strict-mode Azurite Blob server on loopback, pins
Azurite `3.35.0` and its dependency graph, and uses the same official .NET
Blob SDK against Azurite and mk8.sava. It compares container creation,
block-blob upload, properties, metadata, full and ranged downloads,
conditional writes, listing, deletion, and missing-resource errors. Separate
scenarios compare staged-block commit/order and block lists, snapshots,
lease-enforced metadata writes, tags, append blobs, and page ranges/clears. The
container scenario compares metadata, public-access policy, and lease-enforced
deletion; it enables public access only in its disposable mk8.sava fixture to
match Azurite's test account, leaving the service's secure default unchanged. The
service-list scenario compares prefix filtering, container metadata, one-item
pages, and continuation through the official SDK, including an out-of-prefix
container. The service-properties scenario compares a CORS rule round trip
through the SDK, an allowed preflight, a denied origin, and a malformed
preflight. A same-account copy scenario compares source conditions, completion,
and exact downloaded bytes. A blob-list scenario compares filtered metadata,
one-item pages and continuations, and delimiter-based hierarchy prefixes with
an out-of-prefix blob. A stored-access-policy scenario compares a read-only
service SAS, denied overwrite, and revocation after policy removal. Azurite
and mk8.sava also agree on ranged bytes, content-range and MD5, plus conditional
read statuses and error codes. The ranged-read scenario records an emulator
divergence: Azurite accepts `x-ms-range-get-content-md5: true` without a Range
header, while the published Get Blob contract requires 400; mk8.sava returns
400 `InvalidHeaderValue`. Azurite returns the generic `ConditionNotMet` for a
stale copy-source ETag; mk8.sava retains the published Blob-specific
`SourceConditionNotMet` code. The published common error catalog and modern
error-header contract also require `ConditionNotMet` in `x-ms-error-code` on
conditional 304 reads; mk8.sava now emits it with an empty response body.
The header is emitted for service versions `2017-07-29` and later, matching
the published version boundary; older versions omit it. Conditional 304
responses remain bodyless, while ordinary error responses retain XML bodies.
A separate local Get Blob contract test exercises both transactional MD5 and
CRC64 at the inclusive 4 MiB range limit, rejects a 4 MiB + 1 byte range and
checksum-without-range with 400, and verifies that `x-ms-range` takes precedence
over a simultaneous `Range` header.
The script configures Azurite with the test account key used by mk8.sava and
removes its disposable storage root after success. It requires Node.js 20,
Corepack/Yarn, `curl`, and Python 3 for a free loopback port. The lockfile
is run with Yarn's `--ignore-engines` because newer transitive Azure packages
declare Node 22 while this pinned Azurite and exercised lane run on Node 20.
Azurite is an emulator with documented gaps, especially HNS; a matching
result is evidence only for operations in its
[pinned-version support matrix](https://github.com/Azure/Azurite/blob/v3.35.0/README.md#support-matrix). The published
[Blob REST specification](https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api)
and targeted local tests govern unsupported features.
Live Azure accounts are not a review prerequisite.

An opt-in live differential test uses the .NET Azure Blob SDK pinned to service
version `2023-11-03`. Set `MK8_SAVA_LIVE_AZURE_BLOB_CONNECTION_STRING` in the
test process environment to a **disposable flat-namespace Azure account** using
an account-key connection string (the SAS comparison must sign test URLs), and
run `dotnet test Mk8.Sava.Tests/Mk8.Sava.Tests.csproj --filter Category=LiveAzure`.
Without that variable, xUnit reports the lane as skipped, not passed. It
creates a unique `mk8diff-` container in Azure and mk8.sava, compares the same
upload, properties, tags, full/ranged reads, snapshot/overwrite, block/append/
page blob, listing, and missing-blob error observations, then deletes only those
two containers. A second flat-account scenario creates a separate unique
`mk8diff-auth-` container and compares read-only service SAS access, denied
SAS mutation, stale ETag conditions, lease-enforced metadata writes, and final
state. The connection string is never printed. Separate local-only tests always
exercise both scenarios so the harness cannot silently rot. A second opt-in
lane uses
`MK8_SAVA_LIVE_AZURE_HNS_CONNECTION_STRING` for a disposable HNS-enabled Azure
account. It compares Shared Key nested-path creation, directory/file identity
and permissions, bytes, listing order, and nonempty-directory errors; a local
only test exercises that scenario in every normal run. All live scenarios
remain skipped until their respective account variables are supplied; even
when run, they cover only a subset of the full Azure conformance matrix.

## SAS authorization boundaries

Service, account, and user-delegation SAS tokens use Azure's versioned
strings-to-sign and fail closed when a field is not available in the token's
signed service version. Legacy service SAS forms are retained: pre-2015 tokens
use the historical canonical-resource prefix, response overrides enter the
signature in `2013-08-15`, and IP/protocol restrictions enter it in
`2015-04-05`. A service SAS without `sv` is limited to Azure's one-hour ad hoc
lifetime. Permission letters and account-SAS service/resource sets must be
unique and in Azure's canonical order; permissions introduced by a later
service version fail authentication. An HNS account can issue a directory SAS
(`sr=d`) from version
`2020-02-10`; its required directory depth binds the token to the exact signed
virtual directory and its descendants. Parent and sibling paths do not share
that authorization, and flat-namespace accounts reject directory SAS.

Account-SAS resource types follow the operation rather than only the parent
request URI. Service-wide Find Blobs by Tags and Blob Batch parent requests use
the object resource type (`srt=o`), including a batch submitted through a
container-scoped URI. Container-scoped tag searches continue to use the
container resource type (`srt=c`), ordinary service operations use `srt=s`, and
Undelete Blob retains Azure's documented container-resource exception.

A service SAS may divide its start time, expiry, and permissions between its
URI and a named container access policy, but the same field cannot appear in
both places; duplicates fail with `400 InvalidQueryParameterValue`. Stored
permissions are subject to the same canonical and version gates as permissions
carried by the token. Account and user-delegation SAS cannot reference a stored
access policy.

Get Container ACL with a service version before `2015-04-05` returns
`409 FeatureVersionMismatch` when any stored policy grants create or add
permission. Those policies remain readable from `2015-04-05` onward.

Valid SAS protocol and IPv4 restrictions that exclude the request return
Azure's dedicated `403 AuthorizationProtocolMismatch` and
`403 AuthorizationSourceIPMismatch` errors. A malformed `spr` value, malformed
or reversed `sip` range, IPv6 range, or use before service version `2015-04-05`
instead fails SAS authentication. IP ranges are inclusive and are evaluated
against the request's remote IPv4 address.

A container SAS continues to cover snapshots and versions in the container
without signing their individual identifiers. Snapshot (`sr=bs`) and version
(`sr=bv`) tokens instead bind those identifiers explicitly; version SAS begins
with service version `2018-11-09`. A signed encryption scope (`ses`) is honored
only from version `2020-12-06`, is applied when the caller omits the matching
header, and rejects a conflicting `x-ms-encryption-scope` header. Adding `ses`
to an older token cannot inject an unsigned storage policy.

User-delegation `scid` is signature-validated from version `2020-02-10`.
`saoid` and `suoid` are restricted to HNS accounts. `saoid` is accepted only
when the delegation-key owner has the explicit
`Sava:BearerAuthentication:Principals:<object-id>:CanManageOwnership` grant;
it attributes new file ownership to the signed authorized object ID without
an additional POSIX ACL check. A signed `suoid` currently supports Get Blob,
Get Blob Properties, Get Blob Metadata, and Query Blob Contents for an existing
file (or a missing target beneath traversable parents): the impersonated object
ID needs execute permission on the container root and every parent directory,
and read permission on the
file. Owner and stored named-user ACL entries are evaluated, including the
mask; `suoid` alone does not carry group membership evidence. The
delegation-key owner still needs the explicit ownership grant and the token
still needs `r`; signing or changing the object ID cannot bypass the ACL
check. Other `suoid` operations remain fail-closed pending the full POSIX
authorization model.
From version `2025-07-05`, `sduoid` binds use to the matching bearer object and
tenant without treating that identity proof as an additional RBAC grant. From
version `2026-04-06`, `srh` and `srq` bind request headers and query values,
including URL-encoded commas in query-parameter names. User-delegation SAS
tokens always require their own signed permission and expiry; neither value is
implicitly inherited from the delegation key or issuing principal.

Rename Container implements Azure's `PUT ?restype=container&comp=rename`
contract from service version `2020-06-12`, including the source-container and
source-lease headers. The metadata store moves the container, every blob
generation, uncommitted blocks, and their reachability references in one SQLite
transaction. Blob bytes and content-addressed chunks are never read or copied;
the operation preserves logical and physical storage totals even for a large
container.

The five URL-source write operations accept `x-ms-file-request-intent: backup`
from service version `2025-07-05`. It is mandatory when the copy source is an
Azure Files endpoint authenticated by a bearer token and is forwarded with that
token on every source request. It remains optional for public/SAS Azure Files
sources and for bearer-authenticated non-File URLs; asynchronous and incremental
Copy Blob reject it because Azure exposes the header only on Put Blob, synchronous
Copy Blob, Put Block, Put Page, and Append Block from URL.
Outbound URL-source requests do not follow HTTP redirects. A redirected source
fails with `CannotVerifyCopySource` without contacting the redirect target, so
source bearer tokens and customer-provided encryption-key headers cannot be
forwarded to an authority selected by the source. This fail-closed behavior
needs differential validation against Azure before claiming redirect parity.
Outbound URL-source connections resolve and select a permitted IP at connection
time. By default, only public global-unicast addresses are permitted; loopback,
private, link-local, carrier-grade NAT, multicast, and reserved ranges are
blocked even if a DNS name resolves to them. Set
`Sava:UrlTransferAllowedPrivateHosts` to an array of exact DNS names or IP
addresses to trust a private source endpoint deliberately. Entries have no
scheme, port, or wildcard, and trust only that host (not suffix matches).
Source transfers connect directly rather than through an ambient HTTP proxy,
which could bypass the local address policy. Operators using private Azure
endpoints or a controlled egress proxy must account for this deployment setting;
proxy support with an equivalent enforced destination policy remains open.

Version-aware Get Blob and Get Blob Properties responses expose both
`x-ms-version-id` and `x-ms-is-current-version` from service version
`2019-12-12`. Current reads report `true`, explicit historical-version reads
report `false`, and snapshots and non-versioned blobs omit both headers.

Query Blob Contents accepts Azure's `delimited`/`csv`, JSON, and Parquet input
forms and its delimited/CSV, JSON, and Arrow result forms. Parquet input is read
through authenticated seeks over the deduplicated chunk store and retains at
most one row group, rather than copying the complete blob to memory or a
temporary file. Arrow results use the caller's declared schema and are emitted
as bounded record batches inside Azure's Avro query envelope; the service does
not retain or buffer the complete result set. The normal .NET compatibility
suite exercises multiple Parquet row groups and all six Azure Arrow field types,
including null and empty-result stream shapes, through the official SDK.
The SQL evaluator supports Azure's row-wise arithmetic and comparison
precedence, typed `CAST`, `BETWEEN`, `IN`, `NULLIF`, `COALESCE`,
`CHAR_LENGTH`/`CHARACTER_LENGTH`, `LOWER`, `UPPER`, `SUBSTRING`, and `LIMIT`.
Once a `LIMIT` is satisfied, input enumeration and its underlying blob read are
cancelled instead of scanning the remainder of the object.
Date expressions include `DATE_ADD`, `DATE_DIFF`, `EXTRACT`, `TO_STRING`,
`TO_TIMESTAMP`, and `UTCNOW`; `TRIM` accepts Azure's `BOTH`, `LEADING`, and
`TRAILING` forms with caller-selected characters.
`COUNT(*)`, `COUNT(expression)`, `SUM`, `AVG`, `MIN`, and `MAX` retain only
constant accumulator state as input streams through the evaluator; matching
rows are never collected in memory.
JSON queries support Azure's `BlobStorage[*].path[*]` table descriptors, source
aliases, nested object members, zero-based array indexes, and distinct
`IS MISSING`/`IS NOT MISSING` semantics. An explicit JSON null remains different
from a property that is absent from the input object.
`Sys.Split` scans delimited input as bounded raw UTF-8 buffers, honors quoted and
escaped record separators, and emits exact byte counts at complete-record
boundaries. It does not retain a requested 10 MiB-or-larger batch in memory.

## Account capabilities

Storage-account capabilities are configured independently so one deployment can
serve both flat-namespace and hierarchical-namespace accounts. Hierarchical
namespace is immutable account identity in Azure and must likewise be configured
before clients write data:

```json
{
  "Sava": {
    "AccountCapabilities": {
      "datalakeaccount": {
        "HierarchicalNamespaceEnabled": true,
        "HierarchicalNamespaceBlobIndexTagsEnabled": false,
        "HierarchicalNamespaceBlobSnapshotsEnabled": false,
        "LastAccessTimeTrackingEnabled": true
      },
      "complianceaccount": {
        "VersioningEnabled": true,
        "ImmutableStorageWithVersioningEnabled": true
      }
    }
  }
}
```

Every capability entry must name an account in `Sava:Accounts`. The Blob endpoint
reports the setting through Get Account Information and applies the corresponding
Blob API restrictions and hierarchical listing/property shape. The separate Data
Lake `dfs` protocol is outside this service's endpoint boundary.

The configured hierarchical-namespace mode is recorded in the metadata database
for each account on startup. A later restart with the opposite mode fails before
serving requests; changing it requires a separately designed offline migration,
not a configuration flip. Existing schema-6 roots did not record this mode, so
their first schema-7 startup binds the then-configured value. Operators upgrading
such roots must preserve the mode used before that first startup.

Configured account names follow Azure's 3–24-character lowercase ASCII letter
or digit rule. Startup rejects other names, including path separators, rather
than allowing an account identity to alter the chunk-storage path layout.

`AllowSharedKeyAccess` defaults to `true`, matching an Azure account whose
management-plane property is unset. Setting it to `false` rejects Shared Key and
Shared Key Lite headers plus service and account SAS tokens with
`KeyBasedAuthenticationNotPermitted`. Bearer authorization and user-delegation
SAS remain available, as do anonymous reads from containers already configured
for public access.

`Sava:DataEncryptionKeys` may assign a stable base64 key of at least 256 bits
to each configured account. It derives the account's at-rest chunk key; the
corresponding `Sava:Accounts` key remains the Shared Key credential used to
authorize requests. With a stable data key, the Shared Key credential can be
rotated without rewriting or losing the account's encrypted chunks. A new
deployment should provision its data key independently of the Shared Key
credential. The default remains the existing behavior: an account without an
explicit data key uses its Shared Key credential for chunk encryption.

To rotate an existing account that used this default, first configure
`DataEncryptionKeys:<account>` to the *current* `Accounts:<account>` value and
verify a representative full read and a backup. Only then replace the Shared
Key credential while retaining the same data key. Never replace or remove a
data key while chunks in its domain are reachable or physically reusable:
old content would become unreadable, and an orphaned chunk could obstruct a
later upload of identical bytes. Backups fingerprint the effective data key and
reject restore with the wrong key. The live root records the effective key
fingerprint for each account or cross-account sharing domain with reachable or
physically reusable chunks and rejects a mismatch at startup, before accepting
requests. Startup also inventories standalone chunks and registered pack
locators, so crash-abandoned but unreferenced extents keep their key requirement
until garbage collection removes them. On the first start after an older
metadata schema, it verifies every reachable and physically reusable encrypted
chunk before recording the fingerprints. This one-time migration check can take
longer on a large root; regular startups use the recorded fingerprints instead
of repeating it. Online data-key rotation is still outstanding. Preserve data
keys separately from backups.

`AllowSharedKeyAccessForServices:Blob:Enabled` mirrors Azure's Blob-specific
management setting. When set to `false`, it denies Blob Shared Key, Shared Key
Lite, service SAS, and account SAS while bearer and user-delegation SAS continue
to work. An unset Blob setting preserves the account-wide default. The
account-wide `AllowSharedKeyAccess=false` still denies key-based Blob access even
when the Blob-specific setting is `true`.

`EnableHttpsTrafficOnly=true` mirrors Azure's storage-account secure-transfer
setting for Blob requests. Every HTTP request to that account, including
anonymous reads, CORS preflights, and static-website requests, receives
`400 AccountRequiresHttps` before authentication; HTTPS requests continue
normally. This setting is opt-in here so local HTTP SDK endpoints remain usable,
although newly created Azure accounts enable secure transfer by default. If TLS
terminates at a proxy, that proxy must forward requests to mk8.sava over HTTPS;
untrusted `X-Forwarded-Proto` headers do not satisfy the policy.

`AllowBlobPublicAccess` is Azure's per-account anonymous-read policy. When
unset, it falls back to the legacy global `Sava:AllowAnonymousPublicAccess`
setting (false by default). An explicit `true` permits containers in that
account to opt into blob or container public access; an explicit `false` blocks
new public ACLs with `409 PublicAccessNotPermitted` and also overrides public
ACLs already stored in that account for anonymous Blob reads. Signed requests
still work, and a configured `$web` static website remains publicly readable,
as in Azure.

Cross-tenant user-bound user-delegation SAS is denied by default. Set
`AllowCrossTenantDelegationSas` on the account only when the delegated user's
tenant is intentionally different from the delegation-key tenant. The setting
is enforced both when `DelegatedUserTid` is requested and whenever the resulting
SAS is used. `RequireUserBoundUserDelegationSas` can additionally audit or deny
every valid SAS that lacks a signed delegated-user object ID. Its companion
`RequireUserBoundUserDelegationSasAction` accepts `None`, `Log` (the default),
or `Block`; log records identify the request and account without recording the
SAS secret.

`SasExpirationPeriod` configures Azure's maximum SAS validity interval as a
.NET duration (for example, `1.12:05:06`). Every ad hoc service, account, and
user-delegation SAS must carry a signed start time and may span no more than the
configured interval. `SasExpirationAction` defaults to `Log`; set it to `Block`
to deny out-of-policy tokens with `AuthorizationFailure`. A service SAS backed
by a stored access policy is exempt, matching Azure's documented limitation,
and the user-delegation key's own lifetime is not evaluated by this policy.

Blob index tags on HNS accounts remain an Azure preview that requires the
`Microsoft.Storage/BlobIndexForHns` feature registration. Set
`HierarchicalNamespaceBlobIndexTagsEnabled` only for an account on which that
preview behavior is intended; its REST surface additionally requires service
version 2024-11-04 or later. Flat-namespace accounts always retain normal blob
index tag support.

Blob snapshots on HNS accounts are also an Azure preview and the preview is no
longer accepting new Azure customers. Set
`HierarchicalNamespaceBlobSnapshotsEnabled` only when deliberately emulating an
account already enrolled in that preview. Without it, mk8.sava rejects snapshot
creation, selectors, listings, source copies, and delete-snapshot options on the
HNS account instead of applying flat-namespace behavior.

Customer-provided encryption keys are unsupported on HNS accounts, per
Microsoft's [storage-account feature support table](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-feature-support-in-storage-accounts#standard-general-purpose-v2-accounts).
Requests carrying the blob's `x-ms-encryption-key` headers are rejected there
on writes and reads, before a new blob can be published. Encryption scopes
remain supported on HNS accounts;
flat-namespace customer-provided-key behavior is unchanged.
The same feature table marks blob versioning and change feed unsupported on
HNS accounts. Startup configuration rejects either capability when
`HierarchicalNamespaceEnabled` is true; it must not advertise a setting whose
Blob behavior is suppressed by the account mode.

`LastAccessTimeTrackingEnabled` emulates Azure's account-level last-access-time
policy. Data writes update the persisted access time immediately. The first data
read in a 24-hour window updates it; later reads in the same window do not. Get
Blob Properties, Get Blob Metadata, Get Blob Tags, and listing operations expose
but do not update the value. Internal copy-source reads participate in the same
tracking rule. Get Blob, Get Blob Properties, XML List Blobs, and Arrow List
Blobs expose the property when their respective response format supports it; the
REST header and XML element require service version `2020-02-10` or later.
Accounts without the capability omit it.

`VersioningEnabled` represents Azure's control-plane blob-versioning setting.
mk8.sava applies it to the account's durable service state at startup. Put Blob,
Put Block List, Copy Blob, and Set Blob Metadata create versions. Append Block
and Put Page mutate the current append/page version, matching Azure's narrower
versioning surface for those blob types.

`ImmutableStorageWithVersioningEnabled` models account-level version-level WORM
and enables it for every container in that account. To model container-level
enablement instead, leave that flag false and list immutable container names in
`ImmutableStorageWithVersioningContainers`. The setting is persisted into newly
created container records, is projected by Get Container Properties and List
Containers from service version `2020-10-02`, and is a prerequisite for blob
immutability policies, legal holds, and immutability headers on writes. Azure
does not permit a data-plane Delete Container request for such a container, so
mk8.sava returns `ContainerImmutableStorageWithVersioningEnabled` even when the
container is empty. Version-level immutable storage requires
`VersioningEnabled` and cannot be combined with hierarchical namespace or
last-access-time tracking; invalid combinations fail configuration validation.

Object replication is supplied as durable deployment configuration, mirroring
Azure's separation between its management plane and Blob data plane. A source
account requires both `VersioningEnabled` and `ChangeFeedEnabled`; the
destination requires `VersioningEnabled`. Neither account may use hierarchical
namespace. Both containers must be created through the Blob API before eligible
objects can replicate:

```json
{
  "Sava": {
    "AccountCapabilities": {
      "sourceaccount": {
        "VersioningEnabled": true,
        "ChangeFeedEnabled": true
      },
      "destinationaccount": {
        "VersioningEnabled": true
      }
    },
    "ObjectReplicationPolicies": [
      {
        "PolicyId": "70dc1326-15a2-45f4-9408-487f6581f204",
        "SourceAccount": "sourceaccount",
        "DestinationAccount": "destinationaccount",
        "EnabledAt": "2026-09-22T00:00:00Z",
        "Rules": [
          {
            "RuleId": "a2455a77-8f44-4d22-b07e-a03203ae64f0",
            "SourceContainer": "incoming",
            "DestinationContainer": "replica",
            "PrefixMatch": [ "documents/", "media/" ],
            "MinimumCreationTime": "2026-09-01T00:00:00Z",
            "ReplicateBlobTags": true
          }
        ]
      }
    ]
  }
}
```

Policy and rule IDs are GUIDs. Each account pair has at most one policy, each
policy has at most 1,000 rules, and a rule has at most 10 nonempty prefix
filters. `EnabledAt` records when the management-plane policy became active and
is the default creation-time boundary; `MinimumCreationTime` deliberately
overrides that boundary when a rule should include older blobs.

Maintenance asynchronously copies only block blobs and preserves bytes,
versions, HTTP properties, application metadata, and optionally index tags.
Snapshots are not copied. Archive or rehydrating sources and blobs encrypted
with a customer-provided key are reported as failed, as Azure does. The source
projects `x-ms-or-{policy-id}_{rule-id}` and List Blobs `OrMetadata`; the
destination projects `x-ms-or-policy-id`. Replication progress and the exact
source-to-destination generation mapping are committed atomically in SQLite, so
restart and retry cannot manufacture duplicate versions.

An active destination rule rejects Blob writes with
`BlobOperationNotSupported`. Reads, Set Blob Tier, and Delete Blob remain
available. Deleting a destination copy does not cause unchanged source data to
reappear; a later source change may create a new copy. Removing or remapping a
configured policy forgets its execution ledger without deleting copies already
delivered by that policy.

HNS accounts support the Blob REST encryption-context system property. Put Blob
and Put Block List accept `x-ms-encryption-context` from service version
2021-08-06, reject values longer than 1,024 characters, and clear the property
when a replacement omits the header. Get Blob and Get Blob Properties expose it
from 2021-08-06; List Blobs exposes `EncryptionContext` from 2021-06-08. The
header is rejected on flat-namespace accounts and on copy operations, matching
the Azure Blob operation-specific contract.

HNS file and implicit-directory ownership is persisted with each path. Shared
Key, account SAS, and service SAS creations use `$superuser`; bearer and user
delegation SAS creations use the authenticated object ID. A new path inherits
its owning group from its parent directory (or the container root), while an
overwrite keeps the existing path's owner and group. The current Blob-only
surface retains the default POSIX permissions and ACL for files and
directories. Stored custom access ACLs are evaluated for owner, named user,
owning/named group, mask, and other entries, and survive blob overwrite. There
is an offline operator provisioning path for existing HNS container roots and
paths. Stop the service, prepare a JSON manifest outside its data root, then
run the following command with the same account, data-root, and encryption-key
configuration used by the service:

```bash
dotnet Mk8.Sava.API.dll --hns-acl-apply /path/to/hns-acls.json
```

```json
{
  "schemaVersion": 1,
  "entries": [
    {
      "account": "datalakeaccount",
      "container": "documents",
      "path": "",
      "accessAcl": "user::rwx,user:reader-object-id:--x,group::r-x,mask::r-x,other::---"
    },
    {
      "account": "datalakeaccount",
      "container": "documents",
      "path": "reports",
      "accessAcl": "user::rwx,user:reader-object-id:--x,group::r-x,mask::r-x,other::---"
    },
    {
      "account": "datalakeaccount",
      "container": "documents",
      "path": "reports/annual.txt",
      "accessAcl": "user::rw-,user:reader-object-id:r--,group::r--,mask::r--,other::---"
    }
  ]
}
```

Every target must already exist, including any parent directory named in the
manifest; an empty `path` addresses the container root. A directory or root
may include a complete set of `default:` ACL entries. Those entries template
the access ACL of subsequently created children, and new directories also
inherit the default ACL. They do not retroactively change existing children;
files cannot carry default entries. The command rejects non-HNS accounts,
malformed ACLs, duplicate targets, and manifests larger than 4 MiB or 4,096
entries. It commits all changes or none,
and a changed ACL advances that target's ETag and Last-Modified timestamp.
An unchanged ACL is idempotent. The command holds the data-root process lease,
so it cannot run alongside a live service on that root. This is not an
application-facing API or the separate `dfs` ACL mutation protocol. Blob-surface
list/mutation authorization remains incomplete. Live Azure validation is
optional, not a review prerequisite.
For Get Blob, Get Blob Properties, Get Blob Metadata, and Query Blob Contents,
a trusted bearer principal without a configured RBAC read grant can fall back
to the same root/parent
execute and file-read ACL check used for `suoid`, but only when its signed
token contains a valid Entra `oid` claim. A `sub` or application ID is not an
ACL identity. A configured RBAC read grant still authorizes without an ACL
check. Signed bearer `groups` claims can satisfy stored owning/named-group
entries; group memberships absent from the token are not resolved. Bearer ACL
fallback for other operations remains incomplete.
For current HNS blobs, bearer `oid` and signed user-delegation `suoid` ACL
fallback can authorize Put Blob, Put Block, Put Block List, Set Blob Metadata,
Set Blob Properties, Set Blob Tier, Set Blob Expiry, and Delete Blob through
write/execute on the immediate parent directory plus execute on ancestors.
Metadata, property, tier, and expiry mutations recheck that parent permission
immediately before storage mutation. These
rules follow Microsoft's documented
[HNS ACL create/update/delete permissions](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control#common-scenarios-for-acl-permissions).
Unsupported ACL-only mutation shapes still fail closed.
For both bearer fallback and signed `suoid` reads, the ACL decision is bound to
the blob generation served by the endpoint. A replacement or a blob created
after authorization cannot turn an allowed read of an older or missing target
into access to new content.
HNS identity projection follows the Blob REST request shape. List Blobs accepts
`x-ms-upn` only when `include=permissions` is present. Get Blob and Get Blob
Properties accept it only from service version 2023-11-03. The value must be a
Boolean and the header is rejected for flat-namespace accounts. Configure
`Sava:BearerAuthentication:Principals:<object-id>:UserPrincipalName` for each
user whose object ID should be projected when `x-ms-upn=true`; unknown object
IDs and `$superuser` remain unchanged. The mapping is evaluated when a response
is written, so changing a configured name does not rewrite stored paths.
For HNS List Blobs, a file in an intermediate component of `prefix` is a path
conflict, not an empty successful enumeration. The flat namespace still permits
independent blobs named `file` and `file/child`. This follows the published
[List Blobs prefix rule](https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs#uri-parameters);
the exact Azure error-code parity for that HNS edge is not yet established by
the documentation.

Blob expiry is likewise confined to HNS files. Set Blob Expiry is available
from service version 2020-02-10 and rejects directories. Put Blob, Put Block
List, and Put Blob From URL accept `x-ms-expiry-option` and `x-ms-expiry-time`
from 2023-08-03. Put Block List preserves the current expiry when those headers
are omitted; `NeverExpire` removes it explicitly. Get Blob, Get Blob Properties,
and List Blobs expose the expiry from 2020-02-10 only for HNS accounts. Expired
files are reclaimed as expiration rather than converted into soft-deleted data.

## Archive copy and rehydration

Copy Blob accepts an archived block-blob source only when the request supplies
an online destination tier. The destination is published in Archive with its
copy status, target archive status, and rehydration priority visible through
normal Blob properties. Copy completion and rehydration are separate durable
transitions: copied bytes remain unreadable until the destination reaches its
target tier. `Sava:AsyncCopyCompletionDelay`,
`Sava:StandardRehydrationDelay`, and `Sava:HighPriorityRehydrationDelay` control
those transitions.

Omitting `x-ms-rehydrate-priority` defaults to Standard. Set Blob Tier can
upgrade a pending Standard rehydration to High for service version 2020-06-12
or later; older versions preserve the first priority. A High priority is never
lowered to Standard. Aborted or failed copies clear their pending rehydration
state, while a successful copy retains its copy properties across subsequent
tier changes.

## Health and metrics

- `GET /health/live` reports that the process is running.
- `GET /health/ready` checks SQLite availability and the last incremental
  reachable-chunk integrity cycle. It returns HTTP 503 after a reachable chunk
  is found missing or fails authenticated decoding.
- `GET /metrics` emits Prometheus text for request count and duration, 5xx
  responses, logical and physical storage, staging use, integrity findings, and
  lifecycle maintenance. These operator endpoints contain no credentials or
  blob names, but deployments should still restrict them to the monitoring
  network.

The existing chunk, staging, and metadata byte gauges measure serialized file
lengths. On Linux, `mk8_sava_storage_allocated_root_bytes` additionally measures
actual 512-byte filesystem blocks under the complete data root, including
SQLite journals, staging files, directories, and the root lease. It does not
follow symlinks or count a hard-linked inode twice. The value is refreshed by a
sampled inventory no more often than `Sava:PhysicalUsageScanInterval` (one minute
by default), not on every maintenance pass. Each pass advances at most
`Sava:PhysicalUsageEntriesPerMaintenancePass` filesystem steps (1024 by
default), including directory enumeration and entry inspection. On a large
root, the inventory spans multiple passes and the previous complete sample
remains published until the next one finishes. Before the first complete
sample, physical gauges and the last-scan timestamp are zero. The metric
`mk8_sava_storage_physical_last_scan_timestamp_seconds` reports when the last
complete inventory finished. Physical chunk, staging, metadata, allocation,
and unique-chunk gauges share that sampling cadence; logical blob/block bytes,
record counts, and reachable-chunk counts still update each maintenance pass.
The allocation gauge is absent on platforms without the Linux `statx` block
accounting API; `mk8_sava_storage_allocation_available` is 1 only after a
valid measurement and distinguishes an unmeasured or unsupported host from
an empty root. A full inventory still adds I/O on large roots, but it no longer
monopolizes one maintenance pass. Like any live filesystem walk, it is a sampled
inventory rather than an atomic snapshot of concurrent writes.
The serialized chunk and pack inventory also skips linked files and directories,
so an external or cyclic link cannot inflate or trap its maintenance scan.

Integrity scanning is controlled by
`Sava:IntegrityScanChunksPerMaintenancePass`. Customer-provided-key chunks are
structurally checked and counted separately because mk8.sava deliberately does
not retain those keys. An authorized read supplies the key and performs the
full AES-GCM, decompression, digest, and length verification.

Lifecycle work uses independent keyset cursors. Each pass examines at most
`Sava:BlobRecordsPerMaintenancePass` blob/version/snapshot records and
`Sava:ContainerRecordsPerMaintenancePass` container records for due copy,
rehydration, expiry, and retention transitions. It also removes at most
`Sava:UncommittedBlocksPerMaintenancePass` expired uncommitted blocks, oldest
first. A full cursor cycle repeats, so records created or moved behind a cursor
are deferred rather than lost.

Crash-abandoned `.tmp` files are removed after
`Sava:AbandonedStagingRetention`, up to
`Sava:MaximumStagingFilesPerMaintenancePass` deletions per pass. A file still
held by an active request cannot be reclaimed.

Unreachable immutable chunks are considered in filesystem key order, up to
`Sava:GarbageCollectionChunksPerMaintenancePass` per pass. Reachability is
checked through the transactional metadata index both before and after an
exclusive chunk reservation, so reclamation remains bounded without racing a
concurrent publication. A complete sweep resumes from the beginning; chunks
created behind an active cursor are therefore considered during the next
sweep. Deleting a standalone chunk also removes its empty hash-parent
directories, stopping at the `chunks/` root. Publication and directory pruning
share a gate, so a concurrent writer can safely recreate a pruned path before
publishing its chunk. At startup, a one-time bottom-up sweep also removes
historical empty directories left by older packed-only writes. It never follows
directory symlinks, never removes the `chunks/` root, and preserves any
nonempty directory; on very large roots, its directory scan adds startup time.

Reachable chunks older than `Sava:BackgroundCompressionMinimumAge` are revisited
in cursor order, up to `Sava:BackgroundCompressionChunksPerMaintenancePass`.
The service authenticates and reconstructs the plaintext, writes a candidate at
`Sava:BackgroundCompressionQuality`, verifies it byte-for-byte, and atomically
publishes it only when it saves at least
`Sava:BackgroundCompressionMinimumSavingsBytes`. Active readers, backups, and
writes pin the old representation and cause that attempt to be deferred. This
physical rewrite does not update blob ETags or last-modified values.
Customer-provided-key chunks are excluded because the service does not retain
their keys. Recompression totals and serialized chunk-file bytes saved are
exported through `/metrics`.

Chunks no larger than `Sava:SmallChunkPackingThresholdBytes` are placed in
authenticated append-only packs when `Sava:EnableSmallChunkPacking` is enabled.
`Sava:ChunkPackTargetBytes` and `Sava:ChunkPackMaximumRecords` bound each pack.
Metadata publishes a chunk locator only after its complete framed record has
been flushed durably, and reads verify the frame identity and digest before the
normal AES-GCM, decompression, plaintext digest, and length checks. Packs older
than `Sava:ChunkPackSealAge` are sealed; maintenance considers at most
`Sava:ChunkPacksPerMaintenancePass` sealed packs and rewrites one only when its
unreachable bytes meet both `Sava:ChunkPackCompactionMinimumSavingsBytes` and
`Sava:ChunkPackCompactionMinimumDeadRatio`. Locator replacement is atomic, and
the old pack is deleted only after that transaction commits.
Packed-only chunks do not create unused hash directories under `chunks/`;
standalone chunk publication creates those directories only when needed.

SQLite connections use WAL mode with `synchronous=FULL` for durable commits.
After a WAL reset, `journal_size_limit` bounds retained WAL allocation to 1 MiB;
an active transaction can still make the WAL temporarily larger. The storage
byte gauges in `/metrics` sum serialized file lengths, not filesystem blocks.
For measured allocation, run the Linux benchmark below, which uses GNU `du`
over the complete live root and a raw-file baseline, including directory,
SQLite, chunk, pack, and staging allocation. It also samples allocated staging
space and process working set every 10 ms during each workload. Those sampled
peaks are lower bounds, not exact maxima; the in-process test host's working
set is not a separate production-server measurement. MSAVA remains an unmeasured
comparison, so do not use this benchmark alone for production performance claims:

```bash
dotnet test Mk8.Sava.Tests/Mk8.Sava.Tests.csproj \
  --filter FullyQualifiedName~StorageAllocationBenchmarkTests \
  --logger 'console;verbosity=detailed'
```

The benchmark uses ordinary .NET Azure Blob SDK calls with production chunk
sizes and reports cumulative logical bytes, allocated bytes per storage
component, upload/read latency, process CPU time, and process working set after
exact duplicates, shifted partial sharing, retained versions, small files, and
incompressible files. Timings are an in-process diagnostic, not a network or
MSAVA production-performance claim; rerun them on the target filesystem and
with a separately characterized MSAVA baseline before setting release budgets.

## Create and validate a backup

The backup command runs without starting the HTTP listener. It takes a
transactionally consistent SQLite snapshot while all reachable immutable
chunks are pinned against garbage collection, verifies their storage integrity,
copies exactly that root set, and publishes the backup directory only after a
versioned manifest and all file hashes are durable. On Unix, each newly created
backup directory and copied file entry is also flushed before the final backup
or restored-root rename is published and its parent directory flushed. Windows
uses write-through publication for the final rename, but directory-entry
durability remains unverified, as it does for live chunks.

```bash
dotnet Mk8.Sava.API.dll --backup-create /srv/backups/mk8-sava-2026-09-21
dotnet Mk8.Sava.API.dll --backup-validate /srv/backups/mk8-sava-2026-09-21
```

The destination must not already exist and must be outside `Sava:DataPath`.
Store the completed directory on independent durable media. Validation checks:

- the backup and metadata schema versions;
- SQLite's own integrity check and the logical inventory recorded in the
  manifest;
- exact agreement between metadata reachability and the chunk manifest;
- SHA-256 and length for the metadata database and every encrypted chunk file;
- absence of undeclared chunk files and symbolic-link traversal; and
- fingerprints of the effective account data or cross-account encryption keys
  needed to read the restored extents; and
- for schema-7 backups, agreement between each configured account's
  hierarchical-namespace mode and the mode recorded in the metadata database.

Key material is never written to the backup manifest. Preserve the deployment's
data encryption keys (or legacy account keys when no separate data key exists)
separately; a fingerprint match cannot reconstruct a lost key.
Customer-provided keys remain the responsibility of their callers.

## Restore

Restore is deliberately offline and non-destructive. Stop the service, choose a
new nonexistent data path, configure the same required data encryption keys,
then run:

```bash
Sava__DataPath=/srv/mk8-sava-restored \
  dotnet Mk8.Sava.API.dll --restore-from /srv/backups/mk8-sava-2026-09-21
```

The command fully validates the source, copies into a private sibling staging
directory with hash verification, checks the copied SQLite database, and then
atomically renames that directory into the configured target. It refuses to
overwrite even an empty target. Start the service only after the command
succeeds, exercise representative full and ranged reads, and retain the old
root until the restored deployment is accepted.

If validation reports a missing or corrupt file, do not edit the manifest or
substitute an extent with a similar name. Recover a complete backup or a known
good durability copy. Deduplicated extents can affect multiple logical blobs.

## Format upgrades and rollback

Metadata uses an explicit SQLite `user_version`; the current metadata schema is
version 7 and the backup container format is version 1. Schema 2 adds
transactionally maintained chunk-reference indexes and logical-length counters;
schema 3 adds a transactional blob-tag search index; schema 4 adds authoritative
pack and packed-chunk locator tables; schema 5 adds object-replication state;
schema 6 adds durable data-key fingerprints for reachable encryption domains;
schema 7 records each configured account's hierarchical-namespace mode.
The JSON blob/block manifests remain
authoritative and startup and backup validation check the derived indexes against
them. The service migrates schema 1, 2, or 3 on startup and can validate or
restore backups using an older supported schema. Backups materialize packed
records as canonical standalone chunk files and remove pack locators from the
copied database, so the existing backup format remains self-contained. The
service refuses metadata newer than it understands, and restore refuses
unsupported backup or schema versions.

Before deploying a build that changes either format:

1. create and validate a fresh backup with the currently running build;
2. stop all writers;
3. retain the old binary, configuration, data root, and validated backup;
4. deploy the new build and allow its documented migration to finish;
5. run integrity/read checks before returning traffic; and
6. if rollback is required, restore the pre-upgrade backup into a new root with
   the old binary rather than attempting to downgrade mutated metadata in
   place.

Never delete the old root or the independent backup merely because an upgrade
or restore command completed.
