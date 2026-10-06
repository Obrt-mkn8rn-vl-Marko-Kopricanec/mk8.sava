# Gateway and Application deployment

Publish and run `Mk8.Sava.Gateway` and `Mk8.Sava.Application` separately. The
`Mk8.Sava.API` project is the Gateway protocol library, not a deployable server.
Use a matched source/artifact pair: the private transport rejects a different
protocol version or authentication/account policy fingerprint.

## Responsibilities and availability

Gateway owns Azure Blob HTTP authentication, authorization, protocol parsing,
response rendering, URL-copy egress and bounded private request staging. It has
no access to the durable data root, separately configured data-encryption keys
or SQLite. Application
owns all authoritative storage, compression, deduplication, leases/publication,
maintenance, durable analytics and offline backup/restore commands. Application
holds the exclusive data-root lease even when no Gateway is running. Multiple
Gateways may use one Application; two Applications may not open one root.

Each process has its own `/health/live`, `/health/ready` and `/metrics`. Gateway
liveness is independent of Application. Gateway readiness requires a successful
authenticated backend readiness call with matching policy/protocol; unavailable
Application returns 503 without killing Gateway. Application remains ready and
maintains storage without Gateway. These are independent process lifecycles,
not an offline queue: Blob operations need Application to complete immediately.
Do not add proxy/RPC retries to a possibly committed append, lease or mutation.
An interrupted response may leave an acknowledged-or-unknown committed write;
use the normal Blob conditions and operation-specific client recovery.

## Transport and configuration

Both components configure `ApplicationTransport:Endpoint`, such as
`http://127.0.0.1:18581/internal/application`, and
`ApplicationTransport:AccessKeyFile`. The file contains base64 text decoding to
32–512 cryptographically random bytes. Keep it outside both the data root and
Gateway staging, protected from other users. A transport key grants access to
the trusted Application boundary; do not give it to Blob clients or put it in
command arguments, repository files, logs or evidence uploads.

Plain HTTP is permitted only for a literal loopback IP, not `localhost`, a
private RFC1918 address, or a DNS name that happens to resolve to loopback.
Application binds only the literal-IP authority in its configured Endpoint and
the configured RPC path must be `/internal/application`; a separate bounded
`/internal/application/control` route is derived from it for session Touch/Close
and readiness only. `ASPNETCORE_URLS` and inherited
Kestrel endpoints cannot broaden its listeners. Non-loopback Application
listeners require HTTPS with `ApplicationHosting:CertificateFile` pointing to
a protected PFX and, when needed, `ApplicationHosting:CertificatePassword`.
The Gateway may use the certificate's trusted DNS name in its HTTPS Endpoint;
Application's binding Endpoint uses the corresponding literal listen IP.
Normal certificate chain/name validation applies. There is no plaintext remote
fallback, disabled certificate validation, proxy inheritance or redirect.
Firewall the private listener to only the required Gateway/operator sources.
Application operator routes permit anonymous loopback requests only; remote
requests need the same private-transport authentication headers. The Gateway's
typed readiness probe supplies those headers, not public user credentials.

The shared policy must match: default account, account signing keys, account
capabilities, object-replication policies, anonymous access policy, maximum
logical request body size and Bearer authentication/role/Graph policy. Data path,
data-encryption keys, compression settings and process-specific settings need
not match. A mismatch fails closed; it is not fixed by disabling the check.
Change a shared policy on both hosts in one planned stop/start boundary.
The private protocol is `mk8-sava-rpc-2`; older/different hosts fail preflight.
The defaults `ConnectTimeout=00:00:05`, `RequestTimeout=00:10:00` and
`MaximumControlFrameBytes=67108864` are private-transport budgets. Blob content
streams with backpressure separately from bounded metadata/control frames.
Large or slow transfers must fit the chosen request timeout; size ingress and
staging budgets for the workload without removing authenticated Blob limits.

Application read sessions default to an idle timeout of two minutes (supported
range one second to one hour). Gateway renews an active session at one third of
that negotiated timeout, at most every 30 seconds, including query CPU/output
gaps. Each renewal attempt is limited to one third of the idle lifetime, not the
long upload deadline. Lost or hung renewal aborts that request instead of continuing after Application
has released the global query/read admission and content pin. A disappeared
Gateway cannot keep pins forever: inactive abandoned sessions expire, while
in-flight Application stream operations retain their pins until they finish.

Application additionally limits private RPC ingress across all Gateways before
decoding metadata: the ordinary lane defaults to 16 active and 128 queued calls;
the independent control lane defaults to 4 active and 32 queued calls with a
4 KiB frame cap and a strict parsed-method whitelist. Queueing has a finite
deadline and saturation returns 503; health/metrics do not consume these lanes.
The lease lasts through the complete input/output stream. These bounds prevent
unbounded aggregate requests but are not a measured RSS budget: metadata buffers,
decoded objects, query memory and native codecs need representative sizing on
the target host, particularly with the 64 MiB ordinary control-frame ceiling.

Gateway initializes its private staging directory before accepting requests.
On restart it reclaims only its recognized, unlocked flat scratch folders;
active owners, links, nested or unrelated directories are never swept. Its
metrics separately expose admission occupancy/rejections and staging
quota-accounted bytes/limit/rejections. Scrape Application for authoritative
storage gauges, not Gateway.
An admitted storage request keeps its Gateway permit through final metrics,
logging and best-effort analytics persistence, not just the Blob endpoint.
Analytics has an independent five-second cancellation deadline, including for
failed or disconnected client requests; ordinary analytics failures do not
rewrite the completed Blob response. Health/metrics remain outside public
storage admission, and saturation still produces a mapped, counted 503.

## Linux systemd templates

The files in `linux/` are templates, not an installer and not an instruction to
change the existing deployment automatically. Create distinct system users
`mk8sava-application` and `mk8sava-gateway`, their private primary groups, and a
restricted supplemental `mk8sava-rpc` group. Make only those two service users
members of the RPC group. Deploy a matched release as:

```text
/opt/mk8-sava/releases/<source-commit>/
    Application/   # Application publish output
    Gateway/       # Gateway publish output
/opt/mk8-sava/current -> releases/<source-commit>
/etc/mk8-sava/
    policy.env
    application.env
    gateway.env
    application-access.key
```

Replace the policy/account placeholders before use. Root owns the three `.env`
files with mode 0600; systemd reads them before changing service identity.
Create the transport key without printing it, then allow the RPC group to read
that exact file:

```bash
sudo openssl rand -base64 -out /etc/mk8-sava/application-access.key 32
sudo chown root:mk8sava-rpc /etc/mk8-sava/application-access.key
sudo chmod 0640 /etc/mk8-sava/application-access.key
```

Protect `/etc/mk8-sava` from non-service users. Data and backup paths are private
to Application, never the RPC group. The templates use a mode-0700 Application
state directory and a mode-0700 Gateway cache directory for ephemeral staging.
The cache is not authoritative storage or a backup; size its filesystem for the
chosen staging budget. A small `/run` tmpfs is unsuitable for multi-GiB staging
unless intentionally provisioned for it. `MaximumStagingBytes` is a cap, not a
reservation of free disk space. Adjust the explicit
`InaccessiblePaths` denial in Gateway if the real durable root is elsewhere;
`ProtectSystem=strict` alone prevents writes, not reads. If retaining an existing
data root outside the template StateDirectory, grant Application ownership and
an exact `ReadWritePaths` exception after validating that root, with no Gateway
read permission. Do not recursively change a broad parent directory.

The Application template limits IP traffic to loopback. For a remote TLS
topology, replace that allowlist with only the required private addresses and
configure the HTTPS certificate. Gateway intentionally has no blanket
loopback-only IP rule: authorized URL copies and configured Entra/Graph
resolution may require outbound HTTPS. Apply destination controls consistent
with those functions; storage URL-copy SSRF protection remains active.
The Gateway unit has ordering only, not `Requires`, `BindsTo` or `PartOf` on
Application, so stopping Application does not stop Gateway.

After installing adapted units and protected configs:

```bash
sudo systemctl daemon-reload
sudo systemctl start mk8-sava-application.service
curl --fail http://127.0.0.1:18581/health/ready
sudo systemctl start mk8-sava-gateway.service
curl --fail http://127.0.0.1:18580/health/ready
```

`Type=simple` means a started unit is not itself a readiness verdict. Check
readiness and actual authenticated SDK operations before returning traffic.
Do not expose a new public listener merely to pass a deployment checklist.
When clients require HTTPS, prefer a configured Gateway Kestrel TLS listener.
A TLS-terminating ingress must preserve the required Blob authority/scheme/IP
semantics through explicitly trusted configuration; do not assume arbitrary
`Forwarded`/`X-Forwarded-*` headers are trusted or that an HTTPS-only account or
SAS will accept an HTTP backend hop. The Application Endpoint is a private RPC
address, never the Blob endpoint supplied to SDK clients.

## Portable Linux and Windows launch

The publish outputs are framework-dependent and require the matching .NET 10
ASP.NET Core runtime. From the repository root, publish both components for the
actual target, not one oversized API executable:

```bash
dotnet publish Mk8.Sava.Application/Mk8.Sava.Application.csproj -c Release -r linux-x64 --self-contained false -p:Mk8StrictAnalyzers=true -o artifacts/publish/Application
dotnet publish Mk8.Sava.Gateway/Mk8.Sava.Gateway.csproj -c Release -r linux-x64 --self-contained false -p:Mk8StrictAnalyzers=true -o artifacts/publish/Gateway
```

Use `win-x64` instead on Windows. Keep complete output directories, including
native libraries, runtimeconfig and dependency files. Configure protected
`appsettings.json` in each process content directory, or inject the same named
settings with `__` separators. Linux's application env template uses root-only
systemd environment-file syntax; do not blindly source it as shell code.
For a portable local loopback launch, set the shared account/policy and key-file
settings for both shells. In the Application shell set its existing
`Sava__DataPath` and data keys, then execute
`./Application/Mk8.Sava.Application` (Linux) or
`& .\Application\Mk8.Sava.Application.exe` (PowerShell). In the separate Gateway
shell set `Gateway__StagingPath` to a private scratch directory and
`ASPNETCORE_URLS=http://127.0.0.1:18580`, then execute
`./Gateway/Mk8.Sava.Gateway` or `& .\Gateway\Mk8.Sava.Gateway.exe`.
Protect the transport key/config files with appropriate service-account ACLs
on Windows; do not grant `Users` or `Everyone` access. Gateway may be started
first: `/health/live` stays 200 and `/health/ready` stays 503 until Application
is ready. Application may run alone for storage maintenance or offline operator
work. Neither portable process is a Windows Service registration; use the
operator's supported service wrapper with the same separate identities and
graceful-stop budget when unattended operation is required.

`Test-PublishedHosts.ps1` executes the actual native apphosts on the current OS
using isolated loopback ports, a disposable storage root and a random transport
key. It checks each host alone, protected ordinary/control RPC, exact full/ranged
storage operations, native Parquet query rows, and Gateway survival after backend
loss. It then invokes Application-only create/validate/restore commands on a
disposable root and verifies restored bytes/query results through the original
Gateway process. It terminates
only its own child processes and removes its private temporary root. Evidence
contains logs/results, never the key or generated secret configuration:

```powershell
./deploy/Test-PublishedHosts.ps1 -PublishRoot artifacts/publish -EvidencePath artifacts/published-smoke
./deploy/Write-ReleaseManifest.ps1 -PublishRoot artifacts/publish -RuntimeIdentifier linux-x64 -OutputPath artifacts/release-manifest.json
```

The release manifest binds every published file, including native dependencies,
to the source commit/tree, target RID and SHA-256. Hosted CI runs both native
publish smokes on their corresponding Linux/Windows runner; it does not imply
Azure parity, production latency or deployment-specific power-loss guarantees.

## Existing-root upgrade and rollback

The host split changes process ownership and configuration, not the chunk,
pack or metadata formats. Preserve all effective data keys: when a root has no
separate `DataEncryptionKeys` value, the legacy account signing key is also its
data key. Do not generate a replacement key during migration. An account-key
change without preserving its effective data key can make stored data
unreadable. Keep the two components' account signing policy synchronized.
Gateway necessarily holds account signing keys to authenticate SharedKey/SAS.
On a legacy root whose data key is that same account key, process/filesystem
separation is not cryptographic key separation. Preserve that effective key
during upgrade; do not claim otherwise or silently generate a new data key.
For independent key isolation, use Application-only `DataEncryptionKeys` and a
planned account-key rotation that retains the root's original effective data
key. No such rotation is performed by these templates or qualification tests.

1. Record the existing source/artifact IDs, unit/config, listener and exact
   durable root. Create and validate an independent backup with the old build
   while obeying its exclusive-writer rules. Preserve encrypted key material
   separately from the backup manifest.
2. Stop/drain the old server and all root-owning operator processes. Never run
   it alongside the new Application on the same root. Do not point Gateway
   staging into that root.
3. Preserve the root and effective keys unchanged; give only Application access
   to them. Retain the old binaries, config, unit and validated backup.
4. Start the new Application against that root, check integrity/readiness, then
   start its matched Gateway and check authenticated upload/list/full/range,
   conditions, SAS and any enabled HNS/copy functions. Check backup/restore on
   a different disposable root; no client traffic means readiness alone is
   insufficient evidence.
5. If reverting, drain/stop both new processes before starting the retained old
   server. Reuse a root only if its schema remains understood by the old binary.
   For any schema/format upgrade, restore the pre-upgrade backup to a new root;
   never downgrade modified SQLite metadata in place. Switching a release link
   alone is not a data rollback and does not undo writes accepted after upgrade.

Offline create/ACL commands still need exclusive ownership: stop Application
before `Mk8.Sava.Application --backup-create ...` or `--hns-acl-apply ...`.
Validation and restore do not start the network listener. Gateway accepts none
of these storage-operator commands. Independent/off-host copies, capacity
alerts, restore drills, monitored TLS ingress/egress and representative workload
budgets remain deployment responsibilities; the two-executable split does not
automatically resolve an existing operational backlog.
