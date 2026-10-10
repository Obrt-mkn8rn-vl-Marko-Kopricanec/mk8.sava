# Deployment agnostic source policy

Tracked Sava source, public documentation, examples, templates and build inputs
must not assign a site's IP addresses, subnets, DNS zones, public/private hostnames,
storage accounts or network topology. Operators supply those values through
validated configuration or caller inputs. A value is not portable merely because
it was moved from code to a tracked template.

## Required configuration

`Sava:DefaultAccount` has no account-name default and must identify an entry in
`Sava:Accounts`. `ApplicationTransport:Endpoint` has no address/port default;
the existing HTTPS or literal-loopback-HTTP, credential/query/fragment and private
listener validation remains required. Gateway needs explicit URLs, ports or named
Kestrel endpoint configuration rather than an implicit framework listener.
The environment examples intentionally leave required assignments empty and
cannot be used unchanged as working deployment configuration.

Select account names and external authority independently of DNS resolution.
Preserve signed Host/path/query fields; do not infer an approved zone or route
from SDK examples. TLS identities, proxy trust, permissions and service ordering
remain explicit decisions, not authority granted by these templates.

## Purpose of retained identities and address samples

These categories describe specific uses, not a blanket exemption for literals.
Any new value or use requires review of its actual consumers.

| Location or use | Purpose and boundary |
| --- | --- |
| `GraphGroupResolutionOptions` and Graph-origin tests | Microsoft's named cloud-service endpoints, selected by the validated cloud enum. They constrain token/continuation destinations, not Sava's deployment zone or peers. |
| Azure Bearer audience and `WWW-Authenticate` resource identity | Azure Storage's protocol resource identifier, not a listener or routing destination. Issuer, tenant, authority and principal policy remain configured inputs. |
| `UrlSourceEgressPolicy` and its tests | Special-purpose address classifications protect against private/non-routable egress. The positive global-address vectors are classification samples only; connection controls inject DNS and an in-memory/throwing connector, never contact those sample addresses. |
| Loopback/wildcard APIs and private-HTTP validation | Standard address semantics and security predicates, not assigned site IPs. Actual listeners still take configured/caller-selected addresses and ports. A port-only Gateway input deliberately requests Kestrel wildcard semantics. |
| The systemd Application sandbox's loopback rule | A generic fail-closed sandbox example, not the Application endpoint. A chosen remote-TLS profile needs its own narrowly adapted operator network policy. |
| `.example`, `.test`, `.invalid`, `localhost` and documentation address fixtures | Finite parsing, authorization, URI, redaction and owned-loopback test cases. They neither allocate a zone nor authorize a production route. Fixed marker ports in process-output fixtures are text/parser observations, not service allocations. |
| Synthetic `.internal` whitelist names and `.local` website authorities in tests | Exact-name authorization and virtual TestServer routing controls. The whitelist controls inject DNS/connectors or call only the pure predicate; the website controls use the owned in-process transport. These names are not private-site defaults or live destinations. |
| In-process and socket test hosts | TestServer supplies explicit synthetic listener/RPC descriptors; real test hosts choose owned loopback ports. Fixture accounts and synthetic/random credentials are explicit test inputs, never production defaults. |
| Azure-shaped Blob/File source URIs in intercepted copy tests | Protocol-shape examples handled by injected test transports. They are not accounts/endpoints to contact or use for deployment. |
| SDK compatibility launchers and published smoke fixture | Disposable local test profiles with explicit fixture accounts, caller endpoints, random keys and owned loopback ports. They are not deployment configuration. |
| Package locks, tool downloads, XML schema namespaces and public citations | Dependency/source/protocol identities, not Sava's runtime account, host, subnet or DNS assignments. Existing pin/digest and security checks remain unchanged. |

Special-use names are described by [RFC 6761](https://www.rfc-editor.org/rfc/rfc6761.html).
Microsoft documents the [Graph cloud endpoints](https://learn.microsoft.com/en-us/graph/deployments)
and [Storage authorization resource](https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-access-azure-active-directory).
Kestrel's [endpoint configuration semantics](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/servers/kestrel/endpoints?view=aspnetcore-10.0)
do not choose a deployment profile for this project.

## Private operator material

Site-specific configuration/scripts belong outside the checkout or in ignored,
untracked `deploy/internal/`. The ignore rule is not secret management: never
force-add such files, place credentials in public examples, or include private
operator material in a release manifest or evidence upload. Keep identity,
permission, key custody, capacity and exact artifact/configuration acceptance
separate from source-policy inspection. Auditing source does not execute or
authorize deployment, DNS, PKI, installation, service or target operations.
