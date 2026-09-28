using System.Net;
using System.Net.Sockets;
using Mk8.Sava.Configuration;

namespace Mk8.Sava.Protocol;

internal sealed class UrlSourceEgressPolicy
{
    private readonly HashSet<string> _allowedPrivateHosts;
    private readonly Func<string, CancellationToken, Task<IPAddress[]>> _resolveHost;
    private readonly Func<IPAddress, int, CancellationToken, ValueTask<Stream>> _connectSocket;

    public UrlSourceEgressPolicy(SavaOptions options)
        : this(options, Dns.GetHostAddressesAsync, ConnectSocketAsync)
    {
    }

    internal UrlSourceEgressPolicy(
        SavaOptions options,
        Func<string, CancellationToken, Task<IPAddress[]>> resolveHost,
        Func<IPAddress, int, CancellationToken, ValueTask<Stream>> connectSocket)
    {
        _allowedPrivateHosts = new(options.UrlTransferAllowedPrivateHosts, StringComparer.OrdinalIgnoreCase);
        _resolveHost = resolveHost;
        _connectSocket = connectSocket;
    }

    internal bool Allows(string host, IPAddress address) =>
        _allowedPrivateHosts.Contains(host) || IsPublicAddress(address);

    internal static bool IsPublicAddress(IPAddress address)
    {
        if (address.IsIPv4MappedToIPv6)
            address = address.MapToIPv4();

        var bytes = address.GetAddressBytes();
        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            return bytes[0] is not (0 or 10 or 127) &&
                   bytes[0] < 224 &&
                   !(bytes[0] == 100 && bytes[1] is >= 64 and <= 127) &&
                   !(bytes[0] == 169 && bytes[1] == 254) &&
                   !(bytes[0] == 172 && bytes[1] is >= 16 and <= 31) &&
                   !(bytes[0] == 192 &&
                     (bytes[1] == 168 || bytes[1] == 0 && bytes[2] is 0 or 2)) &&
                   !(bytes[0] == 198 &&
                     (bytes[1] is 18 or 19 || bytes[1] == 51 && bytes[2] == 100)) &&
                   !(bytes[0] == 203 && bytes[1] == 0 && bytes[2] == 113);
        }

        if (address.AddressFamily != AddressFamily.InterNetworkV6)
            return false;

        // Restrict IPv6 to global unicast, excluding documentation and address-
        // embedding transition ranges that can tunnel to otherwise blocked IPv4.
        return (bytes[0] & 0xe0) == 0x20 &&
               !(bytes[0] == 0x20 && bytes[1] == 0x01 && bytes[2] == 0 && bytes[3] == 0) &&
               !(bytes[0] == 0x20 && bytes[1] == 0x01 && bytes[2] == 0x0d && bytes[3] == 0xb8) &&
               !(bytes[0] == 0x20 && bytes[1] == 0x02);
    }

    internal ValueTask<Stream> ConnectAsync(
        SocketsHttpConnectionContext context,
        CancellationToken cancellationToken) => ConnectEndpointAsync(context.DnsEndPoint, cancellationToken);

    internal async ValueTask<Stream> ConnectEndpointAsync(
        DnsEndPoint endpoint,
        CancellationToken cancellationToken)
    {
        var addresses = IPAddress.TryParse(endpoint.Host, out var literal)
            ? [literal]
            : await _resolveHost(endpoint.Host, cancellationToken).ConfigureAwait(false);
        var permitted = addresses.Where(address => Allows(endpoint.Host, address)).ToArray();
        if (permitted.Length == 0)
            throw new IOException("The copy source resolves only to blocked private or non-routable addresses.");

        Exception? lastFailure = null;
        foreach (var address in permitted)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await _connectSocket(address, endpoint.Port, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is SocketException or IOException)
            {
                lastFailure = exception;
            }
        }

        throw new IOException("The copy source could not be reached at an allowed address.", lastFailure);
    }

    private static async ValueTask<Stream> ConnectSocketAsync(
        IPAddress address,
        int port,
        CancellationToken cancellationToken)
    {
        Socket? socket = new(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(address, port, cancellationToken).ConfigureAwait(false);
            var stream = new NetworkStream(socket, ownsSocket: true);
            socket = null;
            return stream;
        }
        finally
        {
            socket?.Dispose();
        }
    }
}
