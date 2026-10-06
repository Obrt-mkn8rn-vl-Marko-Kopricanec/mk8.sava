using System.Reflection;
using System.Runtime.ExceptionServices;
using Mk8.Sava.Storage;
using Mk8.Sava.Transport;

namespace Mk8.Sava.Application;

internal sealed class ApplicationRpcDispatcher(
    BlobService blobs, MetadataStore metadata, LeaseService leases,
    ApplicationReadSessions sessions, ApplicationReadinessService readiness,
    StorageAnalyticsService analytics) : IApplicationRpcDispatcher
{
    public async ValueTask<object?> InvokeAsync(
        Type contract, MethodInfo method, object?[] arguments, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(contract);
        ArgumentNullException.ThrowIfNull(method);
        ArgumentNullException.ThrowIfNull(arguments);
        var target = ResolveTarget(contract);
        var signature = contract.GetMethods().FirstOrDefault(candidate =>
            candidate == method && typeof(Task).IsAssignableFrom(candidate.ReturnType));
        if (signature is null || signature.GetParameters().Length != arguments.Length)
            throw new InvalidOperationException("The Application method is not in the domain contract.");

        var index = 0;
        foreach (var argument in arguments)
        {
            if (argument is BlobRecord record)
                arguments[index] = await ReloadBlobAsync(record, cancellationToken).ConfigureAwait(false);
            else if (argument is ContainerRecord container)
                arguments[index] = await ReloadContainerAsync(container, cancellationToken).ConfigureAwait(false);
            index++;
        }

        if (contract == typeof(IBlobApplication) &&
            string.Equals(method.Name, nameof(IBlobApplication.ApplyBlobLeaseAsync), StringComparison.Ordinal))
        {
            return await ApplyBlobLeaseAsync(arguments, cancellationToken).ConfigureAwait(false);
        }
        if (contract == typeof(IBlobApplication) &&
            string.Equals(method.Name, nameof(IBlobApplication.ApplyContainerLeaseAsync), StringComparison.Ordinal))
        {
            return await ApplyContainerLeaseAsync(arguments, cancellationToken).ConfigureAwait(false);
        }

        if (contract == typeof(IMetadataApplication) &&
            string.Equals(method.Name, nameof(IMetadataApplication.ReadUserDelegationRoleGrantsAsync), StringComparison.Ordinal))
        {
            var grants = await metadata.ReadUserDelegationRoleGrantsAsync(
                (string)arguments[0]!, cancellationToken).ConfigureAwait(false);
            return grants.HasValue ? new UserDelegationRoleGrants(grants.Value.Roles, grants.Value.IssuedPermissions) : null;
        }

        var parameterTypes = signature.GetParameters().Select(parameter => parameter.ParameterType).ToArray();
        var implementation = target.GetType().GetMethod(
            method.Name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null, parameterTypes, modifiers: null)
            ?? throw new InvalidOperationException("The Application implementation does not match its contract.");
        return await InvokeReflectedAsync(implementation, target, arguments).ConfigureAwait(false);
    }

    private static async Task<object?> InvokeReflectedAsync(MethodInfo implementation, object target, object?[] arguments)
    {
        Task pending;
        try
        {
            pending = (Task)(implementation.Invoke(target, arguments)
                ?? throw new InvalidOperationException("The Application method did not return a task."));
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
            throw;
        }
        await pending.ConfigureAwait(false);
        return implementation.ReturnType.IsGenericType
            ? implementation.ReturnType.GetProperty("Result")!.GetValue(pending)
            : null;
    }

    private async Task<BlobLeaseUpdate> ApplyBlobLeaseAsync(object?[] arguments, CancellationToken cancellationToken)
    {
        var current = (BlobRecord)arguments[0]!;
        var transition = leases.Apply(current.Lease, (LeaseAction)arguments[1]!, (int?)arguments[2],
            (int?)arguments[3], (string?)arguments[4], (string?)arguments[5], (bool)arguments[6]!);
        var updated = await blobs.SetBlobLeaseAsync(current, transition.Lease, cancellationToken).ConfigureAwait(false);
        return new BlobLeaseUpdate(updated, transition.RemainingSeconds);
    }

    private async Task<ContainerLeaseUpdate> ApplyContainerLeaseAsync(object?[] arguments, CancellationToken cancellationToken)
    {
        var current = (ContainerRecord)arguments[0]!;
        var transition = leases.Apply(current.Lease, (LeaseAction)arguments[1]!, (int?)arguments[2],
            (int?)arguments[3], (string?)arguments[4], (string?)arguments[5], (bool)arguments[6]!);
        var updated = await blobs.SetContainerLeaseAsync(
            current, transition.Lease, (bool)arguments[7]!, cancellationToken).ConfigureAwait(false);
        return new ContainerLeaseUpdate(updated, transition.RemainingSeconds);
    }

    private object ResolveTarget(Type contract)
    {
        if (contract == typeof(IBlobApplication))
            return blobs;
        if (contract == typeof(IMetadataApplication))
            return metadata;
        if (contract == typeof(IApplicationReadSessions))
            return sessions;
        if (contract == typeof(IApplicationReadiness))
            return readiness;
        if (contract == typeof(IStorageAnalyticsSink))
            return analytics;
        throw new InvalidOperationException("The requested Application contract is not exposed.");
    }

    private async Task<BlobRecord> ReloadBlobAsync(BlobRecord supplied, CancellationToken cancellationToken)
    {
        var authoritative = await blobs.GetBlobAsync(
            supplied.Account, supplied.Container, supplied.Name, supplied.VersionId, supplied.Snapshot,
            includeDeleted: true, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(supplied.GenerationId, authoritative.GenerationId, StringComparison.Ordinal) ||
            !string.Equals(supplied.Revision, authoritative.Revision, StringComparison.Ordinal))
        {
            throw new StorageConcurrencyException("The blob changed after the Gateway observed it.");
        }
        return authoritative;
    }

    private async Task<ContainerRecord> ReloadContainerAsync(
        ContainerRecord supplied, CancellationToken cancellationToken)
    {
        var authoritative = await blobs.GetContainerAsync(
            supplied.Account, supplied.Name, includeDeleted: true, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(supplied.Revision, authoritative.Revision, StringComparison.Ordinal))
            throw new StorageConcurrencyException("The container changed after the Gateway observed it.");
        return authoritative;
    }
}
