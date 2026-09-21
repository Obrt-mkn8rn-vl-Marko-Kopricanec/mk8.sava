using Microsoft.AspNetCore.Http;
using Mk8.Sava.Protocol;

namespace Mk8.Sava.Storage;

public enum LeaseAction
{
    Acquire,
    Renew,
    Change,
    Release,
    Break
}

public readonly record struct LeaseTransition(LeaseRecord Lease, int? RemainingSeconds);

public sealed class LeaseService(TimeProvider timeProvider)
{
    public LeaseRecord GetEffective(LeaseRecord lease)
    {
        var now = timeProvider.GetUtcNow();
        if (lease.State == LeaseState.Breaking && lease.BreakEndsAt <= now)
        {
            return lease with
            {
                State = LeaseState.Broken,
                DurationSeconds = null,
                AcquiredAt = null,
                ExpiresAt = null,
                BreakEndsAt = null
            };
        }

        if (lease.State == LeaseState.Leased && lease.ExpiresAt <= now)
        {
            return lease with
            {
                State = LeaseState.Expired,
                AcquiredAt = null,
                ExpiresAt = null,
                BreakEndsAt = null
            };
        }

        return lease;
    }

    public LeaseRecord ResetAfterBlobWrite(LeaseRecord lease)
    {
        var effective = GetEffective(lease);
        return effective.State is LeaseState.Expired or LeaseState.Broken
            ? LeaseRecord.Available
            : effective;
    }

    public void EnsureWriteAccess(LeaseRecord lease, string? suppliedId, string resource)
    {
        var effective = GetEffective(lease);
        var supplied = ParseOptionalId(suppliedId, "x-ms-lease-id");
        if (effective.State is LeaseState.Leased or LeaseState.Breaking)
        {
            if (!supplied.HasValue)
                throw AzureStorageException.LeaseIdMissing(resource);
            if (!Matches(effective, supplied.Value))
                throw AzureStorageException.LeaseOperationMismatch(resource);
            return;
        }

        if (supplied.HasValue)
            throw AzureStorageException.LeaseNotPresentForOperation(resource);
    }

    public void ValidateOptionalAccess(LeaseRecord lease, string? suppliedId, string resource)
    {
        if (suppliedId is null)
            return;

        var effective = GetEffective(lease);
        var supplied = ParseRequiredId(suppliedId, "x-ms-lease-id");
        if (effective.State is not (LeaseState.Leased or LeaseState.Breaking))
            throw AzureStorageException.LeaseNotPresentForOperation(resource);
        if (!Matches(effective, supplied))
            throw AzureStorageException.LeaseOperationMismatch(resource);
    }

    public LeaseTransition Apply(
        LeaseRecord lease,
        LeaseAction action,
        int? durationSeconds,
        int? breakPeriodSeconds,
        string? suppliedId,
        string? proposedId)
    {
        var current = GetEffective(lease);
        return action switch
        {
            LeaseAction.Acquire => Acquire(current, RequireDuration(durationSeconds), proposedId),
            LeaseAction.Renew => Renew(current, ParseRequiredId(suppliedId, "x-ms-lease-id")),
            LeaseAction.Change => Change(
                current,
                ParseRequiredId(suppliedId, "x-ms-lease-id"),
                ParseRequiredId(proposedId, "x-ms-proposed-lease-id")),
            LeaseAction.Release => Release(current, ParseRequiredId(suppliedId, "x-ms-lease-id")),
            LeaseAction.Break => Break(current, ValidateBreakPeriod(breakPeriodSeconds)),
            _ => throw new ArgumentOutOfRangeException(nameof(action))
        };
    }

    private LeaseTransition Acquire(LeaseRecord current, int durationSeconds, string? proposedValue)
    {
        var proposed = ParseOptionalId(proposedValue, "x-ms-proposed-lease-id");
        if (current.State == LeaseState.Leased)
        {
            if (!proposed.HasValue || !Matches(current, proposed.Value))
                throw LeaseConflict("LeaseAlreadyPresent", "There is already a lease present.");
        }
        else if (current.State == LeaseState.Breaking)
        {
            if (proposed.HasValue && Matches(current, proposed.Value))
            {
                throw LeaseConflict(
                    "LeaseIsBreakingAndCannotBeAcquired",
                    "The lease ID matched, but the lease is currently in breaking state and cannot be acquired.");
            }

            throw LeaseConflict("LeaseAlreadyPresent", "There is already a lease present.");
        }

        var now = timeProvider.GetUtcNow();
        var id = (proposed ?? Guid.NewGuid()).ToString();
        return new LeaseTransition(new LeaseRecord
        {
            Id = id,
            State = LeaseState.Leased,
            DurationSeconds = durationSeconds,
            AcquiredAt = now,
            ExpiresAt = durationSeconds == -1 ? null : now.AddSeconds(durationSeconds)
        }, null);
    }

    private LeaseTransition Renew(LeaseRecord current, Guid supplied)
    {
        if (!Matches(current, supplied))
            throw AzureStorageException.LeaseIdMismatchWithLeaseOperation();
        if (current.State is LeaseState.Breaking or LeaseState.Broken)
        {
            throw LeaseConflict(
                "LeaseIsBrokenAndCannotBeRenewed",
                "The lease ID matched, but the lease has been broken explicitly and cannot be renewed.");
        }
        if (current.State is not (LeaseState.Leased or LeaseState.Expired))
            throw AzureStorageException.LeaseIdMismatchWithLeaseOperation();

        var duration = current.DurationSeconds ?? 60;
        var now = timeProvider.GetUtcNow();
        return new LeaseTransition(current with
        {
            State = LeaseState.Leased,
            AcquiredAt = now,
            ExpiresAt = duration == -1 ? null : now.AddSeconds(duration),
            BreakEndsAt = null
        }, null);
    }

    private static LeaseTransition Change(LeaseRecord current, Guid supplied, Guid proposed)
    {
        if (current.State == LeaseState.Breaking)
        {
            if (Matches(current, supplied))
            {
                throw LeaseConflict(
                    "LeaseIsBreakingAndCannotBeChanged",
                    "The lease ID matched, but the lease is currently in breaking state and cannot be changed.");
            }

            throw AzureStorageException.LeaseIdMismatchWithLeaseOperation();
        }

        if (current.State != LeaseState.Leased)
            throw AzureStorageException.LeaseNotPresentWithLeaseOperation();
        if (!Matches(current, supplied) && !Matches(current, proposed))
            throw AzureStorageException.LeaseIdMismatchWithLeaseOperation();
        return new LeaseTransition(current with { Id = proposed.ToString() }, null);
    }

    private static LeaseTransition Release(LeaseRecord current, Guid supplied)
    {
        if (!Matches(current, supplied))
            throw AzureStorageException.LeaseIdMismatchWithLeaseOperation();
        if (current.State == LeaseState.Available)
            throw AzureStorageException.LeaseIdMismatchWithLeaseOperation();
        return new LeaseTransition(LeaseRecord.Available, null);
    }

    private LeaseTransition Break(LeaseRecord current, int? requestedSeconds)
    {
        if (current.State == LeaseState.Available)
            throw AzureStorageException.LeaseNotPresentWithLeaseOperation();
        if (current.State is LeaseState.Expired or LeaseState.Broken)
            return new LeaseTransition(ToBroken(current), 0);

        var now = timeProvider.GetUtcNow();
        DateTimeOffset? breakEndsAt;
        if (current.State == LeaseState.Breaking)
        {
            breakEndsAt = current.BreakEndsAt;
            if (requestedSeconds.HasValue)
            {
                var requestedEnd = now.AddSeconds(requestedSeconds.Value);
                if (!breakEndsAt.HasValue || requestedEnd < breakEndsAt.Value)
                    breakEndsAt = requestedEnd;
            }
        }
        else if (!requestedSeconds.HasValue)
        {
            breakEndsAt = current.DurationSeconds == -1 ? now : current.ExpiresAt;
        }
        else
        {
            var requestedEnd = now.AddSeconds(requestedSeconds.Value);
            breakEndsAt = current.ExpiresAt.HasValue && current.ExpiresAt.Value < requestedEnd
                ? current.ExpiresAt
                : requestedEnd;
        }

        if (!breakEndsAt.HasValue || breakEndsAt.Value <= now)
            return new LeaseTransition(ToBroken(current), 0);

        var remaining = Math.Clamp(
            (int)Math.Ceiling((breakEndsAt.Value - now).TotalSeconds),
            0,
            60);
        return new LeaseTransition(current with
        {
            State = LeaseState.Breaking,
            DurationSeconds = null,
            AcquiredAt = null,
            ExpiresAt = null,
            BreakEndsAt = breakEndsAt
        }, remaining);
    }

    private static LeaseRecord ToBroken(LeaseRecord current) => current with
    {
        State = LeaseState.Broken,
        DurationSeconds = null,
        AcquiredAt = null,
        ExpiresAt = null,
        BreakEndsAt = null
    };

    private static int RequireDuration(int? durationSeconds)
    {
        if (!durationSeconds.HasValue)
            throw AzureStorageException.MissingHeader("x-ms-lease-duration");
        if (durationSeconds.Value != -1 && durationSeconds.Value is < 15 or > 60)
        {
            throw AzureStorageException.InvalidHeader(
                "x-ms-lease-duration",
                durationSeconds.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        return durationSeconds.Value;
    }

    private static int? ValidateBreakPeriod(int? breakPeriodSeconds)
    {
        if (breakPeriodSeconds is < 0 or > 60)
        {
            throw AzureStorageException.InvalidHeader(
                "x-ms-lease-break-period",
                breakPeriodSeconds.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        return breakPeriodSeconds;
    }

    private static Guid ParseRequiredId(string? value, string header)
    {
        if (value is null)
            throw AzureStorageException.MissingHeader(header);
        return ParseId(value, header);
    }

    private static Guid? ParseOptionalId(string? value, string header) =>
        value is null ? null : ParseId(value, header);

    private static Guid ParseId(string value, string header)
    {
        if (!Guid.TryParse(value, out var parsed))
            throw AzureStorageException.InvalidHeader(header, value);
        return parsed;
    }

    private static bool Matches(LeaseRecord lease, Guid id) =>
        Guid.TryParse(lease.Id, out var current) && current == id;

    private static AzureStorageException LeaseConflict(string code, string message) =>
        new(StatusCodes.Status409Conflict, code, message);
}
