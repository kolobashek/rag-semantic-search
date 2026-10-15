namespace RagCloudFiles;

internal sealed class HydrationRequests : IDisposable
{
    private readonly object _sync = new();
    private readonly HashSet<Pending> _pending = [];
    private bool _stopped;

    public Pending Add(long transferKey, long requestKey, long offset, long length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_stopped, this);
            Pending pending = new(transferKey, requestKey, offset, length);
            // RequestKey identifies an operation on a file, not a unique FETCH_DATA callback.
            // Every callback must be serviced, including another range of the same operation.
            _pending.Add(pending);
            return pending;
        }
    }

    public int Cancel(long transferKey, long requestKey, long offset, long length)
    {
        lock (_sync)
        {
            int cancelled = 0;
            foreach (Pending pending in _pending.ToArray())
            {
                if (pending.TransferKey != transferKey || pending.RequestKey != requestKey
                    || offset > pending.Offset || offset < 0 || length < -1)
                {
                    continue;
                }

                // A partial cancellation must not abandon bytes still required by Windows.
                if (length == -1 || (length >= pending.Offset - offset
                    && length - (pending.Offset - offset) >= pending.Length))
                {
                    pending.Source.Cancel();
                    cancelled++;
                }
            }
            return cancelled;
        }
    }

    public void Complete(Pending pending)
    {
        lock (_sync)
        {
            if (_pending.Remove(pending))
            {
                pending.Source.Dispose();
            }
        }
    }

    public void Dispose()
    {
        lock (_sync)
        {
            _stopped = true;
            foreach (Pending pending in _pending.ToArray())
            {
                pending.Source.Cancel();
            }
            // Running callbacks dispose their own sources on completion.
        }
    }

    internal sealed class Pending(long transferKey, long requestKey, long offset, long length)
    {
        public long TransferKey { get; } = transferKey;
        public long RequestKey { get; } = requestKey;
        public long Offset { get; } = offset;
        public long Length { get; } = length;
        internal CancellationTokenSource Source { get; } = new();
        public CancellationToken Token => Source.Token;
    }
}
