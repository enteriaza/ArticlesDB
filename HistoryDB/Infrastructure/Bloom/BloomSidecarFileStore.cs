// BloomSidecarFileStore.cs -- infrastructure adapter for bloom sidecar persistence.

using HistoryDB.Contracts;
using HistoryDB.Utilities;

namespace HistoryDB.Infrastructure.Bloom;

internal sealed class BloomSidecarFileStore : IBloomSidecarStore
{
    public void WriteSidecar(string path, ulong[] words, int bitCount, int hashCount) =>
        BloomSidecarFile.WriteSidecar(path, words, bitCount, hashCount);
}
