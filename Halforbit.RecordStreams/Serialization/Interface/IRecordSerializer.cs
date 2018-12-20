using System.Buffers;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Serialization.Interface
{
    public interface IRecordSerializer
    {
        Task<(TRecord record, long bytesRead)> Deserialize<TRecord>(ReadOnlySequence<byte> bytes);

        Task<ReadOnlySequence<byte>> Serialize<TRecord>(TRecord record);
    }
}
