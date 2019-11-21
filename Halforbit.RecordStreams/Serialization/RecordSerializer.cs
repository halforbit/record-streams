using Halforbit.BitBuffers;
using Halforbit.RecordStreams.Serialization.Interface;
using System.Buffers;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Serialization
{
    public abstract class RecordSerializer<TRecordValue> : IRecordSerializer
    {
        public Task<(TRecord record, long bytesRead)> Deserialize<TRecord>(ReadOnlySequence<byte> bytes)
        {
            try
            {
                var (record, bytesRead) = Deserialize(bytes.ToArray());

                return Task.FromResult(((TRecord)(object)record, bytesRead));
            }
            catch (ReadOverflowException)
            {
                return Task.FromResult((default(TRecord), 0L));
            }
        }

        public Task<ReadOnlySequence<byte>> Serialize<TRecord>(TRecord record)
        {
            return Task.FromResult(new ReadOnlySequence<byte>(Serialize((TRecordValue)(object)record)));
        }

        protected abstract (TRecordValue Record, long BytesRead) Deserialize(byte[] data);

        protected abstract byte[] Serialize(TRecordValue value);
    }
}
