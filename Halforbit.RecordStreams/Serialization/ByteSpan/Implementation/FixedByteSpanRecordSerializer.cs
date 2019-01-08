using Halforbit.RecordStreams.Serialization.Interface;
using System;
using System.Buffers;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Serialization.ByteSpan.Implementation
{
    public class FixedByteSpanRecordSerializer : IRecordSerializer
    {
        readonly int _spanLength;

        public FixedByteSpanRecordSerializer(string spanLength)
        {
            _spanLength = int.Parse(spanLength);
        }

        public async Task<(TRecord record, long bytesRead)> Deserialize<TRecord>(ReadOnlySequence<byte> bytes)
        {
            if(!typeof(TRecord).Equals(typeof(byte[])))
            {
                throw new ArgumentException("Byte span serialization may only be used with records of type byte[]");
            }

            if (bytes.Length < _spanLength) return (default, 0);

            return ((TRecord)(object)bytes.Slice(0, _spanLength).ToArray(), _spanLength);
        }

        public async Task<ReadOnlySequence<byte>> Serialize<TRecord>(TRecord record)
        {
            if (!typeof(TRecord).Equals(typeof(byte[])))
            {
                throw new ArgumentException("Byte span serialization may only be used with records of type byte[]");
            }
            
            var recordBytes = (byte[])(object)record;

            if(recordBytes.Length > _spanLength)
            {
                throw new ArgumentException($"Data was {recordBytes.Length} bytes but span length is {_spanLength}");
            }

            var outputBytes = new byte[_spanLength];

            recordBytes.CopyTo(outputBytes, 0);

            return new ReadOnlySequence<byte>(outputBytes);
        }
    }
}
