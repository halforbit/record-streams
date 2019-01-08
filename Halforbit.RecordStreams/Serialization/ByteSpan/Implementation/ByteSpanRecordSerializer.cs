using Halforbit.RecordStreams.Serialization.Interface;
using System;
using System.Buffers;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Serialization.ByteSpan.Implementation
{
    public class ByteSpanRecordSerializer : IRecordSerializer
    {
        public async Task<(TRecord record, long bytesRead)> Deserialize<TRecord>(ReadOnlySequence<byte> bytes)
        {
            if(!typeof(TRecord).Equals(typeof(byte[])))
            {
                throw new Exception("Byte span serialization may only be used with type byte[]");
            }

            if (bytes.Length < 3) return (default, 0);

            var length = ParseLengthBytes(bytes.Slice(0, 3).ToArray());

            if (bytes.Length < length + 3) return (default, 0);

            return ((TRecord)(object)bytes.Slice(3, length).ToArray(), length + 3);
        }

        public async Task<ReadOnlySequence<byte>> Serialize<TRecord>(TRecord record)
        {
            if (!typeof(TRecord).Equals(typeof(byte[])))
            {
                throw new Exception("Byte span serialization may only be used with type byte[]");
            }
            
            var recordBytes = (byte[])(object)record;

            var lengthBytes = CreateLengthBytes(recordBytes.Length);

            var outputBytes = new byte[recordBytes.Length + 3];

            lengthBytes.CopyTo(outputBytes, 0);

            recordBytes.CopyTo(outputBytes, 3);

            return new ReadOnlySequence<byte>(outputBytes);
        }

        static byte[] CreateLengthBytes(int length)
        {
            return new byte[]
            {
                (byte)length,

                (byte)(length >> 8),

                (byte)(length >> 16)
            };
        }

        static long ParseLengthBytes(byte[] bytes)
        {
            return 
                ((long)bytes[0]) |
                (((long)bytes[1]) << 8) |
                (((long)bytes[2]) << 16);
        }
    }
}
