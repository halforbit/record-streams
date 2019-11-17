using Halforbit.RecordStreams.Serialization.Interface;
using System;
using System.Buffers;
using System.Text;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Serialization.Text.Implementation
{
    public class TextLinesRecordSerializer : IRecordSerializer
    {
        readonly static Encoding _encoding = new UTF8Encoding(false);

        public async Task<(TRecord record, long bytesRead)> Deserialize<TRecord>(ReadOnlySequence<byte> bytes)
        {
            if(!typeof(TRecord).Equals(typeof(string)))
            {
                throw new Exception("Text line serialization may only be used with type string");
            }

            if (bytes.Length == 0) return (default, 0);

            var lineBreakPosition = bytes.PositionOf((byte)'\n');
            
            if (lineBreakPosition == null) return (default, 0);

            var endPosition = lineBreakPosition.Value.GetInteger() + 1;

            return (
                (TRecord)(object)_encoding.GetString(bytes.Slice(0, endPosition).ToArray()),
                endPosition);
        }

        public async Task<ReadOnlySequence<byte>> Serialize<TRecord>(TRecord record)
        {
            if (!typeof(TRecord).Equals(typeof(string)))
            {
                throw new Exception("Text line serialization may only be used with type string");
            }
            
            var recordString = (string)(object)record;

            if (!recordString.EndsWith("\n"))
            {
                recordString += "\r\n";
            }

            var recordBytes = _encoding.GetBytes(recordString);

            return new ReadOnlySequence<byte>(recordBytes);
        }
    }
}
