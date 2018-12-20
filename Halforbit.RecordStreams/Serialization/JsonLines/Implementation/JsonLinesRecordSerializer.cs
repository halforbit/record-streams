using Halforbit.RecordStreams.Serialization.Interface;
using Newtonsoft.Json;
using System.Buffers;
using System.Text;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Serialization.JsonLines.Implementation
{
    public class JsonLinesRecordSerializer : IRecordSerializer
    {        
        static readonly UTF8Encoding _utf8Encoding = new UTF8Encoding(false);

        public async Task<(TRecord record, long bytesRead)> Deserialize<TRecord>(ReadOnlySequence<byte> bytes)
        {
            var position = bytes.PositionOf((byte)'\n');

            if (position != null)
            {
                var line = bytes.Slice(0, bytes.GetPosition(1, position.Value));

                var text = GetUtf8String(line);

                text = text.Substring(0, text.Length - 2);

                return (JsonConvert.DeserializeObject<TRecord>(text), line.Length);
            }
            else
            {
                return (default, 0);
            }
        }

        public async Task<ReadOnlySequence<byte>> Serialize<TRecord>(TRecord record)
        {
            return new ReadOnlySequence<byte>(
                _utf8Encoding.GetBytes($"{JsonConvert.SerializeObject(record)}\r\n"));
        }

        string GetUtf8String(ReadOnlySequence<byte> buffer)
        {
            if (buffer.IsSingleSegment)
            {
                return _utf8Encoding.GetString(buffer.First.Span.ToArray());
            }

            var text = new StringBuilder();

            foreach(var segment in buffer)
            {
                text.Append(_utf8Encoding.GetString(segment.ToArray()));
            }

            return text.ToString();
        }
    }
}
