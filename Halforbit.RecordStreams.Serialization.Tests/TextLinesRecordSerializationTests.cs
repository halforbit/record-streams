using Halforbit.RecordStreams.Serialization.Text.Implementation;
using System.Buffers;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Halforbit.RecordStreams.Serialization.Tests
{
    public class TextLinesRecordSerializationTests
    {
        static readonly Encoding _encoding = new UTF8Encoding(false); 

        [Fact]
        public async Task Serialize_Success()
        {
            var serializer = new TextLinesRecordSerializer();

            var source = @"
hello alfa
hello bravo
hello charlie
".TrimStart();

            var serialized = (await serializer.Serialize(source)).ToArray();

            var outcome = _encoding.GetString(serialized);

            Assert.Equal(
                _encoding.GetBytes(source),
                serialized);
        }

        [Fact]
        public async Task Deserialize_Success()
        {
            var serializer = new TextLinesRecordSerializer();

            var source = @"
hello alfa
hello bravo
hello charlie
".TrimStart();

            var serialized = _encoding.GetBytes(source);

            var (actual, bytesRead) = await serializer.Deserialize<string>(
                new ReadOnlySequence<byte>(serialized));

            Assert.Equal("hello alfa\n", actual);

            Assert.Equal(11, bytesRead);
        }
    }
}
