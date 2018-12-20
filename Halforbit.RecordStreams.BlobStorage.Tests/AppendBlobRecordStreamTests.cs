using Halforbit.Facets.Implementation;
using Halforbit.RecordStreams.BlobStorage.Facets;
using Halforbit.RecordStreams.Facets;
using Halforbit.RecordStreams.Interface;
using Halforbit.RecordStreams.Serialization.JsonLines.Facets;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Halforbit.RecordStreams.BlobStorage.Tests
{
    public class AppendBlobRecordStreamTests
    {
        readonly ITestOutputHelper _testOutputHelper;

        readonly IConfiguration _configuration;

        public AppendBlobRecordStreamTests(
            ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

            _configuration = new ConfigurationBuilder()
                .AddUserSecrets("Halforbit.RecordStreams")
                .Build();
        }

        [Fact]
        public async Task AppendBlobRecordStreamIntegrationTest()
        {
            var context = new ContextFactory(new FrameworkConfigurationProvider(_configuration)).Create<ITestContext>();

            var recordStream = context.TestRecordStream;

            var now = new DateTime(2018, 12, 16, 1, 23, 45);

            var streamKey = Guid.NewGuid();

            var existsA = await recordStream.Exists(streamKey);

            Assert.False(existsA);

            var listA = await (await recordStream.EnumerateAsync(streamKey)).ToListAsync();

            Assert.Empty(listA);

            var deleteResultA = await recordStream.Delete(streamKey);

            Assert.False(deleteResultA);

            var batchA = new[]
            {
                new TestRecord(
                    now, 
                    Guid.NewGuid(), 
                    "null", 
                    new TestSubRecord("123 Fake St"),
                    3),

                new TestRecord(
                    now,
                    Guid.NewGuid(),
                    null,
                    new TestSubRecord("125 Fake St"),
                    4,
                    new[] { "James", "John" }),
            };

            await recordStream.Append(
                key: streamKey,
                records: batchA);

            var existsB = await recordStream.Exists(streamKey);

            Assert.True(existsB);

            var listB = await (await recordStream.EnumerateAsync(streamKey)).ToListAsync();

            AssertJsonEqual(batchA, listB);

            now = now.AddHours(1);

            var batchB = new[]
            {
                new TestRecord(
                    now,
                    Guid.NewGuid(),
                    "[Hello record C]",
                    null,
                    2,
                    new[] { "Steve", "Sue" }),

                new TestRecord(
                    now,
                    Guid.NewGuid(),
                    "{Hello record D}",
                    new TestSubRecord("125\tReal\tSt"),
                    0),
            };

            await recordStream.Append(
                key: streamKey,
                records: batchB);

            var listC = await (await recordStream.EnumerateAsync(streamKey)).ToListAsync();

            AssertJsonEqual(batchA.Concat(batchB).ToList(), listC);

            var batchC = new TestRecord[0];

            await recordStream.Append(
                key: streamKey,
                records: batchC);

            var listD = await (await recordStream.EnumerateAsync(streamKey)).ToListAsync();

            AssertJsonEqual(batchA.Concat(batchB).ToList(), listD);

            var deleteResultB = await recordStream.Delete(streamKey);

            Assert.True(deleteResultB);

            var existsC = await recordStream.Exists(streamKey);

            Assert.False(existsC);

            var deleteResultC = await recordStream.Delete(streamKey);

            Assert.False(deleteResultC);
        }

        void AssertJsonEqual<TValue>(
            IReadOnlyList<TValue> a,
            IReadOnlyList<TValue> b)
        {
            if (a.Count != b.Count) throw new Exception();

            for (var i = 0; i < a.Count; i++)
            {
                var expected = JsonConvert.SerializeObject(a[i]);

                var actual = JsonConvert.SerializeObject(b[i]);

                Assert.Equal(expected, actual);
            }
        }
    }

    public interface ITestContext
    {
        [ConnectionString(configKey: "ConnectionString")]
        [ContainerName("record-streams-test")]
        [JsonLinesRecordSerialization]
        [FileExtension(".jsonl"), ContentType("application/x-jsonlines")]
        //[GZipCompression, ContentEncoding("gzip")]
        [KeyMap("append-blob-record-stream/{this}")]
        IRecordStream<Guid, TestRecord> TestRecordStream { get; }
    }

    public class TestRecord
    {
        public TestRecord(
            DateTime createTime,
            Guid id,
            string message,
            TestSubRecord subRecord,
            int puppyCount,
            IReadOnlyList<string> names = default)
        {
            CreateTime = createTime;

            Id = id;

            Message = message;

            SubRecord = subRecord;

            PuppyCount = puppyCount;

            Names = names;
        }

        public DateTime CreateTime { get; }

        public Guid Id { get; }

        public string Message { get; }

        public TestSubRecord SubRecord { get; }

        public int PuppyCount { get; }

        public IReadOnlyList<string> Names { get; }
    }

    public class TestSubRecord
    {
        public TestSubRecord(
            string address)
        {
            Address = address;
        }

        public string Address { get; }
    }


}
