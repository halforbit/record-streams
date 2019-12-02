using Halforbit.Facets.Attributes;
using Halforbit.ObjectTools.Collections;
using Halforbit.ObjectTools.InvariantExtraction.Implementation;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Halforbit.RecordStreams.Exceptions;
using Halforbit.RecordStreams.Interface;
using Halforbit.RecordStreams.Serialization.Interface;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Buffers;
using System.Collections.Async;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.BlobStorage.Implementation
{
    public class AppendBlobRecordStream<TKey, TRecord> : 
        IRecordStream<TKey, TRecord>
    {
        readonly InvariantExtractor _invariantExtractor = new InvariantExtractor();

        readonly IRecordSerializer _recordSerializer;

        readonly ICompressor _compressor;

        readonly CloudBlobContainer _cloudBlobContainer;

        readonly StringMap<TKey> _keyMap;

        readonly string _fileExtension;

        readonly string _contentType;

        readonly string _contentEncoding;

        public AppendBlobRecordStream(
            IRecordSerializer recordSerializer,
            [Optional]ICompressor compressor,
            string connectionString,
            string containerName,
            string keyMap,
            [Optional]string fileExtension,
            [Optional]string contentType,
            [Optional]string contentEncoding)
        {
            _recordSerializer = recordSerializer;

            _compressor = compressor;

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            _cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

            _cloudBlobContainer.CreateIfNotExistsAsync().Wait();

            _keyMap = keyMap;

            _fileExtension = fileExtension;

            _contentType = contentType;

            _contentEncoding = contentEncoding;
        }

        //static int instanceCount = 0;

        //static object lockObj = new object();

        //public static List<string> log = new List<string>();

        public async Task Append(
            TKey key,
            IEnumerable<TRecord> records)
        {
            //Interlocked.Increment(ref instanceCount);

            //lock(log) log.Add($"Up to {instanceCount}");

            //lock (lockObj)
            //{
            //    instanceCount++;

            //    using (var s = File.OpenWrite("out.txt"))
            //    using (var w = new StreamWriter(s))
            //    {
            //        w.WriteLine("out.txt", new[] { $"Up to {instanceCount}" });
            //    }
            //}

            var appendBlob = GetAppendBlob(key);

            if (!await appendBlob.ExistsAsync().ConfigureAwait(false))
            {
                await appendBlob.CreateOrReplaceAsync().ConfigureAwait(false);
            }

            await appendBlob.FetchAttributesAsync().ConfigureAwait(false);

            var serializeTasks = records
                .Select(async r => await _recordSerializer.Serialize(r).ConfigureAwait(false))
                .ToArray();

            await Task.WhenAll(serializeTasks).ConfigureAwait(false);

            var members = serializeTasks.Select(t => t.Result.ToArray());

            if (_compressor != null)
            {
                var compressTasks = members
                    .Select(async m => await _compressor.Compress(m).ConfigureAwait(false))
                    .ToArray();

                await Task.WhenAll(compressTasks).ConfigureAwait(false);

                members = compressTasks.Select(t => t.Result.ToArray());
            }

            var bytes = members.SelectMany(m => m).ToArray();

            if (bytes.Length == 0) return;

            using (var ms = new MemoryStream(bytes))
            {
                await appendBlob.AppendBlockAsync(
                    blockData: ms,
                    contentMD5: null,
                    accesscondition: null,
                    options: new BlobRequestOptions
                    {
                        ParallelOperationThreadCount = Math.Min(64, 8 * 12),

                        DisableContentMD5Validation = true,

                        StoreBlobContentMD5 = false,

                        RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(2), 10)
                    },
                    operationContext: null).ConfigureAwait(false);
            }

            var hasContentType = !string.IsNullOrWhiteSpace(_contentType);

            var hasContentEncoding = !string.IsNullOrWhiteSpace(_contentEncoding);

            if (hasContentType || hasContentEncoding)
            {
                var updateProperties = false;

                if (hasContentType && appendBlob.Properties.ContentType != _contentType)
                {
                    appendBlob.Properties.ContentType = _contentType;

                    updateProperties = true;
                }

                if (hasContentEncoding && appendBlob.Properties.ContentEncoding != _contentEncoding)
                {
                    appendBlob.Properties.ContentEncoding = _contentEncoding;

                    updateProperties = true;
                }

                if (updateProperties)
                {
                    await appendBlob.SetPropertiesAsync().ConfigureAwait(false);
                }
            }

            //Interlocked.Decrement(ref instanceCount);

            //lock(log) log.Add($"Down to {instanceCount}");

            //lock (lockObj)
            //{
            //    instanceCount--;

            //    using (var s = File.OpenWrite("out.txt"))
            //    using (var w = new StreamWriter(s))
            //    {
            //        w.WriteLine("out.txt", new[] { $"Up to {instanceCount}" });
            //    }
            //}
        }

        public async Task<bool> Delete(TKey key) => await GetAppendBlob(key)
            .DeleteIfExistsAsync()
            .ConfigureAwait(false);

        public async Task<IAsyncEnumerable<TRecord>> EnumerateAsync(
            TKey key,
            long startIndex = 0)
        {
            if (!await Exists(key).ConfigureAwait(false))
            {
                return new AsyncEnumerable<TRecord>(async yield => { });
            }

            var appendBlob = GetAppendBlob(key);

            var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 10_000_000));

            var fillPipeTask = FillPipe(appendBlob, pipe.Writer, startIndex);

            return new AsyncEnumerable<TRecord>(async yield =>
            {
                var pipeReader = pipe.Reader;
                
                while (true)
                {
                    var readResult = await pipeReader.ReadAsync().ConfigureAwait(false);

                    var buffer = readResult.Buffer;

                    while (true)
                    {
                        var (record, bytesRead) = await _recordSerializer
                            .Deserialize<TRecord>(buffer)
                            .ConfigureAwait(false);

                        if (bytesRead > 0)
                        {
                            await yield.ReturnAsync(record).ConfigureAwait(false);

                            var next = buffer.GetPosition(bytesRead);

                            buffer = buffer.Slice(next);

                        }
                        else
                        {
                            break;
                        }
                    }

                    pipeReader.AdvanceTo(buffer.Start, buffer.End);

                    if (readResult.IsCompleted)
                    {
                        break;
                    }
                }

                pipeReader.Complete();
            });
        }

        public async Task<IAsyncEnumerable<(long, TRecord)>> EnumerateIndexedAsync(
            TKey key,
            long startIndex = 0)
        {
            var currentIndex = startIndex;

            if (!await Exists(key).ConfigureAwait(false))
            {
                return new AsyncEnumerable<(long, TRecord)>(async yield => { });
            }

            var appendBlob = GetAppendBlob(key);

            var pipe = new Pipe();

            var fillPipeTask = FillPipe(appendBlob, pipe.Writer, startIndex);

            return new AsyncEnumerable<(long, TRecord)>(async yield =>
            {
                var pipeReader = pipe.Reader;

                while (true)
                {
                    var readResult = await pipeReader.ReadAsync().ConfigureAwait(false);

                    var buffer = readResult.Buffer;

                    while (true)
                    {
                        var (record, bytesRead) = await _recordSerializer
                            .Deserialize<TRecord>(buffer)
                            .ConfigureAwait(false);

                        if (bytesRead > 0)
                        {
                            await yield.ReturnAsync((currentIndex, record)).ConfigureAwait(false);

                            var next = buffer.GetPosition(bytesRead);

                            buffer = buffer.Slice(next);

                            currentIndex += bytesRead;
                        }
                        else
                        {
                            break;
                        }
                    }

                    pipeReader.AdvanceTo(buffer.Start, buffer.End);

                    if (readResult.IsCompleted)
                    {
                        break;
                    }
                }

                pipeReader.Complete();
            });
        }

        public async Task<bool> Exists(TKey key) => await GetAppendBlob(key)
            .ExistsAsync()
            .ConfigureAwait(false);

        public async Task<long> GetLength(TKey key)
        {
            var appendBlob = GetAppendBlob(key);

            try
            {
                await appendBlob.FetchAttributesAsync();

                return appendBlob.Properties.Length;
            }
            catch(StorageException stex)
            {
                if (stex.Message.ToLower().Contains("exist")) return 0;

                throw;
            }
        }

        public async Task<IEnumerable<TKey>> ListKeys(Expression<Func<TKey, bool>> predicate = null)
        {
            var pathPrefix = ResolveKeyStringPrefix(predicate);

            var keyStrings = await ListKeyStrings(pathPrefix, _fileExtension);

            var keyPaths = keyStrings.Select(s => _keyMap.Map(s)).Where(k => k != default);

            if (predicate != null)
            {
                Func<TKey, bool> selectorFunc = predicate.Compile();

                keyPaths = keyPaths.Where(k => selectorFunc(k));
            }

            return keyPaths.ToList();
        }

        string ResolveKeyStringPrefix(Expression<Func<TKey, bool>> selector)
        {
            var memberValues = EmptyReadOnlyDictionary<string, object>.Instance as
                IReadOnlyDictionary<string, object>;

            if (selector != null)
            {
                memberValues = _invariantExtractor.ExtractInvariantDictionary(selector, out _);
            }

            return EvaluatePath(memberValues, true);
        }

        async Task<IEnumerable<string>> ListKeyStrings(string pathPrefix, string extension)
        {
            var results = new List<string>();

            var blobContinuationToken = default(BlobContinuationToken);

            do
            {
                var resultSegment = await _cloudBlobContainer.ListBlobsSegmentedAsync(
                    pathPrefix,
                    blobContinuationToken);

                foreach (var item in resultSegment.Results)
                {
                    if (item is CloudBlobDirectory cloudBlobDirectory)
                    {
                        results.AddRange(await ListKeyStrings(cloudBlobDirectory.Prefix, extension));
                    }
                    else if (item is CloudAppendBlob cloudAppendBlob)
                    {
                        var name = cloudAppendBlob.Name;

                        if (name.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
                        {
                            results.Add(name.Substring(0, name.Length - extension.Length));
                        }
                    }
                }

                blobContinuationToken = resultSegment.ContinuationToken;
            }
            while (blobContinuationToken != null);

            return results;
        }

        string EvaluatePath(
            IReadOnlyDictionary<string, object> memberValues,
            bool allowPartialMap = false)
        {
            try
            {
                return _keyMap.Map(memberValues, allowPartialMap);
            }
            catch (ArgumentNullException ex)
            {
                throw new IncompleteKeyException(
                    $"Path for {typeof(TRecord).Name} could not be evaluated " +
                    $"because key {typeof(TKey).Name} was missing a value for {ex.ParamName}.");
            }
        }

        static async Task FillPipe(
            CloudAppendBlob appendBlob,
            PipeWriter pipeWriter,
            long startIndex)
        {
            const int bufferSize = 1000000;

            await appendBlob.FetchAttributesAsync().ConfigureAwait(false);

            var length = appendBlob.Properties.Length;

            var buffer = new byte[bufferSize];

            for (var i = startIndex; i < length; i += bufferSize)
            {
                try
                {
                    var bytesRead = await appendBlob
                        .DownloadRangeToByteArrayAsync(buffer, 0, i, bufferSize)
                        .ConfigureAwait(false);

                    await pipeWriter
                        .WriteAsync(new ReadOnlyMemory<byte>(buffer, 0, bytesRead))
                        .ConfigureAwait(false);
                }
                catch
                {
                    break;
                }

                var flushResult = await pipeWriter.FlushAsync().ConfigureAwait(false);

                if (flushResult.IsCompleted)
                {
                    break;
                }
            }

            pipeWriter.Complete();
        }

        CloudAppendBlob GetAppendBlob(TKey key) => _cloudBlobContainer.GetAppendBlobReference(GetPath(key));

        string GetPath(TKey key) => $"{_keyMap.Map(key)}{_fileExtension}";
    }
}
