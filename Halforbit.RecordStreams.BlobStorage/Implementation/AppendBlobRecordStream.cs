using Halforbit.Facets.Attributes;
using Halforbit.ObjectTools.ObjectStringMap.Implementation;
using Halforbit.RecordStreams.Interface;
using Halforbit.RecordStreams.Serialization.Interface;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Buffers;
using System.Collections.Async;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.BlobStorage.Implementation
{
    public class AppendBlobRecordStream<TKey, TRecord> : 
        IRecordStream<TKey, TRecord>
    {
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

        public async Task Append(
            TKey key,
            IEnumerable<TRecord> records)
        {
            var appendBlob = GetAppendBlob(key);

            if(!await appendBlob.ExistsAsync().ConfigureAwait(false))
            {
                await appendBlob.CreateOrReplaceAsync().ConfigureAwait(false);
            }

            var serializeTasks = records
                .Select(async r => await _recordSerializer.Serialize(r).ConfigureAwait(false))
                .ToArray();

            await Task.WhenAll(serializeTasks).ConfigureAwait(false);

            var members = serializeTasks.Select(t => t.Result.ToArray());

            if(_compressor != null)
            {
                var compressTasks = members
                    .Select(async m => await _compressor.Compress(m).ConfigureAwait(false))
                    .ToArray();

                await Task.WhenAll(compressTasks).ConfigureAwait(false);

                members = compressTasks.Select(t => t.Result.ToArray());
            }

            var bytes = members.SelectMany(m => m).ToArray();

            await appendBlob.AppendFromByteArrayAsync(bytes, 0, bytes.Length).ConfigureAwait(false);

            var hasContentType = !string.IsNullOrWhiteSpace(_contentType);

            var hasContentEncoding = !string.IsNullOrWhiteSpace(_contentEncoding);

            if (hasContentType || hasContentEncoding)
            {
                var updateProperties = false;

                if(hasContentType && appendBlob.Properties.ContentType != _contentType)
                {
                    appendBlob.Properties.ContentType = _contentType;

                    updateProperties = true;
                }

                if(hasContentEncoding && appendBlob.Properties.ContentEncoding != _contentEncoding)
                {
                    appendBlob.Properties.ContentEncoding = _contentEncoding;

                    updateProperties = true;
                }

                if(updateProperties)
                {
                    await appendBlob.SetPropertiesAsync();
                }
            }
        }
        
        public async Task<bool> Delete(TKey key) => await GetAppendBlob(key)
            .DeleteIfExistsAsync()
            .ConfigureAwait(false);

        public async Task<IAsyncEnumerable<TRecord>> EnumerateAsync(TKey key)
        {
            if (!await Exists(key).ConfigureAwait(false))
            {
                return new AsyncEnumerable<TRecord>(async yield => { });
            }

            var appendBlob = GetAppendBlob(key);

            var pipe = new Pipe();

            var fillPipeTask = FillPipe(appendBlob, pipe.Writer);

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

        public async Task<bool> Exists(TKey key) => await GetAppendBlob(key)
            .ExistsAsync()
            .ConfigureAwait(false);
        
        static async Task FillPipe(
            CloudAppendBlob appendBlob,
            PipeWriter pipeWriter)
        {
            const int bufferSize = 1000000;

            await appendBlob.FetchAttributesAsync().ConfigureAwait(false);

            var length = appendBlob.Properties.Length;

            var buffer = new byte[bufferSize];

            for (var i = 0; i < length; i += bufferSize)
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
