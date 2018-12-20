using Halforbit.RecordStreams.Serialization.Interface;
using SharpCompress.Common;
using SharpCompress.Readers;
using SharpCompress.Writers;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Serialization.GZip.Implementation
{
    public class GZipCompressor : ICompressor
    {
        public async Task<byte[]> Compress(byte[] value)
        {
            using (var sourceStream = new MemoryStream(value))
            using (var destStream = new MemoryStream())
            {
                using (var writer = WriterFactory.Open(
                    destStream,
                    ArchiveType.GZip,
                    new WriterOptions(CompressionType.GZip)))
                {
                    writer.Write(null, sourceStream);
                }

                //using (var gZipStream = new GZipStream(
                //    destStream,
                //    CompressionMode.Compress))
                //{

                //    await sourceStream.CopyToAsync(gZipStream);
                //}

                return destStream.ToArray();
            }
        }

        public async Task<byte[]> Decompress(byte[] data)
        {

            using (var sourceStream = new MemoryStream(data))
            using (var reader = ReaderFactory.Open(sourceStream))
            //using (var gZipStream = new GZipStream(sourceStream, CompressionMode.Decompress))
            using (var destStream = new MemoryStream())
            {
                reader.CompressedBytesRead += (s, e) =>
                {

                };

                throw new NotImplementedException();

                //await gZipStream.CopyToAsync(destStream);

                //return destStream.ToArray();
            }
        }

        private void Reader_CompressedBytesRead(object sender, CompressedBytesReadEventArgs e)
        {
            throw new System.NotImplementedException();
        }
    }
}
