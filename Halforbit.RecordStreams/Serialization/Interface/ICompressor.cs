using System.Buffers;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Serialization.Interface
{
    public interface ICompressor
    {
        Task<byte[]> Compress(byte[] value);

        Task<byte[]> Decompress(byte[] data);
    }
}
