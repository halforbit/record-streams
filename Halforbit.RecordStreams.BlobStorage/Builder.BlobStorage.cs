using Halforbit.ObjectTools.DeferredConstruction;
using Halforbit.RecordStreams.BlobStorage;
using Halforbit.RecordStreams.BlobStorage.Implementation;
using Halforbit.RecordStreams.FileStreams;

namespace Halforbit.RecordStreams
{
    namespace BlobStorage
    {
        public interface INeedsConnectionString : IConstructionNode { }

        public interface INeedsContainer : IConstructionNode { }

        public class Builder :
            IConstructionNode,
            INeedsConnectionString,
            INeedsContainer
        {
            public Builder(Constructable root)
            {
                Root = root;
            }

            public Constructable Root { get; }
        }
    }

    public static class BlobStorageBuilderExtensions
    {
        public static INeedsConnectionString BlobStorage(
            this INeedsIntegration target)
        {
            return new BlobStorage.Builder(target.Root.Type(typeof(AppendBlobRecordStream<,>)));
        }

        public static INeedsContainer ConnectionString(
            this INeedsConnectionString target,
            string connectionString)
        {
            return new BlobStorage.Builder(target.Root.Argument("connectionString", connectionString));
        }

        public static INeedsContentType Container(
            this INeedsContainer target,
            string container)
        {
            return new Builder(target.Root.Argument("containerName", container));
        }

        public static INeedsContentEncoding ContentType(
            this INeedsContentType target,
            string contentType)
        {
            return new Builder(target.Root.Argument("contentType", contentType));
        }

        public static INeedsContentEncoding DefaultContentType(
            this INeedsContentType target)
        {
            return target.ContentType("application/octet-stream");
        }

        public static INeedsSerialization ContentEncoding(
            this INeedsContentEncoding target,
            string contentEncoding)
        {
            return new Builder(target.Root.Argument("contentEncoding", contentEncoding));
        }

        public static INeedsSerialization DefaultContentEncoding(
            this INeedsContentEncoding target)
        {
            return target.ContentEncoding(string.Empty);
        }
    }
}
