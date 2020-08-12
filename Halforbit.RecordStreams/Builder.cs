using Halforbit.ObjectTools.DeferredConstruction;
using Halforbit.RecordStreams.FileStreams;
using Halforbit.RecordStreams.Interface;
using Halforbit.RecordStreams.Serialization.ByteSpan.Implementation;
using Halforbit.RecordStreams.Serialization.GZip.Implementation;
using Halforbit.RecordStreams.Serialization.Interface;
using Halforbit.RecordStreams.Serialization.JsonLines.Implementation;

namespace Halforbit.RecordStreams
{
    public class Builder :
        IConstructionNode,
        INeedsIntegration,
        INeedsContentType,
        INeedsContentEncoding,
        INeedsSerialization,
        INeedsCompression,
        INeedsFileExtension,
        INeedsMap
    {
        public Builder(Constructable root)
        {
            Root = root;
        }

        public Constructable Root { get; }
    }

    public class Builder<TKey, TValue> :
        IConstructionNode,
        IRecordStreamDescription<TKey, TValue>
    {
        public Builder(Constructable root)
        {
            Root = root;
        }

        public Constructable Root { get; }
    }

    public interface INeedsIntegration : IConstructionNode { }

    namespace FileStreams
    {
        public interface INeedsContentType : IConstructionNode { }

        public interface INeedsContentEncoding : IConstructionNode { }

        public interface INeedsSerialization : IConstructionNode { }

        public interface INeedsCompression : IConstructionNode { }

        public interface INeedsFileExtension : IConstructionNode { }
    }

    public interface INeedsMap : IConstructionNode { }

    public interface IRecordStreamDescription<TKey, Value> : IConstructionNode { }

    public static class RecordStream
    {
        public static INeedsIntegration Describe()
        {
            return new Builder(null);
        }
    }

    public static class RecordStreamBuilderExtensions
    {
        public static IRecordStreamDescription<TKey, TValue> Map<TKey, TValue>(
            this INeedsMap target,
            string map)
        {
            return new Builder<TKey, TValue>(target.Root
                .TypeArguments(typeof(TKey), typeof(TValue))
                .Argument("keyMap", map));
        }

        public static IRecordStream<TKey, TValue> Build<TKey, TValue>(
            this IRecordStreamDescription<TKey, TValue> description)
        {
            return (IRecordStream<TKey, TValue>)description.Root.Construct();
        }
    }

    public static class FileStreamsBuilderExtensions
    {
        public static INeedsCompression Serialization<TSerializer>(
            this INeedsSerialization target,
            TSerializer serializer)
            where TSerializer : IRecordSerializer
        {
            return new Builder(target.Root.Argument(
                "recordSerializer",
                serializer));
        }

        public static INeedsCompression JsonLinesSerialization(
            this INeedsSerialization target)
        {
            return new Builder(target.Root.Argument(
                "recordSerializer",
                default(Constructable)
                    .Type(typeof(JsonLinesRecordSerializer))));
        }

        public static INeedsCompression ByteSpanSerialization(
            this INeedsSerialization target)
        {
            return new Builder(target.Root.Argument(
                "recordSerializer",
                default(Constructable)
                    .Type(typeof(ByteSpanRecordSerializer))));
        }

        public static INeedsCompression FixedByteSpanSerialization(
            this INeedsSerialization target,
            int spanLength)
        {
            return new Builder(target.Root.Argument(
                "recordSerializer",
                default(Constructable)
                    .Type(typeof(FixedByteSpanRecordSerializer))
                    .Argument("spanLength", spanLength)));
        }

        public static INeedsFileExtension NoCompression(this INeedsCompression target)
        {
            return new Builder(target.Root.ArgumentNull("compressor"));
        }

        public static INeedsFileExtension GZipCompression(this INeedsCompression target)
        {
            return new Builder(target.Root.Argument(
                "compressor",
                default(Constructable).Type(typeof(GZipCompressor))));
        }

        public static INeedsMap FileExtension(
            this INeedsFileExtension target,
            string fileExtension)
        {
            return new Builder(target.Root.Argument("fileExtension", fileExtension));
        }
    }
}
