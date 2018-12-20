using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.BlobStorage.Implementation;
using System;

namespace Halforbit.RecordStreams.BlobStorage.Facets
{
    public class ConnectionStringAttribute : FacetParameterAttribute
    {
        public ConnectionStringAttribute(string value = default, string configKey = default) : base(value, configKey) { }

        public override string ParameterName => "connectionString";

        public override Type TargetType => typeof(AppendBlobRecordStream<,>);
    }
}
