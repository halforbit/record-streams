using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.BlobStorage.Implementation;
using System;

namespace Halforbit.RecordStreams.BlobStorage.Facets
{
    public class ContainerNameAttribute : FacetParameterAttribute
    {
        public ContainerNameAttribute(string value = default, string configKey = default) : base(value, configKey) { }

        public override string ParameterName => "containerName";

        public override Type TargetType => typeof(AppendBlobRecordStream<,>);
    }
}
