using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Serialization.ByteSpan.Implementation;
using System;

namespace Halforbit.RecordStreams.Serialization.ByteSpan.Facets
{
    public class FixedByteSpanRecordSerializationAttribute : FacetParameterAttribute
    {
        public FixedByteSpanRecordSerializationAttribute(string value = default, string configKey = default) : base(value, configKey) { }

        public override string ParameterName => "spanLength";

        public override Type TargetType => typeof(FixedByteSpanRecordSerializer);
    }
}
