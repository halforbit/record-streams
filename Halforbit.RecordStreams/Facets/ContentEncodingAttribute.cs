using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Interface;
using System;

namespace Halforbit.RecordStreams.Facets
{
    public class ContentEncodingAttribute : FacetParameterAttribute
    {
        public ContentEncodingAttribute(string value = default, string configKey = default) : base(value, configKey) { }

        public override string ParameterName => "contentEncoding";

        public override Type TargetType => typeof(IRecordStream<,>);
    }
}