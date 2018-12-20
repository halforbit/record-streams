using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Interface;
using System;

namespace Halforbit.RecordStreams.Facets
{
    public class ContentTypeAttribute : FacetParameterAttribute
    {
        public ContentTypeAttribute(string value = default, string configKey = default) : base(value, configKey) { }

        public override string ParameterName => "contentType";

        public override Type TargetType => typeof(IRecordStream<,>);
    }
}