using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Serialization.Text.Implementation;
using System;

namespace Halforbit.RecordStreams.Serialization.Text.Facets
{
    public class TextLineRecordSerializationAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(TextLinesRecordSerializer);
    }
}
