using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Serialization.JsonLines.Implementation;
using System;

namespace Halforbit.RecordStreams.Serialization.JsonLines.Facets
{
    public class JsonLinesRecordSerialization : FacetAttribute
    {
        public override Type TargetType => typeof(JsonLinesRecordSerializer);
    }
}
