using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Serialization.GZip.Implementation;
using System;

namespace Halforbit.RecordStreams.Serialization.GZip.Facets
{
    public class GZipCompressionAttribute : FacetAttribute
    {
        public override Type TargetType => typeof(GZipCompressor);
    }
}
