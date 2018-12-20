using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Interface;
using System;

namespace Halforbit.RecordStreams.Facets
{
    public class FileExtensionAttribute : FacetParameterAttribute
    {
        public FileExtensionAttribute(string value = default, string configKey = default) : base(value, configKey) { }

        public override string ParameterName => "fileExtension";

        public override Type TargetType => typeof(IRecordStream<,>);
    }
}