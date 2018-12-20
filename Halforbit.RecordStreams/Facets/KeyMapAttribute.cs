using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Interface;
using System;

namespace Halforbit.RecordStreams.Facets
{
    public class KeyMapAttribute : FacetParameterAttribute
    {
        public KeyMapAttribute(string value = default, string configKey = default) : base(value, configKey) { }

        public override string ParameterName => "keyMap";

        public override Type TargetType => typeof(IRecordStream<,>);
    }
}
