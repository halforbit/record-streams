﻿using Halforbit.Facets.Attributes;
using Halforbit.RecordStreams.Serialization.ByteSpan.Implementation;
using System;

namespace Halforbit.RecordStreams.Serialization.ByteSpan.Facets
{
    public class ByteSpanRecordSerialization : FacetAttribute
    {
        public override Type TargetType => typeof(ByteSpanRecordSerializer);
    }
}
