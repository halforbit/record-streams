using System;

namespace Halforbit.RecordStreams.Exceptions
{
    public class IncompleteKeyException : Exception
    {
        public IncompleteKeyException(string message) : base(message) { }
    }
}
