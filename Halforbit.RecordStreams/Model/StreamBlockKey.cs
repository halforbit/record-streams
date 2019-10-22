using System;

namespace Halforbit.RecordStreams.Model
{
    public struct StreamBlockKey
    {
        public StreamBlockKey(
            string name,
            DateTime time)
        {
            Name = name;

            Time = time;
        }

        public string Name { get; }

        public DateTime Time { get; }
    }
}
