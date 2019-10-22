using Halforbit.RecordStreams.Interface;
using System;
using System.Collections.Generic;

namespace Halforbit.RecordStreams.Model.Config
{
    public class RecordExchangerConfig<TRecord>
    {
        public RecordExchangerConfig(
            IRecordStream<StreamBlockKey, byte[]> stream,
            string serviceName,
            bool recordIsFixedSize,
            TimeSpan blockLength,
            TimeSpan dispatchInterval,
            TimeSpan retrieveInterval,
            Func<TRecord, byte[]> serializeRecord,
            Func<byte[], TRecord> deserializeRecord,
            Action<IEnumerable<TRecord>> onReceived,
            Action<Exception> onError)
        {
            Stream = stream;

            ServiceName = serviceName;

            RecordIsFixedSize = recordIsFixedSize;

            BlockLength = blockLength;

            DispatchInterval = dispatchInterval;

            RetrieveInterval = retrieveInterval;

            SerializeRecord = serializeRecord;

            DeserializeRecord = deserializeRecord;

            OnReceived = onReceived;

            OnError = onError;
        }

        public IRecordStream<StreamBlockKey, byte[]> Stream { get; }

        public string ServiceName { get; }

        public bool RecordIsFixedSize { get; }

        public TimeSpan BlockLength { get; }

        public TimeSpan DispatchInterval { get; }

        public TimeSpan RetrieveInterval { get; }

        public Func<TRecord, byte[]> SerializeRecord { get; }

        public Func<byte[], TRecord> DeserializeRecord { get; }

        public Action<IEnumerable<TRecord>> OnReceived { get; }

        public Action<Exception> OnError { get; }
    }
}
