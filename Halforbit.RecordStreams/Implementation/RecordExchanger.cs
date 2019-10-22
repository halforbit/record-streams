using Halforbit.RecordStreams.Interface;
using Halforbit.RecordStreams.Model;
using Halforbit.RecordStreams.Model.Config;
using System;
using System.Collections.Async;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Implementation
{
    public class RecordExchanger<TRecord> : IRecordExchanger<TRecord>
    {
        const int RecordSizeHeaderLength = 3;

        static readonly DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        readonly RecordExchangerConfig<TRecord> _config;

        readonly ConcurrentQueue<TRecord> _outboundRecords = new ConcurrentQueue<TRecord>();

        bool _isStarted;

        public RecordExchanger(
            RecordExchangerConfig<TRecord> config)
        {
            _config = config;
        }

        public void Start()
        {
            var t1 = Task.Run(DispatchOutboundRecords);

            var t2 = Task.Run(RetrieveInboundRecords);

            _isStarted = true;
        }

        public void SubmitRecord(TRecord record)
        {
            if(_isStarted)
            {
                _outboundRecords.Enqueue(record);
            }
        }

        async Task DispatchOutboundRecords()
        {
            var outboundRecords = new List<byte[]>();

            while (true)
            {
                var startTime = DateTime.UtcNow;

                try
                {
                    while (_outboundRecords.TryDequeue(out var outboundRecord))
                    {
                        outboundRecords.Add(_config.SerializeRecord(outboundRecord));
                    }

                    if (outboundRecords.Any())
                    {
                        var currentBlock = GetCurrentBlockNumber();

                        var key = new StreamBlockKey(
                            name: _config.ServiceName,
                            time: GetBlockStartTime(currentBlock));

                        await _config.Stream.Append(key, outboundRecords);

                        outboundRecords.Clear();
                    }
                }
                catch (Exception ex)
                {
                    _config.OnError(ex);
                }

                var elapsed = DateTime.UtcNow - startTime;

                if (_config.DispatchInterval > elapsed)
                {
                    await Task.Delay(_config.DispatchInterval - elapsed);
                }
            }
        }

        async Task RetrieveInboundRecords()
        {
            var activeBlock = GetCurrentBlockNumber();

            var index = 0L;

            while (true)
            {
                var startTime = DateTime.UtcNow;

                var shouldWait = true;

                try
                {
                    var currentBlock = GetCurrentBlockNumber();

                    var key = new StreamBlockKey(
                        name: _config.ServiceName,
                        time: GetBlockStartTime(activeBlock));

                    var inboundRecords = await (await _config.Stream
                        .EnumerateAsync(key, index))
                        .ToListAsync();

                    if (inboundRecords.Any())
                    {
                        _config.OnReceived(inboundRecords
                            .Select(r => _config.DeserializeRecord(r))
                            .ToList());
                    }

                    index +=
                        (_config.RecordIsFixedSize ? 0 : inboundRecords.Count * RecordSizeHeaderLength) +
                        inboundRecords.Sum(r => r.Length);

                    if (currentBlock > activeBlock)
                    {
                        activeBlock = currentBlock;

                        index = 0;

                        shouldWait = false;
                    }
                }
                catch (Exception ex)
                {
                    _config.OnError(ex);
                }

                if (shouldWait)
                {
                    var elapsed = DateTime.UtcNow - startTime;

                    if (_config.RetrieveInterval > elapsed)
                    {
                        await Task.Delay(_config.RetrieveInterval - elapsed);
                    }
                }
            }
        }

        int GetCurrentBlockNumber() => (int)Math.Floor((DateTime.UtcNow - _epoch).TotalMinutes / _config.BlockLength.TotalMinutes);

        DateTime GetBlockStartTime(int blockNumber) => _epoch.AddMinutes(blockNumber * _config.BlockLength.TotalMinutes);
    }
}
