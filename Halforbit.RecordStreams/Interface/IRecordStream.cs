﻿using System.Collections.Async;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Interface
{
    public interface IRecordStream<TKey, TRecord>
    {
        Task<IAsyncEnumerable<TRecord>> EnumerateAsync(TKey key);

        Task Append(
            TKey key, 
            IEnumerable<TRecord> records);

        Task<bool> Delete(TKey key);

        Task<bool> Exists(TKey key);
    }
}
