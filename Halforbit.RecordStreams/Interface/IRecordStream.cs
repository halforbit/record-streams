using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Halforbit.RecordStreams.Interface
{
    public interface IRecordStream<TKey, TRecord>
    {
        Task<IAsyncEnumerable<TRecord>> EnumerateAsync(TKey key, long startIndex = 0);

        Task<IAsyncEnumerable<(long, TRecord)>> EnumerateIndexedAsync(TKey key, long startIndex = 0);

        Task Append(
            TKey key, 
            IEnumerable<TRecord> records);

        Task<bool> Delete(TKey key);

        Task<bool> Exists(TKey key);

        Task<long> GetLength(TKey key);

        Task<IEnumerable<TKey>> ListKeys(Expression<Func<TKey, bool>> predicate = null);
    }
}
