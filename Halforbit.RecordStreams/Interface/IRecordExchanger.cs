
namespace Halforbit.RecordStreams.Interface
{
    public interface IRecordExchanger<TRecord>
    {
        void Start();

        void SubmitRecord(TRecord record);
    }
}
