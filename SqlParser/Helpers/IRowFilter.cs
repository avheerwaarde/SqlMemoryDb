using System.Collections.Generic;

namespace SqlMemoryDb.Helpers
{
    internal interface IRowFilter
    {
        bool IsValid( List<ExecuteSelectStatement.RawData.RawDataRow> rawDataRows );
    }
}