using System.Collections.Generic;

namespace SqlMemoryDb.Helpers
{
    internal interface IRowFilter
    {
        bool IsValid( List<ExecuteQueryStatement.RawData.RawDataRow> rawDataRows );
    }
}