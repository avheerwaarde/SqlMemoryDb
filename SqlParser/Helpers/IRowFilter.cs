using System.Collections.Generic;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb.Helpers
{
    internal interface IRowFilter
    {
        bool IsValid( List<ExecuteQueryStatement.RawData.RawDataRow> rawDataRows );

        bool IsValid( List<List<ExecuteQueryStatement.RawData.RawDataRow>> rawDataRowList,
            List<MemoryDbDataReader.ReaderFieldData> fields );

    }
}