using System.Collections.Generic;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb.Helpers
{
    internal interface IRowFilter
    {
        bool IsValid( List<RawData.RawDataRow> rawDataRows );

        bool IsValid( List<List<RawData.RawDataRow>> rawDataRowList,
            List<MemoryDbDataReader.ReaderFieldData> fields );

    }
}