using System.Collections.Generic;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb.Helpers
{
    internal interface IRowFilter
    {
        bool IsValid( List<RawTableRow> rawDataRows );

        bool IsValid( List<RawTableJoinRow> rawDataRowList,
            List<MemoryDbDataReader.ReaderFieldData> fields );

    }
}