using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.SelectData
{
    interface ISelectData
    {
        object Select( List<ExecuteSelectStatement.RawData.RawDataRow> rows );
    }
}
