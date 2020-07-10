using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SqlMemoryDb.SelectData
{
    interface ISelectData
    {
        Type ReturnType { get; }
        DbType DbType { get; }

        object Select( List<RawData.RawDataRow> rows );
    }
}
