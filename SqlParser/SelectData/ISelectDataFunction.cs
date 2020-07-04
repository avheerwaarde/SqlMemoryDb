using System;
using System.Collections.Generic;
using System.Data;

namespace SqlMemoryDb.SelectData
{
    interface ISelectDataFunction : ISelectData
    {
        bool IsAggregate { get; }
        Type ReturnType { get; }
        DbType DbType { get; }
        object Select( List<List<RawData.RawDataRow>> rows );
    }
}
