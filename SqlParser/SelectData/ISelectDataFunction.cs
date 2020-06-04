using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb.SelectData
{
    interface ISelectDataFunction : ISelectData
    {
        bool IsAggregate { get; }
        Type ReturnType { get; }
        string DbType { get; }
        object Select( List<List<ExecuteQueryStatement.RawData.RawDataRow>> rows );
    }
}
