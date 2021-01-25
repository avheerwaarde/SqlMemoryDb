using System;
using System.Collections.Generic;
using System.Data;

namespace SqlMemoryDb.SelectData
{
    interface ISelectDataAggregate : ISelectData
    {
        object Select( List<RawTableJoinRow> rows );
    }
}
