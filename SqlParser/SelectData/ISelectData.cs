using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    interface ISelectData
    {
        Type ReturnType { get; }
        DbType DbType { get; }
        SqlScalarExpression Expression { get; }

        object Select( RawTableJoinRow rows );
    }
}
