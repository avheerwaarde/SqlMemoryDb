using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromVariables : ISelectData
    {
        public Type ReturnType => _Parameter.NetDataType;
        public DbType DbType => _Parameter.DbType;
        public SqlScalarExpression Expression { get; }

        private readonly MemoryDbParameter _Parameter;

        public SelectDataFromVariables( SqlScalarExpression scalarExpression, MemoryDbCommand command )
        {
            Expression = scalarExpression;
            _Parameter = Helper.GetParameter( command, scalarExpression );
        }

        public object Select( RawTableJoinRow rows )
        {
            return _Parameter.Value;
        }
    }
}
