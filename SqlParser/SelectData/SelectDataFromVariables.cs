using System;
using System.Collections.Generic;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    internal class SelectDataFromVariables : ISelectDataFunction
    {
        private readonly MemoryDbParameter _Parameter;

        public SelectDataFromVariables( SqlScalarExpression scalarExpression, MemoryDbCommand command )
        {
            _Parameter = Helper.GetParameter( command, scalarExpression );
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            return _Parameter.Value;
        }

        public bool IsAggregate => false;
        public Type ReturnType => _Parameter.NetDataType;
        public string DbType => _Parameter.DbType.ToString();

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
