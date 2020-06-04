using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionIdentity: ISelectDataFunction
    {
        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;

        public SelectDataFromFunctionIdentity( SqlBuiltinScalarFunctionCallExpression functionCall, ExecuteQueryStatement.RawData rawData )
        {
            _FunctionCall = functionCall;
        }

        public object Select( List<ExecuteQueryStatement.RawData.RawDataRow> rows )
        {
            throw new NotImplementedException( );
        }

        public bool IsAggregate => false;
        public Type ReturnType => typeof( decimal );
        public string DbType => "decimal";

        public object Select( List<List<ExecuteQueryStatement.RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
