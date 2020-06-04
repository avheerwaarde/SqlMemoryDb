using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionGetDate : ISelectDataFunction
    {
        public SelectDataFromFunctionGetDate( SqlBuiltinScalarFunctionCallExpression functionCall, ExecuteQueryStatement.RawData rawData )
        {

        }

        public object Select( List<ExecuteQueryStatement.RawData.RawDataRow> rows )
        {
            return DateTime.Now;
        }

        public bool IsAggregate => false;
        public Type ReturnType => typeof( DateTime );
        public string DbType => "datetime";

        public object Select( List<List<ExecuteQueryStatement.RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
