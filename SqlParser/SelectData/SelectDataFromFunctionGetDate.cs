using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionGetDate : ISelectDataFunction
    {
        public SelectDataFromFunctionGetDate( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData, SelectDataFunctionInfo info )
        {

        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            return DateTime.Now;
        }

        public bool IsAggregate => false;
        public Type ReturnType => typeof( DateTime );
        public string DbType => "datetime";

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
