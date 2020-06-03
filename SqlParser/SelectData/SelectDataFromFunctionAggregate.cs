using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionAggregate: ISelectDataFunction
    {
        private Type _ReturnType = typeof(int);
        private string _DbType = "int32";
        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;

        public SelectDataFromFunctionAggregate( SqlBuiltinScalarFunctionCallExpression functionCall, ExecuteSelectStatement.RawData rawData )
        {
            _FunctionCall = functionCall;
        }

        public object Select( List<ExecuteSelectStatement.RawData.RawDataRow> rows )
        {
            throw new NotImplementedException( );
        }

        public bool IsAggregate => true;
        public Type ReturnType => _ReturnType;
        public string DbType => _DbType;

        public object Select( List<List<ExecuteSelectStatement.RawData.RawDataRow>> rows )
        {
            switch ( _FunctionCall.FunctionName.ToUpper() )
            {
                case "COUNT":
                    return rows.Count;
                default:
                    throw new NotImplementedException();
            }

        }
    }
}
