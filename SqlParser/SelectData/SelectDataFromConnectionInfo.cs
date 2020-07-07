using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromConnectionInfo : ISelectDataFunction
    {
        public bool IsAggregate => false;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        DbType ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(bool);
        private readonly DbType _DbType = DbType.Boolean;

        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromConnectionInfo( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( info.ReturnDbType.HasValue )
            {
                _ReturnType = info.ReturnType;
                _DbType = info.ReturnDbType.Value;
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            switch ( _FunctionCall.FunctionName.ToUpper( ) )
            {
                case "CURRENT_USER": 
                case "SESSION_USER": 
                case "USER_NAME": 
                    return "dbo";
                case "SYSTEM_USER":
                    return Environment.UserName ?? "Unknown";
                default:
                    throw new NotImplementedException();
            }
        }


        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
