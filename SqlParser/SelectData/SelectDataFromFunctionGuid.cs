using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionGuid : ISelectData
    {
        Type ISelectData.ReturnType => _ReturnType;
        DbType ISelectData.DbType => _DbType;
        public SqlScalarExpression Expression => _FunctionCall;

        private readonly Type _ReturnType = typeof( Guid );
        private readonly DbType _DbType = DbType.Guid;

        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromFunctionGuid( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData, SelectDataFunctionInfo info )
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
            switch ( _FunctionCall.FunctionName.ToUpper() )
            {
                case "NEWID":
                    return Guid.NewGuid();
                default:
                    throw new NotImplementedException();
            }
        }

    }
}
