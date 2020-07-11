using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionRandom : ISelectData
    {
        public Type ReturnType => typeof( double );
        public DbType DbType => DbType.Double;
        public SqlScalarExpression Expression => _FunctionCall;

        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromFunctionRandom( SqlBuiltinScalarFunctionCallExpression functionCall,
            RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            if ( _FunctionCall.Arguments != null && _FunctionCall.Arguments.Any() )
            {
                var seed = Helper.GetValue( _FunctionCall.Arguments.First( ), typeof( int ), _RawData, rows );
                return new Random( (int)seed ).NextDouble(  );
            }
            return new Random().NextDouble(  );
        }
    }
}
