using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionAggregate: ISelectDataFunction
    {
        public bool IsAggregate => true;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        string ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(int);
        private readonly string _DbType = "int32";
        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly ExecuteQueryStatement.RawData _RawData;
        private readonly SqlColumnRefExpression _ColumnRef;

        public SelectDataFromFunctionAggregate( SqlBuiltinScalarFunctionCallExpression functionCall, ExecuteQueryStatement.RawData rawData )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( _FunctionCall.FunctionName.ToUpper() == "MIN" || _FunctionCall.FunctionName.ToUpper() == "MAX" )
            {
                _ColumnRef = GetSingleColumn(  );
                var tc = Helper.GetTableColumn( _ColumnRef, _RawData );
                _ReturnType = tc.Column.NetDataType;
                _DbType = tc.Column.DbDataType.ToString( );
            }
        }

        public object Select( List<ExecuteQueryStatement.RawData.RawDataRow> rows )
        {
            throw new NotImplementedException( );
        }


        public object Select( List<List<ExecuteQueryStatement.RawData.RawDataRow>> rows )
        {
            switch ( _FunctionCall.FunctionName.ToUpper() )
            {
                case "COUNT":
                    return rows.Count;
                case "MIN":
                    return rows.Select(  r => Helper.GetValue( _ColumnRef, null, _RawData, r ) ).Min();
                case "MAX":
                    return rows.Select(  r => Helper.GetValue( _ColumnRef, null, _RawData, r ) ).Max();
                default:
                    throw new NotImplementedException();
            }

        }

        private SqlColumnRefExpression GetSingleColumn( )
        {
            if ( _FunctionCall.Arguments != null && _FunctionCall.Arguments.Count == 1 && _FunctionCall.Arguments[0] is SqlColumnRefExpression )
            {
                return ( SqlColumnRefExpression )_FunctionCall.Arguments[ 0 ];
            }
            throw new NotImplementedException("Currently we only allow a single column as function parameter");
        }
    }
}
