using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionText : ISelectDataFunction
    {
        public bool IsAggregate => false;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        string ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(string);
        private readonly string _DbType = "string";
        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromFunctionText( SqlBuiltinScalarFunctionCallExpression functionCall,
            RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( string.IsNullOrWhiteSpace( info.ReturnDbType ) == false )
            {
                _ReturnType = info.ReturnType;
                _DbType = info.ReturnDbType;
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            var arguments = GetArgumentList( );

            switch ( _FunctionCall.FunctionName.ToUpper() )
            {
                case "ASCII": return string.IsNullOrWhiteSpace( arguments.First( ) ) ? null : (byte?)arguments[ 0 ][ 0 ];
                default:
                    throw new NotImplementedException();
            }
        }

        private List<string> GetArgumentList( )
        {
            var list = new List<string>( );
            if ( _FunctionCall.Arguments != null )
            {
                foreach ( var argument in _FunctionCall.Arguments )
                {
                    var value = Helper.GetValue( argument, typeof(string), _RawData, new List<RawData.RawDataRow>() );
                    list.Add( value?.ToString( ) );
                }
            }
            return list;
        }

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
