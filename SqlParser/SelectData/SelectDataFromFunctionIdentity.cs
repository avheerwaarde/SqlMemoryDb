using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionIdentity: ISelectDataFunction
    {
        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;

        public SelectDataFromFunctionIdentity( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData )
        {
            _FunctionCall = functionCall;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            if ( _FunctionCall.FunctionName.ToUpper() == "SCOPE_IDENTITY" )
            {
                return MemoryDbConnection.GetMemoryDatabase( ).LastIdentitySet;
            }
            if ( _FunctionCall.FunctionName.ToUpper() == "IDENT_CURRENT" )
            {
                var database = MemoryDbConnection.GetMemoryDatabase( );
                var tableName = ((SqlLiteralExpression)_FunctionCall.Arguments.First( )).Value;
                if ( database.Tables.ContainsKey( tableName ) )
                {
                    return database.Tables[ tableName ].LastIdentitySet;
                }

                var tables = database.Tables.Where( t => t.Value.Name == tableName ).ToList( );
                if ( tables.Count == 1 )
                {
                    return tables.First( ).Value.LastIdentitySet;
                }
                throw new SqlInvalidTableNameException( tableName );
            }
            throw new NotImplementedException( );
        }

        public bool IsAggregate => false;
        public Type ReturnType => typeof( decimal );
        public string DbType => "decimal";

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
