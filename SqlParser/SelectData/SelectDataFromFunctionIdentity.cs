﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionIdentity: ISelectData
    {
        public Type ReturnType => typeof( long );
        public DbType DbType => DbType.Int64;
        public SqlScalarExpression Expression => _FunctionCall;

        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromFunctionIdentity( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
        }

        public object Select( RawTableJoinRow rows )
        {
            if ( _FunctionCall.FunctionName.ToUpper() == "SCOPE_IDENTITY" )
            {
                return ((MemoryDbConnection )_RawData.Command.Connection).GetMemoryDatabase( ).LastIdentitySet;
            }
            if ( _FunctionCall.FunctionName.ToUpper() == "IDENT_CURRENT" )
            {
                var database = ((MemoryDbConnection )_RawData.Command.Connection).GetMemoryDatabase( );
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

    }
}
