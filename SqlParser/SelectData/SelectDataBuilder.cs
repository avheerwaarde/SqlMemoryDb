using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace SqlMemoryDb.SelectData
{
    class SelectDataBuilder
    {
        private static readonly Dictionary<string, SelectDataFunctionInfo > _Functions = new Dictionary<string, SelectDataFunctionInfo>
        {
            { "CURRENT_TIMESTAMP"    , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), ReturnType = typeof(DateTime), ReturnDbType = DbType.DateTime }},
            { "GETDATE"              , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), ReturnType = typeof(DateTime), ReturnDbType = DbType.DateTime }},
            { "GETUTCDATE"           , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), ReturnType = typeof(DateTime), ReturnDbType = DbType.DateTime }},
            { "SYSDATETIME"          , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), ReturnType = typeof(DateTime), ReturnDbType = DbType.DateTime }},
            { "SYSUTCDATETIME"       , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), ReturnType = typeof(DateTime), ReturnDbType = DbType.DateTime }},
            { "SYSDATETIMEOFFSET"    , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), ReturnType = typeof(DateTime), ReturnDbType = DbType.DateTime }},
            { "DATEADD"              , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), MinimalArgumentCount = 3, ReturnType = typeof(DateTime), ReturnDbType = DbType.DateTime }},
            { "DATEDIFF"             , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), MinimalArgumentCount = 3, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "DATENAME"             , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), MinimalArgumentCount = 2, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "DATEPART"             , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), MinimalArgumentCount = 2, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "DAY"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), MinimalArgumentCount = 1, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "MONTH"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), MinimalArgumentCount = 1, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "YEAR"                 , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionDate), MinimalArgumentCount = 1, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "COUNT"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "MIN"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "MAX"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "AVG"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "SUM"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "CEILING"              , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "FLOOR"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "ROUND"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "ABS"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "SIGN"                 , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "RAND"                 , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionRandom) }},
            { "SCOPE_IDENTITY"       , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionIdentity), ReturnType = typeof(long), ReturnDbType = DbType.Int64 }},
            { "IDENT_CURRENT"        , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionIdentity), ReturnType = typeof(long), ReturnDbType = DbType.Int64 }},
            { "ASCII"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(byte), ReturnDbType = DbType.Byte }},
            { "CHAR"                 , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(char), ReturnDbType = DbType.Object }},
            { "CHARINDEX"            , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 2, ReturnType = typeof(int), ReturnDbType = DbType.Int32}},
            { "CONCAT"               , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 2, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "DATALENGTH"           , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "LEFT"                 , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 2, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "LEN"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "LOWER"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "LTRIM"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "NCHAR"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(char), ReturnDbType = DbType.Object }},
            { "PATINDEX"             , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 2, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "REPLACE"              , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 3, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "RIGHT"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 2, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "RTRIM"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "SPACE"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "STR"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "STUFF"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 4, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "SUBSTRING"            , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 3, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "UPPER"                , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(string), ReturnDbType = DbType.String }},
            { "CAST"                 , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionConversion), MinimalArgumentCount = 1 }},
            { "TRY_CAST"             , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionConversion), MinimalArgumentCount = 1 }},
            { "CONVERT"              , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionConversion), MinimalArgumentCount = 1 }},
            { "TRY_CONVERT"          , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionConversion), MinimalArgumentCount = 1 }},
            { "CURRENT_USER"         , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromConnectionInfo), ReturnType = typeof(string), ReturnDbType = DbType.String}},
            { "ISDATE"               , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionConversion), MinimalArgumentCount = 1, ReturnType = typeof(int), ReturnDbType = DbType.Int32 }},
            { "ISNULL"               , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionConversion), MinimalArgumentCount = 2 }},
            { "ISNUMERIC"            , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionConversion), MinimalArgumentCount = 1 }},
            { "LEAD"                 , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionLeadLag), MinimalArgumentCount = 2 }},
            { "LAG"                  , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionLeadLag), MinimalArgumentCount = 2 }},
            { "SESSION_USER"         , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromConnectionInfo), ReturnType = typeof(string), ReturnDbType = DbType.String}},
            { "SYSTEM_USER"          , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromConnectionInfo), ReturnType = typeof(string), ReturnDbType = DbType.String}},
            { "USER_NAME"            , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromConnectionInfo)}},
        };

        private static readonly Dictionary<string, Type > _GlobalVariables = new Dictionary<string, Type>
        {
            { "@@IDENTITY", typeof(SelectDataFromGlobalVariables) },
            { "@@VERSION", typeof(SelectDataFromGlobalVariables) }

        };

        public ISelectData Build( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData )
        {
            var functionName =  functionCall.FunctionName.ToUpper();

            if ( _Functions.ContainsKey( functionName ) == false )
            {
                throw new SqlFunctionNotSupportedException( functionName );
            }

            var info = _Functions[ functionName ];
            if ( info.MinimalArgumentCount > 0 
                 && ( functionCall.Arguments == null || functionCall.Arguments.Count < info.MinimalArgumentCount ))
            {
                throw new SqlInvalidFunctionParameterCountException( functionCall.FunctionName, info.MinimalArgumentCount );
            }

            return Activator.CreateInstance( info.SelectType, args:new object[]{ functionCall, rawData, info }) as ISelectData;
        }

        internal ISelectData Build( string fullMethod, RawData rawData )
        {
            // This is a bit of a hack to create an instance of this type.
            // There are no public constructors, so we hack it a little bit.
            // We call an internal static method "Create" to create an instance.
            // Please be aware, that parameters are currently not supported.
            //
            var functionName = fullMethod.Substring( 0, fullMethod.IndexOf( '(' ) ).Trim();
            var function = (SqlBuiltinScalarFunctionCallExpression) typeof(SqlBuiltinScalarFunctionCallExpression)
                .GetMethod( "Create", BindingFlags.NonPublic | BindingFlags.Static)
                .Invoke( null, new object[]{ functionName, null } );
            return Build( function, rawData );

        }

        public ISelectData BuildGlobalVariable( string globalVariableName, RawData rawData )
        {
            if ( _GlobalVariables.ContainsKey( globalVariableName.ToUpper() ) == false )
            {
                throw new SqlFunctionNotSupportedException( globalVariableName );
            }

            return Activator.CreateInstance( _GlobalVariables[ globalVariableName.ToUpper() ], args:new object[]{ globalVariableName, rawData }) as ISelectData;
        }

        public static DataTypeInfo GetDataTypeFromFunction( string functionName )
        {
            functionName = functionName.ToUpper( );
            var keyValue = _Functions.FirstOrDefault( f => f.Key == functionName );
            if ( keyValue.Key != null && keyValue.Value.ReturnType != null  )
            {
                return new DataTypeInfo{ NetDataType =  keyValue.Value.ReturnType, DbDataType = keyValue.Value.ReturnDbType.Value };
            }

            return null;
        }
    }
}
