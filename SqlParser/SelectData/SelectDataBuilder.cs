using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;

namespace SqlMemoryDb.SelectData
{
    class SelectDataBuilder
    {
        private readonly Dictionary<string, SelectDataFunctionInfo > _Functions = new Dictionary<string, SelectDataFunctionInfo>
        {
            { "GETDATE"       , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionGetDate) }},
            { "COUNT"         , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "MIN"           , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "MAX"           , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "AVG"           , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "SUM"           , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMathAggregate) }},
            { "CEILING"       , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "FLOOR"         , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "ROUND"         , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "ABS"           , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "SIGN"          , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionMath) }},
            { "RAND"          , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionRandom) }},
            { "SCOPE_IDENTITY", new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionIdentity) }},
            { "IDENT_CURRENT" , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionIdentity) }},
            { "ASCII" , new SelectDataFunctionInfo{ SelectType = typeof(SelectDataFromFunctionText), MinimalArgumentCount = 1, ReturnType = typeof(byte), ReturnDbType = "byte"}},
        };

        private readonly Dictionary<string, Type > _GlobalVariables = new Dictionary<string, Type>
        {
            { "@@IDENTITY", typeof(SelectDataFromGlobalVariables) }
        };

        public ISelectDataFunction Build( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData )
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

            return Activator.CreateInstance( info.SelectType, args:new object[]{ functionCall, rawData, info }) as ISelectDataFunction;
        }

        internal ISelectDataFunction Build( string fullMethod, RawData rawData )
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

        public ISelectDataFunction BuildGlobalVariable( string globalVariableName, RawData rawData )
        {
            if ( _GlobalVariables.ContainsKey( globalVariableName.ToUpper() ) == false )
            {
                throw new SqlFunctionNotSupportedException( globalVariableName );
            }

            return Activator.CreateInstance( _GlobalVariables[ globalVariableName.ToUpper() ], args:new object[]{ globalVariableName, rawData }) as ISelectDataFunction;
        }
    }
}
