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
        private readonly Dictionary<string, Type > _Functions = new Dictionary<string, Type>
        {
            { "GETDATE", typeof(SelectDataFromFunctionGetDate) },
            { "COUNT", typeof(SelectDataFromFunctionMathAggregate) },
            { "MIN", typeof(SelectDataFromFunctionMathAggregate) },
            { "MAX", typeof(SelectDataFromFunctionMathAggregate) },
            { "AVG", typeof(SelectDataFromFunctionMathAggregate) },
            { "SUM", typeof(SelectDataFromFunctionMathAggregate) },
            { "CEILING", typeof(SelectDataFromFunctionMath) },
            { "FLOOR", typeof(SelectDataFromFunctionMath) },
            { "ROUND", typeof(SelectDataFromFunctionMath) },
            { "ABS", typeof(SelectDataFromFunctionMath) },
            { "SIGN", typeof(SelectDataFromFunctionMath) },
            { "RAND", typeof(SelectDataFromFunctionRandom) },
            { "SCOPE_IDENTITY", typeof(SelectDataFromFunctionIdentity) },
            { "IDENT_CURRENT", typeof(SelectDataFromFunctionIdentity) },
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

            return Activator.CreateInstance( _Functions[ functionName ], args:new object[]{ functionCall, rawData }) as ISelectDataFunction;
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
