using System;
using System.Collections.Generic;
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
            { "COUNT", typeof(SelectDataFromFunctionAggregate) },
            { "MIN", typeof(SelectDataFromFunctionAggregate) },
            { "MAX", typeof(SelectDataFromFunctionAggregate) },
            { "SCOPE_IDENTITY", typeof(SelectDataFromFunctionIdentity) },
            { "@@IDENTITY", typeof(SelectDataFromFunctionIdentity) },
            { "IDENT_CURRENT ", typeof(SelectDataFromFunctionIdentity) },

        };

        public ISelectDataFunction Build( SqlBuiltinScalarFunctionCallExpression functionCall, ExecuteSelectStatement.RawData rawData )
        {
            var functionName =  functionCall.FunctionName.ToUpper();

            if ( _Functions.ContainsKey( functionName ) == false )
            {
                throw new SqlFunctionNotSupportedException( functionName );
            }

            return Activator.CreateInstance( _Functions[ functionName ], args:new object[]{ functionCall, rawData }) as ISelectDataFunction;
        }
    }
}
