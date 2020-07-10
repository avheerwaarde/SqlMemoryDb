using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromBinaryScalarExpression : ISelectData
    {
        Type ISelectData.ReturnType => _ReturnType;
        DbType ISelectData.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(bool);
        private readonly DbType _DbType = DbType.Boolean;
        private readonly SqlBinaryScalarExpression _Expression;
        private readonly RawData _RawData;

        public SelectDataFromBinaryScalarExpression( SqlBinaryScalarExpression expression, RawData rawData )
        {
            _Expression = expression;
            _RawData = rawData;
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
