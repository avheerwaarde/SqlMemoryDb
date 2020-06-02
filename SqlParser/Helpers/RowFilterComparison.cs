using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb.Helpers
{
    class RowFilterComparison: IRowFilter
    {
        private readonly SqlComparisonBooleanExpression _Expression;
        private readonly object _CachedLeftValue;
        private readonly object _CachedRightValue;
        private readonly ExecuteSelectStatement.RawData _RawData;
        private readonly Type _Type;

        public RowFilterComparison( ExecuteSelectStatement.RawData rawData, SqlComparisonBooleanExpression expression )
        {
            _Expression = expression;
            _RawData = rawData;
            _Type = Helper.DetermineType( _Expression.Left, _Expression.Right, _RawData);
            _CachedLeftValue = Helper.GetValue( _Expression.Left, _Type, rawData, null );
            _CachedRightValue = Helper.GetValue( _Expression.Right, _Type, rawData, null );

        }


        public bool IsValid( List<ExecuteSelectStatement.RawData.RawDataRow> rawDataRows )
        {
            var left = _CachedLeftValue ?? Helper.GetValue( _Expression.Left, _Type, _RawData, rawDataRows );
            var right = _CachedRightValue ?? Helper.GetValue( _Expression.Right, _Type, _RawData, rawDataRows );
            return Helper.IsPredicateCorrect( left, right, _Expression.ComparisonOperator );
        }
    }
}
