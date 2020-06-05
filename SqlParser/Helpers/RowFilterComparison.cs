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
        private readonly ExecuteQueryStatement.RawData _RawData;
        private readonly Type _Type;

        public RowFilterComparison( ExecuteQueryStatement.RawData rawData, SqlComparisonBooleanExpression expression )
        {
            _Expression = expression;
            _RawData = rawData;
            _Type = Helper.DetermineType( _Expression.Left, _Expression.Right, _RawData);

        }

        public bool IsValid( List<ExecuteQueryStatement.RawData.RawDataRow> rawDataRows )
        {
            var left = Helper.GetValue( _Expression.Left, _Type, _RawData, rawDataRows );
            var right = Helper.GetValue( _Expression.Right, _Type, _RawData, rawDataRows );
            return Helper.IsPredicateCorrect( left, right, _Expression.ComparisonOperator );
        }

        public bool IsValid( List<List<ExecuteQueryStatement.RawData.RawDataRow>> rawDataRowList,
            List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            var left = Helper.GetValue( _Expression.Left, _Type, _RawData, rawDataRowList );
            var right = Helper.GetValue( _Expression.Right, _Type, _RawData, rawDataRowList );
            return Helper.IsPredicateCorrect( left, right, _Expression.ComparisonOperator );
        }
    }
}
