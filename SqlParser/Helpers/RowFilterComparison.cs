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
        private readonly RawData _RawData;
        private readonly Type _Type;
        private readonly bool _InvertResult;

        public RowFilterComparison( RawData rawData, SqlComparisonBooleanExpression expression, bool invertResult )
        {
            _Expression = expression;
            _RawData = rawData;
            _Type = Helper.DetermineType( _Expression.Left, _Expression.Right, _RawData);
            _InvertResult = invertResult;
        }

        public bool IsValid( List<RawData.RawDataRow> rawDataRows )
        {
            var left = Helper.GetValue( _Expression.Left, _Type, _RawData, rawDataRows );
            var right = Helper.GetValue( _Expression.Right, _Type, _RawData, rawDataRows );
            return HelperConditional.IsPredicateCorrect( left, right, _Expression.ComparisonOperator ) ^ _InvertResult;
        }

        public bool IsValid( List<List<RawData.RawDataRow>> rawDataRowList,
            List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            var left = Helper.GetValue( _Expression.Left, _Type, _RawData, rawDataRowList );
            var right = Helper.GetValue( _Expression.Right, _Type, _RawData, rawDataRowList );
            return HelperConditional.IsPredicateCorrect( left, right, _Expression.ComparisonOperator ) ^ _InvertResult;
        }
    }
}
