using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using System;
using System.Collections.Generic;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb.Helpers
{
    class RowFilterBinary : IRowFilter
    {
        private readonly SqlBinaryBooleanExpression _Expression;
        private readonly RawData _RawData;
        private readonly bool _InvertResult;

        public RowFilterBinary( RawData rawData, SqlBinaryBooleanExpression expression, bool invertResult )
        {
            _Expression = expression;
            _RawData = rawData;
            _InvertResult = invertResult;
        }

        public bool IsValid( List<RawTableRow> rawDataRows )
        {
            var leftIsValid = EvaluateSide( rawDataRows, _Expression.Left );
            var rightIsValid = EvaluateSide( rawDataRows, _Expression.Right );
            return HelperConditional.IsTrue( _Expression.Operator, leftIsValid, rightIsValid ) ^ _InvertResult;
        }

        public bool IsValid( List<RawTableJoinRow> rawDataRowList,
            List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            throw new NotImplementedException( );
        }

        private bool EvaluateSide( List<RawTableRow> rawDataRows, SqlBooleanExpression expression )
        {
            var database = ((MemoryDbConnection )_RawData.Command.Connection).GetMemoryDatabase( );
            var evaluator = new EvaluateBooleanExpression( _RawData, database, null );
            return evaluator.Evaluate( rawDataRows, expression );
        }
    }
}
