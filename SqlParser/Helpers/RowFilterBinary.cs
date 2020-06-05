using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using System;
using System.Collections.Generic;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb.Helpers
{
    class RowFilterBinary : IRowFilter
    {
        private readonly SqlBinaryBooleanExpression _Expression;
        private readonly ExecuteQueryStatement.RawData _RawData;

        public RowFilterBinary( ExecuteQueryStatement.RawData rawData, SqlBinaryBooleanExpression expression )
        {
            _Expression = expression;
            _RawData = rawData;
        }

        public bool IsValid( List<ExecuteQueryStatement.RawData.RawDataRow> rawDataRows )
        {
            var leftIsValid = EvaluateSide( rawDataRows, _Expression.Left );
            var rightIsValid = EvaluateSide( rawDataRows, _Expression.Right );
            return Helper.IsTrue( _Expression.Operator, leftIsValid, rightIsValid );
        }

        public bool IsValid( List<List<ExecuteQueryStatement.RawData.RawDataRow>> rawDataRowList,
            List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            throw new NotImplementedException( );
        }

        private bool EvaluateSide( List<ExecuteQueryStatement.RawData.RawDataRow> rawDataRows, SqlBooleanExpression expression )
        {
            switch ( expression )
            {
                case SqlComparisonBooleanExpression booleanExpression: return EvaluateSide( rawDataRows, booleanExpression );
                case SqlBinaryBooleanExpression binaryExpression     : return EvaluateSide( rawDataRows, binaryExpression );
                default :
                    throw new NotImplementedException();
            }
        }

        private bool EvaluateSide( List<ExecuteQueryStatement.RawData.RawDataRow> rawDataRows, SqlComparisonBooleanExpression expression )
        {
            var type = Helper.DetermineType( expression.Left, expression.Right, _RawData);
            var left = Helper.GetValue(expression.Left, type, _RawData, rawDataRows);
            var right = Helper.GetValue(expression.Right, type, _RawData, rawDataRows);
            return Helper.IsPredicateCorrect(left, right, expression.ComparisonOperator);
        }

        private bool EvaluateSide( List<ExecuteQueryStatement.RawData.RawDataRow> rawDataRows, SqlBinaryBooleanExpression expression )
        {
            var leftIsValid = EvaluateSide( rawDataRows, expression.Left );
            var rightIsValid = EvaluateSide( rawDataRows, expression.Right );
            return Helper.IsTrue( expression.Operator, leftIsValid, rightIsValid );
        }
    }
}
