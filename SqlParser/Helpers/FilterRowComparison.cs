using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb.Helpers
{
    class FilterRowComparison
    {
        private readonly SqlComparisonBooleanExpression _Expression;
        private readonly object _CachedLeftValue;
        private readonly object _CachedRightValue;
        private readonly ExecuteSelectStatement.RawData _RawData;
        private readonly Type _Type;

        public FilterRowComparison( ExecuteSelectStatement.RawData rawData, SqlComparisonBooleanExpression expression )
        {
            _Expression = expression;
            _RawData = rawData;
            _Type = DetermineType( _Expression.Left, _Expression.Right );
            _CachedLeftValue = Helper.GetValue( _Expression.Left, _Type, rawData, null );
            _CachedRightValue = Helper.GetValue( _Expression.Right, _Type, rawData, null );

        }

        private Type DetermineType( SqlScalarExpression expressionLeft, SqlScalarExpression expressionRight )
        {
            return DetermineType( expressionLeft ) ?? DetermineType( expressionRight );
        }

        private Type DetermineType( SqlScalarExpression expression )
        {
            switch ( expression )
            {
                case SqlColumnRefExpression columnRef :
                    var field = Helper.GetTableColumn( columnRef, _RawData );
                    return field.Column.NetDataType;
                case SqlScalarVariableRefExpression variableRef:
                    Helper.GetValueFromParameter( variableRef.VariableName, _RawData.Parameters );
                    return null;
            }

            return null;
        }

        public bool IsValid( List<ExecuteSelectStatement.RawData.RawDataRow> rawDataRows )
        {
            var left = _CachedLeftValue ?? Helper.GetValue( _Expression.Left, _Type, _RawData, rawDataRows );
            var right = _CachedRightValue ?? Helper.GetValue( _Expression.Right, _Type, _RawData, rawDataRows );
            var comparison = ( ( IComparable ) left ).CompareTo( ( IComparable ) right );

            switch ( _Expression.ComparisonOperator )
            {
                case SqlComparisonBooleanExpressionType.Equals: return comparison == 0;
                case SqlComparisonBooleanExpressionType.LessThan: return comparison < 0;
                case SqlComparisonBooleanExpressionType.ValueEqual: return comparison == 0;
                case SqlComparisonBooleanExpressionType.NotEqual: return comparison != 0;
                case SqlComparisonBooleanExpressionType.GreaterThan: return comparison > 0;
                case SqlComparisonBooleanExpressionType.GreaterThanOrEqual: return comparison >= 0;
                case SqlComparisonBooleanExpressionType.LessOrGreaterThan: return comparison != 0;
                case SqlComparisonBooleanExpressionType.LessThanOrEqual: return comparison <= 0;
                case SqlComparisonBooleanExpressionType.NotLessThan: return comparison >= 0;
                case SqlComparisonBooleanExpressionType.NotGreaterThan: return comparison <= 0;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
