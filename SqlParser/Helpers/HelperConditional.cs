using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.Helpers
{
    class HelperConditional
    {
        public static IRowFilter GetRowFilter( SqlBooleanExpression booleanExpression, RawData rawData, bool invertResult = false )
        {
            switch ( booleanExpression )
            {
                case SqlNotBooleanExpression notExpression           : return GetRowFilter( notExpression.Expression, rawData, true );
                case SqlComparisonBooleanExpression compareExpression: return new RowFilterComparison( rawData, compareExpression, invertResult );
                case SqlBinaryBooleanExpression binaryExpression     : return new RowFilterBinary( rawData, binaryExpression, invertResult );
                case SqlInBooleanExpression inExpression             : return new RowFilterIn( rawData, inExpression );
                default :
                    throw new NotImplementedException($"unsupported row filter {booleanExpression}");
            }
        }

        public static bool IsTrue( SqlBooleanOperatorType booleanOperator, bool leftIsValid, bool rightIsValid )
        {
            if ( booleanOperator == SqlBooleanOperatorType.Or )
            {
                return leftIsValid || rightIsValid;
            }
            return leftIsValid && rightIsValid;
        }

        public static bool IsPredicateCorrect( object left, object right, SqlComparisonBooleanExpressionType comparisonOperator )
        {
            var comparison = ( ( IComparable ) left ).CompareTo( ( IComparable ) right );

            switch ( comparisonOperator )
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
