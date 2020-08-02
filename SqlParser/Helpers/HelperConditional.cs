using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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
                case SqlLikeBooleanExpression likeExpression         : return new RowFilterLike( rawData, likeExpression, invertResult );
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
            if ( left.GetType(  ) == typeof(byte[]) && right.GetType(  ) == typeof(byte[])  )
            {
                return IsPredicateArrayCorrect( (byte[])left, (byte[])right, comparisonOperator );
            }

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

        private static bool IsPredicateArrayCorrect( byte[] left, byte[] right, SqlComparisonBooleanExpressionType comparisonOperator )
        {
            if ( comparisonOperator != SqlComparisonBooleanExpressionType.Equals && comparisonOperator != SqlComparisonBooleanExpressionType.NotEqual )
            {
                throw new NotImplementedException( );
            }

            var equals = left.SequenceEqual( right );
            return comparisonOperator == SqlComparisonBooleanExpressionType.Equals && equals;
        }
    }
}
