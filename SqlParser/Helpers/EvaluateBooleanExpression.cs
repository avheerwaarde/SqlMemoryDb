using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.Helpers
{
    class EvaluateBooleanExpression
    {
        private readonly RawData _RawData;
        private readonly MemoryDbCommand _Command;
        private readonly MemoryDatabase _Database;

        public EvaluateBooleanExpression( RawData rawData, MemoryDatabase database, MemoryDbCommand command )
        {
            _RawData = rawData;
            _Database = database;
            _Command = command;
        }

        public bool Evaluate( List<RawTableRow> rawDataRows, SqlBooleanExpression expression, bool invertResult = false )
        {
            switch ( expression )
            {
                case SqlNotBooleanExpression notExpression           : return Evaluate( rawDataRows, notExpression.Expression, true );
                case SqlComparisonBooleanExpression booleanExpression: return EvaluateExpression( rawDataRows, booleanExpression ) ^ invertResult ;
                case SqlBinaryBooleanExpression binaryExpression     : return EvaluateExpression( rawDataRows, binaryExpression ) ^ invertResult;
                case SqlExistsBooleanExpression existsExpression     : return EvaluateExpression( rawDataRows, existsExpression) ^ invertResult;
                case SqlIsNullBooleanExpression isNullExpression     : return EvaluateExpression( rawDataRows, isNullExpression) ^ invertResult;
                case SqlInBooleanExpression inBooleanExpression      : return EvaluateExpression( rawDataRows, inBooleanExpression ) ^ invertResult;
                case SqlLikeBooleanExpression likeBooleanExpression  : return EvaluateExpression( rawDataRows, likeBooleanExpression, invertResult );
                default:
                    throw new NotImplementedException();
            }
        }

        private bool EvaluateExpression( List<RawTableRow> rawDataRows, SqlLikeBooleanExpression expression, bool invertResult )
        {
            var filter = new RowFilterLike( _RawData, expression, invertResult );
            return filter.IsValid( rawDataRows );
        }

        private bool EvaluateExpression( List<RawTableRow> rawDataRows, SqlInBooleanExpression expression )
        {
            var type = Helper.DetermineType( expression.InExpression, expression.InExpression, _RawData );
            var source = Helper.GetValue( expression.InExpression, type, _RawData, rawDataRows );
            foreach ( var child in expression.ComparisonValue.Children )
            {
                if ( child is SqlScalarExpression scalarExpression )
                {
                    var inValue = Helper.GetValue( scalarExpression, type, _RawData, rawDataRows );
                    var compare = HelperConditional.IsPredicateCorrect( source, inValue, SqlComparisonBooleanExpressionType.Equals );
                    if ( compare )
                    {
                        return true;
                    }
                }
                else
                {
                    throw new NotImplementedException( "We expect each compare value to be a scalar expression" );
                }
            }

            return false;
        }

        private bool EvaluateExpression( List<RawTableRow> rawDataRows, SqlComparisonBooleanExpression expression )
        {
            var type = Helper.DetermineType( expression.Left, expression.Right, _RawData);
            var left = Helper.GetValue(expression.Left, type, _RawData, rawDataRows);
            var right = Helper.GetValue(expression.Right, type, _RawData, rawDataRows);
            return HelperConditional.IsPredicateCorrect(left, right, expression.ComparisonOperator);
        }

        private bool EvaluateExpression( List<RawTableRow> rawDataRows, SqlBinaryBooleanExpression expression )
        {
            var leftIsValid = Evaluate( rawDataRows, expression.Left );
            var rightIsValid = Evaluate( rawDataRows, expression.Right );
            return HelperConditional.IsTrue( expression.Operator, leftIsValid, rightIsValid );
        }

        private bool EvaluateExpression( List<RawTableRow> rawDataRows, SqlExistsBooleanExpression expression )
        {
            bool hasRows;
            using ( var reader = new MemoryDbDataReader( CommandBehavior.SingleResult ) )
            {
                _Command.DataReader = reader;
                var tables = ((MemoryDbConnection )_Command.Connection).GetMemoryDatabase( ).Tables;

                var batch = new ExecuteQueryStatement( _Database, _Command ).Execute( tables, _RawData, ( SqlQuerySpecification ) expression.QueryExpression );
                hasRows =  batch.ResultRows.Any();
                _Command.DataReader = null;
            }

            return hasRows;
        }

        private bool EvaluateExpression( List<RawTableRow> rawDataRows, SqlIsNullBooleanExpression expression )
        {
            var type = Helper.DetermineType( expression.Expression, _RawData);
            var val = Helper.GetValue(expression.Expression, type, _RawData, rawDataRows);
            return val == null || val is DBNull;
        }

    }
}
