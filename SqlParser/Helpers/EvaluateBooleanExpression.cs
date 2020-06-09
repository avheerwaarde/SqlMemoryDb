using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.Helpers
{
    class EvaluateBooleanExpression
    {
        private readonly RawData _RawData;
        private readonly MemoryDbCommand _Command;

        public EvaluateBooleanExpression( RawData rawData, MemoryDbCommand command )
        {
            _RawData = rawData;
            _Command = command;
        }

        public bool Evaluate( List<RawData.RawDataRow> rawDataRows, SqlBooleanExpression expression, bool invertResult = false )
        {
            switch ( expression )
            {
                case SqlNotBooleanExpression notExpression           : return Evaluate( rawDataRows, notExpression.Expression, true );
                case SqlComparisonBooleanExpression booleanExpression: return EvaluateExpression( rawDataRows, booleanExpression ) ^ invertResult ;
                case SqlBinaryBooleanExpression binaryExpression     : return EvaluateExpression( rawDataRows, binaryExpression ) ^ invertResult;
                case SqlExistsBooleanExpression existsExpression     : return EvaluateExpression( rawDataRows, existsExpression) ^ invertResult;
                default :
                    throw new NotImplementedException();
            }
        }

        private bool EvaluateExpression( List<RawData.RawDataRow> rawDataRows, SqlComparisonBooleanExpression expression )
        {
            var type = Helper.DetermineType( expression.Left, expression.Right, _RawData);
            var left = Helper.GetValue(expression.Left, type, _RawData, rawDataRows);
            var right = Helper.GetValue(expression.Right, type, _RawData, rawDataRows);
            return HelperConditional.IsPredicateCorrect(left, right, expression.ComparisonOperator);
        }

        private bool EvaluateExpression( List<RawData.RawDataRow> rawDataRows, SqlBinaryBooleanExpression expression )
        {
            var leftIsValid = Evaluate( rawDataRows, expression.Left );
            var rightIsValid = Evaluate( rawDataRows, expression.Right );
            return HelperConditional.IsTrue( expression.Operator, leftIsValid, rightIsValid );
        }

        private bool EvaluateExpression( List<RawData.RawDataRow> rawDataRows, SqlExistsBooleanExpression expression )
        {
            using ( var reader = new MemoryDbDataReader( CommandBehavior.SingleResult ) )
            {
                var tables = MemoryDbConnection.GetMemoryDatabase( ).Tables;

                new ExecuteQueryStatement( _Command, reader ).Execute( tables, _RawData, ( SqlQuerySpecification ) expression.QueryExpression );
                return reader.HasRows;
            }
        }

    }
}
