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

        public bool Evaluate( List<RawData.RawDataRow> rawDataRows, SqlBooleanExpression expression, bool invertResult = false )
        {
            switch ( expression )
            {
                case SqlNotBooleanExpression notExpression           : return Evaluate( rawDataRows, notExpression.Expression, true );
                case SqlComparisonBooleanExpression booleanExpression: return EvaluateExpression( rawDataRows, booleanExpression ) ^ invertResult ;
                case SqlBinaryBooleanExpression binaryExpression     : return EvaluateExpression( rawDataRows, binaryExpression ) ^ invertResult;
                case SqlExistsBooleanExpression existsExpression     : return EvaluateExpression( rawDataRows, existsExpression) ^ invertResult;
                case SqlIsNullBooleanExpression isNullExpression     : return EvaluateExpression( rawDataRows, isNullExpression) ^ invertResult;
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
            bool hasRows;
            using ( var reader = new MemoryDbDataReader( CommandBehavior.SingleResult ) )
            {
                _Command.DataReader = reader;
                var tables = ((MemoryDbConnection )_Command.Connection).GetMemoryDatabase( ).Tables;

                var batch = new ExecuteQueryStatement( _Database, _Command, _Command.DataReader ).Execute( tables, _RawData, ( SqlQuerySpecification ) expression.QueryExpression );
                hasRows =  batch.ResultRows.Any();
                _Command.DataReader = null;
            }

            return hasRows;
        }

        private bool EvaluateExpression( List<RawData.RawDataRow> rawDataRows, SqlIsNullBooleanExpression expression )
        {
            var type = Helper.DetermineType( expression.Expression, _RawData);
            var val = Helper.GetValue(expression.Expression, type, _RawData, rawDataRows);
            return val == null;
        }

    }
}
