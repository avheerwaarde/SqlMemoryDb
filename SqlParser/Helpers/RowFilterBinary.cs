﻿using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
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

        public bool IsValid( List<RawData.RawDataRow> rawDataRows )
        {
            var leftIsValid = EvaluateSide( rawDataRows, _Expression.Left );
            var rightIsValid = EvaluateSide( rawDataRows, _Expression.Right );
            return HelperConditional.IsTrue( _Expression.Operator, leftIsValid, rightIsValid ) ^ _InvertResult;
        }

        public bool IsValid( List<List<RawData.RawDataRow>> rawDataRowList,
            List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            throw new NotImplementedException( );
        }

        private bool EvaluateSide( List<RawData.RawDataRow> rawDataRows, SqlBooleanExpression expression )
        {
            var evaluator = new EvaluateBooleanExpression( _RawData, null );
            return evaluator.Evaluate( rawDataRows, expression );
        }
    }
}
