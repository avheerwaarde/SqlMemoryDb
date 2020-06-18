﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromCaseExpression : ISelectDataFunction
    {
        private readonly SqlSearchedCaseExpression _Expression;
        private readonly RawData _RawData;
        private readonly SqlScalarExpression _ScalarExpression;
        private readonly FullTypeInfo _FullTypeInfo;

        public SelectDataFromCaseExpression( SqlSearchedCaseExpression caseExpression, RawData rawData )
        {
            _Expression = caseExpression;
            _RawData = rawData;
            if ( _Expression.ElseExpression != null )
            {
                _FullTypeInfo = Helper.DetermineFullTypeInfo( _Expression.ElseExpression, rawData );
                _ScalarExpression = _Expression.ElseExpression;
            }

            if ( _Expression.ElseExpression == null || _FullTypeInfo.DbDataType == "Null")
            {
                var whenExpression = _Expression.WhenClauses.First( );
                _FullTypeInfo = Helper.DetermineFullTypeInfo( whenExpression.ThenExpression, rawData );
                _ScalarExpression = whenExpression.ThenExpression;
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            return Helper.GetValue( _ScalarExpression, ReturnType, _RawData, rows );
        }

        public bool IsAggregate => false;
        public Type ReturnType => _FullTypeInfo.NetDataType;
        public string DbType => _FullTypeInfo.DbDataType;

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
