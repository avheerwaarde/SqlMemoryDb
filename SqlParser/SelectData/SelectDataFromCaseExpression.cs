﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromCaseExpression : ISelectData
    {
        public Type ReturnType => _FullTypeInfo.NetDataType;
        public DbType DbType => _FullTypeInfo.DbDataType ?? DbType.Int32 ;
        public SqlScalarExpression Expression => _Expression;

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

            if ( _Expression.ElseExpression == null || _FullTypeInfo.DbDataType == null)
            {
                var whenExpression = _Expression.WhenClauses.First( );
                _FullTypeInfo = Helper.DetermineFullTypeInfo( whenExpression.ThenExpression, rawData );
                _ScalarExpression = whenExpression.ThenExpression;
            }
        }

        public object Select( RawTableJoinRow rows )
        {
            var evaluator = new EvaluateBooleanExpression( _RawData, ((MemoryDbConnection )_RawData.Command.Connection).GetMemoryDatabase( ), _RawData.Command  );
            foreach ( var whenClause in _Expression.WhenClauses )
            {
                if ( evaluator.Evaluate( rows, whenClause.WhenExpression ) )
                {
                    return Helper.GetValue( whenClause.ThenExpression, ReturnType, _RawData, rows );
                }             
            }

            return Helper.GetValue( _Expression.ElseExpression, ReturnType, _RawData, rows );
        }

    }
}
