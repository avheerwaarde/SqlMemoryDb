using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromBinaryScalarExpression : ISelectData
    {
        DbType ISelectData.DbType => _DbType;
        public Type ReturnType { get; } = typeof(decimal);
        public SqlScalarExpression Expression => _Expression;

        private readonly DbType _DbType = DbType.Decimal;
        private readonly SqlBinaryScalarExpression _Expression;
        private readonly RawData _RawData;

        public SelectDataFromBinaryScalarExpression( SqlBinaryScalarExpression expression, RawData rawData )
        {
            _Expression = expression;
            _RawData = rawData;
            var typeLeft = Helper.DetermineType( expression.Left, _RawData );
            var typeRight = Helper.DetermineType( expression.Right, _RawData );
            if ( typeLeft == typeof(string) || typeRight == typeof(string) )
            {
                ReturnType = typeof( string );
                _DbType = DbType.String;
            }
            else if ( HelperReflection.IsInteger(typeLeft) && HelperReflection.IsInteger(typeRight) )
            {
                ReturnType = typeof( long );
                _DbType = DbType.Int64;
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            if ( _DbType == DbType.String )
            {
                return SelectString( rows );
            }
            if ( _DbType == DbType.Int64 )
            {
                return SelectInteger( rows );
            }

            return SelectDecimal( rows );
        }


        private string SelectString( List<RawData.RawDataRow> rows )
        {
            var left = Helper.GetValue( _Expression.Left, typeof( string ), _RawData, rows )?.ToString(  );
            var right = Helper.GetValue( _Expression.Right, typeof( string ), _RawData, rows )?.ToString(  );
            if ( _Expression.Operator != SqlBinaryScalarOperatorType.Add )
            {
                throw new NotImplementedException( $"Operator {_Expression.Operator.GetType(  )} is not implemented for strings.");
            }

            if ( left == null || right == null )
            {
                return null;
            }
            return left + right;
        }

        private long SelectInteger( List<RawData.RawDataRow> rows )
        {
            var left = Convert.ToInt64(Helper.GetValue( _Expression.Left, typeof( long ), _RawData, rows ));
            var right = Convert.ToInt64(Helper.GetValue( _Expression.Right, typeof( long ), _RawData, rows ));
            switch ( _Expression.Operator )
            {
                case SqlBinaryScalarOperatorType.Add: return left + right;
                case SqlBinaryScalarOperatorType.Subtract: return left - right;
                case SqlBinaryScalarOperatorType.Multiply: return left * right;
                case SqlBinaryScalarOperatorType.Divide: return left / right;
                default:
                    throw new NotImplementedException( $"Operator {_Expression.Operator.GetType(  )} is not implemented.");
            }
        }


        private decimal SelectDecimal( List<RawData.RawDataRow> rows )
        {
            var left = Convert.ToDecimal(Helper.GetValue( _Expression.Left, typeof( decimal ), _RawData, rows ));
            var right = Convert.ToDecimal(Helper.GetValue( _Expression.Right, typeof( decimal ), _RawData, rows ));
            switch ( _Expression.Operator )
            {
                case SqlBinaryScalarOperatorType.Add: return left + right;
                case SqlBinaryScalarOperatorType.Subtract: return left - right;
                case SqlBinaryScalarOperatorType.Multiply: return left * right;
                case SqlBinaryScalarOperatorType.Divide: return left / right;
                default:
                    throw new NotImplementedException( $"Operator {_Expression.Operator.GetType(  )} is not implemented.");
            }
        }
    }
}
