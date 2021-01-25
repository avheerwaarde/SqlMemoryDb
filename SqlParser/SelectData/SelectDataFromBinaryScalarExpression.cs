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
            else if ( typeLeft == typeof(byte) && typeRight == typeof( byte ) )
            {
                ReturnType = typeof( byte );
                _DbType = DbType.Byte;
            }
            else if ( typeLeft == typeof( Int16 ) && typeRight == typeof( Int16 ) )
            {
                ReturnType = typeof( short );
                _DbType = DbType.Int16;
            }
            else if ( typeLeft == typeof( Int32 ) && typeRight == typeof( Int32 ) )
            {
                ReturnType = typeof( int );
                _DbType = DbType.Int32;
            }
            else if ( HelperReflection.IsInteger(typeLeft) && HelperReflection.IsInteger(typeRight) )
            {
                ReturnType = typeof( long );
                _DbType = DbType.Int64;
            }
        }

        public object Select( RawTableJoinRow rows )
        {
            if ( _DbType == DbType.String )
            {
                return SelectString( rows );
            }
            if ( _DbType == DbType.Byte )
            {
                return SelectByte( rows );
            }
            if ( _DbType == DbType.Int16 )
            {
                return SelectInteger16( rows );
            }
            if ( _DbType == DbType.Int32 )
            {
                return SelectInteger32( rows );
            }
            if ( _DbType == DbType.Int64 )
            {
                return SelectInteger( rows );
            }

            return SelectDecimal( rows );
        }


        private string SelectString( List<RawTableRow> rows )
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

        private byte SelectByte( List<RawTableRow> rows )
        {
            var left = Convert.ToByte(Helper.GetValue( _Expression.Left, typeof( byte ), _RawData, rows ));
            var right = Convert.ToByte(Helper.GetValue( _Expression.Right, typeof( byte ), _RawData, rows ));
            switch ( _Expression.Operator )
            {
                case SqlBinaryScalarOperatorType.Add: return (byte)(left + right);
                case SqlBinaryScalarOperatorType.Subtract: return (byte)( left - right );
                case SqlBinaryScalarOperatorType.Multiply: return (byte)( left - right );
                case SqlBinaryScalarOperatorType.Divide: return (byte)( left / right );
                default:
                    throw new NotImplementedException( $"Operator {_Expression.Operator.GetType(  )} is not implemented.");
            }
        }

        private short SelectInteger16( List<RawTableRow> rows )
        {
            var left = Convert.ToInt16( Helper.GetValue( _Expression.Left, typeof( short ), _RawData, rows ) );
            var right = Convert.ToInt16( Helper.GetValue( _Expression.Right, typeof( short ), _RawData, rows ) );
            switch ( _Expression.Operator )
            {
                case SqlBinaryScalarOperatorType.Add: return (short) (left + right);
                case SqlBinaryScalarOperatorType.Subtract: return (short) (left - right);
                case SqlBinaryScalarOperatorType.Multiply: return (short) (left * right);
                case SqlBinaryScalarOperatorType.Divide: return (short) (left / right);
                default:
                    throw new NotImplementedException( $"Operator {_Expression.Operator.GetType()} is not implemented." );
            }
        }

        private int SelectInteger32( List<RawTableRow> rows )
        {
            var left = Convert.ToInt32( Helper.GetValue( _Expression.Left, typeof( int ), _RawData, rows ) );
            var right = Convert.ToInt32( Helper.GetValue( _Expression.Right, typeof( int ), _RawData, rows ) );
            switch ( _Expression.Operator )
            {
                case SqlBinaryScalarOperatorType.Add: return left + right;
                case SqlBinaryScalarOperatorType.Subtract: return left - right;
                case SqlBinaryScalarOperatorType.Multiply: return left * right;
                case SqlBinaryScalarOperatorType.Divide: return left / right;
                default:
                    throw new NotImplementedException( $"Operator {_Expression.Operator.GetType()} is not implemented." );
            }
        }


        private long SelectInteger( List<RawTableRow> rows )
        {
            var left = Convert.ToInt64( Helper.GetValue( _Expression.Left, typeof( long ), _RawData, rows ) );
            var right = Convert.ToInt64( Helper.GetValue( _Expression.Right, typeof( long ), _RawData, rows ) );
            switch ( _Expression.Operator )
            {
                case SqlBinaryScalarOperatorType.Add: return left + right;
                case SqlBinaryScalarOperatorType.Subtract: return left - right;
                case SqlBinaryScalarOperatorType.Multiply: return left * right;
                case SqlBinaryScalarOperatorType.Divide: return left / right;
                default:
                    throw new NotImplementedException( $"Operator {_Expression.Operator.GetType()} is not implemented." );
            }
        }


        private decimal SelectDecimal( List<RawTableRow> rows )
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
