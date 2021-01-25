using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionMath: ISelectData
    {
        Type ISelectData.ReturnType => _ReturnType;
        DbType ISelectData.DbType => _DbType;
        public SqlScalarExpression Expression => _FunctionCall;
        
        private readonly Type _ReturnType = typeof(int?);
        private readonly DbType _DbType = DbType.Int32;
        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromFunctionMath( SqlBuiltinScalarFunctionCallExpression functionCall,
            RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
        }

        public object Select( RawTableJoinRow rows )
        {
            var value = Helper.GetValue( _FunctionCall.Arguments.First( ), typeof(double), _RawData, rows );
            if ( value == null )
            {
                return null;
            }
            switch ( _FunctionCall.FunctionName.ToUpper() )
            {
                case "CEILING":
                    return InvokeMathMethod( "Ceiling", value );
                case "FLOOR":
                    return InvokeMathMethod( "Floor", value );
                case "ABS":
                    return InvokeMathMethod( "Abs", value );
                case "SIGN":
                    return InvokeMathMethod( "Sign", value );
                case "ROUND":
                    return InvokeRound( value, rows);
                default:
                    throw new NotImplementedException();
            }
        }

        private object InvokeMathMethod( string methodName, object value )
        {
            var method = HelperReflection.GetMathMethodInfo( methodName, value.GetType( ) );
            return method.Invoke( null, new object[] {value} );
        }


        private object InvokeRound( object value, List<RawTableRow> rows )
        {
            int operation = 0;
            if ( _FunctionCall.Arguments.Count == 3 )
            {
                operation = (int)Helper.GetValue( _FunctionCall.Arguments[ 2 ], typeof( int ), _RawData, rows );
            }
            int digits = (int)Helper.GetValue( _FunctionCall.Arguments[ 1 ], typeof( int ), _RawData, rows );
            if ( operation != 0 )
            {
                return InvokeTruncate( value, digits, rows );
            }

            if ( digits < 0 )
            {
                var step = Math.Pow( 10, 0 - digits );
                var calculated = Math.Round(( double ) value / step, 0, MidpointRounding.AwayFromZero );
                return ( long ) ( calculated * step );
            }
            var method = HelperReflection.GetMathMethodInfo( "Round", value.GetType( ), 3 );
            return method.Invoke( null, new object[] { value, digits, MidpointRounding.AwayFromZero } );
        }


        private object InvokeTruncate( object value, int digits, List<RawTableRow> rows )
        {
            switch ( Type.GetTypeCode(value.GetType(  )) )
            {
                case TypeCode.Decimal:
                    return TruncateDecimal( (decimal)value, digits );
                case TypeCode.Double:
                    return TruncateDouble( (double)value, digits );
                case TypeCode.Single:
                    return TruncateFloat( (float)value, digits );
                default:
                    throw new NotImplementedException();
            }
        }

        public decimal TruncateDecimal( decimal value, int precision )
        {
            decimal step = (decimal)Math.Pow(10, precision);
            decimal tmp = Math.Truncate(step * value);
            return tmp / step;
        }
        
        public double TruncateDouble( double value, int precision )
        {
            double step = (double)Math.Pow(10, precision);
            double tmp = Math.Truncate(step * value);
            return tmp / step;
        }
        
        public float TruncateFloat( float value, int precision )
        {
            float step = (float)Math.Pow(10, precision);
            float tmp = (float)Math.Truncate(step * value);
            return tmp / step;
        }
    }
}
