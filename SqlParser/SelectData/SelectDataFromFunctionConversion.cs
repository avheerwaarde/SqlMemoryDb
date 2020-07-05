using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionConversion : ISelectDataFunction
    {
        public bool IsAggregate => false;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        DbType ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(bool);
        private readonly DbType _DbType = DbType.Boolean;

        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        private readonly Dictionary<int, string> _DateStyles = new Dictionary<int, string>
        {
            [0] = "MMM dd yyyy hh:mmtt",
            [1] = "MM'/'dd'/'yyyy",
            [2] = "yy.MM.dd",
            [3] = "dd'/'MM'/'yy",
            [4] = "dd.MM.yy",
            [5] = "dd-MM-yy",
            [6] = "dd MMM yy",
            [7] = "MMM dd yy",
            [8] = "HH:mm:ss",
            [9] = "MMM dd yyyy hh:mm:ss:ffftt",
            [10] = "MM-dd-yy",
            [11] = "yy'/'MM'/'dd",
            [12] = "yyMMdd",
            [13] = "dd MMM yyyy HH:mm:ss:fff",
            [14] = "HH:mm:ss.fff",
            [20] = "yyyy-MM-dd HH:mm:ss",
            [21] = "yyyy-MM-dd HH:mm:ss:fff",
            [100] = "MMM dd yyyy hh:mmtt",
            [101] = "MM/dd/yyyy",
            [102] = "yy.MM.dd",
            [103] = "dd'/'MM'/'yy",
            [104] = "dd.MM.yy",
            [105] = "dd-MM-yy",
            [106] = "dd MMM yy",
            [107] = "MMM dd yy",
            [108] = "HH:mm:ss",
            [109] = "MMM dd yyyy hh:mm:ss:ffftt",
            [110] = "MM-dd-yy",
            [111] = "yy'/'MM'/'dd",
            [112] = "yyMMdd",
            [113] = "dd MMM yyyy HH:mm:ss:fff",
            [114] = "HH:mm:ss.fff",
            [120] = "yyyy-MM-dd HH:mm:ss",
            [121] = "yyyy-MM-dd HH:mm:ss:fff",
            [126] = "yyyy-MM-ddTHH:mm:ss:fff",
            [127] = "yyyy-MM-ddTHH:mm:ss:fffK",
            [130] = "dd MMM yyyy hh:mm:ss:ffftt",
            [131] = "dd'/'MM'/'yyyy hh:mm:ss:ffftt"
        };

        private readonly Dictionary<int, string> _NumericStyles = new Dictionary<int, string>
        {
            [ 0 ] = "0.00",
            [ 1 ] = "0,0.00",
            [ 2 ] = "0.0000"
        };


        public SelectDataFromFunctionConversion( SqlBuiltinScalarFunctionCallExpression functionCall, RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( info.ReturnDbType.HasValue )
            {
                _ReturnType = info.ReturnType;
                _DbType = info.ReturnDbType.Value;
            }
            else if ( functionCall is SqlCastExpression castFunction )
            {
                var dataTypeInfo = new DataTypeInfo( castFunction.DataType.Sql );
                _ReturnType = dataTypeInfo.NetDataType;
                _DbType = dataTypeInfo.DbDataType;
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            switch ( _FunctionCall.FunctionName.ToUpper( ) )
            {
                case "CAST":
                case "TRY_CAST":
                    try
                    {
                        return FunctionCast( rows );
                    }
                    catch (Exception)
                    {
                        if ( _FunctionCall.FunctionName.ToUpper( ) == "TRY_CAST" )
                        {
                            return null;
                        }
                        throw new SqlInvalidCastException( _DbType.ToString() );
                    }

                case "CONVERT":
                case "TRY_CONVERT":
                    try
                    {
                        return FunctionConvert( rows );
                    }
                    catch (Exception)
                    {
                        if ( _FunctionCall.FunctionName.ToUpper( ) == "TRY_CONVERT" )
                        {
                            return null;
                        }
                        throw new SqlInvalidCastException( _DbType.ToString() );
                    }

                case "ISDATE":
                    return FunctionIsDate( rows );
                case "ISNULL":
                    return FunctionIsNull( rows );
                default:
                    throw new NotImplementedException();
            }
        }

        private object FunctionCast( List<RawData.RawDataRow> rows )
        {
            var value = Helper.GetValue( _FunctionCall.Arguments[0], _ReturnType, _RawData, rows, true );
            value = TruncateDoubleIfReturnTypeHasNoDecimals( value );

            return Convert.ChangeType( value, _ReturnType, CultureInfo.InvariantCulture );
        }

        private object TruncateDoubleIfReturnTypeHasNoDecimals( object value )
        {
            var hasTruncateSource = HelperReflection.HasMathMethodInfo( "Truncate", value.GetType( ) );
            var hasTruncateDestination = HelperReflection.HasMathMethodInfo( "Truncate", _ReturnType );
            if ( hasTruncateSource && hasTruncateDestination == false && _ReturnType != typeof( string ) )
            {
                var funcTruncate = HelperReflection.GetMathMethodInfo( "Truncate", value.GetType( ) );
                value = funcTruncate.Invoke( null, new object[] {value} );
            }

            return value;
        }

        private object FunctionConvert( List<RawData.RawDataRow> rows )
        {
            var value = Helper.GetValue( _FunctionCall.Arguments[0], _ReturnType, _RawData, rows, true );
            value = TruncateDoubleIfReturnTypeHasNoDecimals( value );

            if ( _ReturnType == typeof(string) && _FunctionCall.Arguments.Count == 2 )
            {
                return FormattedAsString( value, rows );
                
            }
            return Convert.ChangeType( value, _ReturnType, CultureInfo.InvariantCulture );
        }

        private string FormattedAsString( object value, List<RawData.RawDataRow> rows )
        {
            if ( value is string && _ReturnType == typeof(string) )
            {
                if ( DateTime.TryParse( value.ToString(  ), out var dateValue ) )
                {
                    value = dateValue;
                }    
                else if ( Decimal.TryParse( value.ToString(  ), out var decimalValue ) )
                {
                    value = decimalValue;
                }
            }

            var style = (int)Helper.GetValue( _FunctionCall.Arguments[1], typeof(int), _RawData, rows );
            switch ( Type.GetTypeCode( value.GetType(  ) ) )
            {
                case TypeCode.DateTime: return ( ( DateTime ) value ).ToString( _DateStyles[ style ] );
                case TypeCode.Decimal:  return ( ( decimal ) value ).ToString( _NumericStyles[ style ] );
                case TypeCode.Single:   return ( ( Single ) value ).ToString( _NumericStyles[ style ] );
                case TypeCode.Double:   return ( ( double ) value ).ToString( _NumericStyles[ style ] );
                default:
                    return (string)Convert.ChangeType( value, _ReturnType, CultureInfo.InvariantCulture );
            }
        }

        private int FunctionIsDate( List<RawData.RawDataRow> rows )
        {
            var value = Helper.GetValue( _FunctionCall.Arguments[0], _ReturnType, _RawData, rows, true );
            if ( value is DateTime )
            {
                return 1;
            }

            if ( value is string )
            {
                try
                {
                    Helper.GetValueFromString( typeof( DateTime ), (string)value );
                    return 1;
                }
                catch ( Exception  )
                {
                    // If string is no date, we will return a 0, not an exception.
                }
            }

            return 0;
        }

        private object FunctionIsNull( List<RawData.RawDataRow> rows )
        {
            foreach ( var argument in _FunctionCall.Arguments )
            {
                var value = Helper.GetValue( argument, _ReturnType, _RawData, rows, true );
                if ( value != null )
                {
                    return value;
                }
            }
            return null;
        }


        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
