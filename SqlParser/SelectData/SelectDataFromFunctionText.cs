﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class SelectDataFromFunctionText : ISelectDataFunction
    {
        public bool IsAggregate => false;
        Type ISelectDataFunction.ReturnType => _ReturnType;
        string ISelectDataFunction.DbType => _DbType;
        
        private readonly Type _ReturnType = typeof(string);
        private readonly string _DbType = "string";
        private readonly SqlBuiltinScalarFunctionCallExpression _FunctionCall;
        private readonly RawData _RawData;

        public SelectDataFromFunctionText( SqlBuiltinScalarFunctionCallExpression functionCall,
            RawData rawData, SelectDataFunctionInfo info )
        {
            _FunctionCall = functionCall;
            _RawData = rawData;
            if ( string.IsNullOrWhiteSpace( info.ReturnDbType ) == false )
            {
                _ReturnType = info.ReturnType;
                _DbType = info.ReturnDbType;
            }
        }

        public object Select( List<RawData.RawDataRow> rows )
        {
            var arguments = GetArgumentList( );

            switch ( _FunctionCall.FunctionName.ToUpper() )
            {
                case "ASCII": return string.IsNullOrWhiteSpace( arguments[0] ) ? null : (byte?)arguments[ 0 ][ 0 ];
                case "CHAR": return string.IsNullOrWhiteSpace( arguments[0] ) ? null : (char?)int.Parse( arguments[0] );
                case "CHARINDEX": return FunctionCharIndex( arguments );
                case "CONCAT": return string.Join( "", arguments );
                case "DATALENGTH": return arguments[0]?.Length;
                case "LEFT": return arguments[ 0 ].Substring( 0, Math.Min(arguments[0].Length, int.Parse( arguments[ 1 ] )) );
                case "LEN": return arguments[0]?.TrimEnd().Length;
                case "LOWER": return arguments[0]?.ToLower();
                case "LTRIM": return arguments[0]?.TrimStart(  );
                case "NCHAR": return string.IsNullOrWhiteSpace( arguments[0] ) ? null : (char?)int.Parse( arguments[0] );
                case "PATINDEX": return FunctionPatIndex( arguments );
                case "REPLACE": return new Regex( arguments[1], RegexOptions.IgnoreCase ).Replace( arguments[0], arguments[2] );
                case "RIGHT": return arguments[ 0 ].Substring( Math.Max(0, arguments[0].Length -int.Parse( arguments[ 1 ] )) );
                default:
                    throw new NotImplementedException();
            }
        }

        private List<string> GetArgumentList( )
        {
            var list = new List<string>( );
            if ( _FunctionCall.Arguments != null )
            {
                foreach ( var argument in _FunctionCall.Arguments )
                {
                    var value = Helper.GetValue( argument, typeof(string), _RawData, new List<RawData.RawDataRow>() );
                    list.Add( value?.ToString( ) );
                }
            }
            return list;
        }

        private int FunctionCharIndex( List<string> arguments )
        {
            int offset = 0;
            if ( arguments.Count == 3 )
            {
                offset = int.Parse( arguments[ 2 ] );
            }

            return arguments[ 1 ].IndexOf( arguments[ 0 ], offset, StringComparison.InvariantCultureIgnoreCase ) + 1;
        }

        private int FunctionPatIndex( List<string> arguments )
        {
            var expression = Helper.GetLikeRegEx( arguments[0] );
            if ( expression.StartsWith( ".*" ) )
            {
                expression = expression.Substring( 2 );
            }
            if ( expression.EndsWith( ".*" ) )
            {
                expression = expression.Substring( 0, expression.Length-2 );
            }

            var match = Regex.Match( arguments[1], expression, RegexOptions.IgnoreCase );
            if (match.Success)
            {
                return match.Index + 1;
            }

            return 0;
        }

        public object Select( List<List<RawData.RawDataRow>> rows )
        {
            throw new NotImplementedException( );
        }
    }
}
