using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb.Helpers
{
    class RowFilterLike : IRowFilter
    {
        private readonly RawData _RawData;
        private readonly bool _InvertResult;
        private readonly string _LikeRegEx;
        private readonly SqlScalarExpression _Expression;

        public RowFilterLike( RawData rawData, SqlLikeBooleanExpression likeExpression, bool invertResult)
        {
            _RawData = rawData;
            _InvertResult = invertResult;
            _Expression = likeExpression.Expression;
            _LikeRegEx = GetLikeRegEx( likeExpression );
        }

        private string GetLikeRegEx( SqlLikeBooleanExpression likeExpression )
        {
            var patternBuilder = new StringBuilder();
            var pattern = Helper.GetValue( likeExpression.LikePattern, typeof(string), _RawData, new List<RawData.RawDataRow>()).ToString(  );
            if ( pattern.StartsWith( "%" ) == false )
            {
                patternBuilder.Append( "^" );
            }

            foreach ( var character in pattern )
            {
                switch ( character )
                {
                    case '%': patternBuilder.Append( ".*" ); break;
                    case '_': patternBuilder.Append( ".{1}" ); break;
                    default:
                        patternBuilder.Append( character );
                        break;
                }
            }
            if ( pattern.EndsWith( "%" ) == false )
            {
                patternBuilder.Append( "$" );
            }
            return patternBuilder.ToString();
        }

        public bool IsValid( List<RawData.RawDataRow> rawDataRows )
        {
            var field = Helper.GetValue( _Expression, typeof( string ), _RawData, rawDataRows ).ToString(  );
            var isMatch = Regex.IsMatch( field, _LikeRegEx, RegexOptions.IgnoreCase );
            return isMatch ^ _InvertResult;
        }

        public bool IsValid( List<List<RawData.RawDataRow>> rawDataRowList, List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            throw new NotImplementedException( );
        }
    }
}
