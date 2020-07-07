using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.Parser;

namespace SqlMemoryDb.Helpers
{
    class TokenFinder
    {
        private readonly IList<Token> _Tokens;
        public int CurrentIndex;

        public TokenFinder( IList<Token> tokens )
        {
            _Tokens = tokens;
            CurrentIndex = 0;
        }

        public string GetTokenAfterToken( string tokenType, string afterTokenType, int startIndex = 0 )
        {
            var index = startIndex;
            while ( _Tokens[ index ].Type != afterTokenType && ++index < _Tokens.Count  ){ }
            return GetToken( tokenType, index );
        }

        private string GetToken( string tokenType, int index )
        {
            while ( ++index < _Tokens.Count )
            {
                if ( IsToken( index, tokenType ) )
                {
                    return _Tokens[ index ].Text.Trim( new[] {'[', ']'} );
                }
            } 

            CurrentIndex = index;
            return null;
        }

        public List<string> GetIdListAfterToken( string afterTokenType )
        {
            var idList = new List<string>( );
            var index = 0;
            while ( _Tokens[ index ].Type != afterTokenType && ++index < _Tokens.Count  ){ }
            string id = "";

            while ( ++index < _Tokens.Count && (IsSkipToken( index, false ) || IsIdToken( index ) || IsSeparatorToken( index )) )
            {
                if ( IsIdToken( index ) )
                {
                    id += GetIdPart( _Tokens[ index ] );
                }

                if ( IsSeparatorToken( index ) && string.IsNullOrWhiteSpace( id ) == false )
                {
                    idList.Add( id );
                    id = "";
                }
            } 
            if ( string.IsNullOrWhiteSpace( id ) == false )
            {
                idList.Add( id );
            }

            return idList;
        }

        public string GetIdAfterToken( string afterTokenType, int startIndex = 0, bool skipParenthesis = true, bool isSingleTokenId = false )
        {
            var index = startIndex;
            while ( _Tokens[ index ].Type != afterTokenType && ++index < _Tokens.Count  ){ }
            return GetId( index, skipParenthesis, isSingleTokenId );
        }

        private string GetId( int index, bool skipParenthesis, bool isSingleTokenId )
        {
            string id = "";

            while ( ++index < _Tokens.Count && (IsSkipToken( index, skipParenthesis ) || IsIdToken( index )) )
            {
                if ( IsIdToken( index ) )
                {
                    id += GetIdPart( _Tokens[ index ] );
                    if ( isSingleTokenId )
                    {
                        return id;
                    }
                }
            } 

            CurrentIndex = index;
            return id;
        }

        private bool IsSkipToken( int index, bool skipParenthesis )
        {
            return _Tokens[ index ].Type == "LEX_WHITE"
                   || ( skipParenthesis
                        && ( _Tokens[ index ].Type == "(" || _Tokens[ index ].Type == ")" ) );
        }

        private bool IsToken( int index, string tokenType )
        {
            return _Tokens[ index ].Type == tokenType;
        }

        private bool IsIdToken( int index )
        {
            return _Tokens[ index ].Type == "." || _Tokens[ index ].Type == "TOKEN_ID";
        }

        private bool IsSeparatorToken( int index )
        {
            return _Tokens[ index ].Type == ",";
        }

        private string GetIdPart( Token token )
        {
            if ( token.Type == "." )
            {
                return token.Text;
            }

            if ( token.Type == "TOKEN_ID" )
            {
                return token.Text.Trim( new[] {'[', ']'} );
            }

            return "";
        }

        public string GetTokensBetween( string afterToken, string untilToken, int startIndex = 0 )
        {
            var builder = new StringBuilder();
            var index = startIndex;
            while ( _Tokens[ index ].Type != afterToken && ++index < _Tokens.Count  ){ }
            while ( ++index < _Tokens.Count && _Tokens[ index ].Type != untilToken )
            {
                builder.Append(_Tokens[ index ].Text.Trim( new[] {'[', ']'} ));
            } 

            CurrentIndex = index;
            return builder.ToString( ).Trim();
        }

        public int FindToken( string tokenType )
        {
            var index = 0;
            while ( _Tokens[ index ].Type != tokenType && ++index < _Tokens.Count  ){ }

            if ( index == _Tokens.Count )
            {
                return -1;
            }
            CurrentIndex = index;
            return index;
        }
    }
}
