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

        public string GetIdAfterToken( string afterTokenType, int startIndex = 0, bool skipParenthesis = true)
        {
            var index = startIndex;
            while ( _Tokens[ index ].Type != afterTokenType && ++index < _Tokens.Count  ){ }
            return GetId( index, skipParenthesis );
        }

        private string GetId( int index, bool skipParenthesis )
        {
            string id = "";

            while ( ++index < _Tokens.Count && (IsSkipToken( index, skipParenthesis ) || IsIdToken( index )) )
            {
                if ( IsIdToken( index ) )
                {
                    id += GetIdPart( _Tokens[ index ] );
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

        private bool IsIdToken( int index )
        {
            return _Tokens[ index ].Type == "." || _Tokens[ index ].Type == "TOKEN_ID";
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
    }
}
