using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlParser;

namespace SqlMemoryDb
{
    public class ExecuteNullStatement
    {
        class TokenAction
        {
            public List<string> Tokens;
            public Action< ExecuteNullStatement, Dictionary<string, Table>, List<Token>> Action;
        }

        private static readonly List<TokenAction> _TokenActions = new List<TokenAction>
        {
            new TokenAction
            {
                Action = AddForeignKeyConstraint,
                Tokens = new List<string>
                {
                    "TOKEN_ALTER", "TOKEN_TABLE", "TOKEN_ADD", "TOKEN_CONSTRAINT", "TOKEN_FOREIGN", "TOKEN_KEY",
                    "TOKEN_REFERENCES"
                }
            },
            new TokenAction
            {
                Action = AddPrimaryKeyConstraint,
                Tokens = new List<string>
                {
                    "TOKEN_ALTER", "TOKEN_TABLE", "TOKEN_ADD", "TOKEN_CONSTRAINT", "TOKEN_PRIMARY", "TOKEN_KEY"
                }
            },
            new TokenAction
            {
                Action = SetIdentityInsert,
                Tokens = new List<string>
                {
                    "TOKEN_SET", "TOKEN_IDENTITY_INSERT", "TOKEN_ID"   
                }
            },
            new TokenAction
            {
                Action = SetGlobalVariable,
                Tokens = new List<string>
                {
                    "TOKEN_SET", "TOKEN_ID", "TOKEN_ID"   
                }
            },
            new TokenAction
            {
                Action = SetCheckConstraint,
                Tokens = new List<string>
                {
                    "TOKEN_ALTER", "TOKEN_TABLE", "TOKEN_CONSTRAINT"   
                }
            }
        };

        private readonly MemoryDbCommand _Command;
        private readonly MemoryDatabase _Database;

        public ExecuteNullStatement( MemoryDatabase memoryDatabase, MemoryDbCommand command )
        {
            _Database = memoryDatabase;
            _Command = command;
        }

        public void Execute( Dictionary<string, Table> tables, SqlNullStatement nullStatement )
        {
            var tokens = nullStatement.Tokens.ToList( ).ConvertAll( t => ( Token ) t ).ToList( );
            var tokenTypeList = tokens.Select( t => t.Type ).ToList( );

            foreach ( var tokenAction in _TokenActions )
            {
                if ( tokenAction.Tokens.All( t => tokenTypeList.Contains( t )  ))
                {
                    tokenAction.Action( this, tables, tokens );
                    break;
                }
            }
        }

        private static void AddForeignKeyConstraint( ExecuteNullStatement statement, Dictionary<string, Table> tables, List<Token> tokens )
        {
            var finder = new TokenFinder( tokens );
            var tableName = finder.GetIdAfterToken( "TOKEN_TABLE" );
            var columnName = finder.GetIdAfterToken( "TOKEN_KEY" );
            var foreignKeyName = finder.GetIdAfterToken( "TOKEN_CONSTRAINT" );
            var referencedTableName = finder.GetIdAfterToken( "TOKEN_REFERENCES", skipParenthesis:false );
            var referencedColumnName = finder.GetIdAfterToken( "(", finder.CurrentIndex - 1 );
            if ( string.IsNullOrWhiteSpace( tableName ) || string.IsNullOrWhiteSpace( columnName )
                || string.IsNullOrWhiteSpace( referencedTableName ) || string.IsNullOrWhiteSpace( referencedColumnName ))
            {
                throw new InvalidOperationException("A foreign key constrains should contain table + column + referenced table + referenced column");
            }

            var table = Helper.FindTableAndColumn( tableName, columnName, tables );
            var referencedTable = Helper.FindTableAndColumn( referencedTableName, referencedColumnName, tables );
            var fk = new ForeignKeyConstraint
            {
                Name = foreignKeyName,
                Columns = new List<string> {columnName},
                ReferencedTableName = referencedTableName,
                ReferencedColumns = new List<string> {referencedColumnName}
            };
            table.Table.ForeignKeyConstraints.Add( fk );
        }

        private static void AddPrimaryKeyConstraint( ExecuteNullStatement statement, Dictionary<string, Table> tables, List<Token> tokens )
        {
            var finder = new TokenFinder( tokens );
            var tableName = finder.GetIdAfterToken( "TOKEN_TABLE" );
            var columnNames = finder.GetIdListAfterToken( "(" );
            var primaryKeyName = finder.GetIdAfterToken( "TOKEN_CONSTRAINT" );

            var table = Helper.FindTable( tableName, tables ).Table;
            var columns = table.Columns.Where( c => columnNames.Contains( c.Name ) ).ToList(  );
            table.PrimaryKeyConstraints.Add( primaryKeyName, columns );
            table.PrimaryKeys.AddRange( columns );
            foreach ( var column in columns )
            {
                column.IsPrimaryKey = true;
            }
        }

        private static void SetIdentityInsert( ExecuteNullStatement statement, Dictionary<string, Table> tables, List<Token> tokens )
        {
            var finder = new TokenFinder( tokens );
            var tableName = finder.GetIdAfterToken( "TOKEN_IDENTITY_INSERT" );
            var isOn = finder.GetTokenAfterToken( "TOKEN_ON", "TOKEN_ID" );
            var tc = Helper.FindTable( tableName, tables );
            tc.Table.Options[ Table.OptionEnum.IdentityInsert ] = (isOn != null ? "on" : "off");
        }

        private static void SetGlobalVariable( ExecuteNullStatement statement, Dictionary<string, Table> tables, List<Token> tokens )
        {
            var finder = new TokenFinder( tokens );
            var variableName = finder.GetIdAfterToken( "TOKEN_SET", isSingleTokenId:true );
            var variableValue = finder.GetTokenAfterToken( "TOKEN_ID", "TOKEN_ID" );
            var options = MemoryDbConnection.GetMemoryDatabase( ).Options;

            if ( string.IsNullOrWhiteSpace( variableName ) == false 
                 && string.IsNullOrWhiteSpace( variableValue ) == false 
                 && options.ContainsKey( variableName ))
            {
                if ( variableValue == "NULL" )
                {
                    variableValue = null;
                }

                options[ variableName ] = variableValue;
            }
        }

        private static void SetCheckConstraint( ExecuteNullStatement statement, Dictionary<string, Table> tables, List<Token> tokens )
        {
            string constraint = null;
            var finder = new TokenFinder( tokens );
            var tableName = finder.GetIdAfterToken( "TOKEN_TABLE" );
            var isCheck = finder.GetTokenAfterToken( "TOKEN_CHECK", "TOKEN_ID" );
            var isAll = finder.GetTokenAfterToken( "TOKEN_ALL", "TOKEN_CONSTRAINT" );
            if ( isAll == null )
            {
                constraint = finder.GetTokenAfterToken( "TOKEN_ID", "TOKEN_CONSTRAINT" );
                constraint = Helper.CleanName( constraint );
            }
            var tc = Helper.FindTable( tableName, tables );
            var constraints = tc.Table.ForeignKeyConstraints;
            if ( isAll != null )
            {
                foreach ( var keyConstraint in constraints )
                {
                    keyConstraint.CheckThrowsException = isCheck != null;
                }
            }
            else
            {
                var keyConstraint = constraints.SingleOrDefault( c => c.Name == constraint );
                if ( keyConstraint == null )
                {
                    throw new SqlInvalidConstraintException( constraint );
                }

                keyConstraint.CheckThrowsException = isCheck != null;
            }
        }

    }
}