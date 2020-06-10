﻿using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;
using SqlParser;

namespace SqlMemoryDb
{
    public class ExecuteNullStatement
    {
        private readonly List<string> _Tokens_AddForeignKeyConstraint = new List<string>
        {
            "TOKEN_ALTER", "TOKEN_TABLE", "TOKEN_ADD", "TOKEN_CONSTRAINT", "TOKEN_FOREIGN", "TOKEN_KEY", "TOKEN_REFERENCES"   
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

            if ( _Tokens_AddForeignKeyConstraint.All( t => tokenTypeList.Contains( t )  ))
            {
                AddForeignKeyConstraint( tables, tokens );
            }
        }

        private void AddForeignKeyConstraint( Dictionary<string, Table> tables, List<Token> tokens )
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
    }
}