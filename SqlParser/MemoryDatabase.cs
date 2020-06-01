using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlParser;

namespace SqlMemoryDb
{
    public class MemoryDatabase
    {
        public Dictionary<string, Table> Tables = new Dictionary<string, Table>();
        public Decimal? LastIdentitySet;

        public int ExecuteSqlStatement( string commandText, MemoryDbCommand command )
        {
            var result = Parser.Parse( commandText );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            foreach ( var batch in result.Script.Batches )
            {
                command.LastIdentitySet = null;
                foreach ( var child in batch.Children )
                {
                    switch ( child )
                    {
                        case SqlCreateTableStatement createTable: new TableInfo( this ).Add( createTable ); break;
                        case SqlInsertStatement insertStatement: new ExecuteInsertStatement( command ).Execute( Tables, insertStatement ); break; 
                    }
                }
            }

            return 1;
        }

        public DbDataReader ExecuteSqlReader( string commandText, MemoryDbCommand command, CommandBehavior behavior )
        {
            var result = Parser.Parse( commandText );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            var reader = new MemoryDbDataReader( behavior  );
            foreach ( var batch in result.Script.Batches )
            {
                command.LastIdentitySet = null;
                foreach ( var child in batch.Children )
                {
                    switch ( child )
                    {
                        case SqlSelectStatement selectStatement: new ExecuteSelectStatement( command, reader ).Execute( Tables, selectStatement ); break; 
                    }
                }
            }

            return reader;
        }
    }
}
