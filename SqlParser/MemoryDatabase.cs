using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Info;
using SqlParser;

namespace SqlMemoryDb
{
    public class MemoryDatabase
    {
        public Dictionary<string, Table> Tables = new Dictionary<string, Table>();


        public int ExecuteSqlStatement( string sql )
        {
            var result = Parser.Parse( sql );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            ProcessParseResult( result );
            return 1;
        }

        private void ProcessParseResult( ParseResult result )
        {
            foreach ( var batch in result.Script.Batches )
            {
                foreach ( var child in batch.Children )
                {
                    switch ( child )
                    {
                        case SqlCreateTableStatement createTable: new TableInfo( this ).Add( createTable ); break;
                        case SqlInsertStatement insertStatement: new ExecuteInsertStatement().Execute( Tables, insertStatement ); break; 
                    }
                }
            }
        }
    }
}
