using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlParser.Info;

namespace SqlParser
{
    public class SqlMetaInfo
    {
        public Dictionary<string, Table> Tables = new Dictionary<string, Table>();


        public void InitializeDb( string sql )
        {
            var result = Parser.Parse( sql );
            if ( result.Errors.Any())
            {
                throw new SqlServerParserException( result.Errors );
            }

            ProcessParseResult( result );
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
                    }
                }
            }
        }

    }
}
