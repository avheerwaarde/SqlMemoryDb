using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Info;
using SqlParser;

namespace SqlMemoryDb
{
    class ExecuteSelectStatement
    {
        private readonly MemoryDbCommand _Command;
        private readonly MemoryDbDataReader _Reader;

        public ExecuteSelectStatement( MemoryDbCommand command, MemoryDbDataReader reader )
        {
            _Command = command;
            _Reader = reader;
        }

        public void Execute( Dictionary<string, Table> tables, SqlSelectStatement selectStatement )
        {
            var columns = new List<SqlCodeObject>();
            SqlFromClause from;
            var rawRows = new List<List<ArrayList>>( );

            var parts = selectStatement.SelectSpecification.QueryExpression.Children;
            foreach ( var part in parts )
            {
                switch ( part )
                {
                    case SqlSelectClause clause: columns = part.Children.ToList(); break;
                    case SqlFromClause 
                        fromClause: from = fromClause; 
                        rawRows = GetTablesFromClause( fromClause, tables ); 
                        break;
                }
            }
            throw new NotImplementedException( );
        }

        private List<List<ArrayList>> GetTablesFromClause( SqlFromClause fromClause, Dictionary<string, Table> tables )
        {
            var rawRows = new List<List<ArrayList>>( );
            foreach ( var expression in fromClause.TableExpressions )
            {
                switch ( expression )
                {
                    case SqlTableRefExpression tableRef:
                        var table = tables[ Helper.GetQualifiedName(tableRef.ObjectIdentifier) ];
                        foreach ( var row in table.Rows )
                        {
                            var rawRow = new List<ArrayList>( );
                            rawRow.Add( row );
                            rawRows.Add( rawRow );
                        }
                        break;
                }
            }

            return rawRows;
        }
    }
}
