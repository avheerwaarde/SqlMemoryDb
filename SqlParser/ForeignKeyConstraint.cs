using System.Collections.Generic;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlParser
{
    public class ForeignKeyConstraint
    {
        public List<string> Columns;
        public string ReferencedTableName;
        public List<string> ReferencedColumns;

        public ForeignKeyConstraint( )
        {
            Columns = new List<string>();
            ReferencedColumns = new List<string>();
        }

        public SqlForeignKeyAction DeleteAction { get; set; }
        public SqlForeignKeyAction UpdateAction { get; set; }
    }
}