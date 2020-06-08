using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb;

namespace SqlParser
{
    public class Table
    {
        public string Name;
        public string SchemaName;
        public string FullName;
        public readonly List<Column> Columns;
        public readonly List<Column> PrimaryKeys;
        public readonly List<ForeignKeyConstraint> ForeignKeyConstraints;
        public readonly List<ArrayList> Rows;
        public Decimal? LastIdentitySet;

        public Table( SqlObjectIdentifier name )
        {
            Name = name.ObjectName.Value;
            SchemaName = name.SchemaName.Value;
            FullName = $"{SchemaName}.{Name}";
            Columns = new List<Column>();
            PrimaryKeys = new List<Column>();
            ForeignKeyConstraints = new List<ForeignKeyConstraint>();
            Rows = new List<ArrayList>();
        }
    }
}
