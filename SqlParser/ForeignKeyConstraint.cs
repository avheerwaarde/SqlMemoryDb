﻿using System.Collections.Generic;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlMemoryDb
{
    public class ForeignKeyConstraint
    {
        public string Name;
        public List<string> Columns;
        public string ReferencedTableName;
        public List<string> ReferencedColumns;
        public bool CheckThrowsException;

        public ForeignKeyConstraint( )
        {
            Columns = new List<string>();
            ReferencedColumns = new List<string>();
            CheckThrowsException = true;
        }

        public SqlForeignKeyAction DeleteAction { get; set; }
        public SqlForeignKeyAction UpdateAction { get; set; }
    }
}