using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlParser
{
    public class Column
    {
        public class ColumnIdentity
        {
            public int Seed;
            public int Increment;
        }

        public string Name;
        public SqlDataType DataType;
        public ColumnIdentity Identity;

        public bool IsNullable { get ; set ; }
        public string DefaultValue { get ; set ; }

        public bool IsIdentity => Identity != null;
        public bool HasDefault => string.IsNullOrWhiteSpace( DefaultValue );
        public bool IsUnique { get ; set ; }
        public bool IsPrimaryKey { get ; set ; }

        public Column( string name, SqlDataType dataType )
        {
            Name = name;
            DataType = dataType;
            IsNullable = true;
        }

    }
}
