using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb;
using SqlMemoryDb.Helpers;

namespace SqlParser
{
    public class Table
    {
        public enum OptionEnum
        {
            IdentityInsert = 1
        }

        public string Name;
        public string SchemaName;
        public string FullName;
        public readonly List<Column> Columns;
        public readonly List<Column> PrimaryKeys;
        public readonly Dictionary<string, List<Column>> PrimaryKeyConstraints;
        public readonly List<ForeignKeyConstraint> ForeignKeyConstraints;
        public readonly List<ArrayList> Rows;
        public readonly Dictionary<OptionEnum, string> Options;
        public long? LastIdentitySet;

        public Table( SqlObjectIdentifier name )
        {
            Name = name.ObjectName.Value;
            SchemaName = name.SchemaName?.Value ?? Helper.DefaultSchemaName;
            FullName = Helper.GetQualifiedName( name );
            Columns = new List<Column>();
            PrimaryKeys = new List<Column>();
            PrimaryKeyConstraints = new Dictionary<string, List<Column>>();
            ForeignKeyConstraints = new List<ForeignKeyConstraint>();
            Rows = new List<ArrayList>();
            Options = new Dictionary<OptionEnum, string>
            {
                [ OptionEnum.IdentityInsert ] = "off"
            };
        }
    }
}
