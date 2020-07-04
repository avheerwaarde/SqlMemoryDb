using SqlParser;

namespace SqlMemoryDb
{
    public class Column : DataTypeInfo
    {
        public class ColumnIdentity
        {
            public int Seed;
            public int Increment;
        }

        public string Name { get ; set ; }
        public int Order;

        public ColumnIdentity Identity;

        public bool IsNullable { get ; set ; }
        public string DefaultValue { get ; set ; }

        public bool IsIdentity => Identity != null;
        public bool HasDefault => string.IsNullOrWhiteSpace( DefaultValue ) == false;
        public bool IsUnique { get ; set ; }
        public bool IsPrimaryKey { get ; set ; }
        public int NextIdentityValue { get; set; }
        public Table ParentTable { get; set; }



        public Column( Table table, string name, string sqlType, int order ) : base( sqlType )
        {
            ParentTable = table;
            Name = name;
            Order = order;
            IsNullable = true;
        }
    }
}
