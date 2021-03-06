﻿using System.Collections.Generic;
using System.Data;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
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
        public SqlBuiltinScalarFunctionCallExpression DefaultCallExpression;

        public bool IsIdentity => Identity != null;
        public bool HasDefault => string.IsNullOrWhiteSpace( DefaultValue ) == false || DefaultCallExpression != null;
        public bool IsUnique { get ; set ; }
        public bool IsRowVersion { get; set; }
        public bool IsPrimaryKey { get ; set ; }
        public int NextIdentityValue { get; set; }
        public Table ParentTable { get; set; }
        public SqlBuiltinScalarFunctionCallExpression ComputedExpression;

        public Column( Table table, string name, int order, 
            SqlBuiltinScalarFunctionCallExpression expression, DataTypeInfo type ) 
            : base( type )
        {
            ParentTable = table;
            Name = name;
            Order = order;
            IsNullable = true;
            ComputedExpression = expression;
        }


        public Column( Table table, string name, string sqlType, Dictionary<string,SqlCreateUserDefinedDataTypeStatement> userDataTypes, int order ) 
            : base( sqlType, userDataTypes )
        {
            ParentTable = table;
            Name = name;
            Order = order;
            IsNullable = true;
            IsRowVersion = sqlType.ToLower( ).Contains( "rowversion" );
        }

        public Column( Column sourceColumn, string name, int order ) 
            : base( sourceColumn )
        {
            ParentTable = sourceColumn.ParentTable;
            Name = name;
            Order = order;
            IsNullable = sourceColumn.IsNullable;
            IsRowVersion = sourceColumn.IsRowVersion;
        }

        public Column( Table table, string name, SqlDataTypeSpecification sqlDataType, Dictionary<string, SqlCreateUserDefinedDataTypeStatement> userDataTypes, int order ) 
            : base( sqlDataType, userDataTypes)
        {
            ParentTable = table;
            Name = name;
            Order = order;
            IsNullable = true;
            IsRowVersion = sqlDataType.DataType.ObjectIdentifier.ObjectName.Value.ToLower() == "rowversion";
        }
    }
}
