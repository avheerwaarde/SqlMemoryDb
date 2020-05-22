using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace SqlParser.Info
{
    internal class TableInfo
    {
        private readonly SqlMetaInfo _Info;

        internal TableInfo( SqlMetaInfo info )
        {
            _Info = info;
        }

        internal void Add( SqlCreateTableStatement createTable )
        {
            var table = new Table( createTable.Name );
            _Info.Tables.Add( table.FullName, table );
            AddColumns( table, createTable.Definition );
        }

        private void AddColumns( Table table, SqlTableDefinition tableDefinition )
        {
            foreach ( var columnDefinition in tableDefinition.ColumnDefinitions )
            {
                var column = new Column( columnDefinition.Name.Value, columnDefinition.DataType.DataType );
                foreach ( var constraint in columnDefinition.Constraints )
                {
                    switch ( constraint.Type )
                    {
                        case SqlConstraintType.Identity:
                            var identity = constraint as SqlColumnIdentity;
                            column.Identity = new Column.ColumnIdentity { Increment = identity.Increment ?? 1, Seed = identity.Seed ?? 1 };
                            break;

                        case SqlConstraintType.NotNull:
                            column.IsNullable = true;
                            break;

                        case SqlConstraintType.Null:
                            column.IsNullable = false;
                            break;

                        case SqlConstraintType.Default:
                            var @default = constraint as SqlDefaultConstraint;
                            if ( @default.Expression is SqlLiteralExpression expression )
                            {
                                column.DefaultValue = expression.Value;
                            }
                            break;

                        case SqlConstraintType.PrimaryKey:
                            column.IsPrimaryKey = true;
                            column.IsUnique = true;
                            table.PrimaryKeys.Add( column );
                            break;

                        case SqlConstraintType.ForeignKey:
                            break;

                        case SqlConstraintType.Unique:
                            column.IsUnique = true;
                            break;

                            //case SqlConstraintType.RowGuidCol:
                            //case SqlConstraintType.Check:

                    }
                }
                table.Columns.Add( column );
            }
        }
    }
}
