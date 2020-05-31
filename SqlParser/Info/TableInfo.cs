using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlParser;
using ForeignKeyConstraint = SqlMemoryDb.ForeignKeyConstraint;

namespace SqlMemoryDb.Info
{
    internal class TableInfo
    {
        private readonly MemoryDatabase _Info;

        internal TableInfo( MemoryDatabase info )
        {
            _Info = info;
        }

        internal void Add( SqlCreateTableStatement createTable )
        {
            var table = new Table( createTable.Name );
            _Info.Tables.Add( table.FullName, table );
            AddColumns( table, createTable.Definition );
            AddConstraints( table, createTable.Definition );
        }

        private void AddConstraints( Table table, SqlTableDefinition createTableDefinition )
        {
            foreach ( var constraint in createTableDefinition.Constraints )
            {
                switch ( constraint.Type )
                {
                    case SqlConstraintType.PrimaryKey: AddPrimaryConstraint( table, constraint ); break;
                    case SqlConstraintType.ForeignKey: AddForeignKeyConstraint( table, (SqlForeignKeyConstraint)constraint ); break;
                }
            }
        }

        private void AddPrimaryConstraint( Table table, SqlConstraint constraint )
        {
            foreach ( var child in constraint.Children )
            {
                if ( child is SqlIndexedColumn code )
                {
                    var column = table.Columns.FirstOrDefault( c => c.Name == code.Name.Value );
                    if ( column == null )
                    {
                        throw new KeyNotFoundException( $"Column with the name [{code.Name.Value}] is not a member of table [{table.FullName}]");
                    }

                    column.IsPrimaryKey = true;
                }
            }
        }

        private void AddForeignKeyConstraint( Table table, SqlForeignKeyConstraint constraint )
        {
            var fk = new ForeignKeyConstraint
            {
                Name = constraint.Name.Value,
                Columns = constraint.Columns.Select(  c => c.Value ).ToList(  ),
                ReferencedTableName = Helper.GetQualifiedName( constraint.ReferencedTable ),
                ReferencedColumns = constraint.ReferencedColumns.Select( c => c.Value ).ToList(  ),
                DeleteAction = constraint.DeleteAction,
                UpdateAction = constraint.UpdateAction
            };
            table.ForeignKeyConstraints.Add( fk );
        }

        private void AddColumns( Table table, SqlTableDefinition tableDefinition )
        {
            foreach ( var columnDefinition in tableDefinition.ColumnDefinitions )
            {
                var column = new Column( table, columnDefinition.Name.Value, columnDefinition.DataType.Sql, table.Columns.Count + 1 );
                foreach ( var constraint in columnDefinition.Constraints )
                {
                    switch ( constraint.Type )
                    {
                        case SqlConstraintType.Identity:
                            var identity = constraint as SqlColumnIdentity;
                            column.Identity = new Column.ColumnIdentity { Increment = identity.Increment ?? 1, Seed = identity.Seed ?? 1 };
                            column.NextIdentityValue = column.Identity.Seed;
                            break;

                        case SqlConstraintType.NotNull:
                            column.IsNullable = false;
                            break;

                        case SqlConstraintType.Null:
                            column.IsNullable = true;
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
