using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Helpers;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb
{
    internal class CreateTable
    {
        private readonly SqlCreateTableStatement _CreateTable;
        private readonly Table _Table;

        internal CreateTable(  SqlCreateTableStatement createTable )
        {
            _CreateTable = createTable;
            _Table = new Table( createTable.Name );
        }

        internal void AddToDatabase( MemoryDatabase info, MemoryDbConnection connection )
        {
            if ( IsTempTable( _Table.Name ) )
            {
                connection.TempTables.Add( _Table.FullName, _Table );
            }
            else
            {
                info.Tables.Add( _Table.FullName, _Table );
            }
            AddColumns( _CreateTable.Definition );
            AddConstraints( _CreateTable.Definition );
        }

        private bool IsTempTable( string tableName )
        {
            return tableName.Length >= 2 && tableName[0] == '#' && tableName[1] != '#';
        }

        private void AddConstraints( SqlTableDefinition createTableDefinition )
        {
            foreach ( var constraint in createTableDefinition.Constraints )
            {
                switch ( constraint.Type )
                {
                    case SqlConstraintType.PrimaryKey: AddPrimaryConstraint( constraint ); break;
                    case SqlConstraintType.ForeignKey: AddForeignKeyConstraint(  (SqlForeignKeyConstraint)constraint ); break;
                }
            }
        }

        private void AddPrimaryConstraint(  SqlConstraint constraint )
        {
            foreach ( var child in constraint.Children )
            {
                if ( child is SqlIndexedColumn code )
                {
                    var column = _Table.Columns.FirstOrDefault( c => c.Name == code.Name.Value );
                    if ( column == null )
                    {
                        throw new KeyNotFoundException( $"Column with the name [{code.Name.Value}] is not a member of table [{_Table.FullName}]");
                    }

                    column.IsPrimaryKey = true;
                }
            }
        }

        private void AddForeignKeyConstraint(  SqlForeignKeyConstraint constraint )
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
            _Table.ForeignKeyConstraints.Add( fk );
        }

        private void AddColumns(  SqlTableDefinition tableDefinition )
        {
            foreach ( var columnDefinition in tableDefinition.ColumnDefinitions )
            {
                Column column;
                if ( columnDefinition is SqlComputedColumnDefinition computedColumn )
                {
                    var expression = computedColumn.Expression as SqlBuiltinScalarFunctionCallExpression;
                    var dataType = GuessDataType( expression, null );
                    column = new Column( _Table, columnDefinition.Name.Value, _Table.Columns.Count, expression, dataType );
                }
                else
                {
                    column = new Column( _Table, columnDefinition.Name.Value, columnDefinition.DataType.Sql, _Table.Columns.Count );
                }
                AddColumnConstrains( columnDefinition, column );
                _Table.Columns.Add( column );
            }
        }



        private DataTypeInfo GuessDataType( SqlScalarExpression expression, DataTypeInfo currentDataType )
        {
            if ( expression is SqlUnaryScalarExpression unaryExpression )
            {
                expression = unaryExpression.Expression;
            }
            switch ( expression )
            {
                case SqlBuiltinScalarFunctionCallExpression function:
                {
                    currentDataType = GetTypeFromFunction( currentDataType, function );
                    break;
                }
                case SqlBinaryScalarExpression binary:
                {
                    var newDataTypeLeft = GuessDataType( binary.Left, currentDataType );
                    var newDataTypeRight = GuessDataType( binary.Right, currentDataType );
                    currentDataType = UpdateDataType( binary, newDataTypeLeft, newDataTypeRight, currentDataType );
                    break;
                }
                case SqlLiteralExpression literal:
                {
                    var newDataType = new DataTypeInfo( literal );
                    currentDataType = UpdateDataType( literal, newDataType, currentDataType );
                    break;
                }
                case SqlColumnRefExpression columnRef:
                {
                    var columnName = Helper.GetColumnName( columnRef );
                    var column = Helper.FindColumn( new TableAndColumn( ), columnName,
                        new Dictionary<string, Table>( ) {[_Table.FullName] = _Table } );
                    currentDataType = UpdateDataType( columnRef, column, currentDataType );
                    break;                  
                }
                default:
                    throw new NotImplementedException($"GuessDataType() does not support expressions of type { expression.GetType(  )}");
            }
            return currentDataType;
        }

        private DataTypeInfo GetTypeFromFunction( DataTypeInfo currentDataType, SqlBuiltinScalarFunctionCallExpression function )
        {
            DataTypeInfo dataType = SelectDataBuilder.GetDataTypeFromFunction( function.FunctionName );
            if ( dataType != null )
            {
                return dataType;
            }

            if ( function is SqlConvertExpression convert )
            {
                return new DataTypeInfo( convert.DataType.Sql );
            }

            var dataTypes = new List<DataTypeInfo>( );
            foreach ( var argument in function.Arguments )
            {
                var newDataType = GuessDataType( argument, null );
                if ( newDataType != null )
                {
                    dataTypes.Add( newDataType );
                }
            }

            dataType = dataTypes.FirstOrDefault( d => d is Column ) ?? dataTypes.FirstOrDefault( );
            if ( dataType != null )
            {
                return dataType;
            }
            return currentDataType;
        }

        private DataTypeInfo UpdateDataType( SqlScalarExpression argument, DataTypeInfo newDataType, DataTypeInfo currentDataType )
        {
            return newDataType;
        }

        private DataTypeInfo UpdateDataType( SqlScalarExpression argument, DataTypeInfo newDataTypeLeft, DataTypeInfo newDataTypeRight, DataTypeInfo currentDataType )
        {
            var list = new List<DataTypeInfo>{ newDataTypeLeft, newDataTypeRight, currentDataType };
            list = list.Where( l => l != null ).ToList( );
            if ( list.Any( d => d.NetDataType == typeof(string) ) )
            {
                return list.First( d => d.NetDataType == typeof( string ) );
            }

            return newDataTypeLeft;
        }

        private  void AddColumnConstrains( SqlColumnDefinition columnDefinition, Column column )
        {
            if ( columnDefinition.Constraints == null )
            {
                return;
            }

            foreach ( var constraint in columnDefinition.Constraints )
            {
                switch ( constraint.Type )
                {
                    case SqlConstraintType.Identity:
                        var identity = constraint as SqlColumnIdentity;
                        column.Identity = new Column.ColumnIdentity
                            {Increment = identity.Increment ?? 1, Seed = identity.Seed ?? 1};
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
                        _Table.PrimaryKeys.Add( column );
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
        }
    }
}
