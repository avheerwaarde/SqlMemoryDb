using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlMemoryDb.SelectData;
using SqlParser;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlMemoryDb
{
    class ExecuteNonQueryStatement
    {
        private readonly MemoryDbCommand _Command;
        private readonly MemoryDatabase _Database;

        public ExecuteNonQueryStatement( MemoryDatabase memoryDatabase, MemoryDbCommand command )
        {
            _Database = memoryDatabase;
            _Command = command;
        }

        public void Execute( Dictionary<string, Table> tables, SqlInsertStatement insertStatement )
        {
            var spec = insertStatement.Children.First( ) as SqlInsertSpecification;
            var table = GetInsertTable( tables, spec );
            var columns = GetInsertColumns( spec, table );

            switch ( spec.Source )
            {
                case SqlTableConstructorInsertSource tableSource:
                    AddRowFromTableConstructor( table, columns, tableSource );
                    break;
                case SqlSelectSpecificationInsertSource selectSource:
                    AddRowsFromSelect( table, columns, selectSource );
                    break;
                default:
                    throw new NotImplementedException($"Not implemented for insert source {spec.Source.GetType(  ) }");
            }
        }

        private void AddRowFromTableConstructor( Table table, List<Column> columns, SqlTableConstructorInsertSource source )
        {
            var row = InitializeNewRow( table, columns );
            var valueList = source.TableConstructorExpression.Rows[ 0 ].Children.ToList(  );

            if ( columns.Count > valueList.Count )
            {
                throw new SqlInsertTooManyColumnsException(  );
            }
            if ( columns.Count < valueList.Count )
            {
                throw new SqlInsertTooManyValuesException(  );
            }

            for ( int index = 0; index < columns.Count; index++ )
            {
                AddRowValue( row, columns[ index ], valueList[ index ] );
            }
            AddRowToTable( table, row );
        }

        private void AddRowsFromSelect( Table table, List<Column> columns, SqlSelectSpecificationInsertSource selectSource )
        {
            var command = new MemoryDbCommand( _Command );
            var rawData = new RawData( command );
            var select = selectSource.SelectSpecification;
            var batch = new ExecuteQueryStatement( _Database, command, columns ).Execute( rawData, _Database.Tables, select.QueryExpression, select.OrderByClause );

            foreach ( var resultRow in batch.ResultRows )
            {
                var row = InitializeNewRow( table, columns );
                for ( int index = 0; index < columns.Count; index++ )
                {
                    row[ columns[ index ].Order ] = resultRow[ index ];
                }

                AddRowToTable( table, row );
            }
        }

        private void AddRowToTable( Table table, ArrayList row )
        {
            ValidateAllRequiredFieldsAreSet( row, table );
            ValidateAllForeignKeyConstraints( row, table );
            table.Rows.Add( row );
            _Command.RowsAffected++;
        }



        private void AddRowValue( ArrayList row, Column column, SqlCodeObject value )
        {
            if ( column.IsIdentity && column.ParentTable.IsIdentityInsertForbidden || column.IsRowVersion )
            {
                throw new SqlUpdateColumnForbiddenException( column.Name );
            }
            switch ( value )
            {
                case SqlLiteralExpression literal:
                {
                    row[ column.Order ] = Helper.GetValueFromString( column, literal );
                    break;
                }
                case SqlBuiltinScalarFunctionCallExpression function:
                {
                    var select = new SelectDataBuilder(  ).Build( function, new RawData( _Command ) );
                    row[ column.Order ] = select.Select( new List<RawData.RawDataRow>() );
                    break;
                }
                case SqlScalarVariableRefExpression variableRef:
                {
                    var select = new SelectDataFromVariables( variableRef, _Command );
                    row[ column.Order ] = select.Select( new List<RawData.RawDataRow>() );
                    break;
                }
                default:
                    throw new NotImplementedException( $"Value of type {value.GetType(  )} is not supported");
            }
            ValidateDataSize( column, row[ column.Order ] );
        }

        private void ValidateDataSize( Column column, object source )
        {
            if ( source == null && column.IsNullable )
            {
                return;
            }

            if ( column.NetDataType == typeof(string) )
            {
                if ( column.Size > 0 && column.Size < ((string)source).Length )
                {
                    throw new SqlDataTruncatedException( column.Size, ((string)source).Length );
                }
            }
            
            if ( column.NetDataType == typeof(byte[]) )
            {
                if ( column.Size > 0 && column.Size < ((byte[])source).Length )
                {
                    throw new SqlDataTruncatedException( column.Size, ((byte[])source).Length );
                }
            }
        }

        private ArrayList InitializeNewRow( Table table, List<Column> columns )
        {
            var row = new ArrayList( );
            for ( int count = 0; count < table.Columns.Count; count++ )
            {
                row.Add( null );
            }

            foreach ( var column in table.Columns )
            {
                if ( column.IsIdentity )
                {
                    if ( columns.Any( c => c.Name == column.Name ) && table.IsIdentityInsertForbidden )
                    {
                        throw new SqlInsertIdentityException( table.Name, column.Name );
                    }
                    row[ column.Order ] = column.NextIdentityValue;
                    table.LastIdentitySet = column.NextIdentityValue;
                    ((MemoryDbConnection )_Command.Connection).GetMemoryDatabase( ).LastIdentitySet = column.NextIdentityValue;
                    _Command.LastIdentitySet = column.NextIdentityValue;
                    column.NextIdentityValue += column.Identity.Increment;
                }
                else if ( column.IsRowVersion )
                {
                    row[ column.Order ] = _Database.NextRowVersion( );
                }
                else if ( column.HasDefault && string.IsNullOrWhiteSpace( column.DefaultValue ) == false )
                {
                    row[ column.Order ] = Helper.GetValueFromString( column, column.DefaultValue );
                }
                else if ( column.HasDefault && column.DefaultCallExpression != null )
                {
                    var rawDataRows = new List<RawData.RawDataRow>{ new RawData.RawDataRow{ Name = table.Name, Table = table, Row = row }};
                    row[ column.Order ] = Helper.GetValue( column.DefaultCallExpression, column.NetDataType, new RawData(_Command), rawDataRows );
                }
            }

            return row;
        }

        private Table GetInsertTable( Dictionary<string, Table> tables, SqlInsertSpecification spec )
        {
            var target = spec.Target as SqlTableRefExpression;
            return Helper.GetTableFromObjectId( target.ObjectIdentifier, tables, ((MemoryDbConnection)_Command.Connection).TempTables );
        }

        private static List<Column> GetInsertColumns( SqlInsertSpecification spec, Table table )
        {
            if ( spec.TargetColumns == null )
            {
                return table.Columns;
            }
            var columns = new List<Column>( );
            var columnRefs = spec.TargetColumns as SqlColumnRefExpressionCollection;
            foreach ( var columnRef in columnRefs )
            {
                var column = table.Columns.FirstOrDefault( c => c.Name == columnRef.ColumnName.Value );
                if ( column == null )
                {
                    throw new SqlInvalidColumnNameException( columnRef.ColumnName.Value );
                }

                columns.Add( column );
            }

            return columns;
        }

        private void ValidateAllRequiredFieldsAreSet( ArrayList row, Table table )
        {
            foreach ( var column in table.Columns.Where( c => c.IsNullable == false ) )
            {
                if ( row[ column.Order ] == null )
                {
                    throw new SqlFieldIsNullException( table.FullName, column.Name );
                }
            }
        }

        private void ValidateAllForeignKeyConstraints( ArrayList row, Table table )
        {
            foreach ( var constraint in table.ForeignKeyConstraints.Where( k => k.CheckThrowsException ) )
            {
                for ( int index = 0; index < constraint.Columns.Count; index++ )
                {
                    var column = table.Columns.Single( c => c.Name == constraint.Columns[ index ] );
                    var foreignKey = row[ column.Order ];
                    if ( foreignKey != null )
                    {
                        var referencedTable = ((MemoryDbConnection )_Command.Connection).GetMemoryDatabase( ).Tables[ constraint.ReferencedTableName ];
                        var referencedColumn = referencedTable.Columns.Single(c => c.Name == constraint.ReferencedColumns[ index ] );

                        if ( referencedTable.Rows.Any( r =>((IComparable)foreignKey).CompareTo((IComparable)r[ referencedColumn.Order ]) == 0 )  == false ) 
                        {
                            throw new SqlInsertInvalidForeignKeyException( constraint.Name, referencedTable.FullName, referencedColumn.Name );
                        }
                    }
                }
            }
        }

        public void Execute( Dictionary<string, Table> tables, SqlIfElseStatement ifElseStatement )
        {
            var rawData = new RawData( _Command );
            var evaluator = new EvaluateBooleanExpression( rawData, _Database, _Command );
            var isTrue = evaluator.Evaluate( new List<RawData.RawDataRow>( ), ifElseStatement.Condition );
            if ( isTrue && ifElseStatement.TrueStatement != null )
            {
                _Database.ExecuteStatement( _Command, ifElseStatement.TrueStatement );                
            }
            else if ( isTrue == false && ifElseStatement.FalseStatement != null )
            {
                _Database.ExecuteStatement( _Command, ifElseStatement.FalseStatement );                
            }
        }

        public void Execute( Dictionary<string, Table> tables, SqlDeleteStatement deleteStatement )
        {
            var rawData = GetRowsForDelete( tables, deleteStatement );
            foreach ( var rows in rawData.RawRowList )
            {
                var rawRow = rows[ 0 ];
                rawRow.Table.Rows.Remove( rawRow.Row );
            }
        }

        private RawData GetRowsForDelete( Dictionary<string, Table> tables, SqlDeleteStatement deleteStatement )
        {
            var rawData = new RawData( _Command );

            var specification = deleteStatement.DeleteSpecification;
            if ( specification.FromClause != null )
            {
                rawData.AddTablesFromClause( specification.FromClause, tables );
            }
            else
            {
                rawData.AddTable( specification.Target, tables );
            }

            if ( specification.WhereClause != null )
            {
                rawData.ExecuteWhereClause( specification.WhereClause );
            }

            if ( specification.TopSpecification != null )
            {
                var rowCount = int.Parse( specification.TopSpecification.Value.Sql );
                rawData.RawRowList = rawData.RawRowList.Take( rowCount ).ToList( );
            }

            return rawData;
        }
    }
}
