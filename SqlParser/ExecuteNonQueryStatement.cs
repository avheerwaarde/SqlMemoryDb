using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlMemoryDb.SelectData;
using SqlParser;

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
            var values = GetValuesFromSql( source.Tokens );

            if ( columns.Count > values.Count )
            {
                throw new SqlInsertTooManyColumnsException(  );
            }
            if ( columns.Count < values.Count )
            {
                throw new SqlInsertTooManyValuesException(  );
            }

            for ( int index = 0; index < columns.Count; index++ )
            {
                AddRowValue( row, columns[ index ], values[ index ] );
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



        private void AddRowValue( ArrayList row, Column column, string value )
        {
            if ( value.StartsWith( "@" ) )
            {
                row[ column.Order ] = Helper.GetValueFromParameter( value, _Command.Parameters, _Command.Variables );
            }
            else
            {
                if ( Regex.IsMatch( value, "\\b[^()]+\\((.*)\\)$" ) || value.StartsWith( "@@" ))
                {
                    var select = new SelectDataBuilder(  ).Build( value, new RawData( _Command ) );
                    row[ column.Order ] = select.Select( new List<RawData.RawDataRow>() );
                }
                else
                {
                    row[ column.Order ] = Helper.GetValueFromString( column, value );
                }
            }

            ValidateDataSize( column, row[ column.Order ] );
        }

        private void ValidateDataSize( Column column, object source )
        {
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

        private List<string> GetValuesFromSql( IEnumerable<Token> tokens )
        {
            var values = new List<string>( );
            var parenthesisCount = 0;
            var id = "";
            foreach ( var token in tokens )
            {
                if ( parenthesisCount == 1 
                     && (token.Type == ")" || token.Type == ",") 
                     && string.IsNullOrWhiteSpace( id ) == false )
                {
                    values.Add( id );
                    id = "";
                }

                if ( token.Type == ")" )
                {
                    parenthesisCount--;
                    if ( parenthesisCount == 0 )
                    {
                        break;
                    }
                }

                if ( parenthesisCount > 0 && token.Type != "," && token.Type != "LEX_WHITE" )
                {
                    id += token.Text;
                }
                if ( token.Type == "(" )
                {
                    parenthesisCount++;
                }
            }
            return values;
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
                    if ( columns.Any( c => c.Name == column.Name ) && table.Options[ Table.OptionEnum.IdentityInsert].ToUpper() == "OFF")
                    {
                        throw new SqlInsertIdentityException( table.Name, column.Name );
                    }
                    row[ column.Order ] = column.NextIdentityValue;
                    table.LastIdentitySet = column.NextIdentityValue;
                    ((MemoryDbConnection )_Command.Connection).GetMemoryDatabase( ).LastIdentitySet = column.NextIdentityValue;
                    _Command.LastIdentitySet = column.NextIdentityValue;
                    column.NextIdentityValue += column.Identity.Increment;
                }
                else if ( column.HasDefault && string.IsNullOrWhiteSpace( column.DefaultValue ) == false )
                {
                    row[ column.Order ] = Helper.GetValueFromString( column, column.DefaultValue );
                }
            }

            return row;
        }

        private static Table GetInsertTable( Dictionary<string, Table> tables, SqlInsertSpecification spec )
        {
            var target = spec.Target as SqlTableRefExpression;
            var tableName = Helper.GetQualifiedName( target.ObjectIdentifier );
            if ( tables.ContainsKey( tableName ) == false )
            {
                throw new SqlInvalidTableNameException( tableName );
            }

            var table = tables[ tableName ];
            return table;
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
