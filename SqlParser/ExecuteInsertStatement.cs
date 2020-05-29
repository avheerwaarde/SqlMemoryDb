using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Info;
using SqlParser;

namespace SqlMemoryDb
{
    class ExecuteInsertStatement
    {
        private readonly MemoryDbCommand _Command;
        public ExecuteInsertStatement( MemoryDbCommand command )
        {
            _Command = command;
        }

        public void Execute( Dictionary<string, Table> tables, SqlInsertStatement insertStatement )
        {
            var spec = insertStatement.Children.First( ) as SqlInsertSpecification;
            var table = GetInsertTable( tables, spec );
            var columns = GetInsertColumns( spec, table );

            var source = spec.Children.First( c => c is SqlTableConstructorInsertSource ) as SqlTableConstructorInsertSource;
            AddRow( table, columns, source );
        }

        private void AddRow( Table table, List<Column> columns, SqlTableConstructorInsertSource source )
        {
            var row = InitializeNewRow( table );
            var sql = Helper.CleanSql( source.Sql.Substring( 6 ) );
            var values = GetValuesFromSql( sql );

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
            table.Rows.Add( row );
        }

        private void AddRowValue( ArrayList row, Column column, string value )
        {
            if ( value.StartsWith( "@" ) )
            {
                row[ column.Order - 1] = Helper.GetValueFromParameter( column, value, _Command.Parameters );
            }
            else
            {
                row[ column.Order - 1] = Helper.GetValueFromString( column, value );
            }

            ValidateDataSize( column, row[ column.Order - 1 ] );
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

        private List<string> GetValuesFromSql( string sql )
        {
            var values = new List<string>( );
            sql = sql.Trim( new[] {'(', ')'} );
            var parts = sql.Split( new[] {','}, StringSplitOptions.RemoveEmptyEntries );
            foreach ( var part in parts )
            {
                var value = Helper.GetStringValue( part.Trim() );
                values.Add( value );
            }
            return values;
        }

        private ArrayList InitializeNewRow( Table table )
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
                    row[ column.Order - 1 ] = column.NextIdentityValue;
                    table.LastIdentitySet = column.NextIdentityValue;
                    MemoryDbConnection.GetMemoryDatabase( ).LastIdentitySet = column.NextIdentityValue;
                    _Command.LastIdentitySet = column.NextIdentityValue;
                    column.NextIdentityValue += column.Identity.Increment;

                }
                else if ( column.HasDefault && string.IsNullOrWhiteSpace( column.DefaultValue ) == false )
                {
                    row[ column.Order - 1 ] = Helper.GetValueFromString( column, column.DefaultValue );
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
    }
}
