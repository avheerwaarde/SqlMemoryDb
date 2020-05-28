using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Info;
using SqlParser;

namespace SqlMemoryDb
{
    class ExecuteInsertStatement
    {
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

            table.Rows.Add( row );
        }

        private static ArrayList InitializeNewRow( Table table )
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
