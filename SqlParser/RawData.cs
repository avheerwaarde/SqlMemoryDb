using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb
{
    internal class RawData
    {
        public class RawDataRow
        {
            public string Name;
            public Table Table;
            public ArrayList Row;
        }
        public List<List<RawDataRow>> RawRowList = new List<List<RawDataRow>>();
        public DbParameterCollection Parameters { get; set; }
        public SqlBooleanExpression HavingClause { get; set; }
        public SqlBooleanExpression WhereClause { get; set; }
        public SqlOrderByItemCollection SortOrder { get; set; }
        public MemoryDbDataReader.ResultBatch Batch { get; set; }
        public Dictionary<string, Table> TableAliasList = new Dictionary<string, Table>();
        public List<TableColumn> GroupByFields = new List<TableColumn>();

        public readonly MemoryDbCommand Command;
        private readonly MemoryDatabase _Database;
        private readonly List<Table> _CommonTableList = new List<Table>();

        public RawData( MemoryDbCommand command, MemoryDbDataReader.ResultBatch batch = null )
        {
            Command = command;
            Parameters = command.Parameters;
            _Database = ((MemoryDbConnection)Command.Connection).GetMemoryDatabase( );
            Batch = batch;
        }

        public void AddTablesFromClause( SqlFromClause fromClause, Dictionary<string, Table> tables )
        {
            foreach (var expression in fromClause.TableExpressions)
            {
                AddTable( expression, tables );
            }
        }

        public void AddTable( SqlTableExpression expression, Dictionary<string, Table> tables )
        {
            switch (expression)
            {
                case SqlTableRefExpression tableRef:
                {
                    var name = Helper.GetAliasName(tableRef);
                    var qualifiedName = Helper.GetQualifiedName(tableRef.ObjectIdentifier);
                    if ( tables.ContainsKey( qualifiedName ) )
                    {
                        var table = tables[ qualifiedName ];
                        AddAllTableRows( table, name );
                    }
                    else if ( _Database.Views.ContainsKey( qualifiedName ) )
                    {
                        var view = _Database.Views[ qualifiedName ];
                        AddAllViewRows( view, name );
                    }
                    else if ( _CommonTableList.Any( t => t.FullName == qualifiedName ) )
                    {
                        var table = _CommonTableList.Single( t => t.FullName == qualifiedName );
                        AddAllTableRows( table, name );
                    }
                    else
                    {
                        throw new SqlInvalidObjectNameException( name );
                    }
                    break;
                }
                case SqlQualifiedJoinTableExpression joinExpression:
                {
                    var name = Helper.GetAliasName((SqlTableRefExpression)joinExpression.Left);
                    var table = tables[Helper.GetQualifiedName(((SqlTableRefExpression)joinExpression.Left).ObjectIdentifier)];
                    AddAllTableRows( table, name );
                    var nameJoin = Helper.GetAliasName((SqlTableRefExpression)joinExpression.Right);
                    var tableJoin = tables[Helper.GetQualifiedName(((SqlTableRefExpression)joinExpression.Right).ObjectIdentifier)];
                    AddAllTableJoinRows( tableJoin, nameJoin, joinExpression.OnClause );
                    break;
                }
            }
        }

        public void AddAllTableRows( Table table, string name )
        {
            if ( TableAliasList.ContainsKey( name ) == false )
            {
                TableAliasList.Add( name, table );
            }
            foreach ( var row in table.Rows )
            {
                var tableRow = new RawData.RawDataRow
                {
                    Name = name,
                    Table = table,
                    Row = row
                };
                var rows = new List<RawDataRow>( ) {tableRow};
                RawRowList.Add( rows );
            }
        }

        private void AddAllViewRows( SqlCreateAlterViewStatementBase view, string name )
        {
            var command = new MemoryDbCommand( Command.Connection, Command.Parameters, Command.Variables );
            var rawData = new RawData( command );
            var batch = new ExecuteQueryStatement( _Database, command ).Execute( _Database.Tables, rawData, (SqlQuerySpecification)view.Definition.QueryExpression );
            var identifier = view.Definition.Name;
            if ( TableAliasList.ContainsKey( name ) == false )
            {
                var table = CreateTable( name, identifier, batch, null );
                TableAliasList.Add( name, table );
            }

            var rowList = ResultBatch2RowList( TableAliasList[ name ], batch );
            RawRowList.AddRange( rowList );
        }

        private List<List<RawDataRow>> ResultBatch2RowList( Table table,
            MemoryDbDataReader.ResultBatch batch, SqlIdentifierCollection columnList = null )
        {
            var rowList = new List<List<RawDataRow>>( );

            foreach ( var row in batch.ResultRows )
            {
                var tableRow = new RawData.RawDataRow
                {
                    Name = table.Name,
                    Table = table,
                    Row = row
                };
                var rows = new List<RawDataRow>( ) {tableRow};
                rowList.Add( rows );
            }

            return rowList;
        }

        private Table CreateTable( string name, SqlObjectIdentifier identifier, MemoryDbDataReader.ResultBatch batch,
            SqlIdentifierCollection columnList )
        {
            if ( columnList != null )
            {
                if ( columnList.Count > batch.Fields.Count )
                {
                    throw new SqlInsertTooManyColumnsException( );
                }

                if ( columnList.Count < batch.Fields.Count )
                {
                    throw new SqlInsertTooManyValuesException( );
                }                
            }
            var table = ( identifier != null ) ? new Table( identifier ) : new Table( name );
            for ( int fieldIndex = 0; fieldIndex < batch.Fields.Count; fieldIndex++ )
            {
                var field = batch.Fields[ fieldIndex ];
                var dataType = field.DbType;
                if ( dataType.ToUpper( ) == "STRING" )
                {
                    dataType = "NVARCHAR(MAX)";
                }

                var columnName = columnList != null ? columnList[ fieldIndex ].Value : field.Name;
                var select = ( field as MemoryDbDataReader.ReaderFieldData )?.SelectFieldData;
                var tc = ( select as SelectDataFromColumn )?.TableColumn;
                var column = tc !=  null 
                    ? new Column( tc.Column, columnName, table.Columns.Count ) 
                    : new Column( table, columnName, dataType, table.Columns.Count );
                table.Columns.Add( column );
            }

            return table;
        }

        public void AddAllTableJoinRows(Table table, string name, SqlConditionClause onClause )
        {
            if ( TableAliasList.ContainsKey( name ) == false )
            {
                TableAliasList.Add( name, table );
            }

            var newTableRows = new List<List<RawDataRow>>( );
            var filter = HelperConditional.GetRowFilter( onClause.Expression, this );
            foreach ( var currentRawRows in RawRowList )
            {
                foreach ( var row in table.Rows )
                {
                    var newRows = new List<RawDataRow>( currentRawRows );
                    var tableRow = new RawData.RawDataRow
                    {
                        Name = name,
                        Table = table,
                        Row = row
                    };
                    newRows.Add( tableRow );
                    if ( filter.IsValid( newRows ) )
                    {
                        newTableRows.Add( newRows );
                    }
                }            
            }

            RawRowList = newTableRows;
        }

        public void AddGroupByClause( SqlGroupByClause groupByClause )
        {
            foreach ( var item in groupByClause.Items )
            {
                switch ( item )
                {
                    case SqlSimpleGroupByItem simpleItem:
                        var tableColumn = Helper.GetTableColumn( ( SqlColumnRefExpression ) simpleItem.Expression, this );
                        GroupByFields.Add( tableColumn );
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public void ExecuteWhereClause( SqlWhereClause whereClause )
        {
            foreach ( var child in whereClause.Children )
            {
                var filter = HelperConditional.GetRowFilter( ( SqlBooleanExpression ) child, this );
                RawRowList = RawRowList.Where( r => filter.IsValid( r )  ).ToList(  );
            }
        }

        public static bool RowsAreEqual( ArrayList row1, ArrayList row2 )
        {
            bool isEqual = true;
            for ( int index = 0; (index < row2.Count) && isEqual; index++ )
            {
                isEqual &= row2[ index ].Equals( row1[ index ] );
            }

            return isEqual;
        }

        public void AddTablesFromCommonTableExpressions( SqlCommonTableExpressionCollection commonTableExpressions, Dictionary<string, Table> tables )
        {
            foreach ( var commonTableExpression in commonTableExpressions )
            {
                var command = new MemoryDbCommand( Command.Connection, Command.Parameters, Command.Variables );
                var rawData = new RawData( command );
                var batch = new ExecuteQueryStatement( _Database, command ).Execute( _Database.Tables, rawData, (SqlQuerySpecification)commonTableExpression.QueryExpression );
                var name = commonTableExpression.Name.Value;
                var table = CreateTable( name, null, batch, commonTableExpression.ColumnList );
                table.Rows.AddRange( batch.ResultRows );
                _CommonTableList.Add( table );
            }           
        }
    }
}