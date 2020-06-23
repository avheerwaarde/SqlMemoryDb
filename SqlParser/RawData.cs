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
        public SqlOrderByItemCollection SortOrder { get; set; }
        public MemoryDbDataReader.ResultBatch Batch { get; set; }
        public Dictionary<string,Table> TableAliasList = new Dictionary<string, Table>();
        public List<TableColumn> GroupByFields = new List<TableColumn>();

        public readonly MemoryDbCommand Command;
        private readonly MemoryDatabase _Database;

        public RawData( MemoryDbCommand command, MemoryDbDataReader.ResultBatch batch = null )
        {
            Command = command;
            Parameters = command.Parameters;
            _Database = MemoryDbConnection.GetMemoryDatabase( );
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
                var rows = new List<RawData.RawDataRow>( ) {tableRow};
                RawRowList.Add( rows );
            }
        }

        private void AddAllViewRows( SqlCreateAlterViewStatementBase view, string name )
        {
            var command = new MemoryDbCommand( Command.Connection, Command.Parameters, Command.Variables );
            var rawData = new RawData( command );
            var reader = new MemoryDbDataReader( CommandBehavior.SingleResult );
            var batch = new ExecuteQueryStatement( _Database, command, reader ).Execute( _Database.Tables, rawData, (SqlQuerySpecification)view.Definition.QueryExpression );
            var identifier = view.Definition.Name;
            var rowList = ResultBatch2RowList( name, identifier, batch );
            RawRowList.AddRange( rowList );
        }

        private List<List<RawDataRow>> ResultBatch2RowList( string name, SqlObjectIdentifier identifier, MemoryDbDataReader.ResultBatch batch )
        {
            var rowList = new List<List<RawDataRow>>( );
            var table = new Table( identifier );
            foreach ( var field in batch.Fields )
            {
                var dataType = field.DbType;
                if ( dataType.ToUpper() == "STRING" )
                {
                    dataType = "NVARCHAR(MAX)";
                }
                var column = new Column( table, field.Name, dataType, table.Columns.Count );
                table.Columns.Add( column );
            }
            if ( TableAliasList.ContainsKey( name ) == false )
            {
                TableAliasList.Add( name, table );
            }

            foreach ( var row in batch.ResultRows )
            {
                var tableRow = new RawData.RawDataRow
                {
                    Name = name,
                    Table = table,
                    Row = row
                };
                var rows = new List<RawData.RawDataRow>( ) {tableRow};
                rowList.Add( rows );
            }

            return rowList;
        }

        public void AddAllTableJoinRows(Table table, string name, SqlConditionClause onClause )
        {
            if ( TableAliasList.ContainsKey( name ) == false )
            {
                TableAliasList.Add( name, table );
            }

            var newTableRows = new List<List<RawData.RawDataRow>>( );
            var filter = HelperConditional.GetRowFilter( onClause.Expression, this );
            foreach ( var currentRawRows in RawRowList )
            {
                foreach ( var row in table.Rows )
                {
                    var newRows = new List<RawData.RawDataRow>( currentRawRows );
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

    }
}