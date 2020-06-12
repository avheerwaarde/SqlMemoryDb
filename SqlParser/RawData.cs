using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
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

        public Dictionary<string,Table> TableAliasList = new Dictionary<string, Table>();
        public List<TableColumn> GroupByFields = new List<TableColumn>();

        public readonly MemoryDbCommand Command;

        public RawData( MemoryDbCommand command )
        {
            Command = command;
            Parameters = command.Parameters;
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
                    var table = tables[Helper.GetQualifiedName(tableRef.ObjectIdentifier)];
                    AddAllTableRows( table, name );
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