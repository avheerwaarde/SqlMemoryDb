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
    class ExecuteSelectStatement
    {
        internal class RawData
        {
            public class RawDataRow
            {
                public string Name;
                public Table Table;
                public ArrayList Row;
            }
            public List<List<RawDataRow>> TableRows = new List<List<RawDataRow>>();
            public List<ISelectData> SelectFieldData = new List<ISelectData>();
            public DbParameterCollection Parameters { get; set; }
        }

        private readonly MemoryDbCommand _Command;
        private readonly MemoryDbDataReader _Reader;


        public ExecuteSelectStatement( MemoryDbCommand command, MemoryDbDataReader reader )
        {
            _Command = command;
            _Reader = reader;
        }

        public void Execute( Dictionary<string, Table> tables, SqlSelectStatement selectStatement )
        {
            var rawData = new RawData{ Parameters = _Command.Parameters };

            var expression = (SqlQuerySpecification)selectStatement.SelectSpecification.QueryExpression;
            if (expression.FromClause != null )
            {
                AddTablesFromClause( expression.FromClause, tables, rawData );
                if ( expression.WhereClause != null )
                {
                    ExecuteWhereClause( rawData, expression.WhereClause );
                }
            }

            var batch = new MemoryDbDataReader.ResultBatch(  );
            InitializeFields( batch, expression.SelectClause.Children.ToList(  ), rawData );
            AddDataToBatch( batch, rawData );
            _Reader.AddResultBatch( batch );
        }


        private void InitializeFields( MemoryDbDataReader.ResultBatch batch, List<SqlCodeObject> columns, RawData rawData )
        {
            foreach ( var column in columns )
            {
                switch ( column )
                {
                    case SqlSelectScalarExpression scalarExpression:
                        var tableColumn = Helper.GetTableColumn( ( SqlColumnRefExpression ) scalarExpression.Expression, rawData );
                        var readerField = new MemoryDbDataReader.ReaderField
                        {
                            Name = Helper.GetColumnAlias( scalarExpression ),
                            DbType = tableColumn.Column.DbDataType.ToString(),
                            NetType = tableColumn.Column.NetDataType,
                            FieldIndex = batch.Fields.Count
                        };
                        batch.Fields.Add( readerField );
                        rawData.SelectFieldData.Add( new SelectDataFromColumn( tableColumn ) );
                        break;
                }
            }

        }

        private void AddTablesFromClause(SqlFromClause fromClause, Dictionary<string, Table> tables, RawData rawData )
        {
            foreach (var expression in fromClause.TableExpressions)
            {
                switch (expression)
                {
                    case SqlTableRefExpression tableRef:
                        {
                            var name = Helper.GetAliasName(tableRef);
                            var table = tables[Helper.GetQualifiedName(tableRef.ObjectIdentifier)];
                            foreach (var row in table.Rows)
                            {
                                var tableRow = new RawData.RawDataRow
                                {
                                    Name = name,
                                    Table = table,
                                    Row = row
                                };
                                var rows = new List<RawData.RawDataRow>() { tableRow };
                                rawData.TableRows.Add(rows);
                            }
                            break;
                        }
                }
            }
        }

        private void AddDataToBatch( MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            foreach ( var row in rawData.TableRows )
            {
                var resultRow = new ArrayList();
                foreach ( var selectData in rawData.SelectFieldData )
                {
                    var value = selectData.Select( row );
                    resultRow.Add( value );
                }
                batch.ResultRows.Add( resultRow );
            }
        }

        private void ExecuteWhereClause( RawData rawData, SqlWhereClause whereClause )
        {
            foreach ( var child in whereClause.Children )
            {
                switch ( child )
                {
                    case SqlComparisonBooleanExpression compareExpression:
                        var filterComparison = new FilterRowComparison( rawData, compareExpression );
                        rawData.TableRows = rawData.TableRows.Where( r => filterComparison.IsValid( r )  ).ToList(  );
                        break;
                }
            }
        }

    }
}
