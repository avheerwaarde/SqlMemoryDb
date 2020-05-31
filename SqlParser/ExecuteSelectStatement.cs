using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Info;
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
            var columns = new List<SqlCodeObject>();
            SqlFromClause from;
            RawData rawData = null;

            var parts = selectStatement.SelectSpecification.QueryExpression.Children;
            foreach ( var part in parts )
            {
                switch ( part )
                {
                    case SqlSelectClause clause: columns = part.Children.ToList(); break;
                    case SqlFromClause 
                        fromClause: from = fromClause; 
                        rawData = GetTablesFromClause( fromClause, tables ); 
                        break;
                }
            }
            var batch = new MemoryDbDataReader.ResultBatch(  );
            InitializeFields( batch, columns, rawData );
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
                        var tableColumn = GeTableColumn( scalarExpression, rawData );
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

        private TableColumn GeTableColumn( SqlSelectScalarExpression expression, RawData rawData )
        {
            var list = new List<TableColumn>( );
            var columnName = Helper.GetColumnName( ( SqlColumnRefExpression ) expression.Expression );
            foreach ( var row in rawData.TableRows )
            {
                foreach ( var tableRow in row )
                {
                    var column = tableRow.Table.Columns.FirstOrDefault( c => c.Name == columnName );
                    if ( column != null )
                    {
                        list.Add( new TableColumn{ TableName = tableRow.Name, Column = column } );
                    }
                }
            }
            return list.First();
        }

        private RawData GetTablesFromClause( SqlFromClause fromClause, Dictionary<string, Table> tables )
        {
            var rawData = new RawData(  );
            foreach ( var expression in fromClause.TableExpressions )
            {
                switch ( expression )
                {
                    case SqlTableRefExpression tableRef:
                    {
                        var name = Helper.GetAliasName( tableRef );
                        var table = tables[ Helper.GetQualifiedName( tableRef.ObjectIdentifier ) ];
                        foreach ( var row in table.Rows )
                        {
                            var tableRow = new RawData.RawDataRow
                            {
                                Name = name, 
                                Table = table, 
                                Row = row
                            };
                            var rows = new List<RawData.RawDataRow>( ) { tableRow };
                            rawData.TableRows.Add( rows );
                        }
                        break;
                    }
                }
            }

            return rawData;
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

    }
}
