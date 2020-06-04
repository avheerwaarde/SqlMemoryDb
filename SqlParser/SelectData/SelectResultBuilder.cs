using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using SqlMemoryDb.Exceptions;

namespace SqlMemoryDb.SelectData
{
    class SelectResultBuilder
    {
        public void AddData( MemoryDbDataReader.ResultBatch batch, ExecuteQueryStatement.RawData rawData )
        {
            var fields = batch.Fields.Cast<MemoryDbDataReader.ReaderFieldData>(  ).ToList();
            var isAggregate = fields
                .Where( s => s.SelectFieldData is ISelectDataFunction )
                .Any( s => ( ( ISelectDataFunction ) s.SelectFieldData ).IsAggregate );

            if ( rawData.GroupByFields.Any() )
            {
                var groups = GroupRows( rawData );
                foreach ( var group in groups )
                {
                    var aggregateRow = CreateAggregateRow( fields, group, rawData.GroupByFields );
                    batch.ResultRows.Add( aggregateRow );
                }
            }
            else if ( isAggregate )
            {
                var aggregateRow = CreateAggregateRow( fields, rawData.TableRows, new List<TableColumn>() );
                batch.ResultRows.Add( aggregateRow );
            }
            else
            {
                AddRowData( batch, fields, rawData );
            }

            if ( batch.MaxRowsCount.HasValue )
            {
                batch.ResultRows = batch.ResultRows.Take( batch.MaxRowsCount.Value ).ToList(  );
            }
        }

        private List<List<List<ExecuteQueryStatement.RawData.RawDataRow>>> GroupRows( ExecuteQueryStatement.RawData rawData )
        {
            var groups = rawData.TableRows.GroupBy( t => CreateGroupByKey( t, rawData.GroupByFields ) );
            return groups.Select( g => g.ToList(  ) ).ToList( );
        }

        private object CreateGroupByKey( List<ExecuteQueryStatement.RawData.RawDataRow> row, List<TableColumn> groupByFields )
        {
            var keys = new List<string>( );
            foreach ( var field in groupByFields )
            {
                keys.Add( new SelectDataFromColumn( field ).Select( row ).ToString(  ) );
            }

            return string.Join( "|", keys );
        }

        private ArrayList CreateAggregateRow( List<MemoryDbDataReader.ReaderFieldData> fields,
            List<List<ExecuteQueryStatement.RawData.RawDataRow>> rawRows, List<TableColumn> groupByFields )
        {
            var resultRow = new ArrayList();
            foreach ( var field in fields )
            {
                object value;
                var selectFunction = field.SelectFieldData as ISelectDataFunction;
                if ( selectFunction == null || selectFunction.IsAggregate == false )
                {
                    if ( field.SelectFieldData is SelectDataFromColumn selectColumn
                         && groupByFields.Any( g => g.TableName == selectColumn.TableColumn.TableName 
                                                && g.Column.Name == selectColumn.TableColumn.Column.Name ) )
                    {
                        value = selectColumn.Select( rawRows.First() );
                    }
                    else
                    {
                        throw new SqlNoAggregateFieldException( field.Name );
                    }
                }
                else
                {
                    value = selectFunction.Select( rawRows );
                }
                resultRow.Add( value );
            }

            return resultRow;
        }

        private void AddRowData( MemoryDbDataReader.ResultBatch batch, 
                                    List<MemoryDbDataReader.ReaderFieldData> fields,
                                    ExecuteQueryStatement.RawData rawData )
        {
            foreach ( var row in rawData.TableRows )
            {
                var resultRow = new ArrayList();
                foreach ( var field in fields )
                {
                    var value = field.SelectFieldData.Select( row );
                    resultRow.Add( value );
                }
                batch.ResultRows.Add( resultRow );
            }
        }

    }
}
