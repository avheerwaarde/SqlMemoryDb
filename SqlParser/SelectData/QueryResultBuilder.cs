using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;

namespace SqlMemoryDb.SelectData
{
    class QueryResultBuilder
    {
        private class InternalResultRow
        {
            public ArrayList ResultRow;
            public List<RawData.RawDataRow> RawRowList = new List<RawData.RawDataRow>();
            public List<List<RawData.RawDataRow>> RawAggregateRowList = new List<List<RawData.RawDataRow>>();
        }

        private readonly RawData _RawData;
        private readonly bool _DataIsDistinct;
        public QueryResultBuilder( RawData rawData, bool isDistinct = false )
        {
            _RawData = rawData;
            _DataIsDistinct = isDistinct;
        }

        public void AddData( MemoryDbDataReader.ResultBatch batch )
        {
            var internalList = new List<InternalResultRow>(  );

            var fields = batch.Fields.Cast<MemoryDbDataReader.ReaderFieldData>(  ).ToList();
            var isAggregate = fields
                .Where( s => s.SelectFieldData is ISelectDataFunction )
                .Any( s => ( ( ISelectDataFunction ) s.SelectFieldData ).IsAggregate );

            if ( _RawData.GroupByFields.Any( ) || isAggregate )
            {
                AddAggregateRowData( internalList, fields );
            }
            else
            {
                AddRowData( internalList, fields );
            }

            batch.ResultRows = AddOrderedResultRows( batch, fields, internalList, _RawData.SortOrder );
            if ( batch.MaxRowsCount.HasValue )
            {
                batch.ResultRows = batch.ResultRows.Take( batch.MaxRowsCount.Value ).ToList(  );
            }
        }

        private void AddAggregateRowData( List<InternalResultRow> internalList, List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            if ( _RawData.GroupByFields.Any( ) )
            {
                var groups = GroupRows( _RawData );
                foreach ( var group in groups )
                {
                    var aggregateRow = CreateAggregateRow( fields, @group, _RawData.GroupByFields );
                    if ( aggregateRow != null )
                    {
                        AddRowToList( internalList, aggregateRow );
                    }
                }
            }
            else
            {
                var aggregateRow = CreateAggregateRow( fields, _RawData.RawRowList, new List<TableColumn>( ) );
                if ( aggregateRow != null )
                {
                    AddRowToList( internalList, aggregateRow );
                }
            }
        }

        private List<List<List<RawData.RawDataRow>>> GroupRows( RawData rawData )
        {
            var groups = rawData.RawRowList.GroupBy( t => CreateGroupByKey( t, rawData.GroupByFields ) );
            return groups.Select( g => g.ToList(  ) ).ToList( );
        }


        private object CreateGroupByKey( List<RawData.RawDataRow> row, List<TableColumn> groupByFields )
        {
            var keys = new List<string>( );
            foreach ( var field in groupByFields )
            {
                keys.Add( new SelectDataFromColumn( field ).Select( row )?.ToString(  ) ?? "<NULL>");
            }

            return string.Join( "|", keys );
        }

        private InternalResultRow CreateAggregateRow( List<MemoryDbDataReader.ReaderFieldData> fields,
            List<List<RawData.RawDataRow>> rawRows, List<TableColumn> groupByFields )
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

            if ( _RawData.HavingClause != null )
            {
                var filter = HelperConditional.GetRowFilter( _RawData.HavingClause, _RawData );
                if ( filter.IsValid( rawRows, fields ) == false )
                {
                    return null;
                }
            }

            return new InternalResultRow {ResultRow = resultRow, RawAggregateRowList = rawRows};
        }

        private void AddRowData( List<InternalResultRow> internalList, List<MemoryDbDataReader.ReaderFieldData> fields )
        {
            foreach ( var row in _RawData.RawRowList )
            {
                var resultRow = new ArrayList();
                foreach ( var field in fields )
                {
                    var value = field.SelectFieldData.Select( row );
                    resultRow.Add( value );
                }

                if ( _RawData.WhereClause != null )
                {
                    var filter = HelperConditional.GetRowFilter( _RawData.WhereClause, _RawData );
                    if ( filter.IsValid( row ) == false )
                    {
                        continue;
                    }
                    
                }
                var internalResult = new InternalResultRow{ ResultRow = resultRow, RawRowList = row };
                AddRowToList( internalList, internalResult );
            }
        }

        private void AddRowToList( List<InternalResultRow> internalList, InternalResultRow internalResult )
        {
            if ( _DataIsDistinct == false ||
                 internalList.Any( i => RawData.RowsAreEqual( internalResult.ResultRow, i.ResultRow ) ) == false )
            {
                internalList.Add( internalResult );
            }
        }

        public List<ArrayList> AddOrderedResultRows( MemoryDbDataReader.ResultBatch batch, SqlOrderByItemCollection sortOrderList )
        {
            var fields = batch.Fields.Cast<MemoryDbDataReader.ReaderFieldData>(  ).ToList();
            var internalList = batch.ResultRows.Select( r => new InternalResultRow { ResultRow = r } ).ToList( );
            return AddOrderedResultRows( batch, fields, internalList, sortOrderList );
        }

        private List<ArrayList> AddOrderedResultRows( MemoryDbDataReader.ResultBatch batch,
            List<MemoryDbDataReader.ReaderFieldData> fields, List<InternalResultRow> internalList,
            SqlOrderByItemCollection sortOrderList )
        {
            if ( sortOrderList == null )
            {
                var rows = internalList.Select( i => i.ResultRow );
                batch.ResultRows = rows.ToList(  );
                if ( batch is MemoryDbDataReader.ResultBatchWithRawRows rawBatch1 )
                {
                    rawBatch1.RawRows = internalList.Select( i => i.RawRowList ).ToList();
                }
                return batch.ResultRows;
            }

            var isAggregateRowList = internalList.Any( i => i.RawAggregateRowList.Any(  ) );
            IOrderedEnumerable<InternalResultRow> orderedRows = null;
            foreach ( var sortOrder in sortOrderList )
            {
                if ( orderedRows == null )
                {
                    orderedRows = OrderRows( internalList, sortOrder, isAggregateRowList );
                }
                else
                {
                    orderedRows = ThenOrderRows( orderedRows, sortOrder, isAggregateRowList );
                }

            }
            batch.ResultRows = orderedRows.Select( i => i.ResultRow ).ToList();
            if ( batch is MemoryDbDataReader.ResultBatchWithRawRows rawBatch )
            {
                rawBatch.RawRows = orderedRows.Select( i => i.RawRowList ).ToList();
            }
            return batch.ResultRows;
        }

        private IOrderedEnumerable<InternalResultRow> OrderRows( List<InternalResultRow> internalList, SqlOrderByItem sortOrder, bool isAggregateRowList )
        {
            IOrderedEnumerable<InternalResultRow> orderedRows;
            if ( sortOrder.SortOrder == SqlSortOrder.Descending )
            {
                orderedRows = isAggregateRowList
                    ? internalList.OrderByDescending( r => Helper.GetValue( sortOrder.Expression, null, _RawData, r.RawAggregateRowList ) )
                    : _RawData.Batch != null 
                        ? internalList.OrderByDescending( r => Helper.GetValue( sortOrder.Expression, _RawData.Batch, r.ResultRow ) )
                        : internalList.OrderByDescending( r => Helper.GetValue( sortOrder.Expression, null, _RawData, r.RawRowList ) );
            }
            else
            {
                orderedRows = isAggregateRowList
                    ? internalList.OrderBy( r => Helper.GetValue( sortOrder.Expression, null, _RawData, r.RawAggregateRowList ) )
                    : _RawData.Batch != null 
                        ? internalList.OrderBy( r => Helper.GetValue( sortOrder.Expression, _RawData.Batch, r.ResultRow ) )
                        : internalList.OrderBy( r => Helper.GetValue( sortOrder.Expression, null, _RawData, r.RawRowList ) );
            }

            return orderedRows;
        }

        private IOrderedEnumerable<InternalResultRow> ThenOrderRows( IOrderedEnumerable<InternalResultRow> orderedRows, SqlOrderByItem sortOrder, bool isAggregateRowList )
        {
            if ( sortOrder.SortOrder == SqlSortOrder.Descending )
            {
                orderedRows = isAggregateRowList
                    ? orderedRows.ThenByDescending( r => Helper.GetValue( sortOrder.Expression, null, _RawData, r.RawAggregateRowList ) )
                    : _RawData.Batch != null 
                        ? orderedRows.ThenByDescending( r => Helper.GetValue( sortOrder.Expression, _RawData.Batch, r.ResultRow ) )
                        : orderedRows.ThenByDescending( r => Helper.GetValue( sortOrder.Expression, null, _RawData, r.RawRowList ) );
            }
            else
            {
                orderedRows = isAggregateRowList
                    ? orderedRows.ThenBy( r => Helper.GetValue( sortOrder.Expression, null, _RawData, r.RawAggregateRowList ) )
                    : _RawData.Batch != null 
                        ? orderedRows.ThenBy( r => Helper.GetValue( sortOrder.Expression, _RawData.Batch, r.ResultRow ) )
                        : orderedRows.ThenBy( r => Helper.GetValue( sortOrder.Expression, null, _RawData, r.RawRowList ) );
            }

            return orderedRows;
        }

    }
}
