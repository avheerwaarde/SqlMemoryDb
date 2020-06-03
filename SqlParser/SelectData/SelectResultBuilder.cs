using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SqlMemoryDb.Exceptions;

namespace SqlMemoryDb.SelectData
{
    class SelectResultBuilder
    {
        public void AddData( MemoryDbDataReader.ResultBatch batch, ExecuteSelectStatement.RawData rawData )
        {
            var fields = batch.Fields.Cast<MemoryDbDataReader.ReaderFieldData>(  ).ToList();
            var isAggregate = fields
                .Where( s => s.SelectFieldData is ISelectDataFunction )
                .Any( s => ( ( ISelectDataFunction ) s.SelectFieldData ).IsAggregate );

            if ( isAggregate )
            {
                var aggregateRow = CreateAggregateRow( fields, rawData.TableRows );
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

        private ArrayList CreateAggregateRow( List<MemoryDbDataReader.ReaderFieldData> fields, List<List<ExecuteSelectStatement.RawData.RawDataRow>> rawRows )
        {
            var resultRow = new ArrayList();
            foreach ( var field in fields )
            {
                var selectFunction = field.SelectFieldData as ISelectDataFunction;
                if ( selectFunction == null || selectFunction.IsAggregate == false )
                {
                    throw new SqlNoAggregateFieldException( field.Name );
                }
                var value = selectFunction.Select( rawRows );
                resultRow.Add( value );
            }

            return resultRow;
        }

        private void AddRowData( MemoryDbDataReader.ResultBatch batch, 
                                    List<MemoryDbDataReader.ReaderFieldData> fields,
                                    ExecuteSelectStatement.RawData rawData )
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
