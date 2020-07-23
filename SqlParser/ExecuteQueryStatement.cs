using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using SqlMemoryDb.Exceptions;
using SqlMemoryDb.Helpers;
using SqlMemoryDb.SelectData;
using SqlParser;

namespace SqlMemoryDb
{
    class ExecuteQueryStatement
    {

        private readonly MemoryDbCommand _Command;
        private readonly List<Column> _InsertColumns;
        private SqlSelectStatement _SelectStatement = null;

        public ExecuteQueryStatement( MemoryDatabase memoryDatabase, MemoryDbCommand command, List<Column> insertColumns = null )
        {
            _Command = command;
            _InsertColumns = insertColumns;
        }

        public void Execute( Dictionary<string, Table> tables, SqlSelectStatement selectStatement, MemoryDbDataReader reader )
        {
            _SelectStatement = selectStatement;
            var rawData = new RawData( _Command );
            if ( _SelectStatement?.QueryWithClause != null )
            {
                rawData.AddTablesFromCommonTableExpressions( _SelectStatement.QueryWithClause.CommonTableExpressions, tables );
            }
            var batch = Execute( rawData, tables, selectStatement.SelectSpecification.QueryExpression, selectStatement.SelectSpecification.OrderByClause );
            if ( (_SelectStatement.SelectSpecification.QueryExpression as SqlQuerySpecification)?.IntoClause != null )
            {
                return;
            }
            reader?.AddResultBatch( batch );
        }

        public MemoryDbDataReader.ResultBatch Execute( RawData rawData, Dictionary<string, Table> tables,
            SqlQueryExpression expression,
            SqlOrderByClause orderByClause = null )
        {
            switch ( expression )
            {
                case SqlBinaryQueryExpression binaryQueryExpression:
                {
                    var commandLeft = new MemoryDbCommand( _Command );
                    var rawDataLeft = new RawData( commandLeft );
                    var batchLeft = Execute( rawDataLeft, tables, binaryQueryExpression.Left );
                    var commandRight = new MemoryDbCommand( _Command );
                    var rawDataRight = new RawData( commandRight );
                    var batchRight = Execute( rawDataRight, tables, binaryQueryExpression.Right );
                    var batch = MergeBatches( batchLeft, batchRight, binaryQueryExpression.Operator );
                    if ( orderByClause != null )
                    {
                        var resultRawData = new RawData( _Command, batch );
                        new QueryResultBuilder( resultRawData ).AddOrderedResultRows( batch, orderByClause.Items );
                    }
                    return batch;
                }
                case SqlQuerySpecification sqlQuery:
                {
                    return Execute( tables, rawData, sqlQuery, orderByClause );
                }
                default:
                    throw new NotImplementedException($"Query expressions of type {expression} are currently not implemented.");
            }
        }

        private MemoryDbDataReader.ResultBatch MergeBatches( MemoryDbDataReader.ResultBatch batchLeft, 
            MemoryDbDataReader.ResultBatch batchRight, 
            SqlBinaryQueryOperatorType @operator)
        {
            if ( batchLeft.Fields.Count != batchRight.Fields.Count )
            {
                throw new SqlUnionExpressionCount(  );
            }

            for ( int fieldIndex = 0; fieldIndex < batchLeft.Fields.Count; fieldIndex++ )
            {
                if ( batchLeft.Fields[ fieldIndex ].NetType != batchRight.Fields[ fieldIndex].NetType )
                {
                    throw new SqlConversionException( batchLeft.Fields[ fieldIndex ].DbType,  batchRight.Fields[ fieldIndex].DbType );
                }
            }
            var batch = new MemoryDbDataReader.ResultBatch{ Fields =  batchLeft.Fields };
            switch ( @operator )
            {
                case SqlBinaryQueryOperatorType.UnionAll:
                    batch.ResultRows.AddRange( batchLeft.ResultRows );
                    batch.ResultRows.AddRange( batchRight.ResultRows );
                    break;
                case SqlBinaryQueryOperatorType.Union:
                    batch.ResultRows.AddRange( batchRight.ResultRows );
                    batch.ResultRows.AddRange( batchLeft.ResultRows.Where( r => batchRight.ResultRows.Any( rr => RawData.RowsAreEqual( rr, r) == false ) ) );
                    break;
                case SqlBinaryQueryOperatorType.Intersect:
                    batch.ResultRows.AddRange( batchLeft.ResultRows.Where( r => batchRight.ResultRows.Any( rr => RawData.RowsAreEqual( rr, r) ) ) );
                    break;
                case SqlBinaryQueryOperatorType.Except:
                    batch.ResultRows.AddRange( batchLeft.ResultRows.Where( r => batchRight.ResultRows.Any( rr => RawData.RowsAreEqual( rr, r)  ) == false ) );
                    break;
                default:
                    throw new NotImplementedException( $"not implemented for type: {@operator}");
            }

            return batch;
        }


        public MemoryDbDataReader.ResultBatch Execute( Dictionary<string, Table> tables, RawData rawData,
            SqlQuerySpecification sqlQuery,
            SqlOrderByClause orderByClause = null )
        {
            if (sqlQuery.FromClause != null )
            {
                rawData.AddTablesFromClause( sqlQuery.FromClause, tables );
                if ( sqlQuery.WhereClause != null )
                {
                    rawData.ExecuteWhereClause( sqlQuery.WhereClause );
                }
            }
            else
            {
                // We do not select data from any table, so we insert an empty row to trigger a result in AddData().
                rawData.RawRowList.Add( new List<RawData.RawDataRow>() );
            }

            var batch = InitializeBatch( rawData, sqlQuery );
            if ( sqlQuery.GroupByClause != null )
            {
                rawData.AddGroupByClause( sqlQuery.GroupByClause );
            }
            
            rawData.HavingClause = sqlQuery.HavingClause?.Expression;
            rawData.SortOrder =  GetSortOrder( orderByClause, sqlQuery );

            new QueryResultBuilder( rawData, sqlQuery.SelectClause.IsDistinct ).AddData( batch );
            if ( sqlQuery.IntoClause != null )
            {
                InsertIntoTable( sqlQuery.IntoClause, tables, batch );
            }
            return batch;
        }

        private void InsertIntoTable( SqlSelectIntoClause sqlQueryIntoClause, Dictionary<string, Table> tables,
            MemoryDbDataReader.ResultBatch batch )
        {
            var connection = ( ( MemoryDbConnection ) _Command.Connection );
            bool throwExceptionIfNotFound = Helper.IsTempTable( sqlQueryIntoClause.IntoTarget ) == false;
            Table table = Helper.GetTableFromObjectId( sqlQueryIntoClause.IntoTarget, tables, connection.TempTables, throwExceptionIfNotFound ) 
                          ?? new CreateTableFromBatch(  ).ToDatabase( sqlQueryIntoClause.IntoTarget, connection, batch );
            table.Rows.AddRange( batch.ResultRows );
            _Command.RowsAffected += batch.ResultRows.Count;
        }



        private MemoryDbDataReader.ResultBatch InitializeBatch( RawData rawData, SqlQuerySpecification sqlQuery )
        {
            var batch = new MemoryDbDataReader.ResultBatch( );
            var fields = sqlQuery.SelectClause.Children.ToList( );
            if ( _InsertColumns != null )
            {
                var fieldCount = fields.Count;
                if ( _InsertColumns.Count > fieldCount )
                {
                    throw new SqlInsertTooManyColumnsException( );
                }

                if ( _InsertColumns.Count < fieldCount )
                {
                    throw new SqlInsertTooManyValuesException( );
                }
            }
            InitializeFields( batch, fields, rawData );

            return batch;
        }

        private static SqlOrderByItemCollection GetSortOrder( SqlOrderByClause orderByClause,
                                                                SqlQuerySpecification sqlQuery )
        {
            if ( sqlQuery.OrderByClause != null && orderByClause == null )
            {
                orderByClause = sqlQuery.OrderByClause;
                if ( IsPartialQuery( sqlQuery.Parent ) && IsMissingTopOffsetOrForXml( sqlQuery ) )
                {
                    throw new SqlOrderByException( );
                }
            }
            return orderByClause?.Items;
        }


        private static bool IsPartialQuery( SqlCodeObject sqlQuery )
        {
            return sqlQuery is SqlViewDefinition
                   || sqlQuery is SqlCommonTableExpression
                   || sqlQuery is SqlScalarSubQueryExpression;
        }

        private static bool IsMissingTopOffsetOrForXml( SqlQuerySpecification sqlQuery )
        {
            var select = sqlQuery.Children.FirstOrDefault( c => c is SqlSelectClause ) as SqlSelectClause;
            if ( select == null )
            {
                return true;
            }
            return select.Children.Any( c => c is SqlTopSpecification ) == false
                   && select.Children.Any( c => c is SqlForXmlClause ) == false
                   && select.Children.Any( c => c is SqlOffsetFetchClause ) == false;
        }

        public void InitializeFields( MemoryDbDataReader.ResultBatch batch, List<SqlCodeObject> columns, RawData rawData )
        {
            foreach ( var column in columns )
            {
                if ( column is SqlSelectScalarExpression scalarExpression )
                {
                    var name = Helper.GetColumnAlias( scalarExpression );
                    InitializeField( batch, rawData, scalarExpression.Expression, name );
                }
                else if ( column is SqlTopSpecification topSpecification )
                {
                    batch.MaxRowsCount = int.Parse( topSpecification.Value.Sql );
                }
                else if ( column is SqlSelectStarExpression selectStarExpression )
                {
                    foreach ( var tableAlias in rawData.TableAliasList )
                    {
                        foreach ( var valueColumn in tableAlias.Value.Columns )
                        {
                            AddFieldFromColumn( valueColumn, tableAlias.Key, batch, rawData );
                        }
                    }
                }
                else
                {
                    throw new NotImplementedException( $"Not implemented column specification {column}" );
                }
            }
        }

        private void InitializeField( MemoryDbDataReader.ResultBatch batch, RawData rawData, SqlScalarExpression scalarExpression, string name )
        {
            switch ( scalarExpression )
            {
                case SqlGlobalScalarVariableRefExpression globalRef:
                    AddFieldFromGlobalVariable( globalRef, name, batch, rawData );
                    break;
                case SqlScalarVariableRefExpression variableRef:
                    AddFieldFromVariable( variableRef, name, batch, rawData );
                    break;
                case SqlScalarRefExpression scalarRef:
                    AddFieldFromColumn( ( SqlObjectIdentifier ) scalarRef.MultipartIdentifier, name, batch, rawData );
                    break;
                case SqlLiteralExpression literalExpression:
                    AddFieldFromLiteral( literalExpression, name, batch, rawData );
                    break;
                case SqlBuiltinScalarFunctionCallExpression functionCall:
                    AddFieldForFunctionCall( functionCall, name, batch, rawData );
                    break;
                case SqlNullScalarExpression nullScalarExpression:
                    AddFieldForNullScalarExpression( nullScalarExpression, name, batch, rawData );
                    break;
                case SqlSearchedCaseExpression caseExpression:
                    AddFieldFromCaseExpression( caseExpression, name, batch, rawData );
                    break;
                case SqlBinaryScalarExpression binaryExpression:
                    AddFieldFromBinaryExpression( binaryExpression, name, batch, rawData );
                    break;
                case SqlUnaryScalarExpression unaryScalarExpression:
                    InitializeField( batch, rawData, unaryScalarExpression.Expression, name );
                    break;
                default:
                    throw new NotImplementedException(
                        $"Currently expression of type {scalarExpression.GetType( )} is not implemented" );
            }
        }

        private void AddFieldFromGlobalVariable( SqlGlobalScalarVariableRefExpression globalRef, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataBuilder(  ).BuildGlobalVariable( globalRef.VariableName, rawData );
            AddFieldFromSelectData( name, batch, select );
        }

        private void AddFieldFromVariable( SqlScalarVariableRefExpression variableRef, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataFromVariables( variableRef, _Command );
            AddFieldFromSelectData( name, batch, select );
        }

        private void AddFieldFromColumn( SqlObjectIdentifier objectIdentifier, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var tableColumn = Helper.GetTableColumn( objectIdentifier, rawData );
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = name,
                DbType = tableColumn.Column.DbDataType.ToString(),
                NetType = tableColumn.Column.NetDataType,
                FieldIndex = batch.Fields.Count,
                SelectFieldData = new SelectDataFromColumn( tableColumn, rawData )
            };
            batch.Fields.Add( readerField );
        }


        private void AddFieldFromColumn( Column column, string tableName, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = column.Name,
                DbType = column.DbDataType.ToString(),
                NetType = column.NetDataType,
                FieldIndex = batch.Fields.Count,
                SelectFieldData = new SelectDataFromColumn( new TableColumn{ Column = column, TableName = tableName }, rawData )
            };
            batch.Fields.Add( readerField );
        }


        private void AddFieldFromLiteral( SqlLiteralExpression literalExpression, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            if ( _InsertColumns != null )
            {
                var column = _InsertColumns[ batch.Fields.Count ];
                var value = Helper.GetValueFromString( column.NetDataType, literalExpression.Value );
                var readerField = new MemoryDbDataReader.ReaderFieldData
                {
                    Name = column.Name,
                    DbType = column.DbDataType.ToString(),
                    NetType = column.NetDataType,
                    FieldIndex = batch.Fields.Count
                };                
                readerField.SelectFieldData = new SelectDataFromObject( value, readerField.DbType );
                batch.Fields.Add( readerField );
            }
            else
            {
                if ( literalExpression.Type == LiteralValueType.Null )
                {
                    var nullField = Helper.BuildFieldFromNullValue( name, batch.Fields.Count );
                    batch.Fields.Add( nullField );
                    return;
                }
                var readerField = Helper.BuildFieldFromLiteral( literalExpression.Type, name, batch.Fields.Count );
                var value = Helper.GetValueFromString( readerField.NetType, literalExpression.Value );
                readerField.SelectFieldData = new SelectDataFromObject( value, readerField.DbType );
                batch.Fields.Add( readerField );
            }
        }

        private void AddFieldForFunctionCall( SqlBuiltinScalarFunctionCallExpression functionCall, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataBuilder(  ).Build( functionCall, rawData );
            AddFieldFromSelectData( name, batch, select );
        }

        private void AddFieldFromCaseExpression( SqlSearchedCaseExpression caseExpression, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataFromCaseExpression( caseExpression, rawData );
            AddFieldFromSelectData( name, batch, select );
        }


        private static void AddFieldFromSelectData( string name, MemoryDbDataReader.ResultBatch batch, ISelectData select )
        {
            var readerField = new MemoryDbDataReader.ReaderFieldData
            {
                Name = name,
                DbType = select.DbType.ToString(),
                NetType = select.ReturnType,
                FieldIndex = batch.Fields.Count,
                SelectFieldData = select
            };
            batch.Fields.Add( readerField );
        }

        private void AddFieldForNullScalarExpression( SqlNullScalarExpression expression, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataFromNullScalarExpression( expression, rawData );
            AddFieldFromSelectData( name, batch, select );
        }

        private void AddFieldFromBinaryExpression( SqlBinaryScalarExpression expression, string name, MemoryDbDataReader.ResultBatch batch, RawData rawData )
        {
            var select = new SelectDataFromBinaryScalarExpression( expression, rawData );
            AddFieldFromSelectData( name, batch, select );
        }

    }
}
