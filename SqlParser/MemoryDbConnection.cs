using System;
using System.Data;
using System.Data.Common;

namespace SqlMemoryDb
{
    public class MemoryDbConnection : DbConnection
    {
        internal string InternalDatabaseName;
        internal ConnectionState InternalState;
        private const string _DatabaseServerVersion = "0.01";
        private static readonly MemoryDatabase _MemoryDatabase = new MemoryDatabase(  );

        public override string ConnectionString { get; set; }
        public override int ConnectionTimeout => -1;
        public override string Database => InternalDatabaseName;
        public override string DataSource => InternalDatabaseName;
        public override string ServerVersion => _DatabaseServerVersion;
        public override ConnectionState State => InternalState;

        internal MemoryDatabase MemoryDatabase => _MemoryDatabase;

        public MemoryDbConnection( )
        {
            InternalState = ConnectionState.Closed;
        }

        public static MemoryDatabase GetMemoryDatabase( )
        {
            return _MemoryDatabase;
        }

        protected override DbTransaction BeginDbTransaction( IsolationLevel isolationLevel )
        {
            throw new NotImplementedException( );
        }


        public override void Close( )
        {
            InternalState = ConnectionState.Closed;
        }

        protected override DbCommand CreateDbCommand( )
        {
            return new MemoryDbCommand( this );
        }

        public override void Open( )
        {
            if ( State != ConnectionState.Closed )
            {
                throw new InvalidOperationException( $"The connection is already open. Current state = '{State}'");
            }

            InternalState = ConnectionState.Open;
            InternalDatabaseName = "Memory";
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        public override DataTable GetSchema( string collectionName )
        {
            switch ( collectionName.ToLower() )
            {
                case "tables": return GetSchemaTables( );
                case "columns": return GetSchemaColumns( );
                default:
                    throw new NotSupportedException();
            }
        }

        private DataTable GetSchemaTables( )
        {
            var dataTable = new DataTable();
            dataTable.Columns.Add( "TABLE_CATALOG" );
            dataTable.Columns.Add( "TABLE_SCHEMA" );
            dataTable.Columns.Add( "TABLE_NAME" );
            dataTable.Columns.Add( "TABLE_TYPE" );

            foreach ( var table in _MemoryDatabase.Tables )
            {
                var row = dataTable.NewRow( );
                row[ "TABLE_CATALOG" ] = "Memory";
                row[ "TABLE_SCHEMA" ] = "dbo";
                row[ "TABLE_NAME" ] = table.Value.Name;
                row[ "TABLE_TYPE" ] = "BASE TABLE";
                dataTable.Rows.Add( row );
            }
            return dataTable;
        }

        private DataTable GetSchemaColumns( )
        {
            var dataTable = new DataTable();
            dataTable.Columns.Add( "TABLE_CATALOG" );
            dataTable.Columns.Add( "TABLE_SCHEMA" );
            dataTable.Columns.Add( "TABLE_NAME" );
            dataTable.Columns.Add( "COLUMN_NAME" );
            dataTable.Columns.Add( "ORDINAL_POSITION" );
            dataTable.Columns.Add( "COLUMN_DEFAULT" );
            dataTable.Columns.Add( "IS_NULLABLE" );
            dataTable.Columns.Add( "DATA_TYPE" );
            dataTable.Columns.Add( "CHARACTER_MAXIMUM_LENGTH" );
            dataTable.Columns.Add( "CHARACTER_OCTET_LENGTH" );
            dataTable.Columns.Add( "NUMERIC_PRECISION" );
            dataTable.Columns.Add( "NUMERIC_PRECISION_RADIX" );
            dataTable.Columns.Add( "NUMERIC_SCALE" );
            dataTable.Columns.Add( "DATETIME_PRECISION" );
            dataTable.Columns.Add( "CHARACTER_SET_CATALOG" );
            dataTable.Columns.Add( "CHARACTER_SET_SCHEMA" );
            dataTable.Columns.Add( "CHARACTER_SET_NAME" );
            dataTable.Columns.Add( "COLLATION_CATALOG" );
            dataTable.Columns.Add( "IS_SPARSE" );
            dataTable.Columns.Add( "IS_COLUMN_SET" );
            dataTable.Columns.Add( "IS_FILESTREAM" );

            foreach ( var table in _MemoryDatabase.Tables.Values )
            {
                var order = 0;
                foreach ( var column in table.Columns )
                {
                    var row = dataTable.NewRow( );
                    row[ "TABLE_CATALOG" ] = "Memory";
                    row[ "TABLE_SCHEMA" ] = "dbo";
                    row[ "TABLE_NAME" ] = table.Name;
                    row[ "COLUMN_NAME" ] = column.Name;
                    row[ "ORDINAL_POSITION" ] = ++order;
                    row[ "COLUMN_DEFAULT" ] = column.DefaultValue;
                    row[ "IS_NULLABLE" ] = column.IsNullable;
                    row[ "DATA_TYPE" ] = column.DbDataType.ToString();

                    dataTable.Rows.Add( row );
                }
            }
            return dataTable;
        }
    }
}
