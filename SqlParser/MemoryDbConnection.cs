using System;
using System.Data;
using System.Data.Common;

namespace SqlMemoryDb
{
    public class MemoryDbConnection : DbConnection
    {
        internal string InternalDatabaseName;
        internal ConnectionState InternalState;
        private const string _DatabaseServerVersion = "0.10.5";
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
            if ( State != ConnectionState.Open )
            {
                throw new InvalidOperationException("The Connection should be opened.");
            }
            return new MemoryDbTransaction( this, isolationLevel );
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
            return GetSchema( collectionName, new string[] { } );
        }

        public override DataTable GetSchema( string collectionName, string[] restrictionValues )
        {
            switch ( collectionName.ToLower() )
            {
                case "tables": return GetSchemaTables( restrictionValues );
                case "columns": return GetSchemaColumns( restrictionValues );
                default:
                    throw new NotSupportedException();
            }
        }

        private DataTable GetSchemaTables( string[] restrictionValues )
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
                row[ "TABLE_SCHEMA" ] = table.Value.SchemaName;
                row[ "TABLE_NAME" ] = table.Value.Name;
                row[ "TABLE_TYPE" ] = "BASE TABLE";
                if ( ShouldAdd( row, restrictionValues ) )
                {
                    dataTable.Rows.Add( row );
                }
            }
            return dataTable;
        }

        private bool ShouldAdd( DataRow row, string[] restrictionValues )
        {
            for ( int index = 0; index < restrictionValues.Length; index++ )
            {
                if ( string.IsNullOrWhiteSpace(restrictionValues[index]) == false 
                     && row[index] != null
                     && restrictionValues[index] == row[index].ToString(  ))
                {
                    return false;
                }
            }
            return true;
        }

        private DataTable GetSchemaColumns( string[] restrictionValues )
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
                foreach ( var column in table.Columns )
                {
                    var row = dataTable.NewRow( );
                    row[ "TABLE_CATALOG" ] = "Memory";
                    row[ "TABLE_SCHEMA" ] = table.SchemaName;
                    row[ "TABLE_NAME" ] = table.Name;
                    row[ "COLUMN_NAME" ] = column.Name;
                    row[ "ORDINAL_POSITION" ] = column.Order;
                    row[ "COLUMN_DEFAULT" ] = column.DefaultValue;
                    row[ "IS_NULLABLE" ] = column.IsNullable;
                    row[ "DATA_TYPE" ] = column.DbDataType.ToString();
                    if ( column.NetDataType == typeof(string) )
                    {
                        row[ "CHARACTER_MAXIMUM_LENGTH" ] = column.Size;
                        row[ "CHARACTER_OCTET_LENGTH" ] = column.Size == -1 ? -1 : column.Size * 2;
                        row[ "CHARACTER_SET_NAME" ] =
                            column.DbDataType == DbType.AnsiString || column.DbDataType == DbType.AnsiStringFixedLength
                                ? "iso_1"
                                : "UNICODE";
                    }

                    if ( column.Precision != 0 )
                    {
                        row[ "NUMERIC_PRECISION" ] = column.Precision;
                        row[ "NUMERIC_PRECISION_RADIX" ] = 10;
                        row[ "NUMERIC_SCALE" ] = column.Scale;
                    }
                    if ( ShouldAdd( row, restrictionValues ) )
                    {
                        dataTable.Rows.Add( row );
                    }
                }
            }
            return dataTable;
        }
    }
}
