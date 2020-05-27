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

    }
}
