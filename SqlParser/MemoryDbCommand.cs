using System;
using System.Data;
using System.Data.Common;

namespace SqlMemoryDb
{
    public class MemoryDbCommand : DbCommand
    {
        public override string CommandText { get; set; }
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        protected override DbConnection DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection { get; }
        protected override DbTransaction DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        public Decimal? LastIdentitySet;
        internal int RowsAffected;
        public MemoryDbDataReader DataReader { get; set; }

        public MemoryDbCommand( DbConnection connection )
        {
            DbConnection = connection;
            DbParameterCollection = new MemoryDataParameterCollection( );
        }


        public override void Cancel( )
        {
        }

        protected override DbParameter CreateDbParameter( )
        {
            var parameter = new MemoryDbParameter(  );
            Parameters.Add( parameter );
            return parameter;
        }

        protected override DbDataReader ExecuteDbDataReader( CommandBehavior behavior )
        {
            Prepare(  );
            var dbConnection = ( MemoryDbConnection ) Connection;
            var db = dbConnection.MemoryDatabase;

            dbConnection.InternalState = ConnectionState.Fetching;
            var returnValue = db.ExecuteSqlReader( CommandText, this, behavior );
            dbConnection.InternalState = ConnectionState.Open;
            return returnValue;
        }

        public override int ExecuteNonQuery( )
        {
            Prepare(  );
            var dbConnection = ( MemoryDbConnection ) Connection;
            var db = dbConnection.MemoryDatabase;

            RowsAffected = 0;
            dbConnection.InternalState = ConnectionState.Executing;
            db.ExecuteSqlStatement( CommandText, this );
            dbConnection.InternalState = ConnectionState.Open;
            return RowsAffected;
        }


        public override object ExecuteScalar( )
        {
            Prepare(  );
            var dbConnection = ( MemoryDbConnection ) Connection;
            var db = dbConnection.MemoryDatabase;

            RowsAffected = 0;
            dbConnection.InternalState = ConnectionState.Executing;
            var result = db.ExecuteSqlScalar( CommandText, this );
            dbConnection.InternalState = ConnectionState.Open;
            return result;
        }

        public override void Prepare( )
        {
            if ( Connection == null )
            {
                throw new InvalidOperationException("The Connection should be set.");
            }
            if ( Connection.State != ConnectionState.Open )
            {
                throw new InvalidOperationException("The Connection should be opened.");
            }
        }

    }
}
