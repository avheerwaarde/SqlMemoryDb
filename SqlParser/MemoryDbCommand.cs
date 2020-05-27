using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

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
            var parameter = new MemoryDbDataParameter(  );
            Parameters.Add( parameter );
            return parameter;
        }

        protected override DbDataReader ExecuteDbDataReader( CommandBehavior behavior )
        {
            throw new NotImplementedException( );
        }

        public override int ExecuteNonQuery( )
        {
            var dbConnection = ( MemoryDbConnection ) Connection;
            if ( dbConnection == null )
            {
                throw new InvalidOperationException( "The Connection should be set" );
            }
            if ( dbConnection.State != ConnectionState.Open )
            {
                throw new InvalidOperationException( "The state of the connection should be Open" );
            }

            var db = dbConnection.MemoryDatabase;

            dbConnection.InternalState = ConnectionState.Executing;
            var returnValue = db.ExecuteSqlStatement( CommandText );
            dbConnection.InternalState = ConnectionState.Open;
            return returnValue;
        }


        public override object ExecuteScalar( )
        {
            throw new NotImplementedException( );
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
