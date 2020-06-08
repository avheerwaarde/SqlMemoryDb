using System;
using System.Data;
using System.Data.Common;

namespace SqlMemoryDb
{
    public class MemoryDbTransaction : DbTransaction
    {
        private readonly MemoryDbConnection _Connection;
        private readonly IsolationLevel _IsolationLevel;

        protected override DbConnection DbConnection { get; }
        public override IsolationLevel IsolationLevel { get; }

        public MemoryDbTransaction( MemoryDbConnection connection, IsolationLevel isolationLevel )
        {
            _Connection = connection;
            _IsolationLevel = isolationLevel;
            _Connection.MemoryDatabase.SaveSnapshotForTransaction(  );
        }

        public override void Commit( )
        {
            _Connection.MemoryDatabase.RemoveSnapshotForTransaction(  );
        }

        public override void Rollback( )
        {
            _Connection.MemoryDatabase.RestoreSnapshotForTransaction(  );
        }



    }
}