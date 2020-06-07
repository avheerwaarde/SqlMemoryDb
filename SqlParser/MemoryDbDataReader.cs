using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using SqlMemoryDb.SelectData;

namespace SqlMemoryDb
{
    public class MemoryDbDataReader : DbDataReader
    {
        public class ReaderField
        {
            public string Name;
            public int FieldIndex;
            public Type NetType;
            public string DbType;
        }

        internal class ReaderFieldData : ReaderField
        {
            public ISelectData SelectFieldData;
        }

        public class ResultBatch
        {
            public List<ArrayList> ResultRows = new List<ArrayList>();
            public List<ReaderField> Fields = new List<ReaderField>();
            public int? MaxRowsCount;
        }

        private readonly CommandBehavior _CommandBehavior;
        private readonly List<ResultBatch> _ResultBatches = new List<ResultBatch>();
        private int _CurrentBatchIndex = -1;
        private ResultBatch _CurrentBatch;
        private int _CurrentRowIndex = -1;
        private ArrayList _CurrentRow;

        public MemoryDbDataReader( CommandBehavior behavior )
        {
            _CommandBehavior = behavior;
        }

        public override bool GetBoolean( int ordinal )
        {
            return ( bool )_CurrentRow[ ordinal ];
        }

        public override byte GetByte( int ordinal )
        {
            return ( byte )_CurrentRow[ ordinal ];
        }

        public override long GetBytes( int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length )
        {
            throw new NotImplementedException( );
        }

        public override char GetChar( int ordinal )
        {
            return ( char )_CurrentRow[ ordinal ];
        }

        public override long GetChars( int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length )
        {
            throw new NotImplementedException( );
        }

        public override DateTime GetDateTime( int ordinal )
        {
            return ( DateTime )_CurrentRow[ ordinal ];
        }

        public override decimal GetDecimal( int ordinal )
        {
            return ( decimal )_CurrentRow[ ordinal ];
        }

        public override double GetDouble( int ordinal )
        {
            return ( double )_CurrentRow[ ordinal ];
        }

        public override float GetFloat( int ordinal )
        {
            return ( float )_CurrentRow[ ordinal ];
        }

        public override Guid GetGuid( int ordinal )
        {
            return ( Guid )_CurrentRow[ ordinal ];
        }

        public override short GetInt16( int ordinal )
        {
            return ( short )_CurrentRow[ ordinal ];
        }

        public override int GetInt32( int ordinal )
        {
            return ( int )_CurrentRow[ ordinal ];
        }

        public override long GetInt64( int ordinal )
        {
            return ( long )_CurrentRow[ ordinal ];
        }

        public override string GetString( int ordinal )
        {
            return ( string )_CurrentRow[ ordinal ];
        }

        public override object GetValue( int ordinal )
        {
            return _CurrentRow[ ordinal ];
        }

        public override Type GetFieldType( int ordinal )
        {
            return _CurrentBatch.Fields[ ordinal ].NetType;
        }

        public override string GetDataTypeName( int ordinal )
        {
            return _CurrentBatch.Fields[ ordinal ].DbType;
        }

        public override string GetName( int ordinal )
        {
            return _CurrentBatch.Fields[ ordinal ].Name;
        }

        public override int GetOrdinal( string name )
        {
            return _CurrentBatch.Fields.Single( f => f.Name == name ).FieldIndex;
        }

        public override int GetValues( object[] values )
        {
            for ( int index = 0; index < _CurrentRow.Count; index++ )
            {
                values[ index ] = _CurrentRow[ index ];
            }
            return _CurrentRow.Count;
        }

        public override bool IsDBNull( int ordinal )
        {
            return _CurrentRow[ ordinal ] == null;
        }


        public override object this[ int ordinal ] => GetValue( ordinal );

        public override object this[ string name ] => GetValue( GetOrdinal( name ) );

        public override int FieldCount => _ResultBatches[ _CurrentBatchIndex ].Fields.Count;
        public override int RecordsAffected => -1;
        public override bool HasRows => _ResultBatches.Any( b => b.ResultRows.Any() );
        public override bool IsClosed => false;
        public override int Depth => 0;

        public override bool NextResult( )
        {
            if ( ++_CurrentBatchIndex < _ResultBatches.Count )
            {
                _CurrentBatch = _ResultBatches[ _CurrentBatchIndex ];
                _CurrentRowIndex = -1;
                return true;
            }
            return false;
        }

        public override bool Read( )
        {
            if ( _CurrentBatch == null )
            {
                if ( NextResult() == false )
                {
                    return false;
                }
            }

            _CurrentRow = ( ++_CurrentRowIndex < _CurrentBatch.ResultRows.Count )
                ? _CurrentBatch.ResultRows[ _CurrentRowIndex ]
                : null;
            return _CurrentRow != null;
        }

        public override IEnumerator GetEnumerator( ) => _CurrentBatch.ResultRows.GetEnumerator(  );

        public void AddResultBatch( ResultBatch batch )
        {
            _ResultBatches.Add( batch );
            if ( _CurrentBatch == null && _CurrentBatchIndex == -1 )
            {
                _CurrentBatchIndex = 0;
                _CurrentBatch = _ResultBatches[ _CurrentBatchIndex ];
            }
        }
    }
}
