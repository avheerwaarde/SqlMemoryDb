using System;
using System.Collections.Generic;
using System.Text;

namespace SqlMemoryDb
{
    internal class RawTableJoinRow : List<RawTableRow>
    {
        public RawTableJoinRow() { }
        public RawTableJoinRow( List<RawTableRow> source ) { AddRange( source ); }
    }
}
