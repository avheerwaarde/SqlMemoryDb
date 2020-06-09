using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseTests.Dto
{
    public class DbScriptRunDto
    {
        public int Id { get; set; }
        public Guid ScriptId { get; set; }
        public DateTime DateInserted { get; set; }
    }
}
