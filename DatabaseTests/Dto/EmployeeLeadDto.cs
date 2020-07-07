using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace DatabaseTests.Dto
{
    [SuppressMessage( "ReSharper", "InconsistentNaming" )]
    public class EmployeeLeadDto
    {
        public int dept_id { get; set; }
        public string last_name { get; set; }
        public int salary { get; set; }  
        public int? next_highest_salary { get; set; }  
    }
}
