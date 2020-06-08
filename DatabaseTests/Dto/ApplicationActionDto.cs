using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseTests.Dto
{ 
    public class ApplicationActionDto
    {
        public int? Id { get; set; }
        public string Name { get; set; }
        public string Action { get; set; }
        public int? Order { get; set; }
        public int? fk_application { get; set; }
    }
}
