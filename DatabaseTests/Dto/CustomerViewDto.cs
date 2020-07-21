using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseTests.Dto
{
    public class CustomerViewDto
    {
        public string CustormerID { get; set; }
        public string CompanyName { get; set; }
        public string ContactName { get; set; }
        public int NumberOfOrders { get; set; }
    }
}
