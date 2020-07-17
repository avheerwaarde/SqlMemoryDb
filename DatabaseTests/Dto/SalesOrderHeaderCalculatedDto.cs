using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseTests.Dto
{
    public class SalesOrderHeaderCalculatedDto
    {
        public int? SalesOrderID { get; set; }
        public string SalesOrderNumber { get; set; }
        public decimal SubTotal { get; set; }
        public decimal TaxAmt { get; set; }
        public decimal Freight { get; set; }
        public decimal TotalDue { get; set; }
        public DateTime OrderDate { get; set; }
        public int Year { get; set; }
    }
}
