using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseTests.Dto
{
    public class OrderDetailsWithCaseDto
    {
        public int OrderId { get; set; }
        public int Quantity { get; set; }
        public string QuantityText { get; set; }
    }
}
