using System;
using System.Collections.Generic;
using System.Text;

namespace DatabaseTests.Dto
{
    public class CalculationDto
    {
        public int ProductID { get; set; }
        public decimal Cost { get; set; }
        public decimal Margin { get; set; }
        public decimal Tax { get; set; }
        public decimal FinalCost { get; set; }

    }
}
