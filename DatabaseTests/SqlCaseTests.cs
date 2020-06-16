using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DatabaseTests
{
    [TestClass]
    public class SqlCaseTests
    {
        [TestInitialize]
        public void InitDb( )
        {
            SqlScripts.InitNorthWindDatabase(  );
        }

        [TestMethod]
        public void Case_Simple_Succeeds( )
        {
        }
    }
}
