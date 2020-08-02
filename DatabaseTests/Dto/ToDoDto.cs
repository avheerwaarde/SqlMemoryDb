using System;

namespace DatabaseTests.Dto
{
    public class ToDoDto
    {
        public int ID { get; set; }
        public string Description { get; set; }
        public DateTime CreatedDate { get; set; }
        public byte[] Version { get; set; }
    }
}
