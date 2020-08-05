using System;
using System.ComponentModel.DataAnnotations;

namespace DotNetCoreSqlDb.Models
{
    public class Todo
    {
        public int ID { get; set; }
        public string Description { get; set; }

        [Display(Name = "Created Date")]
        [DataType(DataType.Date)]
        [DisplayFormat(DataFormatString = "{0:yyyy-MM-dd}", ApplyFormatInEditMode = true)]
        public DateTime CreatedDate { get; set; }

        public byte[] Version { get; set; }
        public string VersionString
        {
            get => Version != null ? Convert.ToBase64String( Version ) : String.Empty;
            set => Version = Convert.FromBase64String( value );
        }
    }
}

