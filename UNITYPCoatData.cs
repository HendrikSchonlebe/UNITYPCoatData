using System;
using System.Data;

namespace UNITYPCoatData
{
    public class UNITYPCoatData
    {
        #region Paint Product Table
        #region Colour Groups
        public Int32 ColourGroupId { get; set; } = -1;
        public String ColourGroupDescription { get; set; } = string.Empty;
        public DataTable ColourGroupRecord { get; set; } = new DataTable();
        public DataTable ColourGroupRecords { get; set; } = new DataTable();

        #endregion
        #endregion

    }
}
