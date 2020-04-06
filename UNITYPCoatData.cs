using System;
using System.Data;
using System.Data.SqlClient;


namespace UNITYPCoatData
{
    public class UNITYPCoatData
    {
        #region Global Variables
        public SqlConnection PCConnection { get; set; }
        public String ErrorMessage { get; set; } = string.Empty;
        #endregion
        #region Paint Product Table
        #region Colour Groups
        public Int32 ColourGroupId { get; set; } = -1;
        public String ColourGroupDescription { get; set; } = string.Empty;
        public DataTable ColourGroupRecord { get; set; } = new DataTable();
        public DataTable ColourGroupRecords { get; set; } = new DataTable();
        public Boolean Insert_Colour_Group(SqlTransaction TRNEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {

            }
            catch (Exception ex)
            {

            }

            return isSuccessful;
        }

        #endregion
        #endregion

    }
}
