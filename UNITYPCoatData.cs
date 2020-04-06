using System;
using System.Data;
using System.Data.SqlClient;


namespace UNITYPCoatData
{
    public class UNITYPCoatData
    {
        private const String INSERT_ERROR = "More than one Record would be Inserted !";
        private const String DELETE_ERROR = "More than one Record would be Deleted !";
        private const String UPDATE_ERROR = "More than one Record would be Updated !";
        private const String GET_ERROR = "Record not Found !";

        #region Global Variables
        public SqlConnection PCConnection { get; set; }
        public String ErrorMessage { get; set; } = string.Empty;
        #endregion
        #region Common Functions
        public String Fix_Hyphon(String strIn)
        {
            String strOut = "";
            int i;

            for (i = 0; i <= strIn.Length - 1; i++)
            {
                if (strIn.Substring(i, 1) == "'")
                    strOut += "''";
                else
                    strOut += strIn.Substring(i, 1);
            }

            return strOut;
        }
        #endregion
        #region Paint Product Related Tables
        #region Colour Groups
        public Int32 ColourGroupId { get; set; } = -1;
        public String ColourGroupDescription { get; set; } = string.Empty;
        public DataTable ColourGroupRecord { get; set; } = new DataTable();
        public DataTable ColourGroupRecords { get; set; } = new DataTable();
        public Boolean Create_Colour_Group_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE ColourGroups (";
                StrSQL += "ColourGroupId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "ColourGroupDescription nvarchar(50))";

                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Colour Group  Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Colour_Group(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO ColourGroups (ColourGroupDescription) VALUES ('" + Fix_Hyphon(ColourGroupDescription) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Colour Group - " + INSERT_ERROR;
                }

            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Colour Group - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Colour_Group(Int32 GroupId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            ColourGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM ColourGroups WHERE ColourGroupId = " + GroupId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    ColourGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_Colour_Group();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Colour Group - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Colour Group - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Colour_Group(Int32 GroupId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;
            
            ErrorMessage = string.Empty;
            ColourGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM ColourGroups WHERE ColourGroupId = " + GroupId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    ColourGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_Colour_Group();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Colour Group - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Colour Group - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Colour_Group()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                ColourGroupId = Convert.ToInt32(ColourGroupRecord.Rows[0]["ColourGroupId"]);
                ColourGroupDescription = ColourGroupRecord.Rows[0]["ColourGroupDescription"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Colour Group - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Colour_Group(Int32 GroupId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE ColourGroups SET ";
                
                if (ColourGroupDescription != ColourGroupRecord.Rows[0]["ColourGroupDescription"].ToString())
                {
                    StrSQL += "ColourGroupDescription = '" + Fix_Hyphon(ColourGroupDescription) + "' ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "WHERE ColourGroupId = " + GroupId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Colour Group - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Colour Group - " + ex.Message + " !";
            }

            return isSuccessful;

        }
        public Boolean Delete_Colour_Group(Int32 GroupId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM ColourGroups WHERE ColourGroupId = " + GroupId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Colour Group - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Colour Group - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Colour_Groups()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            ColourGroupRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM ColourGroups ORDER BY ColourGroupDescription";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    ColourGroupRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Colour Groups - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        #endregion
        #region Paint Finishes
        public Int32 PaintFinishId { get; set; } = -1;
        public String PaintFinishDescription { get; set; } = string.Empty;
        public DataTable PaintFinishRecord { get; set; } = new DataTable();
        public DataTable PaintFinishRecords { get; set; } = new DataTable();
        public Boolean Create_Paint_Finish_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE PaintFinishes (";
                StrSQL += "PaintFinishId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "PaintFinishDescription nvarchar(50))";

                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Paint Finishes Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Paint_Finish(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO PaintFinishes (PaintFinishDescription) VALUES ('" + Fix_Hyphon(PaintFinishDescription) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Paint Finish - " + INSERT_ERROR;
                }

            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Paint Finish - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Finish(Int32 FinishId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            PaintFinishRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintFinishes WHERE PaintFinishId = " + FinishId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintFinishRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_Finish();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Finish - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Finish - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Finish(Int32 FinishId, SqlTransaction TrnEnevelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            PaintFinishRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintFinishes WHERE PaintFinishId = " + FinishId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnevelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintFinishRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_Finish();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Finish - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Finish - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Paint_Finish()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                PaintFinishId = Convert.ToInt32(PaintFinishRecord.Rows[0]["PaintFinishId"]);
                PaintFinishDescription = PaintFinishRecord.Rows[0]["PaintFinishDescription"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Paint Finish - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Paint_Finish(Int32 FinishId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE PaintFinishes SET ";

                if (PaintFinishDescription != PaintFinishRecord.Rows[0]["PaintFinishDescription"].ToString())
                {
                    StrSQL += "PaintFinishDescription = '" + Fix_Hyphon(PaintFinishDescription) + "' ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "WHERE PaintFinishId = " + FinishId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Paint Finish - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Paint Finish - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Paint_Finish(Int32 FinishId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM PaintFinishes WHERE PaintFinishId = " + FinishId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Paint Finish - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Paint Finish - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Finishes()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PaintFinishRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintFinishes ORDER BY PaintFinishDescription";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintFinishRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Finishes - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        #endregion
        #region Paint Price Groups
        public Int32 PaintPriceGroupId { get; set; } = -1;
        public String PaintPriceGroupCode { get; set; } = string.Empty;
        public String PaintPriceGroupDescription { get; set; } = string.Empty;
        public Double PaintPriceGroupMinimumProductionCharge { get; set; } = 0;
        public Double PaintPriceGroupSetupFeeThreshold { get; set; } = 0;
        public Double PaintPriceGroupSetupFee { get; set; } = 0;
        public Int32 PaintPriceGroupMinutesPerBag { get; set; } = 0;
        public String PaintPriceGroupLastUpdate { get; set; } = string.Empty;
        public DataTable PaintPriceGroupRecord { get; set; } = new DataTable();
        public DataTable PaintPriceGroupRecords { get; set; } = new DataTable();
        public Boolean Create_Paint_PriceGroup_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE PaintPriceGroups (";
                StrSQL += "PaintPriceGroupId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "PaintPriceGroupCode nvarchar(20) NOT NULL, ";
                StrSQL += "PaintPriceGroupDescription nvarchar(50), ";
                StrSQL += "PaintPriceGroupMinimumCharges float, ";
                StrSQL += "PaintPriceGroupSetupFeeThreshold float, ";
                StrSQL += "PaintPriceGroupSetupFee float, ";
                StrSQL += "PaintPriceGroupMinutesPerBag bigint, ";
                StrSQL += "PaintFinishLastUpdated nvarchar(50))";

                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Paint Price Group Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Paint_PriceGroup(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO PaintPriceGroups (";
                StrSQL += "PaintPriceGroupCode, ";
                StrSQL += "PaintPriceGroupDescription, ";
                StrSQL += "PaintPriceGroupMinimumCharges, ";
                StrSQL += "PaintPriceGroupSetupFeeThreshold, ";
                StrSQL += "PaintPriceGroupSetupFee, ";
                StrSQL += "PaintPriceGroupMinutesPerBag, ";
                StrSQL += "PaintFinishLastUpdated) VALUES (";
                StrSQL += "'" + Fix_Hyphon(PaintPriceGroupCode) + "', ";
                StrSQL += "'" + Fix_Hyphon(PaintPriceGroupDescription) + "', ";
                StrSQL += PaintPriceGroupMinimumProductionCharge.ToString("N2") + ", ";
                StrSQL += PaintPriceGroupSetupFeeThreshold.ToString("N2") + ", ";
                StrSQL += PaintPriceGroupSetupFee.ToString("N2") + ", ";
                StrSQL += PaintPriceGroupMinutesPerBag.ToString("N0") + ", ";
                StrSQL += "'" + Fix_Hyphon(DateTime.Now.ToString()) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Paint Finish - " + INSERT_ERROR;
                }

            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Paint Price Group - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_PriceGroup(Int32 GroupId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            PaintPriceGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintPriceGroups WHERE PaintPriceGroupId = " + GroupId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintPriceGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_PriceGroup();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Price Group - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint PriceGroup - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_PriceGroup(Int32 GroupId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            PaintPriceGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintPriceGroups WHERE PaintPriceGroupId = " + GroupId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintPriceGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_PriceGroup();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Price Group - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint PriceGroup - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Paint_PriceGroup()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                PaintPriceGroupId = Convert.ToInt32(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupId"]);

                PaintPriceGroupDescription = PaintPriceGroupRecord.Rows[0]["PaintPriceGroupDescription"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Paint Price Group - " + ex.Message + " !";
            }

            return isSuccessful;
        }

        #endregion
        #endregion


        #region Data Base Test

        #endregion
        #region Data Base Create

        #endregion
    }
}
