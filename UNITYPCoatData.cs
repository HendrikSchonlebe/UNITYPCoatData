using System;
using System.Collections.Generic;
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
        public String SQLServerName { get; set; } = string.Empty;
        public String SQLUserName { get; set; } = string.Empty;
        public String SQLUserPassword { get; set; } = string.Empty;
        public String SQLDataBaseName { get; set; } = string.Empty;
        public SqlConnection PCConnection { get; set; } = new SqlConnection();
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
        #region Supplier Related Tables
        #region Supplier Table
        public Int32 SupplierId { get; set; }
        public String SupplierCode { get; set; } = string.Empty;
        public String SupplierName { get; set; } = string.Empty;
        public String SupplierAddress1 { get; set; } = string.Empty;
        public String SupplierAddress2 { get; set; } = string.Empty;
        public String SupplierSuburb { get; set; } = string.Empty;
        public String SupplierState { get; set; } = string.Empty;
        public String SupplierPostCode { get; set; } = string.Empty;
        public String SupplierTelephone { get; set; } = string.Empty;
        public String SupplierFax { get; set; } = string.Empty;
        public String SupplierEmail { get; set; } = string.Empty;
        public String SupplierLastUpdate { get; set; } = string.Empty;
        public DataTable SupplierRecord { get; set; } = new DataTable();
        public DataTable SupplierRecords { get; set; } = new DataTable();
        public Boolean Create_Supplier_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE Suppliers (";
                StrSQL += "SupplierId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "SupplierCode nvarchar(20) NOT NULL, ";
                StrSQL += "SupplierName nvarchar(50) NOT NULL, ";
                StrSQL += "SupplierAddress1 nvarchar(50), ";
                StrSQL += "SupplierAddress2 nvarchar(50), ";
                StrSQL += "SupplierSuburb nvarchar(50), ";
                StrSQL += "SupplierState nvarchar(3), ";
                StrSQL += "SupplierPostCode nvarchar(4), ";
                StrSQL += "SupplierTelephone nvarchar(15), ";
                StrSQL += "SupplierFax nvarchar(15), ";
                StrSQL += "SupplierEmail nvarchar(128), ";
                StrSQL += "SupplierLastUpdate nvarchar(50))";

                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Supplier Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Supplier_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO Suppliers (";
                StrSQL += "SupplierCode, ";
                StrSQL += "SupplierName, ";
                StrSQL += "SupplierAddress1, ";
                StrSQL += "SupplierAddress2, ";
                StrSQL += "SupplierSuburb, ";
                StrSQL += "SupplierState, ";
                StrSQL += "SupplierPostCode, ";
                StrSQL += "SupplierTelephone, ";
                StrSQL += "SupplierFax, ";
                StrSQL += "SupplierEmail, ";
                StrSQL += "SupplierLastUpdate) VALUES (";
                StrSQL += "'" + Fix_Hyphon(SupplierCode) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierName) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierAddress1) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierAddress2) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierSuburb) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierState) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierPostCode) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierTelephone) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierFax) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierEmail) + "', ";
                StrSQL += "'" + Fix_Hyphon(DateTime.Now.ToString()) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Supplier Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Supplier Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Record(Int32 SuppId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM Suppliers WHERE SupplierId = " + SuppId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Record - " + ex.Message + " !";
            }

            return isSuccessful;

        }
        public Boolean Get_Supplier_Record(Int32 SuppId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM Suppliers WHERE SupplierId = " + SuppId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Record - " + ex.Message + " !";
            }

            return isSuccessful;

        }
        public Boolean Get_Supplier_Record(String SuppCode)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM Suppliers WHERE SupplierCode = '" + SuppCode + "'";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Record - " + ex.Message + " !";
            }

            return isSuccessful;

        }
        private Boolean Gather_Supplier_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                SupplierId = Convert.ToInt32(SupplierRecord.Rows[0]["SupplierId"]);
                SupplierCode = SupplierRecord.Rows[0]["SupplierCode"].ToString();
                SupplierName = SupplierRecord.Rows[0]["SupplierName"].ToString();
                SupplierAddress1 = SupplierRecord.Rows[0]["SupplierAddress1"].ToString();
                SupplierAddress2 = SupplierRecord.Rows[0]["SupplierAddress2"].ToString();
                SupplierSuburb = SupplierRecord.Rows[0]["SupplierSuburb"].ToString();
                SupplierState = SupplierRecord.Rows[0]["SupplierState"].ToString();
                SupplierPostCode = SupplierRecord.Rows[0]["SupplierPostCode"].ToString();
                SupplierTelephone = SupplierRecord.Rows[0]["SupplierTelephone"].ToString();
                SupplierFax = SupplierRecord.Rows[0]["SupplierFax"].ToString();
                SupplierEmail = SupplierRecord.Rows[0]["SupplierEmail"].ToString();
                SupplierLastUpdate = SupplierRecord.Rows[0]["SupplierUpdate"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Supplier Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Supplier_Record(Int32 SuppId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE Suppliers SET ";
                if (SupplierCode != SupplierRecord.Rows[0]["SupplierCode"].ToString())
                {
                    StrSQL += "SupplierCode = '" + Fix_Hyphon(SupplierCode) + "', ";
                    hasChanged = true;
                }
                if (SupplierName != SupplierRecord.Rows[0]["SupplierName"].ToString())
                {
                    StrSQL += "SupplierName = '" + Fix_Hyphon(SupplierName) + "', ";
                    hasChanged = true;
                }
                if (SupplierAddress1 != SupplierRecord.Rows[0]["SupplierAddress1"].ToString())
                {
                    StrSQL += "SupplierAddress1 = '" + Fix_Hyphon(SupplierAddress1) + "', ";
                    hasChanged = true;
                }
                if (SupplierAddress2 != SupplierRecord.Rows[0]["SupplierAddress2"].ToString())
                {
                    StrSQL += "SupplierAddress2 = '" + Fix_Hyphon(SupplierAddress2) + "', ";
                    hasChanged = true;
                }
                if (SupplierSuburb != SupplierRecord.Rows[0]["SupplierSuburb"].ToString())
                {
                    StrSQL += "SupplierSuburb = '" + Fix_Hyphon(SupplierSuburb) + "', ";
                    hasChanged = true;
                }
                if (SupplierState != SupplierRecord.Rows[0]["SupplierState"].ToString())
                {
                    StrSQL += "SupplierState = '" + Fix_Hyphon(SupplierState) + "', ";
                    hasChanged = true;
                }
                if (SupplierPostCode != SupplierRecord.Rows[0]["SupplierPostCode"].ToString())
                {
                    StrSQL += "SupplierPostCode = '" + Fix_Hyphon(SupplierPostCode) + "', ";
                    hasChanged = true;
                }
                if (SupplierTelephone != SupplierRecord.Rows[0]["SupplierTelephone"].ToString())
                {
                    StrSQL += "SupplierTelephone = '" + Fix_Hyphon(SupplierTelephone) + "', ";
                    hasChanged = true;
                }
                if (SupplierFax != SupplierRecord.Rows[0]["SupplierFax"].ToString())
                {
                    StrSQL += "SupplierFax = '" + Fix_Hyphon(SupplierFax) + "', ";
                    hasChanged = true;
                }
                if (SupplierEmail != SupplierRecord.Rows[0]["SupplierEmail"].ToString())
                {
                    StrSQL += "SupplierEmail = '" + Fix_Hyphon(SupplierEmail) + "', ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "SupplierLastUpdate = '" + Fix_Hyphon(DateTime.Now.ToString()) + "' WHERE SupplierId = " + SuppId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Supplier Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Supplier Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Supplier_Record(Int32 SuppId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM Suppliers WHERE SupplierId = " + SuppId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Supplier Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Supplier Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_List()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            SupplierRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM Suppliers ORDER BY SupplierCode";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier List - " + ex.Message + " !";
            }

            return isSuccessful;

        }
        #endregion
        #region Supplier Contacts
        public Int32 SupplierContactId { get; set; }
        public Int32 SupplierContactSupplierId { get; set; }
        public String SupplierContactName { get; set; }
        public String SupplierContactPosition { get; set; }
        public String SupplierContactMobile { get; set; }
        public String SupplierContactEmail { get; set; }
        public DataTable SupplierContactRecord { get; set; } = new DataTable();
        public DataTable SupplierContactRecords { get; set; } = new DataTable();
        public Boolean Create_Supplier_Contacts_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE SupplierContacts (";
                StrSQL += "SupplierContactId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "SupplierContactSupplierId bigint NOT NULL, ";
                StrSQL += "SupplierContactName nvarchar(50) NOT NULL, ";
                StrSQL += "SupplierContactPosition nvarchar(50), ";
                StrSQL += "SupplierContactMobile nvarchar(15), ";
                StrSQL += "SupplierContactEmail nvarchar(128)) ";

                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Supplier Contacts Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Supplier_Contacts_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO SupplierContacts (";
                StrSQL += "SupplierContactSupplierId, ";
                StrSQL += "SupplierContactName, ";
                StrSQL += "SupplierContactPosition, ";
                StrSQL += "SupplierContactMobile, ";
                StrSQL += "SupplierContactEmail) VALUES (";
                StrSQL += SupplierContactSupplierId.ToString() + ", ";
                StrSQL += "'" + Fix_Hyphon(SupplierContactName) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierContactPosition) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierContactMobile) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierContactEmail) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Supplier Contacts Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Supplier Contacts Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Contacts_Record(Int32 Contactid)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierContactRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierContacts WHERE SupplierContactId = " + Contactid.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierContactRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Contacts_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Contacts Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Contacts Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Contacts_Record(Int32 Contactid, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierContactRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierContacts WHERE SupplierContactId = " + Contactid.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierContactRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Contacts_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Contacts Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Contacts Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Supplier_Contacts_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                SupplierContactId = Convert.ToInt32(SupplierContactRecord.Rows[0]["SupplierContactId"]);
                SupplierContactSupplierId = Convert.ToInt32(SupplierContactRecord.Rows[0]["SupplierContactSupplierId"]);
                SupplierContactName = SupplierContactRecord.Rows[0]["SupplierContactName"].ToString();
                SupplierContactPosition = SupplierContactRecord.Rows[0]["SupplierContactPosition"].ToString();
                SupplierContactMobile = SupplierContactRecord.Rows[0]["SupplierContactMobile"].ToString();
                SupplierContactEmail = SupplierContactRecord.Rows[0]["SupplierContactEmail"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Supplier Contacts Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Supplier_Contacts_Record(Int32 ContacId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE SupplierContacts SET ";

                if (SupplierContactSupplierId != Convert.ToInt32(SupplierContactRecord.Rows[0]["SupplierContactSupplierId"]))
                {
                    StrSQL += "SupplierContactSupplierId = " + SupplierContactSupplierId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierContactName != SupplierContactRecord.Rows[0]["SupplierContactName"].ToString())
                {
                    StrSQL += "SupplierContactName = '" + Fix_Hyphon(SupplierContactName) + "', ";
                    hasChanged = true;
                }
                if (SupplierContactPosition != SupplierContactRecord.Rows[0]["SupplierContactPosition"].ToString())
                {
                    StrSQL += "SupplierContactPosition = '" + Fix_Hyphon(SupplierContactPosition) + "', ";
                    hasChanged = true;
                }
                if (SupplierContactMobile != SupplierContactRecord.Rows[0]["SupplierContactMobile"].ToString())
                {
                    StrSQL += "SupplierContactMobile = '" + Fix_Hyphon(SupplierContactMobile) + "', ";
                    hasChanged = true;
                }
                if (SupplierContactEmail != SupplierContactRecord.Rows[0]["SupplierContactEmail"].ToString())
                {
                    StrSQL += "SupplierContactEmail = '" + Fix_Hyphon(SupplierContactEmail) + "', ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL = StrSQL.Substring(0, StrSQL.Length - 2) + " WHERE SupplierContactId = " + ContacId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Supplier Contacts Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Supplier Contacts Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Supplier_Contacts_Record(Int32 ContacId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM SupplierContacts WHERE SupplierContactId = " + ContacId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Supplier Contacts Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Supplier Contacts Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Contacts_List(Int32 Suppid)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            SupplierContactRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierContacts WHERE SupplierContactSupplierId = " + Suppid.ToString() + " ORDER BY SupplierContactName";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierContactRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Contacts List - " + ex.Message + " !";
            }

            return isSuccessful;
        }

        #endregion
        #region Supplier Paint Series
        public Int32 SupplierPaintSeriesId { get; set; }
        public Int32 SupplierPaintSeriesSupplierId { get; set; }
        public String SupplierPaintSeriesDescription { get; set; } = string.Empty;
        public DataTable SupplierPaintSeriesRecord { get; set; } = new DataTable();
        public DataTable SupplierPaintSeriesRecords { get; set; } = new DataTable();
        public Boolean Create_Paint_Series_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE SupplierPaintSeries (";
                StrSQL += "PaintSeriesId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "PaintSeriesSupplierId bigint NOT NULL, ";
                StrSQL += "PaintSeriesDescription nvarchar(50))";

                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Paint Series Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Paint_Series_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO SupplierPaintSeries (";
                StrSQL += "PaintSeriesSupplierId, ";
                StrSQL += "PaintSeriesDescription) VALUES (";
                StrSQL += SupplierPaintSeriesSupplierId.ToString() + ", ";
                StrSQL += "'" + Fix_Hyphon(SupplierPaintSeriesDescription) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Paint Series Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Paint Series Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Series_Record(Int32 SeriesId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierPaintSeriesRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierPaintSeriesd WHERE PaintSeriesId = " + SeriesId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierPaintSeriesRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_Series_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "GetPaint Series Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Series Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Series_Record(Int32 SeriesId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierPaintSeriesRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierPaintSeries WHERE PaintSeriesId = " + SeriesId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierPaintSeriesRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_Series_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "GetPaint Series Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Series Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Paint_Series_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                SupplierPaintSeriesId = Convert.ToInt32(SupplierPaintSeriesRecord.Rows[0]["PaintSeriesId"]);
                SupplierPaintSeriesSupplierId = Convert.ToInt32(SupplierPaintSeriesRecord.Rows[0]["PaintSeriesSupplierId"]);
                SupplierPaintSeriesDescription = SupplierPaintSeriesRecord.Rows[0]["PaintSeriesDescription"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Paint Series Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Paint_Series_Record(Int32 SeriesId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE SupplierPaintSeries SET ";
                if (SupplierPaintSeriesSupplierId != Convert.ToInt32(SupplierPaintSeriesRecord.Rows[0]["PaintSeriesSupplierId"]))
                {
                    StrSQL += "PaintSeriesSupplierId = " + SupplierPaintSeriesSupplierId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintSeriesDescription != SupplierPaintSeriesRecord.Rows[0]["PaintSeriesDescription"].ToString())
                {
                    StrSQL += "PaintSeriesDescription = '" + Fix_Hyphon(SupplierPaintSeriesDescription) + "', ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL = StrSQL.Substring(0, StrSQL.Length - 2);
                    StrSQL += " WHERE PaintSeriesId = " + SeriesId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Paint Series Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Paint Series Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Paint_Series_Record(Int32 SeriesId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM SupplierPaintSeries WHERE PaintSeriesId = " + SeriesId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Paint Series Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "DELETE Paint Series Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Series_List()
        {
            Boolean isSuccessful = true; ;

            ErrorMessage = string.Empty;
            SupplierPaintSeriesRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierPaintSeries ORDER BY PaintSeriesDescription";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierPaintSeriesRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Series List - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        #endregion
        #region Supplier Product Groups
        public Int32 SupplierProductGroupId { get; set; }
        public String SupplierProductGroupCode { get; set; } = string.Empty;
        public Int32 SupplierProductGroupSupplierId { get; set; }
        public String SupplierProductGroupDescription { get; set; } = string.Empty;
        public Int32 SupplierProductGroupPaintTypeId { get; set; }
        public Int32 SupplierProductGroupPaintFamilyId { get; set; }
        public Int32 SupplierProductGroupProcessId { get; set; }
        public Double SupplierProductGroupProcessRate { get; set; }
        public Double SupplierProductGroupMinimumProcessCharge { get; set; }
        public Double SupplierProductGroupMinimumMaterialCharge { get; set; }
        public Double SupplierProductGroupUnitSurcharge { get; set; }
        public Double SupplierProductGroupCoverage { get; set; }
        public Double SupplierProductGroupCoverageFactor { get; set; }
        public Int32 SupplierProductGroupVolumeBreakIntervals { get; set; }
        public Boolean SupplierProductGroupIsActive { get; set; } = true;
        public String SupplierProductGroupLastUpdated { get; set; } = string.Empty;
        public DataTable SupplierProductGroupRecord { get; set; } = new DataTable();
        public DataTable SupplierProductGroupRecords { get; set; } = new DataTable();
        public Boolean Create_Supplier_Product_Groups_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE SupplierProductGroups (";
                StrSQL += "SupplierProductGroupId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "SupplierproductGroupCode nvarchar(20) UNIQUE NOT NULL, ";
                StrSQL += "SupplierProductGroupSupplierId bigint NOT NULL, ";
                StrSQL += "SupplierproductGroupDescription nvarchar(50) NOT NULL, ";
                StrSQL += "SupplierProductGroupPaintTypeId bigint NOT NULL, ";
                StrSQL += "SupplierProductGroupPaintFamilyId bigint NOT NULL, ";
                StrSQL += "SupplierProductGroupProcessId bigint NOT NULL, ";
                StrSQL += "SupplierProductGroupProcessRate float, ";
                StrSQL += "SupplierProductGroupMinimumProcessCharge float, ";
                StrSQL += "SupplierProductGroupMinimumMaterialCharge float, ";
                StrSQL += "SupplierProductGroupUnitSurcharge float, ";
                StrSQL += "SupplierProductGroupCoverage float, ";
                StrSQL += "SupplierProductGroupCoverageFactor float, ";
                StrSQL += "SupplierProductGroupBreakIntervals bigint, ";
                StrSQL += "SupplierProductGroupIsActive bit NOT NULL, ";
                StrSQL += "SupplierProdcutGroupLastUpdated nvarchar(50) NOT NULL)";
                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Supplier Product Group Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }



        #endregion
        #endregion
        #region Paint Product Related Tables
        #region Paint Types
        public Int32 PaintTypeId { get; set; }
        public String PaintTypeDescription { get; set; } = string.Empty;
        public String PaintTypeMeasure { get; set; } = string.Empty;
        public DataTable PaintTypeRecord { get; set; } = new DataTable();
        public DataTable PaintTypeRecords { get; set; } = new DataTable();
        public Boolean Create_Paint_Type_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE PaintTypes (";
                StrSQL += "PaintTypeId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "PaintTypeDescription nvarchar(50))";

                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Paint Type Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Paint_Type_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO PaintTypes (PaintTypeDescription) VALUES ('" + Fix_Hyphon(PaintTypeDescription) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Paint Type Record - " + INSERT_ERROR;
                }

            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Paint Type Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Type_Record(Int32 TypeId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            PaintTypeRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintTypes WHERE PaintTypeId = " + TypeId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintTypeRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_Type_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Type Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Type Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Type_Record(Int32 TypeId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            PaintTypeRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintTypes WHERE PaintTypeId = " + TypeId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintTypeRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_Type_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Type Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Type Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Paint_Type_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                PaintTypeId = Convert.ToInt32(PaintFinishRecord.Rows[0]["PaintTypeId"]);
                PaintTypeDescription = PaintFinishRecord.Rows[0]["PaintTypeDescription"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Paint Type Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Paint_Type_Record(Int32 TypeId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE PaintTypes SET ";

                if (PaintTypeDescription != PaintFinishRecord.Rows[0]["PaintTypeDescription"].ToString())
                {
                    StrSQL += "PaintTypeDescription = '" + Fix_Hyphon(PaintTypeDescription) + "' ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "WHERE PaintTypeId = " + TypeId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Paint Type Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Paint Type Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Paint_Type_Record(Int32 TypeId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM PaintTypes WHERE PaintTypeId = " + TypeId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Paint Type Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Paint Type Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Type_List()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PaintTypeRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintTypes ORDER BY PaintTypeDescription";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintTypeRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Type List - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        #endregion
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
                ErrorMessage = "Create Colour Group Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Colour_Group_Record(SqlTransaction TrnEnvelope)
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
                    ErrorMessage = "Insert Colour Group Record - " + INSERT_ERROR;
                }

            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Colour Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Colour_Group_Record(Int32 GroupId)
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
                    isSuccessful = Gather_Colour_Group_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Colour Group Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Colour Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Colour_Group_Record(Int32 GroupId, SqlTransaction TrnEnvelope)
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
                    isSuccessful = Gather_Colour_Group_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Colour Group Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Colour Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Colour_Group_Record()
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
                ErrorMessage = "Gather Colour Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Colour_Group_Record(Int32 GroupId, SqlTransaction TrnEnvelope)
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
                        ErrorMessage = "Update Colour Group Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Colour Group Record - " + ex.Message + " !";
            }

            return isSuccessful;

        }
        public Boolean Delete_Colour_Group_Record(Int32 GroupId, SqlTransaction TrnEnvelope)
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
                    ErrorMessage = "Delete Colour Group Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Colour Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Colour_Group_List()
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
                ErrorMessage = "Get Colour Group List - " + ex.Message + " !";
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
        public Boolean Insert_Paint_Finish_Record(SqlTransaction TrnEnvelope)
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
                    ErrorMessage = "Insert Paint Finish Record - " + INSERT_ERROR;
                }

            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Paint Finish Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Finish_Record(Int32 FinishId)
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
                    isSuccessful = Gather_Paint_Finish_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Finish Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Finish Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Finish_Record(Int32 FinishId, SqlTransaction TrnEnevelope)
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
                    isSuccessful = Gather_Paint_Finish_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Finish Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Finish Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Paint_Finish_Record()
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
                ErrorMessage = "Gather Paint Finish Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Paint_Finish_Record(Int32 FinishId, SqlTransaction TrnEnvelope)
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
                        ErrorMessage = "Update Paint Finish Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Paint Finish Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Paint_Finish_Record(Int32 FinishId, SqlTransaction TrnEnvelope)
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
                    ErrorMessage = "Delete Paint Finish Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Paint Finish Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Finish_List()
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
                ErrorMessage = "Get Paint Finish List - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        #endregion
        #region Paint Families
        public Int32 PaintFamilyId { get; set; }
        public String PaintFamilyDescription { get; set; } = string.Empty;
        public DataTable PaintFamilyRecord { get; set; } = new DataTable();
        public DataTable PaintFamilyRecords { get; set; } = new DataTable();
        public Boolean Create_Paint_Family_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE PaintFamily (";
                StrSQL += "PaintFamilyId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "PaintFamilyDescription nvarchar(50))";

                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Paint Family Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Paint_Family_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO PaintFamily (";
                StrSQL += "PaintFamilyDescription) VALUES (";
                StrSQL += "'" + Fix_Hyphon(PaintFamilyDescription) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Paint Family Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Paint Family Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Family_Record(Int32 FamilyId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            PaintFamilyRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintFamily WHERE PaintFamilyId = " + FamilyId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintFamilyRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_Family_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "GetPaint Family Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Family Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Family_Record(Int32 FamilyId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            PaintFamilyRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintFamily WHERE PaintFamilyId = " + FamilyId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintFamilyRecord.Load(rdrGet);
                    isSuccessful = Gather_Paint_Family_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "GetPaint Family Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Family Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Paint_Family_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                PaintFamilyId = Convert.ToInt32(PaintFamilyRecord.Rows[0]["PaintFamilyId"]);
                PaintFamilyDescription = PaintFamilyRecord.Rows[0]["PaintFamilyDescription"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Paint Family Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Paint_Family_Record(Int32 FamilyId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE PaintFamily SET ";
                if (PaintFamilyDescription != PaintFamilyRecord.Rows[0]["PaintFamilyDescription"].ToString())
                {
                    StrSQL += "PaintFamilydescription = '" + Fix_Hyphon(PaintFinishDescription) + "' ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "WHERE PaintFamilyId = " + FamilyId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Paint Family Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Paint Family Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Paint_Family_Record(Int32 FamilyId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM PaintFamily WHERE PaintFamilyId = " + FamilyId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Paint Family Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "DELETE Paint Family Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_Family_List()
        {
            Boolean isSuccessful = true; ;

            ErrorMessage = string.Empty;
            PaintFamilyRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintFamily ORDER BY PaintFamilydescription";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintFamilyRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Family List - " + ex.Message + " !";
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
        public Boolean PaintPriceGroupChargePaintSeperately { get; set; } = false;
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
                StrSQL += "PaintPriceGroupChargePaintSeperately bit, ";
                StrSQL += "PaintPriceGroupLastUpdated nvarchar(50))";

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
        public Boolean Insert_Paint_PriceGroup_Record(SqlTransaction TrnEnvelope)
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
                StrSQL += "PaintPriceGroupChargePaintSeperately, ";
                StrSQL += "PaintPriceGroupLastUpdated) VALUES (";
                StrSQL += "'" + Fix_Hyphon(PaintPriceGroupCode) + "', ";
                StrSQL += "'" + Fix_Hyphon(PaintPriceGroupDescription) + "', ";
                StrSQL += PaintPriceGroupMinimumProductionCharge.ToString("N2") + ", ";
                StrSQL += PaintPriceGroupSetupFeeThreshold.ToString("N2") + ", ";
                StrSQL += PaintPriceGroupSetupFee.ToString("N2") + ", ";
                StrSQL += PaintPriceGroupMinutesPerBag.ToString("N0") + ", ";
                StrSQL += "'" + PaintPriceGroupChargePaintSeperately.ToString() + "', ";
                StrSQL += "'" + Fix_Hyphon(DateTime.Now.ToString()) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Paint Price Group Record - " + INSERT_ERROR;
                }

            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Paint Price Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_PriceGroup_Record(Int32 GroupId)
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
                    isSuccessful = Gather_Paint_PriceGroup_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Price Group Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Price Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_PriceGroup_Record(Int32 GroupId, SqlTransaction TrnEnvelope)
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
                    isSuccessful = Gather_Paint_PriceGroup_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Paint Price Group Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Price Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Paint_PriceGroup_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                PaintPriceGroupId = Convert.ToInt32(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupId"]);
                PaintPriceGroupCode = PaintPriceGroupRecord.Rows[0]["PaintPriceGroupCode"].ToString();
                PaintPriceGroupDescription = PaintPriceGroupRecord.Rows[0]["PaintPriceGroupDescription"].ToString();
                PaintPriceGroupMinimumProductionCharge = Convert.ToDouble(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupMinimumCharge"]);
                PaintPriceGroupSetupFeeThreshold = Convert.ToDouble(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupSetUpFeeThreshold"]);
                PaintPriceGroupSetupFee = Convert.ToDouble(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupSetUpFee"]);
                PaintPriceGroupMinutesPerBag = Convert.ToInt32(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupMinutesPerBag"]);
                PaintPriceGroupChargePaintSeperately = Convert.ToBoolean(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupChargePaintSeperately"]);
                PaintPriceGroupLastUpdate = PaintPriceGroupRecord.Rows[0]["PaintPriceGroupLastUpdate"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Paint Price Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Paint_PriceGroup_Record(Int32 PGId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE PaintPriceGroups SET ";
                if (PaintPriceGroupCode != PaintPriceGroupRecord.Rows[0]["PaintPriceGroupCode"].ToString())
                {
                    StrSQL += "PaintPriceGroupCode = '" + Fix_Hyphon(PaintPriceGroupCode) + "', ";
                    hasChanged = true;
                }
                if (PaintPriceGroupDescription != PaintPriceGroupRecord.Rows[0]["PaintPriceGroupDescription"].ToString())
                {
                    StrSQL += "PaintPriceGroupDescription = '" + Fix_Hyphon(PaintPriceGroupDescription) + "', ";
                    hasChanged = true;
                }
                if (PaintPriceGroupMinimumProductionCharge != Convert.ToDouble(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupMinimumCharges"]))
                {
                    StrSQL += "PaintPriceGroupMinimumCharges = " + PaintPriceGroupMinimumProductionCharge.ToString("N2") + ", ";
                    hasChanged = true;
                }
                if (PaintPriceGroupSetupFeeThreshold != Convert.ToDouble(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupSetupFeeThreshold"]))
                {
                    StrSQL += "PaintPriceGroupSetupFeeThreshold = " + PaintPriceGroupSetupFeeThreshold.ToString("N2") + ", ";
                    hasChanged = true;
                }
                if (PaintPriceGroupSetupFee != Convert.ToDouble(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupSetupFee"]))
                {
                    StrSQL += "PaintPriceGroupSetupFee = " + PaintPriceGroupSetupFee.ToString("N2") + ", ";
                    hasChanged = true;
                }
                if (PaintPriceGroupMinutesPerBag != Convert.ToInt32(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupMinutesPerBag"]))
                {
                    StrSQL += "PaintPriceGroupMinutesPerBag = " + PaintPriceGroupMinutesPerBag.ToString("N0") + ", ";
                    hasChanged = true;
                }
                if (PaintPriceGroupChargePaintSeperately != Convert.ToBoolean(PaintPriceGroupRecord.Rows[0]["PaintPriceGroupChargePaintSeperately"]))
                {
                    StrSQL += "PaintPriceGroupChargePaintSeperately = '" + PaintPriceGroupChargePaintSeperately.ToString() + "', ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "PaintPriceGroupLastUpdated = '" + Fix_Hyphon(DateTime.Now.ToString()) + "' WHERE PaintPriceGroupId = " + PGId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Paint Price Group Record - " + UPDATE_ERROR;
                    }
                }

            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Paint Price Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Paint_PriceGroup_Record(Int32 GroupId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM PaintPriceGroups WHERE PaintPriceGroupId = " + GroupId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Paint Price Group Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Paint Price Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Paint_PriceGroup_List()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PaintPriceGroupRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PaintPriceGroups ORDER BY PaintPriceGroupCode";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PaintPriceGroupRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Paint Price Group List - " + ex.Message + " !";
            }

            return isSuccessful;
        }

        #endregion
        #endregion
        #region Part Related Tables

        #endregion
        #region Data Base Test
        public Boolean Connect_To_SQL_Server()
        {
            Boolean isSuccessful = true;
            String StrConnectionString;

            ErrorMessage = string.Empty;

            try
            {
                StrConnectionString = "Data Source=" + SQLServerName + ";" + "Initial Catalog=" + SQLDataBaseName + ";Persist Security Info=False;User ID=" + SQLUserName + ";Password=" + SQLUserPassword + ";";
                PCConnection.ConnectionString = StrConnectionString;
                PCConnection.Open();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "** Operator **\r\n\r\n" + ex.Message + " !\r\n" + "Connecting to " + SQLServerName + " " + SQLDataBaseName;
            }


            return isSuccessful;
        }
        #endregion
        #region Data Base Create
        public Boolean Create_Data_Base()
        {
            Boolean isSuccessful;
            SqlConnection MasterConnection = new SqlConnection();

            ErrorMessage = string.Empty;

            try
            {
                // ***** Logon To Master Database
                MasterConnection.ConnectionString = "Data Source = " + SQLServerName + ";" + "Initial Catalog = master" + ";Persist Security Info=False;User ID=" + SQLUserName + ";Password=" + SQLUserPassword + ";";
                MasterConnection.Open();
                // ***** Create New Database
                SqlCommand cmdCreate = new SqlCommand("CREATE DATABASE " + SQLDataBaseName, MasterConnection);
                cmdCreate.CommandTimeout = 30000;
                cmdCreate.ExecuteNonQuery();
                MasterConnection.Close();
                // ***** Logon To the New Database
                Connect_To_SQL_Server();
                // ***** Create Data Tables
                isSuccessful = Create_Data_Tables();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "** Operator **\r\n\r\n" + ex.Message + " !\r\n" + "Creating New Data " + SQLDataBaseName + " on " + SQLServerName;
            }


            return isSuccessful;
        }
        public Boolean Create_Data_Tables()
        {
            Boolean isSuccessful = true;
            List <String> MyTables = Populate_Tables_List();

            SqlTransaction TrnEnvelope = PCConnection.BeginTransaction();

            foreach(String DBTable in MyTables)
            {
                if (DBTable == "PaintTypes")
                    isSuccessful = Create_Paint_Type_Table(TrnEnvelope);
                else if (DBTable == "ColourGroups")
                    isSuccessful = Create_Colour_Group_Table(TrnEnvelope);
                else if (DBTable == "PaintFinishes")
                    isSuccessful = Create_Paint_Finish_Table(TrnEnvelope);
                else if (DBTable == "PaintFamily")
                    isSuccessful = Create_Paint_Family_Table(TrnEnvelope);
                else if (DBTable == "PaintPriceGroups")
                    isSuccessful = Create_Paint_PriceGroup_Table(TrnEnvelope);


                if (isSuccessful == false)
                {
                    TrnEnvelope.Rollback();
                    break;
                }
            }

            if (isSuccessful == true)
                TrnEnvelope.Commit();

            return isSuccessful;
        }
        private List<String> Populate_Tables_List()
        {
            List<String> MyTables = new List<String>();
            MyTables.Add("PaintTypes");
            MyTables.Add("ColourGroups");
            MyTables.Add("PaintFinishes");
            MyTables.Add("PaintFamily");
            MyTables.Add("PaintPriceGroups");

            return MyTables;
        }
        #endregion
    }
} 
