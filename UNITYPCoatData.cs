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
                String StrSQL = "SELECT * FROM SupplierPaintSeries WHERE PaintSeriesId = " + SeriesId.ToString();
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
        public Boolean Insert_Supplier_Product_Groups_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO SupplierProductGroups (";
                StrSQL += "SupplierproductGroupCode, ";
                StrSQL += "SupplierProductGroupSupplierId, ";
                StrSQL += "SupplierproductGroupDescription, ";
                StrSQL += "SupplierProductGroupPaintTypeId, ";
                StrSQL += "SupplierProductGroupPaintFamilyId, ";
                StrSQL += "SupplierProductGroupProcessId, ";
                StrSQL += "SupplierProductGroupProcessRate, ";
                StrSQL += "SupplierProductGroupMinimumProcessCharge, ";
                StrSQL += "SupplierProductGroupMinimumMaterialCharge, ";
                StrSQL += "SupplierProductGroupUnitSurcharge, ";
                StrSQL += "SupplierProductGroupCoverage, ";
                StrSQL += "SupplierProductGroupCoverageFactor, ";
                StrSQL += "SupplierProductGroupBreakIntervals, ";
                StrSQL += "SupplierProductGroupIsActive, ";
                StrSQL += "SupplierProdcutGroupLastUpdated) VALUES (";
                StrSQL += "'" + Fix_Hyphon(SupplierProductGroupCode) + "', ";
                StrSQL += SupplierProductGroupSupplierId.ToString() + ", ";
                StrSQL += "'" + Fix_Hyphon(SupplierProductGroupDescription) + "', ";
                StrSQL += SupplierProductGroupPaintTypeId.ToString() + ", ";
                StrSQL += SupplierProductGroupPaintFamilyId.ToString() + ", ";
                StrSQL += SupplierProductGroupProcessId.ToString() + ", ";
                StrSQL += SupplierProductGroupProcessRate.ToString() + ", ";
                StrSQL += SupplierProductGroupMinimumProcessCharge.ToString() + ", ";
                StrSQL += SupplierProductGroupMinimumMaterialCharge.ToString() + ", ";
                StrSQL += SupplierProductGroupUnitSurcharge.ToString() + ", ";
                StrSQL += SupplierProductGroupCoverage.ToString() + ", ";
                StrSQL += SupplierProductGroupCoverageFactor.ToString() + ", ";
                StrSQL += SupplierProductGroupVolumeBreakIntervals.ToString() + ", ";
                StrSQL += "'" + SupplierProductGroupIsActive.ToString() + "', ";
                StrSQL += "'" + Fix_Hyphon(DateTime.Now.ToString()) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Supplier Product Group Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Supplier Product Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Product_Groups_Record(Int32 GroupId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierProductGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierProductGroups WHERE SupplierProductGroupId = " + GroupId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierProductGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Product_Groups_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Product Group Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Product Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Product_Groups_Record(Int32 GroupId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierProductGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierProductGroups WHERE SupplierProductGroupId = " + GroupId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierProductGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Product_Groups_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Product Group Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Product Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Product_Groups_Record(String GroupCode)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierProductGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierProductGroups WHERE SupplierProductCode = '" + Fix_Hyphon(GroupCode) + "'";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierProductGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Product_Groups_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Product Group Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Product Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Supplier_Product_Groups_Record()
        {
            Boolean isSuccessful = true;

            try
            {
                SupplierProductGroupId = Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupId"]);
                SupplierProductGroupSupplierId = Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupSupplierId"]);
                SupplierProductGroupCode = SupplierProductGroupRecord.Rows[0]["SupplierProductGroupCode"].ToString();
                SupplierProductGroupDescription = SupplierProductGroupRecord.Rows[0]["SupplierProductGroupDescription"].ToString();
                SupplierProductGroupPaintTypeId = Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupPaintTypeId"]);
                SupplierProductGroupPaintFamilyId = Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupPaintFamilyId"]);
                SupplierProductGroupProcessId = Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupProcessId"]);
                SupplierProductGroupProcessRate = Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupProcessRate"]);
                SupplierProductGroupMinimumProcessCharge = Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupMinimumProcessCharge"]);
                SupplierProductGroupMinimumMaterialCharge = Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupMinimumMaterialCharge"]);
                SupplierProductGroupUnitSurcharge = Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupUnitSurcharge"]);
                SupplierProductGroupCoverage = Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupCoverage"]);
                SupplierProductGroupCoverageFactor = Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupCoverageFactor"]);
                SupplierProductGroupVolumeBreakIntervals = Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupVolumeBreakIntervals"]);
                SupplierProductGroupIsActive = Convert.ToBoolean(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupIsActive"]);
                SupplierProductGroupLastUpdated = SupplierProductGroupRecord.Rows[0]["SupplierProductGroupLastUpdated"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Supplier Product Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Supplier_Product_Groups_Record(Int32 GroupId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE SupplierProductGroups SET ";

                if (SupplierProductGroupSupplierId != Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupSupplierId"]))
                {
                    StrSQL += "SupplierProductGroupSupplierId = " + SupplierProductGroupSupplierId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupCode != SupplierProductGroupRecord.Rows[0]["SupplierProductGroupCode"].ToString())
                {
                    StrSQL += "SupplierProductGroupCode = '" + Fix_Hyphon(SupplierProductGroupCode) + "', ";
                    hasChanged = true;
                }
                if (SupplierProductGroupDescription != SupplierProductGroupRecord.Rows[0]["SupplierProductGroupDescription"].ToString())
                {
                    StrSQL += "SupplierProductGroupDescription = '" + Fix_Hyphon(SupplierProductGroupDescription) + "', ";
                    hasChanged = true;
                }
                if (SupplierProductGroupPaintTypeId != Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupPaintTypeId"]))
                {
                    StrSQL += "SupplierProductGroupPaintTypeId = " + SupplierProductGroupPaintTypeId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupPaintFamilyId != Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupPaintFamilyId"]))
                {
                    StrSQL += "SupplierProductGroupPaintFamilyId = " + SupplierProductGroupPaintFamilyId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupProcessId != Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupProcessId"]))
                {
                    StrSQL += "SupplierProductGroupProcessId = " + SupplierProductGroupProcessId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupProcessRate != Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupProcessRate"]))
                {
                    StrSQL += "SupplierProductGroupProcessRate = " + SupplierProductGroupProcessRate.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupMinimumProcessCharge != Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupMinimumProcessCharge"]))
                {
                    StrSQL += "SupplierProductGroupMinimumProcessCharge = " + SupplierProductGroupMinimumProcessCharge.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupMinimumMaterialCharge != Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupMinimumMaterialCharge"]))
                {
                    StrSQL += "SupplierProductGroupMinimumMaterialCharge = " + SupplierProductGroupMinimumMaterialCharge.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupUnitSurcharge != Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupUnitSurcharge"]))
                {
                    StrSQL += "SupplierProductGroupUnitSurcharge = " + SupplierProductGroupUnitSurcharge.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupCoverage != Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupCoverage"]))
                {
                    StrSQL += "SupplierProductGroupCoverage = " + SupplierProductGroupCoverage.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupCoverageFactor != Convert.ToDouble(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupCoverageFactor"]))
                {
                    StrSQL += "SupplierProductGroupCoverageFactor = " + SupplierProductGroupCoverageFactor.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupVolumeBreakIntervals != Convert.ToInt32(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupVolumeBreakIntervals"]))
                {
                    StrSQL += "SupplierProductGroupVolumeBreakIntervals = " + SupplierProductGroupVolumeBreakIntervals.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierProductGroupIsActive != Convert.ToBoolean(SupplierProductGroupRecord.Rows[0]["SupplierProductGroupIsActive"]))
                {
                    StrSQL += "SupplierProductGroupIsActive = '" + SupplierProductGroupIsActive.ToString() + "', ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "SupplierProductGroupLastUpdated = '" + Fix_Hyphon(DateTime.Now.ToString()) + "' WHERE SupplierProductGroupId = " + GroupId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Supplier Product Group Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Supplier Product Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Supplier_Product_Groups_Record(Int32 GroupId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM SupplierProductGroups WHERE SupplierProductGroupId = " + GroupId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Supplier Product Group Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Supplier Product Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Product_Groups_List(Int32 SuppId)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            SupplierProductGroupRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierProductGroups WHERE SupplierProductGroupSupplierId = " + SuppId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierProductGroupRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Product Group List - " + ex.Message + " !";
            }

            return isSuccessful;
        }

        #endregion
        #region Supplier Paint Products
        public Int32 SupplierPaintProductId { get; set; }
        public Int32 SupplierPaintProductSupplierId { get; set; }
        public String SupplierPaintProductCode { get; set; } = string.Empty;
        public String SupplierPaintProductColourName { get; set; } = string.Empty;
        public Int32 SupplierPaintProductLRV { get; set; }
        public Int32 SupplierPaintProductRVal { get; set; }
        public Int32 SupplierPaintProductGVal { get; set; }
        public Int32 SupplierPaintProductBVal { get; set; }
        public Int32 SupplierPaintProductFinishTypeId { get; set; }
        public Int32 SupplierPaintProductSupplierProductGroupId { get; set; }
        public Int32 SupplierPaintProductSupplierPaintSeriesId { get; set; }
        public Boolean SupplierPaintProductIsMTO { get; set; } = false;
        public Boolean SupplierPaintProductIsActive { get; set; } = true;
        public Boolean SupplierPaintProductIsExternal { get; set; } = false;
        public Double SupplierPaintProductCoverageFactor { get; set; }
        public Double SupplierPaintProductStockOnHand { get; set; }
        public Double SupplierPaintProductStockInProduction { get; set; }
        public String SupplierPaintProductBarCode { get; set; } = string.Empty;
        public String SupplierPaintProductLastUpdated { get; set; } = string.Empty;
        public DataTable SupplierPaintProductRecord { get; set; } = new DataTable();
        public DataTable SupplierPaintProductRecords { get; set; } = new DataTable();
        public Boolean Create_Supplier_Paint_Products_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE SupplierPaintProducts (";
                StrSQL += "SupplierPaintProductId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "SupplierPaintProductSupplierId bigint NOT NULL, ";
                StrSQL += "SupplierPainProductCode nvarchar(20) UNIQUE NOT NULL, ";
                StrSQL += "SupplierPaintProductColourName nvarchar(50) NOT NULL, ";
                StrSQL += "SupplierPaintProductLRV bigint NOT NULL, ";
                StrSQL += "SupplierPaintProductRVal bigint NOT NULL, ";
                StrSQL += "SupplierPaintProductGVal bigint NOT NULL, ";
                StrSQL += "SupplierPaintProductBVal bigint NOT NULL, ";
                StrSQL += "SupplierPaintProductFinishTypeId bigint NOT NULL, ";
                StrSQL += "SupplierPaintProductSupplierProductGroupId bigint NOT NULL, ";
                StrSQL += "SupplierPaintProductSupplierPaintSeriesId bigint NOT NULL, ";
                StrSQL += "SupplierPaintProductIsMTO bit NOT NULL, ";
                StrSQL += "SupplierPaintProductIsActive bit NOT NULL, ";
                StrSQL += "SupplierPaintProductIsExternal bit NOT NULL, ";
                StrSQL += "SupplierPaintProductCoverageFactor float, ";
                StrSQL += "SupplierPaintProductStockOnHand float, ";
                StrSQL += "SupplierPaintProductStockInProduction float, ";
                StrSQL += "SupplierPaintProductBarCode nvarchar(50), ";
                StrSQL += "SupplierPaintProductLastUpdated nvarchar(50) NOT NULL)";
                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Supplier Paint Product Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Supplier_Paint_Products_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO SupplierPaintProducts (";
                StrSQL += "SupplierPaintProductSupplierId, ";
                StrSQL += "SupplierPainProductCode, ";
                StrSQL += "SupplierPaintProductColourName, ";
                StrSQL += "SupplierPaintProductLRV, ";
                StrSQL += "SupplierPaintProductRVal, ";
                StrSQL += "SupplierPaintProductGVal, ";
                StrSQL += "SupplierPaintProductBVal, ";
                StrSQL += "SupplierPaintProductFinishTypeId, ";
                StrSQL += "SupplierPaintProductSupplierProductGroupId, ";
                StrSQL += "SupplierPaintProductSupplierPaintSeriesId, ";
                StrSQL += "SupplierPaintProductIsMTO, ";
                StrSQL += "SupplierPaintProductIsActive, ";
                StrSQL += "SupplierPaintProductIsExternal, ";
                StrSQL += "SupplierPaintProductCoverageFactor, ";
                StrSQL += "SupplierPaintProductStockOnHand, ";
                StrSQL += "SupplierPaintProductStockInProduction, ";
                StrSQL += "SupplierPaintProductBarCode, ";
                StrSQL += "SupplierPaintProductLastUpdated) VALUES (";
                StrSQL += SupplierPaintProductSupplierId.ToString() + ", ";
                StrSQL += "'" + Fix_Hyphon(SupplierPaintProductCode) + "', ";
                StrSQL += "'" + Fix_Hyphon(SupplierPaintProductColourName) + "', ";
                StrSQL += SupplierPaintProductLRV.ToString() + ", ";
                StrSQL += SupplierPaintProductRVal.ToString() + ", ";
                StrSQL += SupplierPaintProductGVal.ToString() + ", ";
                StrSQL += SupplierPaintProductBVal.ToString() + ", ";
                StrSQL += SupplierPaintProductFinishTypeId.ToString() + ", ";
                StrSQL += SupplierPaintProductSupplierProductGroupId.ToString() + ", ";
                StrSQL += SupplierPaintProductSupplierPaintSeriesId.ToString() + ", ";
                StrSQL += "'" + SupplierPaintProductIsMTO.ToString() + "', ";
                StrSQL += "'" + SupplierPaintProductIsActive.ToString() + "', ";
                StrSQL += "'" + SupplierPaintProductIsExternal.ToString() + "', ";
                StrSQL += SupplierPaintProductCoverageFactor.ToString() + ", ";
                StrSQL += SupplierPaintProductStockOnHand.ToString() + ", ";
                StrSQL += SupplierPaintProductStockInProduction.ToString() + ", ";
                StrSQL += "'" + Fix_Hyphon(SupplierPaintProductBarCode) + "', ";
                StrSQL += "'" + Fix_Hyphon(DateTime.Now.ToString()) + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Supplier Paint Product Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Supplier Paint Product Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Paint_Products_Record(Int32 ProductId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierPaintProductRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierPaintProducts WHERE SupplierPaintProductId = " + ProductId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierPaintProductRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Paint_Products_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Paint Product Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Paint Product Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Paint_Products_Record(Int32 ProductId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierPaintProductRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierPaintProducts WHERE SupplierPaintProductId = " + ProductId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierPaintProductRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Paint_Products_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Paint Product Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Paint Product Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Paint_Products_Record(String ProductCode)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            SupplierPaintProductRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierPaintProducts WHERE SupplierPaintProductCode = '" + Fix_Hyphon(ProductCode) + "'";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierPaintProductRecord.Load(rdrGet);
                    isSuccessful = Gather_Supplier_Paint_Products_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Supplier Paint Product Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Paint Product Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }

        private Boolean Gather_Supplier_Paint_Products_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                SupplierPaintProductId = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductId"]);
                SupplierPaintProductSupplierId = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductSupplierId"]);
                SupplierPaintProductCode = SupplierPaintProductRecord.Rows[0]["SupplierPaintProductCode"].ToString();
                SupplierPaintProductColourName = SupplierPaintProductRecord.Rows[0]["SupplierPaintProductColourName"].ToString();
                SupplierPaintProductLRV = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductLRV"]);
                SupplierPaintProductRVal = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductRVal"]);
                SupplierPaintProductGVal = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductGVal"]);
                SupplierPaintProductBVal = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductBVal"]);
                SupplierPaintProductFinishTypeId = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductFinishTypeId"]);
                SupplierPaintProductSupplierProductGroupId = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductSupplierProductGroupId"]);
                SupplierPaintProductSupplierPaintSeriesId = Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductSupplierPaintSeriesId"]);
                SupplierPaintProductIsMTO = Convert.ToBoolean(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductIsMTO"]);
                SupplierPaintProductIsActive = Convert.ToBoolean(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductIsActive"]);
                SupplierPaintProductIsExternal = Convert.ToBoolean(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductIsExternal"]);
                SupplierPaintProductCoverageFactor = Convert.ToDouble(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductCoverageFactor"]);
                SupplierPaintProductStockOnHand = Convert.ToDouble(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductStockOnHand"]);
                SupplierPaintProductStockInProduction = Convert.ToDouble(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductStockInProduction"]);
                SupplierPaintProductBarCode = SupplierPaintProductRecord.Rows[0]["SupplierPaintProductBarCode"].ToString();
                SupplierPaintProductLastUpdated = SupplierPaintProductRecord.Rows[0]["SupplierPaintProductLastUpdated"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Supplier Paint Product Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Supplier_Paint_Products_Record(Int32 ProductId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE SupplierPaintProducts SET ";

                if (SupplierPaintProductSupplierId != Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductSupplierId"]))
                {
                    StrSQL += "SupplierPaintProductSupplierId = " + SupplierPaintProductSupplierId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductCode != SupplierPaintProductRecord.Rows[0]["SupplierPaintProductCode"].ToString())
                {
                    StrSQL += "SupplierPaintProductCode = '" + Fix_Hyphon(SupplierPaintProductCode) + "', ";
                    hasChanged = true;
                }
                if (SupplierPaintProductColourName != SupplierPaintProductRecord.Rows[0]["SupplierPaintProductColourName"].ToString())
                {
                    StrSQL += "SupplierPaintProductColourName = '" + Fix_Hyphon(SupplierPaintProductColourName) + "', ";
                    hasChanged = true;
                }
                if (SupplierPaintProductLRV != Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductLRV"]))
                {
                    StrSQL += "SupplierPaintProductLRV = " + SupplierPaintProductLRV.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductRVal != Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductRVal"]))
                {
                    StrSQL += "SupplierPaintProductRVal = " + SupplierPaintProductRVal.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductGVal != Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductGVal"]))
                {
                    StrSQL += "SupplierPaintProductGVal = " + SupplierPaintProductGVal.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductBVal != Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductBVal"]))
                {
                    StrSQL += "SupplierPaintProductBVal = " + SupplierPaintProductBVal.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductFinishTypeId != Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductFinishTypeId"]))
                {
                    StrSQL += "SupplierPaintProductFinishTypeId = " + SupplierPaintProductFinishTypeId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductSupplierProductGroupId != Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductSupplierProductGroupId"]))
                {
                    StrSQL += "SupplierPaintProductSupplierProductGroupId = " + SupplierPaintProductSupplierProductGroupId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductSupplierPaintSeriesId != Convert.ToInt32(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductSupplierPaintSeriesId"]))
                {
                    StrSQL += "SupplierPaintProductSupplierPaintSeriesId = " + SupplierPaintProductSupplierPaintSeriesId.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductIsMTO != Convert.ToBoolean(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductIsMTO"]))
                {
                    StrSQL += "SupplierPaintProductIsMTO = '" + SupplierPaintProductIsMTO.ToString() + "', ";
                    hasChanged = true;
                }
                if (SupplierPaintProductIsActive != Convert.ToBoolean(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductIsActive"]))
                {
                    StrSQL += "SupplierPaintProductIsActive = '" + SupplierPaintProductIsActive.ToString() + "', ";
                    hasChanged = true;
                }
                if (SupplierPaintProductIsExternal != Convert.ToBoolean(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductIsExternal"]))
                {
                    StrSQL += "SupplierPaintProductIsExternal = '" + SupplierPaintProductIsExternal.ToString() + "', ";
                    hasChanged = true;
                }
                if (SupplierPaintProductCoverageFactor != Convert.ToDouble(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductCoverageFactor"]))
                {
                    StrSQL += "SupplierPaintProductCoverageFactor = " + SupplierPaintProductCoverageFactor.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductStockOnHand != Convert.ToDouble(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductStockOnHand"]))
                {
                    StrSQL += "SupplierPaintProductStockOnHand = " + SupplierPaintProductStockOnHand.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductStockInProduction != Convert.ToDouble(SupplierPaintProductRecord.Rows[0]["SupplierPaintProductStockInProduction"]))
                {
                    StrSQL += "SupplierPaintProductStockInProduction = " + SupplierPaintProductStockInProduction.ToString() + ", ";
                    hasChanged = true;
                }
                if (SupplierPaintProductBarCode != SupplierPaintProductRecord.Rows[0]["SupplierPaintProductBarCode"].ToString())
                {
                    StrSQL += "SupplierPaintProductBarCode = '" + Fix_Hyphon(SupplierPaintProductBarCode) + "', ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "SupplierPaintProductLastUpdated = '" + Fix_Hyphon(DateTime.Now.ToString()) + "' WHERE SupplierPaintProductId = " + ProductId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Supplier Paint Product Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Supplier Paint Product Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Supplier_Paint_Products_Record(Int32 ProductId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            
            try
            {
                String StrSQL = "DELETE FROM SupplierPaintProducts WHERE SupplierPaintProductId = " + ProductId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Supplier Paint Product Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Supplier Paint Product Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Supplier_Paint_Products_List(Int32 SuppId, Int32 GroupId)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            SupplierPaintProductRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM SupplierPaintProducts ";
                if (SuppId > 0)
                {
                    StrSQL += "WHERE SupplierPaintProductSupplierId = " + SuppId.ToString() + " ";
                    if (GroupId > 0)
                        StrSQL += "AND SupplierPaintProductSupplierProductGroupId = " + GroupId.ToString() + " ";
                }
                else
                {
                    if (GroupId > 0)
                        StrSQL += "WHERE SupplierPaintProductSupplierProductGroupId = " + GroupId.ToString() + " ";
                }
                StrSQL += "ORDER BY SupplierPaintProductCode";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    SupplierPaintProductRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Supplier Paint Product List - " + ex.Message + " !";
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
                PaintTypeId = Convert.ToInt32(PaintTypeRecord.Rows[0]["PaintTypeId"]);
                PaintTypeDescription = PaintTypeRecord.Rows[0]["PaintTypeDescription"].ToString();
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

                if (PaintTypeDescription != PaintTypeRecord.Rows[0]["PaintTypeDescription"].ToString())
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
                    StrSQL += "PaintFamilydescription = '" + Fix_Hyphon(PaintFamilyDescription) + "' ";
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
        #region Process Rates
        public Int32 ProcessRateId { get; set; }
        public String ProcessRateDescription { get; set; } = string.Empty;
        public Double ProcessRateRate { get; set; }
        public Double ProcessRateMinimumCharge { get; set; }
        public DataTable ProcessRateRecord { get; set; } = new DataTable();
        public DataTable ProcessRateRecords { get; set; } = new DataTable();
        public Boolean Create_Process_Rates_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE ProcessRates (";
                StrSQL += "ProcessRateId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "ProcessRateDescription nvarchar(50), ";
                StrSQL += "ProcessRateRate float, ";
                StrSQL += "ProcessRateMinimumCharge float)";
                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Process Rates Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_Process_Rates_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO ProcessRates (";
                StrSQL += "ProcessRateDescription, ";
                StrSQL += "ProcessRateRate, ";
                StrSQL += "ProcessRateMinimumCharge) VALUES (";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Process Rates Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Process Rates Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Process_Rates_Record(Int32 RateId)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            ProcessRateRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM ProcessRates WHERE ProcessRateId = " + RateId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    ProcessRateRecord.Load(rdrGet);
                    isSuccessful = Gather_Process_Rates_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Process Rates Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Process Rates Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Process_Rates_Record(Int32 RateId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful;

            ErrorMessage = string.Empty;
            ProcessRateRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM ProcessRates WHERE ProcessRateId = " + RateId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    ProcessRateRecord.Load(rdrGet);
                    isSuccessful = Gather_Process_Rates_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Process Rates Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Process Rates Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        private Boolean Gather_Process_Rates_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                ProcessRateId = Convert.ToInt32(ProcessRateRecord.Rows[0]["ProcessRateId"]);
                ProcessRateDescription = ProcessRateRecord.Rows[0]["ProcessRateDescription"].ToString();
                ProcessRateRate = Convert.ToDouble(ProcessRateRecord.Rows[0]["ProcessRateRate"]);
                ProcessRateMinimumCharge = Convert.ToDouble(ProcessRateRecord.Rows[0]["ProcessRateMinimumCharge"]);
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Process Rates Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_Process_Rates_Record(Int32 RateId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE ProcessRates SET ";
                if (ProcessRateDescription != ProcessRateRecord.Rows[0]["ProcessRateDescription"].ToString())
                {
                    StrSQL += "ProcessRateDescription = '" + Fix_Hyphon(ProcessRateDescription) + "', ";
                    hasChanged = true;
                }
                if (ProcessRateRate != Convert.ToDouble(ProcessRateRecord.Rows[0]["ProcessRateRate"]))
                {
                    StrSQL += "ProcessRateRate = " + ProcessRateRate.ToString() + ", ";
                    hasChanged = true;
                }
                if (ProcessRateMinimumCharge != Convert.ToDouble(ProcessRateRecord.Rows[0]["ProcessRateMinimumCharge"]))
                {
                    StrSQL += "ProcessRateMinimumCharge = " + ProcessRateMinimumCharge.ToString() + ", ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL = StrSQL.Substring(0, StrSQL.Length - 2) + " WHERE ProcessRateId = " + RateId.ToString();
                    SqlCommand cmdUpDate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpDate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Process Rates Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Process Rates Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_Process_Rates_Record(Int32 RateId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM ProcessRates WHERE ProcessRateId = " + RateId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Process Rates Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Process Rates Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_Process_Rates_List()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            ProcessRateRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM ProcessRates ORDER BY ProcessRateDescription";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    ProcessRateRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Process Rates List - " + ex.Message + " !";
            }

            return isSuccessful;
        }

        #endregion
        #endregion
        #region Part Related Tables
        #region Part Categories
        public Int32 PartCategoryId { get; set; }
        public String PartCategoryCode { get; set; }
        public String PartCategoryDescription { get; set; }
        public String PartCategoryAreaStockCalc { get; set; }
        public String PartCategoryAreaPriceCalc { get; set; }
        public Boolean PartCategoryIsUnitPart { get; set; }
        public Double PartCategoryPowderCoverage { get; set; }
        public Double PartCategoryPaintCoverage { get; set; }
        public Double PartCategoryPartLoading { get; set; }
        public String PartCategoryLastUpdate { get; set; }
        public DataTable PartCategoryRecord { get; set; } = new DataTable();
        public DataTable PartCategoryRecords { get; set; } = new DataTable();
        public Boolean Create_PartCategory_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE PartCategories (";
                StrSQL += "PartCategoryId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "PartCategoryCode nvarchar(20) NOT NULL, ";
                StrSQL += "PartCategoryDescription nvarchar(50), ";
                StrSQL += "PartCategoryAreaStockCalc nvarchar(2000), ";
                StrSQL += "PartCategoryAreaPriceCalc nvarchar(2000), ";
                StrSQL += "PartCategoryIsUnitPart bit, ";
                StrSQL += "PartCategoryPowderCoverage float, ";
                StrSQL += "PartCategoryPaintCoverage float, ";
                StrSQL += "PartCategoryPartLoading float, ";
                StrSQL += "PartCategoryLastUpdate nvarchar(50))";
                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Part Category Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_PartCategory_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO PartCategories (";
                StrSQL += "PartCategoryCode, ";
                StrSQL += "PartCategoryDescription, ";
                StrSQL += "PartCategoryAreaStockCalc, ";
                StrSQL += "PartCategoryAreaPriceCalc, ";
                StrSQL += "PartCategoryIsUnitPart, ";
                StrSQL += "PartCategoryPowderCoverage, ";
                StrSQL += "PartCategoryPaintCoverage, ";
                StrSQL += "PartCategoryPartLoading, ";
                StrSQL += "PartCategoryLastUpdate) VALUES (";
                StrSQL += "'" + Fix_Hyphon(PartCategoryCode) + "', ";
                StrSQL += "'" + Fix_Hyphon(PartCategoryDescription) + "', ";
                StrSQL += "'" + Fix_Hyphon(PartCategoryAreaStockCalc) + "', ";
                StrSQL += "'" + Fix_Hyphon(PartCategoryAreaPriceCalc) + "', ";
                StrSQL += "'" + PartCategoryIsUnitPart.ToString() + "', ";
                StrSQL += PartCategoryPowderCoverage.ToString() + ", ";
                StrSQL += PartCategoryPaintCoverage.ToString() + ", ";
                StrSQL += PartCategoryPartLoading.ToString() + ", ";
                StrSQL += "'" + DateTime.Now.ToString() + "')";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Part Category Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Part Category Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_PartCategory_Record(Int32 pcId)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PartCategoryRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PartCategories WHERE PartCategoryId = " + pcId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PartCategoryRecord.Load(rdrGet);
                    isSuccessful = Gather_PartCategory_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Part Category Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Part Category Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_PartCategory_Record(Int32 pcId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PartCategoryRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PartCategories WHERE PartCategoryId = " + pcId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PartCategoryRecord.Load(rdrGet);
                    isSuccessful = Gather_PartCategory_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Part Category Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Part Category Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_PartCategory_Record(String pcCode)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PartCategoryRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PartCategories WHERE PartCategoryCode = '" + pcCode + "'";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PartCategoryRecord.Load(rdrGet);
                    isSuccessful = Gather_PartCategory_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Part Category Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Part Category Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_PartCategory_Record(String pcCode, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PartCategoryRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PartCategories WHERE PartCategoryCode = '" + pcCode + "'";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PartCategoryRecord.Load(rdrGet);
                    isSuccessful = Gather_PartCategory_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Part Category Record - " + GET_ERROR;
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Part Category Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Gather_PartCategory_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                PartCategoryId = Convert.ToInt32(PartCategoryRecord.Rows[0]["PartCatergoryId"]);
                PartCategoryCode = PartCategoryRecord.Rows[0]["PartCategoryCode"].ToString();
                PartCategoryDescription = PartCategoryRecord.Rows[0]["PartCategoryDescription"].ToString();
                PartCategoryAreaStockCalc = PartCategoryRecord.Rows[0]["PartCategoryAreaStockCalc"].ToString();
                PartCategoryAreaPriceCalc = PartCategoryRecord.Rows[0]["PartCategoryAreaPriceCalc"].ToString();
                PartCategoryIsUnitPart = Convert.ToBoolean(PartCategoryRecord.Rows[0]["PartCategoryIsUnitPart"]);
                PartCategoryPowderCoverage = Convert.ToDouble(PartCategoryRecord.Rows[0]["PartCategoryPowderCoverage"]);
                PartCategoryPaintCoverage = Convert.ToDouble(PartCategoryRecord.Rows[0]["PartCategoryPaintCoverage"]);
                PartCategoryPartLoading = Convert.ToDouble(PartCategoryRecord.Rows[0]["PartCategoryPartLoading"]);
                PartCategoryLastUpdate = PartCategoryRecord.Rows[0]["PartCategoryLastUpdate"].ToString();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Part Category Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Update_PartCategory_Record(Int32 pcId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;
            Boolean hasChanged = false;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "UPDATE PartCategories SET ";
                if (PartCategoryCode != PartCategoryRecord.Rows[0]["PartCategoryCode"].ToString())
                {
                    StrSQL += "PartCategoryCode = '" + Fix_Hyphon(PartCategoryCode) + "', ";
                    hasChanged = true;
                }
                if (PartCategoryDescription != PartCategoryRecord.Rows[0]["PartCategoryDescription"].ToString())
                {
                    StrSQL += "PartCategoryDescription = '" + Fix_Hyphon(PartCategoryDescription) + "', ";
                    hasChanged = true;
                }
                if (PartCategoryAreaStockCalc != PartCategoryRecord.Rows[0]["PartCategoryAreaStockCalc"].ToString())
                {
                    StrSQL += "PartCategoryAreaStockCalc = '" + Fix_Hyphon(PartCategoryAreaStockCalc) + "', ";
                    hasChanged = true;
                }
                if (PartCategoryAreaPriceCalc != PartCategoryRecord.Rows[0]["PartCategoryAreaPriceCalc"].ToString())
                {
                    StrSQL += "PartCategoryAreaPriceCalc = '" + Fix_Hyphon(PartCategoryAreaPriceCalc) + "', ";
                    hasChanged = true;
                }
                if (PartCategoryIsUnitPart = Convert.ToBoolean(PartCategoryRecord.Rows[0]["PartCategoryIsUnitPart"]))
                {
                    StrSQL += "PartCategoryIsUnitPart = '" + PartCategoryIsUnitPart.ToString() + "', ";
                    hasChanged = true;
                }
                if (PartCategoryPowderCoverage != Convert.ToDouble(PartCategoryRecord.Rows[0]["PartCategoryPowderCoverage"]))
                {
                    StrSQL += "PartCategoryPowderCoverage = " + PartCategoryPowderCoverage.ToString() + ", ";
                    hasChanged = true;
                }
                if (PartCategoryPaintCoverage != Convert.ToDouble(PartCategoryRecord.Rows[0]["PartCategoryPaintCoverage"]))
                {
                    StrSQL += "PartCategoryPaintCoverage = " + PartCategoryPaintCoverage.ToString() + ", ";
                    hasChanged = true;
                }
                if (PartCategoryPartLoading != Convert.ToDouble(PartCategoryRecord.Rows[0]["PartCategoryPartLoading"]))
                {
                    StrSQL += "PartCategoryPartLoading = " + PartCategoryPartLoading.ToString() + ", ";
                    hasChanged = true;
                }

                if (hasChanged == true)
                {
                    StrSQL += "PartCategoryLastUpdated = '" + Fix_Hyphon(DateTime.Now.ToString()) + "' WHERE PartCategoryId = " + pcId.ToString();
                    SqlCommand cmdUpdate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                    if (cmdUpdate.ExecuteNonQuery() != 1)
                    {
                        isSuccessful = false;
                        ErrorMessage = "Update Part Category Record - " + UPDATE_ERROR;
                    }
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Update Part Category Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Delete_PartCategory_Record(Int32 pcId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "DELETE FROM PartCategories WHERE PartCategoryId = " + pcId.ToString();
                SqlCommand cmdDelete = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdDelete.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Delete Part Category Record - " + DELETE_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Delete Part Category Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_PartCategory_List()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PartCategoryRecords.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PartCategoried ORDER BY PartCategoryCode";
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PartCategoryRecords.Load(rdrGet);
                }
                rdrGet.Close();
                cmdGet.Dispose();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Part Category List - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        #endregion
        #region Part Pricing Groups
        public Int32 PartPricingGroupId { get; set; }
        public String PartPricingGroupDescription { get; set; }
        public Double PartPricingGroupLoading { get; set; }
        public DataTable PartPricingGroupRecord { get; set; } = new DataTable();
        public DataTable PartPricingGroupRecords { get; set; } = new DataTable();
        public Boolean Create_PartPricingGroups_Table(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "CREATE TABLE PartPricingGroups (";
                StrSQL += "PartPricingGroupId bigint IDENTITY(1,1) NOT NULL, ";
                StrSQL += "PartPricingGroupDescription nvarchar(50), ";
                StrSQL += "PartPricingLoading float)";
                SqlCommand cmdCreate = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                cmdCreate.CommandTimeout = 1000000;
                cmdCreate.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Create Part Pricing Group Table - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Insert_PartPricingGroup_Record(SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                String StrSQL = "INSERT INTO PartPricingGroups (";
                StrSQL += "PartPricingGroupDescription, ";
                StrSQL += "PartPricingLoading) VALUES (";
                StrSQL += "'" + Fix_Hyphon(PartPricingGroupDescription) + "', ";
                StrSQL += PartPricingGroupLoading.ToString() + ")";
                SqlCommand cmdInsert = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                if (cmdInsert.ExecuteNonQuery() != 1)
                {
                    isSuccessful = false;
                    ErrorMessage = "Insert Part Pricing Group Record - " + INSERT_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Insert Part Pricing Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_PartPricingGroup_Record(Int32 ppgId)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PartPricingGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PartPricingGroups WHERE PartPricingGroupId = " + ppgId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PartPricingGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_PartPricingGroup_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Part Pricing Group Record - " + GET_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Part Pricing Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Get_PartPricingGroup_Record(Int32 ppgId, SqlTransaction TrnEnvelope)
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;
            PartPricingGroupRecord.Clear();

            try
            {
                String StrSQL = "SELECT * FROM PartPricingGroups WHERE PartPricingGroupId = " + ppgId.ToString();
                SqlCommand cmdGet = new SqlCommand(StrSQL, PCConnection, TrnEnvelope);
                SqlDataReader rdrGet = cmdGet.ExecuteReader();
                if (rdrGet.HasRows == true)
                {
                    PartPricingGroupRecord.Load(rdrGet);
                    isSuccessful = Gather_PartPricingGroup_Record();
                }
                else
                {
                    isSuccessful = false;
                    ErrorMessage = "Get Part Pricing Group Record - " + GET_ERROR;
                }
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Get Part Pricing Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }
        public Boolean Gather_PartPricingGroup_Record()
        {
            Boolean isSuccessful = true;

            ErrorMessage = string.Empty;

            try
            {
                PartPricingGroupId = Convert.ToInt32(PartPricingGroupRecord.Rows[0]["PartPricingGroupId"]);
                PartPricingGroupDescription = PartPricingGroupRecord.Rows[0]["PartPricingGroupDescription"].ToString();
                PartPricingGroupLoading = Convert.ToDouble(PartPricingGroupRecord.Rows[0]["PartPricingGroupLoading"]);
            }
            catch (Exception ex)
            {
                isSuccessful = false;
                ErrorMessage = "Gather Part Pricing Group Record - " + ex.Message + " !";
            }

            return isSuccessful;
        }


        #endregion
        #region Parts

        #endregion
        #endregion
        #region Data Base Test / Connection
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
                if (DBTable == "Suppliers")
                    isSuccessful = Create_Supplier_Table(TrnEnvelope);
                else if (DBTable == "SupplierContacts")
                    isSuccessful = Create_Supplier_Contacts_Table(TrnEnvelope);
                else if (DBTable == "SupplierPaintSeries")
                    isSuccessful = Create_Paint_Series_Table(TrnEnvelope);
                else if (DBTable == "SupplierProductGroups")
                    isSuccessful = Create_Supplier_Product_Groups_Table(TrnEnvelope);
                else if (DBTable == "SupplierPaintProducts")
                    isSuccessful = Create_Supplier_Paint_Products_Table(TrnEnvelope);
                else if (DBTable == "PaintTypes")
                    isSuccessful = Create_Paint_Type_Table(TrnEnvelope);
                else if (DBTable == "ColourGroups")
                    isSuccessful = Create_Colour_Group_Table(TrnEnvelope);
                else if (DBTable == "PaintFinishes")
                    isSuccessful = Create_Paint_Finish_Table(TrnEnvelope);
                else if (DBTable == "PaintFamily")
                    isSuccessful = Create_Paint_Family_Table(TrnEnvelope);
                else if (DBTable == "PaintPriceGroups")
                    isSuccessful = Create_Paint_PriceGroup_Table(TrnEnvelope);
                else if (DBTable == "ProcessRates")
                    isSuccessful = Create_Process_Rates_Table(TrnEnvelope);


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
            MyTables.Add("Suppliers");
            MyTables.Add("SupplierContacts");
            MyTables.Add("SupplierPaintSeries");
            MyTables.Add("SupplierProductGroups");
            MyTables.Add("SupplierPaintProducts");
            MyTables.Add("PaintTypes");
            MyTables.Add("ColourGroups");
            MyTables.Add("PaintFinishes");
            MyTables.Add("PaintFamily");
            MyTables.Add("PaintPriceGroups");
            MyTables.Add("ProcessRates");

            return MyTables;
        }
        #endregion
    }
} 
