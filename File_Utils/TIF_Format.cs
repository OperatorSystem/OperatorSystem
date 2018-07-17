using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace File_Utils
{
    class TIF_Format : ICheckDuplicate
    {
        int count = 0;
        public bool CheckFileFormat(string FileName)
        {
            if (FileName.Length == 35)
            {
                string FileType = FileName.Substring(0, 3);
                string SenderId = FileName.Substring(3, 6);
                string Year = FileName.Substring(9, 4);
                string Month = FileName.Substring(13, 2);
                string Day = FileName.Substring(15, 2);
                string Sequence = FileName.Substring(17, 4);
                string U1 = FileName.Substring(21, 1);
                string ReceiverId = FileName.Substring(22, 6);
                string U2 = FileName.Substring(28, 1);
                string Version = FileName.Substring(29, 6);
                if (FileType.Trim().Length == 3 && FileType == "TIF" && SenderId.Trim().Length == 6 && Year.Trim().Length == 4 && Month.Trim().Length == 2 && Day.Trim().Length == 2 && Sequence.Trim().Length == 4 && U1 == "_" && U1.Trim().Length == 1 && ReceiverId.Trim().Length == 6 && U2 == "_" && U2.Trim().Length == 1 && Version.Trim().Length == 6)
                {
                    bool check_duplicate_file = IsDuplicate(FileName);
                    if (check_duplicate_file == false)
                    {
                        if (SenderId == ReceiverId)
                        {
                            bool check_Correct_TC = CheckCorrectTC(SenderId);
                            if (check_Correct_TC == true)
                            {
                                return true;
                            }
                            else
                            {
                                System.Console.WriteLine("TC is not correct");
                                System.Console.ReadLine();
                                return false;
                            }
                        }
                        else
                        {
                            System.Console.WriteLine("Sender Identifier and Receiver Identifier not same.");
                            System.Console.ReadLine();
                            return false;
                        }
                    }
                    else
                    {
                        System.Console.WriteLine("File is duplicate");
                        System.Console.ReadLine();
                        return false;
                    }
                }
                else
                {
                    System.Console.WriteLine("File Format is Incorrect.");
                    System.Console.ReadLine();
                    return false;
                }
            }
            else
            {
                System.Console.WriteLine("File Format is Incorrect.");
                System.Console.ReadLine();
                return false;
            }
        }

        public bool IsDuplicate(string FileName)
        {
            bool result = false;
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = "Data Source=192.168.1.252;Initial Catalog=ATC_FerdeDev; user id=developer;password=Appcino123#;";
            conn.Open();
            SqlCommand command2 = new SqlCommand("select count(*) from ATC_TIF_LOG_FILE where TIF_FILE_NAME = '" + FileName + "' and FILE_STATUS = 'ACCEPTED'", conn);
            SqlDataReader dr = command2.ExecuteReader();
            if (dr.HasRows)
            {
                dr.Read();
                if (Convert.ToInt16(dr[0]) > 0)
                {
                    result = true;
                }
            }
            dr.Close();
            command2.Dispose();
            conn.Close();
            return result;
        }

        public bool CheckCorrectTC(string SenderId)
        {
            bool result = false;
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = "Data Source=192.168.1.252;Initial Catalog=ATC_FerdeDev; user id=developer;password=Appcino123#;";
            conn.Open();
            SqlCommand command2 = new SqlCommand("select ACTOR_ID from ATC_ACT_BODY_DATA where ACTOR_TYPE = '31' AND  ACTOR_NAME = 'Fastlandssamband Halsnøy AS   '", conn);
            SqlDataReader dr = command2.ExecuteReader();
            if (dr.HasRows)
            {
                dr.Read();
                if (dr[0].ToString() == SenderId)
                {
                    result = true;
                }
            }
            dr.Close();
            command2.Dispose();
            conn.Close();
            return result;

        }
        public bool IsValidate(string FileName)
        {
            return true;
        }
        public bool FileParse(string File_Path, string FileName)
        {
            DateTime FileDate = File.GetLastWriteTime(File_Path);
            int id = 0;
            DateTime StartDate = DateTime.Now;
            var watch = System.Diagnostics.Stopwatch.StartNew();
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = "Data Source=192.168.1.252;Initial Catalog=ATC_FerdeDev; user id=developer;password=Appcino123#;";
            string FilePath = "D:\\File\\";
            int LOG_ID = 0;

            try
            {
                string csvPath = File_Path;
                int it = 0;
                int i = 0;
                SqlDataReader dr;
                Object[] a = new Object[82];
                conn.Open();

                SqlCommand cmd2 = new SqlCommand("INSERT INTO ATC_TIF_LOG_FILE (TIF_FILE_NAME, NO_OF_ACCEPTED_BODY_LINES, NO_Of_REJECTED_BODY_LINES, TIME_ELAPSED, FILE_CREATION_DATE, FILE_STORAGE_LOCATION, PROGRESS_STATUS, START_DATE,END_DATE,FILE_STATUS) VALUES ('" + FileName + "'," + count + ",0,'',convert(datetime,'" + FileDate + "',105),'" + FilePath + "','IN PROGRESS',convert(datetime,'" + StartDate + "',105),'','') ", conn);
                cmd2.ExecuteNonQuery();
                cmd2.Dispose();

                SqlCommand command = new SqlCommand("select MAX(TIF_LOG_FILE_ID_PK) from ATC_TIF_LOG_FILE", conn);
                dr = command.ExecuteReader();
                if (dr.HasRows)
                {
                    dr.Read();
                    LOG_ID = Convert.ToInt16(dr[0]);
                }
                dr.Close();
                command.Dispose();

                FileStream fs = File.Open(csvPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

                int t = File.ReadLines(csvPath).Count();
                StreamReader sr = new StreamReader(fs);
                string line;
                DataTable dt = new DataTable();

                SqlCommand comd = new SqlCommand("select * from ATC_TIF_BODY_FORMAT order by TIF_BODY_FORMAT_ID_PK", conn);
                SqlDataAdapter sda = new SqlDataAdapter(comd);
                DataTable dtable = new DataTable();
                sda.Fill(dtable);

                string[] ar = new string[dtable.Rows.Count];
                string[] ar1 = new string[dtable.Rows.Count];
                string[] MandatoryInfo = new string[dtable.Rows.Count];
                string[] FIELDS = new string[dtable.Rows.Count];
                string[] BodyFormat = new string[dtable.Rows.Count];

                for (int b = 0; b < dtable.Rows.Count; b++)
                {
                    ar[b] = dtable.Rows[b]["START_INDEX"].ToString();
                    ar1[b] = dtable.Rows[b]["CHARACTER_LENGTH"].ToString();
                    MandatoryInfo[b] = dtable.Rows[b]["MANDATORY_INFO_BODY_ONLY"].ToString();
                    FIELDS[b] = dtable.Rows[b]["FIELD"].ToString();
                    BodyFormat[b] = dtable.Rows[b]["contains_data"].ToString();
                }
                comd.Dispose();

                int StartVal = 0;
                int EndVal = 10000;

                while (count < t - 2)
                {
                    while (StartVal < EndVal)
                    {
                        if ((line = sr.ReadLine()) != "" && line != null)
                        {
                            if (it == 0)
                            {
                                SqlCommand command1 = new SqlCommand("select * from ATC_TIF_HEADER_FOOTER_FORMAT where TIF_HEADER_FOOTER_FORMAT_ID_PK between 1 and 12 order by TIF_HEADER_FOOTER_FORMAT_ID_PK", conn);

                                dr = command1.ExecuteReader();

                                if (dr.HasRows)
                                {
                                    while (dr.Read())
                                    {
                                        int p = Convert.ToInt16(dr[2]);
                                        int s = Convert.ToInt16(dr[4]);
                                        a[i] = line.Substring(p, s);

                                        string RejectionReason = "00";

                                        if ((dr[7].ToString() == "Mandatory") && a[i].ToString().Trim() == "")
                                        {
                                            return false;
                                        }
                                        
                                        i++;
                                        it++;
                                    }
                                }
                                dr.Close();
                                command1.Dispose();
                                SqlCommand cmd = new SqlCommand("EXEC [SP_HEADER_FOOTER_INSERT] @TIF_LOG_FILE_ID_PK,@Header_Register_Identifier,@Sender_Identifier,@Receiver_Identifier,@File_Sequence,@Previous_File_Sequence,@Currency,@Number_of_records_in_body,@Credit_Or_Debit,@Number_of_transactions,@Moment_of_creation,@List_format_version,@Filler_Header,@End_of_header,@Footer_Register_Identifier,@Total_amount,@Filler_Footer,@End_of_footer", conn);
                                cmd.Parameters.AddWithValue("@TIF_LOG_FILE_ID_PK", LOG_ID);
                                cmd.Parameters.AddWithValue("@Header_Register_Identifier", Convert.ToInt16(a[0]));
                                cmd.Parameters.AddWithValue("@Sender_Identifier", a[1]);
                                cmd.Parameters.AddWithValue("@Receiver_Identifier", a[2]);
                                cmd.Parameters.AddWithValue("@File_Sequence", a[3]);
                                cmd.Parameters.AddWithValue("@Previous_File_Sequence", a[4]);
                                cmd.Parameters.AddWithValue("@Currency", a[5]);
                                cmd.Parameters.AddWithValue("@Number_of_records_in_body", Convert.ToInt64(a[6]));
                                cmd.Parameters.AddWithValue("@Credit_Or_Debit", a[7]);
                                cmd.Parameters.AddWithValue("@Number_of_transactions", Convert.ToInt64(a[8]));
                                cmd.Parameters.AddWithValue("@Moment_of_creation", Convert.ToInt64(a[9]));
                                cmd.Parameters.AddWithValue("@List_format_version", a[10]);
                                cmd.Parameters.AddWithValue("@Filler_Header", a[11]);
                                cmd.Parameters.AddWithValue("@End_of_header", "/");
                                cmd.Parameters.AddWithValue("@Footer_Register_Identifier", 0);
                                cmd.Parameters.AddWithValue("@Total_amount", 0);
                                cmd.Parameters.AddWithValue("@Filler_Footer", "");
                                cmd.Parameters.AddWithValue("@End_of_footer", "/");
                                cmd.ExecuteNonQuery();
                                cmd.Dispose();

                                SqlCommand command2 = new SqlCommand("select max(TIF_HEADER_FOOTER_DATA_ID_PK) from ATC_TIF_HEADER_FOOTER_DATA", conn);
                                dr = command2.ExecuteReader();
                                if (dr.HasRows)
                                {
                                    dr.Read();
                                    id = Convert.ToInt16(dr[0]);
                                }
                                dr.Close();
                                command2.Dispose();
                                Array.Clear(a, 0, a.Length);
                            }
                            else if (line.Length <= 106)
                            {
                                i = 0;
                                Array.Clear(a, 0, a.Length);
                                SqlCommand command1 = new SqlCommand("select * from ATC_TIF_HEADER_FOOTER_FORMAT where TIF_HEADER_FOOTER_FORMAT_ID_PK between 14 and 16 order by TIF_HEADER_FOOTER_FORMAT_ID_PK", conn);

                                dr = command1.ExecuteReader();

                                if (dr.HasRows)
                                {
                                    while (dr.Read())
                                    {
                                        int p = Convert.ToInt16(dr[2]);
                                        int s = Convert.ToInt16(dr[4]);
                                        a[i] = line.Substring(p, s);
                                        i++;
                                    }
                                }
                                dr.Close();
                                command1.Dispose();

                                SqlCommand cmd = new SqlCommand("update ATC_TIF_HEADER_FOOTER_DATA set Footer_Register_Identifier=@Footer_Register_Identifier, Total_amount=@Total_amount, Filler_Footer=@Filler_Footer, End_of_footer=@End_of_footer where TIF_HEADER_FOOTER_DATA_ID_PK = " + id + " ", conn);
                                cmd.Parameters.AddWithValue("@Footer_Register_Identifier", a[0]);
                                cmd.Parameters.AddWithValue("@Total_amount", a[1]);
                                cmd.Parameters.AddWithValue("@Filler_Footer", a[2]);
                                cmd.Parameters.AddWithValue("@End_of_footer", "/");
                                cmd.ExecuteNonQuery();
                                cmd.Dispose();
                            }
                            else
                            {

                                i = 0;
                                if (i == 0)
                                {
                                    a[i] = id;
                                    i++;
                                    a[i] = LOG_ID;
                                    i++;
                                }
                                int k = 0;
                                while (k < ar.Length)
                                {
                                    int p = Convert.ToInt16(ar[k]);
                                    int s = Convert.ToInt16(ar1[k]);
                                    a[i] = line.Substring(p, s);

                                    string Mandatory = MandatoryInfo[k];
                                    string RejectionReason = "";
                                    string q = a[i].ToString();
                                    var l = q.Trim().Length;

                                    var TIF_StationCode = a[16];
                                    var TIF_Lane_Identification = a[17];
                                    var Actor_id = a[5];

                                    if (FIELDS[k] == "License Plate number detected" && q.Trim() == "")
                                    {
                                        var TIC_Line = line.Substring(1, 808);
                                        SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                        cmd3.ExecuteNonQuery();
                                        cmd3.Dispose();
                                        i = -1;

                                        break;
                                    }
                                    else if ((Mandatory == "Mandatory") && q.Trim() == "")
                                    {
                                        var TIC_Line = line.Substring(1, 808);
                                        SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                        cmd3.ExecuteNonQuery();
                                        cmd3.Dispose();
                                        i = -1;

                                        break;
                                    }
                                    else if (FIELDS[k] == "Lane Identification")
                                    {
                                        int cnt = 0;
                                        RejectionReason = "08";
                                        SqlCommand command2 = new SqlCommand("SELECT COUNT(*) FROM ATC_TST_BODY_DATA WHERE ACTORID = '" + Actor_id + "' AND STATION_CODE = '" + TIF_StationCode + "' AND LANE_IDENTIFICATION = '" + TIF_Lane_Identification + "'", conn);
                                        dr = command2.ExecuteReader();
                                        if (dr.HasRows)
                                        {
                                            dr.Read();
                                            cnt = Convert.ToInt16(dr[0]);
                                        }
                                        dr.Close();
                                        command2.Dispose();
                                        if (cnt <= 0)
                                        {
                                            var TIC_Line = line.Substring(1, 808);
                                            SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                            cmd3.ExecuteNonQuery();
                                            cmd3.Dispose();
                                            i = -1;

                                            break;
                                        }
                                    }
                                    else if (BodyFormat[k] != "")
                                    {
                                        RejectionReason = "09";

                                        string sss = BodyFormat[k];

                                        if (FIELDS[k] == "Body Register Identifier" || FIELDS[k] == "Change of class indicator")
                                        {
                                            if (q != "1")
                                            {
                                                var TIC_Line = line.Substring(1, 808);
                                                SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                                cmd3.ExecuteNonQuery();
                                                cmd3.Dispose();
                                                i = -1;

                                                break;
                                            }
                                        }

                                        else if (FIELDS[k] == "Type of Transit")
                                        {
                                            string[] arr = BodyFormat[k].Split(',');
                                            var End = q.Substring(1, 1);
                                            //SDfsdf
                                            var Format = arr.Select(x => new { validate = (q.StartsWith(x.Substring(0, 1)) && (Convert.ToInt32(End) >= 0 && Convert.ToInt32(End) <= 8)) }).Any(x => x.validate == true);
                                            //    arr.Select(x => new { x.StartsWith(q)  })
                                            if (Format == false)
                                            {
                                                var TIC_Line = line.Substring(1, 808);
                                                SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                                cmd3.ExecuteNonQuery();
                                                cmd3.Dispose();
                                                i = -1;

                                                break;
                                            }
                                        }

                                        else if (BodyFormat[k] == "YYYYMMDDHHmmss")
                                        {
                                            try
                                            {
                                                DateTime dt1 = DateTime.ParseExact(q, "yyyyMMddHHmmss", null);
                                            }
                                            catch (Exception ex)
                                            {
                                                var TIC_Line = line.Substring(1, 808);
                                                SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                                cmd3.ExecuteNonQuery();
                                                cmd3.Dispose();
                                                i = -1;

                                                break;
                                            }
                                        }
                                        else if (FIELDS[k] == "Currency")
                                        {
                                            string[] Curr = BodyFormat[k].Split(',');
                                            if (Array.IndexOf(Curr, q) <= 0)
                                            {
                                                var TIC_Line = line.Substring(1, 808);
                                                SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                                cmd3.ExecuteNonQuery();
                                                cmd3.Dispose();
                                                i = -1;

                                                break;
                                            }
                                        }

                                        else if (BodyFormat.Contains(","))
                                        {
                                            bool result = int.TryParse(q, out i);
                                            if (result == true)
                                            {
                                                string[] Curr = BodyFormat[k].Split(',');
                                                if (Array.IndexOf(Curr, q) <= 0)
                                                {
                                                    var TIC_Line = line.Substring(1, 808);
                                                    SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                                    cmd3.ExecuteNonQuery();
                                                    cmd3.Dispose();
                                                    i = -1;

                                                    break;
                                                }
                                            }
                                            else
                                            {
                                                var TIC_Line = line.Substring(1, 808);
                                                SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                                cmd3.ExecuteNonQuery();
                                                cmd3.Dispose();
                                                i = -1;

                                                break;
                                            }
                                        }
                                        else if (BodyFormat.Contains("-"))
                                        {

                                            bool result = int.TryParse(q, out i);
                                            if (result == true)
                                            {
                                                string[] Curr = BodyFormat[k].Split('-');
                                                if (Convert.ToInt16(q) < Convert.ToInt16(Curr[0]) && Convert.ToInt16(q) > Convert.ToInt16(Curr[1]))
                                                {
                                                    var TIC_Line = line.Substring(1, 808);
                                                    SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                                    cmd3.ExecuteNonQuery();
                                                    cmd3.Dispose();
                                                    i = -1;

                                                    break;
                                                }
                                            }
                                            else
                                            {
                                                var TIC_Line = line.Substring(1, 808);
                                                SqlCommand cmd3 = new SqlCommand("INSERT INTO ATC_TIC_BODY_DATA (BODY_REGISTER_IDENTIFIER, COPY_OF_TIF_BODY_LINE, REASON_OF_REJECTION_EASYGO, TIF_LOG_FILE_ID_PK) VALUES (1,'" + TIC_Line + "', '" + RejectionReason + "', " + LOG_ID + ") ", conn);
                                                cmd3.ExecuteNonQuery();
                                                cmd3.Dispose();
                                                i = -1;

                                                break;
                                            }

                                        }
                                    }
                                    i++;
                                    k++;
                                }
                                count++;
                                if (dt.Columns.Count == 0)
                                {
                                    for (int j = 0; j <= i; j++)
                                        //if (j <= 2)
                                        //{
                                        dt.Columns.Add(new DataColumn("Column" + j));
                                    //}
                                    //else
                                    //{
                                    //    dt.Columns.Add(new DataColumn("Column" + j, typeof(string)));
                                    //}
                                }

                                if (i > 0)
                                {
                                    dt.Rows.Add(a);
                                }
                            }
                            StartVal++;
                        }
                        else
                        {
                            StartVal = EndVal;
                        }
                    }
                    EndVal = EndVal + 10000;

                    SqlBulkCopy bulkCopy = new SqlBulkCopy(conn);

                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(0), "TIF_HEADER_FOOTER_DATA_ID_PK");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(1), "TIF_LOG_FILE_ID_PK");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(2), "BODY_REGISTER_IDENTIFIER");
                    bulkCopy.ColumnMappings.Add(3, "TYPE_OF_TRANSIT");
                    bulkCopy.ColumnMappings.Add(4, "PERSONAL_ACCOUNT_NUMBER");
                    bulkCopy.ColumnMappings.Add(5, "ACTOR_ID_OF_TSP");
                    bulkCopy.ColumnMappings.Add(6, "CONTRACT_AUTHENTICATOR");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(7), "DATE_AND_TIME_OF_THE_ENTRY_TRANSIT");
                    bulkCopy.ColumnMappings.Add(8, "ENTRY_STATION_COUNTRY_CODE");
                    bulkCopy.ColumnMappings.Add(9, "ENTRY_STATION_ACTOR_ID");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(10), "ENTRY_STATION_NETWORK_CODE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(11), "ENTRY_STATION_STATION_CODE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(12), "DATE_AND_TIME_OF_THE_EXIT_TRANSIT");
                    bulkCopy.ColumnMappings.Add(13, "EXIT_STATION_COUNTRY_CODE");
                    bulkCopy.ColumnMappings.Add(14, "EXIT_STATION_ACTOR_ID");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(15), "EXIT_STATION_NETWORK_CODE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(16), "EXIT_STATION_STATION_CODE");
                    bulkCopy.ColumnMappings.Add(17, "LANE_IDENTIFICATION");
                    bulkCopy.ColumnMappings.Add(18, "TARIFF_CLASSIFICATION1");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(19), "VEHICLE_CLASS");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(20), "VEHICLE_DIMENSIONS");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(21), "VEHICLE_AXLES");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(22), "VEHICLE_AUTHENTICATOR");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(23), "FEE_VAT_EXCLUDED");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(24), "AMOUNT_OF_VAT");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(25), "FEE_VAT_INCLUDED");
                    bulkCopy.ColumnMappings.Add(26, "CURRENCY");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(27), "APPLIED_VAT_RATE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(28), "TRANSACTION_RESULT");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(29), "OBE_STATUS");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(30), "LEVEL_OF_SECURITY");
                    bulkCopy.ColumnMappings.Add(31, "PAYMENT_AGGREGATION_NUMBER");
                    bulkCopy.ColumnMappings.Add(32, "TEXT_DESCRIPTION");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(33), "TYPE_OF_TOLL_LANE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(34), "TYPE_OF_OPERATION_OF_THE_SPECIFIC_LANE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(35), "MODE_OF_OPERATION_OK_DEGRADED");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(36), "MANUAL_ENTRY_CLASSIFICATION");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(37), "CHANGE_OF_CLASS_INDICATOR");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(38), "PRE_DAC_CLASS_AUTOMATIC_DETECTION_EXIT");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(39), "POST_DAC_EXIT");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(40), "DAC_ENTRY");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(41), "HEIGHT_DETECTOR_ENTRY");
                    bulkCopy.ColumnMappings.Add(42, "FOR_FUTURE_USE");
                    bulkCopy.ColumnMappings.Add(43, "LICENSE_PLATE_NUMBER_DECLARED");
                    bulkCopy.ColumnMappings.Add(44, "NATIONALITY_OF_LICENSE_PLATE_NUMBER_DECLARED");
                    bulkCopy.ColumnMappings.Add(45, "LICENSE_PLATE_NUMBER_DETECTED");
                    bulkCopy.ColumnMappings.Add(46, "NATIONALITY_OF_LICENSE_PLATE_NUMBER_DETECTED");
                    bulkCopy.ColumnMappings.Add(47, "ID_OF_NAT_LIST_USED_FOR_VALIDATION");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(48), "VIDEO_PICTURE_COUNTER");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(49), "KEY_GENERATION");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(50), "RND_1");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(51), "TRANSACTION_TIME");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(52), "TRANSACTION_COUNTER");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(53), "RND_2");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(54), "MAC1_STATUS");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(55), "MAC1");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(56), "MAC2STATUS");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(57), "MAC2");
                    bulkCopy.ColumnMappings.Add(58, "ADDITIONAL_QA_DATA");
                    bulkCopy.ColumnMappings.Add(59, "FOR_LOCAL_USE");
                    bulkCopy.ColumnMappings.Add(60, "EMISSION_CLASS");
                    bulkCopy.ColumnMappings.Add(61, "TARIFF_CLASSIFICATION2");
                    bulkCopy.ColumnMappings.Add(62, "VEHICLE_SPECIAL_CLASSIFICATION");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(63), "LANE_MODE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(64), "SIGNAL_CODE_BITMAP");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(65), "APPLIED_DISCOUNT_RATE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(66), "PRICING_CORRECTION");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(67), "SIGNAL_CODE");
                    bulkCopy.ColumnMappings.Add(68, "APPLIED_PRICING_RULES");
                    bulkCopy.ColumnMappings.Add(69, "CONTEXT_MARK");
                    bulkCopy.ColumnMappings.Add(70, "OBE_ID");
                    bulkCopy.ColumnMappings.Add(71, "TSP_AUTHENTICATOR");
                    bulkCopy.ColumnMappings.Add(72, "RNDRSE");
                    bulkCopy.ColumnMappings.Add(73, "KEYREF_FOR_TSP_KEY");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(74), "INVOICE_TRANSACTION_AGGREGATION_NUMBER");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(75), "UTC_TIME_STAMP");
                    bulkCopy.ColumnMappings.Add(76, "TC_TRANSACTION_IDENTIFICATION");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(77), "EXTERNAL_COSTS_NOISE");
                    bulkCopy.ColumnMappings.Add(Convert.ToInt16(78), "EXTERNAL_COSTS_AIR");
                    bulkCopy.ColumnMappings.Add(79, "FILLER_BODY");
                    bulkCopy.ColumnMappings.Add(80, "END_OF_RECORD");
                    //bulkCopy.ColumnMappings.Add("", "STATION_NAME");
                    //bulkCopy.ColumnMappings.Add("", "LANE_NAME");

                    bulkCopy.DestinationTableName = "ATC_TIF_BODY_DATA";
                    bulkCopy.WriteToServer(dt);
                    dt.Clear();
                }
                LazyValidation(FileName);
                DateTime EndDate = DateTime.Now;
                watch.Stop();
                var elapsedTick = watch.Elapsed;
                String CurrDate = DateTime.Now.ToString();

                string Log = "File Name : " + FileName + Environment.NewLine + "No. of Header : 1 " + Environment.NewLine + "No. of Body : " + count + " " + Environment.NewLine + "No. of Footer : 1 " + Environment.NewLine + "Time Elapsed : " + elapsedTick + Environment.NewLine + "File Creation Date : " + FileDate + Environment.NewLine + "File Storage Location : " + FilePath + Environment.NewLine + Environment.NewLine;

                using (StreamWriter sw = File.AppendText(FilePath + "LogFile_Insert" + FileName))
                {
                    sw.WriteLine("{0}", Log);
                }

                cmd2 = new SqlCommand("UPDATE ATC_TIF_LOG_FILE SET TIME_ELAPSED = '" + elapsedTick + "', PROGRESS_STATUS = 'COMPLETED', END_DATE = convert(datetime,'" + EndDate + "',105), FILE_STATUS = 'ACCEPTED' WHERE TIF_FILE_NAME = '" + FileName + "' AND TIF_LOG_FILE_ID_PK = " + LOG_ID + " ", conn);
                cmd2.ExecuteNonQuery();
                cmd2.Dispose();
                conn.Close();
                return true;
            }
            catch (Exception ex)
            {
                DateTime EndDate = DateTime.Now;
                watch.Stop();
                var TimeElapsed = watch.Elapsed;
                string Log = "File Name : " + FileName + Environment.NewLine + "No. of Header : 1 " + Environment.NewLine + "No. of Body : " + count + " " + Environment.NewLine + "No. of Footer : 0 " + Environment.NewLine + "Time Elapsed : " + TimeElapsed + Environment.NewLine + "File Creation Date : " + FileDate + Environment.NewLine + "File Storage Location : " + FilePath + Environment.NewLine + "Error Message : " + ex.Message + Environment.NewLine + Environment.NewLine;

                using (StreamWriter w = File.AppendText(FilePath + "LogFile_Insert_Error_" + FileName))
                {
                    w.WriteLine("{0}", Log);
                }
                SqlCommand cmd2 = new SqlCommand("UPDATE ATC_TIF_LOG_FILE SET TIME_ELAPSED = '" + TimeElapsed + "', PROGRESS_STATUS = 'COMPLETED', END_DATE = convert(datetime,'" + EndDate + "',105) WHERE TIF_FILE_NAME = '" + FileName + "' AND TIF_LOG_FILE_ID_PK = " + LOG_ID + " ", conn);
                cmd2.ExecuteNonQuery();
                cmd2.Dispose();
                conn.Close();
                return false;
            }


        }
        public bool ParseHeaderFooter(string File_Path, string FileName)
        {
            DateTime FileDate = File.GetLastWriteTime(File_Path);
            DateTime StartDate = DateTime.Now;
            var watch = System.Diagnostics.Stopwatch.StartNew();

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = "Data Source=192.168.1.252;Initial Catalog=ATC_FerdeDev; user id=developer;password=Appcino123#;";
            string FilePath = "D:\\File\\";
            string csvPath = File_Path;
            string table = "";
            conn.Open();

            SqlCommand cmd2 = new SqlCommand("INSERT INTO ATC_TIF_LOG_FILE (TIF_FILE_NAME, NO_Of_ACCEPTED_BODY_LINES, NO_Of_REJECTED_BODY_LINES, TIME_ELAPSED, FILE_CREATION_DATE, FILE_STORAGE_LOCATION, PROGRESS_STATUS, START_DATE,END_DATE,FILE_STATUS) VALUES ('" + FileName + "',0,0,'',convert(datetime,'" + FileDate + "',105),'" + FilePath + "','IN PROCESS',convert(datetime,'" + StartDate + "',105),'','FULLY REJECTED') ", conn);
            //cmd2.ExecuteNonQuery();
            cmd2.Dispose();
            int LOG_ID = 0;
            SqlDataReader dr;

            try
            {
                SqlCommand command = new SqlCommand("select MAX(TIF_LOG_FILE_ID_PK) from ATC_TIF_LOG_FILE", conn);
                dr = command.ExecuteReader();
                if (dr.HasRows)
                {
                    dr.Read();
                    LOG_ID = Convert.ToInt16(dr[0]);
                }
                dr.Close();
                command.Dispose();

                string csvData = File.ReadAllText(csvPath);
                string[] arr;

                arr = csvData.Split(new char[] { '\n' });
                int lenght = arr.Length;
                string header = arr[0];
                string footer = arr[lenght - 2];
                DataTable dt = new DataTable();
                int i = 0;
                string[] a = new string[50];
                command = new SqlCommand("select * from ATC_TIF_HEADER_FOOTER_FORMAT order by TIF_HEADER_FOOTER_FORMAT_ID_PK", conn);
                dr = command.ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        int p = Convert.ToInt16(dr[2]);
                        int s = Convert.ToInt16(dr[4]);

                        if (dr[1].ToString() == "Footer Register Identifier")
                        {
                            table = "Footer";
                        }

                        if (table == "Footer")
                        {
                            a[i] = footer.Substring(p, s);
                        }
                        else
                        {
                            a[i] = header.Substring(p, s);
                        }
                        i++;
                    }
                }
                dr.Close();
                command.Dispose();

                SqlCommand cmd = new SqlCommand("EXEC [SP_HEADER_FOOTER_INSERT] @TIF_LOG_FILE_ID_PK, @Header_Register_Identifier, @Sender_Identifier,@Receiver_Identifier,@File_Sequence,@Previous_File_Sequence,@Currency,@Number_of_records_in_body,@Credit_Or_Debit,@Number_of_transactions,@Moment_of_creation,@List_format_version,@Filler_Header,@End_of_header,@Footer_Register_Identifier,@Total_amount,@Filler_Footer,@End_of_footer", conn);
                cmd.Parameters.AddWithValue("@TIF_LOG_FILE_ID_PK", LOG_ID);
                cmd.Parameters.AddWithValue("@Header_Register_Identifier", a[0]);
                cmd.Parameters.AddWithValue("@Sender_Identifier", a[1]);
                cmd.Parameters.AddWithValue("@Receiver_Identifier", a[2]);
                cmd.Parameters.AddWithValue("@File_Sequence", a[3]);
                cmd.Parameters.AddWithValue("@Previous_File_Sequence", a[4]);
                cmd.Parameters.AddWithValue("@Currency", a[5]);
                cmd.Parameters.AddWithValue("@Number_of_records_in_body", a[6]);
                cmd.Parameters.AddWithValue("@Credit_Or_Debit", a[7]);
                cmd.Parameters.AddWithValue("@Number_of_transactions", a[8]);
                cmd.Parameters.AddWithValue("@Moment_of_creation", a[9]);
                cmd.Parameters.AddWithValue("@List_format_version", a[10]);
                cmd.Parameters.AddWithValue("@Filler_Header", a[11]);
                cmd.Parameters.AddWithValue("@End_of_header", "/");
                cmd.Parameters.AddWithValue("@Footer_Register_Identifier", a[13]);
                cmd.Parameters.AddWithValue("@Total_amount", a[14]);
                cmd.Parameters.AddWithValue("@Filler_Footer", a[15]);
                cmd.Parameters.AddWithValue("@End_of_footer", "/");
                cmd.ExecuteNonQuery();
                cmd.Dispose();
                DateTime EndDate = DateTime.Now;
                watch.Stop();
                var elapsedTick = watch.Elapsed;

                string Log = "File Name : " + FileName + Environment.NewLine + "No. of Header : 1 " + Environment.NewLine + "No. of Body : 0 " + Environment.NewLine + "No. of Footer : 1 " + Environment.NewLine + "Time Elapsed : " + elapsedTick + Environment.NewLine + "File Creation Date : " + FileDate + Environment.NewLine + "File Storage Location : " + FilePath + Environment.NewLine + Environment.NewLine;

                using (StreamWriter sw = File.AppendText(FilePath + "LogFile_Insert" + FileName))
                {
                    sw.WriteLine("{0}", Log);
                }

                cmd2 = new SqlCommand("UPDATE ATC_TIF_LOG_FILE SET TIME_ELAPSED = '" + elapsedTick + "', PROGRESS_STATUS = 'COMPLETED', END_DATE = convert(datetime,'" + EndDate + "',105) WHERE TIF_FILE_NAME = '" + FileName + "' AND TIF_LOG_FILE_ID_PK = " + LOG_ID + " ", conn);
                cmd2.ExecuteNonQuery();
                cmd2.Dispose();
                conn.Close();
                return true;

            }
            catch (Exception ex)
            {
                DateTime EndDate = DateTime.Now;
                watch.Stop();
                var TimeElapsed = watch.Elapsed;
                string Log = "File Name : " + FileName + Environment.NewLine + "No. of Header : 1 " + Environment.NewLine + "No. of Body : 0" + Environment.NewLine + "No. of Footer : 0 " + Environment.NewLine + "Time Elapsed : " + TimeElapsed + Environment.NewLine + "File Creation Date : " + FileDate + Environment.NewLine + "File Storage Location : " + FilePath + Environment.NewLine + "Error Message : " + ex.Message + Environment.NewLine + Environment.NewLine;

                using (StreamWriter w = File.AppendText(FilePath + "LogFile_Insert_Error_" + FileName))
                {
                    w.WriteLine("{0}", Log);
                }

                cmd2 = new SqlCommand("UPDATE ATC_TIF_LOG_FILE SET TIME_ELAPSED = '" + TimeElapsed + "', PROGRESS_STATUS = '" + ex.Message + "', END_DATE = convert(datetime,'" + EndDate + "',105) WHERE TIF_FILE_NAME = '" + FileName + "' AND TIF_LOG_FILE_ID_PK = " + LOG_ID + " ", conn);
                cmd2.ExecuteNonQuery();
                cmd2.Dispose();
                conn.Close();
                return false;
            }
        }
        public bool FileGenerate(string FileName)
        {
            return true;
        }

        public bool LazyValidation(string FileName)
        {
            //bool result = false;
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = "Data Source=192.168.1.252;Initial Catalog=ATC_FerdeDev; user id=developer;password=Appcino123#;";
            conn.Open();
            int Log_Id = 0;
            SqlCommand command2 = new SqlCommand("select TIF_LOG_FILE_ID_PK from ATC_TIF_LOG_FILE where TIF_FILE_NAME = '" + FileName + "' and FILE_STATUS = 'ACCEPTED'", conn);
            SqlDataReader dr = command2.ExecuteReader();
            if (dr.HasRows)
            {
                dr.Read();
                Log_Id = Convert.ToInt16(dr[0]);
            }
            dr.Close();
            command2.Dispose();

            int BodyCount = 0;
            command2 = new SqlCommand("select NUMBER_OF_RECORDS_IN_BODY from ATC_TIF_HEADER_FOOTER_DATA where TIF_LOG_FILE_ID_PK = " + Log_Id + "", conn);
            dr = command2.ExecuteReader();
            if (dr.HasRows)
            {
                dr.Read();
                BodyCount = Convert.ToInt16(dr[0]);
            }
            dr.Close();
            command2.Dispose();

            int TotalAmount = 0;
            command2 = new SqlCommand("select TOTAL_AMOUNT from ATC_TIF_HEADER_FOOTER_DATA where TIF_LOG_FILE_ID_PK = " + Log_Id + "", conn);
            dr = command2.ExecuteReader();
            if (dr.HasRows)
            {
                dr.Read();
                TotalAmount = Convert.ToInt32(dr[0]);
            }
            dr.Close();
            command2.Dispose();

            int FooterAmount = 0;
            command2 = new SqlCommand("select sum(FEE_VAT_EXCLUDED) from ATC_TIF_BODY_DATA where TIF_LOG_FILE_ID_PK = " + Log_Id + "", conn);
            dr = command2.ExecuteReader();
            if (dr.HasRows)
            {
                dr.Read();
                FooterAmount = Convert.ToInt32(dr[0]);
            }
            dr.Close();
            command2.Dispose();
            conn.Close();

            if (count != BodyCount && TotalAmount != FooterAmount)
            {
                Console.WriteLine("File Fully Rejected.");
                return false;
            }
           else
            {
                return true;
            }
        }

    }
}
