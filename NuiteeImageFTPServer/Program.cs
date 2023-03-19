using FluentFTP;
using System;
using System.IO;
using System.Net;
using Microsoft.VisualBasic.FileIO;
using System.Data.SqlClient;
using System.Linq;

class Program
{
    static void Main(string[] args)
    {
        //Enter Your Parameters Value Here
        var DownloadFilesLocation = ""; //Where you want to download files from FTPServer
        var dbConnection = "Data Source=(local);Initial Catalog=dbName;Persist Security Info=True;uid=userid;pwd=password;"; //Your DBConnection
        var Host = "ftp.example.com"; //FTP Server

        string fileDirectory = DownloadFilesLocation;
        string connectionString = dbConnection;
        SqlConnection conn = new SqlConnection(connectionString);
        conn.Open();
        if (!System.IO.Directory.Exists(fileDirectory))
        {
            System.IO.Directory.CreateDirectory(fileDirectory);
        }
        FtpClient ftp = new FtpClient(Host, "FTPUsername", "FTPPassword");
        ftp.Connect();

        FtpListItem[] items = ftp.GetListing(/*"/Directory's Name Optional"*/);
        var fileZipList = items.Where(a => a.Name.Contains("FileName.zip"));
        string zipPathUrl = "";
        foreach (var item in fileZipList)
        {
            zipPathUrl = fileDirectory + item.Name;
            ftp.DownloadFile(zipPathUrl, item.FullName, FtpLocalExists.Overwrite);
        }
        string[] fileList = System.IO.Directory.GetFiles(fileDirectory).Where(a => a.Contains(".csv")).ToArray();
        foreach (var file in fileList)
        {
            if (System.IO.File.Exists(file))
            {
                System.IO.File.Delete(file);
            }
        }
        ftp.Disconnect();
        Console.WriteLine("File downloaded successfully to {0}.", zipPathUrl);
        System.IO.Compression.ZipFile.ExtractToDirectory(zipPathUrl, fileDirectory);
        Console.WriteLine("File successfully extracted.");
        var filePaths = System.IO.Directory.GetFiles(fileDirectory).Where(a => a.Contains(".csv"));
        foreach (string filePath in filePaths)
        {
            using (TextFieldParser parser = new TextFieldParser(filePath))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.Delimiters = new string[] { ";" };
                parser.HasFieldsEnclosedInQuotes = true;
                while (!parser.EndOfData)
                {
                    string deleteQuery = "Delete from TableName";
                    SqlCommand cmd1 = new SqlCommand(deleteQuery, conn);
                    cmd1.ExecuteNonQuery();

                    string[] fields = parser.ReadFields();
                    foreach (var field in fields)
                    {
                        var flds = field.Split(';');
                        string insertQuery = "INSERT INTO TableName (Column1, Column2) VALUES (@Param1, @Param2)";
                        SqlCommand cmd = new SqlCommand(insertQuery, conn);
                        cmd.Parameters.AddWithValue("@Param1", flds[0]);
                        cmd.Parameters.AddWithValue("@Param2", flds[1]);
                        cmd.ExecuteNonQuery();
                    }
                    
                }
            }
            conn.Close();
        }

    }

}