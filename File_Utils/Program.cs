using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace File_Utils
{
    class Program
    {
        static void Main(string[] args)
        {

            string QueueName = "TIF_PARSING_QUEUE";
            var factory = new ConnectionFactory() { HostName = "192.168.1.252", Password = "Appcino123#", UserName = "admin", Port = 5672, RequestedHeartbeat = 60, RequestedConnectionTimeout = 2000, };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                const bool durable = true;
                //channel.ExchangeDeclare(exchange: "TC1", type: "Direct", autoDelete: false);
                channel.QueueDeclare(QueueName, durable, false, false, null);

                var consumer = new QueueingBasicConsumer(channel);
                // const bool autoAck = false;

                var data = channel.BasicGet(QueueName, false);
                if (data != null)
                {
                    var message = System.Text.Encoding.UTF8.GetString(data.Body);

                    channel.BasicConsume(QueueName, false, consumer);
                    int i = 1;
                    //while(true)
                    //{
                    //var ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                    //byte[] body = ea.Body;
                    //string message = System.Text.Encoding.UTF8.GetString(body);
                    if (message != "")
                    {
                        var watch = System.Diagnostics.Stopwatch.StartNew();
                       // FileStream fs = File.Open(message, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

                        System.Console.WriteLine(" [x] Processing {0}", message);
                        String[] file = message.Split(new char[] { '\\' });
                        string FileName = file[file.Length - 1];
                        DateTime FileDate = File.GetLastWriteTime(message);


                        SqlConnection conn = new SqlConnection();
                        conn.ConnectionString = "Data Source=192.168.1.252;Initial Catalog=ATC_FerdeDev; user id=developer;password=Appcino123#;";

                        if (FileName.Substring(0, 3) == "TIF")
                        {
                            TIF_Format TIF_obj = new TIF_Format();

                            bool check_File_Format = TIF_obj.CheckFileFormat(FileName);
                            if (check_File_Format == true)
                            {
                                //conn.Open();

                                //SqlCommand cmd2 = new SqlCommand("INSERT INTO ATC_TIF_LOG_FILE (TIF_FILE_NAME, NO_OF_ACCEPTED_BODY_LINES, NO_Of_REJECTED_BODY_LINES, TIME_ELAPSED, FILE_CREATION_DATE, FILE_STORAGE_LOCATION, PROGRESS_STATUS, START_DATE,END_DATE,FILE_STATUS) VALUES ('" + FileName + "',0,0,'','','','IN PROGRESS','','','ACCEPTED') ", conn);
                                //cmd2.ExecuteNonQuery();
                                //cmd2.Dispose();
                                //conn.Close();
                                //System.Console.WriteLine("File is Valid");
                                //System.Console.ReadLine();
                                TIF_obj.LazyValidation(FileName);
                                bool checkFileParse = TIF_obj.FileParse(message, FileName);
                                if (checkFileParse == true)
                                {
                                    TIF_obj.ParseHeaderFooter(message, FileName);
                                }
                                else
                                {

                                }


                                System.Console.WriteLine(" Processed ", message);
                            }
                            else
                            {
                                //conn.Open();

                                //SqlCommand cmd2 = new SqlCommand("INSERT INTO ATC_TIF_LOG_FILE (TIF_FILE_NAME, NO_OF_ACCEPTED_BODY_LINES, NO_Of_REJECTED_BODY_LINES, TIME_ELAPSED, FILE_CREATION_DATE, FILE_STORAGE_LOCATION, PROGRESS_STATUS, START_DATE,END_DATE,FILE_STATUS) VALUES ('" + FileName + "',0,0,'','','','IN PROGRESS','','','REJECTED') ", conn);
                                //cmd2.ExecuteNonQuery();
                                //cmd2.Dispose();
                                //conn.Close();
                                TIF_obj.ParseHeaderFooter(message, FileName);
                                // Console.Write("File Format Incorrect");
                            }


                        }
                        else if (FileName.Substring(0, 3) == "TIC")
                        {
                            TIC_Format TIC_obj = new TIC_Format();

                            bool check_duplicate_file = TIC_obj.IsDuplicate(FileName);
                            if (check_duplicate_file == false)
                            {
                                bool check_validate_file = TIC_obj.IsValidate(FileName);
                                if (check_validate_file == true)
                                {
                                    TIC_obj.FileParse(message, FileName);
                                    System.Console.WriteLine(" Processed ", message);
                                }
                                else
                                {
                                    Console.Write("File is not valid");
                                }
                            }
                            else
                            {
                                Console.Write("File Already Exist");
                            }
                        }
                        else
                        {
                            Console.WriteLine("File Format not recognize.");
                            Console.ReadLine();
                        }

                        DateTime EndDate = DateTime.Now;
                        watch.Stop();
                        var elapsedTick = watch.Elapsed;
                        String CurrDate = DateTime.Now.ToString();

                        string Log = "File Name : " + FileName + Environment.NewLine + "No. of Header : 1 " + Environment.NewLine + "No. of Body : " + Environment.NewLine + "No. of Footer : 1 " + Environment.NewLine + "Time Elapsed : " + elapsedTick + Environment.NewLine + "File Creation Date : " + FileDate + Environment.NewLine + "File Storage Location : " + message  + Environment.NewLine + Environment.NewLine;

                        using (StreamWriter sw = File.AppendText(message + "LogFileWithValidation" + FileName))
                        {
                            sw.WriteLine("{0}", Log);
                        }


                    }
                    //Console.Write("Received " + message);
                    channel.BasicAck(data.DeliveryTag, false);
                    i++;
                }

            }
        }
        //}
        //}
    }
}
