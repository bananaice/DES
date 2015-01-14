using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using LogService;

namespace Simulation
{
    class Program
    {
        static void Main(string[] args)
        {
            QueueProcess qp = new QueueProcess(); //Assign value for queue

            
            LogService.LogService.ClearLog();
           
            string filepath = System.Environment.CurrentDirectory + "\\DES.txt";
            if (filepath != null)
            {
                //Read the input file for tasks
                StreamReader srReadFile = new StreamReader(filepath);


                while (!srReadFile.EndOfStream)
                {
                    string strReadLine = srReadFile.ReadLine();
                    string[] arr = strReadLine.Split(';');
                    //int offset = 0;
                    //if (arr[1] != null)
                    //{
                       // offset = Int16.Parse(arr[1]);

                    //}
                    DateTime sch = DateTime.Now;
                    
                    
                    //sch = sch.AddSeconds(offset);
                    double sch_offset = sch.ToOADate();

                    //LogService.LogService.WriteLog("Now time is :" + sch.Minute.ToString() + ":" + sch.Second.ToString());


                    string taskpath = string.Empty;

                    string taskdir = args[0];
                    DirectoryInfo d = new DirectoryInfo(taskdir);

                    FileInfo[] allFile = d.GetFiles();
                    foreach (FileInfo fi in allFile)
                    {
                        taskpath = fi.FullName;
                        Task mytask = new Task("W", "CNN", taskpath, null, 0, null);
                       // sch_offset = sch.ToOADate();
                        //mytask.Priority = sch.ToOADate(); //Set the scheduled time
                        qp.Enque(mytask);
                    }
                    /*
                    if (arr[2] == "W")
                    {
                        Task mytask = new Task("W", "CNN", taskpath, null, 0, null);
                        mytask.Priority = sch_offset; //Set the scheduled time
                        qp.Enque(mytask);
                    }
                    else if (arr[2] == "R")
                    {
                        Task mytask = new Task("R", "CNN", taskpath, null, 0, null);
                        mytask.Priority = sch_offset; // Set the scheduled time
                        qp.Enque(mytask);
                    }
                     */
                }


                // Close the stream
                srReadFile.Close();

                
                //Start the queue processing
                qp.QueueScanning();

            }
        }
    }
}
